#!/usr/bin/env bash
#
# e2e/lima.sh — Linux/docker verification path for the StatsHouse e2e harness.
#
# Spins up a lima VM (Ubuntu, rootful Docker), installs a Go toolchain + C
# compiler in the guest, cross-compiles the harness binary on the Mac for
# linux/arm64, then runs it inside the VM over the same-path mounted checkout.
# Inside the VM the harness auto-detects docker and runs the FULL suite
# unchanged — every assertion path is byte-for-byte identical to `go run ./e2e`
# on the Mac (apple/container).
#
# Assumes Apple silicon: created without --arch, the guest is linux/arm64 (an
# Intel Mac would produce an x86_64 guest that cannot exec the harness binary).
#
# Usage:
#   bash e2e/lima.sh                 # full suite, all three clients, fresh stack
#   bash e2e/lima.sh --keep          # leave the stack up; curl the api from the Mac
#   bash e2e/lima.sh --client=go     # any harness flag passes through verbatim
#   bash e2e/lima.sh --conformance   # differential: ClickHouse vs duck-store side
#                                    # by side over one shared metadata, compared
#                                    # by decoded value (CI gate; fails non-zero)
#
# Idempotent: the VM and the guest toolchain are created once and reused; the
# harness binary is rebuilt incrementally.
#
# Design notes:
#   • template:docker-rootful, not the rootless `docker` template: the harness
#     dials container IPs for every probe and wire, and rootless Docker's bridge
#     lives in a network namespace the VM host cannot route to.
#   • Go runs in the guest because the harness builds the daemons with `go
#     build` (the metadata daemon needs CGO for sqlite, hence build-essential);
#     the harness binary itself is cross-compiled on the Mac.
#   • The guest $HOME differs from the Mac's, so the guest keeps its own
#     ~/.cache/statshouse-e2e build caches.
#   • Published guest ports are forwarded to 127.0.0.1 on the Mac, so with
#     --keep `curl http://127.0.0.1:10888/...` reaches the api.
#
set -euo pipefail

VM="e2e"
# Matches the host toolchain; satisfies go.mod (>= 1.24.11). Bump alongside the
# Mac `go` if the daemon build ever demands a newer language version.
GO_VERSION="1.26.5"
# Official sha256 of go${GO_VERSION}.linux-arm64.tar.gz, fetched from
# https://go.dev/dl/?mode=json&include=all . Pinned so the unauthenticated
# tarball download is integrity-checked before it is extracted over /usr/local.
GO_SHA256="fe4789e92b1f33358680864bbe8704289e7bb5fc207d80623c308935bd696d49"

# Repo root = this script's dir/.. (absolute). The Mac $HOME is mounted at the
# identical path in the guest, so REPO_ROOT is valid on both sides.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The cross-compiled harness binary lives under the Mac cache, which the guest
# sees at the same absolute path via the $HOME reverse-sshfs mount.
HARNESS_BIN="$HOME/.cache/statshouse-e2e/lima/e2e"

log() { printf '[lima.sh] %s\n' "$*" >&2; }

# Fail fast with actionable text instead of a confusing create-time failure.
command -v limactl >/dev/null 2>&1 || {
  log "ERROR: limactl not found on PATH — install Lima first (e.g. 'brew install lima')."
  exit 1
}

# --- 1. create the VM once (idempotent) -------------------------------------
if ! limactl list --format '{{.Name}}' 2>/dev/null | grep -qx "$VM"; then
  log "creating VM '$VM' (downloads Ubuntu ~1 GB; first run only)..."
  limactl create \
    --name="$VM" \
    --mount-writable \
    --set '.cpus = 8 | .memory = "16GiB"' \
    --tty=false \
    template:docker-rootful
fi

# --- 2. ensure it is running (idempotent) -----------------------------------
# In Lima v2 `create` only stages the instance; `start` performs first boot +
# the template's provisioning (Docker install, rootful setup).
if ! limactl list "$VM" --format '{{.Status}}' 2>/dev/null | grep -qi '^Running'; then
  log "starting VM '$VM'..."
  limactl start "$VM"
fi

# --- 3. provision the guest toolchain (idempotent): Go + C compiler ---------
# The harness shells out to `go build` for the four daemons; the metadata daemon
# needs CGO (sqlite), so a C compiler is required too.
log "ensuring guest toolchain (Go $GO_VERSION + build-essential)..."
limactl shell "$VM" -- bash -c '
set -e
GO_VERSION="$1"
GO_SHA256="$2"
# install Go if /usr/local/go is absent or its version differs from the pin
if [ ! -x /usr/local/go/bin/go ] || ! /usr/local/go/bin/go version 2>/dev/null | grep -q "go${GO_VERSION} "; then
  echo "[lima.sh:guest] installing Go ${GO_VERSION}"
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-arm64.tar.gz" -o /tmp/go.tgz
  # Verify the download against the pinned checksum before extracting (fail closed).
  echo "${GO_SHA256}  /tmp/go.tgz" | sha256sum -c -
  sudo rm -rf /usr/local/go
  sudo tar -C /usr/local -xzf /tmp/go.tgz
  rm -f /tmp/go.tgz
fi
# build-essential is needed for the metadata daemon CGO (sqlite) static link
# (a gcc-only guard would miss a fresh libc-dev). Guard on dpkg so repeat runs
# never invoke sudo: guest sudo is not passwordless in every provisioning
# context (e.g. no TTY), and apt-get is only needed once.
if ! dpkg -s build-essential >/dev/null 2>&1; then
  echo "[lima.sh:guest] installing build-essential"
  sudo apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential
fi
echo "[lima.sh:guest] $(/usr/local/go/bin/go version 2>&1)"
echo "[lima.sh:guest] gcc $(gcc -dumpversion 2>&1)"
' _ "$GO_VERSION" "$GO_SHA256"

# --- 4. cross-compile the harness on the Mac for linux/arm64 (idempotent) ---
# The e2e package is pure Go; CGO_ENABLED=0 cross-compiles cleanly on darwin.
# `go build` is incremental, so a no-change rerun is near-instant.
log "cross-compiling harness for linux/arm64 -> $HARNESS_BIN"
mkdir -p "$(dirname "$HARNESS_BIN")"
( cd "$REPO_ROOT" && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o "$HARNESS_BIN" ./e2e )

# --- 5. run the harness inside the VM, in the mounted checkout -------------
# --workdir makes repoRoot() (a CWD walk for go.mod + e2e/) resolve the mounted
# repo. PATH adds the guest Go so the harness's `go build` finds it; docker is
# already on the guest PATH (rootful), which is what makes auto-detection pick
# docker without --runtime. The binary is copied off the reverse-sshfs mount
# onto the guest local disk before exec (avoids exec-over-FUSE and is fast).
#
# NOTE: limactl shell runs in Cobra's non-interspersed mode, so --workdir (a
# flag) MUST precede the instance name — once the instance is seen, the rest is
# the guest command. A `--` right after the instance is NOT passed through:
# limactl strips that first `--` (cmd/limactl/shell.go), so the toolchain step
# above (`limactl shell "$VM" -- bash -c ...`) works as written.
#
# The "$@" passthrough forwards the caller's flags verbatim as separate argv
# elements; it assumes no flag value contains whitespace or shell metacharacters
# (the harness flags never do).
log "running harness in VM '$VM' (cwd=$REPO_ROOT); passthrough: $*"
limactl shell --workdir "$REPO_ROOT" "$VM" bash -c '
set -e
export PATH=/usr/local/go/bin:$PATH
cp -f "$1" /tmp/e2e-harness
chmod +x /tmp/e2e-harness
exec /tmp/e2e-harness "${@:2}"
' _ "$HARNESS_BIN" "$@"
