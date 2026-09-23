package main

// Unit tests for the harness's storage-backend swap: flag parsing, daemon
// spec selection (which aggregator binary a backend cross-compiles and how),
// and the per-backend daemon flag wiring (aggregator + api). Everything here
// is pure host-side construction — no container, no daemon, no network — so a
// wiring regression (a duck flag missing from the agg script, the api still
// pointed at ClickHouse, the wrong binary mounted) fails in milliseconds
// instead of after the full stack bring-up.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestDaemonSpecsFor pins the per-backend build list: clickhouse builds the
// usual four (one cgo daemon: metadata), duck swaps only the aggregator for
// the duckdb-tagged static-cgo build cached under its own name, and the other
// three daemons are byte-identical between backends.
func TestDaemonSpecsFor(t *testing.T) {
	ch := daemonSpecsFor(backendClickHouse)
	if len(ch) != 4 {
		t.Fatalf("clickhouse: got %d specs, want 4", len(ch))
	}
	if len(ch) != len(daemonCmds) {
		t.Fatalf("clickhouse: got %d specs, want %d (the default list)", len(ch), len(daemonCmds))
	}
	for i := range ch {
		if ch[i] != daemonCmds[i] {
			t.Errorf("clickhouse: spec %d differs from the default list: %+v vs %+v", i, ch[i], daemonCmds[i])
		}
	}

	duck := daemonSpecsFor(backendDuck)
	if len(duck) != 4 {
		t.Fatalf("duck: got %d specs, want 4", len(duck))
	}
	var agg *daemonSpec
	for i := range duck {
		if duck[i].pkg == "./cmd/statshouse-agg" {
			agg = &duck[i]
		}
	}
	if agg == nil {
		t.Fatal("duck: no aggregator spec found")
	}
	if agg.bin != "statshouse-agg-duck" {
		t.Errorf("duck: agg bin %q, want statshouse-agg-duck (own cache name so the two aggs never collide)", agg.bin)
	}
	if !agg.cgo || !agg.duckDB {
		t.Errorf("duck: agg spec %+v must be cgo+duckDB (duckdb build tag + verified static link)", *agg)
	}
	for i := range duck {
		if duck[i].pkg == "./cmd/statshouse-agg" {
			continue
		}
		if duck[i] != ch[i] {
			t.Errorf("duck: spec %d (%s) differs from clickhouse: %+v vs %+v — only the aggregator may change with the backend",
				i, duck[i].bin, duck[i], ch[i])
		}
	}
}

func TestAggBinName(t *testing.T) {
	if got := aggBinName(backendClickHouse); got != "statshouse-agg" {
		t.Errorf("aggBinName(clickhouse) = %q, want statshouse-agg", got)
	}
	if got := aggBinName(backendDuck); got != "statshouse-agg-duck" {
		t.Errorf("aggBinName(duck) = %q, want statshouse-agg-duck", got)
	}
}

// findSpec returns the spec whose pkg matches, from the backend's build list.
func findSpec(t *testing.T, backend storageBackend, pkg string) daemonSpec {
	t.Helper()
	for _, d := range daemonSpecsFor(backend) {
		if d.pkg == pkg {
			return d
		}
	}
	t.Fatalf("no spec for %q under %s", pkg, backend)
	return daemonSpec{}
}

// TestDuckAggSpecCrossCompileFlags asserts the duck aggregator builds with the
// duckdb tag and the verified static-link flags embedded in buildOneDaemon's
// recipe. The extldflags computation itself needs a real toolchain and is
// covered separately (TestDuckDBExtLDFlags); here the spec the build branches
// on is what matters.
func TestDuckAggSpecCrossCompileFlags(t *testing.T) {
	agg := findSpec(t, backendDuck, "./cmd/statshouse-agg")
	if !agg.duckDB {
		t.Fatal("the duck agg spec must carry duckDB (the -tags duckdb + static-link path in buildOneDaemon)")
	}
	// osusergo is load-bearing for the static link: without it the cgo
	// getgrnam cannot resolve even "root" and the agg dies at ChangeUserGroup
	for _, tag := range []string{"duckdb", "osusergo"} {
		if !strings.Contains(" "+duckBuildTags+" ", " "+tag+" ") {
			t.Fatalf("duckBuildTags %q must carry %q", duckBuildTags, tag)
		}
	}
	plain := findSpec(t, backendClickHouse, "./cmd/statshouse-agg")
	if plain.duckDB || plain.cgo {
		t.Fatalf("the clickhouse agg spec %+v must stay the pure-Go CGO_ENABLED=0 build", plain)
	}
}

// TestDuckDBExtLDFlags exercises the verified static-link flag computation
// against a real compiler: the flags must contain the whole-archive pthread
// recipe with the archive resolved by explicit path. A toolchain that has no
// libpthread.a at all (e.g. Apple clang, where pthread lives in libSystem)
// answers with the bare-name echo-back, which must surface as an error — the
// production path only ever passes a linux cross-compiler, which ships the
// archive. Skipped when no compiler is on PATH (not the machine that runs the
// harness).
func TestDuckDBExtLDFlags(t *testing.T) {
	cc := ""
	for _, cand := range []string{"cc", "clang", "gcc"} {
		if _, err := exec.LookPath(cand); err == nil {
			cc = cand
			break
		}
	}
	if cc == "" {
		t.Skip("no C compiler on PATH to resolve libpthread.a")
	}

	// A CC that cannot resolve the archive (or does not exist) must fail the
	// build loudly rather than emit a broken link line — no real toolchain
	// needed for either check.
	if _, err := duckDBExtLDFlags("definitely-not-a-compiler"); err == nil {
		t.Error("duckDBExtLDFlags with a nonexistent CC must fail")
	}

	// gcc's cannot-resolve behaviour is to echo the argument back unchanged —
	// a bare file name with no directory, which passes a suffix check and
	// would ride into the link flags as a bogus relative path. It must be
	// rejected as a broken toolchain.
	echoBack := filepath.Join(t.TempDir(), "cc-echo")
	if err := os.WriteFile(echoBack, []byte("#!/bin/sh\necho libpthread.a\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := duckDBExtLDFlags(echoBack); err == nil {
		t.Error("duckDBExtLDFlags must reject the bare-name echo-back of libpthread.a")
	}

	flags, err := duckDBExtLDFlags(cc)
	if err != nil {
		// legitimate on a toolchain without libpthread.a; everything below
		// assumes one that ships it
		t.Skipf("%s cannot resolve libpthread.a (%v) — not a toolchain this recipe applies to", cc, err)
	}
	for _, want := range []string{"-static", "-Wl,--allow-multiple-definition", "-Wl,--whole-archive", "-Wl,--no-whole-archive"} {
		if !strings.Contains(flags, want) {
			t.Errorf("flags %q missing %s", flags, want)
		}
	}
	if !strings.Contains(flags, "libpthread.a") {
		t.Errorf("flags %q must pass the pthread archive by explicit path, not -lpthread", flags)
	}
}

func testStackOpts(backend storageBackend) daemonStackOpts {
	return daemonStackOpts{
		network:      "e2e-testnet",
		chIP:         "10.77.0.9",
		binDir:       "/cache/bin",
		runID:        "test",
		rpcKeyPath:   "/tmp/key",
		apiStaticDir: "/static",
		staticMount:  apiStaticMount,
		backend:      backend,
	}
}

// TestAggRunScriptDuckFlags pins the duck aggregator's entrypoint: the duck
// block is present with the store dir, the shard is named locally (there is no
// ClickHouse cluster to autodetect from), and no --kh is passed.
func TestAggRunScriptDuckFlags(t *testing.T) {
	script := aggRunScript(testStackOpts(backendDuck), "10.77.0.2")
	for _, want := range []string{
		"--storage-backend=duck",
		"--duck-store-dir=" + duckStoreMount,
		"--local-shard=1",
		"mkdir -p /cache " + duckStoreMount,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("duck agg script missing %q\nscript:\n%s", want, script)
		}
	}
	if strings.Contains(script, "--kh=") {
		t.Errorf("duck agg script must not pass --kh (duck validation rejects ClickHouse addresses)\nscript:\n%s", script)
	}
}

// TestAggRunScriptSharedFlags pins the flags both backends share verbatim —
// the known e2e hazards (insert sampler, receive-budget warming, agent
// sampling) must be neutralized identically under duck, whose write path
// rides the same machinery.
func TestAggRunScriptSharedFlags(t *testing.T) {
	for _, backend := range []storageBackend{backendClickHouse, backendDuck} {
		script := aggRunScript(testStackOpts(backend), "10.77.0.2")
		for _, want := range []string{
			"--agg-addr=0.0.0.0:" + strconv.Itoa(aggPort),
			"--metadata-addr=10.77.0.2:" + strconv.Itoa(metaPort),
			"--receive-budget-warming=0",
			"--disable-receive-sample-budget",
			"--insert-budget=100000000",
			"--min-insert-budget=100000000",
			"--cluster-shards-addrs=${AGG_IP}:" + strconv.Itoa(aggPort),
			"--auto-create",
		} {
			if !strings.Contains(script, want) {
				t.Errorf("%s agg script missing %q\nscript:\n%s", backend, want, script)
			}
		}
	}
}

// TestAggRunScriptClickHouseFlags pins the clickhouse aggregator's entrypoint:
// the ClickHouse address is present and no duck flag leaks in.
func TestAggRunScriptClickHouseFlags(t *testing.T) {
	script := aggRunScript(testStackOpts(backendClickHouse), "10.77.0.2")
	if !strings.Contains(script, "--kh=10.77.0.9:8123") {
		t.Errorf("clickhouse agg script missing --kh=<ch-ip>:8123\nscript:\n%s", script)
	}
	for _, banned := range []string{"--storage-backend", "--duck-"} {
		if strings.Contains(script, banned) {
			t.Errorf("clickhouse agg script must not contain %q\nscript:\n%s", banned, script)
		}
	}
}

// TestAPIDaemonFlagsDuck pins the duck api wiring: the api reads through the
// aggregator (the single shard the stack runs) instead of ClickHouse.
func TestAPIDaemonFlagsDuck(t *testing.T) {
	flags := strings.Join(apiDaemonFlags(testStackOpts(backendDuck), "10.77.0.2", "10.77.0.3"), " ")
	for _, want := range []string{
		"--storage-backend=duck",
		"--duck-shard-addrs=10.77.0.3:" + strconv.Itoa(aggPort),
	} {
		if !strings.Contains(flags, want) {
			t.Errorf("duck api flags missing %q\ngot: %s", want, flags)
		}
	}
	if strings.Contains(flags, "--clickhouse-v2-addrs") {
		t.Errorf("duck api flags must not address ClickHouse\ngot: %s", flags)
	}
}

// TestAPIDaemonFlagsShared pins the flags both backends share, and the
// clickhouse storage wiring (three-replica cluster shape, no duck flags).
func TestAPIDaemonFlagsSharedAndClickHouse(t *testing.T) {
	for _, backend := range []storageBackend{backendClickHouse, backendDuck} {
		flags := strings.Join(apiDaemonFlags(testStackOpts(backend), "10.77.0.2", "10.77.0.3"), " ")
		for _, want := range []string{
			"--local-mode",
			"--insecure-mode",
			"--listen-addr=0.0.0.0:" + strconv.Itoa(apiPort),
			"--listen-rpc-addr=0.0.0.0:" + strconv.Itoa(apiRPCPort),
			"--metadata-addr=10.77.0.2:" + strconv.Itoa(metaPort),
			"--available-shards=1",
			"--cache-dir=/cache",
			"--rpc-crypto-path=" + rpcKeyMount,
			"--static-dir=" + apiStaticMount,
		} {
			if !strings.Contains(flags, want) {
				t.Errorf("%s api flags missing %q\ngot: %s", backend, want, flags)
			}
		}
	}
	chFlags := strings.Join(apiDaemonFlags(testStackOpts(backendClickHouse), "10.77.0.2", "10.77.0.3"), " ")
	if want := "--clickhouse-v2-addrs=10.77.0.9:9000,10.77.0.9:9000,10.77.0.9:9000"; !strings.Contains(chFlags, want) {
		t.Errorf("clickhouse api flags missing %q\ngot: %s", want, chFlags)
	}
	for _, banned := range []string{"--storage-backend", "--duck-"} {
		if strings.Contains(chFlags, banned) {
			t.Errorf("clickhouse api flags must not contain %q\ngot: %s", banned, chFlags)
		}
	}
}
