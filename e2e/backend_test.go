package main

// Host-only unit tests for the storage-backend wiring (daemon specs, agg and
// api flags), so a wiring regression fails in milliseconds, not after bring-up.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestDaemonSpecsFor: duck swaps only the aggregator (duckdb-tagged static-cgo
// build under its own cache name); the other daemons are identical.
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

func TestDuckAggSpecCrossCompileFlags(t *testing.T) {
	agg := findSpec(t, backendDuck, "./cmd/statshouse-agg")
	if !agg.duckDB {
		t.Fatal("the duck agg spec must carry duckDB (the -tags duckdb + static-link path in buildOneDaemon)")
	}
	// osusergo: without it the static cgo getgrnam cannot resolve even "root"
	// and the agg dies at ChangeUserGroup
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

// TestDuckDBExtLDFlags runs the static-link flag computation against a real
// compiler. A toolchain without libpthread.a (e.g. Apple clang) must error;
// production only passes a linux cross-compiler, which ships it.
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

	if _, err := duckDBExtLDFlags("definitely-not-a-compiler"); err == nil {
		t.Error("duckDBExtLDFlags with a nonexistent CC must fail")
	}

	// gcc echoes an unresolvable name back bare, which would pass a suffix
	// check and ride into the link flags as a bogus relative path.
	echoBack := filepath.Join(t.TempDir(), "cc-echo")
	if err := os.WriteFile(echoBack, []byte("#!/bin/sh\necho libpthread.a\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := duckDBExtLDFlags(echoBack); err == nil {
		t.Error("duckDBExtLDFlags must reject the bare-name echo-back of libpthread.a")
	}

	flags, err := duckDBExtLDFlags(cc)
	if err != nil {
		// legitimate on a toolchain without libpthread.a
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

// TestAggRunScriptDuckFlags: the shard is named locally since there is no
// ClickHouse cluster to autodetect it from.
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

// TestAggRunScriptSharedFlags: the e2e sampling/budget hazards must be
// neutralized identically under duck, whose write path shares the machinery.
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
