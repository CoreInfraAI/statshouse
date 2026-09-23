package main

// The harness's storage-backend selection: the same suite drives either the
// usual ClickHouse stack (the default) or the duck stack, where DuckDB lives
// inside the aggregator, no ClickHouse container exists, and the api reads
// through the aggregator's structured query RPC. Everything backend-specific
// resolves through the helpers here so the boot code in main.go/daemons.go
// stays a branch on one value, and the choice is unit-testable without any
// container.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/VKCOM/tl/pkg/rpc"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
)

// storageBackend names the metric-data backend a run drives.
type storageBackend string

const (
	// backendClickHouse is the default stack: a ClickHouse container plus the
	// four daemons pointed at it.
	backendClickHouse storageBackend = "clickhouse"
	// backendDuck runs the aggregator with DuckDB embedded (the duckdb
	// build-tagged binary): no ClickHouse container, the aggregator serves
	// store queries on its own second address, and the api fans every query
	// out to it over the structured query RPC.
	backendDuck storageBackend = "duck"
)

// parseStorageBackend resolves the --storage-backend flag. An empty value (the
// flag's default) selects clickhouse; anything but the two known names is a
// hard error naming the flag and both choices.
func parseStorageBackend(v string) (storageBackend, error) {
	switch v {
	case "":
		return backendClickHouse, nil
	case string(backendClickHouse):
		return backendClickHouse, nil
	case string(backendDuck):
		return backendDuck, nil
	default:
		return "", fmt.Errorf("unknown --storage-backend %q (want %q or %q)", v, backendClickHouse, backendDuck)
	}
}

// daemonSpecsFor returns the daemons to cross-compile for a backend. The two
// backends differ in exactly one daemon: under duck the aggregator is built
// from the same package with the `duckdb` build tag and the verified static
// cgo link, and is cached under its own binary name so it never collides with
// (or rebuilds over) the pure-Go aggregator of a clickhouse run sharing the
// same cache dir.
func daemonSpecsFor(backend storageBackend) []daemonSpec {
	specs := make([]daemonSpec, len(daemonCmds))
	copy(specs, daemonCmds)
	if backend == backendDuck {
		for i := range specs {
			if specs[i].bin == "statshouse-agg" {
				specs[i] = daemonSpec{
					bin:    aggBinName(backend),
					pkg:    specs[i].pkg,
					cgo:    true,
					duckDB: true,
				}
			}
		}
	}
	return specs
}

// aggBinName is the cached binary name of the aggregator for a backend: the
// pure-Go build under clickhouse, the DuckDB-tagged static build under duck.
// The container mounts either one at /statshouse-agg, so the entrypoint script
// never varies with the backend.
func aggBinName(backend storageBackend) string {
	if backend == backendDuck {
		return "statshouse-agg-duck"
	}
	return "statshouse-agg"
}

// duckDBExtLDFlags computes the verified static-link flags for the
// DuckDB-tagged aggregator cross-compile (the recipe from
// .scratch/duck-store/02-cgo-build-research.md, mirroring the Makefile's
// build-agg-duckdb): a naive -static links but then segfaults at DuckDB
// startup, because under a pre-2.34 glibc libstdc++ probes weak pthread
// symbols and only the pthread archive members that resolved some reference
// get linked in — leaving DuckDB's scheduler with no-op mutexes.
// Whole-archiving libpthread.a fixes it; --allow-multiple-definition absorbs
// the byte-identical members Go's own -lpthread already pulled in. The archive
// must be passed by explicit path (resolved via the cross CC), not -lpthread.
func duckDBExtLDFlags(cc string) (string, error) {
	out, err := exec.Command(cc, "-print-file-name=libpthread.a").Output()
	if err != nil {
		return "", fmt.Errorf("resolve libpthread.a from %s: %w", cc, err)
	}
	p := strings.TrimSpace(string(out))
	if p == "" || !strings.HasSuffix(p, "libpthread.a") || filepath.Base(p) == p {
		// gcc prints the argument back unchanged when it cannot resolve the
		// file — a bare file name with no directory — which a suffix check
		// alone would wave through as a bogus relative link path. Treat
		// anything that is not a resolved path as a broken toolchain.
		return "", fmt.Errorf("%s -print-file-name=libpthread.a returned %q (no libpthread.a in the toolchain?)", cc, p)
	}
	return fmt.Sprintf("-static -Wl,--allow-multiple-definition -Wl,--whole-archive %s -Wl,--no-whole-archive", p), nil
}

func waitStoreQueryReady(ctx context.Context, rt Runtime, container, addr, rpcKeyPath string) error {
	key, err := os.ReadFile(rpcKeyPath)
	if err != nil {
		return fmt.Errorf("read RPC crypto key for the store-query probe: %w", err)
	}
	client := &tlstatshouse.Client{
		Client: rpc.NewClient(
			rpc.ClientWithProtocolVersion(rpc.LatestProtocolVersion),
			rpc.ClientWithCryptoKey(string(key)),
		),
		Network: "tcp4",
		Address: addr,
	}
	defer func() { _ = client.Client.Close() }()

	const (
		timeout  = 3 * time.Minute
		interval = 2 * time.Second
	)
	var lastErr string
	// a real read of the tier the api queries: proves the whole path (TL, DuckDB, the
	// ClickHouse compatibility views) is live, not merely that a TCP port is open
	args := tlstatshouse.StoreQuery{Sql: "SELECT toInt64(count(*)) AS n FROM statshouse_v6_1s_dist"}
	if perr := poll(ctx, timeout, interval, func() (bool, error) {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var resp tlstatshouse.StoreQueryResponse
		err := client.StoreQuery(cctx, args, nil, &resp)
		if err == nil {
			return true, nil
		}
		lastErr = err.Error()
		return false, nil
	}); perr != nil {
		return fmt.Errorf("agg store-query rpc (%s) did not answer a real storeQuery within %s: %v\n%s",
			addr, timeout, perr, diagnose(ctx, rt, container, lastErr))
	}
	return nil
}
