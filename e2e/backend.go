package main

// Backend-specific helpers: under duck, DuckDB lives inside the aggregator, no
// ClickHouse container exists and the api reads through the aggregator's RPC.

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
	"github.com/VKCOM/statshouse/internal/duckstore"
)

type storageBackend = duckstore.StorageBackend

const (
	backendClickHouse = duckstore.BackendClickHouse
	backendDuck       = duckstore.BackendDuck
)

// daemonSpecsFor returns the daemons to cross-compile for a backend. Under duck
// the aggregator is the duckdb-tagged static build, cached under its own name
// so it never overwrites the pure-Go one in the shared cache dir.
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

// aggBinName is the aggregator's cached binary name; either is mounted at
// /statshouse-agg, so the entrypoint never varies with the backend.
func aggBinName(backend storageBackend) string {
	if backend == backendDuck {
		return "statshouse-agg-duck"
	}
	return "statshouse-agg"
}

// duckDBExtLDFlags returns the static-link flags for the duck aggregator (as
// in the Makefile's build-agg-duckdb). A naive -static segfaults at DuckDB
// startup: with pre-2.34 glibc, libstdc++ probes weak pthread symbols and only
// referenced archive members get linked, leaving no-op mutexes. So libpthread.a
// is whole-archived by explicit path, and --allow-multiple-definition absorbs
// the members Go's own -lpthread already pulled in.
func duckDBExtLDFlags(cc string) (string, error) {
	out, err := exec.Command(cc, "-print-file-name=libpthread.a").Output()
	if err != nil {
		return "", fmt.Errorf("resolve libpthread.a from %s: %w", cc, err)
	}
	p := strings.TrimSpace(string(out))
	if p == "" || !strings.HasSuffix(p, "libpthread.a") || filepath.Base(p) == p {
		// gcc echoes the bare name back when it cannot resolve the file.
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
	// A real read of the tier the api queries, not just an open TCP port.
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
