# CLAUDE.md

Agent guidance for this repository. See also [AGENTS.md](./AGENTS.md) (issue
tracker, triage labels, domain docs) and [CONTEXT.md](./CONTEXT.md).

## Build configurations

- The default build is pure Go and must stay that way
  (`CGO_ENABLED=0 go build ./...`; the only failures allowed are the
  pre-existing cgo-only `internal/sqlite/sqlite0` packages, which fail on
  pristine master too).
- Every import of `github.com/duckdb/duckdb-go/v2` sits behind the `duckdb`
  build tag (`//go:build duckdb`), mostly in `internal/duckstore`. Run both
  suites: `go test ./...` and `go test -tags duckdb ./...`. The DuckDB-tagged
  aggregator is built by `make build-agg-duckdb`, which carries the verified
  static link flags — a naive static link of DuckDB produces a binary that
  segfaults on first use.

## The storage-backend seam

Metric data lives in ClickHouse or in duck-store (DuckDB embedded in the
aggregator), selected per process by `--storage-backend=clickhouse|duck`
(parsed by `duckstore.StorageBackend`). One backend per process — no
dual-write, no read-comparison mode. The seam is deliberately thin:

- **Writes**: `goInsert` builds the same RowBinary body for both backends and
  branches only at the send — ClickHouse over HTTP, duck into
  `duckstore.Store.Insert`, which decodes the body and appends every row to
  the three tier tables in one transaction. Compaction collapses closed time
  buckets in place; DuckDB's MVCC is the only concurrency mechanism.
- **Reads**: the API renders SQL with its ClickHouse query builder in a duck
  dialect (`sqlDialect`), `requestHandler.doSelect` sends it to every shard
  over `statshouse.storeQuery` (`internal/api/duck.go`), and the aggregator
  runs it (`aggregator_handlers.go` → `duckstore.Store.Query`) against views
  and macros that mimic the ClickHouse tables and functions
  (`duckstore.compatSQL`). Results come back as ClickHouse Native columns and
  decode through the ClickHouse path; `mergeShardRows` folds rows of one
  series from several shards.

Ground rules when touching the seam:

- The ClickHouse paths must stay behaviourally identical.
- Transactions that involve a `duckdb.Appender` use explicit `BEGIN`/`COMMIT`
  on the `*sql.Conn` (`duckstore.inTx`), never `sql.Tx`.
- The API never links DuckDB — everything crosses the RPC. The aggregator
  gates duck on `duckstore.Available` (false in untagged builds); the API
  accepts `duck` in any build.
- Cross-backend agreement is enforced by the differential conformance run —
  `e2e/conformance.go`, a mode of the e2e binary (`go run ./e2e
  --conformance`, or `bash e2e/lima.sh --conformance` in the Lima VM) that
  boots ClickHouse plus both daemon stacks over one shared metadata, seeds
  the identical deterministic stream to both and compares the two APIs'
  decoded answers to every query shape, with CH as the reference. The e2e
  suite's client assertions and input matrix are frozen. Backend comparisons
  are always by decoded value — never by generated SQL or state bytes. The
  same suite also runs the duck stack alone (`go run ./e2e
  --storage-backend=duck`): no ClickHouse container, the same client
  assertions.
- The operator-facing surface (flags, retention, upgrades, metrics) is
  documented in `docs/duck-store.md`, kept in sync with the code by
  `internal/duckstore/docs_test.go`.
