# StatsHouse

A metrics monitoring system: agents collect samples, aggregators roll them into per-second buckets, and a time-series store serves the API. ClickHouse is the incumbent store; this effort adds DuckDB as an optional alternative for small installations.

## Language

**Storage backend**:
The time-series store that holds aggregated metric data and serves API queries. One of: ClickHouse (incumbent) or duck-store (optional, for small installs). Metadata, mappings, and the journal are NOT part of the storage backend (they live in SQLite/rqlite).
_Avoid_: database, DB, CH (as a synonym for the concept)

**duck-store**:
DuckDB embedded **inside the statshouse-agg binary** — not a separate process. Each aggregator shard owns one DuckDB file holding only that shard's data, written by the agg's own insert conveyor and read by the API over RPC. Selected by config; ClickHouse remains the other supported storage backend.
_Avoid_: DuckDB server, storage node, duck-store process

**Shard fan-out**:
The API's replacement for ClickHouse's `_dist` Distributed tables: to answer one query the API sends the same SQL to every agg shard in parallel (`statshouse.storeQuery`) and merges rows of one series that several shards return, the way `internal/api/tscache.go` already merges across LODs.
_Avoid_: scatter-gather, distributed query

**Duck dialect**:
The SQL the API's ClickHouse query builder renders for duck-store. It differs from the ClickHouse SQL only where DuckDB cannot accept ClickHouse syntax; everything else — table names, `toFloat64`, `uniqMergeState`, `_shard_num` — is made valid by the views and macros each duck-store creates (the **compatibility layer**, `duckstore.compatSQL`).
_Avoid_: DuckDB renderer, SQL translation

**Aggregate state**:
A serialized partial-aggregation value stored per row instead of raw samples — TDigest centroids for percentiles, uniq state for unique counts, argMin/argMax host values. duck-store keeps ClickHouse's byte formats verbatim in BLOB columns, merged by Go functions registered in DuckDB, because **shard fan-out** needs states mergeable across shards and LODs, which a finalized number would not be — and because the API then decodes duck-store answers with its ClickHouse column readers.
_Avoid_: sketch, digest state

**LOD (level of detail)**:
The resolution tier of stored data — 1s, 1m, or 1h — kept in separate tables (`statshouse_v6_1s/1m/1h`, in duck-store `rows_1s/1m/1h`). In ClickHouse three materialized views write each incoming row to all three with only the timestamp truncated; duck-store's insert does the same in one transaction.
_Avoid_: resolution table, downsample tier

**Row collapse**:
Folding rows that share a key into one. ClickHouse gets this from `AggregatingMergeTree`'s background merges; duck-store's compaction collapses each closed time bucket that received more than one insert round. Queries always `GROUP BY` the key, so correctness never depends on it — it only saves space.
_Avoid_: merge, dedup

**Differential conformance run**:
A mode of the e2e harness (`go run ./e2e --conformance`) that boots ClickHouse plus *two* daemon stacks — CH-backed and duck-backed — over one shared metadata, seeds the identical deterministic stream to both agents from the harness itself, and compares the two APIs' decoded answers to every query shape, with ClickHouse as the reference. Comparison is by *decoded value* — never by state bytes, since two valid merge orders of the same **aggregate state** serialize differently.
_Avoid_: dual-write, shadow read, backend diff

**Small installation**:
A statshouse deployment with no ClickHouse process, running on nodes too small to justify one. This is duck-store's *purpose*, not merely one of its uses, so the design spends as little CPU, RAM and disk as it can rather than as much as the node allows. Sharded multi-node duck-store deployments are in scope; replication is not.
_Avoid_: tiny install, lightweight deployment
