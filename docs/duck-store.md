# duck-store: StatsHouse without ClickHouse (operator guide)

duck-store is a second storage backend for StatsHouse: DuckDB embedded in
`statshouse-agg`, selected by a flag. It exists for small installations — the
deployment that cannot justify provisioning a ClickHouse cluster. ClickHouse
remains the default backend and behaves exactly as before. A process runs one
backend; there is no dual-write or read-comparison mode.

Agents send to aggregators as usual; each aggregator shard stores its own rows
in one DuckDB file, and `statshouse-api` reads by sending its queries to every
shard over RPC. There is no standalone duck-store process, no replication, and
no migration path from existing ClickHouse data — a duck-store install starts
empty.

## Selecting the backend

`--storage-backend=clickhouse|duck` on both `statshouse-agg` and
`statshouse-api` (default `clickhouse`).

- The **aggregator** must be built with the `duckdb` build tag:
  `make build-agg-duckdb`. A regular (pure Go) aggregator refuses
  `--storage-backend=duck` at startup. The tagged build links DuckDB's C++
  runtime, so it needs a C/C++ toolchain; on Linux the make target links
  statically with the flags a working static DuckDB binary needs (a naive
  static link segfaults on first use) and with the `osusergo` tag (a static
  glibc cannot load NSS modules for `--user`/`--group`).
- The **API** never embeds DuckDB; any `statshouse-api` binary accepts `duck`.

A minimal single-shard setup (all other standard flags apply as under
ClickHouse):

```shell
statshouse-agg --storage-backend=duck --duck-store-dir=/var/lib/statshouse/duck \
  --agg-addr=0.0.0.0:13336 --local-shard=1

statshouse-api --storage-backend=duck --duck-shard-addrs=agg1.example.com:13336
```

Under duck there is no ClickHouse cluster to detect the shard number from, so
the aggregator takes it from `--local-shard`. `--duck-shard-addrs` lists the
regular RPC address (`--agg-addr`) of **every aggregator that runs a store** —
all replicas of all shards, each once: agents spread a shard's seconds over its
replicas (and fail over between them), so each replica's store holds a part of
the shard's data. `--clickhouse-v2-addrs` is not needed. There is no
redundancy: a query fails while any listed aggregator is unavailable. The queries travel on the
aggregators' RPC port, so the API must present the aggregators' crypto key:
pass the key file with `--rpc-crypto-path` (the aggregators read theirs with
`--aes-pwd-file`).

That port trusts what every aggregator RPC trusts: holders of the key, and
keyless peers on the same host or in a trusted subnet. Anyone it trusts can
read metric data from the store and nothing else: an aggregator runs only a
single SELECT of the shapes the API's query builder renders — checked against
DuckDB's own parse of the query, with an allowlist of functions and of the
tier views — in a read-only transaction, with file access and settings
changes disabled. A query can still cost CPU and memory up to the limits
below.

Every query reads every listed store, and the API merges rows of one series
that several stores hold, as a ClickHouse Distributed table would. One
difference remains with several stores: a tag-values list is cut to its top N
on each store before the merge, so a value that ranks just below N everywhere
can be missing from the merged list, and a value's count only includes the
stores where it made the cut.

## The store

`--duck-store-dir` points the aggregator at a directory it owns; everything
inside is created on first start:

```
<dir>/statshouse.duckdb   rows_1s, rows_1m, rows_1h: ClickHouse's statshouse_v6 columns
<dir>/tmp/                DuckDB's spill directory
```

Each insert round is one transaction appending every row to all three tiers
with its time truncated to the tier — the shape of ClickHouse's materialized
views — and the aggregator acknowledges contributors only after it commits,
as it does after a ClickHouse 200. Aggregate states (percentiles, uniques,
min/max hosts) are stored as ClickHouse's own state bytes and merged by Go
functions registered in DuckDB.

Queries always group by the full key, so partial rows are correct as they
are; compaction only saves space. Every 10 seconds it collapses, in place, each
time bucket that has closed and received more than one insert round — the
1m and 1h buckets once they end, and 1s buckets that historic inserts wrote
again — the way AggregatingMergeTree merges parts. DuckDB's MVCC keeps
queries, inserts and compaction from blocking each other. Which buckets need
collapsing is tracked in memory; after a restart compaction scans the store a
chunk per pass and collapses whatever an earlier process left behind.

## Retention and disk

Defaults mirror ClickHouse's TTLs; `0` keeps a tier forever. The API reads
the 1s tier for ranges up to 52 hours old and the 1m tier up to 33 days, so the
aggregator refuses a shorter nonzero retention for those two tiers:

| Flag | Default | Covers |
| --- | --- | --- |
| `--duck-retention-1s` | 52 hours | second-resolution data |
| `--duck-retention-1m` | 33 days | minute-resolution data |
| `--duck-retention-1h` | unbounded | hour-resolution data |

Retention deletes expired rows once a minute; DuckDB reuses the freed blocks
rather than shrinking the file. With the default unbounded 1h tier the file
never stops growing: each hour adds its collapsed rows for good, as the
ClickHouse 1h table does. Set `--duck-retention-1h` (say `8760h`, a year) to
bound disk. The insert (sampling) budget bounds how fast the file grows, the
same lever as under ClickHouse; size a node by watching `__duck_store_size`.

## Resources

Defaults target the smallest viable node, not the available envelope:

- `--duck-memory-limit` — DuckDB's memory limit, default 256 MB; spilling to
  disk is bounded by the same amount. DuckDB runs one thread.
- `--duck-query-concurrency` — how many queries execute at once, default
  `max(2, GOMAXPROCS)`. A query finding every slot busy waits for one until
  its own deadline and is then refused as overloaded.

## Upgrades, backup, starting clean

The file carries a schema-version stamp. A file stamped with another version
is never read and never upgraded in place: on start the aggregator renames it
to `statshouse.duckdb.v<version>-<unix time>`, logs that, and starts with an
empty store, so an upgrade that changes the schema takes all stored history
out of queries (with a finite 1h retention, at most that much). Delete the
renamed file to reclaim its disk.

Backup and restore are not supported. To start clean, stop the aggregator,
remove the store directory and start it again.

## Observability

duck-store reports itself through builtin metrics:

- `__duck_store_maintenance_time` — duration of compaction and retention
  passes, by outcome.
- `__duck_store_maintenance_age` — seconds since compaction and retention last
  succeeded; growth means a pass is stuck or failing.
- `__duck_store_backlog` — closed time buckets per tier still waiting to be
  collapsed; growth means compaction is falling behind ingestion.
- `__duck_store_query_time` — store query latency by outcome (`ok`, `error`,
  `refused` for a query that found no free slot before its deadline).
- `__duck_store_size` — used and free bytes of the store file.

Ingestion status and the other builtin metrics flow through the duck write
path unchanged; the aggregator's internal log, which under ClickHouse goes to
a log table, is written to the process log.

## How reads work

The API renders a query with the same builder it uses for ClickHouse, in a
duck dialect that differs only where DuckDB cannot accept ClickHouse syntax
(time bucketing, the percentile merge, raw 64-bit tags, string escaping). Each
aggregator creates views named after the ClickHouse tables and macros for the
ClickHouse functions the builder calls, runs the SQL, and answers with the
result in ClickHouse Native encoding, which the API decodes into the same
columns it reads from ClickHouse. An answer travels in one RPC packet, so a
shard refuses a result over 14 MiB with an error asking to narrow the query,
where ClickHouse would stream it. Agreement between the backends is checked by
the e2e harness's differential conformance run (`go run ./e2e --conformance`).
