# One DuckDB file per shard, read through the ClickHouse query builder

Each aggregator shard keeps its rows in a single DuckDB file with the three tier tables of
ClickHouse's statshouse_v6 schema. It ingests the RowBinary body the aggregator already builds for
ClickHouse, appending every row to all three tiers in one transaction, and compaction collapses
closed time buckets in place. DuckDB's MVCC is the only concurrency mechanism: queries, inserts and
compaction never wait on each other. Expired rows are deleted; DuckDB reuses their blocks.

The API renders duck queries with its ClickHouse query builder, in a dialect that differs only where
DuckDB cannot accept ClickHouse syntax. Each aggregator exposes views named after the ClickHouse
tables and macros for the ClickHouse functions the builder calls, runs the SQL it receives over
`statshouse.storeQuery`, and returns ClickHouse Native columns, which the API decodes with the
ClickHouse code path. The API merges rows several shards return for one series.

We rejected a store split into delta and per-window archive files, and a structured query RPC with
an aggregator-side SQL renderer. Both worked, but they re-implemented what DuckDB (transactions,
space reuse) and the existing builder (every query shape) already provide, at several times the code.

## Consequences

The SQL the API sends is trusted like the SQL it sends to ClickHouse: the RPC runs on the aggregator's
crypto-keyed port and the aggregator serves only SELECT. Retention by DELETE cannot shrink the file,
only stop its growth, and a free-space eviction mode was dropped with the window files.
