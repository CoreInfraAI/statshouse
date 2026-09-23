# One storage backend per process; backend comparison lives in e2e only

duck-store and ClickHouse are selected by an explicit `--storage-backend=clickhouse|duck` flag, and a
process serves exactly one of them. We deliberately rejected a production dual-write / read-compare
mode, even though it would have made a ClickHouse-to-duck-store migration path and continuous
differential validation nearly free.

## Consequences

Both backends are read through the same query builder (ADR-0003), but DuckDB runs the SQL, the
aggregate-state folds are Go, and the duck dialect differs in places — so the two can still diverge.
What catches it is the **differential conformance run** (`go run ./e2e --conformance`): it boots
ClickHouse plus both daemon stacks over one shared metadata, seeds the identical deterministic stream
to both and compares the two APIs' decoded answers to every query shape, with ClickHouse as the
reference. That run is load-bearing, not a nicety, and must not be allowed to rot.

Migrating an existing ClickHouse install to duck-store has no supported online path.
