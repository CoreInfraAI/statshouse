# duck-store files are never upgraded in place

Embedding DuckDB makes the aggregator own an on-disk format. The store file carries a duck-store
schema version; a file stamped with another version is never read and never migrated: the aggregator
moves it aside and starts with an empty store. There is no in-place rewrite and no compatibility shim.

This is only affordable because retention is bounded: the worst case of an upgrade is that queries
lose at most one retention window of history while fresh data accumulates, rather than a migration
that must be written, tested and supported for every schema change.

## Consequences

Downgrading StatsHouse across a schema change is equally lossy — never wrong numbers. Backup and
restore are not claimed as features.
