# duck-store files are never upgraded in place

Embedding DuckDB makes the aggregator own an on-disk format. The store file carries a duck-store
schema version; a file stamped with another version is never read and never migrated: the aggregator
moves it aside and starts with an empty store. There is no in-place rewrite and no compatibility shim.

The price is that a schema change takes the stored history out of queries — all of it with the
default unbounded 1h retention — while fresh data accumulates. We accept it for an optional backend
aimed at small installations rather than write, test and support a migration for every schema
change; operators who need a bound on the loss set a finite 1h retention.

## Consequences

Downgrading StatsHouse across a schema change is equally lossy — never wrong numbers. Backup and
restore are not claimed as features.
