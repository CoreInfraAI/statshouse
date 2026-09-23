# duck-store optimises for smallest footprint, not for the available envelope

duck-store exists to make a **small installation** cheap. Its defaults therefore target the smallest
CPU, RAM and disk it can run on, not the largest it is allowed. A reasonable engineer looking at a
node with 2 cores and 4 GB would tune DuckDB to use them; here that envelope is a *ceiling* the agg
must never exceed, while the defaults sit well below it and only grow when an operator raises them.

Concretely: one DuckDB thread and a 256 MB memory limit; `max(2, GOMAXPROCS)` concurrent queries, with
the rest waiting for a slot until their deadline; the DuckDB dependency behind a `-tags duckdb` build
guard so the default aggregator binary stays pure Go and 38.6 MB rather than 73.9 MB; and disk turned
down through the existing sampling budget rather than by provisioning more.

## Consequences

Under sustained load duck-store refuses queries as overloaded where a tuned-to-the-box configuration
would have served them. That is the intended trade: predictable small resident cost over peak
throughput. Any benchmark that concludes "duck-store is slower than it could be" should check whether
it is measuring this decision before treating it as a defect.
