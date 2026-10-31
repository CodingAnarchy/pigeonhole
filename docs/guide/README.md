# Pigeonhole user guide

This guide is for people and agents **using** Pigeonhole in their own projects. If you are working **on** Pigeonhole, read [`../../CONTRIBUTING.md`](../../CONTRIBUTING.md) and [`../../AGENTS.md`](../../AGENTS.md) instead.

> **Maturity: 1.0.** `pigeonhole`'s public API follows semver from 1.0.0 (D282), and every 1.x release reads and writes file format 4 ([`FORMAT.md` §13](../../FORMAT.md#13-compatibility-promise)). 1.0 ships with documented gaps, listed in [`../status.md`](../status.md).

> **Status:** this guide describes `main`. The current release on crates.io is 1.0.0; anything `main` adds after it is listed in [`changelog.d/`](../../changelog.d/README.md). Every name used in this guide exists in the `pigeonhole` crate and works as described, except where a page says otherwise; see [What the current build does not do yet](getting-started.md#what-the-current-build-does-not-do-yet). Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup). Track progress in [`../status.md`](../status.md).

## Contents
1. [Concepts](concepts.md): tables, rows, families (compaction styles, compression, blob separation), qualifiers, timestamps, values, deletes.
2. [Getting started](getting-started.md): install, open, create a table, write, read, scan, batches, conditional writes and transactions, close, maintenance.
3. [Durability](durability.md): the four levels, how they resolve, what each survives, mixed levels, commit results.
4. [Async](async.md): the async front door (on by default): futures and streams for every data operation, cancellation, backpressure on async commits, the sync-only calls.
5. [Scans and filters](scans-and-filters.md): row, prefix and range scans, projection, versions, time ranges, snapshots, pushdown semantics.
6. [Data modeling](data-modeling.md): row-key design, family split, time series with TTL, adjacency lists, aggregate families (counters, min and max, distinct counts), versions, anti-patterns.
7. [Errors](errors.md): every `ErrorCode` with cause and remedy.
8. [Recovering from a failed shard](recovery.md): detect a failed instance (`is_failed`), close, reopen, settle the commits in flight; or let `Options::auto_recover` do it in place.
9. [Agent reference](agent-reference.md): every public type and method, limits, error codes and copy-paste recipes on one page.
10. [The `phdb` command-line tool](cli.md): inspect a database from the shell; how it opens a database, its output formats (stable JSON for programs) and exit statuses.
11. [Arrow export](arrow.md): a scan as Arrow record batches (`pigeonhole-arrow`) for DuckDB, Polars and other Arrow readers; the schema, options and cost.
12. [Reader processes](readers.md): one writer and many reader processes on a host: what readers see, crashes and restarts on either side, long-held snapshots, the limits and the requirements.
13. [Transactions](transactions.md): optimistic multi-row transactions: `transact` and retries, what is read and what conflicts (per cell for `get`, per family for `get_row`), the isolation level and anomalies, read-only transactions, the cost model.
14. [Performance compared with SQLite](performance.md): point reads under a mixed load at equal cache memory: throughput, median, p99 and the far tail, and when each store fits.
15. [Graphs](graphs.md): adjacency rows, reverse edges and degree, walking a hub's row cell by cell or in pages, frontier multi-gets, and breadth-first traversal inside the engine (sync and async).
16. [Using Pigeonhole from AI agents](ai-agents.md): `phdb mcp`, an MCP server that gives an agent tools to list, read, scan, traverse and write a database; client configuration, cursors for long results, and modelling agent memory.

Multi-process readers have their own page, [Reader processes](readers.md); [Concepts](concepts.md#multi-process-readers) has the overview.

## For agents integrating Pigeonhole
Start with [`agent-reference.md`](agent-reference.md): dense tables, no prose padding. An agent that should use a database as a tool, rather than write code against one, wants [`phdb mcp`](ai-agents.md). Use [`errors.md`](errors.md) to decide what to do about a failure. Signatures in `crates/pigeonhole/src/*.rs` are the final authority.
