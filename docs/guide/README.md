# Pigeonhole user guide

This guide is for people and agents **using** Pigeonhole in their own projects. If you are working **on** Pigeonhole, read [`../../CONTRIBUTING.md`](../../CONTRIBUTING.md) and [`../../AGENTS.md`](../../AGENTS.md) instead.

> **Maturity: experimental 0.x.** The core engine (Phase 1) is complete and fault-tested in simulation, and the wide-column model (Phase 2: counter families, blob separation, compaction styles, zstd) is complete in 0.2.0: its performance gate is met as amended by [D193](../design/decisions/phase-2.md#d193), with one documented gap, the read tail of wide, heavily overwritten rows ([#387](https://github.com/CodingAnarchy/pigeonhole/issues/387)). The file format is frozen for 1.x at format 4 ([`FORMAT.md` §13](../../FORMAT.md#13-compatibility-promise) is the compatibility promise, from 1.0.0); the API may still change before 1.0. The latency work (Phase 3) is in progress on `main`: io_uring, direct I/O, the async API, and commit-path and read-path work (see the roadmap). Pigeonhole is **not recommended for production use yet**. See [`../status.md`](../status.md) for the roadmap.

> **Status:** this guide describes `main`. The current release on crates.io is 0.3.0; features added on `main` since then are listed in the [changelog](../../CHANGELOG.md) and [`changelog.d/`](../../changelog.d/README.md), and pages note what is not released yet. Every name used in this guide exists in the `pigeonhole` crate and works as described, except where a page says otherwise; see [What the current build does not do yet](getting-started.md#what-the-current-build-does-not-do-yet). Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup). Track progress in [`../status.md`](../status.md). Labels for later phases: **P4** hardening of reader processes and transactions (both already work).

## Contents
1. [Concepts](concepts.md): tables, rows, families (compaction styles, compression, blob separation), qualifiers, timestamps, values, deletes.
2. [Getting started](getting-started.md): install, open, create a table, write, read, scan, batches, conditional writes and transactions, close, maintenance.
3. [Durability](durability.md): the four levels, how they resolve, what each survives, mixed levels, commit results.
4. [Async](async.md): the async front door (on by default): futures and streams for every data operation, cancellation, backpressure on async commits, the sync-only calls.
5. [Scans and filters](scans-and-filters.md): row, prefix and range scans, projection, versions, time ranges, snapshots, pushdown semantics.
6. [Data modeling](data-modeling.md): row-key design, family split, time series with TTL, adjacency lists, counters, versions, anti-patterns.
7. [Errors](errors.md): every `ErrorCode` with cause and remedy.
8. [Recovering from a failed shard](recovery.md): detect a failed instance (`is_failed`), close, reopen, settle the commits in flight.
9. [Agent reference](agent-reference.md): every public type and method, limits, error codes and copy-paste recipes on one page.
10. [The `phdb` command-line tool](cli.md): inspect a database from the shell; how it opens a database, its output formats (stable JSON for programs) and exit statuses. In progress on `main`.
11. [Arrow export](arrow.md): a scan as Arrow record batches (`pigeonhole-arrow`) for DuckDB, Polars and other Arrow readers; the schema, options and cost. In progress on `main`.
12. [Reader processes](readers.md): one writer and many reader processes on a host: what readers see, crashes and restarts on either side, long-held snapshots, the limits and the requirements.
13. [Transactions](transactions.md): optimistic multi-row transactions: `transact` and retries, what is read and what conflicts ((row, family) granularity), the isolation level and anomalies, read-only transactions, the cost model.

Multi-process readers have their own page, [Reader processes](readers.md); [Concepts](concepts.md#multi-process-readers-phase-4-available-now) has the overview.

## For agents integrating Pigeonhole
Start with [`agent-reference.md`](agent-reference.md): dense tables, no prose padding. Use [`errors.md`](errors.md) to decide what to do about a failure. Signatures in `crates/pigeonhole/src/*.rs` are the final authority.
