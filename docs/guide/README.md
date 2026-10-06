# Pigeonhole user guide

This guide is for people and agents **using** Pigeonhole in their own projects. If you are working **on** Pigeonhole, read [`../../CONTRIBUTING.md`](../../CONTRIBUTING.md) and [`../../AGENTS.md`](../../AGENTS.md) instead.

> **Status: Phase 1 sync API implemented.** Every name used in this guide exists in the `pigeonhole` crate and works as described, except where a page says otherwise; see [What the current build does not do yet](getting-started.md#what-the-current-build-does-not-do-yet). Code samples run as doctests of the `pigeonhole` crate (lines starting with `#` are hidden setup). Track progress in [`../status.md`](../status.md). Features labeled with a later phase: **P2** wide-column model (blobs, zstd, custom merge operators; `commit_if` already works), **P3** async, **P4** hardening of reader processes and transactions (both already work).

## Contents
1. [Concepts](concepts.md): tables, rows, families, qualifiers, timestamps, versions, deletes.
2. [Getting started](getting-started.md): install, open, create a table, write, read, scan, batches, close.
3. [Durability](durability.md): the four levels, how they resolve, what each survives, mixed levels, commit results.
4. [Scans and filters](scans-and-filters.md): row, prefix and range scans, projection, versions, time ranges, snapshots, pushdown semantics.
5. [Data modeling](data-modeling.md): row-key design, family split, time series with TTL, adjacency lists, counters, versions, anti-patterns.
6. [Errors](errors.md): every `ErrorCode` with cause and remedy.
7. [Agent reference](agent-reference.md): every public type and method, limits, error codes and copy-paste recipes on one page.

Multi-process readers (P4) will get their own page when they are hardened; for now see [Concepts](concepts.md#multi-process-readers-phase-4-available-now) (including why readers need write permission on the file) and the API in the agent reference.

## For agents integrating Pigeonhole
Start with [`agent-reference.md`](agent-reference.md): dense tables, no prose padding. Use [`errors.md`](errors.md) to decide what to do about a failure. Signatures in `crates/pigeonhole/src/*.rs` are the final authority.
