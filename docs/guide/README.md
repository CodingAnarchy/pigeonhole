# Pigeonhole user guide

This guide is for people and agents **using** Pigeonhole in their own projects. If you are working **on** Pigeonhole, read [`../../CONTRIBUTING.md`](../../CONTRIBUTING.md) and [`../../AGENTS.md`](../../AGENTS.md) instead.

> **Status: API frozen; implementation in progress (Phase 1).** The public API is final in shape and every name used in this guide exists in the `pigeonhole` crate, but the bodies are not implemented yet, so code samples are marked `rust,ignore`. Track progress in [`../status.md`](../status.md). Features that ship later are labeled with their phase: **P2** wide-column model (blobs, zstd, custom merge operators, `commit_if`), **P3** async, **P4** reader processes and transactions.

## Contents
1. [Concepts](concepts.md): tables, rows, families, qualifiers, timestamps, versions, deletes.
2. [Getting started](getting-started.md): install, open, create a table, write, read, scan, batches, close.
3. [Durability](durability.md): the four levels, how they resolve, what each survives, mixed levels, commit results.
4. [Scans and filters](scans-and-filters.md): row, prefix and range scans, projection, versions, time ranges, snapshots, pushdown semantics.
5. [Data modeling](data-modeling.md): row-key design, family split, time series with TTL, adjacency lists, counters, versions, anti-patterns.
6. [Errors](errors.md): every `ErrorCode` with cause and remedy.
7. [Agent reference](agent-reference.md): every public type and method, limits, error codes and copy-paste recipes on one page.

Multi-process readers (P4) will get their own page when they are implemented; for now see [Concepts](concepts.md#multi-process-readers-phase-4) (including why readers need write permission on the file) and the API in the agent reference.

## For agents integrating Pigeonhole
Start with [`agent-reference.md`](agent-reference.md): dense tables, no prose padding. Use [`errors.md`](errors.md) to decide what to do about a failure. Signatures in `crates/pigeonhole/src/*.rs` are the final authority.
