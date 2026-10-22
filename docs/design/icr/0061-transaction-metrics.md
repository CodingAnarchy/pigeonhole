# 0061: transaction counters in `EngineMetrics`

**Status:** Number assigned by the coordinator (2026-10-21; #694). Lands in #694 PR 3 with ICR 0060's `transact`. Additive: `EngineMetrics` is `#[non_exhaustive]`.

## Change

`pigeonhole::EngineMetrics` gains three cumulative counters, since the handle opened:
- `transactions`: transaction commits that succeeded, write-free ones included;
- `transaction_conflicts`: transaction commits that failed with `Conflict`;
- `transaction_retries`: attempts `Pigeonhole::transact` retried.

The counts are kept by the pigeonhole handle, not the engine, so they span automatic recoveries (`Options::auto_recover`), unlike the engine's counters, which restart with each engine. Blocking and async transaction commits are both counted: an async one through its own future type, `TransactionCommitFuture` (ICR 0060), so the commit futures of row mutations and batches carry nothing for it. `commit-async` stays at main's instruction count.

## Why

An application, and the `transfer` gate row (#708, D272), can then read the conflict rate (`transaction_conflicts / (transactions + transaction_conflicts)`) and the retry cost without instrumenting its own loop.

## Callers

- `pigeonhole`: `metrics.rs`, `retry.rs` (`TxnCounts`), `db.rs`, `write.rs`, `nonblocking.rs`; `tests/transact.rs`.
- Docs: the agent reference's metrics list, and the changelog.
