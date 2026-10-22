### Added
- `Pigeonhole::transact` and `RetryPolicy` (#694, ICR 0060): run a transaction and commit it, retrying `Conflict`, a snapshot a recovery expired, and optionally `Busy`, with exponential backoff and full jitter, up to an attempt cap or deadline. The closure may return the application's own error type (`E: From<pigeonhole::Error>`) to give up without committing. `RetryPolicy::retries` and `delay` let async code drive the same policy with its own timer.
- `EngineMetrics::transactions`, `transaction_conflicts` and `transaction_retries` (#694, ICR 0061): transaction outcomes and retries since open, across automatic recoveries.

### Changed
- With `Options::auto_recover`, a transaction whose commit the failed engine refuses during a recovery now gets `SnapshotExpired` rather than `Busy` (#694, ICR 0060, refining ICR 0050). It's still "not committed", and `transact` retries it by default.
- `Transaction::commit_async` and `commit_with_async` return `nonblocking::TransactionCommitFuture` instead of `CommitFuture` (#694, ICR 0060): a future of the same output that also counts the outcome in the transaction metrics. Code that only awaits it is unaffected.
