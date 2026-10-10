### Added
- **The rest of the async front door** (#42, D196, ICR 0018):
  - `RowMutation::commit_if_async` (a `CheckFuture` resolving to `Option<CommitInfo>`);
  - `Transaction::get_async`, which records the read at the call, so a dropped future still counts at commit;
  - `Pigeonhole::flush_async` and `compact_async` (a `MaintenanceFuture`).

  Every data operation now has an async form. `backup`, `shrink`, open, close and the schema calls stay blocking.
- `pigeonhole-engine`: `Engine::submit_check_and_mutate` with `PendingCheck`, `Engine::submit_flush` and `submit_compact` (`PendingMaintenance`, now always exported and a `Future`), and `Txn::get_async` (ICR 0018).
