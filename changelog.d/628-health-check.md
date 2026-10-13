### Added
- `Pigeonhole::is_failed()` and `failure()`, a cheap health check (ICR 0040): whether the instance failed, which shard, and why (`Failure`, `FailureCause`: a panic, a dropped application-owned driver, an I/O error, an internal error; `Failure::MAIN_FILE` for a failed manifest commit). The user guide's new "Recovering from a failed shard" page says what to do next.

### Fixed
- Closing a database whose shard had stopped after a failed apply no longer hangs: the close gives up flushing that shard and reports the close unclean, and the next open replays its log.
- A commit that may have reached the log before its shard failed returns `OutcomeUnknown` instead of `Busy` or `Closed`: a commit whose apply failed after its log write (the next open replays it), and one in flight when a shard panicked.
