### Changed
- **The `async` feature is on by default** (#42, D17). Every data operation now has an async form by default: `get_async`, `get_at_async`, `read_async`, `Scan::stream`, `commit_async` and `commit_with_async`, and `CommitTicket` is awaitable. Build with `default-features = false` to drop them and their `futures-core` dependency; the blocking API is unchanged. `backup`, `shrink`, open, close and schema calls stay blocking (D196).

### Added
- A user-guide page on the async front door (`docs/guide/async.md`): the call table, executors, cancellation, the counted synchronous fallbacks and the sync-only calls.
