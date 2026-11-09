### Added
- **`Pigeonhole.fiber_scheduler` / `fiber_scheduler=`** (Ruby, #1574, ICR 0126): default `true`. Set it to `false` to keep every call blocking, process-wide, as before 1.2.

### Changed
- **Ruby commits yield to a fiber scheduler** (#1574, ICR 0126). Under Async, Falcon or any `Fiber::Scheduler`, a commit lets the thread's other fibers run while it waits, instead of blocking the thread:
  - `commit` on write batches, transactions and mutations, and `commit_if`, `commit_returning` and `commit_if_returning`;
  - `Table#put`, `incr`, `incr_returning` and `delete`;
  - `transact`.

  The results and errors are unchanged, and the calls behave as Ruby's own `IO` and `Queue` do. A commit is now a point where other fibers run, so finish snapshots and scans before one. A fiber stopped while it waits drops the wait: a commit not yet submitted writes nothing, and one submitted may still land.
  - **To keep the old behaviour:** `Fiber.blocking { }` around a call, or `Pigeonhole.fiber_scheduler = false` for the whole process.
  - **On Windows,** and without a scheduler, nothing changes.
