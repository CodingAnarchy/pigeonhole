# 0090: A cache-only miss carries its read in flight; the async row read's at-call miss is handed to its future

**Status:** Approved (number from the coordinator, 2026-10-29; #709 step 2, #1086). It is additive API on two internal crates under D282, which `pigeonhole` pins exactly. Nothing is removed or changed, and the public `pigeonhole` API is untouched.

## Change
- **pigeonhole-sst:** `Fetch::start(&mut self) -> Completion`. It returns the read already in flight when the cache-only read that missed submitted one (a data or index block whose read did not complete at once, from `Blocks::fetch_now`), or else submits a new one, as `Fetch::submit` does. A `Fetch` now carries that optional in-flight `Completion`. Its `Clone` doesn't share it: a copy submits its own read.
- **pigeonhole-engine:**
  - `MissedRead`: an opaque `#[derive(Debug, Default)]` struct holding the fetch a latest read at the call missed on.
  - `Engine::try_read_row_latest(..) -> Result<Result<bool>, MissedRead>`: as `try_read_row_latest_into`, but `Err` keeps the miss.
  - `Engine::read_row_latest_async_after(missed, ..) -> RowFuture<S>`: as `read_row_latest_async`, but the future starts by waiting on the miss's read.
  - The existing `try_read_row_latest_into` and `read_row_latest_async` stay, as wrappers: the first drops the miss, the second passes `MissedRead::default()`.

## Why
#709's async reads used to unwind on every block-cache miss and run the whole read again. With D292, a page-cache hit completes at submit, so #1086 reads such a block in place (`Blocks::fetch_now`) and submits other reads at once.
- **Without `start`,** the future would submit that read a second time.
- **Without the handoff,** a miss at the call would drop its read in flight, and the future would read the same block again: two device reads on a cold get (perf287's review of #1086, S1).
- Point gets do this inside the engine (`GetFuture::new`). A row read crosses from `pigeonhole` (`RowRead::read_async`) into two engine calls, so the miss needs a type to travel in.

## Callers changed
- **`pigeonhole::RowRead::read_async`** uses `try_read_row_latest` and `read_row_latest_async_after`.
- **The engine's read futures and async scans** start a `WouldBlock`'s fetch with `start`.
- **Tests:** `crates/pigeonhole/tests/inplace_reads.rs` (one read in flight, not two) and `crates/sst/tests/sst.rs` (a miss that must wait carries its read).
