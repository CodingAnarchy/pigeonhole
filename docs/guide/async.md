# Async

> **Status:** this page describes `main`; the async API is new in 0.3.0. In 0.2.x the `async` feature is off by default and gates an empty module.

Every data operation has two forms over the same engine: a blocking method, and an async method with the same semantics. Pick per call site; one table handle serves both. The async forms are behind the `async` feature, which is **on by default**. To build without them and without their one dependency (`futures-core`), use `pigeonhole = { version = "0.3", default-features = false }`.

| Blocking | Async | Resolves to |
|---|---|---|
| `Table::get(row, family, qualifier)` | `Table::get_async(..)` | `Result<Option<Cell>>` |
| `Table::get_at(&snapshot, ..)` | `Table::get_at_async(&snapshot, ..)` | `Result<Option<Cell>>` |
| `RowRead::read()` | `RowRead::read_async()` | `Result<Option<Row>>` |
| `Scan::iter()` | `Scan::stream()` | a `futures_core::Stream` of `Result<Row>` |
| `RowMutation::commit()` | `RowMutation::commit_async()` | `Result<CommitInfo>` |
| `RowMutation::commit_if(&cond)` | `RowMutation::commit_if_async(&cond)` | `Result<Option<CommitInfo>>` (`None`: the condition failed) |
| `WriteBatch::commit()` / `commit_with(d)` | `commit_async()` / `commit_with_async(d)` | `Result<CommitInfo>` |
| `Transaction::get(&t, row, family, qualifier)` | `Transaction::get_async(..)` | `Result<Option<Cell>>` |
| `Transaction::commit()` / `commit_with(d)` | `commit_async()` / `commit_with_async(d)` | `Result<CommitInfo>` (`Conflict` on a conflict) |
| `Pigeonhole::flush()` / `compact()` | `flush_async()` / `compact_async()` | `Result<()>` |
| `CommitTicket::wait()` | `ticket.await` | `Result<CommitInfo>` |

`ReadTable` (reader processes) has `get_async` and `get_at_async` too, and its `row` and `scan` builders have `read_async` and `stream`.

## Any executor, no blocking pool
The futures depend only on `std::task`, so they run on Tokio, smol, async-std or your own executor. None of them spawns a thread or uses `spawn_blocking`:

- A **read** as of now runs at the call first, as the blocking call does. If it hits the memtable or the block cache, it is read there, as of the call, and the future resolves to it on its first poll. A read that needs a block the cache does not hold submits that block's read to the I/O backend and returns `Pending`; the completion wakes the task, the block goes into the cache, and the read runs again. Such a read, and a read at a snapshot, takes its read point on the first poll, so every attempt sees the same data.
- A **commit** is submitted when you call `commit_async`, not when you first poll it. It joins the same commit groups as blocking commits and resolves exactly when the blocking `commit` would return: durable at its level and visible to reads.
- A **scan stream** fetches the blocks its next step will read before it steps, and only as you poll it, so a slow consumer never makes it read ahead.

The samples below use `doc_support::block_on`, a minimal executor for examples. Use your own.

```rust
use pigeonhole::{Durability, Family, Options, Pigeonhole};

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
let pages = db.table("pages")?
    .family("meta", Family::default())
    .create_if_missing()?;

pigeonhole::doc_support::block_on(async {
    pages.mutate(b"com.example/a")
        .put("meta", b"status", b"200")
        .commit_async()
        .await?;

    let mut wb = db.write_batch();
    wb.put(&pages, b"com.example/b", "meta", b"status", b"404");
    wb.commit_with_async(Durability::Sync).await?;

    let status = pages.get_async(b"com.example/a", "meta", b"status").await?;
    assert_eq!(status.map(|c| c.value().to_vec()), Some(b"200".to_vec()));

    let row = pages.row(b"com.example/b").family("meta").read_async().await?;
    assert_eq!(row.map(|r| r.len()), Some(1));
    Ok::<(), pigeonhole::Error>(())
})?;
# db.close()?;
# Ok(())
# }
```

## Scan streams
`Scan::stream` takes the same builder as `Scan::iter` (families, qualifier and time filters, versions, limits, a snapshot) and yields owned `Row`s in key order. Any `Stream` combinator library works; this sample polls it by hand to stay dependency-free.

```rust
use std::pin::Pin;
use futures_core::Stream;
use pigeonhole::{Family, Options, Pigeonhole};

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
let pages = db.table("pages")?.family("links", Family::default()).create_if_missing()?;
for i in 0..10 {
    pages.mutate(format!("com.example/{i}").as_bytes()).put("links", b"to", b"x").commit()?;
}

let mut rows = pages.scan_prefix(b"com.example/").family("links").stream();
let mut keys = Vec::new();
pigeonhole::doc_support::block_on(async {
    while let Some(row) = std::future::poll_fn(|cx| Pin::new(&mut rows).poll_next(cx)).await {
        keys.push(row?.key().to_vec());
    }
    Ok::<(), pigeonhole::Error>(())
})?;
assert_eq!(keys.len(), 10);
# db.close()?;
# Ok(())
# }
```

An error setting up the scan (an unknown family, a closed database) arrives as the stream's first item.

## Cancellation
- **Reads and scans:** dropping the future or the stream is always safe. Nothing is left half done.
- **Transaction reads:** `Transaction::get_async` records the read when you call it, not when it resolves. A read future dropped unpolled still counts when the transaction commits: a write to that cell since the snapshot makes the commit fail with `Conflict`. That is conservative: it can add a conflict, never miss one.
- **Flushes and compactions:** dropping `flush_async` or `compact_async` does not stop the operation; you lose only its result.
- **Commits:** a commit future submits at the call when its shard has room; otherwise it waits for room first (see [Backpressure](#backpressure)). Once submitted, dropping the future does **not** roll the commit back: the commit lands or fails atomically either way, and you lose only its result. To submit now and learn the outcome later, keep the future, or use `WriteBatch::commit_with_ticket(durability)`. That returns a `CommitTicket`, which works without the `async` feature: `wait()` blocks, `try_result()` checks without blocking, `seqno()` is `Some` once the commit succeeded, and with `async` it can be awaited. A ticket always submits at the call.

## Backpressure
A shard holds at most half `memtable_budget` (32 MiB at the defaults) of async commits that are submitted and not yet resolved (D210). This keeps an async producer that outruns its shard, during a long write stall for example, from queuing without limit inside the engine.

- **Over the bound, a commit future waits before it submits.** It submits once earlier commits on its shard resolve. Waiting futures go in the order they were made: a later small commit does not overtake an earlier large one. A commit larger than the bound goes alone.
- **Polling submits it.** A waiting future submits only when it is polled (your executor polls it when it is woken).
- **A future dropped while it waits for room writes nothing.** Only a future that has already submitted keeps D196's "dropping does not roll back".
- **So fire and forget can lose commits.** `let _ = t.mutate(..).commit_async();` is safe only while the shard is under its bound. To write without waiting for the result, use `WriteBatch::commit_with_ticket` (it always submits) or a blocking `commit` on another thread. Otherwise keep the future and await it.
- **Blocking commits are never held.** `commit`, `commit_with`, `commit_if` and `commit_with_ticket` are not counted against the bound, because each waits on its own thread.
- **The bound covers what the engine holds.** A waiting future keeps its own batch, so making many futures without awaiting them still holds their memory in your program. Bound your concurrency (for example, a limited `FuturesUnordered`) and the bound slows you down instead.

```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("bp.phdb"), Options::default())?;
# let t = pigeonhole::doc_support::table(&db, "t", &["f"])?;
// Each commit is awaited: under the bound it submits at once; over it, the await waits for room.
for i in 0..100u32 {
    let commit = t.mutate(&i.to_be_bytes()).put("f", b"q", b"v").commit_async();
    pigeonhole::doc_support::block_on(commit)?;
}
# Ok::<(), pigeonhole::Error>(())
```

## When an async read blocks
A few rare cases still read synchronously, inside the future or, for a read whose other blocks are cached, inside the `get_async` or `read_async` call itself. Each one is counted in `Pigeonhole::async_sync_reads()` (and on `PigeonholeReader`), so you can see whether your workload hits them:

- a separated (blob) value larger than the blob cache limit (an eighth of the block cache, capped at 1 MiB);
- a block the cache cannot keep (a cache of size 0, or a block larger than a cache shard), or a single read needing more than 64 fetches;
- a scan step that needs a block the stream did not predict (for example, after a long skip over deleted or filtered cells).

Making scan steps resumable, so the last case never blocks, is tracked in [#398](https://github.com/CodingAnarchy/pigeonhole/issues/398).

## Application-owned mode
With `open_application_owned`, the blocking `commit`, `commit_if`, `flush` and `compact` refuse to run on a thread that drives a shard, because they could wait on that same shard (D88). The async forms are how you call them from the event loop: submit with `commit_async`, `commit_if_async`, `flush_async` or `compact_async` and await the future there.

**The loop for one shard per thread: `run_once`, poll your futures with `shard.waker()`, and `shard.wait(timeout)` once `run_once` returns `false`** (#553):

```rust,no_run
# use std::time::Duration;
# use pigeonhole::{Options, Pigeonhole};
# let dir = pigeonhole::doc_support::temp_dir();
# let (_db, mut shards) =
#     Pigeonhole::open_application_owned(dir.join("app.phdb"), Options::default().shards(1))?;
# let mut shard = shards.pop().expect("one shard");
let waker = shard.waker();
let mut cx = std::task::Context::from_waker(&waker);
loop {
    let more = shard.run_once(Duration::from_micros(200));
    // poll this thread's futures (commit_async and friends) with `cx`
    # let _ = &mut cx;
    if !more {
        shard.wait(Duration::from_millis(10));
    }
}
# #[allow(unreachable_code)]
# Ok::<(), pigeonhole::Error>(())
```

- `wait` returns when work arrives for the shard, a future's wake reaches `shard.waker()`, this thread's own I/O completes, background work falls due, or the timeout passes. It may return early.
- It works on every backend and needs no file descriptor: with io_uring (the default on Linux) it waits in the thread's ring while the thread's own I/O is in flight, and parks otherwise.
- Give the futures this thread polls `shard.waker()`: a commit can complete on another shard's thread, and its wake must end this thread's wait.
- **What one `run_once(budget)` call does,** in order:
  1. With io_uring, it completes the I/O this thread submitted that has finished.
  2. It handles the shard's queued messages (commits, flush and compact requests, cross-shard rounds), up to 1,024 of them, and then commits them as one group. This part isn't bounded by `budget`.
  3. **The early end (D237).** If that completed a future woken through `shard.waker()` (typically a commit this thread submitted), the call returns `true` now, before the background work, so the loop can poll the future sooner. The background work runs on the next call. After 8 early ends in a row, a call runs it anyway, so flush and compaction get at least one call in nine.
  4. Otherwise, it runs the shard's background work (flush, compaction, the tablet balancer) in slices of at most 500 µs until `budget` is spent or no background work is runnable. Between slices, it handles newly queued messages first, so a commit never waits behind more than one slice.
  5. It returns whether runnable work remains. Background work waiting for a time doesn't count: `next_wakeup` says when that is due.

  A loop that polls its futures only after `run_once` returns `false` gives up the early end's gain, so poll them after every call, as above. A smaller `budget` hands the thread back sooner; it doesn't shorten step 2.
- A thread that drives several shards, or waits on other sources too, uses its own event loop instead: the points below.

**In your own event loop, with io_uring (the default on Linux: `IoBackend::Auto`, D218), wait on each shard's `Shard::io_fd`** (`poll`/`epoll`, readable), as well as on the `set_wakeup` callback and `next_wakeup` (D202):
- **Why.** In application-owned mode the engine starts no threads, so nothing in the background completes I/O. Each thread that drives a shard has its own ring, and completes that ring's I/O in its turns, waits and `io_fd`.
- **A thread that drives no shard runs the I/O it submits inline** (#929): an executor's `get_async` that misses the cache, a scan's readahead or a blob read on such a thread is a blocking `pread` there, as on the pread backend, so the future is ready when the call returns. Do latency-sensitive reads on a driving thread, or accept the blocking. (Before #929 such I/O went to a ring nothing reaps in this mode, and completed only on some driving thread's turn.)
- **A driving thread the kernel refuses a ring** (`ENOMEM` against the locked-memory limit, `ulimit -l`; or a kernel without the support) runs its I/O inline the same way, and retries for a ring later after `ENOMEM`. `Pigeonhole::io_rings` counts both (`attach_failures`, `inline`). A nonzero `inline` on a driving thread's workload is worth a larger `ulimit -l`.
- **Blocking calls are unaffected:** a thread blocked on its own read completes it itself.
- **Asking for `io_fd` is also the opt-in** that lets `next_wakeup` stop reporting in-flight I/O as due now. A loop that never takes it keeps polling while I/O is in flight.
- **Moving a shard to another thread: call `Shard::release()` on the old thread first** (#492). Each driving thread has its own ring, and only that thread can complete what was submitted to it, so a shard moved with I/O still on the old thread's ring would wait on it for ever once that thread stops running turns. `release()` drives the shard until the thread has nothing in flight. Then take `io_fd` again on the new thread (the descriptor belongs to the thread's ring). Debug builds panic on a move that skipped it. On the pread backend and in engine-owned mode, `release()` does nothing.

## Sync-only calls
These calls have no async form (owner decision recorded in [D196](../design/decisions/phase-3.md#d196)):

- **`backup` and `shrink`:** rare and long-running (a full copy of the file, or a relocation of its tail). From async code, run them on your executor's blocking pool so they do not hold an executor thread:

  ```rust,ignore
  let db = db.clone(); // Pigeonhole handles are cheap to clone
  tokio::task::spawn_blocking(move || db.backup("/backups/app.phdb")).await??;
  ```

- **Open, close and schema calls:** `open`, `open_reader`, `open_application_owned`, `close`, `table(..)` with `create`, `create_if_missing` or `open`, and `drop_table`. They are short, and you usually call them at startup and shutdown.
- **Cheap calls** that never wait on I/O in a writer process: `snapshot`, `write_batch`, `transaction`, the builders, `tables`, `shard_stats`, `engine_metrics`, and the durability getters and setters.

## Next
[Durability](durability.md) · [Scans and filters](scans-and-filters.md) · [Agent reference](agent-reference.md)
