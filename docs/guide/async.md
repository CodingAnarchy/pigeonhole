# Async

> **Not released yet:** this page describes `main`. In 0.2.0 on crates.io the `async` feature is off by default and gates an empty module.

Every data operation has two forms over the same engine: a blocking method, and an async method with the same semantics. Pick per call site; one table handle serves both. The async forms are behind the `async` feature, which is **on by default**. To build without them and without their one dependency (`futures-core`), use `pigeonhole = { version = "0.2", default-features = false }`.

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

- A **read** that hits the memtable or the block cache resolves on its first poll, about as cheaply as the blocking call. A read that needs a block the cache does not hold submits that block's read to the I/O backend and returns `Pending`; the completion wakes the task, the block goes into the cache, and the read runs again. The read point is taken on the first poll, so every attempt sees the same data.
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
- **Commits:** dropping a commit future after `commit_async` (or `commit_if_async`) returned does **not** roll the commit back. The commit lands or fails atomically either way, and you lose only its result. To submit now and learn the outcome later, keep the future, or use `WriteBatch::commit_with_ticket(durability)`. That returns a `CommitTicket`, which works without the `async` feature: `wait()` blocks, `try_result()` checks without blocking, `seqno()` is `Some` once the commit succeeded, and with `async` it can be awaited.

## When an async read blocks
A few rare cases still read synchronously inside the future. Each one is counted in `Pigeonhole::async_sync_reads()` (and on `PigeonholeReader`), so you can see whether your workload hits them:

- a separated (blob) value larger than the blob cache limit (an eighth of the block cache, capped at 1 MiB);
- a block the cache cannot keep (a cache of size 0, or a block larger than a cache shard), or a single read needing more than 64 fetches;
- a scan step that needs a block the stream did not predict (for example, after a long skip over deleted or filtered cells).

Making scan steps resumable, so the last case never blocks, is tracked in [#398](https://github.com/CodingAnarchy/pigeonhole/issues/398).

## Application-owned mode
With `open_application_owned`, the blocking `commit`, `commit_if`, `flush` and `compact` refuse to run on a thread that drives a shard, because they could wait on that same shard (D88). The async forms are how you call them from the event loop: submit with `commit_async`, `commit_if_async`, `flush_async` or `compact_async` and await the future there.

**With `IoBackend::Uring`, wait on each shard's `Shard::io_fd`** in your event loop (`poll`/`epoll`, readable), as well as on the `set_wakeup` callback and `next_wakeup` (D202):
- **Why.** In application-owned mode the engine starts no threads, so nothing in the background completes I/O. A read an async call submits from a thread that drives no shard (an executor's `get_async` that misses the cache) completes when a driving thread's turn takes it, and the driving threads' `io_fd` turns readable for exactly that.
- **If your loop doesn't wait on `io_fd`,** such an async read still completes, but only at the loop's next timed turn: up to `next_wakeup` later, which can be seconds when idle.
- **Blocking calls are unaffected:** a thread blocked on its own read completes it itself.
- **Asking for `io_fd` is also the opt-in** that lets `next_wakeup` stop reporting in-flight I/O as due now. A loop that never takes it keeps polling while I/O is in flight.

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
