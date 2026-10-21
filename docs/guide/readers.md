# Reader processes

One writer process and any number of reader processes on the same host can open the same `.phdb` file. Readers see each commit as soon as the writer publishes it. This page covers what a reader sees, what happens when either side crashes or restarts, and the limits.

> **Status:** reader processes work today and are being hardened for 1.0 (#693). This page describes `main`.

## Opening a reader
`Pigeonhole::open_reader(path, ReaderOptions)` returns a `PigeonholeReader`: tables (`ReadTable`) with `get`, `row`, `scan`, snapshots, `backup`, and `info`. There are no write methods. It reads memtables in place from the writer's shared-memory file and SSTs from the database file through its own block cache. It never replays the WAL.

```rust
use pigeonhole::{Family, Options, Pigeonhole, ReaderOptions};

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let path = dir.join("app.phdb");
let db = Pigeonhole::open(&path, Options::default().shards(1))?;
let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
t.mutate(b"r").put("f", b"q", b"v").commit()?;

// Usually in another process; any process on the same host works the same way.
let reader = Pigeonhole::open_reader(&path, ReaderOptions::default())?;
let rt = reader.table("t")?;
assert_eq!(rt.get(b"r", "f", b"q")?.unwrap().value(), b"v");

// The writer's next commit is visible to the reader's next read.
t.mutate(b"s").put("f", b"q", b"w").commit()?;
assert!(rt.get(b"s", "f", b"q")?.is_some());
# drop(rt);
# drop(reader);
# db.close()?;
# Ok(())
# }
```

## What a reader sees
- **Visible commits, not only durable ones** (D227). A reader sees a commit once the writer has applied it, like a read in the writer process. A commit at `Durability::None` or `Buffered` is visible before a power loss could remove it. `PigeonholeReader::durable_snapshot()` reads only commits that are durable at their level.
- **Whole commits.** A snapshot sees a commit entirely or not at all, including across shards.
- **Snapshots stay put.** A snapshot keeps reading the state it was taken at while the writer goes on.

## When the writer stops, crashes or restarts
- **The writer closes or crashes:** readers keep serving their snapshots. New snapshots see the last state the writer published. Rarely, they fail with `Busy` after about a second, if the writer had committed a manifest it never published (D149); retry.
- **A new writer opens:** it recovers the WAL and starts a new shared-memory generation. Reads through snapshots taken before it, including a scan in progress, fail with `SnapshotExpired`, because the new writer may reuse the space they read. Take a new snapshot and redo the read; the reader re-attaches by itself. A clean close alone doesn't expire snapshots: only a new writer does.
- Readers don't need to restart in either case, with one exception on Windows ([#844](https://github.com/CodingAnarchy/pigeonhole/issues/844)): a Windows writer's shared memory disappears once no process maps it. If a new writer opened and then closed before the reader's next snapshot, there's nothing left to re-attach to, and that snapshot fails with `NoWriter`. Reopen the reader once a writer runs again.
- **No writer running:** a reader attaches to a running writer's shared memory. `open_reader` with no writer running (none has opened the file, or the last one closed) fails with `NoWriter`, on every platform. Retry once a writer runs.

## When a reader exits or crashes
- **Closing a reader** releases its slot at once.
- **A reader process that crashes or is killed** releases nothing itself. The writer checks for reader processes that are gone at most every 250 ms, and frees their slots and whatever their snapshots held (#833).
- **A reader the writer wrongly judges dead** (see the PID-namespace requirement below) loses its slot. Its reads then fail with `SnapshotExpired`, and its next snapshot takes a fresh slot. It never reads wrong data.

## Long-held snapshots stall the writer
A reader's oldest live snapshot pins the writer's state as of that snapshot: the memtables and file space the writer would otherwise reclaim. A reader that holds one snapshot for a long time while the writer keeps committing eventually fills the writer's memtable budget. The writer's commits then wait for room, and fail with `Busy` after `Options::write_stall_timeout` (30 s by default).

- **This is the reader's responsibility.** Take a snapshot per unit of work, drop it when done, and take a new one for the next.
- **Watch it from the writer:** `engine_metrics()` reports `reader_slots_active` out of `reader_slots`, `reader_slots_reclaimed`, and how far the oldest reader snapshot lags the current state, as `reader_pin_view_lag` (published views) and `reader_pin_seqno_lag` (commits). A lag that only grows is a reader holding a snapshot.
- A reader process's backup holds its snapshot for the whole copy (ICR 0049), so plan for that on a busy writer.

## Limits
Every limit is an error, never a hang:

| Limit | What happens | Remedy |
|---|---|---|
| Reader slots: `Options::reader_slots` (default 128, at most 65,536) | One more `open_reader` fails with `NoReaderSlot` at once | Close idle readers, or open the writer with more slots |
| Shared memory: about `memtable_budget × shards + 10 MiB` | The writer's open fails with `ShmUnavailable` if it can't be allocated | Free space in `/dev/shm`, give the container more, or use `shm_dir` |
| A view larger than its shared-memory buffer (very many tablets) | `ViewTooLarge` | Fewer tablets |
| A reader snapshot holding the writer's memory | The writer's commits fail with `Busy` after `write_stall_timeout` | Drop old snapshots (see above) |
| A published view without its manifest | A new snapshot fails with `Busy` after about 1 s (D149) | Retry |

## Requirements
- **Same host, local filesystem.** Network filesystems are refused.
- **Write permission on the file.** A reader opens the file read-write (D36), because the processes coordinate with byte-range locks, some of them exclusive. It writes nothing.
- **Same PID namespace.** The writer checks reader liveness by process id and start time. A reader in another PID namespace (another container that shares only the volume and shared memory) looks dead to the writer, so it keeps losing its slot and keeps getting `SnapshotExpired`. Run the writer and its readers in the same PID namespace. On Linux, a reader that `/proc` hides from the writer (`hidepid`, another user's process) still counts as alive.
- **Same build layout.** The writer and every reader need the same shared-memory layout. Stop reader processes before upgrading the writer: a writer with a newer layout fails with `ShmVersionMismatch` while older readers are attached.
- **Windows, across sessions.** Shared memory lives in the session's `Local\` namespace by default, so a reader in another session (a service and a desktop process, for example) can't attach. Give the writer and every reader the same `shm_dir` (`Options::shm_dir`, `ReaderOptions::shm_dir`): a file-backed mapping that any session can open.
- **Block cache per process.** Each reader has its own block cache, 256 MiB by default (`ReaderOptions::block_cache`), on top of the writer's.

## Where next
[Concepts](concepts.md#multi-process-readers-phase-4-available-now) · [Errors](errors.md) · [Agent reference](agent-reference.md)
