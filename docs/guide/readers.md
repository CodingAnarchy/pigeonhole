# Reader processes

One writer process and any number of reader processes on the same host can open the same `.phdb` file. Readers see each commit as soon as the writer publishes it. This page covers what a reader sees, what happens when either side crashes or restarts, and the limits.

> **Status:** reader processes are part of 1.0 (#693). This page describes `main`.

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
- **No writer running:** a reader attaches to a running writer's shared memory. `open_reader` with no writer running (none has opened the file, or the last one closed) fails with `NoWriter`, on every platform. Retry once a writer runs. So does a new database whose file reuses the inode of a deleted one that left shared memory behind (its writer was killed): the reader reports `NoWriter`, not `Corruption`, until the new database's own writer runs, and that writer replaces the leftover region (#1133).

## When a reader exits or crashes
- **Closing a reader** releases its slot at once.
- **A reader process that crashes or is killed** releases nothing itself. The writer checks for reader processes that are gone at most every 250 ms, and frees their slots and whatever their snapshots held (#833).
- **How the writer tells:** each reader holds a byte-range lock on the database file for its slot, and the operating system releases it when the process exits, however it exits. The writer frees a slot only once it can take that lock itself (#693). It never looks at process ids, so a live reader is never judged dead, whatever its PID namespace.

## Overwritten rows read as fast as in the writer
The writer records, in each memtable entry it links in front of an older version of the same column, where that column's versions end (D194). Reader processes jump by the same field (D277), so a row overwritten many times costs a reader process what it costs the writer process: on the 16 hottest rows of ycsb-a it was 3× slower before, with the row cache off.

## Hot rows: each reader process has a row cache
A reader process keeps the newest version of small, hot rows in a row cache of its own, as the writer does (D281): latest row reads and point gets of a cached row cost about what they cost in the writer process. Results are identical to uncached reads. The writer raises a per-row write watermark in shared memory for every write, and a reader's cached copy of a row misses as soon as the reader can see a newer write to it.

- **Size:** `ReaderOptions::row_cache(bytes)`, 64 MiB by default, as the writer's; `0` turns it off. **Each reader process has its own:** N reader processes use up to N × 64 MiB by default, plus about 4 MiB each for the cache's per-row tables. `row_cache_max_row` and `row_cache_family` work as on `Options`.
- **Counts:** `PigeonholeReader::row_cache_stats()`: hits, misses and fills (`write_throughs` stays 0: a reader applies no commits).
- **A new writer empties it:** when the writer restarts, recovers or hands over (a new shared-memory generation), the reader re-attaches with an empty cache and the counts start again.
- **It needs the writer's row cache on** (`Options::row_cache`, on by default): only then does the writer raise the watermarks. A writer opened with `row_cache(0)` gives its readers no cache (their reads are uncached, and still right).
- **What bypasses it:** snapshot reads, scans and multi-version reads. Latest reads go through it, sync and async alike.

## Long-held snapshots stall the writer
A reader's oldest live snapshot pins the writer's state as of that snapshot: the memtables and file space the writer would otherwise reclaim. Only live snapshots pin. Once a reader drops its oldest snapshot the pin moves up to its next oldest, and a reader holding no snapshot pins nothing, however long it stays open (#693). A reader that holds one snapshot for a long time while the writer keeps committing eventually fills the writer's memtable budget. The writer's commits then wait for room, and fail with `Busy` after `Options::write_stall_timeout` (30 s by default).

- **This is the reader's responsibility.** Take a snapshot per unit of work, drop it when done, and take a new one for the next.
- **Watch it from the writer:** `engine_metrics()` reports `reader_slots_active` out of `reader_slots`, `reader_slots_reclaimed`, and how far the oldest reader snapshot lags the current state, as `reader_pin_seqno_lag` (commits). A lag that only grows is a reader holding a snapshot.
- A reader process's backup holds its snapshot for the whole copy (ICR 0049), so plan for that on a busy writer.
- **It also holds file space.** A reader's snapshot or backup keeps the SST space it reads, so a `shrink` meanwhile gives back less. It never grows the file to work around the pin; call `shrink` again once the snapshot is dropped for the rest.

## Limits
Every limit is an error, never a hang:

| Limit | What happens | Remedy |
|---|---|---|
| Reader slots: `Options::reader_slots` (default 128, at most 65,536) | One more `open_reader` fails with `NoReaderSlot` at once | Close idle readers, or open the writer with more slots |
| Shared memory: about `memtable_budget × shards + 12 MiB` | The writer's open fails with `ShmUnavailable` if it can't be allocated | Free space in `/dev/shm`, give the container more, or use `shm_dir` |
| A view larger than its shared-memory buffer (very many tablets) | `InvalidArgument` (the message says the view is too large) | Fewer tablets |
| A reader snapshot holding the writer's memory | The writer's commits fail with `Busy` after `write_stall_timeout` | Drop old snapshots (see above) |
| A published view without its manifest | A new snapshot fails with `Busy` after about 1 s (D149) | Retry |

## Requirements
- **Same host, local filesystem.** Network filesystems are refused. Liveness depends on byte-range locks behaving as on a local disk, which NFS and SMB lock managers don't guarantee.
- **On macOS and BSD, never open the database file with other APIs in a process that has it open** (as writer or reader). There, closing *any* descriptor of the file (a `std::fs::File` your code or a library opens and drops, a file watcher, a copy tool) drops *every* lock the process holds on it, including each reader's slot lock. The writer then frees a live reader's slot, and that reader's reads fail with `SnapshotExpired`. Pigeonhole's own handles are safe, and Linux and Windows aren't affected. Use `backup` to copy a live database.
- **Forked children.** A child forked from a reader process can't use or free the parent's reader:
  - **Using it errors:** a new snapshot fails with `Unsupported` ("opened in another process (fork); open a new reader here"), and reads through the parent's snapshots fail with `SnapshotExpired`. Don't open a new reader in the forked child either: `exec` first (or spawn a new process) and open it there, as [Concepts](concepts.md) explains for every handle.
  - **Dropping or closing it releases nothing:** no slot, no lock, no cleanup.
  - **Keeping the descriptor:** on Linux the slot's lock lives on in the child, so if the parent dies first, its slot stays held until the child exits.
  - **Only `fork(3)` is detected,** through `pthread_atfork`, as Python's `os.fork` and `multiprocessing` use it. `_Fork`, `vfork` and a raw `clone` syscall skip the fork handlers: never use a reader after those without `exec`.
- **Write permission on the file.** A reader opens the file read-write (D36), because the processes coordinate with byte-range locks, some of them exclusive. It writes nothing.
- **Same build layout.** The writer and every reader need the same shared-memory layout. Stop reader processes before upgrading the writer: a writer with a newer layout fails with `ShmVersionMismatch` while older readers are attached. Upgrade reader processes in lockstep with the writer: this release's layout (6, D278) holds decided cross-shard commits in a new shared-memory word, layout 5 (D281) added the row caches' write watermarks, and layout 4 (D277) gave memtable nodes a stale-tail field, so readers from earlier releases can't attach to a writer from this one, nor the reverse (`ShmVersionMismatch`). Once every older process has closed, the new writer rebuilds the shared memory.
- **Windows, across sessions.** Shared memory lives in the session's `Local\` namespace by default, so a reader in another session (a service and a desktop process, for example) can't attach. Give the writer and every reader the same `shm_dir` (`Options::shm_dir`, `ReaderOptions::shm_dir`): a file-backed mapping that any session can open.
- **Block cache per process.** Each reader has its own block cache, 256 MiB by default (`ReaderOptions::block_cache`), on top of the writer's.

## Where next
[Concepts](concepts.md#multi-process-readers) · [Errors](errors.md) · [Agent reference](agent-reference.md)
