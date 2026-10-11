# Agent reference

> **Status:** describes `main`; crates.io has 0.2.0 (what `main` adds: [changelog](../../CHANGELOG.md), [`changelog.d/`](../../changelog.d/README.md)). Signatures are authoritative (from `crates/pigeonhole/src`). Samples run as doctests (`#` lines are hidden setup). **P2/P3/P4** mark the phase a feature ships in; "early" marks one that already works. If this page and the rustdoc disagree, the rustdoc wins.

Import: `use pigeonhole::{...}`. Everything is re-exported at the crate root. Errors: [`errors.md`](errors.md).

## Limits and invariants
| Item | Rule |
|---|---|
| Row key, qualifier | Arbitrary bytes, each ≤ 64 KiB, else `KeyTooLarge`. Sorted byte-wise. |
| Value | ≤ 2³²−2 bytes. Above the inline limit `min(WAL segment payload, 64 MiB, ½ memtable arena)` (D16) a put is written to a blob file at commit (one extra manifest commit and file sync, even with `Durability::None`); a merge operand above it is `ValueTooLarge`. Values above the family's `blob_threshold` are stored in blob files. |
| Timestamp | `u64` **microseconds** since the Unix epoch (D11). Default = `max(now, tablet floor + 1)`, never goes backwards. User timestamps are microseconds for TTL. |
| Version order | Newest timestamp first; the same timestamp is ordered by inverted seqno (later commit first). Multiple mutations to the same (row, family, qualifier, timestamp) **within one commit** collapse to the last one written (D34), except that a counter family's increments of one cell add up in order (`incr(1).incr(2)` adds 3; `put_i64(5).incr(1)` gives 6; D186). |
| Atomicity | One `RowMutation` = one row, all families, all-or-nothing. `WriteBatch` = any rows/tables, atomic, one durability point. |
| Builder errors | Surface at `commit`/`read`/`iter`, not at the builder call. |
| Purges (D74) | Delete markers and versions beyond `max_versions` are purged by a bottommost compaction with no snapshot that needs them. A flush also purges versions beyond `max_versions`, but never delete markers: it does so when no other source of the family's tablet (older SSTs, other memtables, prepared cross-shard commits) holds a delete, and it keeps every version a live snapshot can read (#287). After a purge, a write with an **older explicit timestamp** behaves as if they never existed: a `put_at` below a purged delete becomes visible, and deleting the newest version does not bring back a purged older one. Default timestamps are never affected. Counter families never change reads by a purge: their deletes hide only earlier commits, and versions beyond `max_versions` are kept (D186, D187). |
| Delete rule (D9, D38) | `delete_column`/`delete_family` at ts `T` hides every version in scope with ts ≤ `T`, regardless of commit order. `delete_cell(ts)` hides every version at exactly `ts`, also regardless of commit order: a later `put_at(.., ts, ..)` at that timestamp stays hidden. To rewrite a deleted version, use another timestamp. |
| `delete_row` (D10) | One family marker per family, same commit. |
| Read-your-writes (D19) | `commit` returns after durable at level **and** visible. |
| Durability resolution | per-call → writer default → `GroupSync`. |
| Writer | One writer per file; second open → `WriterLocked`. |
| Reader processes (D36, P4, early) | `open_reader` opens the `.phdb` file **read-write** (it never writes): the coordination locks are exclusive byte-range locks, which need a writable handle, as in SQLite WAL mode. Readers need write permission on the file; read-only media are not supported. |
| Family order (D39) | A row's cells come by family in **creation order**, or in the order you listed families (`family(..)` calls); then qualifier; then newest version first. |
| Application-owned mode (D40) | Starts no shard or compaction threads: you drive each `Shard`, and `open_application_owned` with `compaction_cores(k)`, `k > 0`, fails with `InvalidArgument`. The default I/O backend (`IoBackend::Pread`) still starts a pool of 2–16 I/O threads (the CPUs available, clamped) that run WAL syncs, root commits and reads; they inherit the opener's CPU affinity. With `IoBackend::Uring` the engine starts no threads at all (#408): each driving thread completes its own ring's I/O, so wait on `Shard::io_fd` in the event loop (D202) and call `Shard::release` before moving a shard to another thread (#492). |
| Handles | `Pigeonhole`, `Table`, `Snapshot`, `Cell`, `Row` are cheap `Clone`. `Table`: `Send + Sync`. |
| Snapshots | Pin data. Drop promptly. |
| Filesystem | Local only (`NetworkFilesystem`). |
| macOS/BSD file access | Closing any descriptor of the `.phdb` inside the process drops its writer lock (`fcntl` semantics). Never open the file with `std::fs` while it is open here; use `backup` to copy it. See [Concepts](concepts.md#platform-and-process-notes). |
| Reader liveness | By raw PID: writer and readers must share a PID namespace. |
| Custom `Vfs` clock | `monotonic_nanos` must advance at least every 10 µs; coarser clocks are treated as frozen. |
| Storage | Disk-backed: memtables flush into the file as they fill, so data size is bounded by the disk, not `memtable_budget` (per shard, default 64 MiB; also the shm arena size). A write that finds the arena full stalls while a flush frees room; `Busy` after the stall timeout (`Options::write_stall_timeout`, default 30 s) is transient (back off, retry); a batch that can never fit (more than about half the arena) gets `BatchTooLarge` at once, which is not (split it). |
| Reopen budget | Reopening after a crash with fewer shards or a smaller `memtable_budget` works: when the WAL's unflushed data does not fit the arenas, open writes it to SSTs as it replays (a slower open). Only a single commit larger than a shard's arena fails, with `InvalidArgument`. |
| Open cost (D157, D203) | Opening a small database is cheap, whatever the shard count: the first WAL segment of each stream is allocated, not zero-filled, and spare segments are prepared only once a stream is half way through its segment. A clean reopen waits for no flush (no empty manifest commit, no length sync); the new WAL files' and their directory's syncs are ordered before the first durable commit, so the first `GroupSync`/`Sync` commit after open waits for them and reads and weaker commits do not. |
| WAL size (D155) | Each shard bounds the log written past its oldest still-needed record to twice `memtable_budget` (128 MiB at the defaults). A table that is rarely written does not pin the WAL: past the bound its small memtable is flushed to a small SST. So WAL size and the next open's replay stay bounded however long the process runs. The flushes past the bound run in short slices between commits (D206), so the WAL can briefly exceed the bound by what is written while that work runs: a few MB at most at the defaults, against the 128 MiB bound. Size disk space for the bound plus that margin. There is no public option for the bound yet. |
| Files at rest | One file after a clean last `close`. While open, or after a crash: the file plus WAL sidecars and the shm region. Open replays the sidecars. |
| `None` durability (D94) | A `None` commit buffers its WAL record. A later `GroupSync`/`Sync` commit on the same shard, a `flush`, or a clean close makes it durable; a crash before then loses it. |
| Counters (D179) | Only a counter family (`Family::counter()`) takes `incr`; elsewhere `InvalidArgument`. `incr` writes at a fixed timestamp (0), so a counter is one cell; `incr_at(.., ts, ..)` adds to the bucket (version) at `ts`. Increments combine at read and in compaction. A counter family holds only `i64`s (`put_i64` sets, `put_i64_at` sets a bucket; other puts and untyped `merge`: `InvalidArgument`). A delete there hides only what was written before it, so `incr` after `delete_column` starts from 0. With a TTL: buckets only (`incr`/`put_i64`: `InvalidArgument`). TTL expires each bucket; `max_versions` limits reads per column but compaction keeps the rest (it never changes a counter family's reads), so bound storage with a TTL. |
| 0.1.0 families | Families created by 0.1.0 store `pigeonhole.i64_add` without the counter kind and keep 0.1.0 behavior: `incr` at the commit timestamp, runs folded across timestamps (D41), bytes under an `incr` fail with `MergeFailed`. Migrate a counter by reading it and `put_i64` into a counter family. |
| Typed values | `merge` writes untyped operands (custom operators); the built-in `i64` add refuses them. |

## Types
| Type | Role |
|---|---|
| `Pigeonhole` | Writer handle. |
| `PigeonholeReader` | Read-only handle in another process (P4, early). |
| `Shard` | One shard in application-owned mode. |
| `Snapshot` | Point-in-time view. |
| `Options`, `ReaderOptions`, `Family` | Config builders (consume and return `Self`). |
| `Priority` | `Low`, `Normal` (default), `High`. |
| `IoBackend` | `Pread` (default: a thread pool, every platform), `Uring` (io_uring, Linux; open fails with `Unsupported` where unavailable), `Auto` (io_uring where available, else `Pread`). |
| `IoRings { rings: usize, pooled: usize }` | `Pigeonhole::io_rings` result. |
| `Compaction` | `Leveled` (default; read-heavy), `Tiered` (write-heavy), `FifoByTime` (TTL'd time series: drops whole expired files). See [Compaction styles](concepts.md#compaction-styles). |
| `Durability` | `None`, `Buffered`, `GroupSync` (default), `Sync`. |
| `TableBuilder`, `Table`, `ReadTable` | Define/open a table; read-write handle; read-only handle (P4, early). |
| `RowMutation`, `WriteBatch`, `Transaction` | Writes; `Transaction` is P4, early. |
| `CommitInfo { seqno: u64, durability: Durability }` | Commit result. |
| `RowRead`, `Scan`, `RowIter` | Read builders; scan iterator. |
| `CommitTicket` | A submitted commit to wait on or check later (`commit_with_ticket`). |
| `nonblocking::{GetFuture, RowFuture, RowStream, CommitFuture, CheckFuture, MaintenanceFuture}` | Async results (feature `async`, default on). |
| `ValueFilter`, `Condition` | Value predicate; `commit_if` condition. |
| `CellRef<'a>`, `Cell`, `Row`, `RowRef<'a>`, `CellEntry<'a>`, `Value<'a>` | Borrowed and owned results. |
| `Error`, `ErrorCode`, `Result<T>` | Errors. |
| `MergeOperator`, `MergeError` | Custom merge operators: an associative fold over stored values (tag byte, then payload). |
| `days(n: u64) -> Duration` | TTL helper. |

## `Pigeonhole`
| Signature | Semantics |
|---|---|
| `open(path: impl AsRef<Path>, Options) -> Result<Pigeonhole>` | Open or create as writer; replays WAL. |
| `open_reader(path, ReaderOptions) -> Result<PigeonholeReader>` | P4, early. Read-only, any number of processes. Needs write permission on the file (D36). |
| `open_application_owned(path, Options) -> Result<(Pigeonhole, Vec<Shard>)>` | Writer with no shard or compaction threads (the default `Pread` backend still runs 2–16 I/O threads; `Uring` runs none); you drive each `Shard`. `compaction_cores(k > 0)` → `InvalidArgument` (D40). |
| `table(&self, name: &str) -> Result<TableBuilder<'_>>` | Start define/open. |
| `tables(&self) -> Vec<String>` | Table names. |
| `drop_table(&self, name: &str) -> Result<()>` | Drop table and data. |
| `write_batch(&self) -> WriteBatch` | New multi-row batch. |
| `transaction(&self) -> Result<Transaction>` | P4, early. Optimistic transaction. |
| `row_cache_stats(&self) -> RowCacheStats` | Row cache `hits`, `misses`, `fills` since open (all 0 when off). |
| `io_rings(&self) -> Option<IoRings>` | With `IoBackend::Uring`: `rings` (the shared one plus one per shard thread that got its own) and `pooled` (rings with a registered buffer pool; `pooled < rings` means the locked-memory limit, `RLIMIT_MEMLOCK`, was too low for every ring to get one: correct, but each read pins its pages). `None` on other backends. Also on `PigeonholeReader`. |
| `snapshot(&self) -> Result<Snapshot>` | Consistent view of everything committed. |
| `default_durability(&self) -> Durability` | Writer default. |
| `set_default_durability(&self, Durability)` | Applies to later commits. |
| `flush(&self) -> Result<()>` | Write every memtable into the file; returns when the SSTs are in the manifest. Makes `None` commits durable. Fails with `Busy` if the flush finds no room for its fresh memtables past the stall timeout, and with the flush's own error (`Io`, `NoSpace`) if it fails. |
| `compact(&self) -> Result<()>` | Flush, then merge every level of every table into the last (purges per `max_versions`, TTL and tombstones). Reports only a failure of the compaction it started for this call, at once. A failed background compaction backs off (1 s, doubling to 60 s per table-and-family) and is never handed to a later `compact()`. |
| `shrink(&self) -> Result<u64>` | Truncate free space at the end of the file, relocate live data from the tail into free space and truncate again; returns bytes released (`0` if none). Online; costs a rewrite of the tail data. Call after deletes + `compact`. Relocates SST and blob extents (D185); after `compact` + `shrink` a file above a few MiB is about 1.05–1.2× its live data (D183), a small one about 1 MiB of metadata. Data with nowhere lower to go stays, not an error. Errors: `Closed`, `ReadOnly`, `NoSpace` (disk full while moving the manifest), `Io`. |
| `backup(&self, dest: impl AsRef<Path>) -> Result<()>` | Consistent single-file copy at a snapshot taken now, while writes continue. `dest` must not exist. Holds its snapshot's memtables only while it copies them (at most one arena's worth written to the copy); the long SST copy that follows pins file extents, not memtable space, so writers are not stalled by it. The copy opens with no WAL replay and no sidecars. Separated values are copied into the copy's own blob files (only those it references). |
| `close(self) -> Result<()>` | Flushes memtables, checkpoints the WAL; the last handle out removes the sidecars and shm, leaving one file. |

`PigeonholeReader` (P4, early): `table(&self, &str) -> Result<ReadTable>`, `tables() -> Vec<String>`, `snapshot() -> Result<Snapshot>`. No write methods.
`Snapshot`: `seqno(&self) -> u64`.
`Shard`: `index() -> usize`, `run_once(&mut self, budget: Duration) -> bool` (true if work remains; background work waiting for a time does not count), `next_wakeup(&self) -> Option<Duration>` (time until that background work is due; with `tablet_changes` on, the default, the balancer's next pass keeps it `Some`: at most 100 ms after a write, backing off to 10 s while idle), `set_wakeup(&mut self, Box<dyn Fn() + Send + Sync>)` (fires when work arrives, not when background work falls due). `io_fd(&self) -> Option<i32>` (with `IoBackend::Uring`: a descriptor that turns readable when I/O this thread submitted has finished and waits for `run_once`; take it on the driving thread after its first `run_once`. Taking it is the opt-in that stops `next_wakeup` reporting in-flight I/O as due now, so wait on it from then on; `None` on `Pread`, without io_uring or with a custom VFS; D202). `closed(&self) -> Option<Result<()>>` (the close's outcome once the whole close has finished), `release(&mut self)` (before handing the shard to another thread: drives it until the calling thread has none of its I/O in flight; required with `IoBackend::Uring` in application-owned mode, where only the submitting thread completes its ring's I/O, and a no-op on pread and in engine-owned mode; debug builds panic on a move that skipped it; #492). Loop: `run_once` until `false`, then sleep until the wakeup fires (work arrived or I/O completed) or `next_wakeup` passes. After `close()`, keep looping until `closed()` is `Some`, then drop the shard; `close()` on a thread that drives no shard (once every shard has been run) waits for this and returns the outcome; on a driving thread, or before every shard has run, it returns `Ok(())` at once and the outcome is only in `closed()`. On a thread that drives a shard (it last called `run_once`), `commit`, `check_and_mutate`, transaction commits, `flush` and `compact` fail with `InvalidArgument` before submitting anything; awaiting an already-submitted commit there blocking fails with `WouldDeadlock` (it will apply: poll it from the event loop). A thread holding a shard it never ran is not detected: run the shard first.

## `Options` (all `self -> Self`; process-local, not stored in file)
| Method | Meaning |
|---|---|
| `durability(Durability)` | Writer default (default `GroupSync`). |
| `shards(usize)` | Shard threads (default CPUs available). `1` is valid. With few concurrent writing threads, set it to about that number: thinly spread writes pay a shard wakeup per commit (see [Getting started](getting-started.md)). |
| `compaction_cores(usize)` | Extra threads for flush/compaction (pinned with `pin_threads`). Engine-owned mode only (D40). |
| `pin_threads(bool)` | Pin shard `i` (and compaction threads) to the `i`-th CPU of the opener's affinity set (default off). Engine-owned mode only. Turn on only when this database owns those CPUs: two pinned databases, quota-limited containers or a pinned opener stack shards on the same cores. |
| `memtable_budget(u64)` | Arena bytes per shard (default 64 MiB). The shm region is `memtable_budget × shards` (each arena rounded up to 2 MiB) plus about 10 MiB for views and reader slots, filled as memtables grow; open fails with `ShmUnavailable` if its filesystem lacks that much free space. Something else filling that tmpfs after open can still crash the process with `SIGBUS`. |
| `commit_spin(Duration)` | How long a thread waiting for a `Buffered` or `None` commit polls before it sleeps (default 15 µs; D198). Durable commits sleep at once. `Duration::ZERO` never polls (battery-powered or CPU-constrained hosts). |
| `shard_spin(Duration)` | How long an engine-owned shard that just handled a commit polls for the next one (default 50 µs; D198). Idle databases never poll; ignored by application-owned shards. `Duration::ZERO` never polls. |
| `write_stall_timeout(Duration)` | How long a stalled write, `flush` or `compact` waits before `Busy` (default 30 s; `Duration::ZERO` refuses at once). |
| `block_cache(usize)` | Block cache bytes (default 256 MiB; each reader process has its own). |
| `row_cache(usize)` | Row cache bytes on top of the block cache (default 0 = off; D201). Serves latest row reads of the newest version (no time range; qualifier, column-limit and value filters applied to the cached row) and point gets from cached family rows; results equal uncached reads, and a write makes the row's copy miss at once. Writer process only. |
| `row_cache_max_row(usize)` | Largest family row stored, encoded bytes (default 4 KiB). |
| `row_cache_family(&str, &str)` | Serve only this `(table, family)`; call once per family (default: every family). |
| `io_backend(IoBackend)` | I/O backend (default `IoBackend::Pread`). `Uring` fails the open with `Unsupported` where io_uring is unavailable; `Auto` falls back to `Pread`. |
| `direct_io(bool)` | Read and write SST and blob extents with direct I/O (`O_DIRECT`, `F_NOCACHE`, `FILE_FLAG_NO_BUFFERING`) through a second handle, so the block cache is their only cache (default off; #403). Superblocks, manifest and WAL stay buffered; a file system that refuses direct I/O keeps buffered I/O. Size `block_cache` for the working set when on. |
| `shm_dir(impl Into<PathBuf>)` | Shared-memory file directory (e.g. a tmpfs), instead of `/dev/shm` (Linux), `shm_open` (macOS/BSD) or the pagefile (Windows). Must exist. |
| `create_if_missing(bool)` | Default true. |
| `merge_operator(Arc<dyn MergeOperator>)` | Register a custom operator (families name it). |
| `allow_unregistered_merge_operators(bool)` | Open read-only with compaction off if a family names an unregistered operator. |
| `allow_fuse(bool)` | Accept a database on FUSE (default off; D173). Only for a trusted **local** FUSE mount: its locks may be host-local and its sync may not be durable. Network filesystems stay refused. Also on `ReaderOptions`. |
| `tablet_changes(bool)` | Let tablets split, merge and move between shards so one table's writes spread over every shard (default on; off keeps each table as one tablet on one shard). Tablet owners are not stored; a reopen places tablets again. Commits in flight together on one row may apply in either order while its tablet moves. |

`ReaderOptions` (P4, early): `block_cache(usize)` (default 256 MiB **per reader process**, on top of the writer's), `shm_dir(..)`, `merge_operator(..)`, `allow_fuse(bool)`, `io_backend(IoBackend)`, `direct_io(bool)` (as on `Options`).

## `Family` (all `self -> Self`; stored in file)
| Method | Meaning |
|---|---|
| `Family::default()` | Ordinary family, no merge operator. |
| `Family::counter()` | Counter family: `i64` sums (`incr`, `incr_at`, `put_i64`, `put_i64_at`); D179. Chain the methods below on it. |
| `max_versions(u32)` | Keep ≤ n versions per column (0 = all). |
| `ttl(Duration)` | Expire cells older than this by timestamp. |
| `bloom_bits(u8)` | Filter bits per key (0 off; default 10). |
| `blob_threshold(u32)` | Values longer than this go to blob files at flush or compaction (default 4096; `u32::MAX` never). Blob records are not compressed. Blob GC rewrites files that are half garbage; `compact()` empties every file with garbage. |
| `lz4()` | Default compression. |
| `zstd(i8)` | zstd blocks at a libzstd level (1–22, higher smaller and slower; default 3). |
| `uncompressed()` | No compression. |
| `block_size(u32)` | Data block bytes (default 16 KiB). |
| `merge_operator(&str)` | Name of a registered operator (unregistered: `UnknownMergeOperator`) for `merge` operands. Counters use `Family::counter()` instead; `"pigeonhole.i64_add"` on an ordinary family makes a 0.1.0-style family. |
| `cache_priority(Priority)` | Block cache priority. |
| `compaction(Compaction)` | Strategy. `Leveled`: reads. `Tiered`: write-heavy; keep the default engine depth (a shallow tree makes write amplification grow linearly, D169). `FifoByTime`: drops a file when its newest cell has expired, so it only drops data with a TTL set (without one nothing expires), dropped on a timer at the earliest expiry (D170). The engine's FIFO size cap is lossy (D167) and not exposed. |

## `TableBuilder` / `Table` / `ReadTable`
| Signature | Semantics |
|---|---|
| `TableBuilder::family(self, &str, Family) -> Self` | Declare family; adds if missing, existing keeps stored options. |
| `TableBuilder::create_if_missing(self) -> Result<Table>` | Open or create. |
| `TableBuilder::create(self) -> Result<Table>` | `TableExists` if present. |
| `TableBuilder::open(self) -> Result<Table>` | `TableNotFound` if absent. |
| `Table::name(&self) -> &str` | |
| `Table::families(&self) -> Vec<&str>` | |
| `Table::mutate(&self, row: &[u8]) -> RowMutation<'_>` | Start single-row mutation. |
| `Table::get(&self, row: &[u8], family: &str, qualifier: &[u8]) -> Result<Option<CellRef<'_>>>` | Newest version; zero-copy. |
| `Table::get_at(&self, &Snapshot, row, family, qualifier) -> Result<Option<CellRef<'_>>>` | As of snapshot. |
| `Table::row(&self, row: &[u8]) -> RowRead<'_>` | Start row read. |
| `Table::scan<K: AsRef<[u8]> + ?Sized>(&self, range: impl RangeBounds<&K>) -> Scan<'_>` | Row range. Both ends same type (use slices). |
| `Table::scan_prefix(&self, prefix: &[u8]) -> Scan<'_>` | Rows starting with prefix. |
| `Table::scan_bounds(&self, Bound<&[u8]>, Bound<&[u8]>) -> Scan<'_>` | Explicit bounds. |
| `Table::shard_of(&self, row: &[u8]) -> Option<usize>` | The shard owning `row` now (`Shard::index`): a routing hint for application-owned mode (commit owned rows inline); may change after a split or move; commits are correct either way (ICR 0022). |

`ReadTable` (P4, early) has `name`, `get`, `get_at`, `row`, `scan`, `scan_prefix`, `scan_bounds` with the same signatures.

## `RowMutation` (builder; each `self -> Self`)
| Method | Semantics |
|---|---|
| `put(family, qualifier: &[u8], value: &[u8])` | Bytes at commit timestamp. |
| `put_at(family, qualifier, ts: u64, value)` | Bytes at explicit timestamp (µs). |
| `put_i64(family, qualifier, i64)` / `put_f64(.., f64)` | Typed values. In a counter family `put_i64` sets the counter. |
| `put_i64_at(family, qualifier, ts, i64)` | Typed `i64` at `ts` (a counter family's bucket). |
| `incr(family, qualifier, delta: i64)` | Blind atomic wrapping `i64` add; counter families only. |
| `incr_at(family, qualifier, ts: u64, delta: i64)` | Add to the bucket at `ts`; counter families only. |
| `merge(family, qualifier, operand: &[u8])` | Operand for the family's registered operator. |
| `delete_cell(family, qualifier, ts: u64)` | Delete the version at `ts`; later puts at that `ts` stay hidden (D38). |
| `delete_column(family, qualifier)` | Delete all versions. |
| `delete_family(family)` | Delete all columns of a family in this row. |
| `delete_row()` | Delete whole row. |
| `durability(Durability)` | Override for this commit. |
| `commit(self) -> Result<CommitInfo>` | Commit. |
| `commit_if(self, &Condition) -> Result<Option<CommitInfo>>` | Compare-and-set on this row (BigTable `check_and_mutate`); `None` if the condition failed and nothing was written. |

`Condition` variants: `Exists { family: String, qualifier: Vec<u8> }`, `Absent { .. }`, `Value { family, qualifier, filter: ValueFilter }`.

## `WriteBatch` (methods take `&mut self -> &mut Self`)
| Method | Semantics |
|---|---|
| `put(&Table, row, family, qualifier, value)` | |
| `put_at(&Table, row, family, qualifier, ts, value)` | |
| `put_i64(&Table, row, family, qualifier, i64)` / `put_f64(.., f64)` | Typed values. |
| `put_i64_at(&Table, row, family, qualifier, ts, i64)` | |
| `incr(&Table, row, family, qualifier, delta)` | Counter families only. |
| `incr_at(&Table, row, family, qualifier, ts, delta)` | Counter families only. |
| `merge(&Table, row, family, qualifier, operand)` | Untyped operand (for custom operators). |
| `delete_cell(&Table, row, family, qualifier, ts)` | D38. |
| `delete_column(&Table, row, family, qualifier)` | |
| `delete_family(&Table, row, family)` | |
| `delete_row(&Table, row)` | |
| `len(&self) -> usize`, `is_empty(&self) -> bool` | |
| `commit(self) -> Result<CommitInfo>` | Writer default durability. |
| `commit_with(self, Durability) -> Result<CommitInfo>` | Override. |

`Transaction` (P4, early): `get(&mut self, &Table, row, family, qualifier) -> Result<Option<CellRef<'_>>>`, `put(..)`, `delete_column(..)` (as `WriteBatch`, `&mut Self`), `commit(self)`, `commit_with(self, Durability)`; `Conflict` on a conflicting commit.

## `RowRead` / `Scan` (builders, `self -> Self`)
| Method | `RowRead` | `Scan` |
|---|---|---|
| `families(impl IntoIterator<Item = &str>)`, `family(&str)` | ✓ | ✓ |
| `qualifier_prefix(&[u8])` | ✓ | ✓ |
| `qualifier_range(impl RangeBounds<&K>)`, `qualifier_bounds(Bound<&[u8]>, Bound<&[u8]>)` | ✓ | ✓ |
| `latest()` (default), `versions(u32)` (0 = all retained) | ✓ | ✓ |
| `time_range(Range<u64>)` | ✓ | ✓ |
| `column_limit(u32)` | ✓ | |
| `columns_per_row(u32)` | | ✓ |
| `value_filter(ValueFilter)` | ✓ | ✓ |
| `limit(u64)` rows (0: none) | | ✓ |
| `snapshot(&Snapshot)` | ✓ | ✓ |
| terminal | `read(self) -> Result<Option<RowRef<'t>>>` | `iter(self) -> Result<RowIter<'t>>` |

`ValueFilter`: `Equals(Vec<u8>)`, `Prefix(Vec<u8>)`, `I64(std::cmp::Ordering, i64)`.
`RowIter`: `Iterator<Item = Result<Row>>`; `next_ref(&mut self) -> Result<Option<RowRef<'_>>>` (zero-copy, valid until next call).
Pushdown (D22): qualifier and time filters in the block decoder; versions, column limits, value filters in resolution on snapshot-visible data. Deletes and merge operands always pass the decoder filters. Details: [`scans-and-filters.md`](scans-and-filters.md).

## Results
| Type | Methods |
|---|---|
| `CellRef<'a>` | `value() -> &[u8]`, `typed() -> Value<'_>`, `as_i64() -> Option<i64>`, `timestamp() -> u64`, `to_owned() -> Cell` |
| `Cell` (owned, `'static`, `Send + Sync`) | `value`, `typed`, `as_i64`, `timestamp` |
| `RowRef<'a>` | `key() -> &[u8]`, `len()`, `is_empty()`, `entry(i) -> Option<CellEntry<'_>>`, `iter()`, `get(family, qualifier) -> Option<CellRef<'_>>`, `to_owned() -> Row` |
| `Row` (owned) | `key()`, `len()`, `is_empty()`, `entry(i) -> Option<(&str, &[u8], &Cell)>`, `get(family, qualifier) -> Option<&Cell>`, `view() -> RowRef<'_>` |
| `CellEntry<'a>` | public fields `family: &str`, `qualifier: &[u8]`, `cell: CellRef<'a>` |
| `Value<'a>` | `Bytes(&[u8])`, `I64(i64)`, `F64(f64)`, `Varint(i64)` |
| `CommitInfo` | `seqno`, `durability` |

Cells within a row: ordered by family (creation order, or the order the read listed families; D39), then qualifier, then newest version first.

## Error codes (`ErrorCode`, `#[non_exhaustive]`, `repr(u32)`)
`Io`=1 `Corruption`=2 `WriterLocked`=3 `ShmVersionMismatch`=4 `ShmUnavailable`=5 `UnsupportedFormat`=6 `NetworkFilesystem`=7 `TableNotFound`=8 `TableExists`=9 `FamilyNotFound`=10 `FamilyExists`=11 `UnknownMergeOperator`=12 `MergeFailed`=13 `Conflict`=14 `ReadOnly`=15 `KeyTooLarge`=16 `ValueTooLarge`=17 `NoSpace`=18 `InvalidArgument`=19 `Unsupported`=20 `Closed`=21 `NoReaderSlot`=22 `RecordTooLarge`=23 `Busy`=24 `SnapshotExpired`=25 `WouldDeadlock`=26 `BatchTooLarge`=27. Causes and fixes: [`errors.md`](errors.md). `Error::code()`, `Error::message()`.

## Async (feature `async`, default on; module `nonblocking`)
Same semantics as the blocking call; any executor; no `spawn_blocking`. Guide: [`async.md`](async.md).

| Signature | Semantics |
|---|---|
| `Table::get_async(&self, row, family, qualifier) -> GetFuture` / `get_at_async(&self, &Snapshot, ..)` | `Output = Result<Option<Cell>>`. Also on `ReadTable`. Read point taken on first poll; cache hits ready on first poll. |
| `RowRead::read_async(self) -> RowFuture` | `Output = Result<Option<Row>>`. |
| `Scan::stream(self) -> RowStream<'t>` | `futures_core::Stream<Item = Result<Row>>`, key order; setup error is the first item. Fetches the next step's blocks only as polled. |
| `RowMutation::commit_async(self) -> CommitFuture` | `Output = Result<CommitInfo>`. Submitted at the call; resolves when durable and visible. |
| `WriteBatch::commit_async(self)` / `commit_with_async(self, Durability) -> CommitFuture` | As above. |
| `Transaction::commit_async(self)` / `commit_with_async(self, Durability) -> CommitFuture` | `Conflict` on a conflicting commit. |
| `RowMutation::commit_if_async(self, &Condition) -> CheckFuture` | `Output = Result<Option<CommitInfo>>`; `None` if the condition failed. |
| `Transaction::get_async(&mut self, &Table, row, family, qualifier) -> GetFuture` | Read recorded at the call (a dropped future still counts at commit). |
| `Pigeonhole::flush_async(&self)` / `compact_async(&self) -> MaintenanceFuture` | `Output = Result<()>`; dropping does not stop it. |
| `WriteBatch::commit_with_ticket(self, Durability) -> Result<CommitTicket>` | No feature needed. `CommitTicket`: `wait(self)`, `try_result(&mut self) -> Option<Result<CommitInfo>>`, `seqno(&mut self) -> Option<u64>`; `IntoFuture` with `async`. |
| `Pigeonhole::async_sync_reads(&self) -> u64` (also `PigeonholeReader`) | Reads an async call did synchronously (oversized blob, uncacheable block, unpredicted scan block; #398). |

Dropping a read future or stream: always safe. Dropping a commit future or ticket: the commit still lands or fails atomically.
Sync-only by design (D196): `backup`, `shrink` (long-running; run on your executor's blocking pool), and open, close, table create/open/drop (short).

## Recipes
### 1. Open, create table, write, read
```rust
use pigeonhole::{Family, Options, Pigeonhole};

# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
let users = db.table("users")?
    .family("profile", Family::default().max_versions(1))
    .create_if_missing()?;

users.mutate(b"user:42").put("profile", b"name", b"Ada").commit()?;
let name = users.get(b"user:42", "profile", b"name")?.map(|c| c.value().to_vec());
db.close()?;
# assert_eq!(name.as_deref(), Some(&b"Ada"[..]));
# Ok::<(), pigeonhole::Error>(())
```

### 2. Counter
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let users = pigeonhole::doc_support::table(&db, "users", &["profile"])?;
let stats = db.table("user_stats")?.family("stats", Family::counter()).create_if_missing()?;
stats.mutate(b"user:42").incr("stats", b"logins", 1).commit()?;
let n: i64 = stats.get(b"user:42", "stats", b"logins")?
    .and_then(|c| c.as_i64()).unwrap_or(0);
# assert_eq!(n, 1);
# Ok::<(), pigeonhole::Error>(())
```

### 3. Prefix scan with pagination
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let users = pigeonhole::doc_support::table(&db, "users", &["profile"])?;
# for i in 0..250 { users.mutate(format!("user:{i:03}").as_bytes()).put("profile", b"name", b"x").commit()?; }
# let mut seen = 0;
use std::ops::Bound;

let mut after: Option<Vec<u8>> = None;
loop {
    let start = match &after { Some(k) => Bound::Excluded(k.as_slice()), None => Bound::Included(&b"user:"[..]) };
    // Upper bound of prefix "user:" is "user;" (':' + 1).
    let mut it = users
        .scan_bounds(start, Bound::Excluded(&b"user;"[..]))
        .family("profile")
        .limit(100)
        .iter()?;
    let mut n = 0;
    while let Some(row) = it.next_ref()? {
        after = Some(row.key().to_vec());
        n += 1;
        // use row
#       seen += 1;
    }
    if n < 100 { break; }
}
# assert_eq!(seen, 250);
# Ok::<(), pigeonhole::Error>(())
```

### 4. Atomic multi-row write with chosen durability
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let users = pigeonhole::doc_support::table(&db, "users", &["profile"])?;
use pigeonhole::Durability;

let mut wb = db.write_batch();
wb.put(&users, b"user:1", "profile", b"name", b"A")
  .put(&users, b"user:2", "profile", b"name", b"B")
  .delete_row(&users, b"user:0");
let info = wb.commit_with(Durability::GroupSync)?;
assert_eq!(info.durability, Durability::GroupSync);
# Ok::<(), pigeonhole::Error>(())
```

### 5. Consistent multi-read, versions, event time
```rust
# use pigeonhole::*;
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let users = pigeonhole::doc_support::table(&db, "users", &["profile"])?;
# let (t0_us, t1_us, event_ts_us) = (0u64, u64::MAX, 1_759_622_400_000_000u64);
let snap = db.snapshot()?;
let a = users.get_at(&snap, b"user:1", "profile", b"name")?.map(|c| c.to_owned());
let history = users
    .row(b"user:1").family("profile").qualifier_prefix(b"name")
    .versions(5).time_range(t0_us..t1_us).snapshot(&snap).read()?;
drop(snap); // release promptly

users.mutate(b"user:1").put_at("profile", b"name", event_ts_us, b"Ada").commit()?;
# Ok::<(), pigeonhole::Error>(())
```
