# Agent reference

> **Status:** describes `main`; crates.io has 0.3.3 (what `main` adds: [changelog](../../CHANGELOG.md), [`changelog.d/`](../../changelog.d/README.md)). Signatures are authoritative (from `crates/pigeonhole/src`). Samples run as doctests (`#` lines are hidden setup). **P2/P3/P4** mark the phase a feature ships in; "early" marks one that already works. If this page and the rustdoc disagree, the rustdoc wins.

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
| Visible before durable (D227) | Other readers see a commit once it is applied, before its fsync, at every level. A power loss can remove what they saw. Across shards, a later write may survive an earlier one it was derived from. A backup may hold commits the database later loses. A seqno or default timestamp a reader saw may be reissued after a power loss, so don't use other clients' seqnos as durable cursors. `durable_snapshot()` (ICR 0037) sees only commits durable at their level (`GroupSync`/`Sync`: power loss; `Buffered`: process crash; `None`: nothing); `backup_durable` copies only those. |
| Durability resolution | per-call → writer default → `GroupSync`. |
| Writer | One writer per file; second open → `WriterLocked`. |
| `fork` (ICR 0067, #901) | In a forked child, a handle inherited from the parent may only be dropped or closed: every other call → `Unsupported`. Opening any database or reader in the child is unsupported as well: `exec` (or spawn) first. Without `exec` the child can block on a lock the parent's threads held, and on macOS it aborts. Only `fork(3)` is detected. |
| Reader processes (D36, P4, early) | `open_reader` opens the `.phdb` file **read-write** (it never writes): the coordination locks are exclusive byte-range locks, which need a writable handle, as in SQLite WAL mode. Readers need write permission on the file; read-only media are not supported. |
| Family order (D39) | A row's cells come by family in **creation order**, or in the order you listed families (`family(..)` calls); then qualifier; then newest version first. |
| Application-owned mode (D40) | Starts no shard or compaction threads: you drive each `Shard`, flush and compaction run on it (or on background workers you also drive: `background_workers(n)` with `open_application_owned_with`, D284), `compaction_cores` defaults to 0 there, and `open_application_owned` with `compaction_cores(k)`, `k > 0`, fails with `InvalidArgument`. With io_uring (the default on Linux, `IoBackend::Auto`, D218; or `IoBackend::Uring`) the engine starts no threads at all (#408): each driving thread completes its own ring's I/O, so wait on `Shard::io_fd` in the event loop (D202); a loop that never takes it keeps polling while I/O is in flight. A thread that drives no shard, or a driving thread the kernel refuses a ring (`ENOMEM`, retried later; or a kernel without the support), runs the I/O it submits inline, blocking as on pread (#929; counted in `io_rings()`: `attach_failures`, `inline`). Call `Shard::release` before moving a shard to another thread (#492). With `pread` (other systems, kernels without io_uring, or `IoBackend::Pread`) a pool of 2–16 I/O threads (the CPUs available, clamped) runs WAL syncs, root commits and reads; they inherit the opener's CPU affinity. |
| Handles | `Pigeonhole`, `Table`, `Snapshot`, `Cell`, `Row` are cheap `Clone`. `Table`: `Send + Sync`. |
| Snapshots | Pin data. Drop promptly. |
| Filesystem | Local only (`NetworkFilesystem`). |
| macOS/BSD file access | Closing any descriptor of the `.phdb` inside the process drops its writer lock (`fcntl` semantics). Never open the file with `std::fs` while it is open here; use `backup` to copy it. See [Concepts](concepts.md#platform-and-process-notes). |
| Reader liveness | By a byte-range lock per reader slot (layout 3, #693), not by PID: works across PID namespaces. A killed reader's slot is freed within about 250 ms of the writer's next flush, compaction or room stall. |
| Custom `Vfs` clock | `monotonic_nanos` must advance at least every 10 µs; coarser clocks are treated as frozen. |
| Storage | Disk-backed: memtables flush into the file as they fill, so data size is bounded by the disk, not `memtable_budget` (per shard, default 64 MiB; also the shm arena size). A write that finds the arena full stalls while a flush frees room; `Busy` after the stall timeout (`Options::write_stall_timeout`, default 30 s, each call's own: from when it began waiting for room, or from submission for a commit waiting on a tablet change, including a cross-shard commit's retries, and for a cross-shard commit a participant keeps refusing below its timestamp floor, tablet changes on or off, D240, D241, D244) is transient: the commit was not committed, so back off and retry (an L0 stall only slows commits); a batch that can never fit (more than about half the arena) gets `BatchTooLarge` at once, which is not (split it). |
| Reopen budget | Reopening after a crash with fewer shards or a smaller `memtable_budget` works: when the WAL's unflushed data does not fit the arenas, open writes it to SSTs as it replays (a slower open). Only a single commit larger than a shard's arena fails, with `InvalidArgument`. |
| Open cost (D157, D203) | Opening a small database is cheap, whatever the shard count: the first WAL segment of each stream is allocated, not zero-filled, and spare segments are prepared only once a stream is half way through its segment. A clean reopen waits for no flush (no empty manifest commit, no length sync); the new WAL files' and their directory's syncs are ordered before the first durable commit, so the first `GroupSync`/`Sync` commit after open waits for them and reads and weaker commits do not. |
| WAL size (D155) | Each shard bounds the log written past its oldest still-needed record to twice `memtable_budget` (128 MiB at the defaults). A table that is rarely written does not pin the WAL: past the bound its small memtable is flushed to a small SST. So WAL size and the next open's replay stay bounded however long the process runs. The flushes past the bound run in short slices between commits (D206), so the WAL can briefly exceed the bound by what is written while that work runs: a few MB at most at the defaults, against the 128 MiB bound. Size disk space for the bound plus that margin. There is no public option for the bound yet. |
| Files at rest | One file after a clean last `close`. While open, or after a crash: the file plus WAL sidecars and the shm region. Open replays the sidecars. |
| `None` durability (D94) | A `None` commit buffers its WAL record. A later `GroupSync`/`Sync` commit on the same shard, a `flush`, or a clean close makes it durable; a crash before then loses it. |
| Counters (D179) | Only a counter family (`Family::counter()`) takes `incr`; elsewhere `InvalidArgument`. `incr` writes at a fixed timestamp (0), so a counter is one cell; `incr_at(.., ts, ..)` adds to the bucket (version) at `ts`. Increments combine at read and in compaction; a read that folds many leaves its sum in memory as a base, so later reads fold only what came after (D291). A counter family holds only `i64`s (`put_i64` sets, `put_i64_at` sets a bucket; other puts and untyped `merge`: `InvalidArgument`). A delete there hides only what was written before it, so `incr` after `delete_column` starts from 0. With a TTL: buckets only (`incr`/`put_i64`: `InvalidArgument`). TTL expires each bucket; `max_versions` limits reads per column but compaction keeps the rest (it never changes a counter family's reads), so bound storage with a TTL. |
| 0.1.0 families | Families created by 0.1.0 store `pigeonhole.i64_add` without the counter kind and keep 0.1.0 behavior: `incr` at the commit timestamp, runs folded across timestamps (D41), bytes under an `incr` fail with `MergeFailed`. Migrate a counter by reading it and `put_i64` into a counter family. |
| Typed values | `merge` writes untyped operands (custom operators); the built-in `i64` add refuses them. |

## Types
| Type | Role |
|---|---|
| `Pigeonhole` | Writer handle. |
| `PigeonholeReader` | Read-only handle in another process (P4, early). |
| `Shard` | One shard in application-owned mode. |
| `AppOwned` | `#[non_exhaustive]` `{ db: Pigeonhole, shards: Vec<Shard>, workers: Vec<BackgroundWorker> }`, from `open_application_owned_with` (D284, ICR 0078). |
| `BackgroundWorker` | One background worker for flush and compaction in application-owned mode (D284). |
| `Snapshot` | Point-in-time view. |
| `Options`, `ReaderOptions`, `Family` | Config builders (consume and return `Self`). |
| `Priority` | `Low`, `Normal` (default), `High`. |
| `IoBackend` | `Auto` (default, D218: io_uring where available, else `Pread`), `Pread` (a thread pool, every platform), `Uring` (io_uring, Linux; open fails with `Unsupported` where unavailable). |
| `IoRings { rings: usize, pooled: usize, attach_failures: u64, inline: u64 }` | `#[non_exhaustive]`; `Pigeonhole::io_rings` and `PigeonholeReader::io_rings` result. |
| `Compaction` | `Leveled` (default; read-heavy), `Tiered` (write-heavy), `FifoByTime` (TTL'd time series: drops whole expired files). See [Compaction styles](concepts.md#compaction-styles). |
| `Durability` | `None`, `Buffered`, `GroupSync` (default), `Sync`. |
| `TableBuilder`, `Table`, `ReadTable` | Define/open a table; read-write handle; read-only handle (P4, early). |
| `RowMutation`, `WriteBatch`, `Transaction` | Writes; `Transaction` is P4, early. |
| `CommitInfo { seqno: u64, durability: Durability }` | Commit result. |
| `RowRead`, `Scan`, `RowIter` | Read builders; scan iterator. |
| `CommitTicket` | A submitted commit to wait on or check later (`commit_with_ticket`). |
| `nonblocking::{GetFuture, RowFuture, RowStream, CommitFuture, CheckFuture, MaintenanceFuture, TransactionCommitFuture}` | Async results (feature `async`, default on). |
| `ValueFilter`, `Condition` | Value predicate; `commit_if` condition. |
| `CellRef<'a>`, `Cell`, `Row`, `RowRef<'a>`, `CellEntry<'a>`, `Value<'a>` | Borrowed and owned results. |
| `DatabaseInfo`, `TableInfo`, `FamilyInfo`, `FamilyStorage`, `LevelInfo`, `Compression` | `info()` and `family_info()` results (ICR 0047; all `#[non_exhaustive]`, public fields). |
| `check(path, CheckOptions) -> Result<CheckReport>` | Offline integrity check (ICR 0048): see below. |
| `CheckOptions`, `CheckReport`, `Finding`, `Structure`, `Severity`, `Region`, `RegionKind`, `WalFile`, `CheckCounts` | Its options and result (`#[non_exhaustive]`, public fields). |
| `Error`, `ErrorCode`, `Result<T>` | Errors. |
| `CpuProfile` | `Embedded` (default), `Server`: the `Options::cpu_profile` argument (D289). |
| `Recovery`, `RecoveryStats` | The `Options::auto_recover` argument (`Off` default, `Io`, `Any`) and `recovery_stats()` result (D265). |
| `RetryPolicy` | `transact`'s retry policy (ICR 0060). |
| `EngineMetrics`, `CommitStats`, `CrossShardMetrics`, `RowCacheStats` | `engine_metrics()` and `row_cache_stats()` results. |
| `Failure`, `FailureCause` | What `failure()` reports for a failed instance. |
| `FileFeatures` | `DatabaseInfo::features`: the file's feature bits (D269). |
| `MergeOperator`, `MergeError` | Custom merge operators: an associative fold over stored values (tag byte, then payload). |
| `days(n: u64) -> Duration` | TTL helper. |

## `Pigeonhole`
| Signature | Semantics |
|---|---|
| `open(path: impl AsRef<Path>, Options) -> Result<Pigeonhole>` | Open or create as writer; replays WAL. |
| `open_reader(path, ReaderOptions) -> Result<PigeonholeReader>` | P4, early. Read-only, any number of processes. Needs write permission on the file (D36). |
| `PigeonholeReader::backup(dest)` / `backup_durable(dest)` | ICR 0049: as the writer's, from a reader process while the writer runs. Its shm pin holds the whole view, memtables included, for the copy's length: on a write-heavy database the writer's commits can wait (`write_stall_timeout`, then `Busy`). A writer restart mid-copy gives `SnapshotExpired` and removes the partial copy. |
| `open_application_owned(path, Options) -> Result<(Pigeonhole, Vec<Shard>)>` | Writer with no shard or compaction threads (with io_uring, the default on Linux, it runs no threads at all; with `pread` it runs 2–16 I/O threads); you drive each `Shard`. `compaction_cores(k > 0)` → `InvalidArgument` (D40); so does `background_workers(n > 0)`. |
| `open_application_owned_with(path, Options) -> Result<AppOwned>` | As `open_application_owned`, also handing out `background_workers(n)` workers that run flush and compaction off the shards, driven like shards from any thread (D284, ICR 0078). The close takes their tasks back and never waits for them to be driven. |
| `table(&self, name: &str) -> Result<TableBuilder<'_>>` | Start define/open. |
| `tables(&self) -> Vec<String>` | Table names. |
| `drop_table(&self, name: &str) -> Result<()>` | Drop table and data. |
| `write_batch(&self) -> WriteBatch` | New multi-row batch. |
| `transaction(&self) -> Result<Transaction>` | Optimistic transaction: serializable over what it reads: `get` per cell, `get_row` per family read (D275). `Conflict` at commit: retry it whole. Reads: `get(table, row, family, qualifier)`, `get_async`, `get_row(table, row, &[families])` (ICR 0062: newest version per column; records each family read, all when empty). Writes: every `RowMutation` op with `table, row` first (`put`, `put_at`, `put_i64(_at)`, `put_f64(_at)`, `incr(_at)`, `merge`, `delete_cell`, `delete_column`, `delete_family`, `delete_row`; ICR 0062). No scans inside a transaction. One that writes nothing commits at once and never conflicts (`CommitInfo { seqno: its snapshot, durability: None }`; durable reads use `durable_snapshot`). |
| `transact<T, E: From<Error>>(&self, &RetryPolicy, FnMut(&mut Transaction) -> Result<T, E>) -> Result<(T, CommitInfo), E>` | ICR 0060. Runs the body in a fresh transaction per attempt and commits, retrying `Conflict` and `SnapshotExpired` (and `Busy` with `retry_busy`) with exponential backoff and full jitter until `max_attempts` or `deadline`. The body's own error ends the call, nothing committed. `RetryPolicy`: `max_attempts(n)`, `unlimited_attempts()`, `deadline(d)` (default 30 s), `no_deadline()`, `backoff(base, cap)` (default 5 µs, 100 ms; `transact` yield-spins a pause below 100 µs instead of sleeping, #932), `retry_busy(bool)`, `durability(d)`; `retries(&Error)`, `retries_code(ErrorCode)`, `delay(attempt)` and `next_pause(code, attempt, elapsed) -> Option<Duration>` (the whole decision, as `transact` uses it) for an async caller's own loop. `transact` is Rust-only; the loop is reachable without it. |
| `row_cache_stats(&self) -> RowCacheStats` | Row cache `hits`, `misses`, `fills` and `write_throughs` (copies a commit moved forward, D290) since open (all 0 when off). |
| `io_rings(&self) -> Option<IoRings>` | With io_uring (`IoBackend::Uring`, or the default `Auto` where the kernel offers it): `rings` (the shared one plus one per shard thread that got its own) and `pooled` (rings with a registered buffer pool; `pooled < rings` means the locked-memory limit, `RLIMIT_MEMLOCK`, was too low for every ring to get one: correct, but each read pins its pages), `attach_failures` and `inline` (ICR 0070). `None` on pread. Also on `PigeonholeReader`. |
| `info(&self) -> Result<DatabaseInfo>` | What the database holds and how it is stored, from the current catalog, no data I/O (ICR 0047; also on `PigeonholeReader`): `db_id`, `format_version` (the file's: a writable open upgrades an older file, a read-only one doesn't), `features: FileFeatures { compat, ro_compat, incompat }` (FORMAT §8.2; all 0 until a post-1.0 feature is used), `reader`, `read_only` (a reader, or a writer `read_only_fallback` opened read-only), `shards` (this instance's: a reader's is the writer's), `file_bytes`, `free_bytes` (`None` on a reader), and per table (by name) `name`, `tablets`, `families: Vec<FamilyInfo>` and `storage: Vec<FamilyStorage>` (same order: `family`, `levels: Vec<LevelInfo { level, ssts, bytes, entries }>`, `blob_files`, `blob_bytes`, `blob_live_bytes`; `ssts()`, `sst_bytes()`). Memtable data isn't counted. |
| `engine_metrics(&self) -> EngineMetrics` | The writer's counters since this instance opened (ICR 0044; cheap, non-blocking). Cumulative counters start at 0 at open and never reset: subtract two readings for an interval. Gauges are the value now. Nanoseconds and bytes. `commits(Durability) -> CommitStats { count, p50_nanos, p99_nanos, p999_nanos }` (latency percentiles are half-power-of-two bucket floors); `flushes`, `flush_failures`, `compactions`, `compaction_failures`, `compactions_reclaimed` (held by a background worker when taken back, dropped before commit, D284), `background_fallback_take_backs` and `background_room_take_backs` (tasks the shards took back from undriven background workers; 0 while they are driven); `write_stalls`, `write_stall_nanos` (the sum of the two kinds: `room_stalls`, `room_stall_nanos`, waits for memtable arena room; `pacing_stalls`, `pacing_stall_nanos`, L0 pacing while compaction catches up; ICR 0054); `block_cache_hits`, `block_cache_misses`, `block_cache_capacity_bytes` (gauge), `block_cache_pinned_bytes` (gauge), `block_cache_pinned_warnings`; `wal_unpin_passes`, `wal_unpin_flushes`, `wal_inline_syncs`, `wal_rollover_blocks`; `file_growths`, `file_growth_nanos`, `file_growths_ahead`, `file_growth_ahead_nanos`; `async_sync_reads`; `shard_parks`, `shard_wakes`, `shard_busy_nanos`, `shard_pool_busy_nanos`, `shard_spin_time_nanos`, `shard_commit_inflight_nanos`, `commit_parks`; `threads_started`, `threads_pinned`, `unpinned_reason: Option<&'static str>` (gauges; the reason's wording is not stable); `reader_slots_active`, `reader_slots` (gauges), `reader_slots_reclaimed`, `reader_pin_view_lag`, `reader_pin_seqno_lag` (gauges: how far the oldest reader snapshot lags; a growing lag stalls writers into `Busy`); `visibility_waits`, `visibility_waiters_ahead`, `visibility_wake_passes`, `visibility_woken`, `visibility_contended`; `transactions`, `transaction_conflicts`, `transaction_retries` (ICR 0061: committed transactions, commits that failed with `Conflict`, attempts `transact` retried; kept by the handle, so they span automatic recoveries); `cross_shard: CrossShardMetrics` (ICR 0043): `rounds` (two-phase rounds decided), `refused_rounds` (rounds a participant refused; per cross-shard commit, the wasted rounds), and refusals by reason: `refused_moved` (a tablet split, merge or move, D145), `refused_room` (no arena room for the share, ICR 0042), `refused_pending_overlap` (an undecided cross-shard share on the same rows, D249, D263); `refused_below_floor` and `refused_move_raise` are deprecated, always 0 since D263; `share_waits`, `share_wait_nanos` (singles held behind an undecided share, D263); `failed_fast` (D276), `max_attempts_blind`, `max_attempts_reading`; `chained` and `wounds` are always 0 (D271). `default_ts_lead_micros`, `max_default_ts_lead_micros` (gauges, D271). |
| `snapshot(&self) -> Result<Snapshot>` | Consistent view of everything committed. |
| `durable_snapshot(&self) -> Result<Snapshot>` | Consistent view of the commits durable at their level (D227, ICR 0037; also on `PigeonholeReader`). Lags `snapshot()` by the fsyncs in flight. For reads a later write is derived from, and persisted cursors. |
| `default_durability(&self) -> Durability` | Writer default. |
| `set_default_durability(&self, Durability)` | Applies to later commits. |
| `flush(&self) -> Result<()>` | Write every memtable into the file; returns when the SSTs are in the manifest. Makes `None` commits durable. Fails with `Busy` if the flush finds no room for its fresh memtables past the stall timeout, and with the flush's own error (`Io`, `NoSpace`) if it fails. |
| `compact(&self) -> Result<()>` | Flush, then merge every level of every table into the last (purges per `max_versions`, TTL and tombstones). Reports only a failure of the compaction it started for this call, at once. A failed background compaction backs off (1 s, doubling to 60 s per table-and-family) and is never handed to a later `compact()`. |
| `shrink(&self) -> Result<u64>` | Truncate free space at the end of the file, relocate live data from the tail into free space and truncate again; returns bytes released (`0` if none). Online; costs a rewrite of the tail data. Call after deletes + `compact`. Relocates SST and blob extents (D185); after `compact` + `shrink` a file above a few MiB is about 1.05–1.2× its live data (D183), a small one about 1 MiB of metadata. Data with nowhere lower to go stays, not an error. Errors: `Closed`, `ReadOnly`, `NoSpace` (disk full while moving the manifest), `Io`. |
| `backup(&self, dest: impl AsRef<Path>) -> Result<()>` | Consistent single-file copy at a snapshot taken now, while writes continue; it can hold commits not yet durable here (D227). `dest` must not exist. Holds its snapshot's memtables only while it copies them (at most one arena's worth written to the copy); the long SST copy that follows pins file extents, not memtable space, so writers are not stalled by it. The copy opens with no WAL replay and no sidecars. SST and blob files are copied byte for byte (#715; a blob file only if the snapshot references it), so the copy is about the source's live data and keeps its level shape. |
| `backup_durable(&self, dest: impl AsRef<Path>) -> Result<()>` | As `backup`, at a durable snapshot: the copy holds only commits durable here at their level. |
| `is_failed(&self) -> bool` | Whether the instance failed: a shard panicked, an application-owned driver was dropped before `close`, a shard stopped after a WAL I/O error or an internal error, or a manifest commit failed (ICR 0040). One atomic load. Recovery: [recovery.md](recovery.md). |
| `recovery_stats(&self) -> RecoveryStats` | What automatic recovery (`Options::auto_recover`, D265) did: `generation` (1 at open, +1 per recovery; engine metrics restart at each), `recoveries`, `gave_up`, `last_failure`, `last_recovery`. All zero and `false` with the option off. One lock, no I/O. |
| `failure(&self) -> Option<Failure>` | The first failure: `Failure { shard: u32, cause: FailureCause }` (`#[repr(C)]`; `shard` is `Failure::MAIN_FILE` for the main file; `FailureCause` `#[repr(u8)]`: `Panic` 1, `DriverDropped` 2, `Io` 3, `Internal` 4). Later failures don't replace it. |
| `close(self) -> Result<()>` | Flushes memtables, checkpoints the WAL; the last handle out removes the sidecars and shm, leaving one file. |

`PigeonholeReader` (P4, early): `table(&self, &str) -> Result<ReadTable>`, `tables() -> Vec<String>`, `snapshot() -> Result<Snapshot>`, `durable_snapshot() -> Result<Snapshot>`, `backup(dest)` / `backup_durable(dest)`, `info() -> Result<DatabaseInfo>`, `row_cache_stats() -> RowCacheStats` (D281; `write_throughs` stays 0, a reader applies no commits), `io_rings()`, `async_sync_reads()`. No write methods.
`Snapshot`: `seqno(&self) -> u64`.
`Shard`: `index() -> usize`, `run_once(&mut self, budget: Duration) -> bool` (true if work remains; background work waiting for a time does not count. A call: completes this thread's finished io_uring I/O; handles up to 1,024 queued messages and commits them as one group, not bounded by `budget`; then, unless that completed a future woken through `waker()` (then it returns `true` at once, at most 8 calls in a row, D237), runs background work in slices of at most 500 µs until `budget` is spent, handling new messages between slices), `next_wakeup(&self) -> Option<Duration>` (time until that background work is due; with `tablet_changes` on, the default, the balancer's next pass keeps it `Some`: at most 100 ms after a write, backing off to 10 s while idle), `set_wakeup(&mut self, Box<dyn Fn() + Send + Sync>)` (fires when work arrives, not when background work falls due). `wait(&mut self, timeout: Duration)` (#553: blocks until work arrives, a `waker()` wake, this thread's own I/O completes, background work falls due, or the timeout; every backend, no fd needed; the loop for one shard per thread) and `waker(&self) -> Waker` (ends `wait` from any thread: give it to the futures the driving thread polls). `io_fd(&self) -> Option<i32>` (with io_uring, the default on Linux: a descriptor that turns readable when I/O this thread submitted has finished and waits for `run_once`; take it on the driving thread after its first `run_once`. Taking it is the opt-in that stops `next_wakeup` reporting in-flight I/O as due now, so wait on it from then on; `None` on `Pread`, without io_uring or with a custom VFS; D202). `closed(&self) -> Option<Result<()>>` (the close's outcome once the whole close has finished), `release(&mut self)` (before handing the shard to another thread: drives it until the calling thread has none of its I/O in flight; required with io_uring (the default on Linux) in application-owned mode, where only the submitting thread completes its ring's I/O, and a no-op on pread and in engine-owned mode; debug builds panic on a move that skipped it; #492). Loop: `run_once`, poll this thread's futures with `waker()` after every call, and `wait(timeout)` once `run_once` returned `false`; or, in your own event loop, sleep until the wakeup fires (work arrived), `io_fd` turns readable (I/O completed) or `next_wakeup` passes. After `close()`, keep looping until `closed()` is `Some`, then drop the shard; `close()` on a thread that drives no shard (once every shard has been run) waits for this and returns the outcome; on a driving thread, or before every shard has run, it returns `Ok(())` at once and the outcome is only in `closed()`. On a thread that drives a shard (it last called `run_once`), `commit`, `check_and_mutate`, transaction commits, `flush` and `compact` fail with `InvalidArgument` before submitting anything; awaiting an already-submitted commit there blocking fails with `WouldDeadlock` (it will apply: poll it from the event loop). A thread holding a shard it never ran is not detected: run the shard first.
`BackgroundWorker` (D284): `run_once(&mut self, budget: Duration) -> bool` (flush and compaction slices for up to `budget`, flushes first, at most two tasks held; its I/O completes inside the call, so it may move between threads freely; true if work remains), `next_wakeup(&self) -> Option<Duration>`, `wait(&mut self, timeout)`, `waker() -> Waker`, `set_wakeup(Box<dyn Fn() + Send + Sync>)` (called once right away, then on new work and on the close), `closed() -> bool` (the close took its tasks back: drop it). Dropping it earlier hands its tasks back to the shards. `AppOwned::spawn_background_threads(&mut self) -> Result<Vec<JoinHandle<()>>>` gives each worker a thread (`pigeonhole-worker-<i>`) that drives it until the close; `Io` if the OS refuses a thread, with the workers that got none back in `workers`.

## `Options` (all `self -> Self`; process-local, not stored in file)
| Method | Meaning |
|---|---|
| `durability(Durability)` | Writer default (default `GroupSync`). |
| `shards(usize)` | Shards, the data partitions, each run on its own thread unless `shard_threads` or `adaptive_shard_threads` says otherwise (default: one per physical core the process may run on, D220; SMT siblings count once, a cgroup CPU quota still caps it). `1` is valid. Engine-owned shards pack light writes onto few shards (`tablet_packing`, D217); application-owned, or with packing off, set it to about the number of concurrent writing threads: thinly spread writes pay a shard wakeup per commit (see [Getting started](getting-started.md)). |
| `compaction_cores(usize)` | Threads for flush/compaction (pinned with `pin_threads`, after the shards; unpinned if only the shards fit). Default (D289, ICR 0081): engine-owned, a quarter of the physical cores (`CpuProfile::Server`: half), at least 1 and at most one per shard; application-owned, 0. `0` runs them on the shards in 500 µs slices between commits (a write-tail cost on contended loads). `k > 0` is engine-owned only (D40). |
| `pin_threads(bool)` | Pin each shard (and compaction) thread to a CPU of its own (default **on**, D235): the opener's affinity set only, physical cores first, then SMT siblings; CPUs another database in this process holds are skipped. Never fails an open: unpinned where the platform has no hard affinity (macOS), a CPU quota is narrower than the set, or too few CPUs are free (with room for the shards only, just the compaction threads run unpinned; D252) (see `engine_metrics()`: `threads_started`, `threads_pinned`, `unpinned_reason`). Engine-owned mode only. Several *processes* on one host pin onto the same CPUs: give each its own affinity mask or cpuset, or `pin_threads(false)`. No option names cores; the affinity mask does. |
| `memtable_budget(u64)` | Arena bytes per shard (default 64 MiB). At the defaults a writer can use up to `shards × memtable_budget` + about 12 MiB + `block_cache` (256 MiB) + `row_cache` (64 MiB, plus about 9%): about 0.6 GiB at 4 physical cores, 1.3 GiB at 16 and 4.3 GiB at 64, filled only under load (Getting started, "Memory at the defaults"). The shm region is `memtable_budget × shards` (each arena rounded up to 2 MiB) plus about 12 MiB for views, reader slots and row epochs, filled as memtables grow; open fails with `ShmUnavailable` if its filesystem lacks that much free space. Something else filling that tmpfs after open can still crash the process with `SIGBUS`. |
| `background_workers(usize)` | Application-owned only: `n` background workers for flush and compaction, handed out by `open_application_owned_with` for you to drive (D284, ICR 0078). Default 0: flush and compaction run on the shards. `open_application_owned` refuses `n > 0`; engine-owned `open` ignores it (`compaction_cores` instead). |
| `shard_threads(usize)` | Run the engine-owned shards on `k` threads, shard `i` on thread `i % k` (D289, ICR 0079). Unset or `0` (the default): a thread per shard. A few busy threads suit a light load spread over many shards (they rarely sleep); a heavy load needs one per shard. Pinned like shard threads; ignored by application-owned shards. |
| `cpu_profile(CpuProfile)` | Who the CPUs are for (D289, ICR 0081). `Embedded` (default) leaves CPU for an application in the same process; `Server`, for a database whose clients are elsewhere, gives flush and compaction half the physical cores instead of a quarter. Cores mean those the process may use (affinity, container CPU limits). Explicit `shard_threads` / `compaction_cores` win. |
| `write_stall_timeout(Duration)` | How long a write, `flush` or `compact` waits before `Busy` (default 30 s; `Duration::ZERO` refuses at once). Per call: a wait for memtable room counts from when that call began waiting, kept to the millisecond, so `Busy` can come up to 1 ms early by the caller's clock (D240); a commit waiting on a tablet split, merge or move, or a cross-shard commit retried after one refused it, counts from its submission (D241, no retry cap). |
| `auto_recover(Recovery)` | Recovers a failed instance in place (D265, ICR 0050): `Recovery::Io` (a failed log write or sync, a failed manifest commit), `Recovery::Any` (panics and internal errors too), `Recovery::Off` (default: the recovery guide's close and reopen). A supervisor thread closes the failed engine keeping its writer lock, reopens on it and swaps the engine in behind every handle; operations arriving meanwhile wait up to `write_stall_timeout` for the swap, then `Busy` (not committed, retry); a commit in flight at the failure is `Busy` too; snapshots, transactions and scans from before expire with `SnapshotExpired`. Three recoveries per ten minutes, backoff 0/1/4 s; past that, or on a fatal reopen error, the handle stays failed. Refused by `open_application_owned` and `open_application_owned_with`. |
| `block_cache(usize)` | Block cache bytes (default 256 MiB; each reader process has its own). |
| `row_cache(usize)` | Row cache bytes on top of the block cache (default 64 MiB, D216; 0 = off; D201). Serves latest row reads of the newest version (no time range; qualifier, column-limit and value filters applied to the cached row) and point gets (latest, snapshot and transaction, D290) from cached family rows; gets fill on a row's second miss (not in merge-operator families); a commit that only overwrites cached cells with puts moves the copy forward (commits of at most 16 rows and 64 mutations; the row's other families miss), any other write makes it miss. Results equal uncached reads. Writer process; reader processes have their own (D281). |
| `row_cache_max_row(usize)` | Largest family row stored, encoded bytes (default 4 KiB). |
| `row_cache_family(&str, &str)` | Serve only this `(table, family)`; call once per family (default: every family). |
| `io_backend(IoBackend)` | I/O backend (default `IoBackend::Auto`: io_uring where the kernel offers it, `pread` elsewhere, D218). `Uring` fails the open with `Unsupported` where io_uring is unavailable; `Pread` forces the thread pool. |
| `direct_io(bool)` | Read and write SST and blob extents with direct I/O (`O_DIRECT`, `F_NOCACHE`, `FILE_FLAG_NO_BUFFERING`) through a second handle, so the block cache is their only cache (default off; #403). Superblocks, manifest and WAL stay buffered; a file system that refuses direct I/O keeps buffered I/O. Turn it on only for data much larger than RAM with `block_cache` ≥ about 1/64 of the data (D215): cold gets then measured p99 205 µs direct vs 264 µs buffered at 1 thread, 293 vs 447 µs at 16 (AX102, 64 GiB). With a small block cache it is much slower than buffered, which uses free memory as cache. |
| `shm_dir(impl Into<PathBuf>)` | Shared-memory file directory (e.g. a tmpfs), instead of `/dev/shm` (Linux), `shm_open` (macOS/BSD) or the pagefile (Windows). Must exist. On Windows, the same `shm_dir` on the writer and its readers lets readers in another session attach. |
| `reader_slots(u32)` | Reader handles that can attach at once (default 128, 1..=65,536; ICR 0055). One more `open_reader` fails `NoReaderSlot` at once. A killed reader's slot is freed within about 250 ms. |
| `create_if_missing(bool)` | Default true. |
| `merge_operator(Arc<dyn MergeOperator>)` | Register a custom operator (families name it). |
| `read_only_fallback(bool)` | ICR 0059. Open read-only instead of failing when this build can't write the file: an unknown `ro_compat` feature bit (else `UnsupportedFeature`) or a family naming an unregistered merge operator (else `UnknownMergeOperator`). The handle refuses writes, schema changes, `flush`, `compact`, `shrink` (`ReadOnly`) and writes nothing to the files: no format upgrade, flush, compaction or clean-close mark; the WAL is replayed into memory (`ReadOnly` if it doesn't fit `memtable_budget`). `DatabaseInfo::read_only` tells. |
| `allow_fuse(bool)` | Accept a database on FUSE (default off; D173). Only for a trusted **local** FUSE mount: its locks may be host-local and its sync may not be durable. Network filesystems stay refused. Also on `ReaderOptions`. |
| `tablet_changes(bool)` | Let tablets split, merge and move between shards so one table's writes spread over every shard (default on; off keeps each table as one tablet on one shard). Tablet owners are not stored; a reopen places tablets again. Commits in flight together on one row may apply in either order while its tablet moves. Write skew moves a tablet only once it persists (D214), not while it comes only from other shards going idle, and placement is also corrected on about 10 s of write load (D224); a cold tablet merges or moves to its neighbor only after about 10 s without writes. |
| `tablet_packing(bool)` | While writes are light, keep the written tablets on as few shards as stay about half busy, and spread them again within about a second once those pass 75% busy on average or one passes 90%, never packing while the write rate is still rising (default on, D217, D251, D255, D256; busy is the largest of the shard thread's occupied time (its work, its spin before parking and the compaction pool's work for it, D256), the time its group fsyncs are in flight (D248) and the time its commits waited for memtable room or were paced on L0 depth (D255)), so a commit seldom finds its shard asleep. Off, tablets spread over every shard whatever the load. Only with `tablet_changes` on; application-owned shards are never packed. |

Test and bench hooks are not in this table: `vfs`, `wal_segment_size`, `tablet_balance`, `counter_checkpoint_steps`, `nowait_reads`, `commit_spin`, `shard_spin` and `adaptive_shard_threads` exist only with the `__unstable-hooks` feature (ICR 0085), which applications never turn on.

`ReaderOptions` (P4, early): `block_cache(usize)` (default 256 MiB **per reader process**, on top of the writer's), `row_cache(usize)` (default 64 MiB **per reader process**, 0 = off; needs the writer's row cache on; D281), `row_cache_max_row(usize)`, `row_cache_family(&str, &str)`, `shm_dir(..)`, `merge_operator(..)`, `allow_fuse(bool)`, `io_backend(IoBackend)`, `direct_io(bool)` (as on `Options`).

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
| `Table::family_info(&self, &str) -> Option<FamilyInfo>` | A family's stored options as of when the handle opened (ICR 0047; also on `ReadTable`, with `ReadTable::families`): `name`, `family: Family` (the same options as a builder: pass it to `TableBuilder::family` to define an identical family), `counter`, `max_versions`, `ttl: Option<Duration>`, `bloom_bits`, `blob_threshold`, `compression: Compression` (`None`, `Lz4`, `Zstd(i8)`), `block_size`, `merge_operator: Option<String>` (`None` for counters), `cache_priority`, `compaction`. `None` if no such family. |
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
| `put_f64_at(family, qualifier, ts, f64)` | Typed `f64` at an explicit timestamp (ICR 0051). |
| `put_i64_at(family, qualifier, ts, i64)` | Typed `i64` at `ts` (a counter family's bucket). |
| `incr(family, qualifier, delta: i64)` | Blind atomic wrapping `i64` add; counter families only. |
| `incr_at(family, qualifier, ts: u64, delta: i64)` | Add to the bucket at `ts`; counter families only. |
| `merge(family, qualifier, operand: &[u8])` | Operand for the family's registered operator. |
| `delete_cell(family, qualifier, ts: u64)` | Delete the version at `ts`; later puts at that `ts` stay hidden (D38). |
| `delete_column(family, qualifier)` | Delete all versions. |
| `delete_family(family)` | Delete all columns of a family in this row. |
| `delete_row()` | Delete whole row. |
| `durability(Durability)` | Override for this commit. |
| `commit(self) -> Result<CommitInfo>` | Commit. Returns once durable at its level and visible. `Closed` or `Busy`: not committed (`Busy`: back off, retry). `OutcomeUnknown` (a shard failed or the database closed mid-commit): reopen and read to see whether it landed. |
| `commit_if(self, &Condition) -> Result<Option<CommitInfo>>` | Compare-and-set on this row (BigTable `check_and_mutate`); `None` if the condition failed and nothing was written. |

`Condition` variants: `Exists { family: String, qualifier: Vec<u8> }`, `Absent { .. }`, `Value { family, qualifier, filter: ValueFilter }`.

## `WriteBatch` (methods take `&mut self -> &mut Self`)
| Method | Semantics |
|---|---|
| `put(&Table, row, family, qualifier, value)` | |
| `put_at(&Table, row, family, qualifier, ts, value)` | |
| `put_i64(&Table, row, family, qualifier, i64)` / `put_f64(.., f64)` | Typed values. |
| `put_i64_at(&Table, row, family, qualifier, ts, i64)` | |
| `put_f64_at(&Table, row, family, qualifier, ts, f64)` | ICR 0051. |
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

`Transaction` (P4, early): `get(&mut self, &Table, row, family, qualifier) -> Result<Option<CellRef<'_>>>`, `get_row(..)`, `get_async(..)`, every `RowMutation` write op (as `WriteBatch`, `&mut Self`), `commit(self)`, `commit_with(self, Durability)`; `Conflict` on a conflicting commit. See `transaction` above.

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
| `into_owned() -> Scan<'static>`: holds the table instead of borrowing it, so the `RowIter` is `Send + 'static` (ICR 0052) | | ✓ |
| terminal | `read(self) -> Result<Option<RowRef<'t>>>` | `iter(self) -> Result<RowIter<'t>>` |

`ValueFilter`: `Equals(Vec<u8>)`, `Prefix(Vec<u8>)`, `I64(std::cmp::Ordering, i64)`.
`RowIter`: `Iterator<Item = Result<Row>>`; `next_ref(&mut self) -> Result<Option<RowRef<'_>>>` (zero-copy, valid until next call). After `close` (any handle) the next step is `Closed`, then the end; also `RowStream` and owned scans (#826).
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
`Io`=1 `Corruption`=2 `WriterLocked`=3 `ShmVersionMismatch`=4 `ShmUnavailable`=5 `UnsupportedFormat`=6 `NetworkFilesystem`=7 `TableNotFound`=8 `TableExists`=9 `FamilyNotFound`=10 `FamilyExists`=11 `UnknownMergeOperator`=12 `MergeFailed`=13 `Conflict`=14 `ReadOnly`=15 `KeyTooLarge`=16 `ValueTooLarge`=17 `NoSpace`=18 `InvalidArgument`=19 `Unsupported`=20 `Closed`=21 `NoReaderSlot`=22 `RecordTooLarge`=23 `Busy`=24 `SnapshotExpired`=25 `WouldDeadlock`=26 `BatchTooLarge`=27 `OutcomeUnknown`=28 (a commit's outcome is not known: reopen and read; `Closed` and `Busy` mean not committed) `NoWriter`=29 (a reader process found no running writer to attach to; open the reader again once one runs; ICR 0057) `UnsupportedFeature`=30 (the file uses a feature from a newer version: a value this build doesn't know; the data is intact; ICR 0058). Causes and fixes: [`errors.md`](errors.md). `Error::code()`, `Error::message()`.

## Async (feature `async`, default on; module `nonblocking`)
Same semantics as the blocking call; any executor; no `spawn_blocking`. Guide: [`async.md`](async.md).

| Signature | Semantics |
|---|---|
| `Table::get_async(&self, row, family, qualifier) -> GetFuture` / `get_at_async(&self, &Snapshot, ..)` | `Output = Result<Option<Cell>>`. Also on `ReadTable`. A memtable or cache hit is read at the call (as of then) and ready on first poll; otherwise the read point is taken on first poll. On Linux, a block the page cache holds is copied on the calling thread instead of waiting on the ring (D292). |
| `RowRead::read_async(self) -> RowFuture` | `Output = Result<Option<Row>>`. |
| `Scan::stream(self) -> RowStream<'t>` | `futures_core::Stream<Item = Result<Row>>`, key order; setup error is the first item. Fetches the next step's blocks only as polled. |
| `RowMutation::commit_async(self) -> CommitFuture` | `Output = Result<CommitInfo>`. Submitted at the call when its shard has room, else when polled with room (D210); resolves when durable and visible. |
| `WriteBatch::commit_async(self)` / `commit_with_async(self, Durability) -> CommitFuture` | As above. |
| `Transaction::commit_async(self)` / `commit_with_async(self, Durability) -> TransactionCommitFuture` | `Conflict` on a conflicting commit. Its own future type (ICR 0060): a `CommitFuture` that also counts the outcome in `EngineMetrics`' transaction counters (ICR 0061). |
| `RowMutation::commit_if_async(self, &Condition) -> CheckFuture` | `Output = Result<Option<CommitInfo>>`; `None` if the condition failed. |
| `Transaction::get_async(&mut self, &Table, row, family, qualifier) -> GetFuture` | Read recorded at the call (a dropped future still counts at commit). |
| `Pigeonhole::flush_async(&self)` / `compact_async(&self) -> MaintenanceFuture` | `Output = Result<()>`; dropping does not stop it. |
| `WriteBatch::commit_with_ticket(self, Durability) -> Result<CommitTicket>` | No feature needed. `CommitTicket`: `wait(self)`, `try_result(&mut self) -> Option<Result<CommitInfo>>`, `seqno(&mut self) -> Option<u64>`; `IntoFuture` with `async`. |
| `Pigeonhole::async_sync_reads(&self) -> u64` (also `PigeonholeReader`) | Reads an async call did synchronously (oversized blob, uncacheable block, unpredicted scan block; #398). |

Backpressure (D210): each shard holds at most ½ `memtable_budget` of async commits submitted and not yet resolved; over it, a commit future waits (arrival order) and submits when polled with room. Blocking commits and `commit_with_ticket` are never held or counted.
Dropping a read future or stream: always safe. Dropping a ticket, or a commit future that has submitted: the commit still lands or fails atomically. **Dropping a commit future still waiting for room writes nothing** (fire-and-forget `let _ = ..commit_async()` can lose the commit over the bound; use `commit_with_ticket` to submit without awaiting).
Sync-only by design (D196): `backup`, `shrink` (long-running; run on your executor's blocking pool), and open, close, table create/open/drop (short).

## Command line: `phdb` (#692)
Full page: [`cli.md`](cli.md). Install with `cargo install pigeonhole-cli`; until the release that ships it, use `cargo install --path crates/cli` (CI installs it that way and runs it).

| Item | Rule |
|---|---|
| Commands | `phdb info <file>`; `phdb check <file> [--deep] [--map] [--online]` (offline integrity check, exit 3 on damage; `--online` scans through a reader while a writer runs). `phdb dump <file> [--table T]... [--prefix P] [--durable]` (jsonl by default, or csv, or `--format arrow` with exactly one `--table`: an Arrow IPC stream in the Arrow export's schema with `value_i64`/`value_f64`; all versions at one snapshot); `phdb load <file> [<dump.jsonl>|-] [--create]` (writer only). `phdb backup <file> <dest> [--latest] [--check]` (from a reader process while an application runs) and `phdb compact <file> [--shrink]` (writer only: exit 4 while an application has the database open). `phdb shell <file> [-c CMD]... [-f SCRIPT] [--read-only]`: get/scan/put/incr/delete/create/snapshot and `\\tables`, `\\schema`, `\\info`, `\\format`; for agents, `--format json -c ...` (one document per command; the first failure stops with its status). |
| For programs | `--format json` gives one document; `--format jsonl` gives a header line, then one object per row. Each carries `"phdb": 1`; within it fields are only added, so ignore unknown fields. |
| Open policy | With no writer running, `phdb` becomes the writer for the command, replaying a crashed WAL, and closes. With a writer running, it opens as a reader process. `--reader` / `--writer` force one. |
| Exit status | 0 ok, 1 error, 2 usage, 3 corruption, 4 locked or busy, 5 not found, 6 unsupported. |
| Errors | On stderr. With json/jsonl: `{"phdb":1,"error":{"code","code_num","message"}}`. `code` is the `ErrorCode` name (`ReadOnly` for a write in a read-only shell). Failures outside the database have `code_num` `null`: `FileNotFound`, `BadInput` (a dump `load` can't read; the message names the line), `Unsupported` (a cell `dump --format arrow` can't hold, such as a timestamp above `i64::MAX`; exit 6), `Export` (the Arrow writer failed otherwise; exit 1), `Usage` (exit 2). A failed dump leaves a prefix on stdout: trust it only on exit 0. Shell and script failures name their line. |
| Agent recipe | Inspect: `phdb --format json info DB`. Verify: `phdb --format json check DB` (exit 3 and `findings` on damage). Export: `phdb dump DB > db.jsonl`. Query or edit: `phdb --format json shell DB -c 'scan t --prefix user: --limit 10' -c 'put t r f:q v'` (one JSON document per command; the first failure stops with its status). |

## Arrow export: `pigeonhole-arrow` (#691)
Full page: [`arrow.md`](arrow.md). Depends on the public crate only; pins arrow-rs 60 and re-exports `arrow_array`, `arrow_buffer`, `arrow_schema`.

| Item | Rule |
|---|---|
| `scan_to_arrow(Scan<'t>, &ArrowOptions) -> Result<ArrowScan<'t>>` | Starts the scan (setup errors here); the scan's whole selection passes through, so batches = a plain scan's cells, in order. |
| `ArrowScan<'t>` | `Iterator<Item = Result<RecordBatch, ArrowError>>` + `RecordBatchReader`; `schema() -> SchemaRef`. Finished after an error. Read errors are `ArrowError::ExternalError(pigeonhole::Error)`. From `Scan::into_owned()`: `Send + 'static`. |
| Schema | `row: Binary`, `family: Dictionary<Int32, Utf8>`, `qualifier: Binary`, `ts: Timestamp(Microsecond, "UTC")`, `value: Binary` (`CellRef::value`), `value_type: Dictionary<Int8, Utf8>` (`bytes`/`i64`/`f64`/`varint`). `schema(&ArrowOptions)` without scanning. |
| `ArrowOptions` (`self -> Self`) | `typed_values(bool)` adds nullable `value_i64: Int64`, `value_f64: Float64`; `utf8_keys(bool)` (`Utf8` row/qualifier; invalid UTF-8 fails the batch, naming the key); `row_dictionary(bool)` (per-batch `Dictionary<Int32, _>`); `ts(TsType::Timestamp \| TsType::UInt64)` (`Timestamp` fails above `i64::MAX`); `batch_cells(n)` (default 8192), `batch_bytes(n)` (default 8 MiB, ≤ 1 GiB); a row may span batches. |
| Cost | One copy per key and value into Arrow buffers; a fixed number of allocations per batch, none per cell. |
| Feature `ffi` | `ArrowScan<'static>::into_ffi_stream() -> FFI_ArrowArrayStream` (Arrow C stream interface) for consumers on another arrow major or language; start from `Scan::into_owned()`; the consumer owns the stream and calls its `release`. After `close`, `get_next` is `Closed`, then the end; `release` stays safe. `Send`, not `Sync`. A panic in `get_next` aborts. Adds libc. |

## `check` (offline integrity check, ICR 0048)
`pigeonhole::check(path: impl AsRef<Path>, CheckOptions) -> Result<CheckReport>`

| Item | Rule |
|---|---|
| When | No writer running: it takes the writer lock and fails with `WriterLocked` otherwise. The lock needs write access to the file. It never replays the WAL and never writes; a crashed database checks as it was left. A newer format version fails with `UnsupportedFormat`. |
| Result | `Err` only if it couldn't run (missing file is `Io`, locked, unreadable). Damage is a `Finding` in `report.findings`. `report.is_ok()` is true when no finding is `Severity::Corruption`. |
| Checks | Both superblock slots, the manifest, extents (valid, inside the file, disjoint), every SST block with its checksum forced (key order, agreement with the manifest, blob references), every blob record, and the WAL sidecars from each stream's checkpoint, as replay chains them. |
| `CheckOptions` | `deep(true)`: also read every blob pointer's record, and recount blob live bytes (a mismatch is a `Warning`). `allow_fuse(true)`. |
| `Finding` | `structure` (`Superblock`, `Manifest`, `Allocation`, `Sst`, `Blob`, `Wal`), `severity` (`Corruption`; `Warning` for something a crash can also leave; `Info` for expected after a crash, such as a torn WAL tail; a WAL gap with valid records after it is a `Warning`, since a power loss tearing the last write looks the same; WAL `Corruption` means a segment's successor proves synced records were lost), `object` (slot, SST id, blob file id or stream), `offset` (main file, or the WAL file for `Wal`), `message` (not stable wording). |
| `regions` | Every region of the main file by offset: `Superblock { slot }`, `ManifestSnapshot`, `ManifestLog`, `Sst { sst, table, family, level, used }`, `Blob { blob_file, index, family }`. |
| `wal` | One `WalFile` per sidecar (none after a clean close): `stream`, `path`, `len`, `segment_size`, `segments`, `records` (what the next open replays), `checkpoint` and `end` as (epoch, offset), `end_slot`. |

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
