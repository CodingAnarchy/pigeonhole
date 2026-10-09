# Changelog

All notable changes to Pigeonhole are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, the on-disk format ([`FORMAT.md`](FORMAT.md)) and the API may change in any release.

> **Experimental 0.x.** Not recommended for production use yet. See [`docs/status.md`](docs/status.md).

## [Unreleased]

### Added
- Stall counters (ICR 0015, #64):
  - `pigeonhole-engine`: `Metrics::wal_inline_syncs` and `Metrics::file_growths`;
  - `pigeonhole-pager`: `PagerStats::growths` and `growth_nanos`;
  - `pigeonhole-wal`: `WalCounters` from `WalStream::counters`.

  `phdb-bench` reports what each Pigeonhole run's measured phase stalled on: write stalls, flushes, compactions, WAL unpin passes, inline WAL syncs and file growths. JSON has it in `detail.stalls`, and the markdown summary prints a table.

### Added
- **Async commits** (Phase 3, #42; behind the `async` feature, which stays off by default until the async front door is complete): `RowMutation::commit_async`, `WriteBatch::commit_async` and `commit_with_async`, and `Transaction::commit_async` and `commit_with_async` return a `nonblocking::CommitFuture`. It submits at the call and resolves when the commit is durable at its level and visible, as the sync `commit` returns. Dropping it does not roll the commit back. It runs on any executor (no runtime dependency, no `spawn_blocking`) and on an application-owned shard's own event loop.
- `WriteBatch::commit_with_ticket` returns a `CommitTicket` (D196): `wait`, a non-blocking `try_result`, `seqno` once resolved, and `.await` with the `async` feature. It is available without the feature.
- `pigeonhole-engine`: `Txn::submit`, the non-blocking half of `Txn::commit`.
- `Options::io_backend` and `ReaderOptions::io_backend` choose the I/O backend (#402): `IoBackend::Pread` (the default, as before), `IoBackend::Uring` (Linux: submitted I/O goes through io_uring, and opening fails with `Unsupported` where it is unavailable), or `IoBackend::Auto` (io_uring where available). `pigeonhole-io` adds the backend as `uring::UringVfs`.
- With `IoBackend::Uring`, each shard thread gets an io_uring ring of its own and completes its I/O itself, in its turns and its waits (#402). In application-owned mode, `Shard::next_wakeup` is `Some(Duration::ZERO)` while the driving thread has I/O in flight on its ring (a completion fd is #408). `pigeonhole-io` adds `Vfs::attach_thread`, `own_io_waker` and `OwnIoWaker`.
- `pigeonhole-io`: `OpenOptions::direct` is honored (#403): `O_DIRECT` on Linux, `F_NOCACHE` on macOS, `FILE_FLAG_NO_BUFFERING` on Windows, and `SimVfs` enforces the alignment. `File::direct_align` gives the alignment a direct handle's reads and writes must keep, a misaligned one fails with the new `ErrorKind::Misaligned` on every backend, and a file system that refuses direct I/O fails the open with `Unsupported`. Nothing in the engine opens direct handles yet.
- **Async gets and row reads** (#42, D196; behind `async`): `Table::get_async` and `get_at_async` (and the same on `ReadTable`) resolve to an owned `Cell`, and `RowRead::read_async` to an owned `Row`. Memtable and cache hits resolve on the first poll. A block the cache does not hold is read through the VFS's asynchronous reads and the read runs again, so no executor thread blocks on it. `Pigeonhole::async_sync_reads` counts the reads that still go to the file synchronously: separated values too large to cache, and blocks a cache of size 0 cannot keep.
- `pigeonhole-sst`: `ReadOptions::cache_only`, `Error::WouldBlock(Fetch)`, `Fetch`, `SstReader::open_cache_only`, `BlobReader::cached` and `BlobReader::read_cache_only` (ICR 0014). `pigeonhole-engine`: `Engine::get_latest_async`, `get_async`, `read_row_latest_async` and `read_row_async` (`GetFuture`, `RowFuture`), and `Metrics::async_sync_reads`.

### Changed (breaking)
- `pigeonhole-sst`: `ReadOptions` gains the public field `cache_only`, so building it with a struct literal no longer compiles; start from `ReadOptions::default()` and set fields.

## [0.2.0] - 2026-10-09
The **Phase 2 wide-column model**: counter families, large values and blob separation, per-family compaction styles and zstd, a file at rest near its live size, and the read- and write-path work that met the Phase 2 gate as amended by [D193](docs/design/decisions/phase-2.md#d193). Counters change in a breaking way, and the file format is version 2; see [Migrating from 0.1.0](#migrating-from-010). Every published crate moves to 0.2.0 together.

### Added
- **Counter families** (D179, D186, D187; #274). `Family::counter()` declares one, after Bigtable's aggregate families:
  - `incr` writes at one fixed timestamp (0), so a counter is one cell however often it is incremented: increments combine at read time and in compaction.
  - `incr_at(family, qualifier, ts, delta)` adds to the bucket at `ts` (hourly or daily totals; each bucket is a version). `put_i64` sets the counter and `put_i64_at` a bucket; later increments add to them.
  - Writes of one counter in one mutation or batch apply in order (D186, #295): `incr(c, 1).incr(c, 2)` adds 3, and `put_i64(c, 5).incr(c, 1)` leaves 6. Other families keep D34 (the last write of a cell in a commit wins).
  - Only `i64` values are accepted: `put`, `put_at`, `put_f64` and untyped `merge` operands fail with `InvalidArgument`.
  - A delete hides only what was written before it, so an `incr` after `delete_column` or `delete_row` starts the counter from 0. Deletes are purged by a bottommost compaction (D187).
  - A TTL expires each bucket. With a TTL the fixed timestamp would expire at once, so there `incr` and `put_i64` without a timestamp fail with `InvalidArgument`. `max_versions` limits reads; compaction keeps older buckets, so bound storage with a TTL.
- **Blob separation and blob GC** (#33, D180, D184). Values above a family's `blob_threshold` (default 4096 bytes) move to blob files when flushed or compacted. Blob GC rewrites a blob file once it is about half garbage, following the references each SST records. Value filters test a separated value like an inline one. `backup` copies the separated values its snapshot references (D182), and `shrink` relocates blob extents too (D185).
- **Values up to 4 GiB − 2 bytes** (D188, #230). A put longer than the inline limit (the smallest of the WAL segment payload, 64 MiB and half a shard's memtable arena) is written to a blob file when it is committed, so only a pointer goes through the WAL and the memtable. Such a put costs one extra manifest commit, with its file sync, even under `Durability::None`. `ValueTooLarge` now means a value above 4 GiB − 2, or a merge operand above the inline limit.
- **Per-family compaction styles** (D168; #31, #32, #44). `Compaction::Tiered` (universal, for write-heavy families) and `Compaction::FifoByTime` (drops whole SSTs once their newest timestamp passes the TTL, with no rewrite) are accepted at table creation. `FifoByTime` expiry runs on a timer, so an idle family drops expired files on time (#232). See [Compaction styles](docs/guide/concepts.md#compaction-styles).
- **zstd block compression** (D175, #44). `Family::zstd(level)` is accepted at table creation and stores blocks as zstd frames at the family's level.
- **The FUSE opt-in** (D173, #299). `Options::allow_fuse(true)` and `ReaderOptions::allow_fuse(true)` accept a database on a local FUSE mount the user trusts (ntfs-3g, gocryptfs). FUSE is refused by default, and network and cluster filesystems are refused either way. `pigeonhole-io` adds `File::locality` and `Locality`.
- `Options::write_stall_timeout(Duration)`: how long a stalled write, `flush` or `compact` waits before `Busy` (default 30 s, as before) (#210).
- `pigeonhole-engine`: `Engine::tablet_refusals()`, the tablet splits and moves refused for lack of room (#122).

### Changed
- **Breaking: counters need a counter family** (D179). `Family::default()` no longer carries the `pigeonhole.i64_add` operator, and `incr` on a family that is not a counter family fails at commit with `ErrorCode::InvalidArgument`. Families in files written by 0.1.0 keep 0.1.0 behavior (see Migrating).
- **Breaking: `#[non_exhaustive]` on structs that may grow** (#296), so a later field is not breaking: `FamilyOptions` (`pigeonhole-format`, re-exported by `pigeonhole-engine`), `ModelFamily` and `ModelPurge` (`pigeonhole-sim`), and the result types `Metrics`, `ShardStats`, `TableInfo`, `FamilyInfo`, `CompactionRecord` (engine), `CompactionOutput` (compaction) and `PagerStats` (pager). Each field has a builder setter. The public `pigeonhole` API is unaffected (`Family` and `Options` are builders already).
- **Breaking, lower-level crates:** `pigeonhole-engine`, `pigeonhole-format`, `pigeonhole-compaction` and `pigeonhole-sim` add `FamilyOptions::kind` (`FamilyKind::{Standard, Counter}`), `WriteBatch::merge_at`, `COUNTER_TS`, `ResolveOptions::counter`, `GcPolicy::other_sources`, and the model's `ModelFamily::counter`, `ModelOp::Incr::ts` and `ModelError::CounterWrite`.
- **On-disk format version 2** (FORMAT §12). This release reads 0.1.0 files, but a file it has written can't be opened by 0.1.0.
- **Flushes purge overwritten versions** (D191, #287). A flush runs its memtable through compaction's GC: it drops what a non-bottommost compaction could (expired cells, cells hidden by a delete at every live snapshot), and the versions beyond `max_versions` when no other source of the family's tablet holds a delete. Live snapshots read exactly what they did before. Delete markers are still purged only by a bottommost compaction.
- **A file at rest stays near its live size** (D183, #185). Compaction cuts its level outputs into power-of-two pieces, so after `compact()` and `shrink()` a file above 5 MiB is 1.05–1.16× its live data, where it was 2–4×. `shrink` can also move a large extent down when small extents fragment every hole below it (D190, #314). No migration: each compaction rewrites its outputs in the new shape.
- The write stall follows L0 depth only; deeper levels and tiered space amplification no longer pace writers (D119).
- Custom merge operators now work. In 0.1.0, operators registered with `Options::merge_operator` and `ReaderOptions::merge_operator` never reached the engine (#43, #253). A handle opened with `allow_unregistered_merge_operators(true)` is now read-only, as documented: writes, transactions and table changes fail with `ReadOnly`.

### Fixed
- A write with an explicit timestamp older than a running bottommost compaction's inputs could change a read when the compaction installed, with no write in between: a cell delete exposed an older version the compaction then purged, or a put below a purged delete appeared. The compaction now runs under a guard, as a flush's purge does (D192, #316): such a write voids it before it installs (it compacts again), or waits while it installs. After three voids in a row, the slot compacts once without that purge, so a backfill written newest to oldest can't keep it from finishing.
- With tablet changes off, a shard's memtable arena is sized for the table and family slots it holds, as with them on (D189, #283). A memtable budget under 16 MiB used to serve only 16 slots per shard, and more stalled writes until `Busy`.
- A database on NFS without working locks fails to open with `NetworkFilesystem` instead of an I/O error about locks, and FUSE and GPFS mounts are refused as network filesystems (D173, #147).
- `backup` holds the snapshot's memtables only while it copies them, not for its whole merge, so writers no longer wait for arena room during a long backup (#262).
- Space a moment-old view kept retired is reclaimed as soon as that view goes, not at the next manifest commit, so `shrink` on an idle database gives it back (#337).

### Performance
The Phase 2 gate ([D193](docs/design/decisions/phase-2.md#d193)) is met: on `phdb-bench sparse-wide --scale full` (10 shards, 64 MiB memtables, 256 MiB cache, one client thread; official runs 5–6 on a quiet 10-core Apple M5), Pigeonhole beats hand-keyed RocksDB on every measure and matches or beats SQLite EAV on throughput, get and put p99, and p99.9.

| Store | ops/s | p99 µs | p99.9 µs | get p99 | put p99 | row-read p99 | scan p99 |
|---|--:|--:|--:|--:|--:|--:|--:|
| Pigeonhole | 28.7K / 30.4K | 389 / 395 | 528 / 541 | 98 / 11 | 17 / 17 | 455 / 459 | 489 / 496 |
| SQLite EAV | 27.1K / 29.8K | 245 / 250 | 1,040 / 938 | 156 / 152 | 178 / 172 | 222 / 231 | 293 / 307 |
| RocksDB | 20.4K / 20.4K | 709 / 791 | 1,012 / 1,196 | 161 / 155 | 9 / 9 | 840 / 963 | 881 / 1,016 |

- **Documented gap** ([#387](https://github.com/CodingAnarchy/pigeonhole/issues/387), Phase 3): reads of very wide, heavily overwritten rows have a p99 about 2× SQLite EAV's for row reads and 1.6× for scans. An overwrite adds a version, and those reads step over the superseded versions until compaction removes them; a compacted hot row costs about the same per cell as SQLite. The guide's [Wide, overwritten rows](docs/guide/data-modeling.md#wide-overwritten-rows) says how to keep it down.
- **The work behind it** (#287, #46, #320): commits make a small constant number of allocations; point gets, row reads and scans reuse their thread's resolver in place, and a scan reads each level below 0 through one lazy cursor, opening only the SSTs it reaches; flush-time version GC; and many cheaper steps in the write path, the merge, the resolver and block decoding. **D193's floors** keep what Phase 2 reached: CI's instruction counts fail a change that makes any measured read or write shape worse than its per-change threshold or puts it above its absolute ceiling (`crates/bench/baselines/instruction-ceilings.txt`), and every later official gate run, including the one before each release, is compared with the committed Phase 2 baseline (`crates/bench/baselines/phase2-gate`).

### Migrating from 0.1.0
- **Files.** Files written by 0.1.0 open and read as before. Files written by 0.2.0 use format version 2 and can't be opened by 0.1.0, so keep a backup from before the upgrade if you may need to downgrade.
- **Counters.** Families in 0.1.0 files store `pigeonhole.i64_add` without a kind (the new `kind` byte reads as `Standard` when absent) and keep 0.1.0 behavior: `incr` still works there, at the commit timestamp, with runs of increments folded across timestamps when read (D41), and `put` of bytes still works. New code that declares a family with `Family::default()` and calls `incr` on it fails with `InvalidArgument`: declare it with `Family::counter()`. An existing family keeps its stored kind (declaring it again with other options changes nothing), so to move counters, add a counter family and copy each counter: read it with `get`, write the value with `put_i64` (or `put_i64_at` for a dated bucket) into the new family, then point increments there. For a new family with 0.1.0 behavior, declare `Family::default().merge_operator("pigeonhole.i64_add")`. Per-period counters kept as one qualifier per period can stay as they are, or become buckets of one column with `incr_at`.
- **`FamilyOptions` and the model types.** Code that builds `FamilyOptions`, `ModelFamily` or `ModelPurge` with a struct literal no longer compiles outside its crate. Start from the default and use the setters, one per field: `FamilyOptions::default().max_versions(1).merge_operator("pigeonhole.i64_add")` instead of `FamilyOptions { max_versions: 1, merge_operator: "pigeonhole.i64_add".into(), ..FamilyOptions::default() }`; `ModelFamily::new("f").counter(true)`; `ModelPurge::new("t", "f").snapshots(s).now(now).min_ts_above(ts).max_seqno(n)`. Reading and assigning fields still works.
- **FUSE.** A database on a FUSE filesystem (ntfs-3g, or an encrypted home directory such as gocryptfs) now fails to open with `NetworkFilesystem` (#147). If the mount is local and you trust it, open with `Options::allow_fuse(true)` (and `ReaderOptions::allow_fuse(true)` in reader processes); never for network-backed FUSE such as sshfs or s3fs.

## [0.1.0] - 2026-10-08
The first release: the **Phase 1 core engine** of an embedded, single-file, wide-column store ([release notes](https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.1.0)).

### Added
- One-file embedded database (`.phdb`, plus WAL sidecars and a shared-memory region while open); `Options::default()` is valid.
- Thread-per-core shards with per-shard WAL streams and group commit; durability levels `None`, `Buffered`, `GroupSync` and `Sync`.
- All-or-nothing cross-shard commits (two-phase commit), a global visibility watermark and MVCC snapshots.
- Tablets that split, merge and move between shards (on by default).
- Per-family LSM: shared-memory memtable arenas, SSTs, leveled compaction, HBase-style delete and GC semantics, counters (`incr`), merge operators and TTL.
- Reader processes that attach through shared memory; snapshots expire cleanly across a writer restart (`SnapshotExpired`).
- Maintenance: `flush`, `compact`, `backup` and `shrink`.
- Cheap opens: about 14 ms and well under 1 MiB written for a small database, whatever the shard count.
- A flat, stable `ErrorCode`, including `Busy` (retryable stall), `BatchTooLarge`, `ShmUnavailable` and `WouldDeadlock`.
- The user guide, an agent reference and the design decisions log.

### Testing
- Deterministic simulation with fault injection, process crashes and power loss, checked against a reference model with a record-level recovery oracle.
- Seed sweeps of 1-300 on every suite, Miri on the `unsafe` crates, loom on the concurrent ones, and CI on Linux, macOS and Windows.

### Known limits
- Write throughput does not yet scale with shard count (#154, Phase 3).
- Open latency is about 14 ms against the 5 ms goal (#158, Phase 3).
- The file at rest can be 2-4x live data (#185, Phase 2).
- Application-owned mode still runs the default I/O backend's 2-16 I/O threads (Phase 3).
- `pigeonhole-cli` (`phdb`) is a placeholder until Phase 4.

[Unreleased]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.1.0
