# Changelog

All notable changes to Pigeonhole are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, the on-disk format ([`FORMAT.md`](FORMAT.md)) and the API may change in any release.

> **Experimental 0.x.** Not recommended for production use yet. See [`docs/status.md`](docs/status.md).

## [Unreleased]

### Security
- zstd is now 0.14.3, with zstd-safe 8.2 and zstd-sys 2.1.2, up from 0.13.3. The upgrade clears RUSTSEC-2026-0391, 0398, 0403 and 0410. Pigeonhole called only zstd's one-shot `bulk` API and never the stream encoders or decoders those advisories describe. The bundled libzstd is still 1.5.7, so compressed blocks are byte-identical. Files written by earlier releases read as before, and a test now decodes a zstd-sealed block written by 0.13.

## [0.2.5] - 2026-10-22
One engine fix from `main`, also released as 0.3.2 (#904). **Upgrade recommended** for every 0.2.x user: after an open with a different shard count, or one whose replay spilled, a crash before the next manifest commit could let later default-timestamp writes land below existing versions of a cell (a lost update). The file format is unchanged; files written by 0.2.0 to 0.2.4 open as before.

### Fixed
- **A reopen that flushes its replay keeps the timestamp floor above the replayed writes** (#904). An open with a different shard count, or one whose replay spilled to SSTs, flushes everything it replayed and checkpoints the WAL past it, but its manifest commit left the default-timestamp floor where it was. After a crash before the next manifest commit, the next open found nothing to replay and started its default timestamps from that lower floor, so with the clock behind the replayed writes' timestamps (it stepped back, or default timestamps had run ahead of it) a new default-timestamp write could land below an existing version of the same cell and stay hidden behind it: a lost update (#653's class). The floor now carries the replayed timestamps. Present since 0.2.0.

## [0.2.4] - 2026-10-22
Two engine fixes from `main`, also released as 0.3.1 (#862). **Upgrade recommended** for every 0.2.x user: a close could hang for ever on a full memtable arena, and a `None`-durability commit could come back in part after a shard's WAL failure. The file format is unchanged; files written by 0.2.0 to 0.2.3 open as before.

### Fixed
- **A close no longer hangs when a voided purge guard refuses the flush of its in-place memtables** (#838). A close whose arena had no chunk left flushes its active memtables in place (#111); when that flush also carried a frozen memtable whose purge guard a delete had voided (#287), the manifest refused the whole flush and only the frozen memtables were queued again, so the in-place ones were never flushed, their log records stayed needed, and `close()` waited for ever (the shard idle, nothing to flush). The refused flush now re-queues them too, and an I/O error on such a flush counts as a failure (the close gives up, unclean) instead of a retry. Present since 0.2.0 (#315). Found by the public model check's recovery arm (#837).
- **A flush on a shard whose WAL stream failed can no longer make a commit durable in part** (#847). A flush's sync barrier on a poisoned stream (a write or sync had failed) answered that the stream held once its cross-shard records were durable, which a single-shard database meets at once; so a flush in flight at the failure committed its SST and `SetFlushed` over records the log never held. A `None`-durability commit is applied before its buffered record reaches the file (it goes with the next group's write, the one that failed), so its cells in the slots that flushed survived the reopen and the others did not: the public model check's recovery arm read a part of such a commit after a recovery. The shard's own barrier now holds on a poisoned stream only if the stream is durable through everything appended; a peer's barrier keeps the cross-shard rule (D115, amended). The flush fails instead, the close gives up and is unclean, and the next open replays the log: the commit is there whole or not at all. Present since 0.2.0 (#159's barrier rule for a failed stream; a `None` record is buffered until the next group's write).

## [0.2.3] - 2026-10-20
A fix for a lost write under concurrent cross-shard and single-shard commits. **Upgrade recommended** for every 0.2.x user whose writes span shards. The file format is unchanged, and files written by 0.2.0 to 0.2.2 open as before. Every published crate moves to 0.2.3 together.

**Known issue (#655, also in 0.2.2):** a cross-shard commit that finds a participant's memtable arena full waits for room while holding its sequence number, and that participant's flush waits for the same sequence number to become visible. With a real clock the wait ends at the write-stall timeout (`Options::write_stall_timeout`, 30 s by default): the commit, and writes queued behind it on that shard, get `Busy` and can be retried. It needs a full arena, so a larger `Options::memtable_budget` makes it rarer. 0.3.0 refuses such a commit and retries it once room frees, instead of letting it wait (D245).

### Fixed
- **A write could be lost behind an earlier cross-shard commit to the same row** (#653, D244, D245).
  - **When:** a commit wrote rows on more than one shard, and another thread wrote the same rows on one of those shards, without explicit timestamps, just after it. More likely with many threads and busy shards. Tablet changes on or off.
  - **What happened:** the shard could give the later single-shard write a default timestamp below the cross-shard commit's. Reads then returned the cross-shard commit's older value, and the later write was lost.
  - **The fix:** the coordinating shard raises every participant's timestamp floor before it takes the commit's sequence number, so later writes there stamp above the commit (D245). A participant also refuses a commit whose timestamp would land out of order with writes it has already made, and the commit is retried with a fresh timestamp (D244).
  - **Side effect:** a cross-shard commit refused this way is retried internally. Like one refused for a tablet change, it gets `Busy` after 16 refusals or the write-stall timeout. That is rare, since the floor raise above prevents almost every refusal.

## [0.2.2] - 2026-10-12
Fixes to closing a database after cross-shard commits. **Upgrade recommended** for every 0.2.x user whose writes span shards. The file format is unchanged, and files written by 0.2.0 and 0.2.1 open as before. Every published crate moves to 0.2.2 together.

### Fixed
- **Closing the database could hang forever after concurrent cross-shard commits** (#585, D225).
  - **When:** two commits that each wrote rows on more than one shard were in flight together, and each was coordinated by a shard the other wrote to. Likelier with many shards and many threads writing at once.
  - **What happened:** each shard's WAL checkpoint waited for the other's, so neither could move. `close` waited for them and never returned. While running, the WAL stopped being trimmed past those commits. With many such commits in flight, a shard thread could instead overflow its stack during the close and abort the process.
  - **The fix:** a shard drops a cross-shard commit's record from its WAL once every shard's part of the commit is written to the file, without waiting for the coordinating shard's checkpoint (D225).
- **Closing the database could hang forever after a crash during cross-shard commits** (#591). The reopen recovered correctly, but a commit the recovery had correctly discarded was later treated as undecided, and the close waited for it.
- **Closing the database could hang forever after a tablet move** (#597): a shard was not told when another shard wrote its moved data to the file, so a commit never completed its bookkeeping.

## [0.2.1] - 2026-10-11
A fix to WAL crash recovery. **Upgrade recommended** for every 0.2.0 user. The file format is unchanged, and files written by 0.2.0 open as before. Every published crate moves to 0.2.1 together.

### Fixed
- **Records the WAL had lost could come back after a later recovery** (#508, D209).
  - **When:** a power loss (not a process crash) while a WAL segment was starting in a reused slot. The disk had to keep part of that write (the first records) and lose the rest (the segment's header). The database then had to reopen, write little or nothing more to that WAL stream, and recover again later.
  - **What happened:** recovery correctly ignored those records the first time. But the reopened stream reused the lost header's numbering in the same slot, so at the next recovery the old records read as new ones.
  - **Effect:** writes that a power loss should have discarded could reappear. Their sequence numbers could also be reused by newer commits, mixing old and new data in recovery. The records that come back are only ever ones that were never synced.
  - **The fix:** recovery now starts a reopened stream three numbers above the largest it saw (FORMAT §10.1). Nothing reads or writes differently otherwise.

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

[Unreleased]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.2.5...HEAD
[0.2.5]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.1.0
