# Changelog

All notable changes to Pigeonhole are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, the on-disk format ([`FORMAT.md`](FORMAT.md)) and the API may change in any release.

> **Experimental 0.x.** Not recommended for production use yet. See [`docs/status.md`](docs/status.md).

## [Unreleased]
The next release is **0.2.0**: counters change in a breaking way (below), and every published crate moves to 0.2.0 together.

### Changed (breaking)
- **Counters are declared families** (decision D179, after Bigtable's aggregate families; #274). `Family::counter()` declares a counter family; `Family::default()` no longer carries the `pigeonhole.i64_add` operator, and `incr` on any family that is not a counter family fails at commit with `ErrorCode::InvalidArgument`. In a counter family:
  - `incr` writes at one fixed timestamp (0), so a counter is one cell however often it is incremented: increments combine at read time and in compaction (this replaces the planned cross-timestamp fold, #34).
  - `incr_at(family, qualifier, ts, delta)` (new) adds to the bucket at `ts`, for hourly or daily totals; each bucket is a version. `put_i64` sets the counter and `put_i64_at` (new) a bucket; later increments add to them.
  - Only `i64` values are accepted: `put`, `put_at`, `put_f64` and untyped `merge` operands fail with `InvalidArgument`.
  - A delete hides only what was written before it, so an `incr` after `delete_column` or `delete_row` starts the counter from 0.
  - Writes of one counter in one mutation or batch apply in order (D186, #295): `incr(c, 1).incr(c, 2)` adds 3 and `put_i64(c, 5).incr(c, 1)` leaves 6. Other families keep D34 (the last write of a cell in a commit wins).
  - A TTL expires each bucket; with a TTL the fixed timestamp would expire at once, so `incr` and `put_i64` without a timestamp fail with `InvalidArgument` there. `max_versions` limits reads; compaction keeps older buckets, so bound storage with a TTL.
- `pigeonhole-engine`, `pigeonhole-format`, `pigeonhole-compaction` and `pigeonhole-sim`: `FamilyOptions::kind` (`FamilyKind::{Standard, Counter}`), `WriteBatch::merge_at`, `COUNTER_TS`, `ResolveOptions::counter`, `GcPolicy::other_sources`, and the model's `ModelFamily::counter`, `ModelOp::Incr::ts` and `ModelError::CounterWrite`.

### Migrating from 0.1.0
- Files written by 0.1.0 open and read as before. Their families store `pigeonhole.i64_add` without a kind (FORMAT.md: the new `kind` byte is appended to the family options and reads as `Standard` when absent), and keep 0.1.0 behavior: `incr` still works there, at the commit timestamp, with runs of increments folded across timestamps when read (D41). `put` of bytes into them also still works.
- New code that declares a family with `Family::default()` and calls `incr` on it fails with `InvalidArgument`: declare the family with `Family::counter()` instead. An existing family keeps its stored kind (declaring it again with other options changes nothing), so to move counters, add a counter family and copy each counter: read it with `get` and write the value with `put_i64` (or `put_i64_at` for a dated bucket) into the new family, then point increments there. To keep 0.1.0 behavior for a new family, declare it `Family::default().merge_operator("pigeonhole.i64_add")`.
- Per-period counters kept as one qualifier per period can stay as they are, or become buckets of one column with `incr_at`.

### Added
- `Options::allow_fuse(true)` and `ReaderOptions::allow_fuse(true)` accept a database on a FUSE filesystem, for local FUSE mounts the user trusts (ntfs-3g, gocryptfs). FUSE is still refused by default, and network and cluster filesystems are refused either way (D173, #299). `pigeonhole-io` adds `File::locality` and `Locality`, which tell FUSE apart from other non-local filesystems.
- Per-family compaction strategies: `Compaction::Tiered` (universal/size-tiered, for write-heavy families) and `Compaction::FifoByTime` (drops whole SSTs once their newest timestamp passes the TTL, with no rewrite) are accepted at table creation (#31, #32, #44). See [Compaction styles](docs/guide/concepts.md#compaction-styles) in the guide. `FifoByTime` expiry runs on a timer, so an idle family drops expired files on time (#232).
- Custom merge operators: `Options::merge_operator` / `ReaderOptions::merge_operator` register them and families name them with `Family::merge_operator(name)`; opening over a family whose operator is not registered needs `allow_unregistered_merge_operators(true)` and is then read-only (#43).
- `Options::write_stall_timeout(Duration)`: how long a stalled write, `flush` or `compact` waits before `Busy` (default 30 s, as before) (#210).
- zstd block compression: `Family::zstd(level)` is accepted at table creation and stores blocks as zstd frames at the family's level (#44).
- Blob separation (#33): values above a family's `blob_threshold` (default 4096 bytes) move to blob files when flushed or compacted, and blob GC rewrites blob files once they are half garbage.

### Fixed
- With tablet changes off, a shard's memtable arena is sized for the table and family slots it holds, as with them on. A memtable budget under 16 MiB used to serve only 16 slots per shard, and more (for example 4 tables of 6 families on one shard) stalled writes until `Busy` (#283).
- A database on NFS without working locks now fails to open with `NetworkFilesystem` instead of an I/O error about locks, and FUSE and GPFS mounts are refused as network filesystems (#147).

### Changed
- The write stall follows L0 depth only; deeper levels and tiered space amplification no longer pace writers (D119).
- On-disk format version 2 (FORMAT §12). This build reads 0.1.0 files, but a file it has written cannot be opened by 0.1.0.

Planned for Phase 2 (the wide-column model), see [`docs/status.md`](docs/status.md):
- Raising the Phase 1 value cap now that blob files can span extents (#230).
- A tighter file layout: the file at rest can be 2-4x live data because of power-of-two extents (#185).

Planned for Phase 3 (latency): write throughput that scales with shard count (#154), open latency toward the 5 ms goal (#158), and the async API.

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

[Unreleased]: https://github.com/CodingAnarchy/pigeonhole/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/CodingAnarchy/pigeonhole/releases/tag/v0.1.0
