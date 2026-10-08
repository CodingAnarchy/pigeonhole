# Changelog

All notable changes to Pigeonhole are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, the on-disk format ([`FORMAT.md`](FORMAT.md)) and the API may change in any release.

> **Experimental 0.x.** Not recommended for production use yet. See [`docs/status.md`](docs/status.md).

## [Unreleased]
### Added
- Per-family compaction strategies: `Compaction::Tiered` (universal/size-tiered, for write-heavy families) and `Compaction::FifoByTime` (drops whole SSTs once their newest timestamp passes the TTL, with no rewrite) are accepted at table creation (#31, #32, #44). See [Compaction styles](docs/guide/concepts.md#compaction-styles) in the guide. `FifoByTime` expiry runs on a timer, so an idle family drops expired files on time (#232).

### Changed
- The write stall follows L0 depth only; deeper levels and tiered space amplification no longer pace writers (D119).

Planned for Phase 2 (the wide-column model), see [`docs/status.md`](docs/status.md):
- Blob separation for large values (raising the Phase 1 value cap), zstd compression and custom merge operators.
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
