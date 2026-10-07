<!-- Source of truth: "Task briefs" tab of https://claude.ai/artifact/4zEQ4RyDCMoovUiUNVwrco (exported 2026-10-05). -->
# Task briefs

One brief per crate, in build order. Each agent gets its crate's brief plus [spec.md](spec.md); the brief says what to read there, what the crate owns, and when it is done.

Project-level deviations from the original briefs are recorded in [decisions](decisions/README.md) and win over this file.

## pigeonhole-format
- **Goal:** Every on-disk and in-shared-memory byte layout, as pure encode and decode functions with no I/O.
- **Read:** Data model (key layout, limits and byte order); Storage architecture (file layout, SST and block format); Files and locks; Multi-process readers (shared-memory contents); `FORMAT.md` from the interface freeze.
- **Owns:** `encode_key` / `decode_key`, block builders and readers, SST footer, superblock, manifest edit records, WAL frame and PREPARE/COMMIT records, shared-memory header layout, `FormatVersion`, `ShmLayoutVersion`.
- **Uses:** nothing in the workspace.
- **Done when:** property tests show encoded byte order equals logical order for random rows, qualifiers, timestamps and seqnos; every type round-trips; fuzzed decoders never panic; golden files exist for each record type.
- **Out of scope:** files, threads, caching.

## pigeonhole-io
- **Goal:** The `Vfs` abstraction every other crate uses for files, aligned buffers and asynchronous completions.
- **Read:** Performance design (I/O backends); Files and locks (lock protocol); Workspace rules.
- **Owns:** `Vfs`, `File`, `IoBuf`, `Completion`, byte-range lock API; backends: `pread` thread pool, simulated VFS with fault injection. io_uring arrives in Phase 3.
- **Uses:** format (error and version types only).
- **Done when:** the backend parity suite passes on both backends; the simulated VFS can inject torn writes, reordered fsyncs, ENOSPC and crashes deterministically from a seed; lock semantics match on Linux, macOS and Windows.
- **Out of scope:** caching, retry policy.

## pigeonhole-sim
- **Goal:** Deterministic simulation and the reference model every other crate is tested against.
- **Read:** Risks (crash consistency); Crate breakdown (workspace rules).
- **Owns:** `Sim` (seeded scheduler, simulated time, crash points), `Model` (an in-memory `BTreeMap` implementation of the full Pigeonhole semantics: versions, TTL, snapshots, durability levels), `Workload` generators.
- **Uses:** io (simulated backend); drives the public crate once it exists.
- **Done when:** a toy store built on `io` passes, and the checker catches each bug in a seeded set of known-broken builds.
- **Out of scope:** performance measurement.

## pigeonhole-pager
- **Goal:** The main page file: superblocks, extent allocation and safe freeing.
- **Read:** Storage architecture (file layout); Files and locks; Crash safety.
- **Owns:** `Pager`, `Extent`, `commit_root` (superblock flip), free-space bitmap, epoch-deferred freeing, online `shrink`.
- **Uses:** io, format.
- **Done when:** a crash at every write point never yields an unopenable file; no extent is ever double-allocated; a freed extent is unreachable from every live view.
- **Out of scope:** manifest contents (it stores what the engine hands it).

## pigeonhole-wal
- **Goal:** One durable, recycled log stream per shard, with group commit and cross-shard records.
- **Read:** WAL sidecar decision; Files and locks; Durability; Ordering, snapshots and cross-shard commits.
- **Owns:** `Wal` trait, `WalStream`, `CommitTicket`, `Recovery`, segment recycling with epochs, PREPARE and COMMIT records.
- **Uses:** io, format.
- **Done when:** torn tails are truncated; stale records in reused segments are rejected; group commit preserves seqno order; a returned `Buffered` commit survives process kill and a returned `GroupSync` commit survives simulated power loss.
- **Out of scope:** deciding commit outcomes (the engine does).

## pigeonhole-memtable
- **Goal:** The in-memory write buffer, readable across threads and processes.
- **Read:** Versions, writes and transactions (memtables); Multi-process readers (offset-linked arenas, NUMA placement).
- **Owns:** `Memtable`, `MemIter`, offset-based skiplist over an arena region it is given.
- **Uses:** format; takes its arena region from shm (mock it with a heap region until shm lands).
- **Done when:** loom tests pass for one writer and many readers; a reader in a second process traverses it correctly in the multi-process test; no pointers are stored in the arena.
- **Out of scope:** flushing (the engine drives it).

## pigeonhole-cache
- **Goal:** Per-process block and row caches with pinned, ref-counted handles.
- **Read:** Performance design (read path, owned buffer pool).
- **Owns:** `BlockCache`, `RowCache`, `BlockHandle`, `Cell`.
- **Uses:** io.
- **Done when:** eviction respects pins and priorities; loom shows no use-after-evict; the hit path allocates nothing (benchmarked).
- **Out of scope:** sharing caches across processes.

## pigeonhole-runtime
- **Goal:** Shard threads and their scheduling, in both embedding modes.
- **Read:** Thread-per-core execution.
- **Owns:** `Shard`, `ShardContext`, `Task`, `Submitter`, CPU pinning, MPSC submission queues, cooperative time-sliced scheduler, completion wakeups for sync and async callers.
- **Uses:** io.
- **Done when:** scheduling is deterministic under the simulator; foreground latency stays bounded while background tasks run; engine-owned and application-owned modes pass the same suite.
- **Out of scope:** tablets and routing (the engine owns them).

## pigeonhole-shm
- **Goal:** The shared-memory region and the writer and reader process protocols.
- **Read:** Multi-process readers; Files and locks.
- **Owns:** `ShmRegion`, `ReaderSlot`, `WriterLock`, `Generation`, view publication by version, memory-backed placement and naming, NUMA binding, layout version checks.
- **Uses:** io, format.
- **Done when:** the multi-process suite passes: readers see commits in order and never half a cross-shard commit; killed readers' slots are reclaimed; writer kill and restart leaves readers on a valid snapshot, then remapped; a second writer and a mismatched layout version are refused.
- **Out of scope:** what goes in the arenas (memtable owns that).

## pigeonhole-sst
- **Goal:** Immutable sorted runs and fast iteration over them.
- **Read:** SST and block format; Scans.
- **Owns:** `SstWriter`, `SstReader`, `ScanFilter`, filters, partitioned index, row-skipping block iterator.
- **Uses:** format, io, cache.
- **Done when:** write-then-read equals input for random data; filters never return a false negative; pushed-down filter results equal unfiltered results filtered afterward.
- **Out of scope:** choosing what to compact.

## pigeonhole-compaction
- **Goal:** Keep read and space amplification bounded per family.
- **Read:** Compaction, per family; Background work; Merge operators.
- **Owns:** `CompactionPicker` (leveled in Phase 1; tiered and FIFO-by-time in Phase 2), `CompactionJob`, `MergeOperator`, version, TTL and tombstone GC, blob separation and blob GC.
- **Uses:** sst, pager, format.
- **Done when:** no compaction changes any read result at any live snapshot (checked against the model); TTL'd data is reclaimed.
- **Out of scope:** scheduling (the engine and runtime decide when).

## pigeonhole-engine
- **Goal:** Assemble everything into a correct, thread-per-core database.
- **Read:** the whole spec.
- **Owns:** `Engine`, `Snapshot`, `View`, `WriteBatch`, `Txn`; tablets, routing, splits, merges and rebalancing; single manifest writer; seqno reservation and watermark; cross-shard two-phase commit; durability resolution; online backup.
- **Uses:** every crate above.
- **Done when:** the full model-checked simulation suite, crash-recovery suite and durability matrix pass; cross-shard atomicity holds with a crash at every two-phase-commit step; results are identical for 1 to 64 shards; the scaling gate is met.
- **Out of scope:** public API ergonomics.

## pigeonhole
- **Goal:** The public crate users depend on.
- **Read:** API surface; Sync and async; Durability; Language scope.
- **Owns:** `Pigeonhole::open`, `open_reader`, table and family builders, mutations, gets, scans, `Durability`, typed errors; sync API in Phase 1, async in Phase 3.
- **Uses:** engine.
- **Done when:** every doc example compiles and runs; the sync/async parity suite passes; semver checks run in CI; nothing Rust-only crosses what will become the C ABI boundary.
- **Out of scope:** bindings.

## pigeonhole-bench
- **Goal:** Measure every gate reproducibly.
- **Read:** Goals; How it will be measured; Reference hardware.
- **Owns:** YCSB A to F, sparse-wide, time-series with TTL, adjacency scans, skewed multi-shard writes; runners for RocksDB, SQLite and Fjall; p50, p99 and p99.9 reports.
- **Uses:** pigeonhole.
- **Done when:** repeated runs on the reference hardware agree within a set tolerance.
- **Out of scope:** tuning the engine.

## pigeonhole-arrow (Phase 4)
- **Goal:** Export scans as Arrow `RecordBatch`es.
- **Done when:** batches round-trip to the same cells as a plain scan.

## pigeonhole-cli (Phase 4)
- **Goal:** The `phdb` binary: `shell`, `dump`, `compact`, `check`, `backup`.
- **Done when:** CLI output snapshot tests pass and `phdb check` detects injected corruption.

## pigeonhole-capi (future)
- **Goal:** Stable C ABI and `pigeonhole.h`, per Language scope.
- **Done when:** ABI compatibility checks and a C test program pass.
