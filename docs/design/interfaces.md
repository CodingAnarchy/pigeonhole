# Interfaces

The frozen contracts between crates, from the interface freeze (build step 2). Component agents build against these. Byte layouts are in [`FORMAT.md`](../../FORMAT.md); decisions D7 onward in [decisions.md](decisions.md) explain the choices. Changing anything here takes an interface-change request (`docs/design/icr/`).

Every public item has rustdoc; bodies are `todo!()`. Each stub crate carries `#![allow(unused_variables, clippy::ptr_arg)]` under an "Interface freeze" comment; remove it when implementing.

## Design rules applied

- **Static dispatch on per-cell paths.** Sorted sources implement `format::Cursor`; merging and resolution are generic over `C: Cursor`; the engine wraps heterogeneous sources (memtable, SST) in an enum. No trait objects per entry.
- **Trait objects only at I/O and lifecycle boundaries:** `Arc<dyn Vfs>`, `Arc<dyn File>`, `Box<dyn Wal>` (one call per group commit), `Box<dyn Task>` (background jobs), `Arc<dyn MergeOperator>` (merge resolution only).
- **Zero copy.** Keys and values borrow block buffers or arena memory; anything that must outlive a cursor is a ref-counted pin (`cache::Cell`, `engine::CellData`), never a copy.
- **One generic where it pays:** `runtime::Runtime<H: ShardHandler>`, so shard messages are an engine enum with no boxing.
- **Errors:** one enum per crate with `From` conversions upward; the public crate flattens to `ErrorCode` + message.

## Crates, bottom up

### `pigeonhole-format` (uses nothing)
Pure encode/decode; never panics on input.
- **Vocabulary:** ids (`TableId`, `FamilyId` (unique per database), `TabletId`, `SstId`, `BlobFileId`, `StreamId`, `CommitId`), `Seqno = u64`, `Timestamp = u64`, `Lsn`, `Durability`, `FormatVersion`, `ShmLayoutVersion`, magic constants.
- **`key`:** `encode_key`, `encode_marker_key`, `encode_seek_key`, `encode_row_prefix`, `encode_column_prefix`, `decode_key -> KeyParts` (borrowed `Escaped` parts), `split_suffix`, `row_prefix_len`, `column_prefix_len`. All append to a caller-owned `Vec<u8>`.
- **`value`:** `ValueTag`, `ValueRef`, `encode_value`/`decode_value`, `BlobPointer` (16 bytes).
- **`block`:** `BlockBuilder` (data/index), `Block`/`BlockIter` (zero-copy `Cursor` with O(1)-ish `skip_row` via the row-start table), `seal`/`verify` (trailer + xxh3), `BlockAddr`.
- **`filter`:** `FilterBuilder`, `Filter::may_contain`, `row_hash`, `column_hash`.
- **`sst`:** `Footer`, `Properties`. **`blob`:** extent header, record header. **`superblock`:** `Superblock`, `ExtentRef`, lock-byte offsets. **`manifest`:** `ManifestHeader`, `Edit`, `FamilyOptions`, `SstMeta`, `encode_block`/`decode_block`.
- **`wal`:** `SegmentHeader`, `FrameEncoder`/`FrameDecoder` (fragmentation, torn-tail detection), `WalRecord` (Batch/Prepare/Commit), `BatchBuilder`/`BatchRef`/`Mutation` (the batch encoding that is both the engine's `WriteBatch` and the WAL payload).
- **`shm`:** header field offsets, `ShmHeader`, `ViewRecord`, reader-slot and memtable-node offsets, `region_name`.
- **`compress`, `checksum`, `varint`; `Cursor` trait.**

### `pigeonhole-io` (uses format)
- **`Vfs`** (`VfsRef = Arc<dyn Vfs>`): `open`, `remove`, `exists`, `list_dir`, `sync_dir`, `open_shared`/`remove_shared`, `now_micros`, `monotonic_nanos`, `current_process`, `process_alive`. Clocks and process liveness live here so the simulator controls them.
- **`File`** (`FileRef = Arc<dyn File>`): positional `read_at`/`write_at`, `submit_read`/`submit_write -> Completion`, `sync_data`, `sync_all`, `len`, `set_len`, `allocate`, non-blocking single-byte `lock`/`unlock` (`LockMode::{Shared, Exclusive}`), `identity` (device, inode), `is_local`.
- **`IoBuf`** (4096-aligned owned buffer), **`Completion`** (`wait()` for sync callers, `Future` for async).
- **`SharedRegion`**: mapped shared memory (or `heap(len)`), accessed only via `atomic_u32/atomic_u64(offset)`, `read`/`write` copies, `base_ptr` (for memtable's `unsafe`), `bind_numa`.
- **`sys`:** `available_cpus`, `pin_current_thread`, `numa_node_of`.
- **Backends / mocks:** `pread::PreadVfs` (real files), `sim::SimVfs` (in-memory, seeded; `FaultPlan` with torn writes, reordered unsynced writes, ENOSPC, error injection, `crash_after_ops`; `crash(CrashKind)`, `advance`, simulated processes). `SimVfs::new(seed)` with no faults *is* the in-memory mock.

### `pigeonhole-sim` (uses io, format)
`Sim` (seeded scheduler over closures, simulated time, `crash`), `Rng`, `Model` (BTreeMap semantics: versions, TTL, deletes, `i64` merge, snapshots, `crash_window` per durability), `Workload`/`WorkloadSpec`/`Op`. Full-stack suites live in `engine/tests` and `pigeonhole/tests` (D1). Shards run under `Sim` in application-owned mode.

### `pigeonhole-pager` (uses io, format)
`Pager::create`/`Pager::open -> OpenedPager` (reads superblocks, exposes `root()` and the file so the engine can read the manifest) `-> finish(live_extents) -> Pager`. `allocate(bytes) -> Extent`, `abandon`, `read`/`write`, `commit_root(Root)` (sync, flip superblock, sync), `retire(extent, superseded_at)`, `reclaim(oldest_live_manifest_version)`, `shrink_plan`/`relocate`/`truncate_tail`, `mark_clean`. Shared by all shards (`&self`). Mock: a `Pager` on `SimVfs`.

### `pigeonhole-wal` (uses io, format)
`trait Wal` (one per shard, `&mut self`, owned by the shard thread): `append(&WalRecord, Durability) -> CommitTicket` (buffer only), `write()` (one `write()` syscall per group), `sync()` (one fdatasync per group), `written`/`durable`, `satisfies(&CommitTicket)`, `checkpoint(Lsn)`, `remove`. `WalStream::create`; `Recovery::open(.., checkpoint)` then `next_record()` (lending) and `into_stream()` (truncates the torn tail). `discover_streams`, `stream_path`. Mock: `MemWal` (keeps appended/written/synced separately; `crash(power_loss)`).

### `pigeonhole-memtable` (uses format, io)
`ArenaRegion` (`heap(len)` mock, or `new(SharedRegion, offset, len)`), `ShardArena` (writer-private chunk allocator; `reclaim(Retired)`), `Memtable` (writer: `create`, `insert(&mut ShardArena, key, value)`, `freeze`, `allocated_bytes`, `seqno_range`, `root`, `reader`, `retire`), `MemtableReader` (any thread or process: `open(region, root)`, `iter`), `MemIter: Cursor`. Keys are full internal keys; values are stored values. Depends on io for `SharedRegion` (D14).

### `pigeonhole-cache` (uses io)
`BlockCache` (`new(capacity, shards)`, `disabled()`, `get(BlockKey) -> Option<BlockHandle>` allocation-free, `insert(key, BlockData, Priority)`, `erase_file`), `BlockHandle` (pinned, `Deref<[u8]>`), `Cell` (pinned sub-range or small owned buffer; the unit of zero-copy value return), `RowCache`/`RowHandle`. Mock: the real cache, small or disabled.

### `pigeonhole-runtime` (uses io)
`trait ShardHandler { type Msg; handle(ctx, msg); end_batch(ctx) }` (the engine's per-shard state; `end_batch` is the group-commit point). `Runtime::start` (engine-owned: pinned threads) or `Runtime::application_owned -> Vec<ShardDriver>` (`run_once(deadline)`, `with_handler`, `set_wakeup`). `Submitter<M>` (MPSC enqueue + wake), `ShardContext` (shard id, submitters, `spawn(Box<dyn Task>)`, clock), `trait Task { run(deadline, &TaskWaker) -> TaskPoll }`, `completion() -> (Notifier<T>, Waiter<T>)` (`wait()` or `.await`).

### `pigeonhole-shm` (uses io, format)
`WriterLock::acquire`, `Presence::{acquire, try_become_last}` (lock-page bytes), `ShmRegion::open(vfs, file, identity, db_id, Role, &ShmConfig)` / `in_memory` (mock) / `remove`. Seqnos: `reserve_seqnos`, `publish_pending`, `visible_seqno` (protocol in FORMAT §11.3). Views: `publish_view(&ViewRecord)`, `read_view`, `view_version`, `set_manifest_version`/`manifest_version`. Arenas: `arena(shard) -> (SharedRegion, offset, len)`, `bind_arena`. Readers: `claim_reader_slot -> ReaderSlot { pin, unpin }`, `oldest_reader_pin`, `reclaim_dead_slots`. Owns no `unsafe`: all access goes through `SharedRegion` atomics.

### `pigeonhole-sst` (uses format, io, cache)
`SstWriter::new(file, extent, id, SstWriterOptions)`, `fits`, `add`, `finish -> SstMeta`, `abandon`. `SstReader::open(file, &SstMeta, cache, priority)` (pins top index and filters), `may_contain_row`/`may_contain_column`, `iter(ScanFilter, ReadOptions) -> SstIter: Cursor` (+ `value_cell()` for a zero-copy pin). `ScanFilter` holds only entry-safe conditions: qualifier selection and time range on puts/merges; deletes and markers always pass (D22). `BlobWriter`/`BlobReader` for blob extents. Mock: the real writer/reader on `SimVfs`.

### `pigeonhole-compaction` (uses sst, pager, format, cache, io)
- Read machinery shared with the engine: `MergingCursor<C>`, `CellResolver<C>` (snapshot visibility, deletes, TTL, versions, columns per row, `ValuePredicate`, merge resolution) with `ResolveOptions`, `ResolvedCell`.
- `MergeOperator` (name stored in the file), built-in `I64Add` (`pigeonhole.i64_add`), `MergeRegistry`.
- `Levels`, `CompactionPicker::{new(style, PickerOptions), score, pick}`, `CompactionTask`/`TaskKind`, `GcPolicy` (live snapshot list, `now`, bottommost), `JobContext`, `CompactionJob::{new, run(deadline), finish -> CompactionOutput, abort}`.

### `pigeonhole-engine` (uses all of the above)
`Engine::open` / `open_application_owned -> (Arc<Engine>, Vec<EngineShard>)` / `open_reader`; catalog (`create_table`, `add_family`, `table`, `tables`, `drop_table` with `TableInfo`/`FamilyInfo`); writes (`WriteBatch`, `submit -> PendingCommit`, `commit`, `check_and_mutate(Predicate)`, `begin -> Txn`, `set_default_durability`); reads (`snapshot -> Snapshot`, `get -> Option<CellData>`, `read_row(ReadSpec) -> RowData`, `scan(ScanSpec) -> ScanCursor`); maintenance (`flush`, `compact`, `backup`, `shrink`, `metrics`, `close`). `Snapshot` = seqno + `Arc<View>`; `View` = tablet map + memtables + manifest version; `TabletMap::route`.

### `pigeonhole` (uses engine; format and io for re-exports and the test hook)
`Pigeonhole::{open, open_reader, open_application_owned}`, `table(name) -> TableBuilder { family, create_if_missing, create, open }`, `Table { mutate, get, get_at, row, scan, scan_prefix, scan_bounds }`, `ReadTable` (read-only), `RowMutation { put, put_at, put_i64, put_f64, incr, merge, delete_cell, delete_column, delete_family, delete_row, durability, commit, commit_if }`, `WriteBatch { put, put_at, incr, delete_column, delete_row, commit, commit_with }`, `Transaction`, `RowRead`/`Scan` builders, `RowIter` (`Iterator<Item = Result<Row>>` plus lending `next_ref`), `CellRef<'_>`/`Cell`, `RowRef<'_>`/`Row`, `Value`, `ValueFilter`, `Condition`, `Options`, `ReaderOptions`, `Family`, `Durability`, `Error { code: ErrorCode, message }`. The async API is reserved in `nonblocking` behind the `async` feature (Phase 3, D17).

**C ABI readiness:** every borrowed type has an owned counterpart (`Cell`, `Row`), every iterator has a cursor form (`RowIter::next_ref`, `scan_bounds`), errors are `#[repr(u32)]` codes, and merge operators/codecs/filters are identified by name or number in the file. The generic conveniences (`scan(range)`, `families(iter)`, `impl AsRef<Path>`) all have non-generic equivalents.

## Data flows

### Write path
1. `Table::mutate(row)...commit()` builds an engine `WriteBatch`: each mutation is encoded once by `format::wal::BatchBuilder` (family names resolved to ids from the cached `TableInfo`).
2. `Engine::submit` loads the current `TabletMap` (lock-free) and routes rows. **One shard:** submit `ShardMsg::Commit` through `runtime::Submitter` (or run inline if the caller is that shard's thread). **Several shards:** two-phase commit (below).
3. The shard loop drains its queue (`ShardHandler::handle` per message) and then, in `end_batch`, runs the group: `shm.publish_pending` lower bound, `shm.reserve_seqnos(n)` once, `wal.append` per commit (not for `Durability::None`), `wal.write()` once, `wal.sync()` once if any member asked for `GroupSync` (a `Sync` commit gets its own sync immediately, never batched).
4. Apply: `Memtable::insert` per cell with the encoded internal key (`encode_key` with the commit seqno and timestamp); freeze and swap in a new memtable when `allocated_bytes` crosses the threshold (publishing a new view).
5. Publish: `shm.publish_pending` (next pending or `u64::MAX`), so snapshots include the group; resolve each committer's `Notifier` with `CommitInfo { seqno, durability }`. Commits become visible only after their group's WAL write meets the strongest level any member requested (D19).
6. **Two-phase commit:** the coordinator (the shard owning the first row) reserves one seqno, holds it pending, and sends each participant its share. Each participant appends a `Prepare` (with OCC validation for transactions), meets the durability, and acks. The coordinator appends `Commit` to its own stream, meets the durability, tells participants to apply, and releases the pending seqno once all have applied.
7. Write stalls: a per-shard token bucket on L0 depth (from `CompactionPicker::score`) delays step 3.

### Read path (on the caller's thread)
1. `Engine::snapshot()` = `shm.visible_seqno()` + the current `Arc<View>` (in a reader process: `ReaderSlot::pin` then `read_view`, plus a manifest reload when `manifest_version` changed).
2. `get`: route `(table, row)` in the view's tablet map; for the family, build sources newest first: active then frozen `MemtableReader::iter`, then L0 SSTs newest first, then one SST per deeper level whose key range covers the column. Skip SSTs whose column filter misses both the column key and the row's marker key (FORMAT §6).
3. Seek every source to `encode_seek_key(row, qual, u64::MAX, snapshot)`; merge with `MergingCursor<Source>` (engine enum `Source { Mem(MemIter), Sst(SstIter) }`); resolve with `CellResolver` (`versions = 1`).
4. Return `CellData` pinning the value where it lies: an SST block (`SstIter::value_cell`), a memtable (pin the view), or the resolver's merge buffer (owned). Blob pointers are resolved through `BlobReader`.
5. Scans: `ScanCursor` walks tablets in key order; per family it runs the same merge + resolve with the `ScanFilter` pushed into each `SstIter` and `skip_row` for `columns_per_row` / latest-only.

### Flush
1. A frozen memtable stays in every new view until its SST is in the manifest.
2. A flush `Task` on the owning shard: `pager.allocate`, `SstWriter::new` over `MemtableReader::iter`, cutting a new SST when `fits` is false, `finish -> SstMeta`.
3. Send edits `AddSst` + `SetFlushed { seqno: max flushed }` (and a `WalCheckpoint` when the stream's minimum unflushed position advances) to the manifest task on shard 0.
4. The manifest task batches edits from all shards, writes one manifest block into a new extent, `pager.commit_root`, `shm.set_manifest_version`, and publishes a new view without the frozen memtable.
5. `Memtable::retire`; `ShardArena::reclaim` once no in-process snapshot and no reader slot pins a view listing it. `wal.checkpoint(lsn)` recycles segments below the checkpoint.

### Compaction
1. After each manifest commit (and periodically), each shard scores its `(tablet, family)` `Levels` with `CompactionPicker::score` and runs `pick` for the highest.
2. `CompactionJob::new(task, input readers, JobContext { gc: GcPolicy { snapshots: live snapshot seqnos incl. reader slots, now, bottommost } })` runs as a cooperative `Task` (or on `compaction_threads`), calling `run(deadline)` per time slice.
3. `finish -> CompactionOutput { added, removed, blob_live_delta }` becomes `AddSst`/`RemoveSst`/`PutBlobFile` edits through the manifest task.
4. After the commit at version `v`, each removed SST's extent is `pager.retire(extent, v)` (only when no tablet references it); `pager.reclaim(oldest_live)` frees extents whose views have all been released; `cache.erase_file`.

### Recovery (open as writer)
1. `vfs.open(path)`, `Presence::acquire`, `WriterLock::acquire` (else `WriterLocked`), `file.is_local()` (else `NetworkFilesystem`).
2. `Pager::open`: pick the valid newest superblock. Read the manifest chain from `root()` back to the last snapshot (`format::manifest::decode_block`), apply edits to build the catalog, tablets, `Levels` and per-stream checkpoints. Check every family's merge operator is registered (else `UnknownMergeOperator`, unless `allow_unregistered_merge`).
3. `OpenedPager::finish(live extents: manifest chain, SSTs, blob extents)`.
4. `ShmRegion::open(.., Role::Writer, ..)`: rebuild under a new generation (readers remap); `next_seqno` = `Counters.seqno_ceiling`.
5. For every stream from `wal::discover_streams` (not just `0..shards`): `Recovery::open(checkpoint)`, `next_record` until the end. `Batch` records apply to memtables if `seqno > SetFlushed` for that `(tablet, family)`; `Prepare` records are stashed; `Commit` decisions are collected. Then apply each stashed Prepare whose coordinator holds its Commit; discard the rest. Raise `next_seqno` above every replayed seqno.
6. `Recovery::into_stream` truncates torn tails. If the shard count changed, flush the recovered memtables and checkpoint before removing streams `>= shards` (D20).
7. Assign tablets to shards, publish the first view, start the runtime.

### Reader process (Phase 4)
`Presence::acquire`, `ShmRegion::open(.., Role::Reader, ..)` (refuses a layout mismatch), `claim_reader_slot`, load the manifest named by `manifest_version` through its own `Pager::open` (read-only) and `BlockCache`; memtables via `MemtableReader::open(arena, root)` from `read_view`. On a generation change, remap and re-pin.

## External dependencies

All MIT/Apache-2.0 compatible (D6); none is a storage engine (spec: Pigeonhole owns its full engine).

| Crate | License | Used by | Why |
|---|---|---|---|
| `crc32c` | Apache-2.0/MIT | format | WAL CRC32C with hardware acceleration and a safe fallback; the spec mandates CRC32C. |
| `twox-hash` (`xxhash3_64` only) | MIT | format | xxh3-64 block, footer, manifest and filter hashing; the spec mandates xxh3. Chosen over `xxhash-rust`, which is BSL-1.0 and outside D6's license list. |
| `lz4_flex` (safe encode/decode, no frame) | MIT | format | Default block codec, pure safe Rust. |
| `libc` (unix) | MIT/Apache-2.0 | io | `pread`/`pwrite`, OFD and `fcntl` locks, `shm_open`/`mmap`, `mbind`, affinity, process liveness. |
| `windows-sys` (windows) | MIT/Apache-2.0 | io | `LockFileEx`, file mappings, `FlushFileBuffers`, affinity on Windows. |
| `arc-swap` | MIT/Apache-2.0 | engine | Lock-free publication of the current view and tablet map (`Arc` swap without a reader lock). |
| `proptest` (dev) | MIT/Apache-2.0 | declared | Property tests (format order, round trips, model checks). Crates add it as a dev-dependency when they write those tests. |
| `loom` (dev, `cfg(loom)`) | MIT | memtable, cache | Concurrency model checking required by their briefs. |
| `criterion` (dev) | MIT/Apache-2.0 | declared | Benchmarks for every path with a latency target. |

zstd (Phase 2), io_uring (Phase 3), `futures-core` (Phase 3 async) and the bench comparison runners are not added yet.
