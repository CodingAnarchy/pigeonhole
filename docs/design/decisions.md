# Decisions log

Project-level decisions that refine or deviate from [spec.md](spec.md) and [task-briefs.md](task-briefs.md). Newest last. An entry here wins over those files. Every entry is numbered and approved. Agents: when the spec is silent or contradictory, record the question and your interim behavior in `docs/design/questions/<crate>.md` instead of guessing (see [questions/README.md](questions/README.md)); the coordinator turns answers into numbered decisions here and tracks deferred work as GitHub issues ([status](../status.md#tracked-follow-ups)).

## D1 — `pigeonhole-sim` does not depend on `pigeonhole` (approved)
The spec lists `pigeonhole` as a dependency of `sim` ("drives the public API from above"). That creates a cycle the moment `engine` or `pigeonhole` use `sim` in tests. Instead `sim` depends only on `io` and `format`; the full-stack simulation suites live in `crates/pigeonhole/tests/` and `crates/engine/tests/`, which take `pigeonhole-sim` as a dev-dependency. Same coverage, strictly downward graph.

## D2 — the simulated VFS lives in `pigeonhole-io` (approved)
Per the io brief, `SimVfs` (fault injection, deterministic from a seed) is an `io` backend at `pigeonhole_io::sim`. `pigeonhole-sim` builds the scheduler, crash points and reference model on top of it.

## D3 — writer lock is a byte-range lock on the lock page (approved)
"Multi-process readers" mentions `flock`; "Files and locks" specifies byte-range locks on a reserved lock page (OFD on Linux, `fcntl` with a per-process registry on macOS/BSD, `LockFileEx` on Windows). The more specific section wins.

## D4 — interface-freeze gate (approved)
The spec gates the interface freeze on owner review. The owner directed autonomous progress, so the coordinator reviews and approves interfaces, records the approval here, and the owner may revisit at any time through an interface-change request.

**Approved 2026-10-05:** the interface freeze (PR #1), including D7–D24 as revised after review.

**Approved 2026-10-06 (decisions audit, owner decisions U1–U4):** D25–D61. Every entry in this log is approved; the Open questions section is empty.

## D5 — reference hardware (approved)
No enterprise-NVMe Linux box with power-loss protection is attached to this project yet. Benchmarks run on available hardware (developer macOS arm64 and GitHub Linux runners), are reported in every run, and are labeled as non-reference. Performance gates are evaluated against those numbers until reference hardware is available.

## D6 — dependency policy (approved)
Allowed licenses: MIT, Apache-2.0, BSD-2/3-Clause, ISC, Zlib, Unicode-3.0, CC0-1.0 (enforced by `deny.toml`). Engine crates keep dependencies minimal; each new dependency gets a one-line justification in the PR description.

## D7 — manifest is a snapshot block plus a delta log (approved; revised after review)
The spec calls the manifest "a small copy-on-write tree"; the format brief calls for "manifest edit records". The manifest is one immutable snapshot block (the whole state as edits) in its own extent, plus one 256 KiB delta-log extent holding consecutive delta blocks, one per manifest commit. The superblock names both and the log's live length. A commit appends its delta past the live end of the log (bytes no root references, so nothing live is overwritten and the superblock flip stays the only in-place write of live data) and then commits the root; the root commit's first sync covers the delta, so a commit costs two syncs and no new extent. When the log is full or outgrows the snapshot, the writer writes a new snapshot and an empty log. Open always reads exactly three things (superblocks, snapshot, live log), keeping it well under the 5 ms target, and readers catch up by reading only new log bytes. The first proposal (one extent and fsync per delta, chain of up to 64) failed the open-time budget. Root commits run off the shard foreground loop (D30). Layout: FORMAT §9.

## D8 — the free-space bitmap is not persisted (approved)
The pager keeps the bitmap in memory and rebuilds it at open from the live extents the manifest names (the manifest snapshot and delta log, SSTs, blob extents). The engine reads the manifest anyway at open, so this costs nothing extra, a crash can never leak an extent, and no persisted bitmap can disagree with the manifest.

## D9 — family-in-row deletes use a marker key; BigTable delete rule (approved; revised after review; amended by D74 and D78)
A family-in-row delete is the key `[row][00 01][00 00][!ts][!seqno][FamilyDelete]`: `00 00` never appears in an escaped string and sorts before every qualifier, so a reader sees a row's markers before its cells. **Delete rule:** a `ColumnDelete` or `FamilyDelete` with timestamp `T` hides every version in its scope with timestamp `<= T` regardless of seqno (so a later put with an older timestamp stays hidden); a `CellDelete` hides exactly the versions at its timestamp, also regardless of seqno (D38); seqnos decide only snapshot visibility. Point gets use `CellResolver::seek_column`, which seeks each source to the row's marker prefix before the column (one extra seek per source, normally inside an already-loaded block); filters add a marker key so a column-filter miss never hides a marker.

## D10 — a whole-row delete is one family marker per family (approved)
Each family is its own tree, so there is no single place for a row tombstone. `delete_row` writes a `FamilyDelete` marker into every family of the table in the same atomic commit. Families added later cannot hold older data for that row, so nothing is missed.

## D11 — timestamps are microseconds; default timestamps never go backwards (approved; revised after review)
Timestamps are microseconds since the Unix epoch. Each tablet keeps a floor: the largest default timestamp assigned to it. The default timestamp is `max(now_micros, floor + 1)`. The floor travels with a tablet when it moves between shards, is persisted as `ts_floor` in the manifest's `Counters` edit (the maximum over tablets at each manifest commit), and at open starts from `max(ts_floor, largest commit_ts replayed from the WAL)`, so a clock step back or a restart never reorders default timestamps. User-supplied timestamps are taken as microseconds for TTL.

## D12 — `WriteBatch::commit()` uses the writer default (approved)
The API-surface example shows `wb.commit(Durability::GroupSync)`, the Durability section shows `wb.commit()` plus `wb.commit_with(d)`. The more specific Durability section wins: `commit()` uses the writer default, `commit_with(d)` overrides it.

## D13 — family ids are unique per database; SSTs belong to a (tablet, family) (approved)
A `FamilyId` is never shared across tables, so `(TabletId, FamilyId)` names one LSM tree with one memtable set, one level set and one flushed seqno. After a split both children may reference the parent's SSTs until compaction rewrites them; an extent is retired only when no tablet references it.

## D14 — extra downward dependencies (approved)
`memtable` depends on `io` (the brief lists only `format`) because its arena is an `io::SharedRegion`; without it, `engine` (which forbids `unsafe`) could not hand shared memory to the memtable. `compaction` depends on `cache` and `io` (the brief lists `sst`, `pager`, `format`) because opening input SSTs takes a `BlockCache` and a `FileRef`. Both point down, so the layer rule holds.

## D15 — shared vocabulary types live in `format` (approved)
Ids, `Seqno`, `Timestamp`, `Lsn`, `Durability` and the `Cursor` trait are used by nearly every crate; `format` is the only crate all of them depend on, so they live there rather than being duplicated.

## D16 — value size limits (approved; revised after review)
The blob pointer stores the length as a `u32`, so the ceiling is `2^32 - 1` bytes, one short of the spec's 4 GiB. Until blob separation lands, Phase 1 rejects at write time, with `ValueTooLarge`, any value larger than `min(WAL segment payload, 64 MiB max extent, half the shard's memtable arena)`. Phase 2 blob separation lifts this: blob pointers address a logical blob file whose payload spans many extents, so a value can exceed one extent with no format change.

## D17 — the `async` feature is off by default until Phase 3 (approved)
The spec makes `async` a default-on feature. In Phase 1 it gates only an empty placeholder module (`pigeonhole::nonblocking`), so it is declared but not default; it becomes default-on when the async API is implemented.

## D18 — filters are cache-line-blocked bloom filters in Phase 1 (approved)
The spec allows "a ribbon (or blocked bloom) filter". Blocked bloom is simpler and fast enough for Phase 1; the filter block's kind byte leaves room for ribbon later without a format break.

## D19 — read-your-writes: commits return once visible (approved; revised after review)
A shard publishes a group's seqnos only after the group's WAL write meets the strongest level any member requested, and a commit returns only once it is durable at its level **and** `visible_seqno >= seqno`, so a caller always reads its own write. Idle shards publish `pending = u64::MAX` and never hold back the watermark. Cost: a cross-shard commit becomes visible only after every participant applies, so its latency includes the slowest participant's group, and any shard's in-flight group briefly delays visibility for all. A reader can still observe a `Buffered` or `None` commit that a later power loss removes; that is inherent in those levels.

## D20 — a changed shard count flushes recovered data before dropping streams (approved)
The spec replays every WAL stream at open regardless of shard count. If streams exist beyond the new shard count, the engine flushes the memtables recovered from them and checkpoints before removing those stream files. Open stays fast in the common case (same shard count).

## D21 — lock page byte assignments (approved)
Refines D3. On page 2: offset 8192 is the writer byte, 8193 the presence byte, 8194 an shm-init byte held exclusive while a process creates, validates or rebuilds the shared-memory region (so two processes opening at once never both build it). Locks never block; callers retry.

## D22 — filter pushdown semantics, uniform across sources (approved; revised after review; amended by D82)
`format::scan::ScanFilter` holds the entry-safe conditions (qualifier selection, time range on puts) and one rule, `ScanFilter::admits`. **Amended in the audit:** the qualifier selection applies to every cell entry, deletes and merge operands included (nothing of an excluded column is returned, so its deletes and operands cannot change a result, and `next_admissible` can seek past excluded columns); the time range applies to puts only, so deletes and merge operands always pass it (hiding a delete would resurrect older versions, and dropping some operands would produce partial counters); family markers always pass. `admits` and `next_admissible` agree, so pushdown still equals filter-after. `SstIter` applies it inside the block decoder; memtable sources are wrapped in `compaction::FilteredCursor`, so every source filters identically. Version count, columns per row and value predicates need snapshot visibility and run in `CellResolver`, still before materialization; a value predicate tests the newest visible value of a column. The sst acceptance test (pushdown equals filter-after) is defined over these semantics.

## D23 — one shared-memory view record carries the tablet map and memtables (approved)
The spec lists "the current tablet map" and published views separately. A view already includes the tablet map, so the region holds one double-buffered view record (tablet map, memtable roots, manifest version) published by a single pointer swap; a reader can never pair a tablet map with the wrong memtables.

## D24 — WAL checkpoints never strand a prepared commit (approved)
A participant's PREPARE is applied at recovery only if the coordinator's stream still holds the COMMIT. So a stream's checkpoint may not pass a COMMIT record until every participant's share of that commit is flushed (its `SetFlushed` covers the seqno). The engine computes checkpoints with this rule.

## D25 — WAL segments are chained; recovery never appends to a torn segment (approved)
Each segment header records its predecessor's epoch and the offset where the predecessor's data ends. A writer syncs a full segment before writing its successor's header, and after recovery starts a new segment (epoch above every epoch in any header) chained to where replay ended, never appending to the torn one. Replay distinguishes end of segment (a successor names this exact stop offset) from end of log (no successor), and treats a successor naming a different offset as corruption. This closes the hole where stale-but-valid-looking data past a torn tail, or a later segment, could be resurrected. FORMAT §10.

## D26 — a cross-shard commit's id is its seqno (approved)
The coordinator reserves one seqno per cross-shard commit; PREPARE and COMMIT records carry it and no separate commit id exists. Recovery raises `next_seqno` above every seqno in every replayed record, including discarded PREPAREs, so an id is never reused. A coordinator keeps each such seqno in its `held` set, and every `pending` watermark it publishes is `min(held, ..)`, so no snapshot sees half of the commit (FORMAT §11.3).

## D27 — a shared-memory directory plus generation-named regions (approved)
A one-page directory region with a fixed, never-changing layout records the current generation; the region itself is named with its generation. A writer builds a new generation, marks the old region abandoned, then updates the directory. Readers detect staleness with two atomic loads and re-attach. Changing the name per generation also avoids Windows reusing a named mapping still held by an old process.

## D28 — an oversized view is refused, never truncated (approved)
View buffers default to 4 MiB (configurable). If an encoded view would not fit, `publish_view` fails with `ViewTooLarge` and the writer refuses the change that grew it (typically a split), keeping the old view; this surfaces in metrics instead of corrupting readers.

## D29 — small memtable values are copied; one-shot gets avoid view refcounts (approved)
`CellData` copies memtable values of at most 128 bytes (and merge results and blob reads); larger memtable values are pinned by `ArenaSlice` plus an `Arc<View>`. `Engine::get_latest` loads the view through an `arc-swap` guard, so a hot small point get touches no shared reference count. The threshold is to be confirmed by benchmark once the engine exists ([#15](https://github.com/CodingAnarchy/pigeonhole/issues/15)).

**Measured (#15, Apple M5, memtable-resident, non-reference):** `get_latest` 263–303 ns and `get` with a snapshot 272–309 ns across 16 B–4 KiB values; a miss 194 ns; 10 cores reading one cell 30 ns per get aggregate. The 128-byte threshold and the guard are within noise of the alternatives (two skiplist seeks dominate); kept as written.

## D30 — no fsync on a shard's foreground loop (approved)
WAL group syncs use `Wal::submit_sync` and root commits use `Pager::submit_commit_root`; both return `io::Completion`s served by the I/O backend (the pread pool now, io_uring in Phase 3). Shards keep draining queues and building the next group while syncs run; the manifest task on shard 0 is a background task that waits on its completion.

**One recorded exception (audit, K5/K19).** FORMAT §10.1 rule 1 requires a full WAL segment to be durable before its successor's header is written. When a recyclable or prepared spare slot is ready, `WalStream`'s rollover submits that sync too, and the next write waits for it before writing the successor's header (normally it has already completed). A rollover syncs inline, on the shard thread, **only when no prepared spare is ready** (it then takes a blank slot or grows the file inline). Both are counted in metrics (`WalStream::inline_rollover_syncs`, `WalStream::inline_grows`) and stay at zero when spares are prepared in time (D35). Making rollover fully off-thread is Phase 3 work ([#19](https://github.com/CodingAnarchy/pigeonhole/issues/19)).

## D31 — merge operators are associative folds (approved)
`MergeOperator` is `merge(acc, older)` plus `finish(base, acc)`. The resolver streams operands newest first into one accumulator, with no buffered copies; compaction without a base keeps the accumulator as one combined operand. Non-associative operators are not supported.

## D32 — cursors own what they read (approved)
`BlockIter<B: Deref<Target = [u8]>>` owns its byte owner (a `BlockHandle` in practice), `SstIter` owns an `Arc<SstReader>`, and `MemIter` owns a `MemtableReader` clone. Scan cursors and compaction jobs can then store their sources without self-references or `unsafe`, and values stay zero-copy.

## D33 — configuration structs are non-exhaustive (approved)
Option and config structs (`EngineOptions`, `ShmConfig`, `WalOptions`, `RuntimeConfig`, `SstWriterOptions`, `ReadOptions`, `ScanFilter`, `ResolveOptions`, `PickerOptions`, `GcPolicy`, `JobContext`, `ReadSpec`, `ScanSpec`, `FaultPlan`, `OpenOptions`, `WorkloadSpec`) are `#[non_exhaustive]` with a constructor or `Default`, so adding a field is not a breaking change. `EngineOptions.embedding` was dropped: the open function chooses the mode.

## D34 — a commit holds one entry per (column, timestamp); last write wins (approved)
Within one commit, multiple mutations to the same `(table, row, family, qualifier, timestamp)` collapse to the last one written, at batch-build time (`format`'s `BatchBuilder` or the engine's `WriteBatch` enforces it), because internal keys order entries by `[!ts][!seqno]` and kind only, so write order inside a commit cannot be expressed. `pigeonhole-sim`'s `Model` matches. A family or row delete marker at timestamp `T` in the same commit as a put with timestamp `<= T` hides that put (the marker rule is ts-based, regardless of seqno).

## D35 — WAL spare segments are zero-filled off the shard thread (approved)
A freshly allocated slot (`fallocate`) still costs a metadata update at its first fdatasync, so the spec's "fdatasync on preallocated blocks, no metadata update" needs slots that were zero-filled and synced before use. `WalStream` never does that on the shard thread: `SpareSegments::prepare(n)` (a `Send + Sync` handle from `Wal::spares()`) allocates, zero-fills and syncs spare slots on a background task the engine runs; a rollover takes a recyclable slot first, then a prepared one, and only grows the file inline when it has neither, counted in `WalStream::inline_grows()` for metrics. `create` and `into_stream` zero-fill the one slot they start in at open. A failed write or sync poisons the stream (`Error::Poisoned`) until it is reopened through `Recovery`, so a later successful sync can never acknowledge commits behind a hole.

## D36 — reader processes open the database file read-write (approved; owner decision U1)
POSIX `fcntl` (and Linux OFD) locks refuse a write lock on a descriptor opened read-only; Windows `LockFileEx` does not. For backend parity `pigeonhole-io` refuses `LockMode::Exclusive` on a read-only handle with `ErrorKind::Unsupported` on every platform and in `SimVfs`. The shm-init byte and the presence-byte "last one out" upgrade (D21) are exclusive, so **every process, readers included, opens the `.phdb` file read-write**; a reader writes nothing to it. This is the same requirement SQLite has in WAL mode: reader processes need write permission on the file, and a database on read-only media cannot be opened by readers. The spec's "`open_reader` opens a read-only handle" means the API handle (no write methods), not the OS file mode. Documented on `Pigeonhole::open_reader`, `Engine::open_reader`, in the guide's concepts page and in the agent reference.

## D37 — Windows lock upgrades are not atomic; the writer opens shared memory before taking `Presence` (approved; audit C1, K10)
`LockFileEx` cannot convert a held shared lock, so `PreadVfs` unlocks, tries exclusive, and re-takes shared on failure; if another process takes the byte exclusively in that window, the shared lock is lost (the call still reports `Locked`). That is accepted in `pigeonhole-io`. **No caller may upgrade a lock it must keep.** The only such caller was shm's layout-version probe (D45), which upgraded the writer's held presence lock. The writer's order is therefore `WriterLock::acquire` → `Pager::open` and manifest → `ShmRegion::open(Role::Writer)` → `Presence::acquire`, on one handle. `ShmRegion::open` probes presence from no lock and then takes it shared **before** publishing the new generation, so a closing process can never remove a generation being built; the later `Presence::acquire` only returns the guard. Because the writer is not present between taking the writer byte and opening shared memory, last-one-out cleanup (removing WAL files and the region) must also take the writer byte, which a writer holds while it opens. Engine work: [#20](https://github.com/CodingAnarchy/pigeonhole/issues/20).

## D38 — a cell delete is timestamp-only (approved; owner decision U2; purge behavior in D74)
A `CellDelete` at timestamp `T` hides every version at exactly `T` **whatever its seqno**, including a put or merge operand at `T` committed after the delete, uniform with the column and family rule of D9 (and with HBase). Seqnos only decide what a snapshot sees: a snapshot taken before the delete still sees the version. A cell cannot be rewritten at the same timestamp while the marker exists; write the replacement at another timestamp. Compaction drops the marker only at the bottommost level together with everything at `T` it covers. FORMAT §2, the sim `Model` (ICR 0003) and the guide state this. Resolver and GC work: [#25](https://github.com/CodingAnarchy/pigeonhole/issues/25).

## D39 — a row read returns families in creation order, or in the caller's order (approved; owner decision U3)
Within a row, cells come by family, then qualifier, then newest version first. Families come in creation order (`FamilyId` order, which the engine iterates anyway), or, when the read lists families, in the listed order (a family listed twice appears once). The sim `Model` and its test adapters follow this (ICR 0003); the guide states it. Engine work: [#26](https://github.com/CodingAnarchy/pigeonhole/issues/26).

## D40 — `compaction_cores(k)` is refused in application-owned mode (approved; owner decision U4)
Application-owned mode starts no threads (spec "Threading"), so `compaction_cores(k)` with `k > 0` together with `open_application_owned` fails at open with `InvalidArgument`, before anything is opened, instead of being silently ignored. `pin_threads` does not apply in that mode and is ignored. `pigeonhole-runtime` enforces its half now (`Runtime::application_owned` returns `Error::InvalidConfig`); the option's rustdoc and the guide document it. Engine enforcement: [#22](https://github.com/CodingAnarchy/pigeonhole/issues/22).

## D41 — merge folding across timestamps; a non-`i64` base fails (approved; audit K14, K15, C2; amended by D96)
`Incr` operands carry the commit timestamp, while a base put may carry an older or explicit one. Walking a column newest first, a run of operands folds into one version at the newest operand's timestamp, consuming the next older put as its base, with wrapping addition; deletes and TTL apply to entries before folding and `max_versions` after. So an expired base is dropped before folding and the counter restarts from the operands, which is accepted. A base whose value is not an 8-byte `i64` makes the read fail with `MergeFailed`, as the guide promises (never silently 0). A put no operand folds onto is returned as written. `Incr` on a family without the `i64` operator is rejected (`ModelError::NoMergeOperator` in the model; the engine maps its typed error to the same case). The sim model implements this (ICR 0003). Compaction's `I64Add`: [#21](https://github.com/CodingAnarchy/pigeonhole/issues/21).

## D42 — the reference model's crash windows, per WAL stream (approved; audit K11; amended by D84)
After a power loss the model promises every commit up to the last `GroupSync`/`Sync` one; after a process crash, every commit up to the last `Buffered`-or-stronger one. Survivors are always a prefix (the spec's "only a suffix is lost"), so `None` commits before a stronger one are durable too, as the spec's durability section states. A commit in flight at a crash is registered with `Durability::None`. `Model::recover` truncates to the survivors and, after power loss, marks them durable.

## D43 — WAL segments are at most 4 GiB − 32 KiB (approved; audit K13, C6)
FORMAT §10.1 stores `prev_end` as a `u32`, so a record ending exactly at the end of a 4 GiB segment could not be named. `pigeonhole-wal` refuses a `segment_size` above `4 GiB − 32 KiB` (one frame less) with `InvalidArgument`; `SegmentHeader::decode` still accepts up to 4 GiB, which is harmless. FORMAT §10.1 and `WalOptions::segment_size` state the cap.

## D44 — shared-memory region names are at most 30 bytes (approved; audit K16, C7)
`phdb-<16 hex>-<generation hex>` must fit macOS's `PSHMNAMLEN` of 31 bytes, which counts the leading `/` that `shm_open` needs, so names are at most 30 bytes (generations below 2^32). Generations grow by one per writer open, so this is unreachable in practice; the shm crate checks the length and fails clearly. FORMAT §11 states the limit.

## D45 — "no other process attached" for a layout-version rebuild is decided by the presence byte (approved; audit K17)
A writer that finds a live region with another layout version cannot read that region's slot table, so it probes the presence byte: an exclusive lock through its own handle succeeds only if no other process has the database open. The byte is then left shared (the writer is present). Under D37 the writer holds no presence lock when it probes, so no held lock is ever upgraded. No ICR was needed.

## D46 — only `active` reader slots are reclaimed (approved; audit K18)
A slot in the transient `claiming` state has no trustworthy pid yet (the previous owner's may still be there), so a process that dies between its CAS and its `active` store leaks one slot until the next writer generation clears the table. Reclaiming `claiming` slots by a stale pid could free a slot under a live claimant.

## D47 — the writer removes the old generation's name after switching (approved; audit K20)
`open(Role::Writer)` removes the old region's name right after the directory names the new generation; mappings other processes still hold stay valid and they re-attach by the new name. Without this every writer restart would leak a `/dev/shm` object (or a file in `shm_dir`) until the last process's `ShmRegion::remove`, which only knows the current generation.

## D48 — `read_view` before the first publish returns an empty view 0 (approved; audit K21)
A reader that attaches before the writer's first `publish_view` sees an empty `ViewRecord` with `view_version` 0 and the current manifest version ("no tablets"), not an error. Pinning view 0 means "no view", which is harmless because nothing is reclaimable yet.

## D49 — runtime shutdown, pinning and handler counts (approved; audit K22)
`Runtime::shutdown` handles every queued message but drops unfinished background tasks, so the engine must finish or persist flush and manifest work before calling it ([#16](https://github.com/CodingAnarchy/pigeonhole/issues/16)). Pinning is best-effort where the OS has no affinity control (`Unsupported` is ignored); any other pin failure fails `Runtime::start` with `Error::Spawn`. A handler count that differs from the shard count is a programming error and panics.

## D50 — pinned cache blocks may hold a shard over capacity; `erase_file` drops unpinned blocks only (approved; audit K23)
`pigeonhole-cache` lets a shard run over capacity while everything evictable is pinned and shrinks it on that shard's next insert (no hook on handle drop, so the hit path stays a refcount decrement). `erase_file` drops only unpinned blocks; pinned ones age out normally (file ids are never reused). The 10-thread hit cost (about 366 ns/op under a mutex per shard) is Phase 3 tuning ([#18](https://github.com/CodingAnarchy/pigeonhole/issues/18)).

## D51 — a checkpoint may name a WAL segment that never reached the disk (approved; audit K24)
After a power loss the manifest can hold `Lsn(e, X)` while no segment with epoch `e` exists (a `Buffered` commit opened segment `e`, was flushed and checkpointed, and `e` was never synced). `Recovery` treats the missing segment as one that stopped exactly at `X`: a successor chained to `(e, X)` continues the log, otherwise the log ends at the checkpoint, and a header with an epoch above `e` that nothing reaches is corruption. `into_stream` always picks an epoch above the recovered end's epoch. A file with no valid header and a nonzero checkpoint is reported as corruption; `create` syncs the first header, so that case cannot arise.

## D52 — memtable accounting and misuse (approved; audit K25)
`allocated_bytes` counts arena chunk bytes (not entry bytes); an insert into a frozen memtable is a programming error and panics; a malformed key read back from an arena is `Corrupt`. A 1M-entry lookup measured about 490 ns against the 300 ns target; that is accepted for cache-resident sizes and tracked as Phase 3 tuning ([#17](https://github.com/CodingAnarchy/pigeonhole/issues/17)).

## D53 — `Vfs` edge cases (approved; audit K26)
`allocate` extends the file length; `create` implies `write`; on Windows `remove_shared` without a directory is a no-op (named mappings vanish with their last handle); a zero-length shared region is refused; a zero-length `allocate` is a no-op.

## D54 — `format` keeps a minimal decoder surface (approved; audit K27)
`FrameDecoder` has no position accessor (the WAL computes positions itself); one is added by ICR only if a caller needs it. `FamilyOptions::default` is the spec's defaults. The `format::Error::InvalidArgument` variant of ICR 0001 maps to the engine's `InvalidArgument` (code 19) once the engine's conversion exists ([#14](https://github.com/CodingAnarchy/pigeonhole/issues/14)).

## D55 — `sync_data` does not make a length change durable (approved; audit C3)
`File::sync_data` (fdatasync) makes written bytes durable but callers may not rely on it for a size change; after `set_len`, `allocate` or a write past the end, `sync_all` is needed before the new length must survive a power loss. `SimVfs` models exactly that: a power loss reverts the length to the last `sync_all`'s (data synced past it is lost; an unsynced shrink reads back as zeros), unless a fault plan is active, in which case the pending length may survive. The stricter model found one real bug: `Pager::allocate` grew the file and root commits synced with `sync_data`, so a power loss after a commit could cut the file short of a published extent. The pager now calls `sync_all` once per file growth. The WAL already did (`allocate` + `sync_all` for every new slot). A second gap (#72): a process killed between the growth and its `sync_all` leaves the longer length in the page cache only, and the restarted writer sized its allocator (pager) or slot grid (WAL) from it, then published extents or acknowledged records there under `sync_data` alone. A writable `OpenedPager::finish` and `Recovery::open` now `sync_all` the file before reading its length.

## D56 — shared memory is little-endian only (approved; audit C4, C5)
The memtable arena and the shm region are accessed as native-endian atomics while FORMAT.md fixes integers as little-endian, so `pigeonhole-memtable` and `pigeonhole-shm` refuse to build on big-endian targets (`compile_error!`). Every process mapping a region runs on one host, so this loses nothing on supported platforms. FORMAT §11.6 lists `min_seqno` as atomic, as implemented.

## D57 — the pager's clean-close flag outlives the open that read it (approved; pager question)
Bit 0 of the superblock flags is set by `mark_clean` and cleared only by the next root commit, so a writer that opens a cleanly closed file, appends WAL records and crashes before any root commit leaves it set. The pager reports the flag as stored; the engine never skips WAL replay because of it (or commits a root right after opening if it ever does). Engine duty: [#23](https://github.com/CodingAnarchy/pigeonhole/issues/23).

## D58 — a failed root commit poisons the pager (approved; pager question)
Once a commit's first sync is issued, a failure leaves the on-disk root uncertain, and an fsync error may have dropped written pages, so retrying could publish a root over lost bytes. Every later `commit_root`, `submit_commit_root` and `mark_clean` fails; the engine surfaces the error and requires a reopen, which recovers to the last durable root ([#23](https://github.com/CodingAnarchy/pigeonhole/issues/23)).

## D59 — an interrupted `Pager::create` is refused, never deleted (approved; owner decision)
A crash inside `Pager::create` can leave a file with no valid superblock. Nothing was ever committed to it, and `Pager::open` fails with `Error::Format`. **Pigeonhole never deletes a file the user named.** When the engine's open (with or without `create_if_missing`) finds a file that exists, has no valid superblock and is at most 64 KiB long, it fails with `Corruption` and a message that names the file as an apparent interrupted create, which the application may delete before opening again. Engine work: [#24](https://github.com/CodingAnarchy/pigeonhole/issues/24).

## D60 — `shrink_plan` cannot tell published extents from in-flight output (approved; pager question)
The pager tracks allocated versus retired, not which extents the manifest names, so the plan lists every non-retired extent past the shrink point. The engine relocates only extents its manifest names (or shrinks with no flush or compaction output in flight). `relocate` fails with `NoSpace` when no free extent of that size lies below the one being moved ([#23](https://github.com/CodingAnarchy/pigeonhole/issues/23)).

## D61 — `reclaim` is clamped to the durable root (approved; pager question)
`reclaim(oldest_live)` frees nothing newer than the manifest version of the last *completed* root commit, because until the root that drops an extent is durable a crash recovers to a root that still references it; `truncate_tail` releases nothing while a commit is in flight. Callers may retire and reclaim right after submitting a commit; the extents are freed by a later `reclaim` once it completes ([#23](https://github.com/CodingAnarchy/pigeonhole/issues/23)).

## D62 — each phase gate includes an empty phase milestone (approved; owner)
Deferred work is a GitHub issue labeled with its crate and `phase-N`, and assigned to the matching milestone ("Phase 1 — Core engine" … "Phase 4 — Hardening and 1.0"). A phase's gate passes only when its measurable gate is met **and** its milestone has no open issues (`scripts/phase-gate.sh N`). Work found after a gate passes goes to a later milestone, never back into a closed one.

## D63 — `SstWriterOptions::created_micros` (approved; sst)
`SstWriter` has no clock, so the properties block's `created_micros` comes from an added `SstWriterOptions::created_micros` field (default 0; additive under D33). The engine sets it from `Vfs::now_micros` on flush and compaction, keeping simulated runs deterministic.

## D64 — block-cache namespaces for SSTs and blob files (approved; sst)
`pigeonhole_sst::sst_cache_file(SstId)` is the id with bit 63 clear and `blob_cache_file(BlobFileId)` sets bit 63; the engine passes the same values to `BlockCache::erase_files`. SST ids stay below 2^63 (`debug_assert`; stated in interfaces.md).

## D65 — sst error classification (approved; sst)
`Error::is_corruption()` covers truncated, bad-magic, checksum and corrupt errors; `Error::is_unsupported()` covers an unsupported format version or codec; `KeyTooLarge` is a caller error and in neither.

## D66 — what `SstReader::open` promises about an interrupted build (approved; sst)
The footer is written last in its own write. `open` checks footer, top index, filters and properties but does not checksum data blocks or index partitions (that would read the whole SST); a block lost to reordered unsynced writes fails its checksum when read, as a corruption error, never wrong data. Safe because nothing references an SST until its manifest commit is durable and the root commit syncs the SST first.

## D67 — one block decoder (approved; sst, ICR 0004)
SST data and index blocks are decoded only by `format::block::BlockIter` (O(1) `Block::new`, `Block::validate` for verify/fuzz, `BlockIter::reset`, inline key buffer). No crate keeps a private block decoder.

## D68 — index partitions and readahead (approved; sst)
Index partitions target `min(block_size, 4 KiB)`. `ReadOptions::readahead_blocks` reads up to that many adjacent uncached data blocks in one read during forward movement (never on seeks), within the current index partition; with `fill_cache = false` read-ahead blocks are held by the cursor and dropped on every seek.

## D69 — blob record caching and logical length (approved; sst)
`BlobReader` caches verified records at `Priority::Low`, except records larger than `min(1 MiB, cache capacity / 8)`, which are returned pinned but uncached. Each blob extent's header is verified the first time a read touches that extent. `BlobWriter::finish` returns the logical length (record headers plus values, without extent headers).

## D70 — `GcPolicy::min_ts_above` bounds bottommost purges (approved; compaction)
A bottommost compaction may drop a delete that is visible at every live snapshot, and versions beyond `max_versions`. Both are safe for the inputs, but data *above* the inputs (L0 files and levels not in the task, memtables) is newer by seqno and can still carry older user timestamps (written later with an explicit timestamp): a dropped column, family or cell delete would uncover such an entry, and an upper `CellDelete` at a kept version's timestamp would make a purged older version the newest. Either changes a read at a live snapshot, which done-when (2) forbids, and the random test finds it quickly.

**Decision:** an added public field `GcPolicy::min_ts_above: Timestamp` (additive; `GcPolicy` is `#[non_exhaustive]`): the smallest timestamp of any entry above the inputs in the task's range, `u64::MAX` when there is none. Bottommost delete purges apply only to deletes with `ts < min_ts_above`, and the `max_versions` purge only to columns whose newest timestamp is below it. `GcPolicy::new` sets it to 0, which purges nothing at the bottom (always safe). The engine computes it from the `ts_range` of upper SSTs overlapping the range and the minimum timestamp of each memtable (which the engine has to track on insert; `Memtable` exposes only `seqno_range`). With default timestamps (D11) everything above is newer than old tombstones, so purging works normally. Expired data, and entries hidden or shadowed within the inputs, are dropped at any level regardless.

## D71 — additive `JobContext` fields `target_sst_bytes` and `clock` (approved; compaction)
`CompactionJob::run(deadline_nanos)` must compare against the Vfs monotonic clock, but `JobContext` has no `Vfs`; and nothing tells the job the output SST size (`PickerOptions::target_sst_bytes` lives in the picker).

**Decision:** `JobContext::target_sst_bytes: u64` (default 64 MiB) and `JobContext::clock: Option<VfsRef>` (default `None`), both additive under D33. With a clock, `run` checks it every 64 units of work (a unit is a family marker or one `(column, timestamp)` group); without one, `run` does 4096 units per call, or everything when the deadline is `u64::MAX`. `SstWriterOptions::created_micros` is set from `GcPolicy::now`.

## D72 — other additive public API (approved; compaction)
None of these change a frozen signature:
- `ResolveOptions::time_range` and `ResolveOptions::route_time_range` (see the D22 amendment below).
- `MergingCursor::current()` and `sources()`, `FilteredCursor::inner()`: the engine pins a zero-copy value through the source the merged cursor is on (`ResolvedCell::from_source`).
- `CellResolver::set_upper_bound(Option<&[u8]>)` (a scan over deleted rows stops at its range end instead of running to the next visible cell) and `into_cursor()`.
- `VecCursor`: an in-memory sorted cursor, the mock source for tests and examples above.
- `ValuePredicate::matches`, `Levels::level_bytes`, `PickerOptions::level_target`, `KeyRange::{all, intersect}`, `CompactionPicker::options`, `CompactionJob::{entries_read, entries_written}`.
- `MergeRegistry`'s `Default` is now a manual impl equal to `new()`, so built-ins are always present as documented (the frozen stub derived it, which would have produced an empty registry).

## D73 — counter operands are not folded across timestamps in Phase 1 (approved; Phase 2 folding tracked in #34)
Every `incr` gets its own commit timestamp (D11), so a hot counter accumulates one operand per increment and every read folds them all. Folding a run across timestamps (or onto its base) in compaction is not read-preserving in general: a later `delete_cell` at one operand's timestamp, or a `put_at`/`delete_column` with an explicit timestamp inside the run, splits it in the model; TTL expires operands one by one; and a `time_range` scan sees operands but not a base outside its range (D22).

**Decision:** compaction combines operands only within one `(column, timestamp)` group and one snapshot stripe (preserving every read), never across timestamps and never onto a base. A bad base is therefore never folded (#21): the read keeps failing with `MergeFailed`. Proposal for the owner: fold a run (and its base) at the bottommost level when the family has no TTL and the run lies below `min_ts_above`, accepting that a later explicit-timestamp delete inside the run no longer splits it. **Deferred to Phase 2 (#34)** per review. Until then operands accumulate: the guide (data-modeling.md, counters) says so, and `cargo bench -p pigeonhole-compaction` measures it (`counter_get/operands_N`: about 0.3 µs for 1 operand, 3.4 µs for 100, 307 µs for 10,000, i.e. ~30 ns per operand).

## D74 — purges follow HBase semantics (approved; owner decision; amends D9 and D38)
Before compaction purges a delete marker, or versions beyond `max_versions`, a later write with an older explicit timestamp stays hidden: by the marker (D9, D38), or behind the newer versions. A bottommost compaction may purge them, but only below `GcPolicy::min_ts_above` and only when no live snapshot needs them. After that, such a write behaves as if they never existed: a `put_at` below a purged delete becomes visible, and a `delete_cell` of the newest version does not bring back a purged older one. Writes at default timestamps are never affected.

**Behavior:** as described. The oracle stays strict. `pigeonhole_sim::Model::purge(&ModelPurge)` (approved sim addition) removes exactly what such a compaction may purge from the model:
- deletes visible at every read point below `min_ts_above`, with what they cover and the deletes they make redundant, in the compaction's processing order;
- puts and operands outside the newest `max_versions` versions at every read point.

The compaction tests apply it after each bottommost compaction and then compare reads after unrestricted later writes (explicit older timestamps and cell deletes included). Coverage:
- `compaction_preserves_reads_at_live_snapshots`, which fails without the purge;
- `purges_match_the_model_purge_hook`, deterministic scenarios that pin the redundancy ordering;
- `purge_follows_hbase_semantics` in sim.

The guide (data-modeling.md, Versions) states the rule.

## D75 — `I64Add` accepts only `ValueTag::I64` values (approved; compaction)
The model treats any 8-byte value as an `i64` base (it has no tags). Stored values have tags: `put_i64` and `incr` write tag `0x01`.

**Decision:** operands and bases must be `ValueTag::I64` (tag plus 8 bytes); a `Bytes` value of 8 bytes is a `MergeError`. The engine's model adapter should map the model's 8-byte values to `put_i64` (the compaction tests do), and the guide already says to write counters only with `incr`/`put_i64`.

## D76 — `ResolveOptions::versions` and the family's `max_versions` (approved; compaction)
`ResolveOptions` has no `max_versions`, but the model caps every read at it, and reads must not depend on whether compaction has purged yet.

**Decision:** the caller passes `versions = min(requested, max_versions)` (0 meaning unlimited on either side); documented on the field.

## D77 — value predicates on typed and blob values (approved; compaction)
D22 says a value predicate tests the newest visible value of a column; the byte-level meaning is unstated.

**Decision:** the column is returned (all requested versions) iff its newest visible version matches. Byte predicates compare the payload (stored value without the tag); `I64` matches `i64` and varint values; a blob pointer matches no byte predicate (the resolver does not read blobs).

## D78 — rows split across SSTs of one level move together; point gets consult every overlapping SST of a level (approved; compaction; amends D9)
D9's point get seeks the row's markers and then the column in "one SST per deeper level". If a level splits a row across two SSTs, the marker and the column can be in different SSTs, and a GC that sees only part of a row could drop a family delete that still hides cells of the same row elsewhere in the level.

**Decision:** outputs are cut between rows once less than an eighth of the extent is left, so only a row larger than that is split. The picker takes inputs *and* the overlapping SSTs of the level below by row ranges, and expands both to a clean cut, to a fixpoint: an SST sharing an edge row with a chosen one comes along. A row's data in a level therefore always moves down together, and a bottommost run sees every SST of the output level holding its rows (regression test `a_row_split_across_bottom_ssts_moves_together`; the picker proptest generates shared edge rows and asserts clean cuts). The task's `range` stays `KeyRange::all()`, which covers the expansion. The engine's point get must consult every SST of a level whose range overlaps the row (normally one): D9's "one SST per deeper level" wording needs amending (coordinator).

## D79 — what the engine does with picker tasks (approved; compaction)
The picker does not know the tablet's row range or whether an SST is shared with a sibling after a split (D13).

**Decision:** `pick` returns `range = KeyRange::all()` and one subrange; the engine narrows `range`/`subranges` to the tablet range and turns a `TrivialMove` of a shared SST into a `Rewrite`. `TrivialMove` and `Drop` need no job (the engine has the `SstMeta`s); a job given one finishes at once with an empty output. Subranges run one after another inside one job; splitting large tasks and running subranges in parallel is deferred (#35). `CompactionJob::new` debug-asserts that `range` and every subrange bound is an encoded row prefix.

## D80 — blob accounting in Phase 1 (approved; compaction)
Value separation is Phase 2 (FORMAT §7), so Phase 1 writes no blob files.

**Decision:** `blob_live_delta` records `-(16 + len)` per dropped put holding a blob pointer (the record header plus the value, matching `BlobWriter`'s byte count). `new_blob_files` and `dropped_blob_files` stay empty: the job does not know a file's current live bytes, so the engine decides when a file reaches zero. A `BlobGc` task finishes empty.

## D81 — point gets copy small values (approved; compaction)
A delete can follow the put it hides inside one `(column, timestamp)` group (it was committed earlier), so the resolver must read the whole group before returning the put, and `Cursor` cannot look ahead without moving.

**Decision:** values up to 4 KiB are copied into a reused buffer while the group is read (`from_source == false`); a larger value is re-found with one seek and returned borrowed from the source (`from_source == true`). Nothing allocates per cell once the buffers have grown.

## D82 — time ranges on merge families apply to resolved versions (approved; coordinator; amends D22)
D22 pushes a scan's time range down to puts (operands and deletes always pass). For a family with a merge operator that can drop a counter's base while keeping its operands, so the read returns a wrong sum (for example base 100 at ts 10, operands at 20 and 30, range `[25, 40)`: pushdown returns 3, the counter is 103 at ts 30).

**Interim behavior (coordinator decision, to be numbered):** for a family with a merge operator the time range is not pushed down; it applies to *resolved* versions (`ResolveOptions::time_range`: a version, merged or not, is kept iff its timestamp is in range), after deletes, TTL and folding and before the value predicate and version limits. For a family without merge operands that equals pushdown, so `ResolveOptions::route_time_range(&mut filter, range)` sends the range to `ScanFilter::time_range` when there is no operator and to the resolver when there is one; the engine calls it when building a scan. A version whose fold fails but which is outside the range is not returned and so does not fail the read. Tests: `counter_time_range_applies_to_resolved_versions` and the time-range case of the resolver oracle.

## D83 — a cross-shard commit is recovered all or nothing (approved; engine)
Recovery applies a decided cross-shard commit only if every participant named by its COMMIT record still holds its PREPARE; otherwise it is discarded everywhere. A flush may not persist a share of a cross-shard commit until every PREPARE and the COMMIT are durable (Milestone B, #37). The model checker found that without this rule a `Buffered` cross-shard commit could be recovered in part after a power loss.

## D84 — durability promises are per WAL stream (approved; engine; amends D42)
Single-shard commits keep D42's prefix promise within their shard's stream. A cross-shard commit is durable once every participant's PREPARE and the coordinator's COMMIT meet the requested level, and is otherwise lost as a whole (D83); there is no global prefix across streams.

## D85 — a failed WAL sync after a group was applied leaves its data visible (approved; engine)
The group's members are told the commit failed and the stream is poisoned (wal contract), but entries already applied to memtables stay readable until restart, like `Durability::None` data. Documented on `Engine::commit` next to `Durability::None`.

## D86 — default timestamps use a per-shard floor seeded at replay (approved; engine)
Each shard's default-timestamp floor (D11) is seeded from every replayed commit timestamp at open and persisted in `Counters`. Per-tablet floors arrive with tablet moves (#38).

## D87 — reader processes re-pin when idle (approved; engine; changed in review)
A reader re-pins at its next snapshot once its live-snapshot count drops to zero, so a long-lived reader never blocks reclamation forever. Pinning the oldest live snapshot precisely is #39 (Phase 4).

## D88 — application-owned close does not block (approved; engine)
`close` in application-owned mode returns once shutdown is requested; the application keeps driving its shards until they finish. Calling `PendingCommit::wait` on the thread that drives the commit's shard deadlocks; poll the future from the event loop instead (documented).

## D89 — `From<format::Error>` maps unknown variants to `Corruption` (approved; engine)
`format::Error` is `#[non_exhaustive]`, so the conversion keeps a wildcard arm, which maps to `Corruption`; `InvalidArgument` maps to code 19 (ICR 0001).

## D90 — `Snapshot::at_seqno` is a test hook behind `test-hooks` (approved; engine)
The hook is not public API: it compiles only with the non-default `test-hooks` cargo feature, which only the engine's own tests enable.

## D91 — conditional writes, OCC and prepared shares (approved; changed in review)
A conditional member's written rows, read keys and predicate row are checked against earlier writers in its group (it then runs in the next group) and against prepared, undecided shares (a single member waits for the decision; a PREPARE aborts with `Conflict` rather than wait on another commit's decision). Every shard owning a row a transaction read is a two-phase-commit participant with an empty PREPARE that validates it, so there is no cross-shard write skew. Arena room is reserved per group and per prepared share, and a participant's apply error reaches the coordinator, so a half-applied commit is never acknowledged. Every decide is answered, so an aborted commit never blocks the watermark.

## D92 — one resolver for reads and compaction (approved; coordinator)
The engine's Milestone A resolver (`resolve.rs`) is replaced in Milestone B (#37) by `pigeonhole-compaction`'s `MergingCursor` + `FilteredCursor` + `CellResolver`; where they differ, compaction's behavior wins (D75–D77, D82). `EngineOptions::merge_operators` takes effect then. Milestone A has no tablet splits (#38) and no WAL checkpoints (#37).

## D93 — the per-stream recovery oracle lives in `pigeonhole-sim` (approved; sim, #40; extended by D114)
`pigeonhole-sim` provides `StreamCommit`, `recovered_commits`, `check_acknowledged_survive` and `Model::from_commits`: each WAL stream keeps a prefix of its records, a single-shard commit survives iff its record does, and a cross-shard commit iff every PREPARE and its COMMIT do (D83, D84). `Model::crash_window` remains the single-stream special case. The engine and public suites adopt it in #48, replacing their own copies of the rule.

## D94 — a later stronger commit makes earlier `None` commits durable (approved; owner decision; implemented by #50)
The spec's "Mixed levels" says a `GroupSync` commit also makes earlier `Buffered` or `None`
records on its stream durable; the engine wrote no WAL record for a `None` commit, so it was
lost at the next close or crash even after a later stronger commit.

**Decision:** the spec's rule stands. A `None` commit buffers its WAL record (no write, no
sync of its own), so a later stronger commit on the same shard writes it and makes it
durable. Implemented by engine Milestone B, issue #50; this crate changes nothing.

**Until #50:** the guide's durability page states the rule and marks it as arriving with
#50 (today a `None` commit is lost at the next close or crash). The public model suite
(`tests/model.rs`, `Logged::reaches_the_wal`) expects today's behavior and is updated with
#50 and #45.

_Superseded by D94 and D115: engine Milestone B (#63) implemented the rule; the public docs now state it._

## D95 — Phase 2 family settings are refused at creation (approved; pigeonhole)
`Family::zstd`, `Compaction::Tiered` and `Compaction::FifoByTime` are in the frozen API but
land in Phase 2. `TableBuilder::{create, create_if_missing, open}` refuse a declared family
with any of them with `ErrorCode::Unsupported` before changing the catalog, so they are never
stored only to fail later in flush or compaction. `blob_threshold` is accepted and stored
(values stay inline until blobs exist). Lifting the refusals: #44.

## D96 — every family has the `i64` add operator unless told otherwise (approved; pigeonhole; amends D41)
`Family::default()` stores `merge_operator = "pigeonhole.i64_add"`, so `incr` works on any
family, as `Family::merge_operator`'s documentation promised;
`Family::default().merge_operator("")` stores none (operands then fail at commit with
`InvalidArgument`). Mixing byte puts and `incr` in one column fails at read with
`MergeFailed` (D41); `RowMutation::merge` / `WriteBatch::merge` write untyped operands, which
the built-in operator also refuses at read. The `Family` documentation says so.

## D97 — `Scan::limit(0)` returns no rows (approved; pigeonhole)
`ScanSpec::limit` uses 0 for "unlimited"; the public `limit(0)` yields an empty iterator
without starting an engine scan, and no `limit` call means unlimited.

## D98 — `TableBuilder::open` adds declared families that are missing (approved; pigeonhole)
All three finishers add missing declared families to an existing table (a family listed
twice is declared once, with its first options); an existing family keeps its stored
options. Concurrent creation of the same table or family opens what the other caller
created. Creating a table needs a non-empty name and at least one declared family, else
`InvalidArgument` (coordinator decision in the same review).

## D99 — table handles resolve families added through other handles (approved; pigeonhole)
`Table::families()` returns `Vec<&str>` borrowed from the handle, so it reports the families
the handle was opened with (documented). Mutations, gets and reads resolve a name the handle
does not know against the current catalog, and a row read or scan names every family it
returns through the catalog as of the read.

## D100 — `Error`'s `Display` is the message; unknown engine variants map to `Io` (approved; pigeonhole)
`Display` prints `message()` only. Every current `engine::Error` variant maps to exactly one
`ErrorCode` (tested); a variant the engine adds later (it is `#[non_exhaustive]`) maps to
`Io` with the engine's message until it gets its own code. Messages the engine's unit
variants cannot carry are filled in here: `KeyTooLarge` names the part and its size against
the 64 KiB limit, `ValueTooLarge` the value's size against the D16 limit computed from the
open's options, and `Busy` (coordinator decision: a hard failure until engine Milestone B,
#37) says the memtable arena is full and to raise `Options::memtable_budget`.

_Superseded by D94 and D115 / D124: after Milestone B, `Busy` is a transient write stall or an oversize batch; the public message and the guide say so._

## D101 — a hidden `Options::wal_segment_size` test hook (approved; pigeonhole; ICR 0005)
With the default 64 MiB WAL segments every open on `SimVfs` cost about 0.5 s in a debug
build. The hook (`docs/design/icr/0005-pigeonhole-wal-segment-size-hook.md`) lets the
simulation suites use 256 KiB segments; the public model suite went from about 70 s to about
3 s with three times the operations.

## D102 — registered custom merge operators are not passed to the engine yet (approved; Phase 2 work tracked in #43)
`Options::merge_operator(Arc<dyn MergeOperator>)` keeps the operators; the engine resolves
only `pigeonhole.i64_add` so far, so a family naming any other operator is refused with
`UnknownMergeOperator`. Documented on `Options::merge_operator` and in the guide.

## D103 — features available ahead of their phase (approved; pigeonhole)
The engine already implements conditional commits, optimistic transactions and reader
processes, so `RowMutation::commit_if` (P2), `Transaction` and `Pigeonhole::open_reader` (P4)
work and are tested (`tests/api.rs`); the guide marks them "early". Their hardening stays in
their phases.

## D104 — what crosses the future C ABI (approved; pigeonhole)
Checked against the spec's "Language scope":
- Borrowed results have owned counterparts (`CellRef` → `Cell`, `RowRef` → `Row`), and
  `RowIter::next_ref` is the cursor form of the scan iterator.
- Generic conveniences have non-generic equivalents: `scan(range)` → `scan_bounds`,
  `qualifier_range` → `qualifier_bounds`, `families(iter)` → repeated `family(&str)`;
  `impl AsRef<Path>` parameters accept a `&Path`.
- `WriteBatch` has every mutation `RowMutation` has (`put`, `put_at`, `put_i64`, `put_f64`,
  `incr`, `merge`, `delete_cell`, `delete_column`, `delete_family`, `delete_row`), so a C ABI
  can export one mutation vocabulary.
- Errors are `#[repr(u32)]` codes plus a message; merge operators are identified by name in
  the file.
- Two frozen signatures take Rust-only types by nature: `Shard::set_wakeup(Box<dyn Fn>)` (a
  C ABI wraps a function pointer and context in the box) and
  `Options::merge_operator(Arc<dyn MergeOperator>)` (a C ABI would provide a vtable struct).
- Builders (`RowMutation`, `RowRead`, `Scan`) borrow their table, but a C ABI builds and
  finishes one within a single call. `RowIter<'t>` owns its engine cursor and borrows the
  table only as a lifetime, so a C ABI that keeps the `Table` alive next to it can hold one
  across calls; an owned `Table::scan_owned` could be added later if a binding needs it.

## D105 — `Runner` gains two provided methods, `client` and `describe` (approved; bench)
The frozen `Runner` trait takes `&mut self` per operation and has no way to hand out per-thread handles, so `WorkloadConfig::threads` could not be honored. The scaling gate needs concurrent writers. The trait also could not say which settings (shards, durability) a number was measured with.

**Decision:** two provided methods with defaults, so existing implementors are unaffected and no frozen signature changes: `fn client(&self) -> Option<Box<dyn Client>>` (default `None`, so `run` uses one thread) and `fn describe(&self) -> String` (default empty). `Client` is a new `Send` trait with `execute(&mut self, &BenchOp)`. All four runners implement both. Also added (new items only): `run_detailed`, `RunRecord`, `Suite`, `Environment`, `Histogram`, `Tolerance`, `compare`, `Scaling`, `WorkloadKind::{ALL, name}`, `WorkloadConfig::{smoke, small}`, `PigeonholeRunner::{shards, memtable_budget, memory, sync}`, `MemoryBudget`, `BLOOM_BITS`, `RunOptions` (warmup), and `memory`/`sync` builders on every comparison runner.

**Status:** confirmed in the coordinator's review of PR #55.

## D106 — The scaling gate cannot pass until tablets split or tables spread across shards (approved; bench; tracked in #51)
The gate wants write throughput at N shards ≥ 0.8 × N × single-shard on "workloads whose rows spread across tablets". Today a table is one tablet, tablets never split, and a tablet's shard is `tablet % shards`. The skewed workload writes one table, so every write lands on one shard whatever N is. Tablet splits are not in engine Milestone B (#37).

**Decision:** `phdb-bench scaling` measures and reports exactly what the engine does (one table, N client threads, `shards(1)` against `shards(N)`), with the efficiency and a pass/fail line. The bench does not spread rows over several tables to fake tablets. Tracked in [#51](https://github.com/CodingAnarchy/pigeonhole/issues/51).

**Status:** confirmed in the coordinator's review of PR #55. The gate waits for tablets (#51). Each `scaling` run evaluates the efficiency half; the "no single-shard p99 regression" half is `compare` against a stored `scaling.json`, which the weekly workflow uploads.

## D107 — Sparse-wide: what does "1M rows × 0 to 10K qualifiers, Zipfian" mean? (approved; bench; sizes grow in #52)
Read literally, a Zipfian number of qualifiers per row in 0..10K averages about 1,000 cells a row, which means about 10⁹ cells at 1M rows. That is not a sparse workload, and it cannot fit in memory.

**Decision:** the 10K is the qualifier vocabulary, and qualifier popularity is Zipfian (feature-store shape: a few attributes appear in most rows, most are rare). Each row has 0 to 40 cells, uniformly (mean 20); rows with no cells are not written. Operations: 60% point gets of (Zipfian row, Zipfian qualifier), many of which miss as a sparse store should; 20% puts of 1 to 4 cells; 20% scans of 10 rows. The row count is `--records` (1M once #37 lands: [#52](https://github.com/CodingAnarchy/pigeonhole/issues/52)).

**Status:** confirmed in the coordinator's review of PR #55.

## D108 — comparison durability is "written to the OS, not fsynced" unless `--sync` (approved; bench)
Engines differ in what "commit" means. To compare like with like, every runner defaults to the level that survives a process crash but not power loss: Pigeonhole `Buffered`, RocksDB WAL without `sync`, SQLite WAL with `synchronous=NORMAL`, fjall `PersistMode::Buffer`. With `--sync`, every engine fsyncs each commit: Pigeonhole `Sync`, RocksDB `sync=true`, SQLite `synchronous=FULL`, fjall `SyncAll`.

**Decision:** as above. The Goals-table "p99 commit < 200 µs with fsync batching" needs concurrent committers under `GroupSync`, which the bench does not measure yet: [#53](https://github.com/CodingAnarchy/pigeonhole/issues/53) (Phase 3).

**Status:** confirmed in the coordinator's review of PR #55. Tuning note from the review, now implemented: no engine is tuned, and none gets more memory than another. Every runner gets the same `MemoryBudget`, a write buffer and a read cache of 256 MiB each by default. That is Pigeonhole's memtable per shard plus its block cache; RocksDB's `write_buffer_size` plus LRU block cache; fjall's `max_memtable_size` plus `cache_size`; and SQLite's page cache of the sum. RocksDB gets a 10-bit bloom filter to match Pigeonhole's default `bloom_bits`; fjall builds filters by default. Every runner's `describe()` prints its budget, filter and durability, and the report shows them in the Settings column. Everything else stays at each engine's defaults.

## D109 — reproducibility tolerance (approved; bench)
**Decision:** two runs on one machine agree when throughput and p50 are within ±20% and p99 is within ±40%. p99.9 and max are reported but not checked, because at 2×10⁵ operations p99.9 rests on about 200 samples. `compare` is symmetric: it flags improvements beyond tolerance as well as regressions. `phdb-bench compare --tolerance T` sets T for throughput and p50, and 2T for p99. Comparing runs from different machines or build profiles prints a warning, because the tolerance only applies within one machine. Derived from five runs at `4b59eac` with the 5% warmup: across the four quiet runs (6 pairs), the worst drift was 10.1% on throughput, 13.4% on p50, 17.2% on p99, and 202% on p99.9. The run that started on a busy machine (15-minute load 12.3) fails `compare` against every other run. The first proposal of ±15%/±30%, from two runs, left only 1.1× headroom on p50. Details are in `docs/bench.md`.

**Status:** confirmed in the coordinator's review of PR #55. Re-derived from five runs with the 5% warmup (above).

## D110 — license exceptions for fjall's dependencies (approved; bench)
fjall (2.x and 3.x) depends on `varint-rs` (0BSD) and `xxhash-rust` (BSL-1.0). Both licenses are permissive and MIT-compatible but are not in D6's list.

**Decision:** `deny.toml` allows each license for its one crate only (`[[licenses.exceptions]]`), not workspace-wide. They enter only through `pigeonhole-bench`'s off-by-default `fjall` feature, and the bench crate is `publish = false`. If the exceptions are rejected, remove the fjall runner and report fjall numbers from its own benchmarks instead.

**Status:** confirmed in the coordinator's review of PR #55. The exceptions are scoped to pigeonhole-bench only (`publish = false`), and `deny.toml`'s comments say so. A published crate that pulls either crate in needs its own review.

## D111 — CI builds the bench crate without the `rocksdb` feature (approved; bench)
`ci.yml` runs `--all-features`, which would build RocksDB from C++ source on Linux, macOS and Windows (several minutes per job, plus libclang for bindgen).

**Decision:** CI runs every `--workspace --all-features` step with `--exclude pigeonhole-bench`, then builds and tests the bench crate with `--features sqlite,fjall`, which needs only a C compiler. `cargo deny` still covers every feature (`[graph] all-features = true`). A separate workflow, `bench-rocksdb.yml`, builds and tests the bench crate with every feature, RocksDB included. It runs weekly, on pushes to `main` that touch `crates/bench/**`, and on demand, outside the PR-gating workflow, so the `rocksdb` feature cannot rot.

**Status:** confirmed in the coordinator's review of PR #55. Includes the build job.

## D112 — YCSB fidelity limits imposed by `BenchOp` (approved; deferred to #54 (Phase 2))
`BenchOp::Get` reads one cell and `ReadModifyWrite` has no family or value, so:
- YCSB reads fetch one random field, not all ten (YCSB's `readallfields=true`). Updates write one field, as in YCSB.
- Read-modify-write is a get and then a put, not atomic in any engine, so `ycsb-f` refuses more than one client thread. The value written back is the old value with its first byte incremented (8 zero bytes if the cell is missing), identical across runners.
- YCSB D reads the latest records through a Zipfian over the initial record count. YCSB E picks scan starts from the loaded records, not the growing insert count.

**Decision:** as above, documented in `docs/bench.md`. A family read op (`BenchOp::GetRow`) needs an ICR: [#54](https://github.com/CodingAnarchy/pigeonhole/issues/54).

**Status:** deferred to #54 (coordinator's review of PR #55).

## D113 — Time series: TTL never expires during a run (approved; deferred to #54 (Phase 2))
`BenchOp::Put` carries no timestamp, so cells get commit-time timestamps, and a TTL short enough to expire data mid-run would make results depend on wall-clock timing.

**Decision:** the `metric` family has a 1-day TTL, so reads pay the TTL check but nothing expires. Expiry under load (and FIFO-by-time compaction) needs a timestamped put op (ICR) and the engine's Phase 2 TTL compaction: [#54](https://github.com/CodingAnarchy/pigeonhole/issues/54).

**Status:** deferred to #54 (coordinator's review of PR #55).

## D114 — the recovery oracle also works on record lists (approved; sim, #59; extends D93)
`pigeonhole-sim` offers `recovered_from_records(streams, survivors)`: the caller passes each stream's records in append order as `StreamRecord::{Single, Prepare, Commit { commit, participants }}` plus the surviving prefix length per stream. A single-shard commit survives iff its record does; a cross-shard commit iff its COMMIT survives and every participant the COMMIT names still holds its PREPARE (D83). A commit whose COMMIT was never appended is lost. PREPARE and COMMIT need not be adjacent. `recovered_commits` is now `recovered_from_records` over `commit_records`, so the commit-level API is unchanged.

**Decision:** as described; suites with overlapping commits (engine, pigeonhole) call the record-level helper. Adoption is tracked by #48.

## D115 — Which WAL streams must a flush sync before its SSTs become visible? (approved; engine Milestone B)
The spec says a flush "must not persist a share of a cross-shard commit until every PREPARE and the COMMIT are durable" but does not say how the flush learns that. Checking per commit (which shards hold shares, whether their records are past each stream's durable LSN) needs cross-shard state the flush task does not have.

**Decision:** before committing its manifest edit, a flush task sends a `SyncBarrier` to its own shard always (so an earlier commit of the same stream is never lost while a later one survives in an SST) and to every shard when any flushed memtable holds a share of a cross-shard commit (`MemEntry::has_shares`). Each barrier is one `submit_sync` of that stream; the task waits for all replies. This over-syncs (every stream, not just the participants') but needs no bookkeeping; a flush is rare compared with commits. Confirmed in review; syncing only the participants' streams is a Phase 3 optimization (issue #64).

## D116 — When may a shard checkpoint a PREPARE or COMMIT record? (approved; engine Milestone B)
D24 says a checkpoint never strands a prepared commit, and D83 that a cross-shard commit is recovered all or nothing. Neither says what a participant needs to know about the coordinator's COMMIT before it moves its checkpoint past its own PREPARE.

**Decision:** each shard keeps a deque of its logged records (`Logged`) and advances its checkpoint to the end of the longest prefix it no longer needs: a single commit while any of its `(tablet, family)` slots is unflushed; a PREPARE until its slots are flushed and either the coordinator reported its COMMIT checkpointed (`CommitCheckpointed`) or the commit was the shard's own and is complete; a COMMIT until every participant reported its share flushed (`ShareFlushed`). Aborted prepares pass at once. The checkpoint is clamped to the stream's written LSN and the manifest edit is committed before `wal.checkpoint` runs, so a crash between the two replays harmlessly.

## D117 — Is `SetFlushed` the memtable's max seqno, or the shard's visible seqno? (approved; engine Milestone B)
The manifest brief says `SetFlushed { tablet, family, seqno }` and replay skips mutations at or below it, but a memtable frozen while a group is mid-apply could hold a seqno above the visible watermark while a lower one is still being applied to the active memtable.

**Decision:** a shard freezes only when `active.max_seqno <= visible_seqno`, so every entry up to the memtable's max seqno is in it and `SetFlushed` is exactly that max. The freeze is deferred (`freeze_deferred`) until the condition holds, never skipped.

## D118 — How conservative is `GcPolicy` about reader-process snapshots? (approved; engine Milestone B; precise per-slot pinning is #39)
D70 narrows purge by `min_ts_above`; D74 purge needs the set of live snapshot seqnos. Reader processes only publish a reader-slot pin (a view version), not their snapshot seqnos.

**Decision:** `gc_policy` takes the writer's live snapshot seqnos (`LiveSeqnos`) plus the oldest reader pin's seqno (`oldest_reader_pin`): a reader's snapshots pin its own view, so the oldest pin bounds everything any reader can still read, and everything at or above it counts as reachable. This is conservative (a reader pinned at a version may hold no snapshot at all) and loses only purge work, never visibility. Precise per-slot pinning is issue #39's territory.

## D119 — How should the L0 write stall behave with a frozen or coarse clock? (approved; engine Milestone B; amended by D126)
The spec's token bucket refills with time. Under the simulator the clock advances only when the workload says so, so a stalled shard with nothing else running would wait forever.

**Decision:** the stall engages only while the L0 score is `>= 1.0` and a compaction can run; it arms one `StallTimer` task with a cancel flag, cancelled the moment the score drops (a compaction committed) so a timer never spins on a frozen clock. A commit that cannot get room retries on the next `Kick`. A failed background compaction sets a backoff flag that a stall (score `>= 1.0`) clears, so a device that recovers is retried while a dead one does not loop.

## D120 — What does `backup` write for an engine with memtables and many levels? (approved; engine Milestone B; blob extents are #58)
The spec says a backup is a consistent single-file copy; D60 covers shrink. Copying SST extents verbatim would still need the WAL (unflushed memtables) and the file's free-space layout.

**Decision:** `backup` takes a snapshot and writes a new file: every `(tablet, family)` is merged from the snapshot's memtables and SSTs (raw entries at seqnos `<= snapshot`, no purge) into one SST at the last level, a manifest snapshot names them, and the file is marked clean. The result opens without replay and with no sidecars. Blob extents are not copied yet: a database whose catalog names blob files is refused with `Unsupported` until issue #58 lands; data stays inline below the D29 threshold.

## D121 — What happens at open when the discovered streams do not match `0..shards`? (approved; engine Milestone B)
D20 says streams beyond a reduced shard count are flushed then removed, but says nothing about the opposite direction (more shards than streams) or whether the flush is synchronous.

**Decision:** when the discovered stream set is not exactly `0..shards`, open replays everything, flushes every recovered memtable synchronously (`flush_recovered`), checkpoints each kept stream to its end, removes the extra streams and creates the missing ones. Open then holds no unflushed WAL data, so the new layout starts clean. This makes a shard-count change an expensive open, which the spec accepts ("a one-time cost").

## D122 — Should SST readers open lazily or at manifest apply? (approved; engine Milestone B)
The spec targets `open()` under 5 ms; a manifest can name hundreds of SSTs whose footers would all be read at open.

**Decision:** `OpenSst` holds the metadata and an `OnceLock<Arc<SstReader>>`; the reader (footer, index, filter blocks) is opened on first use by a read or compaction, through the block cache. Open reads only the manifest.

## D123 — Does the sim's recovery helper cover a coordinator that is also a participant? (approved; superseded for new code by D114; engine adoption in #48; amended by D125)
Issue #48 asks the engine suite to adopt `recovered_commits` / `check_acknowledged_survive`. The helper counts one record per stream per commit, so a coordinator's PREPARE and COMMIT on its own stream must be adjacent; the engine interleaves other commits' PREPAREs between them whenever commits overlap.

**Decision:** the harness takes the engine's own append order (the `test-hooks` `AppendedRecord` stream), applies the record-level prefix rule itself, and cross-checks `recovered_commits` only over commits whose records are adjacent per stream and all appended (`sim_helper_recovered`), skipping the check when the streams cannot be represented. `check_acknowledged_survive` and `Model::from_commits` are used as is. A record-level helper in `pigeonhole-sim` (issue #59) will let the check run on every crash and the harness drop its own rule; the engine half of #48 waits for it.

## D124 — How does a group waiting for arena room learn that a flush freed some? (approved; engine Milestone B; amended by D126; flush/compact under arena pressure in the decision folded from #116)
`ShardArena` reports free bytes only through `reserve`; nothing signals the shard when `reclaim` returns memory.

**Decision:** a group that finds no room waits (a write stall, counted in `Metrics::stalls` with its duration): the shard freezes and flushes, and a flush completion (`Flushed`), every `Maintain` message and the stall's timeout timer re-run `reserve_room` for the waiting group (`refresh_free`). A flush that fails is tried again on the next retry. The wait ends with `Busy` only after `EngineOptions::write_stall_timeout_nanos` (30 s by default) passed without room, or at once for a batch that can never fit an empty arena, or with the poison error when the pager is poisoned.

## D125 — the model suites use the sim's record-level oracle on every crash (approved; harness, #48; amends D123)
D123 left the engine harness applying its own record-level prefix rule and cross-checking `recovered_commits` only where the streams could be represented. With `recovered_from_records` (D114) both suites now call the sim oracle on every crash, and neither keeps its own copy of the rule:

- **Engine (`crates/engine/tests/common`).** The engine's append order (`Engine::take_appended`) becomes `StreamRecord`s per stream (a COMMIT names the participants whose PREPAREs the harness saw), the surviving prefix per stream comes from the WAL and SSTs as before, and `recovered_from_records` decides which commits survived; commits fully flushed below a checkpoint are added from the manifest. `sim_helper_recovered` and `Stats::helper_checked` are gone.
- **Records that name no live commit are accounted for, never silently dropped.** Each record carries the number of crashes at its append. A repeat of a `(seqno, kind)` on one stream is legal only across a crash (recovery reused a lost commit's seqno): the latest copy belongs to the live commit, earlier copies are counted in `Stats::reused_records`, and two copies within one epoch fail the run (`Protocol`). A record whose seqno no client commit holds (a refused attempt's PREPARE, a lost commit's records) is counted in `Stats::unowned_records`; a single-shard record of that kind inside the surviving prefix fails the run (`RecoveredFromTheFuture`).
- **Unacknowledged commits are matched to WAL seqnos as a whole.** A surviving share of a cross-shard commit holds a subset of its mutations, and one share can fit several in-flight commits (a family delete inside another commit's row delete). Seqnos and commits are matched by a maximum bipartite matching (exact fits tried first) instead of greedily in seqno order, so no share takes another commit's seqno (sweep seed 288).
- **Public (`crates/pigeonhole/tests/model.rs`).** Crash runs use one shard, so every commit is one record in one stream. The public API does not show how many records survived, so each prefix, longest first, goes through `recovered_from_records`, `check_acknowledged_survive` and `Model::from_commits`; recovery must match one allowed prefix.

**Status:** implemented.

## D126 — write stalls and failed background work on a frozen or moving clock (approved; engine, #70 #79 #88; amends D119 and D124; flush/compact under arena pressure in the decision folded from #116)
D119 cancels the L0 stall's timer when a compaction commits and D124 times a wait for arena room out after `write_stall_timeout_nanos`, but both lean on a clock. Under `SimVfs` the clock moves only when the workload advances it, and the workload is blocked in the commit (or, in application-owned mode, in `run_once`, whose slice deadline never comes). Three things followed: a `StallTimer` polling the clock spun for ever; a failed background compaction with an L0 score `>= 1.0` was retried at once by `maintain`, redoing the merge in a loop against a dead (crashed or poisoned) device (#79); and a stall with nothing running in the background had nothing to end it.

**Interim behavior** (proposed amendments; on a moving clock D119 and D124 are unchanged except where noted):

*Detecting a frozen clock.* Every engine timer (`ClockTimer`: the L0 stall's refill timer, the arena-room timeout, the compaction backoff) polls the VFS clock. After 1024 polls in a row that see the same reading it gives up, records that reading and kicks the shard. The shard treats the clock as **frozen** only while its reading still equals the recorded one; any movement since makes it a moving clock again, and timers are armed as before. The fallbacks below apply only to a frozen clock.

*Amendment to D119 (L0 stall).*
- Moving clock: unchanged. The token bucket refills with time and paces writers whether or not a compaction runs or can start. A stall that finds no compaction running asks `maintain` to start one: it takes the most urgent slot the picker finds work in, falling back to the next slot when the picker returns nothing for the most urgent one; a compaction that fails to start sets the backoff.
- Frozen clock: the bucket never refills, so a stall ends on compaction progress. Every compaction completion (success or failure) kicks a waiting group, and a successful one adds one token. When no compaction runs or can start, writers are admitted. A timer that gave up is not armed again while the clock stays frozen.
- Failed compactions (D119's backoff, both clocks): after a failure, or a failure to start, no background compaction starts until a flush completes, a group is admitted, or, on a moving clock, a backoff timer fires: 1 s after the first failure, doubling with each failure in a row up to 60 s, and back to 1 s after a compaction succeeds (`compaction_backoff_nanos`). The timer's retry happens even with no new writes; on a frozen clock it gives up and only the event-tied retries remain. A dead device therefore never loops. A poisoned shard starts no background compaction until reopen, and a stalled writer on it is admitted and fails with the shard's error.

*Amendment to D124 (wait for arena room).*
- Moving clock: unchanged. The wait ends with `Busy` once `write_stall_timeout_nanos` has passed, and a failed flush is retried on the next run of the waiting group.
- Frozen clock: the timeout never comes, so the wait also ends with `Busy` after 4 failed flushes in a row, or at once when nothing the shard would hear of can free room. That is the **idle case**: no flush running or queued, no deferred freeze, no undecided cross-shard share, no unsynced group, and no retired memtable that a reader process still pins (pinned memtables stay in the retired list and may be released at any time). What remains is held by in-process snapshots, which only the blocked caller can drop. A poisoned pager still ends the wait at once with the poison error.
- Both clocks: `reserve_room` reclaims retired memtables whose readers left before it reports no room, and refusing the waiting members no longer drops the members the same group admitted before the cut (the timeout path used to return without applying them). A wait ends (and its timeout timer is cancelled) when a flush completes with no group left waiting, and at close (issue #88: a wait a flush had ended outlived the writers and kept its timer running into the close).

The spin while a timer waits on a real clock is fixed by runtime timed wakeups (#89).

## D127 — a model harness attributes an error to an armed power loss only once the crash has fired (approved; harness, #62)
An armed power loss (`FaultPlan::crash_after_ops`) can fire on a shard's background I/O (a flush, a compaction or a manifest commit) with no client call in progress. Both harnesses keep a liveness probe, a file opened alongside the store whose handle dies with every other one at a crash. When a step (a read, a scan, a snapshot, a submit or a commit) fails and the probe is dead, the failure is that power loss and the harness recovers from it (`crash_and_recover(Power, already = true)`), whatever error the dead store reported first.

**Armed alone is never sufficient — the probe must be dead.** An error while a crash is armed but has not fired is a real failure and fails the run; with no probe open there is no evidence that a crash fired.

**Status:** implemented. Tests: `a_read_after_a_background_fired_crash_recovers` (engine), `a_read_after_a_background_fired_crash_is_that_crash` and `an_io_error_while_an_unfired_crash_is_armed_is_not_that_crash` (public; the last fails under the old "armed and `Io`" rule).

## D128 — flush and compaction outputs are trimmed to their length before they are published (approved; pager, compaction, #106)
A writer cannot know an SST's final size up front, and `SstWriter` writes into one extent, so each flush or compaction output is allocated with room to spare and trimmed at finish: `Pager::trim(extent, len)` shrinks an extent that is allocated but not yet published to the smallest one holding `len`, keeping its first page and freeing the upper buddy halves at once. Nothing durable references unpublished space (D8), so a trim needs no retirement and a crash after it leaves only free space; the trimmed extent is what `SstMeta` (and the manifest) names. `trim` and `abandon` take only pending extents: the pager refuses (and debug-asserts on) one it knows a durable root references (every extent loaded at open, and the manifest snapshot and log of each committed root), and not abandoning an SST or blob extent published in this session stays the caller's contract, since the pager never reads the manifest. Compaction also sizes each output's extent to the input bytes not yet written out (GC only shrinks data) plus an eighth, capped at `target_sst_bytes`; once that estimate is used up (a codec change can grow data), later outputs take the target size. Flush keeps its memtable-size estimate and relies on the trim.

Before this, every compaction output reserved a whole target-size extent (64 MiB by default) however little it held, and the file grew about that much per compaction. Inputs are still freed by retire and reclaim (D61). Each published SST wastes less than half its extent.

**Status:** implemented. Tests: `small_compactions_keep_the_file_bounded` (engine: 100 compactions at the default target stay within twice the live data plus 2 MiB), `small_outputs_take_small_extents` (compaction), `trim_keeps_the_head_and_frees_the_tail` and the `Trim` op in the pager model, the refusal tests (`published_extents_are_neither_released_nor_shrunk`, `trim_refuses_a_published_extent`, `abandon_refuses_a_published_extent`), and the pager crash sweep, which publishes outputs trimmed to the smallest class and from 1 MiB to 256 KiB, with data placed in a freed tail published in the same root.

## D129 — tablet changes are off by default until hardened (approved; tablets, #97)
Splits, merges, moves and the balancer still have known stalls and hangs (tracked as `[engine] tablets: ...` issues under #38).

**Decision:** `EngineOptions::tablet_changes` (default `false`) turns them on. Off, the balancer never runs and explicit changes (the test hooks) are refused with `Unsupported`, so every table stays one tablet on shard `tablet % shards`, as before #38. Every piece of the tablet work that would change behavior is gated on the switch: per-new-slot arena accounting, idle-slot retirement, the freeze reservation guard, catalog-based checkpoints and share reports (and their extra `Maintain` work), the union of replayed PREPARE slots, the published shard floors and the coordinator's above-the-participants'-floors timestamp. Two fixes stay on because they are bugs without tablets too: the freeze wake (a deferred freeze registers with the seqno it waits for, every time it defers, and is woken once that seqno is visible) and the compaction rewrite of an SST holding rows outside its tablet (inert for a whole-table tablet). What stays on is inert for a whole-table tablet: admission routing checks run only while a change is in flight, and scan clamping is a no-op for an unbounded tablet. The model harness has a matching `Config::tablet_changes`; its tablet-specific allowances apply only when it is on.

## D130 — a tablet's owner is not persisted; owners are re-derived at open (approved; tablets, #97; persisting placement is #104)
The spec says a move "records the new owner in the manifest", but `Edit::PutTablet` has no owner field and the `Edit` tags are frozen in `pigeonhole-format`. Adding an edit is a format change outside the engine.

**Decision:** owners live only in the in-memory catalog and the published tablet map. A move, and the placement of a split's children, is still one manifest commit (`ReqKind::Tablets`, which also writes the `Counters` edit), so it is serialized with every other catalog change. At open every owner is re-derived from the tablet id (`tablet % shards`), as before; the balancer moves tablets again if the load calls for it. Correctness does not depend on the owner surviving: replay routes every record through the tablet map at open, and checkpoints compare slots against the catalog's flushed seqnos rather than the shard's own (see the next question). If owners should survive a reopen, add an `Edit::SetTabletOwner` (or an owner field on `PutTablet`) in `format`.

## D131 — checkpoints compare slots against the catalog, not the shard (approved; tablets, #97)
After a move, or a reopen that re-derives owners, a stream can hold records for slots another shard now owns and flushes. A shard's checkpoint used its own per-slot flushed seqnos, so it never passed such records (the WAL grew and a clean close waited for ever), and a replayed PREPARE applied on another shard was logged with no slots at all (its participant could checkpoint past it before the data reached an SST).

**Interim behavior (with `tablet_changes` on):** `needed` and `report_shares_flushed` read the flushed seqnos of the current view's catalog. A slot whose tablet no longer exists needs nothing: its table was dropped, or a split or merge retired it after every write to it reached SSTs. Replayed PREPAREs record the slots of every shard they were applied on. Every manifest commit that adds SSTs broadcasts `Maintain`, which then also advances checkpoints and share reports. Off, the shard's own flushed seqnos and its `dropped` set decide, as before.

## D132 — a commit routed through an older tablet map (approved; tablets, #97; amended by the #102 decision)
A router can pick a shard just before that shard splits, merges or moves the tablet.

**Decision:**
- **Single-shard commits** touching a tablet being changed are *parked* on the owner before they are logged (they hold no seqno, so the watermark never waits for them) and routed again once the change commits: to the same shard, to the new owner, or through two-phase commit when their rows now span shards. A later commit on any of the same rows parks behind them, so per-row submission order holds on that shard. Commits that reach a shard that no longer owns their rows are forwarded the same way. A client that pipelines two commits on one row from one thread may, during a move, see them applied in the other order (the second can reach the new owner before the first is forwarded); concurrent commits never had an order.
- **PREPAREs** touching a tablet being changed, or routed with a tablet map older than the participant's, are refused with an internal `Moved`. The coordinator aborts the commit everywhere (no COMMIT record, so recovery discards the PREPAREs that were written) and retries it once the tablet map is newer than the one it routed with or a tablet change has finished. The retry keeps its first commit timestamp when that is still at or above every participant's floor, so a refused attempt is invisible to the caller. Each PREPARE carries the whole commit's reads; a participant only checks the reads in tablets it is changing.
- A change waits, before it commits, until no prepared share and no compaction touches its tablets and every memtable of them is in SSTs (shares decided after the freeze are frozen and flushed again).

## D133 — the default-timestamp floor of a moved tablet (approved; tablets, #97)
D11 wants a per-tablet floor that travels with the tablet; D86 keeps it per shard.

**Decision:** the shard floor stays the only floor kept; it is an upper bound on every default timestamp the shard assigned to any of its tablets. Before a change commits, the shard raises the floor of every shard receiving a tablet to its own (`Shared::ts_raises`, read by the receiver's next default timestamp), so the moved tablet's timestamps never go backwards. A coordinator also picks a cross-shard commit timestamp above every participant's published floor and raise. A commit refused with `Moved` keeps its first timestamp on retry only where nothing else reached it: above every participant's floor and raise, or equal to one only where the shards that reached it are the commit's own (its coordinator and the participants that prepared it, and raises those shards made). Otherwise it takes a fresh timestamp, so a retry never ties a write another commit made. No per-mutation bookkeeping is added.

## D134 — what the balancer does, and its options (approved; tablets, #97; stability work in #103; amended by the #95 and #103 decisions)
The spec gives the triggers (size, sustained write skew, small and cold) but no policy.

**Decision:** each shard runs its balancer every `EngineOptions::balance_interval_nanos` (default 100 ms; 0 disables it) and changes at most one thing at a time, in this order:
1. **Size:** a tablet whose SSTs hold at least `tablet_split_bytes` splits in two near the middle of its SST boundary rows and recent writes. A tablet that still shares SSTs with a sibling (D13) does not split by size (their bytes would count twice).
2. **Skew:** a shard that wrote at least `balance_min_writes` rows in the interval and more than `balance_skew` (default 1.25) times the mean over shards moves the tablet whose load is closest to half the gap to the coldest shard. When one tablet carries more than the gap, it splits instead, at quantiles of a 64-row sample of its recent writes, into one child per shard below the mean (children go straight to those shards). The same rule applies to memtable bytes, with `memtable_freeze_bytes` as the minimum.
3. **Merge:** two adjacent tablets of one table on the shard, with no writes for two intervals and empty memtables, holding less than a quarter of `tablet_split_bytes` together, merge. A merge is refused while a sibling still has to compact its copy of a shared SST, since the merged tablet would see those rows twice; merges never move tablets to bring neighbours together.

`balance_interval_nanos`, `balance_min_writes` and `balance_skew` are new, additive `EngineOptions` fields. The test hooks `split_tablet_pending`, `merge_tablets_pending`, `move_tablet_pending`, `balance_pending`, `tablet_changes`, `max_ts_floor` and `TabletMap::ranges` sit behind `test-hooks`, like the other hooks.

## D135 — a split's children and the view buffer (D28) (approved; tablets, #97)
**Decision:** a split whose estimated encoded view would not fit the shared-memory view buffer is refused before it starts (`Unsupported`), so the published view is never refused after the manifest commit (which would poison the pager). Children reference only the parent's SSTs that overlap their own range; scans clamp every tablet's sources to its range, since a shared SST also holds the sibling's rows.

## D136 — arena room for many tablet slots (approved; tablets, #97; refinement in #104)
With many tablets per shard, every `(tablet, family)` slot takes memtable chunks, and memtables pinned by live snapshots stay allocated after their flush.

**Interim behavior (with `tablet_changes` on):** a batch reserves a chunk for each slot it would create; empty slots release their memtables after a flush and before a room wait; a freeze never takes a chunk admitted members reserved; the balancer and tablet-change validation keep each shard's slots to a quarter of its arena's chunks (`max_slots`: with the default 64 MiB budget and 1 MiB chunks, 16 slots per shard). A room wait that nothing can end follows D126 (#84). Smaller arena chunks for shards with many tablets are the longer-term fix.

## D137 — What does `Engine::compact` guarantee while tablets split, merge and move (approved; engine)
`Engine::compact` sends one `CompactAll` to every shard; each shard compacts the slots it owns and replies. With tablet changes on, the balancer kept moving and merging tablets meanwhile: a tablet could leave a shard before that shard's round reached it and arrive at a shard whose round was over, so it was never compacted (seed 13 of `results_are_identical_across_shard_counts_with_tablet_changes`). And a slot holding one SST above the last level was moved there without a rewrite, keeping deletes that a rewrite of the same rows purges (D74). How many SSTs a slot holds depends on when splits and moves flushed it, which depends on the shard count (seed 106).

**Decision (tablet changes on only; off, nothing changes):**
- While a full compaction runs, the balancer starts no change (`Shared::full_compactions`). A round during which a tablet change finished or was given up (`Shared::tablet_epoch` moved) is followed by another round, until one completes with no change. Changes requested explicitly (test hooks) still run; they only add rounds.
- `plan_full` rewrites a lone SST above the last level instead of moving it, so every slot's full compaction purges what a bottommost compaction may.

Proposed decision: a full compaction compacts every slot that exists when it is called, whatever tablets do meanwhile, and leaves each slot as one rewritten run at the last level. Whether the lone-SST rewrite should also apply with tablet changes off (it costs one rewrite per slot with a single L0 SST, and makes `compact` purge consistently) is for the coordinator; this PR keeps it gated, as D129 requires.

**Coordinator:** confirmed. With tablet changes off, a lone SST above the last level is still trivially moved, not rewritten: the move is the cheaper equivalent and purge timing may vary (D74).

## D138 — What do `flush` and `compact` do when the arena has no chunk for the fresh memtables they need (approved; engine, #116; amends D124 and D126)
Every freeze replaces the active memtable with a fresh one from the shard's arena. When in-process snapshots pin the retired memtables, the arena can have fewer free chunks than memtables to freeze. The freeze then left those memtables active, and `flush` and `compact` still reported success with their data only in the WAL. D124 and D126 define how a commit waits for arena room; neither covers `flush` or `compact`. A closing shard flushes such memtables in place (#111, #117), since it admits no more commits. A running shard cannot: it needs an active memtable in every slot it writes.

**Decision:** a freeze of every memtable that leaves one active for lack of a chunk sets `ShardState::starved_all`. `flush` and `compact` callers wait while it is set (`check_flush_waiters`, and the full-compaction loop in `maintain`). At the end of every message batch while they wait, the shard reclaims retired memtables and tries the freeze again (`retry_starved_freeze`), so a flush that freed chunks lets the rest freeze. The wait ends like a stalled write (D124, D126). On a moving clock, the callers get `Busy` once `write_stall_timeout_nanos` has passed. On a frozen clock they get it at once in the idle case: no flush running or queued, no deferred freeze, and no retired memtable a reader process still pins. In-process snapshots, which only the caller can drop, hold the rest. Nothing is lost: the data stays in memtables and the WAL. The public model suite drops its snapshots and retries, as it does for a `Busy` commit. Commits are not paused while the callers wait.

Proposed decision: amend D124 and D126 so that `flush` and `compact` wait for arena room as a stalled write does, ending with `Busy` on the same terms, and never report success while a memtable they were asked to flush is still unflushed.

**Coordinator:** confirmed; this amends D124 and D126: `flush` and `compact` wait for arena room as a stalled write does and end with `Busy` on the same terms, never `Ok` with data left unflushed.

## D139 — Where do tablets go at open, now that owners are not persisted (D130) (approved; engine)
D130 re-derives every owner as `tablet % shards`. After splits, a reopen with fewer shards can put more `(tablet, family)` slots on one shard than its arena has chunks: a 20-family table split into four tablets is 80 slots, all on shard 0 of a one-shard reopen, against 64 chunks at a 4 MiB budget. A commit writing all of them, or a freeze of all of them, then never finds room, and that shard can neither shed slots nor receive moves (D136's `max_slots` refuses both).

Persisting owners (an `Edit::SetTabletOwner`, or an owner on `PutTablet`) needs a `format` change and an ICR, and a reopen with fewer shards still has to place the tablets of the missing shards somewhere. Placement at open is needed either way, so this change does only that.

**Decision (with `tablet_changes` on):** at open, in tablet id order, a tablet goes to shard `tablet % shards` when that keeps the shard within its slot budget, else to the shard holding the fewest slots (`Catalog::place`). Owners are still not persisted: a reopen with the same shard count loses earlier moves, as D130 says, and the balancer moves tablets again if the load calls for it. With `tablet_changes` off, every tablet is on shard `tablet % shards`, as before.

**Coordinator:** confirmed.

## D140 — How large is the slot budget, and what happens when the tablets need more (approved; engine)
D136 caps each shard at a quarter of its arena's chunks (`max_slots`). Chunks are `memtable_budget / 64` capped at 256 KiB, so at budgets up to 16 MiB every shard has 64 chunks and 16 slots. A shard holding a table with more than 16 families could never split by size or receive a move, and nothing reported it.

**Decision (with `tablet_changes` on):** each arena is cut into at least 256 chunks (`arena / 256`, between 1 KiB and 256 KiB), so every shard serves at least 64 slots at any budget. The default 64 MiB budget already had 256 KiB chunks, so it does not change. When the tablets placed at open need more slots on a shard than that, the chunks shrink further (`arena / (4 × slots)`, never below 1 KiB). The chunk size is fixed for the life of the open, so splits and moves past the budget are still refused at run time: an explicit request fails with `Unsupported`, and the balancer passes over a size split it has no slots for. The balancer logs that skip under `PIGEONHOLE_TRACE`, but no metric counts it. Smaller chunks mean entries larger than a chunk take a run of contiguous chunks more often, which a fragmented arena may not have. With `tablet_changes` off, chunks are sized as before.

Open for the coordinator: whether a refused split should be counted in `Metrics`, and whether chunks should shrink at run time (for example, rebuilding a shard's arena once every memtable is flushed) instead of only at open.

**Coordinator:** confirmed. Refused splits get a metric (https://github.com/CodingAnarchy/pigeonhole/issues/122, Phase 2); shrinking chunks at run time is https://github.com/CodingAnarchy/pigeonhole/issues/123 (Phase 3).

## D141 — When do empty slots give back their memtables (approved; engine)
D136 retires every idle slot's memtable after every flush. A slot written once per flush cycle then gets a fresh memtable each cycle. A reader process's pin keeps every chunk retired after the view it pinned (D118), so the arena filled twice as fast as with tablet changes off.

**Decision (with `tablet_changes` on):** idle slots retire only when a commit waits for arena room (the room-wait path of `run_group`), not after each flush. Pinned retired memtables (flushed ones and, under pressure, idle ones) still stay allocated until the pin moves, as with tablet changes off.

**Coordinator:** confirmed.

## D142 — Must a participant check a cross-shard commit's timestamp against its own floor (approved; engine)
D133 has the coordinator pick a commit timestamp above every participant's published floor and raise. A participant publishes its floor after reserving the seqnos it assigns timestamps to, so a coordinator on another thread can reserve a later seqno and still read the older floor. Its commit then lands on that participant at a timestamp below one the participant assigned to an earlier seqno, and a tablet's default timestamps go backwards (D11).

**Decision (tablet changes on only):** a participant refuses a PREPARE that writes rows at a commit timestamp strictly below its floor (its own floor, or the raise a shard handing it a tablet set) with an internal `BelowFloor`, and republishes its floor. The coordinator aborts the attempt everywhere, as for `Moved`, and retries it at once with a fresh timestamp rather than its first one. A tie is accepted: it never reorders, and a retry that kept its timestamp may meet the floor its own first attempt raised. Empty (validate-only) PREPAREs write nothing and are never refused. The check covers every tablet the participant owns, not only received ones, since the race is the same for all. Test hook: `Engine::publish_stale_ts_floor` replays the stale read deterministically.

**Coordinator:** confirmed (strictly below is refused; ties allowed).

## D143 — Should commits pipelined on one row keep their order during a move (approved; engine)
D132 parks a commit on a tablet's old owner during a move and forwards it once the move commits, but a later commit from the same thread can reach the new owner first.

**Decision:** unchanged, and now documented publicly on `Engine::submit` and in `EngineOptions::tablet_changes`'s known limits: a commit submitted after an earlier one was acknowledged is applied after it; commits in flight together have no order. Test: `commit_order_across_a_move`.

Proposed design if the stronger order is wanted: before the move's manifest commit, the old owner sends each receiving shard an `Incoming` message naming the moving ranges. The receiver parks single-shard commits on those rows and refuses PREPAREs on them with `Moved`. Once the change is done (or has failed), the old owner sends `HandoffDone` carrying the parked commits that now route to the receiver. The receiver runs those first, then its own parked commits, and bumps `tablet_epoch` so that refused PREPAREs retry. The message order (A's `Incoming` happens before the view is published, and so before any client routes to the new owner) puts `Incoming` ahead of every new-owner submit. A parked commit that turns cross-shard after the change could still reorder; that would need its own rule.

**Coordinator:** no stronger order. D132's guarantee stands: a commit submitted after an earlier one was acknowledged is applied after it; commits in flight together have no order. The public API's commits are synchronous, so one client's writes stay ordered. The `Incoming`/`HandoffDone` design is not built.

## D144 — Who rewrites a cold child's inherited SST so that the balancer can merge it back (approved; tablets, #95; amends D134)
D134 refuses a merge while a sibling still has to compact its copy of an SST the two shared after a split (D13): once one child has rewritten its copy, the other's copy still holds the first child's rows, and the merged tablet would read them twice. A cold child never reaches its L0 trigger, so nothing ever rewrote its copy, and every balancer pass refused the same merge (issue #95). D134 says nothing about how such a merge gets unblocked.

**Decision (with `tablet_changes` on):** when the balancer finds two adjacent cold tablets small enough to merge and `merged_ssts` refuses them, it queues every slot of the pair that holds an SST sticking out of its tablet's range (`ShardState::cleanups`). `maintain` serves that queue after full compactions, alternating with due compactions (a queued cleanup takes every other background compaction, except while writers stall on L0), so writes elsewhere on the shard never starve it: it compacts the slot into the last level (`compact::plan_full`; a single sticking-out SST becomes a rewrite, D79), which drops the sibling's rows. A later pass then merges. Slots that no longer stick out, whose tablet left the shard or is being changed, are dropped; a slot whose inputs are busy waits for the next `maintain`. Compaction backoff and a poisoned shard stop cleanups like any other background compaction. Off, the balancer never runs, so the queue stays empty.

Proposed amendment to D134, item 3: "…A merge is refused while a sibling still has to compact its copy of a shared SST, since the merged tablet would see those rows twice; the balancer then asks for a rewrite of the slots holding such an SST, at the lowest compaction priority, and merges on a later pass."

The balancer's coldness test also ignores an empty active memtable: since slots keep their memtable after a flush (#116/#120), an empty one still has chunks allocated, which used to keep its tablet from ever counting as cold.

**Coordinator:** confirmed.

## D145 — How long may a commit wait on a tablet change, and when is a PREPARE routed with an older tablet map refused (approved; tablets, #102; amends D132)
D132 parks single-shard commits behind a change and refuses PREPAREs touching a changing tablet, or routed with an older map, with `Moved`; the coordinator retries. It sets no bound: parked commits sat before the arena-room wait (so D124's timeout never applied), a change waits for a running compaction on its tablets, retries had no cap, and a shard that once handed a tablet away refused every PREPARE routed with an older map, even when none of its rows changed owner, so under balancer churn a cross-shard commit could retry for ever. Close also waited for a draining change and everything parked behind it.

**Decision (with `tablet_changes` on; off, none of this is reached):**
- **Parked commits** fail with `Busy` once `write_stall_timeout_nanos` has passed since they were submitted. A clock timer fires at the oldest deadline; like D126's timers it is not armed again while the clock is frozen, where parked commits wait for the change as before.
- **Retries** of a commit refused with `Moved` fail with `Busy` after `MOVED_RETRIES` (16) refusals, or once `write_stall_timeout_nanos` has passed since the commit was submitted (also for a retry still waiting for the map to change). The attempt count travels with the commit (`CommitReq::attempts`, `CoordinateReq::attempts`), through forwarding and both retry paths: a refusal for a moved tablet, and one for a timestamp below a participant's floor (#121).
- **Refusals:** a PREPARE routed with an older map is refused only when, under the participant's current map, a row its share writes belongs to another shard, or a row the commit reads belongs to a shard outside the commit (`PrepareReq::participants`); a read another participant now owns is validated there. Rows in a tablet being changed are still refused.
- **Routing checks after a loss:** the sticky `lost_tablets` flag is replaced by `lost_version`, the map version as of the last change that handed a tablet away. Commits routed with that map or a newer one skip the check; single-shard commits now carry the version they were routed with (`CommitReq::map_version`; forwarded ones take the map that forwarded them).
- **Balancer:** it does not start a change on a tablet that is compacting (it decides again next interval). An explicit change still waits for the compaction.
- **Close** gives up a change that has not reached its manifest commit (`abort_op` with `Closed`), so its parked commits are routed again and fail with `Closed`; a committing change still finishes.

Proposed amendment to D132: add "A parked commit, or a refused cross-shard commit, fails with `Busy` after `write_stall_timeout_nanos` (and a refused commit after 16 attempts). A PREPARE routed with an older map is refused only when one of its rows now routes outside the commit. Close gives up a change that has not started its manifest commit."

**Coordinator:** confirmed.

## D146 — How does the balancer avoid thrashing, oversubscribing a shard's slots and growing the tablet count without bound (approved; tablets, #103; amends D134)
D134 decides on one interval's snapshot: per-interval write counts, and memtable bytes, which swing with every flush, so uniform load moved or split tablets every interval. Each shard checked a move's target against its own view of the slots, so two shards could move tablets onto one shard in the same interval and take it past `max_slots`. Merges needed both neighbours on one shard while skew splits put children on other shards, so the tablet count only grew. And a shard balanced only while it processed messages, so an idle shard never merged its cold tablets.

**Decision (with `tablet_changes` on; off, the balancer never runs):**
- **Smoothed load.** Each shard keeps a moving average of the rows it writes per interval (weight 0.5 on the newest), publishes that in `LoadSlot::writes`, and keeps one per tablet. Write skew compares these averages. The memtable-bytes trigger is dropped.
- **Dwell.** After a shard starts a move or a split over shards, it starts no other one for 10 balancer passes. A tablet that arrived on a shard (by a move or a split) is not moved or split for write skew there for 10 passes; tablets a shard held at its first pass count as settled. Size splits and merges are not delayed.
- **Slot reservations.** A change that hands tablets to shards reserves their slots (`LoadSlot::reserved`) when it starts, and releases them when it ends. Every check counts the slots in the view plus the reservations, so concurrent moves and splits never take a shard past `max_slots`; the losing change is refused with `Unsupported`. A skew move only targets a shard with room.
- **Consolidation.** A settled, cold tablet whose left neighbour lives on another shard moves there when the two hold less than a quarter of `tablet_split_bytes` together and the neighbour's shard has room; that shard then merges them (D134 item 3). Only right to left, so two shards never swap tablets.
- **Idle shards.** After each pass the shard arms a clock timer for its next pass, so the balancer runs without messages. As with D126's timers, a timer that gave up on a frozen clock is not armed again while the clock stays frozen. It is cancelled at close and never armed for an interval of 0 or `u64::MAX`.

Proposed amendment to D134: replace "The same rule applies to memtable bytes…" with the smoothed-load rule and the dwell; add the reservations to item 2, consolidation as item 4, and "each shard wakes itself for its next pass".

**Coordinator:** confirmed.

## D147 — Does a prepared, undecided cross-shard share count as above a compaction's inputs (approved; engine, #132; amends D70)
D70 takes `GcPolicy::min_ts_above` from the upper SSTs and the slot's memtables. A participant holds a cross-shard share between PREPARE and the decision outside the memtables, and a commit applies it under the commit's seqno, which can be below every entry already in the memtables. A bottommost compaction that ran in that window purged a row delete above the share's explicit timestamp, and the share appeared once applied (seed 183 of `tablet_changes_with_a_changed_shard_count`). Tablet changes are not needed: two tables on two shards take the same path.

The test hook's `CompactionRecord::max_seqno` had the same gap: just below the memtables' oldest seqno, which can be above `visible`, so the model counted the share as an input.

**Interim behavior:**
- `min_ts_above` also folds in the smallest cell timestamp (explicit, or the commit's) of every share the shard holds prepared that writes the slot's family, whatever its tablet (routing may change before the decision). An aborted share only makes the bound lower than needed.
- `CompactionRecord::max_seqno` is never above the visible seqno. A memtable freezes only once its seqnos are visible, so no input is above it; a seqno past it may be a commit not applied here yet (prepared, or its PREPARE still on the way). One whose PREPARE arrives after the compaction starts counts as a later write (D74): it may appear below a delete the compaction purged, as any later write with an older explicit timestamp may.

Proposed amendment to D70: `min_ts_above` covers the upper SSTs, the slot's memtables and the shard's prepared shares for the slot's family.

**Coordinator:** confirmed.

## D148 — Reader snapshots from before a writer restart expire with `SnapshotExpired` (approved; process, #140 F7-1; amends the spec's "Writer crash and restart")
The spec said "Writer crash and restart. Readers keep serving their current snapshot." That cannot hold. A reader process's pin lives in the region generation it attached to. The next writer builds a new generation and frees every extent its recovered root does not name (`OpenedPager::finish(live)`). That includes SSTs that old-generation snapshots still read. The new writer sees only pins in the new region, so it compacts and reuses those extents. Reads through the old snapshot then returned `Corruption("block"/"sst footer")` or, worse, `Ok(None)` for rows that exist.

**Interim behavior (implemented):** a reader snapshot keeps the region it was taken in. Every read through it (`get`, `get_latest`, `read_row`, each `ScanCursor::next_row`/`next_cell` step, the `raw_entries` test hook) checks after the read, seqlock style, whether that region went stale: it was marked abandoned, or the directory names another generation. If so, the read fails with the new `Error::SnapshotExpired` (public `ErrorCode::SnapshotExpired` = 25), whatever it read. This is sound because a writer marks the old region abandoned and publishes its generation before it allocates anything: `Pager::open` and `finish` write nothing. So a read that completed before the generation changed read what the snapshot names. A clean writer close alone does not expire snapshots; they keep serving until a new writer opens. `snapshot()` retries (re-attaching) when the region goes stale while it builds a view, including when the build itself failed because the new writer reused the manifest blocks it was reading. `get_latest` in a reader process, which holds no snapshot of its own, retries a read that expired (up to 8 times). Live snapshots are counted per generation: an expired snapshot the application still holds does not keep the new generation's pin from moving forward. Snapshots in the writer process never expire. Spec §(multi-process) "Writer crash and restart" is amended, and the error is documented in `docs/guide/errors.md`.

The full fix keeps old snapshots readable. The new writer would quarantine the free space found at open until every live old-generation reader slot has re-pinned or died, and a re-attaching reader with live snapshots would pin the oldest view in the new region. That is a candidate for Phase 4 reader hardening (with #39) if expiring turns out to be too strict for applications.

**Coordinator:** confirmed. Keeping old snapshots readable (quarantining free space until old-generation readers re-pin) is Phase 4 reader hardening, with #39.

## D149 — A reader builds a view only from the catalog of the record's own manifest version (approved; process, #140 F7-2)
A reader view pairs the memtables of the shared-memory view record with the SSTs of a catalog. The catalog was loaded from the current durable root, keyed on the header's manifest version rather than on the record's. The writer commits the root, publishes the view, and only then sets the header version, so the two could differ in either direction. In one direction a flushed memtable was counted both as a memtable and as an SST (counter operands double-counted). In the other it was in neither, a stale read.

**Interim behavior (implemented):** `reader_snapshot` uses the cached catalog only when its version equals `record.manifest_version`. Otherwise it re-reads the superblocks through the reader's own read-only pager (`Pager::reload_root`) and loads that root only if it names that version. If not (the writer committed a newer root and has not published its view yet), it drops the attempt and re-reads the record, with exponential backoff (16 yields, then sleeps from 50 µs doubling to 1.6 ms) until a 1 s deadline. After that it fails with `Busy`. The only way to stay in that state is a writer that died or stalled between its root commit and its publish; a new writer republishes. The header's manifest version is now used only to refresh table metadata (`Engine::table`). `docs/guide/errors.md` documents the reader-side `Busy`.

**Follow-up question:** After a writer crash between a root commit and its view publish, and before any new writer opens, new reader snapshots fail with `Busy` (above). The reader cannot read the older root that matches the record, since the pager exposes only the newest. Alternatives: (a) as now, `Busy` until a writer restarts; (b) detect that no writer holds the writer byte and build a memtable-free view from the durable root alone, which loses unflushed memtable data that only WAL replay restores.

**Coordinator:** confirmed, and (a): a reader returns `Busy` until a writer restarts. Option (b) would serve a view missing committed writes that only WAL replay restores, which breaks durability for readers; a retryable `Busy` is the honest answer.

## Open questions
_None._
