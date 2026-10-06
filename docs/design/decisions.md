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

## D9 — family-in-row deletes use a marker key; BigTable delete rule (approved; revised after review)
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

## D22 — filter pushdown semantics, uniform across sources (approved; revised after review)
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

## D38 — a cell delete is timestamp-only (approved; owner decision U2)
A `CellDelete` at timestamp `T` hides every version at exactly `T` **whatever its seqno**, including a put or merge operand at `T` committed after the delete, uniform with the column and family rule of D9 (and with HBase). Seqnos only decide what a snapshot sees: a snapshot taken before the delete still sees the version. A cell cannot be rewritten at the same timestamp while the marker exists; write the replacement at another timestamp. Compaction drops the marker only at the bottommost level together with everything at `T` it covers. FORMAT §2, the sim `Model` (ICR 0003) and the guide state this. Resolver and GC work: [#25](https://github.com/CodingAnarchy/pigeonhole/issues/25).

## D39 — a row read returns families in creation order, or in the caller's order (approved; owner decision U3)
Within a row, cells come by family, then qualifier, then newest version first. Families come in creation order (`FamilyId` order, which the engine iterates anyway), or, when the read lists families, in the listed order (a family listed twice appears once). The sim `Model` and its test adapters follow this (ICR 0003); the guide states it. Engine work: [#26](https://github.com/CodingAnarchy/pigeonhole/issues/26).

## D40 — `compaction_cores(k)` is refused in application-owned mode (approved; owner decision U4)
Application-owned mode starts no threads (spec "Threading"), so `compaction_cores(k)` with `k > 0` together with `open_application_owned` fails at open with `InvalidArgument`, before anything is opened, instead of being silently ignored. `pin_threads` does not apply in that mode and is ignored. `pigeonhole-runtime` enforces its half now (`Runtime::application_owned` returns `Error::InvalidConfig`); the option's rustdoc and the guide document it. Engine enforcement: [#22](https://github.com/CodingAnarchy/pigeonhole/issues/22).

## D41 — merge folding across timestamps; a non-`i64` base fails (approved; audit K14, K15, C2)
`Incr` operands carry the commit timestamp, while a base put may carry an older or explicit one. Walking a column newest first, a run of operands folds into one version at the newest operand's timestamp, consuming the next older put as its base, with wrapping addition; deletes and TTL apply to entries before folding and `max_versions` after. So an expired base is dropped before folding and the counter restarts from the operands, which is accepted. A base whose value is not an 8-byte `i64` makes the read fail with `MergeFailed`, as the guide promises (never silently 0). A put no operand folds onto is returned as written. `Incr` on a family without the `i64` operator is rejected (`ModelError::NoMergeOperator` in the model; the engine maps its typed error to the same case). The sim model implements this (ICR 0003). Compaction's `I64Add`: [#21](https://github.com/CodingAnarchy/pigeonhole/issues/21).

## D42 — the reference model's crash windows (approved; audit K11)
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
`File::sync_data` (fdatasync) makes written bytes durable but callers may not rely on it for a size change; after `set_len`, `allocate` or a write past the end, `sync_all` is needed before the new length must survive a power loss. `SimVfs` models exactly that: a power loss reverts the length to the last `sync_all`'s (data synced past it is lost; an unsynced shrink reads back as zeros), unless a fault plan is active, in which case the pending length may survive. The stricter model found one real bug: `Pager::allocate` grew the file and root commits synced with `sync_data`, so a power loss after a commit could cut the file short of a published extent. The pager now calls `sync_all` once per file growth. The WAL already did (`allocate` + `sync_all` for every new slot).

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

## Open questions
_None._
