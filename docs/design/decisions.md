# Decisions log

Project-level decisions that refine or deviate from [spec.md](spec.md) and [task-briefs.md](task-briefs.md). Newest last. An entry here wins over those files. Agents: when the spec is silent or contradictory, add a question under **Open questions** instead of guessing; the coordinator turns answers into numbered decisions.

## D1 — `pigeonhole-sim` does not depend on `pigeonhole`
The spec lists `pigeonhole` as a dependency of `sim` ("drives the public API from above"). That creates a cycle the moment `engine` or `pigeonhole` use `sim` in tests. Instead `sim` depends only on `io` and `format`; the full-stack simulation suites live in `crates/pigeonhole/tests/` and `crates/engine/tests/`, which take `pigeonhole-sim` as a dev-dependency. Same coverage, strictly downward graph.

## D2 — the simulated VFS lives in `pigeonhole-io`
Per the io brief, `SimVfs` (fault injection, deterministic from a seed) is an `io` backend at `pigeonhole_io::sim`. `pigeonhole-sim` builds the scheduler, crash points and reference model on top of it.

## D3 — writer lock is a byte-range lock on the lock page
"Multi-process readers" mentions `flock`; "Files and locks" specifies byte-range locks on a reserved lock page (OFD on Linux, `fcntl` with a per-process registry on macOS/BSD, `LockFileEx` on Windows). The more specific section wins.

## D4 — interface-freeze gate
The spec gates the interface freeze on owner review. The owner directed autonomous progress, so the coordinator reviews and approves interfaces, records the approval here, and the owner may revisit at any time through an interface-change request.

**Approved 2026-10-05:** the interface freeze (PR #1), including D7–D24 as revised after review.

## D5 — reference hardware
No enterprise-NVMe Linux box with power-loss protection is attached to this project yet. Benchmarks run on available hardware (developer macOS arm64 and GitHub Linux runners), are reported in every run, and are labeled as non-reference. Performance gates are evaluated against those numbers until reference hardware is available.

## D6 — dependency policy
Allowed licenses: MIT, Apache-2.0, BSD-2/3-Clause, ISC, Zlib, Unicode-3.0, CC0-1.0 (enforced by `deny.toml`). Engine crates keep dependencies minimal; each new dependency gets a one-line justification in the PR description.

## D7 — manifest is a snapshot block plus a delta log (revised after review)
The spec calls the manifest "a small copy-on-write tree"; the format brief calls for "manifest edit records". The manifest is one immutable snapshot block (the whole state as edits) in its own extent, plus one 256 KiB delta-log extent holding consecutive delta blocks, one per manifest commit. The superblock names both and the log's live length. A commit appends its delta past the live end of the log (bytes no root references, so nothing live is overwritten and the superblock flip stays the only in-place write of live data) and then commits the root; the root commit's first sync covers the delta, so a commit costs two syncs and no new extent. When the log is full or outgrows the snapshot, the writer writes a new snapshot and an empty log. Open always reads exactly three things (superblocks, snapshot, live log), keeping it well under the 5 ms target, and readers catch up by reading only new log bytes. The first proposal (one extent and fsync per delta, chain of up to 64) failed the open-time budget. Root commits run off the shard foreground loop (D30). Layout: FORMAT §9.

## D8 — the free-space bitmap is not persisted (approved)
The pager keeps the bitmap in memory and rebuilds it at open from the live extents the manifest names (the manifest snapshot and delta log, SSTs, blob extents). The engine reads the manifest anyway at open, so this costs nothing extra, a crash can never leak an extent, and no persisted bitmap can disagree with the manifest.

## D9 — family-in-row deletes use a marker key; BigTable delete rule (revised after review)
A family-in-row delete is the key `[row][00 01][00 00][!ts][!seqno][FamilyDelete]`: `00 00` never appears in an escaped string and sorts before every qualifier, so a reader sees a row's markers before its cells. **Delete rule:** a `ColumnDelete` or `FamilyDelete` with timestamp `T` hides every version in its scope with timestamp `<= T` regardless of seqno (so a later put with an older timestamp stays hidden); a `CellDelete` hides exactly the versions at its timestamp; seqnos decide only snapshot visibility. Point gets use `CellResolver::seek_column`, which seeks each source to the row's marker prefix before the column (one extra seek per source, normally inside an already-loaded block); filters add a marker key so a column-filter miss never hides a marker.

## D10 — a whole-row delete is one family marker per family (approved)
Each family is its own tree, so there is no single place for a row tombstone. `delete_row` writes a `FamilyDelete` marker into every family of the table in the same atomic commit. Families added later cannot hold older data for that row, so nothing is missed.

## D11 — timestamps are microseconds; default timestamps never go backwards (revised after review)
Timestamps are microseconds since the Unix epoch. Each tablet keeps a floor: the largest default timestamp assigned to it. The default timestamp is `max(now_micros, floor + 1)`. The floor travels with a tablet when it moves between shards, is persisted as `ts_floor` in the manifest's `Counters` edit (the maximum over tablets at each manifest commit), and at open starts from `max(ts_floor, largest commit_ts replayed from the WAL)`, so a clock step back or a restart never reorders default timestamps. User-supplied timestamps are taken as microseconds for TTL.

## D12 — `WriteBatch::commit()` uses the writer default (approved)
The API-surface example shows `wb.commit(Durability::GroupSync)`, the Durability section shows `wb.commit()` plus `wb.commit_with(d)`. The more specific Durability section wins: `commit()` uses the writer default, `commit_with(d)` overrides it.

## D13 — family ids are unique per database; SSTs belong to a (tablet, family) (approved)
A `FamilyId` is never shared across tables, so `(TabletId, FamilyId)` names one LSM tree with one memtable set, one level set and one flushed seqno. After a split both children may reference the parent's SSTs until compaction rewrites them; an extent is retired only when no tablet references it.

## D14 — extra downward dependencies (approved)
`memtable` depends on `io` (the brief lists only `format`) because its arena is an `io::SharedRegion`; without it, `engine` (which forbids `unsafe`) could not hand shared memory to the memtable. `compaction` depends on `cache` and `io` (the brief lists `sst`, `pager`, `format`) because opening input SSTs takes a `BlockCache` and a `FileRef`. Both point down, so the layer rule holds.

## D15 — shared vocabulary types live in `format` (approved)
Ids, `Seqno`, `Timestamp`, `Lsn`, `Durability` and the `Cursor` trait are used by nearly every crate; `format` is the only crate all of them depend on, so they live there rather than being duplicated.

## D16 — value size limits (revised after review)
The blob pointer stores the length as a `u32`, so the ceiling is `2^32 - 1` bytes, one short of the spec's 4 GiB. Until blob separation lands, Phase 1 rejects at write time, with `ValueTooLarge`, any value larger than `min(WAL segment payload, 64 MiB max extent, half the shard's memtable arena)`. Phase 2 blob separation lifts this: blob pointers address a logical blob file whose payload spans many extents, so a value can exceed one extent with no format change.

## D17 — the `async` feature is off by default until Phase 3 (approved)
The spec makes `async` a default-on feature. In Phase 1 it gates only an empty placeholder module (`pigeonhole::nonblocking`), so it is declared but not default; it becomes default-on when the async API is implemented.

## D18 — filters are cache-line-blocked bloom filters in Phase 1 (approved)
The spec allows "a ribbon (or blocked bloom) filter". Blocked bloom is simpler and fast enough for Phase 1; the filter block's kind byte leaves room for ribbon later without a format break.

## D19 — read-your-writes: commits return once visible (revised after review)
A shard publishes a group's seqnos only after the group's WAL write meets the strongest level any member requested, and a commit returns only once it is durable at its level **and** `visible_seqno >= seqno`, so a caller always reads its own write. Idle shards publish `pending = u64::MAX` and never hold back the watermark. Cost: a cross-shard commit becomes visible only after every participant applies, so its latency includes the slowest participant's group, and any shard's in-flight group briefly delays visibility for all. A reader can still observe a `Buffered` or `None` commit that a later power loss removes; that is inherent in those levels.

## D20 — a changed shard count flushes recovered data before dropping streams (approved)
The spec replays every WAL stream at open regardless of shard count. If streams exist beyond the new shard count, the engine flushes the memtables recovered from them and checkpoints before removing those stream files. Open stays fast in the common case (same shard count).

## D21 — lock page byte assignments (approved)
Refines D3. On page 2: offset 8192 is the writer byte, 8193 the presence byte, 8194 an shm-init byte held exclusive while a process creates, validates or rebuilds the shared-memory region (so two processes opening at once never both build it). Locks never block; callers retry.

## D22 — filter pushdown semantics, uniform across sources (revised after review)
`format::scan::ScanFilter` holds the entry-safe conditions (qualifier selection, time range on puts) and one rule, `ScanFilter::admits`. Delete entries, family markers and merge operands always pass: hiding a delete would resurrect older versions, and dropping some operands would produce partial counters. `SstIter` applies it inside the block decoder; memtable sources are wrapped in `compaction::FilteredCursor`, so every source filters identically. Version count, columns per row and value predicates need snapshot visibility and run in `CellResolver`, still before materialization; a value predicate tests the newest visible value of a column. The sst acceptance test (pushdown equals filter-after) is defined over these semantics.

## D23 — one shared-memory view record carries the tablet map and memtables (approved)
The spec lists "the current tablet map" and published views separately. A view already includes the tablet map, so the region holds one double-buffered view record (tablet map, memtable roots, manifest version) published by a single pointer swap; a reader can never pair a tablet map with the wrong memtables.

## D24 — WAL checkpoints never strand a prepared commit (approved)
A participant's PREPARE is applied at recovery only if the coordinator's stream still holds the COMMIT. So a stream's checkpoint may not pass a COMMIT record until every participant's share of that commit is flushed (its `SetFlushed` covers the seqno). The engine computes checkpoints with this rule.

## D25 — WAL segments are chained; recovery never appends to a torn segment (proposed after review)
Each segment header records its predecessor's epoch and the offset where the predecessor's data ends. A writer syncs a full segment before writing its successor's header, and after recovery starts a new segment (epoch above every epoch in any header) chained to where replay ended, never appending to the torn one. Replay distinguishes end of segment (a successor names this exact stop offset) from end of log (no successor), and treats a successor naming a different offset as corruption. This closes the hole where stale-but-valid-looking data past a torn tail, or a later segment, could be resurrected. FORMAT §10.

## D26 — a cross-shard commit's id is its seqno (proposed after review)
The coordinator reserves one seqno per cross-shard commit; PREPARE and COMMIT records carry it and no separate commit id exists. Recovery raises `next_seqno` above every seqno in every replayed record, including discarded PREPAREs, so an id is never reused. A coordinator keeps each such seqno in its `held` set, and every `pending` watermark it publishes is `min(held, ..)`, so no snapshot sees half of the commit (FORMAT §11.3).

## D27 — a shared-memory directory plus generation-named regions (proposed after review)
A one-page directory region with a fixed, never-changing layout records the current generation; the region itself is named with its generation. A writer builds a new generation, marks the old region abandoned, then updates the directory. Readers detect staleness with two atomic loads and re-attach. Changing the name per generation also avoids Windows reusing a named mapping still held by an old process.

## D28 — an oversized view is refused, never truncated (proposed after review)
View buffers default to 4 MiB (configurable). If an encoded view would not fit, `publish_view` fails with `ViewTooLarge` and the writer refuses the change that grew it (typically a split), keeping the old view; this surfaces in metrics instead of corrupting readers.

## D29 — small memtable values are copied; one-shot gets avoid view refcounts (proposed after review)
`CellData` copies memtable values of at most 128 bytes (and merge results and blob reads); larger memtable values are pinned by `ArenaSlice` plus an `Arc<View>`. `Engine::get_latest` loads the view through an `arc-swap` guard, so a hot small point get touches no shared reference count.

## D30 — no fsync on a shard's foreground loop (proposed after review)
WAL group syncs use `Wal::submit_sync` and root commits use `Pager::submit_commit_root`; both return `io::Completion`s served by the I/O backend (the pread pool now, io_uring in Phase 3). Shards keep draining queues and building the next group while syncs run; the manifest task on shard 0 is a background task that waits on its completion.

## D31 — merge operators are associative folds (proposed after review)
`MergeOperator` is `merge(acc, older)` plus `finish(base, acc)`. The resolver streams operands newest first into one accumulator, with no buffered copies; compaction without a base keeps the accumulator as one combined operand. Non-associative operators are not supported.

## D32 — cursors own what they read (proposed after review)
`BlockIter<B: Deref<Target = [u8]>>` owns its byte owner (a `BlockHandle` in practice), `SstIter` owns an `Arc<SstReader>`, and `MemIter` owns a `MemtableReader` clone. Scan cursors and compaction jobs can then store their sources without self-references or `unsafe`, and values stay zero-copy.

## D33 — configuration structs are non-exhaustive (proposed after review)
Option and config structs (`EngineOptions`, `ShmConfig`, `WalOptions`, `RuntimeConfig`, `SstWriterOptions`, `ReadOptions`, `ScanFilter`, `ResolveOptions`, `PickerOptions`, `GcPolicy`, `JobContext`, `ReadSpec`, `ScanSpec`, `FaultPlan`, `OpenOptions`, `WorkloadSpec`) are `#[non_exhaustive]` with a constructor or `Default`, so adding a field is not a breaking change. `EngineOptions.embedding` was dropped: the open function chooses the mode.

## D34 — a commit holds one entry per (column, timestamp); last write wins (approved)
Within one commit, multiple mutations to the same `(table, row, family, qualifier, timestamp)` collapse to the last one written, at batch-build time (`format`'s `BatchBuilder` or the engine's `WriteBatch` enforces it), because internal keys order entries by `[!ts][!seqno]` and kind only, so write order inside a commit cannot be expressed. `pigeonhole-sim`'s `Model` matches. A family or row delete marker at timestamp `T` in the same commit as a put with timestamp `<= T` hides that put (the marker rule is ts-based, regardless of seqno).

## D?? — WAL spare segments are zero-filled off the shard thread (proposed by wal)
A freshly allocated slot (`fallocate`) still costs a metadata update at its first fdatasync, so the spec's "fdatasync on preallocated blocks, no metadata update" needs slots that were zero-filled and synced before use. `WalStream` never does that on the shard thread: `SpareSegments::prepare(n)` (a `Send + Sync` handle from `Wal::spares()`) allocates, zero-fills and syncs spare slots on a background task the engine runs; a rollover takes a recyclable slot first, then a prepared one, and only grows the file inline when it has neither, counted in `WalStream::inline_grows()` for metrics. `create` and `into_stream` zero-fill the one slot they start in at open. A failed write or sync poisons the stream (`Error::Poisoned`) until it is reopened through `Recovery`, so a later successful sync can never acknowledge commits behind a hole.

## Open questions
- **Q1 (io): exclusive locks need a writable handle.** POSIX `fcntl` (and Linux OFD) locks refuse a write lock on a descriptor opened read-only (`EBADF`); Windows `LockFileEx` does not. For backend parity, `pigeonhole-io` refuses `LockMode::Exclusive` on a read-only handle with `ErrorKind::Unsupported` on every platform and in `SimVfs`. Consequence for `shm`/`engine`: a reader process that must try the presence-byte upgrade at close ("last one out", D21) needs its main-file handle opened with `write: true` (it still writes nothing). Alternative: open reader handles read-only and skip the upgrade for readers, leaving cleanup to the writer or the next opener. Interim behavior: the refusal above.
- **Q2 (io): Windows shared-to-exclusive upgrade is not atomic.** `LockFileEx` cannot convert a held shared lock, so `PreadVfs` unlocks, tries exclusive, and re-takes shared on failure. If another process takes the byte exclusively in that window, the shared lock is lost (the call still reports `Locked`). Only the presence-byte "last one out" check upgrades, at close, where losing the shared lock is harmless. Interim behavior: as described; flag if a caller ever upgrades a lock it must keep.
- **Q?? (sim/model): `CellDelete` versus later writes.** Beyond D34 (same-commit collapse), a `CellDelete` hides only versions at its timestamp with an older seqno, so a later put at that timestamp is visible again. At one timestamp across commits the newest-seqno put is the version, older entries there are shadowed, and merge operands newer than that put fold onto it. Engine crates are checked against this until a decision says otherwise.
- **Q?? (sim/model): merge-operand folding across timestamps.** `Incr` operands carry the commit timestamp, while a base put may have an older (or explicit) one. Interim model behavior: walking a column newest first, a run of operands folds into one version at the newest operand's timestamp, consuming the next older put as its base (an `i64`; a base that is not 8 bytes counts as 0), with wrapping addition; deletes and TTL apply to entries before folding, `max_versions` after. `Incr` on a family without the `i64` operator is `ModelError::NoMergeOperator`; the engine's typed error should map to it.
- **Q?? (sim/model): result order across families.** Reads return cells ordered by family name, then qualifier, then timestamp descending. If the engine orders families by `FamilyId` (declaration order), either the model or the test adapters must normalize; the model sorts by name for determinism.
- **Q?? (sim/model): crash windows.** The model promises after a power loss every commit up to the last `GroupSync`/`Sync` one, after a process crash every commit up to the last `Buffered`-or-stronger one, and that survivors are a prefix (the spec's "only a suffix is lost"); `None` commits before a stronger one are therefore durable too, as the spec's durability section states. A commit in flight at a crash is registered with `Durability::None`. `Model::recover` was added (it truncates and, after power loss, marks survivors durable); no frozen signature changed.
- **Q?? (format): does a `ScanFilter`'s qualifier selection apply to deletes and merge operands?** D22 says deletes, family markers and merge operands "always pass". That holds for the time range, but for the qualifier selection it would make `ScanFilter::next_admissible` useless: any skipped column might hold an admitted delete, so no seek could ever skip a column. Nothing of an excluded column is ever returned, so its deletes and operands cannot change a result. Interim behavior: the qualifier selection applies to every cell entry, deletes and merges included; the time range applies to puts only; family markers always pass. `admits` and `next_admissible` agree, so pushdown still equals filter-after. Please confirm or amend D22.
- **Q?? (format): `prev_end` cannot name the end of a full 4 GiB segment.** FORMAT §10.1 allows segments up to 4 GiB but stores `prev_end` as a `u32`, so a record ending exactly at the end of a 4 GiB segment gives `prev_end = 2^32`. Interim behavior: `SegmentHeader::decode` accepts sizes up to 4 GiB. Suggest capping `segment_size` at `4 GiB - 32 KiB`, enforced by the WAL crate when it creates a stream.
- **Q?? (format): region names can exceed 31 bytes.** `phdb-<16 hex>-<generation hex>` fits in 31 bytes only for generations below `2^36`, or below `2^32` if macOS's `PSHMNAMLEN` counts the leading `/` that `shm_open` needs. Interim behavior: `region_name` follows FORMAT §11. Generations grow by one per writer open, so this is unreachable in practice, but the shm crate should check the length and fail clearly.

### Q1 — does the qualifier selection of a `ScanFilter` apply to deletes and merge operands? (format agent)
D22 says delete entries, family markers and merge operands "always pass". That reasoning holds for the time range, but for the qualifier selection it would make `ScanFilter::next_admissible` useless: every skipped column might hold an admitted delete, so no seek could ever skip a column. Nothing of an excluded column is ever returned, so its deletes and operands cannot change a result. **Chosen for now:** the qualifier selection applies to every cell entry (deletes and merges included); the time range applies to puts only; family markers always pass. `admits` and `next_admissible` agree on this, so pushdown still equals filter-after. Please confirm or amend D22.

### Q2 — `prev_end` cannot name the end of a full 4 GiB segment (format agent)
FORMAT §10.1 allows segments up to 4 GiB and stores `prev_end` as a `u32`. A record that ends exactly at the end of a 4 GiB segment gives `prev_end = 2^32`, which does not fit. **Chosen for now:** nothing changes in the format crate; `SegmentHeader::decode` accepts sizes up to 4 GiB. Suggest capping `segment_size` at `4 GiB - 32 KiB` (the WAL crate can enforce this when creating streams).

### Q3 — region names exceed 31 bytes for large generations (format agent)
`phdb-<16 hex>-<generation hex>` stays within 31 bytes only for generations below `2^36`, and macOS's `PSHMNAMLEN` (31) may count the leading `/` that `shm_open` needs, which leaves room for generations below `2^32`. **Chosen for now:** `region_name` is implemented as FORMAT §11 specifies. Generations grow by one per writer open, so this cannot be reached in practice; the shm crate should check the length and fail clearly anyway.

- **Q?? (shm): "no other process attached" for a layout-version rebuild is decided by the presence byte.** A writer that finds a live region with another layout version cannot read that region's slot table (it may be laid out differently), so it probes the presence byte: an exclusive lock through its own handle succeeds only if no other process has the database open. The byte is then converted back to shared, so the presence lock the writer already holds on that handle is kept (and a writer that held none is now simply present, as it is while the file is open). Assumes the writer passes the same handle to `ShmRegion::open` as to `Presence::acquire`, which the engine's open sequence does. Alternative: an explicit `attached: bool` from the engine; needs an ICR.
- **Q?? (shm): only `active` slots are reclaimed.** A slot in the transient `claiming` state has no trustworthy pid yet (the previous owner's may still be there), so a process that dies between its CAS and its `active` store leaks one slot until the next generation. Reclaiming `claiming` slots by the stale pid could free a slot under a live claimant. **Chosen for now:** leak the slot; the next writer rebuild clears it.
- **Q?? (shm): the writer removes the old generation's name after abandoning it.** FORMAT §11 says a writer marks the old region abandoned and names the new generation in the directory; it does not say when the old name goes. Without removing it, every writer restart leaks a `/dev/shm` object (or a file in `shm_dir`) until the last process's `ShmRegion::remove`, which only knows the current generation. **Chosen for now:** `open(Role::Writer)` removes the old name right after the directory switch; mappings readers still hold stay valid and they re-attach by the new name.
- **Q?? (shm): `read_view` before any view is published.** Returns an empty `ViewRecord` with `view_version` 0 and the current manifest version, rather than an error: a reader that attaches before the writer's first publish sees "no tablets", and pinning view 0 means "no view", which is harmless because nothing is reclaimable yet. Flag if an error is preferred.
- **Q?? (runtime): background tasks at shutdown, and `compaction_threads` in application-owned mode.** The spec says the engine starts no threads in application-owned mode but also offers `compaction_cores(k)`. Interim behavior: `Runtime::application_owned` ignores `compaction_threads` and `pin_threads` (tasks run on the shards); `Runtime::shutdown` handles every queued message but drops unfinished background tasks, so the engine must finish or persist flush/manifest work before calling it. Pinning is best-effort where the OS has no affinity control (`Unsupported` is ignored); any other pin failure fails `Runtime::start` with `Error::Spawn`.
- **Q?? (wal): a segment rollover fdatasyncs on the shard thread.** FORMAT §10.1 rule 1 says a full segment is synced before its successor's header is written, and D30 says no fsync runs on a shard's foreground loop. `WalStream::append` resolves this with one blocking `sync_data` per segment (every 64 MiB by default), then buffers the successor's header with the group that caused the rollover. Alternative: defer the header write until a submitted sync of the old segment completes, which needs the shard to stall its next `write()` anyway. Interim behavior: the blocking per-segment sync.
- **Q?? (wal): a checkpoint may name a segment that never reached the disk.** The engine checkpoints positions whose data was flushed, synced or not, so after a power loss the manifest can hold `Lsn(e, X)` while no segment with epoch `e` exists (a `Buffered` commit opened segment `e`, was flushed and checkpointed, and `e` was never synced). FORMAT §10.1 says replay starts at the checkpoint's segment but not what a missing one means. Chosen for now: `Recovery` treats the missing segment as one that stopped exactly at `X`: a successor chained to `(e, X)` continues the log, otherwise the log ends at the checkpoint, and a header with an epoch above `e` that nothing reaches is reported as corruption. `into_stream` always picks an epoch above the recovered end's epoch (not just above every header), so the new segment never chains to its own epoch.
