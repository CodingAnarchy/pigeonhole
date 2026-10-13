# Decisions made in Phase 3 (D194–)

Indexed in [README.md](README.md). Numbers are permanent and continue from Phase 2; code and docs cite them as `Dn`.

<a id="d194"></a>
## D194 — Reads skip superseded memtable versions through a writer-only, process-local stale-tail index (approved; owner decision, 2026-10-09; memtable, engine, compaction, #387; alternative deferred as #397)
**Owner decisions.**
1. A writer-only acceleration structure is acceptable. Reader processes have no index: they read the same data with the same results, at today's cost.
2. The alternative, a skip offset stored in the shared-memory node, is not taken now. It would serve reader processes too, but needs FORMAT and `ShmLayoutVersion` changes, and since nodes are written once it could only point backward. It is kept open as #397 (Phase 4), to revisit before 1.0 only if reader-process latency matters.

**Why.** Under D193 the remaining Phase 2 gap is the wide-row read tail. On sparse-wide's hot rows a row read or scan steps, per returned cell, about 2.05 memtable entries and 0.017 SST entries: about 1.05 superseded memtable versions per cell. Each one passed costs about 460 instructions (Linux callgrind), whichever source it comes from: the merging heap re-sift, the resolver's column check and the source's own advance (about 63 of the 460 for a memtable). The resolver already stops resolving a column once its version limit is reached (`col_skip`), but it still steps every remaining version, up to `SKIP_STEPS` (8) before it seeks. Making the step cheaper was tried and measured: a tight skip loop (#375, +3.4% flushed), a runner-key cache (#377, +1.1–1.2%), other `SKIP_STEPS` values (all worse than 8), and an early flush (#383/#386, which moved the versions into L0 and made the gate p99 worse). What is left is not stepping the stale versions at all. A fully compacted hot row reads at about SQLite parity (about 1,340 instructions per cell against about 1,300), so the stale memtable versions are most of the gap.

**The idea.** When the writer inserts a node `e` whose level-0 successor at insert time is in the same column, it records `tail[e]`: that successor's own `tail` entry if it has one, else the successor. `tail[e]` is therefore a node of the same column after `e`, as far down the column's version chain as was known when `e` was linked. A reader that has finished a column (the resolver has set `col_skip`) and whose merging cursor's top source is that memtable, positioned on a node `e` with a `tail[e]` entry, moves that one source to `tail[e]` and then steps it, inside the memtable cursor and without the heap or resolver, while the key is still in the column. Then it re-sifts the heap once. A missing entry, any other source on top, or a cursor from a reader process means stepping exactly as today.

In the common overwrite pattern (each new version has a newer timestamp, so it is linked in front of the previous newest), the chain `p3 → p2 → p1` gets `tail[p3] = tail[p2] = p1`. A read that returns `p3` and finds `p2` next jumps to `p1` and steps once, to the next column. A hot cell with hundreds of versions is passed with one lookup and one step, instead of eight steps and a seek.

**Where it lives (FORMAT unchanged).** The index is process memory owned by the writer's `Memtable`, never in the arena or the shared layout. The skiplist nodes, the header, `ShmLayoutVersion` and FORMAT §11 stay exactly as they are, so nodes are still never modified after linking. `Memtable::reader()` hands its `MemtableReader` an `Arc` of the index, and `MemtableReader::open` (reader processes, from a published view) gets none. The index is a fixed-capacity, write-once, open-addressed table of `(node offset, tail offset)` pairs in `u32` atomics, sized from the memtable's chunk budget. The writer fills an entry before it links `e`, so a reader that reaches `e` finds the entry or, if the table was full, falls back to stepping. Entries are never changed or removed, and the table is dropped with the memtable. That lifetime is covered by the `Pin` the reader and its `ArenaSlice`s already hold, because `tail[e]` is always an offset in the same memtable.

**Correctness.** The claim is that every node passed over by a jump is one the read would have stepped past without effect.
1. **Same column, older.** Nodes are only inserted, never removed, and key order is row, family, qualifier, then timestamp descending, then seqno descending. Every node between `e` and `tail[e]` in key order, including nodes linked after the entry was written, is in `e`'s column and sorts after `e`.
2. **Only after the column is finished.** The jump is taken only where the resolver already discards the rest of the column (`col_skip`: the version limit, D76's folded `versions`, is reached, or a predicate failed). There, every remaining entry of the column, from every source, is discarded today. The jump changes how this source passes them, not which entries count.
3. **Deletes (D38, D9).** A `CellDelete` at timestamp `T` hides every version at exactly `T`, whatever its seqno, so it can "un-shadow" nothing older but must be seen before a version at `T` is returned. The resolver already processes every entry at the returned version's timestamp, across sources, before it returns that version and sets `col_skip`, so a delete at the same timestamp is never jumped. Deletes at older timestamps (cell, column or family) only hide versions the read has already decided not to return. Family delete markers sort before the family's columns, so a jump inside a column never passes one.
4. **Snapshots (D191) and time bounds.** The jump starts only after the resolver has applied the read's snapshot and timestamp bound to the column. Entries the snapshot cannot see, or above the time bound, are already skipped by those rules; entries below are discarded by (2). So the index needs no seqno and gives the same result at every snapshot.
5. **Merge operands and counters (D186).** A column whose returned value is still accumulating operands has not set `col_skip`, so its operands are stepped as today.
6. **Concurrent writer.** A reader that loads `next[0]` from `tail[e]` uses the same Acquire load as every step, so new links behind the tail are seen or not exactly as today.
7. **Compaction, flush and backup.** They do not use the index: their cursors have no version limit, and flush GC (D191) reads its memtable through the resolver-free `StreamGc`.

**Reader processes.** They have no index and keep stepping, with unchanged results and cost.

**Expected effect and costs.**
- **Reads:** about 1.05 stale steps per cell (about 480 instructions) become a lookup (a few tens of instructions) and one in-memtable step on the hot rows. That is roughly 10–15% of the as-written hot-row read and scan cost (about 2,560 instructions per cell), and more on the hottest cells, which set p99.
- **Writes:** each insert whose successor is in the same column costs one table insert. `commit-one` and `commit-sixteen` must stay within their floors (D193).
- **Memory:** two `u32`s per indexed node, plus slack, in process memory. That is roughly 10% of the arena bytes on the gate's node sizes. The capacity is fixed per memtable, so it never grows.

**Enabling it (amended by D199).** The index is on by default for the writer process; `EngineOptions::memtable_tail_index` stays an option, so it can be turned off. It ships only with its equivalence, loom and Miri tests and sweeps passing. The write cost it adds is accepted for the read gains (D199): about 200 instructions per overwrite commit (`commit-overwrite` +1.57%) and about 100 per timestamped append (`commit-at` +0.88%). The wide-row read and scan p99 with the index on are reported in the #405 gate run, which no longer decides enabling.

*History: Amended by D199 (on by default, its write cost accepted; it was built off, to be enabled only if no shape regressed beyond noise and a gate run moved the wide-row p99).*

<a id="d195"></a>
## D195 — Three instruction ceilings are raised to main's counts, and a change to the ceilings is measured on the current main (owner decision, 2026-10-09; bench, CI; amends D193)
**Decision.** The D193 ceilings for `flushed` (1905 → 1912) and `compacted` (1282 → 1288) are raised to main 99f8929's counts times their 0.3% headroom. `get-sst` is raised from 17883 to 17983, with its headroom raised from 0.3% to 0.5%.

**Why.** The ceilings were measured on a main without #384 (scans reuse their resolvers), which merged just before them. #384 raised `flushed` by 0.4% and `compacted` by 0.5%, within the per-change thresholds, so main was above both ceilings as soon as they took effect. `get-sst` drifted 0.36% on main after #392 and #396, neither of which touches the SST read path: the code-placement variation already seen on other shapes. None of these is a regression to undo. With main above its ceilings, every PR failed the check.

**Process.** A PR that changes `crates/bench/baselines/instruction-ceilings.txt` merges only when its base is the current main, so its CI measured the code it sets the ceilings for. A PR whose base predates a change to that file is rebased before it merges. The coordinator's merge script enforces both. Any other raise still needs an owner decision (D193).

<a id="d196"></a>
## D196 — The async front door: a commit ticket is a handle, misses wake on I/O through a cache-only read tier, and a scan step's unpredicted miss reads synchronously (approved; coordinator and owner, 2026-10-09; pigeonhole, engine, sst, #42; amends the spec's "Sync and async"; full scan fix deferred as #398; amended by D210)
The spec's "Sync and async" section is built over the existing engine (#42). Three points needed a decision.

### Commit tickets are handles (amends the spec line on `commit_with_ticket`)
The spec says `commit_with_ticket` "returns a sequence number the caller can wait on or check later". A commit's seqno is reserved by its owning shard's group leader after submission, so it is not known when the call returns, and assigning one earlier would change the commit path.

**Decision:** `WriteBatch::commit_with_ticket(durability)` submits and returns a `CommitTicket`, an owned handle for the commit in flight.
- `wait()` blocks as `commit_with` does.
- `try_result()` checks without blocking (`None` while in flight; then the same `Result<CommitInfo>` every time).
- `seqno()` is `Some` once the commit resolved successfully.
- With the `async` feature, the ticket is also a future (`IntoFuture`).

Dropping a ticket, like dropping any submitted commit or commit future, does not roll the commit back. The ticket is available without the `async` feature, and it is the C ABI's form of "submit now, learn the outcome later" (D104's exportable list gains it). The async commit futures (`commit_async`, `commit_with_async` on `RowMutation`, `WriteBatch` and `Transaction`) wrap the engine's `PendingCommit`, the same future sync commits wait on. They submit at the call and resolve exactly when the sync commit would return. The commit path itself is unchanged; the engine only gains `Txn::submit`, the non-blocking half of `Txn::commit`.

### Reads that miss wake on I/O: a cache-only read tier (built in #42's second PR)
"Truly async I/O, not `spawn_blocking`", with memtable and cache hits `Ready` on the first poll, needs a read that can stop at a block-cache miss instead of reading the file. **Decision:**
- An SST block read gains a tier. Sync callers keep reading through the file and stay bit-identical (the instruction ceilings must not move). A cache-only read returns a would-block error naming the block instead of reading it.
- An async get or row read takes its view and seqno on the first poll and holds them across polls. On a would-block it submits the block's read through the VFS's completion (`submit_read`), admits the decoded block to the cache and keeps it pinned when the completion wakes it, and then reruns the read. The blocks it already fetched are hits now.
- The internal sst/engine API change is ICR 0014.
- **Fallbacks, counted** in `Metrics::async_sync_reads` (public as `Pigeonhole::async_sync_reads`):
  - A separated value larger than the blob cache limit (`min(MAX_CACHED_RECORD, cache/8)`) is read synchronously inside the async read. This is option (a), approved by the owner; the alternative, keeping the fetched record in the future, is folded into #398.
  - Smaller separated values are fetched asynchronously like blocks (#42's PR 2b): the header of each blob extent the record touches, verified once, then the record, read in pieces joined into one completion when it spans extents.
  - A block the cache cannot keep (capacity 0, or larger than a cache shard) makes the read synchronous for its next attempt, as do more than 64 fetches for one read.

### Scan streams prefetch, and an unpredicted miss inside a step reads synchronously (owner decision; #398)
A scan step can need a block in the middle of the merging cursor's and the resolver's work. Restarting the step there, as a get restarts, would need resumable steps through those layers. **Decision:**
- `Scan::stream` asks each source for the block its next step will load and fetches the uncached ones asynchronously before it steps. Blocks are fetched only as the consumer polls (the spec's backpressure).
- A step that still misses (a block it did not predict, for example after a long skip) reads that one block synchronously. Each such read is counted in a visible counter, and the behavior is documented on `Scan::stream`.
- The full fix, resumable steps, is deferred as #398 (Phase 4).

### Which calls get an async form (owner decision, 2026-10-09)
The spec says every operation has a sync and an async form. **Decision:**
- Every data operation has both. `RowMutation::commit_if` (through an engine `submit_check_and_mutate`, ICR 0018) and `Transaction::get` gain theirs in #42's fifth PR. The `async` item of #406 is checked only after that PR.
- `flush_async` and `compact_async` are added in the same PR. The engine already hands out pending handles for both, so they use the commits' waker path and start no thread.
- `backup` and `shrink` stay sync-only: they are rare and long-running. The user guide shows async applications how to run them on the executor's blocking pool (`spawn_blocking`).
- `open`, `open_reader`, `open_application_owned`, `close`, table create and open, and `drop_table` stay sync-only, because they are short.

<a id="d197"></a>
## D197 — The Phase 3 gate is binding on its contents: every roadmap item and every Goals-table target, per the #406 checklist (owner decision, 2026-10-09; all crates, bench; amends the spec Phase 3 gate)
**Decision.** Phase 3 passes only when every item of the [#406](https://github.com/CodingAnarchy/pigeonhole/issues/406) checklist is checked, each with its measurement or PR linked, and the Phase 3 milestone is empty. The spec's latency wording ("p50 and p99 targets in the Goals table met; within 1.5× of RocksDB") stays and is one part of it.

The checklist covers:
- **The roadmap contents:** io_uring (#402), direct I/O over the owned buffer pool (#403), the tuned owned block cache, the row cache (#404), group commit tuning, and the async Rust API, default-on (#42).
- **Every Goals-table target:**
  - point gets in memory and on cold data;
  - durable batched writes, both throughput and p99 commit;
  - single-family scan throughput;
  - open to first read;
  - thread-per-core scaling;
  - the 1.5× RocksDB comparison on every benchmark workload;
  - the D193 instruction ceilings.

**Measured on the reference hardware.** Timing items are measured on the reference hardware (spec, Goals: Linux 6.x, enterprise NVMe with power-loss protection, io_uring available), tracked in #405. macOS, consumer SSD and Windows results are reported, never gating.

**Why.** The latency targets alone could pass while a roadmap item, such as io_uring or the row cache, was never built. And Goals-table targets that the gate wording doesn't name could go unmeasured: write throughput, scan throughput, open time and scaling.

**How an item leaves.** An item may leave Phase 3 only by an owner decision that says where it goes (an issue in a later milestone). The same rule applies to a feature measured not to help, such as a row cache that never pays off: removing it, or keeping it off by default, is an owner decision recorded here or in a later D-entry.

<a id="d198"></a>
## D198 — A commit crosses threads with a bounded, adaptive spin before parking; combining only on evidence (owner decision, 2026-10-09; engine, runtime, #64; from proposal C, #416)
**Why.** A buffered commit's latency is mostly the thread handoff, not the work. `crates/bench/examples/commitpath.rs` ran buffered one-field overwrites (ycsb-a's write) from one client to one shard, under several shard drivers:

| how the shard runs | macOS p50 / p99 µs | Linux runner, run 1, p50 / p99 µs | Linux runner, run 2, p50 / p99 µs |
|---|--:|--:|--:|
| engine shard thread (today) | 4.96 / 10.46 | 33.02 / 45.77 | 25.05 / 52.32 |
| application thread that parks (the same wakeups) | 5.92 / 10.79 | 31.87 / 46.10 | 24.49 / 50.62 |
| shard spins, client parks | | | 13.00 / 25.48 |
| shard parks, client spins | | | 7.70 / 16.34 |
| both spin | 3.67 / 7.92 | 3.71 / 7.64 | 3.36 / 8.52 |
| inline: the client runs the shard right after submitting | 1.29 / 3.21 | 3.11 / 7.19 | 1.70 / 4.10 |

macOS is an Apple M5 (not quiet, indicative). The Linux runner is GitHub `ubuntu-latest` (`commit-latency.yml`), a VM.
- **The CPU work** is the `inline` row: about 1.3–3 µs, and about 10.6K instructions per commit on callgrind.
- **Crossing threads** with nobody sleeping adds about 2–3 µs.
- **Wakeups** add about 2 µs on macOS, and 20–30 µs on the Linux VM, where they are about 90% of a buffered commit.
- **Both sides matter:** on Linux run 2, spinning only the client saves 17 µs and spinning only the shard 11.5 µs; the savings overlap.

RocksDB writes on the caller's thread (a write-group leader), which is the main reason `ycsb-a` and `ycsb-f` p50 were 2.2–2.5× RocksDB's in the Phase 3 baseline (`docs/bench.md`).

**Owner decisions.**
1. **Build C1: a bounded, adaptive spin before parking.**
   - **Client:** `PendingCommit::wait` spin-polls its completion before it parks.
   - **Shard:** an engine-owned shard thread spin-polls its queue, once it drains, before it parks.
   - **Adaptive:** each side skips the spin when its recent waits were long (a durable commit waiting for a sync, a stalled shard), and a spinning wait yields between polls.
   - **Not covered:** application-owned shards, whose loop is the application's.
2. **C2 (combining) only on evidence.** A committing thread that runs its idle shard is acceptable in principle only if C1 is measured not to be enough for the Goals-table commit targets.
   - There is no C2 work until then, and it comes back to the owner with that evidence.
   - C2 would change the spec's "every write executes on exactly one pinned shard thread".
3. **C1 defaults.**
   - On for engine-owned shards once measured: about 15 µs on the client and about 50 µs on the shard, tuned on #405.
   - This holds provided the idle-CPU test still passes: an idle database never keeps spinning.
   - Both windows are documented options that can be set to 0, for battery-powered or CPU-constrained users.

<a id="d199"></a>
## D199 — The stale-tail index is on by default; its write cost is accepted for its read gains (owner decision, 2026-10-09; memtable, engine, bench, #387; amends D194)
D194 built the index off by default. It was to be enabled only if no shape, the write shapes included, regressed beyond noise, and only if a gate run moved the wide-row p99. Measured on Linux callgrind, same binary, index off against on (run 38008347250, on main with the insert fix that reuses the level-0 successor):

| shape | off | on | change |
|---|--:|--:|--:|
| as-written (hot row) | 2579 | 2096 | −18.7% |
| ycsb-c | 84008 | 48118 | −42.7% |
| ycsb-a (half overwrites) | 46614 | 30339 | −34.9% |
| row, rows, scan, short-scans | | | −0.3% to −0.7% |
| commit-one, commit-sixteen, flush, compact, other read shapes | | | within ±0.17% |
| commit-overwrite | 12526 | 12723 | +1.57% |
| commit-at | 11654 | 11756 | +0.88% |

The two write shapes pay for the index's bookkeeping:
- **An overwrite's** successor is the version it supersedes, so it records a tail: a column compare, reading the successor's tail and writing its own. That is about 200 instructions per commit.
- **A timestamped append's** successor is another row, so it pays the column compare and records nothing (about 100).

**Owner decisions.**
1. **The marginal write cost is accepted** for the read gains. This amends D194's rule that no shape may regress beyond noise.
2. **The index is on by default for the writer process.** The option stays, so it can be turned off. It ships only with its equivalence, loom and Miri tests and sweeps passing.
3. **Ceilings:** the PR that turns it on raises only the `commit-overwrite` and `commit-at` ceilings, by the measured amount, citing this decision. It lowers the read-shape ceilings the index improves (`instruction-ceilings.py lower`, based on current main, D195). Nothing else is raised.
4. **Trimming the write cost** (a cheaper column test; no tail for short chains) stays a separately measured follow-up. If it wins, it lowers those two ceilings again.
5. **The #405 gate run** still reports the wide-row read and scan p99 with the index on; it no longer decides enabling.

<a id="d200"></a>
## D200 — A WAL rollover never waits on the shard thread for the full segment's sync: only the successor's header waits, and its records are written meanwhile (coordinator decision, 2026-10-10; wal, engine, #19; amends D30)
**Before.** D30 let a rollover submit the full segment's sync when a spare slot was ready, but the next `write` waited for that sync before writing the successor's header. With no spare ready, the rollover synced inline (D30's exception).

**Now (#19 PR 1).** The rollover submits the sync and holds back only the successor's header frame:
- **Records go first.** The records after the header are written as they come, into the slot, at their offsets. FORMAT §10.1 rule 1 now says explicitly that this is allowed.
  - Until the header is written, the slot keeps a stale header: zeros, or a recycled slot's older epoch, at least two below the new one.
  - That header cannot be chained to the full segment, and replay stops at the first fragment of another epoch, so the early frames are never replayed.
- **Nothing is acknowledged on them before the header.** `Wal::written` stays at the full segment's end while the header is held, so a Buffered commit in the successor is not met by a `write`.
  - The engine resolves such a group through a sync, which writes the header first.
  - A Buffered ticket is met by `written` or by `durable`, since durable implies handed over.
- **A sync submitted meanwhile** is chained: the rollover sync completes, then the header is written (a submitted write), then the group's `sync_data`, then the completion resolves.
- **The stream releases the header itself** on its first write after the rollover sync completed, checked without waiting.
- **The edge.** A segment nearly full (a quarter or less left) while its own header is still held: `Wal::blocked` holds the engine's next group back, and `Wal::notify_unblocked` kicks the shard when the sync completes.
  - Counted: `WalCounters::rollover_blocks` (episodes), and `Metrics::wal_rollover_blocks`.
  - A single group larger than that quarter would still wait, counted in `WalCounters::rollover_waits`. At the default 64 MiB segments it should never happen.
- **D30's exception is dropped (#19 PR 2).** With no recyclable or prepared slot ready, a segment's last quarter also makes `Wal::blocked` true.
  - The engine prepares a spare at once, ahead of its usual "half a segment in" schedule. The preparation wakes `Wal::notify_unblocked`'s waiters.
  - So a rollover never grows the file or syncs inline in steady state. `SpareSegments::prepare` now zero-fills blank slots instead of counting them as spares.
- **What is left inline: two counted fallbacks, never steady state.**
  1. A group larger than the segment's last quarter.
  2. A failed preparation (a full disk, say). The stream then stops waiting and rolls over inline, which reports the failure.
  - `inline_rollover_syncs`, `inline_grows` and `rollover_waits` count them.
- **A permanent debug guard checks all of this.**
  - The engine wraps its group commit in `StrictForeground`. Inside it, a blocking sync or wait reached from `append`, `write` or `submit_sync` panics in debug builds, except in those counted fallbacks.
  - Release builds compile it out.
- **Checked by:**
  - the wal crash sweep with deferred I/O, crashing inside the window over recycled slots (closest stale epoch two below the held one): no acknowledged commit is lost, and no early frame replays;
  - a process-kill sweep inside the window, for Buffered;
  - a mutation check: acknowledging Buffered records under a held header loses commits.

<a id="d201"></a>
## D201 — The row cache: per-row write watermarks as epochs, latest reads of the newest version only, gets consult without filling (approved; coordinator, 2026-10-10; cache, engine, pigeonhole, #404; plan approved, measurement decides the default)
The spec lists a per-family row cache "for small, very hot rows, keyed by (row, family, snapshot-epoch)", and `RowCache` exists, keyed by an engine-supplied epoch. #404 wires it.

### The epoch is a write watermark per row
Invalidating an entry on write races with filling it. A reader that read the row before a write can store its stale copy after the write invalidated the old one. So entries are never invalidated. Instead:
- `RowEpochs` (cache crate) is a table of `u64` watermarks indexed by a hash of `(table, row)`. It's sized from the cache, one slot per 256 cache bytes, at least 4096.
- **Writer.** A shard raises the row's slot to the commit's seqno (`fetch_max`, Release) when it applies the commit's first write to the row. That's before the write goes into the memtable, so before anything can make it visible. Cross-shard commits do the same on each shard. Replay skips it, since the cache starts empty.
- **Reader.** A latest read takes its read point (view and seqno `S`), then loads the row's slot `e` (Acquire). Every write at or below `S` was applied before `S` became visible, so `e` covers it.
- **Fill:** the family row read at `S` is stored under epoch `e`, only if `e <= S`. A larger `e` is a write applied but not yet visible.
- **Hit:** only if the entry's epoch equals the slot now and the slot is at most the reader's `S`. A later write raises the slot past the entry's epoch, so the entry misses from then on.
- **Collisions:** rows sharing a slot cost each other misses, never a wrong row, because `RowCache` verifies the family and row.
- **Checked by** a loom model (writer, filler and reader race; a hit always returns the row as of the reader's read point) and by the model check with the cache on.

### What does not change an entry
An entry holds only the newest visible version of each cell of one family row, copied.
- **Flush and compaction** never change the newest visible version: D191's GC drops only what no snapshot can see, and D186 says compaction never changes counter reads. Neither does blob GC, since the values are copied.
- **TTL** does change it. An entry records when its first cell expires (`ts + ttl`, the resolver's rule) and misses from then on.
- **Tablet splits, moves, merges and shard-count changes** don't matter: the cache and watermarks are engine-global and keyed by row, and both start empty at open.

### What it serves
- **Served:** row reads at the latest view (sync and async) that ask for the newest version (`versions == 1`) with no time range. Qualifier prefixes and ranges, column limits and value filters are applied to the cached cells. Each is a function of the family's newest cells: D22's value filter tests the newest visible value.
- **Stored:** a miss stores the family row only when all of these hold:
  - the read was unprojected;
  - the row missed before, at any epoch (a ghost tag per slot), so rows read once aren't stored, and a hot row that was written refills on its next read;
  - the encoded row fits `row_cache_max_row` (default 4 KiB). A row over the cap gets a small marker instead, so reads until its next write don't encode it again.

  An empty family row is stored too.
- **Lookups:** a per-slot tag of the last stored family row is checked first. Only a matching tag takes the cache's lock, so a miss is two atomic loads. The tags are hints; the cache verifies every hit.
- **Point gets** (latest, sync and async) look for a cached family row and answer from it, including "no such qualifier", but never fill.
- **Bypass the cache:** snapshot reads, multi-version and time-range reads, scans and transactions.
- **Reader processes** have no watermarks, so no row cache. Like D194, this is writer-only and process-local.

### Options and cost
- `Options::row_cache(bytes)` (0 means off; the default was 0, and is 64 MiB since [D216](#d216)), `row_cache_max_row(bytes)`, and `row_cache_family(table, family)`, called once per family; without any call, every family is served. All are process-local, with no FORMAT change. A stored per-family switch can come later if users ask.
- **Cache off:** no watermark table, one predictable branch on the commit apply path and on latest row reads and gets. Every shape must be flat with the cache off.
- **Cache on:** a hit copies the cached cells into the caller's row. A write pays one `fetch_max` per row it touches.
- **Linux callgrind, same binary, off vs. on:**
  - ycsb-c −82%, ycsb-a −41%;
  - uniform-random cold row reads +1.3%;
  - gets of rows never cached +1.5%;
  - commit-overwrite +0.6%;
  - a hot row over the cap: flat.
- **Counters:** `Engine::row_cache_stats` and `Pigeonhole::row_cache_stats` report hits, misses and fills.

### The default follows the data (D197)
Measured on Linux callgrind (hot-row states, ycsb-c and ycsb-a, the write shapes with the cache on) and on the gate machine's wall clock. The default stays off unless a hit clearly beats the D199 index's miss path and ycsb-a isn't made worse. If it never pays off, `Options::row_cache` is removed rather than kept as a no-op (owner decision).

The internal interface change is ICR 0019.

<a id="d202"></a>
## D202 — Application-owned io_uring loops wait on a completion fd: a deferred-taskrun ring signals its registered eventfd before its owner reaps, and the fd is an opt-in (coordinator decision, 2026-10-10; io, pigeonhole, #408)
**The fd.** Each io_uring thread ring registers a `notify` eventfd (`IORING_REGISTER_EVENTFD`), signalled on every completion and drained by each reap. It is exposed as `pigeonhole_io::own_io_fd` and `Shard::io_fd`, so an application-owned loop can wait in its own `poll`/`epoll` instead of polling.

**The DEFER_TASKRUN question, settled on CI.**
- Thread rings are `SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG`: their completion work runs only when the owner enters the ring. If the kernel signalled the eventfd only once that work ran, a loop waiting on the fd would sleep through its own I/O.
- It does signal earlier: on the Linux CI kernel, `the_completion_fd_turns_readable_before_any_reap` (#442) turned the fd readable in `poll()` with no reap.
- So application-owned rings keep `DEFER_TASKRUN`. The fallback, dropping it while keeping `SINGLE_ISSUER`, was not needed.

**The fd is an opt-in.**
- `Shard::next_wakeup` treats a thread's in-flight I/O as due now (`Some(0)`, #402) until the application has taken the descriptor (`Shard::io_fd` returned `Some`). Only then does it report background deadlines alone.
- A loop that never asks for the fd keeps the old behavior. #442's first CI run showed why this matters: the `open_application_owned` doctest's loop parks for `next_wakeup` and never polls the fd, and it hung.

**No reaper in application-owned mode (#443).** The shared ring, for threads without a ring of their own, has no reaper thread there:
- **Every reap takes the shared ring's completions too.** Each driving thread's reap looks at the shared ring's completion queue (no system call) and takes what is there.
- **The fd covers both rings.** A driving thread's fd is an epoll descriptor over its own ring's eventfd and the shared ring's, so client completions also turn it readable.
- **Not an io_uring poll of the shared ring's eventfd.** #443's first version used one, and hung on CI after the first client completion: the kernel does not let an eventfd that io_uring signals wake another io_uring poll reliably (`EPOLL_URING_WAKE`).
- **A blocked wait with nobody else to reap for it** reaps the shared ring itself. That covers a blocked wait on one of the shared ring's completions, and on a completion chained after them, such as a WAL's ordered sync. The shared ring registers as an orphan in `pigeonhole-io`, and `Completion::wait` reaps orphans in 1 ms slices.
  - #443's first version hung at open on such a chained wait.
- **The limitation:** a client thread's *async* I/O (an executor's `get_async`) then completes promptly only if the driving loop waits on `io_fd`. Otherwise it completes at the loop's next timed turn. Documented in the async guide.
- **Moving a shard between threads** (#492, ICR 0029): call `Shard::release()` on the old thread first. It drains that thread's ring, which only it can reap, and debug builds panic on a move that skipped it. Then take `io_fd` again on the new thread.

<a id="d203"></a>
## D203 — Open does no blocking flush on a clean reopen; the first durable commit waits for the flushes a read does not need (coordinator decision, 2026-10-10; engine, pager, wal, io, #158; ICR 0021; amended by D211)
**Measured** (`examples/openlat.rs`, #450): on a clean reopen of a small database the open waited for five flushes in a row. A read needs none of them:
1. the pager's `finish` `sync_all`;
2. a manifest root commit (2 `sync_data`) that published nothing;
3. the new WAL stream files' `sync_all`s;
4. their directory's `sync_dir`.
- On Linux CI that cost little: open to first read was 1.4 ms, under the spec's 5 ms.
- On macOS each is `F_FULLFSYNC`, which made 16.8 ms. After this decision it is 1.1 ms.

**Now:**
- **No empty manifest commit.** `flush_recovered` commits only when there is something to publish: a stream to replay, something spilled, or counters that moved.
  - After a clean close the WAL streams are removed, so a clean reopen publishes nothing and skips it.
  - The superblock's clean flag then stays set until the next commit. It is informational only (D57).
- **The page file's length is synced at open unless the last close was clean and the file is exactly as long as that root recorded (`file_pages`).**
  - That length was made durable by the growth or truncation that set it (each syncs before it is used), or by an earlier open.
  - A growth or truncation a crash interrupted leaves the length longer or shorter, so it is synced.
  - A clean flag left stale by a session that committed nothing (the bullet above) is safe: such a session cannot have changed the length durably without it differing from the recorded one.
- **The new WAL streams' file syncs and their directory sync are submitted, not waited for** (`Vfs::submit_sync_dir`, ICR 0021).
  - Each takes a ticket in its stream's sync ordering (D58), so the stream's first durable sync (a GroupSync or Sync commit) counts only once they have finished, and a failed one poisons the stream.
  - Buffered commits need no flush.
  - **No manifest checkpoint names a new stream until both have finished (D211).** A shard holds its checkpoint back until then.
  - **Recovery syncs the directory entry of every stream it adopts** (`Recovery::open`, beside its existing length sync). A process can now die before its creating open's directory sync runs. The next open adopts the file and acknowledges commits in it, and a power loss would then take the file's name. #454's first deferred-I/O sweep found exactly that: a process crash, then a recovering open, then a power loss, losing acknowledged commits. A clean reopen adopts nothing (its streams are new), so it still waits for no flush.
- **The cost of a clean reopen** that stays on the open path is opens, locks, shared memory and thread start: about 1 ms on macOS.

**Checked by** (permanent suites):
- `pigeonhole/tests/open_durability.rs`:
  - **No blocking flush:** a clean reopen makes no blocking `sync_data`, `sync_all` or `sync_dir` on the opening thread. Putting back any one of the three fails it.
  - **Crash sweeps:** power loss after every mutating operation of a reopen and its first GroupSync commit, after a clean close and after a process kill (the recovering path), with deferred I/O. Nothing acknowledged is lost.
  - **Ordering:** with the directory sync held (`SimVfs::hold_dir_syncs`), a GroupSync commit after a clean reopen is not acknowledged, and a power loss then loses nothing. Taking the directory sync out of the streams' ordering fails it.
- `wal/tests/open_io.rs`: a blocking sync right after `create_all`, with nothing else completing I/O, drives the open-time syncs to completion rather than waiting for ever (the hang the first sweep found). Mutation-checked: without the drive, or with fanned-out handles that lose it, it hangs. A second test runs a creating open whose directory sync a process crash drops, then an adopting open with a durable commit, then a power loss, and the commit survives. Without recovery's directory sync the stream file is gone.
- `engine/tests/model_check.rs` (`harness_regressions_from_the_seed_sweep`): seeds 99, 132 and 164 with deferred I/O, the sweep's lost acknowledged commits, as regressions.
- `pager/tests/open_sync.rs`: a clean close at the recorded length skips the length sync. An unclean close, or a length longer or shorter than recorded, syncs. Each condition is mutation-checked.

*History: Amended by D211 (a checkpoint for a new stream waits for its creation syncs; #520).*

<a id="d204"></a>
## D204 — The scaling gate measures application-owned shards writing inline; engine-owned synchronous clients are reported (owner decision, 2026-10-10; bench, #154; amends the spec's Goals scaling definition and the #406 checklist)
**Decision.**
- **The thread-per-core scaling target is measured with Pigeonhole application-owned, at N shards up to the core count.** Each shard thread drives its own shard and commits the `skewed-multi-shard` writes to rows its shard owns (`Table::shard_of`, ICR 0022; owner routing approved by the coordinator) inline, with 16 commits in flight (`commit_async`). The target is unchanged: N-shard write throughput ≥ 0.8 × N × single-shard, with no single-shard p99 regression. `phdb-bench scaling` runs it, and `docs/bench.md` defines it.
- **The engine-owned shape is reported, not gated:** synchronous client threads, at least four per shard, at N ≤ cores/2.

**Why.** The old gate ran N synchronous clients against N engine-owned shards: 2N busy threads.
- At N = cores the clients have no cores. Per-commit handoffs (two cross-thread wakes, a `pwrite`, the visibility wait) and spin-before-park polling (D198) dominate, so N shards ran no faster than one.
- Measured on main 7c0335a (`scaling_modes`, local): 0.60 at 2 shards with 2 clients on a 4-core Linux runner, 0.31 at 4; on macOS 0.17 at 4 and 0.11 at 8.
- With cores to spare, the same engine scales: 0.98 at 2 shards with 8 clients on Linux. The application-owned inline shape reached 0.73 at 2 shards on Linux, with no client threads at all.
- The spec's own thread-per-core model is the application-owned embedding (a writer on the owning shard runs with no handoff). That's the shape that can meet 0.8 × N at N = cores, so it's the one the gate holds the engine to.

**What follows.** The engine work toward the target is measured on this gate (#154). The first corrected profiles point at:
- the per-group `pwrite` on small groups;
- the global visibility watermark and its shared waiter list;
- cross-shard commits when a thread writes rows other shards own;
- tablet balance under skew;
- spin-before-park polling when cores are oversubscribed (a D198 follow-up).

Counters for the visibility and queue waits come first, so each change shows on the gate.

The #406 checklist's scaling item reads: "Thread-per-core scaling (D204: application-owned inline, N up to cores): efficiency ≥ 0.8, single-shard p99 not regressed; the engine-owned synchronous shape reported."

## D205 — The scan target counts decoded bytes on the bench's default shape (owner decision, 2026-10-10; bench, #29; amends the spec's Goals scan line and the #406 checklist)
**Decision.**
- **"> 1 GB/s decoded per core from cache" counts decoded bytes:** the row key, qualifier and value of every cell an ordered single-family scan returns, on one thread.
- **It's measured on the bench's default shape** (100-byte values, 8-cell rows; `scanrate rate`'s `narrow`), with the table compacted and every block cached, on the reference machine (`crates/bench/baselines/phase3-io/scan-rate.sh`, #405). `docs/bench.md` defines it.
- **The small-cell (16 B values) and wide-row (1,000 cells) shapes are reported, not gating,** and so are the memtable-resident runs.

**Why.** The spec didn't say which cell size counts, and the cost is per cell (about 45–50 ns on an Apple M5, almost independent of size), so the cell size decides the result.
- On the M5 (non-reference), the default shape scans at 2.2 GB/s, 16-byte cells at 0.6 GB/s, and 1 KiB cells at 8.2 GB/s.
- Two attempts to cut the per-cell cost were measured and dropped (#29: a group-end hint, +11.6% instructions per cell; a faster unescape, no gain).

**What follows.** #29 is met pending the reference-machine run. A direct single-source scan path, which would cut the cursor-stack cost per cell, isn't pursued now.

The #406 checklist's scan item reads: "Ordered row scan, single family: > 1 GB/s decoded per core from cache — #29 — measured as decoded bytes on the default 100 B / 8-cell shape; small and wide shapes reported (owner decision, D205)."

## D206 — WAL pin passes run in bounded slices; the WAL bound may be exceeded by what is logged while a pass runs (coordinator decision, 2026-10-10; engine, #474, #175; amends D155)
**Decision.**
- **A pass runs in slices.** A pass (a shard's own past `wal_pin_bytes`, or one served for another shard's `Unpin`) does at most `PIN_SLICE` units of work per shard turn (1,024: a record indexed, a slot examined, a cross-shard record examined or an ask pair processed, one unit each; an internal constant). So no pass blocks the shard thread for more than a slice.
  - Its slots are forced and its requests sent as its slices complete.
  - It selects the needed records logged before it started. Later ones are the next pass's.
  - A pass asked for while another runs waits. Served requests waiting merge into the highest `through`, which covers each of them.
- **The bound is amended.** The stream may exceed `wal_pin_bytes` by what the shard logs while a pass is in progress: one pass's worth of slices. Measured at the default sizes (64 MiB budget, 100-byte values, 4 shards, 10% cross-shard commits; Apple M5), a pass spreads ~4–13 ms of shard time over ~95–480 slices. That's a few MB of writes at most at high write rates, against a 128 MiB bound. The bound then holds as before.
- **Indexing never runs on the commit path.** The passes find what pins the checkpoint from an index of the log (per slot, the single-shard records that wrote it; cross-shard records by position and by seqno) instead of a scan of the whole log. The index is built in the same slices, and only while the unindexed log exceeds an eighth of the limit (internal), so a log that never comes near the limit is never indexed.
- **Asks are bounded.** A pass keeps one `through` per shard (O(shards)), applying `asked` per record as it goes, instead of building every (shard, seqno) pair. Forwarding stays once per record and shard, and the own pass always re-asks (D155).

**Why.** #175 measured each pass scanning the shard's whole log on the shard thread: ~11 ms per own pass and ~6 ms per served pass at the default sizes, and served passes are ~13× as frequent as own ones when commits cross shards. That's far past the 200 µs commit-p99 goal.
- An index alone halves that, but the cross-shard work left (~38–59k still-needed records at ~43 ns each, plus the asks) is 2–4 ms per pass whatever the index does.
- Keeping the index current on the commit path cost +1.0–1.4% on every commit shape (~135 instructions per logged commit), paid by workloads that never reach the limit.

**Checked.** Debug builds compare every pass with a scan of the whole log:
- the slots it forced were pinning when it started, and every slot pinning when it ends was forced;
- likewise the requests it saw.

What a pass forces is otherwise unchanged; the mixed-temperature harness gives main's pass and small-flush counts within one, and the same largest WAL.

<a id="d207"></a>
## D207 — A shard keeps a bounded number of group syncs in flight per stream; groups that need one meanwhile batch behind them (coordinator decision, 2026-10-10; engine, #412; the depth is decided on the reference machine, #405)

**Measured** (#412; scratch counters through the `commit-latency` workflow on a Linux runner, fdatasync on ext4 on a VM disk; and macOS, `F_FULLFSYNC`). `run_group` started a sync for every group with an unsynced `GroupSync` member, whatever was in flight:
- **A third of the syncs overlapped one already running** at 4 and 16 clients.
- **Arrivals waited only 8–128 µs to be grouped,** then mostly got a sync of their own: about 2 commits per sync at 4 clients, and 4.5 at 16.
- **D58 counts a sync once every older one finished,** so overlapping syncs buy no earlier acknowledgment.
- **On macOS,** where `F_FULLFSYNC` serializes, 83% of syncs covered one commit. 16 clients reached 578 ops/s with a p99 of 33 ms.

**Now:**
- **The cap:** a shard keeps at most *depth* group syncs in flight per WAL stream. A group that needs one beyond that gets no sync of its own; it waits in `unresolved`.
- **The batch:** when a group sync completes and groups wait, the shard starts one sync covering all of them. It resolves them through the newest group's id, as any sync resolves the groups up to it.
- **No timer:** the syncs in flight are the window, so a lone client never waits behind another sync.
- **Unchanged:** a group with a `Sync` member's own sync doesn't batch (that sync covers only the records before it), and rollover and side syncs (D200) are separate.

**The depth.** The measured A/B:

| | 16 clients ops/s | p99 | 4 clients ops/s |
|---|--:|--:|--:|
| Linux, depth 1 | 22.6K / 24.8K | 1,491 / 1,196 µs | 11.1K |
| Linux, depth 2 | 31.0K / 31.9K | 1,057 / 930 µs | 12.8K |
| Linux, unlimited (before) | 29.5K | 1,458 µs | 11.4K |
| macOS, depth 1 | 2.2K | 11.6 ms | 619 |
| macOS, depth 2 | 1.7K | 16.0 ms | 434 |
| macOS, unlimited (before) | 578 | 33.0 ms | 337 |

- **Linux file systems fold concurrent `fdatasync`s of one file into one journal commit,** so a second sync in flight keeps the device busy. `F_FULLFSYNC` serializes, so there a second one only queues.
- **Interim defaults:** 1 on macOS, 2 elsewhere. They're an internal constant, with `PIGEONHOLE_GROUP_SYNC_DEPTH` (a measurement variable; 0 is unlimited, the behavior before D207) so the gate run on the reference machine (#405) can compare 1, 2 and unlimited. That run decides the default (#476), and the variable is then removed or becomes an option.

**Checked by:**
- **`engine/tests/group_sync_batch.rs`:** 16 clients committing one at a time with the platform's depth. The first *depth* groups each start a sync; the rest batch. No commit is acknowledged before a sync that covers it. There are *depth* + 1 syncs in all. Mutation-checked: without the batching, it fails.
- **The crash sweeps:** `model_check` with and without deferred I/O, and the pigeonhole `model`. They're merge gates.

<a id="d208"></a>
## D208 — The Phase 2 gate's baseline moves to the reference machine: the AX102's two official gate runs (owner decision, 2026-10-10; bench, #387, #405; amends D193)

**Why.** D193 made the Mac's official runs 5–6 the baseline that every later official gate run is compared with. But the Phase 3 gate and every release are judged on the reference machine (D5, #405), and run-to-run limits don't transfer across machines. A Linux runner's gate run "fails" `check.py` against the Mac baseline (#387, run 38098364056) on code that passes it on the Mac.

**Now:**
- **The reference baseline:** the two official `phase2-gate` runs of the first AX102 gate window (`run-window.sh` steps `phase2-gate` and `phase2-gate-2`, #509), committed beside the Mac runs in `crates/bench/baselines/phase2-gate/`. From then on, `run-gate.sh` compares and `check.py` checks against them.
- **Until they're committed,** the Mac runs 5–6 stay the baseline. They remain in the repository as the Phase 2 record.
- **D193's other rules are unchanged:** the same gated measures and noise allowances (re-derived from the AX102's pair if its spread differs, with the owner's agreement, as D193 requires for any widening), and the same "fixed or accepted by the owner" rule.
- **#387's ratio to SQLite EAV** (row-read and scan p99, the documented gap) is judged on the same two runs. On the Mac at main 2d6b6ed it was 1.58× and 1.27× (D193: about 2.0× and 1.6×).

*Checked by:* the first gate window's report (`report.py`), whose Phase 2 rows read both runs.

<a id="d210"></a>
## D210 — Async commits wait for room before they submit: a per-shard bound on async commits in flight; a future dropped before it submitted writes nothing (owner decision, 2026-10-10; engine, pigeonhole, runtime, #149; amends D196)

**Why.** D196 made `commit_async` submit at the call. A shard queues whatever is submitted, so an async producer that outruns its shard (a long write stall, an application-owned shard its loop stopped running) queued without limit. Measured on #149: 200,000 × 4 KiB `commit_async` calls against a stopped shard returned in 116 ms and grew the process to 1,081 MiB, about 5.4 KiB per queued commit. Blocking commits never had this problem, because each holds its thread.

**Now:**
- **The bound.** Each shard holds at most half `memtable_budget` (2 MiB per 4 MiB arena; 32 MiB at the defaults) of async commits submitted and not yet resolved. Bytes are the encoded batch. A cross-shard batch is charged to its coordinator, the shard of its first row. The bound follows `memtable_budget`; there is no separate option (an `EngineOptions` override exists for tests).
- **Over the bound, an async commit waits before it submits.** `commit_async`, `commit_with_async`, `commit_if_async` and `Transaction::commit_async`/`commit_with_async` submit at the call when their shard has room and no earlier async commit waits. Otherwise the future waits in its shard's queue, in arrival order, and submits when it's first and fits. A commit larger than the bound goes alone once the shard holds nothing. A later commit never overtakes an earlier waiting one, even if it would fit.
- **The fast path.** While the async bytes in flight on all shards *together* fit the bound and no commit waits, a commit is charged to that engine-wide count with one compare-and-add, without routing, and submitted at once; every shard is then within the bound. Past that, a commit is routed to its shard and charged there (counting the unrouted bytes as possibly that shard's), or queued.
- **The charge is returned when the commit resolves,** on the resolving thread, whether or not anyone polls the future. It's a field on the completion, set from the waiting side (`Waiter::on_settled(Arc<dyn Settle>, u64)` in `pigeonhole-runtime`): no allocation.
- **A future dropped before it submitted writes nothing** (amends D196, which said dropping a commit future never rolls it back). Once submitted, dropping still does not roll the commit back. So "fire and forget" (`let _ = t.mutate(..).commit_async();`) is safe only while the shard is under its bound. When it's over, the dropped commit is lost. Keep the future and await it, or use a blocking `commit` or `commit_with_ticket`.
- **Blocking commits and `commit_with_ticket` are not charged and never wait for admission.** Their threads bound them, and their path is unchanged.
- **What it bounds.** The memory a *shard* holds for async commits. A waiting future still holds its own batch, so a caller that makes many futures without awaiting them holds that memory itself. Producers that await, such as a bounded `FuturesUnordered` or a task per request, are slowed instead of queuing in the engine.
- **On close,** every waiter is admitted and its submission fails with `Closed`.

*Checked by:* `pigeonhole/tests/backpressure.rs`:
- 200,000 commits against a stopped shard stay within the bound, and all commit in arrival order once it runs;
- a small commit waits behind an earlier larger one;
- a blocking commit is not held;
- a dropped waiting commit writes nothing.

The engine's `admission` tests cover a commit larger than the bound. The blocking commit shapes move by +9 instructions (the completion's `resolving` flag); `writepath commit-async`, an uncontended async commit, is a CI instruction shape with its own ceiling.

<a id="d211"></a>
## D211 — No manifest checkpoint may name a WAL stream whose file creation is not durable (coordinator decision, 2026-10-11; engine, wal, #520; ICR 0031; amends D203)

**Why.** D203 stopped open from waiting for new WAL stream files' file and directory syncs. The stream's own durable syncs were ordered behind them, but a manifest checkpoint was not.
- A shard could commit a checkpoint for its stream while the directory sync was still in flight. It does so as soon as records stop being needed: a refused attempt's PREPARE, a dropped table's rows, or a flush.
- A power loss then kept the manifest and lost the file's name.
- The reopen created the stream afresh, again at epoch 1, but kept the stale checkpoint. Replay and later checkpoints started past the new stream's first records, and the next crash lost acknowledged commits.
- #517's deferred-I/O tablets sweep found it (seed 273); #454 introduced it.
- 0.2.x isn't affected: its `create_all` syncs the directory before open returns (0 of 200 seeds failed on release/0.2.x).

**The invariant:** no manifest checkpoint may reference a stream whose file creation is not durable.

**Now:**
- **The WAL tracks it.** `Wal::created_durable` and `Wal::notify_created` (ICR 0031): a stream `create_all` made is pending until its file sync and its directory sync have both finished, then durable, or failed if either failed.
  - Streams recovery opens are durable at once, because `Recovery::open` syncs an adopted stream's directory entry first (D203).
  - Streams `WalStream::create` makes are durable at once, because it syncs before returning.
- **The shard holds its checkpoint back while pending** (`advance_checkpoint`). It arms one wake (`ShardMsg::Created`) that retries the checkpoint and the close once the creation settles. It never checkpoints a stream whose creation failed: the stream is poisoned anyway.
  - Nothing else waits: commits, flushes and reads go on, and a held-back checkpoint only keeps WAL records a little longer.
- **A debug assertion** where the `WalCheckpoint` edit is built.
- **Not chosen:**
  - *Waiting for the directory sync at open* (0.2.x's way): a clean close removes the stream files, so every clean reopen creates them afresh, and this would put a directory fsync back on every reopen, undoing D203.
  - *A recovery repair of stale checkpoints*: not needed while the invariant holds.

**Checked by:**
- `engine/tests/checkpoint_creation.rs`, deterministic, with the directory sync held:
  1. a fresh database's dropped-table rows make the stream's records unneeded;
  2. no checkpoint is committed;
  3. a power loss takes the stream file's name;
  4. the reopen takes Sync commits;
  5. a second power loss loses none of them.

  Without the gate a checkpoint is committed, and the end-to-end variant loses an acknowledged Sync commit.
- `wal/tests/stream.rs`: a created stream is pending until both syncs finish, and its wake runs then; a recovered stream is durable.
- `engine/tests/tablets.rs` (`crash_at_every_write_point_of_tablet_changes_regressions`): seed 273 with deferred I/O.
- **Sweeps:** deferred `tablets` joins the standard sweep set for engine, WAL and I/O changes (CONTRIBUTING).

<a id="d212"></a>
## D212 — The manifest keeps today's exclusion protocol; no single writer task (owner decision, 2026-10-11; engine, #134 step 2)

**Decided:** keep today's manifest writer design: one exclusion (`manifest_busy`), any of three kinds of thread may hold it, and the pump's commit in flight is held in `Flight`. Don't replace it with one long-lived writer task. Revisit at the public API / C ABI review, or if a new bug lands in these paths.

**Today** (`docs/design/concurrency.md`, "The manifest exclusion and pump"):
- **Three paths take the exclusion:**
  - the shard pump (flush, compaction, tablet changes, WAL checkpoints, large-value release);
  - an application thread that drains the queue itself (catalog changes, `shrink`, large-value separation in `submit`/`commit`, #230);
  - a thread that already holds it (the final close).
- **Three rules hold it together:**
  - whoever releases re-checks the queue;
  - a blocked thread takes over a pump's completed commit (`Flight::take_done`, #135, #166);
  - a blocked thread reaps its own I/O, then orphaned I/O (ICR 0028), and parks only after that, woken by releases, root-commit completions and its own request's answer.
- **The take-over and self-progress rules carry D150 (a)'s promise:** catalog changes and `shrink` are not refused on a thread that drives a shard.

**Why keep it.** The protocol was the riskiest coordination in the engine (#78, #81, #135, #166, #207, #306). It is now:
- **Loom-modelled** around the real `Flight` (`manifest/flight.rs`, #536):
  - one holder at a time and every request answered once;
  - a request queued while a thread holds the exclusion is never stranded;
  - a thread driving the pump's shard finishes its commit.

  The exclusion is a `Mutex<bool>` stand-in until loom ships tokio-rs/loom#391 (#533).
- **Pinned by mutation-checked engine tests:**
  - `milestone_b`'s release window;
  - the take-overs in `blocking_waits` and `deferred_waits`;
  - `manifest_wake` (a root commit's completion wakes a parked thread).
- **Documented** in `concurrency.md`.

The redesign's gain doesn't justify changing a public guarantee and adding API surface outside the API review.

**Rejected: one writer task.** It would own the manifest writer and the queue, and every other caller would only queue and wait.
- **Gain:**
  - about 5 primitives: `manifest_busy`, `Flight`'s three, and the `Shared::manifest` mutex;
  - the release re-check, the take-over and the manifest park/wake protocol;
  - the close gate's try-lock claim (the final close would become the writer's last message);
  - about 300 lines and two test hooks.

  Write commits never touch the manifest, so no commit shape would change.
- **Cost:**
  - **D150 (a) would change.** Application-owned mode runs no engine threads (`compaction_threads` must be 0; application-owned io_uring has no reaper), so the writer would live on an application-driven shard. A thread driving that shard can't block on the writer. So catalog changes and `shrink` would be refused there (`InvalidArgument`, before submitting). For a single-threaded event loop, which drives every shard, that means every blocking catalog call.
  - **New API surface.** Async catalog and maintenance variants: an ICR and C ABI work.
  - **Large-value separation in `submit`** (#230) would need restructuring. It can't block on the writer from the event loop, so the blob-file commit's reply would have to submit the batch.
  - **A new rule for losing the writer's shard** (dropped, or its thread panicked): fail all later manifest work with `Closed`, or move the writer, which is more machinery.
  - **Size:** about 3–4 PRs, each with loom models and the standard sweeps.
- **Also rejected: a writer task in engine-owned mode only.** That's two designs to keep correct instead of one.

**Revisit when:**
- **The public API / C ABI review** wants async catalog calls anyway. Those are the expensive part of the redesign, so it would then cost little more.
- **A new bug lands in these paths.**

The note this replaces was `docs/design/questions/engine.md` (#538).

<a id="d213"></a>
## D213 — The durable group-commit target is measured on a table split over every shard, with commits in flight inline (owner decision, 2026-10-11; bench, #64, #406)

**Decided:** the gate measures the Goals table's batched durable write target (> 1M cells/s across cores, p99 commit < 200 µs) with `phdb-bench group-commit`'s D213 shape:
- **N shards = cores**, application-owned.
- **The table is split up front, and the placement stays fixed.** It's cut at the quantiles of its rows, one piece per shard, before the load (`PigeonholeRunner::presplit`, through a doc-hidden `Table::split_over_shards` that issues one tablet split with an owner per piece). Every shard owns rows from the first commit. The balancer is not relied on, and it doesn't run:
  - The run opens with `tablet_balance(Duration::MAX, u64::MAX, u64::MAX)`. No pass falls due, and no skew or size threshold is ever reached. Tablet changes stay on for the split itself, and commits still track their tablets' load as by default, so the commit path is the default one.
  - The run fails unless every shard owns exactly one tablet after the split and again before the measured phase.
  - Splits, merges and moves during the measured phase are reported (`GroupCommit::tablet_changes`). Any of them makes `phdb-bench` exit non-zero after writing its files, and `report.py` marks the result invalid.
  - **Every shard spends its one balancer pass before the split.** With the balancer off, a shard still runs one pass at its first turn, and that pass counts the tablets it then owns as settled. On the AX102 at 32 shards, shards idle until the split met their cold piece in that first pass and moved it to the left neighbour's shard (consolidation): a run on f333d48 ended with up to 3 tablets on some shards and none on others. The bench now flushes before the split (a message to every shard), then pauses 100 ms. The fix is in the bench only; the engine is the one measured.
  - **Amended 2026-10-11.** The first version left the balancer on. On the AX102, the measured phase saw 65 moves, 4 merges and 1 split. A cold piece moved to its left neighbour's shard before the load, the spread check timed out, and the run had a 5.47 s max. The gate measures commits, not balancer churn.
- **Commits stay in flight inline**, the D204 driver: each shard's thread keeps `INLINE_IN_FLIGHT` (16) `commit_async` commits in flight on the rows its shard owns, between turns of its shard.
- **The commits are as before:** 4-cell `GroupSync` row mutations over 1,000 rows, 200,000 at `full`.

The report gives cells/s in total and per core, commit p50 and p99, and the busiest shard's share (`Suite::group_commit`). It also gives a per-stage breakdown:
- the engine's part of each commit, submission to resolution on its shard (`EngineCommit`, from the engine's own histograms, diffed over the measured phase);
- WAL rollover blocks and inline WAL syncs;
- tablet changes while measuring. The 1, 4 and 16 synchronous-client runs stay as a reported secondary, beside RocksDB, and aren't judged.

The window's setup step (`machine.py`) also times the data drive's raw fdatasync: 2,000 syncs, each after a 4 KiB write. `report.py` shows the p50 and p99 beside the 200 µs target, as the floor under any durable commit. It's measured there rather than in `setup.sh`, which runs before the data drive is formatted and mounted.

**Why.** #64's pre-rental measurement found the old step failing by construction:
- **One shard.** All 200k commits went to one shard: one tablet, never split or spread. "Across cores" was never measured.
- **Synchronous clients.** At 16 clients, 1M cells/s needs 64 µs per durable commit, fsync included. On a CI runner that's about one fdatasync (p50 63 µs).

**Why inline committers rather than async client threads:** it's the scaling gate's style (D204). No client thread competes with the shards for cores, so the result is the engine's, at the thread-per-core embedding the spec targets. Each shard's 16 commits in flight form its fsync groups. The commit latency is submit to durable resolution, the same as the scaling gate measures it.

**Not changed:** the target, the commit size, the row count, and the secondary sweep's numbers, which stay comparable with earlier runs.

<a id="d214"></a>
## D214 — The balancer acts on write skew only once it persists, and keeps acting until the shard is balanced (owner direction, 2026-10-11; tablets, #547; amends D134 and D146)

**Decided:** write skew (D134 item 2, D146) moves or splits a tablet only when it is sustained:
- **Persistence.** A shard starts acting on write skew after its load has been above `balance_skew` (1.25) times the mean, and at least `balance_min_writes`, on **10 consecutive balancer passes** (1 s at the default interval). Any pass below resets the count. The count runs on every pass, dwell or not.
- **Slower average.** The moving averages of write load (a shard's, published for the others, and each tablet's) weigh the newest interval **0.2**, not 0.5.
- **Hysteresis.** Once acting, a shard keeps moving or splitting after each 10-pass dwell (D146) while its load stays above **1.1 times the mean** (`1 + 0.4 × (balance_skew − 1)`). It stops when its load falls below that or below `balance_min_writes`, and then needs 10 skewed passes again. A first move that leaves the shard skewed doesn't end the balancing: a shard at 3 times the mean with several tablets sheds one per dwell until it is below 1.1 times the mean.
- **Proportional skew split.** When one tablet dominates and splits for skew, it makes about `my / mean` children: `round(my / mean)`, at least 2 and at most one more than the shards below the mean. The children go to the coldest of those shards. A shard at 1.3 times the mean sheds into one other shard. A single tablet carrying all of a table's load (the scaling gate's skewed-multi-shard start, about N times the mean) still splits over every shard in one step.
- **Not changed.** Dwell (10 passes per shard after a move or split over shards, and per arrived tablet), size splits, merges, consolidation, slot reservations and idle backoff. The constants are internal; no option is added.

**Why.** On the AX102, a uniform D213 write load over 32 shards caused 11–65 tablet moves in a 10 s measured phase (#547). #547's step 1 reproduced it on the Mac: the D213 driver with a pre-split table, Buffered so the per-shard rates reach `balance_min_writes` (GroupSync on the Mac's fsync stays below it), 60 s per run:
- **Changes under uniform load:** about 35 moves/min at 4 shards, 256 at 16 and 211 at 32. 32 shards also made 66 splits/min and ended with 21–51 tablets per shard.
- **Throughput cost:** 0.52–0.69 M commits/s, against 1.16–1.39 M with decisions logged but not executed.
- **Where the noise comes from:** per-shard writes per 100 ms pass vary by 42–57% (coefficient of variation). With weight 0.5 a shard's average sat above 1.25 times the mean on 4–10% of passes.
- **How it cascades:** one trigger on a shard with a single tablet split it into one child per shard below the mean, up to 16 at 32 shards. The change itself then made real imbalance, which the balancer chased.

Replaying candidate rules on the logged per-shard loads showed that persistence is what separates noise from a real shift. A slower average or a higher threshold alone did not:
- **Noise-triggered changes under uniform load:** from 59 / 60 / 221 per minute at 4 / 16 / 32 shards to 1 / 0 / 0.
- **Reaction to a real skew shift:** a quarter of the threads move to one shard's range, about 1.33 times the mean. Reaction went from 0.5 / 5.1 / 0.8 s to 3.7 / 6.0 / 1.8 s.

The hysteresis is the issue's form (start at 1.25, stop below a lower bound). The proposed form ("don't count again until below 1.1") would have left a shard that is still skewed after its first move stuck.

**Tests.**
- `a_skewed_shard_keeps_shedding_tablets_until_balanced`: 12 tablets, 9 on one shard. It must make six moves, one per dwell, to reach 3 per shard, then nothing more.
- `a_short_burst_of_skew_changes_no_tablet`: 4 passes at 3 times the mean change nothing. Under the old rule they split the hot tablet.

<a id="d215"></a>
## D215 — The cold-get target is measured with a block cache that holds the table's index; the engine reports pinned metadata and warns when it crowds the cache (owner decision, 2026-10-11; bench, engine, #87, #406)

**Decided:**
- **The gate's cold-get step** (`phase3-io/cold-get.sh`, `examples/coldget.rs`) runs with a block cache of **at least 1% of the table**: `COLDGET_CACHE_MIB`, which `run-window.sh` sets to 1/64 of the table (1024 MiB for the 64 GiB table, at least 64 MiB). Each result line records the cache size.
- **The engine reports pinned metadata:** the bytes of SST top indexes and filters pinned in the block cache, in its metrics. It logs a warning when they pass about **50% of the block cache**, because past that the index partitions and data blocks the reads need no longer stay cached.

**Why.** The first gate run (2026-10-11) measured 2.09 device reads per cold get against a target of 1. The second read is the get's index partition:
- **Capped mode** (buffered reads, 8 GiB memory cap) had the same 2.09 block-cache misses per get but 1.087 device reads: the partitions sat in the page cache.
- **The cause is the cache's size.** An open SST pins its top index and both filters in the block cache, counted against its capacity. That is about 2.9 MiB per GiB of table at coldget's row shape (1 KiB cells), so about 185 MiB for the 64 GiB table, inside a 64 MiB cache. The cache was always over capacity, and every index partition read was evicted at once.
- **Raising index partitions' cache priority changes nothing:** S3-FIFO already keeps re-hit blocks, and there was no room to keep them in.
- **The spec's claim** (a cold get is one data-block I/O) already assumes index and filters pinned. RocksDB's default keeps them outside its block cache, in table-reader memory, which holds the same bytes.

The AX102, 16 GiB table, direct I/O, pread, one thread:

| block cache | device reads per get | p50 | p99 |
|---|--:|--:|--:|
| 16 MiB (the first run's 1:1024) | 2.092 | 162 µs | 326 µs |
| 256 MiB (1.6%) | 1.072 | 85.5 µs | 187 µs |
| 512 MiB (3.1%) | 1.055 | 86 µs | 188 µs |

The remaining 0.05–0.07 reads come from filter false positives across levels. On a 1 GiB table (macOS, cache misses only), 1% of the table gave 1.12–1.14 block reads per get and 1.6% gave 1.065, so the gate uses 1/64 (1.56%).

<a id="d216"></a>
## D216 — "In memory" means block-cache resident and warmed; the row cache is on by default, 64 MiB (owner decision, 2026-10-11; bench, pigeonhole, #406; amends D201)

**Decided:**
- **"Point get in memory" (#406: p50 < 2 µs, p99 < 10 µs)** is measured on reads that the cache serves. The gate's `latency` step runs `ycsb-c` with every engine's block cache at 2 GiB, which holds the whole table, and with a warmup as long as the measured phase (`--cache 2147483648 --warmup 1.0`). Before this, the step used the 256 MiB default cache and a 5% warmup, so many "in-memory" gets missed the cache and decoded blocks from the page cache.
- **The row cache is on by default** (`Options::row_cache`, D201): **64 MiB**, a quarter of the default block cache. `row_cache(0)` turns it off. Only the public `Options` changes: the engine's `EngineOptions` stays at 0, a lower-level API whose callers opt in.
- **The gate's `row-cache` step reverses.** It now runs `ycsb-c` (same cache and warmup) and `ycsb-a` with `--row-cache 0`, against the latency step's default. `report.py` shows off → on with the hit rate. `phdb-bench --row-cache` keeps the library default unless given, and a run's description names the row cache only when the flag sets it (`rowcache=off`, `rowcache=NMiB`). So the default run's result key is unchanged, and `compare` against earlier results shows the default's change as a change.
- **The bench reports the row cache's hits, misses and fills** for each measured phase (`Stalls::row_cache_hits`, `row_cache_misses`, `row_cache_fills`).

**Why on.** On the AX102 (blob33, 2026-10-11, the 2 GiB warmed `ycsb-c`):
- **Off:** p50 2.91 µs, p99 5.5 µs.
- **On:** p50 0.67 µs, p99 6.7 µs.

The p50 target is met only with the row cache. `ycsb-a` costs about +0.3 µs at p50. Writes pay one `fetch_max` per row they touch; D201 measured commit-overwrite at +0.6% instructions.

**Why 64 MiB.** The sweep ran on the Mac (M-series, 10 cores, under other load) at `full` scale: 1M rows, 2 GiB block cache, full warmup. Hit rates don't depend on load; throughput and latency do.

| Row cache | ycsb-c hits | ycsb-c ops/s | ycsb-c p50 / p99 | ycsb-a hits |
|---|---|---|---|---|
| off | — | 393K | 2.34 / 4.8 µs | — |
| 8 MiB | 58% | 584K | 0.54 / 5.1 µs | 25% |
| 16 MiB | 63% | 567K | 0.59 / 6.6 µs | 28% |
| 32 MiB | 68% | 636K | 0.54 / 5.6 µs | 32% |
| 64 MiB | 74% | 709K | 0.54 / 5.6 µs | 35% |
| 128 MiB | 81% | 829K | 0.54 / 5.3 µs | 37% |
| 256 MiB | 84% | 875K | 0.50 / 5.0 µs | 39% |

- **p50 doesn't depend on the size.** It reaches hit speed from 8 MiB on, because zipfian gets are mostly to a few hot rows.
- **p99 doesn't depend on the size either.** It is set by the misses, which pay the lookup on top of the uncached read; that hit path is a follow-up.
- **More memory buys throughput slowly.** Each doubling past 64 MiB adds 3–7 points of hits.
- **The cost of the row cache:**
  - **The cache itself:** up to its full size, filled only by rows that missed twice. Rows read once are never stored.
  - **Write watermarks:** three 8-byte slots per 256 bytes of cache (the watermark table, its present tags and its ghost tags), so about 9%, or 6 MiB at 64 MiB. They are allocated and zeroed at open.
  - **Totals:** a writer's default caches come to 256 + 64 + 6 MiB.
  - **Reader processes** have no row cache (D201), so nothing changes for them.
- **At 256 MiB**, the gain over 64 MiB is +10 points of hits on `ycsb-c` and +4 on `ycsb-a`. The cost is +192 MiB, and +18 MiB zeroed at every open. "Open to first read < 5 ms" (#406) already measured 3.08 ms, p99 4.06 ms, on the AX102.
- **At 64 MiB on the Mac, open to first read** was within run-to-run noise of `row_cache(0)`: 1.1–2.5 ms either way, over three alternating fresh-directory runs of 30 opens each. Allocating the watermark tables lazily, zeroed by the OS, is a follow-up if the gate's open step shows a cost.

**The instruction shapes stay off.** The CI callgrind shapes count the engine's read and write paths, and D193's ceilings are set on them. With the row cache on, `readshapes`' repeated gets and row reads, and `hotrow`'s states, would count a cache hit instead of the read path, so the head would not be comparable with any base.
- **Pinned off:** `hotrow`, `readshapes` and `scanrate` now call `row_cache(0)`. Scans bypass the row cache, but the scan shapes' setup writes would pay the watermark `fetch_max`.
- **Already off:** `ycsb` and `writepath` use `support/`, whose `ROW_CACHE` variable defaults to 0.
- **Allocation budgets:** `pigeonhole/tests/alloc.rs` (#287) pins it off too. Its budgets are the read and write paths' own, and a read that fills the row cache allocates the copy it keeps: the 1-cell row read went from at most 10 allocator calls to 20.
- **Not pinned:** `coldget`, `coldscan` and `openlat` measure the user's default. A cold get never hits, and its I/O count is unchanged. Open now includes the watermark allocation.
- **Follow-up:** the hit-path work. Its instruction shapes would run with `ROW_CACHE` set, as separate shapes.

**Not changed:** the row cache's semantics (D201), `row_cache_max_row` (4 KiB), the other targets' steps, and the RocksDB configuration. RocksDB gets the same 2 GiB block cache on `ycsb-c` and no row cache.

<a id="d217"></a>
## D217 — While writes are light, the balancer packs the written tablets onto as few shards as stay about half busy (owner decision, 2026-10-11; tablets, engine, #478; amends D134 and D214)

**Decided:**
- **Measure:** each engine-owned shard counts the time its thread spends handling work (`Submitter::busy_nanos`, ICR 0033). Every balancer pass publishes the shard's busy fraction as a moving average (weight 0.2, as write load) beside its write load.
- **Active shards:** with `B` the summed busy fraction of all shards, every shard computes the same `k = ceil(B / 0.5)`, so each active shard has headroom before commits queue. `k` is never below `ceil(slots in use / max_slots)`, the shards the tablets' memtable slots need (one slot per tablet and family). Without that floor, the first full shard strands the rest of the written tablets across the inactive ones.
- **Hysteresis:** `k` grows (by at least one, to the new `ceil(B / 0.5)`) after 10 consecutive passes (1 s) with `B / k > 0.75`, and shrinks after 50 passes (5 s) with `B / (k − 1) < 0.5`. It rises to the slot floor at once.
- **Packing:** shards `k..` hand their written tablets, the most-written first, one per dwell, to the least busy active shard with slot room. Cold tablets stay: they cost no wakeups. The skew rule (D214) and consolidation act only among shards `0..k`.
- **Never acts** while the store writes fewer than `balance_min_writes` rows per interval over all shards, or while no shard reports busy time (application-owned shards, the simulated clock).
- **On by default.** `Options::tablet_packing(false)` (`EngineOptions::balance_pack`, ICR 0034) turns it off, and off is exactly the placement before D217.

**Why.** A light writer spread over every shard pays a shard wakeup and park per commit. The Phase 3 gate run (2026-10-11) had time-series-ttl at 165K ops/s on 32 shards against 249–260K on 1–2 shards (RocksDB 251K), and skewed-multi-shard at 325K on 32 against 493K on 4 (RocksDB 568K). The shard count that wins depends on the load, so the balancer picks it from measured busy time instead of a fixed setting.

The AX102, 32 shards, uring, `--scale full`, one run each, packing off → on (engine/217-packing before the slot floor):

| workload | B | ops/s | p50 | shard parks per op |
|---|--:|--:|--:|--:|
| skewed-multi-shard | 1.43 | 374K → 464K (+24%) | 8.2 → 5.5 µs | 0.56 → 0.05 |
| ycsb-a | 0.33 | 117K → 136K (+16%) | 8.3 → 5.8 µs | 0.49 → 0.11 |
| time-series-ttl | 0.29 | 181K → 187K | 5.7 → 5.0 µs | 0.39 → 0.31 |
| ycsb-f | 0.19 | 88.5K → 90.1K | 11.0 → 10.5 µs | 0.50 → 0.25 |
| ycsb-c (reads only) | 0.00 | 218K → 213K (noise) | 3.5 → 3.5 µs | 0 |

- **The targets stay at 0.5 and 0.75.** skewed-multi-shard's `B` of 1.43 gave `k = 3` (464K, against 418K on 2 shards and 493K on 4 fixed). Its busiest shard ran at 0.73 from key skew inside the active set, so a tighter target would saturate it.
- **The slot floor came from this run.** The bench table has four families, so a shard holds at most 16 tablets. time-series-ttl (`k = 1`) filled shard 0 and left 18 written tablets one or two per shard across the rest; ycsb-f's gain is small for the same reason.

With the slot floor (931c578, the same setup, two runs each, ranges):

| workload | B (on) | ops/s off → on | p50 off → on | shard parks per op | shards written (on) |
|---|--:|--:|--:|--:|--:|
| time-series-ttl | 0.30–0.37 | 171–173K → 209–211K (+22%) | 5.7 → 3.8–4.1 µs | 0.40 → 0.05–0.14 | 2–3 |
| ycsb-f | 0.24–0.27 | 97K → 104–107K (+9%) | 10.3 → 8.6–9.2 µs | 0.50 → 0.16–0.31 | 2–3 |
| skewed-multi-shard | 1.24–1.28 | 346–355K → 382–412K (+13%) | 8.4–8.7 → 5.8–6.0 µs | 0.56–0.62 → 0.00–0.01 | 2 |

- **time-series-ttl now packs fully** onto the shards its slots need (34 tablets, 2–3 shards) and gains 22%, against 3.5% without the floor.
- **skewed-multi-shard settled on 2 shards** in both runs, against 3 in the first. `B / k` of 0.62–0.64 sits inside the hysteresis band (grow above 0.75, shrink below 0.5), so `k` stays where the load phase left it. With 3 active shards, the first run reached 464K.

## D218 — io_uring is the default I/O backend where the kernel offers it: `IoBackend::Auto` (owner decision, 2026-10-12; pigeonhole, io, #402)

**Decided:**
- **`IoBackend::default()` is `Auto`:** io_uring where the kernel offers it (Linux with io_uring enabled), the `pread` pool otherwise. That covers other systems, old kernels and containers that block io_uring, and it never fails the open. `IoBackend::Pread` and `IoBackend::Uring` still force a backend.
- **The test variable `PIGEONHOLE_IO`** keeps forcing a backend (`pread`, `uring`, `auto`); unset, it no longer means `pread`.
- **Application-owned mode on Linux therefore starts no I/O threads by default** (D202). A driving thread completes its own ring's I/O, so an event loop should wait on `Shard::io_fd`. A loop that never takes it keeps polling while I/O is in flight (`next_wakeup` reports it as due now). Engine-owned mode needs nothing from the application.

**Why.** #402's "done when" asks for io_uring on by default on Linux, measured against `pread` on the reference hardware. The first gate run (2026-10-11) had io_uring behind `pread` on write-heavy workloads (skewed-multi-shard 0.80× throughput, p99 1.45×; ycsb-a and ycsb-f 0.86–0.88×). The cause was in our code, not in io_uring's I/O:
- A shard turn made two system calls with nothing to reap (#555).
- Every wake wrote the ring's eventfd (#556).
- The bench's D213 driver spun instead of waiting on the completion fd (#554, #559).

With those fixed, io_uring matches or beats `pread` everywhere measured (AX102, 2026-10-12):

| workload | io_uring before | io_uring after | pread |
|---|---|---|---|
| ycsb-a | 113–114K ops/s | 131–134K | 129–134K |
| ycsb-f | 89–90K | 97–100K | 97–98K |
| time-series-ttl | 161–162K | 175–182K | 176–178K |
| skewed-multi-shard | 302–315K, p99 22–25 µs | 380–383K, p99 17 | 376–379K, p99 16–17 |
| durable group commit, D213, 32 shards | p99 1,065–1,794 µs | p99 330–352 | p99 252–336 |

Reads were already equal on both backends (ycsb-c 1.01×, cold get identical).

<a id="d219"></a>
## D219 — The gate judges durable group commit on at least 2M commits and measures scaling for at least 8 s, with a stall breakdown (owner decision, 2026-10-11; bench, #406; amends D213, D204)

**Decided:**
- **Durable group commit (D213's shape) is judged on at least 2M commits.** `phdb-bench group-commit --inline-ops N` sets the D213 run's commits apart from the client runs' `--ops`. The gate window's `latency` step passes 2,000,000 on io_uring and on pread. The preset's 200K-commit run, the first window's length, is still reported as `latency-group-commit-short.json`. The summary gives each run's length and the flushes and compactions inside it.
- **The scaling gate (D204) is measured for at least 8 s at every shard count.** `--min-secs S` makes an inline run (scaling, and D213's) repeat its measured writes until S seconds are measured. The `scaling` step passes 8.
- **The summary breaks each scaling run's stalls out:** its length, the share of shard time spent in write stalls, flushes, compactions, and the longest operation.
- The small scale (the CI dry run) keeps both short: 50K commits and 1 s.

**Why.** perf287's runs on the AX102 (2026-10-11, after the inline driver's io_fd fix):
- **The first window's D213 run** measured 0.27–0.33 s and 200K commits. At 32 shards that's about 6K commits per shard: no flush or compaction fell inside it, and its p99 rested on about 2K samples.
- **On 2M commits (3–4 s)**, each of the 32 shards flushed 3 times and compacted once inside the measured phase. Throughput fell from about 2.9M to 2.2M cells/s, and p99.9 went from 1.6–3 ms to about 29 ms. A user's sustained load is the long run.
- **The scaling runs measured 1.1–1.3 s.** At 10M operations (8–10 s), efficiency at 4 shards was 0.76 rather than 1.03, because sustained writes reach the write-stall regime. Up to 3 s of stall in an 8 s run sat outside the short runs.

**Not changed:** the targets, D213's shape (shards, pre-split placement, commits in flight) and D204's. The shard count is D220's.

## D220 — The default shard count is one per physical core (owner decision, 2026-10-11; io, runtime, engine, pigeonhole, bench, #406)

**Decided:**
- **`Options::shards` defaults to the physical cores the process may run on**, within `available_parallelism` (so a cgroup CPU quota still caps it). SMT siblings count once. `pigeonhole_io::sys::default_shards()` resolves it, and `physical_cores()` reports the cores:
  - **Linux:** the sysfs topology of the CPUs in the affinity mask. A core is named by the first CPU of its sibling list (`topology/core_cpus_list`, or `thread_siblings_list` before Linux 5.6). Unlike `core_id`, that name is unique across packages, dies and clusters.
  - **macOS:** `hw.physicalcpu`.
  - **Windows:** `GetLogicalProcessorInformation`, over the CPUs in the process affinity mask (the process's processor group, as pinning already is).
  - **Elsewhere, or where the OS doesn't say** (no sysfs in a container, say): `available_parallelism`, as before.
- **Pinned threads spread over cores first.** With `pin_threads`, shard `i` goes to the first CPU of the `i`-th physical core in the opener's affinity set, then to the cores' SMT siblings, wrapping (`sys::spread_cpu`). Default-count shards never share a core, whatever the CPU numbering. Before, shard `i` went to the `i`-th CPU, which on machines that number siblings next to each other (0 and 1 one core) put two shards on each core.
- **The gate runs at physical cores:** 16 on the AX102. D213's shape and the scaling gate's N (D204) default to `default_shards()`, so the gate window needs no flag, and so does the ycsb-a-shards step's default. The report's "per core" divides by the same count.
- No new dependency: the detection is a few sysfs reads, one `sysctlbyname`, or one Win32 call (a `windows-sys` feature) at open.

**Why.** perf287's AX102 runs (2026-10-11; 16 cores, 32 SMT threads), durable group commit in D213's shape on 2M commits:

| | cells/s | p99 |
|---|---|---|
| io_uring, 16 shards | 4.50M | 199 µs |
| io_uring, 32 shards | 2.28M | 913 µs |
| pread, 16 shards | 4.52M | 169 µs |
| pread, 32 shards | 2.15M | 541 µs |

- **32 shards are SMT pairs on 16 cores, so they slow each other.** Each shard's flushes and compactions run on its own thread in application-owned mode, and a stalled shard holds the visibility watermark for every other one.
- **16 shards give twice the long-run throughput of 32,** and meet the p99 target on both backends.
- The spec already says one shard per physical core ([Thread-per-core execution](../spec.md#thread-per-core-execution)); the code counted logical CPUs.

**Not changed:** an explicit `shards(n)`; D213's and D204's shapes. `Environment::cores` in the bench report stays the logical CPU count: it identifies the machine when comparing runs.

## D221 — The group sync depth stays 2 on Linux and 1 on macOS; `PIGEONHOLE_GROUP_SYNC_DEPTH` is removed (owner decision, 2026-10-12; engine, #476; decides D207's depth)

**Decided:**
- **D207's interim defaults become the defaults:** a shard keeps at most 2 group syncs in flight per WAL stream on Linux (and other systems), 1 on macOS. They stay an internal constant, with no option.
- **The measurement variable `PIGEONHOLE_GROUP_SYNC_DEPTH` is removed,** and so is the gate window's `group-sync-depth` step that varied it.

**Why.** The first gate run on the reference machine (AX102, 2026-10-11, io_uring, durable group commits) compared 1, 2 and unlimited:

| Depth | 1 client | 4 clients | 16 clients |
|---|---|---|---|
| 1 | 17,206 ops/s, p99 130 µs | 51,129 ops/s, p99 132 µs | 106,501 ops/s, p99 248 µs |
| 2 | 17,526 ops/s, p99 122 µs | 51,009 ops/s, p99 138 µs | 113,790 ops/s, p99 191 µs |
| unlimited | 17,704 ops/s, p99 119 µs | 51,381 ops/s, p99 136 µs | 115,458 ops/s, p99 188 µs |

- **Depth 2 is within about 1.5% of unlimited** at every client count.
- **Depth 1 falls behind at 16 clients:** 6% fewer commits per second than depth 2, and a p99 of 248 µs against 191.
- **Depth 2 also bounds the syncs** a shard can pile onto the device.
- macOS keeps 1, for D207's reason: `F_FULLFSYNC` serializes on the device.

## D222 — Scans read 16 data blocks ahead, without merging adjacent fetches; `PIGEONHOLE_READAHEAD` is removed (owner decision, 2026-10-12; engine, #431)

**Decided:**
- **A scan's readahead (`ReadOptions::readahead_blocks`) is 16 data blocks**, up from the interim 4, and **adjacent fetches are not merged** (`readahead_merge` off). Both are an internal constant.
- **The test variable `PIGEONHOLE_READAHEAD` is removed.** The cold-scan script (`phase3-io/cold-scan.sh`) now measures backend × direct/buffered at the default only.

**Why.** The first gate run on the reference machine (AX102, 2026-10-11) scanned a 16 GiB table cold. The table gives the median MiB/s over 3 runs:

| io | direct | readahead 0 | 4 | 4, merged | 16 | 16, merged |
|---|---|--:|--:|--:|--:|--:|
| pread | no | 2,669 | 3,158 | 2,466 | **3,679** | 2,736 |
| io_uring | no | 2,650 | 3,057 | 2,624 | **3,284** | 2,957 |
| pread | yes | 186 | 380 | 473 | 1,028 | 1,037 |
| io_uring | yes | 186 | 380 | 480 | 1,016 | 1,082 |

- **16 blocks ahead is the fastest buffered setting on both backends:** 24–38% above no readahead, and 7–17% above 4.
- **Merging adjacent fetches hurts buffered scans** by 10–26%: the page cache's own readahead already reads sequentially, and a merged fetch only waits longer for its first block.
- With direct I/O (off by default, #403), merging helps by 1–7%. That doesn't outweigh the buffered loss, since buffered is the default.

<a id="d223"></a>
## D223 — The scaling gate's placement check is a busiest share of at most 1.25/N, and the driver starts every shard together (owner decision, 2026-10-12; bench, tablets, #406; amends D204)

**Decided:**
- **The busiest shard may take at most 1.25/N of the measured commits** (`Scaling::MAX_SHARE`), down from 2/N. Above it the run fails as skewed, whatever its efficiency. The gate summary (`report.py`) checks the same bound. [D231](#d231) moves the bound to a separate open-loop placement check (`Placement::MAX_SHARE`); the scaling run is pre-split with the balancer off.
- **The D204 driver starts every shard together.** Each shard thread picks the rows its shard owns, taking a turn of its shard every 64K writes so the shard's balancer keeps publishing its load. All threads then start at a barrier, and the phase clock starts there.
- **The balancer (D214) is not changed for the bound.** With the driver fix, D214 already meets 1.25/N at 8 and 16 shards on the AX102 (below). The balancer work that followed didn't improve placement there on its own, so it isn't shipped with D223. Its claims ship with the churn fix in [D224](#d224) (#582).
- **Packing (D217) keeps its grow threshold at 0.75** busy per active shard. 0.6 was measured and gained nothing.

**Why the bound.** With equal per-shard rates, a phase lasts as long as its busiest shard, so efficiency is at most 1/(N × busiest share). Under 2/N, a placement that capped efficiency at 0.5 still passed, so the check didn't fit the 0.8 target. 1.25/N is the loosest bound that still allows 0.8. On the AX102, perf287 measured busiest shares of 1.2–1.5× the ideal at 8 and 16 shards.

**Why the driver change.** Before, `phase()` picked each shard's rows on the main thread, one shard after another, before spawning that shard's thread. Each pick scans every write. So the shards started, and finished, staggered by the scan time. A shard waiting to start ran no balancer passes, so its published load went stale and counted as 0. The balancer then moved tablets toward phantom skew, sometimes back again. On the Mac at 4 shards, balancer decisions per run fell from 13–18 to 2–4. The old clock also counted the serial picks, which understated efficiency.

**How far 1.25/N can go.** The best contiguous split of the `skewed-multi-shard` key distribution (Zipfian 0.99, scrambled, 1M rows) is computed from the workload's own key and rank functions:

| N | 4 | 8 | 16 | 32 |
|---|--:|--:|--:|--:|
| Best busiest share | 25.2% | 12.8% | 6.6% | 6.5% |
| As a multiple of ideal | 1.01× | 1.02× | 1.05× | 2.08× |
| 1.25/N | 31.3% | 15.6% | 7.8% | 3.9% |

The hottest row alone takes 6.5% of the writes, so 1.25/N can't be met at 32 shards. The gate runs scaling at most at the physical core count (16 on the AX102, D220), where it can.

**Measured (AX102, 2026-10-12; `scaling --scale full --ops 2M×N --min-secs 8`):**

| Run | Busiest share | × ideal | ≤ 1.25/N | Efficiency | Splits / moves while measuring |
|---|--:|--:|---|--:|--:|
| 8 shards, main (e99ace8) | 13.8% | 1.11 | yes | 0.65 | 4 / 18 |
| 16 shards, main, run 1 | 7.80% | 1.25 | yes (limit 7.81%) | 0.37 | 5 / 48 |
| 16 shards, main, run 2 | 7.79% | 1.25 | yes | 0.28 | 17 / 63 |
| 8 shards, balancer change | 15.3% | 1.22 | yes | 0.70 | 2 / 23 |
| 16 shards, balancer change, run 1 | 8.76% | 1.40 | no | 0.35 | 8 / 48 |
| 16 shards, balancer change, run 2 | 8.0% | 1.29 | no | 0.42 | 3 / 30 |

- **The driver fix moved placement.** On the old driver, perf287 measured 1.51× ideal at 8 shards and 1.62× at 16. Efficiency rose too: from 0.49 to 0.65 at 8 shards, and from 0.21–0.25 to 0.28–0.37 at 16. Part of that is the clock no longer counting the picks.
- **16 shards has no margin.** The busiest shard there probably holds the hottest row (6.5% of the writes) and little else.
- **The balancer change tried** had two parts:
  - Claims: a shard just handed load by a skew change isn't another skew change's target for 10 of its passes.
  - Skew split keys cut over 1024 pooled samples weighted by writes. Simulated, they improve the first split's busiest piece from 2.16× to 1.33× ideal at 16 shards.
  - It failed the bound in 2 of 3 runs, because the balancer keeps moving tablets after the first split. The branch `engine/223-balancer` is kept for reference.
- **Churn is what's left** (#582). No run's warmup settled within its 30 s cap. The driver is closed-loop: shards write at equal rates while all are busy, so skew shows only in a phase's tail, when the lighter shards finish. Every still-busy shard then reads above 1.25 × the mean together and sheds tablets, every phase.
- **Packing grow threshold, 0.75 vs 0.6** (16 default shards, 2 runs each):

  | Workload | 0.75 | 0.6 |
  |---|--:|--:|
  | skewed-multi-shard (ops/s) | 453K, 446K | 369K, 439K |
  | time-series-ttl (ops/s) | 285K, 289K | 232K, 284K |
  | ycsb-f (ops/s) | 105K, 107K | 109K, 108K |
  | Moves per run | 0 | up to 4 |

  0.6 gained nothing anywhere, and it was the only threshold that moved tablets.

<a id="d224"></a>
## D224 — Under a closed-loop write load the balancer ignores tail skew, corrects placement on slow averages and claims its targets, and a tablet is cold only after 100 unwritten passes (owner direction, 2026-10-12; tablets, #582; amends D214, D134, D146; follows D223)

**Decided:**
- **Skew while the mean is falling doesn't count.** Each shard keeps a slow average of the mean write load over the active shards (newest pass weighted 0.05, about 2 s). While the mean is below 0.9 times that average, a pass neither counts toward D214's persistence nor resets it, and no skew change is made. A bursty load whose mean dips between bursts still reaches the persistence.
  - A shard looks skewed beside others that just went idle. That's the tail of a closed-loop load, where the lighter shards finished their work first.
  - A standing imbalance, emerging skew and a table's first load all leave the mean steady or rising, so they count as D214 has it.
  - If the others' load stays down, the slow average catches up within a few seconds and the skew counts again.
- **A slow path acts on the shards' shares of the work.**
  - Each shard publishes a slow moving average of its writes per interval (newest weighted 0.01, about 10 s, longer than a closed-loop load's phases), and keeps one per tablet.
  - A shard whose slow load is above `balance_skew` times the mean of the slow loads, and at least `balance_min_writes`, for 10 passes in a row may make one move or skew split chosen on the slow loads. It then waits 100 passes.
  - It acts only while the fast averages also put the shard above D214's stop ratio. Once the fast path has balanced a shard, its slow average still shows the old skew for a while, and acting on that would overshoot.
- **Skew changes claim their targets.**
  - A move or skew split claims every shard it hands load to, with a compare-and-swap in the receiver's `LoadSlot`. The claim holds until the receiver has published 10 more passes (100 for a slow-path change), or as many base intervals have gone by on the clock, whichever comes first. An idle receiver backs off its passes, so the clock bounds a claim whose change was dropped or refused.
  - A claimed shard is no other skew change's target. Shards balancing at once never all shed into the same cold shard.
  - A target must also be below the mean. If none is, nothing is decided that pass.
- **A tablet is cold (for merges and D146's consolidation) only after 100 unwritten passes, or 100 base intervals of clock, and as long on its shard.** Before, 2 unwritten passes were enough, so tablets idle only through a tail were merged or moved away. `EngineOptions::balance_cold_passes` (hidden) shortens it for tests.
- **Not changed:** D214's thresholds, persistence, hysteresis and dwell; how skew splits pick their keys; size splits; packing (D217); the options.

**Why.** D223 put the scaling gate's placement within 1.25/N, but its runs never settled. The inline driver (D204) is closed-loop:
- While all shards are busy they write at the same rate, so skew shows only in each phase's tail.
- There, every still-busy shard read above 1.25 times the falling mean and shed tablets, every phase.
- Merges and consolidation also picked up the tablets idle in the tail.
- On the AX102 at 16 shards, main made 48–63 tablet changes in the measured phase, with efficiency 0.28–0.37.

**Measured** (AX102, 2026-10-12; `scaling --scale full --ops 2M×N --min-secs 8`):

| Run | Efficiency | Busiest share | × ideal | ≤ 1.25/N | Splits / moves while measuring |
|---|--:|--:|--:|---|--:|
| 8 shards, main | 0.55 | 17.8% | 1.42 | no | 4 / 12 |
| 8 shards, falling mean only | 0.79 | 18.6% | 1.49 | no | 0 / 0 |
| 8 shards, + slow path, claims, cold | 0.84 | 15.0% | 1.20 | yes | 1 / 0 |
| 16 shards, main | 0.33 | 7.6% | 1.22 | yes | 13 / 50 |
| 16 shards, falling mean only (2 runs) | 0.45, 0.42 | 11.2%, 13.5% | 1.80, 2.17 | no | 0 / 0 |
| 16 shards, + slow path, claims, cold (2 runs) | 0.48, 0.45 | 9.0%, 8.0% | 1.45, 1.29 | no | 0 / 1, 1 / 7 |

- **The falling-mean rule alone** removes the churn but freezes placement where the first split left it. Under a closed loop, real imbalance is visible only in the tails.
- **The slow path** corrects placement without the churn. At 8 shards the run passes both halves of the gate. At 16 it stalls near 1.3–1.5× ideal.
- **Tried for 16 and not adopted:**
  - Skew split keys cut on pooled, write-weighted samples: three runs at 1.31–1.46×, the same as without them.
  - A 30-pass slow dwell: 1.43× and 1.51×.
  - The cause is followed up separately.

**Tests:**
- D214's two tests pass unchanged.
- `a_closed_loop_load_settles_instead_of_churning_every_tail`:
  - The setup is D204's shape. Rows are assigned to their owning shard at phase start, each shard writes at the same rate, and a shard idles once it's done.
  - It must stop changing tablets and end with no shard over 1.25 times the mean.
  - It fails without each of the four parts: without the falling-mean rule (4–10 changes every phase), without the slow path (no change, imbalance kept), with the 2-pass cold criterion (consolidation churn), and without claims (herd moves).
- `a_standing_imbalance_still_moves_a_tablet`: a shard at 2.3 times the mean from the start moves within D214's persistence.
- `skewed_shards_never_shed_into_the_same_cold_shard_at_once` and `skewed_shards_wait_when_every_cold_shard_is_claimed`.
- Loom: two skew changes never both claim one shard.

<a id="d225"></a>
## D225 — A participant's PREPARE may be checkpointed once its commit is settled, not once the coordinator checkpointed the COMMIT (coordinator decision, 2026-10-12; engine, #585; amends D116)

**Decided:**
- **"Settled"** means the coordinator has counted every participant's `ShareFlushed`, its own share included: every share of the commit is in SSTs, D24's condition for the COMMIT record. At that point it tells the other participants (`CommitSettled`, formerly `CommitCheckpointed`), and each may checkpoint past its PREPARE.
- **Before,** D116 kept a participant's PREPARE until the coordinator's checkpoint passed the COMMIT. The checkpoint-time notice and the `passed_commits` queue are gone.
- **A coordinator counts its own share directly,** not as a message sent to itself.

**Why.**
- **A checkpoint is a stream prefix,** so D116's rule made one stream's checkpoint wait on another's. Two cross-shard commits in flight together whose coordinators take part in each other log each other's PREPARE before their own COMMIT. For example, shard 0 logs PREPARE 715 (coordinated by 2) before COMMIT 719, and shard 2 logs PREPARE 719 (coordinated by 0) before COMMIT 715. Neither checkpoint can then pass. Close waits for empty logs and never returned, and live, the WAL stopped being trimmed past those records (#585, found by the #527 gate-step work, 0.2.1 included).
- **The self-sent share report** re-entered the close once per share, and a close with thousands of them overflowed the shard's stack.

**Why it is safe.** A replay that finds a COMMIT whose participant trimmed its PREPARE treats the commit as incomplete (D116's all-or-nothing replay) and applies none of it. Once the commit is settled, every share is already in SSTs, so nothing is lost and nothing is applied twice.
- `model_check` has a case for it (`settled_commits_trimmed_by_participants_survive_crashes`: 16 cross-shard commits in flight, frequent flushes, crashes between and inside commits, at 2–4 shards).
- The same case runs a second loop with tablets splitting, merging and moving (`tablet_ops_ppm` 20 000, the fast balancer), where a moved slot or a retired tablet reads as flushed through the published catalog.
- Telling participants before the shares are settled fails that case with `LostAckedCommit`. A tablet change that commits before the moving tablet's frozen memtables are flushed (the `frozen.is_empty()` wait in `progress_op` dropped) fails its tablet loop.
- `engine/tests/close_crossed.rs` closes after concurrent cross-shard commits on real files, under a watchdog.


<a id="d226"></a>
## D226 — The durable group-commit p99 is judged under full load: 16 shards, 16 commits in flight, the D223 driver; 4 and 8 in flight are reported beside it (owner decision, 2026-10-12; bench, engine, #406; amends D213)

**Decided:**
- **The judged row stays as D213 defines it:** p99 < 200 µs on the bench side (from the driver's submit to the driver seeing the commit done), at N shards = physical cores (D220; 16 on the AX102), with 16 commits in flight per shard, measured by the D223 driver. The engine has to meet it. A lower in-flight count does not count as meeting it.
- **The trade-off is reported beside it:** the same D213 run at 4 and 8 commits in flight per shard (`phdb-bench --in-flight N`; `latency-group-commit-inflight-{4,8}.json`), reported, not judged.

**Why.** D223's driver changed what the row measures. All shards now start together at a barrier, and the clock starts there, instead of shards starting staggered as their rows were picked and the clock counting the picks.
- On the AX102 the same engine measured 4.7–4.9M cells/s with p99 207–210 µs on the old driver, and 7.3–7.4M cells/s with p99 327–331 µs on the new one. Its engine-side p99 was 131 µs on both.
- The difference is concurrency the old driver never produced, not a regression.
- The owner kept the target at full load rather than moving it to a lighter one.

**The trade-off, measured** (AX102, 16 shards, uring, 2M commits, two runs each, 2026-10-12, before the work below):

| in flight per shard | cells/s | vs 16 | p50 µs | p99 µs | p99.9 µs | engine p99 µs |
|---|---|---|---|---|---|---|
| 4 | 3.29–3.34M | −56% | 66–67 | 158–161 | 675–696 | 65 |
| 8 | 5.17–5.25M | −30% | 80 | 164–168 | 745 | 98 |
| 16 (judged) | 7.38–7.55M | — | 107–108 | 307–323 | 761–790 | 131 |

- Every level clears the throughput half (> 1M cells/s) by 3× or more.
- The p99 half holds at 8 or fewer in flight: halving 16 → 8 gives up 30% of the throughput for a p99 of about 165 µs, and 4 gains almost nothing more.
- The p99.9 hardly moves with the in-flight count: it comes from syncs and flush and compaction steps, not from queueing.

**Work toward the judged row.** The first finding is a lost wake in `ShardDriver::wait` (#567). A commit that resolves during the driver's own `run_once` wakes the driver by setting the same flag the scheduler's `collect_woken` then clears, so the driver's next `wait` sleeps until the next I/O completion or its timeout. Its fix and the remaining attribution (the global watermark, D19; the group-sync wait, D207) are tracked separately and recorded here when they land.

<a id="d229"></a>
## D229 — `wal_pin_bytes` stays at twice the memtable budget (owner decision, 2026-10-12; engine, bench, #527)

**Decided:** the default WAL pin bound (D155, D206) stays at 2× `memtable_budget`: `wal_pin_bytes` 0 means that, as before. 4× is not adopted. No code changes.

**Measured** (the 2026-10-12 gate window on the AX102, `wal-pin` step: #175's mixed-temperature commits, 4 shards, 64 MiB budget, 4M commits, then a reopen after an exit without close; medians over 2 rounds; `crates/bench/baselines/phase3-gate/results/2026-10-12/wal-pin.jsonl`):

| `wal_pin_bytes` | commits/s | unpin passes | small flushes | all flushes | WAL files at most | reopen (replay) | first read |
|---|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 416,600 | 188 | 320 | 225 | 1,152 MiB | 460 ms | 1,535 µs |
| 4× budget | 391,598 | 0 | 0 | 210 | 1,344 MiB | 573 ms | 1,304 µs |

**Why:**
- At 2×, the unpin passes and their small flushes cost no throughput. Commits/s was about 6% higher than at 4× in both rounds (408K and 425K against 383K and 400K). The passes run in slices (D206).
- 2× keeps the WAL about 190 MiB smaller and the replay after a crash about 20% shorter (460 against 573 ms).
- 4×'s gain is fewer small flushes (0 against 320; total flushes 210 against 225). That's write amplification compaction absorbs: compactions were 157–159 against 159–171.
- The first read after reopen differed by a fraction of a millisecond either way.

<a id="d230"></a>
## D230 — The memtable lookup target (≤ 300 ns) is judged at 2^18 entries, the largest one memtable reaches under the defaults with small cells; 1M is reported (owner decision, 2026-10-12; bench, memtable, #17, #406)

**Decided:**
- **The judged row.** The memtable point lookup (≤ 300 ns, #17, the spec's "typically under 300 ns") is judged at **262,144 entries (2^18)**: criterion `seek/262k entries/hit (get)`, pinned to one core, in the gate's `memtable-lookup` step.
- **Reported, not judged:** 1M entries (the old judged size) and 10k entries (cache-resident), in the same step.
- **The skiplist search is unchanged.** Software prefetch of the next level's first node during the search was measured and not adopted (below).

**Why 2^18: the arithmetic from the code.**
- **When a memtable freezes.** A memtable belongs to one (tablet, family). It freezes once its arena allocation reaches `memtable_freeze_bytes`, a quarter of `memtable_budget` by default: 64 MiB → **16 MiB** (`EngineOptions`, `pigeonhole::Options`; the engine's `freeze`, which compares `allocated_bytes` with the threshold).
  - An insert that crosses the threshold freezes the memtable at the next freeze check, so the memtable holds at most about 16 MiB of nodes.
  - (D117 may defer a freeze until every seqno it holds is visible; D227 makes that wait short.)
- **One entry's node:** a 12-byte header (key length, value length, height), then a tower of 4-byte links, then the internal key, then the stored value. The total is rounded up to 4 bytes (memtable `insert`).
  - The height is geometric with ratio 1/4, so the tower averages 4/3 links: 5.33 bytes.
  - The stored value is a 1-byte tag plus the value.
- **The internal key:** the escaped row + 2-byte terminator + the escaped qualifier + 2-byte terminator + a 17-byte suffix (timestamp, seqno, kind; FORMAT §2).

| cell | internal key | node, average | entries in 16 MiB |
|---|--:|--:|--:|
| YCSB shape: 24-byte row (`user` + 20 digits), 6-byte qualifier, 100-byte value | 51 B | 12 + 5.33 + 51 + 101 ≈ 169 B | ~99K |
| the same with a 16-byte value (D205's small cell) | 51 B | 12 + 5.33 + 51 + 17 ≈ 85 B | ~197K |
| **8-byte row, 1-byte qualifier, 16-byte value** | **30 B** | **12 + 5.33 + 30 + 17 ≈ 64 B** | **~262K = 2^18** |
| empty row and qualifier, 1-byte value (degenerate) | 21 B | 12 + 5.33 + 21 + 2 ≈ 40 B | ~420K |

2^18 covers every cell with an internal key of at least 30 bytes and a value of at least 16 bytes: both bench shapes, and small cells with short keys. Only degenerate cells (empty or near-empty keys and values) fill a memtable with more entries. A larger `memtable_budget` (or `memtable_freeze_bytes`) raises all of these in proportion; the judged size follows the defaults.

**The prefetch, measured** (the AX102, 2026-10-12, criterion pinned; off → prefetch of `cur.tower[level-1]`'s node at every level ≥ 1):

| | 1M hit | 10k hit | 10k miss |
|---|--:|--:|--:|
| CPU 10, random rows | 447 → 434 ns (−2.9%) | 121 → 126 (+3.6%) | 101 → 106 (+5.4%) |
| CPU 10, shared-prefix rows | 446 → 430 (−3.5%) | 122 → 130 (+6.0%) | 100 → 107 (+7.2%) |
| CPU 2 (V-cache), random | 479 → 464 (−3.2%) | 129 → 136 (+6.1%) | 106 → 116 (+9.5%) |
| CPU 2, shared-prefix | 477 → 462 (−3.1%) | 131 → 133 (+1.7%) | 106 → 110 (+3.5%) |

- About 3% at 1M, against a 2–9% loss whenever the memtable is cache-resident. An Apple M5 had shown −4 to −10% at 1M without the 10k cost; the reference machine decides.
- blob33's prefix-beside-link layout was rejected earlier (+13–31% at 10k and with shared prefixes; #17).

**Reported beside the judged row:** 1M entries, 446–479 ns on the AX102 (2026-10-12), memory-latency bound (~40 dependent node reads over ~70 MB). The judged 2^18 row is first measured in the next gate window.

<a id="d231"></a>
## D231 — The scaling gate is split: engine scaling on a pre-split table with the balancer off, and balancer placement judged by its own open-loop check (owner decision, 2026-10-13; bench, #406; amends D204, D223)

**Decided:**
- **Engine scaling (≥ 0.8 × N)** is judged on a table split up front with the balancer off, as D213 does for group commit, so it measures per-shard throughput only.
  - `phdb-bench scaling` splits at the quantiles of the measured writes, each row counted once per write, so every shard takes about the same share of them. Where a row covers a cut (the skewed workload's hottest takes 6.5%), the next cut moves past it, so it keeps a piece of its own.
  - The run fails if the split did not leave one tablet per shard.
  - The verdict is efficiency alone. The busiest share is reported.
- **Balancer placement** is judged by `phdb-bench placement`:
  - Engine-owned shards, tablet changes on, packing off. Packing keeps a light load on few shards on purpose, and is not what this judges.
  - The skewed-multi-shard writes are offered **open-loop** at a fixed rate (default 30,000 writes/s per shard, above the balancer's per-pass minimum for an average shard). Clients behind their schedule catch up instead of slowing the offer.
  - The table starts as one tablet. Once every shard owns a tablet and nothing has split, merged or moved for 12 s (longer than the slow skew path's dwell, D224), a 10 s window is measured.
  - **It passes when:**
    - the table settled within 60 s;
    - nothing split, merged or moved in the window;
    - the busiest shard took at most 1.25/N of the window's writes (D223's bound);
    - the store completed at least 95% of the offered rate.
- **The gate window** runs `placement` at 4, 8 and 16 shards after `scaling`, and `report.py` reports the two as separate rows.

**Why.** D223 put a placement bound on the scaling gate, and D224 then made the balancer settle under its driver. But the driver is closed-loop: each shard thread commits only the rows it owns, with commits in flight.
- **What the balancer sees there:** while every shard is busy, their write rates are equal whatever their share of the work. Skew shows only in each phase's tail, and there a still-busy shard runs faster once others stop competing (on the AX102 its rate rose to 1.9× the mean while the mean held).
- **The result:** placement under that driver varied between runs about as much as any balancer change moved it. At 16 shards main measured 1.22× and 1.34×, and two D228 variants 1.25–1.44×. The gate re-run on main measured 1.28×/1.36×/1.28× at 4/8/16.
- **The fix:** an engine-scaling result should not depend on that, and the balancer's placement is better judged under a load where a shard's rate is its share.

**Not changed:** the 0.8 efficiency target, the single-shard p99 half (`phdb-bench compare`), D204's driver otherwise, D213's group-commit shape, and the 1.25/N bound (now on the placement check).
