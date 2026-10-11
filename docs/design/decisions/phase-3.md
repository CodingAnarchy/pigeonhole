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
## D196 — The async front door: a commit ticket is a handle, misses wake on I/O through a cache-only read tier, and a scan step's unpredicted miss reads synchronously (approved; coordinator and owner, 2026-10-09; pigeonhole, engine, sst, #42; amends the spec's "Sync and async"; full scan fix deferred as #398)
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
- `Options::row_cache(bytes)` (0, the default, means off), `row_cache_max_row(bytes)`, and `row_cache_family(table, family)`, called once per family; without any call, every family is served. All are process-local, with no FORMAT change. A stored per-family switch can come later if users ask.
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

<a id="d203"></a>
## D203 — Open does no blocking flush on a clean reopen; the first durable commit waits for the flushes a read does not need (coordinator decision, 2026-10-10; engine, pager, wal, io, #158; ICR 0021)
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
