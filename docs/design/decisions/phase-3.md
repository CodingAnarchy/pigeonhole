# Decisions made in Phase 3 (D194–)

Indexed in [README.md](README.md). Numbers are permanent and continue from Phase 2; code and docs cite them as `Dn`.

<a id="d194"></a>
## D194 — Reads skip superseded memtable versions through a writer-only, process-local stale-tail index (approved; owner decision, 2026-10-09; memtable, engine, compaction, #387; alternative deferred as #397; refined by D277: the tail now lives in the shared node, and reader processes skip too)
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

- **Amendment (coordinator, 2026-10-19; from the 0.3.0 gate on 5933e23e): the shard's window is 200 µs.** `EngineOptions::shard_spin_nanos` and `Options::shard_spin` default to 200 µs instead of 50 µs; the client's 50 µs (D253) and the rule that an idle shard never polls are unchanged. Why (the read at the time; the box verification below corrected the mechanism): with D257 the gate's cross-shard-contention row loads on one shard and regrows to 16 inside the warm-up; the measured phase then started fully regrown (k = 16, one partition per shard, no moves) and still ran at 398K ops/s against 590K with packing off, p50 49 against 40 µs, p99.9 3–5 ms against 0.5, on three gate runs and blob33's A/B. Every shard's turn time per commit was +36%, shard parks ×3 and visibility waits ×2.6 per op, uniformly over the shards: a parked regime, as D253's 15 µs client row was (400K, every client parking). A cross-shard round holds its seqno for two hops, a hop to a parked shard costs a wake paid against 32 polling clients on 32 hardware threads, held seqnos hold the visible watermark, clients park on visibility and submit later, arrivals miss the shards' 50 µs windows, the shards park: self-sustaining for this shape (the contended row ran it for 95 s), never entered on ycsb-a (no coupling) and never entered before D257 (no regrow). On the box (blob33's R1, 2026-10-19), `--shard-spin 200` on the same row read 591.6K ops/s, shard parks 0.002 per op, p99.9 0.76 ms, level with both controls, against 404K at 50 µs; but its single-threaded load ran 4.6 s instead of 7.5 s and ended before D257's first shrink fired (settled on 16 shards, warm-up 2.2 s), so that run shows the window keeping a one-client load off the pack, not a regrown store climbing out of the regime. A long warm-up alone does not cause the regime (packing off with a 21 s warm-up: 576–596K) and 16 clients show it too (383K against 494–503K), so the window, not the client count, is the lever. Box verification (fable252, 2026-10-19, 0bcf0b4b; raw in `/root/scratch/fable252/out/` and the inbox note of 20:49Z): the window is not the cure. On the gate's row (1M records) spin 200 read 599K against 405K ops/s, but as in R1 its load ran 4.7 s and ended before the first shrink, so the store never packed (warm-up 2.1 s, settled on 16 shards). With `--records 2000000` the load packs at both windows (the k trace: 16 → 2 → 16) and the regrown store read 364K at 200 µs against 367K at 50 and 580K with packing off, p99.9 6.8 against 6.2 ms, with the shards awake (0.007 parks per op against 0.106) and still +40% turn time per commit (20.9 against 14.7 µs): the regime is work inside the turns, not wakes. Measured next with scratch counters: every manifest commit's `Maintain` made each shard rescan its whole list of unreported cross-shard shares, and a regrown store commits its manifest twice as often (#761 fixes it). D257's shapes at 200 against 50 µs, 3 reps alternating, medians: ycsb-a t=1 172K against 171K, t=2 277K against 270K, t=16 1270K against 1260K, time-series-ttl t=1 237K against 228K, riders buffered 886K against 867K, riders default 399K against 388K with its write p99.9 289 against 424 µs; `k` at one client 1–5 at both windows, so the window does not keep a store from packing. It costs none of D257's gains, takes the shard parks on every shape to at most 0.075 per op (0.001–0.013 but riders default) and trims the write tails, and the owner took it into 0.3.0 on that reading, not as the fix for the cross-shard row. The gate's ycsb-a-spin rows (one client, shard parks 0.082 → 0 per op, +10–13% ops/s at 100–200 µs) show the same window does no harm where no regime exists; the gate's `ycsb-a-shards` step now runs the spin at 0, 100 and 500 µs beside the default, instead of 100, 200 and 500. Cost: a shard that handled a message polls up to 200 µs for the next one; the window needs a handled message and the poll backoff halves the polls on misses, so an idle store still never spins, and `shard_spin(Duration::ZERO)` still turns it off.

*History: Amended by D253 (the client's window is 50 µs). Amended by the coordinator, 2026-10-19 (the shard's window is 200 µs; the bullet above).*

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
- **Visibility (D227):** a group with a record under a held header is also not visible until a sync resolves it (which writes the header first). Visibility no longer waits for a group's sync, so without this a process crash could remove what readers saw.

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
- **The thread-per-core scaling target is measured with Pigeonhole application-owned, at N shards up to the core count.** Each shard thread drives its own shard and commits the `skewed-multi-shard` writes to rows its shard owns (`Table::shard_of`, ICR 0022; owner routing approved by the coordinator) inline, with 16 commits in flight (`commit_async`). The target is unchanged: N-shard write throughput ≥ 0.8 × N × single-shard, with no single-shard p99 regression. `phdb-bench scaling` runs it, and `docs/bench.md` defines it. [D238](#d238) judges N only up to one chiplet's physical cores and reports larger N.
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

<a id="d205"></a>
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

<a id="d206"></a>
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
- **The reference baseline:** two official `phase2-gate` runs on the AX102 (amended: on main f8afbb2d, see History), committed beside the Mac runs in `crates/bench/baselines/phase2-gate/`. From then on, `run-gate.sh` compares and `check.py` checks against them.
- **Until they're committed,** the Mac runs 5–6 stay the baseline. They remain in the repository as the Phase 2 record.
- **D193's other rules are unchanged:** the same gated measures and noise allowances (re-derived from the AX102's pair if its spread differs, with the owner's agreement, as D193 requires for any widening), and the same "fixed or accepted by the owner" rule.
- **#387's ratio to SQLite EAV** (row-read and scan p99, the documented gap) is judged on the same two runs. On the Mac at main 2d6b6ed it was 1.58× and 1.27× (D193: about 2.0× and 1.6×).

*Checked by:* the first gate window's report (`report.py`), whose Phase 2 rows read both runs.

*History: Amended by the owner, 2026-10-14. The baseline is two official `run-gate.sh` runs on main f8afbb2d (D252–D254), not the first gate window's: sparse-wide's tail had crept past the Mac limits on 175c0ffc (#705), a bisect over every engine commit since 8654ad69 found no single cause (the ~2% creep goes to #724), and the owner chose to install the baseline from current main. The files are the 2 of 8 official runs on f8afbb2d nearest the median; main failed the Mac limits in 4 of the 8. check.py's allowances are unchanged; re-deriving them from the AX102's spread is a separate owner decision.*

<a id="d209"></a>
## D209 — A reopened WAL stream's first segment takes max seen + 3, so it never takes the epoch of a successor whose header never reached the disk (coordinator decision, 2026-10-11; wal, #508; amends FORMAT §10.1 rule 2; shipped in 0.2.1)

**Decided:** recovery starts the reopened segment at max seen + 3 (`REOPEN_EPOCH_GAP`), where max seen covers every segment header and the replayed end. It was max seen + 1.

**Why:**
- **The problem.** A successor segment whose header never reaches the disk leaves its frames in the slot under the epoch the header would have had. That happens two ways:
  - a crash while a rollover sync is in flight after the successor's records were written (D200);
  - a power loss that keeps a header-and-records write's record sectors but not its header (0.2.0 too).
- **What went wrong.** Recovery ends at the full segment and reopened at + 1, usually in the same recycled slot. Once the new header was written, the stale frames replayed as the new segment's records, and their seqnos were reused by new commits.
- **Why + 3.** At most two headers are ever not durable at once, since a header is written only after its predecessor is durable: the full segment's own header, and the held successor's. So surviving frames carry at most the largest durable header's epoch + 2. The reopened segment's header is synced before anything is appended, so skipped epochs are never reached again.
- **Rejected:** zero-filling a recycled slot at open (up to 64 MiB of writes per stream per reopen), and holding a successor's records until its predecessor's header is durable (gives up #19's overlap).
- **Compatibility:** files written before D209 read unchanged, since chaining follows `prev_epoch`. The 0.2.1 backport uses the same constant, so FORMAT has one rule (FORMAT §10.1).

**Checked by** `crates/wal/tests/crash.rs`:
- the held-header and lost-header cases;
- the double window (fails at + 2);
- a reordering power-loss sweep over Buffered rollovers;
- a 0.2.0 stream fixture.

The full record is in `docs/design/questions/wal.md`.

<a id="d210"></a>
## D210 — Async commits wait for room before they submit: a per-shard bound on async commits in flight; a future dropped before it submitted writes nothing (owner decision, 2026-10-10; engine, pigeonhole, runtime, #149; amends D196)

**Why.** D196 made `commit_async` submit at the call. A shard queues whatever is submitted, so an async producer that outruns its shard (a long write stall, an application-owned shard its loop stopped running) queued without limit. Measured on #149: 200,000 × 4 KiB `commit_async` calls against a stopped shard returned in 116 ms and grew the process to 1,081 MiB, about 5.4 KiB per queued commit. Blocking commits never had this problem, because each holds its thread.

**Now:**
- **The bound.** Each shard holds at most half `memtable_budget` (2 MiB per 4 MiB arena; 32 MiB at the defaults) of async commits submitted and not yet resolved. Bytes are the encoded batch. A cross-shard batch is charged to its coordinator, the shard of its first row. The bound follows `memtable_budget`; there is no separate option (an `EngineOptions` override exists for tests).
- **Over the bound, an async commit waits before it submits.** `commit_async`, `commit_with_async`, `commit_if_async` and `Transaction::commit_async`/`commit_with_async` submit at the call when their shard has room and no earlier async commit waits. Otherwise the future waits in its shard's queue, in arrival order, and submits when it's first and fits. A commit larger than the bound goes alone once the shard holds nothing. A later commit never overtakes an earlier waiting one, even if it would fit.
- **The fast path.** While the async bytes in flight on all shards *together* fit the bound and no commit waits, a commit is charged to that engine-wide count with one compare-and-add, without routing, and submitted at once; every shard is then within the bound. Past that, a commit is routed to its shard and charged there (counting the unrouted bytes as possibly that shard's), or queued. *(Replaced by D236: the fast path charges the commit's own shard, once it is routed.)*
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

*Amended by [D251](#d251): when no tablet carries an eighth of the gap and none dominates, the largest tablet that fits moves (a full shard of equal small tablets used to shed nothing).*

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
- **Active shards:** with `B` the summed busy fraction of all shards, `k = ceil(B / 0.5)`, decided by shard 0's balancer and used by every shard, so each active shard has headroom before commits queue. `k` is never below `ceil(slots in use / max_slots)`, the shards the tablets' memtable slots need (one slot per tablet and family). Without that floor, the first full shard strands the rest of the written tablets across the inactive ones.
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

*Amended by [D248](#d248): the busy fraction is the larger of the thread's busy time and its group syncs' utilization, so a load bound by its fsyncs is not packed below the shards it needs.*

*Amended by [D251](#d251): an active shard whose thread is above 0.9 busy also grows `k`, and holds off a shrink while it stays there.*

*History: shard 0's balancer decides `k` and publishes it, and every shard uses that value. Each shard used to run its own hysteresis over loads it read at different passes, so the shards disagreed: in one run, one kept 16 while others used 1 to 3. This is an implementation fix to match the rule above ("every shard computes the same `k`"), from the skewed-multi-shard placement analysis (F3). Amended by [D256](#d256), 2026-10-15: the busy measure adds the thread's spin before parking and the pool's slice time for the shard's tasks to its turn time. Amended by [D257](#d257), 2026-10-19: the spin term goes again; the commits in flight (`round(C / 0.75)`) size the active shards beside the busy sum, grow them and gate a shrink.*

<a id="d218"></a>
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

*History: amended by [D255](#d255), 2026-10-15: a shrink waits while the load is still ramping (the fast write average above 1.25× the slow one), and never goes below the floor a stall-driven growth set.*

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

<a id="d220"></a>
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

<a id="d221"></a>
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

<a id="d222"></a>
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

*Amended by D266 (#724): a PREPARE is checkpointed once its holders' flushed-through seqnos pass the commit, with no `CommitSettled` message; the uncoupling of the stream checkpoints this decided stands.*

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

*Amended by [D254](#d254): the reports and settlements go as `SharesFlushed { seqnos }` and `CommitsSettled { seqnos }`, one message per peer per pass, with the same rules.*

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

<a id="d227"></a>
## D227 — A commit is visible once applied, not once durable; durable snapshots keep the old view (owner decision, 2026-10-12; engine, shm, docs, #406; amends D19)

**Decided:**
- **Visibility.** A shard publishes a group's seqnos once the group's WAL records are handed to the OS (`write()`) and the group is applied to the memtables, without waiting for the group's sync. The snapshot watermark is, per shard, the minimum of:
  - the cross-shard seqnos it coordinates that are not yet applied everywhere (`held`, unchanged);
  - its reserved but not yet applied seqnos;
  - any group with a record under a held segment header (D200), until a sync resolves it.
- **Acknowledgement is unchanged.** A commit returns only once it is durable at its level and visible (D19), so read-your-writes holds after the call returns.
- **Cross-shard commits are unchanged.** One becomes visible only after its COMMIT meets the requested level and every participant has applied it.
- **`Buffered` and `None`** become visible at apply, like every level (they already did).
- **Durable snapshots** (ICR 0037): `durable_snapshot()` (engine, `Pigeonhole` and reader handles) sees a commit only once it is durable at its level on its shard; `backup_durable` backs up from one.
  - Backed by a second published watermark per shard: `durable` at offset 8 of each shard's watermark line, shared-memory layout version 2 (FORMAT §11.2–11.3).
  - It keeps D19's strict view for readers that need it.
- **A memtable freezes only once everything in it is durable** (D117's rule, at the durable seqno instead of the visible one). A deferred freeze waits in a registry of its own, `Shared::durable_waiters`, woken against the durable seqno.
  - Why: GC at flush keeps the newest version at each read point (live snapshots and "latest"). A memtable frozen at the visible seqno could hold versions above the durable seqno, and its flush would collapse one that a later durable snapshot reads (found by the new `durable_freeze` test's mutation check: the durable read came back empty).
  - Frozen at the durable seqno, every entry a flush writes is at or below a durable seqno at or below any later durable snapshot. Compaction inputs inherit that bound.
  - Cost: a freeze waits for the syncs in flight, D19's timing for freezes.
- **A reader process's durable snapshot** pins its slot with a durable seqno, or lowers an existing pin to it while keeping the pinned view.
- **Durable means durable at the commit's own level.** Of what a durable snapshot reads, `GroupSync` and `Sync` commits survive a power loss, `Buffered` ones a process crash, `None` ones nothing.
- **A failed fsync is never durable.** The shard keeps the failed group's lowest seqno as a floor under its durable watermark for good. Every shard's freezes would then stall behind it, so the failure also counts as the shard's death (`shard_died`): the close gives up flushing and is unclean, and the next open replays only what reached the disk (D85's note).
- **Replay after a process crash:** recovered records may sit in the page cache only. Open syncs each recovered stream (the new segment's `sync_all`, `Recovery::into_stream`) before any watermark is published, so they're durable before a durable snapshot can read them. `durable_snapshot.rs` checks this with a power loss after the reopen.
- **After a shard's death, new commits are refused** (owner decision on ctr274's point 5, option (b)). This covers a panic, a dropped application-owned shard, a failed fsync and a failed cross-shard apply. `Inner::submit`, `check_and_mutate` and `commit_local` check `shard_died` and return `Closed`: nothing is written, so not committed. Reads keep working.
  - Why: fail fast beats a 30 s `Busy` stall once memtables can no longer freeze behind a stuck durable watermark, and a commit that could never become visible here.
  - Checked by `blocking_waits`: a dead coordinator, a poisoned shard's failed sync, and a failed share apply each refuse later commits on every shard. Without the check, all three tests fail.
- **Upgrades:** layout 2 refuses mixed versions, so stop reader processes before upgrading the writer.
- **Cost:** the shard computes both watermarks in one pass and skips the durable store when its value is unchanged.
- **The D226 row** (group commit p99 < 200 µs) is judged on the default, this decision.

**Consequences users must know** (the durability guide's "Visibility and durability" section has the examples):
- **Readers can see data a power loss removes,** at every level. That includes readers in other processes. A process crash alone does not remove it: the records are already in the OS.
- **No global prefix across shards** (D84). After a power loss, a later commit on one shard can survive while an earlier one on another shard, which readers saw (and a later write may have been derived from), is lost. On each shard, what survives is still a prefix of what readers saw.
- **Backups** copy what is visible at their snapshot, so they can hold commits the source then loses. A durable snapshot avoids it.
- **Seqnos and default timestamps** that readers saw can be reissued after a power loss: they are assigned in memory, before the sync. For example, a change consumer that stored "up to seqno 1042" from other clients' commits skips a new commit 1042 after the restart. Seqnos and timestamps of acknowledged commits are durable and never reused.

**Why.** The global watermark (D19) made every commit wait for every other shard's in-flight sync. On the reference machine at 16 shards × 16 commits in flight (D226), 56% of commits waited for it, with a p99 of about 200 µs of the bench's ~245 µs p99. RocksDB with `sync=false` and SQLite with `synchronous=NORMAL` expose unsynced commits to readers the same way. Pigeonhole keeps the stronger guarantee for the committer: a `GroupSync` commit has returned only once durable. The owner accepted the cross-shard anomaly at the default level and added the durable-snapshot opt-in for readers that need the old view. The design is `.coord/design-a1-visibility-at-apply.md`. The audit of what relied on "visible implies durable" is `.coord/audit-a1-visible-durable.md`.

**What still holds it up:** the flush's sync barrier (D115). It is now what keeps a flush from persisting a record into an SST ahead of its own stream's sync. Freezing (D117), the flush and compaction guards (D191, D192), checkpoints (D24, D116, D225) and the row cache (D201) rely only on "visible means applied", which still holds.

**A close with a dead shard** (found by this decision's tests, whose visibility pin is a cross-shard commit). A shard that dies (a panic, or an application-owned shard dropped mid-close) used to leave the close waiting for ever on a cross-shard commit's other side. Now:
- **No waiting on the dead side:** the close is unclean and stops waiting for undecided shares and for coordinated commits' answers. Their records stay in the WAL, and the next open decides each share by its COMMIT (D83, D116).
- **Undecided commits abort:** a coordinated commit still undecided is decided abort, and a late `Prepared` finds it failed. Its caller gets `Closed`, which then always means *not committed*.
- **Decided commits keep their reply.** One already decided resolves as usual when every participant applied it.
- **Late decisions apply nothing.** A decision reaching a participant after its close gave up is answered as *not applied*. The same holds for a participant whose apply failed.
- **No half-visible commit.** A commit decision with any share not applied keeps its seqno held at the coordinator, never published. No reader in this process sees one share without the others.
- **A failed share apply counts as a shard's death.** Outside a close, a held-for-good seqno would stall every later commit's visibility wait and the close's deferred freezes. So the coordinator calls `shard_died()`: waiters get `OutcomeUnknown`, the close gives up flushing and is unclean, and the next open decides the commit. A poisoned shard already means the database needs reopening.
- **Outcome unknown (ICR 0038, owner decision 2026-10-12).** `Closed` always means *not committed*. Where the outcome is the WAL's, decided at the next open, a commit fails with the new `Error::OutcomeUnknown` (`ErrorCode::OutcomeUnknown` = 28):
  - a cross-shard commit whose coordinator died, or one decided to commit that a participant did not apply or never answered;
  - a commit in a group whose sync had not resolved when its shard stopped;
  - a commit acknowledged by its shard whose wait for visibility ended because the close finished or a shard died.
  - A shard that stops answers each such commit before it reports (`answer_unsettled`). A commit dropped before it was written still reads as `Closed`.
  - Both error enums are `#[non_exhaustive]`, so the new variant is a minor (semver-compatible) change.

**Checked by:**
- `model_check`'s new case `reads_beside_in_flight_commits_and_crashes_inside_them_keep_every_invariant`:
  - deferred I/O, 3 shards;
  - reads of in-flight commits' own cells, which must show each commit whole or not at all;
  - power losses inside commits at 12%;
  - a process crash after some reads that saw a commit, which recovery must keep.
  - It fails if the watermark ignores held cross-shard seqnos.
- `held_header_visibility.rs`: a commit written under a held segment header is not visible until the header is written. It fails if such groups count as visible: a read, then a process crash, loses the commit.
- `blocking_waits.rs`, closes with a dead shard:
  - **Dead coordinator:** the visibility wait ends `OutcomeUnknown`.
  - **Dead participant:** the decided commit ends `OutcomeUnknown`.
  - **Late decision at a participant that reported:** never applied (a debug assertion), never half visible to a read, `OutcomeUnknown`, present after reopen.
  - **Late `Prepared` at a coordinator whose close gave up:** aborted, `Closed`, absent after reopen.
  - **A failed share apply:** `OutcomeUnknown` for the commit and for a later commit on a healthy shard (a watchdog catches the stall), nothing visible, and an unclean close.
- The checker now applies acknowledged commits to its model in seqno order. A commit is acknowledged at its own durability, so a later seqno's shard may acknowledge first.

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
- [D239](#d239) accepts 337 ns at 2^18 entries: the gate's verdict there is "accepted (D239)", not FAIL.

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
  - `phdb-bench scaling` cuts the measured writes, each row weighing its writes, into one contiguous piece per shard with the heaviest piece as light as possible. A row heavier than a fair share (the skewed workload's hottest takes 6.5%) sits alone, cut just before and just after it, and the other cuts go by write weight over the rest. Cutting at the plain write quantiles gave the hot row's piece a normal slice too: 10.3% of the writes at 16 shards, which capped efficiency at about 0.61 (perf2's box run).
  - The run fails if the split did not leave one tablet per shard.
  - The verdict is efficiency alone. The busiest share is reported.
  - [D238](#d238) judges it for N up to one chiplet's physical cores and reports larger N.
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

<a id="d233"></a>
## D233 — The bench settles every engine after the load before it warms up and measures (owner decision, 2026-10-13; bench, #406 reproducibility)

**Decided:** between the load and the warmup, `run_detailed` (every workload except the inline drivers) runs a settle step for every engine alike:
- The engine flushes its write buffer: Pigeonhole `flush()`, RocksDB `flush` of both column families, fjall `rotate_memtable_and_wait`, SQLite `wal_checkpoint(TRUNCATE)`.
- Then the bench polls the engine's background work every 20 ms until it has been quiet for 1 s, at most 60 s. Past the bound it warns, and the record says `settled: false`.
  - Quiet for Pigeonhole: flushes plus compactions unchanged, and the shard threads' work time rising by under 5 ms per poll.
  - Quiet for RocksDB: no flush or compaction running or pending, and the SST bytes unchanged.
  - Quiet for fjall: no outstanding flush and the table count unchanged.
- It's a bench-side poll of metrics the engines already expose (option (a)). A public "wait until background work is idle" API is #616 (Phase 4).

Each record carries `detail.settle` (milliseconds, settled, the work done while settling) and, for Pigeonhole, `detail.background_work`: the flushes plus compactions completed in the measured phase. `compare` notes every matched pair whose measured phases did different background work (one some and the other none, or more than twice as much). The note is reported, not judged.

**Why:** on the 2026-10-12 gate, ycsb-b on Pigeonhole measured 224.5K and 280.6K ops/s in two identical runs (+25%, beyond the ±20% tolerance). p50 and p99 were the same (1.06/1.05 µs, 13.9/13.4 µs), but the slower run's measured phase held 7 flushes, 7 compactions, 365 write stalls (128 ms) and a 182 ms operation, and the faster one none. The load (1M rows, ~15 s) went straight into a 0.2 s warmup and a 3.5 s measured phase, so how much of its background tail landed in the window was timing luck. Four repeats on the AX102 (2026-10-13) measured 270K–292K: the two slower ones each had a flush and a compaction in the measured phase, the faster two none.

<a id="d234"></a>
## D234 — The L0 write stall starts at twice `l0_trigger` (8 L0 files at the defaults), not at the trigger itself (owner decision, 2026-10-13; compaction, engine, #406; amends D119, D166)

**Decided:** `CompactionPicker::stall_score` is L0 depth over 2 × `l0_trigger`, for every compaction style (FIFO's window too). The token bucket (D119) is unchanged: it engages at a score of 1, now 8 L0 files at the default trigger of 4, and its rate still falls as the score rises (4,000 groups/s ÷ score). There is still no hard stop (spec: "a token bucket rather than hard stops"). The factor is relative, so a family with its own `l0_trigger` keeps the same relation between when compaction starts and when writers slow down.

**Why:** at the trigger, writers were paced whenever a compaction was merely running.
- On the AX102 (2026-10-13, scratch build with the start as a knob), a single shard spent 26% of its thread stalled on the bucket. L0 never went past 4 files: the compaction already running at 4 cleared it.
- With the start at 8 or 12 it spent none, and L0 still stayed at 4 files or fewer: 54% of the time at 4, 0% deeper. So reads saw no deeper L0.

| Stall starts at | 1-shard inline writes | 4-shard inline writes | 1 shard: stalled / compaction share of its thread | sparse-wide ops/s, p99 | ycsb-a ops/s, p99 | ycsb-b ops/s, p99 | L0 SSTs probed per read (sparse-wide / ycsb-a / ycsb-b) |
|---|--:|--:|---|---|---|---|---|
| 4 (D119) | 407K, 407K | 1.69M, 1.67M | 26% / 35% | 28K, 416–422 µs | 159–196K, 14.2–15.6 µs | 263–275K, 14.0–14.1 µs | 0.05–0.08 / 0.14–0.29 / 0.14–0.18 |
| **8 (adopted)** | **433K, 432K** | 1.69M, 1.71M | **0%** / 37% | 28K, 422–440 µs | 166–185K, 14.7–16.1 µs | 266–276K, 13.9–14.1 µs | 0.07–0.15 / 0.28–0.35 / 0.13–0.15 |
| 12 | 430K, 430K | 1.71M, 1.69M | 0% / 37% | 28–29K, 416–426 µs | 177–193K, 14.6–15.2 µs | 262–284K, 13.6–14.3 µs | 0.03–0.07 / 0.23–0.25 / 0.16–0.18 |

- **Runs:** two per arm, order rotated. Pigeonhole only, `--scale full`, uring. The read workloads ran before D233's settle step, so ycsb-a's run-to-run spread is wide. Read p99 and L0 probes per read stay within that spread at every start.
- **Why 8 over 12:** 12 bought nothing over 8 here, and 8 still paces writers well before a backlog can build.
- **What it costs:** the single-shard baseline rises ~6%, which lowers the scaling gate's efficiency at a given N by about as much where per-shard rates are unchanged.

<a id="d235"></a>
## D235 — Engine-owned shard threads are pinned by default, safely; the gate's inline drivers pin theirs, with unpinned reported beside them (owner decision, 2026-10-13; io, runtime, engine, pigeonhole, bench, #406; amends D158)

**Decided:**
1. **The gate pins.** The scaling and group-commit inline drivers pin their shard threads in D220's order (one per physical core first), and that run is judged. The same runs unpinned are reported beside them, not judged.
2. **The engine pins by default.** `pin_threads` defaults to true in `EngineOptions` and `pigeonhole::Options` (engine-owned mode; application-owned shards run on the application's threads, which it places).

**How pinning stays safe** (D158's three failure cases, and the rest):
- **Only inside the affinity set.** At open, `pigeonhole_io::sys::pin_plan` takes the opening thread's affinity set (taskset, cpusets, containers) in D220 order and gives each shard and compaction thread a distinct CPU from it. Threads are never pinned outside it, and never two on one CPU.
- **It runs unpinned instead when:**
  - the platform has no hard affinity: macOS's affinity tags are hints, so they aren't set;
  - a CPU quota (a cgroup `cpu.max`, seen by `available_parallelism`) is narrower than the affinity set: D158's quota-limited containers, which would all pin to the host's first CPUs;
  - fewer free CPUs than threads: D158's opener pinned to a few CPUs.
    - [D252](#d252): when the shards fit but the compaction threads don't, the shards are pinned and only the compaction threads run unpinned.
- **Several databases in one process:** a process-wide registry of claimed CPUs makes each plan skip CPUs another live runtime holds, so the second database pins elsewhere or runs unpinned (D158's first case). Claims are released when the runtime's threads are joined.
- **Never fails an open.** A pin the OS refuses leaves that thread unpinned. `Runtime::start` returned `Spawn` on a pin error before.
  - `Runtime::pinning` and `Metrics::{pinned_threads, unpinned}` report threads, pinned, and why not.
- **Several processes on one host** can't see each other's pins and would pin onto the same CPUs. That's documented on `pin_threads`, in the guide and the agent reference: give each process its own CPUs (an affinity mask or cpuset) or `pin_threads(false)`. No option names cores; the affinity mask chooses them.
- **Windows** pins within the process affinity mask (one processor group) with `SetThreadAffinityMask`, as before.

**Why:** on the AX102 (perf2, 2026-10-13), the shard threads' placement moved throughput more than most engine changes:
- 8 shards pinned on one CCD: 3.24M commits/s. Unpinned under `taskset` on two CCDs: 2.36–2.42M; pinned there: 2.63–2.67M.
- 16 shards: pinned 4.16M, unpinned 4.06M.

A thread-per-core application pins its shard threads, so the gate measures that, and the engine's own threads do it by default.

<a id="d228"></a>
## D228 — The balancer's fast skew path acts only while slow loads agree (considered, not adopted; blob33, #582 follow-up; would have amended D224)

**Proposed:** D224's fast skew path would act only while the slow (~10 s) loads agree, and the bench's settle would follow the slowest dwell (`SPREAD_CAP` 60 s).

**Not adopted:** D231 split the scaling gate. Balancer placement is judged by an open-loop check, which already passes, and closed-loop tails are no longer judged. Nothing measured needed the change.

<a id="d232"></a>
## D232 — An async-admission fast path per shard, with no engine-wide counter (considered, not adopted; perf2, #615; would have amended D210's wording)

**Proposed:** async admission charged on a per-shard counter on its own cache line, with no engine-wide total and no per-commit `Arc` clone. D210's semantics would be unchanged.

**Not adopted:** #615 measured no gain on the AX102 and was closed. D236 (#626) takes the commit-path part of the idea further: a per-shard submission handle, and admission charged on the target shard once the commit is routed.

<a id="d236"></a>
## D236 — A commit writes no engine-wide memory on its way: the async fast path charges the shard the commit is routed to, and a pending commit holds its shard's handle on the shared state (coordinator, 2026-10-13; engine, #406 scaling; amends D210)

**Decided:**
- **Admission (amends D210's fast path).**
  - `submit_async` routes the batch once (`Inner::prepare`), then charges the shard the request goes to (its only shard, or a cross-shard commit's coordinator) with one compare-and-add on that shard's own count, and sends it.
  - Without room, the prepared request waits in that shard's queue and is sent once admitted. A request sent later than it was routed is still correct: a shard that handed a tablet away re-routes commits routed with an older map.
  - D210's engine-wide count is gone. Each shard's count, waiter count and queue sit in their own 128-byte-aligned allocation, and a charge returns to its shard alone.
  - The bound, arrival order, blocking commits and the drop semantics are unchanged.
- **The pending commit's handle.** `Submission` and `PendingCheck` hold an `Arc<ShardRef>` for their target shard (one per shard, made at open, wrapping the engine's `Arc<Shared>`), not an `Arc<Shared>` clone each. Commits to different shards no longer share a reference count.

**Why:** both were cache lines every commit on every shard wrote: a CAS and a `fetch_sub` on the admission count, and an increment and decrement of `Shared`'s reference count. Across the AX102's two CCDs those lines bounce.
- A removal A/B on scratch builds, 8 shards pinned, isolated them (two rounds):
  - main: one CCD 3.23–3.24M commits/s, split 4+4 2.70–2.73M (−16%);
  - without the refcount: split 2.80–2.83M;
  - without both: split 2.95–2.99M (−9%).
- An earlier attempt at the admission part alone (#615) routed every commit twice. It measured −3.4% at 8 shards and was closed; this design routes once.

**Measured** (AX102, 2026-10-13, scaling pre-split, `--scale full --min-secs 8`, two rounds alternated):

| | main | D236 |
|---|--:|--:|
| 8 shards, one CCD, pinned | 3.31M, 3.24M | 3.34M, 3.24M |
| 8 shards, split 4+4, pinned | 2.71M, 2.74M | **2.92M, 2.92M (+7%)** |
| 16 shards, pinned | 4.20M, 4.20M | **4.69M, 4.68M (+11.6%)** |
| 16 shards, unpinned | 4.15M, 4.03M | 4.37M, 4.43M (+7%) |

<a id="d237"></a>
## D237 — An application-owned shard's turn ends early when it woke a future the driving thread awaits, before its background slices, at most 8 turns in a row (owner decision, 2026-10-13; runtime, #406 group commit)

**Decided:**
- **The early end.** `ShardDriver::run_once` (`EngineShard::run_once`, `Shard::run_once`) checks once per turn, after the whole message drain and `end_batch`, whether a `ShardDriver::waker()` wake arrived since the last turn. Typically this is a commit the thread submitted that this drain settled. If one did and background tasks are runnable, the turn returns `true` without running its background slices: the caller polls the woken future now instead of at the turn's deadline.
- **The guard.** After 8 such turns in a row (`MAX_TURN_YIELDS`), a turn runs its slices whatever wakes arrived, so flush and compaction get at least one turn in nine.
- **Scope.** Application-owned mode only: only a `waker()` wake sets the flag (`Signal::turn_woken`, separate from the `app_woken` that ends `wait`, so the next `wait` still returns at once, #592). Engine-owned threads never skip. The check is not repeated inside the drain or between slices.
- **Not adopted:** stopping the drain at the first such wake. On the AX102 it split the message batch into smaller groups and cost throughput and the tail (8.35–8.58M vs 9.20–9.50M cells/s at 16 in flight, p99.9 481–602 vs 340–385 µs) without improving settle→seen.
- Shortening the bench driver's 50 µs turn instead was not chosen either: it would change the judged shape.

**Why:** after D227, the judged group-commit row (16 shards × 16 in flight) had a bench p99 of 183–187 µs, under the 200 µs target with little margin. A per-stage profile on the AX102 put about 70 µs of the p99 between a commit settling and its driver seeing it, and about 47 µs between submit and its group starting. Both are the shard's turn, not I/O: a commit settled early in a turn waited behind the turn's background slices until the deadline.

**Measured** (scratch prototype, AX102, 2026-10-13, bench p99, two runs each; 0 write stalls and the same flush and compaction counts in every run):

| group commit, 16 shards | off | early end, at most 8 in a row |
|---|--:|--:|
| 16 in flight | 182–184 µs | 157–167 µs |
| 8 in flight | 137–139 µs | 102–111 µs |

Throughput is unchanged (8.8–9.6M cells/s at 16 in flight).

**Measured on the merged code** (#632 vs main 000d5e5, AX102, 2026-10-13, alternated ×2):
- **16 in flight:** bench p99 151–177 → 132–133 µs. Throughput is unchanged (9.87–10.12 vs 9.90–10.55M cells/s).
- **8 in flight:** bench p99 118 → 91–92 µs.
- **Background work:** 0 write stalls and 80 flushes / 16 compactions in every run of both builds.

<a id="d238"></a>
## D238 — The scaling gate judges ≥ 0.8 × N up to one chiplet's physical cores and reports larger N; efficiency against a fixed single-shard rate is reported beside it (owner decision, 2026-10-13; bench, #154, #406; amends D204, D231)

**Decided:**
- **Engine scaling (≥ 0.8 × N) is judged per chiplet.** It's judged for every N up to the physical cores of one chiplet (CCD), meaning the cores that share one L3 cache. Larger N are reported, not judged.
  - **On the AX102** (Ryzen 9 7950X3D, 2 CCDs × 8 cores), N = 1, 4 and 8 are judged and N = 16 is reported. About 0.70 at 16 is recorded as accepted for two-chiplet parts.
  - A CPU with a single L3 over all its cores is judged up to its core count, as D204 had it.
- **The CCD size comes from the L3 topology.** The setup step (`machine.py`) reads `/sys/devices/system/cpu/cpu*/cache/index3/shared_cpu_list`, counts each L3's physical cores (SMT siblings once), and records them in `machine.json` (`l3_cores`, and `ccd_cores`, the smallest).
  - `report.py` prints the CCD size and splits the scaling target into a judged row and a reported row.
  - Without a recorded size (a `machine.json` from before D238, or no L3 in sysfs) every N is judged.
- **Efficiency against a fixed single-shard rate** is reported beside the judged column, not judged (owner Q4).
  - The judged efficiency divides by the same build's single shard, so a single-shard speed-up (D234's, about 5.5%) lowers it. The fixed column shows engine progress that the ratio hides.
  - The rate is pinned in `report.py`: 417,922 commits/s, `scaling-1.json`'s single-shard rate (pinned, pre-split, inline, 16 in flight). It was recorded on the AX102 on main 9ff02d00 in the pre-gate check of 2026-10-13.

**Why.** Thread-per-core scaling within a chiplet is the design's claim. The cost of crossing chiplets comes from the hardware.
- **The pre-gate check** (AX102, 2026-10-13, main 9ff02d00, pinned) measured 1.08 at N = 4, 0.98 at 8 and 0.63 at 16. At 16 the busiest shard took 7% of the commits and no shard time was stalled, so the loss isn't placement or stalls.
- **D236** (+11.6% at 16 pinned) brought 16 to about 0.70. perf2's per-thread measurement on top of it found the gap per commit at 16 spread over the whole path, with no single hot spot:
  - about 300 ns more waiting for the global visibility watermark (D19);
  - about 320 ns more in the shard's own turn;
  - about 160 ns more submitting and polling.
- **IPC** drops from 1.88 to 1.39 at the same clock: that's coherence and memory latency across the two CCDs.
- **The same shards on one CCD and split over two:** 8 shards pinned on one CCD ran 3.24M commits/s, and split 4 + 4 they ran 2.92M even after D236 (D235, D236).
- **Tried and dropped:** a cached visible seqno, per-shard waiter sections (about 1–2%, within noise), and a memory-copy hot spot that was a sampling artifact.
- **Possible later:** a commit that resolves once its own shard's data is visible, without waiting on every shard's watermark. That changes D19 / D227's semantics, needs its own design, and would also help latency.

**Not changed:** the 0.8 target, the single-shard p99 half (`phdb-bench compare`), the pre-split table and pinned drivers (D231, D235), balancer placement at 4, 8 and 16 (D231's 1.25/N bound, judged at every N), and D213's group-commit shape.

<a id="d239"></a>
## D239 — The memtable lookup is accepted at 337 ns at 2^18 entries; the gate reports it against 300 ns with the verdict "accepted (D239)" (owner decision, 2026-10-13; bench, memtable, #17, #406; amends D230)

**Decided:**
- **#17 is accepted at 337 ns** at 262,144 entries (2^18, D230's judged size). That's criterion `seek/262k entries/hit (get)`, pinned, as measured on the AX102 in the pre-gate check of 2026-10-13 on main 9ff02d00.
- **The verdict.** The gate summary still reports the row against the 300 ns target.
  - At or under 300 ns it passes.
  - Up to 5% above 337 ns (354 ns) it reads **accepted (D239)**, not FAIL.
  - Slower than that is a FAIL: a regression past the accepted value. The criterion estimate on that box varies by well under 1%.
- **#612** (a read-only lookup index built when a memtable freezes) stays the lever, after 1.0.
- **Reported as before:** 1M entries (484 ns) and 10k entries (128 ns).

**Why.** The lookup is bound by memory latency. Each probe follows dependent node reads through the skiplist. D230 measured software prefetch in the search and didn't adopt it (−3% at 1M, +2–9% when cache-resident). Closing the last 37 ns needs a different structure for frozen memtables (#612), and that isn't Phase 3 work. The gate previously measured 481 ns at 1M; D230's 2^18 entries give 337 ns.

<a id="d240"></a>
## D240 — The write-stall timeout is each waiting write's own: a commit, `flush` or `compact` is refused with `Busy` once it has waited that long itself (coordinator, 2026-10-13; engine, cold-get liveness (c); amends D124)

**Decided:**
- **Per waiter.** A commit waiting for arena room is refused with `Busy` once it has itself waited `write_stall_timeout_nanos`, counted from when it first waited.
  - Room freed for the commits ahead of it, and how long the shard has been short of room, don't count against it.
  - The waiting queue keeps submission order, so the oldest waiter is admitted first, and the timeout timer is armed for its deadline.
- **`flush` and `compact` callers** in a starved freeze (D138) wait the same way: from when they asked, or from when the freeze starved if later.
- **Unchanged:**
  - On a frozen clock, every waiter is still refused after `ROOM_FLUSH_ATTEMPTS` failed flushes, or at once when nothing can free room (D126).
  - The stall metrics (count and nanoseconds) still cover the shard's wait, from its first waiter to its last.
  - `Duration::ZERO` still refuses as soon as a wait would begin.
- **A member's clock restarts** when the shard's wait ends (a group needs no room) and it later waits again, behind a conditional commit for example.
- **The stamp** is a `u32` of milliseconds on the shard clock (it wraps after 49 days, far past any timeout).
  - It sits where `Member::attempts` was: D241 left that counter trace-only, so it's gone from every commit request.
  - So a member stays the size it was. A first build with a `u64` stamp grew `Member` from 248 to 256 bytes and put `commit-sixteen` 46 instructions over its callgrind ceiling. Members are moved several times per group.

**Why.** The code read the timeout as "the shard was never fully caught up for 30 s":
- one clock per shard, started by the first waiter, ending only when a group needed no room or no one waited;
- at that deadline every waiter got `Busy`, including one that arrived a moment earlier.

Under a bulk load with many loaders on one shard, each flush frees room for some waiters but never all of them. So after 30 s of continuous pressure, commits were refused although room had been freed many times: the gate's cold-get load failed this way on 2026-10-13. The documented contract already said otherwise:
- D124: "passed without room";
- errors.md: "nothing freed room in time";
- the option: "how long a write … may wait".

So this is a bug fix in the code, not a contract change. The coordinator approved it, and the owner was told as an FYI.

**Tests:**
- A commit that starts waiting 0.6 × the timeout after another isn't refused at the other's deadline, and commits once room frees. It fails on the old reading.
- Two commits that both wait the whole timeout are both refused.
- The same for a later `flush` caller in a starved freeze. It fails on the old reading.

<a id="d241"></a>
## D241 — A cross-shard commit refused with `Moved` or below a timestamp floor gives up only at the write-stall timeout, never after a number of attempts (owner decision, 2026-10-13; engine, tablets, #102; amends D145)

**Decided:**
- **No attempt cap.** A cross-shard commit that a participant refuses because a tablet is being changed (`Moved`) or because its timestamp is below the participant's floor (`BelowFloor`, D11) is retried until it commits, or until `write_stall_timeout_nanos` has passed since it was submitted. Then it fails with `Busy`. D145's cap of 16 refusals (`MOVED_RETRIES`) is gone.
- **What `Busy` means is unchanged:** stalled past the write-stall timeout (D124, D240). No attempt count is kept: D240 removed the trace-only counter, so that `Member` stays the same size.
- **The `Busy` message** no longer blames only flushes and compaction. It names a tablet change too, and the configurable timeout (`Options::write_stall_timeout`, 30 s by default).

**Why.** The 64 GiB cold-get load of the 2026-10-13 gate stayed on one tablet. Once the balancer could split it (the #102 note in D145), the loaders' in-flight batches spanned the new tablets' shards.
- A busy participant keeps giving its own commits wall-clock default timestamps while a PREPARE waits in its queue. So the coordinator's timestamp, chosen above every published floor, is below the floor again when the PREPARE arrives. The retry runs at once and loses the same race.
- Locally (4 GiB, 8 loaders) a commit gave up after 16 refusals within about 10 s, and 17–446 commits per run got `Busy`. With the timeout as the only bound, none did.
- The cost is aborted two-phase rounds (about 1,000–3,000 per 4 GiB load): each a seqno, a PREPARE and an abort. That is bounded by the timeout.
- Making the `BelowFloor` retry win sooner is a separate design question about D11's timestamps, tracked in #637.

**Measured** (macOS, 4 GiB, 8 loaders, with the balancer's re-decision after a compaction):
- the table spread over 7–10 shards instead of 1;
- the tree settled about 20% sooner (58–62 s against 75–77 s, load included);
- no `Busy`.

The AX102 confirms at 64 GiB.

<a id="d242"></a>
## D242 — Every engine warms up on the measured phase's own client threads until the tablet placement settles, and records how long (owner decision, 2026-10-13; bench, #406 reproducibility; amends D233)

**Decided:**
- **The warmup.** After the settle step (D233), `run_detailed` (every workload but the inline drivers) runs the warmup operations on the measured phase's own client threads (four for skewed-multi-shard, sixteen for the 16-client group commit), dealt the same way as the measured ones.
- **Until placement settles.** It repeats those operations until the store's tablet placement has gone 2 s without a split, merge or move (`WARMUP_SETTLED`), at most 30 s (`WARMUP_CAP`, the inline drivers' former `SPREAD_CAP`). Past the cap the run measures anyway, with a warning.
- **Every engine alike.** A store without shards never changes placement, so it warms for 2 s. The warmup's operations are the same for every engine.
- **The record** carries `detail.warmup_secs` and `detail.warmup_settled`. The inline drivers record theirs too: they already warmed until the table spread and stayed unchanged for 2 s, and now share the constants.
- **Unchanged:** the warmup's size (5% of `--ops` per pass by default; `--warmup`), the settle step, and which operations are measured. Repeated passes reuse the same warmup operations, so the measured stream stays the same.
- **Amendment (coordinator, 2026-10-19; D257's acceptance): a pending growth counts as a placement change.** The balancer publishes, beside the active count `k`, the count it is heading for (`Shared::pack_target`: `k` unless a growth is accruing its ten passes, then the count that growth reaches; the bench hook `pack_target()` on the engine and the public store returns both). The warm-up, `run_detailed`'s and the inline drivers', restarts its quiet window on every pass that reads the target above `k`, so it ends only once the growth has decided and its moves have gone 2 s quiet; the 30 s cap stays. Why: perf2's A/B of D257 (no riders r3) came out of the single-threaded load packed on 4 shards; under sixteen clients the growth needs ten light-to-heavy passes (about 1 s) and the first move then waits a dwell, so the first move landed after about 2 s, just as the quiet window closed. The measured phase then held the 4 → 16 regrow: 70 moves, 13 splits, write p99.9 424 µs against 112 on a run that started spread. No quiet-length constant catches that reliably (the packed start varied from 4 to 12 shards run to run); the balancer's own decision does, and adds nothing on the bench side. A pending shrink is not waited for: its last steps are slow (fifty passes) and cost nothing visible at one client, and the quiet rule covers their moves. The engine side, regrowing faster on a jump, is #744 (Phase 4).
- **Second amendment (coordinator, 2026-10-19; from the 0.3.0 gate on 5933e23e): the warm-up does not end while fewer shards hold a tablet than the active count.** `run_detailed`'s warm-up restarts its quiet window on every pass whose shares show fewer shards holding a tablet than `k` (bounded by the number of tablets: fewer tablets than active shards is as spread as it gets); the 30 s cap stays, and a shrink is still not waited for. Why: blob33's packing A/B of D257 on the cross-shard row (6 packing-on reps) ended the warm-up after 2.6–3.1 s in 2 of them, with the first amendment in place: the growth had decided (k = 16, so no target above `k`), but its moves come one per dwell from the shards that hold the tablets, and a gap of more than 2 s between two moves (a move waits for a compaction on its tablet) closed the window with 11 of 16 shards holding tablets. Those measured phases ran 228–236K ops/s with 9–10 moves and 128–151 moved refusals against 399–414K settled. The balancer's own count says where the placement is heading, and the shares say whether it got there.
- **Third amendment (coordinator, 2026-10-27; from perf2's A/B on #983): the warm-up waits for the store's first pack.** While the store is on all `n` shards and reads light enough for one fewer (the shrink condition D217 counts its passes on), the balancer publishes the count that first pack reaches as `pack_target`. The warm-up, `run_detailed`'s and the inline drivers', restarts its quiet window on every pass that reads the target different from `k`, so it ends only once the first pack has decided and its moves have gone quiet. The cap is now 60 s with or without a durability switch (`WARMUP_CAP`, owner, 2026-10-27; it was 30 s without one): a store still undecided at the cap measures anyway, with `warmup_settled: false`. Later shrinks are still not published or waited for. The record gains `detail.warmup_settle_secs`, the seconds into the warm-up of the last placement change or pending reading; the quiet window follows it. Why: in 3 of perf2's 6 runs of skewed-multi-shard at the default level on #983's head, the warm-up ended after 5.9–6.1 s on a store still unpacked (1–2 tablets on each of 16 shards): an unpacked store reads quiet while its first pack accrues its count. The 16 → 5 pack then ran inside the measured phase, with 16–19 moves and a p99 of 121–138 µs against 94–97 on the runs that packed inside the warm-up. This changes the gate's methodology: a run that packs measures its packed placement, and its warm-up can run up to the cap where it used to end after the 2 s quiet window. **The cap (owner, 2026-10-27):** perf287's check of skewed-multi-shard at Buffered on #993 settled at 27.5 s and ended its warm-up at 29.6 s, 0.4 s inside the 30 s cap (the first pack waits for D255's 300-pass window), so the owner raised `WARMUP_CAP` to 60 s, the after-switch cap's value. A run that settles still ends where it settles; only one that never settles pays the longer wait. The gate report lists every run's `warmup_secs`, `warmup_settle_secs` and `warmup_settled`.

**Why:** ctr274's F3 read (2026-10-13) of skewed-multi-shard on Pigeonhole.
- The load runs on one client thread, which keeps every shard lightly busy, so D217 packs the table: in one pass, 13 shards each handed their tablet to shard 0.
- The 4-thread measured phase then started with shard 0 holding 13 tablets. Unpacking sheds one tablet per dwell, about one per second, so a measured phase of about 5 s spent most of its time packed.
- So the phase measured placement converging after the switch from 1 client to 4, not the steady state. On the 2026-10-13 gate, Pigeonhole's skewed-multi-shard fell from 362K/368K to 285K/325K ops/s between the two pairs, with its background work doubled.
- This is a bench change, not an engine one: users' placement is unchanged. Faster unpacking (E-F3b) and making the shards agree on `k` (E-F3c) are separate engine questions.

*History: amended by [D258](#d258), 2026-10-19: the gate window also trims the data file system before every step, so each warm-up and measured phase starts on a drive with no stale mappings.*

<a id="d243"></a>
## D243 — The reproducibility check judges only runs of about 10 s or more; the short runs are lengthened, not dropped (owner decision, 2026-10-13; bench, #406 reproducibility)

**Decided:**
- **What is judged.** `phdb-bench compare` judges a matched pair only when both measured phases ran at least 10 s (`JUDGED_MIN_SECS`), or, for group commit, at least 2M commits (`JUDGED_MIN_COMMITS`, as D219 judges the durable group-commit target).
  - A shorter pair's metrics are still listed, with no tolerance, and the report names it under "Not judged (D243)" with both lengths.
  - D233's background-work note still applies to every pair, judged or not.
- **The short runs are lengthened.** `--min-secs S` now applies to client runs as well as inline ones. A client run whose configured operations measure under S seconds continues with the workload stream's next operations, in passes sized from the rate so far, until S seconds are measured. Each pass's operations are generated before its clock starts.
- **In the gate.** `run-window.sh` runs every `all` (both io_uring runs and pread) with `--min-secs 10`, and the D213 group-commit run inside it with the judged run's 2M commits (`--inline-ops`). The dry run (`--scale small`) uses 1 s.
- The tolerance itself (±20% throughput and p50, ±40% p99) is unchanged. (D259 later added absolute floors for the latencies.)

**Why:** the 2026-10-13 gate's second pair failed only on short runs:
- the D213 shape inside `all` measured 0.16 s (+53% throughput, −41% p50);
- ycsb-a with one client measured 5.3 s (+29.7%, with 2 flushes and compactions in one measured phase and none in the other);
- the 16-client group commit measured 1.7 s.

At those lengths, one flush or compaction more or less, or a placement change, moves a run by more than the tolerance. That's timing luck, not the engine. In the full-scale `all`, 18 of the 28 runs measured under 10 s, both engines included.

*History: amended by [D258](#d258), 2026-10-19: the gate window trims the data file system before every step, so the reproducibility pair doesn't also differ in how much of the NVMe the drive maps.*

*History: amended by [D259](#d259), 2026-10-19: a judged latency fails only past both its relative tolerance and an absolute floor (p50 3 µs, p99 6 µs at the default tolerance).*

<a id="d244"></a>
## D244 — A participant keeps a cross-shard commit's timestamp in seqno order with the writes it already stamped, with tablet changes on or off (owner decision, 2026-10-13; engine, tablets, #653; amends D142 and D192's note)

**Decided:**
- **(i) Within a group:** every admitted PREPARE raises the shard's timestamp floor to its commit timestamp before the group's singles are stamped. The singles get later seqnos than the PREPARE, whose seqno its coordinator reserved earlier, so none of them lands below its timestamp.
  - A PREPARE that waits for arena room isn't admitted yet and raises nothing.
- **(ii) Across groups:** a participant refuses a PREPARE `(S, T)` with `BelowFloor` (retried at once, until D241's write-stall timeout) in either case:
  - it has already stamped a seqno above `S` with a timestamp below `T`;
  - it has already routed a PREPARE at a seqno below `S` with a timestamp above `T`.
  - Ties are allowed, as in D142. D142's check (`T` below the floor or a received tablet's raise) is unchanged.
- **A refusal raises the participant's floor to the refused `T`** before it is republished (perf287's review). The participant's later singles then stamp above `T`, so the retry's fresh timestamp clears what refused this attempt.
- **A refused or aborted PREPARE's own entry is dropped** (perf287's review). It never lands. Kept, it would refuse PREPAREs out of order with it until the watermark passed it, and their retries could not clear it: no published floor included its timestamp.
- **The history** is `ShardState::stamps`, per shard, kept only with more than one shard:
  - one `(seqno, timestamp)` per group of singles: its first, which is the group's lowest of both;
  - one per PREPARE routed to the shard.
  - Entries at or below the visible watermark are dropped once it holds 32. A PREPARE's seqno stays held by its coordinator until it is applied everywhere, so it is above the watermark, and a dropped entry is always below it.
  - It is shard-local; no shared state changes.
- **With tablet changes off too** (owner decision, 2026-10-13):
  - the coordinator reads every participant's floor, as with tablet changes on;
  - participants run D142's check and (ii), and keep a commit's parts for its retry.
  - So D11 holds for every cross-shard commit. This amends D192's note that a share's timestamp could be below its participant's floor with tablet changes off.

**Why:** issue #653.
- The coordinator reserves `S`, then picks `T` above the floors it reads. A single queued on a participant ahead of the PREPARE got a later seqno but a lower default timestamp, since the participant raised its floor to `T` only after stamping it.
- D142's check (`T` below the floor) cannot see that: the floor was still below `T`.
- A later seqno then sat below an earlier one's timestamp on one tablet, against D11. A latest read of a cell both wrote returned the earlier commit's value.
- Found on `main` and on v0.2.2. The same happens across groups, when the single is stamped before the PREPARE arrives. It also happens between two PREPAREs routed in one group, found by this decision's sweep.
- With tablet changes off, nothing ordered a share against its participant at all: a sim probe over 3 shards, one tablet each, found about 50 inversions per 400-operation run.

**Cost:** more `BelowFloor` refusals, which are retried.
- With tablet changes on, (ii) refuses what D142 used to let through inverted.
- With tablet changes off, D142's own check now applies too, with #637's race. That is the coordinator's timestamp falling behind a busy participant's floor, refused even when seqno order holds.
- On the sim's frozen clock:
  - with tablet changes on, D244 leaves as many commits stuck as `main` (±1 of 22–89 per seed, 12 seeds);
  - with tablet changes off, 73–98 per seed against none, from #637's retry livelock.
- On a moving clock the write-stall timeout ends every such wait with `Busy`. #637's option 4 (D142's check made exact by seqno) would remove most of these refusals.

**Tests** (`crates/engine/tests/tablets.rs`):
- `a_participant_never_stamps_a_later_seqno_below_a_prepared_commit_timestamp`: (i), the #653 interleaving.
- `a_participant_refuses_a_prepare_above_a_later_seqno_it_stamped_in_an_earlier_group`, and its moving-clock twin `a_cross_shard_commit_refused_below_a_later_stamp_commits_on_retry_with_a_moving_clock`: (ii), then the retry commits (in 3 and 4 attempts; the later refusals are D142's check while the participant's queued singles drain).
- `an_aborted_prepare_does_not_keep_refusing_later_commits`: a transaction that fails validation leaves no entry behind. Without the drop, a later commit took 50 attempts; with it, 1.
- `default_timestamps_keep_seqno_order_on_every_shard_under_cross_shard_commits` and `…_with_tablet_changes_off`:
  - a seeded sweep over 3 shards (6 tablets with moves, or 3 tables with tablet changes off), with bursts that run one shard's floor ahead;
  - it checks that every shard's committed WAL records keep their timestamps in seqno order;
  - it runs on a moving clock with a 200 µs write-stall timeout, accepting `Busy`.
- Each part disabled fails its test:
  - the tablet-changes-on sweep fails at seed 1 without D244, and at seeds 1 and 3 without (i) or (ii) alone;
  - the tablet-changes-off sweep fails at seed 1 without D244's tablets-off part.
- 300 seeds of each pass.
- **The model checker's harness:**
  - it excuses a PREPARE-only attempt as refused and retried only when the engine reports that seqno as refused (`Engine::take_refused_attempts`, a test hook), since tablet changes off now retries too;
  - with more than one shard it reads a retried commit's timestamp from its entries and moves the clock past the floors, as it did with tablet changes on;
  - the shard-count comparison no longer compares seqnos, since a retry's seqno depends on the shard count.
- **`purge_race`'s lagging-share case** (D192's note) can no longer arise: the share is refused below its participant's floor. The test now checks that the retry leaves the purge alone.

*Amended by D263 (#717): the timestamp is picked at the decision; (i) and (ii) and the `stamps` history go. Per-cell order comes from admission (a single on an undecided share's rows waits for its decision); per tablet a straddle on other rows is allowed and documented.*

<a id="d245"></a>
## D245 — A cross-shard commit's timestamp is checked exactly by seqno; its coordinator raises the participants' floors before it reserves; a PREPARE short of room is refused, not held (owner decision, 2026-10-13; engine, tablets, #637, #655, ICR 0042; amends D142, D244)

**Decided** (tablet changes on or off, as D244):
- **The coordinator raises, then reserves.** `start_coordination` picks `T` above each participant's published floor, move raise and coordinator raise. It `fetch_max`es `T` into every participant's new `Shared::ts_coord_raises` (Release), and only then reserves the commit's seqno `S`.
  - A participant's later reservation is a read-modify-write on `NEXT_SEQNO` after the coordinator's, and the group loads `ts_coord_raises` (Acquire) once after reserving, before it stamps. So a single at a seqno above `S` always stamps above `T`. That was #653's case, and #637's false refusals: a single stamped at the wall clock while the PREPARE waited in the queue.
  - `ts_coord_raises` is separate from `ts_raises` (a moved tablet's floor), which D142 compares to a PREPARE with no regard to seqno. Coordinator raises for later seqnos there would refuse earlier ones again.
- **The participant's check is exact by seqno.** A PREPARE `(S, T)` is refused (`BelowFloor`, retried as D241) only if one of these holds:
  - a write at a seqno below `S` has a timestamp above `T` (`floor_below`);
  - a write at a seqno above `S` has a timestamp below `T`.
  - The shard's assignment-order floor no longer refuses anything.
  - The history is D244's `stamps` (each group's first single, each routed PREPARE), plus each group's last single (`stamps_last`), plus `pruned_max`: the largest timestamp pruned or stamped before the history (the floor at open, raised by replay).
  - The move-raise check (`T` below `ts_raises`) stays as D142 had it, counted apart.
  - What can still refuse: a straddle (a reservation before `S` stamped after the coordinator's read), or two coordinators' crossing pick→reserve windows. Each refusal raises the participant's floor past what refused it.
- **A retry keeps its first timestamp when it still fits,** as before: at or above every participant's coordinator raise (a tie never reorders), and within `preset_fits`. A participant that stamped since raised its floor past it, so the retry takes a fresh one. A stamp not yet published is caught by the participant's check.
- **A PREPARE that would wait for arena room is refused** (`PrepareError::Room`, ICR 0042) instead of waiting. Its held seqno capped the durable watermark below the memtables a freeze needs durable (D227), and no room ever freed (#655).
  - The refusal starts the room wait's freeze and flush (D124), even with no single waiting.
  - The coordinator parks the retry until the participant reports room freed (`RoomFreed`) in a later room generation than the refusal's.
  - The participant reports when its room wait ends, or when no single waits, no flush is in flight, and more room is free than at the refusal (after reclaiming memtables readers left). Nothing freed, no report: never a loop.
  - A parked retry ends with `Busy` at D241's timeout, with `Closed` at close, and with `Closed` once a shard has died (D227).
- **Issue #70's frozen-clock refusal of a room wait** now waits until the group's admitted members have applied. Their reservations starve the freeze until then. A PREPARE waiting in `prepared` used to mask this.
- **D244's parts:** (i) stays (the admitted PREPARE's raise, now in admission). (ii)'s later-seqno side is subsumed for singles, and stays for PREPAREs.

**Why:** #637.
- On the AX102 (16 shards, 32 clients, xshard_contention), main's default configuration refused each cross-shard commit about 3 times (ctr274: almost all `BelowFloor`) and ran at 11K ops/s, against 401K with tablet changes off.
- D244 brought that cost to tablet changes off too (42K).
- The refusals came from D142's assignment-order check racing the participant's wall-clock stamps, not from real seqno inversions.

**Convergence:**
- **Room refusals** end when a flush frees room; only singles can keep winning it, and D241 bounds that.
- **Ordering refusals** need a straddle or a crossing inside the retry's own pick→reserve window. Each is another commit progressing, so the protocol is lock-free, and D241 bounds a single commit's starvation.
- **On the sim's single-threaded steps** neither can happen, so no ordering refusal ever does. The tests use that as an oracle.

**Tests** (`crates/engine/tests/tablets.rs`, `src/ts_order_loom.rs`):
- the D142 sweeps on and off, with 0 ordering refusals;
- `cross_shard_commits_converge_on_a_frozen_clock`: no `Busy`, nothing stuck, with the default timeout;
- the cross-group tests, which no longer refuse: attempts = 1 + room refusals;
- `a_prepare_short_of_arena_room_is_refused_and_commits_once_room_frees` (#655);
- `a_lone_prepare_against_a_full_arena_commits_on_a_frozen_clock`;
- `a_retry_waiting_for_room_fails_busy_at_the_write_stall_timeout` and `…_ends_closed_at_close`;
- `a_moved_retry_keeping_its_timestamp_still_raises_its_new_participant_first`;
- loom models of raise→reserve against reserve→stamp, the straddle and crossing coordinators.

Each part disabled fails its test.

*Amended by D263 (#717): raise-then-reserve happens at the decision, for the seqno the commit applies at; the exact check by seqno goes with D244's history. The room refusal stays.*

*Amended by D264 (#724): the coordinators' raises and the move raise are one atomic, `ts_raises`, folded once per group after the reservation (`ts_coord_raises` goes).*

<a id="d246"></a>
## D246 — Contended cross-shard commits are a judged gate workload (owner decision, 2026-10-13; bench, #406; part of D247)

**Decided:**
- **The workload.** `cross-shard-contention` joins `phdb-bench`'s suite, and with it `all`. It promotes perf2's `xshard_contention` example:
  - 32 client threads over 16 partitions, `records` rows dealt over them;
  - half the commits write one row in one partition; the other half write one row in each of 2–4 consecutive partitions, in one commit (`BenchOp::Commit`, reported per type as `cross-shard commit`);
  - rows are uniform within their partition.
- **The stores.** Pigeonhole gives each partition a table of its own, created one after another, so with tablet changes off they sit one per shard and every multi-row commit is a two-phase commit; it commits with one `WriteBatch` across the tables. RocksDB commits the rows in one multi-key `WriteBatch`, SQLite in one transaction, fjall in one batch.
- **On the default config, in both `all` runs, judged:**
  - within 1.5× of RocksDB, throughput and p99, as every `all` workload;
  - tablet changes on at least 0.8× the same build with them off, judged with tablet packing off (D251, owner 2026-10-14): `all` and the contended step run Pigeonhole three times, the default, tablet changes off, and tablet changes on with `--no-packing`, and the ratio is the packing-off run's over the tablets-off run's. Packing's target of about half a shard's busy time per active shard (D217; it grows past 0.75 on average, or 0.9 on one shard with D251) caps how many shards a cross-shard load spreads over: at 175c0ffc on the AX102 the default config ran 0.56–0.61× with 4–6 shards active against 16 with tablet changes off. The default run's ratio, packing's gap, is reported beside the judged one (#707). The default run stays the one judged against RocksDB;
  - at most 0.1 wasted two-phase rounds per cross-shard commit, and no `Busy`;
  - the cross-shard commit p99 (owner, from D245's 1.02–1.03 ms at Buffered and the pre-release run's 303 µs at GroupSync): within 1.5× of RocksDB's multi-key batch p99 at each durability, and at most 2 ms at Buffered and 1 ms at GroupSync.
- **Wasted rounds** are the engine's refused two-phase rounds over the measured phase's cross-shard commits (ICR 0043, `Metrics::cross_shard.refused_rounds`, in the run's stall counters, with the refusals by reason beside them, D249's pending-share overlaps among them). perf2's inference, the sequence numbers used while measuring minus the commits measured (`RunDetail::seqnos_used`), stays in the report as a cross-check: each commit uses one sequence number and each refused round one more, and with tablet changes on a split, merge or move may use some too.
- The reproducibility pair (D243) includes it, as part of `all`; at full scale its runs measure at least 10 s.
- D247's `contended` step runs it again at 16 threads under the default durability.

**Why:** the gate had no multi-row or cross-shard commit under contention (perf287's #406 audit, finding 1), so a 36× slowdown in the default config got through: on the AX102 (perf2, main a9ecfc77), 16 shards and 32 clients at 50% cross-shard commits ran 401K ops/s with tablet changes off and 11K with them on. Locally (M5, `--scale small`, busy laptop) the new workload shows the same: 3.0K and 5.9K ops/s with tablet changes on against 219K and 204K off and 192K for RocksDB, with about 5 wasted rounds per cross-shard commit on and none off.

*Amended by [D251](#d251): the tablets on ≥ 0.8× off check is judged with packing off (`Options::tablet_packing(false)`, tablet changes on); the default config's on/off ratio, packing's gap, is reported beside it (#707).*

*Amended again (owner, via the coordinator, 2026-10-22): the check is judged with the default config, packing on, once more; the packing-off run's ratio is reported beside it. #707 is closed as fixed by D257 and D262: on main 9cb3dcb9 the default config read 0.989× tablets off at 32 clients and 0.988× at 16, with `k` at 16 throughout (#724 comment 6330564020).*

*Owner decision, 2026-10-14: the GroupSync cross-shard p99 cap is 1 ms, set from the pre-release gate run (#705: 303 µs at the default durability).*

<a id="d247"></a>
## D247 — The gate's `contended` step measures every public operation under contention on the default options, judged at two durabilities (owner decision, 2026-10-13; bench, #406)

**Decided:**
- **The rule (owner):** every public API operation is measured under contention (at least 4 concurrent clients on a multi-shard engine) with default settings at least once. perf287's #406 audit maps each operation to its gate coverage.
- **The step.** `contended` runs at 16 client threads, the default options (one shard per physical core, tablet changes and packing on, pinned threads, the row cache, the default I/O backend), each judged run measuring at least 60 s from a count every store finishes sooner (`--ops 100000 --min-secs 60`) and each reported run 45 s (owner), against RocksDB and SQLite. Every run is also time-bounded at 3× its minimum (`--max-secs 180`, 135 for reported runs; coordinator): past it no client starts another operation, the record says `capped`, and the summary lists the capped runs. A slow row then reports instead of stretching the window (perf287's D249 A/B found transfer still running after 44 minutes at full scale, the operation count being the target and `--min-secs` only a floor).
- **Two durabilities, both judged, shown side by side (owner):**
  - D108 parity, `--durability buffered`: Pigeonhole `Buffered`, RocksDB's WAL unsynced, SQLite `synchronous=NORMAL`;
  - the default, `--durability default`: Pigeonhole at `Options::default()`'s `GroupSync`, RocksDB and SQLite fsyncing every write.
  - At the default level every store loads at buffered durability (D108's level), the settle step flushes, and only the warmup and the measured phase run at the judged durability (owner, option (a)): a single-client durable load of 1M rows would otherwise be most of each run and about 15 minutes of the window.
  - After that switch, the warmup waits for a quiet window of 5 s instead of D242's 2 s, capped at 60 s instead of 30 (`WARMUP_SETTLED_AFTER_SWITCH`, `WARMUP_CAP_AFTER_SWITCH`; coordinator, from ctr274's data). Packing re-places the tablets for the durable level over 30–40 s, one move per dwell with gaps longer than 2 s. On the AX102 (ctr274's D248 A/B, ycsb-a at 16 threads, default durability), a 2 s window ended the warmup at 2.0–2.2 s and a 5 s minimum before it still ended mid-spread (92.6% of packing-off, p99 1.35×), while 30 s of warmup measured 98.3% and p99 1.05×. Unchanged when the load and the run commit at one level.
- **Judged within 1.5× of RocksDB and of SQLite**, throughput and p99, at both levels: ycsb-a, ycsb-b, ycsb-c, ycsb-e, ycsb-f, sparse-wide, time-series-ttl, cross-shard-contention (D246), and counters.
  - ycsb-f's read-modify-write is atomic across clients in every runner, so it runs at 16 threads: Pigeonhole's is a `commit_if` on the value read (absent when there was none), retried on a lost race; RocksDB's and fjall's hold a striped row lock from the read to the write; SQLite's is an immediate transaction. D246's own checks also apply at both levels: tablets on ≥ 0.8× off, ≤ 0.1 wasted two-phase rounds per cross-shard commit, no `Busy`.
  - `counters` (not in `all`): 16 clients, 90% increments of a Zipfian row's counter and 10% reads. Pigeonhole increments a counter family (`incr`, no read); RocksDB merges with an associative `i64` add; SQLite upserts; fjall reads and writes under a row lock.
- **Reported, not judged:** transactions (`transfer`, in the step: 16 clients each moving one unit between two Zipfian accounts in a transaction that reads both; Pigeonhole's optimistic `Transaction` retried on a conflict, with conflicts per transfer reported; RocksDB and fjall lock both rows, SQLite an immediate transaction; its RocksDB ratio is held against cross-shard-contention's for the 3× rule), async commits and reads (`--async 4` and `--async 16`, Pigeonhole: each client thread keeps 4 or 16 operations in flight through `commit_async`, `read_async` and `get_async`, polled by the thread itself; ycsb-a and ycsb-c, held against the judged synchronous runs on throughput; the p99 is shown, not compared, since in-flight operations queue by design; locally 4 in flight ran 1.6× the synchronous clients and 16 fell back to their level, and if the AX102 shows the same it becomes an engine investigation: placement churn against D210's admission backpressure), filtered scans (`adjacency-prefix`: adjacency's one-vertex scans reading only edges under a qualifier prefix that picks about a tenth of the vertices, held against ycsb-e; `time-series-range`: time-series' newest-ten scans reading only points 12 to 6 hours old, which skips the newer ones, held against time-series-ttl), large values (ycsb-a with 16 KiB values, above the default 4 KiB blob threshold, over 50K records (8 GB), against RocksDB and SQLite, its RocksDB ratio held against the judged ycsb-a's), deletes (`sparse-wide-deletes`: sparse-wide's mix with 5% of the operations deleting, half a column and half a whole row, taken from its puts; held against the judged sparse-wide for the 3× rule), snapshot and durable-snapshot readers riding on ycsb-a (`--riders`, Pigeonhole: a reader thread on each kind of snapshot, re-taken every 100 ms, scanning 10 rows at a time, then a durable backup 2 s in and a full compaction, with the foreground p99 held against the judged ycsb-a run's), reader processes (a follow-up), application-owned mode on the defaults (`phdb-bench app-owned`: skewed writes inline from each shard's own thread, 16 in flight, pinned, no pre-split so the balancer and packing run, about 20% of the commits `WriteBatch`es across shards; at Buffered it is held against the scaling gate's N = 16 for the 3× rule; a commit refused with `Busy` is retried and counted, and any such retry is flagged for an issue, since locally these batches stalled commits past the 30 s write-stall timeout, a BelowFloor storm that D245 addresses), and close (timed for every run, `close_ms`), backup and compact under load (the riders). A reported row more than 3× worse than its nearest judged row gets an issue.
- The step lands in parts: the judged rows first (this entry's PRs), then commit_if, counters and the reported rows.
- **The window.** The pre-release run (#705, 2026-10-14) measured the step at 197 minutes and the whole window at 314. With the packing-off cross-shard runs D246's amendment adds (one per `all` pass and per contended level), plan about 200 minutes for the step and about 5.4 hours for the window. Every contended run is bounded at 3× its minimum (#689), so a slow row reports instead of stretching it.

**Why:** the gate measured almost nothing contended, nothing at the shipped durability, and no cross-shard commit (perf287's audit, findings 1–3), which let #637's 36× slowdown through. Judging both levels keeps the long-standing like-for-like comparison (D108) and adds the one users get by default.

*History: amended by [D261](#d261), 2026-10-19: each operation type's p99 against the same type in each store is reported beside the judged mixed p99, not judged; contended default ycsb-f's mixed p99 (2.13× SQLite) is accepted for Phase 4 (#755).*

<a id="d248"></a>
## D248 — Packing's busy fraction counts group-sync utilization: a shard's busy is the larger of its thread's busy time and the time its group syncs are in flight (owner decision, 2026-10-13; engine, tablets, #406; amends D217)

- **The busy fraction (D217's measure)** is, per engine-owned shard and balancer pass, `max(turn busy, sync utilization)`:
  - `turn busy`: the rise of the thread's busy time over the time since the last pass (as before);
  - `sync utilization`: the group syncs' in-flight time in that interval over the interval, capped at 1. That is the integral of the group syncs in flight (from `submit_sync` to the shard handling the completion).
- **Not divided by `group_sync_depth`** (coordinator, 2026-10-13, after the A/B below). Dividing by the depth (2 on Linux) was the first design. Measured, it stopped packing at 4 shards with each at about 0.6, inside D217's band (grow only above 0.75), while the shards already queued hard. A shard's concurrent `fdatasync`s fold into one journal commit (D221's note), so they behave as one server, not two. On macOS the depth is 1 anyway.
- **`max`, not a sum:** a turn often runs while a sync is in flight, so a sum would count that time twice. A shard bound by its syncs reads about 1, and a CPU-bound one reads as before.
- **Utilization, not waiting commits.** Utilization is linear in the offered load and adds up across shards: without batching, `B` is about the group rate times the sync latency, whatever `k` is. So `k = ceil(B / 0.5)` keeps its meaning, and packing settles as it does for CPU load. A queue length (waiting commits, batched groups) grows faster than linearly near saturation and falls to 0 once spread, so packing on it overshoots, shrinks back, and oscillates.
- **Engine-owned shards only.** Syncs count only while the runtime counts the thread's busy time. An application-owned store keeps D217's behaviour: no packing.
- **Only group syncs:** a group's own and a batched one. Not spare-segment preparation, rollovers or manifest syncs. `Buffered` and `None` stores have no group syncs, so their packing is unchanged.
- **Exposed:** `ShardStats::sync_busy_nanos`, the integral of the shard's group syncs in flight, beside `busy_nanos`.
- **Cost:** a clock read and a few adds when a group sync is submitted and when it completes, on the shard thread. Nothing on the `Buffered` commit path the callgrind ceilings measure.

**Why.** perf287 on the AX102 (2026-10-13), ycsb-a, 16 threads, at the default durability (GroupSync):
- Packing pulled the table onto 2 shards, each about 0.5 busy by turn time. B was about 1.0, `k = 2`, and `B / k` stayed under 0.75, so `k` never grew.
- Against packing off (16 shards, 0.14–0.23 busy each), that cost 36% of throughput (265K against 415K ops/s), +74% put p50 (92 against 53 µs) and about 5× put p99 (~600 against 121 µs).
- `busy_nanos` counts only the thread's turns, so the time commits wait on in-flight group fsyncs was invisible. On macOS (F_FULLFSYNC), a durable table packed onto one shard.

**Tests** (simulated, deterministic: deferred I/O, the clock moved while syncs are in flight):
- Four shards 5% busy whose group syncs stay in flight through each pass keep `k` = 4 and stay spread. This fails without the sync term.
- The same load at `Buffered`, and a durable trickle (syncs in flight 5% of the time), pack onto one shard as before.
- Application-owned shards: a durable load that would pack if counted is not packed. This fails without the engine-owned gate.

**Measured** (ctr274, AX102, 2026-10-13; ycsb-a full, 16 threads, ≥60 s, uring; #662's bench and perf287's timeline; two runs each):

| GroupSync | ops/s | put p50 | put p99 | active shards |
|---|--:|--:|--:|--:|
| main (perf287) | 263–268K | 92 µs | 569–745 µs | 2 |
| sync time ÷ depth (first design) | 309–318K | 73–76 µs | 545 µs | 4 |
| sync time ÷ 1 (adopted) | 363–383K | 57–61 µs | 166–180 µs | 12–13 |
| packing off | 409–418K | 53 µs | 123–137 µs | 16 |

- ÷1's averages include the 30–40 s in which `k` grows inside the measured phase: the bench loads at `Buffered` and switches after the settle. After that, commit rates match packing off. The bench now warms up again after the switch until the placement settles (D242's rule).
- `Buffered`: main 807–815K, D248 815–820K, unchanged, still packed on 4 shards.

**Acceptance (AX102 A/B, perf287's shape):**
- ycsb-a, 16 threads, `--durability default`, defaults against `--no-packing`: at least 95% of packing-off throughput, and put p99 within 1.3×.
- ycsb-a `--durability buffered`, main against D248: unchanged within noise, and packing still packs.
- `k` settles with no oscillation over the 60 s.

*History: amended by [D255](#d255), 2026-10-15: the busy measure gains a third term, the shard's write-stall time, with the same `max`.*

<a id="d249"></a>
## D249 — A cross-shard PREPARE that overlaps only an undecided share is refused and retried, not a `Conflict`; crossing pairs resolve older-first (owner decision, 2026-10-13; engine, #406, ICR 0045; amends D145's refusals, builds on D241 and D245)

**Decided:**
- **The participant (admission of a conditional PREPARE: a transaction or `commit_if`):**
  - Overlapping a row touched by an earlier member of its own group: it waits for the next group, as conditional singles do. That is this shard's own write, which waits on no other coordinator.
  - Overlapping another commit's **undecided** share (`pending_rows`): refused with `PendingOverlap`. Nothing waits, so the wait-for graph keeps only today's edges, and there is no deadlock.
  - `Conflict` reaches the application only from validation (a commit to a read key above the snapshot) or a failed predicate.
- **The coordinator:** the round is aborted and the commit retried with a fresh seqno, the **same snapshot and read set**, and its timestamp kept where `preset_fits` allows (D245). It gives up with `Busy` at D241's timeout.
  - Revalidating the same reads against the snapshot at a later seqno is the same OCC check, so a retry can only reach `Conflict` through a real write.
- **When the retry runs:** on an overlap `Wake` from the refusing participant (D245's wake, generalised: `Wake { from, reason: Room | Overlap }`), never on a timer alone, so it also progresses on a frozen clock.
  - The participant sends it once no undecided share there overlaps the refused commit's rows.
  - A wake that comes before the refused round is decided is kept for the retry.
- **Crossing pairs, older-first:**
  - **Priority** = `(submitted_at, the seqno of the commit's first attempt)`, the same across retries (carried in a boxed `Retried`).
  - A commit refused over an **older** commit's share waits at that participant until one of:
    1. the older commit's next attempt there is decided (a later seqno);
    2. a `Decide` with `last` (the older commit committed, failed, or gave up; the coordinator decides whether it retries when it sends the abort, and keeps to that);
    3. a `Released` (the older commit's retry was routed without that participant).

    Each of these comes, so no deferral outlives the older commit's own progress.

**Why:** blob33's measurement (ICR 0045). Cross-shard transactions conflicted 5–16× more often than same-shard ones, and under `GroupSync` 85 times per transfer. Most were not conflicts: a PREPARE that met another commit's share during its 2PC window failed at once, and the application retried in a tight loop.

**Convergence:**
- Every refusal names an undecided share, and every share is decided in bounded time: its coordinator waits only on replies, which never wait.
- After the decision, the refused commit either proceeds or validates against a real write.
- Older-first orders crossing pairs. The oldest of a set of mutually overlapping commits is never deferred, so by induction every commit in a finite set finishes.
- An endless stream of *new, younger* commits can still beat an old one. D241's timeout bounds that with `Busy`; "intents" are the follow-up if the gate shows it matters.

**Tests** (`crates/engine/tests/tablets.rs`, frozen clock):
- an overlap with an undecided share that then aborts: the transaction commits, with no `Conflict`;
- the same with the share committing a write after the snapshot: `Conflict`;
- two crossing PREPAREs on two shards: both resolve, at least one commits, any `Conflict` is real;
- on three shards, both refused: the older commits first, then the younger. The trace shows the younger deferred behind the older, then released by the older's next attempt.
- **The guards (each fails with older-first disabled, `behind` forced to `None`):**
  - `crossing_prepares_driven_to_recross_every_retry_still_resolve_older_first`: the same pair, with every retry driven in the crossing turn order `[2, 1, 1, 0, 0]`. With the rule, 2 refusals, then A commits and then B. Without it, the pair re-crosses on every retry: 2,002 refusals in 2,000 rounds, and neither resolves on the frozen clock.
  - `a_younger_commit_refused_behind_an_older_one_waits_for_the_older_ones_next_attempt` (ctr274): with A's coordinator stopped after A's first round aborts, B must not go ahead. Without the rule, A commits at seqno 9 and B at 8.
  - **Livelock on the simulator:** ctr274's search of 442 two-shard schedules (42 regular, 400 seeded) found none with the rule off. Serialized shard turns break the symmetry within a round. The three-shard driver above does reproduce one, so the rule is guarded by a test, not only argued. Truly concurrent engine-owned shard threads make it likelier.
- The model checks, the D142 sweeps and D245's frozen convergence sweep (zero non-room ordering refusals) pass unchanged.

**Cost of a dropped future:** a commit whose future is dropped keeps running (as before), and its retries carry the transaction's seqno pin (#672) in `validate`, moved into each retry and never re-created. So under overlap starvation, a dropped commit can hold its seqno pin, and with it compaction's GC of what that snapshot sees, for up to `write_stall_timeout` before it ends with `Busy`. It is accepted: the pin is released on every outcome, and the same bound applies to any waiting commit (D241).

**Shape on the commit path:** `CommitReq`'s inline `Option<Preset>` (32 bytes) becomes `Option<Box<Retried>>` (8), so every queued commit message is smaller. The new check runs only on the existing conditional-PREPARE branch.

*Amended by D263 (#717): every PREPARE that writes a row an undecided share writes, or reads one it writes, is refused with `PendingOverlap` and resolved older-first, not only conditional ones. A commit waiting behind an older one is woken by that commit's next attempt there committing (or its last, or `Released`), not by an aborted attempt. A retry that runs on one shard (#772), or that is answered at its coordinator without a further attempt (#776), releases its earlier participants.*

<a id="d250"></a>
## D250 — Language bindings go native-to-Rust where the language allows; the C ABI is for C and FFI-only languages (owner decision, 2026-10-13; bindings, #690, #696–#699; amends the spec's Language scope)
**Decision:** each per-language library binds the Rust crate directly through that language's native-extension or direct-to-Rust layer where one exists:
- PyO3 for Python (#696);
- `magnus`/`rb-sys` for Ruby (#697);
- UniFFI for Swift and Kotlin (#699);
- comparable layers, such as napi-rs for JavaScript, for languages added later.

The stable C ABI (`pigeonhole.h`, `pigeonhole-capi`, #690) serves C itself and languages that can only reach native code through a C FFI, such as Go through cgo (#698).

**Why:**
- These layers give each language idiomatic types, async integration and error mapping without a C round trip.
- They avoid hand-written marshalling, and they are the common practice for Rust libraries.
- The C ABI stays useful where nothing better exists.

Every binding is still a thin wrapper over the Rust crate, never a reimplementation of the engine.

**Consequences:**
- The Rust crate's public API is the contract every binding follows.
- The C ABI no longer has to precede the Python, Ruby or mobile libraries; only the Go and C users need it.
- AGENTS.md's rule to keep Rust-only types out of what becomes the C ABI boundary still applies to `pigeonhole-capi`.

<a id="d251"></a>
## D251 — Packing grows the active shards when one of them is saturated, and write skew moves small tablets when none dominates (owner decision, 2026-10-14; engine, tablets, #406; amends D214, D217 and D246)

**Decided:**
1. **Hot-shard growth (amends D217).** An active shard whose **turn busy** (its thread's work time, without D248's sync term) is above `PACK_HOT` = 0.9 grows `k`, with D217's persistence (`PACK_GROW_PASSES`, 1 s), whatever `B / k` says. While any active shard is that hot, `k` never shrinks.
   - **Turn busy, not D248's combined busy.** A shard whose group syncs run back to back may not cool however many shards share the load, so its sync term neither grows `k` nor blocks a shrink here; D248's average rule still applies to it. Each shard now also publishes its turn busy (`LoadSlot::turn_busy`).
   - **Once per hot spot.** A hot-triggered growth repeats only after the hot shard's turn busy has fallen by at least 0.02 since the last one (what it shed helped), or when another shard is the hot one. A hot spot that more shards can't cool (one unsplittable tablet) grows `k` once.
2. **Small tablets move (amends D214).** When no tablet carries the eighth of the gap a move needs, and none dominates (the hottest is below the gap), the largest tablet that fits the coldest shard moves. The dominant-tablet case keeps D214's split.
3. **D246's tablets check is judged with packing off (amends D246; owner, 2026-10-14).** `cross-shard-contention`'s tablets on ≥ 0.8× off compares tablet changes on with `tablet_packing(false)` against them off. The default config's ratio (packing on) is reported beside it as packing's gap. Packing targets about 0.5 busy per active shard, which caps a closed-loop, mostly cross-shard load at about half of spreading over every shard (0.48–0.63× in the 2026-10-14 A/B on the AX102, #707). Packing less when commits are mostly cross-shard is a Phase 4 design (#707). *Superseded (owner, 2026-10-22): D257 and D262 closed the gap (0.989× at 32 clients, `k` 16 throughout; #724 comment 6330564020), #707 is closed, and D246's check is judged with packing on again.*

**Why** (the pre-release gate, 175c0ffc, AX102, 2026-10-14): `cross-shard-contention` with tablet changes on ran at 184K ops/s, against 384K off.
- The load had packed the table onto `k` = 2 (the slot floor: 22 tablets × 4 families). In the measured phase, shard 0 held 16 tablets at 0.98 busy and shard 1 held 6 at 0.31.
- **D217:** B = 1.29, B / k = 0.645 sits between the shrink (0.5) and grow (0.75) thresholds, so `k` stayed 2 for good. The average hides a saturated shard.
- **D214:** shard 0 was at 1.75× the mean, but each of its 16 tablets carried about 1/16 of its load, under the eighth of the gap (≈ 0.107) a move needed. The split fallback needed a slot, and 16 × 4 families fill a shard's 64. Nothing moved.

**Guards** (perf287's pre-review): no flapping when more shards can't help (the once-per-hot-spot rule, turn busy only), the shrink as before once the load drops, and a single sync-saturated writer settling at `k` ≤ 2.

**Trade-off** (perf287's review): a hot spot more shards can't cool (one unsplittable tablet, a single hot row) still grows `k` once, and while it stays hot `k` never shrinks. The extra active shards stay until the hot spot ends, and then D217's shrink applies as before. That costs some of packing's wakeup savings for as long as the hot spot lasts, rather than risking a shrink back onto the saturated shard and a grow-shrink cycle.

**Tests** (simulated, busy hooks; each fails with its rule removed):
- A full shard of 16 equal small tablets beside a lightly written one sheds them (fails without the small-tablet move).
- Packed to `k` = 2, a shard at 0.98 beside one at 0.31 grows `k`, keeps it while hot, and lets it shrink back once the load drops (fails without hot growth).
- A hot shard holds off D217's shrink (fails without the shrink guard).
- One unsplittable hot tablet grows `k` at most once (fails without the once-per-hot-spot rule).
- A single sync-saturated durable writer (D248 term about 0.95, turn busy 0.05) settles at `k` ≤ 2 (fails if the hot test counts the sync term).

**Acceptance (AX102, after the gate):** no stuck placement, and a buffered packing check (ycsb-a, time-series-ttl, ycsb-f) unchanged against main. Both held in the 2026-10-14 A/B: a 90 s run settled at `k` = 4 with every shard at 0.80–0.89 busy, with no flapping, and the packing check was within run-to-run noise. Cross-shard-contention with tablet changes on reached 0.48–0.52× off (201K against 384–400K), short of D246's 0.8×; hence item 3.

*History: amended by [D255](#d255), 2026-10-15: the hot test takes the larger of the turn busy and the write-stall fraction (still without the sync term). Amended by [D256](#d256), 2026-10-15: a thread-saturated growth sets the floor D255's stall-driven growth sets; the hot test stays on the turn and stall terms. Amended by [D257](#d257), 2026-10-19: the hot growths and their floors are the capacity side and are not held to the shards the commits in flight call for.*

<a id="d252"></a>
## D252 — Engine-owned mode runs flush and compaction on dedicated threads by default, `clamp(shards / 4, 1, 4)` of them; when they don't fit the CPU set, the shards stay pinned (owner decision, 2026-10-14; pigeonhole, runtime, engine, #406, #721; amends the spec's "Background work", D235)

**Decided:**
1. **The default count.** `Options::compaction_cores` left unset resolves at open, per mode:
   - **Engine-owned:** one thread per four shards, at least one and at most four, from the resolved shard count (16 shards: 4; 8: 2; up to 7: 1).
   - **Application-owned:** 0. That mode starts no threads, and an explicit `k > 0` is still refused (D40, unchanged).

   An explicit value is used as given; `compaction_cores(0)` keeps flush and compaction on the shards in time slices, as before.
2. **`EngineOptions::new` keeps `compaction_threads = 0`.** The engine's deterministic seed-replay harnesses and tests depend on flush running on the shard thread (a thread completing background work breaks seed replay). The default is the public crate's, the documented user surface. The public `model` test runs at the new default.
3. **Pinning fallback (amends D235).** When `pin_plan(shards + k)` finds too few free CPUs, `Runtime::start` plans the shards alone: the shards are pinned, the compaction threads run unpinned, and `Pinning::unpinned` reports `TooFewCpus`. Before, nothing was pinned. With the new default, that would have dropped shard pinning on every machine without SMT (shards = cores). The other refusals (no hard affinity, a CPU quota) still pin nothing. Compaction threads take the CPUs after the shards', so on SMT machines they are the first shards' siblings.

**Why.** The cross-shard p99 investigation (perf2, AX102, 2026-10-14) timed the shard threads' turns. With `compaction_threads` 0, ~81% of turns ≥ 400 µs were flush (33%) or compaction (48%) slices, at the runtime's 500 µs `time_slice`. Each one holds up every commit queued on that shard and, through 2PC, the global visibility wait behind it. A 100 µs slice helped about half as much and worsened the 8-client p99.

The A/B below was run on main 175c0ffc on the AX102 (16 shards). It uses the gate's contended (D247) command lines: 16 threads, `--scale full`, 60–180 s, ct0 and ct4 alternated ×2. Each cell is run 1 / run 2; p99 in µs.

| Buffered | ct0 ops/s | ct4 ops/s | ct0 p99 | ct4 p99 | RocksDB (gate) |
|---|---|---|---|---|---|
| cross-shard, tablets on | 222K / 229K | 230K / 241K | 553 / 533 | 127 / 121 | 640K, 36 |
| cross-shard, tablets off | 304K / 298K | 400K / 406K | 827 / 889 | 94 / 93 | |
| ycsb-a | 807K / 823K | 1,076K / 966K | 391 / 344 | 40 / 46 | 695K, 67 |
| ycsb-f | 466K / 468K | 533K / 503K | 118 / 127 (rmw 469 / 498) | 103 / 106 (rmw 118 / 121) | 625K, 74 |
| ycsb-e | 33.2K / 33.5K | 37.5K / 35.8K | 1,540 / 1,524 | 1,352 / 1,458 | 133K, 270 |
| counters | 160K / 161K | 155K / 167K | 2,753 / 2,523 | 2,818 / 2,802 | 543K, 387 |

| GroupSync | ct0 ops/s | ct4 ops/s | ct0 p99 | ct4 p99 | RocksDB (gate) |
|---|---|---|---|---|---|
| cross-shard, tablets on | 85.1K / 81.2K | 86.4K / 89.2K | 297 / 309 | 281 / 274 | 108K, 211 |
| cross-shard, tablets off | 94.6K / 96.4K | 93.3K / 99.8K | 514 / 348 | 268 / 248 | |
| ycsb-a | 408K / 397K | 397K / 425K | 110 / 121 | 132 / 110 | 191K, 212 |
| ycsb-f | 271K / 281K | 308K / 295K | 191 / 184 | 158 / 166 | 170K, 241 |
| ycsb-e | 33.1K / 34.6K | 35.5K / 32.5K | 1,548 / 1,483 | 1,417 / 1,614 | 111K, 354 |
| counters | 91.9K / 95.7K | 88.9K / 93.2K | 3,719 / 3,441 | 3,785 / 3,228 | 106K, 537 |

No row regresses beyond run-to-run noise, and p99.9 falls almost everywhere (GroupSync ycsb-a and ycsb-f: ~1 ms → ~0.2 ms). The tails of ycsb-e (scans) and counters (counter reads) are not shard-slice stalls, and this does not touch them.

The remaining long shard turns are the flush install and its `ShareFlushed` messages: about 100K messages per flush, one per share (#716).

**The count** (AX102, Buffered, one run each). The D252 build's default is also checked here on a no-SMT 4-core set.
- **16 shards (16 clients):**
  - *cross-shard, tablets on:* p99 is 124–127 µs at ct1, 2, 4 and 8 alike (553 at ct0).
  - *ycsb-a:* throughput goes 805K → 975K → 966–1,076K → 1,072K at ct1, 2, 4, 8. ct1 removes the p99 (45 µs), but its put p99 (94) and p99.9 (1.6 ms) show a single thread falling behind.
  - *ycsb-f:* 502–536K at every count.

  The first thread removes the shard-slice stall; more threads add flush and compaction bandwidth for write-heavy loads, flattening by 4. The D252 build's default (4) ran ycsb-a at 1,058K, p99 45 µs.
- **4 cores with SMT siblings** (`taskset -c 0-3,16-19`, 4 shards, 8 clients):
  - *cross-shard, tablets on:* 168K / 181K / 188K at ct0, 1, 2; p99.9 1.1 ms → 183 µs at ct1.
  - *ycsb-a:* 489K / 531K / 551K; put p99 489 → 81 µs.

  One thread gets most of it, so the default's 1 at up to 7 shards keeps small machines lean.
- **4 cores, no SMT** (`taskset -c 0-3`, the D252 build: explicit 0 against the default 1, whose thread then runs unpinned beside 4 pinned shards):
  - *cross-shard, tablets on:* 134K → 177K, p99 159 → 118 µs, p99.9 1.13 ms → 471 µs.
  - *cross-shard, tablets off:* 142K both; p99 148 → 133 µs, but p99.9 1.11 → 3.05 ms in that single run.
  - *ycsb-a:* 353K → 447K, p99 135 → 79 µs, p99.9 537 → 146 µs.

  The fallback works, with one p99.9 outlier where the unpinned thread shares the shards' CPUs.

**Trade-off:** extra threads, and 1–4 CPUs' worth of background work off the shards. On SMT machines that work runs on the shards' siblings. On small or shared hosts `compaction_cores(0)` gives the threads back, at the cost of the write tail.

**Tests:**
- The public crate's `engine_owned_mode_defaults_to_compaction_threads` checks the threads started at 2, 8 and 20 shards, with an explicit 0 and an explicit 3.
- `application_owned_mode` opens with the default (0).
- The runtime's pinning test starts shards on every CPU plus two compaction threads, and finds the shards pinned and `TooFewCpus` reported (Linux).
- The runtime's `pool_ring` test (Linux, io_uring): a task on the pool finds the write it submitted is its thread's own to complete, is woken by that completion while the pool thread idles, and its blocking read resolves on the pool thread.

*History: the 0.3.0 gate run on the AX102 (2026-10-14c) found the default stalling under io_uring: contended ycsb-a with riders fell from ~700K to 3–8K ops/s, with 6–13 flushes, no compactions and `Busy` after the write-stall timeout, and the cold-get load wrote 45× its data. The pool threads had no ring of their own: `Vfs::attach_thread` was only called for shard threads, so every read and write of every flush and compaction, the pager's grow-ahead sync and the pump's root commit went to the backend's shared ring and its single reaper thread, which decodes every block it completes (client gets and rider scans included) and which the pool's per-block blocking waits (`SstIter::load_data`) then serialized behind. The gdb dumps show three pool threads parked in `Completion::wait` and the reaper decompressing. Fixed by giving pool threads a ring of their own, reaped in each pass and in their idle wait, as shard threads have had since #402: the pre-D252 I/O shape with the work on dedicated threads. The default count is unchanged by that fix; its capacity on write-heavy loads is measured separately on the fixed build.*

<a id="d253"></a>
## D253 — A commit's client waits 50 µs before parking, not 15 µs (owner decision, 2026-10-14; engine, pigeonhole, #406; amends D198)
**Decision:** `EngineOptions::commit_spin_nanos` and `Options::commit_spin` default to 50 µs instead of D198's 15 µs. The shard's window stays 50 µs. Both can still be set to 0, and an idle database still never spins.

**Why.** perf2 measured the gate's cross-shard mix on the AX102 (16 shards, 32 clients, Buffered, tablet changes off, 20 s runs):

| commit spin | ops/s | single-shard p50 / p99 | cross-shard p50 / p99 | client parks per op |
|---|--:|--:|--:|--:|
| 15 µs (D198) | 400K | 47 / 565–660 µs | 50–51 / 602–906 µs | 0.98 |
| **50 µs** | **466K** | **34** / 574 µs | **38** / 671 µs | **0.08** |
| 100 µs | 463K | 35 / 597 µs | 38 / 833 µs | 0.03 |

- A cross-shard commit takes two rounds (PREPARE, then decide and apply): about 41 µs to the reply at p50. A 15 µs window expires before almost every one, so nearly every client parks and pays a wake.
- 50 µs covers those rounds: throughput +16% (0.82× RocksDB's row), p50 −27%. A longer window gains nothing more.
- The p99 is unchanged: it is the global visibility wait behind held 2PC seqnos, not parking.

**Consequences:**
- A thread waiting for a `Buffered` or `None` commit may poll for up to 50 µs before it sleeps. Durable commits still park at once.
- The bench's measured instruction-count shapes set `commit_spin(ZERO)` (ICR 0016), so their counts don't change.

<a id="d254"></a>
## D254 — Share reports and settlements go one message per peer per pass, not one per share (owner decision, 2026-10-14; engine, #716; amends D225's messages)

*Replaced by D266 (#724): the reports and settlements are gone; the holders' flushed-through seqnos decide.*

**Decided:**
- **`SharesFlushed { seqnos }` replaces `ShareFlushed { seqno }`.** A participant's report pass (`report_shares_flushed`: the flush install, `Maintain` with tablet changes on, `Start`, the close) gathers the seqnos of every share it finds in SSTs by coordinator and sends each coordinator one message. A coordinator's own shares are still counted without a message (D225).
- **`CommitsSettled { seqnos }` replaces `CommitSettled { seqno }`.** A coordinator collects the commits it finds settled per participant and sends each one message (`send_settled`). That happens once per handler that can settle: the report pass, the `SharesFlushed` handler, and `run_group`'s logging of COMMIT records.
- **Each handler advances the checkpoint and checks the close once per message**, not once per seqno.
- **The semantics are unchanged.** The reports are commutative: a count per seqno on the coordinator and a set insert on the participant. So batching them changes no decision, only how many messages carry them, and when a handler's batch is sent within its turn. A commit is still settled exactly when its COMMIT is logged and every share has been reported (D24, D225), and a PREPARE or COMMIT is checkpointed under the same rules as before (`needed`).
- **A backstop (perf287's review).** `end_batch` debug-asserts that no settled commit is left unsent, and sends any that are. A loop that settled without `send_settled` would otherwise hold its participants' PREPAREs, and so their WAL, until some later settle.
- **Deterministic order.** The per-peer buffers are ordered maps, so the sends keep a seed replay's message order.
- **An empty share** (validation only) still reports at once, as a one-seqno message.

**Why** (perf2's shard-turn timer on the AX102, scratch_xs 16 shards, 32 clients, 50% cross-shard, Buffered; `.coord/perf2-xshard-p99-20261014/ct-ab/`):
- Installing a flush (`on_flushed`) took 21 ms at p50 and up to 59 ms on the shard thread, and `report_shares_flushed` was nearly all of it. It sent one message per share, about 100K per flush and 15.5–18.9M in 20 s.
- The coordinators then ran about 1 ms turns of 1024 `ShareFlushed`/`CommitSettled` messages, each advancing the checkpoint: 2,921 turns of 400 µs or more with `compaction_threads` = 0, and 7,798 with 4.
- With compaction off the shard threads (D252), these turns and the install were the remaining long-turn sources.

**Tests:**
- `share_reports.rs`: 600 cross-shard commits from two coordinators to one participant.
  - One flush reports in at most 6 messages carrying exactly 1,200 seqnos. With one message per share or commit it was 1,200 messages, and the test fails against that.
  - Every checkpoint then passes all its records, including the participant's 600 PREPAREs. The test fails if `CommitsSettled` is not sent, and the backstop's assertion fails if one settling loop skips `send_settled`.
  - After a power loss every write survives.
- The crash and model sweeps (`model_check` with and without deferred I/O, `model`, `tablets`) check the checkpoints and replay.
- New test hooks: `ShardCounters::{report_msgs, report_seqnos}` (`Engine::share_reports`) and `ShardCounters::logged` (`Engine::logged_records`).

**Acceptance (AX102, 2026-10-14):** perf2's timer build of `scratch_xs` (16 shards, 32 clients, 50% cross-shard, 20 s), on main c2d29b3d (A) and on this change (B). The runs alternated A/B, 2 each at `compaction_threads` 0 and 4. Data: `.coord/ctr274-d254-ab/`.

| | ct0 A | ct0 B | ct4 A | ct4 B |
|---|---|---|---|---|
| ops/s | 375K, 365K | 380K, 381K | 457K, 457K | 471K, 467K |
| shard turns ≥ 400 µs | 18,341, 18,143 | 16,115, 16,568 | 11,504, 12,652 | **1,051, 1,179** |
| `on_flushed` p50 / max | 21.0 / 34–42 ms | 14.7 / 21 ms | 25.2 / 42–67 ms | 21.0 / 29 ms |
| report messages handled | 15.5M | 1,679–1,695 | 18.9M | 1,837–1,877 |
| their turn p50 / p99 / max | 0.1 µs / 1.5 µs / 29 ms | 0.5 ms / 25–29 ms / 34 ms | 0.2 µs / 2–2.6 µs / 42 ms | 0.66 ms / 42 ms / 50 ms |
| long turns (≥ 400 µs) they dominate | 2,187–2,431, avg 1.6–1.8 ms | 686–688, avg 6.5 ms | 7,876–8,060, avg 1.0 ms | 530–622, avg 11–13 ms |

- **Long turns:** about 10× fewer at ct4 (the D252 default), and about 11% fewer at ct0. Throughput is up 2–4%.
- **The install:** `on_flushed` is down about 30% at ct0 and 17% at ct4. The rest of it is `report_shares_flushed`'s per-share scan (each share's slots checked against the catalog), not the messages.
- **The coordinators' longest turn (perf287's review) did not improve.** One `SharesFlushed` carries a flush's ~100K seqnos, and its handler counts each one and then advances the checkpoint past the records they free. That is the same work as before (about 7 s of long turns at ct4, against about 8 s), in fewer, longer turns: 25–42 ms p99, up to 50 ms, against a 29–42 ms max on main. Bounding it is a follow-up: handle a message in slices across turns, and make the report and its receipt cheaper per share.

<a id="d255"></a>
## D255 — Packing counts a shard's write-stall time as busy, beside its turn and sync busy (owner decision, 2026-10-15; engine, #406, #730; amends D217, D248, D251; absorbs #734)

**Decided:**
- **A third term in the busy measure.** A shard's balancer measures its busy fraction as the largest of its thread's turn busy time (D217), its group syncs' utilization (D248) and, now, the time its commits spent stalled, each over the time since its last pass: the per-shard `stall_nanos` the engine already reports (waits for arena room in `wait_for_room`, pacing on L0 depth in `stalled`), with the span of a wait still open added, so the total only moves forward and a pass's share is the difference of two readings.
- **The hot test counts it too.** D251's hot test (`PACK_HOT`, turn busy only) takes the larger of the turn busy and the stall fraction. It still leaves the sync term out: a shard's syncs may run back to back however many shards share the load, while its writers' waits for room and L0 depth do ease once it sheds tablets.
- **Pacing closes when its commits leave.** `run_group` with nothing pending (the paced commits parked or forwarded by a tablet change) ends the pacing span and its timer, as it already ended the room wait: left open, a shard with no writers read stalled, and so hot (the review's (A)).
- **A shard reads stalled when its writers stalled on at least 3 of the last 10 passes.** Each balancer pass records one bit, whether a wait for room or L0 pacing began or was open since the last pass; the term is 1 when at least `PACK_STALL_PASSES` (3) of the last `PACK_STALL_WINDOW` (10) bits are set, else 0: about three stalls a second, or one wait spanning 0.3 s, at the base interval. It is published as is, so the hot test and the shrink rule read a count, not a moving average's tail (ctr274's review of build 5: a 10-pass window smoothed to 0.893 was one pass short of hot); only the busy sum smooths it. A stall's length is the writers' latency, not the shard's load: on the fraction alone the buffered riders row (7,000 short stalls, 11% of the top shard's time) spread to five shards with 26–30% of the commits on one, and pass by pass the few stalls a second its shards took read at about half (perf2's trace: the top shards' stall terms at 0 on 99% of passes). One stall every ten passes is not capacity (a light store with one burst stays packed).
- **A stall-driven growth holds.** The active shards a stall-driven growth reached (the hot shard's stall term above its turn busy) stay active while the store still writes at least half of what it did then: the shards it spread to run cheap turns once their writers stop stalling, so the busy sum alone would call for a shrink after 50 passes, the stalls would come back, and `k` would swing between the two (the review's oscillation risk; on the AX102 `k` went 9 → 7 → 10 in 17 s with the busy sum flat). The hold clears with the writes, as packing's own gate does.
- **The first shrink from the unpacked start waits for a slow-average window** (#734). From open, the active shards shrink only once the store has written on 300 balancer passes (30 s at the base interval); every shrink then needs D217's 50 passes of the light condition as before. A load's phases (a load, a settle, a warm-up) dip between them for a few seconds, and a pack on a dip moves every tablet twice, the second time while the load is measured: on the AX102 the dip packed 16 shards to 2 at about 21 s (5.5 s before the ramp guard) and D251's regrowth then moved tablets back out until about 90 s, a no-riders put p99.9 of 290–362 µs with 48–54 moves in the measured phase against 73 µs with 27 (perf2's three runs; reads flat at 90–96). A light store still packs, 30 s in.
- **No shrink while the load is still ramping** (#734's other half). A light pass on which the fast write average (D217's) is above 1.25× the slow one (D224's) does not advance the shrink count; a hot, busy or stalling pass (any active shard's stall term at or above 0.1: it is no light pass whatever the busy sum reads) resets it, ramping or not (a count carried across a bursty heavy load's bursts would shrink and grow in a slow cycle, ctr274's review); past 600 ramping passes in a row (60 s) the count runs anyway. A periodic batch writer reads as ramping after every batch, so a reset instead of a pause never let a 1–2 Hz writer pack; with the pause it packs on its non-ramping passes. On the AX102 a write lull in the middle of a 16 GiB load (the loader's writers waiting on room, writes 2,479 against 10–48K around it, busy 0.27) completed a count the ramp had let accrue and D217 shrank 16 → 2 mid-load (8.8× against 6.7×); the stalling reset stops that. One-shard-at-a-time shrinks were tried and dropped: they pack a light load over a minute on 16 shards.
- **A stall-driven growth within a minute of a shrink, with the store writing at least twice what it did at the shrink, undoes it in one step.** The load came back and the shrink was wrong; one shard per ten passes took most of a 16 GiB load on the AX102 to climb back from 2 to 16 (the stall-indicated count under-reads there: the shards read hot one at a time). The growth goes straight to the count before the shrink. A stall at the same rate grows one step as usual (ctr274's review: a packed light store with one burst must not go 1 → 4).
- **A stall-driven growth repeats while the hot shard has tablets to shed** (#734's second half). D251's cooled check (another growth only once the hot shard's fraction fell 2%) holds a thread-saturated hot spot to one growth; a stalled shard's fraction stays at 1 however much it sheds, and on the AX102 a load cut to 3 shards regrew one shard per 20 s. A stall-driven hot shard with more than one tablet grows `k` on every 10 passes; one unsplittable tablet is held as before.
- **The floor expires after a quiet window.** Besides the write-rate drop, the floor releases after 300 balancer passes (30 s at the base interval) with no active shard stalling: the stall's cause may go away with the load unchanged (a snapshot released, the pool caught up; ctr274's review). A floor set again within one window of an expiry is the same cause coming back and doubles the next window, up to 16× (8 min), so a cause that comes and goes does not reshuffle the store every half minute; a floor set later, or one released by a write drop, starts from the base window again, so four ordinary episodes do not pin every later transient stall for 8 minutes.
- **Nothing else moves.** Flush and compaction stay on the dedicated threads (D252); the pacer, the room wait and the pickers are unchanged. Application-owned shards count no turn time, so they count no stall time either and stay unpacked, as before. **With `compaction_cores(0)`** (flush and compaction on the shards) the term is active too, a behaviour change from D248: a shard whose writers stall behind its own slices now reads hot by that as well as by its turns. D248's packing check is part of the acceptance for it.
- **The trace names the driver.** `PIGEONHOLE_TRACE` prints every change of the active shard count with the old and new counts, the busy sum, the floor, the write rate and which rule drove it (the busy sum, a saturated thread, stalling writers, a shrink, the stall floor), every balancer pass of shard 0 (the busy sum, the hottest shard, the grow and shrink counters) and every shard's measured terms, so a box run can read `k` over time and why it moved.

**Why** (the 0.3.0 gate's cold-get step and perf2's box traces, `.coord/perf2-d252-uring/fable/`, 2026-10-15):
- With flush and compaction on the pool (D252), a 16 GiB random load wrote 44–51× its data and took 745–845 s at 4, 8 and 16 compaction threads, against 8× and 120 s with them on the shards. The count did not matter, and the io_uring stall (#729) was already fixed in that build.
- The traces showed why: packing kept the table on one or two shards. At ct 4 shard 0 owned 86 of 117 tablets and ran 241 of 334 compaction jobs (perf2's metrics run: 70 and 63 tablets on two shards, 18,283 forced room freezes against 45 size freezes on shard 0, 3,444 write stalls for 62 s); at ct 0 the tablets spread 10–15 per shard. Packing grows the active shards when a shard reads hot by turn busy and sync utilization, and D252 took flush and compaction, the bulk of a loaded shard's CPU, off its thread: a shard carrying the whole load runs cheap turns and reads idle.
- One arena (64 MiB) shared by 86 memtable slots never lets a memtable reach its freeze size; every commit waits for room, and with the last flush long done D124's room freeze forces the largest open memtables, dozens of sub-megabyte ones, into L0 (the ~150 KB L0 files, 55,600 of them against 893). One compaction at a time per shard across 86 always-due L0s then starves L1→L2 (2 jobs against 41), L1 grows, and every L0→L1 rewrites it.
- A shard whose writers wait is hot in the only sense packing needs: more shards would take the load. Counting the stall time restores the spread D252 removed without putting any work back on the shard thread, so D252's tail gains stay.

**Tests:**
- `tablets.rs` `packed_tablets_spread_again_when_the_shard_stalls_for_room`: packed onto shard 0 with every thread reading idle (the busy hook at 0), a load whose batches overflow shard 0's 4 MiB arena with the simulator's I/O deferred makes its commits wait for room while the clock advances; within 300 passes shard 0 stops holding every tablet and the active count passes one. Without the stall term it holds all four after 300 passes with no change.
- `a_stalling_shard_is_hot_even_when_the_busy_sum_is_low`: two active shards whose busy sum is 0.65 (under the 0.75 spread threshold), one with idle turns but stalling writers, grow the active shards to three: the hot test's stall term alone.
- `a_stall_still_open_at_the_balancer_pass_counts`: a pass taken while the commits still wait reads a stall fraction above zero (crediting the wait only when it ends would read zero, a pass late).
- `the_active_shards_a_stall_spread_to_hold_while_the_writes_go_on`: after a stall-driven growth, 120 passes of the same load with every thread 5% busy and no stall keep the active shards at two or more (without the floor they shrink to one after 50).
- `pacing_ends_when_a_tablet_change_takes_its_commits`: two tablets on one shard, one's L0 held at the pacing depth by a held compaction, paced commits to the other, and a move of that other tablet while the first keeps the pacing score; once the compaction ends, ten idle passes later the shard's stall fraction is under 0.1. It checks the property, that a pacing span never outlives its writers across a tablet change, not the empty-pending branch itself: parking and forwarding happen at routing, before a member enters the queue, and the room wait's refusals sit inside admission after the pacer's check, so no sequence from the public API empties the queue while pacing is open on the simulator. The branch's close is kept as a guard.
- `the_active_shards_a_stall_spread_to_hold_while_the_writes_go_on` runs its 120 held passes with 8 KiB rows at the same row rate: no shard stalls on one shard or four, so only the floor keeps `k` up.
- With the stall hook (`Engine::set_shard_stalled`, a chosen stall fraction, as `set_shard_busy` is for turns): `no_shrink_while_the_load_is_ramping` (four shards settled at 30% busy, past the first-shrink window, hold at four through 80 passes of rising rows, then shrink once the rows hold steady); `a_stall_driven_growth_repeats_while_the_shard_has_tablets_to_shed` (a packed shard reading fully stalled takes `k` past two, where the busy sum alone stops); `the_stall_floor_holds_under_the_same_rate_and_releases_when_it_drops` (ctr274's scenario: `k` ≥ 3 holds for 100 passes with the stall gone and every thread 5% busy, and at a quarter of the rate `k` returns to 1); `the_stall_floor_expires_after_a_quiet_window` (the same hold, 250 passes, then `k` returns to 1 past the 300-pass window). `periodic_batch_writers_still_pack` (four shards 10% busy, a batch every fifth pass and nothing between: packed within 600 passes, where a reset of the shrink count on every ramping pass never packed) and `the_floor_backoff_resets_after_a_quiet_episode` (a second stall episode more than a window after the first's expiry releases after the base window, not the doubled one). `stalls_on_three_of_ten_passes_read_as_a_stalled_shard` and `a_stall_every_ten_passes_does_not_read_as_a_stalled_shard` (a packed shard waiting for room on every third pass grows the active shards; on every tenth it stays packed through 150 passes), `a_busy_pass_during_a_ramp_still_resets_the_shrink_count` (40 light passes, three busy and ramping passes, 40 more light passes leave the active shards at four) `a_stalled_pass_resets_the_shrink_count` (40 light passes, one on which shard 0 reads stalled, 40 more leave them at four) `a_stall_soon_after_a_shrink_undoes_it_in_one_step` and `a_stall_at_the_same_rate_does_not_undo_the_shrink` (packed from four onto one at 80 rows a pass, a stalling shard at 240 rows a pass takes the active shards straight back to four; at 80 it grows to two), `stalled_shards_weigh_in_the_busy_sum` (two stalled shards size the first growth to four) and `a_load_that_only_ramps_still_packs_after_the_cap` (rows rising 0.4% a pass hold four shards for 600 passes, then the shrink comes). Each of the first three, the floor test, the open-span test, the ramp test, the backoff test, the window test and the hot-reset test fails with its rule removed.
- With the busy hook alone: `a_dip_between_phases_does_not_pack_an_unpacked_store` (four shards 60% busy for 100 passes, 10% for 100, 60% for 100: the active shards stay at four through the dip, where D217's 50 passes packed them to one) and `a_light_store_packs_after_the_first_window` (four shards 10% busy hold at four through 250 passes and have packed by 400). `the_first_shrink_window_restarts_on_a_reopen` (a light store packed onto one shard, closed and reopened on the same four shards comes back spread, holds at four through 250 writing passes and packs by 400: nothing persisted skips the window, ctr274's ask) and `an_idle_store_stays_unpacked_and_does_not_count_toward_the_window` (writes under `balance_min_writes` for 1,000 passes change no tablet and leave the active shards at four, and the window then runs from the first counted pass; a tablet written on no pass is D224's cold consolidation, not packing). The dip test fails with the first-shrink window at 50 passes; the idle test fails when every pass counts.
- The existing packing tests (light writes pack, a busy shard spreads, the slot floor, D248's sync utilization) are unchanged and still pass: a load that never stalls measures as before.

**Acceptance (AX102, perf2):**
- coldget on 03e643ce (the first cut): 8 GiB 6.8× / 76 s (ct 0: 7.2× / 96 s; before D255 with the pool: 8.3–10.4× / 90 s at 8 GiB, 44.7× / 823 s at 16 GiB), tablets 5–25 per shard over all 16; 16 GiB 7.7× / 139 s (ct 0: 8.0× / 120 s), L1→L2 68 jobs. The riders row at default 366–389K, p99 118–121 µs, 13 shards active (bcea95e2 371K); no riders buffered 1,095K at p99 45 µs (D252's gain kept).
- **The buffered riders row spreads to only 4–5 shards on every build, and on main** (812K there; 762–814K here). perf2's trace of it settled why: `k` reaches 9 by D251's thread-saturated rule (shard 0's turn busy 0.96–0.99; its own stall term is near 0, the thousands of write stalls are on other shards), the mover moves one tablet to each newly active shard, then stops for the rest of the run with shard 0 holding 9 of the 16 tablets and 41% of the commits while three active shards stay empty. That is the write-skew mover, not packing, and not this decision: #735.
- The no-riders buffered row on the first cut with the ramp guard (af8b6da7): 1,195K, p99 41 µs, p99.9 84 µs, 25 write stalls, spread over all 16 shards (before D255: 1,120K, p99.9 106 µs, 1,464 stalls). The cut before it (a8bdd2ce) had a put-only tail (p99.9 532 µs, 7,486 stalls) from the early shrink holding the load on 5 shards; the ramp guard removed it.
- perf2's `k`-over-time traces of that row: on 03e643ce `k` went 9 → 7 at 86 s and back to 10 by 103 s with the busy sum flat at 3.2–3.4 (the thrash the stall floor removes). The same traces on main c843f200 before D255 show 16 → 2 at 5.5 s, about 35 s at `k` = 2, then +1 about every 10 s to 15 with no plateau: the slow start and the creep predate D255 and were filed as #734, which this decision absorbs: the first-shrink window keeps the initial `k` through the load and its warm-up, the ramp guard and the stalling reset keep a lull from packing a running load, and the repeated stall-driven growth takes a packed shard back out in steps larger than one.
- **The final cut (build 6, 58a840ae), the whole set on the AX102 (perf2):**
  - coldget 8 GiB ×2 and 16 GiB ×2: 6.4–7.2×, no change of `k` in any load (before D255: 44.7×).
  - the riders row: buffered 879–902K on all 16 shards, p99 64–68 µs (main 812K on 4–5; build 2's 798–814K); default 358–387K on 16 shards, p99 122–136 µs (main 398K on 13: with the early shrink gone the GroupSync row sits at D248's own packing level, sixteen shards each busy enough on sync utilization for D217 never to pack them; the 4–8% is the cost of smaller sync groups, accepted); no `Busy`, no hang.
  - `k` over five minutes of the measured phase, backup included: no shrink after the early one (which the ramp guard moves from 5.5 s to about 21 s, inside the load), buffered plateaus at 9–10, default at 16 from 92 s; the growths are D251's thread-saturated rule.
  - D248's check at `compaction_cores(0)`: default with packing 381K against 384K without (99%), p99 114–115 against 114–118 µs; the buffered row no longer packs at ct 0 and runs on 16 shards at 898–923K, p99 50–63 µs (main: 812K on 4–5 shards, p99 401–409 µs), the behaviour change this decision records for flush and compaction on the shards.
  - no riders, buffered: 1,071K, p99 47 µs, p99.9 117 µs on the first run with 15 splits and 48 moves (builds 3–5: 84–88 µs with 0–3 splits). perf2's three reruns with the move trace: the put p99.9 tracks the moves inside the measured phase (27 moves 73 µs, 48 moves 290 µs, 54 moves 362 µs; the reads flat at 90–96 µs). The ramp guard had moved the early 16 → 2 to about 21 s, into the dip between the load and the warm-up (busy about 0.8), D242's warm-up settles after 3.8 s, so the pack and D251's regrowth to about 90 s landed in the measured phase. The first-shrink window (above) is the fix; its acceptance is the set below.
- **Build 7 (cce856d3, the first-shrink window) on the AX102 (perf2):** no riders buffered ×3: put p99.9 58 / 99 / 181 µs with 24 / 36 / 60 moves in the measured phase, reads flat at 95–97 µs, 1,108–1,165K. The window moved the drop, it did not remove it: traced, `k` went 16 → 2 at 30.5 s (D217, busy sum 0.81), then 2 → 4 at 38.6 s and 4 → 6 at 44 s by D251's thread-saturated rule, back toward 16, with 18 moves in the 30–40 s bucket; the measured phase starts at 20–28 s. The busy sum is not additive on this load: the shard threads spend 0.73 µs an op at `k` = 16 and 1.8–2.3 µs an op at `k` = 2–4, so D217's projection from the spread cost finds the load fits on two shards every time its count runs, and the regrowth moves every tablet back. Main's own figure on that row is p99.9 106 µs (its drop at 5.5 s and the creep back to 15 over two minutes). The rest of this decision's set held on build 7: coldget 8 GiB 7.0× / 72–76 s and 16 GiB 7.2× / 131–134 s with no change of `k`; the riders row buffered 823K (p99 67 µs; build 6's pair 879–902K, main 812K, inside the spread every build showed) and default 359K (p99 130 µs, build 6's level), 16 shards active on both. The remaining no-riders cost is #734's, reopened with these runs: the candidates are counting the pool's flush and compaction time toward the tablet's shard (D252 took that time out of the busy measure) and a floor from a thread-saturated growth, as the stall-driven growth already sets.

**Follow-ups (not in this change):** a compaction job keeps the view it started with, whose readers pin the chunks of every memtable flushed meanwhile, so room returns late under back-to-back compactions; D124's room freeze takes half the arena across however many small slots (a floor per slot would keep L0 files larger); L1→L2 can starve behind always-due L0s on a shard with many slots.

*History: amended by [D256](#d256), 2026-10-15: the busy measure's turn term becomes turn + spin + pool (the stall term and the hot test unchanged), and a thread-saturated growth sets the stall floor.*

<a id="d256"></a>
## D256 — Packing's busy measure counts a shard thread's spin and the pool's work for the shard, and a thread-saturated growth sets the floor (owner decision, 2026-10-15; runtime, engine, pigeonhole, #406, #734; amends D217, D251, D255)

**Decided:**
- **Two terms add to the turn term.** A shard's balancer measures its busy fraction as the largest of three things (D217, D248, D255): the sum of its thread's turn time, its thread's spin time (D198: after a handled message the thread polls for the spin window before parking, and a shard whose next message arrives within that window never parks: its clients keep it awake) and the pool's slice time for the shard's own tasks (its flushes and compactions on D252's dedicated threads, which ran inside its turns before D252); the sync utilization; and the stall term. The pool term is time on the shard's behalf, not CPU: a slice's wall time, including the I/O waits inside it and whatever other completions the pool thread reaps while it waits. The sum may pass 1 (two pool threads serving one shard at once), and a long background catch-up (an L1→L2 backlog after a bulk load) keeps `k` up under a light foreground load until it ends. D251's hot test stays on the turn and the stall terms: a shard kept awake or served by the pool is not a saturated one; the sum sizes `k`, the hot test grows it.
- **The runtime counts both.** The shard thread adds each poll's time to `Signal::spin` beside its turns in `Signal::busy` (on the VFS clock, as the turns: a simulated clock does not move while polling and counts nothing; a skipped poll, the window zero or the backoff, adds nothing). A task handed to the pool carries the signal of the shard that spawned it (`PoolTask`), and the pool thread adds each slice's time to that shard's `Signal::pool_busy`. `Submitter::spin_nanos` and `Submitter::pool_busy_nanos` read them; `ShardStats::spin_time_nanos` / `pool_busy_nanos` and `EngineMetrics::shard_spin_time_nanos` / `shard_pool_busy_nanos` (the public `Metrics` too; "time", since `shard_spin_nanos` on the options is the window) show them beside `busy_nanos`, whose meaning (turns only) is unchanged.
- **A thread-saturated growth sets the floor** D255's stall-driven growth sets: the active count it reached holds while the store writes at least half of what it did then, and expires after a quiet window, a pass with a saturated or a stalling shard being not quiet. The shards it spread to run cheap turns, so the busy sum alone would shrink the store back after 50 passes and the saturation would return (on the AX102 a 16 → 2 shrink was followed by 2 → 4 → 6 → … → 16 by this rule, a tablet moved twice each).
- **Application-owned stores count nothing**, as before: no turns, no spin, no pool, so they stay unpacked.
- **What "light" now means.** Below about 10K messages a second per shard a load still packs as D217 did; above it the spin term sizes `k` by message rate, about one active shard per 10–12K shard messages a second (ctr274's model of the term alone under Poisson arrivals: 20K → 2, 50K → 4, 100K → 8, 300K and up → 16; an upper bound, since the idle spin backs off after misses). A single fast client at 30K buffered commits a second keeps about three shards where D217 packed it onto one: the owner's trade. Measured on the AX102: 2 clients (231K) settle on about 6 shards, 4 clients (400K) on about 8, 16 clients (1.1M) on 16.
- **Nothing else moves.** D255's stall term, its stall floor, its first-shrink window, its ramp guard and its shrink undo stay as they are; whether the window and the undo are still needed with the spin term is judged on the AX102 afterwards.

**Why** (perf2's per-thread CPU over the measured phase of the no-riders buffered row on #730's build 7, `.coord/perf2-d252-uring/d255g/`, 2026-10-15): at 1.1M ops/s over 16 shards the balancer read the busy sum as 0.81, D217 concluded the load fits on two shards, shrank 16 → 2, and D251's rule grew it back within ten seconds, every tablet moved twice and the put p99.9 tracking the moves (58 / 99 / 181 µs with 24 / 36 / 60 moves in the measured phase, #734). The same phase showed the 16 shard threads at 4.68 cores of CPU and the 4 pool threads at 0.31. The gap between 0.81 and 4.68 is the spin: a message every 14 µs keeps a shard polling (50 µs window) the whole time, which turn busy does not count, so a shard burning a core read as 5% busy. The pool's 0.31 cores is small on this row but is the same gap in kind, and larger on compaction-heavy loads. With the spin counted the no-riders row reads about sixteen shards' worth at k = 16: no first shrink and no regrowth. The floor from a saturated growth stops the shrink-and-regrow cycle wherever a shrink does happen. Packing's purpose (D217) is to let shards that would otherwise sleep go idle; a shard its clients keep awake has no idle time to give, and the busy sum then reflects the threads a load really occupies, not the work inside their turns. Considered and not taken: a trial shrink with an immediate restore (the full moves once per open, at the load's start); a shrink threshold from the measured packed cost (only measured once packed); a per-shard CPU reading from the OS (not portable, and the spin is exactly the part a measure of useful work would exclude).

**Tests:**
- `runtime/tests/modes.rs` `pool_slices_count_to_the_spawning_shard` (a pool task's three 1 ms slices on the simulated clock show as shard 0's pool busy, none of shard 1's, and not as its turns) and `a_shard_counts_its_spin_before_parking` (after a handled message on the real clock, `spin_nanos` rises and `busy_nanos` keeps its meaning).
- `engine/tests/tablets.rs` `pool_work_counts_toward_the_busy_sum` (packed onto one shard with every thread 10% busy, a pool term of 1.5 on it grows the active shards to three or more within 100 passes), `a_shard_kept_awake_by_its_clients_counts_as_busy` (four shards settled, each 5% in turns and 90% in its spin, hold at four through 150 passes and pack once the spin stops), `a_saturated_growth_holds_while_the_writes_go_on` (a thread-saturated growth's count holds through 100 passes at 5% busy and releases at a quarter of the write rate). Each fails with its term or the floor removed (mutations M20–M22 beside D255's).
- Real counters, not the overrides (ctr274's review): `engine/tests/spin_pool_terms.rs` (engine-owned on real files with one compaction thread: after commits and a flush the balancer's published busy exceeds its turn-only fraction, read through the `shard_busy_terms` hook, and both counters are above zero; with the counter reads zeroed the two fractions are equal, mutation M23) and `pigeonhole/tests/api.rs` `spin_and_pool_time_are_counted_on_an_engine_owned_store` (the public `Metrics` and `ShardStats` carry both).

**Acceptance (AX102, perf2, on ea0b5c8e, 2026-10-15; the columns busy / pool / spin are each counter's rise over the measured phase divided by its length, in shards' worth):**
- **No riders, buffered, 16 clients ×3:** 1,092–1,127K, p99 44–46 µs, p99.9 98–101 µs; moves 3 / 3 / 0 and splits 2 / 2 / 0 in the measured phase (build 7: 24–60 moves), 0 write stalls, all 16 shards active throughout: the drop and the regrowth are gone. Terms: turn 1.73–1.76, pool 0.21–0.23, spin 4.71–4.73, sum 6.68–6.69. The spin term reads 4.7 shards' worth, not 16: the idle spin backs off after misses, so the shards park about 70% of the time; 6.7 against the 0.81 before is what holds `k`. The put p99.9 is 108–112 µs (main 106; #730's builds 3–5 84–88 µs with the load packed onto 4–5 shards): with no moves to blame, a commit landing on a parked shard pays its wakeup at the tail, the trade this decision records showing where it does (#738, Phase 4). Reads flat at 90–93 µs.
- **Coldget:** 8 GiB 6.6× / 105 s, 16 GiB 7.0× / 150 s, no change of `k` in either (build 7: 7.0× / 76 s, 7.2× / 134 s).
- **The riders row, two runs each:** buffered 789–796K, p99 85–90 µs, put p99.9 378–440 µs (builds 6 and 7: 823–902K at p99 64–68 µs; main 812K); default 346–357K, p99 136–143 µs (builds 6 and 7: 358–387K at 122–136 µs; level). Both on 16 shards with 0 stalls and 0–6 moves. The buffered row's p99 shift of about 20 µs is real (two runs agree) and is the thin-shard wake cost of the no-riders tail showing at p99: all 16 shards active at a busy sum of 5.7, parked most of the time, so a commit pays the wakeup (#738). On the GroupSync row the spin term is about 1 shard's worth (the shards wait on syncs, not polling), the sum 3.0 mostly turn busy, and the change is small.
- **Mid-rate rows (plain ycsb-a buffered, fewer clients):** 4 clients 400K on about 8 shards (busy sum 3.50: turn 0.69, pool 0.11, spin 2.69), p99.9 42 µs; 2 clients 231K on about 6 shards (sum 2.27), p99.9 31 µs. Packing settles where the model above puts it, with low tails.
- **D248's packing check at the 16-client rate (buffered):** ycsb-a 1,122K (main 812K on 4–5 shards), time-series-ttl 1,104K, ycsb-f 544K, all on 16 shards with busy sums 5.4–6.7 and 0 stalls. "Packing still packs" now holds for light loads (the mid-rate rows), not for these: they read heavy, as they are, and throughput is up or level.
- **Traced runs cannot judge `k`:** the trace slows a run about 3×, the busy sum then reads about 1, and the early 16 → 2 fires after the 300-pass window as before; the untraced runs' move counts (0–3) are the signal.
- **`placement --offered-rate 20000` and `50000`, untraced:** 16 and 15 shards kept taking writes with no tablet change over 60 s, on every build: the placement runner (D231's spread check) turns packing off on purpose (`run_placement` sets `no_packing`, since packing would keep a light load on few shards), so these rows measure nothing about `k`. The light-load scale is measured by the mid-rate ycsb-a rows above (2 clients on about 6 shards, 4 on about 8); a rate-limited ycsb-a or a placement run with packing kept on, plus a `k` gauge in the metrics, is the follow-up measurement for the model's 20K and 50K points.

*History: amended by [D257](#d257), 2026-10-19: the spin term is dropped from the busy measure (the counters and metrics stay). The 0.3.0 gate on f61510dd found it spreading one client over 5–16 shards (ycsb-a −28% at one client): a thin shard spins after every message, so the term grew with the spread it justified. The commits in flight replace it; the pool term and the saturation floor stay. "What light now means" and "the owner's trade" above are superseded.*

<a id="d257"></a>
## D257 — Packing sizes the active shards by the commits in flight: one shard per 0.75 concurrent client commits, and the spin term goes (owner decision, 2026-10-19; engine, pigeonhole, bench, #406; amends D217, D251, D255, D256)

**Decided:**
- **The signal is the store's concurrency, `C`:** the mean number of client commits in flight over a balancer interval. Each shard adds the nanoseconds from a commit's submit (`submitted_at`, the client's clock read the commit already carries) to its reply, committed or failed, to a new per-shard counter `ShardMetrics::commit_inflight_nanos` at the two places that already compute that span for the latency histogram: the single commits a group finishes and the cross-shard commits the shard coordinated. A cross-shard commit counts once (its coordinator, after any retries; forwards and retries keep the client's `submitted_at`); PREPAREs, participants' COMMIT records and reads count nothing. Failures that reply before the shard settles the commit count nothing either: a refusal, a parked commit's `Busy` timeout, a close, an `OutcomeUnknown` from a failed group write. Those are rare and the error is toward fewer shards; a commit the shard fails at validation or application (a conflict, a predicate error, a record too large) counts like a committed one. Per pass the balancer takes the counter's rise over the interval, which by Little's law is the mean number of commits in flight at the shard, smooths it as it does busy (weight 0.2) and publishes it (`LoadSlot::inflight`); shard 0 sums every shard's into `C`. Nothing on the client's path, no new clock read, one `Relaxed` add per completed commit on the shard thread.
- **The shards it calls for:** `k_c = round(C / PACK_CONCURRENCY)`, `PACK_CONCURRENCY` = 0.75, at least 1. `want = max(ceil(B / 0.5), k_c)`, then the slot floor and the stall or saturation floor as before. Growth (10 passes, as D217) when `B / k > 0.75` **or `k < k_c`** or a shard is hot (D251, D255, unchanged). Shrink (50 passes) only when `B / (k − 1) < 0.5` **and `k > k_c`**, no hot or stalling shard, not ramping; to `want.min(k − 1)` as before, so one client goes 16 → 1 in one step after D255's first-shrink window.
- **A shrink from all `n` shards does not wait for D255's window when the concurrency has settled** (coordinator, 2026-10-19): shard 0 keeps a slow average (`SLOW_ALPHA`) of `C` beside the fast one, and once both have called for the same `k_c` for 50 passes in a row (and `C` reads above 0: the simulated clock and application-owned stores settle nothing; an instant of agreement, as when the fast average crosses the slow one on its way back up after a dip, is not enough) a pack from `k` = `n` decides on `PACK_GROW_PASSES` (10) light passes and counts through a write ramp. That is the first pack from the unpacked start, and equally a pack after a burst grew the store back to every shard (ctr274's review: there is no once-only flag; a burst's one-step growth to `n` and the lull after it shrink about 20 s later, the 50 agreeing passes and the 10; a lull after sixteen clients about 29 s later, as the slow average remembers them). Shrinks from fewer than `n` shards keep the 50 passes. The gate's one-client rows load for 4–13 s and warm up for 2–5 s (D242's 2 s quiet) before a 10 s measured phase, so the 30 s window, a 50-pass count restarting after the settle gap and a cold start's ramp guard (the slow write average needs about 16 s from 0; time-series-ttl's warm-up read as ramping) each put the pack inside the measured phase, with #734's moves: locally (small scale, 60 s) the pack landed 9 moves into it. A ramp fools the busy sum, which reads a load's start as light; it does not fool the commits in flight, which read one client as one client at any rate. Shrinks from fewer than `n` shards keep the 50 passes and the ramp guard. A single-threaded bulk load now packs about 6 s in (the gate's loads are single-threaded), where 234c5c2f packed it at about 5 s by the busy sum; a heavier phase after it regrows at once and D214 moves the tablets out one per dwell, inside the warm-up where it fits (13 tablets on ycsb-a), and the stall floor still spreads a loader that waits for room. A dip after a heavy phase still waits: the slow average remembers the phase for about the window (from 16 clients it reads above one shard's worth for some 240 passes), and busy-driven shrinks keep the window.
- **The busy measure drops D256's spin term:** `B = Σ max(turn + pool, sync utilization, stall term)`. The runtime keeps counting the spin (`Signal::spin`, `ShardStats::spin_time_nanos`, `EngineMetrics::shard_spin_time_nanos`), the balancer traces it beside its terms, and nothing sizes `k` by it. D256's pool term, its saturation floor and D255's stall term, floor, undo and ramp guard stay as they are.
- **Why `round` and 0.75.** A lone pure writer has `C` → 1 and stays on one shard, where a ceiling would give it two; two clients at `C` ≈ 1.5 get 2; sixteen ycsb-a clients, whose gets take about a quarter of their loop (`C` ≈ 12), get 16. The constant is the knob if the AX102 reads `C` differently from this estimate.
- **The demand side and the capacity side.** `C` is what the clients can use: `k` above `k_c` only adds a wake per write on thin shards. The stall and saturation terms are what the store needs: their growths and floors are not held to `k_c`, so a single-threaded loader waiting for arena room still spreads (coldget keeps its write amplification), and a saturated shard thread still sheds.
- **Exposed:** `ShardStats::commit_inflight_nanos`, `EngineMetrics::shard_commit_inflight_nanos` and the public `Metrics::shard_commit_inflight_nanos`; the bench's per-shard `ShardShare::commit_inflight_nanos` (its rise over the measured phase divided by the phase's length is the shard's mean commits in flight, summed the store's `C`); the balancer's pass trace prints `C` and `k_c`, and a change of `k` names the commits in flight when they drove it.
- **Application-owned stores** count the span too (the shard's clock reads), but their busy sum is 0 and packing never acts on them, as before. On the simulated clock the span is 0 unless a test moves the clock between a submit and its reply, so the deterministic harnesses keep D217's behaviour: `k_c` = 1 only adds nothing and gates no shrink that would have happened.

**Why** (perf2's read of the 0.3.0 gate on f61510dd, 2026-10-18, `.coord/inbox/done/20261018-235234-perf2.md` §1): D255 and D256 spread a **single** client over 5–16 shards (1–2 on 234c5c2f), and the count varied run to run. ycsb-a at one client fell 180K → 130K ops/s (−28%), put p50 4.6 → 9.4 µs, shard parks 10K → 636K; time-series-ttl −19%, ycsb-f −15%; ycsb-b and ycsb-d p99 followed the shard count (14 µs at 5–7 shards, 30 µs at 15–16). One writer with one commit in flight gains nothing from a second shard and pays a wake per write on a thin one. The spin term read it as heavy load: a thin shard polls for its window after every message, so at one client over 13 shards each read about 0.5 spin and the sum 6.5 shards' worth, and the spread justified itself. Concurrency does not have that property: for `t` closed-loop clients `C ≤ t` whatever `k` is. The owner asked for packing driven by it, with the 16-client rows keeping D256's gains (no riders 1.1M on 16 shards, coldget ~7×, the riders rows).

**Expected on ycsb-a buffered** (the owner's four points; `C` ≈ `t` × the put share of a client's loop):

| clients | `C` | `k_c` | busy `want` | `k` | D256 |
|--:|--:|--:|--:|--:|--:|
| 1 | 0.5–0.7 | 1 | 1 | 1 (time-series-ttl's lone writer `C` ≈ 0.95 → 1; its slot floor gave 2 on 234c5c2f) | 5–16 |
| 2 | 1.2–1.5 | 2 | 1 | 2 | ~6 |
| 4 | 2.5–3 | 3–4 | 2 | 3–4 | ~8 |
| 16 | 11–13 | 15–16 | 4 | 15–16 | 16 |

A durable single writer: sync 1.0 → busy `want` 2, `k_c` 1 → 2 (D251's guard holds). GroupSync 16-client riders: `C` ≈ 16 → 16, unchanged.

**Considered and not taken:** a sampled in-flight count (an instant reads 0 or 1 at one client; the integral averages every commit of the pass); the open time at the shard without `submitted_at` (a buffered commit's span is mostly the thread hop, which the shard cannot see: it read 0.2 of a client); a client-side atomic per commit (a cache line bounced among the clients of a shard on the measured commit path); keeping the spin term under a `k_c` cap (a cap still needs its own shrink rule and leaves the sizing to a term that grows with the spread it drives).

**Tests** (simulated, the `set_shard_inflight` override beside the busy, stall and pool ones; each fails with its rule removed):
- `concurrent_commits_hold_the_active_shards`: four shards settled, each thread 5% busy with 0.75 commits in flight each (3 over the store), hold at four through 150 passes and pack once the commits in flight stop (D256's kept-awake test, with the spin replaced by the concurrency).
- `a_lone_writers_commits_in_flight_pack_to_one_shard`: one writer's worth (1.0 over four shards) packs to one, where a ceiling keeps two.
- `concurrent_commits_grow_a_packed_store`: packed onto one shard at 10% busy, 3 commits in flight take `k` to 4 within 100 passes.
- `a_stall_driven_growth_is_not_held_to_the_commits_in_flight`: packed, 0.9 in flight, writers stalling: `k` still grows.
- `a_concurrency_driven_first_shrink_does_not_wait_for_the_window`: one client's worth over four spread shards packs before 290 passes, where the busy sum alone waits for 300; `a_concurrency_dip_after_a_heavy_phase_does_not_pack_an_unpacked_store`: three commits in flight for 200 passes, then 0.4 for 50, hold at four.
- Real counters (`spin_pool_terms.rs`): one blocking client reads at 0 < `C` ≤ 1.05 and the counter above zero; the same client's cross-shard commits over two shards read 0 < `C` ≤ 1.05 over the store with the counters above zero (`one_clients_cross_shard_commits_read_as_one_commit_in_flight`, which fails without the coordinator's count: the participants count nothing); a hundred transactions that fail validation (`Conflict`) raise the counter (`failed_commits_count_their_time_in_flight`, which fails when only committed singles count). The public `api.rs` metrics test checks `shard_commit_inflight_nanos` and the per-shard stat agree.
- D217's, D248's, D251's, D255's and D256's pool and floor tests pass unchanged (`C` = 0 on the simulated clock: `k_c` = 1).

**Acceptance (AX102, perf2's A/B of ed5ee0ba against main 305e594c, 2026-10-19, `.coord/inbox/done/20261019-050430-perf2.md`; 3 runs each, sides alternated, medians; `k` = shards with any commit / with ≥ 1% of the commits in the measured phase; `C` from the per-shard `commit_inflight_nanos`):**

| row | main ops/s | D257 ops/s | Δ | main `k` | D257 `k` | `C` | write p50 µs | write p99.9 µs |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a t=1 | 132K | 165K | +25% | 14 | 4/3 | 0.36 | 9.41 → 4.67 | 19.2 → 14.9 |
| ycsb-a t=1, 60 s | 134K | 158K | +18% | 11/9 | 4/3 | 0.33 | 8.13 → 4.70 | 16.9 → 14.1 |
| ycsb-a t=2 | 243K | 293K | +21% | 16 | 3 | 0.65 | 9.02 → 4.89 | 20.6 → 15.6 |
| ycsb-a t=4 | 456K | 485K | +6% | 16 | 5 | 1.15 | 8.51 → 5.09 | 27.4 → 16.4 |
| ycsb-a t=16 | 1166K | 1275K | +9% | 16 | 9 | 4.82 | 8.00 → 7.20 | 105 → 43 |
| time-series-ttl t=1 | 170K | 223K | +31% | 16 | 2 | 0.37 | 8.96 → 4.61 | 17.7 → 13.6 |
| ycsb-f t=1 | 70K | 75K | +7% | 11/10 | 4 | 0.38 | 21.1 → 19.5 | 48 → 40 |
| no riders t=16 | 1098K | 1122K | +2% | 16 | 16/13 | 4.57 | 8.45 → 7.55 | 112 → 62.5 |
| riders buffered | 770K | 884K | +15% | 16 | 10/9 | 3.54 | 11.58 → 7.84 | 436 → 80 |
| riders default | 389K | 386K | −1% | 16/14 | 16 | 11.56 | 57.85 → 57.85 | 397 → 436 |

Shard parks fell 650K → 98K at one client and 9.08M → 2.12M on no riders. coldget's 8 GiB load: 93 s at write amplification 7.09 against 92 s at 6.85 (one run each; not a signal). **Accepted** (the owner, 2026-10-19: merge on this A/B), with three readings recorded:
- **`C` reads about 0.35 per ycsb-a client, not the 0.5–0.7 estimated above** (a buffered put's span from submit to reply is about a third of a client's loop), so `k_c` is 1 up to two clients, 2 at four and 6 at sixteen, and binds only on riders default (`C` 11.6 → 16). Above `k_c` the busy sum sizes `k`: one ycsb-a client settles at 3–4 shards (the pool term and the thin shards' turn time, as the Mac pre-check's 2-not-1), two at 3, sixteen at 9, time-series-ttl at 2. The owner's one-client point (`k` = 1) is not met on ycsb-a, and costs nothing visible: put p50 is 234c5c2f's 4.6–4.7 µs and the row beats main by 25%; 8–9 moves still land in its measured phase (the shrink's last steps and D214's skew moves among 3–4 shards). `PACK_CONCURRENCY` stays 0.75: lowering it to make `k_c` ≈ `t` would force 16 shards at sixteen clients, where 9 measured faster with a 2.4× lower write tail.
- **The regrow after a packed load is real, once in three no-riders runs** (fix r3): the single-threaded load left the store on 4 shards, D242's warm-up ended at its 2.0 s minimum before the sixteen clients' growth had moved a tablet, and the 4 → 16 regrow ran inside the measured phase: 70 moves, 13 splits, write p99.9 424 µs against main's 112, row p99.9 142 against 101, throughput held at 1100K. The other two runs started on 8 and 12 shards with 6 and 41 moves and beat main. riders buffered r1 shows the same pattern smaller (60 moves, write p99.9 444, main's 426–455). The follow-up is below.
- **riders default's row p99.9 is +8%** (256–261 µs against 231–242) at identical p50 and `k` = 16; the only regression on the fix side, and within the shape's run-to-run range on main (its measured-phase moves were 8–10 against the fix's 1–2).

**Follow-up (the regrow):** the gate would hit fix r3's pattern at random, and a real application whose load jumps from a single loader to many clients regrows the same way. The plan is in the coordinator's inbox (fable252, 2026-10-19); the smallest change that removes the randomness from the gate and helps that application is preferred over a cap on how far a load packs.

*History: amended by [D262](#d262), 2026-10-19: a cross-shard commit counts its time in flight at every shard its round occupies (each participant adds the span from the client's submit to the decision it applies), so `C` is the mean number of commit rounds in flight; the gate's contended 16-client cross-shard row read `C` 8 at `k` = 16 and shrank to 13–15 inside the measured phase.*

<a id="d258"></a>
## D258 — The gate window trims the data file system before every step (coordinator decision, 2026-10-19; bench, #406; amends the Phase 3 gate procedure, D242, D243)

**Decided:**
- **Every step starts on a trimmed drive.** At full scale `run-window.sh` runs `fstrim -v` on DATA_DIR's file system (its mount point, from `findmnt -T`) before each step it runs; steps already done are skipped untrimmed. The trim is outside the step's time and its hang limit.
- **Logged, never fatal.** Each trim's output and duration go to the window's log (`-- trim before STEP: …`), and a line per step to `RESULTS_DIR/fstrim.tsv` (step, UTC time, bytes trimmed, seconds). A missing `fstrim`, a mount point not found, or an fstrim that fails is logged and the window goes on. The dry run (`--scale small`) doesn't trim.
- **Recorded with the machine.** `setup` writes the bytes the trim before it reported (`trimmed_bytes`) and the NVMe namespace's size and mapped part (`nvme_namespace`: `size_gb`, `used_gb`, `used_pct`, from `nvme id-ns`'s `nsze` and `nuse`) into `machine.json`. The drive may report `nuse` lazily: on the AX102, `nvme list` still showed 1.81 TB mapped right after a 1.5 TiB trim.
- **The quiet-machine check still holds.** fstrim waits in D state, which counts in the load average, so after the trim before `setup` the window waits up to 3 minutes for the 1-minute load to fall back under 1.0 before setup judges it.

**Why:** the data file system is mounted without `discard`. The gate's steps write and delete terabytes (2026-10-14c's cold-get alone wrote 2.9 TB), and an XFS delete without `discard` leaves those blocks mapped on the NVMe until something trims them. Before D258 nothing in the window did.
- **Gate 2026-10-18 (f61510dd):** the D213 group-commit row failed its p99 (222 µs, 5.69M cells/s, the engine's commit p50 in the 131 µs histogram bucket), and `all`'s inline group-commit rows were slow too (1.34–1.40M, p99 222–256 µs). 2026-10-13 through 2026-10-14c had measured 9.5–9.75M with the engine's p50 at 66 µs. The window started after about 40 minutes of other full-scale runs and their deletes.
- **perf2's bisect (2026-10-19)** found no slow commit: 0 slow rows in 96 over 175c0ffc..f61510dd, and about 140 inline rows that day on a trimmed drive were all fast, f61510dd's included.
- **perf2's arm T (2026-10-19, f61510dd)** re-mapped the namespace: 1335 GiB of incompressible data written with `oflag=direct` and deleted took it from 716.5 GB to 1.81 TB mapped. The fresh-process rows right after stayed fast (2.38–2.40M, engine p50 66 µs). `all-uring-1` then put its pinned inline row in gate 18's band: 1.44M, engine p50 131 µs, p99 219 µs, the day's only slow row. RocksDB was within ±2% on every workload, so the drive wasn't slower in general. `fstrim -v /data` then trimmed 1.5 TiB.
- That's one reproduction, so it suggests the drive's mapping state rather than proving it. The trim removes the variable from the gate either way: every window, and every step in it, starts from the same drive state, whatever the earlier steps or other users of the box deleted. The single-file 4 KiB fdatasync probe didn't move during any of this (59–63 µs, the slow row included), so it can't stand in for the trim or detect the state.

**Considered and not taken:** mounting with `discard` (a discard on every delete, inside the measured phases of the steps that delete stores); one trim at setup only (the window's own steps delete terabytes, and the slow rows came about an hour in); a probe that predicts the slow state (none found: the flush probe never moved).

<a id="d259"></a>
## D259 — The reproducibility check gives each latency an absolute floor: it fails only past both its relative tolerance and the floor (owner decision, 2026-10-19; bench, #406, #747; amends D243)

**Decided:**
- **A latency fails only past both.** `phdb-bench compare` fails a judged p50 only when it moved by more than ±20% *and* by more than 3 µs. It fails a judged p99 only when it moved by more than ±40% *and* by more than 6 µs. A move within either one passes.
- **The floors scale with `--tolerance T`** the way the p99's relative tolerance does: `T × 15 µs` at p50 and `T × 30 µs` at p99 (3 and 6 µs at the default 0.20). `Tolerance` carries them as `p50_floor_ns` and `p99_floor_ns`.
- **Unchanged:**
  - throughput has no floor and stays at ±20%;
  - p99.9 and max are reported, not judged;
  - D243's 10 s rule still decides which pairs are judged at all; a pair too short to judge gets neither tolerance nor floor.
- **In the report:** the Allowed column reads `±20% or ±3 µs` for a judged latency.

**Why:** gate 2026-10-19's reproducibility pair failed on one metric only: ycsb-f's Pigeonhole p50 at one thread, 10.2 against 12.7 µs. That is +25.2% against ±20%, but only 2.5 µs. The same row's p99 moved +9.2% and p99.9 +6.3%, and RocksDB's ycsb-f p50 moved −1.8%. At latencies of a few microseconds, a relative tolerance alone judges drifts too small to mean a different store. D243 made the judged runs long enough. D259 makes the judged move large enough.
- Re-checked with the floor, gate 19's `all-uring-1` against `all-uring-2` passes. Gate 2026-10-18's pair still fails on the same 9 metrics. Those moves were tens of percent at several µs, or throughput, so the floor doesn't hide real drift.

**Considered and not taken:** loosening the relative p50 tolerance (it would also loosen the long-latency rows, where ±20% is already µs-scale); a separate `--floor-us` flag (the floor scaling with `--tolerance` keeps one knob, as the p99's 2T does).

<a id="d260"></a>
## D260 — A conditional commit waits for the next group only when its group wrote its predicate cell, and only the commits on its rows wait with it (coordinator decision, 2026-10-19; engine, #406; amends D91)

**Decided:**
- **The predicate's cell, not its row.** `run_group` holds a conditional member (`check_and_mutate`) back for the next group only when an earlier member of its group writes the cell its predicate reads: any mutation of that column, or a delete of its family (a row delete is one family marker per family, D10). Another cell of the same row cannot change the predicate's answer. The row hash (`touched`) is looked at first, so the members' batches are scanned only for a row the group already wrote.
- **Read rows stay row level.** A transaction's read rows (`validate.reads`) are still checked by row against the group, and every row of a conditional member (written, read, predicate) against prepared, undecided shares, as D91 and D249 decided.
- **Its written rows are no longer checked against the group.** A write needs no applied state: plain commits and plain PREPAREs already share a group with earlier writes to their rows, in seqno order.
- **Only the commits on a held-back row wait with it.** A held-back member's rows (written, read, predicate) are held for the rest of the group, and a later single touching one waits too, its own rows then held as well, so per-row submission order holds. A single on other rows is admitted: commits in flight together have no order (D143, `Engine::submit`). A member that waits for arena room or a guarded flush's install still holds back every later single (the room wait's queue keeps submission order, D240). PREPAREs and COMMIT records never wait behind either, as before.

**Why:** gate 19 (5933e23e, `.coord/bench/results-20261019/`) measured contended ycsb-f at default durability (GroupSync) with Pigeonhole's p99 at 2.13× SQLite's (gate 18: 1.92×). The read side is a plain row read (p50 14.7 µs, p99 41 µs), and the run had no stalls, `Busy` retries or conflicts. The tail is the commit wait: read-modify-write minus its read was about 75 µs mean against a ycsb-a put's 59, and its p99 191 µs against 118. Under D91's row-level check, two read-modify-writes on one hot Zipfian row in one group put the second in the next group, behind the first group's sync, with every later single of the group, about one extra sync each. A local Linux repro (16 clients, scratch counters) deferred 6.4% of conditional commits at 16 shards and 11% at 4, where they were about 30% of the commits above p99. ycsb-f picks one of 10 fields, so about 90% of those overlaps were on another cell.

**Tests:** `crates/engine/tests/conditional_groups.rs`, application-owned on one shard, one WAL sync per group:
- a predicate on another cell of a row the group wrote joins the group (one sync);
- a predicate on the cell the group wrote waits, and `Exists` then holds (two syncs);
- the same after a family delete and a row delete;
- behind a held-back member, a commit on its row waits (seqno after it) and one on another row is admitted (seqno before it).

Main fails the first and last; dropping the family-delete clause fails the third, and dropping the held-row check fails the last.

**Acceptance (AX102, 2026-10-19, ctr274):** contended ycsb-f at default durability, the gate's shape (full scale, 16 clients, `--ops 100000 --min-secs 60`), main 5933e23e against the fix (code identical to this PR's), 3 runs each, sides alternated; `--shards 12` on main reported beside it. Raw: `/root/scratch/ctr274/out-ycsbf` on the box.

| run | ops/s | all p99 µs | RMW p50 µs | RMW p99 µs | RMW p99.9 µs | read p99 µs | `C` | shards with commits |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| main r1 / r2 / r3 | 303K / 311K / 310K | 167 / 163 / 168 | 81.9 / 78.8 / 78.8 | 184 / 183 / 187 | 274 / 279 / 283 | 40.4 / 42.0 / 41.7 | 11.3 | 13–16 |
| D260 r1 / r2 / r3 | 307K / 313K / 305K | 164 / 155 / 164 | 79.9 / 79.4 / 80.4 | 182 / 172 / 183 | 272 / 257 / 272 | 42.5 / 43.0 / 45.1 | 11.2 | 15–16 |
| main, `--shards 12` | 308K | 162 | 81.4 | 180 | 281 | 39.7 | 11.4 | 12 |

- **Medians:** RMW p99 184 → 182 µs, p99.9 279 → 272, all-operation p99 167 → 164. Within run-to-run spread: **not the ycsb-f fix the gate asked for.** The change stands as a correctness-neutral narrowing.
- **Why so little on the box:** one run of main with the scratch counters measured 1.0% of conditional commits cut and 0.2% held behind a cut (the VM's 6.4% does not carry over), at commit p50 95 µs and p99 260 µs against 60 and 165 for the rest, 6% of the commits above p99. Nearly every commit was its own group (12.8M groups for about 14M commits): the RMW tail is each commit's own GroupSync, about 150K syncs a second over 16 WAL files.
- **`--shards 12`** reads the same as 16 (RMW p99 180): the active shard count is not it either.

<a id="d261"></a>
## D261 — The contended step keeps judging the mixed p99 and reports each operation type's p99 beside it (owner decision, 2026-10-19; bench, #406; amends D247)

**Decided:**
- **The judged figure is unchanged.** Each judged `contended` row (D247) is judged on throughput and on the p99 over every operation (the mixed p99), within 1.5× of RocksDB and of SQLite at both levels, as before. There is no p99.9 target.
- **Each operation type's p99 is reported, not judged.** `summary.md` adds a table under the side-by-side one, by workload, level and operation type. It shows Pigeonhole's p99 and its ratio to the same type in each store: read-modify-write against read-modify-write, row read against row read, and so on, with ratios past 1.5× in bold. The types come from each run's `detail.by_type`, which every store's runner already records.
- **Accepted for Phase 4:** contended ycsb-f at the default durability. Its mixed p99 is 2.13× SQLite's on gate 2026-10-19, tracked with #755.

**Why:** a mixed p99 measures the operation mix as much as the store. Default ycsb-f's 2.13× against SQLite comes from two different types: its RMW is 1.06× SQLite's (191.5 against 180.2 µs) and its row read 2.31× (41.5 against 17.9 µs). The per-type table says which operation is slow.

The owner first asked for per-type verdicts. On gate 2026-10-19's results these made the gate stricter, not looser. ycsb-a, ycsb-f, sparse-wide and buffered time-series-ttl would have failed on one type each, and no failing row would have passed. So the owner kept the mixed p99 as the verdict and the per-type numbers as data. The slow types go to Phase 4 issues:
- row reads and scans under concurrent writes, at about 1.7–2.3× SQLite's p99;
- sparse-wide's buffered put p99, at 10× RocksDB's.

**Considered and not taken:** judging each operation type (stricter on every row it changes, above); judging only each workload's defining type (it leaves the other types unjudged).

<a id="d262"></a>
## D262 — A cross-shard commit's time in flight counts at every shard its round occupies: each participant adds its span at the decision, so sixteen clients committing across shards keep sixteen shards (coordinator decision, 2026-10-19; engine, #406; amends D257)

**Decided:**
- **A participant counts the round too.** The PREPARE carries the client's `submitted_at` (the coordinator's `Coord::submitted_at`, kept across retries), the participant keeps it with the prepared share, and when it applies the decision, commit or abort, it adds `now − submitted_at` to its `commit_inflight_nanos`, as the coordinator adds its own span at the reply. A PREPARE the participant refused or failed, or dropped because its close gave up, counts nothing, as before, and neither does the coordinator's own share (it prepares it like any participant; its reply span already covers it); the coordinator's count is unchanged; nothing on the client's path.
- **What `C` means now:** the mean number of commit rounds in flight, each client commit counted once per shard it occupies. A client with a two-shard commit outstanding reads as 2 while its rows live on two shards and as 1 once packing puts them on one. The bound is the clients times the shards a commit spans, independent of `k` beyond that: a packed store reads lower, which grows it back out, and the growth stops at the spread the commits' rows have (the spin term D257 removed grew with `k` without bound). The error is toward more shards during a tablet change or a timestamp-floor refusal: a share decided abort because a peer refused (`Moved`, `BelowFloor`) counts its attempt, and the retry counts again from the client's submit (on the contended row about one round in eight is refused below a floor and re-run at once, worth some 10% of its `C`).
- **Exposed:** the same counters and metrics as D257 (`ShardStats::commit_inflight_nanos`, `EngineMetrics::shard_commit_inflight_nanos`, `Metrics::shard_commit_inflight_nanos`, the bench's `ShardShare::commit_inflight_nanos`), now rising at the participants too; their docs say so. No new shared state: the inventory row's text in `concurrency.md` names the second writer site.

**Why** (perf2's xs16 slot, AX102, 0bcf0b4b, 2026-10-19, `.coord/inbox/archive/20261019-215213-perf2.md`): the gate's contended 16-client cross-shard row read `C` 7.9–8.1 at `k` = 16 (a client's commit holds it for about half its loop; its gets are the rest), so `k_c` = round(8 / 0.75) = 11, the busy sum called for about 14, and every run shrank 16 → 13–15 inside the measured phase (22–74 s in), moving 19–32 tablets under load against 2 on gate 19's row; the reps that stayed longer at 16 ran faster (328–350K ops/s). The row's larger loss is the share-report rescan (#761); the shrink is the rest, and it stays after that fix. Half the row's operations are two-shard commits, so each participant's count adds about half the coordinator's: `C` ≈ 12, `k_c` 16, the level ycsb-a reads at sixteen clients, and the store no longer shrinks under the load it is sized for. Counting at the participant fixes the measure rather than adding hysteresis (the alternative was a shrink needing `k_c ≤ k − 2`): a round that holds two shards' seqnos and watermarks does occupy two shards, and D217's busy sum already counts both shards' work for it.

**Acceptance (perf2, AX102, da19ecc8 against its base d9616533, 3 reps alternating, `k` traced per pass, `.coord/inbox/archive/20261020-004829-perf2.md`):** on the gate's contended cross-shard row `k` stays 16 for every measured pass in all three reps (base: 13–15 for 74–95% of the passes), measured moves 35 → 1, splits 1 against 4–6, ops/s 337K → 347K (+3%), p99 143 → 144 µs; p99.9 1.17 → 1.49 ms, consistent across reps at equal p99 and without a mechanism found (flagged, not blocking). `C` 8.0 → 18.5 (packing off 8.35 → 19.7). ycsb-a at one and sixteen clients, time-series-ttl and riders default read the same `k` and `C` within noise: they commit on one shard and run none of the changed path. **Why 18.5, where this text first expected about 12:** the row's cross-shard commits span 2–4 consecutive partitions (D246: a coordinator and 1–3 participants, mean 2), not two shards, and they are half the operations and about 53% of the clients' commit time; a participant's span is about 0.9 of the coordinator's, so a round counts about 2.8 where it counted 1 and the store's `C` about 2.0× the base's by that estimate (16–17); the retries of rounds a participant refused as below its timestamp floor (about 0.12 refusals per round on this row; a retry's participants count their attempt again from the client's submit) take it the rest of the way. The bound argument stands: `C` is at most the clients times the shards a commit spans, `k_c` is clamped to `n` (16 here, `want` 16), and nothing in it grows with `k`.

**Tests:** `spin_pool_terms.rs`'s `one_clients_cross_shard_commits_count_at_the_coordinator_and_the_participant` (one blocking client's two-shard commits on two shards that never pack: the participant's counter is above zero and below the coordinator's, the two shards added the same number of spans (one per round each, `Engine::inflight_counts`; a coordinator counting its own share at the decision too adds twice as many, which a ratio of the nanos could not tell apart on every platform: the macOS runner's reply hop reads 1.7× the decision span against 1.1× on Linux), the store reads at most about 2; without the participant's count the first fails, with the coordinator's own share counted the count check fails). D257's simulated tests use the override and pass unchanged.

<a id="d263"></a>
## D263 — A cross-shard commit takes one round: its PREPAREs carry a provisional id, and its seqno and timestamp are picked at the decision, held only until every participant applied (owner decision, 2026-10-20; engine, wal, format, #717, ICR 0046; amends D26, D142, D244, D245, D249)

**Decided** (option (a) of #717, after the A/B against v0.3.0 and option (b), per-shard visibility, which is shelved):
- **The id.** A commit's coordinator reserves one seqno from `NEXT_SEQNO` at the start, never held, published as pending or applied: the commit's **id**. Nothing is ever written at it, so a snapshot passes it at once. PREPAREs carry it in their `seqno` field, with `commit_ts` 0. It stays unique for the life of the database as D26's seqno id did: recovery raises `next_seqno` above every replayed record's seqno, discarded PREPAREs included.
- **The decision** (`decide_commit`, once every participant answered `Prepared` OK):
  - **`T`** is a default timestamp above every participant's floor: the floor each participant reports with `Prepared` (taken on its own thread after admitting and logging its share), and the published floors, coordinator raises and move raises.
  - **D245's raise-then-reserve moves here:** `fetch_max(T)` into each participant's `ts_coord_raises`, then the coordinator reserves the seqno `S` the commit applies at, and holds it.
  - **The hold** lasts from the decision until every participant applied: the DECISION group, `Decide`, apply, `Applied`. An attempt aborted before its decision holds nothing. Until 0.3.0 the seqno was held from the start, across the PREPARE round too, so every single reserved after it on any shard waited for both rounds to be visible.
  - The coordinator logs a **DECISION** (WAL type 4: id, `S`, `T`, participants; FORMAT §10.3, ICR 0046), and participants apply their shares at `(S, T)`, raising their floors to `T` first.
- **Admission keeps per-row order and OCC exact while a share is undecided:**
  - a single that writes a row an undecided share writes or reads, or reads (validate reads, predicate row) a row it writes, waits for the decision, and later singles on the row keep submission order behind it;
  - a PREPARE that writes such a row, or reads a row an undecided share writes, is refused with `PendingOverlap` and retried on D249's wake, older-first. That was D249's rule for conditional PREPAREs; it now covers every PREPARE;
  - a share's read rows are tracked beside its written rows (`pending_reads`), on validation-only participants too.
  - **Older-first wakes on a commit, not an abort (amends D249).** A commit refused behind an older one's share waits there until the older commit's next attempt there *commits*, or its last attempt is decided, or it retries without that shard (`Released`). Before, the older commit's next attempt being decided either way was enough. D263 refuses every PREPARE over an undecided share, which made this reachable: the older commit A, refused at the younger commit B's coordinator over B's own share, retried. B, woken by A's aborted attempt, retried with it and admitted its own share first again, on every round. #774's `tablets` sweep found it at seed 277, where two commits never resolved on the frozen clock. The younger commit holds no share while it waits, so the older one's retry meets none of its shares.
  - **A retry answered without a further attempt releases the commits behind it** (D249, #776). A retry refused at its coordinator (`start_coordination` on a closing or poisoned shard) sends `Released` to its earlier participants, its coordinator among them, as a retry that leaves the two-phase path already did (#772). Without it, a commit refused behind it there waited for ever. #774's `model_check` sweeps found it at seeds 19, 88, 240 and 266. 0.3.x has it for conditional PREPAREs (#776).
  - **A single held behind an undecided share ends with `Closed` once a shard has died** (perf287's review). Its coordinator may be the dead shard, so the decision may never come, and after a death no commit is accepted (D227). This is as `run_retries` ends room and overlap waits; conditional singles too. The single was never written.
- **Timestamp order (D11, D142, D244, D245):** exact per cell. Every write to a share's rows on a participant was either stamped before its admission (at or below the floor sent with `Prepared`, which `T` exceeds) or admitted after its apply (above `T`, at a seqno above `S`). Per tablet it holds except in one **straddle**: a single on another row of the share's tablet, reserved before `S` and stamped after the coordinator read that participant's floor, can land above `T` at a seqno below `S`. It reorders two different rows' timestamps, never two versions of one cell, and two decisions can cross the same way only on disjoint rows. By the coordinator's ruling (proposed by perf287), this is documented rather than refused, given `purge_race.rs` and the D142 sweeps pass unmodified (they do; the sweeps' check is unchanged, pairing a PREPARE with its DECISION by id and checking it at `(S, T)`).
  - D142's, D244's and D245's PREPARE-time checks go (`floor_refusal`, `floor_below`, `stamps`, `stamps_last`, `pruned_max`, D244's (i) raise): a PREPARE has no timestamp to check. `BelowFloor` and `MoveRaise` refusals no longer happen; `refused_below_floor` stays in the metrics, at 0, and a test hook can still force one.
  - D245's room refusal (`PrepareError::Room`, ICR 0042) is unchanged.
- **Retries** take a fresh timestamp at their own decision; an aborted attempt never picked one, so nothing is preset.
- **Recovery** pairs PREPAREs with a DECISION by (coordinator stream, id) and applies them at its `(S, T)`, all or nothing as before; the flushed checks use `S`, and `next_seqno` covers every DECISION's `S`. A type-3 COMMIT (format version 2) replays as before, its PREPAREs at their own seqno and timestamp. A seqno a crash lost (an undecided id, or an `S` whose DECISION was not logged) can be handed out again, as any lost seqno could: nothing durable names it.
- **Shard death and close:** unchanged. Before the DECISION is written the coordinator answers `Closed` (an abort); after it, `OutcomeUnknown`. `S` is released through `Applied`, or stays held with `not_applied` as before.
- **The format version** is 3 (ICR 0046): a writable open of an older file rewrites both superblock pages first, so 0.2.x and 0.3.x refuse a file this build wrote with `UnsupportedFormat(3)` instead of failing on a DECISION as corruption or skipping its segment as torn.
- **Counters:** `CrossShardMetrics::share_waits` and `share_wait_nanos` (engine and public crate), the singles held back behind an undecided share (once per group that held them) and the time a shard held one.

**Why:** #717. Under 0.3.0 a cross-shard commit's seqno was held across both rounds, so the visibility of every single reserved after it waited for the commit's slowest participant twice over.
- **The A/B** on the AX102 (perf2, 2026-10-20, median of 3, `inbox/done/20261020-200514-perf2-717-ab-results.md`): xs_contention 16 shards / 32 clients / 50% cross-shard +11% ops/s Buffered and +18% GroupSync; the gate's contended cross-shard row +9% (+7% in the `all` run); single-shard p50 37 → 24 µs Buffered and 193 → 104 µs GroupSync. Refused rounds per cross-shard commit on the gate row fell from 0.0102 to 0.0000. ycsb-a/c held.
- **Costs:**
  - cross-shard p99.9 rose about 1.5–2× (xs Buffered 510 → 1041 µs, GroupSync 2.1 → 4.3 ms, gate 223 → 340 µs), with p50 unchanged. perf2's decomposition (`inbox/done/20261020-215940-perf2-717a-tail.md`) traced it to participant stalls that v0.3.0 has too: the `Flushed` handler's turn of about 21 ms, share-report turns, and fsync. (a) runs more rounds into them; the follow-up is #777;
  - on `transfer`'s hot rows, about 2% of cross-shard commits held a single back, about 230 µs each (49–62 ms/s over 16 clients); on the uniform rows about 0.00002 per commit;
  - two seqnos per committed cross-shard round (its id and `S`), so "seqnos used minus commits" overstates wasted rounds by about one per commit.
- (b) gained more (+22–44%) but changed D19/D227's global prefix for single-shard commits, cost 3.5% on ycsb-c t=1, and grew cross-shard tails more.

**Tests** (`crates/engine/tests/tablets.rs` unless named):
- `a_blind_single_after_a_validation_only_prepare_lands_above_the_commit` (fails with the single's wait disabled), `a_single_shard_transaction_reading_an_undecided_shares_row_waits_and_conflicts`, and `review.rs::cross_shard_write_skew_is_prevented`: OCC across the decision;
- `a_single_is_not_held_back_by_a_commit_still_in_its_prepare_round`: the one-round claim;
- `an_undecided_prepare_left_by_a_crash_is_never_applied_after_more_commits_and_a_crash`: ids are never reused for a stale PREPARE;
- `a_cross_shard_commit_in_a_0_3_0_wal_replays`: hand-written type-2/3 records replay at their own seqno and timestamp;
- `a_participant_refuses_a_commit_timestamp_below_its_floor` (a stale published floor): one round, the share above the participant's floor;
- `crossing_commits_regressions_from_the_seed_sweep` (seed 277, fails with the abort wake) and `model_check.rs::harness_regressions_from_the_seed_sweep` (seeds 266, and 19, 88, 240 with deferred I/O; fails without the `Released`);
- `blocking_waits.rs::singles_held_behind_a_share_end_closed_when_its_coordinator_panics` (the gate panics the coordinator's DECISION sync; hangs without the `Closed`);
- the D142 sweeps, `cross_shard_commits_converge_on_a_frozen_clock`, `purge_race.rs` unmodified, and `model_check.rs` with a harness that pairs a PREPARE with its DECISION by id (`tests/common/mod.rs`: a decided commit's PREPAREs are rekeyed from id to `S` within the run that appended them, and an alias whose `S` a crash lost is reverted to the id, history included);
- `crates/pager/tests/format_upgrade.rs`, `crates/engine/tests/format_upgrade.rs`: ICR 0046's upgrade;
- loom (`src/ts_order_loom.rs`): raise→reserve at the decision keeps every later-seqno single above `T`; the straddle and crossing decisions are reachable.

<a id="d264"></a>
## D264 — One timestamp raise per shard: a tablet move's floor and a cross-shard decision's timestamp share `ts_raises`, folded once per group after the reservation (owner decision, 2026-10-20; engine, #724; amends D245)

**Decided:**
- **One atomic.** `Shared::ts_coord_raises` goes; a coordinator `fetch_max`es its decision's timestamp into each participant's `ts_raises` (Release, before it reserves the seqno the commit applies at), where a tablet's old owner already raises the new owner to its floor (`commit_op`, AcqRel, before the move is published).
- **One fold.** A shard folds `ts_raises` into its floor once per group, after the group's reservation and before it stamps (`fold_raises`), as it folded the coordinators' raises. `default_ts` no longer loads `ts_raises` for every single it stamps.
- **Why it still orders both:**
  - a coordinator's raise: unchanged from D245 (the participant's reservation is a read-modify-write on `NEXT_SEQNO` after the coordinator's, so the fold reads from its release sequence);
  - a move's raise, through the commit's routing: the old owner raises, then the move is published in the view (Release). A client routes a write on the moved tablet to the new owner only after loading that view (Acquire), and submits it through the new owner's queue; the new owner then admits it, reserves, and folds. A write the old owner forwards was forwarded after its raise, on the same thread. Either way the raise happens before the fold.
- `decide_commit` reads each participant's published floor and its one raise.

**Why:** D245 kept the coordinators' raises apart from the move raise only because D142's seqno-blind check compared a PREPARE's timestamp with `ts_raises`: coordinator raises for later seqnos there would have refused earlier ones again. D263 removed that check (a PREPARE carries no timestamp). One fewer shared primitive (#724), and one fewer atomic load per stamped single on the single-shard commit path.

**Tests:** the D142 sweeps and `cross_shard_commits_converge_on_a_frozen_clock` (tablet moves among cross-shard commits, every shard's timestamps in seqno order), `purge_race.rs`, `model_check.rs`, unmodified; loom: `loom_a_moved_tablets_floor_and_a_decision_share_one_raise` (the old owner raises then publishes the view, a coordinator decides concurrently, and a client that loads the published view routes a single to the new owner, which stamps it above the moved floor and above a decision at a lower seqno; it fails with the view published Relaxed), beside D245's two models on the same atomic.

<a id="d265"></a>
## D265 — A failed instance can recover in place behind the `pigeonhole` handle: opt-in, by swapping a reopened engine in on the writer lock the failed one kept (owner decision, 2026-10-21; pigeonhole, engine, #620, ICR 0050)

**Decided** (the design on #620, fable252 2026-10-20, with the owner's answers of 2026-10-13 and 2026-10-21):
- **Opt-in.** `Options::auto_recover(Recovery)`, default `Recovery::Off`: today's behaviour, the recovery guide's close-and-reopen. `Recovery::Io` recovers from `FailureCause::Io` on a shard and from a failed manifest commit (`Failure::MAIN_FILE`). `Recovery::Any` also from `Panic` and `Internal`: a bug the same input may repeat, so the user's informed choice. Application-owned mode refuses the option with `InvalidArgument`: the application drives the shards, and handing it new drivers is a separate design.
- **The shape: swap the engine behind `Db`.** The public handle holds the engine in an `ArcSwap` with a generation (1 at open, +1 per swap); every operation loads the current engine. Tables keep working (ids are persisted; the handle re-resolves its entry by name). A supervisor thread per writer handle, started only when the option is on, parks in the engine's failure wait and, on a failure the setting covers: closes the failed engine keeping its locks, waits the backoff, reopens the same path with the same `Options` and the same VFS, swaps the new engine in, bumps the generation and wakes every waiter.
- **The writer lock and presence are handed over, never released and re-taken** (`Engine::close_for_reopen`, `Engine::open_with`, `Handover`). A second in-process open fails with `WriterLocked` while the old handle holds the writer byte on every platform (Linux OFD locks are per open file description; the macOS/BSD registry arbitrates between handles of one process), and releasing first opens a window for another process's writer and for a reader's last-one-out removal of the shm region. The failed engine's final close leaves the locks in place, skips the presence upgrade and removes neither the WAL files nor the region, as after a crashed writer; the reopen replays the WAL and builds the region's next generation. When the final close ran before the handover was asked for (every shard dead), or when a `close` was already under way (the handover call waits for that close, which releases the locks, or hands them over if it is a `close_for_reopen` itself), there is no handover and the supervisor falls back to a plain open. A reopen that fails gives the handover back with the error, the locks still held, so the budget's retries never open the window; `open_with` refuses a path other than the handover's file.
- **Writer-process snapshots from before the swap expire.** Engine snapshots in the writer process never expire (D148 expires reader ones through the shm generation), and an old view stays memory-safe on its own, but the reopen frees the extents its recovered root does not name and reuses them, so a read through an old view could return `Corruption` or wrong rows. `pigeonhole::Snapshot` records its generation, and every use of a snapshot, a transaction, a scan or a row iterator from an older generation fails with `SnapshotExpired` before it touches the engine. The old engine is dropped at the swap.
- **What each call sees.** A commit in flight at the failure is answered as today (`OutcomeUnknown`; `Closed` = not committed, ICR 0038; `Io` = refused before writing, ICR 0040). During the close-to-reopen window, an operation that would otherwise get `Closed` from the closing engine (commits, catalog changes, flush, compact, and reads without a snapshot) waits up to `write_stall_timeout` (30 s by default) for the swap and then runs on the new engine; past the bound it fails with `Busy`, retryable; after a give-up, `Closed`, as on a failed instance today. A commit the dying engine refused with `Closed` is resubmitted once on the new engine: nothing was written. `OutcomeUnknown` and `Io` are never retried. Pending commits and async futures are answered by the engine that took them. `close()` during a recovery waits for it to settle, then closes the current engine. `is_failed()` and `failure()` describe the current engine: true and the failure while a recovery runs, false and `None` after the swap. Reader processes: unchanged (D148).
- **The budget** is constants in 0.4: at most 3 recoveries in a sliding 10-minute window, backoff 0 s, 1 s, 4 s before the reopen. A reopen that fails with `Corruption`, `UnsupportedFormat`, `NoSpace`, `WriterLocked` or `ShmVersionMismatch` ends recovery at once; an `Io` at reopen retries within the budget. Past the budget the handle stays failed as today.
- **Observability:** `Pigeonhole::recovery_stats()` (`generation`, `recoveries`, `gave_up`, the last failure, the last recovery's duration), a separate call because `engine_metrics()` restarts from 0 at the swap (ICR 0044 defines it as cumulative since the engine opened).

**Why:** #620. A failed shard ends the instance, and a service that does not supervise its own process is left with a dead handle until it closes and reopens. The swap reuses the open path recovery already uses and the model checker covers; restarting shards inside one `Engine` would need a restart path through every piece of shared state. Reads through old views had to expire for the same reason D148 expired reader snapshots. The alternative, keeping only the documented reopen pattern, costs nothing; the option buys a short pause instead of a dead handle, at the price of one `ArcSwap` load per operation on every handle (measured against the ceilings before the option lands) and a supervisor thread when it is on.

**Lands in four PRs** (ICR 0050): the engine handover (`tests/handover.rs`); the `Db` swap plumbing with the ceilings and allocation A/B; the option, supervisor, waits, stats, docs and targeted tests (`tests/recovery.rs`); the public model checker's arm (I/O faults with the option on, unacknowledged commits decided by reading back) and its sweep.

*PR 4's arm (`io_failures_are_recovered_in_place_and_match_a_durable_prefix`): one shard, `FaultPlan::io_error_ppm` on reads and writes, `Recovery::Io`. A commit answered `OutcomeUnknown` is logged unacknowledged and decided by reading back; one answered `Busy` at a failure is retried once on the recovered engine; after every swap the store must match an allowed prefix of the commits from before the failure under a process crash's floor (the files are intact; only `None` records in the stream's buffer and the unacknowledged commit may be missing), followed by the commits since; a commit acknowledged while the generation moved is read both ways. Past the budget the run closes and reopens by hand. It found the two PR 3 fixes below before PR 3 merged.*

**Amended 2026-10-21 (owner decision, after the hot-path A/B on #796): the handle caches the engine, and a closed engine releases its memory.** Loading the engine from the `ArcSwap` on every operation costs about 80 instructions on a point get, above the D195 ceilings, and a protected load that costs less (a hazard pointer) still about 30. Instead each handle (`Pigeonhole`, a table, a reader, a batch) caches its `Arc<Engine>` with the generation it was taken in and checks the generation with one load per operation (about 16 instructions on a get, 14 on a commit); a stale handle falls back to the `ArcSwap` and keeps working, and the recovery guide says to open new table handles after a recovery. Since a stale handle keeps the old engine alive, the failed engine's close releases what is large (`Engine::release_memory`, ICR 0050): it publishes an empty view, empties the block and row caches, and on Linux frees the memtable arenas' pages (`madvise(MADV_REMOVE)`) once the reopen superseded the region and no view from before the close lives; a stale handle then holds about 5–10 MiB (on macOS the arenas' resident pages too: a POSIX shared-memory object's pages cannot be punched). The get-mem, get-miss and versions ceilings are re-based from the merged head with the standard 0.3% headroom (D195), in a separate ceilings commit of PR 2. The reader-process contract is unchanged: a reader attached to the superseded generation fails its reads with `SnapshotExpired` after reading (D148), and the memtable reader never faults on zeroed pages.

**Amended 2026-10-21 (#824, from perf287's review of the memory release): the close waits for the stream's submitted I/O.** A submitted WAL operation runs on the I/O backend's own threads, which outlive the engine instance on the pread backend, and lands however its completion is dropped; a poisoned shard's final sync waited for nothing, and a spare preparation's zero-fill writes (D35) and a held successor header's chain (D200) were never waited for. So an instance closed for a reopen in the same process (the handover, or the recovery guide's close and reopen) could hand the file over with such a write in flight, and the reopened generation could start a segment in the slot it then landed on: its durable records lost at the next replay. Every WAL stream now counts its submitted operations (`IoTracker`), and the engine's close, after it joined every shard and pool thread, waits on every stream's count, poisoned or not, and only then releases the writer lock (a plain close released it on the shard thread before, so another process's writer could take the file with such a write in flight: the same reuse across processes); a close that found another under way waits for that whole close. An application-owned engine waits inside its final close, on the application thread that runs it. The data file needs no such wait: its writes are synchronous on joined threads, or root commits the final close already waits for through the manifest exclusion. A device that never answers makes the close block; the supervisor therefore runs `close_for_reopen` under the budget's deadline and never opens a second instance beside a close that has not returned.

**Amended 2026-10-21 (owner decision, PR 3): what a commit sees across the recovery.** A commit that arrives while the recovery runs waits for the swap (bounded by `write_stall_timeout`, then `Busy`) and runs once on the new engine. A commit that was in flight when the shard failed is refused by the failing engine with `Closed` before anything is written, and the engine consumes the batch it refuses: resubmitting it would need a clone of every batch on the hot path (the alloc-budget tests forbid it) or an engine API that hands refused batches back, so it comes back as `Busy` ("not committed, retry") rather than being resubmitted, and the caller retries it on the recovered engine; `OutcomeUnknown` and `Io` are never retried, as before. Async calls (`submit_async`, the futures) do not wait: they return `Closed` or `Busy` at once. The recovery's deadline is four write-stall timeouts from the failure (two minutes by default), well above the operations' own wait, so a waiter gets `Busy` and retries before the handle turns failed for good: a close that has not returned by then (a device that never answers holds its I/O wait, #824) gives the recovery up, the handle stays failed, and the handover that returns later is dropped, which releases the locks; nothing is ever opened beside a close that has not returned.

*Amended 2026-10-21 (PR 3, round 4, from the model check's recovery arm and the instructions run): the empty view the failed engine's memory release publishes (#821) is marked `released`, and a read as of now (`get_latest`, `latest_view`, the writer's `snapshot`) that loads it fails with `Closed` instead of answering from nothing, so the handle waits for the swap as this decision says; a `close()` racing a failure the supervisor has not marked yet does not wait for it (perf287's R5, option (b)): the supervisor finds the handle closed and ends, and the close closes the failed engine as a close without the option would, unclean, the next open replaying the log (test `a_close_racing_an_unmarked_failure_closes_the_failed_engine_at_once`, the supervisor held on its wake by the `FailureWake` hook site); and `retry_closed` is the bare operation with the option off (the first shape cost about 120 instructions per point get).*

<a id="d266"></a>
## D266 — A cross-shard commit's records leave the log once every shard holding its writes has published a flushed-through seqno past it; the share reports go (owner decision, 2026-10-21; engine, #724; amends D24's mechanism, D225 (#585) and D254)

**Decided** (#724 area 4, design B, with perf287's review and the protocol addendum of 2026-10-21):
- **The bound.** Each shard publishes `flushed_through[p]`, the largest seqno at or below which every write shard p will ever hold is in a durable SST (#800, PR 1): `min(oldest unflushed memtable entry − 1, visible watermark)`, published after a flush's root commit, at `Start`, when an `Unpin` or `AwaitFlushed` finds nothing at or below its seqno unflushed, and at close once flushes are idle; Release store, Acquire load, never decreasing.
- **D24 read off the bound.** A cross-shard commit at seqno S is replayed only whole, so its records may leave a log only once every share is in SSTs:
  - **PREPARE:** needed while undecided; once decided, until its own slots are flushed through S and every other holder's `flushed_through` is at or above S. Aborted: never.
  - **COMMIT:** needed until every other holder's `flushed_through` is at or above S. Its own share is its own PREPARE's, logged before it in the same stream. An incomplete COMMIT found at replay has no S (`apply: None`) and is never needed, which replaces #591's `share_reports` test.
  - **A live COMMIT never decides alone** (perf287's review). A coordinator is always a participant: it is the first shard of its commit's split, and a retry re-splits and re-routes. It logs its own PREPARE, a validation-only one too, before it decides, and that PREPARE is undecided, so still in the log, when the COMMIT is logged (a `debug_assert` there). The checkpoint pops in log order, so the PREPARE, waiting on the same holders, always leaves first; the COMMIT's own check is defence in depth, which no mutation of it can fail. A replayed COMMIT may have lost its PREPARE to an earlier checkpoint (passed once every share was in SSTs), and waits on its own.
- **Holders.** A live record's holders are the commit's participants: `Decide` carries them (`Arc<[ShardId]>`, built once per commit). A record replayed at open takes the shards the replay applied the commit's shares to, since a reopen places tablets again and the logged participants may no longer hold them. Within one run a tablet changes shard only once its memtables are flushed (#800's `commit_op` assertion), so the participants stay a sound holder set.
- **Re-checks by request, not broadcast.** A checkpoint stopped at a cross record whose other holders are behind sends each one `AwaitFlushed { from, through: S }`, once per peer and seqno (`awaiting`). The peer answers `FlushedThrough` once its published bound reaches S, at once if it has; with nothing unflushed at or below S it publishes again rather than flush (perf287: a bound held only by the visible watermark needs no flush). A seqno not yet visible is waited for first (a `Wake::Shard` visibility waiter). A decided PREPARE at the front looks at once.
- **`Unpin { through }`** freezes every slot whose open memtable holds an entry at or below `through`, or publishes when there is none. It no longer starts a pass on the receiver: a pin pass asks every holder behind a needed record directly, so the transitive asks (participant → coordinator → its participants) and their dedupe (`asked`, `origin`), the `Through` pass and its seqno index go.
- **Removed:** `unreported.rs` (#777's index), `share_reports`, `settle_to`, `settle_out`, `commit_ckpt`, the `SharesFlushed` and `CommitsSettled` messages and their handlers, `report_shares_flushed`, `settle_if_done`, `send_settled` and D254's backstop, and the `report_msgs`/`report_seqnos` test hooks (replaced by `flushed_msgs`).

**Why:** after #778 the 16-shard cross-shard rows still spent about 6 ms of the `Flushed` turn reporting about 57K shares at about 110 ns each, plus the coordinators' `SharesFlushed` and the participants' `CommitsSettled` turns (#763: 4.3–4.8 ms and 2.8–3.1 ms per run before #778). Budgeting the turn raised the p99 (#777). Per-share work at apply, flush and settle goes; what remains is a check of a few atomics per cross record at the log front, and a message pair per blocked peer. The cost: settlement is conservative. A cold memtable on a holder holds back every cross record involving that holder, not only the commits that wrote to it. The WAL pin's `Unpin` bounds this, as it bounds a cold slot's single-shard records.

**Tests:**
- `cross_checkpoints.rs` (recast from `share_reports.rs`):
  - one flush passes 600 cross records with tablet changes, and 2,100 without (no `Maintain` re-check), in at most 12 `AwaitFlushed`/`FlushedThrough` messages, where D254 sent 6 report messages and per-share reports were 1,200;
  - perf287's validation-only share on an idle shard: the idle shard answers by publishing, and every checkpoint passes after the flush;
  - each survives a power loss.
- `wal_pin.rs`, unmodified: the idle participant and the cold-slot cases.
- The model checker's crash check of the bound (#800), now also against each surviving commit's owners as the reopened engine places them (perf287's #800 follow-up).
- The sweeps: `model_check` with and without deferred I/O, `model`, `tablets`; `crash_at_every_write_point`.

**Acceptance:** a box A/B against main before merge (owner, 2026-10-21), queued after perf2's gate. Rows: the gate's contended cross-shard row at p99 and p99.9, xs Buffered and GroupSync, the regrown 2M row (≥ 0.8× packing off, #763's bar), ycsb-a, WAL pinned bytes and unpin flushes on a hot-and-cold tablet mix, and an injected-write-stall row (WAL bytes pinned on the other shards).

<a id="d268"></a>
## D268 — A full `compact()` leaves no SST that may hold purgeable entries, a last-level run included; an SST a bottommost compaction wrote with nothing held back says so (owner decision, 2026-10-21; engine, compaction, sst, format, #805, #820, ICR 0053)

**Decided:**
- **A full compaction rewrites a lone SST, or a run already at the last level, when any of its SSTs may hold purgeable entries** (`compact::may_hold_purgeable`). Those are delete markers, a cell old enough to have expired under the family's TTL, or more entries than rows in a family that keeps `max_versions` (not a counter). Each slot is rewritten at most once per `compact()` request (the shard's `full_purged`). Before #814 a lone SST was moved, or left, as it was; before #820 a run was always left as it was. One written while a snapshot or an unflushed memtable held its purge back kept every version and marker.
- **The fully-purged property.** A bottommost compaction with no live snapshot, nothing above its inputs (`min_ts_above == u64::MAX`), and for a counter family every other source listed, records `Properties::fully_purged = Some(max_versions)` on its outputs. Such an SST is not a candidate for its markers or versions while the family's policy still covers it: a counter, or every version kept now, or at least as many as then. TTL expiry is still checked. The property goes in format 3's properties block (still unreleased), after `merge_operator`. Blocks written before it read as unknown, so such an SST is judged by the other checks and rewritten once.

**Why:** #805. The owner's intent is that a full `compact()` leaves nothing purgeable. The format-free checks (#814) can't tell a row of several columns from a column with extra versions, so extending them to runs would rewrite a multi-column `max_versions` family's whole last level on every `compact()` (14–19 ms per MB, measured on macOS). The property makes that rewrite happen once. The alternatives were to accept the repeated rewrite, to rewrite runs only on delete markers and TTL (leaving the versions gap open for runs), or to remember clean SSTs in memory (one rewrite after every open).

**Not covered:** in a keep-all family, a put shadowed by a newer put at the same timestamp, written while a snapshot needed it, in an SST without the property. Nothing in the metadata shows it.

**Cost:** for a family that keeps `max_versions`, the manifest's metadata never settles a last-level SST, so every `compact()` reads each one's properties through the open-SST cache, opening its reader the first time. On a quiet database that is the whole cost of a `compact()` with nothing to do (about 10 ms for a fresh 10-SST run on macOS, microseconds once open). If it shows up at thousands of SSTs, an in-memory set of SST ids known to be fully purged, kept per open, avoids the repeated reads without a format change.

<a id="d267"></a>
## D267 — A blind cross-shard PREPARE over an undecided share goes behind it instead of being refused; an older one wounds a younger undecided share (owner decision, 2026-10-21; engine, #816, ICR 0056; amends D263, D249)

**Status: accepted** (owner, 2026-10-21), on the box A/B below and perf287's protocol reviews (R1–R4, the reader-fence fix). **Its chaining, `CHAIN_CAP` and wound-wait are superseded by D271** (2026-10-22): with one allocator a write over an undecided share's write is admitted, so nothing chains. The reader fence and waiting singles stay.

**Decided:**
- **Chaining.** A PREPARE that only writes (no validated reads, no predicate) is admitted on a participant even where an undecided share writes or reads one of its rows. D263 refused it.
  - **What it waits for.** It is tracked behind every undecided share there that writes or reads a row it writes, and behind every single waiting on one of its rows, together with the shares that single waits for.
  - **When it answers.** Its `Prepared` is sent only once those shares are decided at that participant and those singles are logged.
- **When a blind PREPARE is still refused** (`PendingOverlap`, D249):
  - a row it writes already has `CHAIN_CAP` (8) undecided shares, unless it is older than every undecided share on its rows (see the starvation item below);
  - a refused commit that reads, older than it, fences one of its rows (below).
- **A PREPARE that reads** (validation or predicate) keeps D263's refusal. It is also refused when it reads a row a waiting single writes.
- **Order.** The later share's `Prepared` leaves the participant only after the earlier share's `Decide` has been applied there.
  - **Seqno.** The earlier coordinator reserved its `S` before sending that `Decide`, and the later coordinator reserves its `S` after receiving every `Prepared`. So `S_later > S_earlier` in `NEXT_SEQNO`'s modification order.
  - **Timestamp.** The earlier coordinator raised the participant's `ts_raises` to its `T` (Release) before reserving its `S`. The chain of messages carries that raise to the later coordinator's Acquire load. So `T_later ≥ T_earlier`; where they are equal, the seqnos order the two versions, which is still a consistent order.
  - **Behind a waiting single.** Its `Prepared` leaves after the single was logged, so after its seqno was reserved and its timestamp stamped (the participant's floor covers it). One cell's versions keep seqno and timestamp in the same order, as D263 required. The straddle D263 documents is unchanged.
- **Singles.** A single that overlaps an undecided share starts waiting and records the undecided shares on its rows at that moment (`WaitingSingle::ahead`).
  - **It waits for those only.** It goes in once they are decided. Shares that chain on its rows later wait for it, as above.
  - **Bounded.** This bounds its wait by the chain ahead of it, at most `CHAIN_CAP` shares plus older commits exempt from the cap, and keeps per-row submission order.
  - **Ending the wait.** A waiting single that is forwarded or parked for a tablet change ends its wait there. Back, it starts a new one.
- **Wound-wait.** Waits between chained shares could form a cycle: A behind B on one shard, B behind A on another. So a share chained behind a *younger* commit's undecided share sends that commit's coordinator `Wound`. This includes a share it is behind through a waiting single, where the edge is made explicit.
  - The coordinator decides an attempt not yet decided as an abort at once, without waiting for the remaining `Prepared` answers.
  - Answers arriving after the decision are ignored entirely, a late `Conflict` or `Moved` included. The abort already told the participants whether it was the commit's last attempt (D249), and the retry must follow that.
  - The participant records the younger commit as refused behind the older one, D249's older-first wake: the older commit's next attempt there committing, its last attempt, or `Released`.
  - A commit already deciding is not aborted; the older share simply waits for its `Decide`.
  - Every wait that remains runs from a younger commit to an older one, or to a decided commit, or from a share to a single, which waits only for shares admitted before it. So no cycle forms.
- **Starvation.** Among admitted (chained) shares, the oldest commit is never wounded. Two rules keep a commit from being refused for ever:
  - **The cap.** A blind PREPARE older than every undecided share on its rows chains past the cap and wounds them, so the queue holds at most `CHAIN_CAP` shares plus older commits.
  - **The reader fence.** A refused commit that reads, and is older than every undecided share on its rows, *fences* those rows (`reader_fences`). Younger blind PREPAREs writing them are refused, chained or not, so the row drains and the commit's retry gets in. A younger PREPARE that reads is not fenced: it cannot chain, so it cannot keep the row undecided past its own share, and D249 orders it against the older commit as on main. Fencing it too made every reading commit on a hot row wait out an older one's whole retry: on the box A/B the transfer row (reading commits only) ran at 0.61–0.66x main with 2x the p99, and locally at 0.45–0.54x (19.4–20.0K ops/s on main, 8.9–10.5K fenced, 19.4K with reading PREPAREs unfenced). The fence ends once that commit's PREPARE is tracked there, its last attempt is decided there, or it retries without that shard (`Released`). Its last attempt ends the fence even when nothing waits on that shard: for example, the attempt failed validation there, so it was never tracked, and the commit's own refusal had already been woken. Before this was fixed, the fence outlived its reader, and the younger commits it refused waited behind a commit that was gone until the write-stall timeout (the bench's transfer smoke).
  - **Reading commits against each other: no hard bound, as on main.** The fence covers blind chains only. A commit that reads keeps D249/D263's oldest-first refusal order against other reading commits: a refused one waits behind an older commit's share, but a younger reading commit on its first attempt can still land between the row draining and the older one's retry. Each such overtake costs the older one a round, not a queue (the overtaker's share is the only one on the row), and readers never wait for each other's `Prepared`, so no cycle forms. Nothing bounds the number of overtakes, as on main. `CrossShardMetrics::max_attempts_reading` reports the worst case seen. If it grows with run length, the extension is wound-wait for readers (a reading PREPARE refused over a younger share wounds it, perf287's review).
- **What a refusal waits for** (perf287's R4). Each refused PREPARE waits for what actually refused it, never for shares that may already be gone. Otherwise it would be woken at once and refused again every round.
  - **Refused by a fence:** it waits behind the fencing reader, with D249's older-first wake (the reader's next attempt committing there, its last, or `Released`), which is the fence's own end set.
  - **Refused by a waiting single:** it is woken when that single is logged (`end_single_wait`).
- **An attempt aborted before a participant's group admitted its PREPARE** leaves nothing there. The decision removes the share, and the queued PREPARE is dropped unlogged and unanswered (the coordinator waits only for `Applied`).
- **Overlap wakes and wounds go through the shard's queue even to the shard itself.** This includes the wake for `Released`, since every overlap wake goes through `wake_overlap`. Otherwise a `Wake` could overtake a `Wound` queued before it for the same commit: it then reached a coordinator that was not waiting yet and was dropped. Handling a `Wound` inline would also decide a commit in the middle of a group's admission.
- **Recovery** needs nothing new.
  - Several undecided PREPAREs on one row in one stream (D263 had at most one) are each resolved by their coordinator's record, and applied at its own `(S, T)`.
  - A chained share has no decision until the share ahead of it is decided, so a crash mid-chain either drops it or applies it above that share.
- **Counters,** in the engine and public crates (ICR 0056):
  - `CrossShardMetrics::chained`: PREPAREs admitted behind an undecided share or a waiting single.
  - `wounds`: younger undecided shares wounded.
  - `max_attempts_blind` and `max_attempts_reading`: the most attempts one committed cross-shard commit took, since open. Only committed commits count; one that gave up with `Busy` never shows, so read them with the `Busy` failures.
  - The bench reports all four in its stalls detail (`xshard_*`).

**Why:** #816. Under D263 a PREPARE overlapping any undecided share was refused, so cross-shard commits on a hot row ran one at a time, each taking a full undecided window (about 0.5 ms with application-owned inline driving). A herd of retries then multiplied the rounds: about 4.8 attempts per commit.
- **Gate 21:** the app-owned row fell from 886K to 322K commits/s at Buffered (p99 0.5 → 20.6 ms) and from 393K to 120K at the default durability. Bisected locally to #774.
- **Local A/B** (`phdb-bench app-owned --scale small`, Buffered, two interleaved runs each, before the review fixes):

| | ops/s | p99 | rounds |
|---|--:|--:|--:|
| main | 175–200K | 16–18 ms | 174–187K |
| chaining and wounds | 267–308K | 6.4–6.8 ms | 70–71K |
| before #774 | 308–321K | 2.6–3.2 ms | 39K |

- **Box A/B** (AX102, non-reference per D5; medians of 3 reps, sides rotated; Busy 0 in every run). The first pass compared main ae9379c7 with 655239ac; the rows the reader-fence fix touches were re-run against main with only the attempt counters added (`max attempts`: the most attempts one committed commit took, per rep).

| row | ops/s, main → D267 | p99 | p99.9 | max attempts, reading (main / D267) | max attempts, blind (main / D267) |
|---|--:|--:|--:|---|---|
| app-owned, Buffered | 355K → 396K (1.12x) | 18.9 → 18.5 ms | 34.6 → 31.1 ms | – | 422–492 / 91–94 |
| app-owned, default durability (first pass) | 122K → 188K (1.54x) | 51.1 → 21.8 ms | 86.0 → 32.0 ms | – | – |
| mixed (blind writers and reading commits on hot rows), Buffered | 283K → 396K (1.40x) | 23.9 → 11.7 ms | 43.8 → 18.0 ms | 355–386 / 154–159 | 347–430 / 38–39 |
| transfer, Buffered | 4.9K → 5.1K (1.05x) | 51.1 → 51.1 ms | 132.6 → 124.8 ms | 8–10 / 8–12 | – |
| transfer, default durability (5 reps) | 4.3K → 5.3K (1.22x) | 55.3 → 45.4 ms | 147.8 → 118.5 ms | 8–17 / 11–15 | – |
| gate contended, Buffered / default (first pass) | 0.98x / 0.99x | 80 → 80 / 241 → 243 µs | 135 → 135 / 840 → 881 µs | – | – |
| `all` cross-shard (first pass) | 0.99x | 124 → 125 µs | 322 → 326 µs | – | – |
| ycsb-a, 1 / 16 threads (first pass) | 0.98x / 0.98x | level | level | – | – |

- **Transfer at the default durability is bimodal on the box, on both sides:** some runs reach about 11K ops/s with a fraction of the shard parks. Over all 8 runs per side, the medians are about 4.7K (main) and 5.2K (D267). D267 does not act there (no chains, no wounds; reading commits skip the fence).
- **The first pass fenced reading PREPAREs too:** transfer ran at 0.61–0.66x main and mixed at 0.88x. Fixed as described under the reader fence.
- **App-owned at Buffered gains less on the box than locally:** about 18 ms p99 on both sides.

- **Without wounds, older commits starved:** the max latency reached 2–3 s.
- **The alternatives:**
  - Refusing a newcomer while *any* older refused commit waits on the row brought the collapse back. The fence above applies only to commits that read, which cannot chain.
  - Refusing newcomers while a single waits cost 15–20%. Singles are ordered in the chain instead (perf287's review, R1).
  - Waking only the oldest refused commit cut refusals but made hot rows a convoy, below main.

**Tests** (`crates/engine/tests/tablets.rs` unless named):
- **Chaining and wounds:**
  - `a_blind_prepare_over_an_undecided_share_chains_instead_of_refusing` (on main B is refused);
  - `an_older_prepare_chained_behind_a_younger_share_wounds_it` (hangs with the `Wound` send removed).
- **Starvation** (each fails with its rule removed):
  - `a_single_waiting_on_a_hot_row_goes_in_while_commits_keep_chaining`;
  - `the_oldest_blind_commit_chains_past_a_full_row_and_wounds_it`;
  - `a_reading_commit_gets_in_against_a_stream_of_blind_commits`.
- **Refusal waits:** `a_prepare_refused_by_a_fence_waits_behind_the_reader`. Three shards; the reader's coordinator is held still, so the fence stands while `a` is clear. 30 fenced commits are refused at most 32 times; with the refusal waiting for shares instead, 281 times.
- **Fences:**
  - `a_fence_ends_with_its_readers_last_attempt_even_with_nothing_waiting`. The reader's retry fails validation on the fenced shard. A later blind commit on the row goes in unrefused; before the fix, it was refused and never woken.
  - `a_younger_reading_commit_is_not_held_by_a_fence`. The reader's coordinator is held still, so the fence stands; a younger reading commit on the fenced row commits. With reading PREPAREs fenced, it waits behind the reader.
- **Crashes:** `a_crash_mid_chain_applies_the_decided_share_and_drops_the_one_behind_it`. A is decided but not applied on the participant, B is chained and logged behind it, then a crash: the reopen applies A and drops B.
- **Model check** (`model_check.rs`):
  - `hot_rows_chain_and_wound_across_many_shards`: Zipfian blind batches over 3, 6 and 10 shards;
  - `hot_rows_chain_and_wound_through_crashes`: the same shape on 4 shards, with crashes and mid-commit crashes;
  - `hot_rows_mix_reading_commits_and_blind_writers`: transactions and conditional writes beside blind batches on the hot rows, on 3 and 6 shards, with no commit allowed to give up with `Busy`.
- **The existing sweeps** run unchanged.

<a id="d270"></a>
## D270 — A packing move leaves a claimed shard alone and claims nothing itself; a change that waits on a compaction claims no shard (owner decision, 2026-10-21; tablets, engine, #724 area 2; amends D217, D224)

**Decided:**
- **A packing move honours skew claims.** An inactive shard hands its written tablet to the least busy active shard with room that no skew change has claimed (D224). It used to ignore claims, so in one pass a packing move and a skew move could both pick the same receiver before its load showed.
- **A packing move claims nothing.** Packing hands load to the active shards on purpose. So the race remains in the other order: a skew change on another shard can still pick a receiver a packing move chose earlier in the same pass, since packing leaves no claim to see. Slot reservations (`reserved`) still bound the slots, and both kinds firing in one pass happens only at a change of regime (packing runs under light writes, skew under load). Claiming each target for a dwell would allow one packing move per target per dwell: three tablets onto one shard took 321 passes instead of 301 in `light_writes_pack_tablets_onto_fewer_shards`, and a 16-shard store packing to two shards would take about one dwell per two tablets. The design comment had proposed the claim; the measurement replaced it with this.
- **A change that waits on a compaction claims no shard.** `claim_targets` claims nothing for a change touching the compacting tablet (#102's `change_waits`). It claims when `start_waiting_change` decides it again at the compaction's end. Before, the claim was taken inside `choose`, the change was then dropped, and the claim held its targets off every other skew change for its dwell (10 or 100 passes).

**Why:** #724 area 2's inventory (comment 6329234329): the two kinds of tablet move picked targets by different rules, and a dropped change kept a claim.

**Tests** (`tests/tablets.rs`, with test hooks `Engine::claim_shard` and `Engine::shard_claimed`):
- `a_packing_move_waits_for_a_claimed_shard`: with the only active shard claimed for 400 passes, no tablet moves past the shrink; once the claim runs out the three land together.
- `a_change_that_waits_on_a_compaction_claims_no_shard`: a hot tablet's split, held by its compaction, claims no shard; it splits once the compaction ends.
- Each fails without its change.

<a id="d271"></a>
## D271 — One allocator for seqnos and default timestamps; a cross-shard write over an undecided share's write is admitted (owner decision, 2026-10-22; engine, shm, #854, #816, ICR 0063; supersedes D267's chaining and wounds; amends D11, D244, D245, D263, D264)

**Decided** (#854's design note, with perf287's correctness sections and perf2's cost and #816 sections):
- **One allocator.** Every seqno a writer reserves comes from one per-engine word, `Shared::clock` (`(S, H, F)` under a lock): a group's seqnos, a cross-shard commit's id (D263), its decision's seqno. Its default timestamps come from the same reservation. shm `NEXT_SEQNO` becomes a mirror, raised after each reservation by `ShmRegion::advance_next_seqno` (`fetch_max`, ICR 0063), so it never passes a reservation, and FORMAT §11.3's publish order holds: the pending watermark first, then the mirror. No shm layout change.
- **Stamps** (variant B):
  - **A group's singles:** `T0 = max(now, H + F, last_X + 1)`, where `last_X` is the shard's own last stamp; `H' = max(H, T0 + k − 1)`; `F = 0`. Single i is stamped `T0 + rank_i`. Its rank is above every earlier single of the group on one of its rows (two on one cell never share a timestamp), and never below the single before it, so a shard's stamps follow its seqnos.
  - **A decision:** `T_d = max(now, H + 1)`, `H' = T_d`, `F = 1`. So a group reserved after it in the same µs takes `H + 1`, and cannot tie with the decision's cells (the owner's ruling).
  - **A tablet changing owner** sets `F`: the next stamp anywhere is strictly above every one handed out.
- **At open:** the seqno is seeded at replay's catch-up, before anything at open reserves one. `H` is seeded with the largest stamp handed out: every shard's last stamp once replay raised it, and the manifest's `ts_floor`, not the clock (a decision just after the reopen must not be pushed past `now`). The manifest records `H` as `ts_floor`. No format change.
- **Admission** (§1.6): a write over an undecided share's *write* is ordered by the allocator, so it no longer overlaps.
  - A blind PREPARE over it answers `Prepared` at once.
  - A blind single on such a row goes ahead.
  - What still overlaps is optimistic: a read of a row an undecided share writes, and a write to a row it read. A single waits (D263); a PREPARE is refused and woken (D249).
  - D267's reader fence stays (an older refused reader fences younger blind PREPAREs off its rows), and so do its waiting singles: they keep a stream of reading commits from starving a writer single.
  - **A PREPARE that writes a row a waiting single reads is refused until the single is logged** (`OverlapWait::Single`, woken by `end_single_wait`), the mirror of D267's refusal of a PREPARE reading a row a waiting single writes. A waiting single, once the shares it waits for are decided, is admitted without checking newer undecided shares, so without this it would read the row without the PREPARE's write while that write's decision could take the lower seqno: a lost update through `check_and_mutate`, or a missed transaction conflict (perf287's review, M1). D267 had held such a PREPARE's `Prepared` behind the single instead.
  - **A group's conditional member no longer counts its *written* rows against undecided shares** (`conflicts_with_group`, which changes D260's conditional-group rule): a write over an undecided share's read is refused by `overlaps_undecided`'s read check, and a read over an undecided write by the reads loop.
  - D267's chaining, `CHAIN_CAP` and wound-wait go. `CrossShardMetrics::{chained, wounds}` stay at 0 (ICR 0056).
- **The floors** (`ts_floors`, `ts_raises`) were removed in a second PR, with the floor in `Prepared`, `ts_order_loom.rs` and D244's mechanism (its guarantee stays, restated here). Each shard keeps its last stamp (`last_stamp`, passed to `reserve_group` as `last`) in a plain field of its own state: variant B's per-shard strictness. It is seeded from the manifest's `ts_floor`, raised by every replayed or applied record and by the shard's own reservations, and never published. The manifest's and a backup's `ts_floor` are `H` (never below the manifest's last). The test hooks `max_ts_floor` and `publish_stale_ts_floor` became `last_default_ts` (`H`), and the stale-floor case of #105 is gone with the floors.

**Why:** #816. Under D263 a later commit on a hot row had to wait for the earlier one's decision to carry its timestamp, refused (D263) or chained (D267). Chains crossing shards needed wounds: about 2.4 per cross-shard commit on the box, and without them the run deadlocked. With one allocator nothing waits for another commit's decision on a write. Measured locally (small scale, Buffered): app-owned 461K commits/s at 1 round per cross-shard commit and 0 refusals, against main's 157–182K and before #774's 308–321K.

**Cost:** one lock-protected reservation per group; the pending watermark's lower bound is the mirror, not a second lock. On x86 (callgrind) that is +50 instructions per single-group commit (commit-one +0.54%, commit-overwrite +0.44%, commit-sixteen +0.10%). One 128-bit word through `portable-atomic` measured +77 (its compare-exchange is outlined on baseline x86-64), and two locks per group +65. The prototype's inline `cmpxchg16b` was +25 per group, and level on ycsb-a at 1 and 16 threads on the box (#854 §4); the engine forbids `unsafe`.

**Drift gauge** (ICR 0064): `EngineMetrics::default_ts_lead_micros` is how far `H` is ahead of the clock now; `max_default_ts_lead_micros` is the largest lead at any reservation since open. Default timestamps run ahead of the clock only when one shard reserves more than one group a µs, or a group follows a decision in the same µs.

**Tests** (`crates/engine/tests/tablets.rs` unless named):
- **Admission:**
  - `a_blind_prepare_over_an_undecided_share_is_admitted_and_answers_at_once`;
  - `a_blind_single_on_an_undecided_shares_row_goes_ahead`;
  - `a_crash_with_two_shares_on_one_row_applies_the_decided_one_and_drops_the_other`.
- **Order:** `default_timestamps_keep_seqno_order_on_every_shard_under_cross_shard_commits` and its siblings (`d142_sweep`, seeded). They now also check the order across shards: all streams' writes by seqno, timestamps never going back, and each decision strictly above the write before it and strictly below the write after it. They fail with the decision at `H` instead of `H + 1`, and with a group ignoring `F`. On the frozen clock they check that the drift gauge reads a lead.
- **D249's overlap tests** now overlap through reads: `overlap_txn` reads a second family of the rows it writes.
- **The allocator's unit tests** (`clock.rs`).
- **M1:** `a_blind_prepare_writing_a_row_a_waiting_single_reads_lands_after_the_single` (`tablets.rs`): A undecided on `a`, a conditional single C waiting for it, and B blind across shards writing `a:p` (C's predicate column), coordinated by the shard where C waits. Over six turn orders, if C applied it is serialized before B. It fails without the refusal (C applied at seqno 8, B at 7). model_check's `hot_rows_conditional_singles_beside_blind_cross_shard_writers` covers the shape with deferred I/O, but did not reach that window in 200 seeds without the refusal.
- **The model check** in both test targets, its D267 hot-row shapes unchanged (renamed `hot_rows_blind_overlaps_*`); and a no-hang run of the contended cross-shard rows (app-owned, mixed, transfer, gate contended) at both levels.
- **`blocking_waits.rs`:** `singles_held_behind_a_share_end_closed_when_its_coordinator_panics`: the blind single goes ahead; the conditional one still waits and ends `Closed` at the coordinator's death.
- **Loom** (`clock_loom.rs`, `--cfg engine_loom`): a group and a decision reserved in the same µs against a reader's `visible_seqno`. No seqno shows before it is applied, stamps follow seqnos, and a decision is strictly above. It fails if the pending watermark is not published before the mirror rises.

<a id="d272"></a>
## D272 — Transactions in 1.0: no lock mode; conflicts stay at (row, family) granularity; the transfer gap stays open under #708 (owner decision, 2026-10-21; pigeonhole, engine, #694)

**Status: accepted** for C1 and C2 (owner, 2026-10-21). C3 is **open**: the owner rejected accepting the gap, and a perf agent investigates under #708.

**Decided:**
- **C1: no pessimistic or lock-based mode in 1.0.**
  - The spec's transactions have no lock manager.
  - D267's chaining and wound-wait cover blind writers on hot rows.
  - A lock or "intent" mode (D249's named follow-up) is post-1.0 if C3's investigation needs one, and the owner asked for C3 to be solved short of it.
- **C2: conflicts stay at (row, family) granularity.** *Amended by [D275](#d275) (2026-10-23): a `get` is validated per cell; `get_row` keeps the family granularity.*
  - A transaction's reads are recorded per (row, family) (`get`, and `get_row` per family, ICR 0062). A commit that touches any column of such a family in that row makes it conflict.
  - Per-cell tracking would mean qualifier-level read sets and a per-qualifier check in the SSTs. It would only remove false conflicts between transactions that read and write different qualifiers of one family.
  - The guide's transactions page documents the granularity and the modeling advice: put columns that change independently and are read in transactions in different families.
- **C3: the `transfer` row (D247's 3× rule) is not accepted.** At 16 clients and θ 0.99 it's 38× behind RocksDB's pessimistic locking at Buffered (#708, gate 2026-10-14), and the owner judged the mode unusable at that level.
  - **D267 doesn't act on it:** transfer uses only reading commits. It measured 1.05× (Buffered) and 1.22× (default durability) against main.
  - **The work:** improving it without a lock mode goes to a perf agent under #708.
  - **What it can use:** `EngineMetrics::transaction_conflicts` and `transaction_retries` (ICR 0061), `max_attempts_reading` (D267), and `transact`'s backoff (ICR 0060).
  - Until #708 lands, #694's "the transfer row is within the agreed bound or explicitly accepted" stays open.

**Also part of #694's hardening** (recorded here so the transaction decisions are in one place):
- **Write-free transactions** commit at their snapshot without validation, and never conflict (ICR 0060 part 1). This is safe because writers are validated, so serializable in seqno order; it would not hold under plain snapshot-isolation writers (Fekete et al.).
- **`transact`, `RetryPolicy` and the transaction counters** are ICR 0060 part 2 and ICR 0061; `get_row` and the write ops are ICR 0062.
- **The black-box serializability checker** (#868) runs in CI and in sweeps.

<a id="d273"></a>
## D273 — Read validation reads a column's first version only, where D271's order holds (owner decision, 2026-10-22; engine, #708 candidate 3; builds on D271)

**Decided:**
- **The invariant.** Since D271, every default timestamp comes from the one allocator, monotone in seqno. A cell's versions sort newest timestamp first, then newest seqno first among equal timestamps. So when every version of a cell has a default timestamp, **its first version holds its largest seqno**.
- **Validation** (`ShardState::conflicts`, a transaction's or conditional write's read checked at PREPARE or in its group) looks for any entry of the read `(row, family)` with a seqno above the snapshot. In a memtable where the invariant holds, it reads each column's first version, steps once, and seeks past the column only when the next entry is an older version of it. A hot row is O(columns), not O(versions). A conflict still returns at the first newer entry.
- **The fallback is exact.** A memtable is walked whole, as before, when it holds any cell the invariant can't vouch for. Its `MemEntry::explicit_ts` flag is set on every memtable a batch wrote when:
  - the batch holds any cell with an explicit timestamp, whose version can sit behind newer ones with a larger seqno;
  - the batch is a replayed record, which may come from a build before D271.

  It's a superset, so it stays exact. The decode already in `apply` (dedup's scan) notes the batch-level bit once; `apply` ORs it into each memtable it writes. The flag travels with the memtable when it freezes.
- **Write-path cost** (x86 callgrind against main): commit-one +21, commit-at +9, commit-sixteen +141 (+0.28%), the lowest worst case of four forms measured. A per-cell flag was +194 on commit-sixteen; a cold after-loop pass was +59 on commit-at, which writes explicit timestamps.
- **The invariant D273 rests on:** every write path that stamps a *default* timestamp goes through D271's allocator (`reserve_stamps`, `reserve_decision`), and anything else sets `explicit_ts`. Counter families are the standing example: their puts and operands carry D186's fixed timestamp as an explicit one, so a counter memtable is walked whole (needed for bucket increments, `merge_at`, which can land behind a newer bucket). A future ingest or import path writing memtables directly must set the flag (perf287's review).
- **SSTs keep the full walk.** A per-SST flag would need a format change. Validation already skips every SST whose newest seqno is at or below the snapshot. The SST share is measured (test hook `validate_sst_entries`; removed in #951, as no committed test read it). If it matters, the format change follows after 1.0 or under D269's `ro_compat` rules.

**Why:** #708's box decomposition: transfer at 16 clients, a cross-shard commit cycle of 570–647 µs. Validation was the largest single step (215–231 µs, 35–41%), walking every buffered version of the hot account when no conflict was found. It sits on the hot account's serial path.

**Tests** (`tests/tablets.rs`, test hook `Engine::validate_entries`):
- `a_hot_rows_validation_reads_one_version_and_still_conflicts`: a 2,000-version row validates in at most 2 entries, and a write after the snapshot still conflicts.
- `an_explicit_older_timestamp_behind_newer_versions_still_conflicts`: an explicitly older write behind 50 newer versions still conflicts.
- `a_replayed_memtable_is_validated_whole`: after a process crash, the replayed memtable is walked whole (at least 200 entries) and still conflicts.
- `a_counter_increment_after_the_snapshot_conflicts`: a counter family's `put_i64`, then an increment after a transaction's snapshot, conflicts. Every such write carries D186's fixed timestamp, so the versions tie on timestamp and follow seqno order; this passes with or without the fallback.
- `a_counter_bucket_increment_behind_a_newer_bucket_still_conflicts`: a bucket increment (`merge_at`) into an older bucket after the snapshot sits behind a newer bucket with a lower seqno, and still conflicts. It fails without the fallback.
- **Mutations:** ignoring the flag fails the second and third tests; walking always fails the first.

<a id="d274"></a>
## D274 — Transactional rows are judged against the other engine's own optimistic transactions; the bench-side row locks go (owner decision, 2026-10-22; bench, gate, #708; amends D247)

**Decided:**
- **Every store runs the transactional workloads with its own transaction API.** That covers transfer and ycsb-f's read-modify-write, plus fjall's counter increments and row deletes. Nothing is wrapped in a bench-side lock: D247's striped `RowLocks` and its mutex rows are removed, since every former mutex row has a native counterpart.
  - **RocksDB, `rocksdb` (the judged baseline):** `OptimisticTransactionDB`. A transaction takes a snapshot at its start (`set_snapshot(true)`, as Pigeonhole's transactions read at theirs), reads with `get_pinned_for_update` (validated at commit) and writes. A commit that finds a conflicting write fails with `Busy` and runs again, as Pigeonhole's optimistic transactions do. `TryAgain`, which means the memtable history was too short to validate, also runs again and is counted apart (perf287's F2). For transfer and ycsb-f the `rocksdb` engine runs on it (`RocksDbTxnRunner`); every other workload keeps the plain `DB`, so their baselines don't move.
  - **RocksDB, `rocksdb-txn` (reported):** the pessimistic `TransactionDB`. `get_for_update` takes the row's lock until the commit (the default lock timeout), a transfer locks its two accounts in key order, and `Busy` or `TimedOut` runs it again.
  - **fjall:** its `OptimisticTxDatabase`, a write transaction retried on `Conflict`: transfer, read-modify-write, counter increments (fjall has no merge operator) and row deletes.
  - **Unchanged:** RocksDB's counters (a merge operator) and row deletes (a range delete) were already native; SQLite runs immediate transactions.
  - Every store counts its retries (`txn_conflicts`).
- **Pigeonhole's rows are transaction against transaction** (the owner's ruling on perf287's F1):
  - **transfer** runs a `Transaction`, as before;
  - **ycsb-f's judged row is `pigeonhole-txn`:** its read-modify-write in a `Transaction` (read, put, commit, retried on `Conflict`). Its `commit_if` row (`pigeonhole`) is the idiomatic fast path, reported beside it.
  - **A lost `commit_if` race counts as a conflict,** as a transaction's retry does.
- **Targets** (owner, 2026-10-22), at 16 clients in the gate's `contended` step, Buffered and default durability:
  - **Judged, against RocksDB `OptimisticTransactionDB`, on throughput and p99:** a **hard floor of 1.2×** on each: RocksDB's throughput over Pigeonhole's, and Pigeonhole's p99 over RocksDB's, are each at most 1.2. The **goal is 1.0×** on both, Pigeonhole at least level; it's reported as met or not, but not judged. This applies to transfer (judged from now on; D247 reported it) and to ycsb-f (`pigeonhole-txn`). ycsb-f's SQLite comparison keeps D247's 1.5×.
  - **Tracked, not judged, against RocksDB `TransactionDB`:** within **2.5×** on throughput and p99. If the pessimistic row is more than 2× from the optimistic one on throughput, the report flags it and the owner decides.
- **report.py** judges these rows as above. **run-window.sh** runs transfer among the judged workloads, adds `rocksdb-txn` to transfer and ycsb-f, and adds `pigeonhole-txn` to ycsb-f.
- **Retries: each store's out-of-the-box policy** (owner, 2026-10-23: "We want to 'beat RocksDB' out of the box, not by adjusting it to match our approach"):
  - **Pigeonhole's judged rows** (transfer, `pigeonhole-txn`) run through `Pigeonhole::transact` with the **default `RetryPolicy`**, what an application gets from the library. Each attempt after the first is counted in `txn_conflicts`, and `Busy` is retried outside it as for every other write.
  - **RocksDB and fjall retry at once,** as their idiomatic loops do. Neither has a retry policy of its own. No competitor gets a backoff in a judged row.
  - **Backoff variants** (`--txn-backoff US`: `RetryPolicy`'s full-jitter backoff from `US` µs, 0 for none) are reported and never judged. `report.py` skips any run whose config carries `txn_backoff=`.
  - **The default `RetryPolicy`** gets tuned from measurement on transfer, transactional ycsb-f and the mixed rows at both levels. Its 100 µs base dates from before D273. The study covers fixed bases of 0, 5, 10, 25, 50 and 100 µs and an adaptive base scaled from observed commit latency. The default is proposed from the data, and an API change gets its own ICR.

**Why:** #708. D247's RocksDB row was a plain `DB` behind a bench-side 1,024-stripe mutex held from the reads to the write: no retries, and not RocksDB's transaction API. Comparing Pigeonhole's optimistic transactions against it measured something no RocksDB user would deploy. The owner wanted the comparison API to API, with the optimistic one as the bar.

**Tests** (`runners/mod.rs`):
- `rocksdb_transactions_lose_no_update`: 8 clients × 100 read-modify-writes of one cell, both kinds, lose none.
- `rocksdb_transfers_keep_the_total`: transfers among 4 accounts from 8 clients keep the total, both kinds; the optimistic kind counts conflicts.
- `rocksdb_transactions_read_what_pigeonhole_reads`: both kinds read exactly what Pigeonhole reads on transfer and ycsb-f.
- `pigeonhole_transactions_lose_no_update` and `pigeonhole_transactions_read_what_pigeonhole_reads`: ycsb-f through a Pigeonhole transaction loses no update, counts its conflicts, and reads what `commit_if`'s row reads; `pigeonhole_loses_no_update` now checks that lost races count too.
- `fjall_reads_what_pigeonhole_reads` and `fjall_runs_concurrent_rmw`: these now run on fjall's transactions.

<a id="d275"></a>
## D275 — A transaction's cell read is validated per cell; family and row reads keep their granularity (owner decision, 2026-10-23; engine, pigeonhole, #708; amends D272 C2)

**Decided** (design on #708, comment 6356115899; perf287's review, comment 6356142758):
- **What a read records.** `get` (and `get_async`) records the one cell it read, `(table, row, family, qualifier)`. `get_row` records each family it covers, as before (every family when none are named). A transaction's reads are sorted and deduplicated at commit, a family read dropping the cell reads of its family it covers. They are shared by every participant of a cross-shard commit and by its retries (`Arc<Validate>`), not copied per participant. Reads are never logged: no WAL or format change.
- **What conflicts with a cell read:** a later entry (seqno above the snapshot) of its column (a put, a merge operand or counter increment at any bucket, a cell delete, a column delete), or a later family marker of its row in its family (a family delete, or a row delete's marker, D10). A write to another qualifier, family or row doesn't.

  | later write | cell read of `f:q1` | `get_row` of `f` | `get_row` of the row |
  |---|:-:|:-:|:-:|
  | put, merge or increment, cell or column delete of `f:q1` | conflict | conflict | conflict |
  | the same of `f:q2` | **no** | conflict | conflict |
  | family delete of `f`, or row delete | conflict | conflict | conflict |
  | any write to family `g` of the row | no | no | conflict |
  | any write to another row | no | no | no |

- **The walk** (`ShardState::cell_conflicts`). Seek keys are built with the format's encoders (`encode_marker_prefix`, `encode_column_prefix`), so the escaping keeps `q`, `q\0x` and the empty qualifier apart, and the marker prefix (`00 00`) matches no qualifier.
  - **Every marker version of the row** is read, whatever the memtable's `explicit_ts` flag: a family or row delete may carry an explicit timestamp, so the row's markers need not follow seqno order, and there are few of them (perf287's P1).
  - **The column:** its first version only where D273's order holds; all of its versions (and only its) where the memtable's flag is set. That is exact for a cell read: nothing outside the column and the markers changes it.
  - **SSTs** holding anything above the snapshot are probed with the point read's filters first (`Probe::admits`): the column filter is keyed per column (row and qualifier), and every SST holding a marker adds the row's marker prefix to it, so a miss on both rules the SST out; the row filter is used only as a row filter (P3). An admitted SST is read over the markers and the column, every version (SSTs have no flag).
  - **Family reads** walk the family's row as before (D273's per-column step).
- **The group cut** (`conflicts_with_group`). A conditional member waits for the next group when an earlier member of its group wrote what it read. For a cell read, that is an earlier **single** that wrote the cell: `writes_cell` counts every mutation of the column (put, merge or increment, cell delete, column delete) and any family marker of the family (a row delete arrives expanded into markers) (P4). A PREPARE share admitted earlier in the group is undecided and left to the overlap sets (its rows are in `pending_rows`) (P5). Family reads keep the row rule.
- **Unchanged:**
  - **the cross-shard overlap sets** (`overlaps_undecided`, `pending_reads`, `pending_rows`, `held_rows`, D249/D263/D271's refusals and waits) stay keyed by row. They order commits and never report `Conflict`, so row granularity costs latency only, and M1's refusal (#886) relies on them being a superset;
  - **not validated, as before:** a family's TTL or option change, and a cell expiring by TTL between the read and the commit. These are catalog and time effects, not writes.
- **The read set** is a sorted `Vec<ReadKey>` with owned row and qualifier bytes (the design's single-buffer form saves an allocation per read and was left out: it would touch every row-hash site for no measured gain).

**Why:** D274's box run (#708, comment 6355660465): the transactional ycsb-f row at Buffered was 2.37× behind RocksDB's `OptimisticTransactionDB` (p99 18×), with about 180× its conflicts. A ycsb row has 10 fields and each read-modify-write touches one; under (row, family) validation, writes to other fields of a hot row conflicted. RocksDB validates per key.

**Measured** (AX102, io_uring, 16 shards, full scale, 16 clients, 3 reps alternating, medians; base main ab5320ce and head #917 8dfdd2c0, each with #912's bench crate; `/root/scratch/ctr274/out-d275`):

| row | base | D275 | D275 / base | `OptimisticTransactionDB` | D274 judged (D275) |
|---|--:|--:|--:|--:|---|
| ycsb-f txn, Buffered | 301.8K, p99 926 µs, 9.6M conflicts | 731.2K, p99 55 µs, 131K conflicts | 2.42×, p99 0.06× | 661.7K, p99 61 µs, 52K conflicts | 0.90×, p99 0.90×: **goal met** (base 2.19×, p99 15.3×) |
| ycsb-f txn, default | 295.8K, p99 178 µs, 652K conflicts | 352.3K, p99 128 µs, 50K conflicts | 1.19×, p99 0.72× | 154.3K, p99 241 µs | 0.44×, p99 0.53× (was already met) |
| transfer, Buffered | 95.4K, p99 2,884 µs | 105.7K, p99 2,556 µs | 1.11×, p99 0.89× | 375.5K, p99 623 µs | 3.55×, p99 4.11× (base 3.94×, 4.63×): still #708's per-attempt gap |
| transfer, default | 41.7K, p99 5,505 µs | 43.9K, p99 5,177 µs | 1.05×, p99 0.94× | 50.7K, p99 3,539 µs | 1.16×, p99 1.46× (base 1.22×, 1.56×) |

Every rep of head is above every rep of base on all four rows. ycsb-f's conflicts fall to RocksDB's order (about 2.5× at Buffered, 1.2× at default). Transfer reads and writes the one balance cell per row, so its conflicts don't change; its small gain is the cheaper walk and the shared `Validate`.

**Tests:**
- **`tests/tablets.rs`:**
  - `d275_conflict_matrix_on_one_shard`, `d275_conflict_matrix_on_a_participant`: the matrix (three reads × eleven later writes, the delete kinds included), with the write in a memtable and in an SST; the second validates on a shard that doesn't coordinate.
  - `a_cell_read_on_a_wide_row_visits_its_column_only`: a 2,000-column row validates in at most 3 entries.
  - `cell_reads_keep_escaped_and_empty_qualifiers_apart` (P2).
  - `an_explicit_older_family_delete_behind_newer_markers_conflicts` (P1).
  - `a_counter_cell_read_conflicts_only_with_its_own_increments`: increments and bucket increments of the read counter conflict, of another counter of the row don't.
- **`tests/conditional_groups.rs`:** a transaction reading another cell of a row the group wrote joins the group (one sync); after an earlier put, cell delete, column delete, family delete or row delete of the read cell, or an increment of the read counter, it waits and conflicts; a `get_row` reader waits for any write to the row.
- **`pigeonhole/tests/serializability.rs`:** the history check's keys are now two cells to a row.
- **Mutations:** each fails a test: no marker check (the matrix, P1's test, the group conflict); the first marker only (P1's test); a row-level group cut (the two join-the-group tests); a cell read validated as its family (the matrix, the escaping and counter tests); SSTs skipped (the flushed half of the matrix); no column check (the serializability history).

<a id="d276"></a>
## D276 — A commit refused over an overlap fails at once when the share it overlapped commits a write to a row it read (owner decision, 2026-10-23; engine, pigeonhole, #708 candidate 2, ICR 0066; builds on D249, D263)

**Decided:**
- **Where the doom is found.** A participant that refuses a PREPARE over an undecided overlap (`PendingOverlap`, D249) keeps the refused commit's validated reads in its `OverlapRefusal`. When a share that writes one of those rows commits there, `on_decide` has already applied it (`decide_share`). The participant then runs the same validation as a PREPARE (`conflicts`) on the reads of those rows, against the commit's snapshot (`doom_overlaps`).
- **What it sends.** If a read is stale, the participant drops the refusal and sends `Wake { Doomed }` instead of the overlap wake. This happens before `release_overlaps`, which would otherwise wake the commit.
- **What the coordinator does with it.**
  - A retry waiting for a wake fails with `Conflict` at once (`on_doomed`).
  - A round still undecided, or in its abort round, keeps the doom (`doomed`) and fails at the round's end (`on_applied`), instead of queueing the retry.
  - In both cases the commit's last participants hear `Released`: fences it set, and commits waiting behind it, go on as for any commit that runs no further attempt.
  - Otherwise the commit has ended, or it runs a later round whose PREPARE there either comes after the doom and fails validation, or was refused again and is doomed in the same pass.
- **`Released` never precedes a participant's decision of the doomed round.** The coordinator fails a doomed commit only in two places. One is `on_applied`, once every participant has answered the round's abort `Decide`. The other is a retry queued after that. So each participant has already decided the round, and released its fences' last-attempt state, when `Released` arrives. The participant that sent the doom has dropped its refusal before sending; any later wake for that priority finds no retry and no round, and is ignored as a stale overlap wake is.
- **It is exact.** It fails only commits the retry's validation would fail. The snapshot is the same across retries, and the commit's snapshot pin (`Validate::_pin`) keeps any version above it from being purged. A write to a family the commit did not read dooms nothing. A commit's own share never dooms its own refusal.
- **Counter:** `CrossShardMetrics::failed_fast` (ICR 0066) counts **once per commit, at its coordinator** (`fail_doomed`), not per participant that found it doomed: several participants may send a doom for one commit, and only the first acts.
- **Shutdown:** a doom kept for a round (`doomed`) is dropped when the shard's coordinated commits are answered without a retry (a shard's death at close, `answer_unsettled`), like the rest of the coordinator's state there.

**Why:** #708's transfer sizing at Buffered. 58% of cross-shard attempts conflict, and a conflicting attempt costs 74–77 µs against 48 µs for a successful one. Most of the difference is the refused round, the wake, and a second round that only fails validation. About 4.4M of 16.9M rounds were refused.

**Tests** (`tests/tablets.rs`):
- `a_refused_cross_shard_transaction_whose_read_goes_stale_fails_without_another_round`: B is refused over A's undecided share, and A commits a write to the family B read. B fails with `Conflict` after one round (`failed_fast` 1, two rounds in all). Without `doom_overlaps` it takes three rounds.
- `a_refused_cross_shard_transaction_is_not_failed_for_a_write_to_a_family_it_did_not_read`: the same overlap with A writing a family B did not read. B retries and commits.

<a id="d277"></a>
## D277 — The stale tail lives in the shared memtable node, so reader processes skip superseded versions too (owner decision, 2026-10-23; memtable, shm, engine, #930, #397; refines D194)

**Decided:** option (a) of #930. The writer stores the stale tail D194 computes at insert in the shared skiplist node, so reader processes jump past a finished column's superseded versions exactly as the writer does. #397, deferred on a wrong attribution of #930's profile, is undeferred and fixed by the same change.

**Why:** a reader process's hot-row reads cost 15.1 µs against the writer's 4.7 µs (ycsb-a's 16 hottest rows, row cache off; CPU time per read in a Linux VM). With the writer's index off (`PIGEONHOLE_TAIL_INDEX=0`), the writer's own reads cost 14.0 µs. So the whole gap was D194's process-local index, which reader processes did not have. With no writes running, reader and writer read paths cost the same.

**What changes:**
- **Node layout (FORMAT §11.6):** the header grows from 12 to 16 bytes, with `tail` u32 at offset 12 (a later node of the same column, or 0), and the tower moves to offset 16. The 3 reserved bytes could not hold an offset (24 bits of 4-byte units reach only 64 MiB).
- **Versions:** the memtable header's `version` goes 1 → 2, with flag bit 1 ("tails recorded", set by the writer before the memtable is published). `ShmLayoutVersion` goes 3 → 4. Writer and readers upgrade in lockstep: a version 3 process refuses a version 4 region, and a version 4 writer rebuilds a version 3 region once alone.
- **One mechanism:** D194's `TailIndex` (a paged table in the writer's memory, an eighth of the arena) is removed. The writer reads tails from the arena too.
- **Unconditional write (perf287's P2):** every insert writes `tail` (0 by default) before the node is linked, tails recorded or not, because chunks are reused without zeroing.

**Publication:** no new protocol. `tail` is written before the node's Release links, and names the insert-time level-0 successor or that node's own tail, a node linked earlier. A reader that acquired the node's link therefore sees the tail and the target's header and links. A late tail write would be harmless too: the target is older, and the field is one word. So safety rests on P2 and on validation, not on the order.

**Correctness for readers:** D194's seven conditions carry over unchanged, since readers use the same tail values. The only new reader-side condition is untrusted input. A tail read from shared memory is validated as a node (non-null, 4-aligned, header and key in bounds, height 1–16) before its link is loaded (perf287's M1), and must be in the column being skipped (P1). Anything else is `Corrupt`, never a fault or a silent skip.

**Trust (perf287's option 2 on #934):** only the writer's own `Memtable::reader()` follows tails unchecked. That process wrote them, before the node's link, into chunks its pin keeps from reuse: the trust D194's process-local index had. A reader from `MemtableReader::open` checks every tail, in the writer process too (its `open` also gets a writer pin, so trust is never keyed on the pin). The split is a private flag set only by `reader()`, kept by clones and cursors, with no conversion from checked to trusted. A type parameter would have put both kinds through the engine's `MemSet` and its memtable sources, compiling the memtable cursor twice. On the trusted path M1 and P1 stay as debug assertions, so a `record_tail` bug still fails the tests. Callgrind: validating every tail in the writer cost hotrow's as-written state 1.2% per cell (1984 to 2007); with the trusted path it is 1987 (+0.2%, within the 0.5% threshold).

**Cost:** +4 arena bytes per node (about 3–4% of a ycsb node), against the writer's index of one `u32` per 32 arena bytes. Inserts write one more word; a jump reads the node's field instead of the index's directory and page. perf2 reads the callgrind shapes and the VM reader-vs-writer measurement on the PR.

**Tests:**
- `memtable` unit tests: a bad tail (past the arena, misaligned, inside the header, at a height-0 node, at another column, backward) is `Corrupt`; reused chunks never show an earlier memtable's tails, with tails recorded or not. These run under Miri too.
- The loom test of a skip with a concurrent writer, on the node field.
- `engine/tests/tail_index.rs`: D194's seeded equivalence, plus the same state read through a reader process (jumping and stepping equal, and equal to the writer's reads; the reader must jump).
- `shm/tests/sim.rs`: a layout 3 region (and a newer one) is refused while other processes are attached, and rebuilt when the new writer is alone.

<a id="d279"></a>
## D279 — A Buffered PREPARE votes once appended, before its group's write; a decision that overtakes it waits (owner decision, 2026-10-26; engine, #932 3c, #708)

**Decided:** #932's candidate 3c, allowed by the owner at `Buffered` only (`.coord/brief-932-pick.md`), in the form of the design on #932 (comment 6410103337) that perf287 approved (comment 6410157286).

**Why:** a participant voted from `settle`, after its group's write, the apply of every member, the view publish and the turn's freeze, index and flush-spawn work. The coordinator's DECISION waited for all of it. The WAL write is a synchronous `pwrite` on the shard thread, so the vote can leave as soon as the record is appended: the coordinator then writes its DECISION while the participant finishes its turn.

**What changes:**
- **The vote.** At the end of `run_group`'s append loop, each Buffered PREPARE member with a ticket votes `Prepared { error: None }` and is marked `voted`, when its group has no sync of its own (no `GroupSync` member, no `Sync` member). Groups with a sync vote from `settle` as before, so GroupSync's guarantee that every PREPARE is durable before its COMMIT is written stays. A failed append ends the turn before the vote.
- **A decision waits for its PREPARE to settle.** The shard thread handles messages only between turns, and a group with no sync resolves in its own turn. So a `Decide` reaches an unsettled share only when the group resolves through a sync: a held segment header (#19, D200). It parks on the `PreparedShare`, commit or abort alike, and `settle` runs it once the member settles. Groups settle in order, and members in their order, so parked decisions run in seqno order. The share counts down (D278) only then. A close waits for it: `try_finish_close` waits for `prepared` to drain, and the held header's sync is already submitted.
- **A voted member that settles `Err`** (the write, the view publish, the held sync): the shard is already poisoned. The share is removed unapplied and its record left undecided in the log (no `mark_aborted`: the coordinator may have decided commit, and a partly written record may hold the PREPARE). No second vote is sent: `send_vote` asserts that a member votes once. A parked decision, or one arriving later, finds the share gone and answers not applied, so a decided commit ends `OutcomeUnknown` at its coordinator (`on_applied`, #925's rule) and the engine dies; an aborted one is released as before.

**Why it is safe:**
- **An acknowledged commit has both PREPAREs and the DECISION written.** The client's `Ok` comes only from the last count-down (D278), each participant counts down only after its PREPARE settled `Ok` and its share applied, and the DECISION is written before any `Decide` is sent.
- **No torn commit after a failure.** A participant that voted and then failed never applies its share. The other shares are applied but held (`CROSS_HELD`), so they never become durable (`durable_seqno` includes the hold) and never freeze into an SST. Behind that, a PREPARE with a ticket raised its shard's `cross_end`, and a poisoned shard answers a peer flush's `SyncBarrier` with `cross_records_durable()`, false then (perf287's point). Replay is all or nothing for Buffered (a COMMIT whose participant lost its PREPARE is dropped whole), so the next open finds the commit whole or not at all, consistent with `OutcomeUnknown`.

**Tests:** `engine/tests/vote_on_append.rs`:
- a failed PREPARE write after the vote answers `OutcomeUnknown` (it answered `ShardFailed` before), shows nothing, holds back a peer flush, and reopens whole or empty (perf287's T1);
- a failed view publish after the vote (`Fault::NextGroupPublish`), the same route through `Corruption` (T2);
- a decision reaching a PREPARE under a held header waits for its sync (`Engine::parked_decisions` shows it is reached); a commit answered before its I/O completed survives a process crash.
Mutations caught: no early vote (all three), no parking (a commit answered `Ok` is lost by a process crash), a second vote (the coordinator's assert).

<a id="d278"></a>
## D278 — The participant that applies a cross-shard commit last releases its seqno and answers the client; the hold lives in the allocator (owner decision, 2026-10-26; engine, shm, #932 3a, #708; ICR 0074)

**Decided:** #932's candidate 3a, picked by the owner after M0 (`.coord/brief-932-pick.md`), in the form of the design on #932 (comment 6407200875) that perf287 approved.

**Why:** the coordinator held a decided seqno in its own `pending` watermark until every participant's `Applied` reached it, then woke the client. So the client's answer and every later commit's visibility waited for one more hop and one more turn on the coordinator (M0: the `Applied` hop 1.45 µs plus A's handling and wake). Under transfer's hot account, that tail is part of the hot row's cycle, which bounds throughput at `Buffered` (#932's pause study: hot-account utilization 0.70).

**What changes:**
- **The hold moves into the allocator.**
  - `Reservation` (D271's one Mutex) keeps the decided seqnos not yet applied everywhere. It publishes their minimum as shm `CROSS_HELD` (header offset 192, layout version 6), stored under the lock.
  - `reserve_decision` stores it before the `NEXT_SEQNO` mirror passes the seqno. The coordinator no longer publishes a lower bound first: the hold is stored in the same critical section that reserves the seqno.
  - The shard-local `held` set goes.
- **A shared count of the shares still to apply.**
  - When the coordinator's DECISION is written, a `Completion` takes the client's reply and travels in every `Decide`.
  - Each participant counts down (`AcqRel`) once its share is applied and in its published view.
  - The one that reaches zero releases the seqno (`Reservation::release`), wakes visibility waiters, and answers the client, before it sends `Applied`.
- **`Applied` is bookkeeping.** It still reaches the coordinator from every participant, for the `coord` entry, D249/D276's clean-up and the close.
- **Aborts don't change.** A seqno reserved and then aborted (a DECISION never logged, or not encodable) is released by the coordinator once every participant answered.
- **Readers** take `min(NEXT_SEQNO, CROSS_HELD, every pending) − 1`, loading `NEXT_SEQNO` first and `CROSS_HELD` second (perf287's M1, FORMAT §11.3).

**Failures:**
- **A share that can't apply never counts down.** That covers an apply that fails, a close that gave up, and a dead shard. The seqno stays held for good, as before, and the coordinator answers `OutcomeUnknown` from the failed `Applied` and treats it as a shard's death.
- **Exactly once by construction:** only the decrement that reaches zero releases, and it happens only once every share is applied. No death sentinel is needed.
- **The answer is taken exactly once.** A coordinator whose shard stops (`answer_unsettled`) takes it if no participant has, so the client hears `OutcomeUnknown`. A coordinator that dies after its own apply leaves the last participant to release the seqno and answer `Ok`, which is true: the decision was written, and every share is applied.

**Correctness:**
- **D263 (visible whole):** the release comes at zero. The `AcqRel` count-down carries every apply to the releaser, and its Release store under the lock carries them to readers.
- **The DECISION is written (and synced at its level) before any `Decide` is sent,** as before.
- **D266, recovery and the WAL format:** unchanged.

**Tests:**
- `engine/tests/applier_release.rs` covers:
  - the last participant answers without the coordinator running;
  - a participant that can't apply (`Fault::NextApply`);
  - a dropped participant applies its queued decision;
  - a coordinator that dies after its apply;
  - a transaction and a blind commit resubmitted on the same rows at once don't stall (perf287's point 1).
- `clock_loom.rs` models the hold, the release from either thread and the reader in both load orders. The wrong order is caught.
- `clock.rs` unit tests cover holds released out of order.
- `blocking_waits`' failed-share-apply test is unchanged and passes. freeze_wake's hold helper now makes shard 1 the last applier.

## D281 — Reader processes have a row cache, keyed on the writer's write watermarks in shared memory (owner decision, 2026-10-26; cache, shm, engine, pigeonhole, #926; refines D201)

**Decided:** a 1.0 item. Every reader process gets a row cache of its own, run on D201's epoch protocol against the writer's write watermarks, which move from the writer's memory into the shared-memory region (layout version 5). Design on #926, reviewed by perf287, with two must-fixes (M1, M2), both in.

**Why:** #397's measurement put a reader process at 0.09× the writer process on hot rows with the row cache on (the default): 48k against 545k reads per second per reader, with a p50 of about 20 µs against 1 µs. D277 closed the cache-off part of the gap only.

**How:**
- **One table, shared:** `row_epoch_slots` atomic `u64`s in the region (FORMAT §11.7), sized from the writer's row cache (one per 256 bytes of `max(row_cache, 64 MiB)`, a power of two, at least 4,096). The slot hash is specified: `xxh3_64_with_seed(seed + table, row) & (slots − 1)` (the table id added to the seed, wrapping, so the row is hashed in place), with the seed in the header, random per generation, and a hash id (1 = xxh3) an attaching process checks (perf287's must-fix on #947: a build-dependent hasher, `foldhash` over std's `Hash`, could put a reader and a writer of the same layout on different slots, and the reader would serve stale rows for ever). Golden vectors in `format_literals`.
- **The writer** raises a row's slot for every write, before the insert, when its own row cache is on (the default), and says so in the header (`ROW_EPOCHS_MAINTAINED`). A reader caches only when the flag is set (perf287: a reader caching against slots nobody raises would serve stale rows for ever). With the writer's cache off the commit path pays nothing: the always-on raise cost the cache-off commit shapes about 145 instructions a commit (1.5%), the case the owner's ruling kept the flag for. Its own cache uses the same table.
- **Per-process caches** of owned decoded rows: nothing points into the region, so a reader process's crash leaves nothing to repair and its cache pins nothing. Rejected: one cache in the region (a cross-process allocator, robust locks, shared eviction) and a per-process cache dropped whenever the view moves (useless under writes).
- **A reader's latest row read** takes its snapshot, loads the slot after it, and serves or reads and fills as the writer does. A fill is stored only while the snapshot still protects what it read (the seqlock-style check every reader read makes), so a row read from reclaimed memory is never stored.
- **Generations:** a reader's cache belongs to one region generation and is built empty at each attach. Every writer open builds a new generation, including D265's in-process recovery and the lock handover (both reopen through `open_writer`), so an entry filled against one generation's slots never meets another's (M2: the new slots start at zero, and a writer crash can lose visible `None` writes whose seqnos the next session reuses).
- **M1:** the table's offset, slot count and seed are another process's data. `ShmHeader::decode`, run at every attach, refuses a table that is misaligned, not a nonzero power of two, overlapping the reader slots or arenas, or outside the region.
- **Defaults:** 64 MiB per reader process, as the writer's (owner ruling), so N reader processes use up to N × 64 MiB, plus 16 bytes per slot for the cache's presence and admission tables (4 MiB at the defaults). The region grows by 8 bytes per slot (2 MiB). `ReaderOptions::row_cache*` and `PigeonholeReader::row_cache_stats` (ICR 0072).
- **Async reads** in a reader process (the latest point get and row read futures) go through its cache as the sync ones do (#946): a read future's attempt gets its read point's cache, and a fill passes the same snapshot check.

<a id="d286"></a>
## D286 — At Buffered, the coordinator sends `Decide` before its DECISION is written (engine, #708 trim 3d)

**Decided:** #708's trim 3d, in the form of the design on #708 (comment 6425876718), as perf287 reviewed it (comment 6425919769).

**Why:** after the last `Prepared` at Buffered, the coordinator's path was serial: queue the DECISION (~1.9 µs), write and settle its group (~2.2 µs), send `Decide` (~1.7–2.0 µs), then the hop (~2.1 µs) and the participant's apply (~1.5 µs) (#708 comments 6422171206 and 6425793394). The DECISION's write and the `Decide` hop and apply now overlap: ~3–4 µs off the commit, and off transfer's hot-row cycle, since the next transaction's snapshot waits for the release.

**What changes:**
- **The early `Decide`.** `on_prepared`, on a Buffered commit decision, builds the `Completion` with `remaining` = participants + 1 and sends `Decide { commit: true }` to every participant at once (`decide_early`, `Coord::decide_sent`). It then queues the DECISION as before. GroupSync and Sync keep the old order.
- **The DECISION's count-down** (`settle_early_decision`). A written DECISION counts down its one in its settle, not at the append. That is after the group's write and view publish, and after a held header's sync (D200), so the hold outlasts a header that could still keep the record out of the file. A DECISION appended and then failed in its write (`fail_all(.., true)`) has a ticket and commits, answered `OutcomeUnknown`, as before.
- **An early-decided DECISION that ends unlogged.** The coordinator was poisoned before the DECISION's group admitted it, or an earlier member's append failed in that group (`fail_all(.., false)`). It sends no abort `Decide` after the commit one and doesn't `mark_aborted`. It leaves its count, so the seqno stays held and the commit is never visible, and it answers `OutcomeUnknown`. The coordinator's `finish_applied` then treats it as a shard's death (`not_applied`), as for a share that can't apply. On reopen there is no COMMIT, so the PREPAREs are dropped, consistent with `OutcomeUnknown`.
- **What ends the hold's visibility stall** on a poisoned coordinator: inside the failed instance, `shard_died` ends the visibility waits; the poison records the instance's failure, and with automatic recovery the supervisor swaps in a reopened engine (D265), where the commit is absent and later commits are visible. Without recovery the instance stays failed, as for any poison.
- **`Applied` can now arrive before the DECISION settles.** `on_applied` keeps the coordinator's entry until both are in (`Coord::decision_settled`, `finish_applied`).
- **Early `release_overlaps` and `doom_overlaps`** (D249, D276) on a commit that then ends unlogged only make waiters retry against a write that never lands: liveness, not correctness.

**Why it is safe:** the seqno stays in the allocator's held set (`CROSS_HELD`, D278) until `remaining` reaches zero, and one count is the DECISION's. So a commit visible or answered `Ok` always has its DECISION written (in the page cache at Buffered, so it survives a process crash), as before. A held share is never durable and never reaches an SST (D279's argument).

**Tests:**
- `engine/tests/early_decide.rs` (`Fault::HoldDecisionRecords` keeps the DECISION unwritten while the participants apply; `Engine::abort_decides_sent`):
  - nothing visible or answered until the DECISION is written, then committed and surviving a process crash;
  - perf287's path 1: the coordinator poisoned by an unrelated single's failed write before the DECISION's group: no abort `Decide`, `OutcomeUnknown`, nothing visible, absent after a reopen;
  - path 2: an earlier member's append failing in the DECISION's group: the same;
  - a failed write after the append: committed (visible) and answered `OutcomeUnknown`, as before.
  
  Mutations caught: no +1 (all four fail); the unlogged DECISION taking the old abort path (paths 1 and 2 fail).
- `pigeonhole` `recovery.rs`: the poisoned coordinator's held commit ends with the recovery swap; the commit is absent and a later commit on the other shard is visible.
- `clock_loom.rs`: the coordinator's two count-downs (its share and its DECISION) racing the participant's; release exactly once, never visible before the DECISION is written. The model without the DECISION's count panics.
- The D279 parking test (`vote_on_append.rs`) and D278's (`applier_release.rs`) pass unchanged; the early `Decide` reaches a held header's PREPARE and parks.

<a id="d287"></a>
## D287 — A counter family's flush combines its memtable's operands, guarded by the slot's other sources (perf287; engine, #713, D280; refines D186)

**Decided:** a flush of a counter family passes `GcPolicy::other_sources`, so D186's operand combining applies to the one frozen memtable a flush writes. Before, `flush_gc` left it `None`, which D186 reads as "unknown, no combining": every `incr` reached L0 as its own entry, and a hot counter's read folded every operand since its last compaction. That's the counter-read tail of #713: on the AX102, 62–66% of a slow read's fold was in L0 SSTs.

**Why it's safe:** D186 combines two operands only if no other source of the slot can hold a delete with a seqno between theirs. A frozen memtable M spans seqnos `[lo, hi]`, and every other source of its slot lies entirely below `lo` or above `hi`:
1. **Older sources (the slot's SSTs, older frozen memtables) are below.** A memtable freezes only once all it holds is durable (D227), so no lower seqno lands in a newer memtable. That holds on every path that queues a flush: size and forced freezes (moves, WAL unpins, `all`), room-wait freezes (D124, `freeze_with::<true>`), and the close's in-place flush (#111, after the same deferral). A moved or merged tablet's SSTs were flushed before the change.
2. **Newer memtables are above,** by the same rule.
3. **A cross-shard share not applied here yet lands above.** Once decided, its apply seqno is held (`CROSS_HELD`, D278), and `durable_seqno` stays below it; M froze at or below `durable_seqno`. Undecided, its apply seqno isn't reserved yet. So prepared shares aren't listed: their commit id (D263) is never applied.
0. **Open and replay** flush recovered memtables without GC (`write_memtable`), including a replay that spills and the open's flush after a spill or a layout change, so they combine nothing. A recovered memtable kept by a same-layout open holds only its slot's records above the slot's flushed-through, with the allocator seeded above them, so its later flush meets 1–3, though replay itself isn't in seqno order.

**Read points:** the flush's GC takes them when it runs: live snapshots, durable snapshots, and the oldest reader process's pin. One created after M was queued (a snapshot, or a reader process pinning a view) reads at `visible_seqno` or `durable_seqno`, both ≥ `hi`, so it never splits M's operands. One released meanwhile only merges stripes nobody reads.

**How:**
- The shard records the slot's other sources in the `FlushItem` when it queues a counter family's flush (`counter_sources`, in `freeze_with` and `requeue_frozen`): the slot's SSTs with their key and seqno ranges, and its other memtables (exact ranges for frozen ones, unbounded above for the active one). Sources starting above `hi` are left out.
- Real ranges rather than an empty list: the GC then checks the argument on every pair, and simply doesn't combine across an overlap. Debug builds assert there is none.
- D187's bottommost purge is untouched: a flush is never bottommost.

**Not in D287:** the active memtable's share (34–38% of a slow read's fold) is a follow-up, which needs its own design.

**Tests:** `engine/tests/flush_fold.rs`:
- one counter, one combined operand;
- a snapshot splitting the run;
- a delete between operands, every read point unchanged;
- consecutive flushes;
- a source inside the memtable's range keeps every operand (a test hook, `FlushGcMutation::CounterSourceInside`);
- a decided share held while the counter slot's freeze defers, then the flush combining it and a reopen;
- a share decided after a flush landing above it;
- a replay that spills, then a D287 flush and a compaction.

With the old `None`, 4 of the combining tests fail. The `counters` model-check targets cover the rest under sweeps, including shard-count changes across reopens.

<a id="d288"></a>
## D288 — A filtered scan's sources settle only up to the merge's frontier (2026-10-27; compaction, engine, format, sst, #711; refines D22)

**Decided:** design on #711 (comment 6433512006), reviewed by perf287 (comment 6433582625) with two must-fixes, both in. A filtered source under a merge keeps a key that is only a lower bound on its next admitted entry, and the merge settles only its top, up to the next source's key. PR 1 covers memtable sources and the merge protocol; PR 2 covers `SstIter` and `LevelIter`.

**Why:** D22 filters each source on its own, and each source's settle looped until its next admitted entry. A source with nothing admissible near the scan's position therefore walked ahead of the merge, through every later row of the tablet, while other sources supplied the rows the scan returned. On time-series-range (#711), each scan's memtable source walked every later entity's newer points: 23K filter checks per scan against RocksDB's 6.9K steps, about 85% of client CPU. A filtered row read could walk the same way past its row.

**How:**
- **`Cursor`** gains three provided methods: `settled()` (default true), `settle_until(bound)` (default no-op) and `may_park()` (default false). `settle_until` parks only on an entry strictly greater than `bound`; an entry equal to it is checked (perf287's must-fix: a source parked on the runner's key with the lower index would otherwise stay on top unsettled for ever).
- **`FilteredCursor::lazy`:** a move lands without filtering, and `settle_until` runs D22's verdict loop up to the bound. An unfiltered cursor built lazy stays eager, since it has nothing to settle.
- **`MergingCursor`:** heap keys are lower bounds. After every move, through every exit (the `rebuild` paths, the runner swap, the top that stays, and the pop of an exhausted top), the merge settles its top up to the runner's key until the top is settled or the heap is empty (perf287's must-fix). Each round settles the top, exhausts it, or moves it strictly past the runner's key, so the rounds end.
- **A `lazy` flag on the merge,** derived from its sources (`may_park`) at every heap rebuild, so it can't be missed (perf287's nit) and compaction, point gets and unfiltered scans test one bool per move (perf287's should-fix).
- **A lazy source checks the entry it lands on once,** as an eager one does: an admitted landing settles it at once, and a rejected one parks with its verdict kept, so settling doesn't check that entry again.
- **The engine** builds every memtable source lazy (`mem_sources_from`). A filtered scan's lane and a filtered row read set their merge lazy.
- **Results are unchanged by construction:** the filter is a pure function of the key, and snapshots, deletes and TTL are resolved above the merge (`CellResolver`).
- **PR 2 (planned):**
  - `SstIter` parks on the index separator without loading the next block, and `LevelIter` on the next file's `smallest_key` without opening it.
  - The async read-ahead (`upcoming(1)`) predicts from the merge's top only.
  - These are perf287's points; a parked source that pays a block read per tablet open would keep most of the SST cost.

**Tests:**
- `compaction/tests/lazy.rs`:
  - a lazy merge against the eager one, over random sources (equal keys across sources included), filters and movement sequences;
  - an error partway through a settle, then a seek;
  - equal keys across lazy sources (this hangs if a source parks on its bound);
  - every movement method, `skip_column` returning true included (perf287's should-fix);
  - the #711 shape: a source with nothing admissible moves at most once past the merge's frontier, where eagerly it walks all 1,050 entries.
- The pigeonhole model test adds a qualifier-prefix scan after each scan, checked against the model's answer restricted to the prefix. It draws no randomness, but fewer reads shift the shared sim fault RNG, so older failing seeds may not replay.
- `readshapes`' `scan-interleaved` shape: a memtable source that parks once per row between an SST's admitted entries, the regime where lazy costs one sift per park.

**Follow-up:** a lazy source left alone in the merge still settles to its end, past a row read's or a bounded scan's upper bound. The merge could settle up to the resolver's upper bound instead. That was not in the reviewed design, so it comes after PR 2.

<a id="d289"></a>
## D289 — Engine-owned shards run on a load-sized set of pinned driver threads (owner decision, 2026-10-27; runtime, engine, #760, #759; ICR 0079; amends D198, D235)

**Decided:** option (a) of the shard-threading note (#760, perf287's revision 2, comment 6434752774; owner, comment 6441951893).
- **Drivers:** k pinned driver threads run the n shards' turns. Shards and their data are unchanged.
- **PR 1:** a fixed k (`shard_threads`, default 0: a thread per shard, as before), plus the inventory of calls that can block inside a shard's turn (below).
- **PR 1's fixed-k A/B** (#997, comment 6446578729; AX102, 16 shards) found no fixed default; the default stays a thread per shard (coordinator, 2026-10-27).
  - **Light load (sparse-wide buffered):** k = 1–2 put p99 at 34–38 µs against 395–553 µs at k = 16, at flat ops/s.
  - **Heavy shapes lose throughput as k shrinks.** At k = 4: ycsb-f −29%, transfer −18%, ycsb-a −9%. At k = 1: −66–70%. At k = 8: −8% and −1.5%.
  - **Ring waits** end 67–78% of ycsb-f's driver passes at every k, including 16. Spinning with non-blocking reaps while I/O is in flight removes them (+6–11% ops at k ≤ 4).
  - **No-yield:** D198's yield removed at the driver level reproduced #996's trade, and stays out.
- **PR 2:** the load controller, moves at quiet points, the spin through in-flight I/O, and the owner's CPU budget (ICR 0081). Design on #760 (comments 6446658819, 6446737262 and 6446985810), reviewed by simp724 (6446778188). Below: "How the controller runs" and "The CPU budget".
- **Client combining** (D198's C2) stays on hold.
- **D198's yield stays.** The first step was to stop a pinned shard thread's spin from yielding (#996). On 16 per-shard threads that moved the tail rather than removing it, and #996 closed (comments 6442595313 and 6443366250).
  - **Never yielding:** sparse-wide put p99 fell 60%, but ycsb-f's p99.9 rose 28–45%: clients woken onto a spinning shard's CPU waited out the window.
  - **A 50 µs no-yield prefix:** it kept only an 11% put gain.
  - **Next:** yield against no-yield is one arm of PR 1's fixed-k A/B, where only k threads spin.
- **Unpinned shard threads** come back to the owner only if a second #760 run and cli692's ycsb-f check agree. On ycsb-f (#759 round 5), unpinned shard threads raised row-read p99 by 8% and p99.9 by 18%, and cut ops/s by 3.5%.

**Why:** #760's put p99 (444–791 µs against a 20.3 µs floor) is queue wait for a parked shard thread whose pinned core a saturating client holds. The wakeup reaches the CPU after one EEVDF slice (p50 987 µs on Linux 6.8).
- Fewer, busier threads park less. 8 shards gave 27–28 µs at flat ops/s, but they hold half the memtable arena.
- A 2 ms spin reached the floor, but cost 5% of ops/s and about 5 cores.

**How a driver runs (PR 1):** shard `i` runs on driver `i % k`. Each pass runs every held shard's turn in order, each up to its 500 µs slice. While own-ring I/O of any of them is in flight, the driver waits on its ring. Otherwise it spins once for all of them (D198), and then it parks.
- **Wakes:** every held shard's wake target is the driver thread. Each shard announces sleep at the end of its own turn, so a message sent after that wakes the driver.
- **Busy accounting (D256):** each turn's time goes to its shard. The spin's time is split evenly over the shards that handled a message since the last park.
- **Shutdown** returns the handlers in shard order.
- **Names:** with a thread per shard (`shard_threads` 0, or at least the shard count) the threads keep the names `pigeonhole-shard-{i}`. Otherwise every driver is `pigeonhole-driver-{d}`.
- **Placement is round-robin, not blocks of n/k:** packing (D217) keeps shards `0..active` busy, and round-robin spreads them over the drivers where blocks would put them all on driver 0.

**How the controller runs (PR 2):** `RuntimeConfig::adaptive_drivers`. In `pigeonhole` it's **opt-in**: `Options::adaptive_shard_threads(true)`, for engine-owned opens without a fixed `shard_threads`. By default, engine-owned keeps one thread per shard, as before D289.
- **Why opt-in (the owner, 2026-10-28, #1006):** six box rounds showed the mechanism sound but the resize policy noisy across load phases.
  - **Round 5** (3f83e186): transfer, ycsb-a and ycsb-f level with main; sparse-wide put p99 51/350 µs against 236/489.
  - **Round 6** (ec922c8c, after round 5's fixes): ycsb-a −5% and sparse-wide's tail gain lost. The slow-pass rule grew the set on idle windows between a load and a run, ycsb-a at 8 drivers sat just under `GROW_BUSY` with no signal, and sparse-wide's warm-up sibling wait (0.2–0.7) exceeded its steady-phase value (0.04–0.08).
  - Each round fixed one phase-dependent edge and exposed another.
  - **The follow-up is #1015:** per-phase signal traces, then rules that hold across phases, then default-on once two consecutive unchanged rounds are level.
- **Without the controller,** `force_driver_target(None)` (tests, the bench) returns the target to the drivers started rather than leaving it where it was forced.
- **Start:** all the ceiling's drivers start pinned, and the shards start one per driver (target = ceiling). A heavy load never starts short of threads, and a light one shrinks within seconds. Shard `i` belongs to driver `i % target`.
- **The controller** runs between passes on whichever driver first finds its 100 ms window over (a try-lock, so no driver waits for it; one Relaxed load per pass otherwise). Not only driver 0: driver 0 parks like any driver, and with the load on another driver's shards nothing would grow the set (simp724's review of #1006, `a_load_on_another_drivers_shards_grows_the_set`). A window longer than 200 ms ran with every driver parked, so it is dropped rather than averaged in, where its diluted busy would read as a light load. It reads each driver's counters (Relaxed): busy (turn time, excluding spin), passes, slow passes (over 100 µs), sibling wait and ops (messages and slices), plus every shard's producer wakes. Only drivers below the target count towards busy, slow passes and sibling wait, so during a shrink's moves the misplaced shards' time is missed for a window or two.
  - **Grow** (doubling the target, up to the ceiling: a driver's busy saturates at 1 and can't say how many more the work needs; stepping up by one left transfer at 2–12 of 16 drivers through its measured phase, −10% in #1006's second A/B) when, averaged over 10 windows, the busiest driver's awake-busy is above 0.75, or more than 1% of a driver's passes run over 100 µs, or the active drivers' sibling wait is more than 0.15 of their busy (both summed over the drivers and the 10 windows, with at least 100 ops per window: a nearly idle window's ratio, 1.1–2.2 at 0.1% busy between a bench's load and its run, grew ycsb-a and sparse-wide in #1006's fifth A/B when windows were averaged). A pass bounds how long a message waits for its shard's turn: #997's heavy rows at one driver had pass p99s of 128–256 µs.
  - **Sibling wait** is, for each turn that handled work, the time from its pass's start to the turn's start: how long that work waited behind other shards' turns (at most: work that arrived mid-pass waited less). It is zero for a lone shard. It sums waits that pass length misses: transfer at two drivers was neither busy nor slow, but each commit's several hops waited behind sibling turns, and it lost 6% (#1006's fourth A/B). Over busy, it says how long messages queue relative to how long they're served, under a closed or an open loop alike (simp724's design note on #1006, comment 6455726831). It replaced the 100 µs pass rule's blind spot without a throughput probe.
  - **The threshold, from #1006's fixed-k round** (AX102, 2026-10-28, two reps; `--shard-threads k`, the signal logged over the measured phase). The line falls between the loads that gain from another doubling and sparse-wide at 2, where its tail gain is. Summed over the drivers rather than the highest driver's ratio: a nearly idle driver's ratio is noise (0.57–0.64 for sparse-wide at 8, at 2% busy).

    | shape | drivers | Σwait/Σbusy | ops/s | the next doubling |
    |---|--:|--:|--:|--:|
    | transfer | 2 | 1.57–1.62 | 90–95K | +50% |
    | transfer | 4 | 0.54–0.55 | 137–139K | +22% |
    | transfer | 8 | 0.09–0.10 | 169K | +2% (stays at 8) |
    | transfer | 16 | 0.00 | 172–173K | |
    | ycsb-f | 8 | 0.23–0.27 | 307–311K | +5% |
    | ycsb-f | 16 | 0.00 | 325–326K | |
    | sparse-wide | 1 | 0.16–0.19 | 95–98K | grows to 2 |
    | sparse-wide | 2 | 0.04–0.08 | 93–98K | (stays) |
    | sparse-wide | 4–16 | ≤ 0.02 | 89–99K | |
  - **Shrink** straight to the drivers the work needs, `ceil(Σbusy / 0.5)` and at least one, when over 50 windows producer wakes per op are above 0.05, that count is below the target, and no driver has slow passes. Stepping down one at a time took a light store about 100 s from 16 to 2, past the bench's warm-up and most of its measured phase (#1006's first A/B, comment 6451990090).
  - **No shrink without load:** at least 100 ops per window on average. An idle store's drivers park and cost nothing, but shrinking one in a gap between bursts left the next burst regrowing through its first seconds (#1006's third A/B: every heavy run started 16 → 2).
  - **Shrink regret:** a shrink after which the next second's ops per window fall more than 3% below those that led to it is backed up by doubling (not all the way: one driver cost sparse-wide a few percent, and going back to 16 lost its tail gain, #1006's fourth A/B), and the next shrink waits twice as long. The work needed the drivers after all: ycsb-f went 16 → 8 → 16 at −3%, and loses 8% at a fixed 8.
  - **A proven level is the shrink's floor:** after a regret backs up to k, no shrink goes below k until ops per window fall under half the level the regret was measured against. A 3% band was too tight: sparse-wide's ops per window drift down 10–40% between resizes as its data grows, which freed the floor every time (2 → 1 → 2 three times a run in #1006's fifth A/B). Otherwise the shrink rule that fired before fires again at k, and the set shrinks, regrets and doubles back over and over, damped only by the doubled shrink windows (simp724's review). A load that halved may need fewer drivers; one that grew never shrinks anyway.
  - **No throughput probe:** a periodic try at twice the drivers, kept if ops rise, was proposed (#1006, comment 6454108154) and dropped for sibling wait. Under a fixed offered rate the queueing shows only as latency, so a probe would be undone while the cost stayed, and a 3% gain test is inside the box's run-to-run noise. Regret remains the backstop for costs no signal sees.
  - **Damping:** no resize within 2 s of another. A shrink that a grow undoes within 60 s doubles the next shrink's windows, up to 800 (as D255).
- **Moves happen at quiet points.** A driver hands a shard that belongs elsewhere to its owner only between turns, and only with its own ring quiet (nothing of the thread's in flight).
  - **Why not `release()`:** the design moved a shard through `release()`, which drives the thread's ring to quiet inside the move. On a driver that ring is shared by all its shards, so the wait would stall the shards that stay. A quiet point costs them nothing; the price is that a move may wait. #1006's fifth A/B measured it: over 47 resizes across four shapes, the slowest driver held only its own shards within 212 µs and 6 passes of seeing the new target (ycsb-f, with in-flight I/O); every other resize moved within the pass that saw it. So moves need no drain fallback (simp724's should-fix 1).
  - A moved shard's I/O completes only on the ring that submitted it (#402). So nothing is left on the old ring, and no wait inside a turn comes to depend on another driver's ring (#1002, fixed by #1003).
  - The source takes the shard out of its list, so it announces no further sleep for it. It pushes the shard onto the adopter's handoff queue, then wakes the adopter.
  - The adopter drains its queue at the start of each pass, points the shard's wakeups at itself, and runs the shard's turn before it next parks. A producer that read the old target woke the source, and the adopter's turn finds its message (the loom model `loom_a_message_racing_a_driver_move_is_found_by_the_adopter`).
- **No steals:** the design had moves and steals (an idle driver taking a busy one's shard). PR 2 has moves only. A steal breaks `i % target` placement, so the target alone would no longer say where a shard runs, and settling and the controller's per-driver view would need a placement table. The A/Bs showed no imbalance below the ceiling that a steal would fix: a busy driver grows the set, and at the ceiling every shard has its own thread. Steals come back only if a shape shows one driver saturated while others idle at the same target.
- **Shutdown:** `Runtime::stop` closes the drivers before the inboxes: no move after that. A driver holding no shard exits once its handoff queue is empty, so a shard is never handed to a driver that has gone.
- **Spin through in-flight I/O:** with own-ring I/O in flight, a driver spins first, polling its queues and reaping without blocking every 64 polls, before it waits on its ring. In #997's A/B, ring waits ended 67–78% of ycsb-f's passes, and this arm gained 6–11% ops at k ≤ 4.
- **A cross-ring wait's resolver** wakes the waiter's ring wait (simp724's note on #1003), so the wait ends when the operation completes rather than at its 1 ms slice. `cross_ring_wait_slices` counts the slices that still run out.
- **Bench and tests:** `driver_target()` / `drivers_settled()` (engine and `pigeonhole`, doc-hidden) report the target and whether every shard is on its driver; the bench's warm-up does not end while either moves, and records `drivers`. `force_driver_target` fixes the target for tests.

**The CPU budget (ICR 0081; owner, 2026-10-27, #760 comments 6443979201, 6444135824 and 6446722456):** one function sizes the engine-owned threads from the physical cores the process may use, after its affinity mask and a container's CPU quota (`pigeonhole_io::sys::default_shards`).
- **Shards:** one per core, as before (D220).
- **The driver count:** a thread per shard by default (one per core with the default shard count). With the controller opted in, that is its ceiling: under full load a store keeps a thread per shard's throughput (#997's A/B: ycsb-f −8% at 8 of 16), and the controller shrinks a light one.
- **The profiles differ in the compaction share** (`Options::cpu_profile`). These are placeholders until PR 2's compaction-share arms (c = 2, 4, 8 at 16 cores):
  - `Embedded` (the default) gives flush and compaction a quarter of the cores, leaving the application the rest and every SMT sibling;
  - `Server` (the clients are elsewhere: another container, IPC, the network) gives them half.
  - Both scale with the cores (D252's fixed cap of four is gone), at most one per shard, since a shard runs one compaction at a time.
- **Explicit settings win:** `shard_threads(k)` fixes the drivers (the controller off), and `compaction_cores(c)` is kept as given.
- **Application-owned mode** starts no threads (D40).

| Physical cores (after limits) | Shards | Shard threads (the controller's ceiling) | Embedded compaction | Server compaction |
|--:|--:|--:|--:|--:|
| 1 | 1 | 1 | 1 | 1 |
| 2 | 2 | 2 | 1 | 1 |
| 4 | 4 | 4 | 1 | 2 |
| 8 | 8 | 8 | 2 | 4 |
| **16 (AX102)** | 16 | 16 | 4 | 8 |
| 32 | 32 | 32 | 8 | 16 |
| 64 | 64 | 64 | 16 | 32 |
| 128 | 128 | 128 | 32 | 64 |

The AX102 row is measured: 16 drivers is today's default, and 4 compaction threads D252's. The larger rows are reasoned, since there's no larger box. Compaction work scales with the write rate the drivers carry, and a shard compacts one job at a time, so the share stays useful up to one per shard.

**The invariant: no wait inside a turn may depend on another shard's turn.** A wait inside a turn may depend only on I/O the thread reaps itself, or on other threads. A driver blocked inside shard A's turn runs no sibling, so a wait that needed shard B's turn would deadlock whenever B shares the driver.
- **Precedent:** application-owned mode already lets one thread drive several shards (the guide's async chapter), so the engine already keeps this.
- **simp724 checked the in-turn waits** (#997 review): the WAL's `reap_for_wait`, the rollover and slot syncs, and pager growth all wait on I/O and reap the thread's ring, so a sibling's I/O still completes. The manifest writer's `commit_req_from_thread`, which finishes a pump's root commit itself, runs only from application threads.
- **New code** that waits inside a turn must keep this. PR 2's moves don't lift it, since they happen only at turn boundaries.

**Calls that can block a driver inside a turn (PR 1's inventory).** While one shard's turn blocks, its siblings on that driver wait. Found by reading the code at 2ac77f42. Durations are estimates. The defaults have flush and compaction on D252's compaction threads, off the shards.
- **Every durable group:**
  - The WAL group write (`wal/src/stream.rs` `write_buf`) is a synchronous `pwrite` into the page cache. That's microseconds, or milliseconds under dirty-page throttling.
- **Some turns:**
  - A manifest pump slice writes its delta synchronously (`manifest.rs` `append_delta`). Its `reclaim` takes the pager's allocator lock, which another thread may hold across file growth.
  - A conditional write's evaluation reads SST blocks synchronously on a cache miss (`shard.rs` `read_latest`).
- **Rare, in milliseconds:**
  - **WAL rollover with no spare segment:** an inline `durable_sync` (`stream.rs` `rollover`), when a group is larger than the segment's last quarter or a spare's preparation failed. The WAL holds a group back before that otherwise (`wal_held_back`, which doesn't block).
  - **Growing a WAL slot:** fallocate plus `sync_all` (`take_slot`).
  - **A rollover while the previous header's sync is still in flight** (`rollover_waits`).
  - **A spare segment's fallocate,** once per segment.
  - **Pager file growth:** fallocate plus `sync_all` under the allocator lock (`pager/src/lib.rs` `allocate_from`).
  - **io_uring EAGAIN backoff.**
- **With `compaction_threads = 0`** (the engine crate's own default, and application-owned mode without workers), flush and compaction slices run on the shard and do synchronous SST writes and reads too.
- **Close and open only:** the final sync and the final close's root commit, and the open's file extension and syncs.

PR 1 changes none of these. PR 2's moves take a blocked driver's other shards only at its turn boundary, so the box A/B's per-turn time decides whether any of these needs to yield to siblings instead.

**Tests:**
- `runtime/tests/modes.rs` runs the engine-owned suite with every shard on one driver and with two: echo, FIFO, cross-shard forwarding, background tasks, wakes, sleeping tasks, latency bounds, shutdown and lost-wakeup round trips.
- `drivers_hold_shards_round_robin_and_handlers_return_in_order` checks placement, thread names and the handler order.
- `a_driver_charges_its_spin_to_the_shards_that_worked` checks the spin split. Charging every shard fails it.
- `pigeonhole/tests/model.rs` takes `PIGEONHOLE_SHARD_THREADS=k`. Sweeps run it with 1 and 2.
- **PR 2:**
  - `modes.rs` runs the engine-owned suite with the target forced through 1, 2, 3 and back every millisecond, plus `shards_move_between_drivers_under_traffic` (300 resize rounds, more than 100 moves, every reply and the handler order kept).
  - `driver_ring.rs` moves shards with io_uring reads in flight.
  - The loom model above.
  - `a_load_on_another_drivers_shards_grows_the_set`: at two drivers with the load on shard 1 alone, driver 0 parks and driver 1's passes grow the set (it fails with the controller on driver 0 only).
  - The controller's unit tests: the shrink in one step, growth on busy, slow passes and sibling wait, no shrink of an idle store, regret, and a proven level flooring the shrink until the load changes.
  - `pigeonhole/tests/model.rs` takes `PIGEONHOLE_DRIVER_CHURN=1`, forcing seeded resizes after one step in ten.

<a id="d291"></a>
## D291 — A counter get that folds many operands keeps its result as a base in the active memtable (ctr274, 2026-10-27; compaction, engine, pigeonhole, #713, D280; refines D186, D287)
**Why.** After D287 a hot counter's read still folded every operand in the active memtable, about 26 ns each. On the AX102 the counter-read p99 was 4.0–4.5 ms against RocksDB's 1.5–1.8 ms (#992's A/B). Locally a read over 1 ms folded 2^15–2^18 memtable operands, and only about 1.5 SST entries (#713 comment 6456964876). Cheaper steps can't bound a fold that grows with skew and memtable size, so the fold itself has to be bounded.

**(a) A counter group read for its newest version ends at its base** (`CellResolver::group`).
- **The condition:** `versions == 1` and no time range (point gets, newest-version row reads and scans).
- **Why it's safe:** below the base are only older operands, which the base already holds, and older deletes, which in a counter family hide only entries older than themselves (`cover_seqno`, D186).
- **What it skips:** the resolver seeks past the column. The walk it skips had only one effect, `col_cover` for older timestamp groups, which such a read never returns.
- **Kept:** multi-version and time-range reads still walk the whole group.

**(b) The checkpoint.**
- **When:** a writer-process point get of a counter family, sync or in a transaction, folds at least `EngineOptions::counter_checkpoint_steps` entries (default 128; 0 turns it off; `Options::counter_checkpoint_steps`, doc-hidden).
- **What the get does:** it sends the row's shard `ShardMsg::CounterCheckpoint`, holding its result and the seqno `s_top` of the newest entry its fold read (`CellResolver::counter_fold`).
- **What the shard does:** it inserts `Put` at `(column, ts, s_top)` with that value into its active memtable.
  - It isn't logged and raises no row epoch.
  - It reserves and waits for no room and counts as no write load.
  - It's dropped unless all of these hold:
    - the shard is open, not replaying and not poisoned;
    - the row still routes to this shard and tablet;
    - the family is still the same table's counter family;
    - two arena chunks are free beyond what admitted writes reserved;
    - the slot is open and below its freeze threshold;
    - **the entry at `(column, ts, s_top)` is in the active memtable, and isn't a `Put`.**
  - Counters: `Engine::counter_checkpoints` (kept, dropped).

**Why no read can tell.**
- `v(s_top) == v(S)`. `S` was visible, so the read saw every entry of the column at or below `S` (D201's premise), and none lies in `(s_top, S]`.
- `Put` (0x01) sorts before every other kind at the same `(ts, seqno)` (FORMAT §2). A read at `T >= s_top` folds what is newer onto the base and stops (a). A read below `s_top` never sees it.
- **Deletes:** a delete at or below `s_top` is already folded into the value. One above it hides the base as it hides every older entry. This is why checkpoints are for counter families only: a standard family's cell delete hides its timestamp whatever the seqno (FORMAT §2).
- **The operator:** counter families always use the built-in `i64` add. It's associative, and its result is a valid base. Another operator would need both properties.
- **Transactions:** `Txn::get` reads the snapshot with no overlay of its own writes, so a checkpoint never holds an uncommitted operand. A read-your-writes overlay must not checkpoint.
- **D275:** at `s_top` rather than `S`, a checkpoint lies above a transaction's snapshot only when a real operand at `s_top` does, so it adds no conflict (perf287's must-fix in the review, #713 comment 6457133257).
- **The row caches** (D201, D281): no value changes at any seqno, so no epoch is raised.
- **Reader processes:** they read the base as an ordinary node of the writer's memtable. Their own gets don't checkpoint: they have no shards to send to.

**Why the guard is the entry at `s_top`.**
- **The memtable's range is unchanged:** the active memtable already holds `s_top`. So D287's disjoint ranges hold, and D227's freeze point doesn't move.
- **Rotation races drop the checkpoint:** a read whose world changed before the message arrived (the memtable rotated, flushed or moved, or the active memtable is now empty) finds its entry gone, and the checkpoint is dropped. The insert and the guard run on the shard thread, as freezes and tablet changes do.

**Flush, compaction and durability need no change.** A `Put` under operands is a base: D287's flush and D186's GC fold the operands above it onto it per stripe. Every seqno of a frozen memtable is durable before it freezes (D227), so an SST's checkpoint never holds a lost increment, and replay rebuilds a memtable's operands without it.

**The memtable.** A checkpoint is the first insert that lands inside a column's run rather than at its front. Its stale tail (D194, D277) is its successor's, a later node of the same column. That's valid for a skip that finishes the column, in the writer's process and for a reader that checks every tail.

**Where it runs.** Every point get, sync or async, goes through `get_in`, whose closure takes the resolver's fold record after the get (only counter groups set it). A fold record hands the result to a cold function that sends it through the view's sink. The sink is in the writer's `LiveViews`, reached through the view's pin, and is absent in reader processes and forked children. `get_with` itself is unchanged: a checkpoint argument there changed what the compiler inlined into every point get (+1–3% instructions, D193).

**Cost (CI x86 callgrind, D193).** The resolver reads counter groups in an instance of their own (`group::<true>`). `locate_key`, `common_prefix_len` and `split_suffix` are always inlined: with two instances the compiler stopped inlining them, which cost hot-row reads about 14 instructions a cell. A counter group records its fold only when it reaches `COUNTER_FOLD_MIN`, the shortest `counter_checkpoint_steps` any open engine set, so short folds take no cold call. Net against the base: hot-row and `versions` −0.2% to −0.4%, and `counter` +0.49%, the fold accounting itself. `counter`'s threshold is 0.8% under D195.

**Not covered yet:** row reads of counters. They read through the same resolver, so (a) applies, but they don't send checkpoints. A write-side checkpoint for counters that are written heavily but rarely read is a follow-up, only if the gate's p99.9 shows it matters.

**Measured** (local, `counters --scale full --threads 16`, buffered): counter-read p99 2.5–3.9 ms → 33 µs, p99.9 4–5 ms → 116–161 µs, and throughput 150K → 210–290K ops/s. The box A/B against main follows on the PR.

**Tests:**
- **compaction:**
  - `counter_checkpoints_change_no_read`: random counter histories with checkpoints at random read points, every read the harness makes, against the model;
  - `compaction_preserves_reads_with_counter_checkpoints`: leveled, tiered, and with rows split across another source.
- **memtable:** `a_node_inserted_inside_a_columns_run_keeps_the_stale_tails_valid`, through the writer's reader and one from shared memory.
- **engine `counter_checkpoints.rs`:**
  - kept, with reads above, below and across a flush;
  - dropped when its entry left the active memtable (empty, or holding newer entries);
  - a transaction between `s_top` and the read point commits without `Conflict`.
- **pigeonhole:** `PIGEONHOLE_CHECKPOINT_STEPS=1` runs the model check with a checkpoint after nearly every counter read, under flushes, compactions, tablet changes and reopens.
