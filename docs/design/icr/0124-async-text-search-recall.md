# 0124: async text search and memory recall (#1560)

**Status:** Approved (number from the coordinator, 2026-11-05). The design note is on #1560 (issuecomment-6669876325), agreed by perf287 (text hot path, T1–T3) and ai1249 (memory and bindings, §10).

## The change
- **pigeonhole-text** (semver, additive, the `async` feature):
  - `Search::run_async(self) -> Result<Vec<Hit>>`;
  - `Search::run_with_stats_async(self) -> Result<(Vec<Hit>, SearchStats)>`.
  
  Both are `pub async fn`, with `Send` futures, and have the same refusals, hits and stats as `run`.
- **pigeonhole-memory** (semver, additive): `Recall::run_async` and `Recall::run_with_stats_async`, with the same contract as `run`.
- **pigeonhole** (semver, additive, the `async` feature):
  - `RowCells::next_async(&mut self) -> Result<Option<CellEntry<'_>>>`, a lending `pub async fn`;
  - `RowCells::poll_next(&mut self, cx)`, which it wraps;
  - `RowCells::poll_seek(&mut self, cx, family: &str, qualifier: &[u8])`: the first cell at or after a column, never moving back.
- **pigeonhole-engine** (internal 2.x API, additive):
  - `ScanCursor::poll_next_cell(&mut self, cx) -> Poll<Result<bool>>`;
  - `ScanCursor::poll_seek_cell(&mut self, cx, family, qualifier) -> Poll<Result<bool>>` (D196 within a row).
  
  #1625 also adds engine calls so a walk built sync can go on async: `Engine::async_scan_from`, `ScanCursor::families`, `ScanCursor::is_async`, `ScanCursor::cell`, and `ScanCursor::seek_cell` (the sync seek that finishes a pending one, ai1249's R1).

## Semantics
- **A walk within a row fetches before it steps.**
  - The stepping lane's next block is fetched one ahead if uncached, probed once per block it stands in.
  - A seek moves its family's cursors cache-only, fetching the block it lands in on a miss.
  - A `Pending` changes nothing the walk shows, and a seek left `Pending` is finished by the next step, async or sync.
  - A block still missed is read synchronously and counted in `Metrics::async_sync_reads`.
- **Text and memory share their scoring with their sync forms,** generic over their reads, so the results are identical.
- **Amendment (#1560 PR 2, 2026-11-10; perf287 and the coordinator accepted it):** the text search has two document loops, not the one generic core the design note planned. The sync `top_k` keeps its pre-#1560 loop, and the async search runs `TopK::run`'s resumable phases.
  - **Why:** the single core cost the sync text shapes +3.6% (text-term-hub) and +3.0% (text-two-terms) in callgrind. The phase dispatch costs about 14 instructions per candidate, and a `Poll`-wrapped read copied every posting. With two loops the sync shapes measure -0.13% and -0.2% against main.
  - **What's duplicated:** only the loop's control flow. The reads, steps, skips, lookups, bounds, cuts and solo expansions are shared (`Walk`, `Scored`, `Reads`).
  - **Guard:** a seeded property test (`tests/search_async.rs`) checks that `run_async` equals `run` over random queries of every kind. Each loop's doc names its twin.

- **Amendment (#1560 PR 3):** memory's async recall.
  - **Shared decisions:** the recall's decisions are shared with the sync form: the gather's pass (`Pass`), the IVF choice and fall-back (`Ivf::search` and `found`), and the expansion's hop and budget arithmetic (`Walk`, `Hop`). Only the reads differ, and each async method's doc names its sync twin.
  - **M1:** memory's `edges` already was a one-hop `Traverse` with the budget, plus a row read for the weights. So the async form is `Traverse::run_async` plus `RowRead::read_async`, with the same arithmetic, not `read_within_async`.
  - **The reads awaited together:** each namespace's vector, keyword and recency lists, by an in-crate join that adds no dependency.
  - **Reader processes:** every read a recall makes has an async form in a reader process, so `ReadStore`'s async recall refuses nothing beyond what its sync recall refuses.
  - **Zero synchronous reads, after #1652:** at first, the engine's async key-set reads still read a row past its first block synchronously. That covered the multi-get's memory rows, the expansion's traversal and the IVF search's list rows, so the property test bounded the count (28 of 7,680 recalls under load counted one). #1652 closed it, and the test asserts 0.

- **Amendment (#1652, perf287 agreed the design on #1652):** D196 within a row for every async reader that steps a row's cells.
  - **Engine (internal 2.x API, additive):** `KeyCursor::poll_cell_ready(cx)` and `ScanCursor::poll_cell_ready(cx)`, out of line. Each fetches, before a step, the block the step reads if the cache lacks it, probing every source the step moves once per block it stands in, and returns `Pending` without moving. `ScanCursor::poll_next_cell` is now that check plus its step, so the two can't drift.
  - **The readers:** the traversal, the nearest-neighbour search and the async reduce, over keys and over a scan, call it before each cell step. On `Pending` they stop as they do at their poll budgets, keeping the row's place, and the fetch's completion wakes the task. `KeyRows`' async fill is resumable mid-row. A sync `next` after a fill stopped mid-row finishes that row.
  - **Still read synchronously and counted:** a separated (blob) value, and a block a step reads that the probe didn't predict.

- **Amendment (#1560 PR 4):** Python, the design note's §8.
  - **The names:** `Store.recall_async` and `recall_with_stats_async`, and the same on `ReadStore`. They take `recall`'s keywords and resolve to its result. The stubs type them as `asyncio.Future`.
  - **B1:** the binding never polls a recall at the call. `aio::start_on_bridge` starts it on a **poll pool** of threads, private to `aio.rs`. On the one bridge thread, 16 concurrent recalls ran at a fifth of `to_thread`'s rate and held commit completions to 20 ms p50. With the pool, commits stay near 110 µs p50 (`crates/python/bench/recall_concurrency.py`). Commits, gets and feed waits keep the bridge thread and their inline poll (D210).
  - **Offload, not a polled `run_async` (g45, the coordinator's call):** the future runs the sync `run_with_stats` in its one poll, on a pool thread, as Ruby's binding does. A polled `run_with_stats_async` cost 1.6× a recall's time at one caller and ran at about half the offload's rate at 16 and 32 callers, and 2.5× slower with the block cache too small. Rust's own `run_async` costs only 1.18–1.20× at one caller on Linux, so the rest was the binding's per-poll hand-offs and GIL. Both kept commits near 80 µs p99, against `to_thread`'s 2 ms. The deviation from the note's goal: a recall's reads block its pool thread, not the loop's or an executor's, and the pool bounds how many run at once.
  - **The pool's size:** the machine's parallelism, from 2 to 16, or `PIGEONHOLE_ASYNCIO_POOL_THREADS` (1 to 256), read once when the pool starts. At 8 threads the offload was capped near 127 recalls/s at both 16 and 32 callers.
  - **Cancellation:** a recall still queued is dropped without running; one already running finishes on its thread and its result is dropped.

## Why
#1479's tier 2: an async server or agent loop must not block an executor thread on a text search's or a recall's reads.

## Callers changed
None: additive. The sync `run`, `RowCells::next` and `ScanCursor::next_cell` are unchanged.
