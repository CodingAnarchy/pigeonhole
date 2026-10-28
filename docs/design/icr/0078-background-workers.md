# 0078: background workers an application drives (D284)

**Status:** Approved (coordinator, 2026-10-26; D284, the design on #816 in comments 6422142633 and 6423973238, perf287's reviews in comments 6423938940 and 6424076592). The `pigeonhole-runtime`, `pigeonhole-engine` and `pigeonhole-io` items below are in #983 (PR 1); the `pigeonhole` crate's items are in PR 2. Under D282, internal crates follow semver honestly with no stability promise, so the engine items are listed here and not doc-hidden.

## Change

All additions; nothing is removed or renamed.

- **`pigeonhole-runtime`:**
  - `Runtime::application_owned_with_workers(config, handlers) -> Result<AppOwned<H>>`. `#[non_exhaustive] AppOwned<H> { shards: Vec<ShardDriver<H>>, workers: Vec<PoolDriver>, pool: Option<WorkerPool> }`. It starts no thread (D40): `config.compaction_threads` workers are handed out.
  - `PoolDriver`, one background worker:
    - `run_once(deadline_nanos) -> bool`, `next_deadline() -> Option<u64>`, `wait(timeout)`, `waker() -> Waker`, `set_wakeup(Box<dyn Fn() + Send + Sync>)`, `closed() -> bool`;
    - the same driving contract as `ShardDriver`, except that it has no `release`;
    - dropping it hands back what it held.
  - `WorkerPool`: `close()` (takes everything the workers hold back to the shards, waiting only for a slice running at that moment) and `closed()`.
  - `ShardContext::take_back()` adopts what the workers handed back, now rather than at the next turn. `ShardContext::reclaim_room() -> usize` is the room wait's take-back of its own idle commit-path tasks, and never waits. `Submitter::background_take_backs() -> (u64, u64)` gives the fallback and room-wait counts.
  - `Task` gains three methods with defaults, so existing implementors are unaffected:
    - `commit_path() -> bool` (default `false`): commits wait for the task (a flush, the manifest pump, a WAL spare), so a worker takes and runs it ahead of compactions and a room wait can take it back (renamed from `frees_room` in review, perf287's must-fix on #983);
    - `offload() -> bool` (default `true`);
    - `reclaim(&mut self, failed: bool) -> bool` (default: resume unless it failed).
  - `pub use pool::{PoolDriver, WorkerPool}`.
- **`pigeonhole-engine`:**
  - `EngineOptions::background_workers: usize`. The default is 0: no workers, and flushes and compactions run on the shards as before. `Engine::open_application_owned` refuses a nonzero count rather than leave workers undriven.
  - `Engine::open_application_owned_with(path, options) -> Result<AppOwned>`. `#[non_exhaustive] AppOwned { engine: Arc<Engine>, shards: Vec<EngineShard>, workers: Vec<BackgroundWorker> }`.
  - `BackgroundWorker`, wrapping `PoolDriver`: `run_once`, `next_deadline`, `wait`, `waker`, `set_wakeup`, `closed`.
  - `Metrics::compactions_reclaimed: u64` and `Metrics::background_take_backs: (u64, u64)`.
- **`pigeonhole`** (PR 2, the shape agreed on #816 in comment 6424076592):
  - `Pigeonhole::open_application_owned_with(path, Options) -> Result<AppOwned>`. `#[non_exhaustive] AppOwned { db: Pigeonhole, shards: Vec<Shard>, workers: Vec<BackgroundWorker> }`. `open_application_owned`'s tuple form stays, refuses a nonzero worker count (`InvalidArgument`), and its docs point to the new one.
  - `AppOwned::spawn_background_threads(&mut self) -> Result<Vec<JoinHandle<()>>>`: opt-in, one std thread per worker (`pigeonhole-worker-<i>`), each driving its worker until the close finishes.
    - A refused thread (a thread limit, a container's pids cap) is `ErrorCode::Io`, not a panic. The workers that got no thread go back to `AppOwned::workers` for the caller to drive or drop (perf287's review of #1010).
  - `BackgroundWorker`: `run_once(budget: Duration) -> bool`, `next_wakeup() -> Option<Duration>`, `wait(timeout)`, `waker() -> Waker`, `set_wakeup(Box<dyn Fn() + Send + Sync>)`, `closed() -> bool`, the `Duration` forms of the engine's, as `Shard`'s are.
  - `Options::background_workers(n)`, default 0. Application-owned opens only; engine-owned `open` ignores it (`compaction_cores` instead).
  - `EngineMetrics::compactions_reclaimed`, `background_fallback_take_backs` and `background_room_take_backs` (the engine's `background_take_backs` pair, named).
- **`pigeonhole-io`:** `inline_io(f)` and `in_inline_io()`, the scope an application worker's slice runs in (its I/O synchronous on the real backends); `SimVfs::inline_writes_in_flight()`, for tests.
  - Test hooks (`test-hooks` feature, doc-hidden): `Engine::compaction_commits_submitted` and `Engine::compaction_tiny_slices`.

## Why

D284: in application-owned mode, D40 starts no threads, so flushes and compactions ran in 500 µs slices on the shard threads. At the default level, that cost app-owned 0.68× of engine-owned's throughput and 6.8× its p99 (#816's box check). The workers let the application give background work threads of its own. Engine-owned mode's compaction threads drive the same `PoolDriver`, so there is one mechanism.

## Callers

- **`pigeonhole-runtime`:** `lib.rs` (both modes; engine-owned threads loop over their own `PoolDriver`), `pool.rs`, `sched.rs`; tests `modes.rs` (with 1 and 2 workers), `workers.rs`, and the loom models in `pool.rs`.
- **`pigeonhole-engine`:**
  - `engine.rs`: the opens, `close` (which calls `WorkerPool::close` first), `Metrics`;
  - `shard.rs`: room waits, the close message's `take_back`;
  - `flush.rs`, `compact.rs`, `manifest.rs`, `shard.rs` (`SpareTask`, `ClockTimer`): `Task::reclaim`, `commit_path` and `offload`;
  - tests: the stopped-worker close, the room-wait take-back, the inline-I/O and re-pick tests.
- **`pigeonhole`:** `db.rs` (the opens, `AppOwned`, `BackgroundWorker`), `options.rs`, `metrics.rs`; tests `app_owned_workers.rs`; the guide's async page ("Background workers") and agent reference; `pigeonhole-bench`'s application-owned runner (`--background-workers`).
