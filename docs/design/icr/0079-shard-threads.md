# 0079: the shards' driver threads (D289)

**Status:** Approved (coordinator, 2026-10-27; D289, the owner's decision on #760 in comment 6441951893, from perf287's revision 2 in comment 6434752774). It covers PR 1: a fixed count of driver threads. PR 2's controller adds the load-sized default and its own items to this record.

## Change

All additions; nothing is removed or renamed. A count of 0 keeps a thread per shard, as before.

- **`pigeonhole-runtime`:** `RuntimeConfig::shard_threads: usize`, engine-owned mode only.
  - 0 (the default) gives each shard a thread of its own, as before.
  - `k` in `1..shards` runs shard `i` on thread `i % k`. The thread takes its shards' turns one after another, then spins once and parks once for all of them.
  - A larger `k` means one thread per shard.
  - The threads are pinned as shard threads were (D235). Threads holding several shards are named `pigeonhole-driver-{d}`; with one shard per thread the name stays `pigeonhole-shard-{i}`.
- **`pigeonhole-engine`:** `EngineOptions::shard_threads: usize`, passed to the runtime. Application-owned opens ignore it: the application chooses which threads drive which shards.
- **`pigeonhole`:** `Options::shard_threads(k) -> Self`, the builder for the engine option.
- **`pigeonhole-bench`** (not published): `PigeonholeRunner::shard_threads(k)` and `--shard-threads K`, for D289's fixed-k A/B. The record's settings show ` shard_threads=K` when it's set.

## Why

D289. #760's put p99 (444–791 µs against a 20.3 µs floor) is queue wait for a parked shard thread whose pinned core a saturating client holds. The wakeup reaches the CPU after one EEVDF slice, about 1 ms on Linux 6.8.

Fewer, busier threads park less: on the AX102, 8 shards gave 27–28 µs at flat ops/s. Driver threads give a store that effect without giving up its shards' memtable arena, which 8 shards halves.

PR 1 fixes the count, so the box A/B at k ∈ {1, 2, 4, 8, 16} can choose the default. PR 2 sizes it to the load.

## Callers

- **`pigeonhole-runtime`:**
  - `lib.rs`: `Runtime::start` assigns shards to drivers. `driver_main` replaces `shard_main`; the spin before parking polls every held shard and splits its time among the shards that worked (D256). `stop` returns the handlers in shard order.
  - Tests: `modes.rs` runs the whole engine-owned suite with one and two drivers, `drivers_hold_shards_round_robin_and_handlers_return_in_order`, and `a_driver_charges_its_spin_to_the_shards_that_worked`.
- **`pigeonhole-engine`:** `engine.rs` (the runtime config).
- **`pigeonhole`:** `options.rs`; tests: `model.rs` (`PIGEONHOLE_SHARD_THREADS=k`).
- **`pigeonhole-bench`:** `runners/pigeonhole.rs`, `main.rs`.

## PR 2 additions (D289's load controller)

- **`pigeonhole-runtime`:**
  - `RuntimeConfig::adaptive_drivers: bool` (default `false`): engine-owned drivers sized to the load, up to `shard_threads` (one per shard if 0).
  - `Runtime::driver_target() -> usize` and `Runtime::drivers_settled() -> bool`.
  - Doc-hidden, for tests: `force_driver_target(Option<usize>)`, `driver_control()` and `driver_moves()`.
- **`pigeonhole-engine`:**
  - `EngineOptions::adaptive_drivers: bool`, default `false`, so the crate's own tests run fixed.
  - Doc-hidden: `Engine::driver_target() -> Option<(usize, bool)>` and `Engine::force_driver_target`.
- **`pigeonhole`:**
  - Engine-owned opens turn the controller on unless `shard_threads` is set.
  - Doc-hidden: `Pigeonhole::driver_target()` and `force_driver_target()`, for the bench's warm-up and the model check's churn.
- **`pigeonhole-io`:** doc-hidden `cross_ring_wait_slices()`.
- **`pigeonhole-bench`:** `Runner::driver_target()`, and `RunDetail::drivers`.

