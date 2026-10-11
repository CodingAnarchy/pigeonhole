# Concurrency in `pigeonhole-engine` and `pigeonhole-runtime`

This is the inventory of shared-state synchronization in the engine and the shard runtime (#134 step 1). For every `Mutex`, `RwLock`, `Condvar`, atomic, `ArcSwap`, `OnceLock`, thread-local and shared queue, it records:
- what it protects;
- who writes and reads it;
- its memory ordering or lock nesting;
- why it exists, with the issue or decision behind it.

It also states the lock order and the cross-thread protocols built on these primitives. **Nothing here changes code.** Steps 2–4 of #134 (reduce, prove, measure) start from this document.

**How current it is.** Line numbers refer to `main` at the commit this document was merged with. When a primitive is added or removed, update its row, and its lock-order or protocol entry if any.

## The model

The spec's model is thread-per-core. Each shard owns its data (`ShardState`: memtables, WAL stream, tablets, timers, the pending-commit queues). Only the thread driving the shard touches it:
- a runtime thread in engine-owned mode;
- the application's thread in application-owned mode, which may change between turns (#473, #492).

**Shard-owned state needs no synchronization and isn't listed here.** Everything below is the exception: state shared between shards, with clients, with background tasks, with the manifest writer, or with reader processes. Most of it hangs off `Shared` (`crates/engine/src/shard.rs`), the one engine-wide struct every shard and every `Engine` handle holds.

**Who runs where.** The same terms are used throughout:

| Role | Thread |
|---|---|
| Shard thread | The thread running a shard's turn (`ShardState`). |
| Client thread | An application thread calling `Engine` methods. It commits by sending to a shard's inbox and waiting on a `Waiter`. |
| Manifest pump | Whoever holds the manifest exclusion (`Shared::manifest_busy`) and runs `manifest::begin` / `end`: a shard thread's `ManifestPump` task, or a client thread in `commit_req_from_thread` / `drain_sync`. |
| Background task | A cooperative task (flush, compaction, WAL spares, `ClockTimer`) on a shard's scheduler or the runtime's pool. |
| Completion | The thread that resolves an I/O completion or a `Notifier`, and so runs the continuations chained to it. |
| Reader process | Another process attached read-only through shared memory (`Role::Reader`). |

## Counts

**159 primitives** (`main` at #306's head):

| Where | Production | Test hooks only | Notes |
|---|---:|---:|---|
| `engine/src/shard.rs`, `shard/tablets.rs` (incl. `Shared`) | 75 | (mixed in, marked) | Static test hooks: `READS_STEP`, `TAIL_SKIPS` |
| `engine/src/engine.rs`, `engine/hooks.rs` | 8 | 26 | `Hooks` 20, `ShardCounters` 4, `ReaderHooks` 2 |
| other `engine/src` files | 34 | | 10 of them thread-locals (per-thread scratch and caches) |
| `runtime/src` | 16 | | Plus the `spawn_pinned` start ack and the per-thread `SlotCache`, listed |
| **Total** | **159** | | |

A large share are **statistics counters** with `Relaxed` ordering (`ShardMetrics`, `VisibilityWaiters::{waits, scanned, contended, passes, woken}`, ICR 0023 and similar). They synchronize nothing: they're read by `Engine::metrics` and need no reduction. Step 2's count should report them separately from primitives that coordinate.

## Lock order

Every observed nesting, outer → inner. The **manifest exclusion** (`manifest_busy`, a try-lock claimed by `manifest::claim`) is a lock in effect and the outermost level. No site takes two of these locks in the opposite order (none was found in either crate).

1. **Manifest exclusion → `Shared::manifest`** (the writer mutex), in `manifest::begin`, `manifest::end` and `Shared::final_close`.
   - Inside the writer mutex: the flush and compaction catalog closures CAS their purge guards (`AtomicU8`, flush.rs, compact.rs), `Shared::view` is loaded (`ArcSwap`, not a lock), and pager and block-cache internals are taken (not inventoried).
   - `ManifestQueue::reqs` is taken and released *before* the writer mutex, never inside it.
2. **Manifest exclusion → `Shared::view_lock` → { `view_record`, `view_versions`, `flushed_roots` }**, in `Shared::publish_view` called from `manifest::end`.
   - The writer mutex is released before `publish_view` (manifest.rs `end`).
   - Under `view_lock`, `ViewPin::new` takes `LiveViews::versions`.
   - `ShardState::publish_memtables` also takes `view_lock` → `flushed_roots` (through `mem_sets`), releasing it before `publish_view` takes it again.
3. **Manifest exclusion → `Shared::locks`**, in `try_final_close` → `final_close`.
4. **Manifest exclusion → `CloseState::outcome`**, in `try_final_close`. `outcome` is released before the exclusion, and `CloseState::done` is taken after.
5. **`CloseState::outcome` → `CloseState::done`**, in `Engine::close` (engine.rs). The final close takes them one after the other, never together.
6. **`large_logged` → `large_dropped` → `large_pending`**, in `LargeValues::settle_on`'s resolve callback (large.rs), on the completion thread. `note_logged` takes `large_pending` then `large_logged` in sequence, never together.
7. **`ReaderState::shm` → { `ReaderState::view`, `ReaderState::catalog` }**, in `reader_snapshot`'s re-attach branch. `view` and `catalog` are never held together.
8. **`LiveSeqnos::seqnos` → the `shm` visible-seqno load**, in `SeqnoPin::pin_visible` (snapshot.rs). This is deliberate (#315 review).
9. **Lock plus atomic:** `ts_raisers[i]` is held while `ts_raises[i].fetch_max` runs (tablets.rs `commit_op`).

**Callbacks under locks.** These are where a new nesting could appear without anyone writing a second `lock()`:
- `Shared::wake_visible` calls `Waker::wake()` while holding `VisibilityWaiters::list`. Whatever the waker does nests inside it: the runtime's `Signal::target` lock, or an async executor's locks.
- `Inner::reader_catalog` holds `ReaderState::catalog` across the root reload and manifest read, and `reader_view` holds `view` while it builds a view (long work under one lock).
- `flushed_roots` is held across `view.load_full()` in `read_latest`. That's not a lock, but the read relies on the ordering (#131).

**The runtime holds no lock while taking another.**
- `Signal::target` is held only to clone the target; the unpark, eventfd write or callback runs after release.
- `Slot::state` is released before a completion's hook runs and before the condvar or waker is signalled.
- `Waiter::wait` holds `Slot::state` across `cv.wait`, the standard condvar pattern.

## Cross-thread protocols

These are the protocols the primitives implement, and whether a loom model checks each one. #134 step 3 asks for loom models of every remaining one.

| Protocol | Primitives | Where | Loom |
|---|---|---|---|
| **Manifest exclusion and pump.** One writer, at most one commit in flight; any thread may finish a pump's completed commit (`take_done`); a blocked caller reaps its own ring, then orphan rings, then parks (#135, #166, #207, ICR 0028, #306). | `manifest_busy`, `ManifestQueue::reqs`, `Shared::manifest`, `Flight::{state, threads, thread_count}` | manifest.rs; appendix C has the full description | none |
| **Final close.** The last shard to report makes the close pending; whoever next releases the exclusion, or the closer that claims it, runs it, through paired SeqCst fences (#78, #81). | `CloseState::{remaining, final_pending, failed, outcome, done}`, `manifest_busy` | shard.rs `report_closed`, `try_final_close`; manifest.rs `release` | none |
| **View publication.** Built under `view_lock`, published by `ArcSwap` store; readers load without locks; superseded views are pinned until released (`LiveViews`), then extents are reclaimed. | `view_lock`, `view`, `view_record`, `view_versions`, `flushed_roots`, `LiveViews::versions` | shard.rs `publish_view`; snapshot.rs | none |
| **Watermark and visibility waits.** Shards publish the visible seqno. Async commits register `(seqno, waker)`; a count lets publishers skip the lock; a SeqCst fence pairs registration with publication. | `VisibilityWaiters::{count, list}`, `shm` watermark | shard.rs `wait_visible`, `wake_visible`, `publish_watermark` | none |
| **Freeze waiters.** A shard defers a memtable freeze until the watermark passes it. `min_needed` lets publishers skip the lock; SeqCst fences pair `register` and `take_ready` (#97). | `FreezeWaiters::{min_needed, list}` | shard.rs | none (a stress test covers the lost-registration race) |
| **Purge guards.** A flush or bottommost compaction's purge races the shard admitting a delete: three-state CAS (`IN_FLIGHT` → `VOIDED` by the shard, or → `INSTALLING` by the pump under the writer mutex) (D191, D192). | `ShardState::guarded` atomics, `CompactionGuard::state` | shard.rs `admit_against_guards`; flush.rs, compact.rs catalog closures | none |
| **Clock timers.** Cancel against fire: the shard stores `cancel` and takes the waker; the task stores its waker and re-checks `cancel`; `frozen_at` is published before `done` (#84, #93). | `TimerState::{cancel, done, frozen_at, waker}` | shard.rs `ClockTimer` | none |
| **Tablet load publication.** A shard publishes its write rate (`writes`, then `at` with Release); others read `at` (Acquire), then `writes`. Slot reservations for in-flight changes use RMW (#97, #103, #128). | `LoadSlot::{writes, at, reserved}` | shard/tablets.rs | none |
| **Large-value settlement.** A commit's blob files are kept or released depending on where it stopped (#230). | `large_logged`, `large_dropped`, `large_pending` | large.rs | none |
| **Sleep and wake.** Dekker-style with SeqCst fences: the consumer marks itself sleeping, fences, re-checks its sources, then parks; the producer sends, fences, swaps `sleeping`, then wakes the target. D198's idle spin and `set_wakeup`'s initial call are variants. | `Signal::{sleeping, task_woken, target}`, inbox channel | runtime signal.rs, lib.rs, sched.rs | none |
| **Background-task wake.** A wake during a slice keeps the task runnable. | `TaskCell::woken`, `Signal::task_woken` | runtime sched.rs | none |
| **Submit against shutdown.** Store-buffering on `inflight` and `closed` (SeqCst): no message is accepted and then left unhandled. | `Inbox::{inflight, closed}` | runtime lib.rs | none |
| **One-shot completion and slot reuse.** The hook runs before the value is published; the condvar is signalled only if a thread blocked; a slot is reused once the notifier has dropped it. | `Slot::{state, cv}`, `SlotCache` | runtime completion.rs | **yes** (completion.rs: slot reuse against the notifier, and a waiter dropped on another thread) |
| **Shard handover between application threads.** No primitive: the application synchronizes. The runtime detects the new thread (`TOKEN`, `NEXT`) and attaches a ring; `release()` drains the old ring first (#473, #492, ICR 0029). | thread-local `TOKEN`, `static NEXT` | runtime lib.rs | n/a |

## For step 2

These observations come from the inventory. They aren't decisions.

- **Counters are most of the count.** Separate them (Relaxed, read only by metrics) from coordinating primitives before measuring a reduction.
- **Several protocols end in a "skip the lock if nobody waits" counter plus a SeqCst fence:** visibility waiters, freeze waiters and final close. They're the subtlest code here, and none has a loom model. They're the first candidates for either a loom model or the one per-shard event path #134 proposes.
- **The manifest exclusion has many exits:** pump, client thread, `take_done` handover, final close, abandon on drop. Collapsing them is the largest simplification available. It's also where Phase 1's bugs clustered (#78, #81, #135, #166, #207).
- **Wakers run under locks** (`wake_visible`). That's harmless with the runtime's waker, but an application executor's waker runs there too.

## Appendices: the inventory

Each appendix is one part of the code. Rows give `file:line` of the declaration; nesting lists are outer → inner.


## Appendix A: `engine/src/shard.rs` and `shard/tablets.rs` (including `Shared`)

"Shard thread" means the shard's own `ShardState` loop (engine-owned runtime thread, or the application thread driving an application-owned shard). "Manifest pump" means whoever holds the manifest exclusion (`manifest_busy`) and runs `manifest::begin`/`end`: a shard thread in `drain` or an application thread in `commit_from_thread`/`drain_sync`. Writers and readers outside these two files are named by `file:fn`. History is the first commit that added the field (`git log -S`), plus ids from the doc comment.

Not inventoried here because the type is defined elsewhere (they are `Shared` fields that hold primitives internally): `manifest_queue: ManifestQueue` (manifest.rs:425), `manifest_flight: manifest::Flight` (manifest.rs:1035), `drivers: waker::Drivers` (waker.rs:70, `Box<[AtomicU64]>`), `live_views`, `live_seqnos`, `pager`, `cache`, `shm`, `row_cache`, `hooks` (test-hooks), and `ShardMetrics::hooks` (test-hooks).

### `ShardMetrics` (shard.rs:106), one per shard in `Shared::metrics`

Every counter in this struct has one writer, its own shard thread, and the reader is `Engine::metrics` (engine.rs ~1670–1745) on an application thread. All of them use `fetch_add` and `load` with **Relaxed** ordering. Nothing else is published through them.

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `ShardMetrics::commits` shard.rs:107 | `[AtomicU64; 4]` | Commits per durability level | shard thread: `ShardMetrics::record` from `run_group` paths (shard.rs:5518, 5987) | `Engine::metrics` (engine.rs:1713) | Relaxed | Interface freeze 46c75ea (#1); Milestone A c9700de (#41) |
| `ShardMetrics::latency` shard.rs:109 | `[[AtomicU64; 64]; 4]` | Log-scale commit-latency histogram per durability level | shard thread: `record` | `Engine::metrics` (engine.rs:1715) | Relaxed | c9700de (#41) |
| `ShardMetrics::stalls` shard.rs:110 | `AtomicU64` | Write stalls entered | shard thread: `run_group` (shard.rs:4231) | `Engine::metrics` | Relaxed | c9700de (#41) |
| `ShardMetrics::stall_nanos` shard.rs:111 | `AtomicU64` | Time spent stalled | shard thread: `end_room_wait` (2966), `stalled` (3962, 3977) | `Engine::metrics` | Relaxed | c9700de (#41) |
| `ShardMetrics::flushes` shard.rs:112 | `AtomicU64` | Completed flushes | shard thread: `on_flushed` (3334) | `Engine::metrics` | Relaxed | c9700de (#41) |
| `ShardMetrics::flush_nanos` shard.rs:113 | `AtomicU64` | Flush time | shard thread: `on_flushed` (3335) | No reader found in engine.rs: written but apparently never read (unsure) | Relaxed | c9700de (#41) |
| `ShardMetrics::compactions` shard.rs:114 | `AtomicU64` | Completed compactions | shard thread: `on_compaction_done` (6825) | `Engine::metrics` | Relaxed | 939440e Milestone B (#63) |
| `ShardMetrics::compaction_nanos` shard.rs:115 | `AtomicU64` | Compaction time | shard thread: `on_compaction_done` (6826) | No reader found (unsure) | Relaxed | 939440e (#63) |
| `ShardMetrics::flush_failures` shard.rs:118 | `AtomicU64` | Failed flushes (retried on backoff) | shard thread: `on_flushed` (3384) | `Engine::metrics` | Relaxed | #141; a62da94 (#186) |
| `ShardMetrics::compaction_failures` shard.rs:119 | `AtomicU64` | Failed background compactions | shard thread: `back_off_compaction` (6604) | `Engine::metrics` | Relaxed | #141; a62da94 (#186) |
| `ShardMetrics::splits` shard.rs:120 | `AtomicU64` | Tablet splits done | shard thread: tablets.rs `on_tablet_op_done` (885) | `Engine::metrics` (engine.rs:1693) | Relaxed | c1b02a2 (#97, #38) |
| `ShardMetrics::merges` shard.rs:121 | `AtomicU64` | Tablet merges done | as `splits` (tablets.rs:886) | engine.rs:1694 | Relaxed | c1b02a2 (#97) |
| `ShardMetrics::moves` shard.rs:122 | `AtomicU64` | Tablet moves done | as `splits` (tablets.rs:887) | engine.rs:1695 | Relaxed | c1b02a2 (#97) |
| `ShardMetrics::unpin_passes` shard.rs:124 | `AtomicU64` | WAL unpin passes | shard thread: `limit_wal_pin` (4715), `on_unpin` (5220) | `Engine::metrics` | Relaxed | #137; 1ccd22d (#171) |
| `ShardMetrics::unpin_flushes` shard.rs:125 | `AtomicU64` | Memtables frozen below threshold by unpin | shard thread: `freeze` (3092) | `Engine::metrics` | Relaxed | #137; 1ccd22d (#171) |
| `ShardMetrics::refused_slots` shard.rs:128 | `AtomicU64` | Tablet changes refused for lack of memtable slots | shard thread: tablets.rs `reserve_slots` (729), `choose` (1307) | `Engine::metrics` (engine.rs:1676) | Relaxed | D28; #122, 0b46ae1 (#259) |
| `ShardMetrics::refused_view` shard.rs:129 | `AtomicU64` | Splits refused because the shm view would outgrow its buffer | shard thread: tablets.rs `validate_op` (645) | engine.rs:1677 | Relaxed | D28; #122, 0b46ae1 (#259) |
| `ShardMetrics::wal` shard.rs:131 | `OnceLock<Arc<WalCounters>>` | The shard's WAL stream stall counters, set once the stream is open | open thread: `open_writer` (engine.rs:854, 878, 910; `set`, a later set is ignored) | `Engine::metrics` (engine.rs:1742–1743, `get`) | OnceLock (set happens-before a successful get) | ICR 0015, #64; 0f0c7cf (#414) |

### `CloseState` (shard.rs:199), `Shared::close`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `CloseState::remaining` shard.rs:200 | `AtomicUsize` | Shards that have not yet reported their close. The last reporter makes the final close pending | init `shards` at open (engine.rs:602); shard thread: `Shared::report_closed` (`fetch_sub`, shard.rs:752) | `report_closed`; `ShardState::abandon` (7073) and `report_panicked` (7088), which check `> 0` | fetch_sub AcqRel; loads Acquire | Milestone A c9700de (#41) |
| `CloseState::done` shard.rs:201 | `Mutex<Option<Notifier<Result<()>>>>` | The `Engine::close` caller's completion | `Engine::close` (engine.rs:2685, sets it) on an app thread; `try_final_close` (shard.rs:787, takes it) on whoever runs the final close | same two | Taken in `Engine::close` **while `outcome` is held** (see nesting). In `try_final_close` it is taken after `outcome` is released, and after `manifest::release` | c9700de (#41) |
| `CloseState::failed` shard.rs:203 | `AtomicBool` | A shard's final sync, flush or checkpoint failed, or a shard died: the close is unclean | `Shared::fail_close` (swap, shard.rs:467), from shard threads and from `shard_died` | `visibility_ended` (510, SeqCst), `final_close` (685, Acquire), `try_finish_close` (6998, Acquire) | swap AcqRel; loads SeqCst and Acquire | c9700de (#41); "5-6 5.1" in the doc comment |
| `CloseState::final_pending` shard.rs:206 | `AtomicBool` | Every shard has reported; the final close runs once someone holds the manifest exclusion | `report_closed` (store true, 753); `try_final_close` (swap false, 774) | `try_final_close` (768); test hook hooks.rs:1032 | SeqCst for all, plus SeqCst fences paired with `manifest::release` and `MaintenanceGuard` | #78, #81; ff30866 |
| `CloseState::outcome` shard.rs:208 | `Mutex<Option<Result<()>>>` | The final close's result, kept for late `Engine::close` and `EngineShard::closed` callers | `try_final_close` (780), which holds the manifest exclusion | `Engine::close` (engine.rs:2694); `EngineShard::closed` (engine.rs:3200) | Held while `done` is locked in `Engine::close` (outcome → done) | #135; 3b6051d (#166) |

### `VisibilityWaiters` (shard.rs:217), `Shared::waiters`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `VisibilityWaiters::count` shard.rs:219 | `AtomicUsize` | Length of `list`, so a publisher can skip the lock when nobody waits | stored under `list`: `wait_visible` (497), `wake_all_visible` (522), `wake_visible` (577) | `wake_visible` (561) on the shard thread after a watermark publish | stores Release, load Acquire. After the store, `wait_visible` issues a SeqCst fence that pairs with the SeqCst fence in `FreezeWaiters::take_ready`, which runs first in `wake_visible` | c9700de (#41) |
| `VisibilityWaiters::list` shard.rs:220 | `Mutex<Vec<(Seqno, Waker)>>` | Async commits waiting for the global watermark to reach their seqno | app/async threads: `wait_visible` (via write.rs `wait_visible` and the `PendingCommit` polls); shard threads: `wake_visible` after `publish_watermark` (3551); `wake_all_visible` from `shard_died` and `try_final_close` | same | Taken through `VisibilityWaiters::lock` (`try_lock` first, counts contention). No other engine lock is held while it is taken. `wake_visible` calls `Waker::wake()` **while holding it** (shard.rs:570); `wake_all_visible` wakes after releasing it | c9700de (#41) |
| `VisibilityWaiters::waits` shard.rs:224 | `AtomicU64` | Registrations that found their seqno not yet visible | `wait_visible` (489) | `Engine::metrics` (engine.rs:1759) | Relaxed | ICR 0023, #154; f8af55d (#463) |
| `VisibilityWaiters::scanned` shard.rs:225 | `AtomicU64` | List entries scanned by registrations | `wait_visible` (492) | engine.rs:1760 | Relaxed | ICR 0023, #154 |
| `VisibilityWaiters::contended` shard.rs:226 | `AtomicU64` | Lock acquisitions that found `list` held | `VisibilityWaiters::lock` (238) | engine.rs:1766 | Relaxed | ICR 0023, #154 |
| `VisibilityWaiters::passes` shard.rs:227 | `AtomicU64` | Wake passes that took the lock | `wake_visible` (566) | engine.rs:1763 | Relaxed | ICR 0023, #154 |
| `VisibilityWaiters::woken` shard.rs:228 | `AtomicU64` | Waiters woken | `wake_visible` (572) | engine.rs:1764 | Relaxed | ICR 0023, #154 |

### `FreezeWaiters` (shard.rs:250), `Shared::freeze_waiters`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `FreezeWaiters::min_needed` shard.rs:253 | `AtomicU64` | Smallest seqno any registered shard waits for (`u64::MAX` = none). A publish below it skips the lock | under `list`: `register` (shard thread in `freeze`, 3125), `take_ready` (any shard thread in `wake_visible`) | `take_ready` (lock-free check) | SeqCst store and load, plus SeqCst fences: after the store in `register`, and before the load in `take_ready` | c1b02a2 (#97). The field `freeze_waiters` dates from 939440e (#63) in an earlier shape |
| `FreezeWaiters::list` shard.rs:254 | `Mutex<Vec<(u16, Seqno)>>` | Shards with a freeze deferred until the watermark passes their memtable | `register`, `take_ready` | same | No nesting. It is released before `wake_visible` takes `waiters.list` | c1b02a2 (#97); test `a_freeze_waiter_registering_during_a_take_is_never_lost` (7531) |

### Statics

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `static READS_STEP` shard.rs:315 (test-hooks) | `AtomicBool` | Reads step over superseded memtable versions instead of using the stale-tail index | test hook hooks.rs:732 | read.rs:1673 (read paths on any thread) | Relaxed | D194, D199; 2cb8a28 (#439) |
| `static TAIL_SKIPS` shard.rs:319 (test-hooks) | `AtomicU64` | Memtable tail jumps taken by reads | source.rs:129 (readers) | hooks.rs:738 | Relaxed | D194, D199; 2cb8a28 (#439) |
| `static DEPTH` (in `group_sync_depth`) shard.rs:1165 | `OnceLock<u32>` | `PIGEONHOLE_GROUP_SYNC_DEPTH`, read once per process | first caller of `group_sync_depth` (`get_or_init`) | all callers | OnceLock | D207, #412, #405; ef20207 (#477) |
| `static TRACING` shard.rs:7334 | `AtomicU8` | `PIGEONHOLE_TRACE` state: unset, off or on | `tracing()` (CAS from unset), `set_tracing` (test hook, #244) | `tracing()`, on every thread that traces | Relaxed for all, including the CAS | #244; 25d95ad (#252) |

`static ROWS` (shard.rs:1619) is a `LazyLock<FastBuildHasher>`. It is initialized once and is read-only after that. It is not counted, since `LazyLock` is not in the listed categories (#287, 79e1fa6).

### `Shared` (shard.rs:330), engine-wide and shared by every shard and caller

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `Shared::view` shard.rs:334 | `ArcSwap<View>` | The current in-process view: catalog, tablet map, memtable pieces and SST sets | `publish_view` (634, under `view_lock` **and** `flushed_roots`); open, `open_writer` (engine.rs:958) | everywhere: shard thread (`read_latest`, `conflicts`, `maintain`, tablets.rs…), app threads (engine.rs reads and writes, maintenance.rs), manifest pump (`begin`, manifest.rs:591), flush/compaction tasks (flush.rs:296, 332; compact.rs:763) | ArcSwap (load/store; `load_full` where it is kept) | c9700de (#41); #131 for the store under `flushed_roots` |
| `Shared::async_sync_reads` shard.rs:336 | `AtomicU64` | Reads inside async calls that went to the file synchronously | read.rs:767, 789, 871 (async read paths) | `Engine::metrics` (engine.rs:1747); nonblocking.rs:165, 198 | Relaxed | D196, ICR 0014, #42; 7b560a8 (#411) |
| `Shared::commit_parks` shard.rs:338 | `AtomicU64` | Blocking commit waits that parked their thread | write.rs:520 (application thread) | engine.rs:1749 | Relaxed | ICR 0027; edda3e7 (#480) |
| `Shared::view_lock` shard.rs:341 | `Mutex<u64>` | Serializes view publishers and holds the last published view version | `publish_view` (shard.rs:593; called by the shard thread's `publish_memtables`, 2727, and the manifest pump's `manifest::end`, manifest.rs:916); open sets it to 1 (engine.rs:960) | same | Outer lock. Taken while holding the manifest exclusion (`manifest_busy`) when called from `manifest::end`. While held: `flushed_roots` (inside the closure, via `mem_sets`, and again at the store), `view_record`, `view_versions`, and `shm.publish_view` | c9700de (#41); #131 |
| `Shared::view_record` shard.rs:344 | `Mutex<RecordCache>` | The last shm view record, refilled in place by the next publish | `publish_view` (602) | `publish_view` | Taken only under `view_lock` | #499; 05b5335 (#507) |
| `Shared::manifest` shard.rs:345 | `Mutex<ManifestWriter>` | The manifest writer state: prepare, complete and root | manifest pump: `manifest::begin` (manifest.rs:588, held from prepare until `drop(writer)` at 782/822/845, while the request closures run), `manifest::end` (887, around `complete` only); `final_close` (shard.rs:714, `mark_clean`); open: engine.rs:1962 and 2020 (`commit`) | maintenance.rs:420 (shrink: `root()`); hooks.rs:321 | Normally taken with `manifest_busy` claimed (pump, final close). The open paths run before the runtime exists. maintenance.rs:420 and hooks.rs:321 take it briefly for `root()`; whether those claim `manifest_busy` is unverified. While held in `begin`, `ReqKind::Catalog`/`Tablets` closures run; those closures CAS the guard `AtomicU8`s (flush.rs:661, compact.rs:806). Pager-internal locks are taken under it (`prepare`) | c9700de (#41) |
| `Shared::manifest_busy` shard.rs:348 | `AtomicBool` | The manifest writer's **exclusion**, a hand-rolled try-lock held by whoever commits a manifest batch | `manifest::claim` (swap true), `manifest::release` (store false); callers are the shard pump, app threads in `commit_from_thread`/`drain_sync`, and `try_final_close` | `claim`; manifest.rs:1248 (a waiting thread's check) | claim swap AcqRel; release store Release, then a SeqCst fence that pairs with the fence in `try_final_close`; manifest.rs:1248 load SeqCst | #78; 939440e (#63) |
| `Shared::sst_ids` shard.rs:353 | `Arc<AtomicU64>` | Next SST id allocator (shared into flush and compaction jobs) | flush.rs:171 (flush task), compaction job.rs:432 (compaction task), maintenance.rs:511 (shrink) — all `fetch_add` | manifest pump: `begin` snapshots it into the catalog counters (manifest.rs:789); engine.rs:2006 | Relaxed | 939440e (#63) |
| `Shared::blob_ids` shard.rs:354 | `Arc<AtomicU32>` | Next blob-file id allocator | compaction blob.rs:233 (flush, compaction and large-value writers via `Arc::clone`) | manifest.rs:790 | Relaxed | 939440e (#63) |
| `Shared::flushed_roots` shard.rs:362 | `Mutex<HashSet<(u16, u32)>>` | Memtables `(shard, root)` whose SSTs are in the manifest, which views exclude until the owning shard retires them | `publish_view` (630: extended together with `view.store`); shard thread `on_flushed` (3340: removed when retired) | shard thread: `mem_sets` (2545), `read_latest` (3639; `view.load_full` happens inside the lock), `conflicts` (3715); open (engine.rs:928, via `mem_sets`) | Inner to `view_lock` (in `publish_view`, and in `mem_sets` when called from `publish_memtables`). In `read_latest`, held across `view.load_full()` on purpose, so that the shard sees both or neither | #131; 939440e (#63) |
| `Shared::busy_ssts` shard.rs:364 | `Mutex<HashSet<SstId>>` | SSTs that a running compaction or relocation reads or replaces | shard thread: `start_compaction` (6731 add, 6777 remove on error), `on_compaction_done` (6802 remove); maintenance.rs:490 (relocation claim), 742 (`unclaim`) on an app thread | shard thread: `maintain` (6389, 6456), `start_blob_gc` (6540); maintenance.rs:535 (snapshot clone) | No nesting seen. maintenance.rs:535 clones the set before `Pager::clear_for` runs under the allocator lock | D60; 939440e (#63) |
| `Shared::large_pending` shard.rs:368 | `Mutex<HashSet<BlobFileId>>` | Blob files of commit-time separated values whose commit has not settled, or whose `DropBlobFile` has not committed | large.rs:381 (add, app thread), 389 (remove on failure), 165 (remove in a manifest request callback), `settle_on` callback (115) | `note_logged` (large.rs:216, from the shard thread in `run_group`); test hook hooks.rs:425 | Innermost of the three `large_*` locks in `settle_on`: logged → dropped → pending | #230; ec037d5 (#301) |
| `Shared::large_open` shard.rs:371 | `AtomicUsize` | Live `LargeValues` guards. At zero, shards skip scanning logged batches for pointers | `LargeValues::new` (large.rs:84, +1), `Drop` (184, −1), on app threads | shard thread: `run_group` (shard.rs:4402) | fetch_add/sub AcqRel; load Acquire | #230; ec037d5 (#301) |
| `Shared::large_logged` shard.rs:375 | `Mutex<HashSet<BlobFileId>>` | Blob files whose batch reached its commit point (handed to the WAL) | shard thread: `note_logged` (large.rs:222, after releasing `large_pending`) | `settle_on` callback (large.rs:107; the thread that resolves the commit notifier) | Outermost in `settle_on` (logged → dropped → pending) | #230; ec037d5 (#301) |
| `Shared::large_dropped` shard.rs:378 | `Mutex<HashSet<BlobFileId>>` | Blob files the same-commit collapse (D34) dropped at apply, which the settle hook releases | shard thread: `apply` (shard.rs:3803, not during replay) | `settle_on` (large.rs:111) | Middle in `settle_on` | D34, #230; ec037d5 (#301) |
| `Shared::view_versions` shard.rs:380 | `Mutex<BTreeMap<u64, ManifestVersion>>` | Published view version → manifest version, so reader-slot pins map to extents | `publish_view` (613); open (engine.rs:954) | `oldest_live_manifest` (649), called from `reclaim`/`try_reclaim` on any thread that drops a view | Inner to `view_lock` in `publish_view` | 939440e (#63); D61 (reclaim) |
| `Shared::locks` shard.rs:395 | `Mutex<Option<Locks>>` | The writer lock and presence, held for the writer's lifetime | open (engine.rs:592); `final_close` (694, `take`) | `final_close` | Taken while holding the manifest exclusion (`try_final_close` → `final_close`); released at once (`take()` in a single statement) | c9700de (#41) |
| `Shared::default_durability` shard.rs:396 | `AtomicU8` | Writer default durability | `Engine::set_default_durability` (engine.rs:1217, app thread) | `Shared::default_durability` (671), from engine.rs commit entry points (2440, 2535, 3226) | Relaxed | c9700de (#41) |
| `Shared::closed` shard.rs:397 | `AtomicBool` | The close finished | `try_final_close` (786, Release, after `manifest::release`); `close_reader` (engine.rs:2783, Release) | `visibility_ended` (510, SeqCst); `check_open` (engine.rs:2175, Acquire); hooks.rs:1040; Debug (453, Relaxed) | Release store; Acquire, SeqCst or Relaxed loads | c9700de (#41) |
| `Shared::closing` shard.rs:399 | `AtomicBool` | `Engine::close` has started, so background compaction stops | shard thread: `handle_msg(Close)` (7297, Release) | shard thread: `maintain` (6357); tablets.rs `maybe_balance` (1064); maintenance.rs:96, 399, 632 | Release and Acquire. (`Engine::closing` at engine.rs:2681 is a different field) | c9700de (#41) |
| `Shared::maintenance` shard.rs:403 | `AtomicUsize` | App threads inside `shrink`, `backup` or a catalog change. The final close waits for them | `MaintenanceGuard::enter` (823, +1), `Drop` (835, −1) | `try_final_close` (769) | SeqCst for all, plus SeqCst fences in `enter` and in `try_final_close`. Drop then calls `try_final_close` | spec 7 F7-4; #136; 1c3404e (#159) |
| `Shared::pager_poisoned` shard.rs:406 | `AtomicBool` | A manifest root commit failed, so the pager is poisoned and writes fail until reopen | manifest pump: manifest.rs:847, 896, 968 | shard thread: `spawn_flush` (3145), `stalled` (3948), `run_group` (4190), `advance_checkpoint` (6143), `on_checkpointed` (6198, 6205), `maintain` (6361), `try_finish_close` (6960); tablets.rs `validate_op` (593), `progress_op` (755), `maybe_balance` (1065); engine.rs:2178 | Release store; Acquire loads | D58; c9700de (#41) |
| `Shared::ts_floors` shard.rs:411 | `Vec<Padded>` (`Padded(AtomicU64)`, 64-byte aligned, shard.rs:214) | Per-shard largest default timestamp assigned | own shard thread: `default_ts` (3594, store), tablets.rs `route_member` (181, fetch_max), `raise_ts_floor` (2529); open (engine.rs:920) | other shard threads: `start_coordination` (5620); manifest pump: `begin` (manifest.rs:793); maintenance.rs:234; hooks.rs:709, 722 | store Release; fetch_max AcqRel; loads Acquire | D11; c9700de (#41) |
| `Shared::ts_raises` shard.rs:414 | `Vec<Padded>` | Per-shard floor raised by shards handing it tablets | other shard threads: tablets.rs `commit_op` (832, fetch_max, under `ts_raisers[i]`) | own shard: `default_ts` (3584), tablets.rs `below_floor` (272); hooks.rs:711 | fetch_max AcqRel; loads Acquire | D11; c1b02a2 (#97) |
| `Shared::ts_raisers` shard.rs:417 | `Vec<Mutex<(Timestamp, u64)>>` | Per shard, the current raise and a bitmask of the shards that set it, so a retried commit may tie only a raise its own shards made | tablets.rs `commit_op` (823) on the shard handing tablets away | own shard: `default_ts_or` (3571); a coordinator shard: `start_coordination` (5621) | `ts_raises[i].fetch_max` happens while it is held (an atomic, not a lock). It is never held together with another lock | D11; c1b02a2 (#97) |
| `Shared::tablet_epoch` shard.rs:424 | `AtomicU64` | Bumped when a tablet change finishes or is given up, so commits refused meanwhile may retry | tablets.rs `finished_op` (928) | tablets.rs `run_retries` (400), `maybe_balance` (1024, 1122); shard.rs `start_coordination` (5674); engine.rs:360, 366 (the full-compaction round) | fetch_add AcqRel; loads Acquire | c1b02a2 (#97) |
| `Shared::full_compactions` shard.rs:427 | `AtomicUsize` | Full compactions in progress. The balancer starts no change meanwhile | engine.rs:356 (+1), 377 (`Drop`, −1), on the app thread in `Engine::compact` | tablets.rs `maybe_balance` (1066) | fetch_add/sub AcqRel; load Acquire | #94; 6944478 (#115) |
| `Shared::shard_died` shard.rs:430 | `AtomicBool` | A shard's thread panicked, or its `EngineShard` was dropped before close | `Shared::shard_died` (535), from `ShardState::abandon` (7072) and `report_panicked` (7087) | shard thread: `try_finish_close` (6961) | store SeqCst; load Acquire | #135; 3b6051d (#166) |
| `Shared::submitters` shard.rs:443 | `OnceLock<Vec<Submitter<ShardMsg>>>` | Every shard's inbox submitter, set once the runtime is built | open (engine.rs:995, 1004) | `submitter` (460), `broadcast` (474), `Engine::close` (engine.rs:2711), metrics (engine.rs:1750) | OnceLock. Submitters are runtime queues (out of scope here) | c9700de (#41) |

### `SpareTask` / `ShardState`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `ShardState::spares_running` shard.rs:2135 (= `SpareTask::running` shard.rs:1627) | `Arc<AtomicBool>` | A WAL spare-segment task is in flight (one at a time) | shard thread: `maybe_prepare_spares` (5252, swap true); background task: `SpareTask::run` (1635, store false) | `maybe_prepare_spares` | swap AcqRel; store Release | D35 (amended by D157); c9700de (#41) |

### `TimerState` (shard.rs:1646), shared between the shard and a `ClockTimer` background task

The struct is held as `Arc<TimerState>` in `ShardState::flush_retry`, `backoff_timer`, `checkpoint_timer`, `tablet_timer` and `balance_timer`, and in `RoomWait::timer`/`recheck`.

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `TimerState::cancel` shard.rs:1648 | `AtomicBool` | Set by the shard to cancel the timer | shard thread: `TimerState::cancel` (1669) | timer task: `ClockTimer::run` (1743, 1755, 1774); shard: 1682 | Release store; Acquire loads. The task re-checks it after storing its waker | #84; 361e40f |
| `TimerState::done` shard.rs:1650 | `AtomicBool` | The timer fired or gave up | timer task: `ClockTimer::run` (1784, 1788) | shard thread (1682, 1687) | Release and Acquire | #84; 361e40f |
| `TimerState::frozen_at` shard.rs:1653 | `AtomicU64` | The clock reading at which the timer gave up because the clock stopped (`u64::MAX` = it did not give up) | timer task (1783, before `done`) | shard thread (1687, 1693) | Release and Acquire; written before `done` is published | #84, #244; 361e40f |
| `TimerState::waker` shard.rs:1655 | `Mutex<Option<TaskWaker>>` | The sleeping timer task's waker, so a cancel ends the sleep at once | timer task: `ClockTimer::run` (1752, 1771) | shard thread: `cancel` (1671, `take`; wakes after releasing) | No nesting. The handshake is store `cancel` (Release) → lock/take, on one side, and lock/store waker → load `cancel` (Acquire), on the other | #93 (closes #89); d5578ed; #263 |

### Purge guards: shard thread vs. manifest pump

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `ShardState::guarded` shard.rs:2105 (the `Arc<AtomicU8>` per entry; created by `guard_state`, 1382) | `Vec<(u32, FamilyId, Arc<AtomicU8>)>`, with the atomic shared with the flush's manifest request | Per-flush delete-guard state: `GUARD_IN_FLIGHT`, `VOIDED` or `INSTALLING` | shard thread: `admit_against_guards` (5466, CAS IN_FLIGHT→VOIDED); manifest pump: the flush's catalog closure (flush.rs:661, CAS IN_FLIGHT→INSTALLING, under the `manifest` mutex) | shard thread: `on_flushed` (3306) | CAS AcqRel/Acquire; load Acquire | #287; c391e07 (#315) |
| `CompactionGuard::state` shard.rs:1406 (`ShardState::compaction_guard`) | `Arc<AtomicU8>` | The same three-state guard for a bottommost compaction's purge | shard thread: `admit_against_guards` (CAS →VOIDED); manifest pump: compaction catalog closure (compact.rs:806, CAS →INSTALLING, under the `manifest` mutex) | shard thread: `on_compaction_done` (6816); compact.rs:944 | CAS AcqRel/Acquire; loads Acquire | #316; dc1c601 (#328) |

### `LoadSlot` (shard/tablets.rs:144), `Shared::loads`, one per shard, 64-byte aligned

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `LoadSlot::writes` tablets.rs:146 | `AtomicU64` | Moving average of the rows the shard wrote per balancer interval | own shard thread: `maybe_balance` (1056) | other shard threads: `choose` (1343), only when `at` is fresh | store Relaxed; load Relaxed. Publication goes through `at` (Release/Acquire) | c1b02a2 (#97); smoothed in #103 |
| `LoadSlot::reserved` tablets.rs:150 | `AtomicUsize` | `(tablet, family)` slots promised to in-flight changes that will hand this shard tablets | any shard thread running a change: `reserve_slots` (724, fetch_add), `release_slots` (741, fetch_sub) | `slots_fit` (709), `reserve_slots` (via the `fetch_add` result) | AcqRel RMW; load Acquire | #103; b4d6acb (#128) |
| `LoadSlot::at` tablets.rs:152 | `AtomicU64` | When `writes` was published (monotonic ns; 0 = never) | own shard: `maybe_balance` (1058) | other shards: `choose` (1341) | store Release (after `writes`); load Acquire | c1b02a2 (#97) |

Test-only and not counted: `stop: AtomicUsize` in the test `a_freeze_waiter_registering_during_a_take_is_never_lost` (shard.rs:7534), shared by scoped threads, Acquire loads.

### Lock nesting observed

Order is outer → inner. "Exclusion" means the `manifest_busy` flag claimed through `manifest::claim`, which is a lock in effect.

1. **`view_lock` → `flushed_roots`** in `Shared::publish_view` (shard.rs:593 → 630). `view.store` happens inside `flushed_roots`.
2. **`view_lock` → `flushed_roots`** a second time, through the closure in `ShardState::publish_memtables` (shard.rs:2727). The closure runs under `view_lock` and calls `mem_sets` (2545). It is released before (1) takes it again.
3. **`view_lock` → `view_record`** in `publish_view` (shard.rs:602), with `shm.publish_view` called inside.
4. **`view_lock` → `view_versions`** in `publish_view` (shard.rs:613), with `shm.oldest_reader_pin` called inside.
5. **Exclusion → `view_lock` → {`view_record`, `view_versions`, `flushed_roots`}** via `manifest::end` → `publish_view` (manifest.rs:916). The `manifest` mutex is *not* held there: it is released in the block at manifest.rs:885–890.
6. **Exclusion → `manifest` mutex** in `manifest::begin` (manifest.rs:588) and `end` (887), and in `final_close` (shard.rs:714). Inside `begin`, **`manifest` mutex → guard `AtomicU8` CAS**: the flush and compaction catalog closures run under the writer (flush.rs:661, compact.rs:806). Pager-internal locks are taken under the mutex in `prepare`/`complete` (not inventoried).
7. **Exclusion → `locks`** in `try_final_close` → `final_close` (shard.rs:694).
8. **Exclusion → `CloseState::outcome`** in `try_final_close` (shard.rs:780). `outcome` is released before `manifest::release`, and `done` is taken after the release (787).
9. **`CloseState::outcome` → `CloseState::done`** in `Engine::close` (engine.rs:2694 → 2704). The `if let … && let` chain keeps the `outcome` guard alive while `done` is locked. `try_final_close` takes them one after the other, never together, so there is no inversion.
10. **`large_logged` → `large_dropped` → `large_pending`** in `LargeValues::settle_on` (large.rs:105–117), run on whichever thread resolves the commit notifier. `note_logged` takes `large_pending` then `large_logged`, but in sequence: it drops `pending` first (large.rs:221). That avoids an inversion against `settle_on`.
11. **`ts_raisers[i]` held while `ts_raises[i].fetch_max` runs** in tablets.rs `commit_op` (823–834). This is a lock plus an atomic, not two locks.
12. **`waiters.list` held while `Waker::wake()` runs** in `Shared::wake_visible` (shard.rs:565–577). The wake callback runs under the lock. Whatever the waker does (runtime or executor locks) nests inside it.
13. **`flushed_roots` held across `view.load_full()`** in `read_latest` (shard.rs:3636–3650). `ArcSwap` is not a lock, but the ordering is relied on (#131).

No `Condvar` is declared in these files. Blocking waits go through runtime `Notifier`/`completion`, which are out of scope.

**75 primitives**


## Appendix B: `engine/src/engine.rs` and `engine/hooks.rs`

Scope: struct fields, statics and thread_locals **declared** in these two files (the only
submodule of `engine` is `hooks.rs`). Neither file declares a `static` or `thread_local`.

**Scope note on `Shared`.** `Shared` is declared in `crates/engine/src/shard.rs:330`, not in
these files; `engine.rs` only constructs it (`open_writer` engine.rs:552-640, `open_reader`
engine.rs:1070-1130) and touches some of its fields. Its declaration rows are in appendix A. The cross-reference below lists every `Shared` primitive that `engine.rs` /
`hooks.rs` read or write, with the engine.rs sites and orderings, so it can be merged into
appendix A's `Shared` table. Those rows are not in the count.

### `ReaderState` (engine.rs:184): a reader process's attachment (`Inner::reader`, `Some` only for `Role::Reader`)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ReaderState::presence` (engine.rs:186) | `Mutex<Option<Presence>>` | The reader's presence lock on the file (shm-init / last-one-out byte-range locks); `take()`n once at close. | `open_reader` (init, engine.rs:1136); `Inner::close_reader` takes it (engine.rs:2755-2759). Client thread calling `close`/`Drop for Engine`. | Only `close_reader`. | Taken alone (the `shm` guard at 2753 is dropped at the end of its block, 2755). `closing.swap(AcqRel)` gates it to one caller. | Milestone A (#41, c9700de). Last-one-out cleanup: issue #20 (comment at engine.rs:2764). Presence/read-write open: D36 (doc on `open_reader`). |
| `ReaderState::shm` (engine.rs:187) | `Mutex<Attachment>` | The current shm region generation, this reader's slot, whether it holds a pin, and the generation's live-snapshot count; replaced wholesale on re-attach. | `Inner::reader_snapshot` (engine.rs:2851-2867: re-attach on `is_stale`, sets `pinned`); any client thread taking a reader snapshot. | `reader_snapshot` (pin decision, 2870-2885); `Inner::catalog` reader branch (2188-2193, reads `manifest_version()`); `close_reader` (2753, `slot.unpin()`). | Outer lock: while held in `reader_snapshot`, `ReaderState::view` (2864) and `ReaderState::catalog` (2866) are taken (re-attach branch only). Dropped (`drop(guard)`, 2886) before `reader_view`. In `catalog()` it is a statement temporary, released before `reader_catalog`. | #140 (aa37256, PR #157): reader snapshots expire across a writer restart; Attachment split out of the earlier single region field. |
| `ReaderState::catalog` (engine.rs:189) | `Mutex<(ManifestVersion, Arc<Catalog>, FileRef)>` | Cache of the last catalog loaded, its manifest version and the file it was read from. | `Inner::reader_catalog` (2798-2812, reloads root and manifest **under the lock**); `reader_snapshot` re-attach resets `.0 = NO_MANIFEST_VERSION` (2866, under `shm`). | `reader_catalog`; `Inner::catalog` reader branch (2196); `reader_view` via `reader_catalog`. | Taken under `shm` at 2866. In `reader_catalog` held across `pager.reload_root()` and `manifest::load_root` (I/O under lock), and across the test hook `ReaderHooks::before_manifest_load.run()` (2808). Released before `reader_view` takes `view`. | Milestone A (#41). Pairing record with its own manifest version: #140 (doc at 2789-2795). |
| `ReaderState::view` (engine.rs:191) | `Mutex<Option<Arc<View>>>` | The last view built from the shm record (by view version), reused when version and catalog match. | `reader_view` (2826-2843, builds `view_from_record` **under the lock**); `reader_snapshot` re-attach clears it (2864, under `shm`). | `reader_view`. | Taken under `shm` at 2864. In `reader_view` taken after `reader_catalog` returned (catalog guard already dropped), held while building the view from shm. | Milestone A (#41). |
| `Attachment::live` (engine.rs:206; `Attachment` at 199, inside `ReaderState::shm`) | `Arc<AtomicUsize>` | Snapshots of this region generation still alive; when 0 the reader slot's pin may move forward. | `reader_snapshot` `fetch_add(1, AcqRel)` (2879, under `shm` lock); `LiveSnapshot::drop` `fetch_sub(1, AcqRel)` (snapshot.rs:999, any thread dropping a reader snapshot, no lock). Fresh `Arc` on re-attach (2862). | `reader_snapshot` `load(Acquire)` (2872, under `shm`). | Acquire / AcqRel. Increment and the pin decision are under `ReaderState::shm`; the decrement is lock-free. | Milestone A (#41); generation-scoped since #140 (comment 2858-2859). |

### `Inner` (engine.rs:210): everything behind an `Engine` / `Txn`

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `Inner::runtime` (engine.rs:218) | `Mutex<Option<Runtime<ShardState>>>` | The engine-owned shard runtime (threads); `None` for application-owned mode and readers. | `open_writer` engine-owned branch stores it (996, after `shared.submitters.set`); `Inner::close` `take()`s it (2726-2731), client thread in `close` / `Drop for Engine`. | Only `close`. | Taken alone; guard is a statement temporary, released before `rt.shutdown()` (which joins shard threads; panics caught, 2734). | Milestone A (#41). Panic reporting: #135 (`shard_panicked`). |
| `Inner::closing` (engine.rs:220) | `AtomicBool` | This handle's close has started (one-shot latch so `close`/`Drop` run once). Distinct from `Shared::closing`. | `Inner::close` `swap(true, AcqRel)` (2681); `Inner::close_reader` `swap(true, AcqRel)` (2749). | `Inner::check_open` `load(Acquire)` (2175), called by every writing/maintenance Engine method and `EngineShard::commit_local`. | Acquire / AcqRel. | Milestone A (#41, c9700de). |
| `Inner::max_value` (engine.rs:225) | `AtomicUsize` | Inline value limit (D16): longer puts are separated into a blob file at routing. | `open_writer` init (984; 0 for readers, 1160); test hook `Engine::set_inline_value_limit` `store(Relaxed)` (hooks.rs:388). | `Inner::inline_limit` `load(Relaxed)` (2337) from `separate_large` (2347): client threads in `submit`/`submit_check_and_mutate`, shard thread in `EngineShard::commit_local`. | Relaxed. Atomic only so a test can shrink it (doc 222-224). | #230 (ec037d5, PR #301); D16. |

Other structs in `engine.rs` with no primitive fields: `Engine` (Arc<Inner>), `CompactRounds` (344; plain `epoch: u64`, uses `Shared::full_compactions` / `tablet_epoch`), `PendingMaintenance` (2086; owns `Waiter`s, single-owner), `Spilled` (1809; open-time only), `EngineShard` (3152; owns a `ShardDriver`), `ReplayedRecord` (326). `Waiter`/`Notifier` (`pigeonhole_runtime::completion`) are channels owned by the runtime crate; not inventoried here.

### Test hooks only (`#[cfg(feature = "test-hooks")]`, engine/hooks.rs)

`Once` (hooks.rs:44) is `struct Once(Mutex<Option<Box<dyn FnOnce() + Send>>>)`: `set` stores under the lock, `run` takes under the lock and calls the callback **after** releasing it (hooks.rs:52-57), so a `Once` never nests with the callback's own locking. Each `Once` field below is one such Mutex.

#### `Hooks` (hooks.rs:63, lives at `Shared::hooks`)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `Hooks::record` (hooks.rs:66) | `AtomicBool` | Whether `compactions` and `appended` record. | `Engine::record_history` `store(Release)` (hooks.rs:763), test thread. | Shard threads: WAL append (shard.rs:4415), shard.rs:6261; flush (flush.rs:510-514); all `load(Acquire)`. | Release / Acquire. | #148 follow-up of D164 (4777e8e, PR #261). |
| `Hooks::compactions` (hooks.rs:69) | `Mutex<Vec<CompactionRecord>>` | Every committed compaction/flush record, in commit order. | Manifest commit `begin` (manifest.rs:823-829) `extend`, after `drop(writer)` (manifest lock released, 822). | `Engine::take_compactions` (hooks.rs:771-780), `take_gc_records` (787-795), test thread. | Taken alone. | Milestone B (#63, 939440e); flush records #287. |
| `Hooks::appended` (hooks.rs:71) | `Mutex<Vec<AppendedRecord>>` | Every WAL record appended, per-stream append order. | Shard thread after each WAL append (shard.rs:4422-4428). | `Engine::take_appended` (hooks.rs:845-852). | Taken alone (not checked whether a shard-internal lock is held at shard.rs:4422; shard state there is `&mut self`, no Mutex seen). | Milestone B (#63). |
| `Hooks::manifest_race` (hooks.rs:74) | `AtomicBool` | Arms the manifest queue's release-window probe. | `Engine::probe_manifest_release_window` `store(true, Release)` (hooks.rs:1051); `manifest::race_window` `swap(false, AcqRel)` (manifest.rs:1272). | `race_window` (swap). | Release / AcqRel. | Milestone B (#63). |
| `Hooks::manifest_race_waiter` (hooks.rs:75) | `Mutex<Option<Waiter<Result<ManifestVersion>>>>` | Waiter of the request `race_window` pushed. | `race_window` (manifest.rs:1275-1279), after `manifest_queue.push`. | `probe_manifest_release_window` takes it (hooks.rs:1053-1058). | Taken alone. | Milestone B (#63). |
| `Hooks::before_view_publish` (hooks.rs:78) | `Once` | Callback at the start of the next `publish_view`. | `Engine::before_next_view_publish` (hooks.rs:889). | `Shared::publish_view` (shard.rs:593), **before** `view_lock` is taken. | Mutex released before callback. | #131 (5456f2f, PR #153). |
| `Hooks::room_wait_once` (hooks.rs:81) | `AtomicBool` | Makes the next group's second member wait for arena room. | `Engine::force_room_wait_once` `store(true, Release)` (hooks.rs:572-578); shard thread `swap(false, AcqRel)` (shard.rs:4168-4172). | Same swap. | Release / AcqRel. | #315 review (c391e07, #287). |
| `Hooks::before_snapshot_view_load` (hooks.rs:84) | `Once` | Callback between pinning a writer snapshot's seqno and loading the view. | `Engine::before_snapshot_view_load` (hooks.rs:585). | `Inner::snapshot` (engine.rs:2611), client thread, while holding a `SeqnoPin` (not a lock guard). | — | #315 review (c391e07). |
| `Hooks::before_latest_view_load` (hooks.rs:88) | `Once` | Callback in unpinned reads-as-of-now between the visible-seqno read and the view load. | `Engine::before_latest_view_load` (hooks.rs:594). | `Engine::get_latest` (engine.rs:1362), `Inner::latest_view` (engine.rs:2591). | — | #320 (010e824, PR #392). |
| `Hooks::before_shrink_relocates` (hooks.rs:91) | `Once` | Callback in a shrink round between catalog read and relocations. | `Engine::before_shrink_relocates` (hooks.rs:565). | maintenance.rs:435 (manifest guard there is a statement temporary, released). | — | #138 (6e20df3, PR #200). |
| `Hooks::after_backup_releases_memtables` (hooks.rs:94) | `Once` | Callback after backup released its memtables, before its merge. | `Engine::after_backup_releases_memtables` (hooks.rs:609-613). | maintenance.rs:150. | — | #262 (9f352b2, PR #268). |
| `Hooks::before_shrink_commits` (hooks.rs:97) | `Once` | Callback in a committing shrink round, after copies, before commit. | `Engine::before_shrink_commits` (hooks.rs:622). | maintenance.rs:607. | — | #138 (6e20df3). |
| `Hooks::manifest_park` (hooks.rs:100) | `AtomicBool` | Parks background compaction manifest commits before `end`. | `Engine::park_manifest_commits` `store(Release)` (hooks.rs:933). | `manifest::parks` `load(Acquire)` (manifest.rs:1307), called from `manifest::parked` under `manifest_flight.lock()` (manifest.rs:1289-1293). | Release / Acquire. | #78 / #81 (ff30866). |
| `Hooks::manifest_parked` (hooks.rs:101) | `Mutex<Option<TaskWaker>>` | Waker of the parked manifest pump. | `manifest::parked` stores (manifest.rs:1295-1299), **after** the `manifest_flight` lock block ends (1289-1294); `park_manifest_commits(false)` takes (hooks.rs:934-941) and wakes outside the lock. | `Engine::manifest_commit_parked` (hooks.rs:1015-1022). | Not nested with `manifest_flight` (block-scoped). | #78 / #81 (ff30866). |
| `Hooks::compaction_hold` (hooks.rs:104) | `AtomicBool` | Holds each compaction job before its first slice. | `Engine::hold_compactions` `store(Release)` (hooks.rs:979). | Compaction job `load(Acquire)` (compact.rs:919-924), background compaction thread / shard. | Release / Acquire. | #316 (dc1c601, PR #328). |
| `Hooks::compaction_held` (hooks.rs:105) | `Mutex<Option<TaskWaker>>` | The held compaction job's waker; `None` after release. | Compaction job (compact.rs:926-935) stores its waker; `Engine::release_held_compaction` takes and wakes outside the lock (hooks.rs:987-999). | `Engine::compaction_held` (hooks.rs:1003-1011); compaction job checks `is_none()` for release. | Taken alone. | #316 (dc1c601). |
| `Hooks::refuse_checkpoints` (hooks.rs:108) | `AtomicBool` | Refuse checkpoint-only manifest batches with `NoSpace`. | `Engine::refuse_checkpoints` `store(Release)` (hooks.rs:896-901). | `manifest::begin` `load(Acquire)` (manifest.rs:809), while holding `Shared::manifest` (`writer` guard from manifest.rs:587). | Release / Acquire. | #139 (e1a10fd, PR #179). |
| `Hooks::omit_blob_refs` (hooks.rs:111) | `AtomicBool` | Commit no `SstBlobRefs`, as pre-tag-13 builds. | `Engine::omit_blob_refs` `store(Release)` (hooks.rs:908-913). | `manifest::begin` `load(Acquire)` (manifest.rs:804), under `Shared::manifest`. | Release / Acquire. | #240 (acc7e6c, PR #285). |
| `Hooks::fail_next_apply` (hooks.rs:114) | `AtomicBool` | Fail the next single-shard batch's apply with `Busy` after its WAL append. | `Engine::fail_next_apply` `store(true, Release)` (hooks.rs:920-925); shard thread `swap(false, AcqRel)` (shard.rs:4542-4546). | Same swap. | Release / AcqRel. | #230 (ec037d5, PR #301). |
| `Hooks::flush_gc_mutation` (hooks.rs:118) | `AtomicU8` | Deliberate flush-GC fault selector (0 none, 1-3 mutations). | `Engine::mutate_flush_gc` `store(Release)` (hooks.rs:744-756). | `flush_gc` `load(Acquire)` (flush.rs:304-308); shard.rs:5422 `load(Acquire)`. | Release / Acquire. | #287 (c391e07, PR #315). |

#### `ShardCounters` (hooks.rs:139, lives at `ShardMetrics::hooks`, one per shard)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ShardCounters::aborted` (hooks.rs:141) | `AtomicU64` | Size of the shard's aborted-seqno set after its last batch. | Owning shard thread `store(Relaxed)` (shard.rs:7414-7415). | `Engine::aborted_seqnos` `load(Relaxed)` (hooks.rs:805). | Relaxed. | #144 (e18b2b5, PR #160). |
| `ShardCounters::arena` (hooks.rs:145) | `Mutex<(u64, u64, u64)>` | One consistent snapshot of arena free bytes, largest free run, size. | Owning shard `publish_arena` (shard.rs:2483-2488). | `Engine::arena_free` (hooks.rs:832-838). | Taken alone. A Mutex (not 3 atomics) so a reader never mixes batches. | #141; made one snapshot in #336 (ff6ad97). |
| `ShardCounters::run_waits` (hooks.rs:148) | `AtomicU64` | Reservations with enough free bytes but no long-enough run. | Owning shard `fetch_add(1, Relaxed)` (shard.rs:2945-2948). | `Engine::arena_run_waits` `load(Relaxed)` (hooks.rs:812-816). | Relaxed. | #141 (a62da94, PR #186). |
| `ShardCounters::purge_voids` (hooks.rs:151) | `AtomicU64` | Compactions whose purge a write voided before install. | Owning shard `fetch_add(1, Relaxed)` (shard.rs:6847-6850). | `Engine::compaction_purge_voids` `load(Relaxed)` (hooks.rs:822-826). | Relaxed. | #316 (dc1c601, PR #328). |

#### `ReaderHooks` (hooks.rs:156, lives at `ReaderState::hooks`, engine.rs:195)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ReaderHooks::after_record` (hooks.rs:158) | `Once` | Callback between reading the view record and loading its catalog. | `Engine::on_reader_view_record` (hooks.rs:950-954). | `Inner::reader_view` (engine.rs:2822), no reader lock held (shm guard dropped at 2886). | — | #140 (aa37256, PR #157). |
| `ReaderHooks::before_manifest_load` (hooks.rs:161) | `Once` | Callback after the durable root matched the record's version, before the manifest read. | `Engine::on_reader_manifest_load` (hooks.rs:959-963). | `Inner::reader_catalog` (engine.rs:2808), **while holding `ReaderState::catalog`**. | Callback runs under `ReaderState::catalog` (the Once's own mutex is released first). | #140 (aa37256). |

### Cross-reference: `Shared` primitives (declared in shard.rs, appendix A) as used from engine.rs and hooks.rs

Not counted. Sites listed are only those in my two files; other writers/readers live in shard.rs, manifest.rs, flush.rs, compact.rs, maintenance.rs, etc.

| Field (shard.rs decl.) | Type | engine.rs / hooks.rs sites | Ordering / nesting at those sites |
|---|---|---|---|
| `Shared::view` (shard.rs:334) | `ArcSwap<View>` | Writer: `open_writer` `store` first view (958). Readers (client threads, any shard via `commit_local`): `get_latest` (1363), `shard_of` (1660), `shard_stats` (1698), `Inner::catalog` (2198), `catalog_change` (2213, `load_full`), `submit` (2443), `submit_check_and_mutate` (2536), `latest_view` (2592), `snapshot` (2612, `load_full`), `commit_local` (3227), hooks `reader_pin_and_view` (hooks.rs:970). | ArcSwap (SeqCst internally). In open: `view_versions` lock (953-957, temporary) then `view.store` (958) then `view_lock` (959-962, temporary): sequential, not nested. Snapshot: seqno pinned before view load (comment 2603-2606). |
| `Shared::view_lock` (shard.rs:341) | `Mutex<u64>` | `open_writer` sets 1 (959-962). | Alone. |
| `Shared::view_versions` (shard.rs:375) | `Mutex<BTreeMap<u64, ManifestVersion>>` | `open_writer` inserts (1, v) (953-957). | Alone. |
| `Shared::manifest` (shard.rs:345) | `Mutex<ManifestWriter>` | `sweep_unreferenced_blobs` commit (1961-1965), `flush_recovered` commit (2017-2021); open thread. | Alone; held across the manifest commit I/O. |
| `Shared::sst_ids` (shard.rs:353) | `Arc<AtomicU64>` | `spill_recovered` clones the Arc (1880); `flush_recovered` `load(Relaxed)` (2006). | Relaxed (open thread only). |
| `Shared::default_durability` (shard.rs:388) | `AtomicU8` | `Engine::set_default_durability` `store(Relaxed)` (1214-1219); reads via `Shared::default_durability()` (`load(Relaxed)`, shard.rs:671) at 1210, 2440, 2535, 3226. | Relaxed. |
| `Shared::closed` (shard.rs:389) | `AtomicBool` | `check_open` `load(Acquire)` (2175); `close_reader` `store(true, Release)` (2783); hooks `close_finished` `load(Acquire)` (hooks.rs:1040). | Acquire / Release. |
| `Shared::pager_poisoned` (shard.rs:398) | `AtomicBool` | `check_open` `load(Acquire)` (2178). | Acquire. D58. |
| `Shared::close.done` (CloseState, shard.rs:201) | `Mutex<Option<Notifier<Result<()>>>>` | `Inner::close` stores (2684-2689) then may take it (2700-2705). | See nesting N1: taken while `close.outcome` is held. |
| `Shared::close.outcome` (shard.rs:208) | `Mutex<Option<Result<()>>>` | `Inner::close` (2693-2699), `EngineShard::closed` (3199-3207). | N1. shard.rs:778-795 (final close) takes them sequentially, outcome then done, not nested, so no inversion. |
| `Shared::close.final_pending` (shard.rs:206) | `AtomicBool` | hooks `final_close_pending` `load(Acquire)` (hooks.rs:1032). | Acquire. |
| `Shared::ts_floors[i]` (shard.rs:406) | `Vec<Padded(AtomicU64)>` | `open_writer` `store(Release)` (920). | Release (open thread). D11. |
| `Shared::tablet_epoch` (shard.rs:418) | `AtomicU64` | `CompactRounds::new`/`again` `load(Acquire)` (360, 366). | Acquire. #94. |
| `Shared::full_compactions` (shard.rs:421) | `AtomicUsize` | `CompactRounds::new` `fetch_add(1, AcqRel)` (356); `Drop for CompactRounds` `fetch_sub(1, AcqRel)` (377). | AcqRel. #94. |
| `Shared::submitters` (shard.rs:435) | `OnceLock<Vec<Submitter<ShardMsg>>>` | `open_writer` `set` (995, 1004) before the runtime lock store / before shards are handed out; `get` in `metrics` (1750), `close` (2711), `Shared::submitter` (expects set). | OnceLock. |
| `Shared::async_sync_reads`, `commit_parks` (shard.rs:336, 338) | `AtomicU64` | `Engine::metrics` `load(Relaxed)` (1747, 1749). | Relaxed. D196, ICR 0027. |
| `Shared::metrics[i].*` counters, `.wal` (ShardMetrics) | atomics, `OnceLock` (`wal`, inferred from `.set`/`.get`) | `metrics` loads all `Relaxed` (1712-1744); `tablet_refusals` (1674); `shard_stats` (1688); `open_writer` `wal.set(...)` (854, 878, 910). | Relaxed. |
| `Shared::waiters.{waits,scanned,passes,woken,contended}` (VisibilityWaiters) | atomics | `metrics` `load(Relaxed)` (1757-1766). | Relaxed. |
| `Shared::live_seqnos` | `Arc<LiveSeqnos>` (has internal locks, not checked) | `Inner::snapshot` `SeqnoPin::pin_visible` (2607-2608). | Internal. |
| `Shared::drivers` | `waker::Drivers` (internals not checked) | `EngineShard::run_once` `enter` (3176), `Drop for EngineShard` `dropped` (3280-3283), `close` `driven_elsewhere` (2720). | Internal. |
| `Shared::maintenance` | `AtomicUsize` | Via `MaintenanceGuard::enter` from `Inner::enter_maintenance` (2170-2172); internals in shard.rs. | Not checked here. |

### Lock nesting observed (engine.rs / hooks.rs)

- **N1** `Inner::close` (engine.rs:2693-2706): `Shared::close.outcome` lock is held (if-let-chain temporary lives through the condition and body) while `Shared::close.done` is locked and `take()`n, and `tx.notify(...)` runs under `outcome`. Order: `close.outcome` -> `close.done`. The only other site taking both, the final close (shard.rs:778-795), takes them one after the other (outcome released before done), so no inversion. (Earlier at 2684-2689 `done` is locked alone, statement temporary.)
- **N2** `Inner::reader_snapshot` (engine.rs:2851 -> 2864): `ReaderState::shm` -> `ReaderState::view` (re-attach branch, statement temporary).
- **N3** `Inner::reader_snapshot` (engine.rs:2851 -> 2866): `ReaderState::shm` -> `ReaderState::catalog` (re-attach branch, statement temporary).
- **N4** `Inner::reader_catalog` (engine.rs:2798-2812): `ReaderState::catalog` held while running the test hook `ReaderHooks::before_manifest_load` (its `Once` mutex taken then released before the callback) and across `pager.reload_root()` / `manifest::load_root` (pager internals may lock; not checked). Test hook nesting only: `catalog` -> `Once` mutex.
- **N5** `Inner::reader_view` (engine.rs:2826-2842): `ReaderState::view` held across `view_from_record` (shm reads, block cache `Arc` clone); no other engine-level lock taken inside as far as these files show.
- Not nested (checked): `Inner::catalog` reader branch takes `shm` (statement temporary, 2188-2193), then `reader_catalog` takes `catalog`, then `catalog` again at 2196 (separate statements). `close_reader` takes `shm` (block, 2752-2755) then `presence` (2756-2759) sequentially. `reader_view`'s `catalog` (inside `reader_catalog`) is released before it takes `view`, so no `catalog` -> `view` order exists. `open_writer` locks `view_versions`, then `view_lock` sequentially (953-962). `close` releases `runtime` before `rt.shutdown()`.
- No condvar waits in these files. Blocking waits outside locks: `rx.wait()` in `Inner::close` (2722, no lock held), `Waiter::wait` in `PendingMaintenance::wait` (2100, no lock held), `std::thread::sleep`/`yield_now` in `reader_snapshot` (2913-2918, no lock held: `shm` dropped at 2886, `view`/`catalog` released by then).
- Consistent order across the reader path: `ReaderState::shm` before `view` / `catalog`; `catalog` and `view` are never held together.

Uncertainties: `Drivers`, `LiveSeqnos`, `MaintenanceGuard`, `Waiter`/`Notifier` internals were not inspected.

**34 primitives** declared in these files: 8 production (`ReaderState` 4, `Attachment` 1, `Inner` 3) + 26 test-hook-only (`Hooks` 14 atomics/mutexes + 6 `Once`, `ShardCounters` 4, `ReaderHooks` 2 `Once`). `Shared` appendix rows (declared in shard.rs) are not counted.


## Appendix C: The other `engine/src` files (manifest, snapshot, read path, flush, compaction, wakers)

Files covered: catalog.rs, compact.rs, error.rs, flush.rs, large.rs, lib.rs, maintenance.rs,
manifest.rs, nonblocking.rs, options.rs, read.rs, row_cache.rs, snapshot.rs, source.rs,
waker.rs, write.rs. catalog.rs, error.rs, lib.rs, options.rs, large.rs, maintenance.rs and
nonblocking.rs declare no primitive of their own (large.rs, maintenance.rs and nonblocking.rs
use `Shared` fields declared in shard.rs; see "Used here, declared elsewhere" below).

**34 primitives** (24 struct fields / statics + 10 `thread_local!`s).

### Manifest commit path (manifest.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ManifestQueue::reqs` manifest.rs:426 | `Mutex<Vec<ManifestReq>>` | The pending manifest change requests (a `Mutex<Vec>` used as an MPSC queue, taken whole). | `ManifestQueue::push` from `manifest::submit` (shard threads: flush.rs:702, compact.rs:836, shard/tablets.rs:853, shard.rs:6159; large.rs:176 `LargeValues::release` on any thread), `commit_held`, `commit_req_from_thread` (application threads), test hook `race_window`. | `take` in `manifest::begin` (whoever holds `manifest_busy`: pump on a shard thread, or an application thread in `drain_sync`); `is_empty` in `ManifestPump::run` and the re-check loop of `commit_req_from_thread` / hooks.rs:878. | Leaf lock: held only for push/take/is_empty. `begin` takes it (manifest.rs:582) before locking `Shared::manifest` (589), never inside. The push happens-before the `take` through this mutex, which is what lets `begin` read `sst_ids`/`blob_ids` with `Relaxed`. Poison ignored (`PoisonError::into_inner`). | Milestone B (#63, 939440e). |
| `Flight::state` manifest.rs:1036 | `Mutex<FlightState>` (`generation`, `commit: Option<Commit>`, `result: Option<RootResult>`) | The one manifest commit whose root commit is in flight, held in `Shared` (as `Shared::manifest_flight`, shard.rs:432) so a blocked application thread can finish it. | `Flight::start` (`ManifestPump::start`, shard thread, under `manifest_busy`); `Flight::complete` (the root-commit completion callback, on the thread that reaps the pager I/O); `stage` / `take_done` / `abandon` take `commit`+`result` (pump `run`, `commit_req_from_thread`, `ManifestPump::drop`). | `ready` (`commit_req_from_thread` before parking), `parked` (test hook). | Leaf: guard is dropped before `wake_threads` in `complete` (manifest.rs:1087-1093) and before the hooks lock in `parked` (1289-1300). `take_done` loads `hooks.manifest_park` (Acquire) while holding it (test-hooks only). `generation` lets a pump recognise its own commit (`Stage::Gone` if a thread finished it). | #135 via #166 (3b6051d): "otherwise the exclusion would stay held until the pump's shard runs again, which never happens when the blocked thread is the one that drives it (application-owned mode)". D88/D150. |
| `Flight::threads` manifest.rs:1039 | `Mutex<Vec<std::task::Waker>>` | Thread wakers of application threads parked in `commit_req_from_thread`. | `Flight::register` (application threads, deduped by `will_wake`); `wake_threads` empties it. | `wake_threads`, called by `Flight::complete` (I/O completion) and `manifest::release` (any exclusion holder). | Leaf; wakers are woken after the guard is dropped (manifest.rs:1148-1155). | #135, #166. |
| `Flight::thread_count` manifest.rs:1040 | `AtomicUsize` | Length of `threads`, so `wake_threads` skips the lock when no thread waits. | `register` (`store`, SeqCst, under `threads` lock); `wake_threads` (`store(0)`, SeqCst, under lock). | `wake_threads` fast path (`load`, SeqCst, no lock). | SeqCst on all three. Pairs with the waiter's sequence register → `manifest_busy.load(SeqCst)` / `ready()` (manifest.rs:1245-1250) against the releaser's `manifest_busy.store(Release)` + `fence(SeqCst)` → `thread_count.load(SeqCst)`: either the releaser sees the count, or the waiter sees the exclusion free (Dekker-style). | #135, #166. |

### Views, snapshots and pins (snapshot.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `OpenSst::reader` snapshot.rs:184 | `OnceLock<Arc<SstReader>>` | An SST's reader, opened lazily on first use, or pre-set by the flush/compaction that wrote it. | `OpenSst::new` (pre-set; `SstSet::rebuild` inside `manifest::end`'s `publish_view` closure, under `Shared::view_lock`); `reader` / `reader_cache_only` (any thread reading through a `View`: client threads, async read futures, shard threads, flush/compaction tasks). | Same paths (`get`). | OnceLock internal sync. Racing openers both open; the loser's `set` fails and it returns the winner's reader (`self.reader.get().map_or(r, ..)`, snapshot.rs:238). | Milestone B (#63). Pre-set so "a hot path never opens one". |
| `OpenBlob::reader` snapshot.rs:267 | `OnceLock<Arc<BlobReader>>` | A blob file's reader, built on first use (no I/O). | `OpenBlob::reader` via `get_or_init` (any reading thread, blob GC/compaction). | Same. | OnceLock; `get_or_init` (one initializer runs). | Blob separation, #33 via #235 (cbcfaf6). |
| `LiveViews::versions` snapshot.rs:713 | `Mutex<BTreeMap<ManifestVersion, u32>>` | Refcount of manifest versions of every live in-process `View`, so the pager reclaims only extents no view reaches. | `register` from `ViewPin::new` (inside `Shared::publish_view`'s closure, manifest.rs:960, and other publishers in shard.rs); `unregister` from `ViewPin::drop` (any thread dropping the last `Arc<View>`). | `oldest` from `Shared::oldest_live_manifest` (shard.rs:644) → `reclaim` (manifest `end`, shrink, `try_reclaim`). | Taken under `Shared::view_lock` when a view is published (`publish_view` → closure → `ViewPin::new`). Released before `on_oldest_released` runs (`unregister` returns, then the callback; snapshot.rs:790-794). `oldest()` releases it before `oldest_live_manifest` locks `view_versions`. | D61; Milestone B (#63). |
| `LiveViews::on_oldest_released` snapshot.rs:719 | `OnceLock<Box<dyn Fn() + Send + Sync>>` | Callback run when the last view of the oldest version drops (calls `Shared::try_reclaim` through a `Weak`). | Set once at open (engine.rs:650). | `ViewPin::drop` on the dropping thread. | OnceLock; callback runs outside the `versions` lock and must never block. | #337 (a837d06), the large_values flake. |
| `LiveSeqnos::seqnos` snapshot.rs:801 | `Mutex<BTreeMap<Seqno, u32>>` | Refcount of every live in-process snapshot seqno, for GC's `GcPolicy::snapshots`. | `SeqnoPin::pin_visible` (client thread taking a snapshot, engine.rs:2608); `SeqnoPin::drop` (any thread). | `LiveSeqnos::list` from `compact::gc_snapshots` (compact.rs:343: flush and compaction planning/tasks on shard threads). | `pin_visible` reads the visible seqno (`visible()` = `shm.visible_seqno()`, an atomic load) while holding the lock, in one critical section with `gc_snapshots`'s read of the list. | Struct #63; the in-lock read is from #315's review (#287, D191). D70. |
| `LiveSnapshot::count` snapshot.rs:975 | `Arc<AtomicUsize>` (shared with the reader attachment's `live`, engine.rs:206) | Number of live snapshots of a **reader process** in the current region generation; at zero the reader moves its shm slot pin forward. | `fetch_add(AcqRel)` at engine.rs:2879 (reader process taking a snapshot); `fetch_sub(AcqRel)` in `LiveSnapshot::drop` (any thread). | `load(Acquire)` at engine.rs:2872 (next snapshot decides whether to re-pin). | AcqRel / Acquire. The struct's `check_current` also issues `fence(Acquire)` before `shm.is_stale()` (seqlock-style validation, #140). | Milestone A (#41, c9700de); expiry check #140. |

### Read path (read.rs, source.rs, row_cache.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ResolverBlobs::error` read.rs:281 | `Mutex<Option<Error>>` | First error a separated-value fetch hit during a resolve (the `BlobFetch` hook cannot return errors). | `BlobFetch::fetch` (read.rs:324) on the resolving thread. | `ResolverBlobs::check` after each resolver step (same thread). | Leaf. In practice single-threaded per read; the Mutex exists because `BlobFetch: Send + Sync` (compaction/src/blob.rs:314). | #33 via #235 (cbcfaf6). |
| `static READAHEAD` source.rs:226 (fn-local in `scan_readahead`) | `OnceLock<(u32, bool)>` | Process-wide `PIGEONHOLE_READAHEAD` override (blocks, merge), parsed once. | `get_or_init` on first scan (any thread). | Every scan's `read_options`. | OnceLock. | #402 via #432 (5b9f496); to be decided by #431/#405. |
| `RowCaches::present` row_cache.rs:50 | `Box<[AtomicU64]>` | Per slot, the tag of the family row filled last: a lookup whose tag differs misses without taking the cache's lock. A hint only. | `RowCaches::store` (`store`, Relaxed) after `cache.insert` (row-cache fill on a client thread or async read future: engine.rs:1369/1549, nonblocking.rs:256/366). | `RowCaches::get` (`load`, Relaxed). | Relaxed: correctness comes from the `RowCache` itself verifying family/row/epoch under its own lock and from the `RowEpochs` (`lw`) watermark protocol (fetch_max Release / load Acquire, in the cache crate). | #404, D201, ICR 0019 via #446 (e5f8359). |
| `RowCaches::ghost` row_cache.rs:54 | `Box<[AtomicU64]>` | Per slot, the family row that missed last: a miss fills only on the second miss (admission). | `RowCaches::admit` (`swap`, Relaxed) on reading threads. | Same `swap`. | Relaxed; a lost update only costs an extra miss. | #404, D201. |
| `RowCaches::hits` row_cache.rs:61 | `AtomicU64` | Row-cache hit counter. | `get` (`fetch_add`, Relaxed), reading threads. | `stats` (`load`, Relaxed) for `Engine::row_cache_stats` (engine.rs:1646). | Relaxed statistic. | #404, D201. |
| `RowCaches::misses` row_cache.rs:62 | `AtomicU64` | Row-cache miss counter. | `get` (`fetch_add`, Relaxed). | `stats` (Relaxed). | Relaxed statistic. | #404, D201. |
| `RowCaches::fills` row_cache.rs:63 | `AtomicU64` | Row-cache fill counter. | `insert` (`fetch_add`, Relaxed). | `stats` (Relaxed). | Relaxed statistic. | #404, D201. |

### Shard drivers and commit waits (waker.rs, write.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `Drivers.0` waker.rs:70 (`Shared::drivers`) | `Box<[AtomicU64]>` | Per application-owned shard, the token of the thread that last drove it (0 never, `u64::MAX` dropped): such a thread must not block on shard work. | `Drivers::enter` from `EngineShard::run_once` (engine.rs:3176; `load` Relaxed, `store` Release only if changed); `Drivers::dropped` from `EngineShard::drop` (engine.rs:3282, `store` Release). | `current_drives` (write.rs:493/534/571/593, shard.rs:545; `load` Acquire), `driven_elsewhere` (engine.rs:2720 close; `load` Acquire). | Release / Acquire; Relaxed self-check in `enter`. | D88 (amended by D150, D151), #135 via #166 (3b6051d). |
| `static NEXT` waker.rs:78 (inside `TOKEN`'s initializer) | `AtomicU64` | Allocator of per-thread driver tokens (starts at 1). | `TOKEN` initialisation on each thread (`fetch_add`, Relaxed). | Same. | Relaxed (uniqueness only). | #135, #166. |

### Flush / compaction / ids (flush.rs, compact.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `SstSink::sst_ids` flush.rs:91 | `Arc<AtomicU64>` (a clone of `Shared::sst_ids`, shard.rs:353, or a local counter for a backup copy, maintenance.rs:89) | Next SST id; each output SST takes one. | `SstSink` cutting an output (flush.rs:171, `fetch_add` Relaxed): flush tasks on shard threads, engine.rs:1878, maintenance `copy_at`. Also maintenance.rs:511 on `Shared::sst_ids` directly (shrink). | `manifest::begin` (manifest.rs:789, `load` Relaxed) into `Counters`; maintenance.rs:230 for the copy's counters. | Relaxed. A request naming an id is pushed through `ManifestQueue::reqs` (Mutex) after the `fetch_add`, so the `Relaxed` load in `begin` that follows the `take` sees a value above every id it commits. | #63 (939440e). `blob_ids` (flush.rs:124) is only a parameter, moved into `pigeonhole_compaction::BlobSink`. |
| `FlushItem::guard` flush.rs:57 | `Option<Arc<AtomicU8>>` (`GUARD_IN_FLIGHT` 0 / `VOIDED` 1 / `INSTALLING` 2, shard.rs:1377) | The flush GC guard (#287): versions beyond `max_versions` may be purged only if no delete in the family voided the guard before the commit installs it. | Created by the shard (`guard_state`, shard.rs:1387); voided by the shard on a delete (CAS IN_FLIGHT→VOIDED, shard.rs:5468); claimed by the flush's `ReqKind::Catalog` closure (flush.rs:661, CAS IN_FLIGHT→INSTALLING, AcqRel/Acquire) that runs inside `manifest::begin` (pump or application thread). | Shard (`load` Acquire, shard.rs:3306/6816). | CAS AcqRel / Acquire; the CAS runs under the `Shared::manifest` writer Mutex (inside `begin`). A failed claim refuses the request (`Error::Busy`) and `on_refusal` frees its outputs. | #287 via #315 (c391e07), D191. |
| `FlushTask::install_seqnos` flush.rs:394 (test-hooks) | `Vec<Arc<AtomicU64>>` (each the `SeqnoCell` of a `CompactionRecord`) | Where the commit records the visible seqno at which a guarded purge installed. | The flush's catalog closure (flush.rs:678, `store` Release, under the manifest writer lock). | Tests via `SeqnoCell::get` (Acquire). | Release / Acquire. | #287, #315. |
| `SeqnoCell.0` compact.rs:74 | `Arc<AtomicU64>` | The install seqno of a flush record (test hook), compared by value. | As `install_seqnos`. | `SeqnoCell::get` (`load` Acquire) in tests / `PartialEq`. | Acquire. | #287, #315 (c391e07). |
| `CompactionWork::guard` compact.rs:479 | `Option<Arc<AtomicU8>>` | A bottommost purge's guard (#316): installs only if no write below its `min_ts_above` voided it since the plan. | Shard voids it (`Shard::admit_against_guards`); compaction's catalog closure claims it (compact.rs:808, CAS IN_FLIGHT→INSTALLING, AcqRel/Acquire) inside `manifest::begin`. | `CompactionWork::run` (compact.rs:944, `load` Acquire) aborts early when VOIDED. | As `FlushItem::guard`. | #316, #328, D192. |

### Thread-locals

All are per-thread; none is shared across threads. Listed because they carry cross-call state
(buffers handed between threads come back through them).

| Name | Type | Means | Writers / readers | Notes | Why / history |
|---|---|---|---|---|---|
| `SCAN_RESOLVERS` read.rs:1244 | `RefCell<Vec<Resolver>>` | Pool of up to 4 (`SCAN_RESOLVERS_KEPT`) resolvers a scanning thread reuses. | `take_scan_resolver` / lane end / `ScanCursor::drop` (`recycle_lanes`). | `try_with`: falls back to a new resolver during TLS teardown. Pool entries hold no source, so pin nothing. | #287 via #384 (db7b4e9). |
| `ROW_RESOLVER` read.rs:1390 | `Cell<Option<Resolver>>` | The resolver row reads refill in place. | Taken while in use (a nested read gets its own), put back after. | | #287 via #367 (0fe99db). |
| `BETWEEN_ROW_FAMILIES` read.rs:1593 (test-hooks) | `RefCell<Option<Box<dyn FnOnce()>>>` | Test hook run once before a row read's second family. | `Engine::between_row_read_families`; `run_between_row_families`. | | #320 via #392 (010e824). |
| `POINT_RESOLVER` read.rs:1610 | `RefCell<Option<Resolver>>` | The resolver point gets refill in place. | Point gets; dropped if a value ≥ 64 KiB (`KEEP_BUFFERS_BELOW`). | | #46 via #361 (b847393). |
| `ASYNC_READ` read.rs:2204 | `Cell<Option<(bool, u64)>>` | While an async read attempt runs on this thread: cache-only flag and count of synchronous file reads. | `as_async_read` (restored by a drop guard even on panic), `note_sync_read`, `async_read_mode`. | The count feeds `Shared::async_sync_reads` (Relaxed `fetch_add`, nonblocking.rs:166/199). | #42, D196, ICR 0014 via #411 (7b560a8). |
| `KEY_SCRATCH` source.rs:318 | `Cell<Vec<u8>>` | Scratch buffer for a point read's `ColumnKey`. | `ColumnKey::new` takes it; returned on drop. | | #46 via #376 (43dd4ec). |
| `TOKEN` waker.rs:77 | `u64` (lazy) | This thread's driver token for `Drivers`. | Initialised from `static NEXT`. | | #135, #166. |
| `RECYCLED_BATCH` write.rs:66 | `Cell<Option<BatchBuilder>>` | A committed batch's buffer, returned by the shard with the reply, for this thread's next `WriteBatch` (≤ 64 KiB). | `recycle` after `wait`; taken by the next batch (write.rs:96). | The buffer crosses threads inside the `Settled` reply, not through the TLS. | #320 via #458 (0bc0926). |
| `COMMIT_SLOTS` write.rs:352 | `SlotCache<Result<Settled>>` (pigeonhole-runtime) | Spare completion slots a thread's commits reuse. | `commit_completion` (`completion_from`); `PendingCommit::drop` (`recycle_into`). | A slot comes back to the thread that drops the `PendingCommit`, after the shard released it. | #320 via #498 (07a0fe2). |
| `SPIN_BACKOFF` write.rs:418 | `Cell<(u32, u32)>` | (polls to skip, backoff exponent) for this thread's commit spin-waits. | `SpinWait::new` / write.rs:475. | | D198 C1, ICR 0016 via #421 (1741d2e). |

### Adjacent, not counted

- One-shot completions as struct fields: `PendingCommit::waiter` write.rs:338 and write.rs:647
  (`pigeonhole_runtime::Waiter`), `ManifestReq::reply` manifest.rs:376 (boxed closure, usually
  resolving a `Notifier`), `Reading::fetch` nonblocking.rs:81 and `AsyncScan::fetch` read.rs:673
  (`pigeonhole_io::Completion`). Their synchronization lives in the runtime / io crates.
- `StdWaker` (waker.rs:26; fields in `ManifestPump`, `FlushTask`, `CompactionWork`): a
  per-task cached `std::task::Waker`, `&mut` only, no sync.
- Locals: `sst_ids`/`blob_ids` in maintenance.rs:89-90 (one thread, `Arc` only to fit `SstSink`);
  `guards` vec in flush.rs:644 (moved into the closure).

### Used here, declared elsewhere (for cross-reference with the shard.rs / engine.rs inventory)

`Shared::manifest` (Mutex<ManifestWriter>, shard.rs:345), `manifest_queue` (346), `manifest_busy`
(AtomicBool, 348), `manifest_flight` (432), `view_lock` / `view` (publish_view, shard.rs:587),
`pager_poisoned` (Release stores in manifest.rs:847/896/968), `closing` (Acquire loads in
maintenance.rs:96/399/632), `large_open` (AcqRel, large.rs:84/184), `large_logged` /
`large_dropped` / `large_pending` (Mutexes, large.rs), `ts_floors` (Acquire, manifest.rs:795,
maintenance.rs:236), `sst_ids` / `blob_ids` (Relaxed), `async_sync_reads` (Relaxed,
nonblocking.rs), `commit_parks` (Relaxed, write.rs:520), `TAIL_SKIPS` (Relaxed, source.rs:129),
`close.final_pending` / `maintenance` (SeqCst, in `try_final_close`), test hooks
(`hooks.manifest_race`, `manifest_park`, `manifest_parked`, `manifest_race_waiter`,
`compactions`, `compaction_hold`, `compaction_held`, `omit_blob_refs`, `refuse_checkpoints`,
`flush_gc_mutation`, `record`).

### The manifest exclusion and pump (manifest.rs)

There is one manifest writer (`Shared::manifest: Mutex<ManifestWriter>`) and at most one
commit in flight. Requests (`ManifestReq`: edits or a catalog/tablet closure, readers, flushed
memtable roots, a `reply` closure) are pushed onto `Shared::manifest_queue`
(`ManifestQueue::reqs: Mutex<Vec<_>>`). The right to turn the queue into a commit is the
**exclusion** `Shared::manifest_busy: AtomicBool`, a try-lock, not a blocking lock:
`claim` = `!swap(true, AcqRel)` (manifest.rs:1013); `release` = `store(false, Release)`, then
`fence(SeqCst)` (paired with the fence in `Shared::try_final_close`, so either the releaser runs
the pending final close or the closer claims the freed exclusion: #81, closing #78), then
`try_final_close()`, then `manifest_flight.wake_threads()` (manifest.rs:1018-1025). `begin`
and `end` must be called with the exclusion held; each takes the writer Mutex only for its
own critical section: `begin` takes the whole queue (queue lock, then released), locks the
writer, runs the request closures (including the D191/D192 guard CASes) and `prepare`, then
drops the writer before replying; `end` locks the writer only for `complete`, then publishes
the view under `Shared::view_lock`, retires extents, reclaims, answers every request, and
broadcasts `ShardMsg::Maintain`. **Shard/task path:** `submit` pushes and sends
`ShardMsg::PumpManifest`, which spawns a cooperative `ManifestPump` task on that shard
(shard.rs:7247). `ManifestPump::run` claims (if it cannot, it returns `Done`: "someone else is
committing and re-checks the queue before letting go"), calls `begin`, and `start`s the commit:
`Flight::start` stores it in `Shared::manifest_flight` with a new generation, and
`pager.submit_commit_root` is submitted; its completion callback calls `Flight::complete`
(records the result, wakes parked threads) and wakes the task. The exclusion stays held across
that I/O. On the next run, `Flight::stage(generation)` returns `Pending` (stay `Blocked`),
`Done` (run `end`, `release`, loop to drain more), or `Gone` (a thread already finished it).
After a release the pump re-checks `manifest_queue.is_empty()` before it leaves. If the pump is
dropped with its commit in flight (`ManifestPump::drop`), `Flight::abandon` takes the commit
and `end` runs with its result, or with an error (poisoning the pager) if none arrived, then
`release`. **Application-thread path:** `commit_from_thread` / `commit_req_from_thread`
(manifest.rs:1197-1265) push the request, then loop: if `claim` succeeds, `drain_sync` (`begin`
→ blocking `pager.commit_root` → `end` until the queue is empty), `release`, and re-claim if
the queue is non-empty again (a request pushed after the last `begin` saw the exclusion held,
and its pump left; test hook `race_window` forces this window). If `claim` fails and
`Flight::take_done` finds a pump's commit whose root commit completed, the thread runs `end`
and `release` itself: taking the commit hands over the pump's exclusion, so a pump whose shard
this very thread drives (application-owned mode) never holds the exclusion forever (#135, #166).
Otherwise the thread polls its own `Waiter`; if pending it `Flight::register`s its thread waker
(`threads` Mutex + `thread_count` SeqCst), re-checks `manifest_busy.load(SeqCst)` and
`Flight::ready()` to close the lost-wakeup window, then reaps I/O only it can complete
(`pigeonhole_io::own_io_in_flight` / `reap_own_io` in 1 ms slices, #207 via #409), or orphaned
I/O (`reap_orphan_io`, ICR 0028), and only then `std::thread::park()`s. `commit_held` and
`drain_sync` are for callers that already hold the exclusion (`Shared::final_close`,
`forget_checkpoints`, `try_final_close`, test hooks in engine/hooks.rs:860-880).

### Lock nesting observed (outer → inner)

1. `manifest_busy` exclusion (logical outer lock) → `Shared::manifest` writer Mutex: every
   writer lock in `begin` (manifest.rs:587), `end` (manifest.rs:885) and `Shared::final_close`
   (shard.rs:714) is taken with the exclusion held. The exclusion is also held across the
   root-commit I/O by the pump (no Mutex is).
2. `Shared::manifest` (manifest.rs:587-845, `begin`) → request closures, which CAS the
   `FlushItem::guard` / `CompactionWork::guard` atomics (flush.rs:661, compact.rs:808) and
   store `install_seqnos` (flush.rs:678); → `Shared::view` ArcSwap `load_full` (591); →
   pager internals via `pager.abandon` (`abandon_ssts` / `abandon_blobs`, 545-575) and
   `writer.prepare`; → block cache internals via `cache.erase_files` (abandon paths and
   manifest.rs:683). `ManifestQueue::reqs` is taken *before* (582) and released, never nested.
   `hooks.compactions` (test-hooks) is locked only after `drop(writer)` (822-829).
3. `Shared::view_lock` (inside `publish_view`, shard.rs:594, called from `manifest::end`
   manifest.rs:916) → `LiveViews::versions` (via `ViewPin::new` → `register`, manifest.rs:960 →
   snapshot.rs:734); also `OpenSst::reader` OnceLock set in `SstSet::rebuild` there. The writer
   Mutex is released (block at manifest.rs:884-891) before `publish_view`.
4. `LiveSeqnos::seqnos` (`SeqnoPin::pin_visible`, snapshot.rs:830) → `shm.visible_seqno()`
   atomic load inside the critical section (intentional, #315 review).
5. `large_logged` → `large_dropped` → `large_pending`, all three held together in
   `LargeValues::settle_on`'s `on_resolve` callback (large.rs:106-118; runs on the thread that
   resolves the commit's notifier). `note_logged` takes `large_pending` and drops it (large.rs:221)
   before `large_logged` (223): no nesting there. The `release` reply closure (large.rs:163-170)
   locks `large_pending` alone, from `manifest::end`/`begin` after the writer lock is dropped.
6. `Flight::state` → `hooks.manifest_park` atomic load (`take_done`, test-hooks only). Not
   nested with `Flight::threads`: `complete` drops `state` before `wake_threads`; `parked` drops
   `state` before `hooks.manifest_parked`.
7. `Flight::threads` → `thread_count` store (SeqCst) inside `register` / `wake_threads`; wakers
   are woken after the lock is dropped.
8. `LiveViews::versions` is released before the `on_oldest_released` callback
   (`Shared::try_reclaim`) runs (snapshot.rs:788-794); `oldest()` releases it before
   `oldest_live_manifest` locks `Shared::view_versions` (shard.rs:644-651).
9. Test hook only: `hooks.compaction_held` Mutex in `CompactionWork::run` (compact.rs:932), leaf.
10. `RowCaches::store`: the `RowCache`'s internal lock (`cache.insert`) is released before the
    `present` Relaxed store (row_cache.rs:198-202): no nesting.


## Appendix D: `runtime/src`

Scope: `lib.rs` (1184 lines), `signal.rs` (140), `completion.rs` (528), `sched.rs` (430). `lib.rs` has `#![forbid(unsafe_code)]`. `completion.rs` switches `Arc/Mutex/Condvar` to `loom::sync` under `cfg(loom)`; nothing else in the crate does.

Thread roles used below:
- **shard thread**: the thread driving a `ShardCore`: an engine-owned `pigeonhole-shard-{i}` thread (`shard_main`), or an application thread inside `ShardDriver::run_once` / `release` / `shutdown` / `with_handler`.
- **pool thread**: an engine-owned `pigeonhole-compaction-{j}` thread (`sched::pool_main`).
- **producer**: any thread holding a `Submitter` (client threads via the engine API, other shards via `ShardContext::submitter`), a `TaskWaker` clone (whoever a background task handed it to: completion hooks, I/O completion paths, other shards), or a shard thread spawning onto the pool (`Spawner::spawn` -> `PoolShared::submit`).
- **owner thread**: the thread that calls `Runtime::start` / `shutdown` / drops `Runtime`, or owns a `ShardDriver`.

### Signal (idle/wake handshake), `signal.rs`

One `Arc<Signal>` per shard (`Inbox::signal`, lib.rs:262, built at lib.rs:750) and one per pool thread (`PoolShared::signal`, sched.rs:297, built at sched.rs:306). Each shard's `Scheduler` and every `TaskCell` it spawns hold a clone of that shard's signal (lib.rs:765, sched.rs:174).

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `Signal::sleeping` signal.rs:28 | `AtomicBool` | The consumer announced it is about to sleep and must be woken by the next producer. | Consumer: `prepare_sleep` (store true, from `ShardCore::check_before_sleep` lib.rs:652, `pool_main` sched.rs:358); `awake` (store false, from `run_turn` lib.rs:581, `spin_before_park` lib.rs:517/545, `check_before_sleep` lib.rs:657, `pool_main` sched.rs:360/365). Producers: `notify` (swap false, signal.rs:64). Owner thread: `force_wake` (store false, signal.rs:78) via `Inbox::close` lib.rs:268 and `PoolShared::close` sched.rs:321. | Producers in `notify` (load then swap); `Debug` impl. | `prepare_sleep`: store Relaxed then `fence(SeqCst)`. `notify`: `fence(SeqCst)`, load Relaxed, then `swap(false, AcqRel)` (only one notifier wins the wake). `awake`/`force_wake`: store Relaxed. Debug: load Relaxed. | Dekker-style handshake from the first runtime commit, #6 (bf1836a). Spin-before-park use of `awake` added with D198 / ICR 0016 (#421, 1741d2e). |
| `Signal::task_woken` signal.rs:29 | `AtomicBool` | Some background task on this consumer was woken since the consumer last collected wakes. | Producers via `TaskWaker::wake` -> `notify_task` (store true, signal.rs:72). Consumer via `take_task_woken` (swap false, signal.rs:110) from `Scheduler::collect_woken` sched.rs:233. | Consumer: `take_task_woken` (load then swap); `task_woken()` (no clear) in `check_before_sleep` lib.rs:656, `spin_before_park` lib.rs:524, `pool_main` sched.rs:364; Debug. | `notify_task`: store Release, then `notify`'s `fence(SeqCst)`. `take_task_woken`: load Relaxed then `swap(false, Acquire)`. `task_woken()`: load Relaxed (after `prepare_sleep`'s SeqCst fence on the sleep path). | #6 (bf1836a); non-clearing `task_woken()` reader added for the spin path, D198 / ICR 0016 (#421, 1741d2e). |
| `Signal::target` signal.rs:30 | `Mutex<WakeTarget>` | How to wake the sleeping consumer: nothing, `Thread` (unpark), `ThreadRing` (unpark + `pigeonhole_io::OwnIoWaker::wake`, an eventfd/ring interrupt), or an application `Callback`. | `set_target`: engine-owned shard thread at `shard_main` start (lib.rs:681-685, `ThreadRing` if the backend gave it a ring, else `Thread`); pool thread at `pool_main` start (sched.rs:332-334, `Thread`); application thread in `ShardDriver::set_wakeup` (lib.rs:1165-1172, `Callback`). | `wake_target` (signal.rs:118-139), called from `notify` on producers and from `force_wake` on the owner thread. | Lock is held only to clone the target out (signal.rs:125-130); unpark / ring wake / callback run after the guard drops. Poison ignored (`PoisonError::into_inner`). No other lock is held while taking it inside the runtime. | Mutex from #6 (bf1836a). `ThreadRing` variant added with per-thread rings, #402 (#415, ca1dad5). |
| `Signal::parks` signal.rs:32 | `AtomicU64` | Count of times an engine-owned shard thread parked idle (diagnostic). | Engine-owned shard thread: `Signal::parked` from `shard_main` lib.rs:715 (pool threads never call it). | `Signal::idle_counts` via `Submitter::idle_counts` lib.rs:311 (any thread: engine stats, bench). | `fetch_add` Relaxed; load Relaxed (doc says readings may lag). | ICR 0027 (#480, edda3e7). |
| `Signal::wakes` signal.rs:34 | `AtomicU64` | Count of `notify` calls that found the consumer asleep and woke it (diagnostic). | Producers in `notify` (signal.rs:65), only after winning the `sleeping` swap. | `idle_counts` (as above). | `fetch_add` Relaxed; load Relaxed. | ICR 0027 (#480, edda3e7). |

### Shard inbox, `lib.rs`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `Inbox::tx` lib.rs:258 (receiver: `ShardCore::rx` lib.rs:373) | `std::sync::mpsc::Sender<M>` / `Receiver<M>` (unbounded MPSC, lock-free in std) | The shard's foreground message queue. FIFO per sender. | Producers: `Submitter::submit` (lib.rs:325). | Shard thread only: `ShardCore::drain` (`try_recv`, lib.rs:492, up to `MAX_BATCH`), `check_before_sleep` (lib.rs:654, into `stash`), `spin_before_park` (lib.rs:522), `finish` via `drain`. Dropping the `ShardDriver` drops the receiver, which drops queued messages (their `Notifier`s resolve `None`). | Channel's own synchronisation. `submit` calls `signal.notify()` after a successful send (lib.rs:328). | #6 (bf1836a). Crate doc lib.rs:16-20 lists it as one of the only shared mutable state. |
| `Inbox::closed` lib.rs:259 | `AtomicBool` | The shard no longer accepts submits (shutdown started). | Owner thread: `Inbox::close` (lib.rs:267, from `Runtime::stop` lib.rs:946 and `Runtime::drop` lib.rs:978); `CloseOnDrop::drop` (lib.rs:1038, when a `ShardDriver` is dropped or shut down); `ShardDriver::shutdown` (lib.rs:1181). | Producers in `submit` (lib.rs:321); engine-owned shard thread via `is_closed` in `shard_main` (lib.rs:689) and `spin_before_park` (lib.rs:524). | All stores and loads `SeqCst`. `Inbox::close` follows the store with `signal.force_wake()`; `CloseOnDrop` and `ShardDriver::shutdown` do not wake (the application drives the loop itself). | #6 (bf1836a). |
| `Inbox::inflight` lib.rs:261 | `AtomicUsize` | Number of submits between their `closed` check and the end of their `send`; shutdown waits for zero so no message is sent after the final drain. | Producers in `submit`: `fetch_add(1)` before checking `closed` (lib.rs:320), `fetch_sub(1)` after refusing (lib.rs:322) or after `send` (lib.rs:326). | Shard thread in `ShardCore::finish` (lib.rs:665), spinning with `thread::yield_now` until 0, then draining to empty. | `fetch_add` SeqCst; `fetch_sub` Release; load SeqCst in `finish`. Pairs with `closed` (both SeqCst) as a store-buffering pattern: "either shutdown sees our increment and waits for the send, or we see `closed` and refuse" (lib.rs:318-319). | #6 (bf1836a). |

### Thread-locals and statics, `lib.rs`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `thread_local CURRENT_SHARD` lib.rs:334 | `Cell<Option<ShardId>>` | Which shard the calling thread is currently running (the engine's inline-write fast path). Not shared across threads. | `EnterShard::new` / `Drop` (lib.rs:342/348), entered by `shard_main` (lib.rs:678), `run_turn` (lib.rs:580), `ShardDriver::shutdown` (lib.rs:1180), `with_handler`. Restores the previous value on drop (nesting-safe). | `Runtime::current_shard` (lib.rs:931-933) from engine code on any thread. | n/a (thread-local `Cell`). | #6 (bf1836a). |
| `thread_local TOKEN` (inside `thread_token`) lib.rs:356 | `Cell<u64>` | A small nonzero id for the calling thread, cheaper to compare per turn than `thread::current().id()`. | `thread_token` lazily on first call, from `NEXT`. | `ShardCore::run_turn` (lib.rs:588), `attach` (lib.rs:640), `release` (lib.rs:563): decide whether this thread already has its own I/O ring for this shard. | n/a. | A shard moved to another thread needs its own ring, #473 / ICR 0028 (#485, f868540); handover check #492 / ICR 0029. |
| `static NEXT` (inside `thread_token`) lib.rs:358 | `AtomicU64` | Next thread token to hand out (process-wide). | Any thread on its first `thread_token()` call (`fetch_add(1)`, lib.rs:361). | Same (the `fetch_add` return value). | `fetch_add` Relaxed: only uniqueness matters. | #473 / ICR 0028 (#485, f868540). |
| `spawn_pinned` ack channel lib.rs:801 (local, crosses threads) | `mpsc::channel::<io::Result<()>>()` | The new thread's pin result, reported back before `Runtime::start` returns. | The spawned shard/pool thread, once, first thing (lib.rs:805). | Owner thread in `Runtime::start` (`ack.recv()`, lib.rs:880). | Channel. Note `start` returns early on the first pin error while threads already spawned keep running until `rt` drops (its `Drop` closes and joins them). | #6 (bf1836a). |

### Background-task scheduling, `sched.rs`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `TaskCell::woken` sched.rs:102 | `AtomicBool` (one per spawned task, shared through `TaskWaker: Clone`) | This task was woken since its owner last looked; a blocked or sleeping task becomes runnable. | Producers: `TaskWaker::wake` (store true, sched.rs:109), then `signal.notify_task()`. Owner (shard or pool thread): `Scheduler::run_one` clears it before running the slice (sched.rs:264) and swaps it after `Blocked`/`SleepUntil` (sched.rs:269, 277); `collect_woken` swaps it (sched.rs:241). | Owner thread only (the swaps above). | `wake`: store Release. `run_one` pre-slice clear: store Relaxed. All consuming reads: `swap(false, Acquire)`. | #6 (bf1836a). The post-slice swap ("a wake that raced with the slice keeps the task runnable", sched.rs:268) closes the lost-wake window. |
| `PoolShared::tx` sched.rs:295 (receiver passed to `pool_main`, sched.rs:328) | `mpsc::Sender<Box<dyn Task>>` / `Receiver` | One compaction thread's task inbox. | Shard threads: `PoolShared::submit` (sched.rs:314) via `Spawner::spawn` (sched.rs:427, round-robin over the pool), i.e. from `ShardContext::spawn` inside handlers/tasks. | Pool thread: `pool_main` `try_recv` (sched.rs:343, 359). | Channel; `submit` calls `signal.notify()` after a successful send (sched.rs:315). After the receiver drops, sends fail and the task is dropped. | #6 (bf1836a). Pool is engine-owned mode only; application-owned mode rejects `compaction_threads != 0` (D40, lib.rs:891). |
| `PoolShared::closed` sched.rs:296 | `AtomicBool` | The pool thread should exit. | Owner thread: `PoolShared::close` (sched.rs:320) from `Runtime::stop` (lib.rs:958, after every shard thread is joined) and `Runtime::drop` while panicking (lib.rs:981). | Pool thread: `pool_main` at loop top (sched.rs:340) and before parking (sched.rs:368). | store/load SeqCst; `close` then `signal.force_wake()`. No in-flight counter: safe in `stop` because shards (the only submitters) are joined first; in the panicking `Drop` path shards are not joined, and late tasks are dropped. | #6 (bf1836a). |

### Completions, `completion.rs`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `Slot::state` completion.rs:22 | `Mutex<SlotState<T>>` (`value`, `done`, `waker`, `hook`, `blocked`) | Everything in a one-shot completion: the result, whether it resolved, the async waker, the `on_resolve` hook, and whether a thread waits on `cv`. | Resolving thread (usually a shard thread; any thread that drops an unresolved `Notifier`, e.g. a queued message dropped with the receiver): `Slot::resolve` (completion.rs:40-60) via `Notifier::notify` / `Drop`. Notifier owner: `on_resolve` (completion.rs:191). Waiting thread: `Waiter::wait` sets `blocked` (completion.rs:282), `Future::poll` stores the waker (completion.rs:297-299), `recycle_into` resets the slot (completion.rs:255-260). | Same parties; `poll` and `wait` read `done`/`value`. | `resolve` takes the lock twice: once to take the hook, which then runs **outside** the lock before the value is visible; again to publish `value`/`done` and take `waker`/`blocked`. `cv.notify_one()` and `waker.wake()` run after the guard drops. `recycle_into` drops the taken value/waker/hook after releasing the lock (their drops may run arbitrary code). Poison ignored. | Single lock + lazily used condvar so a completion allocates one lock on macOS, #320 (`blocked` flag from #325, 9713020). |
| `Slot::cv` completion.rs:23 | `Condvar` | Wakes a thread blocked in `Waiter::wait`. | `Slot::resolve` `notify_one` only when `blocked` was set (completion.rs:54-56). | `Waiter::wait` (completion.rs:283) / `wait_ref` (loom only, completion.rs:273): `while !done { blocked = true; cv.wait }`. | Paired with `Slot::state`. The invariant (completion.rs:52-53): `blocked` is set under the lock before sleeping and `done` is checked under the same lock first, so a sleeping waiter is always signalled. | #6; `blocked` gate #320 / #325 (9713020). |
| `SlotCache::slots` completion.rs:122 | `RefCell<Vec<Arc<Slot<T>>>>` (not `Sync`; a per-thread pool of up to `CAP = 8` slots) | Finished completion slots kept for reuse by one thread. | `Waiter::recycle_into` (completion.rs:247-262) pushes a slot only when `Arc::strong_count(&slot) == 1` (the notifier released it). | `completion_from` (completion.rs:154) pops one. | `try_borrow_mut`, so re-entrance just skips reuse. Cross-thread safety rests on the `strong_count == 1` check plus `Slot::state` (loom-modelled, below). Engine callers keep it in a `thread_local!` (doc completion.rs:105). | #320 (#498, 07a0fe2). |

Excluded: test-only `thread_local CACHE` (completion.rs:419) and test atomics (completion.rs:312, 329, 458); `Scheduler::runnable` (`VecDeque<usize>`, sched.rs:141) and the rest of `Scheduler`/`Spawner`/`IdleSpin`/`IdlePark` state, which are owned by one thread and never shared.

### Lock nesting observed

The runtime never holds one of its own locks while taking another. Specifically:

- `Signal::target` (signal.rs:125): held only to clone the target; `Thread::unpark`, `OwnIoWaker::wake` and the application callback run after release (signal.rs:131-138). Callers of `notify` may hold arbitrary engine locks; the runtime takes only `target` under them.
- `Slot::state` (completion.rs:42, 47): `resolve` releases it before running the hook (completion.rs:43-45) and before `cv.notify_one` / `Waker::wake` (completion.rs:54-59). A hook or waker that calls `TaskWaker::wake` -> `Signal::notify` therefore takes `Signal::target` with no slot lock held.
- `Waiter::wait` (completion.rs:280-285): `Slot::state` held across `cv.wait` (standard condvar pattern, released while waiting).
- `Waiter::recycle_into` (completion.rs:247-260): holds a `RefCell` borrow of `SlotCache::slots` while taking `Slot::state`. Not a lock pair (the `RefCell` is thread-local), but it is the one place two guards overlap.
- `ShardCore::finish` (lib.rs:665-667): spin-waits on `Inbox::inflight` with no lock held.

### Cross-thread protocols (loom candidates)

1. **Sleep/wake handshake (Dekker with SeqCst fences)**, `Signal::sleeping` + `fence(SeqCst)` on both sides + the work sources (`Inbox` mpsc channel, `Signal::task_woken`, `PoolShared` channel). Consumer: `prepare_sleep` (store `sleeping=true`; fence), then re-check `rx.try_recv()` and `task_woken()` (`check_before_sleep` lib.rs:651-661, `pool_main` sched.rs:358-367), then `thread::park` / `IdlePark::park` / ring wait. Producer: send or set flag; `notify` (fence; Relaxed load; `swap(false, AcqRel)`; `wake_target`). Lost unparks are prevented by `Thread::unpark`'s token semantics: an unpark before `park` makes the next `park` return at once. The `ThreadRing` target also writes the ring's eventfd so a thread blocked in `reap_own_io(Some(until))` (lib.rs:709) wakes. Variants: the D198 idle spin (`spin_before_park` lib.rs:511-548) marks the shard `awake` while polling so producers skip the wake syscall, then reruns the handshake on a miss; `ShardDriver::set_wakeup` (lib.rs:1165-1172) installs a `Callback` and calls it once right away, because a `notify` that won the swap while the target was `None` woke nobody. Not loom-modelled today (`signal.rs` uses `std` atomics directly).
2. **Background-task wake**, `TaskCell::woken` (Release store) + `Signal::task_woken` (Release store) + protocol 1. The owner clears `task_woken` with an Acquire swap in `collect_woken` (sched.rs:233), then scans slots and Acquire-swaps each `woken`. `run_one` clears `woken` (Relaxed) before a slice and Acquire-swaps it after a `Blocked`/`SleepUntil` result (sched.rs:264-282), so a wake during the slice keeps the task runnable. `collect_woken` always clears `task_woken`, even with nothing blocked, so a stale wake cannot keep the owner awake. Not loom-modelled.
3. **Submit vs. shutdown**, `Inbox::inflight` + `Inbox::closed` (store-buffering, SeqCst on both sides). Submitter: `inflight += 1` (SeqCst); load `closed` (SeqCst); send; `inflight -= 1` (Release). Closer: store `closed` (SeqCst), `force_wake`; shard thread sees `is_closed`, then `finish` spins until `inflight == 0` (SeqCst load) and drains until empty. Guarantees no message is accepted (`Ok`) yet left unhandled by `shutdown`. Dropping a `ShardDriver` instead drops the receiver, so late sends fail with `Closed`. Not loom-modelled.
4. **Pool shutdown ordering**, `PoolShared::closed` + `force_wake` + thread joins: `Runtime::stop` joins every shard thread before closing the pool (lib.rs:950-959), so no submit races the close. The panicking-`Drop` path closes both without joining (lib.rs:975-982); late tasks are dropped with the receiver.
5. **One-shot completion**, `Slot::state` (`Mutex`) + `Slot::cv` (`Condvar`) + `blocked` flag + `Waker`: resolve runs the hook outside the lock before publishing, publishes `value`/`done` under the lock, then signals the condvar only if a thread blocked and wakes any stored async waker. Slot reuse (`SlotCache`, `recycle_into`) relies on `Arc::strong_count == 1` meaning the notifier has dropped its `Arc`. **Already loom-modelled**: `loom_recycle_races_the_notifier_releasing_the_slot` (completion.rs:496) and `loom_a_waiter_dropped_on_another_thread_recycles_there` (completion.rs:511), run with `RUSTFLAGS="--cfg loom" cargo test --release -p pigeonhole-runtime --lib loom`. They cover reuse and, incidentally, the blocking `wait_ref` path; there is no dedicated model of the hook-before-value ordering or of poll/wake waker replacement.
6. **Thread start ack**: each spawned thread sends its pin result on a one-shot mpsc channel (lib.rs:801-806) that `Runtime::start` waits on (lib.rs:878-883).
7. **Shard handover between application threads** (#473, #492, ICR 0028/0029): no runtime primitive. A `ShardDriver` moves between threads by the application's own synchronisation; `thread_local TOKEN` / `static NEXT` let `run_turn` detect the new thread and attach a ring there, and `ShardDriver::release` drains the old thread's ring first (debug assertion at lib.rs:591-596).

18 primitives (16 struct fields, statics and thread-locals, with the 2 channel receiver ends folded into their sender rows; plus the `spawn_pinned` ack channel and the non-`Sync` `SlotCache::slots`, listed because cross-thread reuse safety depends on it). Without those last two: 16.
