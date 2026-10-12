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

**151 primitives** (`main` after D221 and D222 replaced the `PIGEONHOLE_GROUP_SYNC_DEPTH` and `PIGEONHOLE_READAHEAD` statics with constants; 153 before, 159 before #134 step 2):

| Where | Production | Test hooks only | Notes |
|---|---:|---:|---|
| `engine/src/shard.rs`, `shard/tablets.rs` (incl. `Shared`) | 65 | (mixed in, marked) | Static test hooks: `READS_STEP`, `TAIL_SKIPS` |
| `engine/src/engine.rs`, `engine/hooks.rs` | 8 | 27 | `Hooks` 21, `ShardCounters` 4, `ReaderHooks` 2 |
| other `engine/src` files | 36 | | 10 of them thread-locals (per-thread scratch and caches) |
| `runtime/src` | 16 | | Plus the `spawn_pinned` start ack and the per-thread `SlotCache`, listed |
| **Total** | **151** | | |

A large share are **statistics counters** with `Relaxed` ordering (`ShardMetrics`, `VisibilityCounts::{waits, scanned, passes, woken}` and `WatermarkWaiters::contended`, ICR 0023 and similar). They synchronize nothing: they're read by `Engine::metrics` and need no reduction. Step 2's count should report them separately from primitives that coordinate.

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
4. **`CloseState` (the close gate's lock) → manifest exclusion, as a try-lock only**, in `try_final_close` (`CloseGate::try_start`). The final close itself then runs with the exclusion held and the gate's lock released. A thread that holds the exclusion may take the gate's lock (`manifest::release` runs `try_final_close`) only after it stored the exclusion free, so it never waits on the gate's lock while holding it.
5. *(Removed by #134 step 2: `CloseState::outcome` → `done` is gone; the gate has one lock.)*
6. **`LargeSettlement` logged → dropped → pending** (large_settle.rs), in `LargeSettlement::settle`, called from `LargeValues::settle_on`'s resolve callback (large.rs), on the completion thread. `note_logged` takes pending then logged in sequence, never together (loom-modelled).
7. **`ReaderState::shm` → { `ReaderState::view`, `ReaderState::catalog` }**, in `reader_snapshot`'s re-attach branch. `view` and `catalog` are never held together.
8. **`LiveSeqnos::seqnos` → the `shm` visible-seqno load**, in `SeqnoPin::pin_visible` (snapshot.rs). This is deliberate (#315 review).
9. **Lock plus atomic:** `ts_raisers[i]` is held while `ts_raises[i].fetch_max` runs (tablets.rs `commit_op`).

**Callbacks under locks.** These are where a new nesting could appear without anyone writing a second `lock()`:
- *(Removed by #134 step 2: `wake_visible` runs wakes after releasing the watermark waiters' lock.)*
- `Inner::reader_catalog` holds `ReaderState::catalog` across the root reload and manifest read, and `reader_view` holds `view` while it builds a view (long work under one lock).
- `flushed_roots` is held across `view.load_full()` in `read_latest` (`FlushedRoots::read`). That's not a lock, but the read relies on the ordering (#131); loom-modelled.

**The runtime holds no lock while taking another.**
- `Signal::target` is held only to clone the target; the unpark, eventfd write or callback runs after release.
- `Slot::state` is released before a completion's hook runs and before the condvar or waker is signalled.
- `Waiter::wait` holds `Slot::state` across `cv.wait`, the standard condvar pattern.

## Cross-thread protocols

These are the protocols the primitives implement, and whether a loom model checks each one. #134 step 3 asks for loom models of every remaining one.

| Protocol | Primitives | Where | Loom |
|---|---|---|---|
| **Manifest exclusion and pump.** One writer, at most one commit in flight; any thread may finish a pump's completed commit (`take_done`); a blocked caller reaps its own ring, then orphan rings, then parks (#135, #166, #207, ICR 0028, #306). | `manifest_busy`, `ManifestQueue::reqs`, `Shared::manifest`, `Flight::{state, threads, thread_count}` | manifest.rs, manifest/flight.rs; appendix C has the full description | **yes** (manifest/flight.rs, around the real `Flight`: one holder at a time and every request answered once, with a shard pump and a thread committing; a request queued while a thread holds the exclusion is not stranded; a thread driving the pump's shard finishes its commit. The exclusion is a `Mutex<bool>` stand-in in the model until loom ships tokio-rs/loom#391, #533) |
| **Final close.** The last shard to report makes the close pending. Whoever then finds maintenance empty and the exclusion free (the last reporter, the last thread out of maintenance, or the thread releasing the exclusion) runs it, and the `Engine::close` caller is answered once. One lock decides (`close_gate`), with no fences (#78, #81, #134). | `CloseState` (`CloseGate`), `manifest_busy` | close_gate.rs; shard.rs `report_closed`, `try_final_close`, `MaintenanceGuard`; manifest.rs `release` | **yes** (close_gate.rs: the close runs exactly once; the caller is answered exactly once) |
| **View publication.** Built under `view_lock`, published by `ArcSwap` store; readers load without locks. A flush's memtable roots join `flushed_roots` with the view store, under its lock, and a shard reading its memtables beside the view loads the view under it too (`FlushedRoots`, #131). Superseded views are pinned until released (`LiveViews`), then extents are reclaimed. | `view_lock`, `view`, `view_record`, `view_versions`, `flushed_roots`, `LiveViews::versions` | shard.rs `publish_view`; flushed_roots.rs; snapshot.rs | **yes** (flushed_roots.rs: a reader sees a flushed memtable's entries exactly once, with one flush or two publishing at once; snapshot.rs: the last view of the oldest version reclaims once, and a newer version released first does not reclaim) |
| **Watermark waiters.** Shards publish the visible seqno. Async commits register `(seqno, waker)` and shards with a deferred freeze `(seqno, shard)` in one registry, split into one part per shard and keyed by waiter; each part's `min_needed` lets a publisher skip its lock unless someone in it is ready; a SeqCst fence after registration pairs with one before the publisher's load. Wakes run outside the lock (#41, #97, #134). | `WatermarkWaiters::{min_needed, list}`, `shm` watermark | watermark_waiters.rs; shard.rs `wait_visible`, `wake_visible`, `publish_watermark`, the freeze's registration | **yes** (watermark_waiters.rs: a waiter registering as the watermark passes is never lost; a raised registration still wakes) |
| **Purge guards.** A flush or bottommost compaction's purge races the shard admitting a write that invalidates it: one three-state cell (`PurgeGuard`: in flight → voided by the shard's `void`, or → installing by the commit's `claim` under the writer mutex); each side accepts its own move again (D191, D192). | `PurgeGuard::state` (`ShardState::guarded`, `CompactionGuard::state`) | purge_guard.rs; shard.rs `admit_against_guards`; flush.rs, compact.rs catalog closures | **yes** (purge_guard.rs: a purge and a write that invalidates it never both take effect, including a write touching the guard twice and a closure run twice; one write against a flush's and a compaction's guard) |
| **Clock timers.** Cancel against fire: the shard stores `cancel` and takes the waker; the task stores its waker and re-checks `cancel`; `frozen_at` is published before `done` (#84, #93). | `TimerState::{cancel, done, frozen_at, waker}` | timer_state.rs; shard.rs `ClockTimer` | **yes** (timer_state.rs: a cancel never leaves the timer asleep; a timer that gave up is never seen as fired) |
| **Tablet load publication.** A shard publishes its write rate (`writes`, then `at` with Release); others read `at` (Acquire), then `writes`. Slot reservations for in-flight changes use RMW (#97, #103, #128). | `LoadSlot::{writes, at, reserved}` | load_slot.rs; shard/tablets.rs | **yes** (load_slot.rs: a fresh load is never older than its publication; concurrent changes reserving all or none never pass the slot limit) |
| **Large-value settlement.** A commit's blob files are kept or released depending on where it stopped (#230). | `large_logged`, `large_dropped`, `large_pending` | large_settle.rs; large.rs | **yes** (large_settle.rs: commits settling at once keep a logged file after failure, release a never-logged or collapsed one, and keep an applied one, with no deadlock between `settle`'s nested locks and `note_logged`) |
| **Sleep and wake.** Dekker-style with SeqCst fences: the consumer marks itself sleeping, fences, re-checks its sources, then parks; the producer sends, fences, swaps `sleeping`, then wakes the target. D198's idle spin, `set_wakeup`'s initial call and an application thread's `ShardDriver::wait` (`Signal::sleep_unless`, #553) are variants. A thread waiting in its own io_uring ring is woken through the ring's eventfd, which a wake writes only while the owner waits there (`RingWake`: store buffering with a SeqCst fence on each side). | `Signal::{sleeping, task_woken, target}`, inbox channel; `pigeonhole-io` `RingWake::{pending, waiting}` (outside these two crates, not counted) | runtime signal.rs, lib.rs, sched.rs; io ring_wake.rs, uring.rs | model in signal.rs (the real `Signal`), `#[ignore]`d: loom 0.7.2 lets `notify`'s swap read past a store (tokio-rs/loom#254). Passes with tokio-rs/loom#391, where dropping `prepare_sleep`'s fence fails it; un-ignored by #533. The application wait has its own model there (`loom_an_application_wait_is_never_slept_through`, the real `sleep_unless` against a submit and a `waker()` wake), `#[ignore]`d alike; with #391 it passes, and dropping `sleep_unless`'s re-check deadlocks it. **yes** for the ring wake (io ring_wake.rs, `--cfg io_loom`: a wake racing the owner's ring wait is never lost; dropping either fence, or never writing the eventfd, deadlocks it) |
| **Background-task wake.** A wake during a slice keeps the task runnable. | `TaskCell::woken`, `Signal::task_woken` | runtime sched.rs | model in sched.rs (the real `TaskWaker`), `#[ignore]`d for the same loom bug; passes with #391, where a wake that skips `notify_task` fails it (#533) |
| **Submit against shutdown.** Store-buffering on `inflight` and `closed` (SeqCst): no message is accepted and then left unhandled. | `SubmitGate::{inflight, closed}` (`Inbox::gate`) | runtime submit_gate.rs, lib.rs | model in submit_gate.rs, `#[ignore]`d: loom models SeqCst *accesses* as AcqRel, so it reports the store buffering the C++ model forbids (it handles SeqCst fences). Not checkable by loom as written |
| **One-shot completion and slot reuse.** The hook runs before the value is published; the condvar is signalled only if a thread blocked; a slot is reused once the notifier has dropped it. | `Slot::{state, cv}`, `SlotCache` | runtime completion.rs | **yes** (completion.rs: slot reuse against the notifier, and a waiter dropped on another thread) |
| **Stream creation durability (D211).** A new WAL stream's checkpoint is held back until its file and directory syncs have both finished. The shard arms one wake (`Wal::notify_created`), which sends `ShardMsg::Created` to retry the checkpoint and the close. The WAL runs waiters outside its lock, on the completing thread (#520, ICR 0031). | `pigeonhole-wal` `Shared::creation` (`Mutex<Creation>`: pending count and waiters; outside these two crates, not counted); `ShardState::creation_wake`; the shard inbox | shard.rs `advance_checkpoint`; wal stream.rs `creation_synced`, `notify_created` | none (`checkpoint_creation.rs`, `wal/tests/stream.rs` test it deterministically) |
| **Page-file growth ahead of need (gate analysis D1b).** An allocation that leaves no free block of the largest class claims the file's tail under the allocator lock (as placeholders, so nothing is allocated there), runs `fallocate` after releasing it, and submits the `sync_all`. The completion runs on whichever thread completes the I/O (the backend's, a ring's owner, or a blocking commit driving it). It takes the allocator lock, then the commit state lock if the sync failed (allocator → state, as in an inline growth). It frees the claim, ends the side sync and runs the parked root-commit steps after dropping the locks (#182, D58). One growth at a time (`ahead`). A blocking root commit first waits on the growth's completion (`ahead_flight`), which drives I/O only its thread may complete, then on the side syncs' condvar. | `pigeonhole-pager` `Inner::{alloc, state, side, ahead, ahead_flight}` (outside these two crates, not counted) | pager lib.rs `maybe_grow_ahead`, `wait_side_idle`; alloc.rs `claim_ahead`, `release_ahead` | none (pager `tests/sync_faults.rs` tests it deterministically: a growth in flight, a commit parked behind it, a failed sync, no space, a blocking commit driving a deferred sync) |
| **Shard handover between application threads.** No primitive: the application synchronizes. The runtime detects the new thread (`TOKEN`, `NEXT`) and attaches a ring; `release()` drains the old ring first (#473, #492, ICR 0029). | thread-local `TOKEN`, `static NEXT` | runtime lib.rs | n/a |

## For step 2

These observations come from the inventory. They aren't decisions.

- **Counters are most of the count.** Separate them (Relaxed, read only by metrics) from coordinating primitives before measuring a reduction.
- **Several protocols end in a "skip the lock if nobody waits" counter plus a SeqCst fence:** visibility waiters, freeze waiters and final close (step 2 replaced the close's with one lock, and merged the two waiter lists into one registry, both loom-modelled). They're the subtlest code here, and none has a loom model. They're the first candidates for either a loom model or the one per-shard event path #134 proposes.
- **The manifest exclusion has many exits:** pump, client thread, `take_done` handover, final close, abandon on drop. Collapsing them is the largest simplification available. It's also where Phase 1's bugs clustered (#78, #81, #135, #166, #207).
- **Wakers run under locks** (`wake_visible`). *(Fixed by #134 step 2: they run after the lock is released.)*

## Appendices: the inventory

Each appendix is one part of the code. Rows give `file:line` of the declaration; nesting lists are outer → inner.


## Appendix A: `engine/src/shard.rs` and `shard/tablets.rs` (including `Shared`)

"Shard thread" means the shard's own `ShardState` loop (engine-owned runtime thread, or the application thread driving an application-owned shard). "Manifest pump" means whoever holds the manifest exclusion (`manifest_busy`) and runs `manifest::begin`/`end`: a shard thread in `drain` or an application thread in `commit_from_thread`/`drain_sync`. Writers and readers outside these two files are named by `file:fn`. History is the first commit that added the field (`git log -S`), plus ids from the doc comment.

Not inventoried here because the type is defined elsewhere (they are `Shared` fields that hold primitives internally): `manifest_queue: ManifestQueue` (manifest.rs:425), `manifest_flight: manifest::Flight` (manifest/flight.rs:23; the type is inventoried in appendix C), `drivers: waker::Drivers` (waker.rs:70, `Box<[AtomicU64]>`), `live_views`, `live_seqnos`, `pager`, `cache`, `shm`, `row_cache`, `hooks` (test-hooks), and `ShardMetrics::hooks` (test-hooks).

### `ShardMetrics` (shard.rs:106), one per shard in `Shared::metrics`

Every counter in this struct has one writer, its own shard thread, and the reader is `Engine::metrics` (engine.rs ~1670–1745) on an application thread. All of them use `fetch_add` and `load` with **Relaxed** ordering. Nothing else is published through them.

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `ShardMetrics::commits` shard.rs:107 | `[AtomicU64; 4]` | Commits per durability level | shard thread: `ShardMetrics::record` from `run_group` paths (shard.rs:5523, 5992) | `Engine::metrics` (engine.rs:1705) | Relaxed | Interface freeze 46c75ea (#1); Milestone A c9700de (#41) |
| `ShardMetrics::latency` shard.rs:109 | `[[AtomicU64; 64]; 4]` | Log-scale commit-latency histogram per durability level | shard thread: `record` | `Engine::metrics` (engine.rs:1707) | Relaxed | c9700de (#41) |
| `ShardMetrics::stalls` shard.rs:110 | `AtomicU64` | Write stalls entered | shard thread: `run_group` (shard.rs:4236) | `Engine::metrics` | Relaxed | c9700de (#41) |
| `ShardMetrics::stall_nanos` shard.rs:111 | `AtomicU64` | Time spent stalled | shard thread: `end_room_wait` (2975), `stalled` (3971, 3982) | `Engine::metrics` | Relaxed | c9700de (#41) |
| `ShardMetrics::flushes` shard.rs:112 | `AtomicU64` | Completed flushes | shard thread: `on_flushed` (3343) | `Engine::metrics` | Relaxed | c9700de (#41) |
| `ShardMetrics::flush_nanos` shard.rs:113 | `AtomicU64` | Flush time | shard thread: `on_flushed` (3344) | No reader found in engine.rs: written but apparently never read (unsure) | Relaxed | c9700de (#41) |
| `ShardMetrics::compactions` shard.rs:114 | `AtomicU64` | Completed compactions | shard thread: `on_compaction_done` (6858) | `Engine::metrics` | Relaxed | 939440e Milestone B (#63) |
| `ShardMetrics::compaction_nanos` shard.rs:115 | `AtomicU64` | Compaction time | shard thread: `on_compaction_done` (6859) | No reader found (unsure) | Relaxed | 939440e (#63) |
| `ShardMetrics::flush_failures` shard.rs:118 | `AtomicU64` | Failed flushes (retried on backoff) | shard thread: `on_flushed` (3393) | `Engine::metrics` | Relaxed | #141; a62da94 (#186) |
| `ShardMetrics::compaction_failures` shard.rs:119 | `AtomicU64` | Failed background compactions | shard thread: `back_off_compaction` (6637) | `Engine::metrics` | Relaxed | #141; a62da94 (#186) |
| `ShardMetrics::splits` shard.rs:120 | `AtomicU64` | Tablet splits done | shard thread: tablets.rs `on_tablet_op_done` (850) | `Engine::metrics` (engine.rs:1685) | Relaxed | c1b02a2 (#97, #38) |
| `ShardMetrics::merges` shard.rs:121 | `AtomicU64` | Tablet merges done | as `splits` (tablets.rs:886) | engine.rs:1686 | Relaxed | c1b02a2 (#97) |
| `ShardMetrics::moves` shard.rs:122 | `AtomicU64` | Tablet moves done | as `splits` (tablets.rs:887) | engine.rs:1687 | Relaxed | c1b02a2 (#97) |
| `ShardMetrics::unpin_passes` shard.rs:124 | `AtomicU64` | WAL unpin passes | shard thread: `limit_wal_pin` (4720), `on_unpin` (5225) | `Engine::metrics` | Relaxed | #137; 1ccd22d (#171) |
| `ShardMetrics::unpin_flushes` shard.rs:125 | `AtomicU64` | Memtables frozen below threshold by unpin | shard thread: `freeze` (3101) | `Engine::metrics` | Relaxed | #137; 1ccd22d (#171) |
| `ShardMetrics::refused_slots` shard.rs:128 | `AtomicU64` | Tablet changes refused for lack of memtable slots | shard thread: tablets.rs `reserve_slots` (715), `choose` (1275) | `Engine::metrics` (engine.rs:1668) | Relaxed | D28; #122, 0b46ae1 (#259) |
| `ShardMetrics::refused_view` shard.rs:129 | `AtomicU64` | Splits refused because the shm view would outgrow its buffer | shard thread: tablets.rs `validate_op` (631) | engine.rs:1669 | Relaxed | D28; #122, 0b46ae1 (#259) |
| `ShardMetrics::wal` shard.rs:131 | `OnceLock<Arc<WalCounters>>` | The shard's WAL stream stall counters, set once the stream is open | open thread: `open_writer` (engine.rs:847, 843, 875; `set`, a later set is ignored) | `Engine::metrics` (engine.rs:1734–1735, `get`) | OnceLock (set happens-before a successful get) | ICR 0015, #64; 0f0c7cf (#414) |

### `CloseState` (shard.rs:199), `Shared::close`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `CloseState` (`CloseGate`, close_gate.rs) shard.rs:199 | `Mutex<Gate>`: shards to report, maintenance count, stage (`Open`, `Pending`, `Running`, `Done(outcome)`), the `Engine::close` waiter | Who runs the final close, and when; its outcome | shard threads: `report_closed` (`report`); app threads: `MaintenanceGuard` (`enter_maintenance`/`leave_maintenance`), `Engine::close` (`register`); whoever runs `try_final_close` (`try_start`, `finish`), including `manifest::release` | `EngineShard::closed` (`outcome`); test hook `final_close_pending` | One lock. `try_start` try-claims the manifest exclusion under it (never blocks); the final close runs outside it. Replaced five primitives, `Shared::maintenance` and three SeqCst fence pairings (#134 step 2) | #41, #78, #81, #117, #135, #166; #134 |
| `CloseGate::failed` close_gate.rs | `AtomicBool` | The close is unclean: a shard's final sync, flush or checkpoint failed, or a shard died | `fail_close` (`fail`, a swap: the first failure is told) | `visibility_ended` on every async commit poll, `try_finish_close`, and `try_start` under the gate's lock (`failed`) | SeqCst. Outside the lock so a poll costs one load. A shard fails the close before it reports under the lock, so the starter sees it | #41; #134 |

### `WatermarkWaiters` (watermark_waiters.rs, `Shared::waiters`, shard.rs:336) and `VisibilityCounts` (shard.rs:221, `Shared::visibility`)

The registry has one part per shard (`WatermarkWaiters::new(shards)`), each on its own cache line with its own lock. A waiter's key picks its part: a commit's seqno (`commit_key`; a commit has one waiter) or the shard's id with the top bit set (`shard_key`). The parts are independent: no thread holds two part locks, and the fence pairing below holds per part.

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `Part::min_needed` watermark_waiters.rs:56 | `AtomicU64` per part | The part's smallest listed seqno, the top of its heap (`u64::MAX` = none). An entry left behind when a key's seqno changed can hold it lower until a pass pops it. A publish below it takes no lock on that part | under the part's `list`: `register` (126; async commits in `wait_visible`, shard.rs:390; shards in `freeze`, 2969), `wake_ready` (158), `wake_all` (197) | `wake_ready` (lock-free check of every part) | SeqCst store and load. `register` stores it even when unchanged, then fences SeqCst after releasing the lock; `wake_ready` fences SeqCst once, then loads each part's. Pairs with the watermark store (`publish_pending`, Release) and the registrant's re-read: either the publisher sees the registration or the registrant sees the watermark. Loom-modelled with two parts (a publisher that skips a part fails it) | #41, #97 (c1b02a2); merged by #134 step 2; split per shard (gate analysis, 2026-10-11) |
| `Part::list` watermark_waiters.rs:57 | `Mutex<List<Wake>>` per part: a map key → (seqno, generation, wake) and a min-heap of (seqno, key, generation) | Async commits (`Wake::Task(Waker)`) waiting for their seqno to become visible, and shards (`Wake::Shard(id)`, kicked) with a freeze deferred until the watermark passes their memtable. A key registered again replaces its wake (and seqno, with a new generation) in the map; no scan. A pass pops only ready heap entries and drops those whose generation is stale | `register`, `wake_ready`, `wake_all` (`shard_died`, `try_final_close` via `wake_all_visible`) | same | Taken through `Part::lock` (225; `try_lock` first, counts contention). No other engine lock is held while it is taken, at most one part's at a time, and nothing is called under it: wakes (`Waker::wake`, the shard kick) run after it is released | #41, #97; #134 step 2; per shard and keyed (gate analysis: one global list scanned 108 entries per registration at 32 shards) |
| `Part::contended` watermark_waiters.rs:59 | `AtomicU64` per part | Lock acquisitions that found the part's `list` held | `Part::lock` (225) | `Engine::metrics` (`contended()` sums the parts, engine.rs:1926) | Relaxed | ICR 0023, #154 |
| `VisibilityCounts::waits` shard.rs:221 | `AtomicU64` | Async-commit registrations that found their seqno not yet visible | `wait_visible` (396) | `Engine::metrics` (engine.rs:1838) | Relaxed | ICR 0023, #154; f8af55d (#463) |
| `VisibilityCounts::scanned` | `AtomicU64` | Waiters already listed in the part each registration joined (a queue depth; nothing is scanned since the split) | `wait_visible` | engine.rs:1926 | Relaxed | ICR 0023, #154 |
| `VisibilityCounts::passes` | `AtomicU64` | Wake passes that took the lock (since #134 step 2: only when a waiter's seqno was reached) | `wake_visible` (463) | engine.rs:1838 | Relaxed | ICR 0023, #154 |
| `VisibilityCounts::woken` | `AtomicU64` | Async commits woken (shard kicks are not counted) | `wake_visible` | engine.rs:1838 | Relaxed | ICR 0023, #154 |

### Statics

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `static READS_STEP` shard.rs:305 (test-hooks) | `AtomicBool` | Reads step over superseded memtable versions instead of using the stale-tail index | test hook hooks.rs:732 | read.rs:1673 (read paths on any thread) | Relaxed | D194, D199; 2cb8a28 (#439) |
| `static TAIL_SKIPS` shard.rs:309 (test-hooks) | `AtomicU64` | Memtable tail jumps taken by reads | source.rs:129 (readers) | hooks.rs:738 | Relaxed | D194, D199; 2cb8a28 (#439) |
| `static TRACING` shard.rs:7366 | `AtomicU8` | `PIGEONHOLE_TRACE` state: unset, off or on | `tracing()` (CAS from unset), `set_tracing` (test hook, #244) | `tracing()`, on every thread that traces | Relaxed for all, including the CAS | #244; 25d95ad (#252) |

`static ROWS` (shard.rs:1587) is a `LazyLock<FastBuildHasher>`. It is initialized once and is read-only after that. It is not counted, since `LazyLock` is not in the listed categories (#287, 79e1fa6).

### `Shared` (shard.rs:320), engine-wide and shared by every shard and caller

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `Shared::view` shard.rs:324 | `ArcSwap<View>` | The current in-process view: catalog, tablet map, memtable pieces and SST sets | `publish_view` (620, under `view_lock` **and** `flushed_roots`); open, `open_writer` (engine.rs:951) | everywhere: shard thread (`read_latest`, `conflicts`, `maintain`, tablets.rs…), app threads (engine.rs reads and writes, maintenance.rs), manifest pump (`begin`, manifest.rs:591), flush/compaction tasks (flush.rs:296, 322; compact.rs:763) | ArcSwap (load/store; `load_full` where it is kept) | c9700de (#41); #131 for the store under `flushed_roots` |
| `Shared::async_sync_reads` shard.rs:326 | `AtomicU64` | Reads inside async calls that went to the file synchronously | read.rs:767, 763, 836 (async read paths) | `Engine::metrics` (engine.rs:1739); nonblocking.rs:165, 198 | Relaxed | D196, ICR 0014, #42; 7b560a8 (#411) |
| `Shared::commit_parks` shard.rs:328 | `AtomicU64` | Blocking commit waits that parked their thread | write.rs:520 (application thread) | engine.rs:1741 | Relaxed | ICR 0027; edda3e7 (#480) |
| `Shared::view_lock` shard.rs:331 | `Mutex<u64>` | Serializes view publishers and holds the last published view version | `publish_view` (shard.rs:579; called by the shard thread's `publish_memtables`, 2736, and the manifest pump's `manifest::end`, manifest.rs:916); open sets it to 1 (engine.rs:953) | same | Outer lock. Taken while holding the manifest exclusion (`manifest_busy`) when called from `manifest::end`. While held: `flushed_roots` (inside the closure, via `mem_sets`, and again at the store), `view_record`, `view_versions`, and `shm.publish_view` | c9700de (#41); #131 |
| `Shared::view_record` shard.rs:334 | `Mutex<RecordCache>` | The last shm view record, refilled in place by the next publish | `publish_view` (588) | `publish_view` | Taken only under `view_lock` | #499; 05b5335 (#507) |
| `Shared::manifest` shard.rs:335 | `Mutex<ManifestWriter>` | The manifest writer state: prepare, complete and root | manifest pump: `manifest::begin` (manifest.rs:588, held from prepare until `drop(writer)` at 759/822/845, while the request closures run), `manifest::end` (852, around `complete` only); `final_close` (shard.rs:700, `mark_clean`); open: engine.rs:1954 and 1990 (`commit`) | maintenance.rs:420 (shrink: `root()`); hooks.rs:321 | Normally taken with `manifest_busy` claimed (pump, final close). The open paths run before the runtime exists. maintenance.rs:420 and hooks.rs:321 take it briefly for `root()`; whether those claim `manifest_busy` is unverified. While held in `begin`, `ReqKind::Catalog`/`Tablets` closures run; those closures CAS the guard `AtomicU8`s (flush.rs:661, compact.rs:806). Pager-internal locks are taken under it (`prepare`) | c9700de (#41) |
| `Shared::manifest_busy` shard.rs:338 | `AtomicBool` | The manifest writer's **exclusion**, a hand-rolled try-lock held by whoever commits a manifest batch | `manifest::claim` (swap true), `manifest::release` (store false); callers are the shard pump, app threads in `commit_from_thread`/`drain_sync`, and `try_final_close` | `claim`; manifest.rs:1123 (a waiting thread's check) | claim swap AcqRel; release store Release before `try_final_close` takes the close gate's lock (`close_gate`, #134); manifest.rs:1123 load SeqCst. Loom-modelled (manifest/flight.rs) with a `Mutex<bool>` stand-in (#533) | #78; 939440e (#63) |
| `Shared::sst_ids` shard.rs:343 | `Arc<AtomicU64>` | Next SST id allocator (shared into flush and compaction jobs) | flush.rs:171 (flush task), compaction job.rs:432 (compaction task), maintenance.rs:511 (shrink) — all `fetch_add` | manifest pump: `begin` snapshots it into the catalog counters (manifest.rs:789); engine.rs:1998 | Relaxed | 939440e (#63) |
| `Shared::blob_ids` shard.rs:344 | `Arc<AtomicU32>` | Next blob-file id allocator | compaction blob.rs:233 (flush, compaction and large-value writers via `Arc::clone`) | manifest.rs:790 | Relaxed | 939440e (#63) |
| `Shared::flushed_roots` shard.rs:279 | `FlushedRoots<(u16, u32)>` (flushed_roots.rs: a `Mutex<HashSet<_>>`) | Memtables `(shard, root)` whose SSTs are in the manifest, which views exclude until the owning shard retires them | `publish_view` (485: `FlushedRoots::publish`, the roots added and `view.store` run under the lock); shard thread `on_flushed` (3256: `retire`) | shard thread: `mem_sets` (2457), `read_latest` (3536; `view.load_full` inside `FlushedRoots::read`), `conflicts` (3606); open (via `mem_sets`) | Inner to `view_lock` (in `publish_view`, and in `mem_sets` when called from `publish_memtables`). In `read_latest`, held across `view.load_full()` on purpose, so that the shard sees both or neither. Loom-modelled | #131; 939440e (#63); #134 step 3 |
| `Shared::busy_ssts` shard.rs:354 | `Mutex<HashSet<SstId>>` | SSTs that a running compaction or relocation reads or replaces | shard thread: `start_compaction` (6764 add, 6810 remove on error), `on_compaction_done` (6835 remove); maintenance.rs:490 (relocation claim), 728 (`unclaim`) on an app thread | shard thread: `maintain` (6422, 6489), `start_blob_gc` (6573); maintenance.rs:535 (snapshot clone) | No nesting seen. maintenance.rs:535 clones the set before `Pager::clear_for` runs under the allocator lock | D60; 939440e (#63) |
| `LargeSettlement::pending` large_settle.rs:36 (`Shared::large`, shard.rs:285) | `Mutex<HashSet<BlobFileId>>` | Blob files of commit-time separated values whose commit has not settled, or whose `DropBlobFile` has not committed | `add_pending` (`large::separate`, app thread), `remove_pending` (on a failed publish, and in the release's manifest reply), `settle` (removed on success) | `note_logged` (shard thread, `run_group`); test hook `check_blob_accounting` (`pending`) | Inner in `settle` (logged → dropped → pending); alone everywhere else. Loom-modelled | #230; ec037d5 (#301); #134 step 3 |
| `Shared::large_open` shard.rs:361 | `AtomicUsize` | Live `LargeValues` guards. At zero, shards skip scanning logged batches for pointers | `LargeValues::new` (large.rs:84, +1), `Drop` (184, −1), on app threads | shard thread: `run_group` (shard.rs:4407) | fetch_add/sub AcqRel; load Acquire | #230; ec037d5 (#301) |
| `LargeSettlement::logged` large_settle.rs:37 | `Mutex<HashSet<BlobFileId>>` | Blob files whose batch reached its commit point (handed to the WAL) | shard thread: `note_logged` (77, after releasing pending) | `settle` (90; the thread that resolves the commit notifier) | Outermost in `settle` (logged → dropped → pending) | #230; ec037d5 (#301) |
| `LargeSettlement::dropped` large_settle.rs:38 | `Mutex<HashSet<BlobFileId>>` | Blob files the same-commit collapse (D34) dropped at apply, which the settle hook releases | shard thread: `apply` → `note_dropped` (85, not during replay) | `settle` | Middle in `settle` | D34, #230; ec037d5 (#301) |
| `Shared::view_versions` shard.rs:370 | `Mutex<BTreeMap<u64, ManifestVersion>>` | Published view version → manifest version, so reader-slot pins map to extents | `publish_view` (599); open (engine.rs:947) | `oldest_live_manifest` (635), called from `reclaim`/`try_reclaim` on any thread that drops a view | Inner to `view_lock` in `publish_view` | 939440e (#63); D61 (reclaim) |
| `Shared::locks` shard.rs:385 | `Mutex<Option<Locks>>` | The writer lock and presence, held for the writer's lifetime | open (engine.rs:592); `final_close` (680, `take`) | `final_close` | Taken while holding the manifest exclusion (`try_final_close` → `final_close`); released at once (`take()` in a single statement) | c9700de (#41) |
| `Shared::default_durability` shard.rs:386 | `AtomicU8` | Writer default durability | `Engine::set_default_durability` (engine.rs:1209, app thread) | `Shared::default_durability` (657), from engine.rs commit entry points (2449, 2544, 3235) | Relaxed | c9700de (#41) |
| `Shared::closed` shard.rs:387 | `AtomicBool` | The close finished | `try_final_close` (760, Release, after `manifest::release`); `close_reader` (engine.rs:2758, Release) | `visibility_ended` (496, SeqCst); `check_open` (engine.rs:2167, Acquire); hooks.rs:1040; Debug (439, Relaxed) | Release store; Acquire, SeqCst or Relaxed loads | c9700de (#41) |
| `Shared::closing` shard.rs:389 | `AtomicBool` | `Engine::close` has started, so background compaction stops | shard thread: `handle_msg(Close)` (7329, Release) | shard thread: `maintain` (6390); tablets.rs `maybe_balance` (1029); maintenance.rs:96, 389, 618 | Release and Acquire. (`Engine::closing` at engine.rs:2673 is a different field) | c9700de (#41) |
| `Shared::pager_poisoned` shard.rs:392 | `AtomicBool` | A manifest root commit failed, so the pager is poisoned and writes fail until reopen | manifest pump: manifest.rs:847, 861, 933 | shard thread: `spawn_flush` (3154), `stalled` (3957), `run_group` (4195), `advance_checkpoint` (6169), `on_checkpointed` (6231, 6238), `maintain` (6394), `try_finish_close` (6993); tablets.rs `validate_op` (579), `progress_op` (741), `maybe_balance` (1030); engine.rs:2170 | Release store; Acquire loads | D58; c9700de (#41) |
| `Shared::ts_floors` shard.rs:397 | `Vec<Padded>` (`Padded(AtomicU64)`, 64-byte aligned, shard.rs:204) | Per-shard largest default timestamp assigned | own shard thread: `default_ts` (3603, store), tablets.rs `route_member` (181, fetch_max), `raise_ts_floor` (2538); open (engine.rs:913) | other shard threads: `start_coordination` (5625); manifest pump: `begin` (manifest.rs:793); maintenance.rs:234; hooks.rs:709, 708 | store Release; fetch_max AcqRel; loads Acquire | D11; c9700de (#41) |
| `Shared::ts_raises` shard.rs:400 | `Vec<Padded>` | Per-shard floor raised by shards handing it tablets | other shard threads: tablets.rs `commit_op` (799, fetch_max, under `ts_raisers[i]`) | own shard: `default_ts` (3593), tablets.rs `below_floor` (262); hooks.rs:711 | fetch_max AcqRel; loads Acquire | D11; c1b02a2 (#97) |
| `Shared::ts_raisers` shard.rs:403 | `Vec<Mutex<(Timestamp, u64)>>` | Per shard, the current raise and a bitmask of the shards that set it, so a retried commit may tie only a raise its own shards made | tablets.rs `commit_op` (791) on the shard handing tablets away | own shard: `default_ts_or` (3580); a coordinator shard: `start_coordination` (5626) | `ts_raises[i].fetch_max` happens while it is held (an atomic, not a lock). It is never held together with another lock | D11; c1b02a2 (#97) |
| `Shared::tablet_epoch` shard.rs:410 | `AtomicU64` | Bumped when a tablet change finishes or is given up, so commits refused meanwhile may retry | tablets.rs `finished_op` (893) | tablets.rs `run_retries` (390), `maybe_balance` (989, 1090); shard.rs `start_coordination` (5679); engine.rs:360, 356 (the full-compaction round) | fetch_add AcqRel; loads Acquire | c1b02a2 (#97) |
| `Shared::full_compactions` shard.rs:413 | `AtomicUsize` | Full compactions in progress. The balancer starts no change meanwhile | engine.rs:356 (+1), 367 (`Drop`, −1), on the app thread in `Engine::compact` | tablets.rs `maybe_balance` (1031) | fetch_add/sub AcqRel; load Acquire | #94; 6944478 (#115) |
| `Shared::shard_died` shard.rs:416 | `AtomicBool` | A shard's thread panicked, or its `EngineShard` was dropped before close | `Shared::shard_died` (521), from `ShardState::abandon` (7103) and `report_panicked` (7116) | shard thread: `try_finish_close` (6994) | store SeqCst; load Acquire | #135; 3b6051d (#166) |
| `Shared::submitters` shard.rs:429 | `OnceLock<Vec<Submitter<ShardMsg>>>` | Every shard's inbox submitter, set once the runtime is built | open (engine.rs:988, 969) | `submitter` (446), `broadcast` (460), `Engine::close` (engine.rs:2686), metrics (engine.rs:1742) | OnceLock. Submitters are runtime queues (out of scope here) | c9700de (#41) |

### `SpareTask` / `ShardState`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `ShardState::spares_running` shard.rs:2141 (= `SpareTask::running` shard.rs:1595) | `Arc<AtomicBool>` | A WAL spare-segment task is in flight (one at a time) | shard thread: `maybe_prepare_spares` (5257, swap true); background task: `SpareTask::run` (1603, store false) | `maybe_prepare_spares` | swap AcqRel; store Release | D35 (amended by D157); c9700de (#41) |
| `ShardState::creation_wake` shard.rs:2239 | `bool` (shard-owned, not a primitive; listed for the D211 protocol) | A wake is armed for the stream's creation to settle, so `advance_checkpoint` registers at most one | shard thread: `advance_checkpoint` (6101, set when it holds a checkpoint back), the `ShardMsg::Created` handler (7225, cleared) | `advance_checkpoint` | none (one thread) | D211, ICR 0031, #520 |

### `TimerState` (timer_state.rs), shared between the shard and a `ClockTimer` background task (on the shard's scheduler, or a compaction pool thread when one is configured)

The struct is held as `Arc<TimerState>` in `ShardState::flush_retry`, `backoff_timer`, `checkpoint_timer`, `tablet_timer` and `balance_timer`, and in `RoomWait::timer`/`recheck`.

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `TimerState::cancel` timer_state.rs:43 | `AtomicBool` | Set by the shard to cancel the timer | shard thread: `TimerState::cancel` (64) | timer task: `cancelled`, and `sleep_with` after storing its waker (`ClockTimer::run`); shard: `finished` | Release store; Acquire loads. The task re-checks it after storing its waker. Loom-modelled | #84; 361e40f; #134 step 3 |
| `TimerState::done` timer_state.rs:45 | `AtomicBool` | The timer fired or gave up | timer task: `fire`, `give_up` (95) | shard thread: `finished`, `fired` | Release and Acquire | #84; 361e40f |
| `TimerState::frozen_at` timer_state.rs:48 | `AtomicU64` | The clock reading at which the timer gave up because the clock stopped (`u64::MAX` = it did not give up) | timer task: `give_up` (95, before `done`) | shard thread: `fired`, `frozen` | Release and Acquire; written before `done` is published. Loom-modelled | #84, #244; 361e40f; #134 step 3 |
| `TimerState::waker` timer_state.rs:50 | `Mutex<Option<W>>` (`W` = `TaskWaker`) | The sleeping timer task's waker, so a cancel ends the sleep at once | timer task: `sleep_with` (83) | shard thread: `cancel` (64, `take`; wakes after releasing) | No nesting. The handshake is store `cancel` (Release) → lock/take, on one side, and lock/store waker → load `cancel` (Acquire), on the other. Loom-modelled | #93 (closes #89); d5578ed; #263; #134 step 3 |

### Purge guards: shard thread vs. manifest pump

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `ShardState::guarded` shard.rs:2017 (an `Arc<PurgeGuard>` per entry; created by `guard_state`, 1256) | `Vec<(u32, FamilyId, Arc<PurgeGuard>)>`, the guard shared with the flush's manifest request | Per-flush delete-guard state (`PurgeGuard`, purge_guard.rs: in flight, voided or installing) | shard thread: `admit_against_guards` (5336, `void`); manifest pump: the flush's catalog closure (flush.rs:660, `claim`, under the `manifest` mutex) | shard thread: `on_flushed` (3226, `voided`) | CAS AcqRel/Acquire; load Acquire (inside `PurgeGuard`). Loom-modelled | #287; c391e07 (#315); #134 step 3 |
| `CompactionGuard::state` shard.rs:1280 (`ShardState::compaction_guard`) | `Arc<PurgeGuard>` | The same guard for a bottommost compaction's purge | shard thread: `admit_against_guards` (`void`); manifest pump: compaction catalog closure (compact.rs:806, `claim`, under the `manifest` mutex) | shard thread: `on_compaction_done` (6754); compact.rs:933 (`voided`) | As `ShardState::guarded` | #316; dc1c601 (#328); #134 step 3 |

### `LoadSlot` (load_slot.rs), `Shared::loads`, one per shard, 64-byte aligned

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `LoadSlot::writes` load_slot.rs:25 | `AtomicU64` | Moving average of the rows the shard wrote per balancer interval | own shard thread: `maybe_balance` → `LoadSlot::publish` (tablets.rs:1068) | other shard threads: `track_skew` and `choose` → `published_loads` → `LoadSlot::read_fresh` (tablets.rs:1317), and `track_pack` (the store's total, D217), only when `at` is fresh | store Relaxed; load Relaxed. Publication goes through `at` (Release/Acquire). Loom-modelled | c1b02a2 (#97); smoothed in #103; #134 step 3; also read every pass for D214's persistence (#547) |
| `LoadSlot::busy` load_slot.rs:27 | `AtomicU64` | Moving average of the fraction of the time the shard was busy, in millionths (D217) | own shard thread: `maybe_balance` → `LoadSlot::publish` (tablets.rs:1068), beside `writes` | other shard threads: `track_pack` (tablets.rs:1218) and `choose`'s packing step (tablets.rs:1515) → `LoadSlot::read_busy_fresh`, only when `at` is fresh | store Relaxed before `at`'s Release; load Relaxed after `at`'s Acquire, as `writes` | D217 (#478) |
| `LoadSlot::reserved` load_slot.rs:31 | `AtomicUsize` | `(tablet, family)` slots promised to in-flight changes that will hand this shard tablets | any shard thread running a change: `reserve_slots` (tablets.rs:732, `LoadSlot::reserve`, fetch_add), `release_slots` (753, `release`, fetch_sub) | `slots_fit` (725, `reserved`), `reserve_slots` (via the `fetch_add` result) | AcqRel RMW; load Acquire. Loom-modelled (all-or-none reservations never pass the limit) | #103; b4d6acb (#128); #134 step 3 |
| `LoadSlot::at` load_slot.rs:33 | `AtomicU64` | When `writes` and `busy` were published (monotonic ns; 0 = never) | own shard: `LoadSlot::publish` | other shards: `LoadSlot::read_fresh`, `read_busy_fresh` | store Release (after `writes` and `busy`); load Acquire | c1b02a2 (#97) |

Test-only and not counted: `stop: AtomicUsize` in the test `a_freeze_waiter_registering_during_a_take_is_never_lost` (shard.rs:7635), shared by scoped threads, Acquire loads.

### Lock nesting observed

Order is outer → inner. "Exclusion" means the `manifest_busy` flag claimed through `manifest::claim`, which is a lock in effect.

1. **`view_lock` → `flushed_roots`** in `Shared::publish_view` (shard.rs:485, `FlushedRoots::publish`). `view.store` happens inside `flushed_roots`.
2. **`view_lock` → `flushed_roots`** a second time, through the closure in `ShardState::publish_memtables` (shard.rs:2635). The closure runs under `view_lock` and calls `mem_sets` (2457, `FlushedRoots::read`). It is released before (1) takes it again.
3. **`view_lock` → `view_record`** in `publish_view` (shard.rs:588), with `shm.publish_view` called inside.
4. **`view_lock` → `view_versions`** in `publish_view` (shard.rs:599), with `shm.oldest_reader_pin` called inside.
5. **Exclusion → `view_lock` → {`view_record`, `view_versions`, `flushed_roots`}** via `manifest::end` → `publish_view` (manifest.rs:916). The `manifest` mutex is *not* held there: it is released in the block at manifest.rs:885–890.
6. **Exclusion → `manifest` mutex** in `manifest::begin` (manifest.rs:588) and `end` (887), and in `final_close` (shard.rs:700). Inside `begin`, **`manifest` mutex → guard `AtomicU8` CAS**: the flush and compaction catalog closures run under the writer (flush.rs:661, compact.rs:806). Pager-internal locks are taken under the mutex in `prepare`/`complete` (not inventoried).
7. **Exclusion → `locks`** in `try_final_close` → `final_close` (shard.rs:680).
8. **`CloseState` → exclusion (try-lock only)** in `try_final_close` (`CloseGate::try_start`); the final close runs after the gate's lock is released (#134 step 2).
9. *(Removed by #134 step 2: the gate has one lock.)*
10. **`LargeSettlement` logged → dropped → pending** in `LargeSettlement::settle` (large_settle.rs:90), run on whichever thread resolves the commit notifier. `note_logged` (77) takes pending, then logged, but in sequence: it drops pending first. That avoids an inversion against `settle`; the loom model deadlocks if it does not.
11. **`ts_raisers[i]` held while `ts_raises[i].fetch_max` runs** in tablets.rs `commit_op` (823–834). This is a lock plus an atomic, not two locks.
12. *(Removed by #134 step 2: `wake_visible` runs wakes after releasing `waiters.list`.)*
13. **`flushed_roots` held across `view.load_full()`** in `read_latest` (shard.rs:3536, `FlushedRoots::read`). `ArcSwap` is not a lock, but the ordering is relied on (#131).

No `Condvar` is declared in these files. Blocking waits go through runtime `Notifier`/`completion`, which are out of scope.

**74 primitives**


## Appendix B: `engine/src/engine.rs` and `engine/hooks.rs`

Scope: struct fields, statics and thread_locals **declared** in these two files (the only
submodule of `engine` is `hooks.rs`). Neither file declares a `static` or `thread_local`.

**Scope note on `Shared`.** `Shared` is declared in `crates/engine/src/shard.rs:320`, not in
these files; `engine.rs` only constructs it (`open_writer` engine.rs:552-633, `open_reader`
engine.rs:1063-1122) and touches some of its fields. Its declaration rows are in appendix A. The cross-reference below lists every `Shared` primitive that `engine.rs` /
`hooks.rs` read or write, with the engine.rs sites and orderings, so it can be merged into
appendix A's `Shared` table. Those rows are not in the count.

### `ReaderState` (engine.rs:184): a reader process's attachment (`Inner::reader`, `Some` only for `Role::Reader`)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ReaderState::presence` (engine.rs:186) | `Mutex<Option<Presence>>` | The reader's presence lock on the file (shm-init / last-one-out byte-range locks); `take()`n once at close. | `open_reader` (init, engine.rs:1128); `Inner::close_reader` takes it (engine.rs:2730-2734). Client thread calling `close`/`Drop for Engine`. | Only `close_reader`. | Taken alone (the `shm` guard at 2728 is dropped at the end of its block, 2730). `closing.swap(AcqRel)` gates it to one caller. | Milestone A (#41, c9700de). Last-one-out cleanup: issue #20 (comment at engine.rs:2739). Presence/read-write open: D36 (doc on `open_reader`). |
| `ReaderState::shm` (engine.rs:187) | `Mutex<Attachment>` | The current shm region generation, this reader's slot, whether it holds a pin, and the generation's live-snapshot count; replaced wholesale on re-attach. | `Inner::reader_snapshot` (engine.rs:2826-2842: re-attach on `is_stale`, sets `pinned`); any client thread taking a reader snapshot. | `reader_snapshot` (pin decision, 2845-2885); `Inner::catalog` reader branch (2180-2193, reads `manifest_version()`); `close_reader` (2728, `slot.unpin()`). | Outer lock: while held in `reader_snapshot`, `ReaderState::view` (2839) and `ReaderState::catalog` (2841) are taken (re-attach branch only). Dropped (`drop(guard)`, 2861) before `reader_view`. In `catalog()` it is a statement temporary, released before `reader_catalog`. | #140 (aa37256, PR #157): reader snapshots expire across a writer restart; Attachment split out of the earlier single region field. |
| `ReaderState::catalog` (engine.rs:189) | `Mutex<(ManifestVersion, Arc<Catalog>, FileRef)>` | Cache of the last catalog loaded, its manifest version and the file it was read from. | `Inner::reader_catalog` (2773-2812, reloads root and manifest **under the lock**); `reader_snapshot` re-attach resets `.0 = NO_MANIFEST_VERSION` (2841, under `shm`). | `reader_catalog`; `Inner::catalog` reader branch (2188); `reader_view` via `reader_catalog`. | Taken under `shm` at 2841. In `reader_catalog` held across `pager.reload_root()` and `manifest::load_root` (I/O under lock), and across the test hook `ReaderHooks::before_manifest_load.run()` (2783). Released before `reader_view` takes `view`. | Milestone A (#41). Pairing record with its own manifest version: #140 (doc at 2764-2795). |
| `ReaderState::view` (engine.rs:191) | `Mutex<Option<Arc<View>>>` | The last view built from the shm record (by view version), reused when version and catalog match. | `reader_view` (2801-2843, builds `view_from_record` **under the lock**); `reader_snapshot` re-attach clears it (2839, under `shm`). | `reader_view`. | Taken under `shm` at 2839. In `reader_view` taken after `reader_catalog` returned (catalog guard already dropped), held while building the view from shm. | Milestone A (#41). |
| `Attachment::live` (engine.rs:206; `Attachment` at 199, inside `ReaderState::shm`) | `Arc<AtomicUsize>` | Snapshots of this region generation still alive; when 0 the reader slot's pin may move forward. | `reader_snapshot` `fetch_add(1, AcqRel)` (2854, under `shm` lock); `LiveSnapshot::drop` `fetch_sub(1, AcqRel)` (snapshot.rs:999, any thread dropping a reader snapshot, no lock). Fresh `Arc` on re-attach (2837). | `reader_snapshot` `load(Acquire)` (2847, under `shm`). | Acquire / AcqRel. Increment and the pin decision are under `ReaderState::shm`; the decrement is lock-free. | Milestone A (#41); generation-scoped since #140 (comment 2833-2859). |

### `Inner` (engine.rs:210): everything behind an `Engine` / `Txn`

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `Inner::runtime` (engine.rs:218) | `Mutex<Option<Runtime<ShardState>>>` | The engine-owned shard runtime (threads); `None` for application-owned mode and readers. | `open_writer` engine-owned branch stores it (989, after `shared.submitters.set`); `Inner::close` `take()`s it (2701-2731), client thread in `close` / `Drop for Engine`. | Only `close`. | Taken alone; guard is a statement temporary, released before `rt.shutdown()` (which joins shard threads; panics caught, 2709). | Milestone A (#41). Panic reporting: #135 (`shard_panicked`). |
| `Inner::closing` (engine.rs:220) | `AtomicBool` | This handle's close has started (one-shot latch so `close`/`Drop` run once). Distinct from `Shared::closing`. | `Inner::close` `swap(true, AcqRel)` (2673); `Inner::close_reader` `swap(true, AcqRel)` (2724). | `Inner::check_open` `load(Acquire)` (2167), called by every writing/maintenance Engine method and `EngineShard::commit_local`. | Acquire / AcqRel. | Milestone A (#41, c9700de). |
| `Inner::max_value` (engine.rs:225) | `AtomicUsize` | Inline value limit (D16): longer puts are separated into a blob file at routing. | `open_writer` init (977; 0 for readers, 1152); test hook `Engine::set_inline_value_limit` `store(Relaxed)` (hooks.rs:388). | `Inner::inline_limit` `load(Relaxed)` (2329) from `separate_large` (2339): client threads in `submit`/`submit_check_and_mutate`, shard thread in `EngineShard::commit_local`. | Relaxed. Atomic only so a test can shrink it (doc 222-224). | #230 (ec037d5, PR #301); D16. |

Other structs in `engine.rs` with no primitive fields: `Engine` (Arc<Inner>), `CompactRounds` (344; plain `epoch: u64`, uses `Shared::full_compactions` / `tablet_epoch`), `PendingMaintenance` (2086; owns `Waiter`s, single-owner), `Spilled` (1809; open-time only), `EngineShard` (3152; owns a `ShardDriver`), `ReplayedRecord` (326). `Waiter`/`Notifier` (`pigeonhole_runtime::completion`) are channels owned by the runtime crate; not inventoried here.

### Test hooks only (`#[cfg(feature = "test-hooks")]`, engine/hooks.rs)

`Once` (hooks.rs:44) is `struct Once(Mutex<Option<Box<dyn FnOnce() + Send>>>)`: `set` stores under the lock, `run` takes under the lock and calls the callback **after** releasing it (hooks.rs:52-57), so a `Once` never nests with the callback's own locking. Each `Once` field below is one such Mutex.

#### `Hooks` (hooks.rs:63, lives at `Shared::hooks`)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `Hooks::record` (hooks.rs:66) | `AtomicBool` | Whether `compactions` and `appended` record. | `Engine::record_history` `store(Release)` (hooks.rs:763), test thread. | Shard threads: WAL append (shard.rs:4420), shard.rs:6294; flush (flush.rs:510-514); all `load(Acquire)`. | Release / Acquire. | #148 follow-up of D164 (4777e8e, PR #261). |
| `Hooks::compactions` (hooks.rs:69) | `Mutex<Vec<CompactionRecord>>` | Every committed compaction/flush record, in commit order. | Manifest commit `begin` (manifest.rs:823-829) `extend`, after `drop(writer)` (manifest lock released, 822). | `Engine::take_compactions` (hooks.rs:771-780), `take_gc_records` (787-795), test thread. | Taken alone. | Milestone B (#63, 939440e); flush records #287. |
| `Hooks::appended` (hooks.rs:71) | `Mutex<Vec<AppendedRecord>>` | Every WAL record appended, per-stream append order. | Shard thread after each WAL append (shard.rs:4427-4433). | `Engine::take_appended` (hooks.rs:845-852). | Taken alone (not checked whether a shard-internal lock is held at shard.rs:4427; shard state there is `&mut self`, no Mutex seen). | Milestone B (#63). |
| `Hooks::manifest_race` (hooks.rs:74) | `AtomicBool` | Arms the manifest queue's release-window probe. | `Engine::probe_manifest_release_window` `store(true, Release)` (hooks.rs:1051); `manifest::race_window` `swap(false, AcqRel)` (manifest.rs:1147). | `race_window` (swap). | Release / AcqRel. | Milestone B (#63). |
| `Hooks::manifest_race_waiter` (hooks.rs:75) | `Mutex<Option<Waiter<Result<ManifestVersion>>>>` | Waiter of the request `race_window` pushed. | `race_window` (manifest.rs:1150-1154), after `manifest_queue.push`. | `probe_manifest_release_window` takes it (hooks.rs:1053-1058). | Taken alone. | Milestone B (#63). |
| `Hooks::manifest_thread_parks` (hooks.rs:78) | `AtomicUsize` | Threads that parked in `commit_req_from_thread` after their last look (`Engine::manifest_thread_parks`, test `manifest_wake`). | `commit_req_from_thread` `fetch_add(AcqRel)` just before `park` (manifest.rs). | `manifest_thread_parks` `load(Acquire)`. | AcqRel / Acquire. | #134 step 2 (#536). |
| `Hooks::before_view_publish` (hooks.rs:78) | `Once` | Callback at the start of the next `publish_view`. | `Engine::before_next_view_publish` (hooks.rs:889). | `Shared::publish_view` (shard.rs:579), **before** `view_lock` is taken. | Mutex released before callback. | #131 (5456f2f, PR #153). |
| `Hooks::room_wait_once` (hooks.rs:81) | `AtomicBool` | Makes the next group's second member wait for arena room. | `Engine::force_room_wait_once` `store(true, Release)` (hooks.rs:572-578); shard thread `swap(false, AcqRel)` (shard.rs:4173-4177). | Same swap. | Release / AcqRel. | #315 review (c391e07, #287). |
| `Hooks::before_snapshot_view_load` (hooks.rs:84) | `Once` | Callback between pinning a writer snapshot's seqno and loading the view. | `Engine::before_snapshot_view_load` (hooks.rs:585). | `Inner::snapshot` (engine.rs:2603), client thread, while holding a `SeqnoPin` (not a lock guard). | — | #315 review (c391e07). |
| `Hooks::before_latest_view_load` (hooks.rs:88) | `Once` | Callback in unpinned reads-as-of-now between the visible-seqno read and the view load. | `Engine::before_latest_view_load` (hooks.rs:594). | `Engine::get_latest` (engine.rs:1354), `Inner::latest_view` (engine.rs:2583). | — | #320 (010e824, PR #392). |
| `Hooks::before_shrink_relocates` (hooks.rs:91) | `Once` | Callback in a shrink round between catalog read and relocations. | `Engine::before_shrink_relocates` (hooks.rs:565). | maintenance.rs:435 (manifest guard there is a statement temporary, released). | — | #138 (6e20df3, PR #200). |
| `Hooks::after_backup_releases_memtables` (hooks.rs:94) | `Once` | Callback after backup released its memtables, before its merge. | `Engine::after_backup_releases_memtables` (hooks.rs:609-613). | maintenance.rs:150. | — | #262 (9f352b2, PR #268). |
| `Hooks::before_shrink_commits` (hooks.rs:97) | `Once` | Callback in a committing shrink round, after copies, before commit. | `Engine::before_shrink_commits` (hooks.rs:622). | maintenance.rs:607. | — | #138 (6e20df3). |
| `Hooks::manifest_park` (hooks.rs:100) | `AtomicBool` | Parks background compaction manifest commits before `end`. | `Engine::park_manifest_commits` `store(Release)` (hooks.rs:933). | `manifest::parks` `load(Acquire)` (manifest.rs:1178), called from `manifest::parked` inside `Flight::completed`, under the flight's `state` lock (manifest.rs:1163-1165), and from `take_done`'s `held` closure (`held_by_hook`). | Release / Acquire. | #78 / #81 (ff30866). |
| `Hooks::manifest_parked` (hooks.rs:101) | `Mutex<Option<TaskWaker>>` | Waker of the parked manifest pump. | `manifest::parked` stores (manifest.rs:1166-1171), **after** `Flight::completed` has released the `manifest_flight` lock; `park_manifest_commits(false)` takes (hooks.rs:934-941) and wakes outside the lock. | `Engine::manifest_commit_parked` (hooks.rs:1015-1022). | Not nested with `manifest_flight` (block-scoped). | #78 / #81 (ff30866). |
| `Hooks::compaction_hold` (hooks.rs:104) | `AtomicBool` | Holds each compaction job before its first slice. | `Engine::hold_compactions` `store(Release)` (hooks.rs:979). | Compaction job `load(Acquire)` (compact.rs:919-924), background compaction thread / shard. | Release / Acquire. | #316 (dc1c601, PR #328). |
| `Hooks::compaction_held` (hooks.rs:105) | `Mutex<Option<TaskWaker>>` | The held compaction job's waker; `None` after release. | Compaction job (compact.rs:926-935) stores its waker; `Engine::release_held_compaction` takes and wakes outside the lock (hooks.rs:987-999). | `Engine::compaction_held` (hooks.rs:1003-1011); compaction job checks `is_none()` for release. | Taken alone. | #316 (dc1c601). |
| `Hooks::refuse_checkpoints` (hooks.rs:108) | `AtomicBool` | Refuse checkpoint-only manifest batches with `NoSpace`. | `Engine::refuse_checkpoints` `store(Release)` (hooks.rs:896-901). | `manifest::begin` `load(Acquire)` (manifest.rs:809), while holding `Shared::manifest` (`writer` guard from manifest.rs:587). | Release / Acquire. | #139 (e1a10fd, PR #179). |
| `Hooks::omit_blob_refs` (hooks.rs:111) | `AtomicBool` | Commit no `SstBlobRefs`, as pre-tag-13 builds. | `Engine::omit_blob_refs` `store(Release)` (hooks.rs:908-913). | `manifest::begin` `load(Acquire)` (manifest.rs:804), under `Shared::manifest`. | Release / Acquire. | #240 (acc7e6c, PR #285). |
| `Hooks::fail_next_apply` (hooks.rs:114) | `AtomicBool` | Fail the next single-shard batch's apply with `Busy` after its WAL append. | `Engine::fail_next_apply` `store(true, Release)` (hooks.rs:920-925); shard thread `swap(false, AcqRel)` (shard.rs:4547-4551). | Same swap. | Release / AcqRel. | #230 (ec037d5, PR #301). |
| `Hooks::flush_gc_mutation` (hooks.rs:118) | `AtomicU8` | Deliberate flush-GC fault selector (0 none, 1-3 mutations). | `Engine::mutate_flush_gc` `store(Release)` (hooks.rs:744-756). | `flush_gc` `load(Acquire)` (flush.rs:304-308); shard.rs:5427 `load(Acquire)`. | Release / Acquire. | #287 (c391e07, PR #315). |

#### `ShardCounters` (hooks.rs:139, lives at `ShardMetrics::hooks`, one per shard)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ShardCounters::aborted` (hooks.rs:141) | `AtomicU64` | Size of the shard's aborted-seqno set after its last batch. | Owning shard thread `store(Relaxed)` (shard.rs:7446-7447). | `Engine::aborted_seqnos` `load(Relaxed)` (hooks.rs:805). | Relaxed. | #144 (e18b2b5, PR #160). |
| `ShardCounters::arena` (hooks.rs:145) | `Mutex<(u64, u64, u64)>` | One consistent snapshot of arena free bytes, largest free run, size. | Owning shard `publish_arena` (shard.rs:2492-2497). | `Engine::arena_free` (hooks.rs:832-838). | Taken alone. A Mutex (not 3 atomics) so a reader never mixes batches. | #141; made one snapshot in #336 (ff6ad97). |
| `ShardCounters::run_waits` (hooks.rs:148) | `AtomicU64` | Reservations with enough free bytes but no long-enough run. | Owning shard `fetch_add(1, Relaxed)` (shard.rs:2954-2957). | `Engine::arena_run_waits` `load(Relaxed)` (hooks.rs:812-816). | Relaxed. | #141 (a62da94, PR #186). |
| `ShardCounters::purge_voids` (hooks.rs:151) | `AtomicU64` | Compactions whose purge a write voided before install. | Owning shard `fetch_add(1, Relaxed)` (shard.rs:6880-6883). | `Engine::compaction_purge_voids` `load(Relaxed)` (hooks.rs:822-826). | Relaxed. | #316 (dc1c601, PR #328). |

#### `ReaderHooks` (hooks.rs:156, lives at `ReaderState::hooks`, engine.rs:195)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ReaderHooks::after_record` (hooks.rs:158) | `Once` | Callback between reading the view record and loading its catalog. | `Engine::on_reader_view_record` (hooks.rs:950-954). | `Inner::reader_view` (engine.rs:2797), no reader lock held (shm guard dropped at 2886). | — | #140 (aa37256, PR #157). |
| `ReaderHooks::before_manifest_load` (hooks.rs:161) | `Once` | Callback after the durable root matched the record's version, before the manifest read. | `Engine::on_reader_manifest_load` (hooks.rs:959-963). | `Inner::reader_catalog` (engine.rs:2783), **while holding `ReaderState::catalog`**. | Callback runs under `ReaderState::catalog` (the Once's own mutex is released first). | #140 (aa37256). |

### Cross-reference: `Shared` primitives (declared in shard.rs, appendix A) as used from engine.rs and hooks.rs

Not counted. Sites listed are only those in my two files; other writers/readers live in shard.rs, manifest.rs, flush.rs, compact.rs, maintenance.rs, etc.

| Field (shard.rs decl.) | Type | engine.rs / hooks.rs sites | Ordering / nesting at those sites |
|---|---|---|---|
| `Shared::view` (shard.rs:324) | `ArcSwap<View>` | Writer: `open_writer` `store` first view (958). Readers (client threads, any shard via `commit_local`): `get_latest` (1363), `shard_of` (1660), `shard_stats` (1698), `Inner::catalog` (2198), `catalog_change` (2213, `load_full`), `submit` (2443), `submit_check_and_mutate` (2536), `latest_view` (2592), `snapshot` (2612, `load_full`), `commit_local` (3227), hooks `reader_pin_and_view` (hooks.rs:970). | ArcSwap (SeqCst internally). In open: `view_versions` lock (953-957, temporary) then `view.store` (958) then `view_lock` (959-962, temporary): sequential, not nested. Snapshot: seqno pinned before view load (comment 2603-2606). |
| `Shared::view_lock` (shard.rs:331) | `Mutex<u64>` | `open_writer` sets 1 (959-962). | Alone. |
| `Shared::view_versions` (shard.rs:365) | `Mutex<BTreeMap<u64, ManifestVersion>>` | `open_writer` inserts (1, v) (953-957). | Alone. |
| `Shared::manifest` (shard.rs:335) | `Mutex<ManifestWriter>` | `sweep_unreferenced_blobs` commit (1961-1965), `flush_recovered` commit (2017-2021); open thread. | Alone; held across the manifest commit I/O. |
| `Shared::sst_ids` (shard.rs:343) | `Arc<AtomicU64>` | `spill_recovered` clones the Arc (1880); `flush_recovered` `load(Relaxed)` (2006). | Relaxed (open thread only). |
| `Shared::default_durability` (shard.rs:378) | `AtomicU8` | `Engine::set_default_durability` `store(Relaxed)` (1214-1219); reads via `Shared::default_durability()` (`load(Relaxed)`, shard.rs:657) at 1210, 2440, 2535, 3226. | Relaxed. |
| `Shared::closed` (shard.rs:379) | `AtomicBool` | `check_open` `load(Acquire)` (2175); `close_reader` `store(true, Release)` (2783); hooks `close_finished` `load(Acquire)` (hooks.rs:1040). | Acquire / Release. |
| `Shared::pager_poisoned` (shard.rs:388) | `AtomicBool` | `check_open` `load(Acquire)` (2178). | Acquire. D58. |
| `Shared::close` (`CloseGate`, close_gate.rs) | `Mutex<Gate>` | `Inner::close` `register`s its waiter and is answered at once if the final close already ran; `EngineShard::closed` reads `outcome`; hook `final_close_pending` reads `pending`. | One lock (appendix A). #134. |
| `Shared::ts_floors[i]` (shard.rs:392) | `Vec<Padded(AtomicU64)>` | `open_writer` `store(Release)` (920). | Release (open thread). D11. |
| `Shared::tablet_epoch` (shard.rs:404) | `AtomicU64` | `CompactRounds::new`/`again` `load(Acquire)` (360, 366). | Acquire. #94. |
| `Shared::full_compactions` (shard.rs:407) | `AtomicUsize` | `CompactRounds::new` `fetch_add(1, AcqRel)` (356); `Drop for CompactRounds` `fetch_sub(1, AcqRel)` (377). | AcqRel. #94. |
| `Shared::submitters` (shard.rs:421) | `OnceLock<Vec<Submitter<ShardMsg>>>` | `open_writer` `set` (995, 1004) before the runtime lock store / before shards are handed out; `get` in `metrics` (1750), `close` (2711), `Shared::submitter` (expects set). | OnceLock. |
| `Shared::async_sync_reads`, `commit_parks` (shard.rs:326, 338) | `AtomicU64` | `Engine::metrics` `load(Relaxed)` (1747, 1749). | Relaxed. D196, ICR 0027. |
| `Shared::metrics[i].*` counters, `.wal` (ShardMetrics) | atomics, `OnceLock` (`wal`, inferred from `.set`/`.get`) | `metrics` loads all `Relaxed` (1712-1744); `tablet_refusals` (1674); `shard_stats` (1688); `open_writer` `wal.set(...)` (854, 878, 910). | Relaxed. |
| `Shared::visibility.{waits,scanned,passes,woken}` (VisibilityCounts), `Shared::waiters.contended()` (WatermarkWaiters) | atomics | `metrics` `load(Relaxed)` (1838-1848). | Relaxed. |
| `Shared::live_seqnos` | `Arc<LiveSeqnos>` (has internal locks, not checked) | `Inner::snapshot` `SeqnoPin::pin_visible` (2607-2608). | Internal. |
| `Shared::drivers` | `waker::Drivers` (internals not checked) | `EngineShard::run_once` `enter` (3176), `Drop for EngineShard` `dropped` (3280-3283), `close` `driven_elsewhere` (2720). | Internal. |

### Lock nesting observed (engine.rs / hooks.rs)

- **N1** *(Removed by #134 step 2: `Inner::close` takes the close gate's one lock, `register`.)*
- **N2** `Inner::reader_snapshot` (engine.rs:2826 -> 2864): `ReaderState::shm` -> `ReaderState::view` (re-attach branch, statement temporary).
- **N3** `Inner::reader_snapshot` (engine.rs:2826 -> 2866): `ReaderState::shm` -> `ReaderState::catalog` (re-attach branch, statement temporary).
- **N4** `Inner::reader_catalog` (engine.rs:2773-2787): `ReaderState::catalog` held while running the test hook `ReaderHooks::before_manifest_load` (its `Once` mutex taken then released before the callback) and across `pager.reload_root()` / `manifest::load_root` (pager internals may lock; not checked). Test hook nesting only: `catalog` -> `Once` mutex.
- **N5** `Inner::reader_view` (engine.rs:2801-2817): `ReaderState::view` held across `view_from_record` (shm reads, block cache `Arc` clone); no other engine-level lock taken inside as far as these files show.
- Not nested (checked): `Inner::catalog` reader branch takes `shm` (statement temporary, 2188-2193), then `reader_catalog` takes `catalog`, then `catalog` again at 2196 (separate statements). `close_reader` takes `shm` (block, 2752-2755) then `presence` (2756-2759) sequentially. `reader_view`'s `catalog` (inside `reader_catalog`) is released before it takes `view`, so no `catalog` -> `view` order exists. `open_writer` locks `view_versions`, then `view_lock` sequentially (953-962). `close` releases `runtime` before `rt.shutdown()`.
- No condvar waits in these files. Blocking waits outside locks: `rx.wait()` in `Inner::close` (2722, no lock held), `Waiter::wait` in `PendingMaintenance::wait` (2100, no lock held), `std::thread::sleep`/`yield_now` in `reader_snapshot` (2913-2918, no lock held: `shm` dropped at 2886, `view`/`catalog` released by then).
- Consistent order across the reader path: `ReaderState::shm` before `view` / `catalog`; `catalog` and `view` are never held together.

Uncertainties: `Drivers`, `LiveSeqnos`, `MaintenanceGuard`, `Waiter`/`Notifier` internals were not inspected.

**35 primitives** declared in these files: 8 production (`ReaderState` 4, `Attachment` 1, `Inner` 3) + 27 test-hook-only (`Hooks` 15 atomics/mutexes + 6 `Once`, `ShardCounters` 4, `ReaderHooks` 2 `Once`). `Shared` appendix rows (declared in shard.rs) are not counted.


## Appendix C: The other `engine/src` files (manifest, snapshot, read path, flush, compaction, wakers)

Files covered: catalog.rs, compact.rs, error.rs, flush.rs, large.rs, lib.rs, maintenance.rs,
manifest.rs, nonblocking.rs, options.rs, read.rs, row_cache.rs, snapshot.rs, source.rs,
waker.rs, write.rs. catalog.rs, error.rs, lib.rs, options.rs, large.rs, maintenance.rs and
nonblocking.rs declare no primitive of their own (large.rs, maintenance.rs and nonblocking.rs
use `Shared` fields declared in shard.rs; see "Used here, declared elsewhere" below).

**33 primitives** (23 struct fields / statics + 10 `thread_local!`s).

### Manifest commit path (manifest.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ManifestQueue::reqs` manifest.rs:426 | `Mutex<Vec<ManifestReq>>` | The pending manifest change requests (a `Mutex<Vec>` used as an MPSC queue, taken whole). | `ManifestQueue::push` from `manifest::submit` (shard threads: flush.rs:702, compact.rs:836, shard/tablets.rs:853, shard.rs:6192; large.rs:176 `LargeValues::release` on any thread), `commit_held`, `commit_req_from_thread` (application threads), test hook `race_window`. | `take` in `manifest::begin` (whoever holds `manifest_busy`: pump on a shard thread, or an application thread in `drain_sync`); `is_empty` in `ManifestPump::run` and the re-check loop of `commit_req_from_thread` / hooks.rs:878. | Leaf lock: held only for push/take/is_empty. `begin` takes it (manifest.rs:582) before locking `Shared::manifest` (589), never inside. The push happens-before the `take` through this mutex, which is what lets `begin` read `sst_ids`/`blob_ids` with `Relaxed`. Poison ignored (`PoisonError::into_inner`). | Milestone B (#63, 939440e). |
| `Flight::state` manifest/flight.rs:24 | `Mutex<FlightState>` (`generation`, `commit: Option<Commit>`, `result: Option<RootResult>`) | The one manifest commit whose root commit is in flight, held in `Shared` (as `Shared::manifest_flight`, shard.rs:418) so a blocked application thread can finish it. | `Flight::start` (`ManifestPump::start`, shard thread, under `manifest_busy`); `Flight::complete` (the root-commit completion callback, on the thread that reaps the pager I/O); `stage` / `take_done` / `abandon` take `commit`+`result` (pump `run`, `commit_req_from_thread`, `ManifestPump::drop`). | `ready` (`commit_req_from_thread` before parking), `parked` (test hook). | Leaf: guard is dropped before `wake_threads` in `complete` (manifest/flight.rs:87-95) and, in `parked`, before the hooks lock (`Flight::completed` returns first, manifest.rs:1163-1171). `take_done` loads `hooks.manifest_park` (Acquire) while holding it (test-hooks only). `generation` lets a pump recognise its own commit (`Stage::Gone` if a thread finished it). | #135 via #166 (3b6051d): "otherwise the exclusion would stay held until the pump's shard runs again, which never happens when the blocked thread is the one that drives it (application-owned mode)". D88/D150. |
| `Flight::threads` manifest/flight.rs:27 | `Mutex<Vec<std::task::Waker>>` | Thread wakers of application threads parked in `commit_req_from_thread`. | `Flight::register` (application threads, deduped by `will_wake`); `wake_threads` empties it. | `wake_threads`, called by `Flight::complete` (I/O completion) and `manifest::release` (any exclusion holder). | Leaf; wakers are woken after the guard is dropped (manifest/flight.rs:146-158). | #135, #166. |
| `Flight::thread_count` manifest/flight.rs:28 | `AtomicUsize` | Length of `threads`, so `wake_threads` skips the lock when no thread waits. | `register` (`store`, SeqCst, under `threads` lock); `wake_threads` (`store(0)`, SeqCst, under lock). | `wake_threads` fast path (`load`, SeqCst, no lock). | SeqCst on all three. Pairs with the waiter's sequence register → `manifest_busy.load(SeqCst)` / `ready()` (manifest.rs:1120-1123). `complete` against `ready()` is ordered by the `state` lock. A release's `wake_threads` has had no SeqCst fence before it since #134 step 2, so a waiter may miss it; it is then woken by its own request's answer (whoever holds the exclusion re-checks the queue after releasing) or by the root commit's completion. The loom models (manifest/flight.rs) pass with release's `wake_threads` removed, so liveness does not rest on it | #135, #166. |

### Views, snapshots and pins (snapshot.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `OpenSst::reader` snapshot.rs:184 | `OnceLock<Arc<SstReader>>` | An SST's reader, opened lazily on first use, or pre-set by the flush/compaction that wrote it. | `OpenSst::new` (pre-set; `SstSet::rebuild` inside `manifest::end`'s `publish_view` closure, under `Shared::view_lock`); `reader` / `reader_cache_only` (any thread reading through a `View`: client threads, async read futures, shard threads, flush/compaction tasks). | Same paths (`get`). | OnceLock internal sync. Racing openers both open; the loser's `set` fails and it returns the winner's reader (`self.reader.get().map_or(r, ..)`, snapshot.rs:238). | Milestone B (#63). Pre-set so "a hot path never opens one". |
| `OpenBlob::reader` snapshot.rs:267 | `OnceLock<Arc<BlobReader>>` | A blob file's reader, built on first use (no I/O). | `OpenBlob::reader` via `get_or_init` (any reading thread, blob GC/compaction). | Same. | OnceLock; `get_or_init` (one initializer runs). | Blob separation, #33 via #235 (cbcfaf6). |
| `LiveViews::versions` snapshot.rs:718 | `Mutex<BTreeMap<ManifestVersion, u32>>` (loom's under `engine_loom`) | Refcount of manifest versions of every live in-process `View`, so the pager reclaims only extents no view reaches. | `register` from `ViewPin::new` (inside `Shared::publish_view`'s closure, manifest.rs:960, and other publishers in shard.rs); `unregister` from `ViewPin::drop` (any thread dropping the last `Arc<View>`). | `oldest` from `Shared::oldest_live_manifest` (shard.rs:630) → `reclaim` (manifest `end`, shrink, `try_reclaim`). | Taken under `Shared::view_lock` when a view is published (`publish_view` → closure → `ViewPin::new`). Released before `on_oldest_released` runs (`unregister` returns, then the callback; snapshot.rs:790-794). `oldest()` releases it before `oldest_live_manifest` locks `view_versions`. | D61; Milestone B (#63). Loom-modelled (#134 step 3) |
| `LiveViews::on_oldest_released` snapshot.rs:719 | `OnceLock<Box<dyn Fn() + Send + Sync>>` | Callback run when the last view of the oldest version drops (calls `Shared::try_reclaim` through a `Weak`). | Set once at open (engine.rs:643). | `ViewPin::drop` on the dropping thread. | OnceLock; callback runs outside the `versions` lock and must never block. | #337 (a837d06), the large_values flake. |
| `LiveSeqnos::seqnos` snapshot.rs:801 | `Mutex<BTreeMap<Seqno, u32>>` | Refcount of every live in-process snapshot seqno, for GC's `GcPolicy::snapshots`. | `SeqnoPin::pin_visible` (client thread taking a snapshot, engine.rs:2600); `SeqnoPin::drop` (any thread). | `LiveSeqnos::list` from `compact::gc_snapshots` (compact.rs:343: flush and compaction planning/tasks on shard threads). | `pin_visible` reads the visible seqno (`visible()` = `shm.visible_seqno()`, an atomic load) while holding the lock, in one critical section with `gc_snapshots`'s read of the list. | Struct #63; the in-lock read is from #315's review (#287, D191). D70. |
| `LiveSnapshot::count` snapshot.rs:975 | `Arc<AtomicUsize>` (shared with the reader attachment's `live`, engine.rs:206) | Number of live snapshots of a **reader process** in the current region generation; at zero the reader moves its shm slot pin forward. | `fetch_add(AcqRel)` at engine.rs:2854 (reader process taking a snapshot); `fetch_sub(AcqRel)` in `LiveSnapshot::drop` (any thread). | `load(Acquire)` at engine.rs:2847 (next snapshot decides whether to re-pin). | AcqRel / Acquire. The struct's `check_current` also issues `fence(Acquire)` before `shm.is_stale()` (seqlock-style validation, #140). | Milestone A (#41, c9700de); expiry check #140. |

### Read path (read.rs, source.rs, row_cache.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `ResolverBlobs::error` read.rs:281 | `Mutex<Option<Error>>` | First error a separated-value fetch hit during a resolve (the `BlobFetch` hook cannot return errors). | `BlobFetch::fetch` (read.rs:324) on the resolving thread. | `ResolverBlobs::check` after each resolver step (same thread). | Leaf. In practice single-threaded per read; the Mutex exists because `BlobFetch: Send + Sync` (compaction/src/blob.rs:314). | #33 via #235 (cbcfaf6). |
| `RowCaches::present` row_cache.rs:50 | `Box<[AtomicU64]>` | Per slot, the tag of the family row filled last: a lookup whose tag differs misses without taking the cache's lock. A hint only. | `RowCaches::store` (`store`, Relaxed) after `cache.insert` (row-cache fill on a client thread or async read future: engine.rs:1361/1549, nonblocking.rs:256/366). | `RowCaches::get` (`load`, Relaxed). | Relaxed: correctness comes from the `RowCache` itself verifying family/row/epoch under its own lock and from the `RowEpochs` (`lw`) watermark protocol (fetch_max Release / load Acquire, in the cache crate). | #404, D201, ICR 0019 via #446 (e5f8359). |
| `RowCaches::ghost` row_cache.rs:54 | `Box<[AtomicU64]>` | Per slot, the family row that missed last: a miss fills only on the second miss (admission). | `RowCaches::admit` (`swap`, Relaxed) on reading threads. | Same `swap`. | Relaxed; a lost update only costs an extra miss. | #404, D201. |
| `RowCaches::hits` row_cache.rs:61 | `AtomicU64` | Row-cache hit counter. | `get` (`fetch_add`, Relaxed), reading threads. | `stats` (`load`, Relaxed) for `Engine::row_cache_stats` (engine.rs:1638). | Relaxed statistic. | #404, D201. |
| `RowCaches::misses` row_cache.rs:62 | `AtomicU64` | Row-cache miss counter. | `get` (`fetch_add`, Relaxed). | `stats` (Relaxed). | Relaxed statistic. | #404, D201. |
| `RowCaches::fills` row_cache.rs:63 | `AtomicU64` | Row-cache fill counter. | `insert` (`fetch_add`, Relaxed). | `stats` (Relaxed). | Relaxed statistic. | #404, D201. |

### Shard drivers and commit waits (waker.rs, write.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `Drivers.0` waker.rs:70 (`Shared::drivers`) | `Box<[AtomicU64]>` | Per application-owned shard, the token of the thread that last drove it (0 never, `u64::MAX` dropped): such a thread must not block on shard work. | `Drivers::enter` from `EngineShard::run_once` (engine.rs:3151; `load` Relaxed, `store` Release only if changed); `Drivers::dropped` from `EngineShard::drop` (engine.rs:3253, `store` Release). | `current_drives` (write.rs:493/534/571/593, shard.rs:531; `load` Acquire), `driven_elsewhere` (engine.rs:2695 close; `load` Acquire). | Release / Acquire; Relaxed self-check in `enter`. | D88 (amended by D150, D151), #135 via #166 (3b6051d). |
| `static NEXT` waker.rs:78 (inside `TOKEN`'s initializer) | `AtomicU64` | Allocator of per-thread driver tokens (starts at 1). | `TOKEN` initialisation on each thread (`fetch_add`, Relaxed). | Same. | Relaxed (uniqueness only). | #135, #166. |

### Flush / compaction / ids (flush.rs, compact.rs)

| Name | Type | Protects / means | Writers | Readers | Ordering / nesting | Why / history |
|---|---|---|---|---|---|---|
| `SstSink::sst_ids` flush.rs:91 | `Arc<AtomicU64>` (a clone of `Shared::sst_ids`, shard.rs:343, or a local counter for a backup copy, maintenance.rs:89) | Next SST id; each output SST takes one. | `SstSink` cutting an output (flush.rs:171, `fetch_add` Relaxed): flush tasks on shard threads, engine.rs:1870, maintenance `copy_at`. Also maintenance.rs:511 on `Shared::sst_ids` directly (shrink). | `manifest::begin` (manifest.rs:789, `load` Relaxed) into `Counters`; maintenance.rs:230 for the copy's counters. | Relaxed. A request naming an id is pushed through `ManifestQueue::reqs` (Mutex) after the `fetch_add`, so the `Relaxed` load in `begin` that follows the `take` sees a value above every id it commits. | #63 (939440e). `blob_ids` (flush.rs:124) is only a parameter, moved into `pigeonhole_compaction::BlobSink`. |
| `FlushItem::guard` flush.rs:57 | `Option<Arc<PurgeGuard>>` (purge_guard.rs) | The flush GC guard (#287): versions beyond `max_versions` may be purged only if no delete in the family voided the guard before the commit installs it. | Created by the shard (`guard_state`, shard.rs:1256); voided by the shard on a delete (`PurgeGuard::void`, `admit_against_guards`); claimed by the flush's `ReqKind::Catalog` closure (flush.rs:660, `PurgeGuard::claim`) that runs inside `manifest::begin` (pump or application thread). | Shard (`PurgeGuard::voided`, shard.rs:3226). | CAS AcqRel / Acquire inside `PurgeGuard`; the claim runs under the `Shared::manifest` writer Mutex (inside `begin`). A failed claim refuses the request (`Error::Busy`) and `on_refusal` frees its outputs. | #287 via #315 (c391e07), D191. |
| `FlushTask::install_seqnos` flush.rs:394 (test-hooks) | `Vec<Arc<AtomicU64>>` (each the `SeqnoCell` of a `CompactionRecord`) | Where the commit records the visible seqno at which a guarded purge installed. | The flush's catalog closure (flush.rs:678, `store` Release, under the manifest writer lock). | Tests via `SeqnoCell::get` (Acquire). | Release / Acquire. | #287, #315. |
| `SeqnoCell.0` compact.rs:74 | `Arc<AtomicU64>` | The install seqno of a flush record (test hook), compared by value. | As `install_seqnos`. | `SeqnoCell::get` (`load` Acquire) in tests / `PartialEq`. | Acquire. | #287, #315 (c391e07). |
| `CompactionWork::guard` compact.rs:479 | `Option<Arc<PurgeGuard>>` | A bottommost purge's guard (#316): installs only if no write below its `min_ts_above` voided it since the plan. | Shard voids it (`admit_against_guards`); compaction's catalog closure claims it (compact.rs:806, `PurgeGuard::claim`) inside `manifest::begin`. | `CompactionWork::run` (compact.rs:933, `voided`) aborts early when voided. | As `FlushItem::guard`. | #316, #328, D192. |

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

`Shared::manifest` (Mutex<ManifestWriter>, shard.rs:335), `manifest_queue` (346), `manifest_busy`
(AtomicBool, 348), `manifest_flight` (432), `view_lock` / `view` (publish_view, shard.rs:573),
`pager_poisoned` (Release stores in manifest.rs:847/896/968), `closing` (Acquire loads in
maintenance.rs:96/399/632), `large_open` (AcqRel, large.rs), `large` (`LargeSettlement`'s three
Mutexes, large_settle.rs), `ts_floors` (Acquire, manifest.rs:795,
maintenance.rs:236), `sst_ids` / `blob_ids` (Relaxed), `async_sync_reads` (Relaxed,
nonblocking.rs), `commit_parks` (Relaxed, write.rs:520), `TAIL_SKIPS` (Relaxed, source.rs:129),
the close gate (`close_gate.rs`, one lock), test hooks
(`hooks.manifest_race`, `manifest_park`, `manifest_parked`, `manifest_race_waiter`,
`compactions`, `compaction_hold`, `compaction_held`, `omit_blob_refs`, `refuse_checkpoints`,
`flush_gc_mutation`, `record`).

### The manifest exclusion and pump (manifest.rs)

There is one manifest writer (`Shared::manifest: Mutex<ManifestWriter>`) and at most one
commit in flight. Requests (`ManifestReq`: edits or a catalog/tablet closure, readers, flushed
memtable roots, a `reply` closure) are pushed onto `Shared::manifest_queue`
(`ManifestQueue::reqs: Mutex<Vec<_>>`). The right to turn the queue into a commit is the
**exclusion** `Shared::manifest_busy: AtomicBool`, a try-lock, not a blocking lock:
`claim` = `!swap(true, AcqRel)` (manifest.rs:1012); `release` = `store(false, Release)`, then
`try_final_close()`, which decides under the close gate's lock (`close_gate`, #134 step 2: the
releaser stores the exclusion free before it takes that lock, so it or the reporter runs the
pending final close; #81, closing #78), then `manifest_flight.wake_threads()`
(manifest.rs:1018-1024). `begin`
and `end` must be called with the exclusion held; each takes the writer Mutex only for its
own critical section: `begin` takes the whole queue (queue lock, then released), locks the
writer, runs the request closures (including the D191/D192 guard CASes) and `prepare`, then
drops the writer before replying; `end` locks the writer only for `complete`, then publishes
the view under `Shared::view_lock`, retires extents, reclaims, answers every request, and
broadcasts `ShardMsg::Maintain`. **Shard/task path:** `submit` pushes and sends
`ShardMsg::PumpManifest`, which spawns a cooperative `ManifestPump` task on that shard
(shard.rs:7279). `ManifestPump::run` claims (if it cannot, it returns `Done`: "someone else is
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
(manifest.rs:1084-1140) push the request, then loop: if `claim` succeeds, `drain_sync` (`begin`
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
   (shard.rs:700) is taken with the exclusion held. The exclusion is also held across the
   root-commit I/O by the pump (no Mutex is).
2. `Shared::manifest` (manifest.rs:587-845, `begin`) → request closures, which CAS the
   `FlushItem::guard` / `CompactionWork::guard` (`PurgeGuard::claim`, flush.rs:660, compact.rs:806) and
   store `install_seqnos` (flush.rs:678); → `Shared::view` ArcSwap `load_full` (591); →
   pager internals via `pager.abandon` (`abandon_ssts` / `abandon_blobs`, 545-575) and
   `writer.prepare`; → block cache internals via `cache.erase_files` (abandon paths and
   manifest.rs:683). `ManifestQueue::reqs` is taken *before* (582) and released, never nested.
   `hooks.compactions` (test-hooks) is locked only after `drop(writer)` (822-829).
3. `Shared::view_lock` (inside `publish_view`, shard.rs:580, called from `manifest::end`
   manifest.rs:916) → `LiveViews::versions` (via `ViewPin::new` → `register`, manifest.rs:960 →
   snapshot.rs:734); also `OpenSst::reader` OnceLock set in `SstSet::rebuild` there. The writer
   Mutex is released (block at manifest.rs:884-891) before `publish_view`.
4. `LiveSeqnos::seqnos` (`SeqnoPin::pin_visible`, snapshot.rs:830) → `shm.visible_seqno()`
   atomic load inside the critical section (intentional, #315 review).
5. `LargeSettlement` logged → dropped → pending, all three held together in
   `LargeSettlement::settle` (large_settle.rs:90), from `LargeValues::settle_on`'s `on_resolve`
   callback (runs on the thread that resolves the commit's notifier). `note_logged` (77) takes
   pending and drops it before logged: no nesting there. The `release` reply closure
   (`remove_pending`) locks pending alone, from `manifest::end`/`begin` after the writer lock is
   dropped.
6. `Flight::state` → `hooks.manifest_park` atomic load (`take_done`'s `held` closure, `manifest::held_by_hook`, test-hooks only). Not
   nested with `Flight::threads`: `complete` drops `state` before `wake_threads`; `parked` drops
   `state` before `hooks.manifest_parked`.
7. `Flight::threads` → `thread_count` store (SeqCst) inside `register` / `wake_threads`; wakers
   are woken after the lock is dropped.
8. `LiveViews::versions` is released before the `on_oldest_released` callback
   (`Shared::try_reclaim`) runs (snapshot.rs:788-794); `oldest()` releases it before
   `oldest_live_manifest` locks `Shared::view_versions` (shard.rs:630-637).
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
| `Signal::target` signal.rs:30 | `Mutex<WakeTarget>` | How to wake the sleeping consumer: nothing, `Thread` (unpark), `ThreadRing` (unpark + `pigeonhole_io::OwnIoWaker::wake`, an eventfd/ring interrupt), or an application `Callback`. | `set_target`: engine-owned shard thread at `shard_main` start (lib.rs:681-685, `ThreadRing` if the backend gave it a ring, else `Thread`); pool thread at `pool_main` start (sched.rs:332-334, `Thread`); application thread in `ShardDriver::set_wakeup` and `ShardDriver::wait` (#553, ICR 0035: one composite `Callback`, the application's callback if any, then the waiting thread's ring waker and an unpark). | `wake_target` (signal.rs:118-139), called from `notify` on producers and from `force_wake` on the owner thread. | Lock is held only to clone the target out (signal.rs:125-130); unpark / ring wake / callback run after the guard drops. Poison ignored (`PoisonError::into_inner`). No other lock is held while taking it inside the runtime. | Mutex from #6 (bf1836a). `ThreadRing` variant added with per-thread rings, #402 (#415, ca1dad5). |
| `Signal::parks` signal.rs:32 | `AtomicU64` | Count of times an engine-owned shard thread parked idle (diagnostic). | Engine-owned shard thread: `Signal::parked` from `shard_main` lib.rs:715 (pool threads never call it). | `Signal::idle_counts` via `Submitter::idle_counts` lib.rs:311 (any thread: engine stats, bench). | `fetch_add` Relaxed; load Relaxed (doc says readings may lag). | ICR 0027 (#480, edda3e7). |
| `Signal::wakes` signal.rs:34 | `AtomicU64` | Count of `notify` calls that found the consumer asleep and woke it (diagnostic). | Producers in `notify` (signal.rs:65), only after winning the `sleeping` swap. | `idle_counts` (as above). | `fetch_add` Relaxed; load Relaxed. | ICR 0027 (#480, edda3e7). |
| `Signal::busy` signal.rs:44 | `AtomicU64` | Nanoseconds an engine-owned shard thread spent in work turns (D217: the balancer's measure of how busy a shard is; diagnostic until D217's packing reads it). | Engine-owned shard thread only: `Signal::add_busy` from `shard_main` lib.rs:696, after each `run_once` (pool threads and application-owned shards never call it). | `Signal::busy_nanos` via `Submitter::busy_nanos` lib.rs:319 (any thread: `Engine::shard_stats`, `Engine::metrics`, bench). | `fetch_add` Relaxed by its one writer; load Relaxed (a reading taken during a turn lags it). | ICR 0033, D217. |

### Shard inbox, `lib.rs`

| Name | Type | Protects / means | Writers | Readers | Ordering | Why / history |
|---|---|---|---|---|---|---|
| `Inbox::tx` lib.rs:258 (receiver: `ShardCore::rx` lib.rs:373) | `std::sync::mpsc::Sender<M>` / `Receiver<M>` (unbounded MPSC, lock-free in std) | The shard's foreground message queue. FIFO per sender. | Producers: `Submitter::submit` (lib.rs:325). | Shard thread only: `ShardCore::drain` (`try_recv`, lib.rs:492, up to `MAX_BATCH`), `check_before_sleep` (lib.rs:654, into `stash`), `spin_before_park` (lib.rs:522), `finish` via `drain`. Dropping the `ShardDriver` drops the receiver, which drops queued messages (their `Notifier`s resolve `None`). | Channel's own synchronisation. `submit` calls `signal.notify()` after a successful send (lib.rs:328). | #6 (bf1836a). Crate doc lib.rs:16-20 lists it as one of the only shared mutable state. |
| `SubmitGate::closed` submit_gate.rs:16 (`Inbox::gate`) | `AtomicBool` | The shard no longer accepts submits (shutdown started). | Owner thread: `Inbox::close` (lib.rs:266, from `Runtime::stop` and `Runtime::drop`); `CloseOnDrop::drop` (lib.rs:1032, when a `ShardDriver` is dropped or shut down); `ShardDriver::shutdown` (lib.rs:1174); all through `SubmitGate::close`. | Producers in `submit` (`SubmitGate::enter`, lib.rs:319); engine-owned shard thread via `is_closed` in `shard_main` and `spin_before_park`. | All stores and loads `SeqCst`. `Inbox::close` follows the store with `signal.force_wake()`; `CloseOnDrop` and `ShardDriver::shutdown` do not wake (the application drives the loop itself). | #6 (bf1836a); #134 step 3 |
| `SubmitGate::inflight` submit_gate.rs:18 (`Inbox::gate`) | `AtomicUsize` | Number of submits between their `closed` check and the end of their `send`; shutdown waits for zero so no message is sent after the final drain. | Producers in `submit`: `SubmitGate::enter` (`fetch_add(1)` before checking `closed`, `fetch_sub(1)` on refusal) and `leave` after `send` (lib.rs:319-323). | Shard thread in `ShardCore::finish` (`SubmitGate::wait_drained`, lib.rs:662), spinning with `thread::yield_now` until 0, then draining to empty. | `fetch_add` SeqCst; `fetch_sub` Release; load SeqCst in `wait_drained`. Pairs with `closed` (both SeqCst) as a store-buffering pattern: either shutdown sees the increment and waits for the send, or the submitter sees `closed` and refuses. | #6 (bf1836a); #134 step 3 |

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
- `ShardCore::finish` (`SubmitGate::wait_drained`): spin-waits on `inflight` with no lock held.

### Cross-thread protocols (loom candidates)

1. **Sleep/wake handshake (Dekker with SeqCst fences)**, `Signal::sleeping` + `fence(SeqCst)` on both sides + the work sources (`Inbox` mpsc channel, `Signal::task_woken`, `PoolShared` channel). Consumer: `prepare_sleep` (store `sleeping=true`; fence), then re-check `rx.try_recv()` and `task_woken()` (`check_before_sleep` lib.rs:651-661, `pool_main` sched.rs:358-367), then `thread::park` / `IdlePark::park` / ring wait. Producer: send or set flag; `notify` (fence; Relaxed load; `swap(false, AcqRel)`; `wake_target`). Lost unparks are prevented by `Thread::unpark`'s token semantics: an unpark before `park` makes the next `park` return at once. The `ThreadRing` target also calls the ring's `OwnIoWaker`, so a thread blocked in `reap_own_io(Some(until))` (lib.rs:709) wakes. The waker writes the ring's eventfd only while the owner waits in the ring (`pigeonhole-io` `RingWake`, ring_wake.rs): it stores `pending`, fences (SeqCst) and loads `waiting`; the owner stores `waiting`, fences and takes `pending`, waiting only if none was pending. A wake while the owner runs costs only the atomics, and its next ring wait returns at once (before the gate analysis, every wake wrote the eventfd, and the ring drained it and re-armed its poll: about three system calls per write on a busy shard). Modelled in io ring_wake.rs (`--cfg io_loom`). Variants: the D198 idle spin (`spin_before_park` lib.rs:511-548) marks the shard `awake` while polling so producers skip the wake syscall, then reruns the handshake on a miss; `ShardDriver::set_wakeup` installs a `Callback` and calls it once right away, because a `notify` that won the swap while the target was `None` woke nobody. `ShardDriver::wait` (#553, ICR 0035) is the application thread's version of the consumer side: `Signal::sleep_unless` announces sleep, re-checks the inbox and `task_woken`, then waits in the thread's ring (own I/O in flight) or parks; its target composes the application's callback with the waiting thread's ring waker and an unpark, and `ShardDriver::waker()` is `notify_task` (a work source the re-check covers). A submit that finds the shard asleep before the first `wait` installed the target wakes nobody, and that `wait`'s re-check finds the message. Modelled in signal.rs (`#[ignore]`d on loom 0.7.2, tokio-rs/loom#254; passes with #391; #533).
2. **Background-task wake**, `TaskCell::woken` (Release store) + `Signal::task_woken` (Release store) + protocol 1. The owner clears `task_woken` with an Acquire swap in `collect_woken` (sched.rs:233), then scans slots and Acquire-swaps each `woken`. `run_one` clears `woken` (Relaxed) before a slice and Acquire-swaps it after a `Blocked`/`SleepUntil` result (sched.rs:264-282), so a wake during the slice keeps the task runnable. `collect_woken` always clears `task_woken`, even with nothing blocked, so a stale wake cannot keep the owner awake. Modelled in sched.rs (`#[ignore]`d on loom 0.7.2; passes with #391; #533).
3. **Submit vs. shutdown**, `SubmitGate::inflight` + `SubmitGate::closed` (submit_gate.rs) (store-buffering, SeqCst on both sides). Submitter: `inflight += 1` (SeqCst); load `closed` (SeqCst); send; `inflight -= 1` (Release). Closer: store `closed` (SeqCst), `force_wake`; shard thread sees `is_closed`, then `finish` spins until `inflight == 0` (SeqCst load) and drains until empty. Guarantees no message is accepted (`Ok`) yet left unhandled by `shutdown`. Dropping a `ShardDriver` instead drops the receiver, so late sends fail with `Closed`. Modelled in submit_gate.rs but `#[ignore]`d: loom treats SeqCst accesses as AcqRel (a store-buffering control with SeqCst accesses fails under loom, the same with SeqCst fences passes).
4. **Pool shutdown ordering**, `PoolShared::closed` + `force_wake` + thread joins: `Runtime::stop` joins every shard thread before closing the pool (lib.rs:950-959), so no submit races the close. The panicking-`Drop` path closes both without joining (lib.rs:975-982); late tasks are dropped with the receiver.
5. **One-shot completion**, `Slot::state` (`Mutex`) + `Slot::cv` (`Condvar`) + `blocked` flag + `Waker`: resolve runs the hook outside the lock before publishing, publishes `value`/`done` under the lock, then signals the condvar only if a thread blocked and wakes any stored async waker. Slot reuse (`SlotCache`, `recycle_into`) relies on `Arc::strong_count == 1` meaning the notifier has dropped its `Arc`. **Already loom-modelled**: `loom_recycle_races_the_notifier_releasing_the_slot` (completion.rs:496) and `loom_a_waiter_dropped_on_another_thread_recycles_there` (completion.rs:511), run with `RUSTFLAGS="--cfg loom" cargo test --release -p pigeonhole-runtime --lib loom`. They cover reuse and, incidentally, the blocking `wait_ref` path; there is no dedicated model of the hook-before-value ordering or of poll/wake waker replacement.
6. **Thread start ack**: each spawned thread sends its pin result on a one-shot mpsc channel (lib.rs:801-806) that `Runtime::start` waits on (lib.rs:878-883).
7. **Shard handover between application threads** (#473, #492, ICR 0028/0029): no runtime primitive. A `ShardDriver` moves between threads by the application's own synchronisation; `thread_local TOKEN` / `static NEXT` let `run_turn` detect the new thread and attach a ring there, and `ShardDriver::release` drains the old thread's ring first (debug assertion at lib.rs:591-596).

18 primitives (16 struct fields, statics and thread-locals, with the 2 channel receiver ends folded into their sender rows; plus the `spawn_pinned` ack channel and the non-`Sync` `SlotCache::slots`, listed because cross-thread reuse safety depends on it). Without those last two: 16.
