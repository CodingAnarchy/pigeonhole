# Phase 1 edge-case review (#90)

Five read-only reviewers covered the nine #90 areas on main at `6756c2c`. Their raw findings follow, one section per reviewer. Finding IDs (for example `5-6 5.1`) are referenced from the issues below. The throwaway probe tests that reproduced the findings are not kept in the tree; they are in commit e49a287a78eb (`git fetch origin pull/152/head`, then `git show e49a287a78eb:docs/design/reviews/phase-1-probes/<file>`), and each fix PR turns its probe into a real regression test.

## Issues filed

| Issue | Phase | Scope |
|---|---|---|
| #135 | 1 | Blocking waits spin or deadlock; application-owned close is lost |
| #136 | 1 | Close and flush failures around 2PC, `drop_table` and `shrink` |
| #137 | 1 | A cold-slot write stops WAL checkpointing |
| #138 | 1 | `shrink` never reclaims space; stale catalog |
| #139 | 1 | Swallowed fsync errors; `NoSpace` in a manifest rewrite poisons |
| #140 | 1 | Reader snapshots across writer restart and mixed manifest versions |
| #141 | 1 | Stall, retry and backoff edges; `Busy` conflates retryable and never-fits |
| #142 | 1 | shm SIGBUS under a small `/dev/shm`; CPU pinning opt-out |
| #143 | 1 | Open writes 192 MiB WAL per shard; shard count defaults |
| #144 | 1 | Leaks: compaction outputs, test-hook list, aborted set |
| #145 | 1 | SimVfs deferred-completion mode |
| #146 | 1 | Documentation truthfulness |
| #147 | 2 | Network-filesystem detection |
| #148 | 2 | Resource and wait cleanups |
| #149 | 3 | Timer and wakeup costs |
| #150 | 4 | Cross-namespace readers; concurrent close leaks |


## Reviewer: areas-1-2

## Issue #90 edge-case review: areas 1 (busy-waits/polling) and 2 (simulation-only assumptions)

Reviewed: `main` at 6756c2c (read-only). Scope: runtime (`crates/runtime/src/{lib,sched}.rs`), engine shard loop and
timers (`crates/engine/src/shard.rs`, `shard/tablets.rs`), manifest pump/thread commits (`engine/src/manifest.rs`),
public commit waits (`engine/src/write.rs`, `engine.rs`), compaction/flush tasks, PreadVfs pool (`io/src/pread.rs`),
SimVfs (`io/src/sim.rs`), shm waits (`shm/src/{region,lock}.rs`), memtable arena reclaim.

Checked and found OK (no finding): PreadVfs worker pool (condvar, parks); `Completion::wait` (condvar);
`ShardCore::finish` in-flight yield (bounded by in-flight submits); shm `release_slot` (bounded, 64 yields + 200x1ms);
shm `ShmInit::acquire` (200x5ms, bounded); shm `attach` ready wait (100x1ms, bounded); `read_view` retry (bounded on
bad copies, retries only on pointer swaps); `ReaderSlot::pin` loop (converges); flush/compaction slice checks always make
progress before testing the deadline; IdlePark (`sched.rs:386`) caps at 10 ms until the clock is seen to keep real time,
then 1 s; ClockTimer now sleeps via `SleepUntil` on a moving clock (#89 fix is in place).

Severity key: **Blocker** = fix before the Phase 1 gate; **Should-fix** = Phase 1 issue unless triaged otherwise;
**Defer** = milestoned issue in a later phase.

---

### F1. Blocking commit waits spin on the global watermark (up to `write_stall_timeout`, 30 s) — Blocker

**Where**
- `crates/engine/src/write.rs:252-263` (`PendingCommit::wait`, used by `Engine::commit` at `engine.rs:1187`): after the
  shard replies, `while visible_seqno() < info.seqno { spin_loop x64; then yield_now }` with no park and no bound.
- `crates/engine/src/engine.rs:2213-2217` (conditional commit, `check_and_*` path): `while visible_seqno() < seqno { yield_now() }`.
- The async path (`write.rs:286-292`) already registers a waker in `Shared::waiters` and is woken by
  `wake_visible` (`shard.rs:384-404`); the sync path does not use it.

**Scenario.** The global watermark is the min over every shard's `watermark()` (`shard.rs:2523`): unresolved groups
(waiting on fsync) and `held` cross-shard seqnos (`shard.rs:3620`, released only in `on_applied` at `shard.rs:3953`
after *every* participant applied). Seqnos are global, so any lower seqno anywhere holds every later commit's visibility.
1. Shard B is in an arena-room stall (D124; waits up to `write_stall_timeout_nanos` = 30 s by default,
   `engine/src/options.rs:128`) and a cross-shard commit A+B is coordinated by A. A holds its seqno until B applies.
2. Every synchronous `commit()` on shard A, C, D... (any durability, including `None`) resolves on its shard and then
   spins in `yield_now` for as long as B stalls, up to 30 s. 64 client threads = 64 cores at 100%, all doing nothing.
   Even without stalls, every `None`/`Buffered` commit spins for the duration of any concurrent `Sync`/`GroupSync`
   fsync on another shard (ms on SSD, tens of ms on HDD/cloud disks).
3. Application-owned mode: a thread that drives shard B and calls `commit(..).wait()` for a commit routed to shard A
   (driven by another thread) livelocks at 100% CPU forever, because only it can advance B's group. The documented
   restriction (`write.rs:248`, D-note at decisions.md:314) covers only the commit's *own* shard.

Never seen in sim: SimVfs syncs complete inline (F6), so an `unresolved` group never outlives a message batch, and the
harness uses the `Future` path.

**Fix.** Make the blocking wait park: register in `Shared::waiters` with a thread-unparking `Waker`
(`std::thread::current()`-based), re-check, then `park()`; `wake_visible` already wakes registered waiters on each
publish. Same for `engine.rs:2215`. Keep a short spin (<= 64 iterations) before registering. Document that
application-owned threads must not block on *any* commit wait while driving a shard.

---

### F2. Thread-side manifest commits (create/drop table, shrink) poll the exclusion and hang forever in application-owned mode — Blocker (app-owned); Should-fix (engine-owned CPU)

**Where**
- `crates/engine/src/manifest.rs:834-868` (`commit_req_from_thread`): if `claim` fails, polls its waiter with a noop
  waker, `yield_now` x64, then `sleep(50 µs)` in an unbounded loop.
- Callers: `Engine::catalog_change` (`engine.rs:1917`, used by `create_table`/`drop_table`/family changes),
  `maintenance.rs:286` (shrink/relocation path).
- Exclusion holder: `ManifestPump` (`manifest.rs:973-1005`) claims, `start`s an async root commit
  (`pager.submit_commit_root`, `manifest.rs:933`) and returns `TaskPoll::Blocked` **while holding the exclusion**. Only
  a later `run_once` of the pump's shard runs `end` + `release`.

**Scenario.** Application-owned mode on PreadVfs, one shard per app thread. A flush completes; its pump on shard 0 is
Blocked with the root commit in flight on the I/O pool. The app's event-loop callback on the thread that drives shard 0
calls `create_table`. `claim` fails; the thread polls every 50 µs forever, since the pump can only finish when this same
thread calls `run_once`. Hard hang (not an error). In engine-owned mode the loop terminates but wakes ~20k/s for the
duration of the other commit's two fsyncs.

Never seen in sim: SimVfs `submit_*` return `Completion::ready` (`io/src/sim.rs:696-710`), so the pump's completion
fires before it returns `Blocked` and it finishes in the same slice; the exclusion is never held across `run_once`.

**Fix.** (a) Wait on the waiter properly (block on its condvar/thread waker) instead of polling; the release path already
re-checks the queue so a request pushed while held is drained by the holder. (b) For application-owned mode, either let
the calling thread run its own shard's pending pump inline when it is the driver (detect via `CURRENT_SHARD`), or
refuse with a documented error (`WouldBlock`/`Busy`) and document "DDL must not be called from a thread that drives a
shard". Add a deterministic test with a deferred-completion VFS (see F6).

---

### F3. Room and starved-freeze waits are not woken when room is freed by a snapshot drop or reader unpin: the wait lasts until the 30 s timeout — Should-fix

**Where**
- `crates/memtable/src/lib.rs:361-380`: retired memtables are freed only when the shard calls `reclaim`/
  `release_unreferenced`, which checks `Arc::strong_count(pin) == 1`; dropping the last in-process handle notifies nobody.
- Reader-process unpins (`ReaderSlot::unpin`, `shm/src/region.rs:917`) notify nobody either; `reclaim_retired`
  (`shard.rs:2471`) only runs on shard events.
- Room wait (`shard.rs:3070-3150`): the only timer is armed for `since + write_stall_timeout_nanos`; nothing re-checks
  in between unless another event (new commit, flush completion, Maintain) arrives.
- Same for the starved full-freeze wait (`retry_starved_freeze`, `shard.rs:2252-2335`).

**Scenario (real clock).** Multi-threaded app: thread T1 holds a long scan snapshot pinning the frozen memtables; the
arena is full; thread T2's commit enters a room wait. T1 finishes and drops its snapshot at t=50 ms. No flush is
running and no other writes arrive, so the shard is never kicked: T2 stays blocked until the 30 s timer fires (the Kick
then re-runs `reserve_room` and admits it). Same with a reader process that unpins at t=10 ms. `flush()`/`compact()`
callers in a starved freeze behave the same way.

Sim divergence: on a frozen clock the "idle case" (`shard.rs:3133`, `2311`) refuses at once with `Busy`, and the model
harness drops snapshots and retries, so the real-clock 30 s hang was never exercised. D126's justification ("in-process
snapshots, which only the blocked caller can drop") does not hold for multi-threaded callers.

**Fix.** Kick the owning shard when the last handle to a deferred memtable drops (the `Pin` `Drop` can `submit(Kick)`
through a weak submitter, or bump a shared "room freed" counter the shard waits on). For reader-process pins, arm a
bounded re-check timer during a room/starve wait (e.g. 1 ms doubling to 100 ms) rather than relying on the 30 s
deadline. Test on a moving SimVfs clock: hold a snapshot from another thread, drop it, assert the commit completes well
before the timeout.

---

### F4. A failing flush during a room wait is retried back-to-back for 30 s on a moving clock — Should-fix

**Where** `crates/engine/src/shard.rs:2389-2420` (flush `Err` arm): `requeue_frozen`, then if `wait_room`, submit
`Kick` immediately; the Kick runs `run_group` -> `need_room` -> `freeze` + `spawn_flush` (`shard.rs:3123-3125`) ->
fails -> Kick... The `ROOM_FLUSH_ATTEMPTS = 4` cap (`shard.rs:3099`) applies only on a frozen clock; on a moving clock
the loop ends only at `write_stall_timeout_nanos` (D126 "a failed flush is retried on the next run of the waiting
group"). The comment at `shard.rs:2390` ("never in a tight loop") is false in this case.

**Scenario.** Disk full (ENOSPC that does not poison the pager) or a persistent per-file write error while writers are
stalled for room. For 30 s the shard rebuilds the same SST from the frozen memtable(s) back-to-back (each attempt
iterates and encodes the whole memtable, allocates pages, fails, and returns them), burning a core and the I/O path, and
foreground messages only interleave between slices. With several waiting groups arriving it repeats per stall.

Sim divergence: the sim clock is frozen, so the 4-attempt cap always applied; the moving-clock retry storm was never run.

**Fix.** Back off failed flushes with a `ClockTimer` (same shape as `compaction_backoff_nanos`: e.g. 10 ms doubling to
1 s, reset on success), and/or keep `ROOM_FLUSH_ATTEMPTS` as a cap on both clocks (refuse with the flush error after N
consecutive failures). Update D126 and the comment.

---

### F5. Compaction backoff is cancelled by every admitted group, so under write load a persistently failing compaction is retried continuously — Should-fix

**Where** `crates/engine/src/shard.rs:2938` (`run_group`: `self.compaction_backoff = false` for every admitted group),
with `maintain` re-triggered by the next flush completion (`2437`) or by `stalled()` (`2844`) when the L0 score is >= 1.
`back_off_compaction` (`4393-4410`) grows the timer to 60 s, but the flag it guards is cleared by the next group.

**Scenario.** A compaction input SST has a corrupt block (checksum failure) or the device returns ENOSPC on output
allocation. Under steady writes, each admitted group clears the backoff; the next flush or stall check restarts the same
compaction, which re-reads inputs up to the failure point and fails again. The 1-60 s exponential backoff only takes
effect when the database is idle, which is when it matters least. With L0 >= 1 this also churns the stall logic (every
failure sets `hopeless`, admitting writers unpaced, then the next group restarts the doomed compaction).

This is D119/D126 as written ("a group is admitted" ends the backoff), so it is a decision with an unintended consequence
on a real clock: the event-tied retries were added for the frozen clock, where the timer never fires.

**Fix.** On a moving clock, let only the backoff timer (and a successful flush that changed the inputs, if desired) end
the backoff; keep the "group admitted" trigger only while the clock is frozen (`TimerState::frozen`). Alternatively
rate-limit event-tied retries to at most one per backoff interval. Amend D126.

---

### F6. SimVfs completes every async I/O inline, so no simulated run ever has I/O in flight across a scheduling point — Should-fix (test infrastructure; gates F1/F2 reproduction)

**Where** `crates/io/src/sim.rs:696-710`: `submit_read`/`submit_write`/`submit_sync_data` return `Completion::ready`.
Consequences in production code paths that the simulator therefore never exercises:
- `ManifestPump` holding the exclusion while `Blocked` (`manifest.rs:973-990`) -> F2.
- `unresolved` WAL groups outliving a batch (`shard.rs:3379-3395`), i.e. the global watermark held across other
  shards' groups -> F1; the room-wait idle test `self.unresolved.is_empty()` (`shard.rs:3139`) is always true in sim.
- Flush/compaction `Stage::Commit` Blocked across foreground messages (close, drop_table, tablet change) —
  `flush.rs:449`, `compact.rs:505`.
- `PendingCommit` / `SyncDone` arriving after close or after a driver is dropped (`ManifestPump::drop`, `manifest.rs:948`,
  is only reachable through this window).

Combined with the frozen clock, background tasks also never yield mid-slice in sim (`flush.rs:300-304`,
`compaction/src/job.rs:467-473` check a deadline that a frozen clock never reaches), so "foreground message handled
between two slices of a flush/compaction" interleavings come only from the handful of PreadVfs tests
(`engine/tests/api.rs:798`, `pigeonhole/tests/api.rs:1014`), which do not stress them.

**Fix.** Add a seeded "deferred completion" mode to SimVfs: queue completions and resolve them from the sim scheduler
(or on the next `advance`/explicit `drain_io`) in seed-chosen order, plus an option to give tasks a tiny slice
(e.g. treat every `CLOCK_EVERY` check as expired with seeded probability). Run the model suites in that mode before the gate.

---

### F7. Frozen-clock detection is a poll-count heuristic: a coarse (but moving) clock is classified as frozen and gets the simulator fallbacks — Should-fix (document) / Defer (redesign)

**Where** `crates/engine/src/shard.rs:59-61` (`STALL_TIMER_FROZEN_POLLS = 1024`), `ClockTimer::run`
(`shard.rs:1351-1388`), and every `frozen(now)` consumer: L0 stall `hopeless` (`2848`), room wait refusal after 4 failed
flushes and idle refusal (`3076-3140`), starved freeze (`2298-2316`), tablet/balance timers not re-armed
(`tablets.rs:494-498`, `1101-1104`). Runtime: `idle_at` wake-sleepers (`runtime/src/lib.rs:416-419`,
`sched.rs:348-351`) uses exact equality of two clock readings.

**Scenario.** `Vfs` is a public trait (`io/src/vfs.rs:32`) whose `monotonic_nanos` doc states no resolution
requirement. A custom Vfs backed by a 1-4 ms clock (e.g. `CLOCK_MONOTONIC_COARSE`, jiffies clocksource on some VMs, or a
cached clock in an app-owned runtime) runs 1024 timer polls (~100 µs) within one tick, so the timer "gives up" and the
shard treats the clock as frozen for the rest of the tick: an L0 stall with no compaction running admits writers
unpaced; a room wait in the idle case is refused with `Busy` immediately instead of waiting; flush failures cap at 4.
This is the #84 class of bug, reachable on a real clock. (PreadVfs uses `Instant`: ns on Linux, ~42 ns on Apple Silicon,
100 ns QPC on Windows — fine today.)

**Fix.** Short term: document on `Vfs::monotonic_nanos` that it must advance at least every ~10 µs of real time (or
that a non-advancing clock means "simulated"). Better: replace the heuristic with an explicit capability, e.g.
`Vfs::clock_is_simulated() -> bool` (SimVfs true), or detect "frozen" against real elapsed time (`std::time::Instant`,
as `IdlePark` already does) rather than poll count. Amend D126.

---

### F8. With `tablet_changes` on, every shard wakes every 100 ms forever, even when idle — Defer (tablets phase) / Should-fix if tablets ship on by default

**Where** `crates/engine/src/shard/tablets.rs:1063` (every `maybe_balance` re-arms) and `1094-1115`
(`arm_balance_timer`), default `balance_interval_nanos = 100_000_000` (`engine/src/options.rs:122`).

**Scenario.** An embedded app with 4 shards and tablet changes on, idle for hours: 40 wakeups/s of pure polling
(battery, cloud CPU credits), each recomputing EWMAs and possibly the balancer decision. #103 added the timer so an idle
shard still merges cold tablets, but it never stops after the loads have decayed and no change is possible.

**Fix.** Stop re-arming once a pass saw zero writes and `decide` returned nothing (and EWMAs are below a merge-relevant
epsilon); re-arm on the next admitted group. Or back the interval off exponentially while idle (100 ms -> 10 s).

---

### F9. L0-stall pacing assumes sub-millisecond timed parks; on Windows (and with macOS timer coalescing) pacing collapses — Defer (Windows/platform phase)

**Where** `crates/engine/src/shard.rs:2884-2902`: the stall timer sleeps `(1 - tokens) / rate`, i.e. ~250 µs * score at
`STALL_RATE = 4000` (`shard.rs:57-58`), via `IdlePark::park` -> `thread::park_timeout` (`runtime/src/sched.rs:393`).
Bucket capacity is `STALL_CAPACITY = 8`.

**Scenario.** On Windows the default timer resolution is ~15.6 ms, so each 250 µs park sleeps 15.6 ms; on wake the
bucket refills only to 8 tokens, so a stalled shard admits at most ~8 groups per 15.6 ms (~510 groups/s) instead of
4000/score groups/s: an ~8x harder stall than designed, with p99 commit latency jumping by 15 ms steps. Sim never shows
this (clock is driven explicitly).

**Fix.** Size the bucket capacity to the platform's measured timer granularity (capacity >= rate * granularity), or
compute admissions from elapsed time with a larger cap; optionally `timeBeginPeriod(1)` on Windows while a stall timer
is armed. Note it in the platform section of the review.

---

### F10. `test-hooks` builds use a different `flush()`/`compact()` wait implementation than production — Defer (low)

**Where** `crates/engine/src/engine.rs:250-310` (test-hooks `PendingMaintenance` is a `Future`; `poll` returns on the
**first** failed reply, dropping the remaining waiters and not starting another round) vs `engine.rs:1824-1850`
(production blocking `wait` waits for **every** shard's reply and returns the last error). The engine's dev-dependency
on itself with `test-hooks` (`crates/engine/Cargo.toml:33`) plus workspace feature unification means `cargo test
--workspace` builds even the `pigeonhole` crate's tests against the test-hooks engine.

**Scenario.** The model harness observes "flush returned error while other shards still flushing/compacting", which
production never produces, and never observes production's "block until every shard (possibly in a 30 s starved
wait) answered". A regression in the production wait path (e.g. a shard that never replies after a sibling failed)
cannot be caught by the sim suites.

**Fix.** Make the future's semantics match production (drain all waiters, then report), or have production's blocking
`wait` be `block_on(future)` so there is one implementation. Run one CI job of the `pigeonhole` tests per-package
(without `test-hooks`).

---

### Summary table

| # | Area | Location | Severity |
|---|------|----------|----------|
| F1 | busy-wait | write.rs:252-263, engine.rs:2213-2217 | Blocker |
| F2 | busy-wait + sim-only | manifest.rs:834-868, 973-990 | Blocker (app-owned) |
| F3 | missing wakeup / sim-only | memtable lib.rs:361-380; shard.rs:3070-3150, 2252-2335 | Should-fix |
| F4 | retry loop / sim-only | shard.rs:2389-2420, 3099 | Should-fix |
| F5 | retry loop / frozen-clock design | shard.rs:2938, 4393-4410 | Should-fix |
| F6 | sim-only (infra) | io/src/sim.rs:696-710 | Should-fix |
| F7 | sim heuristic on real clock | shard.rs:59-61, 1351-1388 | Should-fix (doc) / Defer |
| F8 | polling timer | tablets.rs:1063, 1094-1115; options.rs:122 | Defer / Should-fix |
| F9 | timer resolution | shard.rs:2884-2902; sched.rs:393 | Defer |
| F10 | test-hooks divergence | engine.rs:250-310 vs 1824-1850 | Defer |

## Reviewer: areas-3-4

## Issue #90 edge-case review: areas 3 (embedding modes) and 4 (platform differences)

Reviewed `main` at 6756c2c (read-only). I ran the experiments in a `git archive` copy at
a scratch copy (probe: `docs/design/reviews/phase-1-probes/zz_review_appowned.rs` at commit e49a287a78eb) on macOS (APFS, Rust 1.98.1).

Severity key: **B** = blocker for the Phase 1 gate, **S** = should-fix in Phase 1, **D(n)** = defer to Phase n.

| # | Area | Finding | Sev |
|---|---|---|---|
| 3.1 | embed | App-owned close: the documented loop drops the shards before close finishes. On a real filesystem the close is unclean every time, and close errors are lost | **B** (reproduced) |
| 4.1 | platform | A `/dev/shm` region is sized with sparse `ftruncate`. A small tmpfs (Docker's 64 MB default) raises SIGBUS mid-commit instead of `ShmUnavailable` at open | **S** (arguably B) |
| 3.2 | embed/platform | Engine-owned pinning cannot be turned off. It pins to indexes of the *calling thread's* affinity set, so two databases, two processes, or a pinned opener stack every shard on the same CPUs | **S** |
| 3.3 | embed | `PendingCommit::wait` deadlocks when the waiting thread drives *any* shard with an in-flight group, not just the commit's own shard. The visibility wait is also a `yield_now` spin | **S** |
| 3.4 | embed | A panicking engine-owned shard thread makes `Engine::close`, and `Drop for Engine`, hang forever | **S** |
| 3.5 | embed/docs | "Application-owned mode starts no threads" (D40, spec, rustdoc) is false: the default `PreadVfs` starts 2–16 I/O threads | **S** (docs); D3 (real fix) |
| 4.2 | platform | macOS/BSD: any in-process `std::fs` open and close of the `.phdb` silently drops the writer and presence locks. This is undocumented | **S** (docs); D4 (mitigation) |
| 4.3 | platform | Network-filesystem detection misses FUSE and GPFS. NFS without lockd reports `Io("lock")` instead of `NetworkFilesystem` because the lock is taken first | D2 (the reorder is trivial: do it now) |
| 4.4 | platform | Windows timer granularity (about 15.6 ms) quantizes the 250 µs write-stall pacing. A single committer stalls at about 8× the designed throttle | D3 |
| 4.5 | platform | Reader liveness uses raw PIDs. Across PID namespaces a live reader's slot is reclaimed, so extents are reclaimed under it | D4 (document now) |
| 4.6 | platform | Windows `Local\` mapping namespace: a reader in another session (service vs desktop) cannot attach | D4 |
| 4.7 | platform | "Last one out" race: two processes closing together both fail the presence upgrade, so the sidecars and the `/dev/shm` region leak | D4 |

Things I checked and found correct are listed at the end.

---

### 3.1 App-owned close: the documented loop yields an unclean close, and errors are swallowed — BLOCKER

**Evidence**
- `crates/runtime/src/lib.rs:434-441`: `run_once` returns `false` when the only tasks left are `Blocked`. A blocked task is not counted as remaining work.
- `crates/engine/src/shard.rs:4571-4710` (`try_finish_close`): a shard reaches `CloseStage::Reported` only after its `FlushTask`s, its checkpoint (`checkpoint_inflight`) and the manifest pump finish. All of them block on `pread`-pool completions (`submit_commit_root`, `submit_sync`).
- `crates/engine/src/engine.rs:2642-2648`: `Drop for EngineShard` calls `abandon()` when the shard has not reached `Reported`. That sets `close.failed`, so the close is unclean and the WAL files stay.
- `crates/engine/src/engine.rs:2307-2313`: in app-owned mode `close()` drops `rx` and returns `Ok(())`. The final close's result (`try_final_close` → `done.notify(result)`, `shard.rs:567-578`) is discarded.
- The documented contract, in `crates/pigeonhole/src/db.rs:130-131` ("keep driving each shard until `run_once` returns `false`, then drop it"), `engine.rs:1370-1373` and `engine.rs:422-424`, and the rustdoc example at `db.rs:133-178`, all stop driving at the first `false`.

**Repro** (scratch `crates/engine/tests/zz_review_appowned.rs`): real `PreadVfs`, 2 app-owned shards driven by threads exactly as in the rustdoc example, 2000 `Buffered` commits, `db.close()`, then each thread runs `while run_once {}` and drops its shard. The result is `final_close_pending=false clean=false files=["d.phdb-wal-0","d.phdb-wal-1","d.phdb"]`. When I keep driving for 500 ms after close instead, I get `clean=true files=["d.phdb"]` in 5 of 5 runs. Every SimVfs test passes because sim completions resolve in time.

**Failure scenario:** every application that follows the guide gets an unclean close on every shutdown. The next open replays the WAL, and "one file at rest" never holds. If the final flush or sync fails (ENOSPC, EIO), the application is told `Ok(())`. Buffered commits it believed a clean close had persisted may be lost, with no error anywhere. The application also has no public way to learn when close has finished: `final_close_pending` is only a test hook.

**Fix:** make `run_once` return `true` while the shard is closing and has not reached `Reported`. Equivalently, count `Blocked` tasks as remaining work while closing; the wakeup still fires on completion, so the loop parks rather than spins. Also give app-owned mode a way to get the close result. Options: `close()` returns a `PendingClose` future/waiter, `Shard::is_closed()`, or report the result from the last shard's `run_once`. Add a real-filesystem test of the documented loop, because SimVfs hides this.

### 4.1 `/dev/shm` region is sparse: SIGBUS instead of `ShmUnavailable` — SHOULD-FIX (arguably blocker)

**Evidence**
- `crates/io/src/os/unix.rs:244-258` (`open_region_file`, also used for `/dev/shm` at `:268-277`) sizes the region with `file.set_len(len)`. The non-Linux `shm_open` path at `:303-307` uses `ftruncate`. Neither reserves pages on tmpfs.
- The spec (`docs/design/spec.md:284`) says: "Opening fails up front if the region can't be allocated." `docs/guide/errors.md:35` maps a too-small `/dev/shm` to `ShmUnavailable`.
- Region size is `shards × memtable_budget` (64 MiB default) plus overhead, and shards default to the CPU count (`engine/src/options.rs:104-110`).

**Failure scenario:** Docker and Kubernetes default `/dev/shm` to 64 MB. On a 4-CPU container the region is about 256 MiB, and `ftruncate` succeeds. Once memtables have touched about 64 MB of arena pages, the next store into a new page gets SIGBUS and the process dies mid-commit, with no error code. The same happens on any host whose tmpfs fills up from other users. The `shm_dir` override has the same problem.

**Fix:** on Linux, `fallocate(fd, 0, 0, len)` (or `posix_fallocate`) the region file at `CreateNew`, and map ENOSPC to `ShmUnavailable`. This reserves tmpfs pages up front and so commits RAM; document that. A cheaper partial fix is an `fstatvfs` free-space check against `region_len` at open. Also map ENOENT on a missing `/dev/shm` (Lambda, distroless) to `ShmUnavailable` with a hint to set `shm_dir`.

### 3.2 Thread pinning is always on and pins to the wrong set — SHOULD-FIX

**Evidence**
- `engine/src/options.rs:110` sets `pin_threads: true`, and `pigeonhole::Options` has no setter. `grep pin crates/pigeonhole/src/options.rs` finds only the compaction_cores doc.
- `runtime/src/lib.rs:551-557`: `pin(cpu % available_cpus())`. `io/src/os/unix.rs:462-494`: `pin_current_thread` indexes `allowed_cpus()`, which is `sched_getaffinity(0)` of the newly spawned thread. That thread inherits the affinity of the thread that called `open`.

**Failure scenarios:**
1. Two engine-owned databases in one process, such as a common multi-tenant embed: both pin shard *i* to CPU *i*, so every shard 0 shares CPU 0.
2. Several containers on one host with a CPU *quota* rather than a cpuset: `available_parallelism` is 2 but the affinity set is all CPUs, so every container pins its shards to CPUs 0 and 1.
3. A thread-per-core application that pinned its main thread to CPU 3 and then opens with `shards(8)`: `allowed_cpus()` is `[3]`, so all 8 shards (and the inherited I/O pool) run on CPU 3.

**Fix:** expose `Options::pin_threads(bool)` and make the default `false` for the embedded library, or pin only when shards == CPUs in the cpuset and no other instance pinned. Compute the CPU list once from the *process* affinity (`sched_getaffinity(getpid())`) or from the cgroup cpuset, not from the inherited thread mask.

### 3.3 `PendingCommit::wait` and the visibility wait: deadlock on any driven shard, and a spin otherwise — SHOULD-FIX

**Evidence**
- `engine/src/write.rs:251-264` waits for `visible_seqno() >= info.seqno` in a `spin_loop`/`yield_now` loop. `engine.rs:2214-2217` (`check_and_mutate`) does the same.
- `shm/src/region.rs:110-117`: `visible_seqno` is the minimum over **every** shard's pending watermark. `shard.rs:2523-2531`: a shard's watermark is held by its `unresolved` groups, which are groups whose WAL sync is still in flight.
- D88 and the rustdoc (`write.rs:248-250`) warn only against waiting on the thread that drives *the commit's* shard.

**Failure scenario (app-owned):** thread X drives shard B and has a `GroupSync` group in flight on B. X then calls `table.mutate(..).commit()` for a row owned by shard A, which thread Y drives. Shard A resolves the commit, but visibility needs shard B to publish its watermark, and only X can run B. X yields forever at 100% CPU: a deadlock that the documented rule does not warn about.

In engine-owned mode the same loop busy-yields for the whole duration of another shard's fsync. On macOS, where sync is `F_FULLFSYNC`, that is often 5–20 ms; on slow disks it is longer. That part is area 1.

**Fix:**
- In `wait()`, if `Runtime::current_shard().is_some()` in app-owned mode, return an error (`InvalidArgument("wait on a shard-driving thread")`) instead of deadlocking.
- Replace the spin with the existing `VisibilityWaiters` (`shard.rs:178`) and a thread waker, as the `Future` impl already does.
- Fix D88 and the rustdoc to say "any shard".

### 3.4 A shard thread panic hangs `close` and `Drop` forever (engine-owned) — SHOULD-FIX

**Evidence**
- `runtime/src/lib.rs:472-494`: `shard_main` has no unwind guard.
- `engine.rs:2296-2314`: `close(true)` blocks on `rx.wait()` for `close.done`, a `Notifier` stored in `Shared` (`shard.rs:570-577`). That notifier resolves only when `close.remaining` reaches 0 (`shard.rs:544-548`).
- A panicked shard never calls `report_closed`, and its `Close` submit fails silently (`let _ = s.submit(...)`).
- `Runtime::shutdown`, which would re-raise the panic, runs only *after* `rx.wait()`.
- `Drop for Engine` (`engine.rs:1690-1697`) calls `close(true)` in engine-owned mode.

**Failure scenario:** any shard-side panic, such as an `expect` on an invariant (`shard.rs` has many), turns into a process that cannot shut down. `close()` hangs, and dropping the last handle at the end of `main` hangs. Cross-shard commits waiting on the dead participant also hang. App-owned mode differs: the panic unwinds through the application's `run_once` and `EngineShard::drop` → `abandon` reports the shard closed. So the two modes behave differently.

**Fix:** a drop guard in `ShardState`, or `catch_unwind` in `shard_main`, that on panic marks the close failed and calls `report_closed`. Also make `close` wait with a liveness check: join a finished thread and propagate its panic as `Error::Internal`/`Io`.

### 3.5 "Application-owned mode starts no threads" is false — SHOULD-FIX (docs), D3 (real fix)

**Evidence**
- `pigeonhole/src/options.rs:455-457`: `default_vfs()` is `PreadVfs::new(0)`. `io/src/pread.rs:66-81` spawns `available_cpus().clamp(2, 16)` worker threads, which run every `submit_*` (WAL sync, root commit, reads).
- The claim appears at `spec.md:246` ("The engine starts no threads of its own in this mode"), D40, `db.rs:118`, `options.rs:89-91`, `agent-reference.md:23,59` and `errors.md:49`.

**Failure scenario:** a glommio or monoio application sizes its cores on the promise and finds up to 16 extra unpinned threads doing fsync and reads. They inherit the opener's affinity (see 3.2) and are invisible to its scheduler.

**Fix:** correct the docs now ("starts no shard or compaction threads; the default I/O backend runs a small pool of I/O threads; pass `Options::vfs` with a 1-thread pool..."). Making app-owned mode truly threadless (`io_uring` completions polled from `run_once`) is the Phase 3 backend work. Consider letting `Options` size the pool, because `vfs` is `#[doc(hidden)]`.

### 4.2 macOS/BSD process-wide `fcntl` locks: an external fd close drops the writer lock — SHOULD-FIX (docs)

**Evidence:** `io/src/os/unix.rs:155-163` uses `F_SETLK` on non-Linux systems. The registry (`pread.rs:285-447`) protects only handles opened *through `PreadVfs`*. The guide never mentions macOS or Windows (`grep -i macos docs/guide` finds nothing).

**Failure scenario:** on macOS the application, a library or a test harness in the same process opens the `.phdb` with `std::fs::File` (to hash it, `fs::copy` it for an ad-hoc backup, a file watcher) and closes it. The kernel drops every lock the process holds on that inode: writer, presence and shm-init. A second process can now open as writer, giving two writers on one file, which is corruption. This is SQLite's well-known POSIX-lock hazard.

**Fix:** document it in `concepts.md`/`durability.md` and the `open` rustdoc: "never open the database file outside Pigeonhole in a process that has it open (macOS/BSD)". A later mitigation is to lock a separate `*.phdb-lock` sidecar with `flock` (per open file description on macOS and BSD) for the writer byte, or to use `F_OFD_SETLK` where the OS offers it (FreeBSD 13+ does).

### 4.3 Network-filesystem detection gaps and check order — DEFER (Phase 2); reorder now

**Evidence**
- `io/src/os/unix.rs:35-60` omits FUSE (`0x65735546`: sshfs, s3fs, gcsfuse, JuiceFS, rclone) and GPFS (`0x47504653`).
- `engine.rs:483-486` takes `WriterLock::acquire` *before* `is_local()`.

**Failure scenarios:**
- (a) A database on a gcsfuse or sshfs mount opened from two hosts: OFD locks on FUSE without lock support are local-only, so both hosts become writers. The same applies to the shared mmap of `shm_dir` on such a mount.
- (b) On NFS mounted `nolock`, or with lockd down, `F_OFD_SETLK` fails with ENOLCK, which `set_lock` (`unix.rs:177-184`) maps to a generic `Io("lock")` error. The user sees "I/O error" instead of the documented `NetworkFilesystem`.

**Fix:** call `is_local()` before taking the writer lock; it is a read-only `fstatfs` and D37 does not constrain its position. Treat FUSE and GPFS as non-local, with an explicit opt-in for local FUSE (ntfs-3g) later.

### 4.4 Windows timer granularity distorts write-stall pacing — DEFER (Phase 3)

**Evidence**
- `shard.rs:57-58,2866-2896`: the token bucket refills 4000 groups/s at score 1, so the timer wait is about 250 µs, with a 1 µs floor.
- `runtime/src/sched.rs:388-393`: the engine-owned idle park is `thread::park_timeout(want)`. On Windows that is `WaitOnAddress` with a millisecond timeout, quantized to the roughly 15.6 ms system tick.

**Failure scenario:** one synchronous committer stalled at L0 score 1 on Windows wakes about every 15.6 ms. The refill is capped at `STALL_CAPACITY = 8`, so throughput is about 8 groups per tick (about 510/s) instead of 4000/s, roughly 8× the intended throttle. Linux and macOS are unaffected. With many concurrent committers, arriving messages re-check the bucket and the effect shrinks.

**Fix:** raise the bucket capacity relative to the timer resolution (capacity ≥ rate × 16 ms on Windows), or use a high-resolution waitable timer (`CREATE_WAITABLE_TIMER_HIGH_RESOLUTION`) for idle parks with sub-millisecond deadlines.

### 4.5 Reader liveness across PID namespaces — DEFER (Phase 4); document now

**Evidence**
- `io/src/os/unix.rs:396-411`: liveness is `/proc/<pid>/stat` in the *checker's* PID namespace.
- `shm/src/region.rs:~855-875`: `reclaim_dead_slots` reclaims slots whose owner the writer believes is dead.

**Failure scenario:** a writer and a reader in different containers that share the volume and the IPC namespace (`--ipc=shareable`) but not the PID namespace. The writer sees the reader's PID as absent, or as another process with a different start time, and reclaims the live reader's slot. The pins are cleared, so the writer reclaims memtable chunks and SST extents the reader is reading, and the reader returns garbage or `Corruption`.

**Fix:** document "readers must share the writer's PID namespace". Later, add a per-slot heartbeat or a per-slot `F_OFD` lock byte as the liveness signal instead of the PID.

### 4.6 Windows `Local\` named mappings: cross-session readers cannot attach — DEFER (Phase 4)

**Evidence:** `io/src/os/windows.rs:294-299` creates names as `Local\<name>`.

**Failure scenario:** a writer running as a service in session 0 and a reader in a desktop session (or the reverse) look in different namespaces. The reader gets NotFound from `OpenFileMappingW`, even though the writer is running.

**Fix:** document it, and recommend `shm_dir` for cross-session use (file-backed mappings are namespace-free). Optionally try `Global\` when the writer has `SeCreateGlobalPrivilege`.

### 4.7 Simultaneous last-close race leaks sidecars and the shm region — DEFER (Phase 4)

**Evidence:** `shm/src/lock.rs:44-50` (`try_become_last` upgrades shared to exclusive). It is called by the writer (`shard.rs:494`) and by readers (`engine.rs:2343-2346`).

**Failure scenario:** the last reader and the writer close at the same moment. Each tries to upgrade while the other still holds shared, and both get `Locked` (on Windows, the unlock-and-relock window makes this even likelier). Neither cleans up. The WAL files remain, which is harmless because they are replayed. The current-generation `/dev/shm` region also remains, with its touched memtable pages resident in RAM until the next writer open (D47) or a reboot.

**Fix:** after dropping presence, retry once: take presence exclusive from no lock, then clean up. Alternatively, have the writer's next open, or a reader's open, remove orphaned regions, which D47 already does for older generations.

---

### Checked and fine (no finding)
- **macOS durability:** Rust std `sync_data` and `sync_all` both issue `F_FULLFSYNC` (std `sys/fs/unix.rs:1413-1433`). Directory `sync_all` (`F_FULLFSYNC` on a directory fd) succeeds on APFS (tested).
- **D55 length durability:** the WAL and pager `sync_all` after growth. On Windows `FlushFileBuffers` also covers metadata.
- **Windows `sync_dir`:** opens the directory with `FILE_FLAG_BACKUP_SEMANTICS` and tolerates `ERROR_INVALID_FUNCTION` (FAT); CI runs it on NTFS.
- **Windows mandatory byte-range locks:** they touch only page 2, which is never read or written (FORMAT §8.3), so no pager I/O conflicts.
- **D44 region names ≤30 bytes:** enforced at `shm/src/region.rs:362-368`. The Windows generation-named mapping avoids name reuse (D27).
- **macOS `F_PREALLOCATE` fallback chain** (`CONTIG` → `ALL` → tolerate ENOTSUP) and Linux `fallocate` EOPNOTSUPP → `set_len` (with zero-filled spares per D35).
- **Errors:** ENOSPC, `ERROR_DISK_FULL` and quota errors map to `NoSpace` (`io/src/error.rs:79`).
- **Windows `is_local`:** handles `\\?\UNC\` and mapped drives via `GetFinalPathNameByHandleW`.
- **`set_wakeup` and blocked tasks:** a `Blocked` task's I/O completion fires the app-owned wakeup (`TaskWaker::wake` → `Signal::notify_task`), so app loops park correctly between completions.
- **D40 enforcement:** both `Runtime::application_owned` and `Engine::open_application_owned` refuse `compaction_threads > 0` before opening anything.

## Reviewer: areas-5-6

## Issue #90 review: areas 5 (error paths) and 6 (unbounded resources)

Reviewed: `main` at 6756c2c (read-only). The repro tests ran in a `git archive` copy at
`docs/design/reviews/phase-1-probes/review56.rs` at commit e49a287a78eb (copy into `crates/engine/tests/` to run), built with `cargo test -p pigeonhole-engine --release --test review56`.
Line numbers refer to `main`.

Severity key: **B** = blocker for the Phase 1 gate, **S** = should-fix (Phase 1 unless noted), **D** = defer (phase given).

---

### Area 5: error paths

#### 5.1 [B] Close hangs forever after a cross-shard participant's WAL is poisoned (reproduced)
- **Where:** `crates/engine/src/shard.rs:4660-4666` (close waits in `Flushing` while `!self.log.is_empty()`), `shard.rs:4033-4046` (`needed`: a COMMIT stays needed until every participant reports `ShareFlushed`), `shard.rs:4745-4747` (`on_sync_barrier` on a poisoned shard always errors), `engine.rs:2309-2310` (`close` waits with no timeout).
- **Scenario:** 2 shards. A cross-shard commit puts a large share on shard X, which freezes and flushes it, and a small share on shard Y. Y's WAL then fails a sync and Y is poisoned (D85). At close, Y's flush fails because its own barrier errors, so Y gives up on flushing (`flush_failed`) and never sends `ShareFlushed`. The coordinator flushes successfully, because its memtables hold no unflushed shares, and then waits for that report forever. `Engine::close()` never returns. The same stuck COMMIT also blocks the coordinator's checkpoint during normal operation (see 6.1).
- **Repro:** `close_controls` in review56.rs. With a cross-shard commit and no poisoning, close returns `Ok`. With poisoning and no cross-shard commit, close returns the expected "close is not clean" `Io` error, on 1 shard and on 2. With a cross-shard commit **and** poisoning, close **HUNG** (10 s watchdog, both share orders).
- **Related spreading (same root):** while Y is poisoned, every other shard whose frozen memtables hold cross-shard shares sends a barrier to all shards (`flush.rs:316-321`). Y's barrier always fails (`shard.rs:4745`), so those flushes fail too. Healthy shards' arenas then fill and their writers get `Busy`. One shard's WAL failure takes down writes on every shard that took part in cross-shard commits.
- **Fix:** when a shard gives up flushing at close (or is poisoned), treat its unreported shares as settled for close purposes. One option: send a `ShareFlushed`-equivalent "gave up" message. Another: let a coordinator in close skip records that `shared.close.failed` makes moot, since the close is unclean anyway and replay keeps the WAL. Separately, a poisoned shard's barrier could succeed for LSNs at or below its last durable sync, so peers' flushes do not fail because of it. Add the repro as a regression test.

#### 5.2 [S] A failed flush is retried back-to-back during a room wait on a moving clock
- **Where:** `shard.rs:2410-2420` (failed flush during `wait_room` → immediate `Kick`), `shard.rs:3097-3100` (`ROOM_FLUSH_ATTEMPTS` applies only on a frozen clock), `shard.rs:3122-3126` (each run freezes and `spawn_flush`es again).
- **Scenario:** the disk is full (the `NoSpace` allocate in `SstSink`) or the device returns EIO, and a writer waits for arena room. Each failed flush kicks the shard, which spawns the same flush again at once. For `write_stall_timeout_nanos` (30 s by default), every stalled shard loops: either at 100% CPU (NoSpace fails at the first allocate) or rewriting the whole frozen set, up to `memtable_budget`, per iteration (EIO late in the write). The next commit starts another 30 s stall. D126 added backoff for compactions only.
- **Fix:** apply the compaction backoff (`compaction_backoff_nanos`) to flush retries on both clocks. Retry on the backoff timer or on a `Maintain`, not on the failure itself.

#### 5.3 [S] A stale background compaction error leaks into a later, unrelated `compact()`
- **Where:** `shard.rs:4554-4557` (`None => self.compaction_error = Some(e)`), `shard.rs:4330-4335` (`compact()` returns `compaction_error.take()`). Nothing else clears it, and a later successful compaction does not either (`shard.rs:4534-4545`).
- **Scenario:** a background compaction fails once (a transient EIO, or NoSpace that has since cleared). Hours later, after many successful background compactions, the user calls `compact()`. It does all its work successfully, then returns the old error. Second case: a `compact()` queued while a background compaction runs (`compact_all` is non-empty) receives that background compaction's failure through `pop_front()` at line 4555, and its own full compaction never runs. This is the class of bug #86 fixed for orphans.
- **Fix:** clear `compaction_error` when a compaction succeeds. Better, record only failures of compactions started on behalf of a `compact_all` entry. Do not hand a background failure to a queued caller; let it retry under backoff.

#### 5.4 [S] Compaction output extents leak when building the commit edits fails
- **Where:** `crates/engine/src/compact.rs:452` (`let (edits, readers) = self.edits(&catalog, output)?;`) and `compact.rs:363` (`SstReader::open(...)?` on each output). The error path in `run` (`compact.rs:471-475`) only reports.
- **Scenario:** opening a just-written output SST fails: an EIO reading its footer or index, or a corrupt write. `output.added` and its extents are dropped without `pager.abandon`. The pager is not poisoned, so they stay allocated until reopen. A slot that keeps failing leaks one set of outputs per retry (every ≤60 s under backoff), up to `target_sst_bytes` × outputs each time, and the file grows. Flush handles the same case correctly (`flush.rs:258-266`, `fail` abandons before submit).
- **Fix:** on any error before `manifest::submit`, abandon every `out.added` extent and every `new_blob_files` extent, the same way as the `TableNotFound` branch at `compact.rs:438-449`.

#### 5.5 [S] fsync errors outside a root commit are swallowed, so the next commit can publish over lost pages (Linux)
- **Where:** `crates/pager/src/lib.rs:628` (`allocate` → `sync_all()?`), `pager/src/lib.rs:846` (`truncate_tail` → `sync_all()?`), `crates/wal/src/stream.rs:261` (`SpareSegments::prepare` → `sync_all()?`, called with its result discarded at `engine/src/shard.rs:1246`).
- **Scenario:** D58 poisons the pager because "an fsync error may have dropped written pages". These three syncs run on the **same** file handle as the root commit and WAL syncs, but a failure there only returns `Err` (or is ignored). On Linux an fsync error is reported once per file description. Example: flush writes an SST (dirty pages), and a concurrent growth's `sync_all` in `allocate` gets EIO for those pages, so the error is consumed. The flush's later `commit_root` `sync_data` then succeeds and publishes a root naming the lost SST: silent corruption, caught later only as a checksum `Corruption`. The WAL case is the same: a spare-slot `sync_all` consumes an error for appended record pages, the stream is not poisoned, and the next group `sync` acknowledges commits as durable.
- **Fix:** treat any sync failure on the page file as a commit failure (set `State::poisoned` in `allocate` and `truncate_tail`). In `SpareSegments::prepare`, set the stream's `poisoned` flag on a `sync_all` failure (and on a `zero_fill` write failure).

#### 5.6 [S] A recoverable `NoSpace` in manifest `prepare` poisons the engine before anything was synced
- **Where:** `crates/engine/src/manifest.rs:190` (`result.inspect_err(|_| self.poisoned = true)`), `manifest.rs:220-225` (snapshot rewrite: `pager.allocate` for the snapshot and the 64 KiB log), `manifest.rs:629-633` (`begin` sets `shared.pager_poisoned`).
- **Scenario:** the disk fills. Flush and compaction fail and retry, as designed. The next manifest edit that does not fit the log, which can be just a `WalCheckpoint`, rewrites the snapshot. `allocate` fails with `NoSpace` before any byte was written or synced, but the writer and the engine are poisoned anyway. All writes now fail until reopen, even after the user frees space. D58 justifies poisoning only once a commit's first sync was issued. The requests' SST extents are also leaked until reopen (acceptable only because of the poison).
- **Fix:** do not poison on an allocation failure in `write_snapshot`. Answer the batch with the error, abandon the requests' added extents as the refusal path does, and keep the writer usable. Poison only on write or sync failures. Alternatively, record a decision that ENOSPC requires a reopen and document it on `ErrorCode::NoSpace`.

#### 5.7 [S, or D Phase 4 with a recorded decision] One deterministically failing slot stops compaction for the whole shard
- **Where:** `shard.rs:4342` (shard-wide `compaction_backoff`), `shard.rs:4364-4387` (after the backoff, `maintain` again picks the most urgent slot, the same one).
- **Scenario:** an input SST of slot A has a corrupt block (bit rot). Every compaction of A fails with `Corruption`, and the backoff grows to 60 s. When it expires, the picker chooses A again because its score has not changed. No other `(tablet, family)` on that shard is ever compacted, so their L0 grows and D119's L0 stall throttles writes to unrelated tables on that shard. Every retry also re-reads and rewrites A's inputs before failing.
- **Fix:** keep the backoff per slot, or skip a slot that failed N times in a row and pick the next one. Report the slot in metrics or logs (a quarantine).

#### 5.8 [low, D Phase 2] `retire_idle_slots` drops `Retired` tokens on a failed publish
- **Where:** `shard.rs:1999-2006`. The slots are removed and `retired` collected, then `publish_memtables()?` returns early. The tokens are `#[must_use = "retired chunks leak unless passed to ShardArena::reclaim"]` (`memtable/src/lib.rs:751`).
- **Scenario:** a view publish fails (oversized view). The shard is poisoned (`shard.rs:3123-3124`), and those arena chunks are lost until reopen. The leak is bounded, but it shrinks room for the still-served reads. `drop_tablets` (`shard.rs:2505`) has the same pattern.
- **Fix:** push them onto `self.retired` before publishing, or reclaim them on the error path.

---

### Area 6: unbounded resources

#### 6.1 [B] One cold slot pins the WAL checkpoint: unbounded WAL, `log` memory and replay time (reproduced)
- **Where:** `shard.rs:4114-4136` (`advance_checkpoint` pops only a **prefix** of `log`), `shard.rs:2073-2114` (a slot is frozen only past `memtable_freeze_bytes`, on `flush()`, at close, or under arena pressure), `shard.rs:3321` (one `Logged` with a `slots` Vec per commit). No WAL-size or age trigger exists (`options.rs`).
- **Scenario:** one write lands in a cold slot: a config table, a rarely written family, or a counter. Its memtable never reaches the freeze threshold. Hot slots keep flushing and free arena room, so no room-wait freeze-all ever happens. The stream's checkpoint is stuck behind the cold record for the life of the process. The WAL sidecar grows without bound (no slot is recyclable), `ShardState::log` gains one entry per commit, and the next open replays the whole WAL. 5.1 makes this worse: a stuck cross-shard COMMIT pins the coordinator in the same way.
- **Repro:** `cold_slot_pins_wal` (1 shard, 30 k × 1 KB commits to family `hot`): WAL **1.8 MB** without a cold write, **36.7 MB** with a single prior write to family `cold`. Flushes are equal (32): the growth is linear in total bytes written.
- **Fix:** RocksDB-style `max_total_wal_size`. When a stream's unrecyclable bytes, or the age of `log.front()`, pass a limit (for example a multiple of `memtable_budget` or of the spare segments), freeze and flush the slots named by the oldest `Logged` entries. For a cross-shard COMMIT blocked on a participant, ask the participant to flush that share.

#### 6.2 [S] `Shared::compactions` grows forever in production builds
- **Where:** `crates/engine/src/shard.rs:276` (field not `cfg(feature = "test-hooks")`), pushed at `manifest.rs:563-570` for every rewrite compaction (`shard.rs:4443-4462` builds the record, including the snapshots Vec and row-range keys). Drained only by `take_compactions`, which is gated by `test-hooks` (`engine.rs:1572-1583`).
- **Scenario:** a long-running server accumulates one `CompactionRecord` (about 100 B plus snapshots and two row keys) per compaction forever.
- **Fix:** gate the field, the push and the `record` construction behind `test-hooks`, as `appended` already is.

#### 6.3 [S, low] `aborted` keeps seqnos of cross-shard commits that were never logged
- **Where:** inserts at `shard.rs:3557` (a failed PREPARE), `3582`, `3800`, `3819` and `3906`. The only removal at run time is `shard.rs:4125`, when the record leaves the front of `log`. `clear()` runs only at open (`shard.rs:1896`).
- **Scenario:** a PREPARE refused before it is logged (`Conflict` from optimistic-transaction validation at `shard.rs:2985-2991`, `Busy`, a moved tablet), or a `Durability::None` share that aborts, has no `Logged` entry. Its seqno stays in `aborted` until reopen. A contended transactional workload leaks about 16-32 B per aborted cross-shard commit. Confirm with a test that `prepared` holds the share at admission, so line 3557 runs for conflicts.
- **Fix:** insert into `aborted` only when the seqno has a `Logged` entry, or prune entries below the seqno at `log.front()` on each `advance_checkpoint`.

#### 6.4 [D Phase 2] `loads` is not pruned on `drop_table`, and `dropped` is never pruned
- **Where:** `shard/tablets.rs:934-940` (`note_write`: up to `SAMPLES` = 64 copied row keys per tablet), removed only for moved, split or merged tablets (`tablets.rs:853-855`), not in `drop_tablets` (`shard.rs:2486-2495`). `dropped: HashSet<TabletId>` (`shard.rs:1563`) gains every dropped or changed tablet (`shard.rs:2495`, also from `tablets.rs:850`) and is never pruned.
- **Scenario:** with `tablet_changes` on, an application that creates and drops tables (per-day or temp tables) keeps 64 sampled rows per dropped tablet (up to MBs with large row keys). Every balancer pass walks the stale entries (`tablets.rs:1001`). `dropped` grows 8+ B per tablet change.
- **Fix:** in `drop_tablets`, also `self.loads.remove(t)` and `self.arrived.remove(t)`. Prune `dropped` of tablets below the checkpointed log front, or replace it with the catalog check once tablets are always on.

#### 6.5 [D Phase 3, with #42] The shard inbox and `pending` have no admission bound
- **Where:** `crates/runtime/src/lib.rs:300-313` (unbounded `mpsc::channel`), `shard.rs:1503` (`pending`), `shard.rs:1611` (`parked`), `shard.rs:1621` (`retries`).
- **Scenario:** `Engine::submit` returns a `PendingCommit` without waiting. During a 30 s arena or L0 stall, an async submitter queues unlimited batches in memory. The Phase 1 public API (`pigeonhole` crate) is blocking only, so this is bounded by the caller's thread count today. It becomes real with the async front door (#42).
- **Fix:** before #42 ships, add a per-shard in-flight byte budget: `submit` returns `Busy` or the future waits for admission.

#### Checked and not findings
- The manifest delta log is bounded (`manifest.rs:171-179`: it rewrites once the log passes 64 KiB or the snapshot size).
- `view_versions` is pruned to the oldest reader pin (`shard.rs:419-432`).
- Shard `retired` memtables are bounded by the arena.
- Pager retired extents held by long-lived pins are documented, with reader processes tracked in #39.
- Counter operands are tracked in #34 (Phase 2, D73).
- A flush failure abandons its outputs before submit (`flush.rs:241-268`).
- A refused manifest request abandons its added extents (`manifest.rs:509-518`, `573-576`).
- A failed view publish poisons and is documented (`manifest.rs:731-742`).
- Read I/O errors are not cached (`sst/src/reader.rs:170-184`).
- Cleanups are deduplicated, and each retry is driven by the balancer (`tablets.rs:1055-1060`).
- Metrics are fixed per-shard vectors.

## Reviewer: area-7

## Issue #90, area 7: concurrency edges

Reviewed `main` at 6756c2c. Probes are in the scratch copy at
`docs/design/reviews/phase-1-probes/area7_probes.rs` at commit e49a287a78eb (copy into `crates/engine/tests/`). Run them with
`cargo test -p pigeonhole-engine --test area7_probes -- --nocapture --test-threads=1`.
P4 also needs two env-gated sleeps added to the scratch copy's `engine.rs` and `shard.rs`
(`PROBE_READER_DELAY`, `PROBE_WRITER_DELAY`). The sleeps only widen windows that already exist.

### F7-1: a reader process's snapshot reads freed and reused extents after a writer restart (blocker)

**Where.**
- `crates/engine/src/engine.rs:2398-2402` (`reader_snapshot`, re-attach). The new slot starts unpinned (`pinned = false`). Snapshots the reader still holds from the old generation are protected only by a pin in the abandoned region.
- `crates/pager/src/lib.rs:256` (`OpenedPager::finish(live)`). At open, the new writer treats every extent its recovered root does not name as free. That includes the SSTs that live old-generation reader snapshots still read.
- `crates/shm/src/region.rs:808` (`oldest_reader_pin`). This reads only the new region.
- Spec line 282 promises the opposite: "Writer crash and restart. Readers keep serving their current snapshot."

**Scenario (probe `p1_reader_snapshot_survives_writer_restart`, deterministic).**
1. The writer writes and flushes two SSTs.
2. A reader process takes a snapshot.
3. The writer closes (a crash behaves the same), then reopens, runs `compact()` and writes new data with flushes.
4. The reader reads at its old snapshot. Results:
   - rows 0, 1 and 150 fail with `Err(Corruption("sst footer"))`;
   - rows 250 and 399 return **`Ok(None)`**. The data is silently gone, because a new valid SST now occupies the extent.

`shrink` after the restart can also truncate the file under the old snapshot.

**Fix options.**
- Cheap, correct Phase 1 fix: a reader snapshot remembers the shm generation it was taken in. Every read through it re-checks `is_stale()`/generation **after** the read and fails with a typed error such as `SnapshotExpired` if the generation changed. Seqlock style is sound because the new writer publishes its generation before it allocates anything. Amend the spec line to "the next snapshot remaps; snapshots from before the restart fail with ...".
- Full fix: the new writer quarantines the free space found at open until every live old-generation reader slot has re-pinned or died, and a re-attaching reader with `live > 0` pins the oldest view in the new region. This is larger and could be Phase 4 with #39.

### F7-2: reader views mix a memtable list and a catalog from different manifest versions, so flushed data is counted twice or vanishes (blocker)

**Where.**
- `crates/engine/src/engine.rs:2420-2449` (`reader_snapshot`):
  - memtables come from the shm view record (`read_view`, line 2420);
  - SSTs come from a catalog loaded afterwards by `reader_refresh` (2373-2393). That load is keyed on `shm.manifest_version()`, not on `record.manifest_version`, and it reads the **current durable root** (`Pager::open`, 2385), which can be ahead of both.
- `crates/engine/src/shard.rs:416-417`: the writer publishes the view and only then sets the manifest version.

**Scenario (probe `p4_reader_view_record_vs_catalog_skew`).** One writer adds +1 operands to an `i64_add` counter and flushes every 20 commits. A reader process loops snapshot+get and compares the result with the committed count.
- Reader preempted between `read_view` and `reader_refresh` (simulated with a 300 µs sleep): the record still lists memtable *m*, and the catalog already holds *m*'s SST. The reader got `76` when only `66` were committed (9-13 of about 210 reads). Operands were double counted. The same duplication happens for any `max_versions > 1` read and for `RawEntry`-style dumps.
- Writer preempted between `publish_view` and `set_manifest_version` (500 µs sleep): the record no longer lists *m*, and the cached catalog lacks *m*'s SST. The reader got `0` when 20 were already visible at its snapshot (387 of about 1100 reads). That is a stale read at a snapshot, with data missing.
- Without the sleeps, 3 runs of about 300 reads did not hit the race. The windows are real, though: thread preemption anywhere in them, and the writer's root commit landing before the view is published (the durable root is ahead), both open them.
- With `tablet_changes` on, a skew across a split makes it worse. The record's tablet map routes to the parent tablet, which the newer catalog has dropped, so every SST row of that tablet is missing.

**Fix.** Build the reader view only from a catalog whose version equals `record.manifest_version`:
- after the refresh, if the versions differ, re-read the record and retry (bounded);
- when the durable root is ahead of the record, wait or retry until the writer publishes.

An alternative is to also store the manifest version in the record pointer word, so the record and the version are read atomically. Add a reader-process test with a counter family under concurrent flushes.

### F7-3: `drop_table` racing a flush makes `flush()`, `compact()` and `close()` fail, and close is unclean (should-fix, Phase 1)

**Where.**
- `crates/engine/src/manifest.rs:518-527`: a request with **any** orphaned `AddSst`/`SetFlushed` is refused as a whole.
- `crates/engine/src/flush.rs:346-379`: one flush request carries every slot of the shard, and `DropTablets` filters only `flush_queue`, not the running task (`shard.rs:2486-2492`).
- `crates/engine/src/shard.rs:2385-2420` (`on_flushed` Err path): every `flush_waiters` and `compact_all` caller gets `Err(Io("flush: ... the table was dropped"))`. If the shard is closing, `close.failed` is set, so `close()` returns `Err` and the clean flag is not written.

**Scenario (probe `p2_drop_table_during_flush_fails_flush`, 20/20 seeds).** Tables t and u both have unflushed data. The sequence `flush_pending()`, then `drop_table(t)`, then wait gives `Err(Io: flush: no such table "the table was dropped")`. With `close()` in place of the wait: `Err(Io: a shard's final flush ... failed; the close is not clean)`.
- No data is lost: u's frozen memtable is requeued, and the next open replays.
- But an unrelated table's flush is thrown away and redone.
- A plain background flush in flight when the application does `drop_table; close` hits the same path. A sequential probe (`p2b`) did not hit it on SimVfs because the flush finishes too fast; it needs real I/O timing.
- It also counts toward the room-wait `failed_flushes` (D126 frozen-clock `Busy` after 4).

**Fix.**
- In `manifest::begin`, for `ReqKind::Edits`, drop only the orphaned `AddSst`/`SetFlushed` edits and abandon just those extents instead of refusing the request.
- Alternatively, treat a flush refused only for dropped tablets as success for the remaining items, as `on_compacted` already does for `TableNotFound` (`shard.rs:4549`).

### F7-4: `shrink()` (and other app-thread maintenance) keeps committing after `close()` returns and the writer lock is released (should-fix, Phase 1)

**Where.**
- `crates/engine/src/engine.rs:1320-1327`: `shrink` calls `check_open` once, then runs up to 8 rounds of relocation, manifest commit and truncation (`maintenance.rs:156-299`).
- `crates/engine/src/manifest.rs:469` (`begin`) and `crates/pager/src/lib.rs` do not refuse after the final close.
- `crates/engine/src/shard.rs:503-517`: `final_close` marks the file clean and then `drop(locks)`, which releases the writer byte.

**Scenario (probe `p3_shrink_racing_close`).** Thread A runs `shrink()` and thread B runs `close()`. `close()` returned `Ok`, and `shrink` went on and committed another root about 160-300 µs later (2 of 10 seeds). The file at rest is then **not** clean (`clean_shutdown() == false`), so the next open replays even though close reported success.
- Worse: once `final_close` drops the writer lock, another process can open as writer while this process's `shrink` still commits roots and truncates the file. Two writers on one file corrupt it.
- `catalog_change` (create, drop, add family) and `backup` have the same check-then-act gap, but their windows are microseconds.

**Fix.**
- Keep an in-flight counter for app-thread maintenance (shrink, catalog changes, backup) that `final_close` waits on before marking the file clean and releasing locks. Or make `manifest::begin` refuse with `Closed` once `close.final_pending` or `closed` is set.
- Have `shrink` re-check `closing` at the top of each round.

### F7-5: `shrink` works from a stale catalog: spurious errors on concurrent compaction or `drop_table`, and a trivially moved SST can be put back at its old level (should-fix, Phase 1)

**Where.**
- `crates/engine/src/maintenance.rs:166-172`: the view and catalog are taken once per round.
- `maintenance.rs:205-216`: `busy_ssts` is checked only after that, so a compaction that committed and released its inputs in between is not seen.
- `maintenance.rs:223`: `relocate`.
- `maintenance.rs:268-279`: the edits name the level from the stale catalog.

**Scenarios.**
- **Spurious errors (probe `p6_shrink_racing_background_compaction`).** Shrink loops while writers flush and compact. One of three runs returned `Err(Io: relocate of an extent that is not live (unallocated or retired))`: the compaction retired the input after shrink read the catalog. A `drop_table` committed between shrink's catalog read and its commit makes the whole shrink fail with `TableNotFound("the table was dropped")` (`manifest.rs:518-527`).
- **Stale level (by inspection).** If the compaction was a trivial move (same `SstId`, new level), the extent is still live, so `relocate` succeeds. Shrink's `RemoveSst X` then removes the entry at the new level, and its `AddSst X'` re-adds the SST at the *old* level. When X moved down from L_n, and a later compaction into L_n wrote an overlapping SST within shrink's copy window, L_n would hold overlapping SSTs. The L≥1 non-overlap assumption is then broken for the picker and point reads. This was not reproduced; it needs a long relocation window.

**Fix.**
- Submit shrink's edits as a `ReqKind::Catalog` closure. Against the catalog at commit time, it keeps a move only if X is still present, re-adds X' at X's *current* level(s), and abandons X' otherwise.
- Treat "not live" from `relocate` as skip, not error.
- Skip orphaned tablets instead of failing the request (same mechanism as F7-3).

### F7-6: `backup` holds a full snapshot, memtables included, for its whole duration, so writers stall and then get `Busy` (defer, Phase 2; document now)

**Where.** `crates/engine/src/engine.rs:1308-1316` takes `inner.snapshot()` and passes it to `maintenance::backup`, which merges every slot from the snapshot's memtables and SSTs (`maintenance.rs:35-140`).
- The snapshot's view holds `MemtableReader` pins. The arena frees a retired memtable only when its pin's strong count is 1 (`crates/memtable/src/lib.rs:366-376`).
- So every chunk live at the snapshot stays allocated until the backup finishes.

**Scenario (by inspection; same mechanism as D126/D138 for user snapshots).** A backup of a large file takes minutes. If the arena was mostly allocated when the backup started (many slots, or a burst), writers find no room:
- they wait `write_stall_timeout_nanos` (30 s) and then fail with `Busy`;
- `flush()`/`compact()` callers fail the same way (D138);
- on a frozen clock, the idle case refuses at once.

The doc says backup runs "while writers run" with no such caveat.

**Fix.**
- Copy the snapshot's memtable entries first. They are bounded by the arena budget: spill them to a temporary SST in the destination, or merge them up front per slot. Then drop the memtable half of the view and keep only the SST view, whose extents cost file space, not arena.
- Or `flush()` first and back up an SST-only snapshot.
- At minimum, document the stall.

### Checked, no finding

- **Unflushed WAL records of a dropped table after a process or power crash.** Covers single-shard and cross-shard commits, and a table re-created under the same name. Probe `p5_drop_table_then_crash_replays` passes.
- **Compaction racing `drop_table`.** Output is abandoned, inputs are retired once with the drop, and `TableNotFound` is swallowed (`compact.rs:437`, `shard.rs:4549`).
- **Compaction racing shrink on the same SST.** `busy_ssts` excludes it, and the compaction's commit lands before its inputs are released.
- **A tablet change racing shrink or drop.** `tablet_change` recomputes its edits against the catalog at commit time (`tablets.rs:1702`), and orphan checks refuse stale shrink edits.
- **The reader pin protocol within one generation.** `ReaderSlot::pin` re-checks the view pointer, and `ScanCursor` clones its `Snapshot`.
- **In-process snapshots across `drop_table`, compaction and shrink.** `ViewPin` and `live_views` gate `reclaim`, and `truncate_tail` counts retired extents as allocated.
- **Close versus background compaction (#78).** The final close runs under the manifest exclusion and drains the queue first.

## Reviewer: areas-8-9

## Issue #90 review: areas 8 (defaults) and 9 (error and doc truthfulness)

Reviewed main at 6756c2c. Probes ran against a `git archive` copy at
a scratch copy, with new examples in `docs/design/reviews/phase-1-probes/example_*.rs` at commit e49a287a78eb (copy into `crates/pigeonhole/examples/`).
They ran on macOS (10 CPUs, APFS) in release mode. Findings marked "by inspection" were not executed.

### Defaults table (as found)

| Default | Value | Where |
|---|---|---|
| shards | `available_parallelism()` (affinity and cgroup quota on Linux) | engine.rs:448, io/os/unix.rs:456 |
| memtable_budget | 64 MiB per shard; freeze at budget/4 | pigeonhole/options.rs:58,395 |
| shm arena | budget × shards, sparse `set_len`, no preallocation | engine.rs:518, io/os/unix.rs:243-258 |
| block cache | 256 MiB total (writer), 256 MiB per reader process | engine/options.rs:115 |
| WAL | 64 MiB segments, 2 spares, so 3 slots = 192 MiB per shard, zero-filled | wal/lib.rs:174-180 |
| write_stall_timeout | 30 s, not exposed publicly | engine/options.rs:128 |
| durability | GroupSync | options.rs:55 |
| compaction | L0 trigger 4, L1 256 MiB, ×10, 7 levels, 64 MiB SSTs | compaction/picker.rs:42-52 |
| tablets | off; split 256 MiB, balance every 100 ms, min writes 2000 | engine/options.rs:120-124 |
| reader slots | 126 | engine/options.rs:118 |
| family | max_versions 0 (keep all), no TTL, bloom 10, LZ4 16 KiB, blob 4096 (stored only) | pigeonhole/options.rs:270 |
| thread pinning | on, shard i → CPU i, no public opt-out | runtime/lib.rs:553,626,636 |

Measured footprint:

| Case | Disk while open | Open time | At rest |
|---|---|---|---|
| `shards(1)`, 1 row | 768 KiB file + 192 MiB `-wal-0` | 64–80 ms | 768 KiB |
| default (10 shards), 1000 tiny rows | 768 KiB + 10 × 192 MiB = **1.9 GiB** | **431–519 ms** | 768 KiB |
| 50 MiB incompressible, compact | — | — | **128 MiB**, `shrink()` = 0 |

RSS right after open is about 4 MiB, because the arena is touched lazily.

---

### F1: Every open preallocates and zero-fills 192 MiB of WAL per shard, and the default shard count is every CPU
**Severity:** should-fix for Phase 1. The default directly hurts both target users. The open-latency target itself is gated in Phase 3.

**Evidence**
- `WalOptions::default` sets 64 MiB segments with 2 spares (wal/lib.rs:174-180).
- D35 has `create`/`into_stream` zero-fill the first slot synchronously at open. The spares are zero-filled in the background (wal/stream.rs:254-259).
- A clean close removes the sidecars (engine.rs:1365-1368), so every open starts over.
- The segment size does not scale with `memtable_budget`. The only knob is hidden (`Options::wal_segment_size`, options.rs:185).

**Scenario**
- On a 10-core laptop, `Pigeonhole::open(path, Options::default())` for a tiny DB takes about 0.5 s. It writes 1.9 GiB of zeros (640 MiB of it synchronously) and holds 1.9 GiB of disk while open. A CLI tool that opens per invocation writes about 1.9 GiB on every run.
- On a 128-core server: 24 GiB of WAL preallocation and about 8 GiB of synchronous zero-fill per open, so open takes seconds.
- The spec goal "Open to first read < 5 ms" (spec.md:40, README.md targets) is off by 10–100×.

**Suggested fix (any of)**
- Derive the segment size from the budget, e.g. `clamp(memtable_budget, 1 MiB, 64 MiB)`, and use 1 spare by default.
- Create or zero-fill a stream lazily on its shard's first write.
- Keep (recycle) the sidecars across a clean close instead of deleting and recreating them, as SQLite does with `-wal` reuse. This needs a decision against the "one file at rest" goal.
- Accept one metadata-updating fdatasync on the first segment instead of a synchronous zero-fill.

Record the chosen default as a decision.

### F2: Default shard count = CPUs is mostly cost while `tablet_changes` is off; a shrinking shard count can make a crashed database unopenable with defaults
**Severity:** should-fix for Phase 1. The replay part is by inspection.

**Evidence**
- With tablets off, a table is one tablet on shard `tablet % shards` (D129). A one-table app uses one shard but pays N pinned threads, N WAL streams (F1) and N arenas.
- Replay applies every record into the in-memory shard states with no mid-replay flush. A full arena maps `Busy` to `InvalidArgument("the memtable budget is too small…")` (engine.rs:675-680, 753, 1793-1800).
- D121's `flush_recovered` runs only after all streams are replayed.

**Scenario**
1. A default-config DB crashes on a 16-core host while several tables (each on a different shard) hold about 20 MiB unflushed each.
2. It is reopened with defaults in a 2-CPU container, or after a cgroup quota change, which the default shard derivation follows.
3. The unflushed data from 16 streams lands on 2 arenas of 64 MiB, so open fails with `InvalidArgument`.

The user must guess a larger `memtable_budget`. This contradicts "zero required config".

**Suggested fix**
- Have replay flush recovered memtables when a shard's arena fills (`flush_recovered` exists), instead of failing.
- Consider defaulting `shards` to `min(CPUs, small N)` while `tablet_changes` is off. Or document that the default only pays off with tablets on.
- Test: SimVfs, 4 shards, 4 tables × 40 MiB unflushed, crash, reopen with `shards(1)`.

### F3: Arena "never fits" is about half the arena, not "larger than the arena"; values inside the documented limit get `Busy`; one message covers transient and permanent cases
**Severity:** should-fix for Phase 1. Docs are wrong and a generic retry loop never terminates.

**Evidence**
- `arena_needed` returns `2 * total + (new_slots + 2) * chunk` (shard.rs:1956-1977).
- `Room::Never` is chosen when `needed + 2*chunk > arena` (shard.rs:2042-2046).
- The value limit is `min(segment payload, 64 MiB, budget/2)` (pigeonhole/db.rs:590-597, engine.rs:918-921).
- `engine::Error::Busy` is a unit variant, and pigeonhole/error.rs:156-158 always gives the same message.

**Measured with the default 64 MiB budget and 1 shard (`val.rs`, `batch.rs`)**
- A single value of 33,292,288 B, 33,553,408 B or 33,554,432 B (exactly half the arena, which D16 says is allowed) fails with `Busy`, message "…retry, or raise Options::memtable_budget". 32,505,856 B succeeds. 33 MiB correctly gets `ValueTooLarge`.
- A 40 MiB batch fails with `Busy` in 26 ms, and 50–100 MiB batches in about 100 µs, although the docs say only a batch larger than the 64 MiB arena never fits.
- With budget 1 GiB, a 70 MiB batch gets `RecordTooLarge`.

**Docs that are wrong**
- pigeonhole/options.rs:98-101 ("a batch larger than the arena fails with Busy")
- error.rs:58-62
- errors.md row 24 and the handling table
- agent-reference.md:27
- getting-started.md "What happens when writes outrun the disk"
- error.rs:60 and errors.md say "30 s by default", which implies a public knob that does not exist.

**Suggested fix**
- Give `engine::Error::Busy` a reason (`Stalled`/`NeverFits`). Map `NeverFits` to a distinct message, or better to a non-retryable code (reuse `RecordTooLarge` with a broadened meaning, or add a new code; codes are additive).
- Clamp `max_value` to what the arena accounting admits (≈ `(arena − 4·chunk)/2 − overhead`) so oversized values get `ValueTooLarge`.
- Document the real batch cap as "about half of `memtable_budget`".

### F4: `compact()` + `shrink()` does not give space back; the file at rest is 2.5–5× the live data
**Severity:** should-fix for Phase 1, possibly a blocker once root-caused. The delete-everything case suggests data is not being dropped.

**Measured (`footprint3.rs`, 1 shard, 1 KiB incompressible values, `max_versions(1)`)**

| Load | Delete | File after compact | `shrink()` | At rest |
|---|---|---|---|---|
| 50 MiB | 100% of rows | 128 MiB | 0 (also after reopen + compact) | 128 MiB with 0 rows |
| 20 MiB | 100% | 64 MiB | 0 | 64 MiB |
| 50 MiB | 99% | 192 MiB | 190 MiB | 2 MiB (works) |
| 50 MiB | 90% | 192 MiB | 176 MiB | 16 MiB |
| 200 MiB | 50% | 576 MiB | 0 | 576 MiB (about 100 MiB live) |
| 50 MiB | none, just compact | 128 MiB | 0 | 128 MiB |

**What this suggests**
- Deleting 100% of rows behaves completely differently from deleting 99%. Either a full compaction whose output is empty does not drop its inputs, or the purge never happens. In both cases the deleted data stays in the file, which also matters for privacy.
- In the "live data only" cases, `relocate` finds no free extent of the same power-of-two class below the tail (pager/lib.rs:790-796), so nothing moves.

**Docs this contradicts**
- db.rs:362-366 ("Call this after a large delete followed by compact")
- getting-started.md Maintenance
- agent-reference.md:70

**Separate doc bug:** the pager's `NoSpace` is swallowed (`stop = true`; maintenance.rs:225-228), so `shrink` returns `Ok(0)`. db.rs:375-377, getting-started.md, agent-reference.md:70 and errors.md row 18 all promise `NoSpace` "when there is no free extent below".

**Suggested fix**
- Root-cause the 100%-delete case (`plan_full`/compaction commit with zero outputs; GcPolicy `min_ts_above`).
- Let `shrink` relocate a large extent into several smaller free extents, or split SSTs to fit.
- Make the docs say `Ok(0)` (or actually return `NoSpace`).
- Add a public test: load, delete all, compact, shrink, then assert the file is ≤ a small bound.

### F5: Linux default `/dev/shm` region is sparse, so a small tmpfs gives SIGBUS, not `ShmUnavailable`
**Severity:** should-fix for Phase 1. By inspection; not run on Linux.

**Evidence**
- `open_default_shared` → `open_region_file` → `set_len` (io/os/unix.rs:243-258, 268-277). There is no `posix_fallocate`, `statvfs` check or prefault.
- The arena is `MAP_SHARED` over that file (unix.rs:225-240). `ShmUnavailable` only comes from `NoSpace`/`Other` at creation (shm/region.rs:154-157).

**Scenario**
- Docker's and Kubernetes' default `/dev/shm` is 64 MiB. Even `shards(1)` with the default budget needs 64 MiB plus a header, and defaults on N CPUs need N × 64 MiB.
- Creation succeeds. Once memtables touch more pages than the tmpfs holds, the host process dies with SIGBUS.
- errors.md row 5 promises `ShmUnavailable` with the hint "lower memtable_budget or shards".

**Suggested fix**
- At create, compare `statvfs(dir).f_bavail` against the region size and refuse with `ShmUnavailable`. Or use `posix_fallocate` on the tmpfs file, which commits memory but fails cleanly with ENOSPC.
- Mention containers in errors.md and on `Options::shm_dir`.

### F6: Shard threads are always pinned to CPU i, with no public opt-out
**Severity:** should-fix (small additive API), or defer to Phase 3 with a recorded decision.

**Evidence**
- `RuntimeConfig.pin_threads` defaults to true. Shard i is pinned to CPU `i % cpus` and compaction thread j to `(shards + j) % cpus` (runtime/lib.rs:553, 626, 636).
- `EngineOptions.pin_threads` is not reachable from `pigeonhole::Options`.

**Scenario**
- Two databases in one process, or several processes embedding Pigeonhole, all pin their shard 0 to CPU 0.
- With tablets off, a single-table app's only busy shard is a low-numbered CPU, which often also takes IRQs.
- `compaction_cores(k)` with default shards wraps onto shard CPUs.
- An embedded library hard-pinning threads by default is surprising for small users.

**Suggested fix:** add `Options::pin_threads(bool)`, consider defaulting it off when `shards` is derived, and document it.

### F7: The 256 MiB block-cache default is undocumented and applies per reader process
**Severity:** should-fix (docs). Consider scaling to RAM; can defer to Phase 3 (owned cache).

**Evidence**
- engine/options.rs:115 sets 256 MiB.
- `Options::block_cache` (pigeonhole/options.rs:107), `ReaderOptions::block_cache` (options.rs:218), agent-reference.md:85,93 and getting-started.md:40 never state the default. getting-started passes 256 MiB explicitly as if it were a choice.

**Scenario:** on a 512 MiB device, the writer can use 256 MiB of cache plus 64 MiB × shards, and every reader process adds another 256 MiB.

**Suggested fix:** document the default on both builders and in the agent reference, and consider `min(256 MiB, RAM/8)`.

### F8: The durability guide promises a global suffix, but D84 says durability is per stream
**Severity:** should-fix (docs).

**Evidence**
- durability.md:17 says "In every level a crash loses only a suffix of recent commits: never one from the middle".
- D84 says "there is no global prefix across streams".
- durability.md:61 even shows a counter-example: a Buffered commit on shard A is lost while a later GroupSync commit on shard B survives.

**Suggested fix:** say "a suffix of each shard's commits", and point to the Mixed levels section.

### F9: The tablet_changes docs contradict each other and are stale
**Severity:** should-fix (docs), and the maturity call needs a decision.

**Evidence**
- engine/options.rs:62-63 says "**Not safe to turn on yet:** … open correctness and liveness bugs (#94, #95, #98, #102–#105)". All of those issues are now closed. #38 is still open.
- The public `Options::tablet_changes` (pigeonhole/options.rs:150-160, exposed in #133) gives no maturity warning.
- crates/pigeonhole/README.md says "One table still lives on one shard until tablet splits land", which ignores the opt-in.
- agent-reference.md:91 omits the in-flight ordering caveat that the rustdoc states (D143).

**Suggested fix:** update the engine doc to the current state, add a matching sentence on the public option (experimental or not, per #38's outcome), and update the crate README.

### F10: The HBase purge caveat (D74) is missing from the agent reference and the concepts page
**Severity:** should-fix (docs).

**Evidence**
- agent-reference.md:16 says a later `put_at` at a deleted ts "stays hidden", unconditionally.
- concepts.md:45 says the same for column, family and cell deletes.
- D74 says that after a bottommost purge, such writes become visible. Only data-modeling.md:156 states this.

**Suggested fix:** add "until compaction purges the marker (D74)" with a link to data-modeling.md#… in both places.

### F11: Flush and compact `Busy` (#120) is missing from the quick references
**Severity:** should-fix (minor docs).

**Evidence**
- getting-started.md Maintenance says "`flush()` and `compact()` … both fail with `ErrorCode::Closed` after `close`" and does not mention `Busy`.
- The agent-reference.md:68-69 `flush` and `compact` rows do not list `Busy`.
- The rustdoc (db.rs:264-267, 296-298) and errors.md row 24 are correct.

### Checked and accurate (no finding)
- `KeyTooLarge` at 65,536 B for both row key and qualifier, measured.
- `ValueTooLarge` message and limit formula (apart from F3's boundary).
- `RecordTooLarge` is reachable with a large budget.
- The "Reopen budget" → `InvalidArgument` claim matches `replay_error`.
- D36 reader write access is stated on `open_reader`, in concepts and in the agent reference.
- D94 `None` durability is stated consistently in durability.md, getting-started and the agent reference.
- D40 `compaction_cores` refusal.
- The "one file at rest after clean close" claim was measured true.
- cgroup quota is honored for the shard count on Linux, via std `available_parallelism`.
- Compaction defaults (L0 4, 256 MiB ×10, 64 MiB SSTs), reader slots 126, the 30 s stall timeout, GroupSync and the 128 KiB budget floor all look sane for both small and large users, apart from the issues above.
