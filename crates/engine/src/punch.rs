//! Giving the data file's free space back to the filesystem (D333, #2013; ICR 0144).
//!
//! With [`EngineOptions::punch_freed`](crate::EngineOptions::punch_freed) on, a reclaim
//! after a manifest commit ([`Shared::reclaim`]) checks how much free space is not yet a
//! hole. Once that passes the reserve by a block's worth, it starts one [`PunchPass`] (never
//! two at once): a background task that punches one free block per slice
//! ([`Pager::punch_free`](pigeonhole_pager::Pager::punch_free)), the largest past the
//! reserve first. A pass stops when nothing is left, or after [`PunchTuning::pass_bytes`];
//! one stopped there starts the next at once if the gate still passes, so an idle database
//! keeps punching until it is done (g152 row 2), and sleeps between blocks to stay under [`PunchTuning::bytes_per_sec`] (on a real clock;
//! a simulated one punches without pacing).
//!
//! Only free space is punched: what no recoverable root and no live view names, which
//! the allocator would overwrite anyway. A filesystem that cannot punch (`Unsupported`)
//! stops punching until the next open; any other error ends the pass and counts as a
//! refusal, and the next pass waits for a trigger's worth of new free space, as after an
//! empty one (simp724's S1 on #2034), or 10 s, whichever comes first (#2181). A poisoned pager (a failed root commit) punches
//! nothing: the failed root may be the one recovery opens (simp724's M1).
//!
//! No pass starts or punches once the close began (`Shared::closing`), and none punches
//! after the close released the file's locks: another writer may then have the file, and
//! its allocator knows nothing of a block a pass took. A slice holds [`Punch`]'s `stop`
//! lock across its last check and the punch, and `Shared::release_after_io` sets `stop`
//! under that lock before it drops the locks, so it waits for a punch in flight (#2181:
//! in application-owned mode a shard that reported its part of the close is still driven,
//! and the last shard's release runs on another thread).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pigeonhole_io::ErrorKind;
use pigeonhole_runtime::{Task, TaskPoll, TaskWaker};

use crate::shard::Shared;

const MIB: u64 = 1 << 20;

/// How long after a refused punch the gate stops waiting for new free space and punches
/// what is there again (#2181): a transient error doesn't park punching, and one that
/// persists fails at most once per this, not once per manifest commit (simp724's S1).
const RETRY_AFTER_NANOS: u64 = 10_000_000_000;

/// How a punch pass runs (D333's defaults; tests override them).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PunchTuning {
    /// Free space kept written, at least: the lowest free space, which the allocator hands
    /// out first. The reserve is the larger of this and twice the shards' memtable budgets,
    /// capped at a quarter of the file (default 64 MiB).
    pub min_reserve: u64,
    /// Most bytes one pass punches (default 256 MiB).
    pub pass_bytes: u64,
    /// The punch rate across a pass, in bytes per second (default 64 MiB/s).
    pub bytes_per_sec: u64,
    /// Free space past the reserve, not yet a hole, that starts a pass (default 64 MiB,
    /// the largest block).
    pub trigger_bytes: u64,
}

impl Default for PunchTuning {
    fn default() -> Self {
        Self {
            min_reserve: 64 * MIB,
            pass_bytes: 256 * MIB,
            bytes_per_sec: 64 * MIB,
            trigger_bytes: 64 * MIB,
        }
    }
}

/// The punching state on [`Shared`]: `None` there when punching is off.
#[derive(Debug)]
pub(crate) struct Punch {
    tuning: PunchTuning,
    /// Twice the shards' memtable budgets: the reserve before the file-size cap.
    memtable_reserve: u64,
    /// A pass is queued or running (one at a time).
    running: AtomicBool,
    /// The filesystem refused with `Unsupported`: no more passes until the next open.
    unsupported: AtomicBool,
    /// The unpunched free bytes when the last pass found nothing to punch (0 after one
    /// that punched). The gate can pass with nothing to punch: holes inside the reserve
    /// count toward it, and the reserve's last block may overshoot it by up to a block
    /// (simp724 on #2033). So the next pass waits until that much free space grows by
    /// `trigger_bytes`, rather than starting an empty pass after every commit. The cost:
    /// after an empty pass, up to `trigger_bytes` of newly punchable space can wait, since
    /// free space can move from inside the reserve to past it (allocations take low space
    /// while reclaims free high space) without the unpunched total growing.
    idle_at: AtomicU64,
    /// After a refused punch: when (monotonic nanoseconds) the gate drops `idle_at` and
    /// punches what is there again; 0 when no refusal waits (#2181).
    retry_at: AtomicU64,
    /// When the next block may be punched (monotonic nanoseconds), carried from one pass to
    /// the next so chained passes keep `bytes_per_sec` (simp724's N2 on #2034). One pass at
    /// a time writes it.
    next_at: AtomicU64,
    /// Set once the close is about to release the file's locks (`Punch::stop`); a slice
    /// holds it across its last check and its punch, so no punch lands after the release
    /// (#2181).
    stopped: Mutex<bool>,
    /// Passes started.
    pub passes: AtomicU64,
    /// Punches that failed, the `Unsupported` one included.
    pub refusals: AtomicU64,
}

impl Punch {
    pub(crate) fn new(tuning: PunchTuning, memtable_budget: u64, shards: usize) -> Self {
        Self {
            tuning,
            memtable_reserve: memtable_budget
                .saturating_mul(2)
                .saturating_mul(shards as u64),
            running: AtomicBool::new(false),
            unsupported: AtomicBool::new(false),
            idle_at: AtomicU64::new(0),
            retry_at: AtomicU64::new(0),
            next_at: AtomicU64::new(0),
            stopped: Mutex::new(false),
            passes: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
        }
    }

    /// The reserve for a file of `file_bytes`: min(max(min_reserve, 2 × memtable budget ×
    /// shards), file / 4).
    fn reserve(&self, file_bytes: u64) -> u64 {
        self.tuning
            .min_reserve
            .max(self.memtable_reserve)
            .min(file_bytes / 4)
    }

    /// Whether to start a pass now; if so, marks one running (the caller spawns it, or
    /// calls [`Punch::finished`] if it cannot).
    pub(crate) fn should_start(&self, shared: &Shared) -> bool {
        if self.unsupported.load(Ordering::Relaxed)
            || self.running.load(Ordering::Relaxed)
            || shared.pager_poisoned.load(Ordering::Acquire)
            || shared.closing.load(Ordering::Acquire)
            || shared.closed.load(Ordering::Acquire)
        {
            return false;
        }
        self.retry_if_due(shared.clock_vfs.monotonic_nanos());
        let s = shared.pager.stats();
        if !self.gate(unpunched_free(&s), s.file_bytes) {
            return false;
        }
        self.running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Whether `free` unpunched bytes in a file of `file_bytes` call for a pass: a trigger's
    /// worth past the reserve, and a trigger's worth of new free space since the last empty
    /// or refused pass. The mark follows free space down (#2181): space the workload reuses
    /// after such a pass doesn't have to be freed again before the gate counts new space.
    fn gate(&self, free: u64, file_bytes: u64) -> bool {
        let idle = self.idle_at.fetch_min(free, Ordering::Relaxed).min(free);
        let floor = self.reserve(file_bytes).max(idle);
        free >= floor.saturating_add(self.tuning.trigger_bytes)
    }

    /// A refused punch's wait is over at `now`: the gate punches what is there again.
    fn retry_if_due(&self, now: u64) {
        let at = self.retry_at.load(Ordering::Relaxed);
        if at != 0 && now >= at {
            self.retry_at.store(0, Ordering::Relaxed);
            self.idle_at.store(0, Ordering::Relaxed);
        }
    }

    /// A punch was refused at `now` with `free` unpunched bytes left: the gate waits for new
    /// free space, as after an empty pass, or until [`RETRY_AFTER_NANOS`] pass.
    fn refused(&self, now: u64, free: u64) {
        self.ended(0, free);
        self.retry_at.store(
            now.saturating_add(RETRY_AFTER_NANOS).max(1),
            Ordering::Relaxed,
        );
    }

    /// A pass found nothing more to punch, having punched `punched` bytes, with `free`
    /// unpunched bytes left: sets the idle mark after an empty pass, clears it otherwise.
    fn ended(&self, punched: u64, free: u64) {
        let idle = if punched == 0 { free } else { 0 };
        self.idle_at.store(idle, Ordering::Relaxed);
    }

    /// No punch from now on, for good: called before the close releases the file's locks
    /// (`Shared::release_after_io`). Waits for a punch in flight, at most one block's.
    pub(crate) fn stop(&self) {
        *self.stopped.lock().unwrap_or_else(PoisonError::into_inner) = true;
    }

    /// The pass ended (or never started): the next trigger may start another.
    pub(crate) fn finished(&self) {
        self.running.store(false, Ordering::Release);
    }
}

/// Free bytes that are not holes yet: the file less what is allocated, retired or punched.
fn unpunched_free(s: &pigeonhole_pager::PagerStats) -> u64 {
    s.file_bytes
        .saturating_sub(s.allocated_bytes)
        .saturating_sub(s.retired_bytes)
        .saturating_sub(s.punched_bytes)
}

/// One punch pass (module docs): one block per slice, paced, at most
/// [`PunchTuning::pass_bytes`]. Offloaded to a background worker when there is one.
pub(crate) struct PunchPass {
    shared: Arc<Shared>,
    punched: u64,
    /// Stopped at [`PunchTuning::pass_bytes`] with space possibly left: its drop starts the
    /// next pass if the gate still passes, rather than waiting for a manifest commit.
    capped: bool,
}

impl PunchPass {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        if let Some(p) = &shared.punch {
            p.passes.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            shared,
            punched: 0,
            capped: false,
        }
    }
}

impl Drop for PunchPass {
    fn drop(&mut self) {
        if let Some(p) = &self.shared.punch {
            p.finished();
            if self.capped {
                self.shared.maybe_punch();
            }
        }
    }
}

impl Task for PunchPass {
    fn run(&mut self, _deadline_nanos: u64, _waker: &TaskWaker) -> TaskPoll {
        let Some(punch) = self.shared.punch.as_ref() else {
            return TaskPoll::Done;
        };
        // The pager refuses a poisoned punch itself (M1); this only stops the pass early.
        if self.shared.closing.load(Ordering::Acquire)
            || self.shared.closed.load(Ordering::Acquire)
            || self.shared.pager_poisoned.load(Ordering::Acquire)
        {
            return TaskPoll::Done;
        }
        if self.punched >= punch.tuning.pass_bytes {
            self.capped = true;
            return TaskPoll::Done;
        }
        let simulated = self.shared.vfs.clock_is_simulated();
        let now = self.shared.clock_vfs.monotonic_nanos();
        let next_at = punch.next_at.load(Ordering::Relaxed);
        if !simulated && now < next_at {
            return TaskPoll::SleepUntil(next_at);
        }
        let reserve = punch.reserve(self.shared.pager.stats().file_bytes);
        // Held across the check and the punch: the close's release waits for it (#2181).
        let stopped = punch.stopped.lock().unwrap_or_else(PoisonError::into_inner);
        if *stopped {
            return TaskPoll::Done;
        }
        let result = self.shared.pager.punch_free(reserve);
        drop(stopped);
        match result {
            Ok(None) => {
                // Nothing (more) to punch: the gate waits for free space to grow past this.
                punch.ended(self.punched, unpunched_free(&self.shared.pager.stats()));
                TaskPoll::Done
            }
            Ok(Some(bytes)) => {
                self.punched += bytes;
                punch.idle_at.store(0, Ordering::Relaxed);
                let pause = u128::from(bytes) * 1_000_000_000
                    / u128::from(punch.tuning.bytes_per_sec.max(1));
                let next_at = now.saturating_add(u64::try_from(pause).unwrap_or(u64::MAX));
                punch.next_at.store(next_at, Ordering::Relaxed);
                if simulated {
                    TaskPoll::Pending
                } else {
                    TaskPoll::SleepUntil(next_at)
                }
            }
            Err(e) => {
                punch.refusals.fetch_add(1, Ordering::Relaxed);
                if matches!(&e, pigeonhole_pager::Error::Io(io) if io.kind == ErrorKind::Unsupported)
                {
                    punch.unsupported.store(true, Ordering::Relaxed);
                }
                // A refusal that persists (EIO, EPERM) would otherwise fail once per manifest
                // commit: wait for new free space, as after an empty pass (simp724's S1), but
                // not for ever (#2181).
                punch.refused(now, unpunched_free(&self.shared.pager.stats()));
                TaskPoll::Done
            }
        }
    }

    fn name(&self) -> &'static str {
        "punch"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// simp724 on #2033: after an empty pass the gate waits for a trigger's worth more free
    /// space than it saw; a pass that punches clears the mark.
    #[test]
    fn an_empty_pass_holds_the_gate_until_free_space_grows_by_a_trigger() {
        let p = Punch::new(PunchTuning::default(), 0, 1);
        let file = 4096 * MIB;
        let (reserve, trigger) = (64 * MIB, 64 * MIB);
        assert!(!p.gate(reserve + trigger - 1, file));
        assert!(p.gate(reserve + trigger, file));
        // A pass that found nothing at 200 MiB free: the next needs 264 MiB.
        p.ended(0, 200 * MIB);
        assert!(!p.gate(200 * MIB, file));
        assert!(!p.gate(200 * MIB + trigger - 1, file));
        assert!(p.gate(200 * MIB + trigger, file));
        // A pass that punched clears the mark: back to the reserve's gate.
        p.ended(MIB, 0);
        assert!(p.gate(reserve + trigger, file));
        // #2181: the mark follows free space down. After an empty pass at 200 MiB the
        // workload reuses 50 MiB: a trigger's worth past 150 MiB is new space.
        p.ended(0, 200 * MIB);
        assert!(!p.gate(150 * MIB, file));
        assert!(p.gate(150 * MIB + trigger, file));
        // The reserve: twice the memtable budgets, capped at a quarter of the file.
        let q = Punch::new(PunchTuning::default(), 128 * MIB, 4);
        assert_eq!(q.reserve(file), 1024 * MIB);
        assert_eq!(q.reserve(1024 * MIB), 256 * MIB);
        assert_eq!(p.reserve(file), 64 * MIB);
    }

    /// #2181 (2): one refused punch used to set the mark to all the unpunched space, so
    /// punching waited for that much more free space, perhaps until reopen. Now the mark
    /// goes after `RETRY_AFTER_NANOS`, and not before (a persistent refusal fails once
    /// per that, simp724's S1).
    #[test]
    fn a_refused_punch_holds_the_gate_only_until_the_retry_time() {
        let p = Punch::new(PunchTuning::default(), 0, 1);
        let file = 64 * 1024 * MIB;
        let free = 49 * 1024 * MIB;
        p.refused(1_000, free);
        p.retry_if_due(1_000 + RETRY_AFTER_NANOS - 1);
        assert!(!p.gate(free, file), "before the retry time");
        p.retry_if_due(1_000 + RETRY_AFTER_NANOS);
        assert!(p.gate(free, file), "after it");
        // Cleared: a later check finds nothing due.
        p.retry_if_due(u64::MAX);
        assert_eq!(p.retry_at.load(Ordering::Relaxed), 0);
    }
}
