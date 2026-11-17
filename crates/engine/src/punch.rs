//! Giving the data file's free space back to the filesystem (D333, #2013; ICR 0144).
//!
//! With [`EngineOptions::punch_freed`](crate::EngineOptions::punch_freed) on, a reclaim
//! after a manifest commit ([`Shared::reclaim`]) checks how much free space is not yet a
//! hole. Once that passes the reserve by a block's worth, it starts one [`PunchPass`] (never
//! two at once): a background task that punches one free block per slice
//! ([`Pager::punch_free`](pigeonhole_pager::Pager::punch_free)), the largest past the
//! reserve first. A pass stops after [`PunchTuning::pass_bytes`] or when nothing is left,
//! and sleeps between blocks to stay under [`PunchTuning::bytes_per_sec`] (on a real clock;
//! a simulated one punches without pacing).
//!
//! Only free space is punched: what no recoverable root and no live view names, which
//! the allocator would overwrite anyway. A filesystem that cannot punch (`Unsupported`)
//! stops punching until the next open; any other error ends the pass and counts as a
//! refusal, and the next trigger tries again.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pigeonhole_io::ErrorKind;
use pigeonhole_runtime::{Task, TaskPoll, TaskWaker};

use crate::shard::Shared;

const MIB: u64 = 1 << 20;

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
    /// Passes started.
    pub passes: AtomicU64,
    /// Punches that failed (the `Unsupported` one included).
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
        if self.unsupported.load(Ordering::Relaxed) || self.running.load(Ordering::Relaxed) {
            return false;
        }
        let s = shared.pager.stats();
        let free = s
            .file_bytes
            .saturating_sub(s.allocated_bytes)
            .saturating_sub(s.retired_bytes)
            .saturating_sub(s.punched_bytes);
        if free < self.reserve(s.file_bytes).saturating_add(self.tuning.trigger_bytes) {
            return false;
        }
        self.running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// The pass ended (or never started): the next trigger may start another.
    pub(crate) fn finished(&self) {
        self.running.store(false, Ordering::Release);
    }
}

/// One punch pass (module docs): one block per slice, paced, at most
/// [`PunchTuning::pass_bytes`]. Offloaded to a background worker when there is one.
pub(crate) struct PunchPass {
    shared: Arc<Shared>,
    punched: u64,
    /// When the next block may be punched (monotonic nanoseconds).
    next_at: u64,
}

impl PunchPass {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        if let Some(p) = &shared.punch {
            p.passes.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            shared,
            punched: 0,
            next_at: 0,
        }
    }
}

impl Drop for PunchPass {
    fn drop(&mut self) {
        if let Some(p) = &self.shared.punch {
            p.finished();
        }
    }
}

impl Task for PunchPass {
    fn run(&mut self, _deadline_nanos: u64, _waker: &TaskWaker) -> TaskPoll {
        let Some(punch) = self.shared.punch.as_ref() else {
            return TaskPoll::Done;
        };
        if self.shared.closed.load(Ordering::Acquire) || self.punched >= punch.tuning.pass_bytes
        {
            return TaskPoll::Done;
        }
        let simulated = self.shared.vfs.clock_is_simulated();
        let now = self.shared.vfs.monotonic_nanos();
        if !simulated && now < self.next_at {
            return TaskPoll::SleepUntil(self.next_at);
        }
        let reserve = punch.reserve(self.shared.pager.stats().file_bytes);
        match self.shared.pager.punch_free(reserve) {
            Ok(None) => TaskPoll::Done,
            Ok(Some(bytes)) => {
                self.punched += bytes;
                let pause = u128::from(bytes) * 1_000_000_000 / u128::from(punch.tuning.bytes_per_sec.max(1));
                self.next_at = now.saturating_add(u64::try_from(pause).unwrap_or(u64::MAX));
                if simulated {
                    TaskPoll::Pending
                } else {
                    TaskPoll::SleepUntil(self.next_at)
                }
            }
            Err(e) => {
                punch.refusals.fetch_add(1, Ordering::Relaxed);
                if matches!(&e, pigeonhole_pager::Error::Io(io) if io.kind == ErrorKind::Unsupported) {
                    punch.unsupported.store(true, Ordering::Relaxed);
                }
                TaskPoll::Done
            }
        }
    }

    fn name(&self) -> &'static str {
        "punch"
    }
}
