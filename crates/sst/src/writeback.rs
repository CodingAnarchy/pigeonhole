//! Write-behind for background output (D319, #1621): the bytes of SST and blob output whose
//! write-back to the device is in flight, under one engine-wide budget.
//!
//! A flush or compaction writes its output to the page cache in 1 MiB chunks
//! ([`crate::SstWriter`], [`crate::BlobWriter`]). Without write-behind, nothing writes those
//! pages back until the kernel's flusher or the commit's `sync_data` does, and then tens of
//! MiB reach the device at once: every WAL sync issued then waits behind them (16–63 ms
//! fsyncs at 55K puts/s, #1621). With it, each chunk's write-back starts as soon as the chunk
//! is written ([`File::submit_writeback`]), and a job checks [`Writeback::admit`] at its slice
//! checkpoints, ending its slice blocked when the bytes in flight reach the budget, so the
//! device's queue of background writes stays short.
//!
//! Pacing only: write-back makes nothing durable, and a failed one only loses the pacing
//! (the commit's sync still writes and checks the data).
//!
//! **Flush first.** A flush frees arena room the writers wait on, so it may use the whole
//! budget; a compaction stops at [`COMPACTION_SHARE`] of it, leaving the rest for flushes.
//!
//! **In flight past a slice.** A completion releases its bytes on whichever thread resolves
//! it. An engine closing waits for [`Writeback::wait_idle`] once its threads are joined, so
//! no write-back of a closed instance is left on the file it hands over. Inside an
//! [`pigeonhole_io::inline_io`] scope (an application-owned worker's slice) a write-back runs
//! before [`Writeback::submit`] returns, so a slice still ends with nothing in flight.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, PoisonError};
use std::task::Waker;

use pigeonhole_io::File;

/// The default budget: background write-back bytes in flight, engine-wide (D319). About
/// 2–3 ms of device work at NVMe speed, so a WAL sync queues behind at most that.
pub const DEFAULT_BUDGET: u64 = 4 << 20;

/// The share of the budget a compaction may use, in quarters: the last quarter stays for
/// flushes (D319, perf287 on #1621).
pub const COMPACTION_SHARE: u64 = 3;

/// The engine-wide write-back budget (D319). Shared by every shard's flush and compaction.
#[derive(Debug)]
pub struct Writeback {
    budget: u64,
    /// Bytes whose write-back was submitted and hasn't completed.
    in_flight: AtomicU64,
    /// The most bytes in flight at once (for `Metrics`).
    peak: AtomicU64,
    /// Times a job found the budget full and ended its slice (for `Metrics`).
    waits: AtomicU64,
    /// Jobs waiting for room: woken, all, by the release that drops the bytes in flight
    /// below the budget. Taken under this lock by both sides, so no wake is lost: `admit`
    /// rechecks under it before it registers.
    waiters: Mutex<Vec<Waker>>,
    /// Signalled when the bytes in flight reach zero ([`Writeback::wait_idle`]).
    idle: Condvar,
    /// Threads that write nothing back ([`Writeback::skip_on`]).
    skip: Option<fn() -> bool>,
}

impl Writeback {
    /// A budget of `budget` bytes in flight.
    pub fn new(budget: u64) -> Self {
        Self {
            budget: budget.max(1),
            in_flight: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            waits: AtomicU64::new(0),
            waiters: Mutex::new(Vec::new()),
            idle: Condvar::new(),
            skip: None,
        }
    }

    /// Writes nothing back on a thread where `skip` is true: the engine's pinned shard
    /// threads, so a job running there never starts io-wq workers on a shard's CPU (D319,
    /// #1460). Its chunks still count nothing, and its jobs still wait for room.
    #[must_use]
    pub fn skip_on(mut self, skip: fn() -> bool) -> Self {
        self.skip = Some(skip);
        self
    }

    /// Starts write-back of `[offset, offset + len)` of `file`, counted in flight until it
    /// completes. It never waits for room: a writer submits each chunk it wrote, and its job
    /// checks [`Writeback::admit`] between chunks. The result is ignored (pacing only).
    pub fn submit(self: &std::sync::Arc<Self>, file: &dyn File, offset: u64, len: u64) {
        if len == 0 || self.skip.is_some_and(|skip| skip()) {
            return;
        }
        let now = self.in_flight.fetch_add(len, Ordering::AcqRel) + len;
        self.peak.fetch_max(now, Ordering::Relaxed);
        let me = std::sync::Arc::clone(self);
        // Runs on the resolving thread, or here when it's already done (inline I/O).
        drop(file.submit_writeback(offset, len).map(move |_| {
            me.release(len);
            Ok(())
        }));
    }

    fn release(&self, len: u64) {
        let now = self.in_flight.fetch_sub(len, Ordering::AcqRel) - len;
        if now < self.budget {
            let woken = std::mem::take(&mut *lock(&self.waiters));
            for w in woken {
                w.wake();
            }
        }
        if now == 0 {
            // Under the lock `wait_idle` checks under, so its wait can't miss this.
            let _g = lock(&self.waiters);
            self.idle.notify_all();
        }
    }

    /// Whether a job may write its next chunk now: below the budget for a flush, below
    /// [`COMPACTION_SHARE`] quarters of it for a compaction. When not, `waker` is woken once
    /// write-backs complete, and the job ends its slice blocked (never waiting on its thread).
    pub fn admit(&self, flush: bool, waker: &Waker) -> bool {
        let limit = self.limit(flush);
        if self.in_flight.load(Ordering::Acquire) < limit {
            return true;
        }
        let mut waiters = lock(&self.waiters);
        // Rechecked under the lock every waking release takes: a release before this
        // finds no waker but leaves room this sees.
        if self.in_flight.load(Ordering::Acquire) < limit {
            return true;
        }
        if !waiters.iter().any(|w| w.will_wake(waker)) {
            waiters.push(waker.clone());
        }
        self.waits.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Whether the budget is full for a flush (`true`) or a compaction: a cheap check a job
    /// makes between chunks, ending its slice; [`Writeback::admit`] then registers its wait.
    pub fn full(&self, flush: bool) -> bool {
        self.in_flight.load(Ordering::Relaxed) >= self.limit(flush)
    }

    fn limit(&self, flush: bool) -> u64 {
        if flush {
            self.budget
        } else {
            (self.budget / 4 * COMPACTION_SHARE).max(1)
        }
    }

    /// Waits until no write-back is in flight: an engine closing calls it after its threads
    /// are joined (D319), as it waits for the WAL's submitted I/O (#824). Like that wait, it
    /// makes progress meanwhile on I/O only this thread would complete: its own ring's, or
    /// the rings nobody reaps (application-owned io_uring, the simulator's deferred device).
    pub fn wait_idle(&self) {
        const SLICE: std::time::Duration = std::time::Duration::from_millis(1);
        let mut g = lock(&self.waiters);
        while self.in_flight.load(Ordering::Acquire) > 0 {
            drop(g);
            let reaped = if pigeonhole_io::own_io_in_flight() {
                pigeonhole_io::reap_own_io(Some(SLICE));
                true
            } else {
                pigeonhole_io::reap_orphan_io(SLICE).is_some()
            };
            g = lock(&self.waiters);
            if reaped {
                continue;
            }
            g = self
                .idle
                .wait_timeout(g, SLICE)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Bytes in flight now.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// The most bytes that were in flight at once.
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Times a job found the budget full.
    pub fn waits(&self) -> u64 {
        self.waits.load(Ordering::Relaxed)
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_io::sim::SimVfs;
    use pigeonhole_io::{OpenOptions, Vfs};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;

    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_full_budget_refuses_compaction_first_and_wakes_on_release() {
        let vfs = SimVfs::new(7);
        vfs.set_deferred_io(true);
        let f = vfs
            .open(Path::new("/db/f"), OpenOptions::read_write_create())
            .unwrap();
        let wb = Arc::new(Writeback::new(4 << 20));
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        for i in 0..3 {
            wb.submit(&*f, i << 20, 1 << 20);
        }
        assert_eq!(wb.in_flight(), 3 << 20);
        // 3 MiB in flight: a compaction is at its share, a flush still has room.
        assert!(!wb.admit(false, &waker));
        assert!(wb.admit(true, &waker));
        wb.submit(&*f, 3 << 20, 1 << 20);
        assert!(!wb.admit(true, &waker));
        assert_eq!((wb.peak(), wb.waits()), (4 << 20, 2));
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        // One completes: below the budget, so the (deduplicated) waiter is woken once.
        assert!(vfs.complete_io());
        assert_eq!(wb.in_flight(), 3 << 20);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        vfs.complete_all_io();
        assert_eq!(wb.in_flight(), 0);
        wb.wait_idle();
    }

    #[test]
    fn inline_io_releases_before_submit_returns() {
        let vfs = SimVfs::new(7);
        let f = vfs
            .open(Path::new("/db/f"), OpenOptions::read_write_create())
            .unwrap();
        let wb = Arc::new(Writeback::new(1 << 20));
        wb.submit(&*f, 0, 8 << 20);
        assert_eq!((wb.in_flight(), wb.peak()), (0, 8 << 20));
    }
}
