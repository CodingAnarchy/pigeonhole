//! Deterministic stores for the measured examples (`hotrow.rs`, `readshapes.rs`): the store is
//! opened with application-owned shards, each driven by a thread of its own, so no flush,
//! compaction or tablet change runs except the ones the setup asks for, and the measurement
//! starts with every shard idle. Instruction counts then do not depend on thread timing.
//!
//! (Not an example of its own: examples include it with `#[path]`.)
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use pigeonhole::{Options, Pigeonhole, Shard};

/// The shard threads of an application-owned store.
pub struct Driver {
    busy: Arc<AtomicUsize>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Driver {
    /// Opens `path` with application-owned shards (one shard, no tablet changes, a memtable
    /// budget no setup reaches, so nothing flushes on its own), each driven by a thread.
    pub fn open(path: &Path, options: Options) -> (Pigeonhole, Driver) {
        let (db, shards) = Pigeonhole::open_application_owned(
            path,
            options
                .shards(1)
                .tablet_changes(false)
                .memtable_budget(1 << 30),
        )
        .expect("open");
        let busy = Arc::new(AtomicUsize::new(0));
        let threads = shards
            .into_iter()
            .map(|shard| {
                let busy = Arc::clone(&busy);
                busy.fetch_add(1, Ordering::AcqRel);
                thread::spawn(move || drive(shard, &busy))
            })
            .collect();
        (db, Driver { busy, threads })
    }

    /// Waits until every shard thread is parked: whatever the setup started (a flush's or a
    /// compaction's last steps, view updates) has finished.
    pub fn wait_idle(&self) {
        while self.busy.load(Ordering::Acquire) > 0 {
            thread::yield_now();
        }
    }

    /// Waits for the shard threads to finish (after `Pigeonhole::close`).
    pub fn join(self) {
        for t in self.threads {
            t.join().unwrap();
        }
    }
}

/// Runs `f` (the measured work) on a thread spawned for it. glibc gives a new thread its own
/// allocator arena and cache, so the allocations it measures do not depend on the heap the
/// setup's threads left behind, which varies with their timing (as did a realloc's copy or
/// a consolidation of free chunks, by up to 3% of a shape).
pub fn on_fresh_thread<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    thread::scope(|s| s.spawn(f).join().expect("the measured thread panicked"))
}

/// A shard's loop (see `Pigeonhole::open_application_owned`); `busy` counts it while it runs.
fn drive(mut shard: Shard, busy: &AtomicUsize) {
    let me = thread::current();
    shard.set_wakeup(Box::new(move || me.unpark()));
    loop {
        while shard.run_once(Duration::from_millis(1)) {}
        if let Some(closed) = shard.closed() {
            busy.fetch_sub(1, Ordering::AcqRel);
            closed.unwrap();
            return;
        }
        busy.fetch_sub(1, Ordering::AcqRel);
        match shard.next_wakeup() {
            Some(due) => thread::park_timeout(due),
            None => thread::park(),
        }
        busy.fetch_add(1, Ordering::AcqRel);
    }
}
