//! Application-owned shards whose work is counted with the measured work's
//! (`writepath.rs`, `ycsb.rs`): each shard runs on a thread of its own, and while a shape
//! is measured it is driven by a thread spawned for the measurement, inside
//! `shape_run_shard`. One shard, tablet changes off and a memtable that holds every write
//! keep the work deterministic.
//!
//! (Not an example of its own: examples include it with `#[path]`.)
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use pigeonhole::{Durability, Options, Pigeonhole, Shard};

use crate::measure::Measured;

/// The shard threads of an application-owned store, measured on request.
pub struct Shards {
    drivers: Vec<thread::JoinHandle<()>>,
    ctl: Arc<Ctl>,
}

impl Shards {
    /// Opens `path` (one shard, `Buffered`, no tablet changes, a memtable budget no setup
    /// reaches, a 256 MiB block cache), each shard driven by a thread.
    pub fn open(path: &Path) -> (Pigeonhole, Shards) {
        let (db, drivers, ctl) = open(path);
        (db, Shards { drivers, ctl })
    }

    /// Runs `f`, the measured work, with the shards' side counted too.
    ///
    /// The measured work starts and ends with every shard idle: what the setup left running
    /// finishes outside `shape_run_shard`, and what the measured work leaves running (the
    /// shard's side of the last commit, say) finishes inside it, before the close.
    ///
    /// The measured work runs on threads spawned for it, the shards' side and the caller's:
    /// glibc gives a new thread its own allocator arena and cache, so its allocations do not
    /// depend on the heap the setup left, which varies with thread timing (perf287's #354
    /// found up to 3%). The setup's shard threads stay alive meanwhile, so a measured thread
    /// cannot take over one of their arenas.
    pub fn measure(&self, f: &mut (dyn FnMut() + Send)) {
        let ctl = &self.ctl;
        ctl.wait_idle();
        ctl.measuring.store(true, Ordering::Release);
        ctl.wake();
        // Every shard is with its measured thread before the measured work starts.
        while ctl.handed.load(Ordering::Acquire) < ctl.shards {
            thread::yield_now();
        }
        thread::scope(|s| s.spawn(f).join().expect("the measured thread panicked"));
        ctl.wait_idle();
        ctl.measuring.store(false, Ordering::Release);
        ctl.wake();
        while ctl.handed.load(Ordering::Acquire) > 0 {
            thread::yield_now();
        }
    }

    /// Waits for the shard threads to finish (after `Pigeonhole::close`).
    pub fn join(self) {
        for d in self.drivers {
            d.join().unwrap();
        }
    }
}

/// What the shard threads share with the main thread.
#[derive(Default)]
struct Ctl {
    /// Whether a shape is being measured: each shard is then driven, in `shape_run_shard`,
    /// by a thread spawned for the measurement.
    measuring: AtomicBool,
    /// Shard threads running (not parked).
    busy: AtomicUsize,
    /// The number of shards.
    shards: usize,
    /// Shards with their measured thread.
    handed: AtomicUsize,
    /// The threads driving the shards now, to wake when `measuring` changes.
    drivers: Mutex<Vec<thread::Thread>>,
}

impl Ctl {
    /// Wakes every thread driving a shard.
    fn wake(&self) {
        for t in self.drivers.lock().unwrap().iter() {
            t.unpark();
        }
    }

    /// Waits until every shard thread is parked: nothing left to run.
    fn wait_idle(&self) {
        while self.busy.load(Ordering::Acquire) > 0 {
            thread::yield_now();
        }
    }
}

/// Opens with application-owned shards, each driven by a thread of its own.
fn open(path: &Path) -> (Pigeonhole, Vec<thread::JoinHandle<()>>, Arc<Ctl>) {
    let (db, shards) = Pigeonhole::open_application_owned(
        path,
        Options::default()
            .shards(1)
            .tablet_changes(false)
            .durability(Durability::Buffered)
            .memtable_budget(1 << 30)
            .block_cache(256 << 20)
            // A waiting client's poll (D198) runs a number of times that depends on thread
            // timing: counted, it would make the commit shapes nondeterministic.
            .commit_spin(std::time::Duration::ZERO),
    )
    .expect("open");
    let ctl = Arc::new(Ctl {
        shards: shards.len(),
        ..Ctl::default()
    });
    let drivers = shards
        .into_iter()
        .map(|shard| {
            let ctl = Arc::clone(&ctl);
            ctl.busy.fetch_add(1, Ordering::AcqRel);
            thread::spawn(move || run(shard, &ctl))
        })
        .collect();
    (db, drivers, ctl)
}

/// A shard's thread: drives it through the setup, hands it to a thread spawned for the
/// measurement (waiting, alive, until it comes back), then drives it to the close.
fn run(mut shard: Shard, ctl: &Ctl) {
    loop {
        let Some(next) = drive(shard, ctl, false) else {
            return;
        };
        shard = next;
        let back = thread::scope(|s| {
            s.spawn(|| {
                ctl.busy.fetch_add(1, Ordering::AcqRel);
                ctl.handed.fetch_add(1, Ordering::AcqRel);
                let back = drive(shard, ctl, true);
                ctl.handed.fetch_sub(1, Ordering::AcqRel);
                back
            })
            .join()
            .expect("a measured shard thread panicked")
        });
        let Some(back) = back else {
            return;
        };
        shard = back;
        ctl.busy.fetch_add(1, Ordering::AcqRel);
    }
}

/// A shard's loop (see `Pigeonhole::open_application_owned`) on this thread, in
/// `shape_run_shard` if `measured`, until the shard closes (`None`) or `measuring` changes
/// (the shard, idle, for the next thread). `busy` counts the thread while it runs (the
/// caller counted it in).
fn drive(mut shard: Shard, ctl: &Ctl, measured: bool) -> Option<Shard> {
    let me = thread::current();
    let wake = me.clone();
    shard.set_wakeup(Box::new(move || wake.unpark()));
    ctl.drivers.lock().unwrap().push(me.clone());
    let driven = loop {
        if measured {
            shape_run_shard(&mut shard);
        } else {
            run_shard(&mut shard);
        }
        if let Some(closed) = shard.closed() {
            closed.unwrap();
            break None;
        }
        if ctl.measuring.load(Ordering::Acquire) != measured {
            break Some(shard);
        }
        ctl.busy.fetch_sub(1, Ordering::AcqRel);
        match shard.next_wakeup() {
            Some(due) => thread::park_timeout(due),
            None => thread::park(),
        }
        ctl.busy.fetch_add(1, Ordering::AcqRel);
    };
    ctl.drivers.lock().unwrap().retain(|t| t.id() != me.id());
    ctl.busy.fetch_sub(1, Ordering::AcqRel);
    driven
}

// The two loops must not compile to the same code: rustc merges identical functions, and
// then the setup's shard work would be counted as the shape's. They differ in their slice
// budget, which changes nothing measured.
#[inline(never)]
fn shape_run_shard(shard: &mut Shard) {
    let _measured = Measured::start();
    while shard.run_once(Duration::from_micros(200)) {}
}

#[inline(never)]
fn run_shard(shard: &mut Shard) {
    while shard.run_once(Duration::from_micros(1000)) {}
}
