//! #929: application-owned shards on io_uring when no driving thread gets a ring of its own
//! (the kernel refused every one: `ENOMEM` against the locked-memory limit, or a kernel
//! without the support). Their I/O runs inline (as the pread backend's does), so durable
//! commits, cross-shard commits and a flush's durability barrier all complete; before #929
//! it went to the shared ring, which nothing reaps in this mode, and the shards livelocked.
//! A test binary of its own: the hook that fails the attaches is process-wide. Linux only.
#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pigeonhole::{Durability, Family, IoBackend, Options, Pigeonhole, Shard};

/// Fails the test binary instead of hanging it (#443): aborts the process once `secs` pass
/// without the guard being dropped, naming `what` and the last step reached.
/// Dropping it (the sender) disarms the watchdog.
struct Watchdog(#[allow(dead_code)] std::sync::mpsc::Sender<()>);

static STEP: std::sync::Mutex<&'static str> = std::sync::Mutex::new("start");

/// Records the step the test is on (printed if the watchdog fires).
fn step(what: &'static str) {
    *STEP.lock().unwrap() = what;
    eprintln!("step: {what}");
}

impl Watchdog {
    fn new(what: &'static str, secs: u64) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            if rx.recv_timeout(Duration::from_secs(secs))
                == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            {
                eprintln!(
                    "{what}: still running after {secs} s at step '{}': aborting",
                    STEP.lock().unwrap()
                );
                std::process::abort();
            }
        });
        Watchdog(tx)
    }
}

/// A temporary directory, removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("phdb-app-uring-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn drive_until(mut shard: Shard, stop: Arc<AtomicBool>) -> Shard {
    while !stop.load(Ordering::Acquire) {
        if !shard.run_once(Duration::from_millis(1)) {
            std::thread::sleep(Duration::from_micros(200));
        }
    }
    shard.release();
    shard
}

#[test]
fn with_no_driving_thread_ring_commits_and_flush_barriers_complete() {
    let _watchdog = Watchdog::new("no driving thread ring", 120);
    pigeonhole_io::uring::fail_ring_attaches(usize::MAX);
    let dir = TempDir::new("no-rings");
    let options = Options::default()
        .shards(2)
        .memtable_budget(4 << 20)
        .io_backend(IoBackend::Uring);
    let (db, shards) =
        Pigeonhole::open_application_owned(dir.0.join("db.phdb"), options).expect("io_uring");
    db.set_default_durability(Durability::GroupSync);
    let mut shards = shards;
    let mut table = None;
    // Two phases, each on new driving threads, as phdb-bench's inline runner does.
    for phase in 0..2u32 {
        let stop = Arc::new(AtomicBool::new(false));
        let drivers: Vec<_> = shards
            .drain(..)
            .map(|shard| {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || drive_until(shard, stop))
            })
            .collect();
        step("create and split the table");
        let t = table.get_or_insert_with(|| {
            let t = db
                .table("t")
                .unwrap()
                .family("f", Family::default())
                .create_if_missing()
                .unwrap();
            t.split_over_shards(vec![b"m".to_vec()]).unwrap();
            t
        });
        step("durable commits");
        for i in 0..50u32 {
            for side in ["a", "z"] {
                t.mutate(format!("{side}{phase}-{i:03}").as_bytes())
                    .put("f", b"q", b"v")
                    .commit()
                    .unwrap();
            }
        }
        step("cross-shard transactions");
        for i in 0..20u32 {
            let mut txn = db.transaction().unwrap();
            txn.put(t, format!("a-x{phase}-{i}").as_bytes(), "f", b"q", b"1");
            txn.put(t, format!("z-x{phase}-{i}").as_bytes(), "f", b"q", b"1");
            txn.commit().unwrap();
        }
        step("flush (the durability barrier over both shards)");
        db.flush().unwrap();
        stop.store(true, Ordering::Release);
        step("hand the shards back");
        shards = drivers.into_iter().map(|d| d.join().unwrap()).collect();
    }
    let rings = db.io_rings().expect("an io_uring backend");
    assert_eq!(rings.rings, 1, "only the shared ring: {rings:?}");
    assert!(rings.attach_failures > 0, "{rings:?}");
    assert!(rings.inline > 0, "{rings:?}");
    let t = table.take().unwrap();
    assert!(t.get(b"z1-049", "f", b"q").unwrap().is_some());
    drop(t);
    step("close");
    let stop = Arc::new(AtomicBool::new(false));
    let drivers: Vec<_> = shards
        .into_iter()
        .map(|mut shard| {
            std::thread::spawn(move || {
                while shard.closed().is_none() {
                    if !shard.run_once(Duration::from_millis(1)) {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                }
            })
        })
        .collect();
    db.close().unwrap();
    stop.store(true, Ordering::Release);
    for d in drivers {
        d.join().unwrap();
    }
    pigeonhole_io::uring::fail_ring_attaches(0);
}
