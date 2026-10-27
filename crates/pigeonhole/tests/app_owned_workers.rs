//! Application-owned background workers through the public API (D284, ICR 0078):
//! `open_application_owned_with`, `AppOwned::spawn_background_threads`, a worker the
//! application never drives, and the options the other opens refuse or ignore.

use std::thread::{self, JoinHandle};
use std::time::Duration;

use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, Shard};

/// A temporary directory, removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("phdb-app-workers-{}-{name}", std::process::id()));
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

/// Fails the test binary instead of hanging it: aborts once `secs` pass without the guard
/// being dropped.
struct Watchdog(#[allow(dead_code)] std::sync::mpsc::Sender<()>);

impl Watchdog {
    fn new(what: &'static str, secs: u64) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        thread::spawn(move || {
            if rx.recv_timeout(Duration::from_secs(secs))
                == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            {
                eprintln!("{what}: still running after {secs} s: aborting");
                std::process::abort();
            }
        });
        Watchdog(tx)
    }
}

/// Drives `shard` on a thread of its own until the close finishes, as the guide's loop does.
fn drive(mut shard: Shard) -> JoinHandle<pigeonhole::Result<()>> {
    thread::spawn(move || {
        let me = thread::current();
        shard.set_wakeup(Box::new(move || me.unpark()));
        loop {
            while shard.run_once(Duration::from_micros(200)) {}
            if let Some(closed) = shard.closed() {
                return closed;
            }
            match shard.next_wakeup() {
                Some(due) => thread::park_timeout(due),
                None => thread::park(),
            }
        }
    })
}

#[test]
fn flushes_run_on_spawned_worker_threads_and_the_close_ends_them() {
    let _watchdog = Watchdog::new("spawned workers", 120);
    let dir = TempDir::new("spawned");
    let options = Options::default().shards(2).background_workers(2);
    let mut owned =
        Pigeonhole::open_application_owned_with(dir.0.join("db.phdb"), options).unwrap();
    assert_eq!(owned.shards.len(), 2);
    assert_eq!(owned.workers.len(), 2);
    let workers = owned.spawn_background_threads();
    assert!(owned.workers.is_empty(), "the threads took the workers");
    let shards: Vec<_> = owned.shards.drain(..).map(drive).collect();
    let db = owned.db;

    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    for round in 0..3u32 {
        for i in 0..200u32 {
            let row = (round * 1000 + i).to_be_bytes();
            t.mutate(&row).put("f", b"q", &[7; 64]).commit().unwrap();
        }
        db.flush().unwrap();
    }
    let m = db.engine_metrics();
    assert!(m.flushes >= 3, "every round flushed: {}", m.flushes);
    assert_eq!(
        (
            m.background_fallback_take_backs,
            m.background_room_take_backs
        ),
        (0, 0),
        "the workers are driven, so the shards took nothing back"
    );
    assert_eq!(
        t.get(&5u32.to_be_bytes(), "f", b"q")
            .unwrap()
            .unwrap()
            .value(),
        &[7; 64]
    );

    drop(t);
    db.close().unwrap();
    for shard in shards {
        shard.join().unwrap().unwrap();
    }
    // The close ended the workers' loops.
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn a_worker_nobody_drives_delays_its_flush_until_the_shard_takes_it_back() {
    let _watchdog = Watchdog::new("undriven worker", 120);
    let dir = TempDir::new("undriven");
    let options = Options::default().shards(1).background_workers(1);
    let mut owned =
        Pigeonhole::open_application_owned_with(dir.0.join("db.phdb"), options).unwrap();
    // Kept, never driven.
    let worker = owned.workers.pop().unwrap();
    let shards: Vec<_> = owned.shards.drain(..).map(drive).collect();
    let db = owned.db;

    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    t.mutate(b"row").put("f", b"q", b"v").commit().unwrap();
    // Queued for the worker; the shard takes it back once no worker looked for 10 ms.
    db.flush().unwrap();
    let m = db.engine_metrics();
    assert!(m.flushes >= 1);
    assert!(
        m.background_fallback_take_backs >= 1,
        "the shard took the flush back: {m:?}"
    );

    drop(t);
    assert!(!worker.closed());
    // The close does not wait for the worker to be driven.
    db.close().unwrap();
    assert!(worker.closed(), "the close took the worker's tasks back");
    for shard in shards {
        shard.join().unwrap().unwrap();
    }
}

#[test]
fn a_worker_driven_by_hand_runs_until_the_close() {
    let _watchdog = Watchdog::new("hand-driven worker", 120);
    let dir = TempDir::new("byhand");
    let options = Options::default().shards(1).background_workers(1);
    let mut owned =
        Pigeonhole::open_application_owned_with(dir.0.join("db.phdb"), options).unwrap();
    let mut worker = owned.workers.pop().unwrap();
    let driver = thread::spawn(move || {
        let me = thread::current();
        worker.set_wakeup(Box::new(move || me.unpark()));
        let mut turns = 0u64;
        while !worker.closed() {
            while worker.run_once(Duration::from_millis(1)) {
                turns += 1;
            }
            match worker.next_wakeup() {
                Some(due) => thread::park_timeout(due.min(Duration::from_millis(100))),
                None => thread::park_timeout(Duration::from_millis(100)),
            }
        }
        turns
    });
    let shards: Vec<_> = owned.shards.drain(..).map(drive).collect();
    let db = owned.db;
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    t.mutate(b"row").put("f", b"q", b"v").commit().unwrap();
    db.flush().unwrap();
    assert_eq!(db.engine_metrics().background_fallback_take_backs, 0);
    drop(t);
    db.close().unwrap();
    driver.join().unwrap();
    for shard in shards {
        shard.join().unwrap().unwrap();
    }
}

#[test]
fn the_tuple_open_refuses_workers_and_the_engine_owned_open_ignores_them() {
    let dir = TempDir::new("refuse");
    let options = Options::default().shards(1).background_workers(1);
    let Err(err) = Pigeonhole::open_application_owned(dir.0.join("a.phdb"), options.clone()) else {
        panic!("refused");
    };
    assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");

    let db = Pigeonhole::open(dir.0.join("b.phdb"), options).unwrap();
    db.close().unwrap();

    // Zero workers: the new open behaves as the tuple form.
    let owned =
        Pigeonhole::open_application_owned_with(dir.0.join("c.phdb"), Options::default().shards(1))
            .unwrap();
    assert!(owned.workers.is_empty());
    assert_eq!(owned.shards.len(), 1);
    let shards: Vec<_> = owned.shards.into_iter().map(drive).collect();
    owned.db.close().unwrap();
    for shard in shards {
        shard.join().unwrap().unwrap();
    }
}
