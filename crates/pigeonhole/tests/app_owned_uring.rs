//! Application-owned shards on io_uring (#408): a loop that waits only in its own `poll`,
//! on the shard's completion fd ([`Shard::io_fd`]) and its own wakeup fd, with
//! [`Shard::next_wakeup`] as the timeout, completes the shard's I/O without spinning.
//! Linux only; it fails where io_uring is unavailable, so a run meant to cover it cannot
//! pass without it.
#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

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

/// An eventfd the shard's `set_wakeup` callback signals.
struct Wakeup(i32);

impl Wakeup {
    fn new() -> Self {
        // SAFETY: plain syscall.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        assert!(fd >= 0);
        Wakeup(fd)
    }

    fn signal(fd: i32) {
        let one = 1u64;
        // SAFETY: writes 8 bytes from a live u64.
        let _ = unsafe { libc::write(fd, std::ptr::from_ref(&one).cast(), 8) };
    }

    fn drain(&self) {
        let mut n = 0u64;
        // SAFETY: reads 8 bytes into a live u64.
        let _ = unsafe { libc::read(self.0, std::ptr::from_mut(&mut n).cast(), 8) };
    }
}

impl Drop for Wakeup {
    fn drop(&mut self) {
        // SAFETY: the descriptor is ours.
        unsafe { libc::close(self.0) };
    }
}

/// Drives `shard` the way an event-loop application does: run it until idle, then sleep in
/// `poll` on its completion fd and the wakeup fd until one is readable or `next_wakeup`
/// passes. Returns the loop's sleeps and how many of them timed out at zero (a spin).
fn drive(mut shard: Shard) -> (u64, u64) {
    let wakeup = Wakeup::new();
    let w = wakeup.0;
    shard.set_wakeup(Box::new(move || Wakeup::signal(w)));
    let (mut sleeps, mut zero) = (0u64, 0u64);
    loop {
        while shard.run_once(Duration::from_micros(500)) {}
        // Until the close the client thread runs has finished.
        if shard.closed().is_some() {
            break;
        }
        let timeout = shard
            .next_wakeup()
            .unwrap_or(Duration::from_millis(100))
            .min(Duration::from_millis(100));
        if timeout.is_zero() {
            zero += 1;
        }
        let mut fds = vec![libc::pollfd {
            fd: wakeup.0,
            events: libc::POLLIN,
            revents: 0,
        }];
        if let Some(fd) = shard.io_fd() {
            fds.push(libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            });
        }
        // SAFETY: a live array of pollfds.
        unsafe {
            libc::poll(
                fds.as_mut_ptr(),
                fds.len() as libc::nfds_t,
                timeout.as_millis() as i32,
            )
        };
        wakeup.drain();
        sleeps += 1;
    }
    (sleeps, zero)
}

/// The threads of this process the library named (`pigeonhole-*`: shard, compaction, pool
/// and reaper threads). `/proc` keeps the first 15 bytes of a name.
fn library_threads() -> Vec<String> {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
        .map(|name| name.trim().to_owned())
        .filter(|name| name.starts_with("pigeonhole"))
        .collect()
}

#[test]
fn an_event_loop_waiting_on_the_completion_fd_completes_the_shards_io() {
    let _watchdog = Watchdog::new("an_event_loop_waiting_on_the_completion_fd", 60);
    let dir = TempDir::new("loop");
    let options = Options::default()
        .shards(1)
        .memtable_budget(4 << 20)
        .io_backend(IoBackend::Uring);
    let (db, mut shards) =
        Pigeonhole::open_application_owned(dir.0.join("db.phdb"), options.clone())
            .expect("io_uring");
    // The engine starts no threads in application-owned mode (#408): not even io_uring's
    // reaper.
    assert_eq!(library_threads(), Vec::<String>::new());
    let shard = shards.pop().unwrap();
    let driver = std::thread::spawn(move || drive(shard));
    let started = Instant::now();
    step("create table");
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    // Commits (WAL appends and syncs), a flush and a compaction: SST writes and reads, all
    // submitted on the driving thread's ring.
    for round in 0..3u32 {
        step("commits");
        for i in 0..200u32 {
            t.mutate(format!("row{round}-{i:04}").as_bytes())
                .put("f", b"q", &[7u8; 512])
                .commit()
                .unwrap();
        }
        step("flush");
        db.flush().unwrap();
    }
    step("compact");
    db.compact().unwrap();
    step("scan");
    let n = t.scan_prefix(b"row").iter().unwrap().count();
    assert_eq!(n, 600);
    drop(t);
    step("close");
    db.close().unwrap();
    step("join driver");
    let (sleeps, zero) = driver.join().unwrap();
    // Reopened with a cold cache, an async get from this thread (which has no ring) fetches
    // its blocks through the shared ring, which has no reaper: the driving thread's fd fires
    // for those completions and its next turn takes them.
    #[cfg(feature = "async")]
    {
        step("reopen");
        let (db, mut shards) =
            Pigeonhole::open_application_owned(dir.0.join("db.phdb"), options).unwrap();
        let driver = std::thread::spawn({
            let shard = shards.pop().unwrap();
            move || drive(shard)
        });
        let t = db.table("t").unwrap().open().unwrap();
        step("async get");
        let cell = pigeonhole::doc_support::block_on(t.get_async(b"row1-0100", "f", b"q"))
            .unwrap()
            .expect("the row is there");
        assert_eq!(cell.value(), &[7u8; 512]);
        assert_eq!(
            db.async_sync_reads(),
            0,
            "every block was fetched asynchronously"
        );
        assert_eq!(library_threads(), Vec::<String>::new());
        drop(t);
        step("close after the async get");
        db.close().unwrap();
        step("join the second driver");
        driver.join().unwrap();
    }
    // The loop slept on its fds rather than spinning: `next_wakeup` was zero only now and
    // then (a write stall's pacing, say), not once per I/O in flight.
    assert!(
        zero * 10 <= sleeps.max(10),
        "{zero} of {sleeps} sleeps timed out at once (a spin) in {:?}",
        started.elapsed()
    );
}

/// Drives `shard` on this thread until `stop` is set, then hands it back, released.
fn drive_until(mut shard: Shard, stop: Arc<AtomicBool>) -> Shard {
    drive_until_unreleased(&mut shard, &stop);
    shard.release();
    shard
}

/// [`drive_until`] without the release.
fn drive_until_unreleased(shard: &mut Shard, stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) {
        if !shard.run_once(Duration::from_millis(1)) {
            std::thread::sleep(Duration::from_micros(200));
        }
    }
}

#[test]
fn a_shard_moved_to_another_thread_gets_a_ring_there_and_its_durable_commits_complete() {
    // The #473 scaling hang: a shard handed to a new thread kept submitting to the ring of no
    // thread (its runtime attached a ring once, on the first thread), so its group syncs
    // never completed there.
    let _watchdog = Watchdog::new("a_shard_moved_to_another_thread", 60);
    let dir = TempDir::new("moved");
    let options = Options::default()
        .shards(1)
        .memtable_budget(4 << 20)
        .io_backend(IoBackend::Uring);
    let (db, mut shards) =
        Pigeonhole::open_application_owned(dir.0.join("db.phdb"), options).expect("io_uring");
    db.set_default_durability(Durability::GroupSync);
    let mut shard = shards.pop().unwrap();
    let mut table = None;
    for phase in 0..3u32 {
        // Each phase drives the shard on a new thread, as phdb-bench's inline runner does.
        let stop = Arc::new(AtomicBool::new(false));
        let driver = std::thread::spawn({
            let stop = Arc::clone(&stop);
            move || drive_until(shard, stop)
        });
        step("create table");
        let t = table.get_or_insert_with(|| {
            db.table("t")
                .unwrap()
                .family("f", Family::default())
                .create_if_missing()
                .unwrap()
        });
        step("durable commits");
        for i in 0..50u32 {
            t.mutate(format!("row{phase}-{i:03}").as_bytes())
                .put("f", b"q", b"v")
                .commit()
                .unwrap();
        }
        stop.store(true, Ordering::Release);
        step("hand the shard back");
        shard = driver.join().unwrap();
    }
    let driver = std::thread::spawn(move || drive(shard));
    drop(table);
    step("close");
    db.close().unwrap();
    step("join the last driver");
    driver.join().unwrap();
}

#[test]
fn a_released_shard_moves_off_a_thread_that_stays_alive_and_its_io_completes() {
    // #492: the old thread stays alive but stops running turns, which strands any I/O still
    // on its ring. `release()` drains it first, so the shard's commits and its close complete
    // on the new thread.
    let _watchdog = Watchdog::new("a_released_shard_moves_off_a_live_thread", 60);
    let dir = TempDir::new("released");
    let options = Options::default()
        .shards(1)
        .memtable_budget(4 << 20)
        .io_backend(IoBackend::Uring);
    let (db, mut shards) =
        Pigeonhole::open_application_owned(dir.0.join("db.phdb"), options).expect("io_uring");
    db.set_default_durability(Durability::GroupSync);
    let shard = shards.pop().unwrap();
    let (hand_over, take) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let stop = Arc::new(AtomicBool::new(false));
    // Thread A drives, then releases and hands the shard over, then stays alive (blocked)
    // until the end of the test.
    let a = std::thread::spawn({
        let stop = Arc::clone(&stop);
        move || {
            let shard = drive_until(shard, stop);
            hand_over.send(shard).unwrap();
            let _ = release_rx.recv();
        }
    });
    step("create table and commit on thread A");
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    for i in 0..20u32 {
        t.mutate(format!("a{i:03}").as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    stop.store(true, Ordering::Release);
    let shard = take.recv().unwrap();
    step("commit and close on thread B, A still alive");
    let b = std::thread::spawn(move || drive(shard));
    for i in 0..20u32 {
        t.mutate(format!("b{i:03}").as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    drop(t);
    db.close().unwrap();
    b.join().unwrap();
    release_tx.send(()).unwrap();
    a.join().unwrap();
}

#[cfg(feature = "async")]
#[test]
fn release_drains_a_sync_in_flight_on_the_old_threads_ring_before_the_shard_moves() {
    // #492, with I/O actually left behind: a durable commit's sync is in flight on this
    // thread's ring when the shard moves. This thread stays alive and runs no more turns, so
    // without `release()` nothing reaps that sync and the commit never resolves on thread B.
    let _watchdog = Watchdog::new("release_drains_a_sync_in_flight", 60);
    let dir = TempDir::new("release-drains");
    let options = Options::default()
        .shards(1)
        .memtable_budget(4 << 20)
        .io_backend(IoBackend::Uring);
    let (db, mut shards) =
        Pigeonhole::open_application_owned(dir.0.join("db.phdb"), options).expect("io_uring");
    db.set_default_durability(Durability::GroupSync);
    let mut shard = shards.pop().unwrap();
    let t = {
        let stop = Arc::new(AtomicBool::new(false));
        let driver = std::thread::spawn({
            let stop = Arc::clone(&stop);
            move || drive_until(shard, stop)
        });
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        stop.store(true, Ordering::Release);
        shard = driver.join().unwrap();
        t
    };
    step("leave a sync in flight on this thread's ring");
    let pending = t.mutate(b"r").put("f", b"q", b"v").commit_async();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !pigeonhole_io::own_io_in_flight() {
        shard.run_once(Duration::from_millis(1));
    }
    assert!(
        pigeonhole_io::own_io_in_flight(),
        "no I/O in flight to leave behind"
    );
    shard.release();
    step("thread B resolves the commit, this thread alive and idle");
    let (done_tx, done) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let r = pigeonhole::doc_support::block_on(pending);
        let _ = done_tx.send(r.is_ok());
    });
    let b = std::thread::spawn(move || drive(shard));
    assert_eq!(
        done.recv_timeout(Duration::from_secs(10)),
        Ok(true),
        "the commit whose sync was in flight at the move did not resolve on the new thread"
    );
    waiter.join().unwrap();
    step("close on thread B");
    drop(t);
    db.close().unwrap();
    b.join().unwrap();
}

#[cfg(all(debug_assertions, feature = "async"))]
#[test]
fn debug_builds_reject_a_shard_moved_with_its_io_left_on_the_old_thread() {
    // #492's detector: the old thread's last turn left I/O on its ring and nothing released
    // it; the new thread's first turn panics instead of the shard hanging later.
    let _watchdog = Watchdog::new("debug_builds_reject_an_unreleased_move", 60);
    let dir = TempDir::new("unreleased");
    let options = Options::default()
        .shards(1)
        .memtable_budget(4 << 20)
        .io_backend(IoBackend::Uring);
    let (db, mut shards) =
        Pigeonhole::open_application_owned(dir.0.join("db.phdb"), options).expect("io_uring");
    db.set_default_durability(Durability::GroupSync);
    let mut shard = shards.pop().unwrap();
    let t = {
        let stop = Arc::new(AtomicBool::new(false));
        let driver = std::thread::spawn({
            let stop = Arc::clone(&stop);
            move || {
                drive_until_unreleased(&mut shard, &stop);
                shard
            }
        });
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        stop.store(true, Ordering::Release);
        shard = driver.join().unwrap();
        t
    };
    // A durable commit queued, then one turn on this thread submits its sync to this
    // thread's ring and leaves it in flight; the shard then moves, unreleased.
    let pending = t.mutate(b"r").put("f", b"q", b"v").commit_async();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        shard.run_once(Duration::from_millis(1));
        if pigeonhole_io::own_io_in_flight() {
            break;
        }
    }
    assert!(
        pigeonhole_io::own_io_in_flight(),
        "no I/O in flight to leave behind"
    );
    // Borrowed by the other thread, so its panic does not drop the shard there (whose final
    // sync would then wait on this thread's ring).
    let moved = std::thread::scope(|s| {
        s.spawn(|| {
            shard.run_once(Duration::from_millis(1));
        })
        .join()
    });
    let message = moved
        .expect_err("the unreleased move was accepted")
        .downcast::<String>()
        .map(|s| *s)
        .unwrap_or_default();
    assert!(message.contains("#492"), "{message}");
    // This thread still owns the ring: it finishes the commit and the close. One turn first
    // makes it the shard's driver again (the other thread's turn took that before it
    // panicked), so `close` returns at once instead of waiting for a driver.
    step("close on the old thread");
    shard.run_once(Duration::from_millis(1));
    drop(t);
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(Duration::from_millis(1));
    }
    drop(pending);
}
