//! Application-owned shards on io_uring (#408): a loop that waits only in its own `poll`,
//! on the shard's completion fd ([`Shard::io_fd`]) and its own wakeup fd, with
//! [`Shard::next_wakeup`] as the timeout, completes the shard's I/O without spinning.
//! Linux only; it fails where io_uring is unavailable, so a run meant to cover it cannot
//! pass without it.
#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use pigeonhole::{Family, IoBackend, Options, Pigeonhole, Shard};

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
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    // Commits (WAL appends and syncs), a flush and a compaction: SST writes and reads, all
    // submitted on the driving thread's ring.
    for round in 0..3u32 {
        for i in 0..200u32 {
            t.mutate(format!("row{round}-{i:04}").as_bytes())
                .put("f", b"q", &[7u8; 512])
                .commit()
                .unwrap();
        }
        db.flush().unwrap();
    }
    db.compact().unwrap();
    let n = t.scan_prefix(b"row").iter().unwrap().count();
    assert_eq!(n, 600);
    drop(t);
    db.close().unwrap();
    let (sleeps, zero) = driver.join().unwrap();
    // Reopened with a cold cache, an async get from this thread (which has no ring) fetches
    // its blocks through the shared ring, which has no reaper: the driving thread's fd fires
    // for those completions and its next turn takes them.
    #[cfg(feature = "async")]
    {
        let (db, mut shards) =
            Pigeonhole::open_application_owned(dir.0.join("db.phdb"), options).unwrap();
        let driver = std::thread::spawn({
            let shard = shards.pop().unwrap();
            move || drive(shard)
        });
        let t = db.table("t").unwrap().open().unwrap();
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
        db.close().unwrap();
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
