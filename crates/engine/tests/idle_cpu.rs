//! Issue #89: a shard waiting out a failed compaction's backoff on a real clock parks
//! instead of spinning, and the retry still fires on time.
//!
//! Its own test binary, so no other test's threads add to the process's CPU time.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Engine, EngineOptions, FamilyOptions, PickerOptions, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{Vfs, VfsRef};

const DB: &str = "/db/idle.phdb";

/// `SimVfs` storage on a real clock, whose main-file data-block reads fail (so every
/// compaction that rewrites its inputs fails) and are logged with the time they happened.
#[derive(Debug)]
struct FailingReads {
    inner: Arc<SimVfs>,
    start: Instant,
    fail: Arc<AtomicBool>,
    failed: FailureLog,
}

/// When each injected read failure happened.
type FailureLog = Arc<Mutex<Vec<Instant>>>;

#[derive(Debug)]
struct FailingFile {
    inner: pigeonhole_io::FileRef,
    fail: Option<(Arc<AtomicBool>, FailureLog)>,
}

impl FailingFile {
    fn check(&self, len: usize) -> pigeonhole_io::Result<()> {
        match &self.fail {
            Some((fail, log)) if fail.load(Ordering::Acquire) && len >= 1024 => {
                log.lock().unwrap().push(Instant::now());
                Err(pigeonhole_io::Error::new(
                    pigeonhole_io::ErrorKind::Other,
                    "injected read failure",
                ))
            }
            _ => Ok(()),
        }
    }
}

impl Vfs for FailingReads {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<pigeonhole_io::FileRef> {
        let inner = self.inner.open(path, opts)?;
        Ok(Arc::new(FailingFile {
            inner,
            fail: (path == Path::new(DB))
                .then(|| (Arc::clone(&self.fail), Arc::clone(&self.failed))),
        }))
    }
    fn remove(&self, path: &Path) -> pigeonhole_io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> pigeonhole_io::Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> pigeonhole_io::Result<Vec<std::path::PathBuf>> {
        self.inner.list_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> pigeonhole_io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: pigeonhole_io::SharedOpen,
    ) -> pigeonhole_io::Result<pigeonhole_io::SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> pigeonhole_io::Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros() + self.start.elapsed().as_micros() as u64
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos() + self.start.elapsed().as_nanos() as u64
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}

impl pigeonhole_io::File for FailingFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.check(buf.len())?;
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> pigeonhole_io::Completion {
        match self.check(buf.len()) {
            Ok(()) => self.inner.submit_read(buf, offset),
            Err(e) => pigeonhole_io::Completion::ready(Err(e)),
        }
    }
    fn submit_write(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> pigeonhole_io::Completion {
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> pigeonhole_io::Result<()> {
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> pigeonhole_io::Completion<()> {
        self.inner.submit_sync_data()
    }
    fn sync_all(&self) -> pigeonhole_io::Result<()> {
        self.inner.sync_all()
    }
    fn len(&self) -> pigeonhole_io::Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> pigeonhole_io::Result<()> {
        self.inner.set_len(len)
    }
    fn allocate(&self, offset: u64, len: u64) -> pigeonhole_io::Result<()> {
        self.inner.allocate(offset, len)
    }
    fn lock(&self, byte: u64, mode: pigeonhole_io::LockMode) -> pigeonhole_io::Result<()> {
        self.inner.lock(byte, mode)
    }
    fn unlock(&self, byte: u64) -> pigeonhole_io::Result<()> {
        self.inner.unlock(byte)
    }
    fn identity(&self) -> pigeonhole_io::Result<pigeonhole_io::FileIdentity> {
        self.inner.identity()
    }
    fn is_local(&self) -> pigeonhole_io::Result<bool> {
        self.inner.is_local()
    }
}

/// This process's user plus system CPU time.
#[cfg(target_os = "linux")]
fn process_cpu() -> Duration {
    // Fields 14 and 15 of /proc/self/stat (after the parenthesized command name), in clock
    // ticks of 1/100 s.
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let rest = &stat[stat.rfind(')').unwrap() + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let ticks: u64 = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    Duration::from_millis(ticks * 10)
}

/// This process's user plus system CPU time.
#[cfg(target_os = "macos")]
fn process_cpu() -> Duration {
    // `ps` prints `[[hh:]mm:]ss.cc`.
    let out = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    let mut secs = 0.0f64;
    for part in text.trim().split(':') {
        secs = secs * 60.0 + part.parse::<f64>().unwrap();
    }
    Duration::from_secs_f64(secs)
}

/// Splits failed reads into compaction attempts (reads less than 300 ms apart belong to one).
fn attempts(log: &[Instant]) -> Vec<Instant> {
    let mut out: Vec<Instant> = Vec::new();
    for &t in log {
        if out
            .last()
            .is_none_or(|&l| t.duration_since(l) > Duration::from_millis(300))
        {
            out.push(t);
        }
    }
    out
}

#[test]
fn a_shard_in_compaction_backoff_parks_and_retries_on_time() {
    let fail = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(Mutex::new(Vec::new()));
    let vfs: VfsRef = Arc::new(FailingReads {
        inner: SimVfs::new(89),
        start: Instant::now(),
        fail: Arc::clone(&fail),
        failed: Arc::clone(&failed),
    });
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 1 << 20;
    o.memtable_freeze_bytes = 8 << 10;
    o.block_cache_bytes = 0;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    let mut c = PickerOptions::default();
    c.l0_trigger = 2;
    c.level_base_bytes = 48 << 10;
    c.level_multiplier = 2;
    c.max_levels = 4;
    c.target_sst_bytes = 64 << 10;
    o.compaction = c;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    // #232: a FIFO-by-time family whose one SST expires in an hour arms the shard's retry
    // timer for then. The backoff retries below must still fire on time (the timer keeps the
    // earliest deadline), and the parked shard must not wake for the expiry.
    let fifo = FamilyOptions::default()
        .compaction(pigeonhole_format::manifest::CompactionStyle::FifoByTime)
        .ttl_micros(3_600_000_000);
    let ft = db.create_table("fifo", &[("f".into(), fifo)]).unwrap();
    let mut wb = WriteBatch::new();
    wb.put(
        ft.id,
        ft.families[0].id,
        b"r",
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    db.commit(wb, Some(Durability::None)).unwrap();
    db.flush().unwrap();
    fail.store(true, Ordering::Release);
    // Repeating rows with incompressible values: every compaction past the first rewrites
    // (and reads) its inputs, and fails.
    let mut i = 0u32;
    while attempts(&failed.lock().unwrap()).is_empty() {
        assert!(i < 20_000, "no compaction ever failed: {:?}", db.metrics());
        let mut x = u64::from(i).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let value: Vec<u8> = (0..2048)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let mut wb = WriteBatch::new();
        let row = format!("row{:05}", i % 97);
        wb.put(t.id, f, row.as_bytes(), b"q", None, ValueRef::Bytes(&value))
            .unwrap();
        db.commit(wb, Some(Durability::None)).unwrap();
        i += 1;
    }
    // No more writes: only the backoff timer retries (1 s, 2 s, then 4 s).
    let deadline = Instant::now() + Duration::from_secs(20);
    while attempts(&failed.lock().unwrap()).len() < 3 {
        assert!(Instant::now() < deadline, "the backoff never retried");
        std::thread::sleep(Duration::from_millis(20));
    }
    let (second, third) = {
        let a = attempts(&failed.lock().unwrap());
        (a[1], a[2])
    };
    // Inside the backoff after the third failure (at least 4 s: 1 s doubling per failure in
    // a row, and writes may have failed some first): the process idles.
    std::thread::sleep(Duration::from_millis(300));
    let (cpu0, wall0) = (process_cpu(), Instant::now());
    std::thread::sleep(Duration::from_millis(3_000));
    let (cpu, wall) = (process_cpu() - cpu0, wall0.elapsed());
    assert_eq!(
        attempts(&failed.lock().unwrap()).len(),
        3,
        "retried during the backoff"
    );
    assert!(
        cpu.as_secs_f64() < wall.as_secs_f64() * 0.01,
        "{cpu:?} of CPU in {wall:?} of backoff (issue #89)"
    );
    // The fourth attempt fires on time: twice the previous wait after the third.
    let deadline = Instant::now() + Duration::from_secs(30);
    while attempts(&failed.lock().unwrap()).len() < 4 {
        assert!(Instant::now() < deadline, "the backoff never retried");
        std::thread::sleep(Duration::from_millis(20));
    }
    let gap = attempts(&failed.lock().unwrap())[3].duration_since(third);
    let previous = third.duration_since(second);
    let ratio = gap.as_secs_f64() / previous.as_secs_f64();
    assert!(
        gap >= Duration::from_secs(4) && (1.8..2.2).contains(&ratio),
        "retried {gap:?} after the third failure, after {previous:?} before it"
    );
    fail.store(false, Ordering::Release);
    db.close().unwrap();
}
