//! Open to first read (#158; spec: < 5 ms): opens a small existing database, reads one cell,
//! closes, `RUNS` times, and reports the medians of `open`, the first read, and of the time
//! the open spent in each VFS operation (through a timing wrapper around the real backend),
//! with the rest as "other" (CPU: shared-memory setup in memory, manifest decode, threads).
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example openlat -- DIR [RUNS] [pread|uring]
//! ```
//!
//! With `OPENLAT_JSON=1` it also prints one JSON line (medians, p99, the per-operation
//! breakdown) for the Phase 3 gate run (#405, `baselines/phase3-io/open-latency.sh`).
//! Diagnostics: `OPENLAT_WHO=1` prints the call stack of each sync during one open (who
//! syncs), and `OPENLAT_NOSYNC=1` makes syncs free (not durable: the floor without them).
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_io::{
    Completion, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions, ProcessId, Result,
    SharedOpen, SharedRegion, Vfs, VfsRef,
};

/// Time spent per operation name while recording.
#[derive(Default)]
struct Times {
    on: bool,
    ops: BTreeMap<&'static str, (u32, Duration)>,
}

#[derive(Clone)]
struct Clock(Arc<Mutex<Times>>);

impl Clock {
    fn time<T>(&self, op: &'static str, f: impl FnOnce() -> T) -> T {
        if op.contains("sync") && std::env::var("OPENLAT_WHO").is_ok() && self.0.lock().unwrap().on
        {
            let bt = std::backtrace::Backtrace::force_capture().to_string();
            let frames: Vec<&str> = bt
                .lines()
                .filter(|l| l.contains("pigeonhole_") && !l.contains("openlat"))
                .map(str::trim)
                .take(6)
                .collect();
            eprintln!("== {op}: {}", frames.join(" <- "));
        }
        let t = Instant::now();
        let r = f();
        self.add(op, t.elapsed());
        r
    }

    fn add(&self, op: &'static str, d: Duration) {
        let mut t = self.0.lock().unwrap();
        if t.on {
            let e = t.ops.entry(op).or_default();
            e.0 += 1;
            e.1 += d;
        }
    }

    /// A submitted operation, timed from submit to completion.
    fn submitted<T: Send + 'static>(&self, op: &'static str, c: Completion<T>) -> Completion<T> {
        if std::env::var("OPENLAT_WHO").is_ok() && self.0.lock().unwrap().on {
            let bt = std::backtrace::Backtrace::force_capture().to_string();
            let frames: Vec<&str> = bt
                .lines()
                .filter(|l| l.contains("pigeonhole_") && !l.contains("openlat"))
                .map(str::trim)
                .take(6)
                .collect();
            eprintln!("== {op}: {}", frames.join(" <- "));
        }
        let (clock, t) = (self.clone(), Instant::now());
        c.map(move |r| {
            clock.add(op, t.elapsed());
            r
        })
    }
}

#[derive(Debug)]
struct TimedVfs {
    inner: VfsRef,
    clock: ClockRef,
}

/// `Clock` behind `Debug` for the trait bounds.
struct ClockRef(Clock);

impl std::fmt::Debug for ClockRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Clock")
    }
}

#[derive(Debug)]
struct TimedFile {
    inner: FileRef,
    clock: ClockRef,
}

impl File for TimedFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.clock
            .0
            .time("read", || self.inner.read_at(buf, offset))
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.clock
            .0
            .time("write", || self.inner.write_at(buf, offset))
    }
    fn direct_align(&self) -> Option<usize> {
        self.inner.direct_align()
    }
    fn read_buf(&self, len: usize) -> IoBuf {
        self.inner.read_buf(len)
    }
    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion {
        self.clock
            .0
            .submitted("submit_read", self.inner.submit_read(buf, offset))
    }
    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        self.clock
            .0
            .submitted("submit_write", self.inner.submit_write(buf, offset))
    }
    fn sync_data(&self) -> Result<()> {
        if no_sync() {
            return Ok(());
        }
        self.clock.0.time("sync_data", || self.inner.sync_data())
    }
    fn submit_sync_data(&self) -> Completion<()> {
        if no_sync() {
            return Completion::ready(Ok(()));
        }
        self.clock
            .0
            .submitted("submit_sync_data", self.inner.submit_sync_data())
    }
    fn sync_all(&self) -> Result<()> {
        if no_sync() {
            return Ok(());
        }
        self.clock.0.time("sync_all", || self.inner.sync_all())
    }
    fn submit_sync_all(&self) -> Completion<()> {
        if no_sync() {
            return Completion::ready(Ok(()));
        }
        self.clock
            .0
            .submitted("submit_sync_all", self.inner.submit_sync_all())
    }
    fn len(&self) -> Result<u64> {
        self.clock.0.time("len", || self.inner.len())
    }
    fn set_len(&self, len: u64) -> Result<()> {
        self.clock.0.time("set_len", || self.inner.set_len(len))
    }
    fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        self.clock
            .0
            .time("allocate", || self.inner.allocate(offset, len))
    }
    fn lock(&self, byte: u64, mode: LockMode) -> Result<()> {
        self.clock.0.time("lock", || self.inner.lock(byte, mode))
    }
    fn unlock(&self, byte: u64) -> Result<()> {
        self.clock.0.time("unlock", || self.inner.unlock(byte))
    }
    fn identity(&self) -> Result<FileIdentity> {
        self.clock.0.time("identity", || self.inner.identity())
    }
    fn is_local(&self) -> Result<bool> {
        self.clock.0.time("is_local", || self.inner.is_local())
    }
}

impl Vfs for TimedVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        let inner = self.clock.0.time("open", || self.inner.open(path, opts))?;
        Ok(Arc::new(TimedFile {
            inner,
            clock: ClockRef(self.clock.0.clone()),
        }))
    }
    fn remove(&self, path: &Path) -> Result<()> {
        self.clock.0.time("remove", || self.inner.remove(path))
    }
    fn exists(&self, path: &Path) -> Result<bool> {
        self.clock.0.time("exists", || self.inner.exists(path))
    }
    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        self.clock.0.time("list_dir", || self.inner.list_dir(dir))
    }
    fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.clock.0.time("sync_dir", || self.inner.sync_dir(dir))
    }
    fn submit_sync_dir(&self, dir: &Path) -> Completion<()> {
        self.clock
            .0
            .submitted("submit_sync_dir", self.inner.submit_sync_dir(dir))
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion> {
        self.clock.0.time("open_shared", || {
            self.inner.open_shared(name, dir, len, mode)
        })
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()> {
        self.clock
            .0
            .time("remove_shared", || self.inner.remove_shared(name, dir))
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos()
    }
    fn current_process(&self) -> ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: ProcessId) -> bool {
        self.inner.process_alive(process)
    }
    fn attach_thread(&self) {
        self.inner.attach_thread();
    }
}

/// `OPENLAT_NOSYNC=1`: syncs return at once (not durable; the floor without them).
fn no_sync() -> bool {
    std::env::var_os("OPENLAT_NOSYNC").is_some()
}

fn backend(name: &str) -> VfsRef {
    match name {
        #[cfg(target_os = "linux")]
        "uring" => pigeonhole_io::uring::UringVfs::new().expect("io_uring"),
        _ => pigeonhole_io::pread::PreadVfs::new(0),
    }
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn p99(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[(v.len() * 99 / 100).min(v.len() - 1)]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(
        args.get(1)
            .expect("usage: openlat DIR [RUNS] [pread|uring]"),
    );
    let runs: usize = args.get(2).map_or(30, |r| r.parse().unwrap());
    let io = args.get(3).map_or("pread", String::as_str);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("open.phdb");
    let clock = Clock(Arc::default());
    let options = || {
        Options::default().vfs(Arc::new(TimedVfs {
            inner: backend(io),
            clock: ClockRef(clock.clone()),
        }))
    };
    {
        let db = Pigeonhole::open(&path, options()).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("f", Family::default())
            .create_if_missing()
            .unwrap();
        t.mutate(b"row").put("f", b"q", b"v").commit().unwrap();
        drop(t);
        db.close().unwrap();
    }
    let (mut opens, mut reads, mut totals) = (Vec::new(), Vec::new(), Vec::new());
    let mut per_op: BTreeMap<&'static str, Vec<(u32, Duration)>> = BTreeMap::new();
    for _ in 0..runs {
        {
            let mut t = clock.0.lock().unwrap();
            t.on = true;
            t.ops.clear();
        }
        let start = Instant::now();
        let db = Pigeonhole::open(&path, options()).unwrap();
        let opened = start.elapsed();
        let t = db.table("t").unwrap().open().unwrap();
        let cell = t.get(b"row", "f", b"q").unwrap().unwrap();
        assert_eq!(cell.value(), b"v");
        let total = start.elapsed();
        let ops = {
            let mut t = clock.0.lock().unwrap();
            t.on = false;
            std::mem::take(&mut t.ops)
        };
        for (op, v) in ops {
            per_op.entry(op).or_default().push(v);
        }
        opens.push(opened);
        reads.push(total - opened);
        totals.push(total);
        drop(t);
        db.close().unwrap();
    }
    let ms = |d: Duration| d.as_secs_f64() * 1e3;
    let (opens_for_json, reads_for_json) = (opens.clone(), reads.clone());
    println!(
        "{io}: open to first read {:.2} ms (open {:.2}, first read {:.2}), median of {runs}",
        ms(median(totals.clone())),
        ms(median(opens)),
        ms(median(reads))
    );
    let mut io_total = Duration::ZERO;
    println!("  {:<18} {:>6} {:>9}", "op (in the open)", "count", "ms");
    for (op, v) in &per_op {
        let count = v[v.len() / 2].0;
        let d = median(v.iter().map(|x| x.1).collect());
        io_total += d;
        println!("  {op:<18} {count:>6} {:>9.3}", ms(d));
    }
    println!(
        "  {:<18} {:>6} {:>9.3}   (submitted ops overlap others; an upper bound on I/O)",
        "all vfs ops",
        "",
        ms(io_total)
    );
    println!(
        "  {:<18} {:>6} {:>9.3}",
        "other (cpu, threads)",
        "",
        ms(median(totals.clone()).saturating_sub(io_total))
    );
    if std::env::var_os("OPENLAT_JSON").is_some() {
        let ops: Vec<String> = per_op
            .iter()
            .map(|(op, v)| {
                format!(
                    "\"{op}\":{{\"count\":{},\"ms\":{:.4}}}",
                    v[v.len() / 2].0,
                    ms(median(v.iter().map(|x| x.1).collect()))
                )
            })
            .collect();
        println!(
            "{{\"io\":\"{io}\",\"runs\":{runs},\"open_to_first_read_ms\":{:.4},\"p99_ms\":{:.4},\"open_ms\":{:.4},\"first_read_ms\":{:.4},\"ops\":{{{}}}}}",
            ms(median(totals.clone())),
            ms(p99(totals)),
            ms(median(opens_for_json)),
            ms(median(reads_for_json)),
            ops.join(",")
        );
    }
}
