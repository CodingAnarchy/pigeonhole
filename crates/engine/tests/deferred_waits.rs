//! Issue #164: the #135 wait and close fixes (D150–D152) under the simulator's deferred I/O
//! (#145). Submitted reads, writes and syncs stay in flight until the test completes them,
//! in an order the seed picks, so a WAL group sync or a manifest root commit spans
//! `run_once` calls, catalog changes and drops. The thread running a case drives every
//! application-owned shard and is the I/O device too: unless a case says otherwise,
//! completions are reaped on the shard-driving thread, as an io_uring backend would reap
//! them. Where a thread blocks on I/O nobody else would complete (a catalog change behind
//! an in-flight root commit, a dropped shard's final sync), another thread completes it,
//! as today's completion pool does; with the blocked thread as the only reaper these hang
//! (#207). Helper threads submit only while the shards are idle and wait for it, so a seed
//! replays the same I/O trace, which every case checks by running each seed twice.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, Error, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{Completion, FileRef, Vfs, VfsRef};

const DB: &str = "/db/d.phdb";

fn seeds() -> Vec<u64> {
    let n: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6);
    let base: u64 = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    (base..base + n).collect()
}

/// Runs `case` twice for every seed, each on a thread of its own (it drives the shards), and
/// checks that both runs did the same I/O. A run that has not finished within 60 s is a
/// hang.
fn each_seed(case: fn(u64)) {
    for seed in seeds() {
        let run = || {
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                case(seed);
                let trace = CASE_VFS.with(|v| v.borrow().as_ref().map(|v| v.recorded_ops()));
                let _ = tx.send(trace.expect("the case opened a rig"));
            });
            rx.recv_timeout(Duration::from_secs(60))
                .unwrap_or_else(|_| panic!("seed {seed}: hung or failed (see above)"))
        };
        let (first, second) = (run(), run());
        assert!(
            first == second,
            "seed {seed}: the second run's I/O diverged"
        );
        eprintln!("seed {seed}: {} mutating ops", first.len());
    }
}

thread_local! {
    /// The `SimVfs` of the rig the case on this thread opened (for `each_seed`'s check).
    static CASE_VFS: std::cell::RefCell<Option<Arc<SimVfs>>> =
        const { std::cell::RefCell::new(None) };
}

// ---- the device: SimVfs with deferred I/O, per-file in-flight counts and sync failures ----

/// What the test knows about the device.
#[derive(Debug, Default)]
struct DevState {
    /// Submitted operations in flight, per file.
    in_flight: HashMap<PathBuf, usize>,
    /// Blocking syncs started, per file.
    blocking_syncs: HashMap<PathBuf, usize>,
    /// Files whose blocking syncs fail.
    fail_syncs: HashSet<PathBuf>,
}

#[derive(Debug)]
struct Dev {
    sim: Arc<SimVfs>,
    state: Arc<Mutex<DevState>>,
}

impl Dev {
    /// Operations submitted to `path` and not completed.
    fn in_flight(&self, path: &Path) -> usize {
        let st = self.state.lock().unwrap();
        st.in_flight.get(path).copied().unwrap_or(0)
    }

    /// Blocking syncs of `path` started so far.
    fn blocking_syncs(&self, path: &Path) -> usize {
        let st = self.state.lock().unwrap();
        st.blocking_syncs.get(path).copied().unwrap_or(0)
    }

    /// Waits (up to 10 s) until another thread starts a blocking sync of `path`, the
    /// `before`+1-th.
    fn await_blocking_sync(&self, path: &Path, before: usize) {
        for _ in 0..10_000 {
            if self.blocking_syncs(path) > before {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("no blocking sync of {path:?} started");
    }

    /// Fails every later blocking sync of `path` (as the final sync at close is).
    fn fail_syncs(&self, path: &Path) {
        let mut st = self.state.lock().unwrap();
        st.fail_syncs.insert(path.to_path_buf());
    }
}

#[derive(Debug)]
struct DevFile {
    path: PathBuf,
    inner: FileRef,
    state: Arc<Mutex<DevState>>,
}

impl DevFile {
    /// Counts `op` in flight until it completes.
    fn track<T: Send + 'static>(&self, op: Completion<T>) -> Completion<T> {
        *self
            .state
            .lock()
            .unwrap()
            .in_flight
            .entry(self.path.clone())
            .or_default() += 1;
        let (state, path) = (Arc::clone(&self.state), self.path.clone());
        op.map(move |r| {
            *state
                .lock()
                .unwrap()
                .in_flight
                .get_mut(&path)
                .expect("counted") -= 1;
            r
        })
    }

    /// Counts a blocking sync and says whether it fails.
    fn blocking_sync(&self) -> bool {
        let mut st = self.state.lock().unwrap();
        *st.blocking_syncs.entry(self.path.clone()).or_default() += 1;
        st.fail_syncs.contains(&self.path)
    }

    fn injected() -> pigeonhole_io::Error {
        pigeonhole_io::Error::new(pigeonhole_io::ErrorKind::Other, "injected sync failure")
    }
}

impl Vfs for Dev {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<FileRef> {
        Ok(Arc::new(DevFile {
            path: path.to_path_buf(),
            inner: self.sim.open(path, opts)?,
            state: Arc::clone(&self.state),
        }))
    }
    fn remove(&self, path: &Path) -> pigeonhole_io::Result<()> {
        self.sim.remove(path)
    }
    fn exists(&self, path: &Path) -> pigeonhole_io::Result<bool> {
        self.sim.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> pigeonhole_io::Result<Vec<PathBuf>> {
        self.sim.list_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> pigeonhole_io::Result<()> {
        self.sim.sync_dir(dir)
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: pigeonhole_io::SharedOpen,
    ) -> pigeonhole_io::Result<pigeonhole_io::SharedRegion> {
        self.sim.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> pigeonhole_io::Result<()> {
        self.sim.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.sim.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.sim.monotonic_nanos()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.sim.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.sim.process_alive(process)
    }
    fn random_u64(&self) -> u64 {
        self.sim.random_u64()
    }
}

impl pigeonhole_io::File for DevFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> Completion {
        self.track(self.inner.submit_read(buf, offset))
    }
    fn submit_write(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> Completion {
        self.track(self.inner.submit_write(buf, offset))
    }
    fn sync_data(&self) -> pigeonhole_io::Result<()> {
        if self.blocking_sync() {
            return Err(Self::injected());
        }
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> Completion<()> {
        self.track(self.inner.submit_sync_data())
    }
    fn sync_all(&self) -> pigeonhole_io::Result<()> {
        if self.blocking_sync() {
            return Err(Self::injected());
        }
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

// ---- the rig: an application-owned engine, its shards, and the seed's schedule ----

struct Rig {
    seed: u64,
    rng: u64,
    sim: Arc<SimVfs>,
    dev: Arc<Dev>,
    db: Arc<Engine>,
    shards: Vec<EngineShard>,
    /// Set by any shard's wakeup: work arrived for a shard that reported idle.
    woke: Arc<AtomicBool>,
}

fn options(vfs: VfsRef, shards: usize) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    o
}

fn wal(stream: u32) -> PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), pigeonhole_format::StreamId(stream))
}

fn put(t: &TableInfo, row: &str) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

fn has(db: &Engine, t: &TableInfo, row: &str) -> bool {
    db.get_latest(t.id, t.families[0].id, row.as_bytes(), b"q")
        .unwrap()
        .is_some()
}

/// Polls `f` once, without blocking (the event loop's way to await a commit).
fn poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    std::pin::Pin::new(f).poll(&mut Context::from_waker(Waker::noop()))
}

impl Rig {
    /// Opens a database of `shards` shards on a deferred-I/O `SimVfs` seeded with `seed`,
    /// and runs every shard once, so this thread counts as their driver.
    fn open(seed: u64, shards: usize) -> Self {
        let sim = SimVfs::new(seed);
        sim.record_ops();
        sim.set_deferred_io(true);
        CASE_VFS.with(|v| *v.borrow_mut() = Some(Arc::clone(&sim)));
        let dev = Arc::new(Dev {
            sim: Arc::clone(&sim),
            state: Arc::default(),
        });
        let vfs: VfsRef = Arc::clone(&dev) as VfsRef;
        let (db, mut shards) =
            Engine::open_application_owned(Path::new(DB), options(vfs, shards)).unwrap();
        let woke = Arc::new(AtomicBool::new(false));
        for s in &mut shards {
            let woke = Arc::clone(&woke);
            s.set_wakeup(Box::new(move || woke.store(true, Ordering::Release)));
        }
        let mut rig = Self {
            seed,
            rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
            sim,
            dev,
            db,
            shards,
            woke,
        };
        rig.settle();
        rig
    }

    fn vfs(&self) -> VfsRef {
        Arc::clone(&self.dev) as VfsRef
    }

    fn draw(&mut self, n: u64) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng % n
    }

    /// Runs every shard until none has work and none was woken, completing no I/O.
    fn run(&mut self) {
        loop {
            self.woke.store(false, Ordering::Release);
            let mut more = false;
            for s in &mut self.shards {
                more |= s.run_once(u64::MAX);
            }
            if !more && !self.woke.load(Ordering::Acquire) {
                return;
            }
        }
    }

    /// One scheduling point: runs the shards (a seed-chosen number of passes, possibly
    /// none), then completes one operation in flight. Returns whether one was.
    fn step(&mut self) -> bool {
        for _ in 0..self.draw(3) {
            for s in &mut self.shards {
                s.run_once(u64::MAX);
            }
        }
        let completed = self.sim.complete_io();
        self.run();
        completed
    }

    /// Steps until no I/O is in flight and the shards are idle.
    fn settle(&mut self) {
        self.run();
        while self.step() {}
    }

    /// Steps until `cond` holds (at most 10,000 steps), completing I/O one operation at a
    /// time; returns whether it did.
    fn step_until(&mut self, mut cond: impl FnMut(&mut Self) -> bool) -> bool {
        self.run();
        for _ in 0..10_000 {
            if cond(self) {
                return true;
            }
            if !self.step() && !cond(self) {
                return false;
            }
        }
        false
    }

    /// Runs the shards until idle, then runs `f` on a new thread, which submits work to a
    /// shard and blocks, and waits (up to 10 s) until that work arrived (a shard's wakeup
    /// fired), so the run's I/O does not depend on when the thread gets to submit.
    fn spawn_submitter<T: Send + 'static>(
        &mut self,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> thread::JoinHandle<T> {
        self.run();
        let h = thread::spawn(f);
        for _ in 0..10_000 {
            if self.woke.load(Ordering::Acquire) {
                return h;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("seed {}: no work arrived", self.seed);
    }

    fn table(&mut self, name: &str) -> Arc<TableInfo> {
        let t = self
            .db
            .create_table(name, &[("f".into(), FamilyOptions::default())])
            .unwrap();
        self.settle();
        t
    }

    /// The shard owning `t` (one tablet per new table): commits to it and looks at the
    /// stream the record went to.
    fn shard_of(&mut self, t: &TableInfo) -> u32 {
        self.db.take_appended();
        let mut p = self
            .db
            .submit(put(t, "probe"), Some(Durability::Buffered))
            .unwrap();
        self.settle();
        assert!(matches!(poll(&mut p), Poll::Ready(Ok(_))));
        u32::from(self.db.take_appended().last().expect("one record").stream)
    }

    /// Two tables, ordered by owning shard (0, then 1).
    fn tables_on_0_and_1(&mut self) -> (Arc<TableInfo>, Arc<TableInfo>) {
        let a = self.table("a");
        let b = self.table("b");
        match (self.shard_of(&a), self.shard_of(&b)) {
            (0, 1) => (a, b),
            (1, 0) => (b, a),
            other => panic!("seed {}: tables on shards {other:?}", self.seed),
        }
    }

    /// The documented application-owned close loop on this thread, which drives every
    /// shard and reaps their I/O: keep driving until `closed()` reports the outcome.
    fn close_loop(&mut self) -> pigeonhole_engine::Result<()> {
        for _ in 0..100_000 {
            if let Some(outcome) = self.shards[0].closed() {
                for s in &self.shards {
                    assert_eq!(
                        s.closed().map(|r| r.is_ok()),
                        Some(outcome.is_ok()),
                        "seed {}: shards report different outcomes",
                        self.seed
                    );
                }
                return outcome;
            }
            self.step();
        }
        panic!("seed {}: the close never finished", self.seed);
    }
}

// ---- D150: blocking commit waits ----

/// A `Sync` commit's group sync stays in flight across `run_once` calls. On the thread
/// that drives the shards (and reaps their I/O), blocking calls are refused before they
/// submit anything (`InvalidArgument`) and `PendingCommit::wait` on a commit already
/// submitted returns `WouldDeadlock`; the event loop's way (polling the future) sees it
/// complete once the sync does. A blocking `commit` on another thread parks until the
/// driving thread completes the sync, then returns.
#[test]
fn commit_waits_park_until_their_sync_completes_and_the_driver_is_refused() {
    each_seed(|seed| {
        let mut rig = Rig::open(seed, 2);
        let (on_a, on_b) = rig.tables_on_0_and_1();

        // Shard 1's group sync is in flight (it holds the global watermark too).
        let held = rig
            .db
            .submit(put(&on_b, "held"), Some(Durability::Sync))
            .unwrap();
        rig.run();
        assert!(
            rig.dev.in_flight(&wal(1)) > 0,
            "seed {seed}: no sync in flight"
        );
        let in_flight = rig.sim.io_in_flight();

        // On the driving thread: refused before submitting, or WouldDeadlock after.
        let refused = rig.db.commit(put(&on_a, "refused"), Some(Durability::None));
        assert!(
            matches!(refused, Err(Error::InvalidArgument(ref m)) if m.contains("nothing was submitted")),
            "seed {seed}: {refused:?}"
        );
        let flushed = rig.db.flush();
        assert!(
            matches!(flushed, Err(Error::InvalidArgument(_))),
            "seed {seed}: {flushed:?}"
        );
        let mut later = rig
            .db
            .submit(put(&on_a, "later"), Some(Durability::None))
            .unwrap();
        rig.run();
        let waited = rig
            .db
            .submit(put(&on_a, "waited"), Some(Durability::None))
            .unwrap()
            .wait();
        assert!(
            matches!(waited, Err(Error::WouldDeadlock)),
            "seed {seed}: {waited:?}"
        );
        assert!(
            poll(&mut later).is_pending(),
            "seed {seed}: visible past shard 1's unresolved group"
        );
        assert_eq!(
            rig.sim.io_in_flight(),
            in_flight,
            "seed {seed}: the refused calls submitted I/O"
        );

        // Another thread's blocking commit parks behind the sync.
        let (db, t) = (Arc::clone(&rig.db), Arc::clone(&on_a));
        let waiter =
            rig.spawn_submitter(move || db.commit(put(&t, "blocking"), Some(Durability::Sync)));
        rig.run();
        assert!(
            !waiter.is_finished(),
            "seed {seed}: a blocking commit returned with its group's sync in flight"
        );
        // Only the syncs completing (on this thread) lets it, and the futures, finish.
        assert!(rig.step_until(|r| r.sim.io_in_flight() == 0), "seed {seed}");
        rig.settle();
        waiter.join().unwrap().unwrap();
        assert!(matches!(poll(&mut later), Poll::Ready(Ok(_))));
        // Polling the held commit from the event loop: done and visible.
        let mut held = held;
        assert!(matches!(poll(&mut held), Poll::Ready(Ok(_))), "seed {seed}");
        for (row, landed) in [
            ("held", true),
            ("later", true),
            ("waited", true),
            ("blocking", true),
            ("refused", false),
        ] {
            let t = if row == "held" { &on_b } else { &on_a };
            assert_eq!(has(&rig.db, t, row), landed, "seed {seed}: row {row}");
        }
        rig.db.close().unwrap();
        rig.close_loop().unwrap();
    });
}

// ---- D150 (a): catalog changes and shrink while a pump's root commit is in flight ----

/// Starts a flush and steps until its manifest pump's root commit has a sync of the
/// database file in flight; returns the flush's future.
fn pump_commit_in_flight(
    rig: &mut Rig,
    t: &TableInfo,
    batch: u32,
) -> pigeonhole_engine::PendingMaintenance {
    for i in 0..50 {
        drop(
            rig.db
                .submit(put(t, &format!("r{batch}-{i:03}")), Some(Durability::None))
                .unwrap(),
        );
    }
    rig.settle();
    let flushed = rig.db.flush_pending().unwrap();
    let seed = rig.seed;
    assert!(
        rig.step_until(|r| r.dev.in_flight(Path::new(DB)) > 0),
        "seed {seed}: the flush's root commit never started"
    );
    flushed
}

/// Completes the pump's root commit while the shard is not run: on this thread before
/// the catalog change (`now`), or on a completion thread while the change is blocked on
/// the manifest writer. Either way the driving thread finishes the pump's commit itself
/// (`manifest::Flight`), so the change does not wait for a shard only it can run.
fn complete_root_commit(rig: &Rig, now: bool) -> Option<thread::JoinHandle<()>> {
    let dev = Arc::clone(&rig.dev);
    let device = move || {
        while dev.in_flight(Path::new(DB)) > 0 {
            assert!(dev.sim.complete_io());
        }
    };
    if now {
        device();
        None
    } else {
        Some(thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            device();
        }))
    }
}

/// Catalog changes (`create_table`, `drop_table`) and `shrink` on the thread that drives
/// the only shard, while that shard's manifest pump holds the writer with its root commit
/// in flight: none waits for the shard (D150 (a)), and the flush the pump commits
/// completes.
#[test]
fn catalog_changes_and_shrink_on_the_driver_finish_an_in_flight_pump_commit() {
    each_seed(|seed| {
        let mut rig = Rig::open(seed, 1);
        let t = rig.table("t");
        let pool = rig.draw(2) == 0;

        let mut flushed = vec![pump_commit_in_flight(&mut rig, &t, 0)];
        let device = complete_root_commit(&rig, !pool);
        let u = rig
            .db
            .create_table("u", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        if let Some(d) = device {
            d.join().unwrap();
        }

        flushed.push(pump_commit_in_flight(&mut rig, &t, 1));
        let device = complete_root_commit(&rig, pool);
        rig.db.drop_table(u.id).unwrap();
        if let Some(d) = device {
            d.join().unwrap();
        }
        assert!(rig.db.table("u").is_none());

        flushed.push(pump_commit_in_flight(&mut rig, &t, 2));
        let device = complete_root_commit(&rig, rig.seed.is_multiple_of(2));
        rig.db.shrink().unwrap();
        if let Some(d) = device {
            d.join().unwrap();
        }

        rig.settle();
        for mut f in flushed {
            assert!(
                matches!(poll(&mut f), Poll::Ready(Ok(()))),
                "seed {seed}: a flush did not complete"
            );
        }
        assert!(has(&rig.db, &t, "r2-049"));
        rig.db.close().unwrap();
        rig.close_loop().unwrap();
        let vfs = rig.vfs();
        drop(rig);
        assert!(Engine::inspect_manifest(&vfs, Path::new(DB)).unwrap().clean);
    });
}

// ---- D151: the application-owned close ----

/// Commits rows of every durability, some with their syncs still in flight.
fn write_and_leave_in_flight(rig: &mut Rig, t: &TableInfo) {
    for i in 0..60 {
        let d = [Durability::None, Durability::Buffered, Durability::Sync][i % 3];
        drop(rig.db.submit(put(t, &format!("r{i:03}")), Some(d)).unwrap());
        if rig.draw(4) == 0 {
            rig.step();
        }
    }
    // The last one's group sync, at least, stays in flight.
    drop(
        rig.db
            .submit(put(t, "last"), Some(Durability::Sync))
            .unwrap(),
    );
    rig.run();
}

/// `close()` on the thread that drives (and reaps for) every shard returns at once; the
/// documented loop keeps driving with the close's flushes, checkpoints and syncs in flight
/// across `run_once` calls until `closed()` reports the outcome, and the close is clean.
#[test]
fn app_owned_close_with_io_in_flight_completes_through_closed() {
    each_seed(|seed| {
        let mut rig = Rig::open(seed, 2);
        let t = rig.table("t");
        write_and_leave_in_flight(&mut rig, &t);
        assert!(rig.sim.io_in_flight() > 0, "seed {seed}: nothing in flight");
        rig.db.close().unwrap();
        assert!(rig.shards.iter().all(|s| s.closed().is_none()));
        rig.close_loop()
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let vfs = rig.vfs();
        drop(rig);
        assert!(Engine::inspect_manifest(&vfs, Path::new(DB)).unwrap().clean);
        for s in 0..2 {
            assert!(
                !vfs.exists(&wal(s)).unwrap(),
                "seed {seed}: {:?} left",
                wal(s)
            );
        }
    });
}

/// A failed final sync: `closed()` reports it on every shard (it used to be lost), and a
/// `close()` on a thread that drives no shard waits and returns it.
#[test]
fn app_owned_close_reports_a_failed_final_sync() {
    each_seed(|seed| {
        let mut rig = Rig::open(seed, 2);
        let t = rig.table("t");
        write_and_leave_in_flight(&mut rig, &t);
        let s = rig.draw(2) as u32;
        rig.dev.fail_syncs(&wal(s));
        let db = Arc::clone(&rig.db);
        let closer = rig.spawn_submitter(move || db.close());
        let outcome = rig.close_loop();
        assert!(outcome.is_err(), "seed {seed}: a failed close reported Ok");
        let closed = closer.join().unwrap();
        assert!(closed.is_err(), "seed {seed}: close() reported Ok");
        let vfs = rig.vfs();
        drop(rig);
        assert!(!Engine::inspect_manifest(&vfs, Path::new(DB)).unwrap().clean);
    });
}

// ---- #190: dropping a shard with a WAL sync in flight ----

/// A shard dropped mid-group: its final sync waits for the group's sync, which is still in
/// flight (#179). The drop returns once that sync completes (here on the test thread while
/// another thread drops the shard: if the dropping thread were the only one reaping
/// completions, it would wait for ever). The close is then unclean, and the commit whose
/// sync completed is replayed at the next open.
#[test]
fn dropping_a_shard_with_a_wal_sync_in_flight_completes_once_the_sync_does() {
    each_seed(|seed| {
        let mut rig = Rig::open(seed, 1);
        let t = rig.table("t");
        drop(
            rig.db
                .submit(put(&t, "synced"), Some(Durability::Sync))
                .unwrap(),
        );
        rig.run();
        assert!(
            rig.dev.in_flight(&wal(0)) > 0,
            "seed {seed}: no sync in flight"
        );

        let shard = rig.shards.pop().unwrap();
        let before = rig.dev.blocking_syncs(&wal(0));
        let dropper = thread::spawn(move || drop(shard));
        rig.dev.await_blocking_sync(&wal(0), before);
        assert!(
            !dropper.is_finished(),
            "seed {seed}: the drop did not wait for the in-flight sync"
        );
        rig.sim.complete_all_io();
        dropper.join().unwrap();

        assert!(
            rig.db.close().is_err(),
            "seed {seed}: unclean close reported Ok"
        );
        let vfs = rig.vfs();
        let sim = Arc::clone(&rig.sim);
        drop(rig);
        sim.set_deferred_io(false);
        let (db, mut shards) =
            Engine::open_application_owned(Path::new(DB), options(vfs, 1)).unwrap();
        while shards[0].run_once(u64::MAX) {}
        let t = db.table("t").unwrap();
        assert!(
            has(&db, &t, "synced"),
            "seed {seed}: the synced commit was lost"
        );
        db.close().unwrap();
        while shards[0].closed().is_none() {
            shards[0].run_once(u64::MAX);
        }
    });
}

// ---- D150: a visibility wait ends when the shard holding it dies or closes ----

/// Shard 1's group sync is in flight (holding the global watermark) when another thread's
/// commit on shard 0 waits for visibility. Shard 1 is dropped (its group never resolves):
/// the wait ends with `Closed` instead of parking for ever, and the close is unclean.
#[test]
fn a_visibility_wait_ends_with_closed_when_the_shard_holding_it_dies_mid_io() {
    each_seed(|seed| {
        let mut rig = Rig::open(seed, 2);
        let (on_a, on_b) = rig.tables_on_0_and_1();
        drop(
            rig.db
                .submit(put(&on_b, "held"), Some(Durability::Sync))
                .unwrap(),
        );
        rig.run();
        assert!(
            rig.dev.in_flight(&wal(1)) > 0,
            "seed {seed}: no sync in flight"
        );

        let (db, t) = (Arc::clone(&rig.db), Arc::clone(&on_a));
        let waiter =
            rig.spawn_submitter(move || db.commit(put(&t, "waits"), Some(Durability::None)));
        rig.run();
        assert!(
            !waiter.is_finished(),
            "seed {seed}: visible past shard 1's group"
        );

        // Shard 1 dies with its group's sync in flight: another thread drops it, and its
        // final sync waits for the group's (#179) until this thread completes it.
        let b = rig.shards.pop().unwrap();
        let before = rig.dev.blocking_syncs(&wal(1));
        let dropper = thread::spawn(move || drop(b));
        rig.dev.await_blocking_sync(&wal(1), before);
        rig.sim.complete_all_io();
        dropper.join().unwrap();
        let ended = waiter.join().unwrap();
        assert!(
            matches!(ended, Err(Error::Closed)),
            "seed {seed}: {ended:?}"
        );
        // This thread still drives shard 0: `close` cannot wait here.
        rig.db.close().unwrap();
        let outcome = rig.close_loop();
        assert!(
            outcome.is_err(),
            "seed {seed}: a dropped shard's close is unclean"
        );
    });
}

/// The same wait while the database closes, the close's I/O reaped on the driving
/// thread: the wait ends (with its commit visible, or `Closed`) instead of parking for
/// ever, and the close is clean.
#[test]
fn a_visibility_wait_ends_when_the_database_closes_mid_io() {
    each_seed(|seed| {
        let mut rig = Rig::open(seed, 2);
        let (on_a, on_b) = rig.tables_on_0_and_1();
        drop(
            rig.db
                .submit(put(&on_b, "held"), Some(Durability::Sync))
                .unwrap(),
        );
        rig.run();
        let (db, t) = (Arc::clone(&rig.db), Arc::clone(&on_a));
        let waiter =
            rig.spawn_submitter(move || db.commit(put(&t, "waits"), Some(Durability::None)));
        rig.run();
        assert!(
            !waiter.is_finished(),
            "seed {seed}: visible past shard 1's group"
        );
        rig.db.close().unwrap();
        rig.close_loop()
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let ended = waiter.join().unwrap();
        assert!(
            matches!(ended, Ok(_) | Err(Error::Closed)),
            "seed {seed}: {ended:?}"
        );
    });
}
