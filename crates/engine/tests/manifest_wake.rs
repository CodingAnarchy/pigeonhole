//! A thread parked on the manifest writer is woken by the pump's root commit completing
//! (`Flight::complete`, #135), not only when the pump's shard runs again.
//!
//! A test binary of its own: with a reaper-less backend registered anywhere in the process
//! (`deferred_waits`' simulator, application-owned io_uring), a blocked thread runs that
//! backend in 1 ms slices instead of parking (ICR 0028), and this wake is never needed. Here
//! the simulator completes I/O inline and the device below holds the database file's
//! submitted operations until the test releases them, on the test's thread, as a
//! completion pool would.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{Completion, FileRef, Vfs, VfsRef};

const DB: &str = "/db/w.phdb";

type Held = Arc<Mutex<Vec<Box<dyn FnOnce() + Send>>>>;

/// The simulator, holding the database file's submitted operations while `armed`.
struct Gate {
    sim: Arc<SimVfs>,
    armed: Arc<AtomicBool>,
    held: Held,
}

impl std::fmt::Debug for Gate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gate").finish_non_exhaustive()
    }
}

impl Gate {
    /// Completes every held operation; those their completions submit are held again.
    fn release(&self) -> usize {
        let ops = std::mem::take(&mut *self.held.lock().unwrap());
        let n = ops.len();
        for op in ops {
            op();
        }
        n
    }

    fn held(&self) -> usize {
        self.held.lock().unwrap().len()
    }
}

struct GateFile {
    gated: bool,
    inner: FileRef,
    armed: Arc<AtomicBool>,
    held: Held,
}

impl std::fmt::Debug for GateFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GateFile").finish_non_exhaustive()
    }
}

impl GateFile {
    fn hold<T: Send + 'static>(&self, op: Completion<T>) -> Completion<T> {
        if !self.gated || !self.armed.load(Ordering::Acquire) {
            return op;
        }
        let (held, resolver) = Completion::pair();
        self.held
            .lock()
            .unwrap()
            .push(Box::new(move || resolver.resolve(op.wait())));
        held
    }
}

impl Vfs for Gate {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<FileRef> {
        Ok(Arc::new(GateFile {
            gated: path == Path::new(DB),
            inner: self.sim.open(path, opts)?,
            armed: Arc::clone(&self.armed),
            held: Arc::clone(&self.held),
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
    fn clock_is_simulated(&self) -> bool {
        self.sim.clock_is_simulated()
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

impl pigeonhole_io::File for GateFile {
    fn direct_align(&self) -> Option<usize> {
        self.inner.direct_align()
    }
    fn read_at(&self, buf: &mut [u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> Completion {
        self.hold(self.inner.submit_read(buf, offset))
    }
    fn submit_write(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> Completion {
        self.hold(self.inner.submit_write(buf, offset))
    }
    fn sync_data(&self) -> pigeonhole_io::Result<()> {
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> Completion<()> {
        self.hold(self.inner.submit_sync_data())
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

fn run(shard: &mut EngineShard) {
    while shard.run_once(u64::MAX) {}
}

/// Up to 10 s for `cond`, polling every millisecond.
fn eventually(mut cond: impl FnMut() -> bool) -> bool {
    for _ in 0..10_000 {
        if cond() {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    cond()
}

#[test]
fn a_root_commit_completion_wakes_a_thread_parked_on_the_manifest_writer() {
    let gate = Arc::new(Gate {
        sim: SimVfs::new(135),
        armed: Arc::new(AtomicBool::new(false)),
        held: Arc::default(),
    });
    let mut o = EngineOptions::new(Arc::clone(&gate) as VfsRef);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.compaction_threads = 0;
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let mut shard = shards.pop().unwrap();
    let db = Arc::new(db);
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for i in 0..50 {
        let mut wb = WriteBatch::new();
        let row = format!("r{i:03}");
        wb.put(
            t.id,
            t.families[0].id,
            row.as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(b"v"),
        )
        .unwrap();
        drop(db.submit(wb, Some(Durability::None)).unwrap());
    }
    run(&mut shard);

    // A flush, run until its pump's root commit is in flight on the held device.
    gate.armed.store(true, Ordering::Release);
    let mut flushed = db.flush_pending().unwrap();
    let mut in_flight = false;
    for _ in 0..1_000 {
        run(&mut shard);
        if db.manifest_commit_in_flight() && gate.held() > 0 {
            in_flight = true;
            break;
        }
        gate.release();
    }
    assert!(in_flight, "the pump's root commit never started");

    // A catalog change on a thread that drives no shard parks behind it.
    let (tx, rx) = mpsc::channel();
    let changer = {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            let u = db.create_table("u", &[("f".into(), FamilyOptions::default())]);
            let _ = tx.send(u.map(|u| u.id));
        })
    };
    assert!(
        eventually(|| db.manifest_thread_parks() > 0),
        "the catalog change never parked"
    );

    // The root commit completes on this thread; the shard does not run until the change
    // returns. Only the completion's wake gets the parked thread to finish the pump's commit
    // (`take_done`) and then its own.
    while gate.release() > 0 {}
    let u = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the root commit's completion did not wake the parked thread")
        .unwrap();
    changer.join().unwrap();

    gate.armed.store(false, Ordering::Release);
    while gate.release() > 0 {}
    run(&mut shard);
    let ready = std::pin::Pin::new(&mut flushed).poll(&mut Context::from_waker(Waker::noop()));
    assert!(
        matches!(ready, Poll::Ready(Ok(()))),
        "the flush did not complete"
    );
    assert_eq!(db.table("u").map(|t| t.id), Some(u));
    db.close().unwrap();
    while shard.closed().is_none() {
        run(&mut shard);
    }
    assert!(shard.closed().unwrap().is_ok());
}
