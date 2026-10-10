//! Open durability (#158, D203): a clean reopen does no blocking flush on the opening thread
//! (its WAL streams' file and directory syncs are submitted and ordered before the first
//! durable commit; no manifest commit publishes nothing; the page file's length is not synced
//! again when it is exactly as committed), and a power loss anywhere through the open and the
//! first durable commit after it loses nothing that was acknowledged.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread::ThreadId;

use pigeonhole::{Durability, Family, Options, Pigeonhole, Table};
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{
    Completion, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions, ProcessId, Result,
    SharedOpen, SharedRegion, Vfs, VfsRef,
};

const DB: &str = "/db/open.phdb";
const ROWS: u32 = 40;

fn options(vfs: VfsRef) -> Options {
    Options::default()
        .vfs(vfs)
        .shards(2)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
}

fn table(db: &Pigeonhole) -> Table {
    db.table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap()
}

fn row(i: u32) -> Vec<u8> {
    format!("row{i:04}").into_bytes()
}

/// A database of `ROWS` group-synced rows: closed cleanly, or left by a process kill.
fn prepare(seed: u64, clean: bool) -> Arc<SimVfs> {
    let vfs = SimVfs::new(seed);
    let db = Pigeonhole::open(DB, options(vfs.clone())).unwrap();
    let t = table(&db);
    for i in 0..ROWS {
        t.mutate(&row(i))
            .durability(Durability::GroupSync)
            .put("f", b"q", &i.to_le_bytes())
            .commit()
            .unwrap();
    }
    drop(t);
    if clean {
        db.close().unwrap();
    } else {
        drop(db);
        vfs.crash(CrashKind::Process);
    }
    vfs
}

/// Every row, and `extra` if given, is readable.
fn check(vfs: &Arc<SimVfs>, extra: Option<&[u8]>, ctx: &str) {
    let db = Pigeonhole::open(DB, options(vfs.clone())).unwrap_or_else(|e| panic!("{ctx}: {e}"));
    let t = db.table("t").unwrap().open().unwrap();
    for i in 0..ROWS {
        let got = t.get(&row(i), "f", b"q").unwrap();
        assert!(got.is_some(), "{ctx}: row {i} lost");
    }
    if let Some(key) = extra {
        assert!(
            t.get(key, "f", b"q").unwrap().is_some(),
            "{ctx}: acknowledged commit lost"
        );
    }
    drop(t);
    db.close().unwrap();
}

/// Power loss after each mutating operation of a reopen and the first durable commit after
/// it, with submitted I/O completing on the simulator's device thread (so the open's deferred
/// syncs are really in flight when it crashes).
fn sweep(seed: u64, clean: bool) -> u32 {
    let mut points = 0;
    for n in 1.. {
        let vfs = prepare(seed, clean);
        vfs.set_deferred_io(true);
        let device = vfs.complete_io_in_background();
        let base = vfs.mutating_ops();
        let mut plan = FaultPlan::none();
        plan.crash_after_ops = Some(base + n);
        vfs.set_faults(plan);
        let mut acked = None;
        if let Ok(db) = Pigeonhole::open(DB, options(vfs.clone())) {
            if let Ok(t) = db.table("t").and_then(|b| b.open()) {
                let key = b"after-open".to_vec();
                if t.mutate(&key)
                    .durability(Durability::GroupSync)
                    .put("f", b"q", b"v")
                    .commit()
                    .is_ok()
                {
                    acked = Some(key);
                }
            }
            let _ = db.close();
        }
        let crashed = vfs.mutating_ops() >= base + n;
        vfs.set_deferred_io(false);
        drop(device);
        if !crashed {
            assert!(
                acked.is_some(),
                "seed {seed} clean {clean}: the run without a crash"
            );
            break;
        }
        points += 1;
        vfs.set_faults(FaultPlan::none());
        check(
            &vfs,
            acked.as_deref(),
            &format!("seed {seed} clean {clean} crash after op {n} of the reopen"),
        );
        assert!(n < 5_000, "runaway sweep");
    }
    points
}

#[test]
fn a_power_loss_through_a_clean_reopen_and_its_first_commit_loses_nothing_acknowledged() {
    let points: u32 = (0..3).map(|seed| sweep(seed, true)).sum();
    assert!(points > 10, "{points} crash points");
}

#[test]
fn a_power_loss_through_a_recovering_reopen_and_its_first_commit_loses_nothing_acknowledged() {
    let points: u32 = (10..13).map(|seed| sweep(seed, false)).sum();
    assert!(points > 10, "{points} crash points");
}

// ---- no blocking flush on a clean reopen ----

/// Counts the blocking syncs made on `thread` (submitted ones are not counted: their caller
/// does not wait).
#[derive(Debug)]
struct Counting {
    inner: VfsRef,
    thread: ThreadId,
    syncs: Arc<AtomicU32>,
}

#[derive(Debug)]
struct CountingFile {
    inner: FileRef,
    thread: ThreadId,
    syncs: Arc<AtomicU32>,
}

fn count(thread: ThreadId, syncs: &AtomicU32) {
    if std::thread::current().id() == thread {
        syncs.fetch_add(1, Ordering::Relaxed);
    }
}

impl File for CountingFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.inner.write_at(buf, offset)
    }
    fn direct_align(&self) -> Option<usize> {
        self.inner.direct_align()
    }
    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion {
        self.inner.submit_read(buf, offset)
    }
    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> Result<()> {
        count(self.thread, &self.syncs);
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> Completion<()> {
        self.inner.submit_sync_data()
    }
    fn sync_all(&self) -> Result<()> {
        count(self.thread, &self.syncs);
        self.inner.sync_all()
    }
    fn submit_sync_all(&self) -> Completion<()> {
        self.inner.submit_sync_all()
    }
    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> Result<()> {
        self.inner.set_len(len)
    }
    fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        self.inner.allocate(offset, len)
    }
    fn lock(&self, byte: u64, mode: LockMode) -> Result<()> {
        self.inner.lock(byte, mode)
    }
    fn unlock(&self, byte: u64) -> Result<()> {
        self.inner.unlock(byte)
    }
    fn identity(&self) -> Result<FileIdentity> {
        self.inner.identity()
    }
    fn is_local(&self) -> Result<bool> {
        self.inner.is_local()
    }
}

impl Vfs for Counting {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        Ok(Arc::new(CountingFile {
            inner: self.inner.open(path, opts)?,
            thread: self.thread,
            syncs: Arc::clone(&self.syncs),
        }))
    }
    fn remove(&self, path: &Path) -> Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        self.inner.list_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> Result<()> {
        count(self.thread, &self.syncs);
        self.inner.sync_dir(dir)
    }
    fn submit_sync_dir(&self, dir: &Path) -> Completion<()> {
        self.inner.submit_sync_dir(dir)
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos()
    }
    fn clock_is_simulated(&self) -> bool {
        self.inner.clock_is_simulated()
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

#[test]
fn a_clean_reopen_makes_no_blocking_sync_on_the_opening_thread() {
    let vfs = prepare(21, true);
    let syncs = Arc::new(AtomicU32::new(0));
    let counting: VfsRef = Arc::new(Counting {
        inner: vfs.clone(),
        thread: std::thread::current().id(),
        syncs: Arc::clone(&syncs),
    });
    let db = Pigeonhole::open(DB, options(counting)).unwrap();
    assert_eq!(
        syncs.load(Ordering::Relaxed),
        0,
        "blocking syncs during a clean reopen"
    );
    let t = db.table("t").unwrap().open().unwrap();
    assert!(t.get(&row(0), "f", b"q").unwrap().is_some());
    drop(t);
    db.close().unwrap();
}

#[test]
fn the_first_durable_commit_after_a_clean_reopen_waits_for_the_streams_directory_sync() {
    // D203: open submits the new WAL streams' directory sync and does not wait for it, but a
    // GroupSync commit must not be acknowledged before it: a power loss could otherwise take
    // the stream file's directory entry, and the commit with it.
    let vfs = prepare(31, true);
    vfs.set_deferred_io(true);
    vfs.hold_dir_syncs(true);
    let device = vfs.complete_io_in_background();
    let db = Pigeonhole::open(DB, options(vfs.clone())).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let committer = {
        let db = db.clone();
        std::thread::spawn(move || {
            let t = db.table("t").unwrap().open().unwrap();
            let r = t
                .mutate(b"held")
                .durability(Durability::GroupSync)
                .put("f", b"q", b"v")
                .commit();
            let _ = tx.send(r.is_ok());
        })
    };
    // Held: the commit is written and its own sync completes, but it is not acknowledged.
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(300)).is_err(),
        "a GroupSync commit was acknowledged before the streams' directory sync"
    );
    // A power loss now loses nothing that was acknowledged.
    vfs.crash(CrashKind::Power);
    vfs.hold_dir_syncs(false);
    vfs.set_deferred_io(false);
    let _ = committer.join();
    let _ = rx.try_recv();
    drop(db);
    drop(device);
    vfs.set_faults(FaultPlan::none());
    check(&vfs, None, "power loss while the directory sync was held");

    // Released, the commit completes.
    let vfs = prepare(32, true);
    vfs.set_deferred_io(true);
    vfs.hold_dir_syncs(true);
    let device = vfs.complete_io_in_background();
    let db = Pigeonhole::open(DB, options(vfs.clone())).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let committer = {
        let db = db.clone();
        std::thread::spawn(move || {
            let t = db.table("t").unwrap().open().unwrap();
            let r = t
                .mutate(b"released")
                .durability(Durability::GroupSync)
                .put("f", b"q", b"v")
                .commit();
            let _ = tx.send(r.is_ok());
        })
    };
    assert!(rx.recv_timeout(std::time::Duration::from_millis(300)).is_err());
    vfs.hold_dir_syncs(false);
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(10)),
        Ok(true),
        "the commit completes once the directory sync does"
    );
    committer.join().unwrap();
    db.close().unwrap();
    vfs.set_deferred_io(false);
    drop(device);
    check(&vfs, Some(b"released"), "after the release");
}
