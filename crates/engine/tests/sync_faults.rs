//! Engine-level reproducers for issue #139. A failed sync outside a root commit or a group
//! sync (a page-file growth, a WAL spare-slot preparation) poisons, so no later commit
//! publishes over pages the failed sync may have lost (decision D58). A full disk during a
//! manifest snapshot rewrite refuses that commit but needs no reopen once space is freed,
//! and a refused checkpoint is retried on a timer.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, StreamId};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{
    Completion, ErrorKind, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions, ProcessId,
    Result, SharedOpen, SharedRegion, Vfs, VfsRef,
};

const DB: &str = "/db/data.phdb";

/// Takes one from `n` if it is positive (an armed fault fires once per unit).
fn take_one(n: &AtomicU32) -> bool {
    let mut cur = n.load(Ordering::Acquire);
    while cur > 0 {
        match n.compare_exchange(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(now) => cur = now,
        }
    }
    false
}

/// Fails the next `fail_sync_all` calls of `sync_all`; fails the `sync_all` after the next
/// growth of at least `fail_growth_sync` bytes (once; 0 is off); and fails `allocate` with
/// `NoSpace` while `no_space` is set.
#[derive(Debug, Default)]
struct Faults {
    fail_sync_all: AtomicU32,
    fail_growth_sync: AtomicU64,
    /// Set by a growth `fail_growth_sync` matched: its sync fails.
    growth_armed: AtomicBool,
    no_space: AtomicBool,
}

/// Injects `faults` into the file at `target` only.
#[derive(Debug)]
struct FaultVfs {
    inner: Arc<SimVfs>,
    faults: Arc<Faults>,
    target: PathBuf,
    /// When set, the clock is real time from this instant (it moves on its own).
    real_clock: Option<Instant>,
}

#[derive(Debug)]
struct FaultFile {
    inner: FileRef,
    faults: Arc<Faults>,
}

impl File for FaultFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion {
        self.inner.submit_read(buf, offset)
    }
    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> Result<()> {
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> Completion<()> {
        self.inner.submit_sync_data()
    }
    fn sync_all(&self) -> Result<()> {
        if take_one(&self.faults.fail_sync_all)
            || self.faults.growth_armed.swap(false, Ordering::AcqRel)
        {
            return Err(pigeonhole_io::Error::new(
                ErrorKind::Other,
                "injected sync_all failure",
            ));
        }
        self.inner.sync_all()
    }
    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> Result<()> {
        self.inner.set_len(len)
    }
    fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        if self.faults.no_space.load(Ordering::Acquire) {
            return Err(pigeonhole_io::Error::new(ErrorKind::NoSpace, "allocate"));
        }
        let grows = offset + len > self.inner.len()?;
        let at_least = self.faults.fail_growth_sync.load(Ordering::Acquire);
        if at_least > 0
            && len >= at_least
            && grows
            && self
                .faults
                .fail_growth_sync
                .compare_exchange(at_least, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.faults.growth_armed.store(true, Ordering::Release);
        }
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

impl Vfs for FaultVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        let inner = self.inner.open(path, opts)?;
        if path != self.target {
            return Ok(inner);
        }
        Ok(Arc::new(FaultFile {
            inner,
            faults: Arc::clone(&self.faults),
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
        self.inner.sync_dir(dir)
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
        match self.real_clock {
            Some(start) => self.inner.now_micros() + start.elapsed().as_micros() as u64,
            None => self.inner.now_micros(),
        }
    }
    fn monotonic_nanos(&self) -> u64 {
        match self.real_clock {
            Some(start) => self.inner.monotonic_nanos() + start.elapsed().as_nanos() as u64,
            None => self.inner.monotonic_nanos(),
        }
    }
    fn current_process(&self) -> ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}

fn options(vfs: VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 4 << 20;
    o.memtable_freeze_bytes = 1 << 20;
    o.reader_slots = 8;
    o.wal.segment_size = 8 * 32 * 1024;
    o.wal.spare_segments = 1;
    o
}

/// An engine whose file at `target` (the database file, or a WAL stream) takes faults.
fn open(sim: &Arc<SimVfs>, faults: &Arc<Faults>, target: PathBuf) -> Arc<Engine> {
    open_with_clock(sim, faults, target, None)
}

fn open_with_clock(
    sim: &Arc<SimVfs>,
    faults: &Arc<Faults>,
    target: PathBuf,
    real_clock: Option<Instant>,
) -> Arc<Engine> {
    let vfs: VfsRef = Arc::new(FaultVfs {
        inner: Arc::clone(sim),
        faults: Arc::clone(faults),
        target,
        real_clock,
    });
    Engine::open(Path::new(DB), options(vfs)).unwrap()
}

fn batch(t: &TableInfo, from: u32, rows: u32) -> WriteBatch {
    let mut wb = WriteBatch::new();
    for i in from..from + rows {
        let row = format!("row{i:06}");
        wb.put(
            t.id,
            t.families[0].id,
            row.as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[i as u8; 2048]),
        )
        .unwrap();
    }
    wb
}

fn get(db: &Engine, t: &TableInfo, i: u32) -> Option<Vec<u8>> {
    let snap = db.snapshot().unwrap();
    let row = format!("row{i:06}");
    db.get(&snap, t.id, t.families[0].id, row.as_bytes(), b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
}

fn family() -> Vec<(String, FamilyOptions)> {
    vec![("f".into(), FamilyOptions::default())]
}

#[test]
fn a_failed_page_file_growth_sync_refuses_later_flushes() {
    let sim = SimVfs::new(1395);
    let faults = Arc::new(Faults::default());
    let db = open(&sim, &faults, PathBuf::from(DB));
    let t = db.create_table("t", &family()).unwrap();
    // A flush grows the file for its SST (larger than any manifest extent); that growth's
    // sync fails (on Linux it may have consumed the error for pages written before it).
    faults.fail_growth_sync.store(256 << 10, Ordering::Release);
    for from in (0..256).step_by(64) {
        db.commit(batch(&t, from, 64), Some(Durability::GroupSync))
            .unwrap();
    }
    let next = 256;
    assert!(db.flush().is_err(), "the flush whose growth failed");
    assert_eq!(
        faults.fail_growth_sync.load(Ordering::Acquire),
        0,
        "no SST growth ran"
    );
    // The fault is gone. A flush now would publish a root over possibly lost pages.
    let _ = db.commit(batch(&t, next, 64), Some(Durability::GroupSync));
    assert!(
        db.flush().is_err(),
        "a flush after a failed growth sync committed a root"
    );
    let _ = db.close();
    drop(db);
    // Every acknowledged commit is recovered (from the WAL).
    let db = open(&sim, &faults, PathBuf::from(DB));
    let t = db.table("t").unwrap();
    for i in 0..next {
        assert_eq!(get(&db, &t, i), Some(vec![i as u8; 2048]), "row {i}");
    }
    db.close().unwrap();
}

#[test]
fn a_failed_wal_spare_sync_fails_later_commits() {
    let sim = SimVfs::new(1396);
    let faults = Arc::new(Faults::default());
    let wal = pigeonhole_wal::stream_path(Path::new(DB), StreamId(0));
    let db = open(&sim, &faults, wal);
    let t = db.create_table("t", &family()).unwrap();
    // Segments fill and roll over; the spare preparation (or an inline growth) syncs the
    // stream file, and that sync fails.
    faults.fail_sync_all.store(1, Ordering::Release);
    let mut next = 0;
    let mut acked = Vec::new();
    for _ in 0..200 {
        if faults.fail_sync_all.load(Ordering::Acquire) == 0 {
            break;
        }
        if db
            .commit(batch(&t, next, 8), Some(Durability::GroupSync))
            .is_ok()
        {
            acked.push(next);
        }
        next += 8;
    }
    assert_eq!(
        faults.fail_sync_all.load(Ordering::Acquire),
        0,
        "no slot was synced"
    );
    // The fault is gone, but a group sync now proves nothing about pages the failed sync
    // was told were lost: commits must fail from here on (the failure lands at once, or as
    // soon as the background preparation records it).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let r = db.commit(batch(&t, next, 8), Some(Durability::GroupSync));
        next += 8;
        if r.is_err() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "commits kept succeeding after a failed WAL sync"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let r = db.commit(batch(&t, next, 8), Some(Durability::GroupSync));
    assert!(r.is_err(), "the stream stays poisoned");
    let _ = db.close();
}

#[test]
fn a_full_disk_during_a_manifest_rewrite_needs_no_reopen() {
    let sim = SimVfs::new(1397);
    let faults = Arc::new(Faults::default());
    let db = open(&sim, &faults, PathBuf::from(DB));
    let t = db.create_table("t", &family()).unwrap();
    db.commit(batch(&t, 0, 16), Some(Durability::GroupSync))
        .unwrap();
    // The disk is full: the file cannot grow. Catalog changes rewrite the manifest snapshot
    // (each outgrows the last), and one finds no space for its extents: a held snapshot
    // keeps the replaced manifest extents from being reused.
    let pin = db.snapshot().unwrap();
    faults.no_space.store(true, Ordering::Release);
    let mut refused = None;
    for i in 0..200 {
        if let Err(e) = db.create_table(&format!("x{i}"), &family()) {
            refused = Some(e);
            break;
        }
    }
    assert!(
        matches!(refused, Some(Error::NoSpace)),
        "a manifest rewrite was refused for space: {refused:?}"
    );
    // A flush that cannot grow the file reports the full disk as such.
    db.commit(batch(&t, 16, 16), Some(Durability::GroupSync))
        .unwrap();
    let flushed = db.flush();
    assert!(
        matches!(flushed, Err(Error::NoSpace)),
        "a flush on a full disk: {flushed:?}"
    );
    // The user frees space: writes, flushes and catalog changes work without a reopen.
    faults.no_space.store(false, Ordering::Release);
    drop(pin);
    db.create_table("after", &family()).unwrap();
    db.flush().unwrap();
    for i in 0..32 {
        assert_eq!(get(&db, &t, i), Some(vec![i as u8; 2048]), "row {i}");
    }
    db.close().unwrap();
    drop(db);
    let db = open(&sim, &faults, PathBuf::from(DB));
    assert!(db.table("after").is_some());
    let t = db.table("t").unwrap();
    for i in 0..32 {
        assert_eq!(get(&db, &t, i), Some(vec![i as u8; 2048]), "row {i}");
    }
    db.close().unwrap();
}

fn checkpoint(sim: &Arc<SimVfs>) -> pigeonhole_format::Lsn {
    let vfs: VfsRef = Arc::clone(sim) as VfsRef;
    Engine::inspect_manifest(&vfs, Path::new(DB))
        .unwrap()
        .checkpoints
        .get(&StreamId(0))
        .copied()
        .unwrap_or_default()
}

#[test]
fn a_refused_checkpoint_is_retried_on_an_idle_shard_once_space_is_freed() {
    let sim = SimVfs::new(1398);
    let faults = Arc::new(Faults::default());
    // A real clock: it moves on its own, so a backoff timer can fire.
    let db = open_with_clock(&sim, &faults, PathBuf::from(DB), Some(Instant::now()));
    let t = db.create_table("t", &family()).unwrap();
    // The manifest finds no space for checkpoints: a flush commits, its checkpoint is
    // refused (as a snapshot rewrite that cannot allocate is).
    db.refuse_checkpoints(true);
    db.commit(batch(&t, 0, 32), Some(Durability::GroupSync))
        .unwrap();
    let before = checkpoint(&sim);
    db.flush().unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(checkpoint(&sim), before, "the checkpoint was refused");
    // Space is freed. Nothing else happens on the shard: the backoff timer retries.
    db.refuse_checkpoints(false);
    let deadline = Instant::now() + Duration::from_secs(20);
    while checkpoint(&sim) == before {
        assert!(
            Instant::now() < deadline,
            "an idle shard never retried its refused checkpoint"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    db.close().unwrap();
}
