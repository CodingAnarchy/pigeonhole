//! Real files through `PreadVfs`, the non-blocking shape of `submit_sync`, and poisoning after
//! a failed write or sync.

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use common::*;
use pigeonhole_format::wal::WalRecord;
use pigeonhole_format::{Durability, Lsn, StreamId};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{
    Completion, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions, ProcessId, Resolver,
    SharedOpen, SharedRegion, Vfs, VfsRef,
};
use pigeonhole_wal::{Error, Recovery, Wal, WalStream, discover_streams};

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("pigeonhole-wal-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn real_files_roundtrip_and_are_removed() {
    let dir = TempDir::new("roundtrip");
    let db = dir.0.join("data.phdb");
    let vfs: VfsRef = real_vfs();
    let opts = opts(4, 1);
    let mut wal = WalStream::create(&vfs, &db, StreamId(0), DB_ID, opts).unwrap();
    let path = pigeonhole_wal::stream_path(&db, StreamId(0));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), opts.segment_size);
    assert_eq!(wal.spares().prepare(1).unwrap(), 1);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        2 * opts.segment_size
    );
    let mut tickets = Vec::new();
    for i in 1..=20u64 {
        tickets.push(
            wal.append(&batch(i, 30_000).record(), Durability::GroupSync)
                .unwrap(),
        );
        if i % 5 == 0 {
            wal.write().unwrap();
            let lsn = wal.submit_sync().unwrap().wait().unwrap();
            assert_eq!(lsn, tickets.last().unwrap().end);
            assert!(wal.durable() >= lsn);
        }
    }
    for t in &tickets {
        assert!(wal.satisfies(t));
    }
    let cp = tickets[9].end;
    wal.checkpoint(cp).unwrap();
    drop(wal);
    assert_eq!(discover_streams(&vfs, &db).unwrap(), [StreamId(0)]);

    let mut r = Recovery::open(&vfs, &db, StreamId(0), DB_ID, cp).unwrap();
    let mut seqnos = Vec::new();
    while let Some((end, rec)) = r.next_record().unwrap() {
        if let WalRecord::Batch { seqno, .. } = rec {
            assert_eq!(end, tickets[seqno as usize - 1].end);
            seqnos.push(seqno);
        }
    }
    assert_eq!(seqnos, (11..=20).collect::<Vec<_>>());
    let wal = r.into_stream(opts).unwrap();
    assert!(wal.written().epoch() > tickets.last().unwrap().end.epoch());
    Box::new(wal).remove().unwrap();
    assert!(!path.exists());
    assert!(discover_streams(&vfs, &db).unwrap().is_empty());
}

/// Syncs submitted through a [`Gated`] vfs and not yet completed.
type Pending = Arc<Mutex<Vec<(FileRef, Resolver<()>)>>>;

/// A `Vfs` over `SimVfs` whose submitted syncs complete only when the test releases them,
/// and whose blocking writes and syncs fail while `fail` is set.
#[derive(Debug)]
struct Gated {
    inner: Arc<SimVfs>,
    pending: Pending,
    fail: Arc<AtomicBool>,
}

impl Gated {
    fn new(seed: u64) -> Arc<Self> {
        Arc::new(Self {
            inner: SimVfs::new(seed),
            pending: Default::default(),
            fail: Default::default(),
        })
    }

    /// Runs every held sync now.
    fn release(&self) {
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());
        for (file, resolver) in pending {
            resolver.resolve(file.sync_data());
        }
    }

    fn held(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::Relaxed);
    }
}

fn injected() -> pigeonhole_io::Error {
    pigeonhole_io::Error::new(pigeonhole_io::ErrorKind::Other, "injected")
}

#[derive(Debug)]
struct GatedFile {
    inner: FileRef,
    pending: Pending,
    fail: Arc<AtomicBool>,
}

impl File for GatedFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> pigeonhole_io::Result<()> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(injected());
        }
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion {
        self.inner.submit_read(buf, offset)
    }
    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> pigeonhole_io::Result<()> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(injected());
        }
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> Completion<()> {
        let (done, resolver) = Completion::pair();
        self.pending
            .lock()
            .unwrap()
            .push((Arc::clone(&self.inner), resolver));
        done
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
    fn lock(&self, byte: u64, mode: LockMode) -> pigeonhole_io::Result<()> {
        self.inner.lock(byte, mode)
    }
    fn unlock(&self, byte: u64) -> pigeonhole_io::Result<()> {
        self.inner.unlock(byte)
    }
    fn identity(&self) -> pigeonhole_io::Result<FileIdentity> {
        self.inner.identity()
    }
    fn is_local(&self) -> pigeonhole_io::Result<bool> {
        self.inner.is_local()
    }
}

impl Vfs for Gated {
    fn open(&self, path: &Path, opts: OpenOptions) -> pigeonhole_io::Result<FileRef> {
        Ok(Arc::new(GatedFile {
            inner: self.inner.open(path, opts)?,
            pending: Arc::clone(&self.pending),
            fail: Arc::clone(&self.fail),
        }))
    }
    fn remove(&self, path: &Path) -> pigeonhole_io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> pigeonhole_io::Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> pigeonhole_io::Result<Vec<PathBuf>> {
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
        mode: SharedOpen,
    ) -> pigeonhole_io::Result<SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> pigeonhole_io::Result<()> {
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
}

#[test]
fn submit_sync_lets_the_shard_build_the_next_group() {
    let gated = Gated::new(77);
    let vfs: VfsRef = gated.clone();
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts(4, 1)).unwrap();

    // Group 1: appended, written, sync submitted but not yet complete.
    let t1 = wal
        .append(&batch(1, 100).record(), Durability::GroupSync)
        .unwrap();
    let t1b = wal
        .append(&batch(2, 100).record(), Durability::Buffered)
        .unwrap();
    let c1 = wal.submit_sync().unwrap();
    assert_eq!(gated.held(), 1);
    assert!(!c1.is_ready());
    assert!(wal.satisfies(&t1b), "Buffered is met by the write");
    assert!(!wal.satisfies(&t1), "GroupSync waits for the sync");

    // Group 2 is built and submitted while the first sync is still pending.
    let t2 = wal
        .append(&batch(3, 40_000).record(), Durability::GroupSync)
        .unwrap();
    assert_eq!(wal.write().unwrap(), t2.end);
    let c2 = wal.submit_sync().unwrap();
    assert_eq!(gated.held(), 2);
    assert_eq!(wal.written(), t2.end);
    assert!(wal.durable() < t1.end);

    // Completing the syncs resolves the committers and moves `durable`.
    gated.release();
    assert_eq!(c1.wait().unwrap(), t1b.end);
    assert!(wal.satisfies(&t1));
    assert_eq!(c2.wait().unwrap(), t2.end);
    assert_eq!(wal.durable(), t2.end);
    assert!(wal.satisfies(&t2));

    // A failed sync (here: abandoned, its resolver dropped) leaves `durable` alone and
    // poisons the stream: nothing more can be appended, written or synced.
    let t3 = wal
        .append(&batch(4, 10).record(), Durability::GroupSync)
        .unwrap();
    let c3 = wal.submit_sync().unwrap();
    gated.pending.lock().unwrap().clear();
    assert!(c3.wait().is_err());
    assert!(!wal.satisfies(&t3));
    assert_eq!(wal.durable(), t2.end);
    assert!(matches!(wal.sync(), Err(Error::Poisoned)));
    assert!(matches!(wal.write(), Err(Error::Poisoned)));
    assert!(matches!(wal.submit_sync(), Err(Error::Poisoned)));
    assert!(matches!(
        wal.append(&batch(5, 10).record(), Durability::Buffered),
        Err(Error::Poisoned)
    ));
    assert!(!wal.satisfies(&t3));
    assert_eq!(wal.written(), t3.end, "positions are still readable");
    wal.checkpoint(t1.end).unwrap();
    drop(wal);
    // Recovery reopens it: what reached the kernel is there, and the fresh stream works.
    let (got, r) = replay(&vfs, Lsn::default()).unwrap();
    assert_eq!(got.seqnos(), [1, 2, 3, 4]);
    let mut wal = r.into_stream(opts(4, 1)).unwrap();
    let t = wal
        .append(&batch(6, 10).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    assert!(wal.satisfies(&t));
    assert_eq!(Lsn::default(), Lsn(0));
}

#[test]
fn a_failed_blocking_sync_or_write_poisons_the_stream() {
    let gated = Gated::new(78);
    let vfs: VfsRef = gated.clone();
    let mut wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts(4, 1)).unwrap();
    let t1 = wal
        .append(&batch(1, 10).record(), Durability::GroupSync)
        .unwrap();
    gated.set_fail(true);
    assert!(matches!(wal.sync(), Err(Error::Io(_))));
    assert!(!wal.satisfies(&t1));
    gated.set_fail(false);
    assert!(
        matches!(wal.sync(), Err(Error::Poisoned)),
        "poison outlives the fault"
    );
    assert!(matches!(
        wal.append(&batch(2, 10).record(), Durability::Buffered),
        Err(Error::Poisoned)
    ));

    // A failed write poisons too.
    let mut wal = WalStream::create(&vfs, db(), StreamId(1), DB_ID, opts(2, 1)).unwrap();
    wal.append(&batch(1, 10).record(), Durability::Buffered)
        .unwrap();
    gated.set_fail(true);
    assert!(matches!(wal.write(), Err(Error::Io(_))));
    gated.set_fail(false);
    assert!(matches!(wal.write(), Err(Error::Poisoned)));

    // Including the write and sync a full segment triggers inside append.
    let mut wal = WalStream::create(&vfs, db(), StreamId(2), DB_ID, opts(2, 1)).unwrap();
    wal.append(&batch(1, 20_000).record(), Durability::Buffered)
        .unwrap();
    gated.set_fail(true);
    assert!(matches!(
        wal.append(&batch(2, 20_000).record(), Durability::Buffered),
        Err(Error::Io(_))
    ));
    gated.set_fail(false);
    assert!(matches!(wal.sync(), Err(Error::Poisoned)));

    // Argument errors never poison.
    let mut wal = WalStream::create(&vfs, db(), StreamId(3), DB_ID, opts(2, 1)).unwrap();
    assert!(matches!(
        wal.append(&batch(1, 100_000).record(), Durability::Buffered),
        Err(Error::RecordTooLarge)
    ));
    wal.append(&batch(1, 10).record(), Durability::Buffered)
        .unwrap();
    wal.sync().unwrap();
}

/// Real files through the backend `PIGEONHOLE_IO` names: `uring` runs on io_uring (Linux;
/// fails where it is unavailable, #402), anything else on `pread`.
fn real_vfs() -> pigeonhole_io::VfsRef {
    match std::env::var("PIGEONHOLE_IO").as_deref() {
        #[cfg(target_os = "linux")]
        Ok("uring") => pigeonhole_io::uring::UringVfs::new().expect("io_uring is available"),
        #[cfg(not(target_os = "linux"))]
        Ok("uring") => panic!("PIGEONHOLE_IO=uring needs Linux"),
        _ => pigeonhole_io::pread::PreadVfs::new(2),
    }
}
