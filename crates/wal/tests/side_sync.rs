//! Failed syncs of a stream file that no commit waits on (issue #139, decision D58): the
//! spare-slot preparation's and an inline growth's `sync_all`. On Linux an fsync error is
//! reported to one sync only, so such a sync may have been handed the error for appended
//! records, and the next group sync would succeed over their lost pages. Such a failure
//! poisons the stream, and a group sync that succeeded while one failed does not count. A
//! failed allocation (a full disk) syncs nothing and leaves the stream usable.

mod common;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use common::*;
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{
    Completion, ErrorKind, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions, ProcessId,
    Resolver, Result, SharedOpen, SharedRegion, Vfs, VfsRef,
};
use pigeonhole_wal::{Error, Wal, WalStream};

/// Fails the next `fail_sync_all` calls of `sync_all`, fails `allocate` with `NoSpace` while
/// `no_space` is set, and holds submitted syncs while `hold` is set.
#[derive(Debug, Default)]
struct Faults {
    fail_sync_all: AtomicU32,
    no_space: AtomicBool,
    hold: AtomicBool,
    held: Mutex<VecDeque<(FileRef, Resolver<()>)>>,
}

impl Faults {
    /// Runs the oldest held sync and resolves it (running its continuation here).
    fn release_one(&self) {
        let (file, resolver) = self.held.lock().unwrap().pop_front().expect("a held sync");
        resolver.resolve(file.sync_data());
    }

    fn held(&self) -> usize {
        self.held.lock().unwrap().len()
    }
}

#[derive(Debug)]
struct FaultVfs {
    inner: Arc<SimVfs>,
    faults: Arc<Faults>,
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
        if !self.faults.hold.load(Ordering::Acquire) {
            return self.inner.submit_sync_data();
        }
        let (done, resolver) = Completion::pair();
        self.faults
            .held
            .lock()
            .unwrap()
            .push_back((Arc::clone(&self.inner), resolver));
        done
    }
    fn sync_all(&self) -> Result<()> {
        let armed =
            self.faults
                .fail_sync_all
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
        if armed.is_ok() {
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
        Ok(Arc::new(FaultFile {
            inner: self.inner.open(path, opts)?,
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
}

/// A stream (4 frames per segment, one spare) over a fault-injecting `SimVfs`.
fn setup(seed: u64) -> (Arc<Faults>, WalStream) {
    let faults = Arc::new(Faults::default());
    let vfs: VfsRef = Arc::new(FaultVfs {
        inner: SimVfs::new(seed),
        faults: Arc::clone(&faults),
    });
    let wal = WalStream::create(&vfs, db(), STREAM, DB_ID, opts(4, 1)).unwrap();
    (faults, wal)
}

#[test]
fn a_failed_spare_sync_poisons_the_stream() {
    let (faults, mut wal) = setup(1391);
    let t1 = wal
        .append(&batch(1, 100).record(), Durability::GroupSync)
        .unwrap();
    wal.write().unwrap();
    // The spare preparation's sync fails (and on Linux may have consumed the error for
    // record 1's pages).
    faults.fail_sync_all.store(1, Ordering::Release);
    let spares = wal.spares();
    assert!(spares.prepare(spares.target() + 2).is_err());
    // The fault is gone, but record 1 must not be acknowledged as durable.
    assert!(matches!(wal.sync(), Err(Error::Poisoned)));
    assert!(!wal.satisfies(&t1));
    assert!(matches!(wal.submit_sync(), Err(Error::Poisoned)));
    assert!(matches!(
        wal.append(&batch(2, 10).record(), Durability::Buffered),
        Err(Error::Poisoned)
    ));
}

#[test]
fn a_spare_sync_failing_during_a_group_sync_fails_the_group() {
    let (faults, mut wal) = setup(1392);
    let t1 = wal
        .append(&batch(1, 100).record(), Durability::GroupSync)
        .unwrap();
    faults.hold.store(true, Ordering::Release);
    let synced = wal.submit_sync().unwrap();
    assert_eq!(faults.held(), 1);
    // While the group sync is in flight, the spare preparation's sync fails: the group's
    // own sync then succeeds, possibly only because the other was handed its error.
    faults.fail_sync_all.store(1, Ordering::Release);
    let spares = wal.spares();
    assert!(spares.prepare(spares.target() + 2).is_err());
    faults.release_one();
    assert!(synced.wait().is_err(), "the group sync must not count");
    assert!(!wal.satisfies(&t1));
    assert!(wal.durable() < t1.end);
    faults.hold.store(false, Ordering::Release);
    assert!(matches!(wal.sync(), Err(Error::Poisoned)));
}

#[test]
fn a_failed_inline_growth_sync_poisons_the_stream() {
    let (faults, mut wal) = setup(1393);
    // Fill segments until one rolls over with no spare ready: the stream grows inline,
    // and that growth's sync fails.
    faults.fail_sync_all.store(1, Ordering::Release);
    let mut failed = None;
    for i in 1..=64u64 {
        match wal.append(&batch(i, 30_000).record(), Durability::GroupSync) {
            Ok(_) => {}
            Err(e) => {
                failed = Some(e);
                break;
            }
        }
    }
    assert!(
        matches!(failed, Some(Error::Io(_))),
        "an inline growth ran: {failed:?}"
    );
    assert!(matches!(wal.sync(), Err(Error::Poisoned)));
}

#[test]
fn a_spare_allocation_without_space_leaves_the_stream_usable() {
    let (faults, mut wal) = setup(1394);
    let t1 = wal
        .append(&batch(1, 100).record(), Durability::GroupSync)
        .unwrap();
    faults.no_space.store(true, Ordering::Release);
    let spares = wal.spares();
    assert!(spares.prepare(spares.target() + 2).is_err());
    faults.no_space.store(false, Ordering::Release);
    wal.sync().unwrap();
    assert!(wal.satisfies(&t1));
    // The space is back: preparation succeeds.
    assert!(spares.prepare(spares.target() + 2).unwrap() > 0);
}
