//! Failed syncs outside a root commit (issue #139, decision D58). On Linux an fsync error is
//! reported to one sync only, so a growth's or truncation's failed `sync_all` may have been
//! handed the error for pages a flush wrote, and the next commit's sync would succeed over
//! them. Such a failure poisons the pager like a failed commit; a commit whose own syncs
//! succeeded while one failed is refused before its superblock is written. A full disk
//! (`NoSpace` from the growth itself) syncs nothing and leaves the pager usable.

mod common;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{
    Completion, ErrorKind, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions, ProcessId,
    Resolver, Result, SharedOpen, SharedRegion, Vfs, VfsRef,
};
use pigeonhole_pager::{Error, Pager, Root};

const PATH: &str = "/db/data.phdb";

fn path() -> &'static Path {
    Path::new(PATH)
}

fn root(version: u64) -> Root {
    Root {
        manifest_version: version,
        ..Root::default()
    }
}

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

/// Fails the next `fail_sync_all` calls of `sync_all`, fails `allocate` with `NoSpace` while
/// `no_space` is set, holds submitted syncs while `hold` is set, and holds `sync_all` itself
/// while the gate is closed.
#[derive(Debug, Default)]
struct Faults {
    fail_sync_all: AtomicU32,
    no_space: AtomicBool,
    hold: AtomicBool,
    held: Mutex<VecDeque<(FileRef, Resolver<()>)>>,
    /// Closed: `sync_all` waits for it to open (a growth's sync in flight). Whether one waits.
    gate: Mutex<(bool, bool)>,
    gate_moved: Condvar,
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

    fn close_gate(&self) {
        self.gate.lock().unwrap().0 = true;
    }

    /// Waits until a `sync_all` waits at the closed gate.
    fn wait_at_gate(&self) {
        let mut g = self.gate.lock().unwrap();
        while !g.1 {
            g = self.gate_moved.wait(g).unwrap();
        }
    }

    fn open_gate(&self) {
        self.gate.lock().unwrap().0 = false;
        self.gate_moved.notify_all();
    }

    fn pass_gate(&self) {
        let mut g = self.gate.lock().unwrap();
        while g.0 {
            g.1 = true;
            self.gate_moved.notify_all();
            g = self.gate_moved.wait(g).unwrap();
        }
        g.1 = false;
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
    fn direct_align(&self) -> Option<usize> {
        self.inner.direct_align()
    }

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
        self.faults.pass_gate();
        if take_one(&self.faults.fail_sync_all) {
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

/// A pager over a fault-injecting `SimVfs`, with root 1 committed.
fn setup() -> (VfsRef, Arc<Faults>, Pager) {
    let faults = Arc::new(Faults::default());
    let vfs: VfsRef = Arc::new(FaultVfs {
        inner: SimVfs::new(common::seed()),
        faults: Arc::clone(&faults),
    });
    let pager = Pager::create(&vfs, path()).unwrap();
    pager.commit_root(root(1)).unwrap();
    (vfs, faults, pager)
}

fn reopened_root(vfs: &VfsRef) -> Root {
    Pager::open(vfs, path(), true).unwrap().root()
}

#[test]
fn a_failed_growth_sync_poisons_later_commits() {
    let (vfs, faults, pager) = setup();
    // A flush's output, written but not yet synced.
    let sst = pager.allocate(64 << 10).unwrap();
    pager.write(sst, 0, &[7; 4096]).unwrap();
    // The next growth's sync fails (and on Linux may have consumed the error for `sst`).
    faults.fail_sync_all.store(1, Ordering::Release);
    assert!(pager.allocate(256 << 10).is_err());
    // The fault is gone, but a commit naming `sst` must not succeed now.
    let err = pager.commit_root(root(2)).unwrap_err();
    assert!(err.to_string().contains("reopen"), "{err}");
    assert!(pager.submit_commit_root(root(2)).wait().is_err());
    drop(pager);
    assert_eq!(reopened_root(&vfs), root(1));
}

#[test]
fn a_failed_truncation_sync_poisons_later_commits() {
    let (vfs, faults, pager) = setup();
    let kept = pager.allocate(64 << 10).unwrap();
    pager.write(kept, 0, &[7; 4096]).unwrap();
    let tail = pager.allocate(64 << 10).unwrap();
    pager.abandon(tail);
    faults.fail_sync_all.store(1, Ordering::Release);
    assert!(pager.truncate_tail().is_err());
    assert!(pager.commit_root(root(2)).is_err());
    drop(pager);
    assert_eq!(reopened_root(&vfs), root(1));
}

#[test]
fn a_growth_sync_failing_during_a_commit_fails_the_commit_before_its_superblock() {
    let (vfs, faults, pager) = setup();
    let sst = pager.allocate(64 << 10).unwrap();
    pager.write(sst, 0, &[7; 4096]).unwrap();
    // The commit's first sync is in flight when a growth's sync fails: the commit's own
    // sync then succeeds, possibly only because the growth's was handed its error.
    faults.hold.store(true, Ordering::Release);
    let done = pager.submit_commit_root(root(2));
    assert_eq!(faults.held(), 1);
    faults.fail_sync_all.store(1, Ordering::Release);
    assert!(pager.allocate(256 << 10).is_err());
    faults.release_one();
    let second_sync = faults.held();
    while faults.held() > 0 {
        faults.release_one();
    }
    assert!(done.wait().is_err(), "the commit must not succeed");
    assert_eq!(second_sync, 0, "no superblock written, no second sync");
    assert_eq!(pager.root(), root(1));
    faults.hold.store(false, Ordering::Release);
    assert!(pager.commit_root(root(3)).is_err());
    drop(pager);
    assert_eq!(reopened_root(&vfs), root(1));
}

#[test]
fn a_commit_never_waits_for_a_growth_sync_on_the_thread_that_completes_its_sync() {
    // #182: a growth's `sync_all` is in flight on another thread when the commit's first sync
    // completes. The thread resolving that sync (the I/O backend's) must not wait for the
    // growth: the commit's next step parks, and the growth resumes it when its sync ends.
    // If that sync failed, the commit fails before its superblock (D58).
    for fail in [false, true] {
        let (vfs, faults, pager) = setup();
        let pager = Arc::new(pager);
        let sst = pager.allocate(64 << 10).unwrap();
        pager.write(sst, 0, &[7; 4096]).unwrap();
        faults.close_gate();
        if fail {
            faults.fail_sync_all.store(1, Ordering::Release);
        }
        let grower = std::thread::spawn({
            let pager = Arc::clone(&pager);
            move || pager.allocate(256 << 10).map(drop)
        });
        faults.wait_at_gate();
        faults.hold.store(true, Ordering::Release);
        let done = pager.submit_commit_root(root(2));
        assert_eq!(faults.held(), 1, "the first sync");
        // Resolved on another thread, as the backend's would be: it must return while the
        // growth is still held (before #182 it waited for the growth's sync).
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn({
            let faults = Arc::clone(&faults);
            move || {
                faults.release_one();
                let _ = tx.send(());
            }
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("the thread resolving the commit's sync waited for the growth");
        assert!(
            !done.is_ready(),
            "fail {fail}: the commit went on past a growth in flight"
        );
        assert_eq!(
            faults.held(),
            0,
            "fail {fail}: no second sync before the growth ends"
        );
        faults.hold.store(false, Ordering::Release);
        faults.open_gate();
        assert_eq!(grower.join().unwrap().is_err(), fail);
        let r = done.wait();
        assert_eq!(r.is_err(), fail, "fail {fail}: {r:?}");
        let want = if fail { root(1) } else { root(2) };
        assert_eq!(pager.root(), want);
        drop(pager);
        assert_eq!(reopened_root(&vfs), want);
    }
}

#[test]
fn a_growth_without_space_leaves_the_pager_usable() {
    let (vfs, faults, pager) = setup();
    faults.no_space.store(true, Ordering::Release);
    assert!(matches!(pager.allocate(256 << 10), Err(Error::NoSpace)));
    // Space is freed: allocation and commits work without a reopen.
    faults.no_space.store(false, Ordering::Release);
    let e = pager.allocate(256 << 10).unwrap();
    pager.write(e, 0, &[1; 4096]).unwrap();
    pager.commit_root(root(2)).unwrap();
    drop(pager);
    assert_eq!(reopened_root(&vfs), root(2));
}
