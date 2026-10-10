//! `submit_commit_root` with syncs that stay pending until the test releases them (a
//! `SimVfs` resolves submitted syncs at once, which would hide ordering bugs):
//!
//! - the superblock is written only after the first sync resolves, and the commit completes
//!   only after the second;
//! - an extent retired by a commit still in flight is not reclaimed, reused or truncated
//!   away, and a crash while the commit is pending recovers the previous root intact.

mod common;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pigeonhole_format::PAGE_SIZE;
use pigeonhole_format::superblock::Superblock;
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{
    Completion, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions, ProcessId, Resolver,
    Result, SharedOpen, SharedRegion, Vfs, VfsRef,
};
use pigeonhole_pager::{Pager, Root};

const PATH: &str = "/db/data.phdb";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    Write(u64),
    SyncSubmitted,
    SyncDone,
}

/// Holds submitted syncs while `held` is set; logs writes and syncs.
#[derive(Debug, Default)]
struct Gate {
    held: AtomicBool,
    pending: Mutex<VecDeque<(FileRef, Resolver<()>)>>,
    log: Mutex<Vec<Event>>,
}

impl Gate {
    fn record(&self, e: Event) {
        self.log.lock().unwrap().push(e);
    }

    fn events(&self) -> Vec<Event> {
        self.log.lock().unwrap().clone()
    }

    fn pending(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Runs the oldest held sync and resolves its completion (running any continuation on
    /// this thread).
    fn release_one(&self) {
        let (file, resolver) = self
            .pending
            .lock()
            .unwrap()
            .pop_front()
            .expect("a held sync");
        let r = file.sync_data();
        self.record(Event::SyncDone);
        resolver.resolve(r);
    }
}

#[derive(Debug)]
struct GateVfs {
    inner: Arc<SimVfs>,
    gate: Arc<Gate>,
}

#[derive(Debug)]
struct GateFile {
    inner: FileRef,
    gate: Arc<Gate>,
}

impl Vfs for GateVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        Ok(Arc::new(GateFile {
            inner: self.inner.open(path, opts)?,
            gate: Arc::clone(&self.gate),
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

impl File for GateFile {
    fn direct_align(&self) -> Option<usize> {
        self.inner.direct_align()
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.gate.record(Event::Write(offset));
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion {
        self.inner.submit_read(buf, offset)
    }
    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        self.gate.record(Event::Write(offset));
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> Result<()> {
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> Completion<()> {
        self.gate.record(Event::SyncSubmitted);
        if !self.gate.held.load(Ordering::SeqCst) {
            let r = self.inner.sync_data();
            self.gate.record(Event::SyncDone);
            return Completion::ready(r);
        }
        let (done, resolver) = Completion::pair();
        self.gate
            .pending
            .lock()
            .unwrap()
            .push_back((Arc::clone(&self.inner), resolver));
        done
    }
    fn sync_all(&self) -> Result<()> {
        self.inner.sync_all()
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

struct Setup {
    sim: Arc<SimVfs>,
    gate: Arc<Gate>,
    pager: Pager,
}

fn setup() -> Setup {
    let sim = SimVfs::new(common::seed());
    let gate = Arc::new(Gate::default());
    let vfs: VfsRef = Arc::new(GateVfs {
        inner: Arc::clone(&sim),
        gate: Arc::clone(&gate),
    });
    let pager = Pager::create(&vfs, Path::new(PATH)).unwrap();
    Setup { sim, gate, pager }
}

fn root_with(extent: pigeonhole_pager::Extent, version: u64) -> Root {
    Root {
        snapshot: Some(extent),
        snapshot_len: 5,
        manifest_version: version,
        ..Root::default()
    }
}

fn superblock_slot(offset: u64) -> bool {
    offset < 2 * PAGE_SIZE as u64
}

fn sequence_at(sim: &Arc<SimVfs>, slot: u64) -> Option<u64> {
    let vfs: VfsRef = sim.clone();
    let file = vfs.open(Path::new(PATH), OpenOptions::read()).unwrap();
    let mut page = [0u8; PAGE_SIZE];
    file.read_at(&mut page, slot * PAGE_SIZE as u64).unwrap();
    Superblock::decode(&page).ok().map(|sb| sb.sequence)
}

#[test]
fn superblock_is_written_only_after_the_first_sync_resolves() {
    let s = setup();
    let e = s.pager.allocate(1).unwrap();
    s.pager.write(e, 0, b"first").unwrap();
    s.pager.commit_root(root_with(e, 1)).unwrap(); // slot B, sequence 2

    s.gate.held.store(true, Ordering::SeqCst);
    let mark = s.gate.events().len();
    s.pager.write(e, 4096, b"delta").unwrap();
    let c = s.pager.submit_commit_root(root_with(e, 2));
    assert!(!c.is_ready());
    assert_eq!(s.gate.pending(), 1, "first sync submitted and held");
    let since: Vec<Event> = s.gate.events()[mark..].to_vec();
    assert!(
        !since
            .iter()
            .any(|ev| matches!(ev, Event::Write(o) if superblock_slot(*o))),
        "superblock written before the first sync resolved: {since:?}"
    );
    assert_eq!(sequence_at(&s.sim, 0), Some(1), "slot A untouched");
    assert_eq!(s.pager.root(), root_with(e, 1));

    // First sync resolves: the superblock goes to slot A, then the second sync is held.
    s.gate.release_one();
    let since: Vec<Event> = s.gate.events()[mark..].to_vec();
    let done = since.iter().position(|e| *e == Event::SyncDone).unwrap();
    let sb = since
        .iter()
        .position(|ev| matches!(ev, Event::Write(o) if superblock_slot(*o)))
        .expect("superblock written after the first sync");
    assert!(done < sb, "{since:?}");
    assert_eq!(since[sb], Event::Write(0), "the non-current slot");
    assert_eq!(sequence_at(&s.sim, 0), Some(3));
    assert!(!c.is_ready(), "not committed before the second sync");
    assert_eq!(s.pager.root(), root_with(e, 1));
    assert_eq!(s.gate.pending(), 1);

    s.gate.release_one();
    c.wait().unwrap();
    assert_eq!(s.pager.root(), root_with(e, 2));
}

#[test]
fn retired_extent_survives_a_crash_during_its_commit() {
    for release_first_sync in [false, true] {
        let s = setup();
        let old = s.pager.allocate(1).unwrap();
        s.pager.write(old, 0, b"old!!").unwrap();
        s.pager.commit_root(root_with(old, 1)).unwrap();

        s.gate.held.store(true, Ordering::SeqCst);
        let new = s.pager.allocate(1).unwrap();
        s.pager.write(new, 0, b"new!!").unwrap();
        let c = s.pager.submit_commit_root(root_with(new, 2));
        // The engine retires and reclaims as soon as it submits; no view uses version 1.
        s.pager.retire(old, 2);
        assert_eq!(s.pager.reclaim(2), 0, "version 2 is not durable yet");
        assert_eq!(s.pager.stats().retired_bytes, old.len());
        for _ in 0..4 {
            let e = s.pager.allocate(1).unwrap();
            assert_ne!(e, old, "an extent the durable root references was reused");
            s.pager.write(e, 0, b"junk!").unwrap();
        }
        assert_eq!(
            s.pager.truncate_tail().unwrap(),
            0,
            "no truncation mid-commit"
        );
        if release_first_sync {
            s.gate.release_one(); // superblock written, second sync still held
        }

        s.sim.crash(CrashKind::Power);
        drop(c);
        let vfs: VfsRef = s.sim.clone();
        let opened = Pager::open(&vfs, Path::new(PATH), false).unwrap();
        let root = opened.root();
        let expected = if root.manifest_version == 1 { old } else { new };
        assert!(
            root == root_with(old, 1) || root == root_with(new, 2),
            "{root:?}"
        );
        let mut buf = [0u8; 5];
        opened.file().read_at(&mut buf, expected.offset()).unwrap();
        let want: &[u8; 5] = if expected == old { b"old!!" } else { b"new!!" };
        assert_eq!(&buf, want, "release_first_sync={release_first_sync}");
    }
}

#[test]
fn reclaim_proceeds_once_the_commit_is_durable() {
    let s = setup();
    let old = s.pager.allocate(1).unwrap();
    s.pager.commit_root(root_with(old, 1)).unwrap();
    s.gate.held.store(true, Ordering::SeqCst);
    let new = s.pager.allocate(1).unwrap();
    let c = s.pager.submit_commit_root(root_with(new, 2));
    s.pager.retire(old, 2);
    assert_eq!(s.pager.reclaim(2), 0);
    s.gate.release_one();
    s.gate.release_one();
    c.wait().unwrap();
    assert_eq!(s.pager.reclaim(2), 1);
    assert_eq!(s.pager.allocate(1).unwrap(), old);
}
