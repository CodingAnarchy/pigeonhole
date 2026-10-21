//! #834 review: a commit stalled for arena room behind a dead reader's pin gets in, with no
//! flush left to run. Every memtable since the reader's snapshot is flushed but kept for
//! the reader, so the writer stalls with nothing to flush; only then is the reader killed.
//! The stalled commit's room recheck (`reserve_room`) is then the only point that frees the
//! dead slot: without it the commit would wait until `write_stall_timeout` and fail `Busy`.
//!
//! Application-owned shards on a clock that moves a nanosecond per read, so room rechecks
//! run as the test drives the shards (a frozen simulated clock refuses a room wait at once).

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{FileRef, OpenOptions, ProcessId, SharedOpen, SharedRegion, Vfs, VfsRef};

const DB: &str = "/db/data.phdb";
const WRITER: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};
const READER: ProcessId = ProcessId {
    pid: 2,
    start_time: 1,
};

/// A clock that moves a nanosecond per read (`SimVfs`'s moves only when a test advances it),
/// so a shard pass with a deadline ends even while a full compaction keeps finding work.
#[derive(Debug)]
struct Ticking {
    inner: VfsRef,
    ticks: AtomicU64,
}

impl Vfs for Ticking {
    fn open(&self, path: &Path, opts: OpenOptions) -> pigeonhole_io::Result<FileRef> {
        self.inner.open(path, opts)
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
        self.inner.monotonic_nanos() + self.ticks.fetch_add(1, Ordering::Relaxed)
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

fn poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    Pin::new(f).poll(&mut Context::from_waker(Waker::noop()))
}

fn pass(shards: &mut [EngineShard]) {
    for s in shards.iter_mut() {
        s.run_once(u64::MAX);
    }
}

#[test]
fn a_commit_stalled_behind_a_dead_readers_pin_gets_in_without_a_flush() {
    let sim = SimVfs::new(8340);
    sim.enter_process(WRITER);
    let mut o = common::options(Arc::clone(&sim), 1, 2 << 20);
    let clock: VfsRef = Arc::new(Ticking {
        inner: Arc::clone(&o.vfs),
        ticks: AtomicU64::new(0),
    });
    o.vfs = Arc::clone(&clock);
    o.tablet_changes = false;
    o.compaction_threads = 0;
    let reader_options = o.clone();
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let (tid, fid) = (t.id, t.families[0].id);
    let mut next = 0u32;
    let mut submit = |db: &Engine| {
        let mut wb = WriteBatch::new();
        wb.put(
            tid,
            fid,
            format!("row{next:06}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[7; 1000]),
        )
        .unwrap();
        next += 1;
        db.submit(wb, Some(Durability::Buffered)).unwrap()
    };

    // A reader process pins the first view.
    sim.enter_process(READER);
    let reader = Engine::open_reader(Path::new(DB), reader_options).unwrap();
    let snap = reader.snapshot().unwrap();
    sim.enter_process(WRITER);

    // Commit until one stalls: every memtable since the pin is flushed but kept, until the
    // arena has no room and nothing left to flush.
    let mut stalled = None;
    for _ in 0..100_000 {
        let mut pc = submit(&db);
        let mut done = false;
        for _ in 0..2_000 {
            if let Poll::Ready(r) = poll(&mut pc) {
                r.expect("the writer refused a commit before it stalled");
                done = true;
                break;
            }
            pass(&mut shards);
        }
        if !done {
            stalled = Some(pc);
            break;
        }
    }
    let mut stalled = stalled.expect("the writer never stalled behind the reader's pin");
    for _ in 0..2_000 {
        pass(&mut shards);
    }
    assert!(
        poll(&mut stalled).is_pending(),
        "the commit got in while the live reader pinned the arena"
    );

    // The reader is killed (no destructors run), and the liveness interval passes.
    sim.crash_process(READER);
    std::mem::forget(snap);
    std::mem::forget(reader);
    sim.advance(300_000_000);

    // The stalled commit's room recheck frees the dead slot and it gets in.
    for _ in 0..200_000 {
        if let Poll::Ready(r) = poll(&mut stalled) {
            r.expect("the stalled commit failed instead of getting in");
            assert_eq!(db.reader_pin_and_view().0, None);
            db.close().unwrap();
            for _ in 0..10_000 {
                pass(&mut shards);
            }
            return;
        }
        pass(&mut shards);
    }
    panic!("the stalled commit never got in after the reader died");
}
