//! D240: the write-stall timeout is each waiting write's own. A commit is refused with
//! `Busy` once it has waited `write_stall_timeout_nanos` for arena room itself, not when the
//! shard has been short of room that long: a commit that started waiting later keeps its
//! place, and commits once room frees. Application-owned shard on `SimVfs`, the clock moved
//! by hand (and by a nanosecond at every reading, so the stall's timers never take it for
//! stopped); in-process snapshots pin the memtables, so nothing frees room until they drop.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, Error, FamilyOptions, PendingCommit, Snapshot, TableInfo,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const TIMEOUT: u64 = 100_000_000;

struct Db {
    vfs: Arc<SimVfs>,
    clock: pigeonhole_io::VfsRef,
    engine: Arc<Engine>,
    shards: Vec<EngineShard>,
    table: Arc<TableInfo>,
}

fn open() -> Db {
    let vfs = SimVfs::new(240);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    o.memtable_budget = 1 << 20;
    o.memtable_freeze_bytes = 64 << 10;
    o.arena_chunk_bytes = Some(32 << 10);
    o.write_stall_timeout_nanos = TIMEOUT;
    o.vfs = Arc::new(Ticking {
        inner: Arc::clone(&o.vfs),
        ticks: AtomicU64::new(0),
    });
    let clock = Arc::clone(&o.vfs);
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/deadline.phdb"), o).expect("open");
    let table = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    Db {
        vfs,
        clock,
        engine,
        shards,
        table,
    }
}

impl Db {
    /// One turn of the shard, the clock moved `nanos` first.
    fn step(&mut self, nanos: u64) {
        self.vfs.advance(nanos);
        let now = self.engine_now();
        for s in &mut self.shards {
            s.run_once(now + 1_000);
        }
    }

    fn engine_now(&self) -> u64 {
        self.clock.monotonic_nanos()
    }

    fn submit(&self, row: &[u8]) -> PendingCommit {
        let mut wb = WriteBatch::new();
        wb.put(
            self.table.id,
            self.table.families[0].id,
            row,
            b"q",
            None,
            ValueRef::Bytes(&[0x5A; 2048]),
        )
        .unwrap();
        self.engine
            .submit(wb, Some(Durability::Buffered))
            .expect("submit")
    }
}

fn poll(
    pc: &mut PendingCommit,
) -> Option<pigeonhole_engine::Result<pigeonhole_engine::CommitInfo>> {
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(pc).poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
}

/// Commits rows, pinning every memtable with a snapshot, until one commit waits for room;
/// returns it with the snapshots.
fn stall(db: &mut Db) -> (PendingCommit, Vec<Snapshot>) {
    let mut pins = Vec::new();
    for i in 0..10_000u32 {
        pins.push(db.engine.snapshot().unwrap());
        let mut pc = db.submit(format!("row{i:05}").as_bytes());
        for _ in 0..20 {
            db.step(1_000);
            if let Some(r) = poll(&mut pc) {
                r.expect("commit");
                break;
            }
        }
        if poll(&mut pc).is_none() {
            return (pc, pins);
        }
    }
    panic!("the arena never filled")
}

/// Steps the clock `total` nanos forward in `n` steps.
fn run_for(db: &mut Db, total: u64, n: u64) {
    for _ in 0..n {
        db.step(total / n);
    }
}

#[test]
fn a_commit_that_starts_waiting_later_is_not_refused_at_an_earlier_ones_deadline() {
    let mut db = open();
    let (mut first, pins) = stall(&mut db);
    run_for(&mut db, TIMEOUT * 6 / 10, 60);
    assert!(
        poll(&mut first).is_none(),
        "the first commit should still wait"
    );
    let mut second = db.submit(b"second");
    db.step(1_000);
    assert!(
        poll(&mut second).is_none(),
        "the second commit should wait too"
    );
    // Past the first commit's deadline, short of the second's: only the first is refused,
    // although the shard has now been short of room for longer than the timeout.
    run_for(&mut db, TIMEOUT * 5 / 10, 50);
    let r = poll(&mut first).expect("the first commit is refused at its deadline");
    assert!(matches!(r, Err(Error::Busy)), "{r:?}");
    assert!(
        poll(&mut second).is_none(),
        "the second commit was refused at the first one's deadline"
    );
    // Room frees before the second commit's own deadline: it commits.
    drop(pins);
    for _ in 0..2_000 {
        db.step(10_000);
        if let Some(r) = poll(&mut second) {
            r.expect("the second commit");
            db.engine.close().unwrap();
            for _ in 0..8 {
                db.step(1_000);
            }
            return;
        }
    }
    panic!("the second commit never resolved");
}

#[test]
fn a_waiting_commit_is_still_refused_once_it_waited_the_whole_timeout() {
    let mut db = open();
    let (mut first, _pins) = stall(&mut db);
    let mut second = db.submit(b"second");
    run_for(&mut db, TIMEOUT * 9 / 10, 90);
    assert!(poll(&mut first).is_none());
    assert!(poll(&mut second).is_none());
    run_for(&mut db, TIMEOUT * 2 / 10, 20);
    for pc in [&mut first, &mut second] {
        let r = poll(pc).expect("refused at the timeout");
        assert!(matches!(r, Err(Error::Busy)), "{r:?}");
    }
    db.engine.close().unwrap();
    for _ in 0..8 {
        db.step(1_000);
    }
}

/// `SimVfs` whose clock also moves a nanosecond at every reading.
#[derive(Debug)]
struct Ticking {
    inner: pigeonhole_io::VfsRef,
    ticks: AtomicU64,
}

impl pigeonhole_io::Vfs for Ticking {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<pigeonhole_io::FileRef> {
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
        mode: pigeonhole_io::SharedOpen,
    ) -> pigeonhole_io::Result<pigeonhole_io::SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> pigeonhole_io::Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros() + self.ticks.fetch_add(1, Ordering::Relaxed) / 1_000
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos() + self.ticks.fetch_add(1, Ordering::Relaxed)
    }
    fn clock_is_simulated(&self) -> bool {
        self.inner.clock_is_simulated()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}
