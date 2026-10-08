//! Error-path and resource edges from the #90 review: close and flush failures around
//! cross-shard commits, `drop_table` and `shrink` (issue #136), and leaks (issue #144).
//! Shards are application-owned and driven by hand, so every interleaving is fixed.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{FaultPlan, SimVfs};
use pigeonhole_io::{Vfs, VfsRef};

const DB: &str = "/db/data.phdb";

struct Db {
    vfs: Arc<SimVfs>,
    engine: Arc<Engine>,
    shards: Vec<EngineShard>,
    /// A table owned by shard 0 and one owned by shard 1.
    tables: [Arc<TableInfo>; 2],
}

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.reader_slots = 4;
    o.memtable_budget = 4 << 20;
    o.memtable_freeze_bytes = 64 << 10;
    o.wal.segment_size = 256 << 10;
    o
}

fn open(seed: u64) -> Db {
    let vfs = SimVfs::new(seed);
    let (engine, shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs)).expect("open");
    let mut owned: [Option<Arc<TableInfo>>; 2] = [None, None];
    for i in 0.. {
        let t = engine
            .create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
            .expect("table");
        let snap = engine.snapshot().unwrap();
        let (_, shard) = snap.view().tablets().route(t.id, b"row").unwrap();
        let slot = &mut owned[usize::from(shard.0)];
        if slot.is_none() {
            *slot = Some(t);
        }
        if owned.iter().all(Option::is_some) {
            break;
        }
    }
    let [a, b] = owned;
    Db {
        vfs,
        engine,
        shards,
        tables: [a.unwrap(), b.unwrap()],
    }
}

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

impl Db {
    /// Runs shard `i` once; returns whether it has work left.
    fn step(&mut self, i: usize) -> bool {
        let now = self.vfs.monotonic_nanos();
        self.shards[i].run_once(now + 1_000)
    }

    /// Runs every shard until none has work left: two idle passes in a row, since shard 1's
    /// turn may send shard 0 a message after shard 0's turn.
    fn settle(&mut self) {
        let mut idle = 0;
        for _ in 0..10_000 {
            let a = self.step(0);
            let b = self.step(1);
            idle = if a || b { 0 } else { idle + 1 };
            if idle == 2 {
                return;
            }
        }
        panic!("the shards never went idle");
    }

    fn put(&self, batch: &mut WriteBatch, shard: usize, row: &[u8], value: &[u8]) {
        let t = &self.tables[shard];
        batch
            .put(
                t.id,
                t.families[0].id,
                row,
                b"q",
                None,
                ValueRef::Bytes(value),
            )
            .unwrap();
    }

    /// Commits `batch`, running only the shards in `run` until it resolves.
    fn commit_on(
        &mut self,
        batch: WriteBatch,
        durability: Durability,
        run: &[usize],
    ) -> pigeonhole_engine::Result<()> {
        let mut pc: PendingCommit = self.engine.submit(batch, Some(durability))?;
        for _ in 0..10_000 {
            if let Some(r) = ready(&mut pc) {
                return r.map(|_| ());
            }
            for &i in run {
                self.step(i);
            }
        }
        panic!("the commit never resolved");
    }

    /// Closes and runs the shards until both finish; panics on a hang.
    fn close(mut self) -> (Arc<SimVfs>, Arc<Engine>) {
        self.engine.close().unwrap();
        for _ in 0..10_000 {
            let a = self.step(0);
            let b = self.step(1);
            if !a && !b && self.engine.close_finished() {
                drop(self.shards);
                return (self.vfs, self.engine);
            }
        }
        panic!("close never finished (a hang)");
    }
}

fn clean_at_rest(vfs: &Arc<SimVfs>) -> bool {
    let vfs: VfsRef = vfs.clone();
    pigeonhole_pager::Pager::open(&vfs, Path::new(DB), false)
        .unwrap()
        .clean_shutdown()
}

fn read(engine: &Engine, t: &TableInfo, row: &[u8]) -> Option<Vec<u8>> {
    let snap = engine.snapshot().unwrap();
    engine
        .get(&snap, t.id, t.families[0].id, row, b"q")
        .unwrap()
        .map(|c| match c.value() {
            ValueRef::Bytes(b) => b.to_vec(),
            other => panic!("unexpected value {other:?}"),
        })
}

/// A cross-shard commit with a large share on `big` and a small one on the other shard,
/// then the other shard's WAL fails a sync. `flush_big_first` lets the large share reach
/// SSTs before the failure.
fn poisoned_participant_closes(big: usize, flush_big_first: bool) {
    let small = 1 - big;
    let mut db = open(11 + big as u64 * 2 + u64::from(flush_big_first));
    let value = vec![1u8; 1000];
    let mut wb = WriteBatch::new();
    // 150 KiB freezes (64 KiB threshold); 30 KiB stays in the active memtable.
    let rows: u32 = if flush_big_first { 150 } else { 30 };
    for i in 0..rows {
        db.put(&mut wb, big, &i.to_be_bytes(), &value);
    }
    db.put(&mut wb, small, b"small", b"small");
    db.commit_on(wb, Durability::Sync, &[0, 1]).unwrap();
    db.settle();
    let flushes = db.engine.metrics().flushes;
    assert_eq!(flushes > 0, flush_big_first);

    // Poison the small share's shard: its WAL fails a sync (D85).
    let mut faults = FaultPlan::none();
    faults.io_error_ppm = 1_000_000;
    db.vfs.set_faults(faults);
    let mut wb = WriteBatch::new();
    db.put(&mut wb, small, b"poison", b"x");
    let r = db.commit_on(wb, Durability::Sync, &[small]);
    db.vfs.set_faults(FaultPlan::none());
    assert!(r.is_err(), "the poisoning commit fails");

    // The other shard still flushes: its shares' records on the poisoned stream are durable,
    // so the poisoned shard's barrier holds for them.
    let mut wb = WriteBatch::new();
    for i in 0..150u32 {
        db.put(&mut wb, big, format!("more{i}").as_bytes(), &value);
    }
    db.commit_on(wb, Durability::Buffered, &[0, 1]).unwrap();
    db.settle();
    assert!(
        db.engine.metrics().flushes > flushes,
        "a healthy shard's flush failed because a peer's WAL is poisoned"
    );

    let tables = [db.tables[0].name.clone(), db.tables[1].name.clone()];
    let (vfs, engine) = db.close();
    drop(engine);
    assert!(
        !clean_at_rest(&vfs),
        "a poisoned shard makes the close unclean"
    );

    // The next open replays: the cross-shard commit is whole, the poisoning one is not.
    let engine = Engine::open(Path::new(DB), threaded(&vfs)).unwrap();
    let t = [
        engine.table(&tables[0]).unwrap(),
        engine.table(&tables[1]).unwrap(),
    ];
    assert_eq!(
        read(&engine, &t[big], &7u32.to_be_bytes()),
        Some(value.clone())
    );
    assert_eq!(read(&engine, &t[small], b"small"), Some(b"small".to_vec()));
    assert_eq!(read(&engine, &t[big], b"more7"), Some(value));
    engine.close().unwrap();
}

/// Engine-owned options on `vfs`, for checking what a closed database holds.
fn threaded(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = options(vfs);
    o.create_if_missing = false;
    o
}

#[test]
fn close_finishes_after_a_cross_shard_participant_is_poisoned() {
    // 5-6 5.1: the shard that could not flush its share never reported it, and the other
    // shard waited for that report at close for ever. Both share placements, with the large
    // share in SSTs before the failure or not.
    for big in 0..2 {
        for flush_big_first in [true, false] {
            poisoned_participant_closes(big, flush_big_first);
        }
    }
}

impl Db {
    /// Creates tables named `{prefix}{i}` until one lands on `shard`; returns it.
    fn table_on(&self, shard: usize, prefix: &str) -> Arc<TableInfo> {
        for i in 0.. {
            let t = self
                .engine
                .create_table(
                    &format!("{prefix}{i}"),
                    &[("f".into(), FamilyOptions::default())],
                )
                .expect("table");
            let snap = self.engine.snapshot().unwrap();
            let (_, s) = snap.view().tablets().route(t.id, b"row").unwrap();
            if usize::from(s.0) == shard {
                return t;
            }
            self.engine.drop_table(t.id).unwrap();
        }
        unreachable!()
    }

    /// Commits 40 rows of 1 KiB to `t` (below the freeze threshold).
    fn fill(&mut self, t: &TableInfo) {
        let mut wb = WriteBatch::new();
        for i in 0..40u32 {
            wb.put(
                t.id,
                t.families[0].id,
                format!("r{i:03}").as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&[7u8; 1000]),
            )
            .unwrap();
        }
        self.commit_on(wb, Durability::Buffered, &[0, 1]).unwrap();
    }

    /// Runs the shards until `pm` resolves.
    fn wait(
        &mut self,
        mut pm: pigeonhole_engine::PendingMaintenance,
    ) -> pigeonhole_engine::Result<()> {
        for _ in 0..10_000 {
            if let Some(r) = ready(&mut pm) {
                return r;
            }
            self.step(0);
            self.step(1);
        }
        panic!("the flush never resolved");
    }

    /// Starts a flush of every shard without running it.
    fn start_flush(&mut self) -> pigeonhole_engine::PendingMaintenance {
        let pm = self.engine.flush_pending().unwrap();
        // A deadline already passed: the shards take the message and start the flush
        // task, but run no slice of it.
        self.shards[0].run_once(0);
        self.shards[1].run_once(0);
        pm
    }
}

#[test]
fn a_flush_racing_drop_table_commits_the_other_tables() {
    // 7 F7-3: one flush request carries every slot of the shard; a table dropped while it
    // ran made the manifest refuse the whole request, so `flush()` failed and the other
    // table's flush was thrown away.
    let mut db = open(40);
    let keep = db.table_on(0, "keep");
    let gone = db.table_on(0, "gone");
    db.fill(&keep);
    db.fill(&gone);
    let flushes = db.engine.metrics().flushes;
    let pm = db.start_flush();
    db.engine.drop_table(gone.id).unwrap();
    db.wait(pm).expect("the flush of the kept table succeeds");
    assert!(db.engine.metrics().flushes > flushes);
    // The dropped table's output was freed, not leaked.
    assert_eq!(db.engine.unreferenced_bytes(), 0);
    let (vfs, engine) = db.close();
    drop(engine);
    assert!(clean_at_rest(&vfs));
    let engine = Engine::open(Path::new(DB), threaded(&vfs)).unwrap();
    let keep = engine.table(&keep.name).unwrap();
    assert_eq!(read(&engine, &keep, b"r007"), Some(vec![7u8; 1000]));
    assert!(engine.table(&gone.name).is_none());
    engine.close().unwrap();
}

#[test]
fn close_racing_drop_table_and_a_flush_is_clean() {
    // 7 F7-3: the refused flush made the final flush fail, so `close()` reported an
    // unclean close and the next open replayed.
    let mut db = open(41);
    let keep = db.table_on(0, "keep");
    let gone = db.table_on(0, "gone");
    db.fill(&keep);
    db.fill(&gone);
    let _pm = db.start_flush();
    db.engine.drop_table(gone.id).unwrap();
    let (vfs, engine) = db.close();
    drop(engine);
    assert!(clean_at_rest(&vfs), "the close is clean");
    let engine = Engine::open(Path::new(DB), threaded(&vfs)).unwrap();
    let keep = engine.table(&keep.name).unwrap();
    assert_eq!(read(&engine, &keep, b"r007"), Some(vec![7u8; 1000]));
    engine.close().unwrap();
}

/// While armed, holds the database file's `set_len` on the thread named `shrink`, or a
/// write to another file on the thread named `backup`, until released: the tests close the
/// engine while `shrink` is between its commit and its truncation, or `backup` is copying.
/// While `fail_written` is set, reads of database-file ranges written since it was set
/// fail.
#[derive(Debug)]
struct GateVfs {
    inner: Arc<SimVfs>,
    gate: Arc<Gate>,
}

#[derive(Debug, Default)]
struct Gate {
    armed: AtomicBool,
    entered: AtomicBool,
    released: AtomicBool,
    fail_written: AtomicBool,
    written: std::sync::Mutex<Vec<(u64, u64)>>,
}

#[derive(Debug)]
struct GateFile {
    inner: pigeonhole_io::FileRef,
    gate: Arc<Gate>,
    /// The database file (else, say, a backup's destination).
    db: bool,
}

impl GateFile {
    fn check_read(&self, offset: u64, len: usize) -> pigeonhole_io::Result<()> {
        let g = &self.gate;
        if !self.db || !g.fail_written.load(Ordering::Acquire) {
            return Ok(());
        }
        let end = offset + len as u64;
        let written = g.written.lock().unwrap();
        if written.iter().any(|&(a, b)| offset < b && a < end) {
            return Err(pigeonhole_io::Error::new(
                pigeonhole_io::ErrorKind::Other,
                "injected read failure",
            ));
        }
        Ok(())
    }

    fn note_write(&self, offset: u64, len: usize) {
        if self.db && self.gate.fail_written.load(Ordering::Acquire) {
            self.gate
                .written
                .lock()
                .unwrap()
                .push((offset, offset + len as u64));
        }
    }

    /// While armed, holds the first call on `thread` here until released (at most two
    /// seconds: a final close that wrongly runs meanwhile may wait for what this thread
    /// holds; the test then fails on its assertions rather than hanging).
    fn hold(&self, thread: &str) {
        let g = &self.gate;
        if std::thread::current().name() == Some(thread) && g.armed.swap(false, Ordering::AcqRel) {
            g.entered.store(true, Ordering::Release);
            let start = std::time::Instant::now();
            while !g.released.load(Ordering::Acquire)
                && start.elapsed() < std::time::Duration::from_secs(2)
            {
                std::thread::yield_now();
            }
        }
    }
}

impl Vfs for GateVfs {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<pigeonhole_io::FileRef> {
        let inner = self.inner.open(path, opts)?;
        Ok(Arc::new(GateFile {
            inner,
            gate: Arc::clone(&self.gate),
            db: path == Path::new(DB),
        }))
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
        self.inner.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}

impl pigeonhole_io::File for GateFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.check_read(offset, buf.len())?;
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> pigeonhole_io::Result<()> {
        if !self.db {
            self.hold("backup");
        }
        self.note_write(offset, buf.len());
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> pigeonhole_io::Completion {
        match self.check_read(offset, buf.len()) {
            Ok(()) => self.inner.submit_read(buf, offset),
            Err(e) => pigeonhole_io::Completion::ready(Err(e)),
        }
    }
    fn submit_write(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> pigeonhole_io::Completion {
        if !self.db {
            self.hold("backup");
        }
        self.note_write(offset, buf.len());
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> pigeonhole_io::Result<()> {
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> pigeonhole_io::Completion<()> {
        self.inner.submit_sync_data()
    }
    fn sync_all(&self) -> pigeonhole_io::Result<()> {
        self.inner.sync_all()
    }
    fn len(&self) -> pigeonhole_io::Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> pigeonhole_io::Result<()> {
        if self.db {
            self.hold("shrink");
        }
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

#[test]
fn close_waits_for_a_shrink_in_flight() {
    // 7 F7-4: `shrink` checked for a close once, then kept relocating, committing roots and
    // truncating the file after the final close had marked it clean and released the
    // writer lock (another process could be the writer by then).
    let (mut db, gate) = open_gated(50, |_| {});
    let sim = Arc::clone(&db.vfs);
    // Interleaved SSTs of two tables; dropping the first leaves holes for the second's
    // tail extents.
    let gone = db.table_on(0, "gone");
    let keep = db.table_on(0, "keep");
    for _ in 0..4 {
        db.fill(&gone);
        let pm = db.engine.flush_pending().unwrap();
        db.wait(pm).unwrap();
        db.fill(&keep);
        let pm = db.engine.flush_pending().unwrap();
        db.wait(pm).unwrap();
    }
    db.engine.drop_table(gone.id).unwrap();

    gate.armed.store(true, Ordering::Release);
    let engine = Arc::clone(&db.engine);
    let shrink = std::thread::Builder::new()
        .name("shrink".into())
        .spawn(move || engine.shrink())
        .unwrap();
    for _ in 0..1_000_000 {
        if gate.entered.load(Ordering::Acquire) {
            break;
        }
        db.step(0);
        db.step(1);
        std::thread::yield_now();
    }
    assert!(
        gate.entered.load(Ordering::Acquire),
        "shrink reached its truncation"
    );

    // Close while shrink is between its commit and its truncation.
    db.engine.close().unwrap();
    for _ in 0..1_000 {
        db.step(0);
        db.step(1);
    }
    assert!(
        !db.engine.close_finished(),
        "the final close ran (file marked clean, writer lock released) while shrink was still running"
    );
    gate.released.store(true, Ordering::Release);
    let shrunk = shrink.join().unwrap();
    assert!(shrunk.is_ok(), "shrink: {shrunk:?}");
    assert!(
        db.engine.close_finished(),
        "the shrink's end ran the final close"
    );
    let Db { engine, shards, .. } = db;
    drop(shards);
    drop(engine);
    assert!(
        clean_at_rest(&sim),
        "nothing committed after the clean mark"
    );
    let engine = Engine::open(Path::new(DB), threaded(&sim)).unwrap();
    let keep = engine.table(&keep.name).unwrap();
    assert_eq!(read(&engine, &keep, b"r007"), Some(vec![7u8; 1000]));
    engine.close().unwrap();
}

#[test]
fn a_commit_whose_write_failed_after_its_ticket_holds_peers_barriers() {
    // Review of 5-6 5.1: a COMMIT record that got a ticket decides commit even when its
    // group's write then fails, so it may never reach the disk. The poisoned coordinator's
    // barrier must not let a participant flush (and checkpoint) its share of that commit:
    // a power loss would keep the share and lose the COMMIT, a torn transaction (D83).
    let mut db = open(70);
    // The coordinator is the first shard of the commit: shard 0. Shard 1's share is large,
    // so it freezes and flushes as soon as it is applied.
    let value = vec![3u8; 1000];
    let mut wb = WriteBatch::new();
    db.put(&mut wb, 0, b"c0", b"coordinator");
    for i in 0..150u32 {
        db.put(&mut wb, 1, format!("c1-{i:03}").as_bytes(), &value);
    }
    let _ = db.engine.take_appended();
    let mut pc = db.engine.submit(wb, Some(Durability::Sync)).unwrap();
    // Run both shards until both PREPAREs are written, and no further: one step at a time,
    // so the coordinator never gets the turn in which it would append the COMMIT.
    let mut prepares = 0;
    'prepare: for _ in 0..1_000 {
        for shard in [1, 0] {
            let appended = db.engine.take_appended();
            assert!(
                !appended
                    .iter()
                    .any(|r| r.kind == pigeonhole_engine::AppendedKind::Commit),
                "the COMMIT was appended before the faults were armed"
            );
            prepares += appended
                .iter()
                .filter(|r| r.kind == pigeonhole_engine::AppendedKind::Prepare)
                .count();
            if prepares == 2 {
                break 'prepare;
            }
            db.step(shard);
        }
    }
    assert_eq!(prepares, 2, "both shares prepared");
    // The participant's sync completes and it replies `Prepared`. It runs until it has
    // nothing left (its share is applied only at the decision): a fixed count of steps
    // would depend on its background work, such as preparing WAL spares once its stream is
    // half way through a segment (#143).
    let mut idle = false;
    for _ in 0..100 {
        if !db.step(1) {
            idle = true;
            break;
        }
    }
    assert!(idle, "the participant never went idle");
    // The coordinator appends its COMMIT (a ticket), then the group's write fails.
    let mut faults = FaultPlan::none();
    faults.io_error_ppm = 1_000_000;
    db.vfs.set_faults(faults);
    db.step(0);
    db.vfs.set_faults(FaultPlan::none());
    let appended = db.engine.take_appended();
    assert!(
        appended
            .iter()
            .any(|r| r.kind == pigeonhole_engine::AppendedKind::Commit),
        "the COMMIT got a ticket: {appended:?}"
    );
    // The participant applies its share and tries to flush it.
    for _ in 0..1_000 {
        if ready(&mut pc).is_some() {
            break;
        }
        db.step(1);
        db.step(0);
    }
    db.settle();

    // Power loss: only synced data survives.
    db.vfs.crash(pigeonhole_io::sim::CrashKind::Power);
    let (vfs, tables) = (Arc::clone(&db.vfs), db.tables.clone());
    drop(db);
    let engine = Engine::open(Path::new(DB), threaded(&vfs)).unwrap();
    let t = [
        engine.table(&tables[0].name).unwrap(),
        engine.table(&tables[1].name).unwrap(),
    ];
    let coordinator = read(&engine, &t[0], b"c0").is_some();
    let participant = read(&engine, &t[1], b"c1-007").is_some();
    assert_eq!(
        coordinator, participant,
        "a torn cross-shard commit: coordinator share {coordinator}, participant share {participant}"
    );
    engine.close().unwrap();
}

/// An application-owned engine on a `GateVfs` over a fresh `SimVfs` (its `tables` are
/// placeholders; tests make their own with `table_on`).
fn open_gated(seed: u64, tweak: impl FnOnce(&mut EngineOptions)) -> (Db, Arc<Gate>) {
    let sim = SimVfs::new(seed);
    let gate = Arc::new(Gate::default());
    let mut o = options(&sim);
    o.vfs = Arc::new(GateVfs {
        inner: Arc::clone(&sim),
        gate: Arc::clone(&gate),
    });
    tweak(&mut o);
    let (engine, shards) = Engine::open_application_owned(Path::new(DB), o).expect("open");
    let family = || vec![("f".into(), FamilyOptions::default())];
    let tables = [
        engine.create_table("x", &family()).unwrap(),
        engine.create_table("y", &family()).unwrap(),
    ];
    let db = Db {
        vfs: sim,
        engine,
        shards,
        tables,
    };
    (db, gate)
}

#[test]
fn close_stops_a_backup_in_flight() {
    // Review of 7 F7-4: the final close waits for a backup (its maintenance guard), so a
    // long copy must notice the close and stop rather than hold it up to the end.
    let (mut db, gate) = open_gated(52, |_| {});
    let t = db.table_on(0, "t");
    db.fill(&t);
    let pm = db.engine.flush_pending().unwrap();
    db.wait(pm).unwrap();

    gate.armed.store(true, Ordering::Release);
    let engine = Arc::clone(&db.engine);
    let backup = std::thread::Builder::new()
        .name("backup".into())
        .spawn(move || engine.backup(Path::new("/db/backup.phdb")))
        .unwrap();
    for _ in 0..1_000_000 {
        if gate.entered.load(Ordering::Acquire) {
            break;
        }
        db.step(0);
        db.step(1);
        std::thread::yield_now();
    }
    assert!(
        gate.entered.load(Ordering::Acquire),
        "the backup started copying"
    );

    db.engine.close().unwrap();
    for _ in 0..1_000 {
        db.step(0);
        db.step(1);
    }
    assert!(
        !db.engine.close_finished(),
        "the final close ran while a backup was copying"
    );
    gate.released.store(true, Ordering::Release);
    let r = backup.join().unwrap();
    assert!(
        matches!(r, Err(pigeonhole_engine::Error::Closed)),
        "the backup stops at the close: {r:?}"
    );
    assert!(
        db.engine.close_finished(),
        "the backup's end ran the final close"
    );
    assert!(
        !db.vfs.exists(Path::new("/db/backup.phdb")).unwrap(),
        "the partial copy is removed"
    );
    let Db {
        vfs,
        engine,
        shards,
        ..
    } = db;
    drop(shards);
    drop(engine);
    assert!(clean_at_rest(&vfs));
}

#[test]
fn compaction_outputs_that_fail_to_open_are_freed() {
    // 5-6 5.4: a compaction whose new SSTs could not be opened (an EIO reading a footer)
    // dropped them without abandoning their extents, which then stayed allocated until
    // reopen; a slot that kept failing leaked one set of outputs per retry.
    let (mut db, gate) = open_gated(60, |o| {
        o.compaction.l0_trigger = u32::MAX;
        o.compaction.level_base_bytes = u64::MAX;
    });
    let t = db.table_on(0, "t");
    for _ in 0..2 {
        db.fill(&t);
        let pm = db.engine.flush_pending().unwrap();
        db.wait(pm).unwrap();
    }
    assert_eq!(db.engine.unreferenced_bytes(), 0);
    // What the compaction writes (its outputs) cannot be read back.
    gate.fail_written.store(true, Ordering::Release);
    let pm = db.engine.compact_pending(Some(t.id)).unwrap();
    let r = db.wait(pm);
    gate.fail_written.store(false, Ordering::Release);
    assert!(
        r.is_err(),
        "the compaction failed to open its outputs: {r:?}"
    );
    db.settle();
    assert_eq!(
        db.engine.unreferenced_bytes(),
        0,
        "the failed compaction's outputs stay allocated"
    );
    // The inputs are intact, and a retry succeeds.
    let pm = db.engine.compact_pending(Some(t.id)).unwrap();
    db.wait(pm).unwrap();
    assert_eq!(read(&db.engine, &t, b"r007"), Some(vec![7u8; 1000]));
    let (_, engine) = db.close();
    drop(engine);
}

#[test]
fn refused_cross_shard_commits_leave_no_aborted_seqnos() {
    // 5-6 6.3: a cross-shard commit refused before its records were logged (an optimistic
    // transaction's conflict) kept its seqno in the shards' aborted sets until reopen.
    let mut db = open(61);
    let (a, b) = (Arc::clone(&db.tables[0]), Arc::clone(&db.tables[1]));
    for i in 0..20u32 {
        let mut txn = db.engine.begin().unwrap();
        let _ = txn.get(a.id, a.families[0].id, b"x", b"q").unwrap();
        let mut wb = WriteBatch::new();
        db.put(&mut wb, 0, b"x", &i.to_be_bytes());
        db.commit_on(wb, Durability::Buffered, &[0, 1]).unwrap();
        for (t, row) in [(&a, b"x"), (&b, b"y")] {
            txn.batch()
                .put(
                    t.id,
                    t.families[0].id,
                    row,
                    b"q",
                    None,
                    ValueRef::Bytes(b"txn"),
                )
                .unwrap();
        }
        // `Txn::commit` blocks: it runs on a thread while this one drives the shards.
        let h = std::thread::spawn(move || txn.commit(Some(Durability::Buffered)));
        while !h.is_finished() {
            db.step(0);
            db.step(1);
            std::thread::yield_now();
        }
        let r = h.join().unwrap();
        assert!(
            matches!(r, Err(pigeonhole_engine::Error::Conflict)),
            "{r:?}"
        );
    }
    db.settle();
    assert_eq!(
        db.engine.aborted_seqnos(),
        0,
        "refused commits' seqnos are kept"
    );
    let (_, engine) = db.close();
    drop(engine);
}
