//! D286: a Buffered cross-shard commit's coordinator sends `Decide` when it decides, before
//! its DECISION is written. The commit's `Completion` counts the DECISION as one more
//! count-down, so the seqno stays held (nothing visible) and the client unanswered until the
//! record is written and every share applied. A DECISION that ends unlogged after the early
//! `Decide` (the coordinator poisoned before its group, or an earlier member's append
//! failing in it) sends no abort after it: the commit stays held, answers `OutcomeUnknown`,
//! and a reopen finds it absent. Both shards are driven by hand; shard 0 coordinates, shard 1
//! participates. `Fault::HoldDecisionRecords` keeps the DECISION unwritten while the
//! participants apply.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, Error, FamilyOptions, Fault, PendingCommit, TableInfo,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};

const DB: &str = "/db/data.phdb";

struct Db {
    vfs: Arc<SimVfs>,
    engine: Arc<Engine>,
    shards: Vec<EngineShard>,
    /// A table owned by shard 0 and one owned by shard 1.
    tables: [Arc<TableInfo>; 2],
}

fn options(vfs: &Arc<SimVfs>, segment: Option<u64>) -> EngineOptions {
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.reader_slots = 4;
    if let Some(size) = segment {
        o.wal.segment_size = size;
        o.wal.spare_segments = 1;
    }
    o
}

fn open(seed: u64, segment: Option<u64>) -> Db {
    let vfs = SimVfs::new(seed);
    let (engine, shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs, segment)).expect("open");
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
    let mut db = Db {
        vfs,
        engine,
        shards,
        tables: [a.unwrap(), b.unwrap()],
    };
    db.idle();
    db
}

impl Db {
    /// Runs shard `i` once; returns whether it has work left.
    fn step(&mut self, i: usize) -> bool {
        let now = pigeonhole_io::Vfs::monotonic_nanos(&*self.vfs);
        self.shards[i].run_once(now + 1_000)
    }

    fn idle(&mut self) {
        for _ in 0..64 {
            let a = self.step(0);
            let b = self.step(1);
            if !a && !b {
                break;
            }
        }
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

    /// A Buffered cross-shard commit of `row` on both shards, submitted to shard 0.
    fn submit(&self, row: &[u8], value: &[u8]) -> PendingCommit {
        let mut b = WriteBatch::new();
        self.put(&mut b, 0, row, value);
        self.put(&mut b, 1, row, value);
        self.engine
            .submit(b, Some(Durability::Buffered))
            .expect("submit")
    }

    /// Whether `row` is visible in the table shard `shard` owns.
    fn visible(&self, shard: usize, row: &[u8]) -> bool {
        visible(&self.engine, &self.tables[shard], row)
    }

    /// Drives both shards until `pc` is answered (at most `turns` rounds).
    fn answer(
        &mut self,
        pc: &mut PendingCommit,
        turns: usize,
    ) -> Option<pigeonhole_engine::Result<()>> {
        for _ in 0..turns {
            if let Some(r) = ready(pc) {
                return Some(r.map(|_| ()));
            }
            self.step(0);
            self.step(1);
        }
        ready(pc).map(|r| r.map(|_| ()))
    }

    /// A process crash (Buffered writes survive it), then a fresh open of the same files:
    /// whether `row` is visible on shard 0's table and on shard 1's.
    fn crash_and_reopen(self, row: &[u8]) -> (bool, bool) {
        let Db {
            vfs,
            engine,
            shards,
            tables,
        } = self;
        vfs.crash(CrashKind::Process);
        drop(shards);
        drop(engine);
        vfs.set_faults(FaultPlan::none());
        vfs.set_deferred_io(false);
        vfs.complete_all_io();
        let (engine, mut shards) =
            Engine::open_application_owned(Path::new(DB), options(&vfs, None)).expect("reopen");
        for _ in 0..64 {
            let busy = shards
                .iter_mut()
                .fold(false, |b, s| s.run_once(u64::MAX) | b);
            if !busy {
                break;
            }
        }
        let t = [
            engine.table(&tables[0].name).expect("table 0"),
            engine.table(&tables[1].name).expect("table 1"),
        ];
        let seen = (visible(&engine, &t[0], row), visible(&engine, &t[1], row));
        let _ = engine.close();
        for _ in 0..64 {
            for s in &mut shards {
                s.run_once(u64::MAX);
            }
        }
        seen
    }
}

fn visible(engine: &Engine, t: &TableInfo, row: &[u8]) -> bool {
    engine
        .get_latest(t.id, t.families[0].id, row, b"q")
        .unwrap()
        .is_some()
}

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

impl Db {
    /// A single-shard Buffered commit of `row` on shard 0.
    fn single(&self, row: &[u8]) -> PendingCommit {
        let mut b = WriteBatch::new();
        self.put(&mut b, 0, row, b"s");
        self.engine
            .submit(b, Some(Durability::Buffered))
            .expect("submit")
    }

    /// Drives `pc` to its early decision with the DECISION held: shard 0 coordinates and
    /// prepares, shard 1 prepares and votes, shard 0 decides (its own share applies, the
    /// DECISION is set aside), and shard 1 applies its share. Every share has counted down;
    /// only the DECISION's count is left.
    fn decide_early_with_decision_held(&mut self) {
        self.engine.inject(Fault::HoldDecisionRecords, true);
        self.step(0);
        self.step(1);
        self.step(0);
        self.step(1);
        self.step(0);
    }
}

/// The early `Decide` is applied on both shards while the DECISION is unwritten: nothing is
/// visible and the client has no answer. Once the DECISION is written, the commit is visible
/// and answered. Mutation: a `Completion` without the DECISION's count lets the last share
/// release the commit before its DECISION is written.
#[test]
fn an_early_decided_commit_is_visible_only_once_its_decision_is_written() {
    let mut db = open(286, None);
    let mut pc = db.submit(b"x", b"v");
    db.decide_early_with_decision_held();
    assert!(
        ready(&mut pc).is_none(),
        "answered before the DECISION was written"
    );
    assert!(
        !db.visible(0, b"x") && !db.visible(1, b"x"),
        "visible before the DECISION was written"
    );
    db.engine.release_held_decisions();
    db.answer(&mut pc, 16)
        .expect("answered once the DECISION is written")
        .expect("committed");
    assert!(db.visible(0, b"x") && db.visible(1, b"x"));
    assert_eq!(db.engine.abort_decides_sent(0), 0);
    let (a, b) = db.crash_and_reopen(b"x");
    assert!(
        a && b,
        "an answered commit lost by a process crash: {a} {b}"
    );
}

/// perf287's path 1: the coordinator is poisoned by an unrelated single's failed write, in a
/// group after the early `Decide` and before the DECISION's. The DECISION then settles
/// unlogged: no abort `Decide` follows the commit one, the commit stays held (invisible), the
/// client hears `OutcomeUnknown`, and a reopen finds it absent.
#[test]
fn a_decision_unlogged_after_an_early_decide_sends_no_abort_and_ends_outcome_unknown() {
    let mut db = open(287, None);
    let mut pc = db.submit(b"x", b"v");
    db.decide_early_with_decision_held();
    let aborts = db.engine.abort_decides_sent(0);
    let mut plan = FaultPlan::none();
    plan.fail_on = Some("-wal-0");
    db.vfs.set_faults(plan);
    let mut single = db.single(b"s");
    db.step(0);
    assert_eq!(db.vfs.scheduled_fired(), 1, "the single's WAL write failed");
    assert!(
        matches!(ready(&mut single), Some(Err(_))),
        "the single failed"
    );
    db.vfs.set_faults(FaultPlan::none());
    db.engine.release_held_decisions();
    let outcome = db.answer(&mut pc, 16).expect("answered");
    assert!(matches!(outcome, Err(Error::OutcomeUnknown)), "{outcome:?}");
    assert_eq!(
        db.engine.abort_decides_sent(0),
        aborts,
        "an abort Decide after the commit one"
    );
    assert!(!db.visible(0, b"x") && !db.visible(1, b"x"));
    let (a, b) = db.crash_and_reopen(b"x");
    assert!(!a && !b, "the unlogged commit survived a reopen: {a} {b}");
}

/// perf287's path 2: the DECISION's own group fails at an earlier member's append (`fail_all`
/// with the DECISION never appended). The same outcome as path 1.
#[test]
fn a_decision_unappended_behind_a_failed_append_sends_no_abort() {
    let mut db = open(288, None);
    let mut pc = db.submit(b"x", b"v");
    db.decide_early_with_decision_held();
    let aborts = db.engine.abort_decides_sent(0);
    // Two singles ahead of the DECISION in its group: the second one's append fails.
    let mut s1 = db.single(b"s1");
    let mut s2 = db.single(b"s2");
    db.engine.release_held_decisions();
    db.engine.inject(Fault::SecondAppend, true);
    db.step(0);
    assert!(
        matches!(ready(&mut s2), Some(Err(_))),
        "the second append failed"
    );
    let _ = ready(&mut s1);
    let outcome = db.answer(&mut pc, 16).expect("answered");
    assert!(matches!(outcome, Err(Error::OutcomeUnknown)), "{outcome:?}");
    assert_eq!(
        db.engine.abort_decides_sent(0),
        aborts,
        "an abort Decide after the commit one"
    );
    assert!(!db.visible(0, b"x") && !db.visible(1, b"x"));
    let (a, b) = db.crash_and_reopen(b"x");
    assert!(!a && !b, "the unappended commit survived a reopen: {a} {b}");
}

/// A DECISION appended whose write then fails decides commit, as before D286: it has a
/// ticket, so `fail_all` settles it written, with `OutcomeUnknown` as its error. The commit
/// is applied everywhere (visible, unlike the unlogged paths) and answered `OutcomeUnknown`;
/// no abort follows the early `Decide`.
#[test]
fn a_decision_whose_write_fails_after_its_append_commits_answered_in_error() {
    let mut db = open(289, None);
    let mut pc = db.submit(b"x", b"v");
    db.step(0);
    db.step(1);
    let mut plan = FaultPlan::none();
    plan.fail_on = Some("-wal-0");
    db.vfs.set_faults(plan);
    // Shard 0 decides (the early Decide goes out), appends its DECISION and fails the write.
    db.step(0);
    assert_eq!(db.vfs.scheduled_fired(), 1, "the DECISION's write failed");
    db.vfs.set_faults(FaultPlan::none());
    let outcome = db.answer(&mut pc, 16).expect("answered");
    assert!(matches!(outcome, Err(Error::OutcomeUnknown)), "{outcome:?}");
    assert_eq!(db.engine.abort_decides_sent(0), 0);
    assert!(db.visible(0, b"x") && db.visible(1, b"x"));
}
