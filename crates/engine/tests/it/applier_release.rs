//! D278: the participant that applies a cross-shard commit's share last releases the commit's
//! seqno and answers its caller, without a hop back to the coordinator. Both shards are
//! driven by hand, so which one applies last, and which one dies, is chosen.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, Error, FamilyOptions, PendingCommit, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

struct Db {
    vfs: Arc<SimVfs>,
    engine: Arc<Engine>,
    shards: Vec<Option<EngineShard>>,
    /// A table owned by shard 0 and one owned by shard 1.
    tables: [Arc<TableInfo>; 2],
}

fn open() -> Db {
    let vfs = SimVfs::new(278);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.reader_slots = 4;
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
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
        shards: shards.into_iter().map(Some).collect(),
        tables: [a.unwrap(), b.unwrap()],
    }
}

impl Db {
    /// Runs shard `i` once (if it is alive); returns whether it has work left.
    fn step(&mut self, i: usize) -> bool {
        let now = pigeonhole_io::Vfs::monotonic_nanos(&*self.vfs);
        self.shards[i]
            .as_mut()
            .is_some_and(|s| s.run_once(now + 1_000))
    }

    /// The shard dies: its `EngineShard` is dropped before it closed.
    fn kill(&mut self, i: usize) {
        drop(self.shards[i].take());
    }

    fn put(&self, batch: &mut WriteBatch, shard: usize, row: &[u8]) {
        let t = &self.tables[shard];
        batch
            .put(
                t.id,
                t.families[0].id,
                row,
                b"q",
                None,
                ValueRef::Bytes(b"v"),
            )
            .unwrap();
    }

    /// Whether `row` is visible in the table shard `shard` owns.
    fn visible(&self, shard: usize, row: &[u8]) -> bool {
        let t = &self.tables[shard];
        self.engine
            .get_latest(t.id, t.families[0].id, row, b"q")
            .unwrap()
            .is_some()
    }

    /// A cross-shard commit coordinated by shard 0, decided, written and applied there,
    /// with shard 1's share decided but not applied: shard 1 applies last.
    fn decided_on_0(&mut self) -> PendingCommit {
        let mut b = WriteBatch::new();
        self.put(&mut b, 0, b"x");
        self.put(&mut b, 1, b"x");
        let pc = self
            .engine
            .submit(b, Some(Durability::Buffered))
            .expect("submit");
        self.step(0);
        self.step(1);
        self.step(0);
        self.step(0);
        pc
    }
}

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

#[test]
fn the_last_participant_releases_and_answers_without_the_coordinator() {
    let mut db = open();
    let mut pc = db.decided_on_0();
    // Applied on shard 0 but held: nothing of it is visible, and the caller waits.
    assert!(ready(&mut pc).is_none());
    assert!(!db.visible(0, b"x") && !db.visible(1, b"x"));
    // Only shard 1 runs: its apply is the last, so it makes the commit visible whole and
    // answers the caller. Shard 0 has not heard `Applied` yet.
    let mut done = None;
    for _ in 0..8 {
        db.step(1);
        done = ready(&mut pc);
        if done.is_some() {
            break;
        }
    }
    done.expect("answered by the last participant")
        .expect("committed");
    assert!(db.visible(0, b"x") && db.visible(1, b"x"));
    // Shard 0 then cleans up after `Applied`, and the close is clean.
    for _ in 0..8 {
        db.step(0);
    }
    db.engine.close().unwrap();
    for _ in 0..64 {
        db.step(0);
        db.step(1);
    }
}

/// perf287's point 1 on D278: the caller hears its answer before the coordinator ran its
/// clean-up for the commit (D249, D276). A transaction resubmitted at once on the same rows,
/// reading them, then a blind commit on them, each commit within a few turns: nothing left
/// of the first commit refuses them or makes them wait.
#[test]
fn a_commit_resubmitted_on_the_same_rows_right_after_the_answer_does_not_stall() {
    let mut db = open();
    let mut pc = db.decided_on_0();
    let mut first = None;
    for _ in 0..8 {
        db.step(1);
        first = ready(&mut pc);
        if first.is_some() {
            break;
        }
    }
    first.expect("answered").expect("committed");
    // Shard 0 has not run since: `Applied` waits in its queue, ahead of what follows.
    let mut txn = db.engine.begin().unwrap();
    for shard in 0..2 {
        let t = &db.tables[shard];
        assert!(
            txn.get(t.id, t.families[0].id, b"x", b"q")
                .unwrap()
                .is_some()
        );
        txn.batch()
            .put(
                t.id,
                t.families[0].id,
                b"x",
                b"q",
                None,
                ValueRef::Bytes(b"w"),
            )
            .unwrap();
    }
    let mut again = txn.submit(Some(Durability::Buffered)).unwrap();
    let mut blind = None;
    let mut turns = 0;
    let mut answers = (None, None);
    while turns < 16 && (answers.0.is_none() || answers.1.is_none()) {
        db.step(0);
        db.step(1);
        turns += 1;
        if answers.0.is_none() {
            answers.0 = ready(&mut again);
            if answers.0.is_some() {
                let mut b = WriteBatch::new();
                db.put(&mut b, 0, b"x");
                db.put(&mut b, 1, b"x");
                blind = Some(db.engine.submit(b, Some(Durability::Buffered)).unwrap());
            }
        }
        if let Some(b) = blind.as_mut()
            && answers.1.is_none()
        {
            answers.1 = ready(b);
        }
    }
    answers
        .0
        .expect("the resubmitted transaction stalled")
        .expect("committed");
    answers
        .1
        .expect("the blind commit after it stalled")
        .expect("committed");
    db.engine.close().unwrap();
    for _ in 0..64 {
        db.step(0);
        db.step(1);
    }
}

#[test]
fn a_participant_that_cannot_apply_leaves_the_commit_held_and_unknown() {
    let mut db = open();
    let mut pc = db.decided_on_0();
    // Shard 0 applied already: the next apply, shard 1's, fails (its shard poisons itself).
    db.engine.inject(pigeonhole_engine::Fault::NextApply, true);
    for _ in 0..8 {
        db.step(1);
    }
    // Shard 1 never counted down: the seqno stays held, and only the coordinator, hearing
    // `Applied` with the error, answers.
    assert!(ready(&mut pc).is_none());
    assert!(!db.visible(0, b"x") && !db.visible(1, b"x"));
    for _ in 0..8 {
        db.step(0);
    }
    let outcome = ready(&mut pc).expect("answered by the coordinator");
    assert!(matches!(outcome, Err(Error::OutcomeUnknown)), "{outcome:?}");
    assert!(!db.visible(0, b"x") && !db.visible(1, b"x"));
    let _ = db.engine.close();
    for _ in 0..64 {
        db.step(0);
        db.step(1);
    }
}

#[test]
fn a_participant_dropped_with_the_decision_queued_applies_it_first() {
    // Dropping an application-owned shard shuts its driver down, which handles what is
    // queued: the decision is applied, and that last apply releases the commit whole.
    let mut db = open();
    let mut pc = db.decided_on_0();
    db.kill(1);
    assert!(db.visible(0, b"x") && db.visible(1, b"x"));
    ready(&mut pc).expect("answered").expect("committed");
    let _ = db.engine.close();
    for _ in 0..64 {
        db.step(0);
    }
}

#[test]
fn a_coordinator_dying_after_its_apply_leaves_the_last_participant_to_finish() {
    let mut db = open();
    let mut pc = db.decided_on_0();
    db.kill(0);
    for _ in 0..64 {
        db.step(1);
    }
    // Every share is applied: the commit is visible whole or not at all, and its caller
    // hears `Ok` or `OutcomeUnknown` (a shard died), never `Closed` (not committed).
    let (a, b) = (db.visible(0, b"x"), db.visible(1, b"x"));
    assert_eq!(a, b, "visible half applied");
    let _ = db.engine.close();
    for _ in 0..64 {
        db.step(1);
    }
    match ready(&mut pc).expect("answered") {
        Ok(_) => assert!(a, "answered Ok but not visible"),
        Err(Error::OutcomeUnknown) => {}
        other => panic!("{other:?}"),
    }
}
