//! D279: a Buffered PREPARE votes once its record is appended, before its group's write, so
//! its coordinator can decide while the participant writes and applies. A failure after the
//! vote (the write, the view publish) poisons the participant, which votes no second time
//! and never applies its share: the commit ends `OutcomeUnknown`, nothing of it is visible,
//! and the next open finds it whole or not at all. A decision that reaches a share whose
//! PREPARE has not settled (its group waits for a held segment header's sync, D200) waits
//! for it. Both shards are driven by hand; shard 0 coordinates, shard 1 participates.

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

/// The write of shard 1's PREPARE fails after its vote went out. Before D279 the vote carried
/// the failure and the commit aborted (`ShardFailed`, not committed); now the coordinator
/// decides commit on the vote, writes its DECISION, and hears from shard 1 that the share was
/// not applied: `OutcomeUnknown`, with nothing visible. T1 (perf287): shard 0 applied its
/// share; a flush of it must not persist it, and the reopened database holds the commit whole
/// or not at all. Two layers protect it, and mutations show each: the held seqno alone (the
/// barrier made to answer durable: the flush still waits, the test passes), the barrier alone
/// (`durable_seqno` made to ignore the hold: the flush fails at shard 1's barrier), and
/// neither (the flush installs the share, and the reopen finds the commit torn).
#[test]
fn a_failed_prepare_write_after_its_vote_ends_outcome_unknown_and_recovers_whole() {
    let mut db = open(279, None);
    let mut pc = db.submit(b"x", b"v");
    // Shard 0 prepares its own share and sends shard 1 its PREPARE.
    db.step(0);
    let mut plan = FaultPlan::none();
    plan.fail_on = Some("-wal-1");
    db.vfs.set_faults(plan);
    // Shard 1 appends its PREPARE, votes, and fails the write: it poisons itself.
    db.step(1);
    assert_eq!(db.vfs.scheduled_fired(), 1, "shard 1's WAL write failed");
    // Shard 0 hears the vote, decides commit, writes the DECISION and applies its share.
    db.step(0);
    db.step(0);
    assert!(
        ready(&mut pc).is_none(),
        "answered before every share applied"
    );
    assert!(!db.visible(0, b"x") && !db.visible(1, b"x"));
    // A flush on shard 0 now: whatever it does, the held share stays out of every SST.
    let mut flush = db.engine.submit_flush().expect("a flush is accepted");
    let outcome = db.answer(&mut pc, 64).expect("answered");
    assert!(matches!(outcome, Err(Error::OutcomeUnknown)), "{outcome:?}");
    assert!(!db.visible(0, b"x") && !db.visible(1, b"x"));
    let mut flushed = None;
    for _ in 0..64 {
        db.step(0);
        db.step(1);
        if flushed.is_none() {
            flushed = ready(&mut flush);
        }
    }
    // The held seqno keeps shard 0's share from ever being durable, so its memtable never
    // freezes and the flush waits (it never reaches shard 1's barrier). Were the share
    // durable, shard 1's barrier would fail the flush (`cross_records_durable`). Either way
    // it never succeeds with the share in an SST.
    assert!(flushed.is_none(), "the flush finished: {flushed:?}");
    let (a, b) = db.crash_and_reopen(b"x");
    assert_eq!(a, b, "a torn cross-shard commit after reopen: {a} {b}");
}

/// T2 (perf287): the voted member's failure comes from the view publish after the write
/// (`Corruption`), not from I/O: the same route, `OutcomeUnknown`, no second vote. A single
/// on shard 1 shares the PREPARE's group, so the group publishes a view.
#[test]
fn a_failed_view_publish_after_the_vote_ends_outcome_unknown() {
    let mut db = open(2792, None);
    let mut single = WriteBatch::new();
    db.put(&mut single, 1, b"s", b"v");
    let mut single = db
        .engine
        .submit(single, Some(Durability::Buffered))
        .expect("submit");
    let mut pc = db.submit(b"x", b"v");
    db.step(0);
    db.engine.inject(Fault::NextGroupPublish, true);
    db.step(1);
    let single = db
        .answer(&mut single, 1)
        .expect("the single is answered in its turn");
    assert!(single.is_err(), "{single:?}");
    let outcome = db.answer(&mut pc, 64).expect("answered");
    assert!(matches!(outcome, Err(Error::OutcomeUnknown)), "{outcome:?}");
    assert!(!db.visible(0, b"x") && !db.visible(1, b"x"));
    let (a, b) = db.crash_and_reopen(b"x");
    assert_eq!(a, b, "a torn cross-shard commit after reopen: {a} {b}");
}

/// Three 32 KiB frames: a few commits fill a segment, and values up to 32 KiB stay inline.
const SEGMENT: u64 = 3 * 32 * 1024;
const COMMITS: u32 = 40;

/// Commits `0..last` with their I/O completed, then commit `last` with both shards' turns run
/// but no I/O completed. Returns whether shard 1 parked the decision. A commit answered `Ok`
/// before its I/O completed must survive a process crash on both shards: had a parked
/// decision been applied, a PREPARE under a held segment header would be lost with the
/// header, and the commit with it.
fn held(last: u32) -> bool {
    let mut db = open(u64::from(last), Some(SEGMENT));
    db.vfs.set_deferred_io(true);
    // Incompressible, so the WAL records keep their size.
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    let value: Vec<u8> = (0..4000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    for i in 0..last {
        let mut pc = db.submit(&i.to_be_bytes(), &value);
        for _ in 0..64 {
            if ready(&mut pc).is_some() {
                break;
            }
            db.step(0);
            db.step(1);
            db.vfs.complete_all_io();
        }
        ready(&mut pc).expect("answered").expect("committed");
    }
    let parked_before = db.engine.parked_decisions(1);
    let mut pc = db.submit(&last.to_be_bytes(), &value);
    for _ in 0..16 {
        db.step(0);
        db.step(1);
    }
    let parked = db.engine.parked_decisions(1) > parked_before;
    let early = ready(&mut pc);
    if parked {
        assert!(
            early.is_none(),
            "commit {last} answered with its decision parked"
        );
        assert!(!db.visible(1, &last.to_be_bytes()));
    }
    match early {
        Some(Ok(_)) => {
            let (a, b) = db.crash_and_reopen(&last.to_be_bytes());
            assert!(
                a && b,
                "commit {last} answered Ok but a process crash lost it: {a} {b}"
            );
        }
        Some(Err(e)) => panic!("commit {last}: {e:?}"),
        None => {
            // The sync completes: the parked decision runs, and the commit commits whole.
            for _ in 0..64 {
                db.vfs.complete_all_io();
                db.step(0);
                db.step(1);
                if ready(&mut pc).is_some() {
                    break;
                }
            }
            ready(&mut pc)
                .expect("answered once the I/O completed")
                .expect("committed");
            assert!(db.visible(0, &last.to_be_bytes()) && db.visible(1, &last.to_be_bytes()));
            let _ = db.engine.close();
            db.idle();
        }
    }
    parked
}

#[test]
fn a_decision_reaching_a_prepare_under_a_held_header_waits_for_its_sync() {
    let parked: Vec<u32> = (0..COMMITS).filter(|&last| held(last)).collect();
    assert!(
        !parked.is_empty(),
        "no run parked a decision: the test does not reach a held header"
    );
}
