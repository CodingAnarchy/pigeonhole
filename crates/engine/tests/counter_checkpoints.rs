//! D291 (#713): a counter get that folds many operands leaves its result in the active
//! memtable as a `Put` base at the seqno of the newest entry its fold read, so the counter's
//! next reads stop there. No read at any seqno can tell, and the shard keeps a checkpoint only
//! while that entry is in its active memtable. One engine-owned shard over `SimVfs`,
//! compaction off.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyId, FamilyOptions, Snapshot, TableId, ValueRef, WriteBatch,
};
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/counter_checkpoints.phdb";

struct Rig {
    db: Arc<Engine>,
    t: TableId,
    c: FamilyId,
}

impl Rig {
    /// `steps`: `EngineOptions::counter_checkpoint_steps` (1: every counter get that folds
    /// anything asks for a checkpoint).
    fn open(seed: u64, steps: u64) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o: EngineOptions = common::options(Arc::clone(&vfs), 1, 16 << 20);
        o.pin_threads = false;
        o.compaction.l0_trigger = u32::MAX;
        o.wal.segment_size = 4 << 20;
        o.counter_checkpoint_steps = steps;
        let db = Engine::open(Path::new(DB), o).unwrap();
        let family = FamilyOptions::default()
            .merge_operator("pigeonhole.i64_add")
            .kind(FamilyKind::Counter);
        let info = db.create_table("t", &[("c".into(), family)]).unwrap();
        let c = info.family("c").unwrap().id;
        Self { db, t: info.id, c }
    }

    fn incr(&self, row: &[u8], d: i64) {
        let mut wb = WriteBatch::new();
        wb.merge(self.t, self.c, row, b"n", ValueRef::I64(d))
            .unwrap();
        self.db.commit(wb, None).unwrap();
    }

    fn value(&self, cell: Option<pigeonhole_engine::CellData>) -> i64 {
        let Some(cell) = cell else {
            return 0;
        };
        match cell.value() {
            ValueRef::I64(v) => v,
            v => panic!("not an i64: {v:?}"),
        }
    }

    fn latest(&self, row: &[u8]) -> i64 {
        self.value(self.db.get_latest(self.t, self.c, row, b"n").unwrap())
    }

    fn at(&self, snap: &Snapshot, row: &[u8]) -> i64 {
        self.value(self.db.get(snap, self.t, self.c, row, b"n").unwrap())
    }

    /// Waits until the shard has handled `n` checkpoints in all (kept or dropped): a get
    /// sends its checkpoint without waiting. Returns `(kept, dropped)`.
    fn handled(&self, n: u64) -> (u64, u64) {
        for _ in 0..10_000 {
            let (k, d) = self.db.counter_checkpoints();
            if k + d >= n {
                return (k, d);
            }
            std::thread::sleep(Duration::from_micros(200));
        }
        panic!(
            "the shard handled {:?} checkpoints, not {n}",
            self.db.counter_checkpoints()
        );
    }
}

/// A get that folds the default 128 entries leaves a checkpoint; reads at, above and below
/// it agree with the operands, before and after later increments and a flush.
#[test]
fn a_long_fold_leaves_a_checkpoint_and_no_read_changes() {
    let r = Rig::open(1, 128);
    for _ in 0..100 {
        r.incr(b"r", 1);
    }
    let old = r.db.snapshot().unwrap();
    for _ in 0..200 {
        r.incr(b"r", 1);
    }
    assert_eq!(r.latest(b"r"), 300);
    assert_eq!(r.handled(1), (1, 0), "one checkpoint, kept");
    // The next read stops at it: too short a fold to ask for another.
    assert_eq!(r.latest(b"r"), 300);
    r.incr(b"r", 5);
    assert_eq!(r.latest(b"r"), 305);
    assert_eq!(r.at(&old, b"r"), 100, "a snapshot below the checkpoint");
    assert_eq!(r.db.counter_checkpoints(), (1, 0));
    // The checkpoint flushes as a base under the later operand (D287's combine).
    r.db.flush().unwrap();
    assert_eq!(r.latest(b"r"), 305);
    assert_eq!(r.at(&old, b"r"), 100);
}

/// The shard keeps a checkpoint only while the entry it stands at is in the active memtable:
/// one whose entry was flushed is dropped, with the active memtable empty or holding newer
/// entries (perf287's rotation race), and nothing reads differently.
#[test]
fn a_checkpoint_whose_entry_left_the_active_memtable_is_dropped() {
    let r = Rig::open(2, 1);
    for _ in 0..3 {
        r.incr(b"r", 1);
    }
    let snap = r.db.snapshot().unwrap();
    r.db.flush().unwrap();
    // The active memtable is empty: dropped.
    assert_eq!(r.at(&snap, b"r"), 3);
    assert_eq!(r.handled(1), (0, 1));
    // The active memtable holds a newer operand, but not the one the read stopped at.
    r.incr(b"r", 1);
    assert_eq!(r.at(&snap, b"r"), 3);
    assert_eq!(r.handled(2), (0, 2));
    // A read whose newest entry is in the active memtable: kept.
    assert_eq!(r.latest(b"r"), 4);
    assert_eq!(r.handled(3), (1, 2));
    assert_eq!(r.at(&snap, b"r"), 3);
    assert_eq!(r.latest(b"r"), 4);
}

/// D275: a checkpoint lands at the newest entry the read folded, not at the read point, so a
/// transaction whose snapshot lies between the two sees no write above its snapshot and
/// commits (perf287's must-fix: at the read point it would abort with `Conflict`).
#[test]
fn a_transaction_below_the_read_point_does_not_conflict_with_a_checkpoint() {
    let r = Rig::open(3, 1);
    r.incr(b"r", 1);
    r.incr(b"r", 1);
    let mut txn = r.db.begin().unwrap();
    // Later commits elsewhere move the read point past the transaction's snapshot.
    r.incr(b"other", 1);
    r.incr(b"other", 1);
    assert_eq!(r.latest(b"r"), 2);
    assert_eq!(r.handled(1), (1, 0));
    let got = txn.get(r.t, r.c, b"r", b"n").unwrap();
    assert_eq!(r.value(got), 2);
    txn.batch()
        .merge(r.t, r.c, b"r", b"n", ValueRef::I64(10))
        .unwrap();
    match txn.commit(None) {
        Ok(_) => {}
        Err(Error::Conflict) => panic!("a checkpoint read as a write above the snapshot"),
        Err(e) => panic!("{e}"),
    }
    assert_eq!(r.latest(b"r"), 12);
}
