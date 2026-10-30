//! D291 (#713): a counter get that folds many operands leaves its result in the active
//! memtable as a `Put` base at the seqno of the newest entry its fold read, so the counter's
//! next reads stop there. No read at any seqno can tell, and the shard keeps a checkpoint only
//! while that entry is in its active memtable. One engine-owned shard over `SimVfs`,
//! compaction off.

use crate::common;

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
    assert_eq!(
        r.db.counter_checkpoints(),
        (1, 0),
        "only the first fold asked for one"
    );
    // The snapshot read below the checkpoint passes the ~200 entries above its snapshot too,
    // and may send a second checkpoint, at its own newest folded entry, asynchronously: its
    // outcome is not asserted (a race with this thread, perf2 on #1055); only the reads.
    assert_eq!(r.at(&old, b"r"), 100, "a snapshot below the checkpoint");
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

/// D287's flush of a memtable holding a checkpoint, with a snapshot stripe below it (perf287's
/// should-fix): the flush combines each stripe around the base, every read point reads what it
/// read before, and the SST holds a few entries rather than every operand.
#[test]
fn a_flush_of_a_memtable_holding_a_checkpoint_combines_each_stripe() {
    use pigeonhole_format::Kind;
    use pigeonhole_format::key::decode_key;

    let r = Rig::open(4, 1);
    for _ in 0..100 {
        r.incr(b"r", 1);
    }
    let mid = r.db.snapshot().unwrap();
    for _ in 0..100 {
        r.incr(b"r", 2);
    }
    // The checkpoint, at the newest operand, above the snapshot.
    assert_eq!(r.latest(b"r"), 300);
    assert_eq!(r.handled(1), (1, 0));
    for _ in 0..50 {
        r.incr(b"r", 4);
    }
    let entries = |r: &Rig| -> Vec<(Kind, u64)> {
        let snap = r.db.snapshot().unwrap();
        r.db.raw_entries(&snap)
            .unwrap()
            .iter()
            .filter(|e| e.family == r.c)
            .map(|e| {
                let k = decode_key(&e.key).unwrap();
                (k.kind, k.seqno)
            })
            .collect()
    };
    let before = entries(&r);
    assert_eq!(before.len(), 251, "250 operands and the checkpoint");
    let puts: Vec<u64> = before
        .iter()
        .filter(|(k, _)| *k == Kind::Put)
        .map(|(_, s)| *s)
        .collect();
    assert_eq!(puts.len(), 1);
    assert!(
        puts[0] > mid.seqno(),
        "the checkpoint is above the snapshot"
    );

    r.db.flush().unwrap();
    assert_eq!(r.at(&mid, b"r"), 100, "the snapshot's stripe");
    assert_eq!(r.latest(b"r"), 500, "the newest read");
    let after = entries(&r);
    assert!(
        after.len() <= 3,
        "each stripe combined around the base: {after:?}"
    );
    // The snapshot's stripe kept its own (combined) operand below it.
    assert!(
        after.iter().any(|(_, s)| *s <= mid.seqno()),
        "the snapshot stripe's operands: {after:?}"
    );
}

/// #1051's hazard at the engine (D287's flush combine and the compaction of L0 files): each
/// round leaves a checkpoint above that round's operands, then more operands, a snapshot and
/// a flush, so every L0 file holds a checkpoint between combinable operands, and the slot's
/// other files sit beside it. Every snapshot reads its own total after the flushes and after
/// a full compaction.
#[test]
fn checkpoints_in_l0_files_keep_every_read_through_flushes_and_compaction() {
    let r = Rig::open(5, 1);
    let mut total = 0i64;
    let mut snaps = Vec::new();
    let mut asked = 0;
    for round in 0..6i64 {
        for i in 0..20 {
            r.incr(b"r", round * 100 + i + 1);
            total += round * 100 + i + 1;
        }
        assert_eq!(r.latest(b"r"), total);
        asked += 1;
        r.handled(asked);
        for _ in 0..5 {
            r.incr(b"r", 1);
            total += 1;
        }
        snaps.push((r.db.snapshot().unwrap(), total));
        r.db.flush().unwrap();
        for (s, want) in &snaps {
            assert_eq!(r.at(s, b"r"), *want, "after flush {round}");
        }
    }
    assert!(r.db.counter_checkpoints().0 >= 1, "no checkpoint was kept");
    r.db.compact(None).unwrap();
    for (s, want) in &snaps {
        assert_eq!(r.at(s, b"r"), *want, "after the compaction");
    }
    assert_eq!(r.latest(b"r"), total);
}

/// A read at an old snapshot counts toward the checkpoint trigger only the entries its fold
/// reads, not the newer ones it steps over: 100 operands under the snapshot and 200 above
/// it ask for nothing at the default 128. A latest read of all 300 then asks for one, and
/// the only checkpoint is that read's, above the snapshot (the shard handles a client's
/// checkpoints in the order sent).
#[test]
fn an_old_snapshot_read_does_not_count_the_entries_above_it() {
    use pigeonhole_format::Kind;
    use pigeonhole_format::key::decode_key;

    let r = Rig::open(6, 128);
    for _ in 0..100 {
        r.incr(b"r", 1);
    }
    let old = r.db.snapshot().unwrap();
    for _ in 0..200 {
        r.incr(b"r", 1);
    }
    assert_eq!(r.at(&old, b"r"), 100);
    assert_eq!(r.latest(b"r"), 300);
    assert_eq!(r.handled(1), (1, 0));
    let snap = r.db.snapshot().unwrap();
    let puts: Vec<u64> =
        r.db.raw_entries(&snap)
            .unwrap()
            .iter()
            .filter(|e| e.family == r.c)
            .filter_map(|e| {
                let k = decode_key(&e.key).unwrap();
                (k.kind == Kind::Put).then_some(k.seqno)
            })
            .collect();
    assert_eq!(puts.len(), 1, "one checkpoint: {puts:?}");
    assert!(
        puts[0] > old.seqno(),
        "the old snapshot's read asked for a checkpoint: {puts:?}"
    );
    assert_eq!(r.db.counter_checkpoints(), (1, 0));
    assert_eq!(r.at(&old, b"r"), 100);
}
