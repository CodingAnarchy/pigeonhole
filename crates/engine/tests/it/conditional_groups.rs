//! A conditional commit (`check_and_mutate`) waits for the next group only when an earlier
//! member of its group wrote the cell its predicate reads (a write to that column, or a
//! delete of its family or row), and only the later commits on its rows wait with it.
//! Another cell of the same row cannot change the predicate, and commits in flight together
//! have no order across rows (D143), so neither waits a group, and a sync, for nothing.
//!
//! Application-owned with the one shard driven by the test: every commit a case submits
//! before driving the shard reaches it in one batch, so they form one group, and each group
//! at `GroupSync` is one WAL sync in the simulator's trace.

use crate::common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineShard, FamilyOptions, Predicate, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{SimOp, SimVfs};

const DB: &str = "/db/conditional.phdb";
const DURABLE: Option<Durability> = Some(Durability::GroupSync);

struct Rig {
    vfs: Arc<SimVfs>,
    db: Arc<Engine>,
    shard: EngineShard,
    t: Arc<TableInfo>,
}

fn open(seed: u64) -> Rig {
    let vfs = SimVfs::new(seed);
    let options = || common::options(Arc::clone(&vfs), 1, 64 << 20);
    let db = Engine::open(Path::new(DB), options()).unwrap();
    db.create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.close().unwrap();
    drop(db);
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options()).unwrap();
    let t = db.table("t").unwrap();
    let shard = shards.pop().unwrap();
    Rig { vfs, db, shard, t }
}

impl Rig {
    fn put(&self, row: &[u8], q: &[u8]) -> WriteBatch {
        let mut wb = WriteBatch::new();
        wb.put(
            self.t.id,
            self.t.families[0].id,
            row,
            q,
            None,
            ValueRef::Bytes(b"v"),
        )
        .unwrap();
        wb
    }

    fn exists(&self, q: &[u8]) -> Predicate {
        Predicate::Exists {
            family: self.t.families[0].id,
            qualifier: q.to_vec(),
        }
    }

    fn absent(&self, q: &[u8]) -> Predicate {
        Predicate::Absent {
            family: self.t.families[0].id,
            qualifier: q.to_vec(),
        }
    }

    /// Runs the shard until `pending` resolves.
    fn drive<F: Future + Unpin>(&mut self, mut pending: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..100_000 {
            self.shard.run_once(u64::MAX);
            if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
                return r;
            }
        }
        panic!("never resolved");
    }

    /// Commits `wb` on its own, before a case.
    fn commit(&mut self, wb: WriteBatch) {
        let pending = self.db.submit(wb, DURABLE).unwrap();
        self.drive(pending).unwrap();
    }

    /// Starts counting syncs: everything submitted from here to `syncs` is the case.
    fn start(&self) {
        self.vfs.record_ops();
    }

    /// The syncs since `start`: one per group at `GroupSync`.
    fn syncs(&self) -> usize {
        let ops = self.vfs.recorded_ops();
        ops.iter()
            .filter(|op| matches!(op, SimOp::Sync { .. }))
            .count()
    }
}

/// The case the narrowed check is for (ycsb-f's read-modify-write on another field of a hot
/// row): the conditional commit joins the group and shares its sync.
#[test]
fn a_predicate_on_another_cell_of_a_row_the_group_wrote_joins_the_group() {
    let mut r = open(7601);
    r.start();
    let a = r.db.submit(r.put(b"r", b"q1"), DURABLE).unwrap();
    let b =
        r.db.submit_check_and_mutate(r.t.id, b"r", &r.absent(b"q2"), r.put(b"r", b"q2"), DURABLE)
            .unwrap();
    let a = r.drive(a).unwrap();
    let (applied, b) = r.drive(b).unwrap();
    assert!(applied, "q2 was absent");
    assert!(b.unwrap().seqno > a.seqno);
    assert_eq!(r.syncs(), 1, "one group: {:?}", r.vfs.recorded_ops());
    r.db.close().unwrap();
}

/// A predicate on the cell an earlier member wrote is evaluated on the applied state, in
/// the next group: `Exists` holds only once that write applied.
#[test]
fn a_predicate_on_a_cell_the_group_wrote_waits_for_the_next_group() {
    let mut r = open(7602);
    r.start();
    let a = r.db.submit(r.put(b"r", b"q1"), DURABLE).unwrap();
    let b =
        r.db.submit_check_and_mutate(r.t.id, b"r", &r.exists(b"q1"), r.put(b"r", b"q3"), DURABLE)
            .unwrap();
    r.drive(a).unwrap();
    let (applied, _) = r.drive(b).unwrap();
    assert!(applied, "the predicate missed the earlier member's write");
    assert_eq!(r.syncs(), 2, "two groups: {:?}", r.vfs.recorded_ops());
    r.db.close().unwrap();
}

/// A family delete, and a row delete (one family marker per family, D10), covers every
/// column of the family: a predicate on any of them waits, and sees the delete.
#[test]
fn a_predicate_after_a_family_or_row_delete_in_the_group_sees_the_delete() {
    for (seed, whole_row) in [(7603, false), (7604, true)] {
        let mut r = open(seed);
        r.commit(r.put(b"r", b"q1"));
        r.start();
        let mut del = WriteBatch::new();
        if whole_row {
            del.delete_row(r.t.id, b"r", None).unwrap();
        } else {
            del.delete_family(r.t.id, r.t.families[0].id, b"r", None)
                .unwrap();
        }
        let a = r.db.submit(del, DURABLE).unwrap();
        let b = r
            .db
            .submit_check_and_mutate(r.t.id, b"r", &r.absent(b"q1"), r.put(b"r", b"q2"), DURABLE)
            .unwrap();
        r.drive(a).unwrap();
        let (applied, _) = r.drive(b).unwrap();
        assert!(
            applied,
            "row delete {whole_row}: the predicate missed the earlier delete"
        );
        assert_eq!(r.syncs(), 2, "row delete {whole_row}: two groups");
        r.db.close().unwrap();
    }
}

/// Behind a conditional commit held for the next group, a later commit on its row waits
/// with it (per-row submission order), and a commit on another row does not.
#[test]
fn only_commits_on_a_held_back_row_wait_with_it() {
    let mut r = open(7605);
    r.start();
    let a = r.db.submit(r.put(b"r", b"q1"), DURABLE).unwrap();
    let held =
        r.db.submit_check_and_mutate(r.t.id, b"r", &r.exists(b"q1"), r.put(b"r", b"q3"), DURABLE)
            .unwrap();
    let same_row = r.db.submit(r.put(b"r", b"q4"), DURABLE).unwrap();
    let other_row = r.db.submit(r.put(b"x", b"q1"), DURABLE).unwrap();
    let a = r.drive(a).unwrap().seqno;
    let other_row = r.drive(other_row).unwrap().seqno;
    let (applied, held) = r.drive(held).unwrap();
    assert!(applied);
    let held = held.unwrap().seqno;
    let same_row = r.drive(same_row).unwrap().seqno;
    assert!(
        a < other_row && other_row < held && held < same_row,
        "seqnos: a {a}, other row {other_row}, held {held}, same row {same_row}"
    );
    assert_eq!(r.syncs(), 2, "two groups: {:?}", r.vfs.recorded_ops());
    r.db.close().unwrap();
}

// ---- D275: a transaction's cell read in a group ----

/// A transaction that read `f:read` of `row` and writes `f:write` there, its snapshot taken
/// now: submitted once the case's earlier members are.
fn txn(r: &Rig, row: &[u8], read: &[u8], write: &[u8]) -> pigeonhole_engine::Txn {
    let f = r.t.families[0].id;
    let mut t = r.db.begin().unwrap();
    t.get(r.t.id, f, row, read).unwrap();
    t.batch()
        .put(r.t.id, f, row, write, None, ValueRef::Bytes(b"t"))
        .unwrap();
    t
}

/// ycsb-f's transactional read-modify-write on two fields of one hot row: the second
/// transaction reads a cell the first did not write, so it joins the group and shares its
/// sync, and both commit.
#[test]
fn a_transaction_reading_another_cell_of_a_row_the_group_wrote_joins_the_group() {
    let mut r = open(7611);
    r.commit(r.put(b"r", b"q1"));
    r.commit(r.put(b"r", b"q2"));
    let a = txn(&r, b"r", b"q1", b"q1");
    let b = txn(&r, b"r", b"q2", b"q2");
    r.start();
    let a = a.submit(DURABLE).unwrap();
    let b = b.submit(DURABLE).unwrap();
    let a = r.drive(a).unwrap();
    let b = r.drive(b).unwrap();
    assert!(b.seqno > a.seqno);
    assert_eq!(r.syncs(), 1, "one group: {:?}", r.vfs.recorded_ops());
    r.db.close().unwrap();
}

/// A write a case puts earlier in the group.
type EarlierWrite = fn(&Rig, &mut WriteBatch);

/// Each kind of write of the read cell earlier in the group (perf287's P4: a put, a cell
/// delete, a column delete, a family delete, a row delete) holds the transaction for the next
/// group, where its validation sees the write and it conflicts.
#[test]
fn a_transaction_reading_a_cell_the_group_wrote_waits_and_conflicts() {
    let kinds: [(&str, EarlierWrite); 5] = [
        ("put", |r, wb| {
            wb.put(
                r.t.id,
                r.t.families[0].id,
                b"r",
                b"q1",
                None,
                ValueRef::Bytes(b"x"),
            )
            .unwrap()
        }),
        ("delete_cell", |r, wb| {
            wb.delete_cell(r.t.id, r.t.families[0].id, b"r", b"q1", 1)
                .unwrap()
        }),
        ("delete_column", |r, wb| {
            wb.delete_column(r.t.id, r.t.families[0].id, b"r", b"q1", None)
                .unwrap()
        }),
        ("delete_family", |r, wb| {
            wb.delete_family(r.t.id, r.t.families[0].id, b"r", None)
                .unwrap()
        }),
        ("delete_row", |r, wb| {
            wb.delete_row(r.t.id, b"r", None).unwrap()
        }),
    ];
    for (seed, (kind, write)) in (7612..).zip(kinds) {
        let mut r = open(seed);
        r.commit(r.put(b"r", b"q1"));
        let t = txn(&r, b"r", b"q1", b"q3");
        let mut wb = WriteBatch::new();
        write(&r, &mut wb);
        let a = r.db.submit(wb, DURABLE).unwrap();
        let b = t.submit(DURABLE).unwrap();
        r.drive(a).unwrap();
        let b = r.drive(b);
        assert!(
            matches!(b, Err(pigeonhole_engine::Error::Conflict)),
            "{kind} of the read cell earlier in the group was missed: {b:?}"
        );
        r.db.close().unwrap();
    }
}

/// A transaction that read a row whole (`get_row`) still waits for any earlier write to the
/// row in its group, and conflicts with it.
#[test]
fn a_transaction_reading_a_row_whole_waits_for_any_write_to_it() {
    let mut r = open(7620);
    r.commit(r.put(b"r", b"q1"));
    let mut t = r.db.begin().unwrap();
    let mut sink = pigeonhole_engine::RowData::default();
    t.read_row_into(
        r.t.id,
        b"r",
        &[],
        &pigeonhole_engine::ReadSpec::default(),
        &mut sink,
    )
    .unwrap();
    t.batch()
        .put(
            r.t.id,
            r.t.families[0].id,
            b"r",
            b"q3",
            None,
            ValueRef::Bytes(b"t"),
        )
        .unwrap();
    let a = r.db.submit(r.put(b"r", b"q2"), DURABLE).unwrap();
    let b = t.submit(DURABLE).unwrap();
    r.drive(a).unwrap();
    let b = r.drive(b);
    assert!(
        matches!(b, Err(pigeonhole_engine::Error::Conflict)),
        "a write to the row read whole was missed: {b:?}"
    );
    r.db.close().unwrap();
}

/// In a counter family (perf287's P4): an increment of the read counter earlier in the group
/// holds the transaction and it conflicts; an increment of another counter of the row lets it
/// join the group.
#[test]
fn a_transaction_after_an_increment_in_the_group_waits_only_for_its_own_counter() {
    for (seed, incremented, conflicts) in [(7621, &b"n1"[..], true), (7622, b"n2", false)] {
        let mut r = open(seed);
        let ctr =
            r.db.create_table(
                "ctr",
                &[(
                    "c".into(),
                    FamilyOptions::default()
                        .merge_operator("pigeonhole.i64_add")
                        .kind(pigeonhole_format::manifest::FamilyKind::Counter),
                )],
            )
            .unwrap();
        let c = ctr.family("c").unwrap().id;
        let mut wb = WriteBatch::new();
        for q in [&b"n1"[..], b"n2"] {
            wb.put(ctr.id, c, b"r", q, None, ValueRef::I64(100))
                .unwrap();
        }
        r.commit(wb);
        let mut t = r.db.begin().unwrap();
        t.get(ctr.id, c, b"r", b"n1").unwrap();
        t.batch()
            .put(
                r.t.id,
                r.t.families[0].id,
                b"out",
                b"q",
                None,
                ValueRef::Bytes(b"t"),
            )
            .unwrap();
        r.start();
        let mut wb = WriteBatch::new();
        wb.merge(ctr.id, c, b"r", incremented, ValueRef::I64(1))
            .unwrap();
        let a = r.db.submit(wb, DURABLE).unwrap();
        let b = t.submit(DURABLE).unwrap();
        r.drive(a).unwrap();
        let b = r.drive(b);
        if conflicts {
            assert!(
                matches!(b, Err(pigeonhole_engine::Error::Conflict)),
                "an increment of the read counter earlier in the group was missed: {b:?}"
            );
        } else {
            b.unwrap();
            assert_eq!(r.syncs(), 1, "one group: {:?}", r.vfs.recorded_ops());
        }
        r.db.close().unwrap();
    }
}
