//! D314 (simp724's Q2 on #1440): a returning commit folds its counters at its apply, so
//! every seqno below it that writes its row must have applied on its shard first. A
//! returning single counts its row as read, as a conditional single's predicate row is:
//! it waits for a share undecided on the row (`overlaps_undecided`), and a PREPARE writing
//! the row waits for it (`read_rows`). Both shards are driven by hand; shard 0 coordinates
//! the cross-shard commits, shard 1 owns the counter. `Fault::HoldDecides` keeps a share's
//! decision from reaching shard 1, so its seqno is reserved but not applied there.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyId, FamilyOptions, Fault, TableId, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

struct Db {
    vfs: Arc<SimVfs>,
    engine: Arc<Engine>,
    shards: Vec<EngineShard>,
    /// A counter table owned by shard 0 and one owned by shard 1, each with family `c`.
    tables: [(TableId, FamilyId); 2],
}

fn open(seed: u64) -> Db {
    let vfs = SimVfs::new(seed);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.reader_slots = 4;
    let (engine, shards) = Engine::open_application_owned(Path::new(DB), o).expect("open");
    let counter = FamilyOptions::default()
        .merge_operator("pigeonhole.i64_add")
        .kind(FamilyKind::Counter);
    let mut owned: [Option<(TableId, FamilyId)>; 2] = [None, None];
    for i in 0.. {
        let t = engine
            .create_table(&format!("t{i}"), &[("c".into(), counter.clone())])
            .expect("table");
        let snap = engine.snapshot().unwrap();
        let (_, shard) = snap.view().tablets().route(t.id, b"r").unwrap();
        let slot = &mut owned[usize::from(shard.0)];
        if slot.is_none() {
            *slot = Some((t.id, t.families[0].id));
        }
        if owned.iter().all(Option::is_some) {
            break;
        }
    }
    let mut db = Db {
        vfs,
        engine,
        shards,
        tables: [owned[0].unwrap(), owned[1].unwrap()],
    };
    db.drive(64);
    db
}

impl Db {
    /// Runs each shard once, `rounds` times.
    fn drive(&mut self, rounds: usize) {
        for _ in 0..rounds {
            for s in &mut self.shards {
                let now = pigeonhole_io::Vfs::monotonic_nanos(&*self.vfs);
                s.run_once(now + 1_000);
            }
        }
    }

    /// Drives the shards until `f` is answered (at most 200 rounds).
    fn answer<F: std::future::Future + Unpin>(&mut self, f: &mut F) -> F::Output {
        for _ in 0..200 {
            if let Some(v) = ready(f) {
                return v;
            }
            self.drive(1);
        }
        panic!("not answered");
    }

    fn incr(&self, b: &mut WriteBatch, shard: usize, d: i64) {
        let (t, c) = self.tables[shard];
        b.merge(t, c, b"r", b"n", ValueRef::I64(d)).unwrap();
    }

    /// A cross-shard commit adding `d` to the counter on shard 1 (and 1 on shard 0's),
    /// submitted to its coordinator, shard 0.
    fn cross(&self, d: i64) -> pigeonhole_engine::PendingCommit {
        let mut b = WriteBatch::new();
        self.incr(&mut b, 0, 1);
        self.incr(&mut b, 1, d);
        self.engine
            .submit(b, Some(Durability::Buffered))
            .expect("submit")
    }

    /// A returning `+d` of the counter on shard 1.
    fn returning(&self, d: i64) -> pigeonhole_engine::PendingReturning {
        let mut b = WriteBatch::new();
        self.incr(&mut b, 1, d);
        self.engine
            .submit_returning(b, Some(Durability::Buffered))
            .expect("submit")
    }

    fn value(&self) -> i64 {
        let (t, c) = self.tables[1];
        let cell = self.engine.get_latest(t, c, b"r", b"n").unwrap().unwrap();
        i64::from_le_bytes(cell.stored()[1..9].try_into().unwrap())
    }
}

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// Base 5; a cross-shard `+1000` decided, its decision held from shard 1; then a returning
/// `+1` on shard 1. The returning commit must wait for the share (it would otherwise apply
/// above it and fold without it) and return 1006.
#[test]
fn a_returning_commit_waits_for_a_share_undecided_on_its_row() {
    let mut db = open(1440);
    let mut base = db.returning(5);
    db.answer(&mut base).unwrap();
    db.engine.inject(Fault::HoldDecides, true);
    let mut cross = db.cross(1000);
    db.drive(20);
    let mut r = db.returning(1);
    db.drive(20);
    assert!(
        ready(&mut r).is_none(),
        "the returning commit was answered while a share on its row was undecided there"
    );
    db.engine.inject(Fault::HoldDecides, false);
    assert_eq!(db.engine.release_held_decides(usize::MAX), 1);
    let (info, cells) = db.answer(&mut r).unwrap();
    let x = db.answer(&mut cross).unwrap();
    assert!(x.seqno < info.seqno);
    assert_eq!(cells.len(), 1);
    assert_eq!(*cells[0].value.as_ref().unwrap(), Some(1006));
    assert_eq!(db.value(), 1006);
}

/// As above, then a second cross-shard `+20000` submitted while the returning commit waits.
/// Its PREPARE writes the row the waiting commit reads, so it must wait for that commit; let
/// in, it would decide below it, and the returning value would leave out an operand at a
/// lower seqno. With the rule its seqno is above the returning commit's, and the value is
/// 1006.
#[test]
fn a_share_writing_a_waiting_returning_row_comes_after_it() {
    let mut db = open(1441);
    let mut base = db.returning(5);
    db.answer(&mut base).unwrap();
    db.engine.inject(Fault::HoldDecides, true);
    let mut first = db.cross(1000);
    db.drive(20);
    let mut r = db.returning(1);
    db.drive(20);
    let mut second = db.cross(20_000);
    db.drive(20);
    // Only the first decision lands: the returning commit then runs, while the second
    // share (if it was let in) is still undecided on shard 1.
    assert_eq!(db.engine.release_held_decides(1), 1);
    db.drive(20);
    db.engine.inject(Fault::HoldDecides, false);
    db.engine.release_held_decides(usize::MAX);
    let (info, cells) = db.answer(&mut r).unwrap();
    let a = db.answer(&mut first).unwrap();
    let b = db.answer(&mut second).unwrap();
    assert!(a.seqno < info.seqno);
    assert!(
        b.seqno > info.seqno,
        "a share writing the row decided at {} below the waiting returning commit's {}",
        b.seqno,
        info.seqno
    );
    assert_eq!(*cells[0].value.as_ref().unwrap(), Some(1006));
    assert_eq!(db.value(), 21_006);
}

/// D314 amendment 1 (#1445): a commit that only asks for a previous value (it writes the
/// row, but no counter cell: a column delete of another qualifier) waits as a counting one
/// does, through `Reply::Returning`. Base 5, a cross-shard `+1000` with its decision held;
/// the previous value of the counter must be 1005, not 5.
#[test]
fn a_previous_only_commit_waits_for_a_share_undecided_on_its_row() {
    let mut db = open(1445);
    let mut base = db.returning(5);
    db.answer(&mut base).unwrap();
    db.engine.inject(Fault::HoldDecides, true);
    let mut cross = db.cross(1000);
    db.drive(20);
    let (t, c) = db.tables[1];
    let mut b = WriteBatch::new();
    b.delete_column(t, c, b"r", b"other", None).unwrap();
    let mut r = db
        .engine
        .submit_returning_previous(b, Some(Durability::Buffered), vec![(c, b"n".to_vec())])
        .expect("submit");
    db.drive(20);
    assert!(
        ready(&mut r).is_none(),
        "the previous-only commit was answered while a share on its row was undecided there"
    );
    db.engine.inject(Fault::HoldDecides, false);
    assert_eq!(db.engine.release_held_decides(usize::MAX), 1);
    let (info, cells) = db.answer(&mut r).unwrap();
    let x = db.answer(&mut cross).unwrap();
    assert!(x.seqno < info.seqno);
    assert!(cells.is_empty());
    let previous = r.take_previous();
    assert_eq!(previous.len(), 1);
    let cell = previous[0].value.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(
        i64::from_le_bytes(cell.stored()[1..9].try_into().unwrap()),
        1005
    );
}

/// A returning batch is one row (simp724's B1 on #1453): its cells come back by family,
/// qualifier and timestamp, which don't say which row. Two rows on one shard are refused
/// before anything is sent, by the blocking and the async form alike.
#[test]
fn a_returning_batch_of_two_rows_is_refused() {
    let db = open(1442);
    let (t, c) = db.tables[1];
    let two = || {
        let mut b = WriteBatch::new();
        b.merge(t, c, b"r", b"n", ValueRef::I64(1)).unwrap();
        b.merge(t, c, b"r2", b"n", ValueRef::I64(1)).unwrap();
        b
    };
    let e = db.engine.submit_returning(two(), None).unwrap_err();
    assert!(
        matches!(e, pigeonhole_engine::Error::InvalidArgument(_)),
        "{e:?}"
    );
    let e = db.engine.commit_returning(two(), None).unwrap_err();
    assert!(
        matches!(e, pigeonhole_engine::Error::InvalidArgument(_)),
        "{e:?}"
    );
    assert!(db.engine.get_latest(t, c, b"r", b"n").unwrap().is_none());
}
