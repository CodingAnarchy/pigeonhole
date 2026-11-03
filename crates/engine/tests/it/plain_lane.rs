//! #1283 lever 3: a reduction lane over one SST with nothing to resolve reads its cells
//! straight from the block (`ReduceStats::plain_lanes`). Wherever it is taken, a reduction
//! returns exactly the cells a scan (through the MVCC resolver) returns, including columns
//! with several versions in any ts and seqno order. Wherever one of its conditions fails
//! (simp724's review on #1283), it is not taken, and the result is still the scan's.

use crate::common;

use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    CellVisitor, Engine, EngineShard, FamilyOptions, Limit, ReadInput, ReadSpec, ReduceSpec,
    ScanSpec, Snapshot, ValuePredicate, ValueRef, Visit, WriteBatch,
};
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_format::{Durability, FamilyId, TableId, Timestamp};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const ROWS: u32 = 200;

type Cell = (Vec<u8>, FamilyId, Vec<u8>, Timestamp, Vec<u8>);

fn drive<F: Future + Unpin>(shards: &mut [EngineShard], mut pending: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
            return r;
        }
    }
    panic!("never resolved");
}

fn row(i: u32) -> Vec<u8> {
    format!("r{i:04}").into_bytes()
}

struct Db {
    engine: Arc<Engine>,
    shards: Vec<EngineShard>,
    table: TableId,
    family: FamilyId,
}

impl Db {
    fn open(options: FamilyOptions) -> Db {
        let vfs = SimVfs::new(0x1283);
        let opts = || common::options(Arc::clone(&vfs), 2, 64 << 20);
        let db = Engine::open(Path::new(DB), opts()).unwrap();
        let t = db.create_table("t", &[("f".into(), options)]).unwrap();
        db.close().unwrap();
        drop(db);
        let (engine, shards) = Engine::open_application_owned(Path::new(DB), opts()).unwrap();
        Db {
            engine,
            shards,
            table: t.id,
            family: t.families[0].id,
        }
    }

    fn commit(&mut self, f: impl FnOnce(&mut WriteBatch, TableId, FamilyId)) {
        let mut wb = WriteBatch::new();
        f(&mut wb, self.table, self.family);
        let pending = self.engine.submit(wb, Some(Durability::Buffered)).unwrap();
        drive(&mut self.shards, pending).unwrap();
    }

    fn flush(&mut self) {
        let pending = self.engine.submit_flush().unwrap();
        drive(&mut self.shards, pending).unwrap();
    }

    fn compact(&mut self) {
        let pending = self.engine.submit_compact(Some(self.table)).unwrap();
        drive(&mut self.shards, pending).unwrap();
    }

    /// Columns with several versions in every order the resolver ranks: a newer timestamp
    /// written later, an older one written later (a `put_at` with an old event time: a
    /// higher seqno that sorts lower), and the same timestamp overwritten.
    fn write_versions(&mut self) {
        for i in 0..ROWS {
            self.commit(|wb, t, f| {
                for q in [b"a", b"b", b"c"] {
                    let v = format!("{i}/{}/100", q[0] as char);
                    wb.put(t, f, &row(i), q, Some(100), ValueRef::Bytes(v.as_bytes()))
                        .unwrap();
                }
            });
        }
        for i in (0..ROWS).step_by(3) {
            self.commit(|wb, t, f| {
                wb.put(t, f, &row(i), b"a", Some(200), ValueRef::Bytes(b"newer"))
                    .unwrap();
                wb.put(
                    t,
                    f,
                    &row(i),
                    b"b",
                    Some(50),
                    ValueRef::Bytes(b"older, later"),
                )
                .unwrap();
                wb.put(
                    t,
                    f,
                    &row(i),
                    b"c",
                    Some(100),
                    ValueRef::Bytes(b"same ts, later"),
                )
                .unwrap();
            });
        }
    }

    /// The reduction's cells (every cell, in order) and how many lanes were plain, beside
    /// the scan's cells at the same snapshot.
    fn both(&self, snap: &Snapshot, read: &ReadSpec) -> (Vec<Cell>, u64, Vec<Cell>) {
        let mut spec = ReduceSpec::new(ReadInput::Ranges(vec![(
            Bound::Unbounded,
            Bound::Unbounded,
        )]));
        spec.read = read.clone();
        let r = self
            .engine
            .reduce(snap, self.table, &spec, || Every, || Limit::new(usize::MAX))
            .unwrap();
        let reduced = r
            .items
            .into_iter()
            .map(|(k, v)| (k.row, k.family, k.qualifier, k.ts, v))
            .collect();
        let mut scan = ScanSpec::new(Bound::Unbounded, Bound::Unbounded);
        scan.read = read.clone();
        let mut cur = self.engine.scan(snap, self.table, scan).unwrap();
        let mut scanned = Vec::new();
        while cur.next_row().unwrap() {
            let r = cur.row().to_vec();
            while let Some(c) = cur.next_cell().unwrap() {
                scanned.push((
                    r.clone(),
                    c.family,
                    c.qualifier.to_vec(),
                    c.ts,
                    c.stored.to_vec(),
                ));
            }
        }
        (reduced, r.stats.plain_lanes, scanned)
    }

    /// Asserts the reduction equals the scan, with the plain lane taken or not.
    fn check(&self, read: &ReadSpec, plain: bool, what: &str) {
        let snap = self.engine.snapshot().unwrap();
        let (reduced, plain_lanes, scanned) = self.both(&snap, read);
        assert!(!scanned.is_empty(), "{what}: nothing read");
        assert_eq!(
            reduced, scanned,
            "{what}: the reduction differs from the scan"
        );
        assert_eq!(plain_lanes > 0, plain, "{what}: plain lanes {plain_lanes}");
    }
}

/// The newest version of each column (the family keeps every version, so its SST holds
/// several).
fn one() -> ReadSpec {
    let mut r = ReadSpec::default();
    r.versions = 1;
    r
}

/// Every cell, its stored value as the item.
struct Every;

impl CellVisitor for Every {
    type Item = Vec<u8>;
    fn visit(&mut self, _: &[u8], _: FamilyId, _: &[u8], _: Timestamp, v: &[u8]) -> Visit<Vec<u8>> {
        Visit::Emit(v.to_vec())
    }
}

#[test]
fn one_compacted_sst_reads_plain_and_equals_the_scan() {
    let mut db = Db::open(FamilyOptions::default());
    db.write_versions();
    db.flush();
    db.compact();
    db.check(&one(), true, "compacted");
    // The newest of each column, whatever order its versions were written in.
    let snap = db.engine.snapshot().unwrap();
    let (cells, _, _) = db.both(&snap, &one());
    let get = |i: u32, q: &[u8]| {
        cells
            .iter()
            .find(|c| c.0 == row(i) && c.2 == q)
            .map(|c| (c.3, c.4[1..].to_vec()))
    };
    assert_eq!(get(0, b"a"), Some((200, b"newer".to_vec())));
    assert_eq!(get(0, b"b"), Some((100, b"0/b/100".to_vec())));
    assert_eq!(get(0, b"c"), Some((100, b"same ts, later".to_vec())));
    assert_eq!(get(1, b"a"), Some((100, b"1/a/100".to_vec())));
}

#[test]
fn a_condition_that_fails_keeps_the_resolver() {
    // A delete in the one SST: a cell delete, and (separately) only a row's family marker.
    let mut db = Db::open(FamilyOptions::default());
    db.write_versions();
    db.commit(|wb, t, f| wb.delete_cell(t, f, &row(4), b"a", 100).unwrap());
    db.flush();
    db.check(&one(), false, "a cell delete in the SST");

    let mut db = Db::open(FamilyOptions::default());
    db.write_versions();
    db.commit(|wb, t, _| wb.delete_row(t, &row(7), None).unwrap());
    db.flush();
    db.check(&one(), false, "only a row delete's family marker");

    // A memtable beside the SST, with an entry in the range.
    let mut db = Db::open(FamilyOptions::default());
    db.write_versions();
    db.flush();
    db.compact();
    db.commit(|wb, t, f| {
        wb.put(
            t,
            f,
            &row(9),
            b"a",
            Some(300),
            ValueRef::Bytes(b"in the memtable"),
        )
        .unwrap();
    });
    db.check(&one(), false, "a memtable entry");

    // Read options that need the resolver.
    let mut db = Db::open(FamilyOptions::default());
    db.write_versions();
    db.flush();
    db.compact();
    let mut versions = one();
    versions.versions = 0;
    db.check(&versions, false, "every version");
    let mut time = one();
    time.time_range = Some((0, 150));
    db.check(&time, false, "a time range");
    let mut columns = one();
    columns.columns_per_row = 1;
    db.check(&columns, false, "a column limit");
    let mut value = one();
    value.value = Some(ValuePredicate::Range(Bound::Unbounded, Bound::Unbounded));
    db.check(&value, false, "a value predicate");

    // Families that need the resolver: a TTL, and a merge operator.
    let mut db = Db::open(FamilyOptions::default().ttl_micros(u64::MAX / 4));
    db.write_versions();
    db.flush();
    db.compact();
    db.check(&one(), false, "a TTL");

    let mut db = Db::open(
        FamilyOptions::default()
            .merge_operator("pigeonhole.i64_add")
            .kind(FamilyKind::Counter),
    );
    for i in 0..ROWS {
        db.commit(|wb, t, f| {
            wb.merge(t, f, &row(i), b"n", ValueRef::I64(i64::from(i)))
                .unwrap();
            wb.merge(t, f, &row(i), b"n", ValueRef::I64(1)).unwrap();
        });
    }
    db.flush();
    db.compact();
    db.check(&one(), false, "a counter family");
}

/// simp724's last case on #1283: after a split, both tablets read the one compacted SST,
/// which holds rows past the left tablet's end. Each tablet's plain lane stops at its own
/// end, and the reduction still equals the scan.
#[test]
fn a_shared_sst_after_a_split_reads_plain_within_each_tablet() {
    let mut db = Db::open(FamilyOptions::default());
    db.write_versions();
    db.flush();
    db.compact();
    for at in [row(ROWS / 3), row(2 * ROWS / 3)] {
        let split = db.engine.split_tablet_pending(db.table, &at).unwrap();
        drive(&mut db.shards, split).unwrap();
    }
    let snap = db.engine.snapshot().unwrap();
    let (reduced, plain_lanes, scanned) = db.both(&snap, &one());
    assert_eq!(reduced, scanned, "the reduction differs from the scan");
    assert!(plain_lanes >= 3, "a plain lane per tablet: {plain_lanes}");
    // No row twice, none missing.
    let rows: std::collections::BTreeSet<_> = reduced.iter().map(|c| c.0.clone()).collect();
    assert_eq!(rows.len(), ROWS as usize);
}
