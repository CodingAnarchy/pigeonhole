//! Behavior of the public API that the guide and the decisions pin down: table builders,
//! deferred builder errors, the error-code mapping, read projections and filters (D22, D39),
//! delete rules (D9, D38), batches, conditional commits, transactions, durability, snapshots,
//! the owned and borrowed result types, the C-ABI-shaped forms, application-owned mode
//! (D40), the writer lock, and reopening real files.

use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole::{
    Cell, Compaction, Condition, Durability, ErrorCode, Family, Options, Pigeonhole, Priority, Row,
    Table, Value, ValueFilter, days,
};
use pigeonhole_io::sim::SimVfs;

/// A fresh in-memory database (no disk).
fn db() -> Pigeonhole {
    let vfs = SimVfs::new(7);
    Pigeonhole::open("/db/api.phdb", sim_options(&vfs)).expect("open")
}

fn sim_options(vfs: &Arc<SimVfs>) -> Options {
    Options::default()
        .vfs(Arc::clone(vfs) as _)
        .shards(2)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
}

fn table(db: &Pigeonhole) -> Table {
    db.table("t")
        .unwrap()
        .family("a", Family::default())
        .family("b", Family::default().max_versions(2))
        .create_if_missing()
        .unwrap()
}

/// `(family, qualifier, value)` of every cell of a row read.
fn cells(t: &Table, row: &[u8]) -> Vec<(String, Vec<u8>, Vec<u8>)> {
    t.row(row)
        .versions(0)
        .read()
        .unwrap()
        .map(|r| {
            r.iter()
                .map(|e| {
                    (
                        e.family.to_owned(),
                        e.qualifier.to_vec(),
                        e.cell.value().to_vec(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn value(t: &Table, row: &[u8], family: &str, q: &[u8]) -> Option<Vec<u8>> {
    t.get(row, family, q).unwrap().map(|c| c.value().to_vec())
}

/// A temporary directory removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("pigeonhole-api-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// ---- tables ----

#[test]
fn table_builder_create_open_and_families() {
    let db = db();
    assert_eq!(
        db.table("t").unwrap().open().unwrap_err().code(),
        ErrorCode::TableNotFound
    );
    let t = db
        .table("t")
        .unwrap()
        .family("a", Family::default().max_versions(1))
        .family("a", Family::default())
        .create()
        .unwrap();
    assert_eq!(t.name(), "t");
    assert_eq!(t.families(), ["a"]);
    assert_eq!(
        db.table("t").unwrap().create().unwrap_err().code(),
        ErrorCode::TableExists
    );
    // An existing family keeps its stored options; a missing one is added.
    let t2 = db
        .table("t")
        .unwrap()
        .family("a", Family::default().max_versions(0))
        .family("b", Family::default())
        .create_if_missing()
        .unwrap();
    assert_eq!(t2.families(), ["a", "b"]);
    for ts in 1..=3 {
        t2.mutate(b"r")
            .put_at("a", b"q", ts, b"v")
            .commit()
            .unwrap();
    }
    assert_eq!(cells(&t2, b"r").len(), 1, "max_versions(1) kept");
    // The older handle resolves the family added through the newer one.
    t.mutate(b"r").put("b", b"q", b"x").commit().unwrap();
    assert_eq!(value(&t, b"r", "b", b"q").as_deref(), Some(&b"x"[..]));
    assert_eq!(db.tables(), ["t"]);
}

#[test]
fn drop_table_removes_it_and_its_data() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r").put("a", b"q", b"v").commit().unwrap();
    db.drop_table("t").unwrap();
    assert!(db.tables().is_empty());
    assert_eq!(
        db.drop_table("t").unwrap_err().code(),
        ErrorCode::TableNotFound
    );
    let t = table(&db);
    assert_eq!(value(&t, b"r", "a", b"q"), None);
}

#[test]
fn later_phase_family_settings_are_refused() {
    let db = db();
    for family in [
        Family::default().zstd(3),
        Family::default().compaction(Compaction::Tiered),
        Family::default()
            .ttl(days(1))
            .compaction(Compaction::FifoByTime),
    ] {
        let err = db
            .table("t")
            .unwrap()
            .family("f", family)
            .create_if_missing()
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Unsupported, "{err}");
    }
    assert!(db.tables().is_empty());
    // A custom merge operator is not registered in this process.
    let err = db
        .table("t")
        .unwrap()
        .family("f", Family::default().merge_operator("app.append"))
        .create_if_missing()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::UnknownMergeOperator);
    // Everything else is accepted and stored.
    let t = db
        .table("t")
        .unwrap()
        .family(
            "f",
            Family::default()
                .max_versions(3)
                .ttl(days(30))
                .bloom_bits(0)
                .blob_threshold(1 << 20)
                .uncompressed()
                .lz4()
                .block_size(4096)
                .cache_priority(Priority::High)
                .compaction(Compaction::Leveled),
        )
        .create()
        .unwrap();
    assert_eq!(t.families(), ["f"]);
}

// ---- errors ----

#[test]
fn builder_errors_surface_at_the_terminal_call() {
    let db = db();
    let t = table(&db);
    let code = |r: pigeonhole::Result<_>| r.map(|_: ()| ()).unwrap_err().code();
    // The first error wins; later builder calls are ignored.
    let err = t
        .mutate(b"r")
        .put("nope", b"q", b"v")
        .put("a", &[0u8; 70_000], b"v")
        .commit()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::FamilyNotFound);
    assert!(err.message().contains("nope"), "{err}");
    assert_eq!(err.to_string(), err.message());
    assert_eq!(
        code(
            t.mutate(b"r")
                .put("a", &[0u8; 70_000], b"v")
                .commit()
                .map(|_| ())
        ),
        ErrorCode::KeyTooLarge
    );
    assert_eq!(
        code(t.mutate(&[0u8; 70_000]).delete_row().commit().map(|_| ())),
        ErrorCode::KeyTooLarge
    );
    assert_eq!(
        code(t.get(b"r", "nope", b"q").map(|_| ())),
        ErrorCode::FamilyNotFound
    );
    assert_eq!(
        code(t.row(b"r").family("a").family("nope").read().map(|_| ())),
        ErrorCode::FamilyNotFound
    );
    assert_eq!(
        code(t.scan_prefix(b"").families(["nope"]).iter().map(|_| ())),
        ErrorCode::FamilyNotFound
    );
    let mut wb = db.write_batch();
    wb.put(&t, b"r", "nope", b"q", b"v")
        .put(&t, b"r", "a", b"q", b"v");
    assert_eq!(code(wb.commit().map(|_| ())), ErrorCode::FamilyNotFound);
    // Nothing was applied by the failed commits.
    assert_eq!(cells(&t, b"r"), []);
}

#[test]
fn error_codes_are_stable_numbers() {
    let codes = [
        (ErrorCode::Io, 1),
        (ErrorCode::Corruption, 2),
        (ErrorCode::WriterLocked, 3),
        (ErrorCode::ShmVersionMismatch, 4),
        (ErrorCode::ShmUnavailable, 5),
        (ErrorCode::UnsupportedFormat, 6),
        (ErrorCode::NetworkFilesystem, 7),
        (ErrorCode::TableNotFound, 8),
        (ErrorCode::TableExists, 9),
        (ErrorCode::FamilyNotFound, 10),
        (ErrorCode::FamilyExists, 11),
        (ErrorCode::UnknownMergeOperator, 12),
        (ErrorCode::MergeFailed, 13),
        (ErrorCode::Conflict, 14),
        (ErrorCode::ReadOnly, 15),
        (ErrorCode::KeyTooLarge, 16),
        (ErrorCode::ValueTooLarge, 17),
        (ErrorCode::NoSpace, 18),
        (ErrorCode::InvalidArgument, 19),
        (ErrorCode::Unsupported, 20),
        (ErrorCode::Closed, 21),
        (ErrorCode::NoReaderSlot, 22),
        (ErrorCode::RecordTooLarge, 23),
        (ErrorCode::Busy, 24),
    ];
    for (code, n) in codes {
        assert_eq!(code as u32, n, "{code:?}");
    }
}

#[test]
fn every_engine_error_maps_to_its_code() {
    use pigeonhole_engine::Error as E;
    let io = || pigeonhole_io::Error::new(pigeonhole_io::ErrorKind::Other, "x");
    let merge = pigeonhole_engine::MergeError {
        operator: "op".into(),
        message: "bad".into(),
    };
    let cases: Vec<(E, ErrorCode)> = vec![
        (E::Io(io()), ErrorCode::Io),
        (E::Corruption("x".into()), ErrorCode::Corruption),
        (E::WriterLocked, ErrorCode::WriterLocked),
        (
            E::ShmVersionMismatch {
                found: 1,
                expected: 2,
            },
            ErrorCode::ShmVersionMismatch,
        ),
        (E::ShmUnavailable, ErrorCode::ShmUnavailable),
        (E::UnsupportedFormat(9), ErrorCode::UnsupportedFormat),
        (E::NetworkFilesystem, ErrorCode::NetworkFilesystem),
        (E::TableNotFound("t".into()), ErrorCode::TableNotFound),
        (E::TableExists("t".into()), ErrorCode::TableExists),
        (E::FamilyNotFound("f".into()), ErrorCode::FamilyNotFound),
        (E::FamilyExists("f".into()), ErrorCode::FamilyExists),
        (
            E::UnknownMergeOperator("op".into()),
            ErrorCode::UnknownMergeOperator,
        ),
        (E::Merge(merge), ErrorCode::MergeFailed),
        (E::Conflict, ErrorCode::Conflict),
        (E::ReadOnly, ErrorCode::ReadOnly),
        (E::KeyTooLarge, ErrorCode::KeyTooLarge),
        (E::ValueTooLarge, ErrorCode::ValueTooLarge),
        (E::NoSpace, ErrorCode::NoSpace),
        (E::InvalidArgument("x".into()), ErrorCode::InvalidArgument),
        (E::Unsupported("x"), ErrorCode::Unsupported),
        (E::Closed, ErrorCode::Closed),
        (E::NoReaderSlot, ErrorCode::NoReaderSlot),
        (E::RecordTooLarge, ErrorCode::RecordTooLarge),
        (E::Busy, ErrorCode::Busy),
    ];
    for (e, code) in cases {
        let what = format!("{e:?}");
        let message = match &e {
            E::Merge(_) => "merge operator \"op\" failed: bad".to_owned(),
            E::Busy => "memtable arena full: a flush did not free room within the write-stall \
                        timeout, or one batch is larger than the arena; retry, or raise \
                        Options::memtable_budget"
                .to_owned(),
            _ => e.to_string(),
        };
        let public: pigeonhole::Error = e.into();
        assert_eq!(public.code(), code, "{what}");
        assert_eq!(public.message(), message);
    }
}

// ---- reads ----

#[test]
fn row_reads_order_families_by_creation_or_request() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r")
        .put("b", b"y", b"1")
        .put("a", b"z", b"2")
        .put("a", b"x", b"3")
        .commit()
        .unwrap();
    let order = |r: Option<pigeonhole::RowRef<'_>>| -> Vec<(String, Vec<u8>)> {
        r.unwrap()
            .iter()
            .map(|e| (e.family.to_owned(), e.qualifier.to_vec()))
            .collect()
    };
    let s = |f: &str, q: &[u8]| (f.to_owned(), q.to_vec());
    // D39: creation order, or the listed order (a family listed twice appears once).
    assert_eq!(
        order(t.row(b"r").read().unwrap()),
        [s("a", b"x"), s("a", b"z"), s("b", b"y")]
    );
    assert_eq!(
        order(t.row(b"r").families(["b", "a", "b"]).read().unwrap()),
        [s("b", b"y"), s("a", b"x"), s("a", b"z")]
    );
    assert_eq!(
        order(t.row(b"r").family("b").read().unwrap()),
        [s("b", b"y")]
    );
    assert!(t.row(b"missing").read().unwrap().is_none());
    // A projection or filter that matches nothing is `None`, not an empty row.
    assert!(t.row(b"r").qualifier_prefix(b"q").read().unwrap().is_none());
}

#[test]
fn qualifier_version_time_and_column_filters() {
    let db = db();
    let t = table(&db);
    for (q, ts) in [
        (&b"q1"[..], 10u64),
        (b"q1", 20),
        (b"q1", 30),
        (b"q2", 40),
        (b"q3", 50),
    ] {
        t.mutate(b"r")
            .put_at("a", q, ts, &ts.to_be_bytes())
            .commit()
            .unwrap();
    }
    let read = |r: pigeonhole::RowRead<'_>| -> Vec<(Vec<u8>, u64)> {
        r.read()
            .unwrap()
            .map(|r| {
                r.iter()
                    .map(|e| (e.qualifier.to_vec(), e.cell.timestamp()))
                    .collect()
            })
            .unwrap_or_default()
    };
    let v = |q: &[u8], ts: u64| (q.to_vec(), ts);
    assert_eq!(
        read(t.row(b"r")),
        [v(b"q1", 30), v(b"q2", 40), v(b"q3", 50)]
    );
    assert_eq!(
        read(t.row(b"r").versions(2)),
        [v(b"q1", 30), v(b"q1", 20), v(b"q2", 40), v(b"q3", 50)]
    );
    assert_eq!(
        read(t.row(b"r").versions(0).time_range(15..45)),
        [v(b"q1", 30), v(b"q1", 20), v(b"q2", 40)]
    );
    assert_eq!(
        read(t.row(b"r").versions(0).latest()),
        [v(b"q1", 30), v(b"q2", 40), v(b"q3", 50)]
    );
    assert_eq!(read(t.row(b"r").qualifier_prefix(b"q2")), [v(b"q2", 40)]);
    assert_eq!(
        read(t.row(b"r").qualifier_range(&b"q2"[..]..)),
        [v(b"q2", 40), v(b"q3", 50)]
    );
    assert_eq!(
        read(
            t.row(b"r")
                .qualifier_bounds(Bound::Excluded(b"q1"), Bound::Included(b"q2"))
        ),
        [v(b"q2", 40)]
    );
    assert_eq!(
        read(t.row(b"r").column_limit(2)),
        [v(b"q1", 30), v(b"q2", 40)]
    );
    // Value filters test the newest visible value of a column.
    assert_eq!(
        read(
            t.row(b"r")
                .value_filter(ValueFilter::Equals(40u64.to_be_bytes().to_vec()))
        ),
        [v(b"q2", 40)]
    );
    assert_eq!(
        read(
            t.row(b"r")
                .value_filter(ValueFilter::Equals(20u64.to_be_bytes().to_vec()))
        ),
        []
    );
    assert_eq!(
        read(t.row(b"r").value_filter(ValueFilter::Prefix(vec![0; 7]))),
        [v(b"q1", 30), v(b"q2", 40), v(b"q3", 50)]
    );
}

#[test]
fn scans_bounds_prefixes_limits_and_both_iterator_forms() {
    let db = db();
    let t = table(&db);
    let keys: [&[u8]; 6] = [b"a", b"user:1", b"user:2", b"user:3", b"user;", b"\xff\xff"];
    for k in keys {
        t.mutate(k)
            .put("a", b"q", k)
            .put("b", b"n", b"x")
            .commit()
            .unwrap();
    }
    let owned = |s: pigeonhole::Scan<'_>| -> Vec<Vec<u8>> {
        s.iter()
            .unwrap()
            .map(|r| r.unwrap().key().to_vec())
            .collect()
    };
    let lent = |s: pigeonhole::Scan<'_>| -> Vec<Vec<u8>> {
        let mut it = s.iter().unwrap();
        let mut out = Vec::new();
        while let Some(r) = it.next_ref().unwrap() {
            out.push(r.key().to_vec());
        }
        // A finished iterator stays finished.
        assert!(it.next_ref().unwrap().is_none());
        assert!(it.next().is_none());
        out
    };
    let k = |ks: &[&[u8]]| ks.iter().map(|k| k.to_vec()).collect::<Vec<_>>();
    assert_eq!(owned(t.scan_prefix(b"user:")), k(&keys[1..4]));
    assert_eq!(lent(t.scan_prefix(b"user:")), k(&keys[1..4]));
    assert_eq!(owned(t.scan_prefix(b"")), k(&keys));
    assert_eq!(owned(t.scan_prefix(b"\xff")), k(&keys[5..]));
    assert_eq!(owned(t.scan(&b"user:2"[..]..&b"user;"[..])), k(&keys[2..4]));
    assert_eq!(
        owned(t.scan(&b"user:2"[..]..=&b"user;"[..])),
        k(&keys[2..5])
    );
    assert_eq!(owned(t.scan(b"user:2"..)), k(&keys[2..]));
    assert_eq!(owned(t.scan::<[u8]>(..)), k(&keys));
    assert_eq!(
        lent(
            t.scan_bounds(Bound::Excluded(b"user:1"), Bound::Unbounded)
                .limit(2)
        ),
        k(&keys[2..4])
    );
    assert_eq!(owned(t.scan_prefix(b"").limit(0)), k(&[]));
    // An inverted range is empty.
    assert_eq!(owned(t.scan(&b"z"[..]..&b"a"[..])), k(&[]));
    // Projection and per-row column limit.
    let row = t
        .scan_prefix(b"a")
        .family("b")
        .iter()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.entry(0).unwrap().0, "b");
    let row = t
        .scan_prefix(b"a")
        .columns_per_row(1)
        .iter()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(row.len(), 2, "one column per family");
    // Value filters in scans.
    assert_eq!(
        owned(
            t.scan_prefix(b"")
                .family("a")
                .value_filter(ValueFilter::Prefix(b"user:".to_vec()))
        ),
        k(&keys[1..4])
    );
}

#[test]
fn row_and_rowref_agree_and_cells_outlive_their_handles() {
    let db = db();
    let t = table(&db);
    let big = vec![9u8; 4000];
    t.mutate(b"r")
        .put("a", b"big", &big)
        .put_i64("a", b"n", -5)
        .put_f64("b", b"f", 2.5)
        .commit()
        .unwrap();
    let r = t.row(b"r").read().unwrap().unwrap();
    let row: Row = r.to_owned();
    let view = row.view();
    assert_eq!(r.len(), 3);
    assert_eq!(row.len(), 3);
    assert!(!row.is_empty() && !view.is_empty());
    for i in 0..3 {
        let (f, q, c) = row.entry(i).unwrap();
        let e = view.entry(i).unwrap();
        let e2 = r.entry(i).unwrap();
        assert_eq!((f, q, c.value()), (e.family, e.qualifier, e.cell.value()));
        assert_eq!((e.family, e.qualifier), (e2.family, e2.qualifier));
        assert_eq!(c.timestamp(), e2.cell.timestamp());
    }
    assert!(row.entry(3).is_none() && r.entry(3).is_none());
    assert_eq!(row.get("a", b"n").unwrap().as_i64(), Some(-5));
    assert_eq!(view.get("b", b"f").unwrap().typed(), Value::F64(2.5));
    assert_eq!(r.get("a", b"big").unwrap().value(), &big[..]);
    assert!(row.get("a", b"none").is_none() && row.get("zz", b"n").is_none());
    assert_eq!(r.get("b", b"f").unwrap().as_i64(), None);
    let cell: Cell = t.get(b"r", "a", b"big").unwrap().unwrap().to_owned();
    // Owned results survive the table handle, the database handle and later writes.
    drop(r);
    drop(t);
    let t = table(&db);
    t.mutate(b"r").delete_row().commit().unwrap();
    drop(db);
    assert_eq!(cell.value(), &big[..]);
    assert_eq!(cell.typed(), Value::Bytes(&big));
    assert_eq!(row.get("a", b"big").unwrap().value(), &big[..]);
    let moved = std::thread::spawn(move || cell.value().len())
        .join()
        .unwrap();
    assert_eq!(moved, 4000);
}

#[test]
fn snapshots_isolate_reads() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r").put("a", b"q", b"1").commit().unwrap();
    let snap = db.snapshot().unwrap();
    t.mutate(b"r").put("a", b"q", b"2").commit().unwrap();
    t.mutate(b"s").put("a", b"q", b"3").commit().unwrap();
    assert!(db.snapshot().unwrap().seqno() > snap.seqno());
    assert_eq!(
        t.get_at(&snap, b"r", "a", b"q").unwrap().unwrap().value(),
        b"1"
    );
    let row = t.row(b"r").snapshot(&snap).read().unwrap().unwrap();
    assert_eq!(row.get("a", b"q").unwrap().value(), b"1");
    assert_eq!(
        t.scan_prefix(b"").snapshot(&snap).iter().unwrap().count(),
        1
    );
    assert_eq!(t.scan_prefix(b"").iter().unwrap().count(), 2);
    let clone = snap.clone();
    drop(snap);
    assert!(t.get_at(&clone, b"s", "a", b"q").unwrap().is_none());
}

// ---- writes ----

#[test]
fn deletes_follow_the_timestamp_rules() {
    let db = db();
    let t = table(&db);
    for ts in [10u64, 20, 30] {
        t.mutate(b"r")
            .put_at("a", b"q", ts, &[ts as u8])
            .commit()
            .unwrap();
    }
    // D38: a cell delete hides exactly its timestamp, even from a later put.
    t.mutate(b"r").delete_cell("a", b"q", 30).commit().unwrap();
    assert_eq!(value(&t, b"r", "a", b"q"), Some(vec![20]));
    t.mutate(b"r")
        .put_at("a", b"q", 30, b"again")
        .commit()
        .unwrap();
    assert_eq!(value(&t, b"r", "a", b"q"), Some(vec![20]));
    // D9: a column delete at the commit time hides every older version; a newer put shows.
    t.mutate(b"r").delete_column("a", b"q").commit().unwrap();
    assert_eq!(value(&t, b"r", "a", b"q"), None);
    t.mutate(b"r")
        .put_at("a", b"q", 40, b"old")
        .commit()
        .unwrap();
    assert_eq!(value(&t, b"r", "a", b"q"), None);
    t.mutate(b"r").put("a", b"q", b"new").commit().unwrap();
    assert_eq!(value(&t, b"r", "a", b"q").as_deref(), Some(&b"new"[..]));
    // Family and row deletes.
    t.mutate(b"r")
        .put("b", b"x", b"1")
        .put("a", b"y", b"2")
        .commit()
        .unwrap();
    t.mutate(b"r").delete_family("a").commit().unwrap();
    assert_eq!(
        cells(&t, b"r"),
        [("b".into(), b"x".to_vec(), b"1".to_vec())]
    );
    t.mutate(b"r").delete_row().commit().unwrap();
    assert_eq!(cells(&t, b"r"), []);
    // D34: the same column twice in one commit keeps the last write.
    t.mutate(b"s")
        .put("a", b"q", b"1")
        .put("a", b"q", b"2")
        .commit()
        .unwrap();
    assert_eq!(value(&t, b"s", "a", b"q").as_deref(), Some(&b"2"[..]));
}

/// D74 (HBase semantics), the behavior behind issues #67 and #68: a put with an older explicit
/// timestamp stays hidden by a column or family delete until a bottommost compaction purges
/// the delete; after that it shows. The engine also compacts in the background (after a
/// reopen, say), so the model suite leaves such writes out.
#[test]
fn a_compaction_purge_uncovers_later_puts_at_older_timestamps() {
    let db = db();
    let t = table(&db);
    t.mutate(b"c").put("a", b"q", b"v").commit().unwrap();
    t.mutate(b"c").delete_column("a", b"q").commit().unwrap();
    t.mutate(b"f").put("a", b"q", b"v").commit().unwrap();
    t.mutate(b"f").delete_family("a").commit().unwrap();
    // Two SSTs, so the compaction below rewrites them (one would only move down a level).
    db.flush().unwrap();
    for row in [&b"c"[..], b"f"] {
        t.mutate(row)
            .put_at("a", b"q", 1, b"before")
            .commit()
            .unwrap();
        assert_eq!(value(&t, row, "a", b"q"), None, "hidden by the delete");
    }
    db.compact().unwrap();
    for row in [&b"c"[..], b"f"] {
        // The put hidden before the purge was dropped with the delete.
        assert_eq!(value(&t, row, "a", b"q"), None);
        t.mutate(row)
            .put_at("a", b"q", 2, b"after")
            .commit()
            .unwrap();
        assert_eq!(value(&t, row, "a", b"q").as_deref(), Some(&b"after"[..]));
    }
}

#[test]
fn counters_and_typed_values() {
    let db = db();
    let t = table(&db);
    for d in [5, -2, 10] {
        t.mutate(b"r").incr("a", b"hits", d).commit().unwrap();
    }
    assert_eq!(
        t.get(b"r", "a", b"hits").unwrap().unwrap().as_i64(),
        Some(13)
    );
    // A put and an operand of the same column in one commit share its timestamp, so the
    // last one written wins (D34): reset the base and add in separate commits.
    t.mutate(b"r")
        .put_i64("a", b"hits", 100)
        .incr("a", b"hits", 7)
        .commit()
        .unwrap();
    assert_eq!(
        t.get(b"r", "a", b"hits").unwrap().unwrap().as_i64(),
        Some(20)
    );
    t.mutate(b"r").put_i64("a", b"hits", 100).commit().unwrap();
    t.mutate(b"r").incr("a", b"hits", 1).commit().unwrap();
    assert_eq!(
        t.get(b"r", "a", b"hits").unwrap().unwrap().as_i64(),
        Some(101)
    );
    let mut wb = db.write_batch();
    wb.incr(&t, b"r", "a", b"hits", 1)
        .incr(&t, b"s", "a", b"hits", 1);
    wb.commit().unwrap();
    assert_eq!(
        t.get(b"r", "a", b"hits").unwrap().unwrap().as_i64(),
        Some(102)
    );
    // `merge` writes an operand for the family's operator (here the default i64 add).
    t.mutate(b"r")
        .merge("a", b"hits", &3i64.to_le_bytes())
        .commit()
        .unwrap();
    // Bytes on top of a counter, or an operand onto bytes, fail at read with MergeFailed.
    t.mutate(b"m").put("a", b"c", b"text").commit().unwrap();
    t.mutate(b"m").incr("a", b"c", 1).commit().unwrap();
    assert_eq!(
        t.get(b"m", "a", b"c").map(|_| ()).unwrap_err().code(),
        ErrorCode::MergeFailed
    );
    // A family without an operator refuses operands at commit.
    let plain = db
        .table("plain")
        .unwrap()
        .family("p", Family::default().merge_operator(""))
        .create()
        .unwrap();
    assert_eq!(
        plain
            .mutate(b"r")
            .incr("p", b"c", 1)
            .commit()
            .unwrap_err()
            .code(),
        ErrorCode::InvalidArgument
    );
}

#[test]
fn write_batches_span_tables_and_refuse_foreign_tables() {
    let db = db();
    let t = table(&db);
    let u = db
        .table("u")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let mut wb = db.write_batch();
    assert!(wb.is_empty());
    wb.put(&t, b"r1", "a", b"q", b"1")
        .put_at(&t, b"r2", "a", b"q", 77, b"2")
        .put(&u, b"x", "f", b"q", b"3")
        .delete_column(&t, b"r3", "a", b"q")
        .delete_row(&u, b"gone");
    assert_eq!(wb.len(), 5);
    let info = wb.commit_with(Durability::Buffered).unwrap();
    assert_eq!(info.durability, Durability::Buffered);
    assert_eq!(value(&t, b"r1", "a", b"q").as_deref(), Some(&b"1"[..]));
    assert_eq!(t.get(b"r2", "a", b"q").unwrap().unwrap().timestamp(), 77);
    assert_eq!(u.get(b"x", "f", b"q").unwrap().unwrap().value(), b"3");

    let other = self::db();
    let foreign = table(&other);
    let mut wb = db.write_batch();
    wb.put(&foreign, b"r", "a", b"q", b"v");
    assert_eq!(wb.commit().unwrap_err().code(), ErrorCode::InvalidArgument);
}

#[test]
fn durability_defaults_and_overrides() {
    let vfs = SimVfs::new(3);
    let db = Pigeonhole::open(
        "/db/d.phdb",
        sim_options(&vfs).durability(Durability::Buffered),
    )
    .unwrap();
    let t = table(&db);
    assert_eq!(db.default_durability(), Durability::Buffered);
    let info = t.mutate(b"r").put("a", b"q", b"v").commit().unwrap();
    assert_eq!(info.durability, Durability::Buffered);
    db.set_default_durability(Durability::None);
    assert_eq!(db.default_durability(), Durability::None);
    let mut wb = db.write_batch();
    wb.put(&t, b"s", "a", b"q", b"v");
    assert_eq!(wb.commit().unwrap().durability, Durability::None);
    for d in [
        Durability::None,
        Durability::Buffered,
        Durability::GroupSync,
        Durability::Sync,
    ] {
        let info = t
            .mutate(b"r")
            .put("a", b"q", b"v")
            .durability(d)
            .commit()
            .unwrap();
        assert_eq!(info.durability, d);
        let mut wb = db.write_batch();
        wb.put(&t, b"s", "a", b"q", b"v");
        assert_eq!(wb.commit_with(d).unwrap().durability, d);
    }
}

#[test]
fn conditional_commits() {
    let db = db();
    let t = table(&db);
    let absent = Condition::Absent {
        family: "a".into(),
        qualifier: b"lock".to_vec(),
    };
    let exists = Condition::Exists {
        family: "a".into(),
        qualifier: b"lock".to_vec(),
    };
    assert!(
        t.mutate(b"r")
            .put("a", b"x", b"1")
            .commit_if(&exists)
            .unwrap()
            .is_none()
    );
    assert!(
        t.mutate(b"r")
            .put("a", b"lock", b"w1")
            .commit_if(&absent)
            .unwrap()
            .is_some()
    );
    assert!(
        t.mutate(b"r")
            .put("a", b"lock", b"w2")
            .commit_if(&absent)
            .unwrap()
            .is_none()
    );
    let owner_is = |who: &[u8]| Condition::Value {
        family: "a".into(),
        qualifier: b"lock".to_vec(),
        filter: ValueFilter::Equals(who.to_vec()),
    };
    assert!(
        t.mutate(b"r")
            .delete_column("a", b"lock")
            .commit_if(&owner_is(b"w2"))
            .unwrap()
            .is_none()
    );
    let info = t
        .mutate(b"r")
        .delete_column("a", b"lock")
        .durability(Durability::Sync)
        .commit_if(&owner_is(b"w1"))
        .unwrap()
        .unwrap();
    assert_eq!(info.durability, Durability::Sync);
    assert_eq!(value(&t, b"r", "a", b"lock"), None);
    assert_eq!(value(&t, b"r", "a", b"x"), None);
    t.mutate(b"n").put_i64("a", b"v", 7).commit().unwrap();
    let gt = |n| Condition::Value {
        family: "a".into(),
        qualifier: b"v".to_vec(),
        filter: ValueFilter::I64(Ordering::Greater, n),
    };
    assert!(
        t.mutate(b"n")
            .put_i64("a", b"v", 8)
            .commit_if(&gt(7))
            .unwrap()
            .is_none()
    );
    assert!(
        t.mutate(b"n")
            .put_i64("a", b"v", 8)
            .commit_if(&gt(6))
            .unwrap()
            .is_some()
    );
    let bad = Condition::Exists {
        family: "nope".into(),
        qualifier: vec![],
    };
    assert_eq!(
        t.mutate(b"n")
            .put("a", b"v", b"x")
            .commit_if(&bad)
            .unwrap_err()
            .code(),
        ErrorCode::FamilyNotFound
    );
}

#[test]
fn transactions_commit_or_conflict() {
    let db = db();
    let t = table(&db);
    t.mutate(b"a").put("a", b"bal", b"10").commit().unwrap();
    let mut txn = db.transaction().unwrap();
    assert_eq!(
        txn.get(&t, b"a", "a", b"bal").unwrap().unwrap().value(),
        b"10"
    );
    txn.put(&t, b"a", "a", b"bal", b"5")
        .put(&t, b"b", "a", b"bal", b"5")
        .delete_column(&t, b"c", "a", b"bal");
    let info = txn.commit_with(Durability::GroupSync).unwrap();
    assert_eq!(info.durability, Durability::GroupSync);
    assert_eq!(value(&t, b"b", "a", b"bal").as_deref(), Some(&b"5"[..]));

    let mut txn = db.transaction().unwrap();
    let _ = txn.get(&t, b"a", "a", b"bal").unwrap();
    txn.put(&t, b"a", "a", b"bal", b"0");
    t.mutate(b"a").put("a", b"bal", b"99").commit().unwrap();
    assert_eq!(txn.commit().unwrap_err().code(), ErrorCode::Conflict);
    assert_eq!(value(&t, b"a", "a", b"bal").as_deref(), Some(&b"99"[..]));

    // Errors while buffering surface at commit.
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"a", "nope", b"q", b"v");
    assert_eq!(txn.commit().unwrap_err().code(), ErrorCode::FamilyNotFound);
    let mut txn = db.transaction().unwrap();
    assert_eq!(
        txn.get(&t, b"a", "nope", b"q")
            .map(|_| ())
            .unwrap_err()
            .code(),
        ErrorCode::FamilyNotFound
    );
}

// ---- lifecycle ----

#[test]
fn closed_handles_fail_with_closed() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r").put("a", b"q", b"v").commit().unwrap();
    db.clone().close().unwrap();
    assert_eq!(
        t.mutate(b"r")
            .put("a", b"q", b"v")
            .commit()
            .unwrap_err()
            .code(),
        ErrorCode::Closed
    );
}

#[test]
fn application_owned_mode() {
    let vfs = SimVfs::new(5);
    let err =
        Pigeonhole::open_application_owned("/db/app.phdb", sim_options(&vfs).compaction_cores(1))
            .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument);

    let (db, mut shards) =
        Pigeonhole::open_application_owned("/db/app.phdb", sim_options(&vfs)).unwrap();
    assert_eq!(shards.len(), 2);
    assert_eq!(shards.iter().map(|s| s.index()).collect::<Vec<_>>(), [0, 1]);
    let woken = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for s in &mut shards {
        let w = Arc::clone(&woken);
        s.set_wakeup(Box::new(move || {
            w.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let threads: Vec<_> = shards
        .into_iter()
        .map(|mut s| {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while s.run_once(Duration::from_millis(1))
                    || !stop.load(std::sync::atomic::Ordering::Acquire)
                {
                    std::thread::yield_now();
                }
            })
        })
        .collect();
    let t = table(&db);
    t.mutate(b"r").put("a", b"q", b"v").commit().unwrap();
    assert_eq!(value(&t, b"r", "a", b"q").as_deref(), Some(&b"v"[..]));
    assert!(woken.load(std::sync::atomic::Ordering::Relaxed) > 0);
    drop(t);
    db.close().unwrap();
    stop.store(true, std::sync::atomic::Ordering::Release);
    for th in threads {
        th.join().unwrap();
    }
}

#[test]
fn real_files_reopen_and_lock() {
    let dir = TempDir::new("reopen");
    let path = dir.0.join("db.phdb");
    let opts = || Options::default().shards(2).memtable_budget(4 << 20);
    {
        let db = Pigeonhole::open(&path, opts()).unwrap();
        let t = table(&db);
        t.mutate(b"r").put("a", b"q", b"durable").commit().unwrap();
        let second = Pigeonhole::open(&path, opts()).unwrap_err();
        assert_eq!(second.code(), ErrorCode::WriterLocked);
        db.flush().unwrap();
        db.close().unwrap();
    }
    let missing = Pigeonhole::open(dir.0.join("none.phdb"), opts().create_if_missing(false));
    assert!(missing.is_err());
    let db = Pigeonhole::open(&path, opts()).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    assert_eq!(value(&t, b"r", "a", b"q").as_deref(), Some(&b"durable"[..]));
    // Engine Milestone B: compaction and backup work; the backup is one file that opens on
    // its own and holds the data.
    db.compact().unwrap();
    db.backup(dir.0.join("copy.phdb")).unwrap();
    db.close().unwrap();
    let copy = Pigeonhole::open(dir.0.join("copy.phdb"), opts().create_if_missing(false)).unwrap();
    let t = copy.table("t").unwrap().open().unwrap();
    assert_eq!(value(&t, b"r", "a", b"q").as_deref(), Some(&b"durable"[..]));
    drop(t);
    copy.close().unwrap();
}

#[test]
fn reader_handles_see_the_writers_commits() {
    let vfs = SimVfs::new(9);
    let db = Pigeonhole::open("/db/r.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r").put("a", b"q", b"1").commit().unwrap();
    let reader = Pigeonhole::open_reader(
        "/db/r.phdb",
        pigeonhole::ReaderOptions::default()
            .vfs(Arc::clone(&vfs) as _)
            .block_cache(1 << 20),
    )
    .unwrap();
    assert_eq!(reader.tables(), ["t"]);
    let rt = reader.table("t").unwrap();
    assert_eq!(rt.name(), "t");
    assert_eq!(rt.get(b"r", "a", b"q").unwrap().unwrap().value(), b"1");
    t.mutate(b"s").put("a", b"q", b"2").commit().unwrap();
    let snap = reader.snapshot().unwrap();
    assert_eq!(
        rt.get_at(&snap, b"s", "a", b"q").unwrap().unwrap().value(),
        b"2"
    );
    assert_eq!(rt.scan_prefix(b"").iter().unwrap().count(), 2);
    assert_eq!(rt.scan(&b"s"[..]..).iter().unwrap().count(), 1);
    assert_eq!(
        rt.scan_bounds(Bound::Unbounded, Bound::Excluded(b"s"))
            .iter()
            .unwrap()
            .count(),
        1
    );
    assert!(rt.row(b"r").read().unwrap().is_some());
    assert_eq!(
        reader.table("nope").unwrap_err().code(),
        ErrorCode::TableNotFound
    );
    drop(rt);
    drop(reader);
    drop(t);
    db.close().unwrap();
}

// ---- review fixes ----

#[test]
fn snapshots_from_another_database_are_refused() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r").put("a", b"q", b"v").commit().unwrap();
    let other = self::db();
    let foreign = other.snapshot().unwrap();
    let code = |r: pigeonhole::Result<()>| r.unwrap_err().code();
    assert_eq!(
        code(t.get_at(&foreign, b"r", "a", b"q").map(|_| ())),
        ErrorCode::InvalidArgument
    );
    assert_eq!(
        code(t.row(b"r").snapshot(&foreign).read().map(|_| ())),
        ErrorCode::InvalidArgument
    );
    assert_eq!(
        code(t.scan_prefix(b"").snapshot(&foreign).iter().map(|_| ())),
        ErrorCode::InvalidArgument
    );
    // Its own snapshots still work, including from a clone of the handle.
    let own = db.clone().snapshot().unwrap();
    assert!(t.get_at(&own, b"r", "a", b"q").unwrap().is_some());
}

#[test]
fn reads_after_close_fail_with_closed() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r").put("a", b"q", b"v").commit().unwrap();
    let snap = db.snapshot().unwrap();
    let handle = db.clone();
    db.close().unwrap();
    let code = |r: pigeonhole::Result<()>| r.unwrap_err().code();
    assert_eq!(code(t.get(b"r", "a", b"q").map(|_| ())), ErrorCode::Closed);
    assert_eq!(
        code(t.get_at(&snap, b"r", "a", b"q").map(|_| ())),
        ErrorCode::Closed
    );
    assert_eq!(code(t.row(b"r").read().map(|_| ())), ErrorCode::Closed);
    assert_eq!(
        code(t.scan_prefix(b"").iter().map(|_| ())),
        ErrorCode::Closed
    );
    assert_eq!(code(handle.snapshot().map(|_| ())), ErrorCode::Closed);
    assert_eq!(code(handle.transaction().map(|_| ())), ErrorCode::Closed);
    assert_eq!(
        code(handle.table("t").unwrap().open().map(|_| ())),
        ErrorCode::Closed
    );
    let mut wb = handle.write_batch();
    wb.put(&t, b"r", "a", b"q", b"v");
    assert_eq!(code(wb.commit().map(|_| ())), ErrorCode::Closed);
}

#[test]
fn a_full_memtable_arena_is_busy_with_a_precise_message() {
    let vfs = SimVfs::new(11);
    let opts = |budget| {
        Options::default()
            .vfs(Arc::clone(&vfs) as _)
            .shards(1)
            .memtable_budget(budget)
            .wal_segment_size(256 << 10)
    };
    let db = Pigeonhole::open("/db/busy.phdb", opts(1 << 20)).unwrap();
    let t = table(&db);
    // Engine Milestone B: a full arena waits for a flush, so steady writes never see
    // `Busy`; a batch that can never fit the arena is refused at once.
    let cell = vec![1u8; 1000];
    for i in 0..3_000u32 {
        t.mutate(&i.to_be_bytes())
            .put("a", b"q", &cell)
            .commit()
            .unwrap();
    }
    let mut wb = db.write_batch();
    let big = vec![2u8; 4096];
    for i in 0..400u32 {
        wb.put(&t, &i.to_be_bytes(), "a", b"big", &big);
    }
    let err = wb
        .commit()
        .expect_err("a batch larger than the arena is refused");
    assert_eq!(err.code(), ErrorCode::Busy);
    assert!(err.message().contains("memtable_budget"), "{err}");
    drop(t);
    db.close().unwrap();
    // A clean close leaves nothing to replay: a smaller budget reopens fine.
    let db = Pigeonhole::open("/db/busy.phdb", opts(256 << 10)).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    assert_eq!(
        value(&t, &0u32.to_be_bytes(), "a", b"q").as_deref(),
        Some(&cell[..])
    );
    drop(t);
    db.close().unwrap();
}

#[test]
fn size_errors_name_the_size_and_the_limit() {
    let db = db();
    let t = table(&db);
    let err = t
        .mutate(&[0u8; 70_000])
        .put("a", b"q", b"v")
        .commit()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::KeyTooLarge);
    assert!(err.message().contains("row key of 70000 bytes"), "{err}");
    assert!(err.message().contains("65536"), "{err}");
    let err = t
        .mutate(b"r")
        .put("a", &[0u8; 66_000], b"v")
        .commit()
        .unwrap_err();
    assert!(err.message().contains("qualifier of 66000 bytes"), "{err}");
    let mut wb = db.write_batch();
    wb.delete_row(&t, &[0u8; 70_000]);
    assert!(
        wb.commit()
            .unwrap_err()
            .message()
            .contains("row key of 70000 bytes")
    );
    // 256 KiB segments: the limit is the segment payload, 256 KiB - 64 KiB.
    let big = vec![0u8; 300_000];
    let err = t.mutate(b"r").put("a", b"q", &big).commit().unwrap_err();
    assert_eq!(err.code(), ErrorCode::ValueTooLarge);
    assert!(err.message().contains("value of 300000 bytes"), "{err}");
    assert!(err.message().contains(&(192 * 1024).to_string()), "{err}");
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"r", "a", b"q", &big);
    let err = txn.commit().unwrap_err();
    assert_eq!(err.code(), ErrorCode::ValueTooLarge);
    assert!(err.message().contains("value of 300000 bytes"), "{err}");
}

#[test]
fn write_batches_have_every_row_mutation() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r")
        .put_at("a", b"x", 10, b"old")
        .put("b", b"y", b"1")
        .commit()
        .unwrap();
    let mut wb = db.write_batch();
    wb.put_i64(&t, b"r", "a", b"n", 41)
        .put_f64(&t, b"s", "a", b"f", 1.5)
        .delete_cell(&t, b"r", "a", b"x", 10)
        .delete_family(&t, b"r", "b");
    assert_eq!(wb.len(), 4);
    wb.commit().unwrap();
    let mut wb = db.write_batch();
    wb.incr(&t, b"r", "a", b"n", 1);
    wb.commit().unwrap();
    assert_eq!(t.get(b"r", "a", b"n").unwrap().unwrap().as_i64(), Some(42));
    // `merge` writes an untyped operand (for custom operators); the built-in i64 add
    // refuses it at read time, so counters use `incr`.
    let mut wb = db.write_batch();
    wb.merge(&t, b"m", "a", b"n", &1i64.to_le_bytes());
    wb.commit().unwrap();
    assert_eq!(
        t.get(b"m", "a", b"n").map(|_| ()).unwrap_err().code(),
        ErrorCode::MergeFailed
    );
    assert_eq!(
        t.get(b"s", "a", b"f").unwrap().unwrap().typed(),
        Value::F64(1.5)
    );
    assert!(t.get(b"r", "a", b"x").unwrap().is_none());
    assert!(t.get(b"r", "b", b"y").unwrap().is_none());
}

#[test]
fn durability_defaults_and_overrides_on_every_commit_path() {
    let vfs = SimVfs::new(13);
    let db = Pigeonhole::open("/db/dd.phdb", sim_options(&vfs)).unwrap();
    assert_eq!(db.default_durability(), Durability::GroupSync);
    let t = table(&db);
    assert_eq!(
        t.mutate(b"r")
            .put("a", b"q", b"v")
            .commit()
            .unwrap()
            .durability,
        Durability::GroupSync
    );
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"r", "a", b"q", b"t");
    assert_eq!(
        txn.commit_with(Durability::Buffered).unwrap().durability,
        Durability::Buffered
    );
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"r", "a", b"q", b"u");
    assert_eq!(txn.commit().unwrap().durability, Durability::GroupSync);
    let always = Condition::Exists {
        family: "a".into(),
        qualifier: b"q".to_vec(),
    };
    let info = t
        .mutate(b"r")
        .put("a", b"q", b"w")
        .durability(Durability::None)
        .commit_if(&always)
        .unwrap()
        .unwrap();
    assert_eq!(info.durability, Durability::None);
}

#[test]
fn empty_table_names_and_tables_without_families_are_refused() {
    let db = db();
    let err = db
        .table("")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument);
    for r in [
        db.table("t").unwrap().create(),
        db.table("t").unwrap().create_if_missing(),
    ] {
        assert_eq!(r.unwrap_err().code(), ErrorCode::InvalidArgument);
    }
    assert!(db.tables().is_empty());
    // Opening an existing table needs no declared family.
    table(&db);
    assert_eq!(
        db.table("t").unwrap().open().unwrap().families(),
        ["a", "b"]
    );
}

#[test]
fn days_saturates() {
    assert_eq!(days(2), Duration::from_secs(2 * 86_400));
    assert_eq!(days(u64::MAX), Duration::from_secs(u64::MAX));
}

#[test]
fn data_beyond_the_memtable_budget_lives_in_one_file() {
    let dir = TempDir::new("beyond-budget");
    let path = dir.0.join("big.phdb");
    let opts = || Options::default().shards(1).memtable_budget(2 << 20);
    let cell = vec![7u8; 1000];
    let db = Pigeonhole::open(&path, opts()).unwrap();
    let t = table(&db);
    // About 8 MiB of cells through a 2 MiB arena: every commit succeeds because flushes
    // free room as memtables fill.
    for i in 0..8_000u32 {
        t.mutate(&i.to_be_bytes())
            .put("a", b"q", &cell)
            .durability(Durability::Buffered)
            .commit()
            .unwrap();
    }
    db.compact().unwrap();
    drop(t);
    db.close().unwrap();
    let names: Vec<_> = std::fs::read_dir(&dir.0)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        names,
        [std::ffi::OsString::from("big.phdb")],
        "one file at rest"
    );
    assert!(std::fs::metadata(&path).unwrap().len() > 1 << 20);

    let db = Pigeonhole::open(&path, opts()).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    assert_eq!(t.scan_prefix(b"").iter().unwrap().count(), 8_000);
    assert_eq!(
        value(&t, &7_999u32.to_be_bytes(), "a", b"q").as_deref(),
        Some(&cell[..])
    );
    drop(t);
    db.close().unwrap();
}

#[test]
fn flush_compact_and_backup_through_the_public_api() {
    let dir = TempDir::new("maintenance");
    let path = dir.0.join("db.phdb");
    let opts = || Options::default().shards(2).memtable_budget(4 << 20);
    let db = Pigeonhole::open(&path, opts()).unwrap();
    let t = table(&db);
    t.mutate(b"none")
        .put("a", b"q", b"v1")
        .durability(Durability::None)
        .commit()
        .unwrap();
    // Versions beyond `max_versions(2)` on family `b` are compacted away.
    for v in 0..5u8 {
        t.mutate(b"ver").put("b", b"q", &[v]).commit().unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    assert_eq!(cells(&t, b"ver").len(), 2);

    let copy_path = dir.0.join("copy.phdb");
    db.backup(&copy_path).unwrap();
    // The copy is a point in time: later commits are not in it.
    t.mutate(b"none").put("a", b"q", b"v2").commit().unwrap();
    t.mutate(b"later").put("a", b"q", b"x").commit().unwrap();
    // A backup never overwrites.
    assert!(db.backup(&copy_path).is_err());

    let copy = Pigeonhole::open(&copy_path, opts().create_if_missing(false)).unwrap();
    let ct = copy.table("t").unwrap().open().unwrap();
    assert_eq!(value(&ct, b"none", "a", b"q").as_deref(), Some(&b"v1"[..]));
    assert_eq!(value(&ct, b"later", "a", b"q"), None);
    assert_eq!(cells(&ct, b"ver").len(), 2);
    drop(ct);
    copy.close().unwrap();

    let handle = db.clone();
    drop(t);
    db.close().unwrap();
    assert_eq!(handle.flush().unwrap_err().code(), ErrorCode::Closed);
    assert_eq!(handle.compact().unwrap_err().code(), ErrorCode::Closed);
    assert_eq!(
        handle.backup(dir.0.join("late.phdb")).unwrap_err().code(),
        ErrorCode::Closed
    );
}

#[test]
fn shrink_releases_file_space_after_deletes_and_keeps_data() {
    let dir = TempDir::new("shrink");
    let path = dir.0.join("db.phdb");
    let opts = || Options::default().shards(1).memtable_budget(4 << 20);
    let len = || std::fs::metadata(&path).unwrap().len();
    let db = Pigeonhole::open(&path, opts()).unwrap();
    let t = table(&db);
    t.mutate(b"keep").put("a", b"q", b"kept").commit().unwrap();
    let junk = db
        .table("junk")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let big = [9u8; 1024];
    for round in 0..4u32 {
        for i in 0..2_000u32 {
            let row = (round * 2_000 + i).to_be_bytes();
            junk.mutate(&row)
                .put("f", b"q", &big)
                .durability(Durability::None)
                .commit()
                .unwrap();
        }
        db.flush().unwrap();
    }
    let grown = len();
    for i in 0..8_000u32 {
        junk.mutate(&i.to_be_bytes())
            .delete_row()
            .durability(Durability::None)
            .commit()
            .unwrap();
    }
    drop(junk);
    db.compact().unwrap();
    let before = len();
    assert!(before >= grown);
    let released = db.shrink().unwrap();
    let after = len();
    assert!(released > 0, "no space released");
    // Not an exact `before - after == released`: a background compaction may allocate
    // between the two measurements.
    assert!(after < before, "{after} >= {before}");
    assert_eq!(value(&t, b"keep", "a", b"q").as_deref(), Some(&b"kept"[..]));
    // Writes after a shrink land and survive a reopen.
    t.mutate(b"late").put("a", b"q", b"x").commit().unwrap();
    db.shrink().unwrap();
    drop(t);
    db.close().unwrap();

    let db = Pigeonhole::open(&path, opts().create_if_missing(false)).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    assert_eq!(value(&t, b"keep", "a", b"q").as_deref(), Some(&b"kept"[..]));
    assert_eq!(value(&t, b"late", "a", b"q").as_deref(), Some(&b"x"[..]));
    let handle = db.clone();
    drop(t);
    db.close().unwrap();
    assert_eq!(handle.shrink().unwrap_err().code(), ErrorCode::Closed);
}

#[test]
fn a_flush_makes_none_commits_survive_a_power_loss() {
    let vfs = SimVfs::new(21);
    let opts = || sim_options(&vfs).shards(1);
    let db = Pigeonhole::open("/db/flush-crash.phdb", opts()).unwrap();
    let t = table(&db);
    t.mutate(b"flushed")
        .put("a", b"q", b"v")
        .durability(Durability::None)
        .commit()
        .unwrap();
    db.flush().unwrap();
    t.mutate(b"after")
        .put("a", b"q", b"v")
        .durability(Durability::None)
        .commit()
        .unwrap();
    vfs.crash(pigeonhole_io::sim::CrashKind::Power);
    drop(t);
    drop(db);

    let db = Pigeonhole::open("/db/flush-crash.phdb", opts()).unwrap();
    let t = db.table("t").unwrap().open().unwrap();
    // Flushed: in the file, so it survives. The commit after it was only buffered.
    assert_eq!(value(&t, b"flushed", "a", b"q").as_deref(), Some(&b"v"[..]));
    assert_eq!(value(&t, b"after", "a", b"q"), None);
    drop(t);
    db.close().unwrap();
}

#[test]
fn compact_after_drop_table_covers_the_live_tables() {
    // #83: `compact()` after `drop_table` failed with `TableNotFound` when a compaction of
    // the dropped table was in flight (the engine test forces that interleaving).
    let vfs = SimVfs::new(83);
    let path = "/db/drop-compact.phdb";
    let db = Pigeonhole::open(path, sim_options(&vfs)).unwrap();
    let make = |name: &str| {
        db.table(name)
            .unwrap()
            .family("a", Family::default())
            .create_if_missing()
            .unwrap()
    };
    let keep = make("keep");
    let gone = make("gone");
    for round in 0..4u32 {
        for i in round * 100..round * 100 + 100 {
            let row = format!("row{i:05}");
            keep.mutate(row.as_bytes())
                .put("a", b"q", &[7; 300])
                .commit()
                .unwrap();
            gone.mutate(row.as_bytes())
                .put("a", b"q", &[9; 300])
                .commit()
                .unwrap();
        }
        db.flush().unwrap();
    }
    drop(gone);
    db.drop_table("gone").unwrap();
    db.compact().unwrap();
    assert_eq!(keep.scan_prefix(b"").iter().unwrap().count(), 400);
    // Compacting again (nothing left of the dropped table) succeeds too.
    db.compact().unwrap();
    drop(keep);
    db.close().unwrap();

    let db = Pigeonhole::open(path, sim_options(&vfs)).unwrap();
    assert!(db.table("gone").unwrap().open().is_err());
    let keep = db.table("keep").unwrap().open().unwrap();
    assert_eq!(keep.scan_prefix(b"").iter().unwrap().count(), 400);
    assert_eq!(
        value(&keep, b"row00399", "a", b"q").as_deref(),
        Some(&[7u8; 300][..])
    );
    drop(keep);
    db.close().unwrap();
}
