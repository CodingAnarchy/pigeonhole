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
    Aggregation, Cell, Compaction, Condition, Durability, ErrorCode, Family, IoBackend, Options,
    Pigeonhole, Priority, Row, Table, Value, ValueFilter, days,
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
fn family_settings_are_stored_and_unregistered_operators_refused() {
    let db = db();
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
                .compression(pigeonhole::Compression::None)
                .compression(pigeonhole::Compression::Lz4)
                .block_size(4096)
                .cache_priority(Priority::High)
                .compaction(Compaction::Leveled),
        )
        .create()
        .unwrap();
    assert_eq!(t.families(), ["f"]);
    // Tiered and FIFO-by-time compaction and zstd are accepted since they landed (#44).
    let u = db
        .table("u")
        .unwrap()
        .family("tiered", Family::default().compaction(Compaction::Tiered))
        .family(
            "fifo",
            Family::default()
                .ttl(days(1))
                .compaction(Compaction::FifoByTime),
        )
        .family(
            "zstd",
            Family::default().compression(pigeonhole::Compression::Zstd(19)),
        )
        .create()
        .unwrap();
    assert_eq!(u.families(), ["tiered", "fifo", "zstd"]);
    // zstd blocks round-trip through a flush and a compaction.
    let value: Vec<u8> = (0..3000u32).map(|i| (i % 7) as u8).collect();
    for row in 0..50u32 {
        u.mutate(format!("r{row:03}").as_bytes())
            .put("zstd", b"q", &value)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    for row in 0..50u32 {
        let cell = u
            .get(format!("r{row:03}").as_bytes(), "zstd", b"q")
            .unwrap();
        assert_eq!(cell.map(|c| c.value().to_vec()), Some(value.clone()));
    }
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
        (ErrorCode::SnapshotExpired, 25),
        (ErrorCode::WouldDeadlock, 26),
        (ErrorCode::BatchTooLarge, 27),
        (ErrorCode::OutcomeUnknown, 28),
        (ErrorCode::NoWriter, 29),
        (ErrorCode::UnsupportedFeature, 30),
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
        (E::ReadOnlyOpen("x".into()), ErrorCode::ReadOnly),
        (E::KeyTooLarge, ErrorCode::KeyTooLarge),
        (E::ValueTooLarge, ErrorCode::ValueTooLarge),
        (E::NoSpace, ErrorCode::NoSpace),
        (E::InvalidArgument("x".into()), ErrorCode::InvalidArgument),
        (E::Unsupported("x"), ErrorCode::Unsupported),
        (E::Closed, ErrorCode::Closed),
        (E::NoReaderSlot, ErrorCode::NoReaderSlot),
        (E::RecordTooLarge, ErrorCode::RecordTooLarge),
        (E::Busy, ErrorCode::Busy),
        (E::BatchTooLarge, ErrorCode::BatchTooLarge),
        (E::SnapshotExpired, ErrorCode::SnapshotExpired),
        (E::WouldDeadlock, ErrorCode::WouldDeadlock),
        (E::OutcomeUnknown, ErrorCode::OutcomeUnknown),
        (E::ShardFailed, ErrorCode::Io),
        (E::NoWriter, ErrorCode::NoWriter),
        (
            E::UnsupportedFeature("value tag 4".into()),
            ErrorCode::UnsupportedFeature,
        ),
    ];
    for (e, code) in cases {
        let what = format!("{e:?}");
        let message = match &e {
            E::Merge(_) => "merge operator \"op\" failed: bad".to_owned(),
            E::Busy => {
                "stalled past the write-stall timeout (Options::write_stall_timeout, 30 s by \
                        default): no flush or compaction freed room in time, or a tablet split \
                        or move kept the commit waiting; back off and retry, and drop old \
                        snapshots"
                    .to_owned()
            }
            E::BatchTooLarge => "the batch can never fit a shard's memtable arena (about half \
                                 of Options::memtable_budget); split it or raise the budget"
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
    assert_eq!(row.entry(0).unwrap().family, "b");
    let row = t
        .scan_prefix(b"a")
        .column_limit(1)
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

/// ICR 0051: `put_f64_at` on a mutation and a batch stores the typed `f64` at the timestamp
/// given, versions included.
#[test]
fn put_f64_at_keeps_its_timestamp() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r")
        .put_f64_at("b", b"f", 1_000, 1.5)
        .commit()
        .unwrap();
    let mut wb = db.write_batch();
    wb.put_f64_at(&t, b"r", "b", b"f", 2_000, -0.25);
    wb.commit().unwrap();
    let r = t.row(b"r").family("b").versions(0).read().unwrap().unwrap();
    let got: Vec<(u64, Option<f64>)> = r
        .iter()
        .map(|e| match e.cell.typed() {
            Value::F64(x) => (e.cell.timestamp(), Some(x)),
            _ => (e.cell.timestamp(), None),
        })
        .collect();
    assert_eq!(got, [(2_000, Some(-0.25)), (1_000, Some(1.5))]);
    let latest = t.get(b"r", "b", b"f").unwrap().unwrap();
    assert_eq!(
        (latest.timestamp(), latest.typed()),
        (2_000, Value::F64(-0.25))
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
        let o = row.entry(i).unwrap();
        let e = view.entry(i).unwrap();
        let e2 = r.entry(i).unwrap();
        assert_eq!(
            (o.family, o.qualifier, o.cell.value()),
            (e.family, e.qualifier, e.cell.value())
        );
        assert_eq!((e.family, e.qualifier), (e2.family, e2.qualifier));
        assert_eq!(o.cell.timestamp(), e2.cell.timestamp());
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

/// A table with a counter family `c` and an ordinary family `a` (decision D179).
fn counters(db: &Pigeonhole) -> Table {
    db.table("counters")
        .unwrap()
        .family("c", Family::counter())
        .family("a", Family::default())
        .create_if_missing()
        .unwrap()
}

fn count(t: &Table, row: &[u8], q: &[u8]) -> Option<i64> {
    t.get(row, "c", q).unwrap().map(|c| c.as_i64().unwrap())
}

/// `(timestamp, value)` of every version of a counter column.
fn buckets(t: &Table, row: &[u8], q: &[u8]) -> Vec<(u64, i64)> {
    t.row(row)
        .family("c")
        .qualifier_range(q..=q)
        .versions(0)
        .read()
        .unwrap()
        .map(|r| {
            r.iter()
                .map(|e| (e.cell.timestamp(), e.cell.as_i64().unwrap()))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn counters_and_typed_values() {
    let db = db();
    let t = counters(&db);
    for d in [5, -2, 10] {
        t.mutate(b"r").incr("c", b"hits", d).commit().unwrap();
    }
    assert_eq!(count(&t, b"r", b"hits"), Some(13));
    // One cell at the fixed timestamp, however many increments.
    assert_eq!(buckets(&t, b"r", b"hits"), [(0, 13)]);
    // Writes of one counter in one mutation apply in order (D186, #295): a put then an
    // increment add up, two increments add both, and a put after an increment resets.
    t.mutate(b"r")
        .put_i64("c", b"hits", 100)
        .incr("c", b"hits", 7)
        .commit()
        .unwrap();
    assert_eq!(count(&t, b"r", b"hits"), Some(107));
    t.mutate(b"r")
        .incr("c", b"hits", 1)
        .incr("c", b"hits", 2)
        .commit()
        .unwrap();
    assert_eq!(count(&t, b"r", b"hits"), Some(110));
    t.mutate(b"r")
        .incr("c", b"hits", 50)
        .put_i64("c", b"hits", 100)
        .commit()
        .unwrap();
    assert_eq!(count(&t, b"r", b"hits"), Some(100));
    t.mutate(b"r").incr("c", b"hits", 1).commit().unwrap();
    assert_eq!(count(&t, b"r", b"hits"), Some(101));
    // A batch over rows on different shards combines per cell too.
    let mut wb = db.write_batch();
    wb.incr(&t, b"r", "c", b"hits", 1)
        .incr(&t, b"s", "c", b"hits", 1)
        .incr(&t, b"s", "c", b"hits", 1)
        .incr_at(&t, b"s", "c", b"hits", 9, 4)
        .incr_at(&t, b"s", "c", b"hits", 9, 5);
    wb.commit().unwrap();
    assert_eq!(count(&t, b"r", b"hits"), Some(102));
    assert_eq!(buckets(&t, b"s", b"hits"), [(9, 9), (0, 2)]);
    // Overflow wraps.
    t.mutate(b"w")
        .put_i64("c", b"n", i64::MAX)
        .commit()
        .unwrap();
    t.mutate(b"w").incr("c", b"n", 1).commit().unwrap();
    assert_eq!(count(&t, b"w", b"n"), Some(i64::MIN));
}

#[test]
fn counter_buckets_are_versions() {
    let db = db();
    let t = counters(&db);
    let hour = 3_600_000_000;
    for (h, d) in [(7, 1), (8, 4), (8, 1), (9, 2)] {
        t.mutate(b"r")
            .incr_at("c", b"hourly", h * hour, d)
            .commit()
            .unwrap();
    }
    let mut wb = db.write_batch();
    wb.incr_at(&t, b"r", "c", b"hourly", 9 * hour, 3);
    wb.commit().unwrap();
    assert_eq!(
        buckets(&t, b"r", b"hourly"),
        [(9 * hour, 5), (8 * hour, 5), (7 * hour, 1)]
    );
    // put_i64_at sets a bucket; later increments add to it.
    t.mutate(b"r")
        .put_i64_at("c", b"hourly", 8 * hour, 0)
        .commit()
        .unwrap();
    t.mutate(b"r")
        .incr_at("c", b"hourly", 8 * hour, 2)
        .commit()
        .unwrap();
    let mut wb = db.write_batch();
    wb.put_i64_at(&t, b"r", "c", b"hourly", 7 * hour, 50);
    wb.commit().unwrap();
    assert_eq!(
        buckets(&t, b"r", b"hourly"),
        [(9 * hour, 5), (8 * hour, 2), (7 * hour, 50)]
    );
    // A time range reads a window of buckets.
    let window: Vec<i64> = t
        .row(b"r")
        .family("c")
        .versions(0)
        .time_range(8 * hour..10 * hour)
        .read()
        .unwrap()
        .unwrap()
        .iter()
        .map(|e| e.cell.as_i64().unwrap())
        .collect();
    assert_eq!(window, [5, 2]);
    // A delete of one bucket leaves the others; flushes and compactions change nothing.
    t.mutate(b"r")
        .delete_cell("c", b"hourly", 9 * hour)
        .commit()
        .unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    assert_eq!(
        buckets(&t, b"r", b"hourly"),
        [(8 * hour, 2), (7 * hour, 50)]
    );
    // max_versions and the TTL apply per bucket.
    let capped = db
        .table("capped")
        .unwrap()
        .family("c", Family::counter().max_versions(2))
        .create()
        .unwrap();
    for h in 1..=4 {
        capped
            .mutate(b"r")
            .incr_at("c", b"n", h * hour, 1)
            .commit()
            .unwrap();
    }
    assert_eq!(buckets(&capped, b"r", b"n"), [(4 * hour, 1), (3 * hour, 1)]);
}

#[test]
fn counter_deletes_hide_only_what_came_before() {
    let db = db();
    let t = counters(&db);
    t.mutate(b"r").incr("c", b"n", 5).commit().unwrap();
    t.mutate(b"r").delete_column("c", b"n").commit().unwrap();
    assert_eq!(count(&t, b"r", b"n"), None);
    // The counter starts again from 0: the delete does not hide later increments.
    t.mutate(b"r").incr("c", b"n", 2).commit().unwrap();
    assert_eq!(count(&t, b"r", b"n"), Some(2));
    let before = db.snapshot().unwrap();
    t.mutate(b"r").delete_row().commit().unwrap();
    t.mutate(b"r").incr("c", b"n", 1).commit().unwrap();
    assert_eq!(count(&t, b"r", b"n"), Some(1));
    db.flush().unwrap();
    db.compact().unwrap();
    assert_eq!(count(&t, b"r", b"n"), Some(1));
    // A snapshot reads the sum of the increments it sees.
    assert_eq!(
        t.get_at(&before, b"r", "c", b"n")
            .unwrap()
            .unwrap()
            .as_i64(),
        Some(2)
    );
    t.mutate(b"r").delete_family("c").commit().unwrap();
    t.mutate(b"r").delete_cell("c", b"m", 0).commit().unwrap();
    t.mutate(b"r").incr("c", b"m", 4).commit().unwrap();
    assert_eq!(count(&t, b"r", b"n"), None);
    assert_eq!(count(&t, b"r", b"m"), Some(4));
}

#[test]
fn counter_family_write_rules() {
    let db = db();
    let t = counters(&db);
    let refused = |r: pigeonhole::Result<pigeonhole::CommitInfo>, what: &str| {
        let err = r.unwrap_err();
        assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
        assert!(err.message().contains(what), "{err}");
    };
    // `incr` needs a counter family, and says so.
    refused(
        t.mutate(b"r").incr("a", b"n", 1).commit(),
        "family \"a\" of table \"counters\" has no merge operator",
    );
    refused(
        t.mutate(b"r").incr_at("a", b"n", 5, 1).commit(),
        "not a counter family",
    );
    // A counter family holds only i64s.
    for m in [
        t.mutate(b"r").put("c", b"n", b"text"),
        t.mutate(b"r").put_at("c", b"n", 5, b"text"),
        t.mutate(b"r").put_f64("c", b"n", 1.5),
        t.mutate(b"r").merge("c", b"n", &1i64.to_le_bytes()),
    ] {
        refused(m.commit(), "only i64 values");
    }
    let mut wb = db.write_batch();
    wb.put(&t, b"r", "c", b"n", b"text");
    refused(wb.commit(), "only i64 values");
    // With a TTL the fixed timestamp would expire at once: buckets only.
    let ttl = db
        .table("ttl")
        .unwrap()
        .family("c", Family::counter().ttl(days(1)))
        .create()
        .unwrap();
    refused(ttl.mutate(b"r").incr("c", b"n", 1).commit(), "TTL");
    refused(ttl.mutate(b"r").put_i64("c", b"n", 1).commit(), "TTL");
    let bucket = 4_000_000_000_000_000; // well after the sim clock: not expired
    ttl.mutate(b"r")
        .incr_at("c", b"n", bucket, 3)
        .commit()
        .unwrap();
    assert_eq!(buckets(&ttl, b"r", b"n"), [(bucket, 3)]);
    // A counter family sums with the built-in operator only.
    let err = db
        .table("bad")
        .unwrap()
        .family("c", Family::counter().merge_operator(""))
        .create()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
    // Nothing refused was written.
    assert!(t.row(b"r").read().unwrap().is_none());
}

/// D294: min and max families keep the smallest and largest input of each bucket; a put
/// sets a bucket; `incr` is refused there (`aggregate` writes their inputs); their info
/// round-trips through `FamilyInfo::family`.
#[test]
fn min_and_max_families() {
    let db = db();
    let t = db
        .table("agg")
        .unwrap()
        .family("lo", Family::aggregate(Aggregation::Min))
        .family("hi", Family::aggregate(Aggregation::Max))
        .family("c", Family::counter())
        .create()
        .unwrap();
    let get = |f: &str, q: &[u8]| t.get(b"r", f, q).unwrap().map(|c| c.as_i64().unwrap());
    assert_eq!(get("lo", b"x"), None, "no input: absent");
    for v in [4, -9, 2] {
        t.mutate(b"r")
            .aggregate("lo", b"x", v)
            .aggregate("hi", b"x", v)
            .aggregate("c", b"x", v)
            .commit()
            .unwrap();
    }
    assert_eq!((get("lo", b"x"), get("hi", b"x")), (Some(-9), Some(4)));
    assert_eq!(
        get("c", b"x"),
        Some(-3),
        "aggregate in a counter family is incr"
    );
    // A put sets the cell, and later inputs combine with it, in one mutation too.
    t.mutate(b"r")
        .put_i64("lo", b"x", 100)
        .aggregate("lo", b"x", 50)
        .commit()
        .unwrap();
    assert_eq!(get("lo", b"x"), Some(50));
    // Buckets.
    let mut wb = db.write_batch();
    for (ts, v) in [(10, 3), (10, 7), (20, 1)] {
        wb.aggregate_at(&t, b"r", "hi", b"b", ts, v);
    }
    wb.commit().unwrap();
    let versions: Vec<(u64, i64)> = t
        .row(b"r")
        .family("hi")
        .qualifier_range(&b"b"[..]..=&b"b"[..])
        .versions(0)
        .read()
        .unwrap()
        .unwrap()
        .iter()
        .map(|e| (e.cell.timestamp(), e.cell.as_i64().unwrap()))
        .collect();
    assert_eq!(versions, [(20, 1), (10, 7)]);
    let mut txn = db.transaction().unwrap();
    txn.aggregate(&t, b"r", "lo", b"x", -60);
    txn.commit().unwrap();
    assert_eq!(get("lo", b"x"), Some(-60));
    // `incr` is refused in a min or max family, through every write path.
    let refused = |r: pigeonhole::Result<pigeonhole::CommitInfo>, what: &str| {
        let err = r.unwrap_err();
        assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
        assert!(err.message().contains(what), "{err}");
    };
    refused(
        t.mutate(b"r").incr("lo", b"x", 1).commit(),
        "is a min family: write its inputs with aggregate",
    );
    refused(
        t.mutate(b"r").incr_at("hi", b"x", 5, 1).commit(),
        "is a max family",
    );
    let mut wb = db.write_batch();
    wb.incr(&t, b"r", "hi", b"x", 1);
    refused(wb.commit(), "is a max family");
    let mut txn = db.transaction().unwrap();
    txn.incr_at(&t, b"r", "lo", b"x", 5, 1);
    refused(txn.commit(), "is a min family");
    // `aggregate` needs an aggregate family (perf287's S3 on #1194), even one whose operator
    // would take the operand.
    let plain = db
        .table("plain")
        .unwrap()
        .family("f", Family::default())
        .family(
            "legacy",
            Family::default().merge_operator("pigeonhole.i64_add"),
        )
        .create()
        .unwrap();
    for f in ["f", "legacy"] {
        refused(
            plain.mutate(b"r").aggregate(f, b"x", 1).commit(),
            "is not an aggregate family",
        );
    }
    let mut wb = db.write_batch();
    wb.aggregate_at(&plain, b"r", "legacy", b"x", 5, 1);
    refused(wb.commit(), "is not an aggregate family");
    let mut txn = db.transaction().unwrap();
    txn.aggregate(&plain, b"r", "f", b"x", 1);
    refused(txn.commit(), "is not an aggregate family");
    // Only i64s; a counter family's message still points at incr.
    refused(
        t.mutate(b"r").put("lo", b"x", b"text").commit(),
        "only i64 values",
    );
    refused(
        t.mutate(b"r").put("c", b"x", b"text").commit(),
        "(put_i64 / incr)",
    );
    assert_eq!(get("lo", b"x"), Some(-60), "nothing refused was written");
    // The info names the aggregation, and recreates the family.
    let lo = t.family_info("lo").unwrap();
    assert_eq!(lo.aggregation, Some(Aggregation::Min));
    assert!(!lo.counter);
    assert_eq!(lo.merge_operator, None);
    let c = t.family_info("c").unwrap();
    assert_eq!((c.counter, c.aggregation), (true, Some(Aggregation::Sum)));
    let a = db.table("copy").unwrap().family("f", Family::default());
    let copy = a.family("lo", lo.family.clone()).create().unwrap();
    assert_eq!(
        copy.family_info("lo").unwrap().aggregation,
        Some(Aggregation::Min)
    );
    assert_eq!(copy.family_info("f").unwrap().aggregation, None);
}

/// D294: an HLL family counts distinct `aggregate` inputs per bucket, read with
/// `hll_estimate`; `incr` and `put_i64` are refused; its info names it.
#[test]
fn hll_families_estimate_distinct_inputs() {
    let db = db();
    let t = db
        .table("visits")
        .unwrap()
        .family("users", Family::aggregate(Aggregation::Hll))
        .create()
        .unwrap();
    let estimate = |q: &[u8]| {
        t.get(b"page", "users", q)
            .unwrap()
            .map(|c| c.hll_estimate().unwrap())
    };
    assert_eq!(estimate(b"all"), None, "no input: absent");
    // 3000 inputs of 1000 distinct users, in batches.
    for chunk in 0..30i64 {
        let mut m = t.mutate(b"page");
        for i in 0..100 {
            m = m.aggregate("users", b"all", (chunk * 100 + i) % 1_000);
        }
        m.commit().unwrap();
    }
    let n = estimate(b"all").unwrap();
    assert!((950..=1_050).contains(&n), "{n}");
    // Hourly buckets.
    let hour = 3_600_000_000;
    t.mutate(b"page")
        .aggregate_at("users", b"hourly", hour, 1)
        .aggregate_at("users", b"hourly", hour, 2)
        .aggregate_at("users", b"hourly", 2 * hour, 1)
        .commit()
        .unwrap();
    let row = t
        .row(b"page")
        .family("users")
        .qualifier_range(&b"hourly"[..]..=&b"hourly"[..])
        .versions(0)
        .read()
        .unwrap()
        .unwrap();
    let hourly: Vec<(u64, u64)> = row
        .iter()
        .map(|e| (e.cell.timestamp(), e.cell.hll_estimate().unwrap()))
        .collect();
    assert_eq!(hourly, [(2 * hour, 1), (hour, 2)]);
    // A non-sketch value has no estimate.
    let c = db
        .table("c")
        .unwrap()
        .family("n", Family::counter())
        .create()
        .unwrap();
    c.mutate(b"r").incr("n", b"q", 5).commit().unwrap();
    assert_eq!(
        c.get(b"r", "n", b"q").unwrap().unwrap().hll_estimate(),
        None
    );
    let refused = |r: pigeonhole::Result<pigeonhole::CommitInfo>, what: &str| {
        let err = r.unwrap_err();
        assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
        assert!(err.message().contains(what), "{err}");
    };
    refused(
        t.mutate(b"page").incr("users", b"all", 1).commit(),
        "is an hll family",
    );
    refused(
        t.mutate(b"page").put_i64("users", b"all", 1).commit(),
        "hll family",
    );
    refused(
        t.mutate(b"page").put("users", b"all", b"x").commit(),
        "hll family",
    );
    let info = t.family_info("users").unwrap();
    assert_eq!(info.aggregation, Some(Aggregation::Hll));
    assert!(!info.counter && info.merge_operator.is_none());
    // Sketches stay inline: a lower blob threshold is refused at creation.
    let err = db
        .table("small")
        .unwrap()
        .family("u", Family::aggregate(Aggregation::Hll).blob_threshold(100))
        .create()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
}

/// D298: bitwise families fold `u64` inputs (`aggregate_u64`, read with `as_u64`) with OR,
/// AND or XOR; `f64` families keep the smallest or largest `f64` (`aggregate_f64`, read with
/// `as_f64`) by total order, never NaN; `incr` is refused, and each info names its kind.
#[test]
fn bitwise_and_f64_families() {
    let db = db();
    let t = db
        .table("scalars")
        .unwrap()
        .family("or", Family::aggregate(Aggregation::BitOr))
        .family("and", Family::aggregate(Aggregation::BitAnd))
        .family("xor", Family::aggregate(Aggregation::BitXor))
        .family("cold", Family::aggregate(Aggregation::MinF64))
        .family("warm", Family::aggregate(Aggregation::MaxF64))
        .create()
        .unwrap();
    let bits = |f: &str| t.get(b"r", f, b"q").unwrap().map(|c| c.as_u64().unwrap());
    let float = |f: &str| t.get(b"r", f, b"q").unwrap().map(|c| c.as_f64().unwrap());
    assert_eq!(bits("or"), None, "no input: absent");
    for (b, x) in [(0b1100u64, 0.0), (0b1010, -0.0), (u64::MAX - 1, 2.5)] {
        t.mutate(b"r")
            .aggregate_u64("or", b"q", b)
            .aggregate_u64("and", b"q", b)
            .aggregate_u64("xor", b"q", b)
            .aggregate_f64("cold", b"q", x)
            .aggregate_f64("warm", b"q", x)
            .commit()
            .unwrap();
    }
    assert_eq!(bits("or"), Some(u64::MAX - 1));
    assert_eq!(bits("and"), Some(0b1000));
    assert_eq!(bits("xor"), Some(0b0110 ^ (u64::MAX - 1)));
    // -0.0 orders below +0.0.
    assert_eq!(float("cold").map(f64::to_bits), Some((-0.0f64).to_bits()));
    assert_eq!(float("warm"), Some(2.5));
    // A put sets the cell; later inputs combine with it. Batches, transactions, buckets.
    t.mutate(b"r")
        .put_f64("warm", b"q", f64::NEG_INFINITY)
        .aggregate_f64("warm", b"q", -1.0)
        .commit()
        .unwrap();
    assert_eq!(float("warm"), Some(-1.0));
    let mut wb = db.write_batch();
    wb.aggregate_u64(&t, b"r", "xor", b"q", 0b0110)
        .aggregate_f64_at(&t, b"r", "cold", b"b", 10, 4.0)
        .aggregate_f64_at(&t, b"r", "cold", b"b", 10, f64::INFINITY);
    wb.commit().unwrap();
    assert_eq!(bits("xor"), Some(u64::MAX - 1));
    let cell = t.get(b"r", "cold", b"b").unwrap().unwrap();
    assert_eq!((cell.timestamp(), cell.as_f64()), (10, Some(4.0)));
    let mut txn = db.transaction().unwrap();
    txn.aggregate_u64(&t, b"r", "and", b"q", 0).aggregate_f64(
        &t,
        b"r",
        "cold",
        b"q",
        f64::NEG_INFINITY,
    );
    txn.commit().unwrap();
    assert_eq!(bits("and"), Some(0));
    // put_u64 sets a bitwise bucket.
    t.mutate(b"r")
        .put_u64("and", b"q", u64::MAX)
        .aggregate_u64("and", b"q", 0xf0)
        .commit()
        .unwrap();
    assert_eq!(bits("and"), Some(0xf0));
    let mut wb = db.write_batch();
    wb.put_u64_at(&t, b"r", "or", b"b", 5, 1 << 63);
    wb.commit().unwrap();
    let cell = t.get(b"r", "or", b"b").unwrap().unwrap();
    assert_eq!((cell.timestamp(), cell.as_u64()), (5, Some(1 << 63)));
    assert_eq!(float("cold"), Some(f64::NEG_INFINITY));
    // Refused: incr, NaN, and the other numeric type.
    let refused = |r: pigeonhole::Result<pigeonhole::CommitInfo>, what: &str| {
        let err = r.unwrap_err();
        assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
        assert!(err.message().contains(what), "{err}");
    };
    refused(
        t.mutate(b"r").incr("or", b"q", 1).commit(),
        "is a bit_or family: write its inputs with aggregate_u64",
    );
    refused(
        t.mutate(b"r").incr("warm", b"q", 1).commit(),
        "is a max_f64 family: write its inputs with aggregate_f64",
    );
    refused(
        t.mutate(b"r")
            .aggregate_f64("cold", b"q", f64::NAN)
            .commit(),
        "never NaN",
    );
    refused(
        t.mutate(b"r").put_f64("warm", b"q", f64::NAN).commit(),
        "never NaN",
    );
    refused(t.mutate(b"r").aggregate("cold", b"q", 1).commit(), "f64");
    refused(
        t.mutate(b"r").aggregate_f64("xor", b"q", 1.0).commit(),
        "aggregate_u64",
    );
    // Typed inputs of the wrong type are refused at the builder, naming the method to use
    // (perf287's N2 on #1213): a u64 into a counter would otherwise add its bits.
    let other = db
        .table("other")
        .unwrap()
        .family("c", Family::counter())
        .family("lo", Family::aggregate(Aggregation::Min))
        .create()
        .unwrap();
    refused(
        other.mutate(b"r").aggregate_u64("c", b"q", 1).commit(),
        "is a counter family: write its inputs with incr, not aggregate_u64",
    );
    refused(
        other.mutate(b"r").aggregate_f64("lo", b"q", 1.0).commit(),
        "is a min family: write its inputs with aggregate, not aggregate_f64",
    );
    refused(
        t.mutate(b"r").aggregate("warm", b"q", 1).commit(),
        "is a max_f64 family: write its inputs with aggregate_f64, not aggregate",
    );
    let mut wb = db.write_batch();
    wb.aggregate_u64_at(&other, b"r", "lo", b"q", 5, 1);
    refused(wb.commit(), "not aggregate_u64");
    let mut txn = db.transaction().unwrap();
    txn.aggregate_f64(&other, b"r", "c", b"q", 1.0);
    refused(txn.commit(), "not aggregate_f64");
    assert!(other.get(b"r", "c", b"q").unwrap().is_none());
    assert_eq!(
        float("cold"),
        Some(f64::NEG_INFINITY),
        "nothing refused was written"
    );
    for (f, a) in [
        ("or", Aggregation::BitOr),
        ("and", Aggregation::BitAnd),
        ("xor", Aggregation::BitXor),
        ("cold", Aggregation::MinF64),
        ("warm", Aggregation::MaxF64),
    ] {
        let info = t.family_info(f).unwrap();
        assert_eq!(info.aggregation, Some(a), "{f}");
        assert!(!info.counter && info.merge_operator.is_none(), "{f}");
    }
}

/// D298 PR 2: an exact-sum family reads the correctly rounded sum of its `f64` inputs with
/// `as_f64`; a mean-variance family reads count, mean and variances with `mean_var`. Both
/// take finite inputs only, `put_f64` sets a bucket, `incr` and `aggregate(i64)` are refused,
/// and each info names its kind.
#[test]
fn exact_sum_and_mean_var_families() {
    let db = db();
    let t = db
        .table("stats")
        .unwrap()
        .family("total", Family::aggregate(Aggregation::SumF64))
        .family("lat", Family::aggregate(Aggregation::MeanVar))
        .create()
        .unwrap();
    let total = || {
        t.get(b"r", "total", b"q")
            .unwrap()
            .map(|c| c.sum_f64().unwrap())
    };
    let lat = || {
        t.get(b"r", "lat", b"q")
            .unwrap()
            .map(|c| c.mean_var().unwrap())
    };
    assert_eq!(total(), None, "no input: absent");
    assert!(lat().is_none());
    for x in [1e300, 1.0, -1e300, 2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
        t.mutate(b"r")
            .aggregate_f64("total", b"q", x)
            .commit()
            .unwrap();
    }
    // Floating-point addition would give 40 (1e300 + 1.0 is 1e300); the exact sum keeps the
    // 1.0.
    assert_eq!(total(), Some(41.0));
    // `as_f64` reads `f64` values only: a state is not one (perf287's answer on #1220), so a
    // standard family's bytes that happen to parse as a state never read as a number.
    let cell = t.get(b"r", "total", b"q").unwrap().unwrap();
    assert_eq!(cell.as_f64(), None);
    let plain = db
        .table("plain_bytes")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    plain
        .mutate(b"r")
        .put("f", b"q", b"\x02\x00")
        .commit()
        .unwrap();
    assert_eq!(plain.get(b"r", "f", b"q").unwrap().unwrap().as_f64(), None);
    let mut wb = db.write_batch();
    for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
        wb.aggregate_f64(&t, b"r", "lat", b"q", x);
    }
    wb.commit().unwrap();
    let m = lat().unwrap();
    assert_eq!((m.count(), m.mean(), m.variance()), (8, 5.0, 4.0));
    assert_eq!(m.sample_variance(), Some(32.0 / 7.0));
    // A put sets the bucket; later inputs fold onto it. Transactions and buckets too.
    t.mutate(b"r")
        .put_f64("total", b"q", 0.5)
        .aggregate_f64("total", b"q", 0.25)
        .commit()
        .unwrap();
    assert_eq!(total(), Some(0.75));
    let mut txn = db.transaction().unwrap();
    txn.aggregate_f64_at(&t, b"r", "lat", b"hour", 3_600, 10.0);
    txn.commit().unwrap();
    let cell = t.get(b"r", "lat", b"hour").unwrap().unwrap();
    let one = cell.mean_var().unwrap();
    assert_eq!(
        (
            cell.timestamp(),
            one.count(),
            one.mean(),
            one.sample_variance()
        ),
        (3_600, 1, 10.0, None)
    );
    // Refused: non-finite inputs, incr, and i64 inputs.
    let refused = |r: pigeonhole::Result<pigeonhole::CommitInfo>, what: &str| {
        let err = r.unwrap_err();
        assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
        assert!(err.message().contains(what), "{err}");
    };
    for bad in [f64::NAN, f64::INFINITY] {
        refused(
            t.mutate(b"r").aggregate_f64("total", b"q", bad).commit(),
            "finite f64",
        );
        refused(
            t.mutate(b"r").put_f64("lat", b"q", bad).commit(),
            "finite f64",
        );
    }
    refused(
        t.mutate(b"r").incr("total", b"q", 1).commit(),
        "is a sum_f64 family: write its inputs with aggregate_f64",
    );
    refused(
        t.mutate(b"r").aggregate("lat", b"q", 1).commit(),
        "is a mean_var family: write its inputs with aggregate_f64, not aggregate",
    );
    assert_eq!(total(), Some(0.75), "nothing refused was written");
    for (f, a) in [
        ("total", Aggregation::SumF64),
        ("lat", Aggregation::MeanVar),
    ] {
        let info = t.family_info(f).unwrap();
        assert_eq!(info.aggregation, Some(a), "{f}");
        assert!(!info.counter && info.merge_operator.is_none(), "{f}");
    }
}

/// D298 PR 3: a histogram family counts `f64` inputs in the buckets of its
/// `HistogramParams`; a quantile family reads quantiles within 2^-p. Their parameters
/// round-trip through `FamilyInfo`, bad ones are refused, and `incr` is refused.
#[test]
fn histogram_and_quantile_families() {
    use pigeonhole::{HistogramParams, QuantileParams};
    let db = db();
    let bounds = HistogramParams::new([0.0, 10.0, 100.0]).unwrap();
    let q = QuantileParams::default().mantissa_bits(6);
    let t = db
        .table("lat")
        .unwrap()
        .family(
            "h",
            Family::aggregate(Aggregation::Histogram(bounds.clone())),
        )
        .family("q", Family::aggregate(Aggregation::Quantiles(q)))
        .create()
        .unwrap();
    let mut wb = db.write_batch();
    for x in 1..=1000 {
        let x = f64::from(x) / 4.0; // 0.25 to 250
        wb.aggregate_f64(&t, b"r", "h", b"ms", x)
            .aggregate_f64(&t, b"r", "q", b"ms", x);
    }
    wb.aggregate_f64(&t, b"r", "h", b"ms", f64::INFINITY);
    wb.commit().unwrap();
    let h = t.get(b"r", "h", b"ms").unwrap().unwrap();
    // [-inf, 0): 0; [0, 10): 0.25..9.75 = 39; [10, 100): 40..399 = 360; [100, inf): 601 + inf.
    assert_eq!(h.histogram_counts(), Some(vec![0, 39, 360, 602]));
    assert_eq!(bounds.bucket_of(10.0), 2);
    assert_eq!(bounds.bucket_of(-0.0), 1, "-0.0 shares 0.0's bucket (IEEE)");
    let c = t.get(b"r", "q", b"ms").unwrap().unwrap();
    assert_eq!(c.quantiles_count(), Some(1000));
    for (qq, exact) in [(0.0, 0.25), (0.5, 125.0), (0.99, 247.5), (1.0, 250.0)] {
        let got = c.quantile(qq).unwrap();
        assert!(
            (got - exact).abs() / exact <= 2f64.powi(-6),
            "q {qq}: {got} vs {exact}"
        );
    }
    assert_eq!(c.quantile(2.0), None);
    // Parameters round-trip through FamilyInfo, and recreate the family.
    assert_eq!(
        t.family_info("h").unwrap().aggregation,
        Some(Aggregation::Histogram(bounds))
    );
    let info = t.family_info("q").unwrap();
    assert_eq!(info.aggregation, Some(Aggregation::Quantiles(q)));
    let copy = db
        .table("copy")
        .unwrap()
        .family("q", info.family.clone())
        .create()
        .unwrap();
    assert_eq!(
        copy.family_info("q").unwrap().aggregation,
        Some(Aggregation::Quantiles(q))
    );
    // Refused: bad bounds at construction, bad quantile parameters at creation, incr, NaN.
    for bad in [&[][..], &[1.0, 1.0], &[2.0, 1.0], &[f64::NAN], &[-0.0]] {
        let err = HistogramParams::new(bad).unwrap_err();
        assert_eq!(err.code(), ErrorCode::InvalidArgument, "{bad:?}: {err}");
    }
    let err = db
        .table("bad")
        .unwrap()
        .family(
            "q",
            Family::aggregate(Aggregation::Quantiles(
                QuantileParams::default().max_buckets(4),
            )),
        )
        .create()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
    let refused = |r: pigeonhole::Result<pigeonhole::CommitInfo>, what: &str| {
        let err = r.unwrap_err();
        assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
        assert!(err.message().contains(what), "{err}");
    };
    refused(
        t.mutate(b"r").incr("h", b"ms", 1).commit(),
        "is a histogram family: write its inputs with aggregate_f64",
    );
    refused(
        t.mutate(b"r").aggregate_f64("h", b"ms", f64::NAN).commit(),
        "never NaN",
    );
    refused(
        t.mutate(b"r")
            .aggregate_f64("q", b"ms", f64::INFINITY)
            .commit(),
        "finite f64",
    );
}

/// A family that names `pigeonhole.i64_add` without being a counter family reads and writes
/// as 0.1.0 created them (D41): increments at the commit timestamp, folded across
/// timestamps, and bytes on top of a counter fail at read.
#[test]
fn families_with_the_i64_operator_keep_0_1_0_semantics() {
    let db = db();
    let t = db
        .table("legacy")
        .unwrap()
        .family("a", Family::default().merge_operator("pigeonhole.i64_add"))
        .create()
        .unwrap();
    for d in [5, -2, 10] {
        t.mutate(b"r").incr("a", b"hits", d).commit().unwrap();
    }
    let cell = t.get(b"r", "a", b"hits").unwrap().unwrap();
    assert_eq!(cell.as_i64(), Some(13));
    assert!(cell.timestamp() > 0, "at the commit timestamp");
    // `merge` writes an operand for the family's operator; the built-in takes typed
    // operands only, so untyped bytes fail at read.
    t.mutate(b"r")
        .merge("a", b"hits", &3i64.to_le_bytes())
        .commit()
        .unwrap();
    assert_eq!(
        t.get(b"r", "a", b"hits").map(|_| ()).unwrap_err().code(),
        ErrorCode::MergeFailed
    );
    t.mutate(b"m").put("a", b"c", b"text").commit().unwrap();
    t.mutate(b"m").incr("a", b"c", 1).commit().unwrap();
    assert_eq!(
        t.get(b"m", "a", b"c").map(|_| ()).unwrap_err().code(),
        ErrorCode::MergeFailed
    );
    // Buckets belong to counter families.
    let err = t
        .mutate(b"r")
        .incr_at("a", b"n", 5, 1)
        .commit()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument, "{err}");
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

/// Conditional commits through either front door (#42's parity: the same cases, the same
/// results).
fn conditional_commits(front: Front) {
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
            .commit_if_via(front, &exists)
            .unwrap()
            .is_none()
    );
    assert!(
        t.mutate(b"r")
            .put("a", b"lock", b"w1")
            .commit_if_via(front, &absent)
            .unwrap()
            .is_some()
    );
    assert!(
        t.mutate(b"r")
            .put("a", b"lock", b"w2")
            .commit_if_via(front, &absent)
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
            .commit_if_via(front, &owner_is(b"w2"))
            .unwrap()
            .is_none()
    );
    let info = t
        .mutate(b"r")
        .delete_column("a", b"lock")
        .durability(Durability::Sync)
        .commit_if_via(front, &owner_is(b"w1"))
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
            .commit_if_via(front, &gt(7))
            .unwrap()
            .is_none()
    );
    assert!(
        t.mutate(b"n")
            .put_i64("a", b"v", 8)
            .commit_if_via(front, &gt(6))
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
            .commit_if_via(front, &bad)
            .unwrap_err()
            .code(),
        ErrorCode::FamilyNotFound
    );
}

/// Transactions through either front door.
fn transactions_commit_or_conflict(front: Front) {
    let db = db();
    let t = table(&db);
    t.mutate(b"a").put("a", b"bal", b"10").commit().unwrap();
    let mut txn = db.transaction().unwrap();
    assert_eq!(
        txn.get_via(front, &t, b"a", "a", b"bal")
            .unwrap()
            .unwrap()
            .value(),
        b"10"
    );
    txn.put(&t, b"a", "a", b"bal", b"5")
        .put(&t, b"b", "a", b"bal", b"5")
        .delete_column(&t, b"c", "a", b"bal");
    let info = txn.commit_with_via(front, Durability::GroupSync).unwrap();
    assert_eq!(info.durability, Durability::GroupSync);
    assert_eq!(value(&t, b"b", "a", b"bal").as_deref(), Some(&b"5"[..]));

    let mut txn = db.transaction().unwrap();
    let _ = txn.get_via(front, &t, b"a", "a", b"bal").unwrap();
    txn.put(&t, b"a", "a", b"bal", b"0");
    t.mutate(b"a").put("a", b"bal", b"99").commit().unwrap();
    assert_eq!(
        txn.commit_via(front).unwrap_err().code(),
        ErrorCode::Conflict
    );
    assert_eq!(value(&t, b"a", "a", b"bal").as_deref(), Some(&b"99"[..]));

    // Errors while buffering surface at commit.
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"a", "nope", b"q", b"v");
    assert_eq!(
        txn.commit_via(front).unwrap_err().code(),
        ErrorCode::FamilyNotFound
    );
    let mut txn = db.transaction().unwrap();
    assert_eq!(
        txn.get_via(front, &t, b"a", "nope", b"q")
            .map(|_| ())
            .unwrap_err()
            .code(),
        ErrorCode::FamilyNotFound
    );
}

#[test]
fn conditional_commits_sync() {
    conditional_commits(Front::Sync);
}

#[cfg(feature = "async")]
#[test]
fn conditional_commits_async() {
    conditional_commits(Front::Async);
}

#[test]
fn transactions_commit_or_conflict_sync() {
    transactions_commit_or_conflict(Front::Sync);
}

#[cfg(feature = "async")]
#[test]
fn transactions_commit_or_conflict_async() {
    transactions_commit_or_conflict(Front::Async);
}

/// Which front door a parity test goes through (#42).
#[derive(Debug, Clone, Copy)]
enum Front {
    Sync,
    #[cfg(feature = "async")]
    Async,
}

/// `commit_if` through a front door.
trait CommitIfVia {
    fn commit_if_via(
        self,
        front: Front,
        c: &Condition,
    ) -> pigeonhole::Result<Option<pigeonhole::CommitInfo>>;
}

impl CommitIfVia for pigeonhole::RowMutation<'_> {
    fn commit_if_via(
        self,
        front: Front,
        c: &Condition,
    ) -> pigeonhole::Result<Option<pigeonhole::CommitInfo>> {
        match front {
            Front::Sync => self.commit_if(c),
            #[cfg(feature = "async")]
            Front::Async => pigeonhole::doc_support::block_on(self.commit_if_async(c)),
        }
    }
}

/// A transaction's reads and commits through a front door.
trait TxnVia {
    fn get_via(
        &mut self,
        front: Front,
        t: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> pigeonhole::Result<Option<Cell>>;
    fn commit_via(self, front: Front) -> pigeonhole::Result<pigeonhole::CommitInfo>;
    fn commit_with_via(
        self,
        front: Front,
        d: Durability,
    ) -> pigeonhole::Result<pigeonhole::CommitInfo>;
}

impl TxnVia for pigeonhole::Transaction {
    fn get_via(
        &mut self,
        front: Front,
        t: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> pigeonhole::Result<Option<Cell>> {
        match front {
            Front::Sync => self
                .get(t, row, family, qualifier)
                .map(|c| c.map(|c| c.to_owned())),
            #[cfg(feature = "async")]
            Front::Async => {
                pigeonhole::doc_support::block_on(self.get_async(t, row, family, qualifier))
            }
        }
    }

    fn commit_via(self, front: Front) -> pigeonhole::Result<pigeonhole::CommitInfo> {
        match front {
            Front::Sync => self.commit(),
            #[cfg(feature = "async")]
            Front::Async => pigeonhole::doc_support::block_on(self.commit_async()),
        }
    }

    fn commit_with_via(
        self,
        front: Front,
        d: Durability,
    ) -> pigeonhole::Result<pigeonhole::CommitInfo> {
        match front {
            Front::Sync => self.commit_with(d),
            #[cfg(feature = "async")]
            Front::Async => pigeonhole::doc_support::block_on(self.commit_with_async(d)),
        }
    }
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
fn the_default_shard_count_is_one_per_physical_core() {
    let vfs = SimVfs::new(5);
    let options = Options::default()
        .vfs(Arc::clone(&vfs) as _)
        .memtable_budget(4 << 20);
    let pigeonhole::AppOwned { db, mut shards, .. } =
        Pigeonhole::open_application_owned("/db/cores.phdb", options).unwrap();
    // D220: SMT siblings count once, within the CPUs the process may use.
    assert_eq!(shards.len(), pigeonhole_io::sys::default_shards());
    // The close needs every shard's turn: drive them all until each has closed.
    let closing = std::thread::spawn(move || db.close());
    while shards.iter().any(|s| s.closed().is_none()) {
        for shard in &mut shards {
            shard.run_once(Duration::from_millis(1));
        }
    }
    closing.join().unwrap().unwrap();
}

#[test]
fn engine_owned_mode_sizes_its_threads_from_the_cpu_budget() {
    // D289 (ICR 0081): with nothing set, a thread per shard and the compaction threads from
    // the CPU budget of the machine running the test, so only their bounds are checked here
    // (the formula's tables are unit tests in `options.rs`): at least one of each, at most
    // one per shard. Explicit settings are kept as given: `shard_threads(0)` is a thread
    // per shard, `compaction_cores(0)` keeps flush and compaction on the shards.
    for shards in [2, 8, 20] {
        let vfs = SimVfs::new(5);
        let o = sim_options(&vfs).shards(shards).pin_threads(false);
        let db = Pigeonhole::open("/db/x.phdb", o).unwrap();
        let started = db.engine_metrics().threads_started;
        // A thread per shard (the load controller is opt-in), plus at least one for flush
        // and compaction.
        assert!(
            (shards as u64 + 1..=2 * shards as u64).contains(&started),
            "{shards} shards: {started} threads"
        );
        assert_eq!(db.driver_target(), Some((shards, true)));
        db.close().unwrap();
    }
    for (shards, drivers, cores, started) in [
        (2, 0, 0, 2),
        (2, 0, 3, 5),
        (8, 2, 1, 3),
        (20, 4, 4, 8),
        (20, 0, 4, 24),
    ] {
        let vfs = SimVfs::new(5);
        let o = sim_options(&vfs)
            .shards(shards)
            .pin_threads(false)
            .shard_threads(drivers)
            .compaction_cores(cores);
        let db = Pigeonhole::open("/db/x.phdb", o).unwrap();
        assert_eq!(
            db.engine_metrics().threads_started,
            started,
            "{shards} shards, shard_threads {drivers}, compaction_cores {cores}"
        );
        db.close().unwrap();
    }
}

#[test]
fn application_owned_mode() {
    let vfs = SimVfs::new(5);
    let err =
        Pigeonhole::open_application_owned("/db/app.phdb", sim_options(&vfs).compaction_cores(1))
            .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgument);

    // The default starts compaction threads only in engine-owned mode (D252): here it is 0.
    let pigeonhole::AppOwned { db, mut shards, .. } =
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

/// The balancer's spin and pool terms come from real counters (D256, ctr274's review of
/// #737: the engine tests drive them through the overrides): on real files with one
/// compaction thread, a flush runs on the pool and shows as the shard's pool busy time, and
/// the shard thread's spin after a handled commit shows as its spin time, both beside
/// `busy_nanos`, which keeps counting only the turns.
#[test]
fn spin_and_pool_time_are_counted_on_an_engine_owned_store() {
    let dir = TempDir::new("spin-pool");
    let db = Pigeonhole::open(
        dir.0.join("db.phdb"),
        Options::default()
            .shards(1)
            .compaction_cores(1)
            .memtable_budget(4 << 20)
            .pin_threads(false),
    )
    .unwrap();
    let t = table(&db);
    for i in 0..64u32 {
        t.mutate(format!("r{i:03}").as_bytes())
            .put("a", b"q", &[0x5A; 512])
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let (settled, inner) = loop {
        let m = db.engine_metrics();
        let i = db.internal_metrics();
        let (pool, spin) = (
            internal(&i, "shard_pool_busy_nanos"),
            internal(&i, "shard_spin_time_nanos"),
        );
        if pool > 0 && spin > 0 {
            break (m, i);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "pool {pool} spin {spin} after 10 s"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    };
    assert!(settled.shard_busy_nanos > 0);
    // D257: the 64 blocking commits' time in flight is counted, and summed per shard.
    let inflight = internal(&inner, "shard_commit_inflight_nanos");
    assert!(inflight > 0);
    let stats = db.shard_stats();
    assert_eq!(stats.len(), 1);
    // Two live reads: the pool (the flush's compaction) and the spinning shard thread may
    // add time between them, so the per-shard stat is at least the summed metric. Nothing
    // commits between the reads, so the time in flight is equal.
    assert!(stats[0].pool_busy_nanos >= internal(&inner, "shard_pool_busy_nanos"));
    assert!(stats[0].spin_time_nanos >= internal(&inner, "shard_spin_time_nanos"));
    assert_eq!(stats[0].commit_inflight_nanos, inflight);
    drop(t);
    db.close().unwrap();
}

#[test]
fn io_rings_report_the_backend() {
    let dir = TempDir::new("io-rings");
    let path = dir.0.join("db.phdb");
    let opts = || Options::default().shards(2).memtable_budget(4 << 20);
    let db = Pigeonhole::open(&path, opts().io_backend(IoBackend::Pread)).unwrap();
    assert_eq!(db.io_rings(), None, "pread has no rings");
    db.close().unwrap();
    #[cfg(target_os = "linux")]
    {
        let db = Pigeonhole::open(&path, opts().io_backend(IoBackend::Uring)).unwrap();
        let t = table(&db);
        t.mutate(b"r").put("a", b"q", b"v").commit().unwrap();
        // The shared ring, plus the shard threads' own; how many got pools depends on the
        // locked-memory limit and on the other tests' rings in this process.
        let rings = db.io_rings().expect("io_uring reports its rings");
        let pooled = internal(&db.internal_metrics(), "io_rings.pooled");
        assert!(
            rings.rings >= 1 && pooled <= rings.rings as u64,
            "{rings:?} {pooled}"
        );
        drop(t);
        db.close().unwrap();
    }
}

#[test]
fn the_default_backend_is_io_uring_where_the_kernel_offers_it() {
    // D218: `IoBackend::Auto`, io_uring where available and `pread` elsewhere.
    assert_eq!(IoBackend::default(), IoBackend::Auto);
    // A CI step that forces a backend through the test variable decides it there.
    if std::env::var_os("PIGEONHOLE_IO").is_some() {
        return;
    }
    let dir = TempDir::new("io-default");
    let db = Pigeonhole::open(
        dir.0.join("db.phdb"),
        Options::default().shards(1).memtable_budget(4 << 20),
    )
    .unwrap();
    // Linux CI runners offer io_uring; other systems open on `pread`, with no rings.
    if cfg!(target_os = "linux") {
        assert!(db.io_rings().is_some(), "the default did not pick io_uring");
    } else {
        assert_eq!(db.io_rings(), None);
    }
    db.close().unwrap();
}

/// ICR 0052: an owned scan keeps its selection, holds its table, and its iterator is
/// `Send + 'static`: it moves to another thread after the table handle is gone, on the
/// writer and on a reader process.
#[test]
fn an_owned_scan_outlives_its_table_handle() {
    fn send_static<T: Send + 'static>(_: &T) {}
    #[cfg(feature = "async")]
    fn _stream_send_static(s: pigeonhole::nonblocking::RowStream<'static>) -> impl Send + 'static {
        s
    }

    let vfs = SimVfs::new(52);
    let db = Pigeonhole::open("/db/owned.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    for k in [&b"k1"[..], b"k2", b"k3", b"x1"] {
        t.mutate(k)
            .put("a", b"q", k)
            .put("b", b"n", b"old")
            .commit()
            .unwrap();
    }
    let snap = db.snapshot().unwrap();
    t.mutate(b"k1").put("b", b"n", b"new").commit().unwrap();

    // The selection (prefix, family, snapshot, limit) carries over.
    let it = t
        .scan_prefix(b"k")
        .family("b")
        .snapshot(&snap)
        .limit(2)
        .into_owned()
        .iter()
        .unwrap();
    send_static(&it);
    drop(t);
    let rows = std::thread::spawn(move || {
        it.map(|r| {
            let r = r.unwrap();
            let cells: Vec<_> = r
                .view()
                .iter()
                .map(|e| (e.family.to_owned(), e.cell.value().to_vec()))
                .collect();
            (r.key().to_vec(), cells)
        })
        .collect::<Vec<_>>()
    })
    .join()
    .unwrap();
    let old = vec![("b".to_owned(), b"old".to_vec())];
    assert_eq!(
        rows,
        [(b"k1".to_vec(), old.clone()), (b"k2".to_vec(), old)],
        "two rows, family b only, as of the snapshot"
    );

    // An owned scan that is never started holds nothing open but its handle.
    let t = db.table("t").unwrap().open().unwrap();
    let unstarted = t.scan_prefix(b"").into_owned();
    drop(t);
    assert_eq!(unstarted.iter().unwrap().count(), 4);

    let reader = Pigeonhole::open_reader(
        "/db/owned.phdb",
        pigeonhole::ReaderOptions::default()
            .vfs(Arc::clone(&vfs) as _)
            .block_cache(1 << 20),
    )
    .unwrap();
    let rt = reader.table("t").unwrap();
    let it = rt.scan(&b"x"[..]..).into_owned().iter().unwrap();
    drop(rt);
    assert_eq!(std::thread::spawn(move || it.count()).join().unwrap(), 1);
    drop(reader);
    db.close().unwrap();
}

/// #826: an iterator held past `close` fails with `Closed` instead of reading on. A new
/// writer on the same file may reuse the pages the old cursor would read next (simp724's
/// repro read 1,100 more cells, then `Corruption`): borrowed and owned iterators, through
/// `next` and `next_ref`, whether or not they had started.
#[test]
fn iterators_fail_with_closed_after_close() {
    let vfs = SimVfs::new(826);
    let db = Pigeonhole::open("/db/closed.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    for i in 0..200u32 {
        t.mutate(format!("r{i:03}").as_bytes())
            .put("a", b"q", &[7u8; 200])
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    let mut started = t.scan_prefix(b"").iter().unwrap();
    assert!(started.next_ref().unwrap().is_some());
    let mut owned = t.scan_prefix(b"").into_owned().iter().unwrap();
    assert!(owned.next().unwrap().is_ok());
    let fresh = t.scan_prefix(b"").into_owned().iter().unwrap();
    db.close().unwrap();

    let e = started.next_ref().unwrap_err();
    assert_eq!(e.code(), ErrorCode::Closed);
    assert!(
        started.next_ref().unwrap().is_none(),
        "finished after the error"
    );

    // A new writer on the same file frees and rewrites what the old cursor would read.
    let db = Pigeonhole::open("/db/closed.phdb", sim_options(&vfs)).unwrap();
    let t2 = table(&db);
    for i in 0..200u32 {
        t2.mutate(format!("r{i:03}").as_bytes())
            .delete_row()
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    for i in 0..200u32 {
        t2.mutate(format!("s{i:03}").as_bytes())
            .put("a", b"q", &[9u8; 200])
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    let mut owned_errors = owned.by_ref().map(|r| r.unwrap_err().code());
    assert_eq!(owned_errors.next(), Some(ErrorCode::Closed));
    assert_eq!(owned_errors.next(), None);
    let fresh: Vec<_> = fresh.map(|r| r.map(|_| ()).map_err(|e| e.code())).collect();
    assert_eq!(fresh, [Err(ErrorCode::Closed)]);
    drop((t, t2));
    db.close().unwrap();
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
    // ICR 0047: the reader sees the catalog's families, their options and the database info.
    assert_eq!(rt.families(), t.families());
    assert_eq!(rt.family_info("a"), t.family_info("a"));
    let info = reader.info().unwrap();
    assert!(info.reader);
    assert_eq!(info.free_bytes, None);
    assert_eq!(info.shards, db.info().unwrap().shards);
    assert_eq!(info.tables[0].name, "t");
    assert_eq!(
        info.tables[0].families,
        db.info().unwrap().tables[0].families
    );
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

/// ICR 0049: a reader process backs up while the writer runs; the copy holds what the
/// reader's snapshot saw, and opens as a database of its own.
#[test]
fn a_reader_backs_up_while_the_writer_runs() {
    let vfs = SimVfs::new(49);
    let db = Pigeonhole::open("/db/r.phdb", sim_options(&vfs)).unwrap();
    let t = table(&db);
    for i in 0..50u32 {
        t.mutate(format!("r{i:02}").as_bytes())
            .put("a", b"q", b"v")
            .commit()
            .unwrap();
    }
    let reader = Pigeonhole::open_reader(
        "/db/r.phdb",
        pigeonhole::ReaderOptions::default()
            .vfs(Arc::clone(&vfs) as _)
            .block_cache(1 << 20),
    )
    .unwrap();
    reader.backup_durable("/db/copy.phdb").unwrap();
    assert_eq!(
        reader.backup("/db/copy.phdb").unwrap_err().code(),
        ErrorCode::Io,
        "dest must not exist"
    );
    t.mutate(b"later").put("a", b"q", b"v").commit().unwrap();
    drop(reader);
    drop(t);
    db.close().unwrap();
    let copy = Pigeonhole::open("/db/copy.phdb", sim_options(&vfs)).unwrap();
    let ct = copy.table("t").unwrap().open().unwrap();
    assert_eq!(ct.scan_prefix(b"r").iter().unwrap().count(), 50);
    assert!(ct.get(b"later", "a", b"q").unwrap().is_none());
    drop(ct);
    copy.close().unwrap();
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

/// A transaction begun before the database closed fails every later operation with
/// `Closed`: its reads, its row read, its async read and both commit forms. (The generation
/// check alone decides this since the separate open check went, perf-nit PR-b.)
#[test]
fn transaction_operations_after_close_fail_with_closed() {
    let db = db();
    let t = table(&db);
    t.mutate(b"r").put("a", b"q", b"v").commit().unwrap();
    let mut reads = db.transaction().unwrap();
    let commit = db.transaction().unwrap();
    let commit_async = db.transaction().unwrap();
    db.clone().close().unwrap();
    let code = |r: pigeonhole::Result<()>| r.unwrap_err().code();
    assert_eq!(
        code(reads.get(&t, b"r", "a", b"q").map(|_| ())),
        ErrorCode::Closed
    );
    assert_eq!(
        code(reads.get_row(&t, b"r", &["a"]).map(|_| ())),
        ErrorCode::Closed
    );
    assert_eq!(
        code(pigeonhole::doc_support::block_on(reads.get_async(&t, b"r", "a", b"q")).map(|_| ())),
        ErrorCode::Closed
    );
    assert_eq!(code(commit.commit().map(|_| ())), ErrorCode::Closed);
    assert_eq!(
        code(pigeonhole::doc_support::block_on(commit_async.commit_async()).map(|_| ())),
        ErrorCode::Closed
    );
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
    // `Busy`; a batch that can never fit the arena is refused at once, with its own
    // non-retryable code (issue #141).
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
    assert_eq!(err.code(), ErrorCode::BatchTooLarge);
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
    // 256 KiB segments: the inline limit is the segment payload, 256 KiB - 64 KiB. A longer
    // put is separated into a blob file at commit time (#230); a longer merge operand is
    // refused.
    let big = vec![7u8; 300_000];
    t.mutate(b"r").put("a", b"q", &big).commit().unwrap();
    assert_eq!(
        t.get(b"r", "a", b"q").unwrap().map(|c| c.value().to_vec()),
        Some(big.clone())
    );
    let mut txn = db.transaction().unwrap();
    txn.put(&t, b"r2", "a", b"q", &big);
    txn.commit().unwrap();
    let m = db
        .table("m")
        .unwrap()
        .family("a", Family::default().merge_operator("pigeonhole.i64_add"))
        .create_if_missing()
        .unwrap();
    let err = m.mutate(b"r").merge("a", b"n", &big).commit().unwrap_err();
    assert_eq!(err.code(), ErrorCode::ValueTooLarge);
    assert!(err.message().contains("value of 300000 bytes"), "{err}");
    assert!(err.message().contains(&(192 * 1024).to_string()), "{err}");
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
    assert_eq!(t.get(b"r", "a", b"n").unwrap().unwrap().as_i64(), Some(41));
    // Increments and untyped operands need a family with an operator (counters: see
    // `counters_and_typed_values`; custom operators: `registered_merge_operators_resolve_through_flush_compaction_and_reopen`).
    let mut wb = db.write_batch();
    wb.merge(&t, b"m", "a", b"n", &1i64.to_le_bytes());
    assert_eq!(wb.commit().unwrap_err().code(), ErrorCode::InvalidArgument);
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
fn shrink_never_grows_the_file_while_a_snapshot_pins_retired_space() {
    // Issue #1098: with a snapshot pinning the SSTs a compaction retired, every shrink round
    // rewrote the manifest's snapshot into new space past the end of the file (the space
    // below was pinned), and the next round planned to move it again: 1 MiB grew to 3.5 MiB.
    // Sizes are compared within a run only: with two shards' background flushes and
    // compactions, how far a shrink gets varies between runs (512 KiB or 1 MiB unpinned).
    let file = |db: &Pigeonhole| db.info().unwrap().file_bytes;
    for pin in [false, true] {
        let vfs = SimVfs::new(7092);
        let opts = Options::default()
            .vfs(Arc::clone(&vfs) as _)
            .shards(2)
            .pin_threads(false)
            .block_cache(16 << 10)
            .row_cache(0);
        let db = Pigeonhole::open("/db/s.phdb", opts).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("d", Family::default().max_versions(3))
            .create()
            .unwrap();
        for round in 0..3u32 {
            for i in 0..400u32 {
                t.mutate(format!("row{:03}", i % 40).as_bytes())
                    .put_at("d", &[(i % 4) as u8], 1, &[(i % 251) as u8; 900])
                    .commit()
                    .unwrap();
            }
            if round < 2 {
                db.flush().unwrap();
            }
        }
        db.flush().unwrap();
        db.compact().unwrap();
        db.shrink().unwrap();
        let snapshot = pin.then(|| db.snapshot().unwrap());
        for r in (0..40u32).filter(|r| ![3, 7].contains(r)) {
            t.mutate(format!("row{r:03}").as_bytes())
                .put("d", &[9], b"new")
                .commit()
                .unwrap();
        }
        db.flush().unwrap();
        db.compact().unwrap();
        let (compacted, grown) = (file(&db), db.engine_metrics().file_growths);
        db.shrink().unwrap();
        let shrunk = file(&db);
        assert!(shrunk <= compacted, "pin {pin}: {compacted} -> {shrunk}");
        if pin {
            // Not even for a while.
            assert_eq!(db.engine_metrics().file_growths, grown);
        }
        drop(snapshot);
        // Once the snapshot is gone, the next shrink may give back what the pin kept.
        db.shrink().unwrap();
        assert!(file(&db) <= shrunk, "pin {pin}: {shrunk} -> {}", file(&db));
        assert_eq!(
            value(&t, b"row000", "d", &[9]).as_deref(),
            Some(&b"new"[..])
        );
        assert_eq!(value(&t, b"row003", "d", &[3]).map(|v| v.len()), Some(900));
        drop(t);
        db.close().unwrap();
    }
}

#[test]
fn shrink_never_grows_the_file_while_a_snapshot_taken_just_before_it_is_held() {
    shrink_with_a_snapshot_taken_just_before(false);
}

#[test]
fn shrink_never_grows_the_file_while_a_reader_process_holds_a_snapshot() {
    shrink_with_a_snapshot_taken_just_before(true);
}

/// #1098: a snapshot taken just before the shrink pins nothing yet, but keeps what the
/// shrink's own first round retires. Unpinned, that round rewrites the manifest past the
/// end of the file here and gives the space back once the old extents are reclaimed; with
/// the snapshot held they are not, so the shrink must not grow the file at all. With
/// `reader`, a reader process holds the snapshot: its slot pins the view, and the writer
/// sees no seqno pin.
fn shrink_with_a_snapshot_taken_just_before(reader: bool) {
    let writer = pigeonhole_io::ProcessId {
        pid: 1,
        start_time: 1,
    };
    let reader_process = pigeonhole_io::ProcessId {
        pid: 2,
        start_time: 1,
    };
    let vfs = SimVfs::new(7092);
    vfs.enter_process(writer);
    let opts = Options::default()
        .vfs(Arc::clone(&vfs) as _)
        .shards(1)
        .pin_threads(false)
        .block_cache(16 << 10)
        .row_cache(0);
    let db = Pigeonhole::open("/db/s.phdb", opts).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("d", Family::default().max_versions(1))
        .create()
        .unwrap();
    for round in 0..3u32 {
        for i in 0..40u32 {
            t.mutate(format!("row{:04}", (i * 7 + round) % 10).as_bytes())
                .put("d", &[(i % 4) as u8], &[(i % 251) as u8; 900])
                .commit()
                .unwrap();
        }
        db.flush().unwrap();
    }
    db.compact().unwrap();
    for r in (0..10u32).filter(|r| r % 2 != 0) {
        t.mutate(format!("row{r:04}").as_bytes())
            .delete_row()
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    let (snapshot, reader_snapshot) = if reader {
        vfs.enter_process(reader_process);
        let r = Pigeonhole::open_reader(
            "/db/s.phdb",
            pigeonhole::ReaderOptions::default().vfs(Arc::clone(&vfs) as _),
        )
        .unwrap();
        let snap = r.snapshot().unwrap();
        vfs.enter_process(writer);
        (None, Some((r, snap)))
    } else {
        (Some(db.snapshot().unwrap()), None)
    };
    let file = |db: &Pigeonhole| db.info().unwrap().file_bytes;
    let (compacted, grown) = (file(&db), db.engine_metrics().file_growths);
    db.shrink().unwrap();
    assert!(file(&db) <= compacted, "{compacted} -> {}", file(&db));
    assert_eq!(db.engine_metrics().file_growths, grown);
    assert_eq!(value(&t, b"row0004", "d", &[0]).map(|v| v.len()), Some(900));
    assert_eq!(value(&t, b"row0003", "d", &[0]), None);
    drop(snapshot);
    if let Some((r, snap)) = reader_snapshot {
        vfs.enter_process(reader_process);
        drop(snap);
        drop(r);
        vfs.enter_process(writer);
    }
    drop(t);
    db.close().unwrap();
}

#[test]
fn shrink_after_deleting_every_row_leaves_a_small_file() {
    // Issue #138: with every row deleted and compacted away nothing live was left to move,
    // and shrink returned 0 without truncating: the file kept the length the load gave it,
    // also after a reopen.
    let dir = TempDir::new("shrink-all");
    let path = dir.0.join("db.phdb");
    let opts = || Options::default().shards(1);
    let len = || std::fs::metadata(&path).unwrap().len();
    let db = Pigeonhole::open(&path, opts()).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default().max_versions(1))
        .create_if_missing()
        .unwrap();
    // Incompressible 1 KiB values, as in the review's probe.
    let mut x = 88_172_645_463_325_252u64;
    for i in 0..3_000u32 {
        let mut v = [0u8; 1024];
        for b in &mut v {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        t.mutate(&i.to_be_bytes())
            .put("f", b"q", &v)
            .durability(Durability::Buffered)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    for i in 0..3_000u32 {
        t.mutate(&i.to_be_bytes())
            .delete_row()
            .durability(Durability::Buffered)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    let before = len();
    assert!(before >= 3 << 20, "the load grew the file: {before}");
    let released = db.shrink().unwrap();
    let after = len();
    assert!(released > 0 && after < before, "{before} -> {after}");
    assert!(
        after <= 1 << 20,
        "no row is left, yet the file is {after} bytes"
    );
    assert_eq!(t.scan_prefix(b"").iter().unwrap().count(), 0);
    drop(t);
    db.close().unwrap();

    let db = Pigeonhole::open(&path, opts()).unwrap();
    db.compact().unwrap();
    db.shrink().unwrap();
    db.close().unwrap();
    assert!(len() <= 1 << 20, "after a reopen: {}", len());
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

/// Drives application-owned shards on a thread. [`Driver::quiesce`] runs them until a pass
/// finds no work and holds them there (nothing in flight, no shard holding a view) until
/// [`Driver::resume`].
struct Driver {
    ctl: Arc<(std::sync::Mutex<DriverCtl>, std::sync::Condvar)>,
    thread: std::thread::JoinHandle<()>,
}

#[derive(Default)]
struct DriverCtl {
    hold: bool,
    held: bool,
    stop: bool,
}

impl Driver {
    fn start(mut shards: Vec<pigeonhole::Shard>) -> Self {
        let ctl = Arc::new((
            std::sync::Mutex::new(DriverCtl::default()),
            std::sync::Condvar::new(),
        ));
        let c = Arc::clone(&ctl);
        let thread = std::thread::spawn(move || {
            loop {
                let mut busy = false;
                for s in &mut shards {
                    busy |= s.run_once(Duration::from_millis(1));
                }
                let (m, cv) = &*c;
                let mut st = m.lock().unwrap();
                if st.stop && !busy {
                    return;
                }
                if st.hold && !busy {
                    st.held = true;
                    cv.notify_all();
                    while st.hold {
                        st = cv.wait(st).unwrap();
                    }
                    st.held = false;
                }
                drop(st);
                if !busy {
                    std::thread::yield_now();
                }
            }
        });
        Self { ctl, thread }
    }

    /// Returns once every shard is idle and held.
    fn quiesce(&self) {
        let (m, cv) = &*self.ctl;
        let mut st = m.lock().unwrap();
        st.hold = true;
        while !st.held {
            st = cv.wait(st).unwrap();
        }
    }

    fn resume(&self) {
        let (m, cv) = &*self.ctl;
        m.lock().unwrap().hold = false;
        cv.notify_all();
    }

    /// After the close: the shards finish it, then the thread ends.
    fn stop(self) {
        self.ctl.0.lock().unwrap().stop = true;
        self.thread.join().unwrap();
    }
}

#[test]
fn compact_after_drop_table_covers_the_live_tables() {
    // #83: `compact()` after `drop_table` failed with `TableNotFound` (or `Corruption`)
    // when a compaction of the dropped table was in flight (the engine test forces that
    // interleaving), and the dropped table's space could not be released.
    let vfs = SimVfs::new(83);
    let path = "/db/drop-compact.phdb";
    let file_len = || {
        use pigeonhole_io::Vfs;
        vfs.open(path.as_ref(), pigeonhole_io::OpenOptions::read())
            .unwrap()
            .len()
            .unwrap()
    };
    // Application-owned shards, driven by `Driver`: before `shrink` they are run until idle and
    // held there, so no background work (a compaction of the dropped table still finishing,
    // a shard's momentary view) keeps the dropped table's extents retired (#366: on
    // engine-owned threads that happened once in CI and left the file unshrunk).
    let pigeonhole::AppOwned { db, shards, .. } =
        Pigeonhole::open_application_owned(path, sim_options(&vfs)).unwrap();
    let driver = Driver::start(shards);
    let make = |name: &str| {
        db.table(name)
            .unwrap()
            .family("a", Family::default())
            .create_if_missing()
            .unwrap()
    };
    let keep = make("keep");
    let gone = make("gone");
    // The dropped table holds almost all of the data.
    for i in 0..400u32 {
        let row = format!("row{i:05}");
        keep.mutate(row.as_bytes())
            .put("a", b"q", &[7; 300])
            .commit()
            .unwrap();
        for j in 0..10u32 {
            gone.mutate(format!("{row}-{j}").as_bytes())
                .put("a", b"q", &[9; 1000])
                .commit()
                .unwrap();
        }
    }
    db.flush().unwrap();
    let before = file_len();
    drop(gone);
    db.drop_table("gone").unwrap();
    db.compact().unwrap();
    assert_eq!(keep.scan_prefix(b"").iter().unwrap().count(), 400);
    // Compacting again (nothing left of the dropped table) succeeds too.
    db.compact().unwrap();
    // The dropped table's SSTs were retired and reclaimed: shrink gives their space back.
    // It commits on this thread, so it runs with the shards held idle.
    driver.quiesce();
    db.shrink().unwrap();
    driver.resume();
    let after = file_len();
    assert!(after < before / 2, "{after} vs {before}");
    assert_eq!(keep.scan_prefix(b"").iter().unwrap().count(), 400);
    drop(keep);
    db.close().unwrap();
    driver.stop();

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

// ---- #43: custom merge operators ----

/// Concatenates byte values, oldest first.
#[derive(Debug)]
struct Append;

impl pigeonhole::MergeOperator for Append {
    fn name(&self) -> &str {
        "app.append"
    }
    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), pigeonhole::MergeError> {
        // Stored values: a tag byte, then the payload.
        let newer = acc.split_off(1);
        acc.extend_from_slice(&older[1..]);
        acc.extend_from_slice(&newer);
        Ok(())
    }
    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), pigeonhole::MergeError> {
        if let Some(base) = base {
            let newer = acc.split_off(1);
            acc.extend_from_slice(&base[1..]);
            acc.extend_from_slice(&newer);
        }
        Ok(())
    }
}

fn log_table(db: &Pigeonhole) -> pigeonhole::Result<Table> {
    db.table("log")?
        .family("l", Family::default().merge_operator("app.append"))
        .family("plain", Family::default())
        .create_if_missing()
}

fn log_value(t: &Table, row: &[u8]) -> pigeonhole::Result<Option<Vec<u8>>> {
    Ok(t.get(row, "l", b"q")?.map(|c| c.value().to_vec()))
}

#[test]
fn registered_merge_operators_resolve_through_flush_compaction_and_reopen() {
    let vfs = SimVfs::new(43);
    let with_append = || sim_options(&vfs).merge_operator(Arc::new(Append));
    let db = Pigeonhole::open("/db/merge.phdb", with_append()).unwrap();
    let t = log_table(&db).unwrap();
    t.mutate(b"r").put("l", b"q", b"a").commit().unwrap();
    for part in [&b"b"[..], b"c"] {
        t.mutate(b"r").merge("l", b"q", part).commit().unwrap();
    }
    t.mutate(b"r").put("plain", b"q", b"p").commit().unwrap();
    assert_eq!(log_value(&t, b"r").unwrap().as_deref(), Some(&b"abc"[..]));
    // Flushed and compacted (operands fold within a timestamp, the rest at read), then
    // more operands on top.
    db.flush().unwrap();
    db.compact().unwrap();
    t.mutate(b"r").merge("l", b"q", b"d").commit().unwrap();
    assert_eq!(log_value(&t, b"r").unwrap().as_deref(), Some(&b"abcd"[..]));
    drop(t);
    db.close().unwrap();

    // Reopened with the operator registered: the same value.
    let db = Pigeonhole::open("/db/merge.phdb", with_append()).unwrap();
    let t = log_table(&db).unwrap();
    assert_eq!(log_value(&t, b"r").unwrap().as_deref(), Some(&b"abcd"[..]));
    drop(t);
    db.close().unwrap();

    // Without it: refused at open.
    let err = Pigeonhole::open("/db/merge.phdb", sim_options(&vfs))
        .expect_err("opened without the family's merge operator");
    assert_eq!(err.code(), ErrorCode::UnknownMergeOperator, "{err}");
    // Allowed: read-only; the operator's cells fail to read, the other family reads.
    let db =
        Pigeonhole::open("/db/merge.phdb", sim_options(&vfs).read_only_fallback(true)).unwrap();
    assert!(db.info().unwrap().read_only);
    let t = db.table("log").unwrap().open().unwrap();
    let err = log_value(&t, b"r").unwrap_err();
    assert_eq!(err.code(), ErrorCode::UnknownMergeOperator, "{err}");
    assert_eq!(
        t.get(b"r", "plain", b"q")
            .unwrap()
            .map(|c| c.value().to_vec()),
        Some(b"p".to_vec())
    );
    let err = t
        .mutate(b"s")
        .put("plain", b"q", b"x")
        .commit()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ReadOnly, "{err}");
    for err in [db.flush().unwrap_err(), db.compact().unwrap_err()] {
        assert_eq!(err.code(), ErrorCode::ReadOnly, "{err}");
    }
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_family_naming_an_unregistered_operator_is_refused_at_creation() {
    let db = db();
    let err = log_table(&db).unwrap_err();
    assert_eq!(err.code(), ErrorCode::UnknownMergeOperator, "{err}");
    assert!(db.tables().is_empty());
    // `read_only_fallback` does not lift the check (ICR 0059): a writable handle never
    // creates a family the next open could not write.
    let vfs = SimVfs::new(8);
    let db = Pigeonhole::open("/db/api.phdb", sim_options(&vfs).read_only_fallback(true)).unwrap();
    let err = log_table(&db).unwrap_err();
    assert_eq!(err.code(), ErrorCode::UnknownMergeOperator, "{err}");
}

// ---- #210: the write-stall timeout ----

/// On a real clock, a writer waiting for arena room that snapshots hold gets `Busy` after
/// `Options::write_stall_timeout`, not the 30 s default, and writes go on once the
/// snapshots are released.
#[test]
fn the_write_stall_timeout_bounds_a_wait_for_room() {
    let dir = TempDir::new("stall-timeout");
    let timeout = Duration::from_millis(200);
    let options = Options::default()
        .shards(1)
        .memtable_budget(4 << 20)
        .tablet_changes(false)
        .write_stall_timeout(timeout);
    let db = Pigeonhole::open(dir.0.join("stall.phdb"), options).unwrap();
    let t = table(&db);
    let value = vec![7u8; 16 << 10];
    let mut snaps = Vec::new();
    let mut refused = None;
    for i in 0..5_000u32 {
        let started = std::time::Instant::now();
        match t
            .mutate(format!("r{i:05}").as_bytes())
            .put("a", b"q", &value)
            .commit()
        {
            Ok(_) => snaps.push(db.snapshot().unwrap()),
            Err(e) => {
                assert_eq!(e.code(), ErrorCode::Busy, "{e}");
                refused = Some(started.elapsed());
                break;
            }
        }
    }
    let waited = refused.expect("snapshots holding the arena never stalled a write");
    // A room wait's start is kept to the millisecond (`room_stamp`), so the refusal can come
    // up to 1 ms before the timeout by this clock (macOS CI: 199.85 ms of 200).
    assert!(
        waited + Duration::from_millis(1) >= timeout && waited < Duration::from_secs(10),
        "refused after {waited:?}, the timeout is {timeout:?}"
    );
    drop(snaps);
    t.mutate(b"after").put("a", b"q", b"v").commit().unwrap();
}

#[test]
fn a_registered_operator_folds_onto_a_separated_base() {
    // #33 with #43: a base above the family's blob threshold lives in a blob file once
    // flushed; operands fold onto the value it holds, not onto its pointer.
    let vfs = SimVfs::new(44);
    let db = Pigeonhole::open(
        "/db/merge-blob.phdb",
        sim_options(&vfs).merge_operator(Arc::new(Append)),
    )
    .unwrap();
    let t = db
        .table("log")
        .unwrap()
        .family(
            "l",
            Family::default()
                .merge_operator("app.append")
                .blob_threshold(100),
        )
        .create_if_missing()
        .unwrap();
    let base = vec![b'b'; 500];
    t.mutate(b"r").put("l", b"q", &base).commit().unwrap();
    db.flush().unwrap();
    t.mutate(b"r").merge("l", b"q", b"xy").commit().unwrap();
    let mut want = base.clone();
    want.extend_from_slice(b"xy");
    for stage in ["operand in a memtable", "flushed", "compacted"] {
        match stage {
            "flushed" => db.flush().unwrap(),
            "compacted" => db.compact().unwrap(),
            _ => {}
        }
        assert_eq!(
            log_value(&t, b"r").unwrap().as_deref(),
            Some(&want[..]),
            "{stage}: get"
        );
        let row = t.row(b"r").read().unwrap().unwrap();
        assert_eq!(
            row.get("l", b"q").map(|c| c.value().to_vec()).as_deref(),
            Some(&want[..]),
            "{stage}: row read"
        );
    }
    drop(t);
    db.close().unwrap();
}

/// #892: a read at a snapshot names and resolves families from the catalog as of the
/// snapshot. The catalog can only gain families today (none is dropped or renamed), so the
/// case is a family added after the snapshot: a get at the snapshot that names it reports it
/// unknown there and a row read returns nothing of it (both as before), and reads of every
/// family return, and name, only those the snapshot saw. The same for a transaction, whose snapshot is taken at `begin`.
#[test]
fn reads_at_a_snapshot_name_the_families_the_snapshot_saw() {
    let db = db();
    let t = db
        .table("s")
        .unwrap()
        .family("a", Family::default())
        .create_if_missing()
        .unwrap();
    t.mutate(b"r").put("a", b"q", b"1").commit().unwrap();
    let snap = db.snapshot().unwrap();
    let mut txn = db.transaction().unwrap();
    let t = db
        .table("s")
        .unwrap()
        .family("late", Family::default())
        .create_if_missing()
        .unwrap();
    t.mutate(b"r").put("late", b"q", b"2").commit().unwrap();
    let families = |r: Option<pigeonhole::RowRef<'_>>| -> Vec<String> {
        r.map(|r| r.iter().map(|e| e.family.to_owned()).collect())
            .unwrap_or_default()
    };

    // Named: a get reports the family unknown at the snapshot (the engine's check, as
    // before); a row read resolves it and returns nothing of it.
    assert_eq!(
        t.get_at(&snap, b"r", "late", b"q").unwrap_err().code(),
        ErrorCode::FamilyNotFound
    );
    assert_eq!(
        families(t.row(b"r").snapshot(&snap).family("late").read().unwrap()),
        Vec::<String>::new()
    );
    // Every family: only those the snapshot saw.
    assert_eq!(families(t.row(b"r").snapshot(&snap).read().unwrap()), ["a"]);
    let scanned: Vec<String> = t
        .scan_prefix(b"r")
        .snapshot(&snap)
        .iter()
        .unwrap()
        .map(|row| row.unwrap())
        .flat_map(|row| {
            (0..row.len())
                .filter_map(|i| row.entry(i).map(|e| e.family.to_owned()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(scanned, ["a"]);
    // As of now, both.
    assert_eq!(families(t.row(b"r").read().unwrap()), ["a", "late"]);

    // The transaction began before the family was added.
    assert_eq!(
        txn.get(&t, b"r", "late", b"q").unwrap_err().code(),
        ErrorCode::FamilyNotFound
    );
    let row = txn
        .get_row(&t, b"r", &[])
        .unwrap()
        .map(|r| r.iter().map(|e| e.family.to_owned()).collect::<Vec<_>>());
    assert_eq!(row, Some(vec!["a".to_owned()]));
    drop(txn);
    drop(t);
    db.close().unwrap();
}

/// A counter from `Pigeonhole::internal_metrics` by name (ICR 0087); 0 if absent.
fn internal(v: &[(&'static str, u64)], name: &str) -> u64 {
    v.iter().find(|(n, _)| *n == name).map_or(0, |&(_, x)| x)
}
