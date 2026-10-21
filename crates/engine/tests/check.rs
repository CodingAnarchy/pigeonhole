//! The offline check (ICR 0048): clean on a healthy database, and every structure it covers
//! reported when bytes in it are damaged (#692: "phdb check detects injected corruption").

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    CheckOptions, CheckReport, Engine, Error, ExtentKind, FamilyOptions, Severity, Structure,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{FileRef, OpenOptions, Vfs};

const DB: &str = "/db/data.phdb";

fn options(vfs: &Arc<SimVfs>) -> pigeonhole_engine::EngineOptions {
    common::options(Arc::clone(vfs), 1, 4 << 20)
}

/// A database with SSTs (two flushes) and a blob file (values over the family's 100-byte
/// threshold), closed cleanly unless `crash`: then more commits stay in the WAL only and
/// the process dies.
fn build(seed: u64, crash: bool) -> Arc<SimVfs> {
    let vfs = SimVfs::new(seed);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let t = db
        .create_table(
            "t",
            &[("f".into(), FamilyOptions::default().blob_threshold(100))],
        )
        .unwrap();
    let f = t.families[0].id;
    for round in 0..2u32 {
        let mut wb = WriteBatch::new();
        for i in 0..200u32 {
            let value = if i % 10 == 0 {
                vec![b'b'; 300]
            } else {
                vec![b's'; 16]
            };
            let row = format!("r{round}-{i:04}");
            wb.put(t.id, f, row.as_bytes(), b"q", None, ValueRef::Bytes(&value))
                .unwrap();
        }
        db.commit(wb, Some(Durability::Sync)).unwrap();
        db.flush().unwrap();
    }
    if crash {
        for i in 0..40u32 {
            let mut wb = WriteBatch::new();
            let row = format!("w-{i:04}");
            wb.put(
                t.id,
                f,
                row.as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(b"in the wal"),
            )
            .unwrap();
            db.commit(wb, Some(Durability::Sync)).unwrap();
        }
        vfs.crash(CrashKind::Process);
        drop(db);
    } else {
        db.close().unwrap();
        drop(db);
    }
    vfs
}

fn check(vfs: &Arc<SimVfs>, deep: bool) -> CheckReport {
    let v: pigeonhole_io::VfsRef = Arc::clone(vfs) as _;
    Engine::check(&v, Path::new(DB), &CheckOptions::default().deep(deep)).unwrap()
}

fn file(vfs: &SimVfs, path: &str) -> FileRef {
    let mut opts = OpenOptions::read();
    opts.write = true;
    vfs.open(Path::new(path), opts).unwrap()
}

/// Flips every bit of the byte at `offset` of `path`.
fn flip(vfs: &SimVfs, path: &str, offset: u64) {
    let f = file(vfs, path);
    let mut b = [0u8; 1];
    f.read_at(&mut b, offset).unwrap();
    b[0] ^= 0xff;
    f.write_at(&b, offset).unwrap();
    f.sync_all().unwrap();
}

/// The corruption findings, asserting there is one about `structure`.
fn corrupt_in(report: &CheckReport, structure: Structure) {
    assert!(
        !report.is_ok(),
        "no corruption found: {:#?}",
        report.findings
    );
    assert!(
        report
            .findings
            .iter()
            .any(|f| f.structure == structure && f.severity == Severity::Corruption),
        "no {structure:?} corruption: {:#?}",
        report.findings
    );
}

fn extent(report: &CheckReport, pick: impl Fn(&ExtentKind) -> bool) -> (u64, u64, ExtentKind) {
    let e = report
        .extents
        .iter()
        .find(|e| pick(&e.kind))
        .unwrap_or_else(|| panic!("no such extent: {:#?}", report.extents));
    (e.offset, e.len, e.kind.clone())
}

#[test]
fn a_healthy_database_is_clean() {
    let vfs = build(1, false);
    for deep in [false, true] {
        let r = check(&vfs, deep);
        assert!(r.findings.is_empty(), "{:#?}", r.findings);
        assert!(r.clean_shutdown);
        assert_eq!(r.counts.superblocks, 2);
        assert_eq!(r.counts.tables, 1);
        assert!(r.counts.ssts >= 2, "{:?}", r.counts);
        assert_eq!(r.counts.sst_entries, 400);
        assert!(r.counts.blob_files >= 1, "{:?}", r.counts);
        assert_eq!(r.counts.blob_records, 40);
        assert_eq!(r.counts.blob_pointers, if deep { 40 } else { 0 });
        assert!(r.wal.is_empty(), "a clean close removes the sidecars");
        let kinds: Vec<_> = r.extents.iter().map(|e| &e.kind).collect();
        assert!(
            kinds
                .iter()
                .any(|k| matches!(k, ExtentKind::ManifestSnapshot))
        );
        assert!(kinds.iter().any(
            |k| matches!(k, ExtentKind::Sst { table, family, .. } if table == "t" && family == "f")
        ));
        assert!(kinds.iter().any(|k| matches!(k, ExtentKind::Blob { .. })));
        // By offset, superblocks first.
        assert!(r.extents.windows(2).all(|w| w[0].offset <= w[1].offset));
        assert!(matches!(
            r.extents[0].kind,
            ExtentKind::Superblock { slot: 0 }
        ));
    }
}

#[test]
fn a_crashed_database_with_a_wal_is_clean() {
    let vfs = build(2, true);
    let r = check(&vfs, true);
    assert!(r.is_ok(), "{:#?}", r.findings);
    assert!(!r.clean_shutdown);
    assert_eq!(r.wal.len(), 1, "{:#?}", r.wal);
    assert!(r.counts.wal_records >= 40, "{:?}", r.counts);
    // Nothing was replayed or written: the database opens and recovers every commit.
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let t = db.table("t").unwrap();
    let snap = db.snapshot().unwrap();
    for i in 0..40u32 {
        let row = format!("w-{i:04}");
        assert!(
            db.get(&snap, t.id, t.families[0].id, row.as_bytes(), b"q")
                .unwrap()
                .is_some(),
            "{row}"
        );
    }
    drop(snap);
    db.close().unwrap();
}

#[test]
fn a_live_writer_refuses_the_check() {
    let vfs = SimVfs::new(3);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let v: pigeonhole_io::VfsRef = Arc::clone(&vfs) as _;
    let e = Engine::check(&v, Path::new(DB), &CheckOptions::default()).unwrap_err();
    assert!(matches!(e, Error::WriterLocked), "{e:?}");
    db.close().unwrap();
}

#[test]
fn a_damaged_superblock_slot_is_reported() {
    let vfs = build(4, false);
    // Inside slot B's checksummed bytes (the sequence field).
    flip(&vfs, DB, 4096 + 16);
    let r = check(&vfs, false);
    corrupt_in(&r, Structure::Superblock);
    // The other slot still names the manifest, so the rest is checked.
    assert!(r.counts.ssts >= 2);
}

#[test]
fn a_damaged_manifest_is_reported() {
    let vfs = build(5, false);
    let (offset, ..) = extent(&check(&vfs, false), |k| {
        matches!(k, ExtentKind::ManifestSnapshot)
    });
    flip(&vfs, DB, offset + 80);
    corrupt_in(&check(&vfs, false), Structure::Manifest);
}

#[test]
fn a_damaged_sst_data_block_is_reported() {
    let vfs = build(6, false);
    let (offset, ..) = extent(&check(&vfs, false), |k| matches!(k, ExtentKind::Sst { .. }));
    flip(&vfs, DB, offset + 20);
    corrupt_in(&check(&vfs, false), Structure::Sst);
}

#[test]
fn a_damaged_sst_footer_is_reported() {
    let vfs = build(7, false);
    let (offset, _, kind) = extent(&check(&vfs, false), |k| matches!(k, ExtentKind::Sst { .. }));
    let ExtentKind::Sst { used, .. } = kind else {
        unreachable!()
    };
    flip(&vfs, DB, offset + used - 30);
    corrupt_in(&check(&vfs, false), Structure::Sst);
}

#[test]
fn a_damaged_blob_record_is_reported() {
    let vfs = build(8, false);
    let (offset, ..) = extent(&check(&vfs, false), |k| {
        matches!(k, ExtentKind::Blob { .. })
    });
    // Past the extent header (64 bytes) and the first record's header (16): its value.
    flip(&vfs, DB, offset + 64 + 16 + 5);
    corrupt_in(&check(&vfs, false), Structure::Blob);
}

#[test]
fn a_damaged_blob_extent_header_is_reported() {
    let vfs = build(9, false);
    let (offset, ..) = extent(&check(&vfs, false), |k| {
        matches!(k, ExtentKind::Blob { .. })
    });
    flip(&vfs, DB, offset + 3);
    corrupt_in(&check(&vfs, false), Structure::Blob);
}

#[test]
fn a_truncated_file_is_reported() {
    let vfs = build(10, false);
    let r = check(&vfs, false);
    let last = r.extents.iter().map(|e| e.offset + e.len).max().unwrap();
    let f = file(&vfs, DB);
    f.set_len(last - 64 * 1024).unwrap();
    f.sync_all().unwrap();
    let r = check(&vfs, false);
    corrupt_in(&r, Structure::Allocation);
}

#[test]
fn damage_inside_the_wal_is_reported() {
    let vfs = build(11, true);
    let r = check(&vfs, false);
    let w = &r.wal[0];
    let wal = w.path.to_str().unwrap().to_owned();
    // The log ends in the segment holding the commits after the last flush. Damage the
    // payload of the first record replay reads there: valid records follow it.
    let slot = w.end_slot.expect("a log");
    let from = if w.checkpoint.0 == w.end.0 {
        u64::from(w.checkpoint.1).max(32 * 1024)
    } else {
        32 * 1024
    };
    assert!(w.end.1 > from + 200, "{w:#?}");
    flip(&vfs, &wal, slot * w.segment_size + from + 12 + 4);
    let r = check(&vfs, false);
    corrupt_in(&r, Structure::Wal);
}

/// No crash leaves anything the check calls corruption: crashed at random points of a
/// workload (commits at every durability, flushes, separated values), by a process kill
/// and by a power loss, a database checks clean and then opens.
#[test]
fn crashes_never_look_like_corruption() {
    let mut ssts = 0;
    let mut wal_records = 0;
    for seed in 0..40u64 {
        let vfs = SimVfs::new(1000 + seed);
        // Small memtables: background flushes (and compactions) run during the workload, so
        // crashes land inside them too.
        let small = common::options(Arc::clone(&vfs), 1, 256 << 10);
        let db = Engine::open(Path::new(DB), small).unwrap();
        let t = db
            .create_table(
                "t",
                &[("f".into(), FamilyOptions::default().blob_threshold(100))],
            )
            .unwrap();
        let f = t.families[0].id;
        let n = 500 + (seed * 37) % 2000;
        for i in 0..n {
            let mut wb = WriteBatch::new();
            let value = if i % 7 == 0 {
                vec![b'b'; 200]
            } else {
                vec![b's'; 120]
            };
            let row = format!("r{:05}", (i * 7919) % 3000);
            wb.put(t.id, f, row.as_bytes(), b"q", None, ValueRef::Bytes(&value))
                .unwrap();
            let durability = match (i + seed) % 3 {
                0 => Durability::None,
                1 => Durability::Buffered,
                _ => Durability::Sync,
            };
            db.commit(wb, Some(durability)).unwrap();
            if (i + seed) % 97 == 0 {
                db.flush().unwrap();
            }
        }
        let kind = if seed % 2 == 0 {
            CrashKind::Process
        } else {
            CrashKind::Power
        };
        vfs.crash(kind);
        drop(db);
        let r = check(&vfs, true);
        ssts += r.counts.ssts;
        wal_records += r.counts.wal_records;
        let bad: Vec<_> = r
            .findings
            .iter()
            .filter(|f| f.severity != Severity::Info)
            .collect();
        assert!(bad.is_empty(), "seed {seed} ({kind:?}): {bad:#?}");
        let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
        db.close().unwrap();
        let r = check(&vfs, true);
        assert!(
            r.findings.is_empty(),
            "seed {seed} after reopen: {:#?}",
            r.findings
        );
    }
    // The crashes left both flushed SSTs and WAL records to check.
    assert!(
        ssts > 80,
        "{ssts} SSTs over 40 seeds: no background flushes"
    );
    assert!(
        wal_records > 1000,
        "{wal_records} WAL records over 40 seeds"
    );
}
