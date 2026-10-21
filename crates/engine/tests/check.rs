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

/// Flips a byte in the payload of the first record replay reads in the segment the log
/// ends in (valid records follow it), returning the WAL file's path.
fn damage_last_segment(vfs: &SimVfs, r: &CheckReport) -> String {
    let w = &r.wal[0];
    let wal = w.path.to_str().unwrap().to_owned();
    let slot = w.end_slot.expect("a log");
    let from = if w.checkpoint.0 == w.end.0 {
        u64::from(w.checkpoint.1).max(32 * 1024)
    } else {
        32 * 1024
    };
    assert!(w.end.1 > from + 200, "{w:#?}");
    flip(vfs, &wal, slot * w.segment_size + from + 12 + 4);
    wal
}

fn wal_findings(r: &CheckReport, severity: Severity) -> usize {
    r.findings
        .iter()
        .filter(|f| f.structure == Structure::Wal && f.severity == severity)
        .count()
}

/// After a crash, a gap at the end of the log looks the same as a power loss that tore the
/// last unsynced write (the WAL does not record what was synced): a warning, not damage.
#[test]
fn a_gap_at_the_end_of_a_crashed_wal_is_a_warning() {
    let vfs = build(11, true);
    damage_last_segment(&vfs, &check(&vfs, false));
    let r = check(&vfs, false);
    assert!(r.is_ok(), "{:#?}", r.findings);
    assert_eq!(wal_findings(&r, Severity::Warning), 1, "{:#?}", r.findings);
}

/// Damage inside a segment that has a successor: FORMAT §10.1 syncs a full segment before
/// its successor's header, so the successor's `prev_end` past the stop proves synced
/// records were lost, crash or not.
#[test]
fn damage_before_a_successor_segment_is_corruption() {
    let vfs = SimVfs::new(13);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    // About 700 KiB of log after the checkpoint: three 256 KiB segments.
    let value = vec![b'v'; 200];
    for i in 0..3000u32 {
        let mut wb = WriteBatch::new();
        let row = format!("w-{i:05}");
        wb.put(
            t.id,
            t.families[0].id,
            row.as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        db.commit(wb, Some(Durability::Buffered)).unwrap();
    }
    db.commit(WriteBatch::new(), Some(Durability::Sync)).ok();
    vfs.crash(CrashKind::Process);
    drop(db);
    let r = check(&vfs, false);
    assert!(r.is_ok(), "{:#?}", r.findings);
    let w = &r.wal[0];
    assert!(w.segments >= 3, "{w:#?}");
    // The segment replay starts in: the checkpoint's (epoch 0: the oldest).
    let f = file(&vfs, w.path.to_str().unwrap());
    let mut first = None;
    for slot in 0..w.len / w.segment_size {
        let mut page = [0u8; pigeonhole_format::wal::SEGMENT_HEADER_LEN];
        f.read_at(&mut page, slot * w.segment_size).unwrap();
        if let Ok(h) = pigeonhole_format::wal::SegmentHeader::decode(&page)
            && first.is_none_or(|(e, _)| h.epoch < e)
        {
            first = Some((h.epoch, slot));
        }
    }
    let (epoch, slot) = first.unwrap();
    assert_ne!(Some(slot), w.end_slot, "the log ends in a later segment");
    let from = if w.checkpoint.0 == epoch {
        u64::from(w.checkpoint.1).max(32 * 1024)
    } else {
        32 * 1024
    };
    flip(
        &vfs,
        w.path.to_str().unwrap(),
        slot * w.segment_size + from + 12 + 4,
    );
    let r = check(&vfs, false);
    corrupt_in(&r, Structure::Wal);
    assert!(
        r.findings.iter().any(|f| f.message.contains("successor")),
        "{:#?}",
        r.findings
    );
}

/// No crash leaves anything the check calls corruption: crashed at random points of a
/// workload (commits at every durability, background flushes, separated values), by a
/// process kill and by a power loss that drops unsynced writes whole, a database checks
/// clean and then opens; after a clean close, nothing at all.
#[test]
fn crashes_never_look_like_corruption() {
    let mut ssts = 0;
    let mut wal_records = 0;
    for seed in 0..40u64 {
        let vfs = SimVfs::new(1000 + seed);
        let db = crash_workload(&vfs, seed);
        let kind = if seed.is_multiple_of(2) {
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

/// The crash workload of `crashes_never_look_like_corruption`, left open for the caller to
/// crash (perf287's review of #793).
fn crash_workload(vfs: &Arc<SimVfs>, seed: u64) -> Arc<Engine> {
    let small = common::options(Arc::clone(vfs), 1, 256 << 10);
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
        if (i + seed).is_multiple_of(97) {
            db.flush().unwrap();
        }
    }
    db
}

/// Power losses that tear and reorder unsynced writes (the simulator's fault plans) leave
/// nothing the check calls corruption, also after a recovery and a second crash, and every
/// database opens. Under `torn_writes` alone, 7 of 400 seeds left a WAL stop followed by
/// valid fragments of the same epoch (seed 12 among them) at ea4fa96f.
#[test]
fn power_losses_that_tear_or_reorder_never_look_like_corruption() {
    for (name, torn, reorder, twice) in [
        ("torn", true, false, false),
        ("reorder", false, true, false),
        ("torn+reorder", true, true, false),
        ("torn+reorder, reopen, crash again", true, true, true),
    ] {
        for seed in 0..100u64 {
            let mut plan = pigeonhole_io::sim::FaultPlan::none();
            plan.torn_writes = torn;
            plan.reorder_unsynced = reorder;
            let vfs = SimVfs::with_faults(1000 + seed, plan);
            let db = crash_workload(&vfs, seed);
            vfs.crash(CrashKind::Power);
            drop(db);
            if twice {
                let db = Engine::open(Path::new(DB), options(&vfs))
                    .unwrap_or_else(|e| panic!("{name} seed {seed}: reopen: {e:?}"));
                let t = db.table("t").unwrap();
                for i in 0..50u32 {
                    let mut wb = WriteBatch::new();
                    let row = format!("x{i}");
                    wb.put(
                        t.id,
                        t.families[0].id,
                        row.as_bytes(),
                        b"q",
                        None,
                        ValueRef::Bytes(b"v"),
                    )
                    .unwrap();
                    db.commit(wb, Some(Durability::Sync)).unwrap();
                }
                vfs.crash(CrashKind::Process);
                drop(db);
            }
            let r = check(&vfs, false);
            let bad: Vec<_> = r
                .findings
                .iter()
                .filter(|f| f.severity == Severity::Corruption)
                .collect();
            assert!(bad.is_empty(), "{name} seed {seed}: {bad:#?}");
            Engine::open(Path::new(DB), options(&vfs))
                .unwrap_or_else(|e| panic!("{name} seed {seed}: open after the check: {e:?}"))
                .close()
                .unwrap_or_else(|e| panic!("{name} seed {seed}: close: {e:?}"));
        }
    }
}

/// A file a newer build wrote isn't damaged: the check refuses it with `UnsupportedFormat`
/// instead of reporting two damaged superblock slots (perf287's review of #793).
#[test]
fn a_newer_format_is_refused_not_reported() {
    use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
    let vfs = build(14, false);
    let f = file(&vfs, DB);
    for p in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        let mut page = [0u8; 4096];
        f.read_at(&mut page, p * 4096).unwrap();
        let mut sb = Superblock::decode(&page).unwrap();
        sb.version = pigeonhole_format::FormatVersion(99);
        sb.encode(&mut page);
        f.write_at(&page, p * 4096).unwrap();
    }
    f.sync_all().unwrap();
    let v: pigeonhole_io::VfsRef = Arc::clone(&vfs) as _;
    let e = Engine::check(&v, Path::new(DB), &CheckOptions::default()).unwrap_err();
    assert!(matches!(e, Error::UnsupportedFormat(99)), "{e:?}");
}

/// An SST with blob pointers but no recorded references (no release writes one; the engine
/// treats it as pointing anywhere) is a warning, not damage (perf287's review of #793).
#[test]
fn unrecorded_blob_references_are_a_warning() {
    let vfs = SimVfs::new(15);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    db.inject(pigeonhole_engine::Fault::OmitBlobRefs, true);
    let t = db
        .create_table(
            "t",
            &[("f".into(), FamilyOptions::default().blob_threshold(100))],
        )
        .unwrap();
    let mut wb = WriteBatch::new();
    for i in 0..20u32 {
        let row = format!("r{i:03}");
        wb.put(
            t.id,
            t.families[0].id,
            row.as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[b'b'; 300]),
        )
        .unwrap();
    }
    db.commit(wb, Some(Durability::Sync)).unwrap();
    db.flush().unwrap();
    db.close().unwrap();
    drop(db);
    let r = check(&vfs, false);
    assert!(r.is_ok(), "{:#?}", r.findings);
    assert!(
        r.findings
            .iter()
            .any(|f| f.structure == Structure::Sst && f.severity == Severity::Warning),
        "{:#?}",
        r.findings
    );
}

/// perf287's re-review of #793: a torn power loss in a session after a clean close. A clean
/// reopen commits no root ("the clean flag stays set until the next commit"), so the newest
/// root still says clean while the session's WAL tail is torn; that is never corruption (8
/// of 600 seeds reported it while the gap rule trusted the clean flag).
#[test]
fn a_crash_after_a_clean_reopen_never_looks_like_corruption() {
    for seed in 0..200u64 {
        let mut plan = pigeonhole_io::sim::FaultPlan::none();
        plan.torn_writes = true;
        let vfs = SimVfs::with_faults(3000 + seed, plan);
        // Session 1: create, write, close cleanly (the WAL files go).
        let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        for i in 0..20u32 {
            let mut wb = WriteBatch::new();
            let row = format!("a{i}");
            wb.put(
                t.id,
                t.families[0].id,
                row.as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(b"v"),
            )
            .unwrap();
            db.commit(wb, Some(Durability::Sync)).unwrap();
        }
        db.close().unwrap();
        drop(db);
        // Session 2: the reopen commits no root; WAL-only commits (no flush), then a power loss.
        let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
        let t = db.table("t").unwrap();
        for i in 0..(500 + (seed as u32 * 37) % 2000) {
            let mut wb = WriteBatch::new();
            let row = format!("b{i}");
            wb.put(
                t.id,
                t.families[0].id,
                row.as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&[b'x'; 120]),
            )
            .unwrap();
            let d = match (u64::from(i) + seed) % 3 {
                0 => Durability::None,
                1 => Durability::Buffered,
                _ => Durability::Sync,
            };
            db.commit(wb, Some(d)).unwrap();
        }
        vfs.crash(CrashKind::Power);
        drop(db);
        let r = check(&vfs, false);
        assert!(
            r.clean_shutdown,
            "seed {seed}: the state this test is about"
        );
        let bad: Vec<_> = r
            .findings
            .iter()
            .filter(|f| f.severity == Severity::Corruption)
            .collect();
        assert!(bad.is_empty(), "seed {seed}: {bad:#?}");
        Engine::open(Path::new(DB), options(&vfs))
            .unwrap()
            .close()
            .unwrap();
    }
}
