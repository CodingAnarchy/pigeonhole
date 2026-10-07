//! Issue #90 area 7 probes (concurrency edges). Not for merge.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, Error, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::SimVfs;

fn block<F: std::future::Future + Unpin>(mut f: F) -> F::Output {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let std::task::Poll::Ready(v) = std::pin::Pin::new(&mut f).poll(&mut cx) {
            return v;
        }
        std::thread::sleep(std::time::Duration::from_micros(100));
    }
}

const DB: &str = "/db/data.phdb";

fn owned(vfs: Arc<SimVfs>, shards: usize) -> EngineOptions {
    let mut o = common::options(vfs, shards, 4 << 20);
    o.pin_threads = false;
    o
}

fn put(wb: &mut WriteBatch, t: &pigeonhole_engine::TableInfo, row: &[u8], v: &[u8]) {
    let f = t.family("f").unwrap().id;
    wb.put(t.id, f, row, b"q", None, ValueRef::Bytes(v)).unwrap();
}

fn get(db: &Engine, snap: &pigeonhole_engine::Snapshot, t: &pigeonhole_engine::TableInfo, row: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    let f = t.family("f").unwrap().id;
    db.get(snap, t.id, f, row, b"q")
        .map(|c| c.map(|c| common::value_bytes(c.value())))
}

fn val(tag: &str, i: u32) -> Vec<u8> {
    let mut v = format!("{tag}-{i:05}-").into_bytes();
    let mut x = u64::from(i).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    while v.len() < 2000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v
}

/// P1: spec says "Writer crash and restart. Readers keep serving their current snapshot."
/// A reader snapshot taken before a writer restart names SST extents the new writer does not
/// know are pinned (the pin lives in the abandoned generation).
#[test]
fn p1_reader_snapshot_survives_writer_restart() {
    let vfs = SimVfs::new(11);
    let pw = ProcessId { pid: 1, start_time: 1 };
    let pr = ProcessId { pid: 2, start_time: 1 };
    vfs.enter_process(pw);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for round in 0..2u32 {
        let mut wb = WriteBatch::new();
        for i in 0..200u32 {
            put(&mut wb, &t, format!("row{:05}", round * 200 + i).as_bytes(), &val("old", round * 200 + i));
            if i % 20 == 19 { db.commit(std::mem::take(&mut wb), Some(Durability::Buffered)).unwrap(); }
        }
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        db.flush().unwrap();
    }
    vfs.enter_process(pr);
    let reader = Engine::open_reader(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let rt = reader.table("t").unwrap();
    let snap = reader.snapshot().unwrap();

    // Writer restarts (clean close here; a crash is the same for the pins).
    vfs.enter_process(pw);
    db.close().unwrap();
    drop(db);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db.table("t").unwrap();
    db.compact(None).unwrap();
    // New data reuses the freed extents.
    for round in 0..6u32 {
        let mut wb = WriteBatch::new();
        for i in 0..200u32 {
            put(&mut wb, &t, format!("zzz{:05}", round * 200 + i).as_bytes(), &val("NEW", round * 200 + i));
            if i % 20 == 19 { db.commit(std::mem::take(&mut wb), Some(Durability::Buffered)).unwrap(); }
        }
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        db.flush().unwrap();
    }

    vfs.enter_process(pr);
    let mut bad = Vec::new();
    for i in [0u32, 1, 150, 250, 399] {
        let r = get(&reader, &snap, &rt, format!("row{i:05}").as_bytes());
        match r {
            Ok(Some(v)) if v == val("old", i) => {}
            other => bad.push((i, other.map(|o| o.map(|v| String::from_utf8_lossy(&v[..12]).into_owned())))),
        }
    }
    eprintln!("P1 bad reads at the pre-restart snapshot: {bad:?}");
    assert!(bad.is_empty(), "reader snapshot broke across a writer restart: {bad:?}");
    drop(snap);
    reader.close().unwrap();
    vfs.enter_process(pw);
    db.close().unwrap();
}

/// P2: a drop_table that commits while a flush of the same shard is writing makes the whole
/// flush request (including other tables' SSTs) refused; `flush()` reports an error.
#[test]
fn p2_drop_table_during_flush_fails_flush() {
    let mut failures = 0;
    for seed in 0..20u64 {
        let vfs = SimVfs::new(100 + seed);
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let u = db
            .create_table("u", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let mut wb = WriteBatch::new();
        for i in 0..300u32 {
            put(&mut wb, &u, format!("u{i:05}").as_bytes(), &val("u", i));
            put(&mut wb, &t, format!("t{i:05}").as_bytes(), &val("t", i));
            if i % 10 == 9 { db.commit(std::mem::take(&mut wb), Some(Durability::Buffered)).unwrap(); }
        }
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        let pending = db.flush_pending().unwrap();
        db.drop_table(t.id).unwrap();
        match block(pending) {
            Ok(()) => {}
            Err(e) => {
                failures += 1;
                eprintln!("P2 seed {seed}: flush() failed after a concurrent drop_table: {e}");
            }
        }
        // And close: does an in-flight flush refusal make it unclean?
        let mut wb = WriteBatch::new();
        let v = db
            .create_table("v", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        for i in 0..300u32 {
            put(&mut wb, &v, format!("v{i:05}").as_bytes(), &val("v", i));
            if i % 20 == 19 { db.commit(std::mem::take(&mut wb), Some(Durability::Buffered)).unwrap(); }
        }
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        let pending = db.flush_pending().unwrap();
        db.drop_table(v.id).unwrap();
        let r = db.close();
        drop(pending);
        if let Err(e) = r {
            failures += 1;
            eprintln!("P2 seed {seed}: close() failed after drop_table racing a flush: {e}");
        }
        drop(db);
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
        let u = db.table("u").unwrap();
        let snap = db.snapshot().unwrap();
        assert_eq!(get(&db, &snap, &u, b"u00007").unwrap(), Some(val("u", 7)));
        drop(snap);
        db.close().unwrap();
    }
    assert_eq!(failures, 0, "flush/close failed after a concurrent drop_table");
}

/// P3: shrink running on another thread when close() returns keeps committing manifest
/// roots after the writer lock is released (clean flag cleared, or a new writer races it).
#[test]
fn p3_shrink_racing_close() {
    let mut after_close = 0;
    for seed in 0..10u64 {
        let vfs = SimVfs::new(300 + seed);
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
        let a = db
            .create_table("a", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let b = db
            .create_table("b", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        for round in 0..8u32 {
            let mut wb = WriteBatch::new();
            for i in 0..100u32 {
                put(&mut wb, &a, format!("a{:05}", round * 100 + i).as_bytes(), &val("a", i));
            }
            db.commit(wb, Some(Durability::Buffered)).unwrap();
            db.flush().unwrap();
            let mut wb = WriteBatch::new();
            for i in 0..100u32 {
                put(&mut wb, &b, format!("b{:05}", round * 100 + i).as_bytes(), &val("b", i));
            }
            db.commit(wb, Some(Durability::Buffered)).unwrap();
            db.flush().unwrap();
        }
        db.drop_table(a.id).unwrap();
        let db2 = Arc::clone(&db);
        let h = std::thread::spawn(move || db2.shrink());
        std::thread::sleep(std::time::Duration::from_micros(200 + seed * 100));
        let closed = db.close();
        let closed_at = std::time::Instant::now();
        let shrunk = h.join().unwrap();
        let shrink_ended_after = closed_at.elapsed();
        drop(db);
        let vfs_ref: pigeonhole_io::VfsRef = vfs.clone();
        let clean = pigeonhole_pager::Pager::open(&vfs_ref, Path::new(DB), false)
            .map(|o| o.clean_shutdown());
        eprintln!(
            "P3 seed {seed}: close={closed:?} shrink={shrunk:?} (+{shrink_ended_after:?}) clean={clean:?}"
        );
        if closed.is_ok() && matches!(clean, Ok(false)) {
            after_close += 1;
        }
    }
    assert_eq!(after_close, 0, "a manifest commit landed after a clean close");
}

/// P4: a reader process builds a view from the shared-memory view record (memtables) and a
/// catalog loaded from the durable root (SSTs). The two can come from different manifest
/// versions: a flushed memtable is then seen twice (operands double counted) or not at all.
#[test]
fn p4_reader_view_record_vs_catalog_skew() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let vfs = SimVfs::new(77);
    let pw = ProcessId { pid: 1, start_time: 1 };
    let pr = ProcessId { pid: 2, start_time: 1 };
    vfs.enter_process(pw);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let mut fo = FamilyOptions::default();
    fo.merge_operator = "pigeonhole.i64_add".to_owned();
    let t = db.create_table("t", &[("f".into(), fo)]).unwrap();
    let f = t.family("f").unwrap().id;
    let committed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (c2, s2, v2) = (Arc::clone(&committed), Arc::clone(&stop), Arc::clone(&vfs));
    let reader = std::thread::spawn(move || {
        v2.enter_process(pr);
        let reader = Engine::open_reader(Path::new(DB), owned(Arc::clone(&v2), 1)).unwrap();
        let rt = reader.table("t").unwrap();
        let mut over = 0u64;
        let mut under = 0u64;
        let mut reads = 0u64;
        let mut first = None;
        while !s2.load(Ordering::Acquire) {
            let before = c2.load(Ordering::Acquire);
            let snap = match reader.snapshot() {
                Ok(s) => s,
                Err(e) => panic!("snapshot: {e}"),
            };
            let got = match reader.get(&snap, rt.id, f, b"ctr", b"q") {
                Ok(Some(c)) => match c.value() {
                    ValueRef::I64(v) => v as u64,
                    other => panic!("{other:?}"),
                },
                Ok(None) => 0,
                Err(e) => panic!("get: {e}"),
            };
            let after = c2.load(Ordering::Acquire);
            reads += 1;
            if got > after + 1 {
                over += 1;
                first.get_or_insert(format!("over: got {got} > committed {after}"));
            }
            if got < before {
                under += 1;
                first.get_or_insert(format!("under: got {got} < committed-before-snapshot {before}"));
            }
        }
        reader.close().unwrap();
        (reads, over, under, first)
    });
    for i in 0..3000u64 {
        let mut wb = WriteBatch::new();
        wb.merge(t.id, f, b"ctr", b"q", ValueRef::I64(1)).unwrap();
        db.commit(wb, Some(Durability::None)).unwrap();
        committed.store(i + 1, Ordering::Release);
        if i % 20 == 19 {
            db.flush().unwrap();
        }
    }
    stop.store(true, Ordering::Release);
    let (reads, over, under, first) = reader.join().unwrap();
    eprintln!("P4 reads={reads} over={over} under={under} first={first:?}");
    db.close().unwrap();
    assert_eq!((over, under), (0, 0), "{first:?}");
}

/// P5: unflushed WAL records of a dropped table (single- and cross-shard) replayed after a
/// process or power crash.
#[test]
fn p5_drop_table_then_crash_replays() {
    use pigeonhole_io::sim::CrashKind;
    for kind in [CrashKind::Process, CrashKind::Power] {
        let vfs = SimVfs::new(5);
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let mut tabs = Vec::new();
        for n in ["a", "b", "c", "d"] {
            tabs.push(db.create_table(n, &[("f".into(), FamilyOptions::default())]).unwrap());
        }
        // Cross-shard commit touching all four tables (two shards).
        let mut wb = WriteBatch::new();
        for t in &tabs {
            put(&mut wb, t, b"r", b"v1");
        }
        db.commit(wb, Some(Durability::Sync)).unwrap();
        db.drop_table(tabs[0].id).unwrap();
        db.drop_table(tabs[1].id).unwrap();
        let again = db.create_table("a", &[("f".into(), FamilyOptions::default())]).unwrap();
        let mut wb = WriteBatch::new();
        put(&mut wb, &again, b"r2", b"v2");
        put(&mut wb, &tabs[2], b"r2", b"v2");
        db.commit(wb, Some(Durability::Sync)).unwrap();
        vfs.crash(kind);
        drop(db);
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let a = db.table("a").unwrap();
        let c = db.table("c").unwrap();
        let d = db.table("d").unwrap();
        assert!(db.table("b").is_none());
        let snap = db.snapshot().unwrap();
        assert_eq!(get(&db, &snap, &a, b"r").unwrap(), None, "{kind:?}: old a's row leaked into the new a");
        assert_eq!(get(&db, &snap, &a, b"r2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(get(&db, &snap, &c, b"r").unwrap(), Some(b"v1".to_vec()), "{kind:?}");
        assert_eq!(get(&db, &snap, &d, b"r").unwrap(), Some(b"v1".to_vec()), "{kind:?}");
        drop(snap);
        db.close().unwrap();
    }
}

/// P6: shrink on one thread while writers flush and compact on others.
#[test]
fn p6_shrink_racing_background_compaction() {
    let vfs = SimVfs::new(9);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db.create_table("t", &[("f".into(), FamilyOptions::default())]).unwrap();
    let junk = db.create_table("j", &[("f".into(), FamilyOptions::default())]).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (d2, s2) = (Arc::clone(&db), Arc::clone(&stop));
    let h = std::thread::spawn(move || {
        let mut errs = Vec::new();
        let mut n = 0;
        while !s2.load(std::sync::atomic::Ordering::Acquire) {
            n += 1;
            if let Err(e) = d2.shrink() {
                errs.push(e.to_string());
            }
        }
        (n, errs)
    });
    let _ = junk;
    for round in 0..60u32 {
        let junk = db.table("j").unwrap();
        let mut wb = WriteBatch::new();
        for i in 0..20u32 {
            put(&mut wb, &t, format!("row{:05}", (round * 7 + i) % 300).as_bytes(), &val("x", round * 20 + i));
            put(&mut wb, &junk, format!("j{:05}", round * 20 + i).as_bytes(), &val("j", i));
        }
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        db.flush().unwrap();
        if round % 10 == 9 {
            // free low extents so shrink has work
            let j = db.table("j").unwrap();
            db.drop_table(j.id).unwrap();
            db.create_table("j", &[("f".into(), FamilyOptions::default())]).unwrap();
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Release);
    let (n, errs) = h.join().unwrap();
    eprintln!("P6 shrinks={n} errors={}: {:?}", errs.len(), errs.iter().take(5).collect::<Vec<_>>());
    // Data still readable
    let snap = db.snapshot().unwrap();
    for r in 0..300u32 {
        let _ = get(&db, &snap, &t, format!("row{r:05}").as_bytes()).unwrap();
    }
    drop(snap);
    db.close().unwrap();
    assert!(errs.is_empty());
}

/// P2b: no explicit flush: a background flush (memtable full) is in flight when the
/// application drops a table and then closes, sequentially, on one thread.
#[test]
fn p2b_sequential_drop_then_close_with_background_flush() {
    let mut bad = 0;
    for seed in 0..20u64 {
        let vfs = SimVfs::new(500 + seed);
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
        let t = db.create_table("t", &[("f".into(), FamilyOptions::default())]).unwrap();
        let u = db.create_table("u", &[("f".into(), FamilyOptions::default())]).unwrap();
        let n = 500 + (seed as u32) * 3;
        for i in 0..n {
            let mut wb = WriteBatch::new();
            put(&mut wb, &t, format!("t{i:06}").as_bytes(), &val("t", i));
            put(&mut wb, &u, format!("u{i:06}").as_bytes(), &val("u", i));
            db.commit(wb, Some(Durability::None)).unwrap();
        }
        db.drop_table(t.id).unwrap();
        let r = db.close();
        if let Err(e) = &r { bad += 1; eprintln!("P2b seed {seed}: rows={n} close: {e}"); }
        drop(db);
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
        let u = db.table("u").unwrap();
        let snap = db.snapshot().unwrap();
        assert_eq!(get(&db, &snap, &u, b"u000003").unwrap(), Some(val("u", 3)));
        drop(snap);
        db.close().unwrap();
    }
    eprintln!("P2b unclean closes: {bad}/20");
}
