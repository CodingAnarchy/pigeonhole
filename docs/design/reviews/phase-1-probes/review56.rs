mod common;
use std::path::Path;
use std::sync::Arc;
use pigeonhole_engine::{Engine, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/data.phdb";

fn wal_bytes(vfs: &Arc<SimVfs>) -> u64 {
    let mut total = 0;
    for p in vfs.list_dir(Path::new("/db")).unwrap() {
        if p.to_string_lossy().contains("wal") {
            total += vfs.open(&p, OpenOptions::read()).unwrap().len().unwrap();
        }
    }
    total
}

fn run(cold: bool) -> (u64, u64) {
    let vfs = SimVfs::new(7);
    let mut o = common::options(Arc::clone(&vfs), 1, 4 << 20);
    o.pin_threads = false;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("cold".into(), FamilyOptions::default()), ("hot".into(), FamilyOptions::default())])
        .unwrap();
    let cold_f = t.family("cold").unwrap().id;
    let hot_f = t.family("hot").unwrap().id;
    if cold {
        let mut wb = WriteBatch::new();
        wb.put(t.id, cold_f, b"r", b"q", None, ValueRef::Bytes(b"v")).unwrap();
        db.commit(wb, None).unwrap();
    }
    let v = vec![7u8; 1000];
    for i in 0..30_000u32 {
        let mut wb = WriteBatch::new();
        wb.put(t.id, hot_f, &i.to_be_bytes(), b"q", None, ValueRef::Bytes(&v)).unwrap();
        db.commit(wb, None).unwrap();
    }
    let m = db.metrics();
    let w = wal_bytes(&vfs);
    db.close().unwrap();
    (w, m.flushes)
}

#[test]
fn cold_slot_pins_wal() {
    let (a, fa) = run(false);
    let (b, fb) = run(true);
    eprintln!("no cold: wal={a} flushes={fa}; cold: wal={b} flushes={fb}");
}

use pigeonhole_format::Durability;
use pigeonhole_io::sim::FaultPlan;

fn close_hang(big_in_a: bool) -> bool {
    let vfs = SimVfs::new(11);
    let mut o = common::options(Arc::clone(&vfs), 2, 4 << 20);
    o.pin_threads = false;
    o.memtable_freeze_bytes = 64 << 10;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let fo = || vec![("f".into(), FamilyOptions::default())];
    let a = db.create_table("a", &fo()).unwrap();
    let b = db.create_table("b", &fo()).unwrap();
    let (fa, fb) = (a.family("f").unwrap().id, b.family("f").unwrap().id);
    let big = vec![1u8; 1000];
    let mut wb = WriteBatch::new();
    let (bt, bf, st, sf) = if big_in_a { (a.id, fa, b.id, fb) } else { (b.id, fb, a.id, fa) };
    for i in 0..150u32 { wb.put(bt, bf, b"r", &i.to_be_bytes(), None, ValueRef::Bytes(&big)).unwrap(); }
    wb.put(st, sf, b"r", b"q", None, ValueRef::Bytes(b"small")).unwrap();
    db.commit(wb, Some(Durability::Sync)).unwrap();
    // let the big side flush
    std::thread::sleep(std::time::Duration::from_millis(300));
    eprintln!("flushes before poison: {}", db.metrics().flushes);
    // poison the small side's shard
    { let mut p = FaultPlan::none(); p.io_error_ppm = 1_000_000; vfs.set_faults(p); }
    let mut wb = WriteBatch::new();
    wb.put(st, sf, b"r2", b"q", None, ValueRef::Bytes(b"x")).unwrap();
    let r = db.commit(wb, Some(Durability::Sync));
    vfs.set_faults(FaultPlan::none());
    eprintln!("poisoning commit: {:?}", r.as_ref().err());
    let db2 = Arc::clone(&db);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || { let r = db2.close(); let _ = tx.send(r); });
    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(r) => { eprintln!("close returned {:?}", r); false }
        Err(_) => { eprintln!("close HUNG"); true }
    }
}

#[test]
fn close_after_participant_poisoned() {
    let h1 = close_hang(true);
    let h2 = close_hang(false);
    eprintln!("hang(big_in_a)={h1} hang(big_in_b)={h2}");
}

fn close_case(cross: bool, poison: bool, shards: usize) -> bool {
    let vfs = SimVfs::new(11);
    let mut o = common::options(Arc::clone(&vfs), shards, 4 << 20);
    o.pin_threads = false;
    o.memtable_freeze_bytes = 64 << 10;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let fo = || vec![("f".into(), FamilyOptions::default())];
    let a = db.create_table("a", &fo()).unwrap();
    let b = db.create_table("b", &fo()).unwrap();
    let (fa, fb) = (a.family("f").unwrap().id, b.family("f").unwrap().id);
    let big = vec![1u8; 1000];
    let mut wb = WriteBatch::new();
    for i in 0..150u32 { wb.put(a.id, fa, b"r", &i.to_be_bytes(), None, ValueRef::Bytes(&big)).unwrap(); }
    if cross { wb.put(b.id, fb, b"r", b"q", None, ValueRef::Bytes(b"small")).unwrap(); }
    db.commit(wb, Some(Durability::Sync)).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    if poison {
        { let mut p = FaultPlan::none(); p.io_error_ppm = 1_000_000; vfs.set_faults(p); }
        let mut wb = WriteBatch::new();
        wb.put(b.id, fb, b"r2", b"q", None, ValueRef::Bytes(b"x")).unwrap();
        let _ = db.commit(wb, Some(Durability::Sync));
        vfs.set_faults(FaultPlan::none());
    }
    let db2 = Arc::clone(&db);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || { let r = db2.close(); let _ = tx.send(r); });
    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(r) => { eprintln!("cross={cross} poison={poison} shards={shards}: close returned {:?}", r.err()); false }
        Err(_) => { eprintln!("cross={cross} poison={poison} shards={shards}: close HUNG"); true }
    }
}

#[test]
fn close_controls() {
    close_case(true, false, 2);
    close_case(false, true, 2);
    close_case(false, true, 1);
    close_case(true, true, 2);
}
