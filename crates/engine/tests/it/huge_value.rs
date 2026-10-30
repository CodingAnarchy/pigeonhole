//! One real value above 64 MiB (#230): larger than D16's inline limit with the default WAL
//! segment, and than the largest extent, so its blob record spans extents. Its own test
//! binary, with this one test, so it never runs beside others (it holds a few hundred MiB
//! of simulated file and copies of the value).

use std::path::Path;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_io::sim::SimVfs;

#[test]
#[cfg_attr(miri, ignore = "hundreds of MiB under Miri")]
fn a_value_above_64_mib_round_trips() {
    let vfs = SimVfs::new(230);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 8 << 20;
    o.memtable_freeze_bytes = 2 << 20;
    let db = Engine::open(Path::new("/db/huge.phdb"), o.clone()).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let mut value = vec![0u8; (65 << 20) + 12_345];
    for (i, b) in value.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, b"big", b"q", None, ValueRef::Bytes(&value))
        .unwrap();
    db.commit(wb, None).unwrap();
    let check = |db: &Engine| {
        let snap = db.snapshot().unwrap();
        let got = db.get(&snap, t.id, f, b"big", b"q").unwrap().unwrap();
        assert!(
            got.value() == ValueRef::Bytes(&value),
            "the value reads back"
        );
        db.check_blob_accounting().unwrap();
    };
    check(&db);
    db.flush().unwrap();
    check(&db);
    db.close().unwrap();
    let db = Engine::open(Path::new("/db/huge.phdb"), o).unwrap();
    check(&db);
    db.close().unwrap();
}
