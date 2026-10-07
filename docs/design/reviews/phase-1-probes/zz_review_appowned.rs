use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;
use pigeonhole_io::pread::PreadVfs;

const DRAIN_LONG: bool = true;
fn run(drain_after_close_only_until_false: bool) {
    let dir = std::env::temp_dir().join(format!("phrev-{}-{}", std::process::id(), drain_after_close_only_until_false));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("d.phdb");
    let vfs: VfsRef = PreadVfs::new(2);
    let mut o = EngineOptions::new(Arc::clone(&vfs));
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    let (db, shards) = Engine::open_application_owned(&path, o).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let ths: Vec<_> = shards.into_iter().map(|mut s| {
        let stop = stop.clone();
        thread::spawn(move || {
            let me = thread::current();
            s.set_wakeup(Box::new(move || me.unpark()));
            loop {
                let now = std::time::Instant::now();
                let _ = now;
                while s.run_once(u64::MAX) {}
                if stop.load(Ordering::Acquire) {
                    // documented: keep driving until run_once returns false, then drop.
                    let mut n = 0;
                    if !DRAIN_LONG { while s.run_once(u64::MAX) { n += 1; } } else {
                        let end = std::time::Instant::now() + Duration::from_millis(500);
                        while std::time::Instant::now() < end { if s.run_once(u64::MAX) { n += 1; } else { thread::park_timeout(Duration::from_millis(1)); } }
                    }
                    eprintln!("shard {} extra loops {}", s.index(), n);
                    return;
                }
                match s.next_deadline() {
                    Some(_) => thread::park_timeout(Duration::from_millis(1)),
                    None => thread::park(),
                }
            }
        })
    }).collect();
    let t = db.create_table("t", &[("f".into(), FamilyOptions::default())]).unwrap();
    for i in 0..2000u32 {
        let mut wb = WriteBatch::new();
        wb.put(t.id, t.families[0].id, format!("r{i:05}").as_bytes(), b"q", None, ValueRef::Bytes(&[7u8; 200])).unwrap();
        db.commit(wb, Some(Durability::Buffered)).unwrap();
    }
    db.close().unwrap();
    stop.store(true, Ordering::Release);
    for t in &ths { t.thread().unpark(); }
    for t in ths { t.join().unwrap(); }
    let pending = db.final_close_pending();
    drop(db);
    let info = Engine::inspect_manifest(&vfs, Path::new(&path)).unwrap();
    let files: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name()).collect();
    eprintln!("final_close_pending={pending} clean={} files={files:?}", info.clean);
    assert!(info.clean, "close was not clean");
}

#[test]
fn app_owned_close_real_fs() {
    for _ in 0..5 { run(true); }
}
