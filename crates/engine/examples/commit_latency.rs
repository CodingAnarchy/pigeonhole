//! Prints the mean latency of single-shard commits per durability level (a sanity check for
//! the criterion numbers). Run with `cargo run --release -p pigeonhole-engine --example
//! commit_latency`.

use std::time::Instant;

use pigeonhole_engine::{Durability, Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_io::VfsRef;
use pigeonhole_io::pread::PreadVfs;

fn main() {
    let dir =
        std::env::temp_dir().join(format!("pigeonhole-engine-latency-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let vfs: VfsRef = PreadVfs::new(2);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 256 << 20;
    o.memtable_freeze_bytes = 32 << 20;
    let db = Engine::open(&dir.join("data.phdb"), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let mut i = 0u64;
    for (d, n) in [
        (Durability::None, 50_000u64),
        (Durability::Buffered, 50_000),
        (Durability::GroupSync, 200),
        (Durability::Sync, 200),
    ] {
        let start = Instant::now();
        for _ in 0..n {
            i += 1;
            let mut wb = WriteBatch::new();
            wb.put(
                t.id,
                f,
                &i.to_be_bytes(),
                b"q",
                None,
                ValueRef::Bytes(b"value-of-32-bytes-padding-......"),
            )
            .unwrap();
            db.commit(wb, Some(d)).unwrap();
        }
        let per = start.elapsed() / n as u32;
        println!("{d:?}: {per:?} per commit ({n} commits)");
    }
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
