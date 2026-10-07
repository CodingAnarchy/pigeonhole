use pigeonhole::{Family, Options, Pigeonhole};
use std::time::Instant;
fn main() {
    let budget: u64 = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(64);
    let dir = std::env::temp_dir().join(format!("phb-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Pigeonhole::open(dir.join("a.phdb"), Options::default().shards(1).memtable_budget(budget<<20)).unwrap();
    let t = db.table("t").unwrap().family("f", Family::default()).create_if_missing().unwrap();
    let v = vec![3u8; 64<<10];
    for mib in [8u32, 16, 24, 30, 40, 50, 60, 70, 100] {
        let mut wb = db.write_batch();
        for i in 0..(mib*16) { wb.put(&t, &i.to_be_bytes(), "f", b"q", &v); }
        let t0 = Instant::now();
        match wb.commit() { Ok(_) => println!("budget {budget} MiB: {mib} MiB batch ok {:?}", t0.elapsed()), Err(e) => println!("budget {budget} MiB: {mib} MiB batch {:?} after {:?}", e.code(), t0.elapsed()) }
    }
    drop(t); db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
