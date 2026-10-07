use pigeonhole::{Family, Options, Pigeonhole, Durability};
fn sz(p: &std::path::Path) -> u64 { std::fs::metadata(p).unwrap().len() }
fn main() {
    let mib: u32 = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(50);
    let keep_pct: u32 = std::env::args().nth(2).map(|s| s.parse().unwrap()).unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("phfp3-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("a.phdb");
    let db = Pigeonhole::open(&p, Options::default().shards(1)).unwrap();
    let t = db.table("t").unwrap().family("f", Family::default().max_versions(1)).create_if_missing().unwrap();
    let mut x: u64 = 88172645463325252;
    let n = mib*1024;
    for i in 0..n { let mut v = vec![0u8;1024]; for b in v.iter_mut() { x ^= x<<13; x^=x>>7; x^=x<<17; *b = x as u8; } t.mutate(&i.to_be_bytes()).put("f", b"q", &v).durability(Durability::Buffered).commit().unwrap(); }
    db.flush().unwrap(); db.compact().unwrap();
    println!("after load+compact: {} MiB", sz(&p)>>20);
    for i in 0..n { if i % 100 >= keep_pct { t.mutate(&i.to_be_bytes()).delete_row().durability(Durability::Buffered).commit().unwrap(); } }
    db.flush().unwrap(); db.compact().unwrap();
    println!("after delete+compact: {} MiB", sz(&p)>>20);
    let r = db.shrink().unwrap();
    println!("shrink released {} MiB; file {} KiB", r>>20, sz(&p)>>10);
    let r = db.shrink().unwrap();
    println!("shrink2 released {} KiB; file {} KiB", r>>10, sz(&p)>>10);
    let c = t.scan_prefix(b"").iter().unwrap().count();
    println!("rows left {c}");
    drop(t);
    db.close().unwrap();
    println!("closed: {} KiB", sz(&p)>>10);
    let db = Pigeonhole::open(&p, Options::default().shards(1)).unwrap();
    db.compact().unwrap();
    let r = db.shrink().unwrap();
    println!("after reopen+compact shrink released {} KiB; file {} KiB", r>>10, sz(&p)>>10);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
