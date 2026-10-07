use pigeonhole::{Family, Options, Pigeonhole, ErrorCode};
use std::time::Instant;
fn du(dir: &std::path::Path) {
    let out = std::process::Command::new("sh").arg("-c").arg(format!("ls -la {0}; du -sh {0}; du -sk {0}/*", dir.display())).output().unwrap();
    println!("{}", String::from_utf8_lossy(&out.stdout));
}
fn rss() { let out = std::process::Command::new("ps").args(["-o","rss=","-p",&std::process::id().to_string()]).output().unwrap(); println!("RSS KiB {}", String::from_utf8_lossy(&out.stdout).trim()); }
fn main() {
    let shards: usize = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("phfp-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("a.phdb");
    rss();
    let t0 = Instant::now();
    let db = Pigeonhole::open(&p, Options::default().shards(shards)).unwrap();
    println!("open took {:?}", t0.elapsed()); rss();
    let t = db.table("t").unwrap().family("f", Family::default()).create_if_missing().unwrap();
    t.mutate(b"r").put("f", b"q", b"v").commit().unwrap();
    du(&dir);
    // value limits
    let big = vec![0u8; 33<<20];
    match t.mutate(b"r").put("f", b"q", &big).commit() { Ok(_) => println!("33MiB ok"), Err(e) => println!("33MiB err {:?} {}", e.code(), e.message()) }
    let big = vec![0u8; 31<<20];
    match t.mutate(b"r").put("f", b"q", &big).commit() { Ok(_) => println!("31MiB ok"), Err(e) => println!("31MiB err {:?} {}", e.code(), e.message()) }
    let k = vec![1u8; 70000];
    match t.mutate(&k).put("f", b"q", b"v").commit() { Ok(_) => println!("70k key ok"), Err(e) => println!("70k key err {:?} {}", e.code(), e.message()) }
    match t.mutate(b"r").put("f", &k, b"v").commit() { Ok(_) => println!("70k qual ok"), Err(e) => println!("70k qual err {:?} {}", e.code(), e.message()) }
    // batch of many 1MiB values > arena
    let mut wb = db.write_batch();
    let v = vec![2u8; 1<<20];
    for i in 0..80u32 { wb.put(&t, &i.to_be_bytes(), "f", b"q", &v); }
    let t1 = Instant::now();
    match wb.commit() { Ok(_) => println!("80MiB batch ok"), Err(e) => println!("80MiB batch err {:?} {} after {:?}", e.code(), e.message(), t1.elapsed()) }
    db.flush().unwrap();
    du(&dir);
    let t2 = Instant::now();
    db.close().unwrap();
    println!("close took {:?}", t2.elapsed());
    du(&dir);
    let _ = ErrorCode::Busy;
    let _ = std::fs::remove_dir_all(&dir);
}
