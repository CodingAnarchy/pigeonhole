use pigeonhole::{Family, Options, Pigeonhole};
use std::time::Instant;
fn du(dir: &std::path::Path) {
    let out = std::process::Command::new("sh").arg("-c").arg(format!("ls -l {0} | head -5; ls {0} | wc -l; du -sh {0}", dir.display())).output().unwrap();
    println!("{}", String::from_utf8_lossy(&out.stdout));
}
fn main() {
    let shards: usize = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(0);
    let mode: u32 = std::env::args().nth(2).map(|s| s.parse().unwrap()).unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("phfp2-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("a.phdb");
    let t0 = Instant::now();
    let db = Pigeonhole::open(&p, Options::default().shards(shards)).unwrap();
    println!("open took {:?}", t0.elapsed());
    let t = db.table("t").unwrap().family("f", Family::default()).create_if_missing().unwrap();
    if mode == 0 {
        for i in 0..1000u32 { t.mutate(&i.to_be_bytes()).put("f", b"q", b"hello").commit().unwrap(); }
    } else {
        // random-ish incompressible 1 KiB values, total ~ mode MiB
        let mut x: u64 = 88172645463325252;
        for i in 0..(mode*1024) { let mut v = vec![0u8;1024]; for b in v.iter_mut() { x ^= x<<13; x^=x>>7; x^=x<<17; *b = x as u8; } t.mutate(&i.to_be_bytes()).put("f", b"q", &v).durability(pigeonhole::Durability::Buffered).commit().unwrap(); }
    }
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("-- open, after writes"); du(&dir);
    db.flush().unwrap();
    db.compact().unwrap();
    println!("-- after flush+compact"); du(&dir);
    let r = db.shrink().unwrap(); println!("shrink released {r}"); du(&dir);
    let t2 = Instant::now();
    db.close().unwrap();
    println!("close took {:?}", t2.elapsed());
    println!("-- closed"); du(&dir);
    let t3 = Instant::now();
    let db = Pigeonhole::open(&p, Options::default().shards(shards)).unwrap();
    println!("reopen took {:?}", t3.elapsed());
    db.close().unwrap();
    println!("-- closed again"); du(&dir);
    let _ = std::fs::remove_dir_all(&dir);
}
