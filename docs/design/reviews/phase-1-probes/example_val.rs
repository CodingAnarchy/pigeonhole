use pigeonhole::{Family, Options, Pigeonhole};
fn main() {
    let dir = std::env::temp_dir().join(format!("phv-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for budget in [64u64<<20, 1<<20] {
    let db = Pigeonhole::open(dir.join(format!("a{budget}.phdb")), Options::default().shards(1).memtable_budget(budget)).unwrap();
    let t = db.table("t").unwrap().family("f", Family::default()).create_if_missing().unwrap();
    let half = (budget/2) as usize;
    for sz in [half - (1<<20).min(half/2), half - (256<<10).min(half/4), half - 1024, half] {
        let v = vec![5u8; sz];
        match t.mutate(b"r").put("f", b"q", &v).commit() { Ok(_) => println!("budget {budget}: value {sz} ok"), Err(e) => println!("budget {budget}: value {sz} {:?}: {}", e.code(), e.message()) }
    }
    drop(t); db.close().unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
