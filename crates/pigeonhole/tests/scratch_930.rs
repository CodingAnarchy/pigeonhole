//! #930 scratch (not for merge): the same hot-row reads through the writer's `Table` and a
//! reader handle's `ReadTable`, in one process, no writes running.
use pigeonhole::{Options, Pigeonhole, ReaderOptions};

#[test]
fn writer_table_vs_reader_table() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("s.phdb");
    let db = Pigeonhole::open(&path, Options::default().shards(1).row_cache(0)).unwrap();
    let t = pigeonhole::doc_support::table(&db, "bench", &["ycsb"]).unwrap();
    let rows: Vec<Vec<u8>> = (0..16u32).map(|i| format!("user{i:020}").into_bytes()).collect();
    for round in 0..40u8 {
        for r in &rows {
            let mut m = t.mutate(r);
            for f in 0..10 {
                m = m.put("ycsb", format!("field{f}").as_bytes(), &[round; 100]);
            }
            m.commit().unwrap();
        }
    }
    db.flush().unwrap();
    let reader = Pigeonhole::open_reader(&path, ReaderOptions::default()).unwrap();
    let rt = reader.table("bench").unwrap();
    let n = 200_000usize;
    for pass in 0..2 {
        let start = std::time::Instant::now();
        for i in 0..n {
            assert!(t.row(&rows[i % 16]).family("ycsb").read().unwrap().is_some());
        }
        let w = start.elapsed().as_nanos() as f64 / n as f64;
        let start = std::time::Instant::now();
        for i in 0..n {
            assert!(rt.row(&rows[i % 16]).family("ycsb").read().unwrap().is_some());
        }
        let r = start.elapsed().as_nanos() as f64 / n as f64;
        eprintln!("pass {pass}: writer Table {w:.0} ns/read, reader ReadTable {r:.0} ns/read");
    }
}
