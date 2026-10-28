//! SCRATCH #759 (not for merge): a fully cached YCSB-shaped row read over N sources.
//! `scratch759_read <dir> <sources 1..4> <reads>`: 200k rows x 10 fields x 100 B, compacted to
//! one level; then (sources-1) rounds of one-field updates of every row, the last left in the
//! memtable and the others flushed to L0. 1 GiB block cache, no row cache, one shard.
use std::time::Instant;

use pigeonhole::{Durability, Family, Options, Pigeonhole};

const ROWS: u64 = 200_000;

fn key(i: u64) -> Vec<u8> {
    format!(
        "user{:016}",
        i.wrapping_mul(0x9E37_79B9_7F4A_7C15) % 1_000_000_000_000
    )
    .into_bytes()
}

fn main() -> pigeonhole::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&a[1]);
    let sources: u64 = a[2].parse().unwrap();
    let reads: u64 = a[3].parse().unwrap();
    // Versions of every field left in the memtable on top (one-field RMW-style updates).
    let versions: u64 = a.get(4).map_or(0, |v| v.parse().unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("s{sources}.phdb"));
    let _ = std::fs::remove_file(&path);
    let db = Pigeonhole::open(
        &path,
        Options::default()
            .durability(Durability::Buffered)
            .shards(1)
            .block_cache(1 << 30)
            .row_cache(0),
    )?;
    let t = db
        .table("t")?
        .family("y", Family::default().max_versions(1).bloom_bits(10))
        .create_if_missing()?;
    let v = [b'x'; 100];
    let quals: Vec<Vec<u8>> = (0..10).map(|f| format!("field{f}").into_bytes()).collect();
    for i in 0..ROWS {
        let mut m = t.mutate(&key(i));
        for q in &quals {
            m = m.put("y", q, &v);
        }
        m.commit()?;
    }
    db.compact()?;
    for round in 1..sources {
        for i in 0..ROWS {
            t.mutate(&key(i))
                .put("y", &quals[(round as usize) % 10], &v)
                .commit()?;
        }
        if round + 1 < sources {
            db.flush()?;
        }
    }
    for v_ in 0..versions {
        for i in 0..ROWS {
            t.mutate(&key(i))
                .put("y", &quals[(v_ as usize) % 10], &v)
                .commit()?;
        }
    }
    // Warm: every block cached.
    let mut cells = 0usize;
    for i in 0..ROWS {
        if let Some(r) = t.row(&key(i)).family("y").read()? {
            cells += r.len();
        }
    }
    assert_eq!(cells, (ROWS * 10) as usize);
    pigeonhole_engine::phases759::ON.store(true, std::sync::atomic::Ordering::Relaxed);
    let el = std::thread::scope(|sc| {
        sc.spawn(|| {
            let mut x = 0x1234_5678u64;
            let start = Instant::now();
            for _ in 0..reads {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let r = t.row(&key(x % ROWS)).family("y").read().unwrap();
                std::hint::black_box(r);
            }
            start.elapsed()
        })
        .join()
        .unwrap()
    });
    println!(
        "sources {sources} versions {versions}: {:.0} ns per read over {reads} reads",
        el.as_nanos() as f64 / reads as f64
    );
    drop(t);
    db.close()?;
    Ok(())
}
