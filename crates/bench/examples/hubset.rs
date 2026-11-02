//! Common neighbours of two hubs (D298 #488 PR 5b, perf287's graph case on #489): two set
//! cells of `N` random elements each, out of `UNIVERSE` node ids, read with two gets and
//! intersected with `SetRef::intersection_len`. perf287 measured this at 308 ms with one
//! qualifier per edge, and estimated 2 ms or less with roaring cells.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example hubset -- [N] [UNIVERSE] [READS] [DIR]
//! ```
//!
//! Defaults: 2,000,000 elements of 20,000,000, 200 reads. Each set is about 2.5 MB, past the
//! default blob threshold, so it lives in a blob file after the flush. Prints one line: the
//! set size in bytes, the p50 and p99 of the two gets alone, and of the two gets plus the
//! intersection, in µs, and the intersection's size.

use std::time::Instant;

use pigeonhole::{Aggregation, Family, Options, Pigeonhole};

fn percentile(sorted: &[u64], p: f64) -> f64 {
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i] as f64 / 1_000.0
}

/// `n` distinct pseudo-random ids below `universe`, sorted.
fn random_set(seed: u64, n: usize, universe: u32) -> Vec<u32> {
    let mut x = seed | 1;
    let mut set = std::collections::BTreeSet::new();
    while set.len() < n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        set.insert((x % u64::from(universe)) as u32);
    }
    set.into_iter().collect()
}

fn main() -> pigeonhole::Result<()> {
    let mut args = std::env::args().skip(1);
    let n: usize = args.next().map_or(2_000_000, |s| s.parse().expect("N"));
    let universe: u32 = args
        .next()
        .map_or(20_000_000, |s| s.parse().expect("UNIVERSE"));
    let reads: usize = args.next().map_or(200, |s| s.parse().expect("READS"));
    let base = args
        .next()
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let dir = base.join(format!("phdb-hubset-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let db = Pigeonhole::open(dir.join("hubset.phdb"), Options::default().shards(1))?;
    let t = db
        .table("graph")?
        .family("out", Family::aggregate(Aggregation::Set))
        .create_if_missing()?;
    let mut size = 0;
    for (row, seed) in [
        (b"hub-a", 0x9e37_79b9_7f4a_7c15u64),
        (b"hub-b", 0x2545_f491_4f6c_dd1d),
    ] {
        let mut bytes = Vec::new();
        pigeonhole_format::roaring::encode_sorted(&mut bytes, random_set(seed, n, universe));
        size = bytes.len();
        t.mutate(row).put("out", b"", &bytes).commit()?;
    }
    db.flush()?;

    let (mut gets, mut both) = (Vec::with_capacity(reads), Vec::with_capacity(reads));
    let mut common = 0;
    for _ in 0..reads {
        let t0 = Instant::now();
        let a = t.get(b"hub-a", "out", b"")?.expect("hub-a");
        let b = t.get(b"hub-b", "out", b"")?.expect("hub-b");
        let t1 = Instant::now();
        common = a
            .as_set()
            .expect("a set")
            .intersection_len(&b.as_set().expect("a set"));
        let t2 = Instant::now();
        gets.push((t1 - t0).as_nanos() as u64);
        both.push((t2 - t0).as_nanos() as u64);
    }
    drop(t);
    db.close()?;
    let _ = std::fs::remove_dir_all(&dir);
    gets.sort_unstable();
    both.sort_unstable();
    println!(
        "hubset\tn={n}\tuniverse={universe}\tset_bytes={size}\treads={reads}\t\
         gets p50={:.1} p99={:.1}\tgets+intersection p50={:.1} p99={:.1}\tcommon={common}",
        percentile(&gets, 0.5),
        percentile(&gets, 0.99),
        percentile(&both, 0.5),
        percentile(&both, 0.99),
    );
    Ok(())
}
