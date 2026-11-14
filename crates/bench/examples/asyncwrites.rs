//! The async forms of the returning commits (#1828), one shape per run, for
//! `scripts/instructions-per-cell.sh`. A binary of its own, not shapes in `writepath.rs`:
//! adding them there moved `writepath`'s code layout, and `commit-incr-returning` by about
//! 50 instructions (+0.3%), with no change to the engine.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example asyncwrites -- SHAPE ITERATIONS DIR
//! ```
//!
//! | Shape | One iteration | Units (`units N` on stderr) |
//! |---|---|---|
//! | `commit-put-previous-async` | 500 commits of one 100-byte cell over one of 2,000 existing rows, through `commit_returning_async`, each asking for the cell's previous value and awaited (`writepath.rs`' `commit-put-previous`, async) | commits |
//! | `commit-if-returning-async` | the same commits through `commit_if_returning_async` on the cell existing, which always holds, each awaited (`commit-if-returning`, async) | commits |
//!
//! As in `writepath.rs`: the shards are application-owned (`support/shards.rs`), so the
//! shard's side of each commit is counted along with the caller's, and only the measured work
//! runs inside functions named `shape_*`, under a `Measured` guard.
#[path = "support/measure.rs"]
mod measure;
#[path = "support/shards.rs"]
mod shards;
use measure::Measured;
use shards::Shards;

use pigeonhole::{Family, Table};

const VALUE: [u8; 100] = [7; 100];

fn main() {
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let usage = "usage: asyncwrites SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let dir = base.join(format!("phdb-asyncwrites-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let (db, shards) = Shards::open(&dir.join("w.phdb"), None);
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    // `SHAPE_SETUP_ONLY=1`: the setup alone, without the measured work.
    let setup_only = std::env::var_os("SHAPE_SETUP_ONLY").is_some();
    let measure = |f: &mut (dyn FnMut() + Send)| {
        if !setup_only {
            shards.measure(f);
        }
    };
    // A fixed set of rows, written first; the measured commits overwrite them in a scattered
    // order, as `writepath.rs`' commit-overwrite does.
    let set: Vec<Vec<u8>> = (0..2_000)
        .map(|i| format!("ow:{i:010}").into_bytes())
        .collect();
    for row in &set {
        t.mutate(row).put("f", b"q", &VALUE).commit().unwrap();
    }
    let order: Vec<Vec<u8>> = (0..500 * iters)
        .map(|i| set[i * 7_919 % set.len()].clone())
        .collect();
    match shape.as_str() {
        "commit-put-previous-async" => measure(&mut || shape_commit_put_previous_async(&t, &order)),
        "commit-if-returning-async" => measure(&mut || shape_commit_if_returning_async(&t, &order)),
        other => {
            panic!("unknown shape {other}: commit-put-previous-async or commit-if-returning-async")
        }
    }
    eprintln!("units {}", if setup_only { 0 } else { order.len() });
    drop(t);
    db.close().unwrap();
    shards.join();
    std::fs::remove_dir_all(&dir).ok();
}

#[inline(never)]
fn shape_commit_put_previous_async(t: &Table, rows: &[Vec<u8>]) {
    let _measured = Measured::start();
    let mut n = 0usize;
    for row in rows {
        let r = pigeonhole::doc_support::block_on(
            t.mutate(row)
                .put("f", b"q", &VALUE)
                .return_previous("f", b"q")
                .commit_returning_async(),
        )
        .unwrap();
        n += r
            .previous("f", b"q")
            .unwrap()
            .map_or(0, |c| c.value().len());
    }
    std::hint::black_box(n);
}

#[inline(never)]
fn shape_commit_if_returning_async(t: &Table, rows: &[Vec<u8>]) {
    let exists = pigeonhole::Condition::Exists {
        family: "f".into(),
        qualifier: b"q".to_vec(),
    };
    let _measured = Measured::start();
    let mut n = 0usize;
    for row in rows {
        match pigeonhole::doc_support::block_on(
            t.mutate(row)
                .put("f", b"q", &VALUE)
                .commit_if_returning_async(&exists),
        )
        .unwrap()
        {
            pigeonhole::Checked::Applied(r) => {
                n += r.current("f", b"q").unwrap().map_or(0, |c| c.value().len());
            }
            pigeonhole::Checked::NotApplied(_) => unreachable!("every row was written"),
        }
    }
    std::hint::black_box(n);
}
