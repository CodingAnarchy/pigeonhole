//! Counters in a counter family (`Family::counter`): `incr` is a blind write, so many
//! threads can count concurrently without losing updates, and a counter stays one cell;
//! `incr_at` counts into per-day buckets (one version each); `put_i64` resets a counter;
//! `commit_if` makes a compare-and-set.
//!
//! Run with `cargo run -p pigeonhole --example counters`.

use std::path::Path;
use std::thread;

use pigeonhole::{Condition, Family, Options, Pigeonhole, ValueFilter};

/// Runs the example in a temporary directory, removed afterwards.
pub fn main() -> pigeonhole::Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-example-counters-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create a temporary directory");
    let result = run(&dir.join("counters.phdb"));
    let _ = std::fs::remove_dir_all(&dir);
    result
}

fn run(path: &Path) -> pigeonhole::Result<()> {
    let db = Pigeonhole::open(path, Options::default())?;
    let pages = db
        .table("pages")?
        .family("hits", Family::counter())
        .create_if_missing()?;

    // Two days as bucket timestamps (microseconds since the epoch).
    const DAY: u64 = 86_400_000_000;
    let (oct5, oct6) = (20_366 * DAY, 20_367 * DAY);

    // Eight threads, 250 increments each, on the same counter and on per-day buckets.
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let pages = pages.clone();
            thread::spawn(move || -> pigeonhole::Result<()> {
                for i in 0..250 {
                    let day = if i % 2 == 0 { oct5 } else { oct6 };
                    pages
                        .mutate(b"com.example/a")
                        .incr("hits", b"total", 1)
                        .incr_at("hits", b"daily", day, 1)
                        .commit()?;
                }
                Ok(())
            })
        })
        .collect();
    for w in workers {
        w.join().expect("worker panicked")?;
    }

    let read = |q: &[u8]| -> pigeonhole::Result<i64> {
        Ok(pages
            .get(b"com.example/a", "hits", q)?
            .and_then(|c| c.as_i64())
            .unwrap_or(0))
    };
    println!("total = {}", read(b"total")?);
    assert_eq!(read(b"total")?, 2000);
    // Every bucket is a version of the `daily` column, newest first.
    let daily = pages
        .row(b"com.example/a")
        .qualifier_prefix(b"daily")
        .versions(0)
        .read()?
        .expect("the row exists");
    let days: Vec<(u64, i64)> = daily
        .iter()
        .map(|e| (e.cell.timestamp(), e.cell.as_i64().unwrap_or(0)))
        .collect();
    println!("daily = {days:?}");
    assert_eq!(days, [(oct6, 1000), (oct5, 1000)]);

    // Reset the base, then keep counting.
    pages
        .mutate(b"com.example/a")
        .put_i64("hits", b"total", 0)
        .commit()?;
    pages
        .mutate(b"com.example/a")
        .incr("hits", b"total", 5)
        .commit()?;
    assert_eq!(read(b"total")?, 5);

    // Compare-and-set: reset only if the counter passed a threshold.
    let over = |n| Condition::Value {
        family: "hits".into(),
        qualifier: b"total".to_vec(),
        filter: ValueFilter::I64(std::cmp::Ordering::Greater, n),
    };
    let skipped = pages
        .mutate(b"com.example/a")
        .put_i64("hits", b"total", 0)
        .commit_if(&over(10))?;
    let applied = pages
        .mutate(b"com.example/a")
        .put_i64("hits", b"total", 0)
        .commit_if(&over(1))?;
    println!(
        "reset over 10: {}, over 1: {}",
        skipped.is_some(),
        applied.is_some()
    );
    assert!(skipped.is_none() && applied.is_some());
    assert_eq!(read(b"total")?, 0);
    db.close()
}
