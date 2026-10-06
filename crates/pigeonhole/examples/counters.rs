//! Counters with the built-in `i64` add merge operator: `incr` is a blind write, so many
//! threads can count concurrently without losing updates; per-day buckets give windowed
//! counts; `put_i64` resets a base; `commit_if` makes a compare-and-set.
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
        .family("hits", Family::default().max_versions(1))
        .create_if_missing()?;

    // Eight threads, 250 increments each, on the same counter and on per-day buckets.
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let pages = pages.clone();
            thread::spawn(move || -> pigeonhole::Result<()> {
                for i in 0..250 {
                    let day = if i % 2 == 0 {
                        "2026-10-05"
                    } else {
                        "2026-10-06"
                    };
                    pages
                        .mutate(b"com.example/a")
                        .incr("hits", b"total", 1)
                        .incr("hits", day.as_bytes(), 1)
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
    println!("2026-10-05 = {}", read(b"2026-10-05")?);
    assert_eq!(read(b"total")?, 2000);
    assert_eq!(read(b"2026-10-05")? + read(b"2026-10-06")?, 2000);

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
