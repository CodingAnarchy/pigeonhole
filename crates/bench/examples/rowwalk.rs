//! A cached wide-row walk, sync against async (#1625, perf287's measurement): one row of `N`
//! cells in an SST, walked whole `PASSES` times warm with `RowCells::next` and with
//! `RowCells::poll_next` (polled with a waker that does nothing: a warm walk is never
//! `Pending`). The async walk should cost within a few percent of the sync one per cell.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example rowwalk -- [N] [PASSES] [DIR]
//! ```
//!
//! Defaults: 100,000 cells, 20 passes. Prints the best pass of each, in ns per cell, and the
//! async walk's ratio to the sync one.

use std::task::{Context, Poll, Waker};
use std::time::Instant;

use pigeonhole::{Family, Options, Pigeonhole};

fn main() -> pigeonhole::Result<()> {
    let mut args = std::env::args().skip(1);
    let n: u32 = args.next().map_or(100_000, |s| s.parse().expect("N"));
    let passes: u32 = args.next().map_or(20, |s| s.parse().expect("PASSES"));
    let given = args.next().map(std::path::PathBuf::from);
    let dir = given
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("rowwalk-{}", std::process::id())));
    std::fs::create_dir_all(&dir).expect("dir");
    let db = Pigeonhole::open(dir.join("w.phdb"), Options::default().shards(1))?;
    let t = db
        .table("t")?
        .family("post", Family::default())
        .create_if_missing()?;
    for chunk in (0..n).collect::<Vec<_>>().chunks(10_000) {
        let mut m = t.mutate(b"hub");
        for &i in chunk {
            m = m.put("post", &i.to_be_bytes(), &u64::from(i).to_le_bytes());
        }
        m.commit()?;
    }
    db.flush()?;
    let mut cx = Context::from_waker(Waker::noop());
    let (mut sync_best, mut async_best) = (f64::MAX, f64::MAX);
    let mut sum = 0u64;
    for pass in 0..=passes {
        let start = Instant::now();
        let mut cells = t.row(b"hub").cells()?;
        let mut seen = 0u32;
        while let Some(e) = cells.next()? {
            sum = sum.wrapping_add(e.qualifier[3].into());
            seen += 1;
        }
        let sync_ns = start.elapsed().as_nanos() as f64 / f64::from(seen);
        let start = Instant::now();
        let mut cells = t.row(b"hub").cells()?;
        let mut seen_async = 0u32;
        loop {
            match cells.poll_next(&mut cx) {
                Poll::Ready(r) => match r? {
                    Some(e) => {
                        sum = sum.wrapping_add(e.qualifier[3].into());
                        seen_async += 1;
                    }
                    None => break,
                },
                Poll::Pending => std::thread::yield_now(),
            }
        }
        let async_ns = start.elapsed().as_nanos() as f64 / f64::from(seen_async);
        assert_eq!(seen, seen_async);
        // Pass 0 warms the cache.
        if pass > 0 {
            sync_best = sync_best.min(sync_ns);
            async_best = async_best.min(async_ns);
        }
    }
    println!(
        "cells {n}: sync {sync_best:.1} ns/cell, async {async_best:.1} ns/cell, ratio {:.3} ({sum})",
        async_best / sync_best
    );
    db.close()?;
    // Only the directory this run made for itself goes; a given one is the caller's.
    if given.is_none() {
        std::fs::remove_dir_all(&dir).ok();
    }
    Ok(())
}
