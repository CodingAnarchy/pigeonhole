//! The balancer placement check (D231): the skewed-multi-shard writes at a fixed offered rate
//! over engine-owned shards, open-loop, so the balancer sees each shard's share of the
//! writes. Under the scaling gate's closed-loop driver a shard that finished its work idles
//! and the busy ones look skewed beside it (D224, D228); here the clients keep to their
//! schedule whatever the store does, so a shard's write rate is its share of the load.
//!
//! The table starts as one tablet and the balancer spreads it. The check waits until the table
//! has spread and nothing has split, merged or moved for [`Placement::QUIET_SECS`] (at most
//! `settle_bound`), then measures a window: each shard's share of the writes, the tablet
//! changes, and the writes completed against the offered rate.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pigeonhole::{Pigeonhole, ShardStats};

use super::pigeonhole::{create_table, err, options};
use crate::{BenchOp, PigeonholeRunner, Placement, Workload, WorkloadConfig};

/// How the placement check runs (D231).
#[derive(Debug, Clone)]
pub struct PlacementOptions {
    /// Writes offered per second, over every client.
    pub offered_rate: f64,
    /// Client threads sharing the offered rate.
    pub clients: usize,
    /// Longest the table may take to settle.
    pub settle_bound: Duration,
    /// How long to measure once settled.
    pub window: Duration,
}

/// Writes offered per shard per second by default: above the balancer's 2,000 rows per
/// 100 ms pass (`balance_min_writes`) for an average shard, so write skew is acted on as with
/// a busy application, and well under a shard's capacity, so the load stays open-loop.
pub const PLACEMENT_RATE_PER_SHARD: f64 = 30_000.0;

/// How far a measuring window that saw no writes may grow, as a multiple of its length
/// (#802).
pub const WINDOW_GROWTH: f64 = 10.0;

/// The default settle bound (D231).
pub const PLACEMENT_SETTLE_BOUND: Duration = Duration::from_secs(60);

/// Every shard's commits and the store's tablet changes so far.
fn snapshot(db: &Pigeonhole) -> (Vec<u64>, u64, bool) {
    let stats: Vec<ShardStats> = db.shard_stats();
    let commits = stats.iter().map(|s| s.commits).collect();
    let changes = stats.iter().map(|s| s.splits + s.merges + s.moves).sum();
    let spread = stats.iter().all(|s| s.tablets > 0);
    (commits, changes, spread)
}

impl PigeonholeRunner {
    /// Runs the placement check (D231) for `config`'s skewed writes on a fresh store in `dir`:
    /// engine-owned shards, packing off (it would keep a light load on few shards on
    /// purpose), the table spread by the balancer, the writes offered open-loop.
    pub fn run_placement(
        &self,
        config: &WorkloadConfig,
        dir: &Path,
        opts: &PlacementOptions,
    ) -> Result<Placement, String> {
        let mut settings = self.settings.clone();
        settings.no_packing = true;
        let db = Pigeonhole::open(dir.join("bench.phdb"), options(&settings)).map_err(err)?;
        let shards = db.shard_stats().len();
        let t = create_table(&db)?;
        let mut load = Workload::new(config.clone());
        for op in load.load_ops() {
            if let BenchOp::Put { row, family, cells } = op {
                let mut m = t.mutate(&row);
                for (q, v) in &cells {
                    m = m.put(family, q, v);
                }
                m.commit().map_err(err)?;
            }
        }
        let clients = opts.clients.max(1);
        let per_client = opts.offered_rate / clients as f64;
        let stop = AtomicBool::new(false);
        let completed = AtomicU64::new(0);
        let failed: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
        let start = Instant::now();
        let result = std::thread::scope(|s| {
            for c in 0..clients {
                let (t, stop, completed, failed) = (&t, &stop, &completed, &failed);
                // Each client its own stream of the workload's writes (a seed of its own), long
                // enough for any run; generated lazily.
                let stream = WorkloadConfig {
                    seed: config.seed.wrapping_add(1 + c as u64),
                    operations: u64::MAX / 4,
                    ..config.clone()
                };
                s.spawn(move || {
                    let mut w = Workload::new(stream);
                    let mut ops = w.run_ops();
                    let mut issued: u64 = 0;
                    while !stop.load(Ordering::Acquire) {
                        // Open loop: the schedule says how many writes are due by now, and
                        // a client behind it catches up rather than slowing the offer.
                        let due = (start.elapsed().as_secs_f64() * per_client) as u64;
                        if issued >= due {
                            std::thread::sleep(Duration::from_micros(200));
                            continue;
                        }
                        while issued < due && !stop.load(Ordering::Relaxed) {
                            let Some(BenchOp::Put { row, family, cells }) = ops.next() else {
                                return;
                            };
                            let mut m = t.mutate(&row);
                            for (q, v) in &cells {
                                m = m.put(family, q, v);
                            }
                            if let Err(e) = m.commit() {
                                *failed.lock().unwrap() = Some(err(e));
                                stop.store(true, Ordering::Release);
                                return;
                            }
                            issued += 1;
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
            // Wait for the table to spread and settle, at most `settle_bound`.
            let (mut last_changes, mut last_change) = (snapshot(&db).1, Instant::now());
            let mut settle_secs = None;
            while start.elapsed() < opts.settle_bound && !stop.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(100));
                let (_, changes, spread) = snapshot(&db);
                if changes != last_changes {
                    (last_changes, last_change) = (changes, Instant::now());
                }
                if spread && last_change.elapsed().as_secs_f64() >= Placement::QUIET_SECS {
                    settle_secs = Some(start.elapsed().as_secs_f64());
                    break;
                }
            }
            // The window: each shard's share of the writes, the tablet changes, the rate.
            let (before, changes_before, _) = snapshot(&db);
            let done_before = completed.load(Ordering::Relaxed);
            let window_start = Instant::now();
            std::thread::sleep(opts.window);
            // A window with no writes measures nothing: on a loaded runner a debug build's
            // commits can stall through all of it (#802). It then grows until a write lands,
            // up to `WINDOW_GROWTH` times its length; the record names the window it got.
            let cap = opts.window.mul_f64(WINDOW_GROWTH);
            while completed.load(Ordering::Relaxed) == done_before
                && window_start.elapsed() < cap
                && !stop.load(Ordering::Acquire)
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            let (after, changes_after, _) = snapshot(&db);
            let window = window_start.elapsed().as_secs_f64();
            let done = completed.load(Ordering::Relaxed) - done_before;
            stop.store(true, Ordering::Release);
            let delta: Vec<u64> = after
                .iter()
                .zip(&before)
                .map(|(a, b)| a.saturating_sub(*b))
                .collect();
            let total: u64 = delta.iter().sum();
            let top = delta.iter().copied().max().unwrap_or(0);
            Placement {
                shards,
                offered_rate: opts.offered_rate,
                achieved_rate: done as f64 / window,
                settle_secs,
                settle_bound_secs: opts.settle_bound.as_secs_f64(),
                window_secs: window,
                changes_measured: changes_after - changes_before,
                max_share: if total == 0 {
                    0.0
                } else {
                    top as f64 / total as f64
                },
                shares: delta
                    .iter()
                    .map(|d| {
                        if total == 0 {
                            0.0
                        } else {
                            *d as f64 / total as f64
                        }
                    })
                    .collect(),
            }
        });
        if let Some(e) = failed.into_inner().unwrap() {
            return Err(format!("a placement client's write failed: {e}"));
        }
        db.close().map_err(err)?;
        Ok(result)
    }
}
