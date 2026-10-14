//! SCRATCH (perf2, cross-shard p99 investigation; not for commit). Engine-owned shards,
//! tablet changes off, one table per shard; each client loops single-shard puts (with
//! probability `single_pct`) and cross-shard commits over 2-4 shards.
//!
//! `scratch_xs DIR shards clients secs single_pct commit_spin_us shard_spin_us`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use pigeonhole_engine::{Durability, Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_io::VfsRef;
use pigeonhole_io::pread::PreadVfs;

fn arg<T: std::str::FromStr>(a: &[String], i: usize, d: T) -> T {
    a.get(i).and_then(|s| s.parse().ok()).unwrap_or(d)
}

fn pct(v: &mut Vec<u32>, p: f64) -> u32 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&a[1]);
    let shards: usize = arg(&a, 2, 4);
    let clients: usize = arg(&a, 3, 8);
    let secs: u64 = arg(&a, 4, 10);
    let single_pct: u64 = arg(&a, 5, 50);
    let commit_spin_us: u64 = arg(&a, 6, 15);
    let shard_spin_us: u64 = arg(&a, 7, 50);
    std::fs::create_dir_all(&dir).unwrap();
    let vfs: VfsRef = PreadVfs::new(4);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = shards;
    o.tablet_changes = false;
    o.commit_spin_nanos = commit_spin_us * 1_000;
    o.shard_spin_nanos = shard_spin_us * 1_000;
    let db = Engine::open(&dir.join("d.phdb"), o).unwrap();
    let tables = Arc::new(
        (0..shards)
            .map(|i| {
                db.create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
                    .unwrap()
            })
            .collect::<Vec<_>>(),
    );
    // Warm up 1 s, then measure.
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let hs: Vec<_> = (0..clients)
        .map(|c| {
            let (db, tables, stop, measuring) =
                (db.clone(), tables.clone(), stop.clone(), measuring.clone());
            std::thread::spawn(move || {
                let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ (c as u64 + 1) * 0xA24B_AED4;
                let mut rng = move |n: u64| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x % n
                };
                let v = [7u8; 100];
                let (mut ls, mut lc) = (Vec::with_capacity(1 << 20), Vec::with_capacity(1 << 20));
                while !stop.load(Ordering::Relaxed) {
                    let n = tables.len() as u64;
                    let cross = rng(100) >= single_pct && n > 1;
                    let parts = if cross { 2 + rng(3.min(n - 1)) } else { 1 };
                    let first = rng(n);
                    let mut wb = WriteBatch::new();
                    for k in 0..parts {
                        let t = &tables[((first + k) % n) as usize];
                        wb.put(t.id, t.families[0].id, &rng(1 << 20).to_be_bytes(), b"q", None, ValueRef::Bytes(&v))
                            .unwrap();
                    }
                    pigeonhole_engine::scratch_phases::set_cross(cross);
                    let t0 = Instant::now();
                    db.commit(wb, Some(Durability::Buffered)).unwrap();
                    if measuring.load(Ordering::Relaxed) {
                        let us = t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
                        if cross { lc.push(us) } else { ls.push(us) }
                    }
                }
                (ls, lc)
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(1));
    let m0 = db.metrics();
    let _ = pigeonhole_engine::scratch_phases::take();
    measuring.store(true, Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_secs(secs));
    measuring.store(false, Ordering::Relaxed);
    let el = t0.elapsed().as_secs_f64();
    let m1 = db.metrics();
    let phases = pigeonhole_engine::scratch_phases::take();
    stop.store(true, Ordering::Relaxed);
    let (mut ls, mut lc) = (Vec::new(), Vec::new());
    for h in hs {
        let (s, c) = h.join().unwrap();
        ls.extend(s);
        lc.extend(c);
    }
    let ops = (ls.len() + lc.len()) as f64;
    let per = |x: u64| x as f64 / ops;
    println!(
        "shards={shards} clients={clients} single%={single_pct} cspin={commit_spin_us}us sspin={shard_spin_us}us | ops/s {:.0} | single p50/p99/p999 {}/{}/{} us | cross {}/{}/{} us | per op: commit_parks {:.2} shard_parks {:.2} shard_wakes {:.2} vis_waits {:.2} vis_contended {:.2} vis_wake_passes {:.2} xrounds {:.2} | busy {:.2} shards",
        ops / el,
        pct(&mut ls, 0.5), pct(&mut ls, 0.99), pct(&mut ls, 0.999),
        pct(&mut lc, 0.5), pct(&mut lc, 0.99), pct(&mut lc, 0.999),
        per(m1.commit_parks - m0.commit_parks),
        per(m1.shard_idle.0 - m0.shard_idle.0),
        per(m1.shard_idle.1 - m0.shard_idle.1),
        per(m1.visibility_waits.0 - m0.visibility_waits.0),
        per(m1.visibility_contended - m0.visibility_contended),
        per(m1.visibility_wakes.0 - m0.visibility_wakes.0),
        per(m1.cross_shard.rounds - m0.cross_shard.rounds),
        (m1.shard_busy_nanos - m0.shard_busy_nanos) as f64 / 1e9 / el,
    );
    for (name, h) in phases {
        let total: u64 = h.iter().sum();
        if total == 0 {
            continue;
        }
        let q = |p: f64| {
            let want = (total as f64 * p).ceil() as u64;
            let mut acc = 0;
            for (b, n) in h.iter().enumerate() {
                acc += n;
                if acc >= want {
                    return pigeonhole_engine::scratch_phases::upper(b);
                }
            }
            0
        };
        let mean: f64 = h
            .iter()
            .enumerate()
            .map(|(b, n)| *n as f64 * pigeonhole_engine::scratch_phases::upper(b) as f64)
            .sum::<f64>()
            / total as f64;
        let nonzero = 1.0 - h[0] as f64 / total as f64;
        println!(
            "  {name:<48} n={total:>9} p50 {:>8.1} p99 {:>8.1} p999 {:>9.1} mean {:>8.1} us  (nonzero {:.0}%)",
            q(0.5) as f64 / 1e3,
            q(0.99) as f64 / 1e3,
            q(0.999) as f64 / 1e3,
            mean / 1e3,
            nonzero * 100.0
        );
    }
    db.close().unwrap();
}
