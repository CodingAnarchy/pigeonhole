//! SCRATCH #759 (not for merge): a per-read phase breakdown of row reads, bucketed by the
//! read's engine time. On only while `ON` is set (the bench sets it for the measured phase).
//! Each thread keeps its own stats and merges them into `GLOBAL` at exit, rewriting the file
//! `PHDB_PHASES_OUT` names.

use std::cell::RefCell;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

pub static ON: AtomicBool = AtomicBool::new(false);

pub const EDGES_US: [u64; 7] = [10, 20, 30, 40, 60, 100, u64::MAX];

#[derive(Clone, Copy, Default)]
pub struct Bucket {
    pub count: u64,
    pub total: u64,
    pub sources: u64,
    pub seek: u64,
    pub cells: u64,
    pub miss_ns: u64,
    pub misses: u64,
    pub with_miss: u64,
    pub nsources: u64,
    pub offcpu: u64,
    pub preempted: u64,
    /// Misses by block kind (index 0 unused; 1 data, 2 index, 3 top index, 4 filter, 5 properties).
    pub kind: [u64; 6],
    /// Misses by level (7: level 7 and up, or an SST this read did not probe).
    pub mlevel: [u64; 8],
    /// SSTs whose range held the row, by level, and how many of them passed the row filter.
    pub probed: [u64; 8],
    pub passed: [u64; 8],
    /// Block cache lookups, and the misses' file-read time alone.
    pub lookups: u64,
    pub miss_read_ns: u64,
}

#[derive(Clone)]
pub struct Stats {
    pub buckets: [Bucket; 7],
    pub hist: Vec<u64>,
    /// Each missed block's file read, 1 us buckets.
    pub miss_hist: Vec<u64>,
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            buckets: [Bucket::default(); 7],
            hist: vec![0; 501],
            miss_hist: vec![0; 128],
        }
    }
}

impl Stats {
    fn merge(&mut self, o: &Stats) {
        for (a, b) in self.buckets.iter_mut().zip(&o.buckets) {
            a.count += b.count;
            a.total += b.total;
            a.sources += b.sources;
            a.seek += b.seek;
            a.cells += b.cells;
            a.miss_ns += b.miss_ns;
            a.misses += b.misses;
            a.with_miss += b.with_miss;
            a.nsources += b.nsources;
            a.offcpu += b.offcpu;
            a.preempted += b.preempted;
            a.lookups += b.lookups;
            a.miss_read_ns += b.miss_read_ns;
            for i in 0..6 {
                a.kind[i] += b.kind[i];
            }
            for i in 0..8 {
                a.mlevel[i] += b.mlevel[i];
                a.probed[i] += b.probed[i];
                a.passed[i] += b.passed[i];
            }
        }
        for (a, b) in self.hist.iter_mut().zip(&o.hist) {
            *a += b;
        }
        for (a, b) in self.miss_hist.iter_mut().zip(&o.miss_hist) {
            *a += b;
        }
    }

    fn render(&self) -> String {
        let mut out = String::new();
        let n: u64 = self.hist.iter().sum();
        let pct = |q: f64| {
            let want = (n as f64 * q).ceil() as u64;
            let mut acc = 0;
            for (i, c) in self.hist.iter().enumerate() {
                acc += c;
                if acc >= want {
                    return i;
                }
            }
            500
        };
        out += &format!(
            "engine row reads {n}: p50 {} us, p99 {} us, p99.9 {} us (1 us buckets, 500 = 500+)\n",
            pct(0.5),
            pct(0.99),
            pct(0.999)
        );
        out += "bucket_us count share total_us sources_us seek_us cells_us miss_us misses/read with_miss% nsources offcpu_us preempted%\n";
        let mut lo = 0;
        for (b, &hi) in self.buckets.iter().zip(&EDGES_US) {
            let c = b.count.max(1) as f64;
            out += &format!(
                "{lo}-{} {} {:.2}% {:.1} {:.1} {:.1} {:.1} {:.1} {:.2} {:.1}% {:.2} {:.1} {:.1}%\n",
                if hi == u64::MAX {
                    "inf".into()
                } else {
                    hi.to_string()
                },
                b.count,
                100.0 * b.count as f64 / n.max(1) as f64,
                b.total as f64 / c / 1e3,
                b.sources as f64 / c / 1e3,
                b.seek as f64 / c / 1e3,
                b.cells as f64 / c / 1e3,
                b.miss_ns as f64 / c / 1e3,
                b.misses as f64 / c,
                100.0 * b.with_miss as f64 / c,
                b.nsources as f64 / c,
                b.offcpu as f64 / c / 1e3,
                100.0 * b.preempted as f64 / c,
            );
            lo = hi;
        }
        out += "\nper read, by bucket: misses by kind (data index top-index filter props) | misses by level L0..L6,other | SSTs in range by level | of them past the row filter\n";
        let mut lo = 0;
        for (b, &hi) in self.buckets.iter().zip(&EDGES_US) {
            let c = b.count.max(1) as f64;
            let f = |v: &[u64]| {
                v.iter()
                    .map(|x| format!("{:.3}", *x as f64 / c))
                    .collect::<Vec<_>>()
                    .join(" ")
            };
            out += &format!(
                "{lo}-{} | {} | {} | {} | {}\n",
                if hi == u64::MAX {
                    "inf".into()
                } else {
                    hi.to_string()
                },
                f(&b.kind[1..]),
                f(&b.mlevel),
                f(&b.probed),
                f(&b.passed),
            );
            lo = hi;
        }
        out += "\nper read, by bucket: block lookups, misses, hit rate, miss file-read us (of miss_us)\n";
        let mut lo = 0;
        for (b, &hi) in self.buckets.iter().zip(&EDGES_US) {
            let c = b.count.max(1) as f64;
            out += &format!(
                "{lo}-{} | {:.2} {:.2} {:.1}% | {:.1} of {:.1}\n",
                if hi == u64::MAX {
                    "inf".into()
                } else {
                    hi.to_string()
                },
                b.lookups as f64 / c,
                b.misses as f64 / c,
                100.0 * (1.0 - b.misses as f64 / (b.lookups.max(1) as f64)),
                b.miss_read_ns as f64 / c / 1e3,
                b.miss_ns as f64 / c / 1e3,
            );
            lo = hi;
        }
        let mn: u64 = self.miss_hist.iter().sum();
        let mq = |q: f64| {
            let want = (mn as f64 * q).ceil() as u64;
            let mut acc = 0;
            for (i, c) in self.miss_hist.iter().enumerate() {
                acc += c;
                if acc >= want {
                    return i;
                }
            }
            127
        };
        out += &format!(
            "\nmissed-block file reads {mn}: p50 {} us, p90 {} us, p99 {} us, p99.9 {} us; share >= 20 us {:.2}%\n",
            mq(0.5),
            mq(0.9),
            mq(0.99),
            mq(0.999),
            100.0 * self.miss_hist[20..].iter().sum::<u64>() as f64 / mn.max(1) as f64
        );
        out
    }
}

static GLOBAL: Mutex<Option<Stats>> = Mutex::new(None);

struct Local(Stats);

impl Drop for Local {
    fn drop(&mut self) {
        if self.0.hist.iter().sum::<u64>() == 0 {
            return;
        }
        let mut g = GLOBAL.lock().unwrap();
        let g = g.get_or_insert_with(Stats::default);
        g.merge(&self.0);
        if let Ok(path) = std::env::var("PHDB_PHASES_OUT") {
            let _ = std::fs::write(path, g.render());
        }
    }
}

thread_local! {
    static LOCAL: RefCell<Local> = RefCell::new(Local(Stats::default()));
    /// The current read's phase sums: sources, seek, cells, source count.
    pub static CUR: std::cell::Cell<(u64, u64, u64, u64)> = const { std::cell::Cell::new((0, 0, 0, 0)) };
    /// The current read's probed SSTs: id, level, past the filter.
    static PROBED: RefCell<Vec<(pigeonhole_format::SstId, usize, bool)>> = const { RefCell::new(Vec::new()) };
    /// The last engine row read's time on this thread, for the bench to take (0 = none).
    pub static LAST: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// SCRATCH #759: takes this thread's last engine row read time (0 if none since the last take).
pub fn take_last() -> u64 {
    LAST.with(|c| c.replace(0))
}

pub fn probed(id: pigeonhole_format::SstId, level: usize, passed: bool) {
    if ON.load(std::sync::atomic::Ordering::Relaxed) {
        let _ = PROBED.try_with(|p| p.borrow_mut().push((id, level, passed)));
    }
}

pub fn thread_cpu_ns() -> u64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

pub fn record(total: u64, cpu: u64, misses: u64, miss_ns: u64, lookups: u64, miss_read_ns: u64) {
    LAST.with(|c| c.set(total));
    let (sources, seek, cells, nsources) = CUR.with(|c| c.replace((0, 0, 0, 0)));
    let _ = LOCAL.try_with(|l| {
        let s = &mut l.borrow_mut().0;
        let us = total / 1000;
        s.hist[(us as usize).min(500)] += 1;
        let i = EDGES_US.iter().position(|&e| us < e).unwrap_or(6);
        let b = &mut s.buckets[i];
        let off = total.saturating_sub(cpu);
        b.count += 1;
        b.total += total;
        b.sources += sources;
        b.seek += seek;
        b.cells += cells;
        b.miss_ns += miss_ns;
        b.misses += misses;
        b.with_miss += u64::from(misses > 0);
        b.nsources += nsources;
        b.offcpu += off;
        b.preempted += u64::from(off > 2_000);
        b.lookups += lookups;
        b.miss_read_ns += miss_read_ns;
        let _ = pigeonhole_sst::MISS_READ_HIST.try_with(|h| {
            let mut h = h.borrow_mut();
            for (a, x) in s.miss_hist.iter_mut().zip(h.iter_mut()) {
                *a += std::mem::take(x);
            }
        });
        let _ = PROBED.try_with(|p| {
            let mut p = p.borrow_mut();
            for &(_, l, passed) in p.iter() {
                b.probed[l.min(7)] += 1;
                b.passed[l.min(7)] += u64::from(passed);
            }
            let _ = pigeonhole_sst::MISS_LOG.try_with(|m| {
                for (kind, id) in m.borrow_mut().drain(..) {
                    b.kind[(kind as usize).min(5)] += 1;
                    let l = p.iter().find(|x| x.0 == id).map_or(7, |x| x.1.min(7));
                    b.mlevel[l] += 1;
                }
            });
            p.clear();
        });
    });
}

pub fn add(i: usize, ns: u64) {
    CUR.with(|c| {
        let mut v = c.get();
        match i {
            0 => v.0 += ns,
            1 => v.1 += ns,
            2 => v.2 += ns,
            _ => v.3 += ns,
        }
        c.set(v);
    });
}
