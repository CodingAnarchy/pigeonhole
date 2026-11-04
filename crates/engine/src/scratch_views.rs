//! SCRATCH #1477 (not for merge): live views and sampled snapshots, by age.
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static NEXT: AtomicU64 = AtomicU64::new(1);
static VIEWS: Mutex<BTreeMap<u64, Instant>> = Mutex::new(BTreeMap::new());
static SNAPS: Mutex<BTreeMap<u64, (Instant, String)>> = Mutex::new(BTreeMap::new());

/// Registered while the view lives.
#[derive(Debug)]
pub(crate) struct Live(u64);

impl Live {
    pub(crate) fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        VIEWS.lock().unwrap().insert(id, Instant::now());
        Self(id)
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        VIEWS.lock().unwrap().remove(&self.0);
    }
}

/// A sampled snapshot (1 in 256), with where it was taken.
#[derive(Debug)]
pub(crate) struct SnapLive(u64);

pub(crate) fn snap() -> Option<std::sync::Arc<SnapLive>> {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    if id % 256 != 0 {
        return None;
    }
    let bt = std::backtrace::Backtrace::force_capture().to_string();
    SNAPS.lock().unwrap().insert(id, (Instant::now(), bt));
    Some(std::sync::Arc::new(SnapLive(id)))
}

impl Drop for SnapLive {
    fn drop(&mut self) {
        SNAPS.lock().unwrap().remove(&self.0);
    }
}

/// One line: live views, the oldest one's age, and sampled snapshots older than 50 ms.
/// One lock at a time, never both (a monitor thread and a shard thread call it).
pub(crate) fn summary() -> String {
    let now = Instant::now();
    let (n, oldest, old) = {
        let v = VIEWS.lock().unwrap();
        let ages = v.values().map(|t| now.duration_since(*t).as_millis());
        (
            v.len(),
            ages.clone().max().unwrap_or(0),
            ages.filter(|a| *a > 50).count(),
        )
    };
    let old_snaps = {
        let s = SNAPS.lock().unwrap();
        s.values()
            .filter(|(t, _)| now.duration_since(*t).as_millis() > 50)
            .count()
    };
    format!(
        "views {n} (>50ms {old}) oldest ~{} ms, sampled snaps >50ms {old_snaps}",
        oldest / 10 * 10
    )
}

/// Prints the sampled snapshots older than 50 ms with their backtraces (at most `n`).
pub fn scratch_old_snapshots(n: usize) {
    let now = Instant::now();
    let line = summary();
    let mut old: Vec<(Instant, String)> = {
        let s = SNAPS.lock().unwrap();
        s.values()
            .filter(|(t, _)| now.duration_since(*t).as_millis() > 50)
            .map(|(t, b)| (*t, b.clone()))
            .collect()
    };
    old.sort_by_key(|(t, _)| *t);
    eprintln!(
        "SCRATCH-OLDSNAPS {} sampled snapshots older than 50 ms; {line}",
        old.len()
    );
    for (t, bt) in old.iter().take(n) {
        eprintln!(
            "SCRATCH-OLDSNAP age {} ms:\n{}",
            now.duration_since(*t).as_millis(),
            bt.lines()
                .filter(|l| l.contains("pigeonhole") || l.contains("memrecall"))
                .take(30)
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}
