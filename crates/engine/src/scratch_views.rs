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
pub(crate) fn summary() -> String {
    let now = Instant::now();
    let v = VIEWS.lock().unwrap();
    let oldest = v
        .values()
        .map(|t| now.duration_since(*t).as_millis())
        .max()
        .unwrap_or(0);
    let old = v
        .values()
        .filter(|t| now.duration_since(**t).as_millis() > 50)
        .count();
    let s = SNAPS.lock().unwrap();
    let old_snaps = s
        .values()
        .filter(|(t, _)| now.duration_since(*t).as_millis() > 50)
        .count();
    format!(
        "views {} (>50ms {}) oldest ~{} ms, sampled snaps >50ms {}",
        v.len(),
        old,
        oldest / 10 * 10,
        old_snaps
    )
}

/// Prints the sampled snapshots older than 50 ms with their backtraces (at most `n`).
pub fn scratch_old_snapshots(n: usize) {
    let now = Instant::now();
    let s = SNAPS.lock().unwrap();
    let mut old: Vec<_> = s
        .values()
        .filter(|(t, _)| now.duration_since(*t).as_millis() > 50)
        .collect();
    old.sort_by_key(|(t, _)| *t);
    eprintln!(
        "SCRATCH-OLDSNAPS {} sampled snapshots older than 50 ms; {}",
        old.len(),
        summary()
    );
    for (t, bt) in old.into_iter().take(n) {
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
