//! Change-feed subscribers beside a measured phase (D310 PR 5b, `--feed`): writer-process
//! threads following the bench table's feed (durable, or visible), and `phdb-bench reader
//! --feed` children following it from reader processes. Each delivery's latency is the wall
//! clock minus its commit's timestamp ([`FeedReport`]). A sampler keeps the retained bytes'
//! peak, and the feed's sync counters are read around the phase.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pigeonhole::{ErrorCode, Feed, Pigeonhole, Table};

use crate::{FeedReport, Histogram, LatencyStats};

/// How long a subscriber's `next` waits before it looks at the stop flag again.
const POLL: Duration = Duration::from_millis(100);

/// How often the retained bytes are sampled.
const SAMPLE_EVERY: Duration = Duration::from_millis(100);

/// Wall-clock microseconds, the commit timestamps' clock.
pub(crate) fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

/// What one subscriber saw.
#[derive(Debug, Default)]
pub(crate) struct Followed {
    pub(crate) hist: Histogram,
    pub(crate) expired: u64,
    pub(crate) errors: u64,
}

/// Follows `feed` until `stop`, recording each commit's delivery latency. An expired feed is
/// counted and reopened at now (the row is void anyway, [`FeedReport::expired`]).
pub(crate) fn follow(
    mut feed: Feed,
    reopen: impl Fn() -> Result<Feed, pigeonhole::Error>,
    stop: &AtomicBool,
) -> Followed {
    let mut out = Followed::default();
    while !stop.load(Ordering::Acquire) {
        match feed.next(POLL) {
            Ok(Some(c)) => {
                let late = now_micros().saturating_sub(c.timestamp());
                out.hist.record(Duration::from_micros(late));
            }
            Ok(None) => {}
            Err(e) => {
                if e.code() == ErrorCode::FeedExpired {
                    out.expired += 1;
                } else {
                    out.errors += 1;
                }
                match reopen() {
                    Ok(f) => feed = f,
                    Err(_) => {
                        // Wait out a writer restart or close without spinning.
                        std::thread::sleep(POLL);
                    }
                }
            }
        }
    }
    out
}

/// The feed's counters at a point, to subtract.
#[derive(Debug, Clone, Copy, Default)]
struct Counters {
    background: u64,
    group: u64,
    sidecar: u64,
    taken: u64,
}

impl Counters {
    fn read(db: &Pigeonhole) -> Self {
        let m = db.engine_metrics();
        Self {
            background: m.feed_background_syncs,
            group: m.group_syncs,
            sidecar: m.feed_sidecar_syncs,
            taken: db.shard_stats().iter().map(|s| s.feed_retained_taken).sum(),
        }
    }
}

/// The subscribers while the measured phase runs.
#[derive(Debug)]
pub(crate) struct Running {
    db: Pigeonhole,
    stop: Arc<AtomicBool>,
    durable: Vec<JoinHandle<Followed>>,
    visible: Vec<JoinHandle<Followed>>,
    readers: Option<super::reader_process::FeedChildren>,
    sampler: Option<JoinHandle<(u64, u64)>>,
    before: Counters,
}

/// Starts `durable` and `visible` writer-process subscribers and `readers` reader
/// processes on `table`'s feed (from now), and the retained-bytes sampler.
pub(crate) fn start(
    db: &Pigeonhole,
    table: &Table,
    path: &std::path::Path,
    durable: usize,
    visible: usize,
    readers: usize,
) -> Result<Running, String> {
    let stop = Arc::new(AtomicBool::new(false));
    let spawn = |visible: bool| -> Result<JoinHandle<Followed>, String> {
        let open = {
            let table = table.clone();
            move || {
                let b = table.feed();
                if visible { b.visible() } else { b }.open()
            }
        };
        // Opened here, so the phase's first commit is already followed.
        let feed = open().map_err(|e| e.to_string())?;
        let stop = Arc::clone(&stop);
        Ok(std::thread::spawn(move || follow(feed, open, &stop)))
    };
    let durable = (0..durable)
        .map(|_| spawn(false))
        .collect::<Result<Vec<_>, _>>()?;
    let visible = (0..visible)
        .map(|_| spawn(true))
        .collect::<Result<Vec<_>, _>>()?;
    let readers = if readers > 0 {
        Some(super::reader_process::spawn_feed(path, readers)?)
    } else {
        None
    };
    let sampler = {
        let (db, stop) = (db.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut peak = (0u64, 0u64);
            loop {
                if let Ok(Some(f)) = db.info().map(|i| i.feed) {
                    peak.0 = peak.0.max(f.retained_wal_bytes);
                    peak.1 = peak.1.max(f.held_blob_bytes);
                }
                if stop.load(Ordering::Acquire) {
                    return peak;
                }
                std::thread::sleep(SAMPLE_EVERY);
            }
        })
    };
    Ok(Running {
        db: db.clone(),
        stop,
        durable,
        visible,
        readers,
        sampler: Some(sampler),
        before: Counters::read(db),
    })
}

impl Running {
    /// Stops every subscriber and reports them with the feed's cost over the phase.
    pub(crate) fn stop(mut self) -> Result<FeedReport, String> {
        let after = Counters::read(&self.db);
        self.stop.store(true, Ordering::Release);
        let (mut expired, mut errors) = (0, 0);
        let mut join = |handles: Vec<JoinHandle<Followed>>| -> Result<(u32, Histogram), String> {
            let n = handles.len() as u32;
            let mut hist = Histogram::new();
            for h in handles {
                let f = h.join().map_err(|_| "a feed subscriber panicked")?;
                hist.merge(&f.hist);
                expired += f.expired;
                errors += f.errors;
            }
            Ok((n, hist))
        };
        let (durable_subscribers, durable) = join(std::mem::take(&mut self.durable))?;
        let (visible_subscribers, visible) = join(std::mem::take(&mut self.visible))?;
        let (retained_wal_peak, held_blob_peak) = self
            .sampler
            .take()
            .map_or((0, 0), |s| s.join().unwrap_or((0, 0)));
        let mut report = FeedReport {
            durable_subscribers,
            durable_delivery: LatencyStats::of(&durable),
            visible_subscribers,
            visible_delivery: LatencyStats::of(&visible),
            expired,
            errors,
            background_syncs: after.background.saturating_sub(self.before.background),
            group_syncs: after.group.saturating_sub(self.before.group),
            sidecar_syncs: after.sidecar.saturating_sub(self.before.sidecar),
            retained_taken: after.taken.saturating_sub(self.before.taken),
            retained_wal_peak,
            held_blob_peak,
            ..FeedReport::default()
        };
        if let Some(r) = self.readers.take() {
            let f = r.stop()?;
            report.reader_subscribers = f.processes;
            report.reader_delivery = LatencyStats::of(&f.hist);
            report.reader_cpu_per_sec = f.cpu_per_sec;
            report.reader_wakeups_per_sec = f.wakeups_per_sec;
            report.expired += f.expired;
            report.errors += f.errors;
        }
        Ok(report)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // A run that failed before `stop` leaves no subscriber behind.
        self.stop.store(true, Ordering::Release);
    }
}
