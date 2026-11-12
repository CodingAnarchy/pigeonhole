//! Change-feed subscribers beside a measured phase (D310 PR 5b, `--feed`): writer-process
//! threads following the bench table's feed (durable, or visible), and `phdb-bench reader
//! --feed` children following it from reader processes. Each delivery's latency is the wall
//! clock minus its commit's timestamp ([`FeedReport`]). A sampler keeps the retained bytes'
//! peak, and the feed's sync counters are read around the phase. When the phase ends, every
//! subscriber goes on until it has covered the phase's last commit (bounded by
//! [`DRAIN_WAIT`]), so each reports every commit of the phase.

use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pigeonhole::{ErrorCode, Feed, Pigeonhole, Table};

use crate::{FeedReport, Histogram, LatencyStats};

/// How long a subscriber's `next` waits before it looks at the stop flag again.
const POLL: Duration = Duration::from_millis(100);

/// How often the retained bytes are sampled.
const SAMPLE_EVERY: Duration = Duration::from_millis(100);

/// The longest a subscriber goes on after the phase for its last commits: a durable one
/// waits for the sync that covers them, which a slow runner disk can take over a second
/// for (#1553's Windows run).
pub(crate) const DRAIN_WAIT: Duration = Duration::from_secs(30);

/// When subscribers stop: once [`drain`](Self::drain) was called, at the first look that
/// finds the feed covering its seqno, or past its deadline.
#[derive(Debug, Default)]
pub(crate) struct Stop {
    through: OnceLock<(u64, Instant)>,
}

impl Stop {
    /// Stop once `through` is covered, or after [`DRAIN_WAIT`]; 0 stops at once. Only the
    /// first call counts.
    pub(crate) fn drain(&self, through: u64) {
        let _ = self.through.set((through, Instant::now() + DRAIN_WAIT));
    }

    fn draining(&self) -> bool {
        self.through.get().is_some()
    }

    /// `None` to go on; else whether the deadline ended it (with `through` not covered).
    fn ended(&self, covered: u64) -> Option<bool> {
        let &(through, deadline) = self.through.get()?;
        if covered >= through {
            Some(false)
        } else if Instant::now() >= deadline {
            Some(true)
        } else {
            None
        }
    }
}

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
    /// 1 if the drain's deadline stopped it before it covered the phase's last commit.
    pub(crate) drain_timeouts: u64,
    /// The first [`KEPT_ERRORS`] errors (expired or not), each with its code and where the
    /// feed had got to, so a record with any says what they were (#1773).
    pub(crate) error_messages: Vec<String>,
}

/// How many of a subscriber's errors [`Followed::error_messages`] keeps.
pub(crate) const KEPT_ERRORS: usize = 4;

/// Keeps `what` in `out`'s first [`KEPT_ERRORS`], and prints it while there's room: a feed
/// that keeps failing every poll prints the same first few the record keeps, not a line per
/// poll (ai1249's N1 on #1774).
fn keep(out: &mut Followed, what: String) {
    if out.error_messages.len() < KEPT_ERRORS {
        eprintln!("phdb-bench: a feed subscriber's error: {what}");
        out.error_messages.push(what);
    }
}

/// Follows `feed` until `stop` says so, recording each commit's delivery latency. An expired
/// feed is counted and reopened at now (the row is void anyway, [`FeedReport::expired`]).
pub(crate) fn follow(
    mut feed: Feed,
    reopen: impl Fn() -> Result<Feed, pigeonhole::Error>,
    stop: &Stop,
) -> Followed {
    let mut out = Followed::default();
    loop {
        if let Some(timed_out) = stop.ended(feed.progress().covered_seqno()) {
            out.drain_timeouts = u64::from(timed_out);
            return out;
        }
        match feed.next(POLL) {
            Ok(Some(c)) => {
                let late = now_micros().saturating_sub(c.timestamp());
                out.hist.record(Duration::from_micros(late));
            }
            Ok(None) => {}
            Err(e) => {
                let what = format!(
                    "{:?} after seqno {}: {e}",
                    e.code(),
                    feed.progress().covered_seqno()
                );
                keep(&mut out, what);
                if e.code() == ErrorCode::FeedExpired {
                    out.expired += 1;
                } else {
                    out.errors += 1;
                }
                match reopen() {
                    Ok(f) => feed = f,
                    Err(e) => {
                        // The reopen's own error is kept too: if it's what keeps failing,
                        // it's the one that says why (ai1249's N1 on #1774).
                        keep(&mut out, format!("reopen: {:?}: {e}", e.code()));
                        // Wait out a writer restart or close without spinning.
                        std::thread::sleep(POLL);
                    }
                }
            }
        }
    }
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
    stop: Arc<Stop>,
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
    let stop = Arc::new(Stop::default());
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
                if stop.draining() {
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
    /// Stops every subscriber once it has covered the phase's last commit (bounded by
    /// [`DRAIN_WAIT`]), and reports them with the feed's cost over the phase.
    pub(crate) fn stop(mut self) -> Result<FeedReport, String> {
        let after = Counters::read(&self.db);
        // The phase's writers are done: every subscriber delivers through the last commit.
        let through = self.db.visible_seqno();
        let drained = Instant::now();
        self.stop.drain(through);
        if let Some(r) = &mut self.readers {
            r.drain(through);
        }
        let (mut expired, mut errors, mut drain_timeouts) = (0, 0, 0);
        let mut error_messages = Vec::new();
        // Each entry names its subscriber's kind (ai1249's N2 on #1774), as a reader
        // process's does.
        let mut join = |kind: &str,
                        handles: Vec<JoinHandle<Followed>>|
         -> Result<(u32, Histogram), String> {
            let n = handles.len() as u32;
            let mut hist = Histogram::new();
            for h in handles {
                let f = h.join().map_err(|_| "a feed subscriber panicked")?;
                hist.merge(&f.hist);
                expired += f.expired;
                errors += f.errors;
                drain_timeouts += f.drain_timeouts;
                error_messages.extend(f.error_messages.into_iter().map(|m| format!("{kind}: {m}")));
            }
            Ok((n, hist))
        };
        let (durable_subscribers, durable) = join("durable", std::mem::take(&mut self.durable))?;
        let (visible_subscribers, visible) = join("visible", std::mem::take(&mut self.visible))?;
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
            drain_timeouts,
            background_syncs: after.background.saturating_sub(self.before.background),
            group_syncs: after.group.saturating_sub(self.before.group),
            sidecar_syncs: after.sidecar.saturating_sub(self.before.sidecar),
            retained_taken: after.taken.saturating_sub(self.before.taken),
            retained_wal_peak,
            held_blob_peak,
            error_messages,
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
            report.drain_timeouts += f.drain_timeouts;
            report.error_messages.extend(
                f.error_messages
                    .into_iter()
                    .map(|m| format!("reader process: {m}")),
            );
        }
        report.drain_ms = drained.elapsed().as_millis() as u64;
        Ok(report)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // A run that failed before `stop` leaves no subscriber behind.
        self.stop.drain(0);
    }
}
