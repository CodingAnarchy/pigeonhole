//! D331 PR 4b: a range move's copy and catch-up, the target's side (the range follower).
//!
//! The node owning a range move's target runs one follower per move, from COPYING until it
//! has applied the source's history through the move's `final_seqno` (HANDED_OVER). It
//! copies the range chunk by chunk ([`super::range_copy`]), then applies the source's
//! history of it from the copy's first token ([`super::range_feed`]).
//!
//! - **Writes** (cli692's M1): every commit is a feed-silent [`pigeonhole::LoggedBatch`]
//!   written through the target's [`WriteGate`], under its lease, so two followers never
//!   write the range, even across a failover of the target mid-move.
//! - **Progress** (in `__phdb_cluster`'s `moved` family, beside the moved-away and serving
//!   rows, which every reader takes by their own prefixes, M4): `copy/<id>` holds the next
//!   row to ask for, the copy's first token, the chunk count and `done`; `copy/<id>/<n>`
//!   holds record `n`'s end row and snapshot seqno; `follow/<id>` the catch-up's token.
//! - **Nothing is copied twice** (cli692's M-a on #2107): a chunk is written as commits of
//!   whole rows of about 256 KiB, each with its own record (ending just past its last row)
//!   and the head's `next`, so a resume asks only after the last row written. A row too big
//!   for one commit, or holding a cell twice at one timestamp (D34), spans several under the
//!   head's `partial`. A stop waits for such a row unless much of it is left or it takes too
//!   long; a crash or such a stop leaves `partial`, and the resume stops the move, naming the
//!   row, rather than copy it again.
//! - **The catch-up** (B4): a change to row `k` is applied only if its seqno is above the
//!   snapshot of `k`'s chunk, which already holds everything at or below it.
//! - **Stopping** (simp724's M2, cli692's M2): on a map that no longer gives the move, or
//!   the target, to this node, the follower stops between commits, never inside one; PR 5's
//!   abort waits for it before it drops the range.
//! - **Failures** retry with a backoff (a missing family on the target is the schema
//!   reconcile's lag, B3), except those that can't heal: a resume past the source's history
//!   (`FeedExpired`), a range drop of the range on the source, or a row left partly copied.
//!   Those are reported, and the follower stops until the move is aborted.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use futures_core::Stream;
use pigeonhole::replication::{CopiedCell, CopiedChunk};
use pigeonhole::{ChangeKind, Family, Pigeonhole};
use tokio::sync::watch;

use super::identity;
use crate::serve::Shared;
use crate::serve::auth::Caller;
use crate::serve::errors::ApiError;
use crate::serve::gate::WriteGate;
use crate::serve::proto::{self, RangeMove};

/// The progress rows' family in `__phdb_cluster` (the moved-away rows').
const FAMILY: &str = "moved";

/// What a chunk asks the source to read.
const CHUNK_BYTES: u64 = 2 << 20;

/// The most a copy commit writes (perf287): a chunk is cut into commits of about this
/// much at cell boundaries.
const SUB_COMMIT: usize = 256 << 10;

/// The most of a row a stopped follower still writes before it honours the stop.
const ROW_FINISH: usize = 4 * SUB_COMMIT;

/// The least time between two catch-up commits that only move its token.
const TOKEN_EVERY: Duration = Duration::from_secs(1);

/// How often the catch-up looks at the map for the move's `final_seqno` (a map read
/// copies it: not per event).
const MAP_TICK: Duration = Duration::from_millis(200);

const MIN_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// One move's follower, as status and PR 5's driver read it.
#[derive(Debug, Default)]
pub(crate) struct Progress {
    /// Bytes of the range copied so far (`MoveStatus.copied_bytes`).
    pub copied_bytes: AtomicU64,
    /// The copy is done (`MoveStatus.copy_done`).
    pub copy_done: AtomicBool,
    /// The source's seqno the catch-up has passed (`MoveStatus.source_applied_seqno`).
    pub applied: AtomicU64,
    /// How far behind the source's commits the catch-up applies, in milliseconds.
    pub lag_ms: AtomicU64,
    /// It applied the source's history through the move's `final_seqno`.
    pub done: AtomicBool,
    /// Why it stopped for good, if it did (the move must be aborted).
    pub failed: Mutex<Option<String>>,
    /// What it waits for before it copies (`MoveStatus.follower_waiting`, D331 PR 5): an
    /// earlier move's drop of an overlapping range here. Empty when it waits for nothing.
    pub waiting: Mutex<String>,
}

/// The range followers this node runs, by move id.
#[derive(Debug, Default)]
pub(crate) struct Followers {
    /// Set by [`stop_all`](Self::stop_all): a map installed during shutdown starts nothing
    /// (cli692's N1 on #2107).
    closed: AtomicBool,
    running: Mutex<HashMap<String, Running>>,
    /// Stopped followers' tasks, until they end: a later follower of the same move waits
    /// for its predecessor first, so two never write the range at once (M1).
    stopping: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

#[derive(Debug)]
struct Running {
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    progress: Arc<Progress>,
}

impl Followers {
    /// After an install of `map`: starts a follower for each range move whose target `map`
    /// gives this node (`me`), from COPYING through HANDED_OVER, and stops those of moves it
    /// no longer does (M2). A stopped follower finishes the commit it's in (it stops only
    /// between commits); a later follower of the same move waits for it to end first. PR 5's
    /// abort adds a wait for it before it drops the range (simp724's M2).
    pub(crate) fn sync(&self, map: &proto::ClusterMap, me: &str, shared: &Arc<Shared>) {
        // Every follower runs on the server's runtime: without one (no follower started),
        // there's nothing to stop either.
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let wanted: Vec<&RangeMove> = map
            .moves
            .iter()
            .filter(|m| {
                !super::map::node_move(m)
                    && super::map::follows(super::map::phase(m))
                    && map.partitions.iter().any(|p| p.id == m.to && p.node == me)
            })
            .collect();
        let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        // Under the lock: `stop_all` sets `closed` before it takes it, so either it drains
        // what this adds, or this sees `closed`.
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let mut stopping = self.stopping.lock().unwrap_or_else(PoisonError::into_inner);
        stopping.retain(|_, t| !t.is_finished());
        let gone: Vec<String> = running
            .keys()
            .filter(|id| !wanted.iter().any(|m| &&m.id == id))
            .cloned()
            .collect();
        for id in gone {
            let Some(r) = running.remove(&id) else {
                continue;
            };
            r.stop.send_replace(true);
            stopping.insert(id, r.task);
        }
        for m in wanted {
            if running.contains_key(&m.id) {
                continue;
            }
            let (stop, rx) = watch::channel(false);
            let progress = Arc::new(Progress::default());
            let follower = Follower {
                shared: Arc::downgrade(shared),
                id: m.id.clone(),
                stop: rx,
                progress: Arc::clone(&progress),
                after: stopping.remove(&m.id),
                #[cfg(test)]
                asked: 0,
            };
            let task = rt.spawn(follower.run());
            running.insert(
                m.id.clone(),
                Running {
                    stop,
                    task,
                    progress,
                },
            );
        }
    }

    /// Stops every follower and waits until each has (an apply in flight finishes first): at
    /// the server's shutdown, before the partitions' databases close.
    pub(crate) async fn stop_all(&self) {
        self.closed.store(true, Ordering::Release);
        let stopped: Vec<Running> = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(_, r)| r)
            .collect();
        for r in &stopped {
            r.stop.send_replace(true);
        }
        let earlier: Vec<tokio::task::JoinHandle<()>> = self
            .stopping
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(_, t)| t)
            .collect();
        for task in stopped.into_iter().map(|r| r.task).chain(earlier) {
            let _ = task.await;
        }
    }

    /// Waits until move `id`'s stopped follower here has ended (D331 PR 5, simp724's M2):
    /// an abort's drop of its range runs after its last commit. A follower still wanted
    /// (the map still gives it the move) isn't waited for.
    pub(crate) async fn join(&self, id: &str) {
        if self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(id)
        {
            return;
        }
        let task = self
            .stopping
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id);
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// Move `id`'s follower here, if one runs or ran.
    pub(crate) fn progress(&self, id: &str) -> Option<Arc<Progress>> {
        self.running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .map(|r| Arc::clone(&r.progress))
    }

    /// `pigeonhole_cluster_move_follower_lag_ms`, per move followed here.
    pub(crate) fn render_metrics(&self, text: &mut String) {
        use std::fmt::Write as _;
        let running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        if running.is_empty() {
            return;
        }
        let _ = writeln!(
            text,
            "# HELP pigeonhole_cluster_move_follower_lag_ms How far a range move's catch-up \
             on its target is behind the source's commits (D331).\n# TYPE \
             pigeonhole_cluster_move_follower_lag_ms gauge"
        );
        for (id, r) in running.iter() {
            let _ = writeln!(
                text,
                "pigeonhole_cluster_move_follower_lag_ms{{move=\"{id}\"}} {}",
                r.progress.lag_ms.load(Ordering::Relaxed)
            );
        }
    }
}

/// One move's follower task.
struct Follower {
    shared: Weak<Shared>,
    id: String,
    stop: watch::Receiver<bool>,
    progress: Arc<Progress>,
    /// A stopped follower of the same move, still finishing a commit: waited for first.
    after: Option<tokio::task::JoinHandle<()>>,
    /// Chunks this follower has asked for (the tests' hold seams count them).
    #[cfg(test)]
    asked: u64,
}

/// What a step left to do.
enum Step {
    More,
    Done,
}

/// A step's failure: retried, or for good.
enum Failed {
    Retry(ApiError),
    Stop(String),
}

impl From<ApiError> for Failed {
    fn from(e: ApiError) -> Self {
        if e.code == "MoveLapsed" {
            return Failed::Stop(e.message);
        }
        if e.code == "FeedExpired" {
            Failed::Stop(format!(
                "the source's history no longer reaches the copy's position: {}",
                e.message
            ))
        } else {
            Failed::Retry(e)
        }
    }
}

impl From<pigeonhole::Error> for Failed {
    fn from(e: pigeonhole::Error) -> Self {
        Failed::from(ApiError::from(e))
    }
}

/// What a step works on: the move as the map has it, and the target served here.
struct Ctx {
    shared: Arc<Shared>,
    m: RangeMove,
    db: Pigeonhole,
    gate: Arc<WriteGate>,
}

impl Follower {
    async fn run(mut self) {
        if let Some(earlier) = self.after.take() {
            let _ = earlier.await;
        }
        let mut backoff = MIN_BACKOFF;
        let mut last_error = String::new();
        loop {
            if *self.stop.borrow() {
                return;
            }
            match self.step().await {
                Ok(Step::More) => backoff = MIN_BACKOFF,
                Ok(Step::Done) => {
                    self.progress.done.store(true, Ordering::Release);
                    return;
                }
                Err(Failed::Stop(why)) => {
                    eprintln!(
                        "phdb serve: range move {:?}'s follower stopped: {why}; the move must be \
                         aborted",
                        self.id
                    );
                    *self
                        .progress
                        .failed
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner) = Some(why);
                    return;
                }
                Err(Failed::Retry(e)) => {
                    if e.message != last_error {
                        eprintln!(
                            "phdb serve: range move {:?}'s follower: {}; it tries again",
                            self.id, e.message
                        );
                        last_error = e.message.clone();
                    }
                    if self.pause(backoff).await {
                        return;
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    /// Waits `d`, or until stopped (true).
    async fn pause(&mut self, d: Duration) -> bool {
        let stopped = tokio::select! {
            _ = self.stop.wait_for(|s| *s) => true,
            () = tokio::time::sleep(d) => false,
        };
        stopped || *self.stop.borrow()
    }

    /// The move and its target, now: `Retry` until the target is open and leased here.
    fn ctx(&self) -> Result<Ctx, Failed> {
        let shared = self
            .shared
            .upgrade()
            .ok_or_else(|| Failed::Stop("the server stopped".into()))?;
        let cluster = shared
            .router
            .cluster
            .as_ref()
            .ok_or_else(|| Failed::Stop("not a cluster node".into()))?;
        let map = cluster.node.get_map()?;
        let m = map
            .moves
            .iter()
            .find(|m| m.id == self.id)
            .cloned()
            .ok_or_else(|| Failed::Stop("the map no longer has the move".into()))?;
        // cli692's S2 on #2107: the source failed over (its epoch moved off the one the copy
        // began at): its history from the copy's token is another's, and the move must be
        // aborted. A record from before `from_epoch` existed (0) isn't compared.
        if let Some(p) = map.partitions.iter().find(|p| p.id == m.from)
            && !super::map::source_epoch_holds(&m, p.epoch)
        {
            return Err(Failed::Stop(format!(
                "the source {:?} is at epoch {}, not {} as when the copy began (it failed \
                 over)",
                m.from, p.epoch, m.from_epoch
            )));
        }
        let service = cluster.here(&m.to).ok_or_else(|| {
            Failed::Retry(ApiError::partition_unavailable(format!(
                "its target {:?} isn't open here yet",
                m.to
            )))
        })?;
        let gate = service
            .gate()
            .cloned()
            .ok_or_else(|| Failed::Stop("its target has no write gate".into()))?;
        if !gate.leased() {
            return Err(Failed::Retry(ApiError::partition_unavailable(format!(
                "its target {:?} holds no lease here yet",
                m.to
            ))));
        }
        let db = service.part()?.db.clone();
        drop(map);
        Ok(Ctx {
            shared,
            m,
            db,
            gate,
        })
    }

    async fn step(&mut self) -> Result<Step, Failed> {
        let cx = self.ctx()?;
        // #2141: an aborted record's drop under this move's own id runs to its end first, as
        // it deletes the progress rows `read_head` would read, and the range with them.
        if super::range_drop::pending_own(&cx.db, &self.id)
            .map_err(|e| Failed::Retry(ApiError::internal(e)))?
        {
            let why = format!(
                "waiting for an earlier drop of move {:?}'s own copy here (an aborted record \
                 of the same id)",
                self.id
            );
            *self
                .progress
                .waiting
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = why.clone();
            return Err(Failed::Retry(ApiError::partition_unavailable(why)));
        }
        let head = read_head(&cx.db, &self.id)?;
        self.progress
            .copied_bytes
            .store(head.bytes, Ordering::Relaxed);
        if !head.done {
            if head.n == 0 && head.partial.is_none() {
                // D331 PR 5 (cli692's Must 4): an earlier move's drop of an overlapping range
                // here runs to its end before this copy begins, said in the status.
                let waiting = super::range_drop::pending_overlap(&cx.db, &cx.m)
                    .map_err(|e| Failed::Retry(ApiError::internal(e)))?;
                *self
                    .progress
                    .waiting
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = waiting
                    .as_ref()
                    .map(|id| {
                        format!("waiting for move {id:?}'s drop of an overlapping range here")
                    })
                    .unwrap_or_default();
                if let Some(id) = waiting {
                    return Err(Failed::Retry(ApiError::partition_unavailable(format!(
                        "waiting for move {id:?}'s drop of an overlapping range here"
                    ))));
                }
                // The range, so a reconcile finds it after the map dropped the move.
                record_range(&cx.db, &cx.m)?;
            }
            self.copy_chunk(&cx, &head).await?;
            return Ok(Step::More);
        }
        self.progress.copy_done.store(true, Ordering::Relaxed);
        self.catch_up(&cx, &head).await
    }

    /// Asks the source for the chunk after `head` and writes it ([`build_commits`]), each
    /// commit with its own progress, so nothing is ever copied twice (cli692's M-a on
    /// #2107).
    async fn copy_chunk(&mut self, cx: &Ctx, head: &Head) -> Result<(), Failed> {
        if let Some(row) = &head.partial {
            return Err(Failed::Stop(format!(
                "a crash (or a stop past its bound) left row {:?} of table {:?} partly copied: \
                 abort the move and run it again",
                String::from_utf8_lossy(row),
                cx.m.table
            )));
        }
        let req = proto::RangeCopyRequest {
            move_id: self.id.clone(),
            table: cx.m.table.clone(),
            after: head.next.clone().unwrap_or_default().into(),
            max_bytes: CHUNK_BYTES,
        };
        #[cfg(test)]
        let asked = {
            self.asked += 1;
            hold_at(&HOLD_CHUNK, &cx.shared, self.asked, &mut self.stop).await;
            self.asked
        };
        let resp = ask_chunk(&cx.shared, &cx.m.from, req).await?;
        if head.first.is_empty() && resp.token.is_empty() {
            return Err(Failed::Retry(ApiError::internal(
                "the source sent the copy's first chunk without its token",
            )));
        }
        let chunk = CopiedChunk {
            cells: resp
                .cells
                .into_iter()
                .map(|c| {
                    Ok(CopiedCell {
                        family: c.family,
                        kind: change_kind(c.kind)?,
                        row: c.row.to_vec(),
                        qualifier: c.qualifier.to_vec(),
                        ts: c.timestamp,
                        stored: c.stored.to_vec(),
                    })
                })
                .collect::<Result<_, ApiError>>()?,
            next: resp.next.map(|n| n.to_vec()),
        };
        #[cfg(test)]
        hold_at(&HOLD_COMMIT, &cx.shared, asked, &mut self.stop).await;
        let done = chunk.next.is_none();
        let mut commits = build_commits(&chunk.cells);
        if commits.is_empty() {
            // Nothing left in the range (or, never seen, an empty chunk before its end): the
            // progress alone.
            commits.push(Commit {
                cells: Vec::new(),
                step: Step2::Rows(None),
            });
        }
        let mut at = Progressed {
            n: head.n,
            bytes: head.bytes,
            first: head.first.is_empty().then(|| resp.token.to_vec()),
        };
        let last = commits.len() - 1;
        // Per commit inside a row, the bytes of the row from it on.
        let mut row_left = vec![0usize; commits.len()];
        for i in (0..commits.len()).rev() {
            let b: usize = commits[i].cells.iter().map(cell_bytes).sum();
            row_left[i] = match commits[i].step {
                Step2::RowMid | Step2::RowFirst(_) if i + 1 < commits.len() => b + row_left[i + 1],
                _ => b,
            };
        }
        let mut stopped_at: Option<tokio::time::Instant> = None;
        let bound = cx.shared.limits.request_timeout;
        for (i, c) in commits.into_iter().enumerate() {
            #[cfg(test)]
            if matches!(c.step, Step2::RowMid | Step2::RowLast(_)) {
                hold_at(&HOLD_ROW, &cx.shared, asked, &mut self.stop).await;
            }
            if *self.stop.borrow() {
                let inside = matches!(c.step, Step2::RowMid | Step2::RowLast(_));
                // A stop waits for the row it's inside, if little of it is left (simp724) and
                // for at most `bound` (cli692), on the M-a proposal: else `partial` stays, as
                // after a crash.
                let since = *stopped_at.get_or_insert_with(tokio::time::Instant::now);
                if !inside || row_left[i] > ROW_FINISH || since.elapsed() >= bound {
                    return Ok(());
                }
            }
            // The head's `bytes` counts this commit's too.
            let bytes: usize = c.cells.iter().map(cell_bytes).sum();
            at.bytes += bytes as u64;
            let progress = match &c.step {
                Step2::Rows(last_row) => Some(at.record(
                    &self.id,
                    // The copy's last commit: its record reaches the range's end.
                    if i == last && done {
                        None
                    } else {
                        Some(just_past(last_row.as_deref().unwrap_or_default()))
                    },
                    i == last && done,
                    resp.snapshot_seqno,
                )),
                Step2::RowLast(row) => Some(at.record(
                    &self.id,
                    if i == last && done {
                        None
                    } else {
                        Some(just_past(row))
                    },
                    i == last && done,
                    resp.snapshot_seqno,
                )),
                Step2::RowFirst(row) => Some(at.partial(&self.id, row)),
                Step2::RowMid => None,
            };
            self.commit(cx, c.cells, progress).await?;
            self.progress
                .copied_bytes
                .store(at.bytes, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Writes `cells` (copied, by name) and, if any, `progress`, as one feed-silent commit
    /// through the target's gate.
    async fn commit(
        &self,
        cx: &Ctx,
        cells: Vec<CopiedCell>,
        progress: Option<HeadWrite>,
    ) -> Result<(), Failed> {
        let db = cx.db.clone();
        let table = cx.m.table.clone();
        let held = cx.gate.enter()?;
        tokio::task::spawn_blocking(move || {
            let t = db.table(&table)?.open()?;
            let rows = progress_table(&db)?;
            let mut b = db.logged_batch();
            for c in &cells {
                b.push(&t, &c.row, &c.family, c.kind, &c.qualifier, c.ts, &c.stored);
            }
            if let Some(p) = &progress {
                p.push(&mut b, &rows);
            }
            b.mark_feed_silent();
            b.commit().map(|_| ())
        })
        .await
        .map_err(|e| ApiError::internal(format!("a range copy's commit: {e}")))??;
        held.committed()?;
        Ok(())
    }

    /// Applies the source's history of the range from the copy's token (or the catch-up's
    /// own), until the move's `final_seqno` is passed.
    async fn catch_up(&mut self, cx: &Ctx, head: &Head) -> Result<Step, Failed> {
        let chunks = read_chunks(&cx.db, &self.id)?;
        let token = read_follow(&cx.db, &self.id)?.unwrap_or_else(|| head.first.clone());
        if self.finished(&cx.m, &token) {
            return Ok(Step::Done);
        }
        let mut events = open_stream(&cx.shared, &cx.m, token.clone()).await?;
        // The last event's token, and the last one stored (at most once a second when nothing
        // is applied): the end is judged on the former.
        let mut latest = token.clone();
        let mut stored = token;
        let mut stored_at = tokio::time::Instant::now();
        let mut m = cx.m.clone();
        let mut looked = tokio::time::Instant::now();
        loop {
            let ev = tokio::select! {
                biased;
                _ = self.stop.wait_for(|s| *s) => return Ok(Step::More),
                ev = next(&mut events) => ev,
            };
            let ev = match ev {
                Some(Ok(ev)) => ev,
                Some(Err(e)) if e.code == "MovedAway" => {
                    // The source passed the move's last seqno; its last position came first
                    // (feed1248's Q1 on #2107: judged on that, not the last token stored).
                    let m = self.ctx()?.m;
                    if self.finished(&m, &latest) {
                        if stored != latest {
                            self.follow(cx, Vec::new(), latest).await?;
                        }
                        return Ok(Step::Done);
                    }
                    return Err(Failed::Retry(e));
                }
                Some(Err(e)) if e.code == "RangeDropped" => {
                    return Err(Failed::Stop(e.message));
                }
                Some(Err(e)) => return Err(e.into()),
                None => {
                    return Err(Failed::Retry(ApiError::partition_unavailable(
                        "the source's catch-up stream ended",
                    )));
                }
            };
            let token = ev.token.to_vec();
            let seqno = super::token::token_seqno(&token);
            match ev.event {
                Some(proto::feed_event::Event::RawCommits(batch)) => {
                    // One commit for the event's commits, which share no key (the source
                    // batches so, D34), with their token; B4 per commit, the lag per commit.
                    let mut cells: Vec<CopiedCell> = Vec::new();
                    for c in batch.commits {
                        let lag = now_micros().saturating_sub(c.timestamp) / 1000;
                        self.progress.lag_ms.store(lag, Ordering::Relaxed);
                        for ch in c.changes {
                            if c.seqno <= chunk_snapshot(&chunks, &ch.row) {
                                continue;
                            }
                            cells.push(CopiedCell {
                                family: ch.family,
                                kind: change_kind(ch.kind)?,
                                row: ch.row.to_vec(),
                                qualifier: ch.qualifier.to_vec(),
                                ts: ch.timestamp,
                                stored: ch.stored.to_vec(),
                            });
                        }
                    }
                    if !cells.is_empty() || stored_at.elapsed() >= TOKEN_EVERY {
                        self.follow(cx, cells, token.clone()).await?;
                        stored.clone_from(&token);
                        stored_at = tokio::time::Instant::now();
                    }
                }
                Some(proto::feed_event::Event::Progress(_)) => {
                    self.progress.lag_ms.store(0, Ordering::Relaxed);
                    if stored_at.elapsed() >= TOKEN_EVERY {
                        self.follow(cx, Vec::new(), token.clone()).await?;
                        stored.clone_from(&token);
                        stored_at = tokio::time::Instant::now();
                    }
                }
                _ => continue,
            }
            latest.clone_from(&token);
            if let Some(s) = seqno {
                self.progress.applied.store(s, Ordering::Release);
            }
            if looked.elapsed() >= MAP_TICK {
                m = self.ctx()?.m;
                looked = tokio::time::Instant::now();
            }
            if self.finished(&m, &token) {
                if stored != token {
                    self.follow(cx, Vec::new(), token.clone()).await?;
                }
                return Ok(Step::Done);
            }
        }
    }

    /// Whether the catch-up, at `token`, has passed handed-over move `m`'s `final_seqno`.
    fn finished(&self, m: &RangeMove, token: &[u8]) -> bool {
        let at = super::token::token_seqno(token).unwrap_or(0);
        if at > 0 {
            self.progress.applied.fetch_max(at, Ordering::AcqRel);
        }
        super::map::committed(super::map::phase(m)) && m.final_seqno != 0 && at >= m.final_seqno
    }

    /// Writes `cells` (the catch-up's, by name) and the catch-up's token as one feed-silent
    /// commit through the target's gate.
    async fn follow(&self, cx: &Ctx, cells: Vec<CopiedCell>, token: Vec<u8>) -> Result<(), Failed> {
        let db = cx.db.clone();
        let table = cx.m.table.clone();
        let row = [b"follow/".as_slice(), self.id.as_bytes()].concat();
        let held = cx.gate.enter()?;
        tokio::task::spawn_blocking(move || {
            let t = db.table(&table)?.open()?;
            let rows = progress_table(&db)?;
            let mut b = db.logged_batch();
            for c in &cells {
                b.push(&t, &c.row, &c.family, c.kind, &c.qualifier, c.ts, &c.stored);
            }
            put(&mut b, &rows, &row, b"token", &token);
            b.mark_feed_silent();
            b.commit().map(|_| ())
        })
        .await
        .map_err(|e| ApiError::internal(format!("a range catch-up's commit: {e}")))??;
        held.committed()?;
        Ok(())
    }
}

/// One range copy's head row, `copy/<id>`.
#[derive(Debug, Default)]
struct Head {
    /// The row to ask for next: just past the last row written; `None` before the first.
    next: Option<Vec<u8>>,
    /// The source's feed token at the first chunk's snapshot.
    first: Vec<u8>,
    /// Records written (`copy/<id>/<n>`).
    n: u64,
    bytes: u64,
    done: bool,
    /// A row whose commits began and didn't end: left only by a crash, or a stop past its
    /// bound.
    partial: Option<Vec<u8>>,
}

/// Where a chunk's commits have got to: the next record's number, the bytes copied, and
/// the copy's first token while it's still to be written.
struct Progressed {
    n: u64,
    bytes: u64,
    first: Option<Vec<u8>>,
}

impl Progressed {
    /// The progress of a commit that ends at a row boundary: its record (`end` just past its
    /// last row, `None` to the range's end), the head's `next`, and `partial` cleared.
    fn record(&mut self, id: &str, end: Option<Vec<u8>>, done: bool, snap: u64) -> HeadWrite {
        let w = HeadWrite {
            id: id.to_owned(),
            first: self.first.take(),
            step: HeadStep::Record {
                n: self.n,
                end,
                done,
                snap,
            },
            bytes: self.bytes,
        };
        self.n += 1;
        w
    }

    /// The progress of a row's first commit when the row spans several: `partial`.
    fn partial(&mut self, id: &str, row: &[u8]) -> HeadWrite {
        HeadWrite {
            id: id.to_owned(),
            first: self.first.take(),
            step: HeadStep::Partial(row.to_vec()),
            bytes: self.bytes,
        }
    }
}

/// A commit's progress, written in it.
#[derive(Debug, Clone)]
struct HeadWrite {
    id: String,
    /// The copy's first token, on its first commit.
    first: Option<Vec<u8>>,
    step: HeadStep,
    /// The bytes copied through this commit.
    bytes: u64,
}

#[derive(Debug, Clone)]
enum HeadStep {
    /// Record `n`: the rows up to `end` copied at snapshot `snap`; the head's `next` moves
    /// there and `partial` clears.
    Record {
        n: u64,
        end: Option<Vec<u8>>,
        done: bool,
        snap: u64,
    },
    /// A row's commits begin.
    Partial(Vec<u8>),
}

impl HeadWrite {
    fn push(&self, b: &mut pigeonhole::replication::LoggedBatch<'_>, t: &pigeonhole::Table) {
        let head = [b"copy/".as_slice(), self.id.as_bytes()].concat();
        if let Some(first) = &self.first {
            put(b, t, &head, b"first", first);
        }
        put(b, t, &head, b"bytes", self.bytes.to_string().as_bytes());
        match &self.step {
            HeadStep::Record { n, end, done, snap } => {
                let record = chunk_row(&self.id, *n);
                if let Some(end) = end {
                    put(b, t, &head, b"next", end);
                    put(b, t, &record, b"end", end);
                }
                if *done {
                    put(b, t, &head, b"done", b"1");
                }
                put(b, t, &head, b"partial", b"");
                put(b, t, &head, b"n", (n + 1).to_string().as_bytes());
                put(b, t, &record, b"snap", snap.to_string().as_bytes());
            }
            HeadStep::Partial(row) => {
                // Non-empty, so a row with the empty key is told from none.
                put(b, t, &head, b"partial", &[b"r".as_slice(), row].concat());
            }
        }
    }
}

/// The smallest key after `row`: a record ends just past its last row (cli692 on the M-a
/// proposal), never at the source's next row, so a row inserted between them later is
/// another record's.
fn just_past(row: &[u8]) -> Vec<u8> {
    [row, &[0u8]].concat()
}

/// A hold seam (tests): the follower on the node in a listed cluster directory waits,
/// once, before it asks for its listed chunk (counted from 1 by that follower), until the
/// sender sends or is dropped, or the follower is stopped.
#[cfg(test)]
pub(crate) static HOLD_CHUNK: Mutex<Vec<HeldChunk>> = Mutex::new(Vec::new());

/// [`HOLD_CHUNK`]'s seam after the chunk arrived and before its first commit: a stop that
/// comes while it waits is the follower's own to honour, as the source already served it.
#[cfg(test)]
pub(crate) static HOLD_COMMIT: Mutex<Vec<HeldChunk>> = Mutex::new(Vec::new());

/// [`HOLD_CHUNK`]'s seam before a commit inside a row that spans several, in the listed
/// chunk: a stop that comes while it waits meets the row.
#[cfg(test)]
pub(crate) static HOLD_ROW: Mutex<Vec<HeldChunk>> = Mutex::new(Vec::new());

/// [`HOLD_CHUNK`]'s entry: the node's cluster directory and the chunk's number, and the
/// release.
#[cfg(test)]
pub(crate) type HeldChunk = (
    (std::path::PathBuf, u64),
    tokio::sync::oneshot::Receiver<()>,
);

#[cfg(test)]
async fn hold_at(
    seam: &Mutex<Vec<HeldChunk>>,
    shared: &Shared,
    n: u64,
    stop: &mut watch::Receiver<bool>,
) {
    let Some(cluster) = shared.router.cluster.as_ref() else {
        return;
    };
    let wait = {
        let mut armed = seam.lock().unwrap_or_else(PoisonError::into_inner);
        armed
            .iter()
            .position(|((d, k), _)| d == cluster.node.dir() && *k == n)
            .map(|i| armed.swap_remove(i).1)
    };
    if let Some(wait) = wait {
        tokio::select! {
            _ = wait => {}
            _ = stop.wait_for(|s| *s) => {}
        }
    }
}

/// Chunk `n`'s row: numbered so they sort in order.
fn chunk_row(id: &str, n: u64) -> Vec<u8> {
    format!("copy/{id}/{n:020}").into_bytes()
}

/// A logged put of `value` to `row`'s `q` in the progress family.
fn put(
    b: &mut pigeonhole::replication::LoggedBatch<'_>,
    t: &pigeonhole::Table,
    row: &[u8],
    q: &[u8],
    value: &[u8],
) {
    // A plain value's stored form: its tag, then its bytes.
    let stored = [&[0u8][..], value].concat();
    b.push(t, row, FAMILY, ChangeKind::Put, q, now_micros(), &stored);
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

/// `__phdb_cluster` with the progress family.
fn progress_table(db: &Pigeonhole) -> pigeonhole::Result<pigeonhole::Table> {
    db.table(identity::TABLE)?
        .family(FAMILY, Family::default())
        .create_if_missing()
}

fn read_head(db: &Pigeonhole, id: &str) -> Result<Head, ApiError> {
    let t = progress_table(db)?;
    let row = [b"copy/".as_slice(), id.as_bytes()].concat();
    let get = |q: &[u8]| -> Result<Option<Vec<u8>>, ApiError> {
        Ok(t.get(&row, FAMILY, q)?.map(|c| c.value().to_vec()))
    };
    let number = |q: &[u8]| -> Result<u64, ApiError> {
        get(q)?.map_or(Ok(0), |b| {
            std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    ApiError::internal(format!("range copy {id:?}'s progress is malformed"))
                })
        })
    };
    Ok(Head {
        next: get(b"next")?,
        first: get(b"first")?.unwrap_or_default(),
        n: number(b"n")?,
        bytes: number(b"bytes")?,
        done: get(b"done")?.is_some(),
        partial: get(b"partial")?
            .filter(|p| !p.is_empty())
            .map(|p| p[1..].to_vec()),
    })
}

/// Records move `m`'s table and range in its copy's head row (D331 PR 5), unless there: a
/// reconcile reads them once the map no longer has the move.
fn record_range(db: &Pigeonhole, m: &RangeMove) -> Result<(), ApiError> {
    let t = progress_table(db)?;
    let row = [b"copy/".as_slice(), m.id.as_bytes()].concat();
    if t.get(&row, FAMILY, b"table")?.is_some() {
        return Ok(());
    }
    let (start, end) = super::map::move_range(m).map_err(ApiError::internal)?;
    let mut w = t
        .mutate(&row)
        .put(FAMILY, b"table", m.table.as_bytes())
        .put(FAMILY, b"start", &start);
    if let Some(end) = &end {
        w = w.put(FAMILY, b"end", end);
    }
    w.commit()?;
    Ok(())
}

/// A copy's table and range, as its head row records them.
pub(crate) type CopyRange = (String, Vec<u8>, Option<Vec<u8>>);

/// The copies whose progress `db` holds, by move id, each with its range when its head
/// records it (D331 PR 5).
pub(crate) fn copies(db: &Pigeonhole) -> Result<Vec<(String, Option<CopyRange>)>, ApiError> {
    let t = db.table(identity::TABLE)?.open()?;
    if !t.families().contains(&FAMILY) {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for row in t.scan_prefix(b"copy/").family(FAMILY).iter()? {
        let row = row?;
        let id = &row.key()[b"copy/".len()..];
        // A head row only: a record's key is `copy/<id>/<n>`.
        if id.contains(&b'/') {
            continue;
        }
        let cell = |q: &[u8]| row.get(FAMILY, q).map(|c| c.value().to_vec());
        let range = match (cell(b"table"), cell(b"start")) {
            (Some(table), Some(start)) => Some((
                String::from_utf8_lossy(&table).into_owned(),
                start,
                cell(b"end"),
            )),
            _ => None,
        };
        out.push((String::from_utf8_lossy(id).into_owned(), range));
    }
    Ok(out)
}

/// Deletes move `id`'s progress rows (`copy/<id>`, its records and `follow/<id>`).
pub(crate) fn prune(db: &Pigeonhole, id: &str) -> Result<(), ApiError> {
    let t = progress_table(db)?;
    let mut rows = vec![
        [b"copy/".as_slice(), id.as_bytes()].concat(),
        [b"follow/".as_slice(), id.as_bytes()].concat(),
    ];
    let prefix = format!("copy/{id}/").into_bytes();
    for row in t.scan_prefix(&prefix).family(FAMILY).iter()? {
        rows.push(row?.key().to_vec());
    }
    for chunk in rows.chunks(256) {
        let mut b = db.write_batch();
        for r in chunk {
            b.delete_row(&t, r);
        }
        b.commit()?;
    }
    Ok(())
}

/// A copy's chunks, in row order: each one's end row (`None`: to the range's end) and
/// snapshot seqno.
type Chunks = Vec<(Option<Vec<u8>>, u64)>;

/// The copy's records ([`Chunks`]), consecutive ones at one snapshot merged (perf287 on the
/// M-a proposal): a chunk's commits share its snapshot, so B4 needs only the boundaries
/// where the snapshot changes, about one entry per chunk.
fn read_chunks(db: &Pigeonhole, id: &str) -> Result<Chunks, ApiError> {
    let t = progress_table(db)?;
    let prefix = format!("copy/{id}/").into_bytes();
    let mut out = Vec::new();
    for row in t.scan_prefix(&prefix).family(FAMILY).iter()? {
        let row = row?;
        let snap = row
            .get(FAMILY, b"snap")
            .and_then(|c| std::str::from_utf8(c.value()).ok()?.parse().ok())
            .ok_or_else(|| ApiError::internal(format!("range copy {id:?}'s chunk is malformed")))?;
        out.push((row.get(FAMILY, b"end").map(|c| c.value().to_vec()), snap));
    }
    Ok(merge_records(out))
}

/// `records` with consecutive ones at one snapshot merged into the last of them.
fn merge_records(records: Chunks) -> Chunks {
    let mut out: Chunks = Vec::with_capacity(records.len());
    for (end, snap) in records {
        match out.last_mut() {
            Some(last) if last.1 == snap => last.0 = end,
            _ => out.push((end, snap)),
        }
    }
    out
}

/// The catch-up's token, once it has stored one.
fn read_follow(db: &Pigeonhole, id: &str) -> Result<Option<Vec<u8>>, ApiError> {
    let t = progress_table(db)?;
    let row = [b"follow/".as_slice(), id.as_bytes()].concat();
    Ok(t.get(&row, FAMILY, b"token")?.map(|c| c.value().to_vec()))
}

/// B4: the snapshot of the chunk holding `row` (0 past the last: none holds it).
fn chunk_snapshot(chunks: &[(Option<Vec<u8>>, u64)], row: &[u8]) -> u64 {
    let i = chunks.partition_point(|(end, _)| end.as_deref().is_some_and(|e| e <= row));
    chunks.get(i).map_or(0, |(_, s)| *s)
}

/// One commit of a chunk, and what it ends.
#[derive(Debug, Clone, PartialEq)]
struct Commit {
    cells: Vec<CopiedCell>,
    step: Step2,
}

/// Where a chunk's commit ends.
#[derive(Debug, Clone, PartialEq)]
enum Step2 {
    /// Whole rows, the last of them this one (`None`: no row).
    Rows(Option<Vec<u8>>),
    /// The first commit of a row that spans several.
    RowFirst(Vec<u8>),
    /// A middle one.
    RowMid,
    /// Its last.
    RowLast(Vec<u8>),
}

/// `cells` (a chunk's, rows whole, in row order) as its commits (cli692's M-a on #2107):
/// whole rows packed to about [`SUB_COMMIT`]; a row too big for one, or holding a cell
/// twice at one timestamp (D34: one commit keeps only the last), alone in several, cut
/// before the cell that would pass the size or repeat a key.
fn build_commits(cells: &[CopiedCell]) -> Vec<Commit> {
    type Key<'a> = (&'a str, &'a [u8], u64, bool);
    let mut out = Vec::new();
    let mut rows: Vec<CopiedCell> = Vec::new();
    let mut rows_bytes = 0usize;
    let flush = |out: &mut Vec<Commit>, rows: &mut Vec<CopiedCell>, bytes: &mut usize| {
        if let Some(last) = rows.last().map(|c| c.row.clone()) {
            out.push(Commit {
                cells: std::mem::take(rows),
                step: Step2::Rows(Some(last)),
            });
        }
        *bytes = 0;
    };
    for row in cells.chunk_by(|a, b| a.row == b.row) {
        // The row's parts.
        let mut parts: Vec<Vec<CopiedCell>> = vec![Vec::new()];
        let mut seen: std::collections::HashSet<Key<'_>> = std::collections::HashSet::new();
        let mut bytes = 0usize;
        for c in row {
            let key = (
                c.family.as_str(),
                &c.qualifier[..],
                c.ts,
                c.kind == ChangeKind::DeleteFamily,
            );
            let part = parts.last_mut().expect("a part");
            if !part.is_empty() && (seen.contains(&key) || bytes + cell_bytes(c) > SUB_COMMIT) {
                parts.push(Vec::new());
                seen.clear();
                bytes = 0;
            }
            seen.insert(key);
            bytes += cell_bytes(c);
            parts.last_mut().expect("a part").push(c.clone());
        }
        if let [whole] = &parts[..] {
            let b: usize = whole.iter().map(cell_bytes).sum();
            if !rows.is_empty() && rows_bytes + b > SUB_COMMIT {
                flush(&mut out, &mut rows, &mut rows_bytes);
            }
            rows_bytes += b;
            rows.extend(whole.iter().cloned());
            continue;
        }
        flush(&mut out, &mut rows, &mut rows_bytes);
        let key = row[0].row.clone();
        let n = parts.len();
        for (i, cells) in parts.into_iter().enumerate() {
            let step = match i {
                0 => Step2::RowFirst(key.clone()),
                _ if i + 1 == n => Step2::RowLast(key.clone()),
                _ => Step2::RowMid,
            };
            out.push(Commit { cells, step });
        }
    }
    flush(&mut out, &mut rows, &mut rows_bytes);
    out
}

fn cell_bytes(c: &CopiedCell) -> usize {
    c.row.len() + c.qualifier.len() + c.stored.len() + c.family.len() + 16
}

/// A logged kind number (FORMAT §2) as the library's kind.
fn change_kind(k: u32) -> Result<ChangeKind, ApiError> {
    Ok(match k {
        1 => ChangeKind::Put,
        2 => ChangeKind::Merge,
        3 => ChangeKind::DeleteCell,
        4 => ChangeKind::DeleteColumn,
        5 => ChangeKind::DeleteFamily,
        _ => {
            return Err(ApiError::internal(format!(
                "a range move's change of kind {k}"
            )));
        }
    })
}

/// Asks the source `partition`'s owner for a chunk: served here when that's this node.
async fn ask_chunk(
    shared: &Arc<Shared>,
    partition: &str,
    req: proto::RangeCopyRequest,
) -> Result<proto::RangeCopyResponse, ApiError> {
    let cluster = shared
        .router
        .cluster
        .as_ref()
        .ok_or_else(|| ApiError::unsupported("this node isn't a cluster node"))?;
    let (owner, map_version) = owner_of(&cluster.node, partition)?;
    if owner.node == cluster.node.id() {
        return cluster.range_copies.serve(cluster, partition, req).await;
    }
    let fwd = proto::ForwardRequest {
        map_version,
        caller: Some(super::forward::forwarded_caller(&Caller::anonymous())?),
        partition: partition.to_owned(),
        op: Some(proto::forward_request::Op::RangeCopy(req)),
        ..Default::default()
    };
    let timeout = shared.limits.request_timeout;
    match cluster
        .forwarder
        .send(&owner.address, fwd, timeout, false)
        .await?
        .reply
    {
        Some(proto::forward_response::Reply::RangeCopy(r)) => Ok(r),
        _ => Err(ApiError::internal(
            "a source answered a range copy with something else",
        )),
    }
}

fn owner_of(node: &super::Node, partition: &str) -> Result<(super::route::Owner, u64), ApiError> {
    let routes = node.routes();
    let routes = routes
        .as_ref()
        .ok_or_else(|| ApiError::wrong_partition(0, "this node has no cluster map yet"))?;
    let owner = routes.owner(partition).cloned().ok_or_else(|| {
        ApiError::partition_unavailable(format!("the map no longer lists partition {partition:?}"))
    })?;
    Ok((owner, routes.map_version()))
}

/// The catch-up's events, decoded.
type Events = std::pin::Pin<Box<dyn Stream<Item = Result<proto::FeedEvent, ApiError>> + Send>>;

/// Opens move `m`'s catch-up stream on its source from `token`.
async fn open_stream(
    shared: &Arc<Shared>,
    m: &RangeMove,
    token: Vec<u8>,
) -> Result<Events, ApiError> {
    let cluster = shared
        .router
        .cluster
        .as_ref()
        .ok_or_else(|| ApiError::unsupported("this node isn't a cluster node"))?;
    let (owner, map_version) = owner_of(&cluster.node, &m.from)?;
    let req = proto::ForwardSubscribeRequest {
        map_version,
        caller: Some(super::forward::forwarded_caller(&Caller::anonymous())?),
        partition: m.from.clone(),
        request: Some(proto::SubscribeRequest {
            tables: vec![m.table.clone()],
            start: Some(proto::subscribe_request::Start::Resume(token.into())),
            ..Default::default()
        }),
        range_start: m.start.clone(),
        range_end: m.end.clone(),
        raw: true,
        ..Default::default()
    };
    if owner.node == cluster.node.id() {
        let events = super::range_feed::serve(cluster, req).await?;
        return Ok(Box::pin(events));
    }
    let lease = cluster
        .forwarder
        .stream_channel(&owner.address, shared.limits.request_timeout)
        .await?;
    let request = cluster.node.request(req).map_err(ApiError::internal)?;
    let mut grpc = tonic::client::Grpc::new(lease.channel()).max_decoding_message_size(usize::MAX);
    let address = owner.address.clone();
    let failed = |status: tonic::Status| super::forward::classify(&status, &address, false);
    grpc.ready()
        .await
        .map_err(|e| failed(tonic::Status::unavailable(e.to_string())))?;
    let path = tonic::codegen::http::uri::PathAndQuery::from_static(super::feed::FORWARD_SUBSCRIBE);
    let stream = grpc
        .server_streaming(
            request,
            path,
            tonic_prost::ProstCodec::<proto::ForwardSubscribeRequest, proto::FeedEvent>::default(),
        )
        .await
        .map_err(failed)?
        .into_inner();
    Ok(Box::pin(Remote {
        stream,
        address: owner.address,
        _lease: lease,
    }))
}

/// A remote catch-up stream, holding its connection.
struct Remote {
    stream: tonic::Streaming<proto::FeedEvent>,
    address: String,
    _lease: super::forward::StreamLease,
}

impl Stream for Remote {
    type Item = Result<proto::FeedEvent, ApiError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = &mut *self;
        std::pin::Pin::new(&mut this.stream)
            .poll_next(cx)
            .map(|item| {
                item.map(|r| r.map_err(|s| super::forward::classify(&s, &this.address, false)))
            })
    }
}

async fn next(events: &mut Events) -> Option<Result<proto::FeedEvent, ApiError>> {
    std::future::poll_fn(|cx| events.as_mut().poll_next(cx)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rows_chunk_is_the_first_whose_end_is_past_it() {
        let chunks = vec![
            (Some(b"m".to_vec()), 10),
            (Some(b"t".to_vec()), 20),
            (None, 30),
        ];
        assert_eq!(chunk_snapshot(&chunks, b"a"), 10);
        assert_eq!(chunk_snapshot(&chunks, b"l"), 10);
        // An end is exclusive: the next chunk starts there.
        assert_eq!(chunk_snapshot(&chunks, b"m"), 20);
        assert_eq!(chunk_snapshot(&chunks, b"t"), 30);
        assert_eq!(chunk_snapshot(&chunks, b"zzz"), 30);
        let bounded = vec![(Some(b"m".to_vec()), 10)];
        assert_eq!(chunk_snapshot(&bounded, b"m"), 0, "past the last chunk");
    }

    fn cell(row: &str, kind: ChangeKind, bytes: usize) -> CopiedCell {
        CopiedCell {
            family: "f".into(),
            kind,
            row: row.as_bytes().to_vec(),
            qualifier: b"q".to_vec(),
            ts: 1,
            stored: vec![0; bytes],
        }
    }

    #[test]
    fn whole_rows_are_packed_to_the_cap_and_cut_only_between_rows() {
        let cells: Vec<CopiedCell> = (0..10)
            .flat_map(|i| {
                let row = format!("r{i}");
                [
                    cell(&row, ChangeKind::Put, SUB_COMMIT / 6),
                    CopiedCell {
                        qualifier: b"q2".to_vec(),
                        ..cell(&row, ChangeKind::Put, SUB_COMMIT / 6)
                    },
                ]
            })
            .collect();
        let commits = build_commits(&cells);
        assert!(commits.len() >= 4, "{}", commits.len());
        assert_eq!(commits.iter().map(|c| c.cells.len()).sum::<usize>(), 20);
        for c in &commits {
            assert!(c.cells.iter().map(cell_bytes).sum::<usize>() <= SUB_COMMIT);
            let Step2::Rows(Some(last)) = &c.step else {
                panic!("{:?}", c.step)
            };
            assert_eq!(&c.cells.last().unwrap().row, last);
            // Both of a row's cells in one commit.
            assert_eq!(c.cells.len() % 2, 0);
        }
    }

    #[test]
    fn a_big_row_or_one_with_a_repeated_key_spans_commits_marked_as_a_row() {
        // r1 three times the cap, between two small rows.
        let mut cells = vec![cell("r0", ChangeKind::Put, 8)];
        cells.extend((0..3).map(|i| CopiedCell {
            qualifier: format!("q{i}").into_bytes(),
            ..cell("r1", ChangeKind::Put, SUB_COMMIT - 64)
        }));
        cells.push(cell("r2", ChangeKind::Put, 8));
        let steps: Vec<Step2> = build_commits(&cells).into_iter().map(|c| c.step).collect();
        assert_eq!(
            steps,
            [
                Step2::Rows(Some(b"r0".to_vec())),
                Step2::RowFirst(b"r1".to_vec()),
                Step2::RowMid,
                Step2::RowLast(b"r1".to_vec()),
                Step2::Rows(Some(b"r2".to_vec())),
            ]
        );
        // A value beside its delete at one timestamp (D34): two commits.
        let mut del = cell("r1", ChangeKind::DeleteCell, 0);
        del.stored.clear();
        let cells = vec![del, cell("r1", ChangeKind::Put, 8)];
        let steps: Vec<Step2> = build_commits(&cells).into_iter().map(|c| c.step).collect();
        assert_eq!(
            steps,
            [
                Step2::RowFirst(b"r1".to_vec()),
                Step2::RowLast(b"r1".to_vec())
            ]
        );
    }

    #[test]
    fn merged_records_answer_b4_as_the_full_list_does() {
        // perf287 on the M-a proposal: records of one chunk share its snapshot.
        let mut x = 0x2107u64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..50 {
            let mut full: Chunks = Vec::new();
            let mut snap = 1;
            for k in 0..(next() % 40 + 1) {
                if next() % 3 == 0 {
                    snap += 1 + next() % 5;
                }
                full.push((Some(format!("k{k:03}").into_bytes()), snap));
            }
            if next() % 2 == 0 {
                full.last_mut().unwrap().0 = None;
            }
            let merged = merge_records(full.clone());
            assert!(merged.len() <= full.len());
            for k in 0..45 {
                for row in [
                    format!("k{k:03}"),
                    format!("k{k:03}\0"),
                    format!("k{k:03}a"),
                ] {
                    assert_eq!(
                        chunk_snapshot(&full, row.as_bytes()),
                        chunk_snapshot(&merged, row.as_bytes()),
                        "{row} in {full:?}"
                    );
                }
            }
        }
    }
}
