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
//!   holds chunk `n`'s end row and snapshot seqno; `follow/<id>` the catch-up's token. A
//!   chunk's progress goes in its last commit only (simp724): a crash between two of its
//!   commits copies the whole chunk again at a fresh snapshot, which
//!   [`CopiedChunk::batches`] makes safe.
//! - **A chunk holding a merge** commits whole (its D34 split only): a named operator's
//!   operands apply twice if a crash splits them (simp724's (b)). One past
//!   [`MERGE_CHUNK_CAP`] stops the move, naming the table, family and row.
//! - **The catch-up** (B4): a change to row `k` is applied only if its seqno is above the
//!   snapshot of `k`'s chunk, which already holds everything at or below it.
//! - **Stopping** (simp724's M2, cli692's M2): on a map that no longer gives the move, or
//!   the target, to this node, the follower stops between commits, never inside one; PR 5's
//!   abort waits for it before it drops the range.
//! - **Failures** retry with a backoff (a missing family on the target is the schema
//!   reconcile's lag, B3), except those that can't heal: a resume past the source's history
//!   (`FeedExpired`), a range drop of the range on the source, or a chunk past the merge
//!   cap. Those are reported, and the follower stops until the move is aborted.

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
use crate::serve::proto::{self, MovePhase, RangeMove};

/// The progress rows' family in `__phdb_cluster` (the moved-away rows').
const FAMILY: &str = "moved";

/// What a chunk asks the source to read.
const CHUNK_BYTES: u64 = 2 << 20;

/// The most a copy commit writes (perf287): a chunk is cut into commits of about this
/// much at cell boundaries.
const SUB_COMMIT: usize = 256 << 10;

/// The most a chunk holding a merge may write in its one commit (`k` × the read unit).
const MERGE_CHUNK_CAP: usize = 4 * CHUNK_BYTES as usize;

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
}

/// The range followers this node runs, by move id.
#[derive(Debug, Default)]
pub(crate) struct Followers {
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
                    && (MovePhase::Copying..=MovePhase::HandedOver).contains(&super::map::phase(m))
                    && map.partitions.iter().any(|p| p.id == m.to && p.node == me)
            })
            .collect();
        let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
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
        let head = read_head(&cx.db, &self.id)?;
        self.progress
            .copied_bytes
            .store(head.bytes, Ordering::Relaxed);
        if !head.done {
            self.copy_chunk(&cx, &head).await?;
            return Ok(Step::More);
        }
        self.progress.copy_done.store(true, Ordering::Relaxed);
        self.catch_up(&cx, &head).await
    }

    /// Asks the source for the chunk after `head` and writes it, its progress in its last
    /// commit.
    async fn copy_chunk(&mut self, cx: &Ctx, head: &Head) -> Result<(), Failed> {
        let req = proto::RangeCopyRequest {
            move_id: self.id.clone(),
            table: cx.m.table.clone(),
            after: head.next.clone().unwrap_or_default().into(),
            max_bytes: CHUNK_BYTES,
        };
        #[cfg(test)]
        hold_at(&HOLD_CHUNK, &cx.shared, head.n, &mut self.stop).await;
        let resp = ask_chunk(&cx.shared, &cx.m.from, req).await?;
        let first = if head.first.is_empty() {
            if resp.token.is_empty() {
                return Err(Failed::Retry(ApiError::internal(
                    "the source sent the copy's first chunk without its token",
                )));
            }
            resp.token.to_vec()
        } else {
            head.first.clone()
        };
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
        hold_at(&HOLD_COMMIT, &cx.shared, head.n, &mut self.stop).await;
        let bytes: usize = chunk.cells.iter().map(cell_bytes).sum();
        let commits = sub_commits(&chunk);
        if chunk.cells.iter().any(|c| c.kind == ChangeKind::Merge) && bytes > MERGE_CHUNK_CAP {
            let c = chunk
                .cells
                .iter()
                .find(|c| c.kind == ChangeKind::Merge)
                .expect("a merge");
            return Err(Failed::Stop(format!(
                "a chunk of table {:?} holding merges (family {:?}, row {:?}) is {bytes} bytes, \
                 past the {MERGE_CHUNK_CAP} a chunk with merges may commit at once",
                cx.m.table,
                c.family,
                String::from_utf8_lossy(&c.row)
            )));
        }
        let progress = HeadWrite {
            id: self.id.clone(),
            n: head.n,
            next: chunk.next.clone(),
            first: (head.first.is_empty()).then_some(first),
            bytes: head.bytes + bytes as u64,
            snapshot: resp.snapshot_seqno,
        };
        let last = commits.len().saturating_sub(1);
        if commits.is_empty() {
            if *self.stop.borrow() {
                return Ok(());
            }
            self.commit(cx, Vec::new(), Some(progress)).await?;
        } else {
            for (i, cells) in commits.into_iter().enumerate() {
                if *self.stop.borrow() {
                    // Stopped before a commit of the chunk: the next follower copies it again,
                    // whole.
                    return Ok(());
                }
                let p = (i == last).then(|| progress.clone());
                self.commit(cx, cells, p).await?;
            }
        }
        self.progress
            .copied_bytes
            .store(head.bytes + bytes as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Writes `cells` (copied, by name) and, if any, the chunk's progress, as one
    /// feed-silent commit through the target's gate.
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
                    // The source passed the move's last seqno; its last position came first.
                    let m = self.ctx()?.m;
                    if self.finished(&m, &stored) {
                        return Ok(Step::Done);
                    }
                    return Err(Failed::Retry(e));
                }
                Some(Err(e))
                    if e.code == "FailedPrecondition" && e.message.contains("range drop") =>
                {
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
                Some(proto::feed_event::Event::RawCommit(c)) => {
                    let cells: Vec<CopiedCell> = c
                        .changes
                        .into_iter()
                        .filter(|ch| c.seqno > chunk_snapshot(&chunks, &ch.row))
                        .map(|ch| {
                            Ok(CopiedCell {
                                family: ch.family,
                                kind: change_kind(ch.kind)?,
                                row: ch.row.to_vec(),
                                qualifier: ch.qualifier.to_vec(),
                                ts: ch.timestamp,
                                stored: ch.stored.to_vec(),
                            })
                        })
                        .collect::<Result<_, ApiError>>()?;
                    let lag = now_micros().saturating_sub(c.timestamp) / 1000;
                    self.progress.lag_ms.store(lag, Ordering::Relaxed);
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
        super::map::phase(m) >= MovePhase::HandedOver && m.final_seqno != 0 && at >= m.final_seqno
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
    /// The row to ask for next; `None` before the first chunk.
    next: Option<Vec<u8>>,
    /// The source's feed token at the first chunk's snapshot.
    first: Vec<u8>,
    /// Chunks written.
    n: u64,
    bytes: u64,
    done: bool,
}

/// A chunk's progress, written in its last commit.
#[derive(Debug, Clone)]
struct HeadWrite {
    id: String,
    /// The chunk's number (the head's count before it).
    n: u64,
    /// The source's next row; `None`: the copy is done.
    next: Option<Vec<u8>>,
    /// The copy's first token, on its first chunk.
    first: Option<Vec<u8>>,
    bytes: u64,
    snapshot: u64,
}

impl HeadWrite {
    fn push(&self, b: &mut pigeonhole::replication::LoggedBatch<'_>, t: &pigeonhole::Table) {
        let head = [b"copy/".as_slice(), self.id.as_bytes()].concat();
        let chunk = chunk_row(&self.id, self.n);
        match &self.next {
            Some(next) => put(b, t, &head, b"next", next),
            None => put(b, t, &head, b"done", b"1"),
        }
        if let Some(first) = &self.first {
            put(b, t, &head, b"first", first);
        }
        put(b, t, &head, b"n", (self.n + 1).to_string().as_bytes());
        put(b, t, &head, b"bytes", self.bytes.to_string().as_bytes());
        if let Some(next) = &self.next {
            put(b, t, &chunk, b"end", next);
        }
        put(b, t, &chunk, b"snap", self.snapshot.to_string().as_bytes());
    }
}

/// A hold seam (tests): the follower on the node in a listed cluster directory waits,
/// once, before it asks for the listed chunk, until the sender sends or is dropped, or the
/// follower is stopped (then it commits nothing more).
#[cfg(test)]
pub(crate) static HOLD_CHUNK: Mutex<Vec<HeldChunk>> = Mutex::new(Vec::new());

/// [`HOLD_CHUNK`]'s seam after the chunk arrived and before its first commit: a stop that
/// comes while it waits is the follower's own to honour, as the source already served it.
#[cfg(test)]
pub(crate) static HOLD_COMMIT: Mutex<Vec<HeldChunk>> = Mutex::new(Vec::new());

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
    })
}

/// A copy's chunks, in row order: each one's end row (`None`: to the range's end) and
/// snapshot seqno.
type Chunks = Vec<(Option<Vec<u8>>, u64)>;

/// The copy's chunks ([`Chunks`]).
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
    Ok(out)
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

/// `chunk` as its commits: its D34 split, then (a chunk holding no merge) cut further at
/// cell boundaries into commits of about [`SUB_COMMIT`].
fn sub_commits(chunk: &CopiedChunk) -> Vec<Vec<CopiedCell>> {
    let merges = chunk.cells.iter().any(|c| c.kind == ChangeKind::Merge);
    let mut out = Vec::new();
    for batch in chunk.batches() {
        if merges {
            out.push(batch.to_vec());
            continue;
        }
        let mut cur = Vec::new();
        let mut bytes = 0;
        for c in batch {
            if !cur.is_empty() && bytes + cell_bytes(c) > SUB_COMMIT {
                out.push(std::mem::take(&mut cur));
                bytes = 0;
            }
            bytes += cell_bytes(c);
            cur.push(c.clone());
        }
        if !cur.is_empty() {
            out.push(cur);
        }
    }
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
    fn a_chunk_is_cut_into_commits_of_about_the_cap_unless_it_holds_a_merge() {
        let chunk = CopiedChunk {
            cells: (0..10)
                .map(|i| cell(&format!("r{i}"), ChangeKind::Put, SUB_COMMIT / 3))
                .collect(),
            next: None,
        };
        let commits = sub_commits(&chunk);
        assert!(commits.len() >= 4, "{}", commits.len());
        assert_eq!(commits.iter().map(Vec::len).sum::<usize>(), 10);
        assert!(
            commits
                .iter()
                .all(|c| c.iter().map(cell_bytes).sum::<usize>() <= SUB_COMMIT)
        );
        let mut merged = chunk.clone();
        merged.cells[3].kind = ChangeKind::Merge;
        assert_eq!(
            sub_commits(&merged).len(),
            1,
            "a chunk with a merge commits once"
        );
    }
}
