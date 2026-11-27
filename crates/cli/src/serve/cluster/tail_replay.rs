//! D336 PR 3a (#2148, §5–§7): replaying an old owner's tail into a partition's owner
//! (`Cluster.ReplayTail`, `phdb cluster replay-tail`).
//!
//! - **One task per partition**, on its owner, through its write gate: a deposed owner
//!   can't replay (D326), and a lease lapse or a hand-over stops the task between commits.
//!   The next owner resumes from the mark.
//! - **The mark:** `replay/<history_epoch>` in `__phdb_cluster`'s family `replay`, holding
//!   the last origin seqno applied, skipped or refused, and the old feed's token after it.
//!   It's written in the same commit as what it covers, at `GroupSync` at least, so a crash
//!   leaves both or neither, and it replicates with the data.
//! - **One old commit, one new commit** (Amendment 1's M3): the kept changes as the old file
//!   logged them (kinds, timestamps, stored values), by name, tagged with the commit's first
//!   origin (§8, [`replay_tag`](super::replay_tag)), and the mark. A commit with nothing kept
//!   commits the mark alone, untagged and feed-silent.
//! - **What's passed over**, by name (§7): a change to a row the partition doesn't own now
//!   (S1, `not_owned`); feed-silent commits, range drops (S2) and catalog changes (S3);
//!   internal tables (S4); a table the owner lacks, or holds as made at another map version
//!   than the old file's was (S5, `dropped`). An index's or a memory store's commit is
//!   refused whole (R1, PR 3a: PR 3b replays them through the index).
//! - **What stops the run** (the mark stays, so a rerun meets it again): a lost lease or
//!   map, a family the owner's table lacks (its reconcile hasn't caught up), a corrupt tag,
//!   and a D329 share or decision whose outcome isn't settled (Amendment 5).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_core::Stream;
use pigeonhole::{ChangeKind, Family, Pigeonhole};
use tokio::sync::watch;

use super::replay_tag::{self, Origin};
use super::{Node, identity};
use crate::serve::errors::ApiError;
use crate::serve::gate::WriteGate;
use crate::serve::proto::{self, read_tail_event::Event};

/// The marks' family in `__phdb_cluster` (the coordinator, PR 3 numbers).
pub(crate) const FAMILY: &str = "replay";

/// A replay's answer, when the request sets no timeout: the replay goes on past it.
const DEFAULT_WAIT: Duration = Duration::from_secs(60);

/// The named skips and refusals a replay's progress keeps (`ReplayProgress.skips`).
const SKIPS_KEPT: usize = 32;

/// The replays on this node, one per partition, as `Status` shows them.
#[derive(Debug, Default)]
pub(crate) struct Replays {
    runs: Mutex<BTreeMap<String, Arc<Run>>>,
    /// `pigeonhole_cluster_tail_replay_commits_total`, by (partition, outcome).
    counts: Mutex<BTreeMap<(String, &'static str), u64>>,
}

/// One replay of one event's tail: its progress, and whether it runs.
#[derive(Debug)]
struct Run {
    from_history_epoch: u64,
    from_epoch: u64,
    dry_run: bool,
    progress: Mutex<Progress>,
    /// `true` once the run has ended.
    ended: watch::Sender<bool>,
}

/// What a run has done so far.
#[derive(Debug, Default, Clone)]
struct Progress {
    source_end: u64,
    mark: u64,
    applied: u64,
    skipped: u64,
    refused: u64,
    last_error: String,
    skips: VecDeque<(u64, String)>,
}

impl Replays {
    fn runs(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Arc<Run>>> {
        self.runs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Partition `id`'s last replay here (`LocalPartition.replay`).
    pub(crate) fn shown(&self, id: &str) -> Option<proto::ReplayProgress> {
        let run = Arc::clone(self.runs().get(id)?);
        let p = run.progress.lock().unwrap_or_else(PoisonError::into_inner);
        Some(proto::ReplayProgress {
            from_history_epoch: run.from_history_epoch,
            from_epoch: run.from_epoch,
            source_end: p.source_end,
            mark: p.mark,
            applied: p.applied,
            skipped: p.skipped,
            refused: p.refused,
            running: !*run.ended.borrow(),
            last_error: p.last_error.clone(),
            skips: p
                .skips
                .iter()
                .map(|(seqno, reason)| proto::ReplaySkip {
                    seqno: *seqno,
                    reason: reason.clone(),
                })
                .collect(),
        })
    }

    fn count(&self, partition: &str, outcome: &'static str) {
        *self
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((partition.to_owned(), outcome))
            .or_default() += 1;
    }

    /// `pigeonhole_cluster_tail_replay_commits_total{partition,outcome}` (§9): tail commits
    /// a replay here applied, found already applied, skipped or refused (a dry run counts
    /// nothing).
    pub(crate) fn render_metrics(&self, text: &mut String) {
        use std::fmt::Write as _;
        let counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        if counts.is_empty() {
            return;
        }
        let _ = writeln!(
            text,
            "# HELP pigeonhole_cluster_tail_replay_commits_total Old owners' tail commits a \
             replay on this node applied, found already applied, skipped or refused (D336).\n\
             # TYPE pigeonhole_cluster_tail_replay_commits_total counter"
        );
        for ((partition, outcome), n) in counts.iter() {
            let _ = writeln!(
                text,
                "pigeonhole_cluster_tail_replay_commits_total{{partition=\"{partition}\",\
                 outcome=\"{outcome}\"}} {n}"
            );
        }
    }
}

impl Run {
    fn progress(&self) -> std::sync::MutexGuard<'_, Progress> {
        self.progress.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn named(&self, seqno: u64, reason: impl Into<String>) {
        let mut p = self.progress();
        if p.skips.len() == SKIPS_KEPT {
            p.skips.pop_front();
        }
        p.skips.push_back((seqno, reason.into()));
    }
}

/// A replay mark: the last origin seqno passed, and the old feed's token after it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Mark {
    pub(crate) seqno: u64,
    pub(crate) token: Vec<u8>,
}

/// History `h`'s mark row.
fn mark_row(h: u64) -> Vec<u8> {
    format!("replay/{h}").into_bytes()
}

/// `__phdb_cluster` with the marks' family.
fn marks_table(db: &Pigeonhole) -> pigeonhole::Result<pigeonhole::Table> {
    db.table(identity::TABLE)?
        .family(FAMILY, Family::default())
        .create_if_missing()
}

/// History `h`'s mark in `db`: `None` before any replay of it marked a commit. Read-only:
/// the family isn't created here.
pub(crate) fn read_mark(db: &Pigeonhole, h: u64) -> Result<Option<Mark>, ApiError> {
    let t = match db.table(identity::TABLE).and_then(|b| b.open()) {
        Ok(t) if t.families().contains(&FAMILY) => t,
        Ok(_) => return Ok(None),
        Err(e) if e.code() == pigeonhole::ErrorCode::TableNotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let row = mark_row(h);
    let seqno = t.get(&row, FAMILY, b"seqno")?;
    let Some(seqno) = seqno else {
        return Ok(None);
    };
    let seqno = std::str::from_utf8(seqno.value())
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ApiError::internal(format!("history {h}'s replay mark is malformed")))?;
    let token = t
        .get(&row, FAMILY, b"token")?
        .map(|c| c.value().to_vec())
        .unwrap_or_default();
    Ok(Some(Mark { seqno, token }))
}

/// A logged put of `value` to `row`'s `q` in the marks' family, at `ts`.
fn put(
    b: &mut pigeonhole::replication::LoggedBatch<'_>,
    t: &pigeonhole::Table,
    row: &[u8],
    q: &[u8],
    ts: u64,
    value: &[u8],
) {
    // A plain value's stored form: its tag, then its bytes.
    let stored = [&[0u8][..], value].concat();
    b.push(t, row, FAMILY, ChangeKind::Put, q, ts, &stored);
}

fn push_mark(
    b: &mut pigeonhole::replication::LoggedBatch<'_>,
    t: &pigeonhole::Table,
    h: u64,
    m: &Mark,
) {
    // Each mark cell's timestamp is the mark's own seqno, which only moves forward in its
    // history, so the newest mark is the one a read returns whatever the clocks of the owners
    // that wrote them (cli692's M-a on #2265: a logged batch takes no timestamp floor, and an
    // owner whose clock is behind would hide its mark behind the last one).
    let row = mark_row(h);
    put(
        b,
        t,
        &row,
        b"seqno",
        m.seqno,
        m.seqno.to_string().as_bytes(),
    );
    put(b, t, &row, b"token", m.seqno, &m.token);
}

/// The tables an index or a memory store keeps (R1): derived data, refused whole in PR 3a.
fn derived_tables(map: &proto::ClusterMap) -> BTreeMap<String, &'static str> {
    let mut out = BTreeMap::new();
    for i in &map.indexes {
        out.insert(i.name.clone(), "index");
    }
    if let Some(m) = &map.memory {
        for suffix in ["", "_text", "_ivf"] {
            out.insert(format!("mem_{}{suffix}", m.store), "memory_store");
        }
    }
    out
}

/// What a tail commit comes to on this owner.
#[derive(Debug, PartialEq)]
enum Verdict {
    /// Its kept changes (by index into the commit's), with the reason any others were
    /// passed over.
    Apply {
        kept: Vec<usize>,
        passed: Option<&'static str>,
    },
    /// Nothing to apply: the mark alone moves.
    Skip(&'static str),
    /// Refused whole (R1): the mark moves past it, and it's named.
    Refuse(String),
}

/// What a tail commit's D329 side comes to (Amendment 5).
#[derive(Debug)]
enum Txn {
    /// No transaction's: as §7 judges it.
    Plain,
    /// Abort markers to write, each `(row, stored, ts)` (d).
    Rows(Vec<(Vec<u8>, Vec<u8>, u64)>),
    /// A branch's decided share, applied with its xid; with `drop_first`, the copy's prepare
    /// of it goes first (b).
    Share { drop_first: bool },
    /// A home's commit point (c): its row's latest value in the tail, then its share.
    CommitPoint((Vec<u8>, Vec<u8>, u64)),
    /// Refused whole, named.
    Refuse(String),
}

/// What the owner knows to judge a tail commit by.
struct Judge<'a> {
    partition: &'a str,
    routes: &'a super::route::Routes,
    derived: &'a BTreeMap<String, &'static str>,
    /// The owner's schema records: each map-made table's `created`.
    records: &'a BTreeMap<String, u64>,
}

impl Judge<'_> {
    /// §7's rules for `c`, change by change, for a commit with no D329 xid.
    fn judge(&self, c: &proto::TailCommit) -> Verdict {
        match c.skip() {
            proto::TailSkip::Catalog => return Verdict::Skip("catalog"),
            proto::TailSkip::RangeDrop => return Verdict::Skip("range_drop"),
            proto::TailSkip::None => {}
        }
        if c.silent {
            return Verdict::Skip("silent");
        }
        // R1 before anything is kept: the commit is refused whole, never torn.
        let mut derived: Vec<&str> = c
            .changes
            .iter()
            .filter(|ch| self.derived.contains_key(&ch.table))
            .map(|ch| ch.table.as_str())
            .collect();
        derived.sort_unstable();
        derived.dedup();
        if let Some(t) = derived.first() {
            return Verdict::Refuse(format!(
                "{}: it writes {}'s table {}; put its rows again through the {} API",
                self.derived[*t],
                self.derived[*t].replace('_', " "),
                derived.join(", "),
                self.derived[*t].replace('_', " "),
            ));
        }
        let mut kept = Vec::new();
        let mut passed = None;
        for (i, ch) in c.changes.iter().enumerate() {
            let why = if ch.table.starts_with("__phdb") {
                Some("internal")
            } else if c
                .created
                .get(&ch.table)
                .is_none_or(|&old| old == 0 || self.records.get(&ch.table) != Some(&old))
            {
                Some("dropped")
            } else if self.routes.partition(&ch.table, &ch.row) != Some(self.partition) {
                Some("not_owned")
            } else {
                None
            };
            match why {
                None => kept.push(i),
                // The first reason that isn't S4's (always there: the old owner's own rows)
                // is the one worth naming.
                Some("internal") => {}
                Some(w) => {
                    passed.get_or_insert(w);
                }
            }
        }
        if kept.is_empty() {
            Verdict::Skip(passed.unwrap_or("nothing_kept"))
        } else {
            Verdict::Apply { kept, passed }
        }
    }
}

/// The run's context: the owner's database and gate, now.
struct Ctx {
    db: Pigeonhole,
    gate: Arc<WriteGate>,
    service: Arc<crate::serve::service::Service>,
}

impl Node {
    /// `Cluster.ReplayTail`: starts the replay of `r`'s tail of its partition here (or joins
    /// the one running), and waits for it up to `r.timeout_ms`.
    pub(crate) async fn replay_tail(
        self: &Arc<Self>,
        r: proto::ReplayTailRequest,
    ) -> Result<(), ApiError> {
        let id = r.partition.clone();
        self.pending_tail(&id, r.from_history_epoch, r.from_epoch)?;
        self.replay_ctx(&id)?;
        let wait = match r.timeout_ms {
            0 => DEFAULT_WAIT,
            ms => Duration::from_millis(ms),
        };
        let run = {
            let mut runs = self.replays.runs();
            match runs.get(&id) {
                Some(run) if !*run.ended.borrow() => {
                    if (run.from_history_epoch, run.from_epoch, run.dry_run)
                        != (r.from_history_epoch, r.from_epoch, r.dry_run)
                    {
                        return Err(ApiError::unavailable(format!(
                            "partition {id} is replaying another tail (history epoch {}, epoch \
                             {}{}): one replay at a time",
                            run.from_history_epoch,
                            run.from_epoch,
                            if run.dry_run { ", a dry run" } else { "" }
                        )));
                    }
                    Arc::clone(run)
                }
                _ => {
                    let run = Arc::new(Run {
                        from_history_epoch: r.from_history_epoch,
                        from_epoch: r.from_epoch,
                        dry_run: r.dry_run,
                        progress: Mutex::default(),
                        ended: watch::channel(false).0,
                    });
                    runs.insert(id.clone(), Arc::clone(&run));
                    let (node, run2, id2) = (Arc::clone(self), Arc::clone(&run), id.clone());
                    tokio::spawn(async move {
                        let outcome = node.run_replay(&id2, &run2).await;
                        if let Err(e) = outcome {
                            eprintln!(
                                "phdb serve: partition {id2}'s tail replay (history epoch {}) \
                                 stopped: {}",
                                run2.from_history_epoch, e.message
                            );
                            run2.progress().last_error = e.message;
                        } else {
                            eprintln!(
                                "phdb serve: partition {id2}'s tail replay (history epoch {}) \
                                 ended at the source's end, origin seqno {}",
                                run2.from_history_epoch,
                                run2.progress().mark
                            );
                        }
                        run2.ended.send_replace(true);
                    });
                    run
                }
            }
        };
        let mut ended = run.ended.subscribe();
        let _ = tokio::time::timeout(wait, ended.wait_for(|e| *e)).await;
        Ok(())
    }

    /// The PENDING failover event of partition `id` with the old file's epochs, in this
    /// node's map, which gives `id` to this node.
    fn pending_tail(
        &self,
        id: &str,
        from_history_epoch: u64,
        from_epoch: u64,
    ) -> Result<proto::FailoverEvent, ApiError> {
        use proto::{FailoverKind as K, TailState as T};
        let map = self.get_map()?;
        if !map
            .partitions
            .iter()
            .any(|p| p.id == id && p.node == self.id)
        {
            return Err(ApiError::failed_precondition(format!(
                "this node's map (version {}) doesn't give partition {id:?} to this node: a \
                 tail is replayed on its owner",
                map.map_version
            )));
        }
        map.failovers
            .iter()
            .find(|e| {
                e.kind() == K::Partition
                    && e.partition == id
                    && e.from_history_epoch == from_history_epoch
                    && e.from_epoch == from_epoch
            })
            .filter(|e| e.tail_state() == T::Pending)
            .cloned()
            .ok_or_else(|| {
                ApiError::failed_precondition(format!(
                    "this node's map (version {}) has no PENDING tail of partition {id:?} at \
                     history epoch {from_history_epoch} and epoch {from_epoch}",
                    map.map_version
                ))
            })
    }

    /// Partition `id` open here with a lease: what the replay writes through.
    fn replay_ctx(&self, id: &str) -> Result<Ctx, ApiError> {
        let shared = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .ok_or_else(|| ApiError::partition_unavailable("the server is stopping"))?;
        let cluster = shared
            .router
            .cluster
            .as_ref()
            .ok_or_else(|| ApiError::internal("not a cluster node"))?;
        let service = cluster.here(id).ok_or_else(|| {
            ApiError::partition_unavailable(format!("partition {id} isn't open here yet"))
        })?;
        let gate = service
            .gate()
            .cloned()
            .ok_or_else(|| ApiError::internal(format!("partition {id} has no write gate")))?;
        if !gate.leased() {
            return Err(ApiError::partition_unavailable(format!(
                "partition {id} holds no lease here yet"
            )));
        }
        Ok(Ctx {
            db: service.part()?.db.clone(),
            gate,
            service,
        })
    }

    /// One run: from the mark to the source's end, a commit at a time.
    async fn run_replay(self: &Arc<Self>, id: &str, run: &Arc<Run>) -> Result<(), ApiError> {
        let (h, e) = (run.from_history_epoch, run.from_epoch);
        let event = self.pending_tail(id, h, e)?;
        let cx = self.replay_ctx(id)?;
        let mut mark = read_mark(&cx.db, h)?.unwrap_or_else(|| Mark {
            seqno: 0,
            token: event.fork_token.to_vec(),
        });
        if mark.token.is_empty() {
            return Err(ApiError::failed_precondition(format!(
                "partition {id}'s failover event (to node {:?} at epoch {}) has no fork token: \
                 its tail can't be read; discard it",
                event.to, event.to_epoch
            )));
        }
        run.progress().mark = mark.seqno;
        // Each decision row's last value in the tail (R2 as Amendment 1's M1 has it: a
        // commit point's branches as their later acks left them).
        let latest = self.latest_decisions(&event, &mark).await?;
        let mut stream = self.open_tail(&event, &mark).await?;
        loop {
            let msg = std::future::poll_fn(|c| stream.as_mut().poll_next(c))
                .await
                .ok_or_else(|| ApiError::unavailable("the tail source's stream ended early"))??;
            match msg.event {
                Some(Event::End(end)) => {
                    run.progress().source_end = end.last_seqno;
                    // Seqnos past the last event (none in the feed) are the tail's too: the
                    // mark reaches the end, which DONE asks for.
                    if !run.dry_run && mark.seqno < end.last_seqno {
                        mark.seqno = end.last_seqno;
                        let cx = self.replay_ctx(id)?;
                        self.commit_mark_only(&cx, h, &mark).await?;
                    }
                    run.progress().mark = mark.seqno.max(end.last_seqno);
                    return Ok(());
                }
                Some(Event::Commit(c)) => {
                    if c.seqno <= mark.seqno {
                        continue;
                    }
                    self.pending_tail(id, h, e)?;
                    let cx = self.replay_ctx(id)?;
                    self.replay_one(id, run, &cx, &mut mark, c, &latest).await?;
                }
                None => return Err(ApiError::internal("an empty ReadTail message")),
            }
        }
    }

    /// One tail commit: judged, then applied, skipped or refused, with the mark.
    #[allow(clippy::too_many_lines)]
    async fn replay_one(
        &self,
        id: &str,
        run: &Run,
        cx: &Ctx,
        mark: &mut Mark,
        c: proto::TailCommit,
        latest: &HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), ApiError> {
        let h = run.from_history_epoch;
        let (origin, xid) = match replay_tag::split(&c.tag) {
            Ok((o, xid)) => (o, xid.to_vec()),
            Err(e) => {
                return Err(ApiError::failed_precondition(format!(
                    "tail commit {}: {e}; the replay stops before it",
                    c.seqno
                )));
            }
        };
        // Its first origin (§5): a commit replayed once already keeps the one it names.
        let origin = origin.unwrap_or(Origin {
            history_epoch: h,
            seqno: c.seqno,
        });
        let next = Mark {
            seqno: c.seqno,
            token: c.token.to_vec(),
        };
        // Another history's commit, which an earlier replay brought into this one: skipped
        // if that history's mark here already passed it.
        let other = (origin.history_epoch != h)
            .then(|| read_mark(&cx.db, origin.history_epoch))
            .transpose()?
            .flatten();
        if other.as_ref().is_some_and(|m| m.seqno >= origin.seqno) {
            return self
                .pass_tail(id, run, cx, mark, next, "already", None)
                .await;
        }
        let map = self.get_map()?;
        let verdict = {
            let routes = self.routes();
            let routes = routes
                .as_ref()
                .ok_or_else(|| ApiError::partition_unavailable("this node has no map yet"))?;
            let derived = derived_tables(&map);
            let records = super::schema::records(&cx.db).map_err(ApiError::internal)?;
            Judge {
                partition: id,
                routes,
                derived: &derived,
                records: &records,
            }
            .judge(&c)
        };
        let (kept, passed) = match verdict {
            Verdict::Refuse(why) => {
                eprintln!(
                    "phdb serve: partition {id}'s tail replay refused commit {} (history epoch \
                     {h}): {why}",
                    c.seqno
                );
                return self
                    .pass_tail(id, run, cx, mark, next, "refused", Some(why))
                    .await;
            }
            // Catalog changes, range drops and feed-silent commits hold no D329 row.
            Verdict::Skip(why @ ("catalog" | "range_drop" | "silent")) => {
                return self.pass_tail(id, run, cx, mark, next, why, None).await;
            }
            Verdict::Skip(why) => (Vec::new(), Some(why)),
            Verdict::Apply { kept, passed } => (kept, passed),
        };
        // D329 (Amendment 5): what the commit's transaction rows and xid come to.
        let txn = self.judge_txn(id, cx, &map, &c, &xid, latest).await?;
        let (rows, drop_first, keep_xid) = match txn {
            Txn::Plain => (Vec::new(), false, !xid.is_empty()),
            Txn::Rows(rows) => (rows, false, false),
            Txn::Share { drop_first } => (Vec::new(), drop_first, true),
            Txn::CommitPoint(row) => (vec![row], false, true),
            Txn::Refuse(why) => {
                eprintln!(
                    "phdb serve: partition {id}'s tail replay refused commit {} (history epoch \
                     {h}): {why}",
                    c.seqno
                );
                return self
                    .pass_tail(id, run, cx, mark, next, "refused", Some(why))
                    .await;
            }
        };
        if kept.is_empty() && rows.is_empty() {
            return self
                .pass_tail(
                    id,
                    run,
                    cx,
                    mark,
                    next,
                    passed.unwrap_or("nothing_kept"),
                    None,
                )
                .await;
        }
        if let Some(why) = passed {
            run.named(c.seqno, why);
        }
        if run.dry_run {
            run.progress().applied += 1;
            *mark = next;
            return Ok(());
        }
        // (b), simp724's and the coordinator's shape: the copy's prepare of the share is
        // dropped first, writing no data, no decision and no feed event; the share then
        // lands below with the mark, in one commit. A crash between the two finds the share
        // no longer in doubt and the mark unmoved, and the rerun asks the home (path (a)).
        // D331's range gate (ICR 0147): a row a move fences, moved away or doesn't serve here
        // yet stops the run before anything is dropped or written; a rerun resumes from the
        // mark.
        if let Some(ranges) = &*cx.gate.ranges().load() {
            ranges.check_rows(
                kept.iter()
                    .map(|&i| (c.changes[i].table.as_str(), &c.changes[i].row[..])),
            )?;
        }
        if drop_first {
            let (db, x) = (cx.db.clone(), xid.clone());
            let held = cx.gate.enter()?;
            tokio::task::spawn_blocking(move || db.decide(&x, false).map_err(ApiError::from))
                .await
                .map_err(|e| ApiError::internal(format!("a tail replay's drop: {e}")))??;
            held.committed()?;
            #[cfg(test)]
            seams::after_drop(&self.dir)?;
        }
        // The other history's mark moves with it, with that history's token as the earlier
        // replay wrote it in this very commit (S3's chain).
        let other_mark = (origin.history_epoch != h).then(|| Mark {
            seqno: origin.seqno,
            token: carried_token(&c, origin.history_epoch)
                .or_else(|| other.map(|m| m.token))
                .unwrap_or_default(),
        });
        let tag =
            replay_tag::encode(origin, if keep_xid { &xid } else { b"" }).ok_or_else(|| {
                ApiError::internal(format!(
                    "tail commit {}'s replay tag passes 255 bytes",
                    c.seqno
                ))
            })?;
        let (db, changes, created) = (cx.db.clone(), c.changes, c.created);
        let (m, om) = (next.clone(), other_mark);
        let decisions = if rows.is_empty() {
            None
        } else {
            Some(cx.service.decisions_table().await?)
        };
        let held = cx.gate.enter()?;
        tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
            let marks = marks_table(&db)?;
            let mut tables: BTreeMap<&str, pigeonhole::Table> = BTreeMap::new();
            let mut b = db.logged_batch();
            // A commit point's row first, before the share it carries (Amendment 1's M1).
            if let Some(t) = &decisions {
                for (row, stored, ts) in &rows {
                    b.push(
                        t,
                        row,
                        super::spanning::DECISION_FAMILY,
                        ChangeKind::Put,
                        super::spanning::DECISION_QUALIFIER,
                        *ts,
                        stored,
                    );
                }
            }
            for i in kept {
                let ch = &changes[i];
                let t = match tables.get(ch.table.as_str()) {
                    Some(t) => t,
                    None => {
                        let t = db.table(&ch.table)?.open()?;
                        tables.entry(ch.table.as_str()).or_insert(t)
                    }
                };
                if !t.families().contains(&ch.family.as_str()) {
                    return Err(ApiError::failed_precondition(format!(
                        "table {} here has no family {}: its schema reconcile hasn't caught \
                         up; the replay stops, and a rerun resumes",
                        ch.table, ch.family
                    )));
                }
                b.push(
                    t,
                    &ch.row,
                    &ch.family,
                    super::range_follow::change_kind(ch.kind)?,
                    &ch.qualifier,
                    ch.timestamp,
                    &ch.stored,
                );
            }
            // cli692's S-a on #2265: each table, opened above, is still the one the judge
            // matched by `created`. A table dropped and made again since would take the old
            // commit into its new incarnation; the run stops instead, and a rerun judges again.
            // A drop after this fails the commit itself.
            if !tables.is_empty() {
                let now = super::schema::records(&db).map_err(ApiError::internal)?;
                for name in tables.keys() {
                    if now.get(*name) != created.get(*name) {
                        return Err(ApiError::failed_precondition(format!(
                            "table {name} here changed while the replay judged its commit (a \
                             drop or a make again): the replay stops, and a rerun judges it \
                             again"
                        )));
                    }
                }
            }
            push_mark(&mut b, &marks, h, &m);
            if let Some(om) = &om {
                push_mark(&mut b, &marks, origin.history_epoch, om);
            }
            b.tag_xid(&tag);
            b.commit_with(pigeonhole::Durability::GroupSync)?;
            Ok(())
        })
        .await
        .map_err(|e| ApiError::internal(format!("a tail replay's commit: {e}")))??;
        held.committed()?;
        run.progress().applied += 1;
        self.replays.count(id, "applied");
        *mark = next;
        run.progress().mark = mark.seqno;
        #[cfg(test)]
        seams::after_apply(&self.dir)?;
        Ok(())
    }

    /// Amendment 5's rules for tail commit `c` of partition `id`, whose tag's xid is `xid`
    /// (empty for none), with `latest` each decision row's last value in the tail.
    async fn judge_txn(
        &self,
        id: &str,
        cx: &Ctx,
        map: &proto::ClusterMap,
        c: &proto::TailCommit,
        xid: &[u8],
        latest: &HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<Txn, ApiError> {
        use proto::TxnOutcomeState as S;
        let decisions: Vec<&proto::RawChange> = c
            .changes
            .iter()
            .filter(|ch| ch.table == super::spanning::DECISIONS)
            .collect();
        let x = String::from_utf8_lossy(xid).into_owned();
        // (c): a home's commit point, its share and its decision row in one commit.
        if !xid.is_empty() && decisions.iter().any(|ch| *ch.row == *xid) {
            if cx.service.decision(&x).await?.is_some() {
                return Ok(Txn::Refuse(format!(
                    "r2: transaction {x}'s commit point, which this owner already holds a \
                     decision for"
                )));
            }
            let ch = decisions
                .iter()
                .find(|ch| *ch.row == *xid && !ch.stored.is_empty())
                .ok_or_else(|| ApiError::internal("a commit point's row with no value"))?;
            let value = latest
                .get(xid)
                .cloned()
                .unwrap_or_else(|| ch.stored.get(1..).unwrap_or_default().to_vec());
            let text = String::from_utf8_lossy(&value).into_owned();
            let mut unknown = Vec::new();
            for b in text.lines().skip(1).filter(|l| !l.starts_with('+')) {
                if !self.share_in_doubt(map, b, &x).await? {
                    unknown.push(b.to_owned());
                }
            }
            if !unknown.is_empty() {
                return Ok(Txn::Refuse(format!(
                    "r2: transaction {x}'s commit point, whose branches {} neither acked it nor \
                     hold it in doubt: replaying it could break the transaction's atomicity",
                    unknown.join(", ")
                )));
            }
            let stored = [&[0u8][..], &value].concat();
            return Ok(Txn::CommitPoint((xid.to_vec(), stored, ch.timestamp)));
        }
        // (d): abort markers, each only over no decision here; a row's acks and its
        // collection are the commit point's and the home's own.
        if xid.is_empty() && !decisions.is_empty() {
            let mut rows = Vec::new();
            for ch in decisions {
                if !ch
                    .stored
                    .get(1..)
                    .is_some_and(|v| v.starts_with(b"aborted "))
                {
                    continue;
                }
                let tx = String::from_utf8_lossy(&ch.row).into_owned();
                if cx.service.decision(&tx).await?.is_some() {
                    return Ok(Txn::Refuse(format!(
                        "an abort marker of transaction {tx}, which this owner already holds a \
                         decision for"
                    )));
                }
                rows.push((ch.row.to_vec(), ch.stored.to_vec(), ch.timestamp));
            }
            return Ok(if rows.is_empty() {
                Txn::Plain
            } else {
                Txn::Rows(rows)
            });
        }
        if xid.is_empty() {
            return Ok(Txn::Plain);
        }
        let home = crate::serve::sessions::spanning_parts(&x).map(|(home, ..)| home.to_owned());
        if home.as_deref() == Some(id) {
            return Ok(Txn::Plain);
        }
        // A branch's decided share. (b): this copy still holds its prepare in doubt.
        if cx.db.in_doubt().iter().any(|(held, _)| *held == *xid) {
            return Ok(Txn::Share { drop_first: true });
        }
        // (a): its home's decision row, read only.
        let asked = self.ask_home_read_only(&x).await;
        match asked {
            Ok(r) if r.state == S::Committed as i32 => Ok(Txn::Share { drop_first: false }),
            Ok(r) if r.state == S::Aborted as i32 => Ok(Txn::Refuse(format!(
                "conflict: transaction {x}'s share was applied on the old owner, but its home \
                 records it aborted"
            ))),
            // UNKNOWN: no decision row, which for a share the old owner applied means its
            // home collected the row past its retention (simp724's N1 on #2265).
            Ok(r) => Err(ApiError::failed_precondition(format!(
                "in_doubt: transaction {x}'s share was applied on the old owner, but its home \
                 holds no decision for it ({}): its record was collected past \
                 --txn-outcome-retention, so the replay can't tell whether it committed. \
                 Discard the tail (and, if you know the transaction committed, put its rows \
                 again yourself)",
                r.reason
            ))),
            Err(e) => Err(ApiError::failed_precondition(format!(
                "in_doubt: transaction {x}'s share, whose home didn't answer ({}): run \
                 replay-tail again once it does, or discard the tail",
                e.message
            ))),
        }
    }

    /// Whether branch `branch` of transaction `xid` is held in doubt on its owner now. An
    /// owner that doesn't answer, or doesn't report the branch, is an error that stops the
    /// run with the mark unmoved (simp724's S1 on #2265): "couldn't ask" isn't "not in doubt",
    /// which would refuse the commit point for good. A branch the map no longer has holds
    /// nothing.
    async fn share_in_doubt(
        &self,
        map: &proto::ClusterMap,
        branch: &str,
        xid: &str,
    ) -> Result<bool, ApiError> {
        let Some(p) = map.partitions.iter().find(|p| p.id == branch) else {
            return Ok(false);
        };
        let couldnt = |why: String| {
            ApiError::failed_precondition(format!(
                "in_doubt: transaction {xid}'s commit point: branch {branch}'s owner {:?} \
                 couldn't be asked whether it holds its share in doubt ({why}): run \
                 replay-tail again once it answers, or discard the tail",
                p.node
            ))
        };
        match self.row_of(&p.node, branch).await {
            Ok(Some(lp)) => Ok(lp.in_doubt_shares.iter().any(|s| s.xid == xid)),
            Ok(None) => Err(couldnt(format!("it doesn't report partition {branch}"))),
            Err(e) => Err(couldnt(e.message)),
        }
    }

    /// Amendment 5's (a): transaction `xid`'s home's decision row, read only.
    async fn ask_home_read_only(&self, xid: &str) -> Result<proto::TxnResolution, ApiError> {
        #[cfg(test)]
        seams::ASKED_HOME
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((self.dir.clone(), xid.to_owned()));
        let shared = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .ok_or_else(|| ApiError::partition_unavailable("the server is stopping"))?;
        let cluster = shared
            .router
            .cluster
            .as_ref()
            .ok_or_else(|| ApiError::internal("not a cluster node"))?;
        super::spanning::ask_home(
            &cluster.node,
            &cluster.parts,
            &cluster.forwarder,
            &cluster.spanning,
            xid,
            None,
            shared.limits.request_timeout,
            true,
        )
        .await
    }

    /// A tail commit passed over (`why`), or refused (`refused` names it): its mark alone.
    #[allow(clippy::too_many_arguments)]
    async fn pass_tail(
        &self,
        id: &str,
        run: &Run,
        cx: &Ctx,
        mark: &mut Mark,
        next: Mark,
        why: &'static str,
        refused: Option<String>,
    ) -> Result<(), ApiError> {
        let seqno = next.seqno;
        if !run.dry_run {
            self.commit_mark_only(cx, run.from_history_epoch, &next)
                .await?;
        }
        *mark = next;
        let outcome = match (&refused, why) {
            (Some(_), _) => "refused",
            (None, "already") => "already",
            (None, _) => "skipped",
        };
        {
            let mut p = run.progress();
            match outcome {
                "refused" => p.refused += 1,
                _ => p.skipped += 1,
            }
            p.mark = mark.seqno;
        }
        match refused {
            Some(r) => run.named(seqno, r),
            // Feed-silent commits and catalog changes are routine: counted, not named.
            None if !matches!(why, "silent" | "catalog" | "already") => run.named(seqno, why),
            None => {}
        }
        if !run.dry_run {
            self.replays.count(id, outcome);
        }
        Ok(())
    }

    /// The mark alone, untagged and feed-silent, through the gate.
    async fn commit_mark_only(&self, cx: &Ctx, h: u64, m: &Mark) -> Result<(), ApiError> {
        let (db, m) = (cx.db.clone(), m.clone());
        let held = cx.gate.enter()?;
        tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
            let marks = marks_table(&db)?;
            let mut b = db.logged_batch();
            push_mark(&mut b, &marks, h, &m);
            b.mark_feed_silent();
            b.commit_with(pigeonhole::Durability::GroupSync)?;
            Ok(())
        })
        .await
        .map_err(|e| ApiError::internal(format!("a tail replay's mark: {e}")))??;
        held.committed()?;
        Ok(())
    }

    /// Reads the tail from `mark` once for each decision row's last value in it.
    async fn latest_decisions(
        &self,
        event: &proto::FailoverEvent,
        mark: &Mark,
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>, ApiError> {
        let mut out = HashMap::new();
        let mut stream = self.open_tail(event, mark).await?;
        loop {
            let msg = std::future::poll_fn(|c| stream.as_mut().poll_next(c))
                .await
                .ok_or_else(|| ApiError::unavailable("the tail source's stream ended early"))??;
            match msg.event {
                Some(Event::Commit(c)) => {
                    for ch in c.changes.iter().filter(|ch| {
                        ch.table == super::spanning::DECISIONS && !ch.stored.is_empty()
                    }) {
                        out.insert(ch.row.to_vec(), ch.stored[1..].to_vec());
                    }
                }
                Some(Event::End(_)) | None => return Ok(out),
            }
        }
    }

    /// `ReadTail` on `event`'s old node, from `mark`: retried a moment while the source is
    /// still busy with the stream before.
    async fn open_tail(
        &self,
        event: &proto::FailoverEvent,
        mark: &Mark,
    ) -> Result<
        std::pin::Pin<Box<dyn Stream<Item = Result<proto::ReadTailEvent, ApiError>> + Send>>,
        ApiError,
    > {
        let mut wait = Duration::from_millis(20);
        loop {
            match self.open_tail_once(event, mark).await {
                Err(e) if e.code == "Unavailable" && wait < Duration::from_secs(2) => {
                    tokio::time::sleep(wait).await;
                    wait *= 2;
                }
                other => return other,
            }
        }
    }

    async fn open_tail_once(
        &self,
        event: &proto::FailoverEvent,
        mark: &Mark,
    ) -> Result<
        std::pin::Pin<Box<dyn Stream<Item = Result<proto::ReadTailEvent, ApiError>> + Send>>,
        ApiError,
    > {
        let shared = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .ok_or_else(|| ApiError::partition_unavailable("the server is stopping"))?;
        let cluster = shared
            .router
            .cluster
            .as_ref()
            .ok_or_else(|| ApiError::internal("not a cluster node"))?;
        let map = self.get_map()?;
        let address = map
            .nodes
            .iter()
            .find(|n| n.id == event.from)
            .map(|n| n.address.clone())
            .ok_or_else(|| {
                ApiError::failed_precondition(format!(
                    "the old owner {:?} isn't in the map: its tail can't be read; discard it",
                    event.from
                ))
            })?;
        let req = proto::ReadTailRequest {
            partition: event.partition.clone(),
            from_history_epoch: event.from_history_epoch,
            from_epoch: event.from_epoch,
            token: mark.token.clone().into(),
            after_seqno: mark.seqno,
        };
        let lease = cluster
            .forwarder
            .stream_channel(&address, shared.limits.request_timeout)
            .await?;
        let request = self.request(req).map_err(ApiError::internal)?;
        let mut grpc =
            tonic::client::Grpc::new(lease.channel()).max_decoding_message_size(usize::MAX);
        let failed = |s: tonic::Status| super::forward::classify(&s, &address, false);
        grpc.ready()
            .await
            .map_err(|e| failed(tonic::Status::unavailable(e.to_string())))?;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(super::api::READ_TAIL);
        let stream = grpc
            .server_streaming(
                request,
                path,
                tonic_prost::ProstCodec::<proto::ReadTailRequest, proto::ReadTailEvent>::default(),
            )
            .await
            .map_err(failed)?
            .into_inner();
        Ok(Box::pin(Remote {
            stream,
            address,
            _lease: lease,
        }))
    }
}

/// History `h`'s mark token as tail commit `c` carries it: an earlier replay wrote the mark
/// in the same commit as what it replayed.
fn carried_token(c: &proto::TailCommit, h: u64) -> Option<Vec<u8>> {
    let row = mark_row(h);
    c.changes
        .iter()
        .find(|ch| {
            ch.table == identity::TABLE
                && ch.family == FAMILY
                && *ch.row == *row
                && &*ch.qualifier == b"token"
        })
        .map(|ch| ch.stored.get(1..).unwrap_or_default().to_vec())
}

/// The tail source's stream, holding its connection.
struct Remote {
    stream: tonic::Streaming<proto::ReadTailEvent>,
    address: String,
    _lease: super::forward::StreamLease,
}

impl Stream for Remote {
    type Item = Result<proto::ReadTailEvent, ApiError>;

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

impl Node {
    /// Partition `partition`'s line in node `id`'s `Status` (this node's own, or a peer's
    /// within the push timeout).
    async fn row_of(
        &self,
        id: &str,
        partition: &str,
    ) -> Result<Option<proto::LocalPartition>, ApiError> {
        let st = if id == self.id {
            let me = self
                .me
                .upgrade()
                .ok_or_else(|| ApiError::partition_unavailable("the node is stopping"))?;
            tokio::task::spawn_blocking(move || me.status()).await?
        } else {
            let address = self
                .address_of(id)
                .map_err(|f| ApiError::unavailable(f.message().to_owned()))?;
            tokio::time::timeout(self.peers.push_timeout, async {
                let mut c = self.client(&address).await?;
                c.status(self.request(proto::StatusRequest::default())?)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(|s| s.message().to_owned())
            })
            .await
            .map_err(|_| ApiError::unavailable(format!("node {id:?}: timed out")))?
            .map_err(|e| ApiError::unavailable(format!("node {id:?}: {e}")))?
        };
        Ok(st.partitions.into_iter().find(|lp| lp.id == partition))
    }

    /// The writer's checks on `new` over `current` for old owners' tails (D336 PR 3a):
    /// - **C4:** a tail goes DONE only as its owner reports it: P's owner at P's epoch in
    ///   `current` answers a finished replay of that event (not running, no error) whose
    ///   mark reached the source's end and equals `replayed_through`;
    /// - **C3:** a node a PENDING tail names as its old owner leaves the map only by a forced
    ///   map, which records the tail DISCARDED ([`discard_removed_sources`]).
    pub(super) async fn check_tails(
        &self,
        current: &proto::ClusterMap,
        new: &proto::ClusterMap,
    ) -> Result<(), ApiError> {
        use proto::TailState as T;
        for e in current
            .failovers
            .iter()
            .filter(|e| e.tail_state() == T::Pending)
        {
            if !new.nodes.iter().any(|n| n.id == e.from) {
                return Err(ApiError::failed_precondition(format!(
                    "node {:?} holds partition {:?}'s old owner's tail, PENDING: replay it or \
                     discard it (phdb cluster replay-tail {} [--discard]) before the map drops \
                     the node, or drop it with a forced map, which discards the tail",
                    e.from, e.partition, e.partition
                )));
            }
            let Some(n) = new.failovers.iter().find(|n| {
                n.kind == e.kind
                    && n.partition == e.partition
                    && n.to == e.to
                    && n.to_epoch == e.to_epoch
            }) else {
                continue;
            };
            // A DONE with no mark: the map's validation refuses it, naming what's missing.
            if n.tail_state() != T::Done || n.replayed_through == 0 {
                continue;
            }
            let Some(p) = current.partitions.iter().find(|p| p.id == e.partition) else {
                return Err(ApiError::failed_precondition(format!(
                    "partition {:?} isn't in the map: its tail can't be confirmed replayed",
                    e.partition
                )));
            };
            let lp = self.row_of(&p.node, &p.id).await?;
            let done = lp.as_ref().and_then(|lp| {
                let r = lp.replay.as_ref()?;
                (lp.epoch == p.epoch
                    && r.from_history_epoch == e.from_history_epoch
                    && r.from_epoch == e.from_epoch
                    && !r.running
                    && r.last_error.is_empty()
                    && r.source_end > 0
                    && r.mark >= r.source_end)
                    .then_some(r.mark)
            });
            if done != Some(n.replayed_through) {
                return Err(ApiError::failed_precondition(format!(
                    "partition {:?}'s owner {:?} reports no finished replay of its old owner's \
                     tail (history epoch {}) at mark {}: a tail is set DONE only by its replay \
                     (phdb cluster replay-tail {})",
                    e.partition, p.node, e.from_history_epoch, n.replayed_through, e.partition
                )));
            }
        }
        Ok(())
    }
}

/// C3 (cli692 on the design): a forced map that drops a node a PENDING tail names as its old
/// owner records that tail DISCARDED, its fork token cleared: nothing can read it now.
pub(super) fn discard_removed_sources(map: &mut proto::ClusterMap) -> Vec<String> {
    let mut discarded = Vec::new();
    let nodes: Vec<String> = map.nodes.iter().map(|n| n.id.clone()).collect();
    for e in map
        .failovers
        .iter_mut()
        .filter(|e| e.tail_state() == proto::TailState::Pending && !nodes.contains(&e.from))
    {
        e.set_tail_state(proto::TailState::Discarded);
        e.fork_token = Default::default();
        discarded.push(format!(
            "partition {}'s old owner's tail (node {}, history epoch {}): source node removed",
            e.partition, e.from, e.from_history_epoch
        ));
    }
    discarded
}

impl Node {
    /// After an install of `map` (D336 PR 3a, Amendment 4): each partition open here as its
    /// owner, with a lease, drops the promotes' fork rows that no PENDING tail of it names
    /// (DONE, DISCARDED, NONE, or an event gone from the map). Best effort, through the gate,
    /// on a task of its own: a partition without its lease yet is pruned at a later map.
    pub(super) fn prune_forks_soon(&self, map: &proto::ClusterMap) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let Some(shared) = self.server.get().and_then(|o| o.shared.upgrade()) else {
            return;
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return;
        };
        for (id, service) in cluster.parts.load().iter() {
            let keep: Vec<u64> = map
                .failovers
                .iter()
                .filter(|e| e.partition == *id && e.tail_state() == proto::TailState::Pending)
                .map(|e| e.to_epoch)
                .collect();
            let (Some(gate), Ok(part)) = (service.gate().cloned(), service.part()) else {
                continue;
            };
            if !gate.leased() {
                continue;
            }
            let (db, id) = (part.db.clone(), id.clone());
            #[cfg(test)]
            let dir = self.dir.clone();
            handle.spawn(async move {
                let Ok(held) = gate.enter() else { return };
                let pruned = tokio::task::spawn_blocking(move || {
                    let gone: Vec<u64> = identity::fork_epochs(&db)?
                        .into_iter()
                        .filter(|e| !keep.contains(e))
                        .collect();
                    identity::drop_forks(&db, &gone).map(|()| gone)
                })
                .await;
                match pruned {
                    Ok(Ok(gone)) => {
                        let _ = held.committed();
                        #[cfg(test)]
                        if !gone.is_empty() {
                            seams::PRUNED
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push((dir, id, gone));
                        }
                        #[cfg(not(test))]
                        let _ = gone;
                    }
                    Ok(Err(e)) => {
                        eprintln!("phdb serve: partition {id}'s fork rows weren't pruned: {e}")
                    }
                    Err(e) => eprintln!("phdb serve: partition {id}'s fork row prune: {e}"),
                }
            });
        }
    }
}

/// Test seams (D336 PR 3a's crash tests).
#[cfg(test)]
pub(crate) mod seams {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use crate::serve::errors::ApiError;

    /// Cluster directories whose next (b) drop stops the run right after it, as a crash
    /// between the drop and the apply would.
    pub(crate) static STOP_AFTER_DROP: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    /// Cluster directories whose next applied tail commit stops the run right after its
    /// commit, as a crash after the apply would.
    pub(crate) static STOP_AFTER_APPLY: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    /// The inspection seam: each fork row prune that removed any, by cluster directory,
    /// partition and the epochs it removed.
    pub(crate) static PRUNED: Mutex<Vec<(PathBuf, String, Vec<u64>)>> = Mutex::new(Vec::new());

    /// The inspection seam: each read-only ask of a home (Amendment 5's (a)), by cluster
    /// directory and xid.
    pub(crate) static ASKED_HOME: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

    pub(crate) fn after_drop(dir: &Path) -> Result<(), ApiError> {
        stop_once(&STOP_AFTER_DROP, dir, "the test's stop after the drop")
    }

    pub(crate) fn after_apply(dir: &Path) -> Result<(), ApiError> {
        stop_once(&STOP_AFTER_APPLY, dir, "the test's stop after the apply")
    }

    fn stop_once(list: &Mutex<Vec<PathBuf>>, dir: &Path, why: &str) -> Result<(), ApiError> {
        let mut armed = list
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match armed.iter().position(|d| d == dir) {
            Some(i) => {
                armed.swap_remove(i);
                Err(ApiError::unavailable(why.to_owned()))
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// cli692's M-a on #2265: a mark's cells take the mark's seqno as their timestamp, so the
    /// newest mark is the one read back, whatever the clock of the owner that wrote it: no
    /// clock is read at all. Each mark's cells carry its seqno, and each later mark is read.
    #[test]
    fn a_marks_cells_take_its_seqno_as_their_timestamp_so_the_newest_mark_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let db = Pigeonhole::open(
            dir.path().join("p.phdb"),
            pigeonhole::Options::default().create_if_missing(true),
        )
        .unwrap();
        let t = marks_table(&db).unwrap();
        for seqno in [5u64, 9, 12] {
            let mut b = db.logged_batch();
            push_mark(
                &mut b,
                &t,
                3,
                &Mark {
                    seqno,
                    token: vec![u8::try_from(seqno).unwrap()],
                },
            );
            b.commit_with(pigeonhole::Durability::GroupSync).unwrap();
            assert_eq!(
                read_mark(&db, 3).unwrap(),
                Some(Mark {
                    seqno,
                    token: vec![u8::try_from(seqno).unwrap()],
                })
            );
            for q in [&b"seqno"[..], b"token"] {
                let cell = t.get(&mark_row(3), FAMILY, q).unwrap().unwrap();
                assert_eq!(cell.timestamp(), seqno, "{q:?}");
            }
        }
        // Another history's mark is its own row.
        assert_eq!(read_mark(&db, 4).unwrap(), None);
        db.close().unwrap();
    }
}
