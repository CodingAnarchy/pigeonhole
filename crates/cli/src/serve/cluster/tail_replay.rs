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

use std::collections::{BTreeMap, VecDeque};
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

/// A logged put of `value` to `row`'s `q` in the marks' family, now.
fn put(b: &mut pigeonhole::replication::LoggedBatch<'_>, t: &pigeonhole::Table, row: &[u8], q: &[u8], value: &[u8]) {
    // A plain value's stored form: its tag, then its bytes.
    let stored = [&[0u8][..], value].concat();
    b.push(t, row, FAMILY, ChangeKind::Put, q, now_micros(), &stored);
}

fn push_mark(b: &mut pigeonhole::replication::LoggedBatch<'_>, t: &pigeonhole::Table, h: u64, m: &Mark) {
    let row = mark_row(h);
    put(b, t, &row, b"seqno", m.seqno.to_string().as_bytes());
    put(b, t, &row, b"token", &m.token);
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
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
    Apply { kept: Vec<usize>, passed: Option<&'static str> },
    /// Nothing to apply: the mark alone moves.
    Skip(&'static str),
    /// Refused whole (R1): the mark moves past it, and it's named.
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
                    self.replay_one(id, run, &cx, &mut mark, c).await?;
                }
                None => return Err(ApiError::internal("an empty ReadTail message")),
            }
        }
    }

    /// One tail commit: judged, then applied, skipped or refused, with the mark.
    async fn replay_one(
        &self,
        id: &str,
        run: &Run,
        cx: &Ctx,
        mark: &mut Mark,
        c: proto::TailCommit,
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
            return self.pass_tail(id, run, cx, mark, next, "already", None).await;
        }
        if !xid.is_empty() {
            return Err(ApiError::failed_precondition(format!(
                "tail commit {} is a share of transaction {}: PR 3a's replay stops at a D329 \
                 share or decision; discard the tail, or wait for a build that settles it",
                c.seqno,
                String::from_utf8_lossy(&xid)
            )));
        }
        let map = self.get_map()?;
        let routes = self.routes();
        let routes = routes
            .as_ref()
            .ok_or_else(|| ApiError::partition_unavailable("this node has no map yet"))?;
        let derived = derived_tables(&map);
        let records = super::schema::records(&cx.db).map_err(ApiError::internal)?;
        let verdict = Judge {
            partition: id,
            routes,
            derived: &derived,
            records: &records,
        }
        .judge(&c);
        match verdict {
            Verdict::Skip(why) => self.pass_tail(id, run, cx, mark, next, why, None).await,
            Verdict::Refuse(why) => {
                eprintln!(
                    "phdb serve: partition {id}'s tail replay refused commit {} (history epoch \
                     {h}): {why}",
                    c.seqno
                );
                self.pass_tail(id, run, cx, mark, next, "refused", Some(why)).await
            }
            Verdict::Apply { kept, passed } => {
                if let Some(why) = passed {
                    run.named(c.seqno, why);
                }
                if run.dry_run {
                    run.progress().applied += 1;
                    *mark = next;
                    return Ok(());
                }
                // The other history's mark moves with it, with that history's token as the
                // earlier replay wrote it in this very commit (S3's chain).
                let other_mark = (origin.history_epoch != h).then(|| Mark {
                    seqno: origin.seqno,
                    token: carried_token(&c, origin.history_epoch)
                        .or_else(|| other.map(|m| m.token))
                        .unwrap_or_default(),
                });
                let tag = replay_tag::encode(origin, &xid).ok_or_else(|| {
                    ApiError::internal(format!(
                        "tail commit {}'s replay tag passes 255 bytes",
                        c.seqno
                    ))
                })?;
                let (db, changes) = (cx.db.clone(), c.changes);
                let (m, om) = (next.clone(), other_mark);
                let held = cx.gate.enter()?;
                tokio::task::spawn_blocking(move || -> Result<(), ApiError> {
                    let marks = marks_table(&db)?;
                    let mut tables: BTreeMap<&str, pigeonhole::Table> = BTreeMap::new();
                    let mut b = db.logged_batch();
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
                                "table {} here has no family {}: its schema reconcile hasn't \
                                 caught up; the replay stops, and a rerun resumes",
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
                Ok(())
            }
        }
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
            self.commit_mark_only(cx, run.from_history_epoch, &next).await?;
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

    /// `ReadTail` on `event`'s old node, from `mark`.
    async fn open_tail(
        &self,
        event: &proto::FailoverEvent,
        mark: &Mark,
    ) -> Result<std::pin::Pin<Box<dyn Stream<Item = Result<proto::ReadTailEvent, ApiError>> + Send>>, ApiError>
    {
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
