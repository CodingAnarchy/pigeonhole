//! Transactions across partitions (D329, ICR 0140; `.coord/simp724-design-1483.md` §4).
//!
//! A transaction begun with `BeginRequest.spans_partitions` has a home, the partition its
//! `Begin` routed to, and an id that is also its xid: `<home>:h<history_epoch>-<32 hex>`
//! ([`super::super::sessions::spanning_parts`]). Every call on it routes to the home's owner,
//! the coordinator. A call on a row of another partition P opens a branch there (a session
//! in P's table under the same id, at P's snapshot then) and goes on to P's owner as a
//! forward naming P. A branch on the home's own node is a local call.
//!
//! **Commit, when anything was written** (by a `Write`, or by the `Commit`'s own rows,
//! anywhere):
//! 1. Every branch prepares, durably, in parallel (a branch that only read too: its hold is
//!    what the commit point relies on).
//! 2. The commit point: the home's own transaction reads the decision row
//!    `__phdb_txn_decisions/<xid>` (it must be absent) and writes `COMMITTED` there, through
//!    the home's gate, at `GroupSync` or stronger. Before it, any refusal aborts the
//!    prepared branches and answers `Conflict`. An `OutcomeUnknown` there passes through:
//!    the branches stay in doubt for their resolver (D329 PR 2b).
//! 3. Every branch is decided, and syncs before it answers. The reply waits for them up to
//!    the call's deadline, then answers with the parts not yet applied marked.
//!
//! **Commit, when nothing was written:** there is no commit point. Every partition that
//! read, the home too, holds its reads in memory ([`pigeonhole::Transaction::hold_reads`]),
//! then a release round confirms each hold lasted, under the same lease term as it was
//! taken in (audit120's conditions). Any lost hold answers `Conflict`.
//!
//! The coordinator's table ([`Spanning::homes`]) and the branch side's holds
//! ([`Spanning::holds`]) are this node's, in memory: a restart loses both, which a home
//! answers as an expired transaction and a branch's release as a lost hold.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::super::auth::Caller;
use super::super::errors::ApiError;
use super::super::proto;
use super::super::proto::forward_request::Op;
use super::super::proto::forward_response::Reply;
use super::super::router::Parts;
use super::super::service::{Service, ServiceOp, ServiceReply};
use super::Node;
use super::forward::{self, Forwarder};

/// The table a spanning transaction's decision rows live in, on its home partition: reserved
/// (`__phdb_`), so no client reads or writes it, and hidden like `__phdb_cluster`.
pub(crate) const DECISIONS: &str = "__phdb_txn_decisions";
/// Its family and the decision's qualifier.
pub(crate) const DECISION_FAMILY: &str = "d";
pub(crate) const DECISION_QUALIFIER: &[u8] = b"decision";

/// The map feature every node must keep before a transaction may span partitions.
pub(crate) const SPANNING_TXNS_FEATURE: &str = "spanning_txns";

/// A `COMMITTED` decision row's value: the home's history epoch and the branches, one per
/// line after the first (2b's resolver and `Outcome` read it).
pub(crate) fn committed_value(history_epoch: u64, branches: &[String]) -> Vec<u8> {
    let mut v = format!("committed h{history_epoch}");
    for b in branches {
        v.push('\n');
        v.push_str(b);
    }
    v.into_bytes()
}

/// What one partition did in a spanning transaction, as its home sees it.
#[derive(Debug, Default, Clone, Copy)]
struct Touched {
    read: bool,
    wrote: bool,
    /// When the branch was last opened or renewed on its owner (`None` for the home).
    renewed: Option<Instant>,
}

/// A spanning transaction's coordinator state: what each partition did, the home included.
#[derive(Debug, Default)]
struct Home {
    parts: BTreeMap<String, Touched>,
}

/// A read-only branch's hold, kept until its release.
struct Hold {
    hold: pigeonhole::ReadHold,
    partition: String,
    term: u64,
}

/// This node's spanning transactions: those it coordinates, and the read-only holds its
/// partitions keep for others' release rounds.
pub(crate) struct Spanning {
    homes: Mutex<HashMap<Arc<str>, Home>>,
    holds: Mutex<HashMap<u64, Hold>>,
    /// The high half of every hold id: random per process, so a hold id from before a
    /// restart never names one taken after it (audit120's condition 1).
    nonce: u64,
    next: AtomicU64,
}

impl std::fmt::Debug for Spanning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spanning").finish_non_exhaustive()
    }
}

impl Spanning {
    pub(crate) fn new() -> Arc<Self> {
        // Random, or the clock's nanoseconds if the system's random source fails: either
        // way different from any earlier process's on this node in practice.
        let nonce = super::super::sessions::random_hex()
            .ok()
            .and_then(|h| u64::from_str_radix(&h[..8], 16).ok())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::from(d.subsec_nanos()) ^ d.as_secs())
                    & 0xffff_ffff
            });
        Arc::new(Self {
            homes: Mutex::new(HashMap::new()),
            holds: Mutex::new(HashMap::new()),
            nonce: nonce << 32,
            next: AtomicU64::new(1),
        })
    }

    fn homes(&self) -> std::sync::MutexGuard<'_, HashMap<Arc<str>, Home>> {
        self.homes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn holds(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Hold>> {
        self.holds.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records that `xid` touched `partition` (reading, or writing).
    fn touch(&self, xid: &str, partition: &str, wrote: bool) {
        let mut homes = self.homes();
        let home = homes.entry(xid.into()).or_default();
        let t = home.parts.entry(partition.to_owned()).or_default();
        t.read |= !wrote;
        t.wrote |= wrote;
    }

    /// Whether branch `partition` of `xid` must be opened or renewed on its owner now: never
    /// opened, or last renewed `renew_after` ago (S4: renewed with the home's own calls).
    fn needs_begin(&self, xid: &str, partition: &str, renew_after: Duration) -> bool {
        let homes = self.homes();
        homes
            .get(xid)
            .and_then(|h| h.parts.get(partition))
            .and_then(|t| t.renewed)
            .is_none_or(|at| at.elapsed() >= renew_after)
    }

    /// The branches `xid` has opened (every partition touched but `home`).
    fn branches(&self, xid: &str, home: &str) -> std::collections::BTreeSet<String> {
        self.homes()
            .get(xid)
            .map(|h| {
                h.parts
                    .iter()
                    .filter(|(p, t)| *p != home && t.renewed.is_some())
                    .map(|(p, _)| p.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn begun(&self, xid: &str, partition: &str) {
        let mut homes = self.homes();
        let home = homes.entry(xid.into()).or_default();
        home.parts.entry(partition.to_owned()).or_default().renewed = Some(Instant::now());
    }

    /// What `xid` touched, by partition, and forgets it: its commit or abort has begun.
    fn take(&self, xid: &str) -> BTreeMap<String, Touched> {
        self.homes()
            .remove(xid)
            .map(|h| h.parts)
            .unwrap_or_default()
    }

    /// Keeps `hold`, taken under lease term `term` on `partition`, until its release.
    pub(crate) fn keep_hold(&self, hold: pigeonhole::ReadHold, partition: &str, term: u64) -> u64 {
        let id = self.nonce | (self.next.fetch_add(1, Ordering::Relaxed) & 0xffff_ffff);
        self.holds().insert(
            id,
            Hold {
                hold,
                partition: partition.to_owned(),
                term,
            },
        );
        id
    }

    /// Hold `id`, if this process took it for `partition`. Whoever takes it releases it.
    pub(crate) fn take_hold(
        &self,
        id: u64,
        partition: &str,
    ) -> Option<(pigeonhole::ReadHold, u64)> {
        let mut holds = self.holds();
        if holds.get(&id).is_none_or(|h| h.partition != partition) {
            return None;
        }
        holds.remove(&id).map(|h| (h.hold, h.term))
    }
}

/// Whether forwarded request `msg` is a spanning transaction's branch call (D329): one of
/// the branch arms, or a transaction call on a branch, which names its partition.
pub(crate) fn is_branch(msg: &proto::ForwardRequest) -> bool {
    match &msg.op {
        Some(Op::BranchBegin(_) | Op::BranchPrepare(_) | Op::BranchDecide(_)) => true,
        Some(Op::TxnGet(_) | Op::TxnGetRow(_) | Op::TxnWrite(_)) => !msg.partition.is_empty(),
        _ => false,
    }
}

/// The table and row of each row a transaction call on a branch names.
fn rows_of(op: &ServiceOp) -> Vec<(&str, &[u8])> {
    match op {
        ServiceOp::TxnGet(r) => vec![(r.table.as_str(), &r.row[..])],
        ServiceOp::TxnGetRow(r) => vec![(r.table.as_str(), &r.row[..])],
        ServiceOp::TxnWrite(r) => r
            .rows
            .iter()
            .filter_map(|w| w.row.as_ref())
            .map(|k| (k.table.as_str(), &k.row[..]))
            .collect(),
        _ => Vec::new(),
    }
}

/// A branch call on `partition`, which `service` serves on this node (D329): another node's
/// forward ([`is_branch`]) or the home's own call to a branch on its node.
pub(crate) async fn serve_branch(
    node: &Node,
    spanning: &Spanning,
    service: Arc<Service>,
    partition: &str,
    op: Op,
    caller: &Caller,
) -> Result<proto::ForwardResponse, ApiError> {
    let reply = match op {
        Op::BranchBegin(b) => {
            service.begin_branch(&b.xid, caller)?;
            Reply::Empty(proto::Empty {})
        }
        Op::BranchPrepare(b) => Reply::BranchPrepare(
            service
                .prepare_branch(&b.xid, b.read_only, spanning, partition)
                .await?,
        ),
        Op::BranchDecide(b) => {
            match service
                .decide_branch(&b.xid, b.commit, b.hold, b.term, spanning, partition)
                .await?
            {
                Some(c) => Reply::Commit(proto::TxnCommitResponse {
                    commit: Some(super::super::convert::commit_info(c)),
                    parts: Vec::new(),
                }),
                None => Reply::Empty(proto::Empty {}),
            }
        }
        op => {
            let op = forward::op_of(Some(op))?;
            let id = match &op {
                ServiceOp::TxnGet(r) => &r.txn_id,
                ServiceOp::TxnGetRow(r) => &r.txn_id,
                ServiceOp::TxnWrite(r) => &r.txn_id,
                _ => return Err(ApiError::invalid("not a branch's call")),
            };
            if super::super::sessions::spanning_parts(id).is_none() {
                return Err(ApiError::invalid(
                    "a branch's call names a spanning transaction",
                ));
            }
            // Every row on this partition by this node's map; one that isn't is a map change
            // since the home routed it, which fails the transaction (the client retries it).
            {
                let routes = node.routes();
                let routes = routes.as_ref().ok_or_else(|| {
                    ApiError::wrong_partition(0, "this node has no cluster map yet")
                })?;
                for (table, row) in rows_of(&op) {
                    if routes.partition(table, row) != Some(partition) {
                        return Err(ApiError::wrong_partition(
                            routes.map_version(),
                            format!("a row of the call isn't in partition {partition:?} here"),
                        ));
                    }
                }
            }
            let reply = service.call(op, caller, None).await?;
            return Ok(forward::reply_to_forward(reply, 0)?.0);
        }
    };
    Ok(proto::ForwardResponse {
        reply: Some(reply),
        ..Default::default()
    })
}

/// A spanning transaction's calls to its branches, from its home's node: the coordinator's
/// side. Built per call; cheap (handles to the node's parts).
#[derive(Clone)]
pub(crate) struct Driver {
    pub(crate) node: Arc<Node>,
    pub(crate) parts: Parts,
    pub(crate) forwarder: Arc<Forwarder>,
    pub(crate) spanning: Arc<Spanning>,
    pub(crate) xid: Arc<str>,
    pub(crate) home: Arc<str>,
    pub(crate) caller: Caller,
    pub(crate) deadline: tokio::time::Instant,
    /// How long a branch may go unrenewed: half the transactions' idle lease.
    pub(crate) renew_after: Duration,
    /// `--txn-max-branches`.
    pub(crate) max_branches: usize,
}

/// One partition's prepared share, as the home's release or decide round needs it.
#[derive(Debug, Clone)]
pub(crate) struct Prepared {
    pub(crate) partition: String,
    pub(crate) hold: u64,
    pub(crate) term: u64,
}

impl Driver {
    fn left(&self) -> Result<Duration, ApiError> {
        self.deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(ApiError::deadline_exceeded)
    }

    /// Sends `op` to branch `partition`'s owner, or serves it here if that's this node.
    async fn send(
        &self,
        partition: &str,
        op: Op,
        writes: bool,
    ) -> Result<proto::ForwardResponse, ApiError> {
        let (owner, map_version) = {
            let routes = self.node.routes();
            let routes = routes
                .as_ref()
                .ok_or_else(|| ApiError::wrong_partition(0, "this node has no cluster map yet"))?;
            let owner = routes.owner(partition).cloned().ok_or_else(|| {
                ApiError::wrong_partition(
                    routes.map_version(),
                    format!("the map no longer lists partition {partition:?}"),
                )
            })?;
            (owner, routes.map_version())
        };
        if owner.node == self.node.id() {
            let service = self
                .parts
                .load()
                .get(partition)
                .cloned()
                .ok_or_else(|| super::super::router::not_open(partition))?;
            return serve_branch(
                &self.node,
                &self.spanning,
                service,
                partition,
                op,
                &self.caller,
            )
            .await;
        }
        let req = proto::ForwardRequest {
            map_version,
            caller: Some(forward::forwarded_caller(&self.caller)?),
            partition: partition.to_owned(),
            op: Some(op),
            ..Default::default()
        };
        self.forwarder
            .send(&owner.address, req, self.left()?, writes)
            .await
    }

    /// Opens (or renews) branch `partition` if it's due, then records what the call does.
    async fn branch(&self, partition: &str, wrote: bool) -> Result<(), ApiError> {
        if self
            .spanning
            .needs_begin(&self.xid, partition, self.renew_after)
        {
            let open = self.spanning.branches(&self.xid, &self.home);
            if !open.contains(partition) && open.len() >= self.max_branches {
                return Err(ApiError::failed_precondition(format!(
                    "the transaction already touches {} partitions besides its home, the most \
                     --txn-max-branches allows",
                    open.len()
                )));
            }
            let begin = Op::BranchBegin(proto::BranchBegin {
                xid: self.xid.to_string(),
            });
            match self.send(partition, begin, false).await {
                Ok(_) => self.spanning.begun(&self.xid, partition),
                // A branch begun at an owner the map has since moved: the transaction can't
                // go on there (its snapshot and buffered writes are the old owner's).
                Err(e) => return Err(e),
            }
        }
        self.spanning.touch(&self.xid, partition, wrote);
        Ok(())
    }

    /// A transaction call (`TxnGet`, `TxnGetRow`, `TxnWrite`) on branch `partition`.
    pub(crate) async fn call(
        &self,
        partition: &str,
        op: ServiceOp,
    ) -> Result<ServiceReply, ApiError> {
        let wrote = matches!(op, ServiceOp::TxnWrite(_));
        self.branch(partition, wrote).await?;
        let reply = self
            .send(partition, forward::op_to_forward(op)?, wrote)
            .await?;
        forward::reply_of(reply)
    }

    /// Records the home's own call (`wrote`: a write).
    pub(crate) fn home_touched(&self, wrote: bool) {
        let home = self.home.to_string();
        self.spanning.touch(&self.xid, &home, wrote);
    }

    /// Takes what the transaction touched, by partition, as its commit or abort starts.
    pub(crate) fn finish(&self) -> Touches {
        Touches(self.spanning.take(&self.xid))
    }

    /// Checks again that every node keeps `spanning_txns` (a node on an older build may
    /// have joined since `Begin`).
    pub(crate) async fn check_feature(&self) -> Result<(), ApiError> {
        check_feature(&self.node, self.deadline).await
    }

    /// Prepares every branch in `parts` at once: read-only holds, or durable prepares.
    /// Returns those that prepared, and the first refusal.
    pub(crate) async fn prepare(
        &self,
        parts: &[String],
        read_only: bool,
    ) -> (Vec<Prepared>, Option<ApiError>) {
        let calls = parts.iter().map(|p| {
            let (d, p) = (self.clone(), p.clone());
            tokio::spawn(async move {
                let op = Op::BranchPrepare(proto::BranchPrepare {
                    xid: d.xid.to_string(),
                    read_only,
                });
                let r = d.send(&p, op, !read_only).await;
                (p, r)
            })
        });
        let mut done = Vec::new();
        let mut refused = None;
        for (partition, r) in joined(calls.collect()).await {
            match r.and_then(|r| match r.reply {
                Some(Reply::BranchPrepare(b)) => Ok(b),
                _ => Err(ApiError::internal(
                    "a branch's prepare answered something else",
                )),
            }) {
                Ok(b) => done.push(Prepared {
                    partition,
                    hold: b.hold,
                    term: b.term,
                }),
                Err(e) => {
                    refused.get_or_insert(e);
                }
            }
        }
        (done, refused)
    }

    /// Decides every branch in `parts` (commit or abort), at once, each syncing before it
    /// answers. A branch that answers in time is applied; one that doesn't stays in doubt
    /// for its resolver, which the parts say.
    pub(crate) async fn decide(&self, parts: &[String], commit: bool) -> Vec<proto::TxnPart> {
        let calls = parts.iter().map(|p| {
            let (d, p) = (self.clone(), p.clone());
            tokio::spawn(async move {
                let op = Op::BranchDecide(proto::BranchDecide {
                    xid: d.xid.to_string(),
                    commit,
                    hold: 0,
                    term: 0,
                });
                let r = d.send(&p, op, true).await;
                (p, r)
            })
        });
        joined(calls.collect())
            .await
            .into_iter()
            .map(|(partition, r)| {
                let seqno = match &r {
                    Ok(proto::ForwardResponse {
                        reply: Some(Reply::Commit(c)),
                        ..
                    }) => c.commit.as_ref().map_or(0, |c| c.seqno),
                    _ => 0,
                };
                proto::TxnPart {
                    partition,
                    seqno,
                    applied: r.is_ok(),
                }
            })
            .collect()
    }

    /// Releases the read-only holds in `held`, at once: whether every one lasted.
    pub(crate) async fn release(&self, held: &[Prepared]) -> bool {
        let calls = held.iter().map(|h| {
            let (d, h) = (self.clone(), h.clone());
            tokio::spawn(async move {
                let op = Op::BranchDecide(proto::BranchDecide {
                    xid: d.xid.to_string(),
                    commit: true,
                    hold: h.hold,
                    term: h.term,
                });
                let r = d.send(&h.partition, op, false).await.map(drop);
                (h.partition, r)
            })
        });
        joined(calls.collect())
            .await
            .into_iter()
            .all(|(_, r)| r.is_ok())
    }

    /// Aborts the branches in `parts` (best effort): a prepared one's share is dropped, an
    /// open one's session ends. One that doesn't answer is its resolver's (2b) or expires.
    pub(crate) async fn abort(&self, parts: &[String]) {
        let _ = self.decide(parts, false).await;
    }
}

/// Each spawned branch call's answer, in order: a task that panicked is that branch's
/// failure, never a lost answer.
async fn joined<T>(
    calls: Vec<tokio::task::JoinHandle<(String, Result<T, ApiError>)>>,
) -> Vec<(String, Result<T, ApiError>)> {
    let mut out = Vec::with_capacity(calls.len());
    for c in calls {
        out.push(c.await.unwrap_or_else(|e| {
            (
                String::new(),
                Err(ApiError::internal(format!(
                    "a branch call's task failed: {e}"
                ))),
            )
        }));
    }
    out
}

/// What a spanning transaction touched, by partition, the home included.
pub(crate) struct Touches(BTreeMap<String, Touched>);

impl Touches {
    /// Whether any partition buffered writes.
    pub(crate) fn wrote(&self) -> bool {
        self.0.values().any(|t| t.wrote)
    }

    /// Whether `partition` read anything.
    pub(crate) fn read(&self, partition: &str) -> bool {
        self.0.get(partition).is_some_and(|t| t.read)
    }

    /// The branches: every partition touched but `home`.
    pub(crate) fn branches(&self, home: &str) -> Vec<String> {
        self.0.keys().filter(|p| *p != home).cloned().collect()
    }
}

/// Whether every node of the map keeps `spanning_txns`: `FailedPrecondition` naming any
/// that doesn't. A node that doesn't answer isn't counted here: a branch on it fails when
/// it's reached.
pub(crate) async fn check_feature(
    node: &Arc<Node>,
    deadline: tokio::time::Instant,
) -> Result<(), ApiError> {
    let owners: Vec<(String, String)> = {
        let routes = node.routes();
        let Some(routes) = routes.as_ref() else {
            return Err(ApiError::wrong_partition(
                0,
                "this node has no cluster map yet",
            ));
        };
        routes.nodes().collect()
    };
    tokio::time::timeout_at(
        deadline,
        node.owners_keep(
            owners.into_iter(),
            SPANNING_TXNS_FEATURE,
            "a transaction across partitions",
        ),
    )
    .await
    .map_err(|_| ApiError::deadline_exceeded())??;
    Ok(())
}

/// Whether `op` is a spanning transaction's call (D329): a `Begin` that opts in, or a
/// transaction call on a spanning id. Its home's owner serves it with [`serve_home`].
pub(crate) fn spanning_call(op: &ServiceOp) -> bool {
    let spanning = |id: &str| super::super::sessions::spanning_parts(id).is_some();
    match op {
        ServiceOp::Begin(r) => r.spans_partitions,
        ServiceOp::TxnGet(r) => spanning(&r.txn_id),
        ServiceOp::TxnGetRow(r) => spanning(&r.txn_id),
        ServiceOp::TxnWrite(r) => spanning(&r.txn_id),
        ServiceOp::Commit(r) => spanning(&r.txn_id),
        ServiceOp::Abort(r) => spanning(&r.txn_id),
        _ => false,
    }
}

/// A spanning transaction's call at its home's owner (D329), `home` the home partition's
/// service here: a row of the home is the home's, and any other partition's row goes to
/// that partition's branch through `driver`.
pub(crate) async fn serve_home(
    driver: Driver,
    home: Arc<Service>,
    op: ServiceOp,
    caller: &Caller,
    prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<ServiceReply, ApiError> {
    let partition_of = |table: &str, row: &[u8]| -> Result<String, ApiError> {
        let routes = driver.node.routes();
        let routes = routes
            .as_ref()
            .ok_or_else(|| ApiError::wrong_partition(0, "this node has no cluster map yet"))?;
        routes
            .partition(table, row)
            .map(str::to_owned)
            .ok_or_else(|| {
                ApiError::wrong_partition(
                    routes.map_version(),
                    format!("no partition holds table {table:?}'s row"),
                )
            })
    };
    // Rows by partition, the home's apart.
    let split = |rows: Vec<proto::RowWrites>| -> Result<_, ApiError> {
        let mut mine = Vec::new();
        let mut others: BTreeMap<String, Vec<proto::RowWrites>> = BTreeMap::new();
        for rw in rows {
            let key = rw
                .row
                .as_ref()
                .ok_or_else(|| ApiError::invalid("each row's writes name its table and row"))?;
            let p = partition_of(&key.table, &key.row)?;
            if *p == *driver.home {
                mine.push(rw);
            } else {
                others.entry(p).or_default().push(rw);
            }
        }
        Ok((mine, others))
    };
    match op {
        ServiceOp::Begin(r) => {
            check_feature(&driver.node, driver.deadline).await?;
            let epoch = {
                let routes = driver.node.routes();
                routes
                    .as_ref()
                    .and_then(|r| r.owner(&driver.home))
                    .map(|o| o.history_epoch)
                    .ok_or_else(|| super::super::router::not_open(&driver.home))?
            };
            let admit = ServiceOp::Begin(r.clone()).admit();
            drop(home.admit(&admit, caller, prepaid).await?);
            Ok(ServiceReply::Begin(
                home.begin_spanning(r, caller, epoch).await?,
            ))
        }
        ServiceOp::TxnGet(ref r) if partition_of(&r.table, &r.row)? != *driver.home => {
            let p = partition_of(&r.table, &r.row)?;
            let budget = home.admit(&op.admit(), caller, prepaid).await?;
            let reply = driver.call(&p, op).await;
            drop(budget);
            reply
        }
        ServiceOp::TxnGetRow(ref r) if partition_of(&r.table, &r.row)? != *driver.home => {
            let p = partition_of(&r.table, &r.row)?;
            let budget = home.admit(&op.admit(), caller, prepaid).await?;
            let reply = driver.call(&p, op).await;
            drop(budget);
            reply
        }
        ServiceOp::TxnGet(_) | ServiceOp::TxnGetRow(_) => {
            driver.home_touched(false);
            home.call(op, caller, prepaid).await
        }
        ServiceOp::TxnWrite(r) => {
            let admit = ServiceOp::TxnWrite(r.clone()).admit();
            let budget = home.admit(&admit, caller, prepaid).await?;
            let (mine, others) = split(r.rows)?;
            for (p, rows) in others {
                let w = ServiceOp::TxnWrite(proto::TxnWriteRequest {
                    txn_id: r.txn_id.clone(),
                    rows,
                });
                driver.call(&p, w).await?;
            }
            if !mine.is_empty() {
                driver.home_touched(true);
                let w = ServiceOp::TxnWrite(proto::TxnWriteRequest {
                    txn_id: r.txn_id,
                    rows: mine,
                });
                drop(budget);
                return home.call(w, caller, None).await;
            }
            Ok(ServiceReply::Empty(proto::Empty {}))
        }
        ServiceOp::Commit(r) => {
            let admit = ServiceOp::Commit(r.clone()).admit();
            let budget = home.admit(&admit, caller, prepaid).await?;
            let (mine, others) = split(r.rows)?;
            for (p, rows) in others {
                let w = ServiceOp::TxnWrite(proto::TxnWriteRequest {
                    txn_id: r.txn_id.clone(),
                    rows,
                });
                driver.call(&p, w).await?;
            }
            if !mine.is_empty() {
                driver.home_touched(true);
            }
            let r = proto::TxnCommitRequest {
                txn_id: r.txn_id,
                rows: mine,
            };
            Ok(ServiceReply::Commit(
                home.commit_spanning(r, caller, budget, driver).await?,
            ))
        }
        ServiceOp::Abort(r) => {
            let touched = driver.finish();
            driver.abort(&touched.branches(&driver.home)).await;
            home.call(ServiceOp::Abort(r), caller, prepaid).await
        }
        _ => Err(ApiError::internal("not a spanning transaction's call")),
    }
}

/// Hold seams for a spanning commit's steps (D329's crash steps, tests only): a test pauses
/// one transaction's commit at a step, does what it needs meanwhile (fence a partition), and
/// lets it go on, or abandon there as if its home had died (nothing after the step runs).
/// Keyed by xid, so tests running at once don't meet.
#[cfg(test)]
pub(crate) mod seams {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, PoisonError};

    use tokio::sync::{Notify, oneshot};

    /// Where a commit pauses: just before each step.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub(crate) enum Step {
        /// Before a read-only commit's release round: every hold taken, none released.
        Release,
        /// Before the commit point: every branch prepared, the home's commit not yet run.
        CommitPoint,
        /// Before the decides: the commit point committed, no branch decided.
        Decide,
    }

    type Waiting = (Arc<Notify>, oneshot::Receiver<bool>);

    static PAUSES: Mutex<Option<HashMap<(String, Step), Waiting>>> = Mutex::new(None);

    /// A paused step: `reached` once the commit is there; `go` lets it on (`false`) or
    /// abandons it there (`true`).
    pub(crate) struct Pause {
        pub(crate) reached: Arc<Notify>,
        pub(crate) go: oneshot::Sender<bool>,
    }

    /// Pauses transaction `xid`'s commit at `step`.
    pub(crate) fn pause(xid: &str, step: Step) -> Pause {
        let reached = Arc::new(Notify::new());
        let (go, rx) = oneshot::channel();
        PAUSES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(HashMap::new)
            .insert((xid.to_owned(), step), (Arc::clone(&reached), rx));
        Pause { reached, go }
    }

    /// At `step` of `xid`'s commit: waits if a test paused it there; whether to abandon.
    pub(crate) async fn at(xid: &str, step: Step) -> bool {
        let waiting = PAUSES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .and_then(|m| m.remove(&(xid.to_owned(), step)));
        let Some((reached, go)) = waiting else {
            return false;
        };
        reached.notify_one();
        go.await.unwrap_or(false)
    }
}
