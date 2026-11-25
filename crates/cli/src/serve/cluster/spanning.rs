//! Transactions across partitions (D329, ICR 0140; `.coord/simp724-design-1483.md` §4).
//!
//! A transaction begun with `BeginRequest.spans_partitions` has a home, the partition its
//! `Begin` routed to, and an id that is also its xid: `<home>:h<history_epoch>-<32 hex>`
//! ([`super::super::sessions::spanning_parts`]). Every call on it routes to the home's owner,
//! the coordinator, which takes the home's session first: its caller, its renewal, and the
//! coordinator's state ([`Span`]: what each partition did) all live in it (audit120's M1 on
//! #2062). A call on a row of another partition P opens a branch there (a session in P's
//! table under the same id, at P's snapshot then) and goes on to P's owner as a forward
//! naming P; every later call of the transaction renews its branches (S4, M2). A branch on
//! the home's own node is a local call.
//!
//! **Commit, when anything was written** (by a `Write`, or by the `Commit`'s own rows,
//! anywhere):
//! 1. Every branch that read or wrote prepares, durably, in parallel (a branch that only
//!    read too: its hold is what the commit point relies on).
//! 2. The commit point: the home's own transaction reads the decision row
//!    `__phdb_txn_decisions/<xid>` (it must be absent) and writes `COMMITTED` there, through
//!    the home's gate, at `GroupSync` or stronger. Before it, any refusal aborts the
//!    prepared branches and answers `Conflict`. An `OutcomeUnknown` there passes through:
//!    the branches stay in doubt for their resolver (D329 PR 2b).
//! 3. Every branch is decided, and syncs before it answers. The decides run on a budget of
//!    their own; the reply waits for them up to the call's deadline, then answers with the
//!    parts not yet applied marked.
//!
//! **Commit, when nothing was written:** there is no commit point. Every partition that
//! read, the home too, holds its reads in memory ([`pigeonhole::Transaction::hold_reads`]),
//! then a release round confirms each hold lasted, under the same lease term as it was
//! taken in (audit120's conditions). Any lost hold answers `Conflict`. A hold whose release
//! never comes goes when its time is up ([`Spanning::keep_hold`]).
//!
//! The branch side's holds ([`Spanning::holds`]) are this node's, in memory: a restart loses
//! them, which a branch's release answers as a lost hold.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::super::auth::{Caller, Principal};
use super::super::errors::ApiError;
use super::super::proto;
use super::super::proto::forward_request::Op;
use super::super::proto::forward_response::Reply;
use super::super::router::Parts;
use super::super::service::{Service, ServiceOp, ServiceReply};
use super::super::sessions::{Span, Taken};
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

/// Now, in ms since the epoch: a decision row's time.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Decision row `value` with branches `acked` marked as having applied the commit, or
/// `None` when it isn't a commit or nothing changes.
pub(crate) fn with_acks(value: &[u8], acked: &[String]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(value).ok()?;
    let mut lines = text.lines();
    let head = lines.next()?;
    if !head.starts_with("committed ") {
        return None;
    }
    let mut changed = false;
    let mut out = head.to_owned();
    for line in lines {
        out.push('\n');
        if !line.starts_with('+') && acked.iter().any(|a| a == line) {
            out.push('+');
            changed = true;
        }
        out.push_str(line);
    }
    changed.then(|| out.into_bytes())
}

/// Whether decision row `value` may go at `now_ms` with `retention_ms` (D329 2b): an abort
/// marker, or a commit every branch acknowledged, whose time is that far past.
pub(crate) fn collectable(value: &[u8], now_ms: u64, retention_ms: u64) -> bool {
    let Ok(text) = std::str::from_utf8(value) else {
        return false;
    };
    let mut lines = text.lines();
    let Some(head) = lines.next() else {
        return false;
    };
    let Some(at) = head
        .split(' ')
        .find_map(|t| t.strip_prefix("at"))
        .and_then(|t| t.parse::<u64>().ok())
    else {
        return false;
    };
    if at.saturating_add(retention_ms) > now_ms {
        return false;
    }
    match head.split(' ').next() {
        Some("aborted") => true,
        Some("committed") => lines.all(|l| l.starts_with('+')),
        _ => false,
    }
}

/// A `COMMITTED` decision row's value: `committed h<epoch> at<ms>`, then one line per branch,
/// `+`-prefixed once it acknowledged applying the commit (2b's resolver, `Outcome` and GC
/// read it).
pub(crate) fn committed_value(history_epoch: u64, branches: &[String]) -> Vec<u8> {
    let mut v = format!("committed h{history_epoch} at{}", now_ms());
    for b in branches {
        v.push('\n');
        v.push_str(b);
    }
    v.into_bytes()
}

/// An abort marker: the decision row's value for a transaction made final as aborted by
/// the resolver or `Outcome` (D329), with its home's history epoch.
pub(crate) fn aborted_value(history_epoch: u64) -> Vec<u8> {
    format!("aborted h{history_epoch} at{}", now_ms()).into_bytes()
}

/// A decision row's outcome: committed (with the branches it names) or aborted.
fn read_decision(value: &[u8]) -> Result<(bool, Vec<String>), ApiError> {
    let text =
        std::str::from_utf8(value).map_err(|_| ApiError::internal("a decision row isn't UTF-8"))?;
    let mut lines = text.lines();
    match lines.next().and_then(|l| l.split_once(' ')) {
        Some(("committed", _)) => Ok((
            true,
            lines
                .map(|l| l.strip_prefix('+').unwrap_or(l).to_owned())
                .collect(),
        )),
        Some(("aborted", _)) => Ok((false, Vec::new())),
        _ => Err(ApiError::internal("a decision row this build can't read")),
    }
}

/// Who asks [`settle`] (#2077, audit120's S5 on #2076).
#[derive(Clone, Copy)]
pub(crate) enum Asker {
    /// A client's `Outcome`: only its own transaction's session answers. Another
    /// principal's is `NotFound`, and nothing further is read or written for it, so a
    /// client can't learn another's outcome while its session lives, nor make its home
    /// record an abort over it.
    Client(Principal),
    /// The node's own call (the resolver's `ResolveTxn`, which only `Cluster.Forward`
    /// carries): a spanning session answers whoever its client is.
    Node,
}

/// What became of transaction `id`, at its home `home` (D329): `Outcome`'s answer, and the
/// resolver's. Its live session answers in progress; its recorded outcome answers as it is
/// (but an unknown commit, which only the decision row settles); then a spanning
/// transaction's decision row. An absent row is made final as aborted while the home's
/// history is the one the transaction began in, and is unknown after a history change
/// (cli692's M1 on the design). A plain transaction past its session is `NotFound`.
pub(crate) async fn settle(
    node: &Node,
    home: &Service,
    id: &str,
    asker: Asker,
) -> Result<proto::TxnOutcomeResponse, ApiError> {
    use super::super::sessions::Peek;
    use proto::TxnOutcomeState as S;
    let answer = |state: S, reason: &str| proto::TxnOutcomeResponse {
        state: state as i32,
        reason: reason.to_owned(),
        ..Default::default()
    };
    let principal = match asker {
        Asker::Client(p) => Some(p),
        Asker::Node => None,
    };
    match home.peek_txn(id, principal) {
        // Another client's transaction, its session still here: not found, and nothing
        // more (its decision row isn't read, and no abort is recorded over it).
        Peek::Foreign => return Err(ApiError::not_found("no such transaction")),
        Peek::Live => {
            return Ok(answer(
                S::InProgress,
                "the transaction is open or committing",
            ));
        }
        Peek::Done(Ok(c)) => {
            return Ok(proto::TxnOutcomeResponse {
                state: S::Committed as i32,
                commit: c.commit,
                parts: c.parts,
                reason: String::new(),
            });
        }
        Peek::Done(Err(e)) if e.code != "OutcomeUnknown" => {
            return Ok(answer(S::Aborted, &e.message));
        }
        // A plain transaction's lost answer: unknown, as one node answers it (audit120's S2
        // on #2076). A spanning one's is settled from its decision row below.
        Peek::Done(Err(e)) if super::super::sessions::spanning_parts(id).is_none() => {
            return Ok(answer(S::Unknown, &e.message));
        }
        Peek::Done(Err(_)) | Peek::Gone => {}
    }
    let Some((home_id, epoch, _)) = super::super::sessions::spanning_parts(id) else {
        return Err(ApiError::not_found(
            "no such transaction (its outcome is kept only while its session's record is)",
        ));
    };
    let value = match home.decision(id).await? {
        Some(v) => v,
        None => {
            // From begin plus retention on, an absent record may be a collected commit: not
            // found, never presumed aborted (D329 2b; audit120's M1 on #2076).
            let begun = super::super::sessions::spanning_begun_ms(id).unwrap_or(0);
            let horizon = u64::try_from(home.outcome_horizon().as_millis()).unwrap_or(u64::MAX);
            let now = now_ms();
            // A client's id dated later than any transaction could have begun (#2077): no
            // transaction, so nothing is recorded for it.
            let lifetime = u64::try_from(home.txn_lifetime().as_millis()).unwrap_or(u64::MAX);
            if matches!(asker, Asker::Client(_)) && begun > now.saturating_add(lifetime) {
                return Err(ApiError::not_found(
                    "no such transaction: its id's begin time is in the future",
                ));
            }
            if begun.saturating_add(horizon) <= now {
                return Err(ApiError::not_found(
                    "no record of the transaction: its outcome is kept for \
                     --txn-outcome-retention after it ends",
                ));
            }
            let current = {
                let routes = node.routes();
                routes
                    .as_ref()
                    .and_then(|r| r.owner(home_id))
                    .map(|o| o.history_epoch)
            };
            if current != Some(epoch) {
                return Ok(answer(
                    S::Unknown,
                    "the home partition's history changed since the transaction began (a \
                     promotion or a restore): its decision may be lost, so nothing is presumed",
                ));
            }
            home.decide_absent(id, epoch).await?
        }
    };
    let (committed, branches) = read_decision(&value)?;
    if !committed {
        return Ok(answer(S::Aborted, "the transaction was not committed"));
    }
    let mut resp = answer(S::Committed, "");
    resp.parts = std::iter::once(home_id.to_owned())
        .chain(branches)
        .map(|partition| proto::TxnPart {
            partition,
            seqno: 0,
            applied: false,
        })
        .collect();
    Ok(resp)
}

/// Asks transaction `xid`'s home what was decided (D329: the resolver, and `DecideTxn`'s
/// check that the home is gone): a `ResolveTxn` to the home's owner by this node's map, or
/// answered here if that's this node. The node's own call, under the cluster token.
pub(crate) async fn ask_home(
    node: &Arc<Node>,
    parts: &Parts,
    forwarder: &Forwarder,
    spanning: &Arc<Spanning>,
    xid: &str,
    acked: Option<&str>,
    timeout: Duration,
) -> Result<proto::TxnResolution, ApiError> {
    let (home, _, _) = super::super::sessions::spanning_parts(xid)
        .ok_or_else(|| ApiError::invalid("not a spanning transaction's id"))?;
    let (owner, map_version) = {
        let routes = node.routes();
        let routes = routes
            .as_ref()
            .ok_or_else(|| ApiError::wrong_partition(0, "this node has no cluster map yet"))?;
        // Retry-safe: this node's map may predate the home (a split's new partition), which
        // a later map lists (ai1249's S2 on #2131). If a merge removed the home instead (it
        // does so only once no share homed there is in doubt on any node that answers), the
        // message names the way out (simp724's S1 on the D331 PR 6 design).
        let owner = routes.owner(home).cloned().ok_or_else(|| {
            ApiError::partition_unavailable(format!(
                "the cluster map here (version {}) doesn't list the transaction's home \
                 partition {home:?}: retry once this node has a later map; if a merge removed \
                 it, decide this share with phdb cluster txn decide",
                routes.map_version()
            ))
        })?;
        (owner, routes.map_version())
    };
    let op = Op::ResolveTxn(proto::ResolveTxn {
        xid: xid.to_owned(),
        acked: acked.unwrap_or_default().to_owned(),
    });
    let caller = Caller::anonymous();
    let reply = if owner.node == node.id() {
        let service = parts
            .load()
            .get(home)
            .cloned()
            .ok_or_else(|| super::super::router::not_open(home))?;
        serve_branch(node, spanning, service, home, op, &caller).await?
    } else {
        let req = proto::ForwardRequest {
            map_version,
            caller: Some(forward::forwarded_caller(&caller)?),
            partition: home.to_owned(),
            op: Some(op),
            ..Default::default()
        };
        forwarder.send(&owner.address, req, timeout, true).await?
    };
    match reply.reply {
        Some(Reply::ResolveTxn(r)) => Ok(r),
        _ => Err(ApiError::internal(
            "a home answered ResolveTxn with something else",
        )),
    }
}

/// What a spanning transaction's [`Span`] says, for its commit.
pub(crate) trait SpanView {
    /// Whether any partition buffered writes.
    fn wrote(&self) -> bool;
    /// Whether `partition` read anything.
    fn read(&self, partition: &str) -> bool;
    /// The branches that read or wrote: every such partition but `home`.
    fn branches(&self, home: &str) -> Vec<String>;
    /// The branches opened that did nothing (their first call failed): ended, not prepared
    /// (audit120's S3).
    fn idle(&self, home: &str) -> Vec<String>;
}

impl SpanView for Span {
    fn wrote(&self) -> bool {
        self.parts.values().any(|t| t.wrote)
    }

    fn read(&self, partition: &str) -> bool {
        self.parts.get(partition).is_some_and(|t| t.read)
    }

    fn branches(&self, home: &str) -> Vec<String> {
        self.parts
            .iter()
            .filter(|(p, t)| *p != home && (t.read || t.wrote))
            .map(|(p, _)| p.clone())
            .collect()
    }

    fn idle(&self, home: &str) -> Vec<String> {
        self.parts
            .iter()
            .filter(|(p, t)| *p != home && !t.read && !t.wrote && t.renewed.is_some())
            .map(|(p, _)| p.clone())
            .collect()
    }
}

/// A read-only branch's hold, kept until its release or its time is up.
struct Hold {
    hold: pigeonhole::ReadHold,
    partition: String,
    term: u64,
}

/// The read-only holds this node's partitions keep for other nodes' release rounds, and why
/// each share the resolver couldn't settle is still in doubt (for `Status`).
pub(crate) struct Spanning {
    holds: Mutex<HashMap<u64, Hold>>,
    /// The resolver's last word on a share it couldn't settle: (partition, xid) → reason.
    reasons: Mutex<HashMap<(String, Vec<u8>), String>>,
    /// Acks still owed to a home (#2078): (xid, branch partition) → (when to send it, the
    /// backoff's step). A branch that applied a commit, by its resolver or by an operator's
    /// `txn decide --commit`, tells the home so its record can be collected; the resolver
    /// sends each due one, and keeps it only while the home doesn't answer or answers in
    /// progress. In memory: a restart loses it, and the home's record then stays (#2078).
    acks: Mutex<HashMap<(String, String), (Instant, Duration)>>,
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
            holds: Mutex::new(HashMap::new()),
            reasons: Mutex::new(HashMap::new()),
            acks: Mutex::new(HashMap::new()),
            nonce: nonce << 32,
            next: AtomicU64::new(1),
        })
    }

    fn acks(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), (Instant, Duration)>> {
        self.acks.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Owes `xid`'s home the ack of branch `partition`'s applied commit, due now (#2078).
    /// Past [`MAX_ACKS`] owed, it's dropped and said so: the home's record then stays.
    pub(crate) fn queue_ack(&self, xid: &str, partition: &str) {
        let mut acks = self.acks();
        if acks.len() >= MAX_ACKS && !acks.contains_key(&(xid.to_owned(), partition.to_owned())) {
            eprintln!(
                "phdb serve: {MAX_ACKS} acks already owed to transactions' homes: {partition}'s \
                 for {xid} is dropped, and that home's record stays"
            );
            return;
        }
        acks.insert(
            (xid.to_owned(), partition.to_owned()),
            (Instant::now(), Duration::ZERO),
        );
    }

    /// The acks due at `now`, as (xid, partition).
    fn due_acks(&self, now: Instant) -> Vec<(String, String)> {
        self.acks()
            .iter()
            .filter(|(_, (at, _))| *at <= now)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// An ack sent: kept for another try after a backoff (`keep`), or done.
    fn ack_sent(&self, key: (String, String), keep: bool) {
        let mut acks = self.acks();
        if !keep {
            acks.remove(&key);
            return;
        }
        if let Some((at, step)) = acks.get_mut(&key) {
            *step = (*step * 2).clamp(Duration::from_secs(1), Duration::from_secs(60));
            *at = Instant::now() + *step;
        }
    }

    fn holds(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Hold>> {
        self.holds.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keeps `hold`, taken under lease term `term` on `partition`, until its release, or
    /// for `ttl` at most: a release that never comes (its home died, its round ran out of
    /// time) leaves no rows held past it (audit120's M4 on #2062). A release after that
    /// finds no hold and answers lost.
    pub(crate) fn keep_hold(
        self: &Arc<Self>,
        hold: pigeonhole::ReadHold,
        partition: &str,
        term: u64,
        ttl: Duration,
    ) -> u64 {
        let id = self.nonce | (self.next.fetch_add(1, Ordering::Relaxed) & 0xffff_ffff);
        self.holds().insert(
            id,
            Hold {
                hold,
                partition: partition.to_owned(),
                term,
            },
        );
        let me = Arc::downgrade(self);
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                tokio::time::sleep(ttl).await;
                if let Some(me) = me.upgrade() {
                    // Dropped outside the lock: dropping a hold releases it on its shards.
                    let gone = me.holds().remove(&id);
                    drop(gone);
                }
            });
        }
        id
    }

    /// Why partition `partition`'s share of `xid` is still in doubt, as the resolver last
    /// found it (`Status`'s `InDoubtShare.reason`).
    pub(crate) fn reason(&self, partition: &str, xid: &[u8]) -> Option<String> {
        self.reasons
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(partition.to_owned(), xid.to_vec()))
            .cloned()
    }

    fn note(&self, partition: &str, xid: &[u8], reason: Option<String>) {
        let mut r = self.reasons.lock().unwrap_or_else(PoisonError::into_inner);
        let key = (partition.to_owned(), xid.to_vec());
        match reason {
            Some(why) => {
                r.insert(key, why);
            }
            None => {
                r.remove(&key);
            }
        }
    }

    /// Hold `id`, if this process took it for `partition` and it's still kept. Whoever
    /// takes it releases it.
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
        Some(
            Op::BranchBegin(_) | Op::BranchPrepare(_) | Op::BranchDecide(_) | Op::ResolveTxn(_),
        ) => true,
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
    spanning: &Arc<Spanning>,
    service: Arc<Service>,
    partition: &str,
    op: Op,
    caller: &Caller,
) -> Result<proto::ForwardResponse, ApiError> {
    let reply = match op {
        Op::BranchBegin(b) => {
            service.begin_branch(&b.xid, caller, b.renew)?;
            Reply::Empty(proto::Empty {})
        }
        Op::BranchPrepare(b) => Reply::BranchPrepare(
            service
                .prepare_branch(&b.xid, b.read_only, spanning, partition)
                .await?,
        ),
        // A share in doubt elsewhere asks this home what was decided (D329's resolver).
        Op::ResolveTxn(r) => {
            if !r.acked.is_empty() {
                service
                    .ack_decision(&r.xid, std::slice::from_ref(&r.acked))
                    .await?;
            }
            let settled = settle(node, &service, &r.xid, Asker::Node).await?;
            Reply::ResolveTxn(proto::TxnResolution {
                state: settled.state,
                reason: settled.reason,
            })
        }
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
/// side. Built per call; cheap (handles to the node's parts). It holds no state of the
/// transaction's: that's the home session's [`Span`], which the caller holds out of its
/// entry with the session.
#[derive(Clone)]
pub(crate) struct Driver {
    pub(crate) node: Arc<Node>,
    pub(crate) parts: Parts,
    pub(crate) forwarder: Arc<Forwarder>,
    pub(crate) spanning: Arc<Spanning>,
    pub(crate) xid: Arc<str>,
    pub(crate) home: Arc<str>,
    pub(crate) caller: Caller,
    /// The client call's deadline.
    pub(crate) deadline: tokio::time::Instant,
    /// How long a branch may go unrenewed: half the transactions' idle lease.
    pub(crate) renew_after: Duration,
    /// `--txn-max-branches`.
    pub(crate) max_branches: usize,
    /// The budget of a round that must reach every branch whatever the client's deadline
    /// (decides, aborts, releases): the request timeout (audit120's S1 on #2062).
    pub(crate) round: Duration,
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

    /// This driver with a deadline of its own, [`round`](Self::round) from now: for a round
    /// that runs in the commit's detached task and must reach every branch even after the
    /// client's deadline passed (a slow prepare round, or a prepare that timed out).
    pub(crate) fn for_round(&self) -> Driver {
        let mut d = self.clone();
        d.deadline = tokio::time::Instant::now() + self.round;
        d
    }

    /// Branch `partition`'s owner by this node's map now, and the map's version.
    fn owner(&self, partition: &str) -> Result<(super::route::Owner, u64), ApiError> {
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
        Ok((owner, routes.map_version()))
    }

    /// Sends `op` to branch `partition`'s owner, or serves it here if that's this node.
    async fn send(
        &self,
        partition: &str,
        op: Op,
        writes: bool,
    ) -> Result<proto::ForwardResponse, ApiError> {
        let (owner, map_version) = self.owner(partition)?;
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

    /// Opens branch `partition` (`renew`: renews the open one). An open that meets a newer
    /// map at the owner (`WrongPartition`) takes that map and tries once more where it now
    /// puts the partition; a renew never moves, since the branch's snapshot and writes are
    /// that owner's.
    async fn begin(&self, partition: &str, renew: bool) -> Result<(), ApiError> {
        let begin = || {
            Op::BranchBegin(proto::BranchBegin {
                xid: self.xid.to_string(),
                renew,
            })
        };
        match self.send(partition, begin(), false).await {
            Err(e) if e.code == "WrongPartition" && !renew => {
                let (owner, version) = self.owner(partition)?;
                tokio::time::timeout(
                    self.left()?,
                    self.node.pull_now(&owner.node, &owner.address, version),
                )
                .await
                .map_err(|_| ApiError::deadline_exceeded())?
                .map_err(|why| {
                    ApiError::partition_unavailable(format!(
                        "node {} has a newer cluster map, which this node couldn't take: {why}",
                        owner.node
                    ))
                })?;
                self.send(partition, begin(), false).await.map(drop)
            }
            r => r.map(drop),
        }
    }

    /// Renews every open branch in `span` last renewed [`renew_after`](Self::renew_after)
    /// ago, at once (S4): a transaction working on its home, or on one branch, keeps the
    /// others. A branch that's gone (expired) fails the call with `TransactionExpired`,
    /// never reopened (audit120's M2 on #2062).
    pub(crate) async fn renew(&self, span: &mut Span) -> Result<(), ApiError> {
        let due: Vec<String> = span
            .parts
            .iter()
            .filter(|(p, t)| {
                **p != *self.home && t.renewed.is_some_and(|at| at.elapsed() >= self.renew_after)
            })
            .map(|(p, _)| p.clone())
            .collect();
        if due.is_empty() {
            return Ok(());
        }
        let calls: Vec<_> = due
            .iter()
            .map(|p| {
                let (d, p) = (self.clone(), p.clone());
                tokio::spawn(async move { d.begin(&p, true).await })
            })
            .collect();
        for (p, r) in joined(&due, calls).await {
            r?;
            if let Some(t) = span.parts.get_mut(&p) {
                t.renewed = Some(Instant::now());
            }
        }
        Ok(())
    }

    /// A transaction call (`TxnGet`, `TxnGetRow`, `TxnWrite`) on branch `partition`, with the
    /// home session's `span` (held out of its entry by the caller): opens the branch if this
    /// is its first call, then records what it did once it did it (audit120's S3).
    pub(crate) async fn call(
        &self,
        span: &mut Span,
        partition: &str,
        op: ServiceOp,
    ) -> Result<ServiceReply, ApiError> {
        let wrote = matches!(op, ServiceOp::TxnWrite(_));
        let opened = span
            .parts
            .get(partition)
            .is_some_and(|t| t.renewed.is_some());
        if !opened {
            let open = span
                .parts
                .iter()
                .filter(|(p, t)| **p != *self.home && t.renewed.is_some())
                .count();
            if open >= self.max_branches {
                return Err(ApiError::failed_precondition(format!(
                    "the transaction already touches {open} partitions besides its home, the \
                     most --txn-max-branches allows"
                )));
            }
            self.begin(partition, false).await?;
            span.parts.entry(partition.to_owned()).or_default().renewed = Some(Instant::now());
        }
        let reply = self
            .send(partition, forward::op_to_forward(op)?, wrote)
            .await?;
        #[cfg(test)]
        if wrote && seams::write_reply_lost(&self.xid) {
            return Err(ApiError::outcome_unknown(
                "the branch's answer was lost (a test's fault); the write may have been applied",
            ));
        }
        let reply = forward::reply_of(reply)?;
        let t = span.parts.entry(partition.to_owned()).or_default();
        t.read |= !wrote;
        t.wrote |= wrote;
        Ok(reply)
    }

    /// Whether `span` has room for the branches `partitions` would open
    /// (`--txn-max-branches`): checked before a call sends any row (audit120's M3b).
    pub(crate) fn room<'p>(
        &self,
        span: &Span,
        partitions: impl Iterator<Item = &'p String>,
    ) -> Result<(), ApiError> {
        let open = span
            .parts
            .iter()
            .filter(|(p, t)| **p != *self.home && t.renewed.is_some())
            .count();
        let new = partitions
            .filter(|p| span.parts.get(*p).is_none_or(|t| t.renewed.is_none()))
            .count();
        if open + new > self.max_branches {
            return Err(ApiError::failed_precondition(format!(
                "the transaction would touch {} partitions besides its home, more than \
                 --txn-max-branches allows",
                open + new
            )));
        }
        Ok(())
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
        let calls: Vec<_> = parts
            .iter()
            .map(|p| {
                let (d, p) = (self.clone(), p.clone());
                tokio::spawn(async move {
                    let op = Op::BranchPrepare(proto::BranchPrepare {
                        xid: d.xid.to_string(),
                        read_only,
                    });
                    d.send(&p, op, !read_only).await
                })
            })
            .collect();
        let mut done = Vec::new();
        let mut refused = None;
        for (partition, r) in joined(parts, calls).await {
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
    /// answers. A part is `applied` only if its branch answered with its commit: one that
    /// didn't answer in time stays in doubt for its resolver, and one that found nothing in
    /// doubt (its partition's history changed) is no applied share (audit120's S2).
    pub(crate) async fn decide(&self, parts: &[String], commit: bool) -> Vec<proto::TxnPart> {
        let calls: Vec<_> = parts
            .iter()
            .map(|p| {
                let (d, p) = (self.clone(), p.clone());
                tokio::spawn(async move {
                    let op = Op::BranchDecide(proto::BranchDecide {
                        xid: d.xid.to_string(),
                        commit,
                        hold: 0,
                        term: 0,
                    });
                    d.send(&p, op, true).await
                })
            })
            .collect();
        joined(parts, calls)
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
                    applied: seqno > 0,
                }
            })
            .collect()
    }

    /// Releases the read-only holds in `held`, at once: whether every one lasted.
    pub(crate) async fn release(&self, held: &[Prepared]) -> bool {
        let names: Vec<String> = held.iter().map(|h| h.partition.clone()).collect();
        let calls: Vec<_> = held
            .iter()
            .map(|h| {
                let (d, h) = (self.clone(), h.clone());
                tokio::spawn(async move {
                    let op = Op::BranchDecide(proto::BranchDecide {
                        xid: d.xid.to_string(),
                        commit: true,
                        hold: h.hold,
                        term: h.term,
                    });
                    d.send(&h.partition, op, false).await.map(drop)
                })
            })
            .collect();
        joined(&names, calls)
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

/// Each spawned branch call's answer, by its partition (`parts`, in order): a task that
/// panicked is that branch's failure, never a lost answer.
async fn joined<T>(
    parts: &[String],
    calls: Vec<tokio::task::JoinHandle<Result<T, ApiError>>>,
) -> Vec<(String, Result<T, ApiError>)> {
    let mut out = Vec::with_capacity(calls.len());
    for (p, c) in parts.iter().zip(calls) {
        let r = c.await.unwrap_or_else(|e| {
            Err(ApiError::internal(format!(
                "partition {p:?}'s branch call failed: {e}"
            )))
        });
        out.push((p.clone(), r));
    }
    out
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
/// service here. The home's session is taken first, for every call: it checks the caller,
/// renews the session and its branches, and holds the transaction's [`Span`] for the call
/// (audit120's M1 on #2062). A row of the home is then the home's, and any other partition's
/// row goes to that partition's branch through `driver`.
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
        let mut others: std::collections::BTreeMap<String, Vec<proto::RowWrites>> =
            std::collections::BTreeMap::new();
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
    if let ServiceOp::Begin(r) = op {
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
        return Ok(ServiceReply::Begin(
            home.begin_spanning(r, caller, epoch).await?,
        ));
    }
    let budget = home.admit(&op.admit(), caller, prepaid).await?;
    // The home's session first, whatever the call (M1, M3): a repeated `Commit` finds its
    // tombstone here, before any row goes anywhere.
    let mut g = match home.take_txn(&driver.xid, caller)? {
        Taken::Open(g) => g,
        Taken::Done(outcome) => {
            return match op {
                ServiceOp::Commit(_) => outcome.map(ServiceReply::Commit),
                _ => Err(ApiError::transaction_finished(&outcome)),
            };
        }
    };
    let buffered = g.wrote();
    let Some(span) = g.span() else {
        return Err(ApiError::internal("a spanning id on a plain session"));
    };
    if !matches!(op, ServiceOp::Abort(_)) {
        driver.renew(span).await?;
    }
    match op {
        ServiceOp::TxnGet(ref r) if partition_of(&r.table, &r.row)? != *driver.home => {
            let p = partition_of(&r.table, &r.row)?;
            let reply = driver.call(span, &p, op).await;
            drop((g, budget));
            reply
        }
        ServiceOp::TxnGetRow(ref r) if partition_of(&r.table, &r.row)? != *driver.home => {
            let p = partition_of(&r.table, &r.row)?;
            let reply = driver.call(span, &p, op).await;
            drop((g, budget));
            reply
        }
        ServiceOp::TxnGet(_) | ServiceOp::TxnGetRow(_) => {
            span.parts.entry(driver.home.to_string()).or_default().read = true;
            // The home's own read takes the session itself.
            drop(g);
            home.call(op, caller, Some(budget)).await
        }
        ServiceOp::TxnWrite(r) => {
            // All or nothing, as on one node (audit120's M3b): everything that can refuse
            // is checked before the first row goes to a branch, and a failure after that
            // ends the transaction, so a retry can't apply the sent rows twice.
            let (mine, others) = split(r.rows)?;
            let rows = home.txn_rows(&mine).await?;
            if !mine.is_empty() {
                home.writable()?;
                // D331 (#2155): the home's rows, as a plain `TxnWrite`'s are checked.
                home.home_ranges(&mine, None)?;
            }
            driver.room(span, others.keys())?;
            for (p, rows) in others {
                let w = ServiceOp::TxnWrite(proto::TxnWriteRequest {
                    txn_id: r.txn_id.clone(),
                    rows,
                });
                if let Err(e) = driver.call(span, &p, w).await {
                    drop(budget);
                    return Err(Service::end_spanning(g, e, &driver));
                }
            }
            if !rows.is_empty() {
                span.parts.entry(driver.home.to_string()).or_default().wrote = true;
                Service::buffer_rows(&mut g, &rows);
            }
            drop((g, budget));
            Ok(ServiceReply::Empty(proto::Empty {}))
        }
        ServiceOp::Commit(r) => {
            // As `TxnWrite`: what the commit can refuse before any row goes out (M3b).
            let (mine, others) = split(r.rows)?;
            let writes = buffered
                || !mine.is_empty()
                || !others.is_empty()
                || span.parts.values().any(|t| t.wrote);
            if writes && !caller.scope.allows(super::super::auth::Scope::Write) {
                return Err(ApiError::permission_denied("commit writes"));
            }
            drop(home.txn_rows(&mine).await?);
            // D331 (#2155): the home's rows, before any row goes to a branch. The commit
            // checks them again, with M3, under its gate hold.
            home.home_ranges(&mine, None)?;
            if writes {
                drop(home.decisions_table().await?);
            }
            driver.room(span, others.keys())?;
            let forwarded = !others.is_empty();
            for (p, rows) in others {
                let w = ServiceOp::TxnWrite(proto::TxnWriteRequest {
                    txn_id: r.txn_id.clone(),
                    rows,
                });
                if let Err(e) = driver.call(span, &p, w).await {
                    drop(budget);
                    return Err(Service::end_spanning(g, e, &driver));
                }
            }
            if !mine.is_empty() {
                span.parts.entry(driver.home.to_string()).or_default().wrote = true;
            }
            let r = proto::TxnCommitRequest {
                txn_id: r.txn_id,
                rows: mine,
            };
            Ok(ServiceReply::Commit(
                home.commit_spanning(g, r, caller, budget, driver, forwarded)
                    .await?,
            ))
        }
        ServiceOp::Abort(_) => {
            let mut branches = span.branches(&driver.home);
            branches.extend(span.idle(&driver.home));
            drop(budget);
            driver.for_round().abort(&branches).await;
            g.abort();
            Ok(ServiceReply::Empty(proto::Empty {}))
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

    static LOST_WRITE_REPLIES: Mutex<Vec<String>> = Mutex::new(Vec::new());

    /// A fault (#2161): the answer to the next write `xid`'s home sends a branch is lost
    /// after the branch took it, as a connection cut between the two would lose it.
    pub(crate) fn lose_write_reply(xid: &str) {
        LOST_WRITE_REPLIES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(xid.to_owned());
    }

    /// Whether `xid`'s write reply is to be lost ([`lose_write_reply`]), once.
    pub(crate) fn write_reply_lost(xid: &str) -> bool {
        let mut lost = LOST_WRITE_REPLIES
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(i) = lost.iter().position(|x| x == xid) else {
            return false;
        };
        lost.swap_remove(i);
        true
    }

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

/// The most acks a node keeps owing to transactions' homes (#2078): about a hundred bytes
/// each.
const MAX_ACKS: usize = 4096;

/// How many homes a resolver pass asks at once (#2078): a dead home costs its task one
/// request timeout, and the others go on beside it.
const RESOLVE_HOMES: usize = 8;

/// One ask a resolver pass makes of a home (#2078): what a share in doubt here should be,
/// or the ack of a commit a branch here applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Ask {
    Share { partition: String, xid: String },
    Ack { partition: String, xid: String },
}

/// Whether `e` says the home gave no answer (its node unreachable or too slow, or no owner
/// for it in the map), as against its own answer about that one transaction (audit120's
/// Q1 on #2078): only the first skips the home's other asks this pass.
pub(crate) fn unanswered(e: &ApiError) -> bool {
    matches!(
        e.code.as_str(),
        "PartitionUnavailable" | "DeadlineExceeded" | "Unavailable" | "WrongPartition"
    )
}

/// Whether an ack sent with `answer` is still owed (audit120's Q2 on #2078): only while the
/// home gave no answer ([`unanswered`], or not asked this pass) or is still committing. A
/// COMMITTED answer took the ack; an aborted or unknown record, or the home's own error
/// about it, never will.
pub(crate) fn keep_ack(answer: &Option<Result<proto::TxnResolution, ApiError>>) -> bool {
    match answer {
        None => true,
        Some(Ok(r)) => r.state == proto::TxnOutcomeState::InProgress as i32,
        Some(Err(e)) => unanswered(e),
    }
}

/// A resolver pass's asks, one home's in turn and up to `limit` homes at once (#2078), each
/// through `ask`. Once a home gives no answer ([`unanswered`]), its remaining asks this
/// pass aren't made (`None`), so a dead home costs one request timeout, not one per share,
/// and every other home's asks go on beside it. All are joined before it returns.
pub(crate) async fn ask_homes<F, Fut>(
    homes: Vec<Vec<Ask>>,
    limit: usize,
    ask: F,
) -> Vec<(Ask, Option<Result<proto::TxnResolution, ApiError>>)>
where
    F: Fn(Ask) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<proto::TxnResolution, ApiError>> + Send + 'static,
{
    let mut homes = homes.into_iter();
    let mut set = tokio::task::JoinSet::new();
    let mut out = Vec::new();
    loop {
        while set.len() < limit.max(1)
            && let Some(asks) = homes.next()
        {
            let ask = ask.clone();
            set.spawn(async move {
                let mut done = Vec::with_capacity(asks.len());
                let mut silent = false;
                for a in asks {
                    if silent {
                        done.push((a, None));
                        continue;
                    }
                    let r = ask(a.clone()).await;
                    silent = r.as_ref().is_err_and(unanswered);
                    done.push((a, Some(r)));
                }
                done
            });
        }
        match set.join_next().await {
            Some(Ok(done)) => out.extend(done),
            // A home's task that panicked: its asks are made again next pass.
            Some(Err(e)) => eprintln!("phdb serve: a resolver ask failed: {e}"),
            None => return out,
        }
    }
}

/// D329's resolver, started once the node serves: each partition's shares held in doubt
/// longer than `delay` (default: the request timeout, so it never fires on a healthy
/// commit) ask their home what was decided (`ResolveTxn`), and are decided by the answer.
/// A home that's unreachable, still committing, or whose history changed leaves the share
/// in doubt, with the reason kept for `Status`, and it's asked again after a backoff that
/// doubles to a minute. The loop holds the server only while it runs a pass.
pub(crate) fn start_resolver(
    shared: std::sync::Weak<super::super::Shared>,
    delay: Duration,
) -> Box<dyn FnOnce() + Send> {
    Box::new(move || {
        tokio::spawn(async move {
            let period = (delay / 4).clamp(Duration::from_millis(100), Duration::from_secs(5));
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // (partition, xid) → (when to ask again, the backoff's step).
            let mut next: HashMap<(String, Vec<u8>), (Instant, Duration)> = HashMap::new();
            let sweep_every = shared
                .upgrade()
                .map_or(Duration::from_secs(600), |s| {
                    s.limits.txn_outcome_retention / 24
                })
                .clamp(Duration::from_secs(60), Duration::from_secs(600));
            let mut last_sweep = Instant::now();
            loop {
                tick.tick().await;
                let Some(shared) = shared.upgrade() else {
                    return;
                };
                let Some(cluster) = shared.router.cluster.as_ref() else {
                    return;
                };
                let timeout = shared.limits.request_timeout;
                let parts = cluster.parts.load_full();
                // The decision records past retention on the partitions this node owns (GC),
                // every `retention / 24`, between one and ten minutes: first, so it never
                // waits on a pass's asks (#2078).
                if last_sweep.elapsed() >= sweep_every {
                    last_sweep = Instant::now();
                    for service in parts.values() {
                        if let Err(e) = service.sweep_decisions(now_ms()).await
                            && e.code != "Fenced"
                        {
                            eprintln!("phdb serve: sweeping decision records: {}", e.message);
                        }
                    }
                }
                // This pass's asks, by home: each due share in doubt, and each ack owed.
                let mut seen = std::collections::HashSet::new();
                let mut by_home: HashMap<String, Vec<Ask>> = HashMap::new();
                let home_of = |xid: &str| {
                    super::super::sessions::spanning_parts(xid).map(|(h, _, _)| h.to_owned())
                };
                for (partition, service) in parts.iter() {
                    let Ok(part) = service.part() else {
                        continue;
                    };
                    for (xid, age, _) in part.db.in_doubt_shares() {
                        let key = (partition.clone(), xid.clone());
                        seen.insert(key.clone());
                        if age < delay || next.get(&key).is_some_and(|(at, _)| Instant::now() < *at)
                        {
                            continue;
                        }
                        let Ok(text) = String::from_utf8(xid) else {
                            continue;
                        };
                        let Some(home) = home_of(&text) else {
                            cluster.spanning.note(
                                partition,
                                text.as_bytes(),
                                Some("not a spanning transaction's id".into()),
                            );
                            continue;
                        };
                        by_home.entry(home).or_default().push(Ask::Share {
                            partition: partition.clone(),
                            xid: text,
                        });
                    }
                }
                for (xid, partition) in cluster.spanning.due_acks(Instant::now()) {
                    match home_of(&xid) {
                        Some(home) => by_home
                            .entry(home)
                            .or_default()
                            .push(Ask::Ack { partition, xid }),
                        None => cluster.spanning.ack_sent((xid, partition), false),
                    }
                }
                let asked = {
                    let shared = Arc::clone(&shared);
                    ask_homes(by_home.into_values().collect(), RESOLVE_HOMES, move |a| {
                        let shared = Arc::clone(&shared);
                        async move {
                            let Some(cluster) = shared.router.cluster.as_ref() else {
                                return Err(ApiError::unavailable("this node isn't serving"));
                            };
                            let (xid, acked) = match &a {
                                Ask::Share { xid, .. } => (xid, None),
                                Ask::Ack { xid, partition } => (xid, Some(partition.as_str())),
                            };
                            ask_home(
                                &cluster.node,
                                &cluster.parts,
                                &cluster.forwarder,
                                &cluster.spanning,
                                xid,
                                acked,
                                timeout,
                            )
                            .await
                        }
                    })
                    .await
                };
                use proto::TxnOutcomeState as St;
                for (a, answer) in asked {
                    match a {
                        Ask::Ack { partition, xid } => {
                            let keep = keep_ack(&answer);
                            cluster.spanning.ack_sent((xid, partition), keep);
                        }
                        Ask::Share { partition, xid } => {
                            let key = (partition.clone(), xid.clone().into_bytes());
                            let why = match answer {
                                None => Some(
                                    "its home didn't answer (asked about another of its \
                                     transactions this pass)"
                                        .to_owned(),
                                ),
                                Some(Err(e)) if unanswered(&e) => {
                                    Some(format!("its home didn't answer: {}", e.message))
                                }
                                Some(Err(e)) => {
                                    Some(format!("its home answered {}: {}", e.code, e.message))
                                }
                                Some(Ok(r)) => {
                                    match St::try_from(r.state).unwrap_or(St::Unspecified) {
                                        St::Committed | St::Aborted => {
                                            let commit = r.state == St::Committed as i32;
                                            match parts.get(&partition) {
                                                None => Some("its partition closed here".into()),
                                                Some(service) => match service
                                                    .decide_branch(
                                                        &xid,
                                                        commit,
                                                        0,
                                                        0,
                                                        &cluster.spanning,
                                                        &partition,
                                                    )
                                                    .await
                                                {
                                                    Ok(_) => {
                                                        // A commit applied here: its home's
                                                        // record may go once every branch has
                                                        // said so (GC), told next pass.
                                                        if commit {
                                                            cluster
                                                                .spanning
                                                                .queue_ack(&xid, &partition);
                                                        }
                                                        None
                                                    }
                                                    Err(e) => Some(format!(
                                                        "deciding it failed: {}",
                                                        e.message
                                                    )),
                                                },
                                            }
                                        }
                                        St::InProgress => {
                                            Some("its home is still committing it".into())
                                        }
                                        St::Unknown | St::Unspecified => Some(r.reason),
                                    }
                                }
                            };
                            match &why {
                                None => {
                                    next.remove(&key);
                                }
                                Some(_) => {
                                    let step = next
                                        .get(&key)
                                        .map_or(Duration::from_secs(1), |(_, s)| *s * 2)
                                        .min(Duration::from_secs(60));
                                    next.insert(key.clone(), (Instant::now() + step, step));
                                }
                            }
                            cluster.spanning.note(&partition, &key.1, why);
                        }
                    }
                }
                // Shares no longer in doubt (decided, or their partition closed) are forgotten.
                next.retain(|k, _| seen.contains(k));
                cluster
                    .spanning
                    .reasons
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .retain(|k, _| seen.contains(k));
            }
        });
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ApiError, Ask, MAX_ACKS, Spanning, ask_homes, collectable, keep_ack, proto, with_acks,
    };
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[test]
    fn an_ack_is_kept_only_while_its_home_gives_no_answer_or_is_still_committing() {
        // audit120's Q2 and S1 on #2078.
        use proto::TxnOutcomeState as St;
        let said = |s: St| {
            Some(Ok(proto::TxnResolution {
                state: s as i32,
                reason: String::new(),
            }))
        };
        assert!(keep_ack(&None), "not asked this pass");
        assert!(keep_ack(&said(St::InProgress)));
        assert!(keep_ack(&Some(Err(ApiError::deadline_exceeded()))));
        assert!(keep_ack(&Some(Err(ApiError::partition_unavailable(
            "cut off"
        )))));
        assert!(!keep_ack(&said(St::Committed)), "taken");
        assert!(!keep_ack(&said(St::Aborted)), "a marker never takes it");
        assert!(
            !keep_ack(&said(St::Unknown)),
            "a lost record never takes it"
        );
        assert!(
            !keep_ack(&Some(Err(ApiError::not_found("no record")))),
            "the home's own word"
        );
    }

    #[test]
    fn an_owed_ack_is_due_now_kept_after_a_backoff_dropped_when_done_and_capped() {
        // audit120's S1 on #2078: queued due now; a kept one isn't due again until its
        // backoff (doubling from a second); a done one goes; past the cap a new one drops.
        let s = Spanning::new();
        let key = ("p2:h1-00".to_owned(), "p1".to_owned());
        s.queue_ack(&key.0, &key.1);
        assert_eq!(s.due_acks(Instant::now()), vec![key.clone()]);
        s.ack_sent(key.clone(), true);
        assert!(
            s.due_acks(Instant::now()).is_empty(),
            "kept, not due before its backoff"
        );
        let later = |ms| Instant::now() + Duration::from_millis(ms);
        assert_eq!(s.due_acks(later(1100)), vec![key.clone()]);
        s.ack_sent(key.clone(), true);
        assert!(s.due_acks(later(1100)).is_empty(), "the backoff doubled");
        assert_eq!(s.due_acks(later(2100)), vec![key.clone()]);
        s.ack_sent(key.clone(), false);
        assert!(s.due_acks(later(120_000)).is_empty(), "done");
        for i in 0..MAX_ACKS {
            s.queue_ack(&format!("p2:h1-{i:032x}"), "p1");
        }
        s.queue_ack("p2:h1-over", "p1");
        let due = s.due_acks(Instant::now());
        assert_eq!(due.len(), MAX_ACKS, "capped");
        assert!(
            !due.iter().any(|(x, _)| x == "p2:h1-over"),
            "the one past the cap dropped"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_dead_home_costs_one_ask_a_pass_and_the_other_homes_go_on_beside_it() {
        // #2078 (audit120's S3 on #2076, Q1 on the plan): three shares of a home that never
        // answers cost one ask and one wait, not three; a live home's share is answered
        // beside it; and a home that answers an error about one transaction (its record
        // gone) is still asked about its next. Two homes at a time, three homes.
        let share = |home: &str, n: u32| Ask::Share {
            partition: "p1".into(),
            xid: format!("{home}:h1-{n:032x}"),
        };
        let homes = vec![
            vec![share("dead", 1), share("dead", 2), share("dead", 3)],
            vec![share("live", 1)],
            vec![share("odd", 1), share("odd", 2)],
        ];
        let asked: Arc<Mutex<Vec<String>>> = Arc::default();
        let log = Arc::clone(&asked);
        let wait = Duration::from_millis(300);
        // Paused time (audit120's N1): the sleeps advance it exactly, whatever the runner.
        let started = tokio::time::Instant::now();
        let out = ask_homes(homes, 2, move |a| {
            let log = Arc::clone(&log);
            async move {
                let Ask::Share { xid, .. } = &a else {
                    unreachable!("only shares here")
                };
                log.lock().unwrap().push(xid.clone());
                if xid.starts_with("dead") {
                    tokio::time::sleep(wait).await;
                    return Err(ApiError::deadline_exceeded());
                }
                if xid.starts_with("odd") && xid.ends_with('1') {
                    return Err(ApiError::not_found("no record of the transaction"));
                }
                Ok(proto::TxnResolution {
                    state: proto::TxnOutcomeState::Committed as i32,
                    reason: String::new(),
                })
            }
        })
        .await;
        let took = started.elapsed();
        assert!(
            took < wait * 2,
            "a pass took {took:?}, more than one dead home's wait"
        );
        let asked = asked.lock().unwrap().clone();
        let count = |home: &str| asked.iter().filter(|x| x.starts_with(home)).count();
        assert_eq!(
            (count("dead"), count("live"), count("odd")),
            (1, 1, 2),
            "{asked:?}"
        );
        let answer = |home: &str, n: u32| {
            out.iter()
                .find(|(a, _)| *a == share(home, n))
                .map(|(_, r)| r.as_ref().map(|r| r.as_ref().map_err(|e| e.code.clone())))
                .unwrap_or_else(|| panic!("{home} {n} missing"))
        };
        assert_eq!(
            answer("dead", 1).map(|r| r.err()),
            Some(Some("DeadlineExceeded".into()))
        );
        assert_eq!(
            answer("dead", 2),
            None,
            "not asked: its home didn't answer this pass"
        );
        assert_eq!(answer("dead", 3), None);
        assert!(matches!(answer("live", 1), Some(Ok(_))));
        assert_eq!(
            answer("odd", 1).map(|r| r.err()),
            Some(Some("NotFound".into()))
        );
        assert!(
            matches!(answer("odd", 2), Some(Ok(_))),
            "a home that answered is asked again"
        );
        assert_eq!(out.len(), 6);
    }

    #[test]
    fn a_decision_row_is_collected_only_past_retention_and_once_every_branch_acked() {
        // D329 2b's GC: an abort marker goes once its time is `retention` past; a commit, only
        // once every branch it names is `+`-acked as well. Anything unreadable stays.
        let retention = 1_000;
        let aborted = b"aborted h3 at5000".to_vec();
        assert!(!collectable(&aborted, 5_999, retention));
        assert!(collectable(&aborted, 6_000, retention));
        let committed = b"committed h3 at5000\np1\np4".to_vec();
        assert!(!collectable(&committed, 99_999, retention), "nothing acked");
        let one = with_acks(&committed, &["p4".to_owned()]).unwrap();
        assert_eq!(one, b"committed h3 at5000\np1\n+p4");
        assert!(!collectable(&one, 99_999, retention), "p1 not acked");
        let both = with_acks(&one, &["p1".to_owned(), "p4".to_owned()]).unwrap();
        assert_eq!(both, b"committed h3 at5000\n+p1\n+p4");
        assert!(!collectable(&both, 5_999, retention), "within retention");
        assert!(collectable(&both, 6_000, retention));
        // Acking again, or an unknown branch, changes nothing; an abort takes no acks.
        assert_eq!(with_acks(&both, &["p1".to_owned()]), None);
        assert_eq!(with_acks(&one, &["p9".to_owned()]), None);
        assert_eq!(with_acks(&aborted, &["p1".to_owned()]), None);
        // A commit naming no branch (a home-only one) goes on its time alone.
        assert!(collectable(b"committed h3 at5000", 6_000, retention));
        for bad in [&b""[..], b"committed h3", b"atx", b"\xff", b"maybe h1 at1"] {
            assert!(!collectable(bad, u64::MAX, 0), "{bad:?}");
        }
    }
}
