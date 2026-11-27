//! Fencing a cluster node's partitions (D317 ticket 6, rule 3) and their leases (D326): each
//! partition served here has a gate ([`WriteGate`]). `phdb cluster fence` and `unfence` set
//! and lift the operator's fence on it; the node's lease rounds ([`Node::round`]) grant it
//! its lease, without which it serves nothing, and renew it every third of a lease for as
//! long as a majority of the map's nodes grant it.
//!
//! **`DIR/fenced.json`:** the partitions an operator fenced, `{"fenced": ["p1", ...]}`,
//! written (temporary file, sync, rename, directory sync) before a fence or unfence
//! answers. An operator's fence survives a restart; a start fence is never persisted.
//!
//! **Order (cli692's S4):** a fence sets the in-memory flag first, then persists; an unfence
//! persists first, then clears the flag. Whatever fails or crashes in between leaves the
//! partition fenced, never open with the file saying fenced or the reverse.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use super::Node;
use super::proto::{ClusterMap, FenceReason, LeasePart, LeaseRequest, LocalPartitionState};
use crate::serve::errors::ApiError;
use crate::serve::gate::{Fence, WriteGate};

/// The operator's fences, in a node's directory.
pub(crate) const FENCED_FILE: &str = "fenced.json";

/// How long `Fence` waits for the writes admitted before it to finish. Past it, the
/// partition stays fenced and the answer says how many still run: `fence` again waits again.
const DRAIN_WAIT: Duration = Duration::from_secs(30);

/// How often a node whose partition has no lease asks its peers again (cli692's S3): the
/// first wait, doubled up to the last, and never more than a third of a lease.
pub(super) const RETRY_FIRST: Duration = Duration::from_millis(250);
pub(super) const RETRY_MAX: Duration = Duration::from_secs(5);

/// Reads `dir`'s fenced partitions; none when there's no file.
pub(crate) fn read_fenced(dir: &Path) -> Result<BTreeSet<String>, String> {
    let path = dir.join(FENCED_FILE);
    match std::fs::read_to_string(&path) {
        Ok(json) => parse_fenced(&json).map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
        Err(e) => Err(format!("read {}: {e}", path.display())),
    }
}

/// `{"fenced": ["p1", ...]}`'s ids.
fn parse_fenced(json: &str) -> Result<BTreeSet<String>, String> {
    let v: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let list = v
        .get("fenced")
        .and_then(serde_json::Value::as_array)
        .ok_or("no \"fenced\" list")?;
    list.iter()
        .map(|p| {
            p.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{p} isn't a partition id"))
        })
        .collect()
}

/// Writes `fenced` as `dir`'s fenced partitions, atomically, as the map file is written.
fn write_fenced(dir: &Path, fenced: &BTreeSet<String>) -> std::io::Result<()> {
    let json = serde_json::json!({ "fenced": fenced }).to_string();
    super::write_synced(dir, FENCED_FILE, json.as_bytes())
}

/// A partition served here: its gate, and its file's epoch (what a map must name this node
/// at for the partition to unfence).
#[derive(Debug)]
struct Served {
    gate: Arc<WriteGate>,
    epoch: u64,
}

/// What a partition is on this node at one moment, read under one lock ([`Fences::now`]).
/// Served and closed win over an open that is running beside them.
pub(super) enum Now {
    /// Served here, behind this gate.
    Served(Arc<WriteGate>),
    Closed,
    /// Being opened, and neither served nor closed yet.
    Opening,
    Nothing,
}

/// What a partition is on this node: served or closed, and whether an open of it is running.
/// An entry that is none of them is removed.
#[derive(Debug, Default)]
struct Local {
    /// Being opened while the node serves ([`Node::open_part`]): `Status` shows it opening
    /// and doesn't read its files (#1770). Set from the open's start to its end, so beside
    /// a closed partition's entry while it reopens, and a served one's while it reconciles.
    opening: bool,
    is: Is,
}

#[derive(Debug, Default)]
enum Is {
    #[default]
    Neither,
    Served(Served),
    /// Closed here ([`Node::close`]), with its files once hashed.
    Closed(super::close::Closed),
}

/// A node's fences.
#[derive(Debug, Default)]
pub(crate) struct Fences {
    /// Each partition served, closed or being opened here, under one lock, so a partition
    /// that changes from one to another is never seen as neither (#1830).
    local: Mutex<BTreeMap<String, Local>>,
    /// The operator's fences as `fenced.json` has them, partitions not open here included.
    operator: Mutex<BTreeSet<String>>,
    /// One fence, unfence or close at a time, so the file follows the flags in order.
    pub(super) changing: tokio::sync::Mutex<()>,
    /// [`Node::confirm_soon`]'s rounds: one running, and whether another is owed.
    rounds: Mutex<Rounds>,
    /// [`Node::renew_leases`]'s loop is running.
    retrying: std::sync::atomic::AtomicBool,
    /// The node's lease rounds, numbered: a grantor ignores an older round's request that
    /// arrives after a newer one (`LeaseRequest.round`).
    round: std::sync::atomic::AtomicU64,
    /// The node's last lease round asked for something: once it has nothing to ask (its
    /// last partition moved away or closed), one more round asks for nothing, which ends
    /// its grants at once rather than a lease later, so a planned move's new owner needn't
    /// wait them out.
    asked: std::sync::atomic::AtomicBool,
    /// Peers whose build answers `Cluster.Lease` with `Unimplemented` (1.2, #2047), counted
    /// as granting: kept so the log says so once per peer, and again if it comes back.
    pre_lease: Mutex<BTreeSet<String>>,
    /// Peers that report `leases` but whose `Cluster.Lease` answers `Unimplemented`
    /// (audit120's S1 on #2048): counted as nothing, and logged once per peer.
    unrouted: Mutex<BTreeSet<String>>,
    /// D335 PR 2b: a partition entering its fallback asks for a lease round now
    /// ([`crate::serve::acks::ReplicaAcks::set_kick`]); the renewal loop wakes on it.
    pub(crate) kick: Arc<tokio::sync::Notify>,
}

/// [`Node::confirm_soon`]'s state.
#[derive(Debug, Default)]
struct Rounds {
    running: bool,
    again: bool,
}

/// What one confirmation round found ([`Node::round`]).
#[derive(Debug)]
pub(crate) struct Round {
    /// The node's map after the round: the newest that answered, installed if newer.
    pub(crate) map: ClusterMap,
    /// The nodes that hold `map` now, this one included: answered with it, or took its push.
    pub(crate) holders: usize,
    /// The peers that answered.
    pub(crate) answered: usize,
    /// Peers holding a different map at the same version (two forced maps), or a newer one
    /// this node refused: nothing is confirmed while there's one.
    pub(crate) divergent: Vec<String>,
    /// Peers that answered with an older map and didn't take this node's: nothing is
    /// confirmed while there's one (audit120's F2 on #1899).
    pub(crate) behind: Vec<String>,
    /// The node's clock before it sent the round's first request: a lease it grants runs
    /// from here (D326).
    pub(crate) t0: u64,
    /// The map's nodes that granted each partition asked, this node included.
    pub(crate) granted: BTreeMap<String, usize>,
    /// Partitions a node withheld, the node holding them there, and the ms left.
    pub(crate) withheld: Vec<(String, String, u64)>,
    /// The longest start grace a node answered with, in ms (0: none).
    pub(crate) grace_ms: u64,
}

impl Round {
    /// Whether a majority of `map`'s nodes hold it, with no divergent map among them.
    pub(crate) fn majority(&self) -> bool {
        self.divergent.is_empty()
            && self.behind.is_empty()
            && self.holders * 2 > self.map.nodes.len()
    }

    /// Whether it confirms partition `id` at `epoch` on node `node`: a majority hold the
    /// map, and the map names `node` for it there (D317 rule 3, as amended for #1760). An
    /// unfence asks this; serving takes a lease as well ([`leased`](Self::leased)).
    pub(crate) fn confirms(&self, node: &str, id: &str, epoch: u64) -> bool {
        self.majority() && names(&self.map, node, id, epoch)
    }

    /// Whether it gives this node `id`'s lease at `epoch` (D326): it confirms it, and a
    /// majority of the map's nodes granted it.
    pub(crate) fn leased(&self, node: &str, id: &str, epoch: u64) -> bool {
        self.confirms(node, id, epoch)
            && self.granted.get(id).copied().unwrap_or(0) * 2 > self.map.nodes.len()
    }

    /// Why it gave this node no lease on `id`, for a log or a refusal: who withheld it, the
    /// start grace, or how few granted.
    pub(crate) fn why_not_leased(&self, id: &str) -> String {
        let withheld: Vec<String> = self
            .withheld
            .iter()
            .filter(|(p, _, _)| p == id)
            .map(|(_, holder, ms)| format!("node {holder}'s lease on it has {ms} ms left"))
            .collect();
        if !withheld.is_empty() {
            return withheld.join("; ");
        }
        if self.grace_ms > 0 {
            return format!(
                "a node restarted and grants no lease for {} ms more (its start grace)",
                self.grace_ms
            );
        }
        format!(
            "{} of {} nodes granted it",
            self.granted.get(id).copied().unwrap_or(0),
            self.map.nodes.len()
        )
    }
}

/// A served partition's state behind `gate`, and why it's fenced when it is.
pub(super) fn gate_state(gate: &WriteGate) -> (LocalPartitionState, FenceReason) {
    match gate.fenced() {
        Some(Fence::Operator) => (LocalPartitionState::Fenced, FenceReason::Operator),
        Some(Fence::Unconfirmed) => (LocalPartitionState::Fenced, FenceReason::AwaitingPeerMap),
        Some(Fence::Lapsed) => (LocalPartitionState::Fenced, FenceReason::LeaseLapsed),
        None => (LocalPartitionState::Serving, FenceReason::Unspecified),
    }
}

/// Adds `peer` to `set` (`true` when it wasn't there) or removes it (`false`).
fn mark(set: &Mutex<BTreeSet<String>>, peer: &str, is: bool) -> bool {
    let mut set = set.lock().unwrap_or_else(PoisonError::into_inner);
    if is {
        set.insert(peer.to_owned())
    } else {
        set.remove(peer);
        false
    }
}

impl Fences {
    fn local(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Local>> {
        self.local.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Notes whether `peer` answered a lease request as a build before D326 (#2047); `true`
    /// when that's news, so the log says it once.
    fn pre_lease(&self, peer: &str, is: bool) -> bool {
        mark(&self.pre_lease, peer, is)
    }

    /// The same for a peer that reports `leases` but whose `Cluster.Lease` answered
    /// `Unimplemented` (audit120's S1 on #2048).
    fn unrouted(&self, peer: &str, is: bool) -> bool {
        mark(&self.unrouted, peer, is)
    }

    /// How many partitions have an entry (tests): one that is nothing here has none.
    #[cfg(test)]
    pub(super) fn entries(&self) -> usize {
        self.local().len()
    }

    /// Runs `f` on each partition served here, in id order, under the lock.
    fn for_served(&self, mut f: impl FnMut(&str, &Served)) {
        for (id, l) in self.local().iter() {
            if let Is::Served(s) = &l.is {
                f(id, s);
            }
        }
    }

    fn rounds(&self) -> std::sync::MutexGuard<'_, Rounds> {
        self.rounds.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn operator(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        self.operator.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether partition `id` is being opened here.
    pub(super) fn is_opening(&self, id: &str) -> bool {
        self.local().get(id).is_some_and(|l| l.opening)
    }

    /// Marks partition `id` as being opened here, or no longer.
    pub(super) fn set_opening(&self, id: &str, opening: bool) {
        let mut local = self.local();
        let l = local.entry(id.to_owned()).or_default();
        l.opening = opening;
        if !l.opening && matches!(l.is, Is::Neither) {
            local.remove(id);
        }
    }

    /// Partition `id` is served here from now, behind `gate`, at its file's `epoch`. A
    /// closed partition reopened stops being closed in the same step: its files change.
    fn serve(&self, id: &str, gate: Arc<WriteGate>, epoch: u64) {
        self.local().entry(id.to_owned()).or_default().is = Is::Served(Served { gate, epoch });
    }

    /// Partition `id`, served here, is at `epoch` now (D331's hand-over raised it on its
    /// own node): its lease rounds ask at it.
    pub(super) fn raise_epoch(&self, id: &str, epoch: u64) {
        if let Some(Local {
            is: Is::Served(s), ..
        }) = self.local().get_mut(id)
        {
            s.epoch = s.epoch.max(epoch);
        }
    }

    pub(super) fn gate(&self, id: &str) -> Option<(Arc<WriteGate>, u64)> {
        match &self.local().get(id)?.is {
            Is::Served(s) => Some((Arc::clone(&s.gate), s.epoch)),
            _ => None,
        }
    }

    /// What partition `id` is here, in one read, so a close or a reopen is seen before or
    /// after and never between. `closed` runs on a closed partition's entry, under the lock,
    /// so it can't take it again.
    pub(super) fn now(&self, id: &str, closed: impl FnOnce(&super::close::Closed)) -> Now {
        let local = self.local();
        match local.get(id) {
            Some(Local {
                is: Is::Served(s), ..
            }) => Now::Served(Arc::clone(&s.gate)),
            Some(Local {
                is: Is::Closed(c), ..
            }) => {
                closed(c);
                Now::Closed
            }
            Some(Local { opening: true, .. }) => Now::Opening,
            _ => Now::Nothing,
        }
    }

    /// Partition `id` is closed here from now, as `closed`, and no longer served: its gate
    /// goes in the same step, so it is never seen as neither.
    pub(super) fn close(&self, id: &str, closed: super::close::Closed) {
        self.local().entry(id.to_owned()).or_default().is = Is::Closed(closed);
    }

    /// What `f` makes of partition `id`'s close, `None` if it isn't closed here. `f` runs
    /// under the lock, so it can't take it again.
    pub(super) fn closed<R>(
        &self,
        id: &str,
        f: impl FnOnce(&mut super::close::Closed) -> R,
    ) -> Option<R> {
        match &mut self.local().get_mut(id)?.is {
            Is::Closed(c) => Some(f(c)),
            _ => None,
        }
    }

    /// Partition `id` is no longer closed here (stamped over, or its files dropped).
    /// Nothing for one that isn't closed.
    pub(super) fn forget_closed(&self, id: &str) {
        let mut local = self.local();
        let Some(l) = local.get_mut(id) else {
            return;
        };
        if matches!(l.is, Is::Closed(_)) {
            l.is = Is::Neither;
            if !l.opening {
                local.remove(id);
            }
        }
    }

    /// Partitions a peer's map can still confirm: without a live lease (operator-fenced or
    /// not, #1829), and named by `map`, this node's (`node`), at their file's epoch. One the
    /// map no longer names there can never be confirmed (a map's epochs only rise), so the
    /// rounds don't hurry for it (audit120's S1 on #1897: a move's old node, restarted with
    /// its copy fenced, would otherwise ask its peers for ever).
    pub(crate) fn confirmable(&self, map: &ClusterMap, node: &str) -> Vec<String> {
        let mut out = Vec::new();
        self.for_served(|id, s| {
            if s.gate.awaiting_peers() && names(map, node, id, s.epoch) {
                out.push(id.to_owned());
            }
        });
        out
    }

    /// What a lease round asks for (D326): every partition served here that `map` names
    /// this node (`node`) for at its file's epoch, operator-fenced or not (an operator's
    /// fence stops writes, not reads).
    pub(crate) fn askable(&self, map: &ClusterMap, node: &str) -> Vec<LeasePart> {
        let mut out = Vec::new();
        self.for_served(|id, s| {
            if names(map, node, id, s.epoch) {
                out.push(LeasePart {
                    partition: id.to_owned(),
                    epoch: s.epoch,
                    term: 0,
                    ..LeasePart::default()
                });
            }
        });
        out
    }

    /// Ends the lease of every partition served here that `map` doesn't name this node
    /// (`node`) for at its file's epoch (audit120's S1): a map that moved it away. Run after
    /// an install publishes the map, so a round that read the map before it either set its
    /// lease first (and this clears it) or saw the new map (and set none).
    pub(crate) fn lapse_unnamed(&self, map: &ClusterMap, node: &str) {
        self.for_served(|id, s| {
            if !names(map, node, id, s.epoch) && !s.gate.awaiting_peers() {
                s.gate.lapse();
                eprintln!(
                    "phdb serve: partition {id}'s lease here ended: map version {} doesn't name \
                     this node for it at epoch {}",
                    map.map_version, s.epoch
                );
            }
        });
    }
}

/// Whether `map` names node `node` for partition `id` at `epoch`.
fn names(map: &ClusterMap, node: &str, id: &str, epoch: u64) -> bool {
    map.partitions
        .iter()
        .any(|p| p.id == id && p.node == node && p.epoch == epoch)
}

impl Node {
    /// Loads `DIR/fenced.json`, before any partition opens.
    pub(crate) fn load_fences(&self) -> Result<(), String> {
        *self.fences.operator() = read_fenced(&self.dir)?;
        Ok(())
    }

    /// Partition `id`'s gate as it opens here: with no lease until a majority of the map's
    /// nodes grant one (D326; rule 3: a node that was down may have missed the partition's
    /// move), and fenced by the operator as well if `fenced.json` lists it. The two are
    /// separate (#1829), so an operator's fence doesn't hide the wait. Not served yet:
    /// [`serve_gate`](Self::serve_gate) does that, once writes route to it.
    pub(crate) fn new_gate(&self, id: &str) -> Arc<WriteGate> {
        let operator = self.fences.operator().contains(id);
        WriteGate::opened(id, operator, Arc::clone(self.grants.clock()))
    }

    /// Partition `id` is served here from now, behind `gate`, at its file's `epoch`: `Status`
    /// shows it open, its lease rounds ask for it, and the operator can fence it. Only once
    /// the router's parts hold it, so `Status` never shows it open before a write can route
    /// to it (#2113).
    pub(crate) fn serve_gate(&self, id: &str, gate: &Arc<WriteGate>, epoch: u64) {
        self.fences.serve(id, Arc::clone(gate), epoch);
    }

    /// Partition `id`'s state here, and why it's fenced when it is, for `Status`: `None`
    /// when it isn't open here.
    pub(crate) fn part_state(&self, id: &str) -> Option<(LocalPartitionState, FenceReason)> {
        let (gate, _) = self.fences.gate(id)?;
        Some(gate_state(&gate))
    }

    /// Partition `id`'s state as `Status` reports it, for `Admin.Info` (#1852): open
    /// ([`part_state`](Self::part_state)), else closed, else being opened. Unspecified for one
    /// the node hasn't opened (no file, or a failed open).
    pub(crate) fn local_state(&self, id: &str) -> (LocalPartitionState, FenceReason) {
        match self.fences.now(id, |_| ()) {
            Now::Served(gate) => gate_state(&gate),
            Now::Closed => (LocalPartitionState::Closed, FenceReason::Unspecified),
            Now::Opening => (LocalPartitionState::Opening, FenceReason::Unspecified),
            Now::Nothing => (LocalPartitionState::Unspecified, FenceReason::Unspecified),
        }
    }

    /// Why partition `id` is fenced here, for the startup line and the log.
    pub(crate) fn fence_of(&self, id: &str) -> Option<Fence> {
        self.fences.gate(id).and_then(|(g, _)| g.fenced())
    }

    /// The time left on partition `id`'s lease here, zero without one (`Status`'s
    /// `lease_remaining_ms`, D326).
    pub(crate) fn lease_remaining(&self, id: &str) -> Duration {
        self.fences
            .gate(id)
            .map_or(Duration::ZERO, |(g, _)| g.lease_remaining())
    }

    /// Whether partition `id`, open here, holds no lease, whatever the operator's fence
    /// (`Status`'s `awaiting_peers`, #1829).
    pub(crate) fn awaiting_peers(&self, id: &str) -> bool {
        // Not for a copy the node's map no longer names at its epoch: no peer can confirm
        // that one (audit120's note on #1897).
        self.fences.gate(id).is_some_and(|(g, epoch)| {
            g.awaiting_peers() && self.get_map().is_ok_and(|m| names(&m, &self.id, id, epoch))
        })
    }

    /// `Cluster.Fence`: from now, P's writes answer `Fenced`; answers once every write
    /// admitted before has finished, and its feed's durable frontier has reached the last of
    /// them (both within [`DRAIN_WAIT`]). Persisted, so it survives a restart. Fencing a
    /// fenced partition waits again.
    pub(crate) async fn fence(&self, id: &str) -> Result<(), ApiError> {
        let deadline = tokio::time::Instant::now() + DRAIN_WAIT;
        let _one = self.fences.changing.lock().await;
        let (gate, _) = self.fences.gate(id).ok_or_else(|| not_here(id))?;
        // The flag first (S4): from here no write is admitted, whatever happens next.
        gate.fence();
        let mut fenced = self.fences.operator().clone();
        if fenced.insert(id.to_owned()) {
            self.persist_fences(fenced).await?;
        }
        eprintln!("phdb serve: partition {id} fenced: it takes no writes");
        gate.drained(DRAIN_WAIT).await.map_err(|n| {
            ApiError::deadline(format!(
                "partition {id:?} is fenced, but {n} write(s) admitted before the fence are \
                 still running: run fence again to wait for them"
            ))
        })?;
        // Then the frontier (feed1248's S1 on #2184): a fed `Buffered` or `None` commit
        // answered before the fence passes the durable frontier, which the feed reports and a
        // promote counts the loss from, only once the shard's background sync covers it
        // (*T_sync*). Nothing commits now, so once the frontier reaches the visible seqno it
        // is final. A flush wouldn't hasten it: the hold goes at the sync's timer either way.
        let Some(db) = self
            .server
            .get()
            .and_then(|s| s.shared.upgrade())
            .and_then(|shared| shared.router.partition(id).ok())
            .map(|part| part.db.clone())
        else {
            return Ok(());
        };
        // Read once (feed1248's point 1): a commit that doesn't pass the write gate (a D329
        // decide, a D331 range copy's batch) can still raise it, and the wait mustn't chase
        // those. They're replicated, so the frontier covers them too, but a fence vouches
        // only for what committed before it answered.
        let last = db.visible_seqno();
        while db.replication_frontier() < last {
            if tokio::time::Instant::now() >= deadline {
                return Err(ApiError::deadline(format!(
                    "partition {id:?} is fenced, but its feed's durable frontier is at {} of \
                     {last}: its background sync hasn't covered its last writes yet; run \
                     fence again to wait for it",
                    db.replication_frontier()
                )));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    /// `Cluster.Unfence`: lifts the operator's fence, so P takes writes again once it also
    /// holds its lease (D326). Refused unless this node's map names it for P at its file's
    /// epoch (a write would break rule 1 or 2 anyway), and unless a majority's newest map
    /// says the same (or `force`): this node's map alone may be behind. `force` never gives
    /// P a lease: that still takes a majority's grants (the owner's call on D326). P first
    /// takes the map's schema it skipped while fenced.
    pub(crate) async fn unfence(self: &Arc<Self>, id: &str, force: bool) -> Result<(), ApiError> {
        let _one = self.fences.changing.lock().await;
        // The checks first, at the epoch of the open partition or of the closed one's file:
        // a refused unfence leaves a closed partition closed, its hashes kept (simp724's N1
        // on #1826).
        let closed = self.fences.closed(id, |c| c.epoch);
        let (epoch, unconfirmed) = match closed {
            Some(epoch) => (epoch, false),
            None => {
                let (gate, epoch) = self.fences.gate(id).ok_or_else(|| not_here(id))?;
                (epoch, gate.awaiting_peers())
            }
        };
        // This node's own map may be stale, whichever fence it is: it was down or missed a
        // push, and a move may have passed it by. So its peers' newest map must confirm the
        // partition first, as at start, unless the operator forces it (cli692's S1 on
        // #1755). An operator's fence needs that as much as a start fence does (audit120's
        // P2-F1): a move fences its old node as the operator, and that node is the one whose
        // map is behind when it misses the move's map. Asked whichever flags are set
        // (audit120's D1 on the #1829 design): a move's old node that never restarted has
        // been confirmed long since, and only the operator's fence is set.
        if !force {
            let round = self.round().await?;
            self.apply_leases(&round);
            if !round.confirms(&self.id, id, epoch) {
                let since = if unconfirmed {
                    "fenced, and unconfirmed since it opened here,"
                } else {
                    "fenced"
                };
                let why = if !round.divergent.is_empty() {
                    format!(
                        "node{} {} hold{} a map that conflicts with this node's version {}",
                        if round.divergent.len() > 1 { "s" } else { "" },
                        round.divergent.join(", "),
                        if round.divergent.len() > 1 { "" } else { "s" },
                        round.map.map_version
                    )
                } else if !round.behind.is_empty() {
                    format!(
                        "node{} {} answered with an older map and didn't take version {}",
                        if round.behind.len() > 1 { "s" } else { "" },
                        round.behind.join(", "),
                        round.map.map_version
                    )
                } else if !names(&round.map, &self.id, id, epoch) {
                    format!(
                        "the newest map among the peers that answer, version {}, doesn't name \
                         this node for it at epoch {epoch}: the partition moved on",
                        round.map.map_version
                    )
                } else {
                    format!(
                        "{} of {} nodes hold map version {} ({} of the peers answered), not a \
                         majority",
                        round.holders,
                        round.map.nodes.len(),
                        round.map.map_version,
                        round.answered
                    )
                };
                return Err(ApiError::failed_precondition(format!(
                    "partition {id:?} is {since}, and its peers don't confirm it's still this \
                     node's at epoch {epoch}: {why}. A majority of the map's nodes must hold \
                     the newest map, and it must name this node (D317 rule 3). Wait for the \
                     peers to answer. --force lifts the operator's fence without asking them, \
                     but the partition serves only once a majority grants its lease (D326)"
                )));
            }
        }
        // The node's map as it is now: the peers' newest was taken if it was newer.
        let map = self.get_map()?;
        if !names(&map, &self.id, id, epoch) {
            return Err(ApiError::failed_precondition(format!(
                "the cluster map (version {}) doesn't name this node for partition {id:?} at \
                 its file's epoch {epoch}: it can't take writes here",
                map.map_version
            )));
        }
        // A closed partition reopens, still fenced (D317 amendment 2).
        if closed.is_some() {
            self.open_part(id).await?;
        }
        let (gate, _) = self.fences.gate(id).ok_or_else(|| not_here(id))?;
        let mut fenced = self.fences.operator().clone();
        if fenced.remove(id) {
            self.persist_fences(fenced).await?;
        }
        // The schema an operator's fence held back, before the first write: this partition
        // reconciles to the node's current map although its gate is still fenced, and the
        // gate lifts under the install lock, so a schema change can't land between the two
        // and skip it (audit120's F6 and its first should). The flag last (S4).
        let node = Arc::clone(self);
        let part = id.to_owned();
        tokio::task::spawn_blocking(move || {
            let _installing = node.reconcile_current(Some(&part));
            gate.unfence();
        })
        .await?;
        // A partition reopened here (it was closed), or one whose lease hasn't come yet:
        // one round now, so the answer says whether it serves (D326).
        if self.awaiting_peers(id) {
            self.confirm_round().await;
        }
        if self.awaiting_peers(id) {
            eprintln!(
                "phdb serve: partition {id} unfenced: it takes writes once a majority of the \
                 map's nodes grant its lease"
            );
        } else {
            eprintln!("phdb serve: partition {id} unfenced: it takes writes");
        }
        Ok(())
    }

    /// Drops `id` from the operator's fences (and `fenced.json`) if it's there: its files are
    /// gone, so the fence was another partition's of that name. The caller holds `changing`.
    pub(super) async fn forget_operator_fence(&self, id: &str) -> Result<(), ApiError> {
        let mut fenced = self.fences.operator().clone();
        if fenced.remove(id) {
            self.persist_fences(fenced).await?;
        }
        Ok(())
    }

    pub(super) async fn persist_fences(&self, fenced: BTreeSet<String>) -> Result<(), ApiError> {
        let dir = self.dir.clone();
        let written = fenced.clone();
        tokio::task::spawn_blocking(move || write_fenced(&dir, &written))
            .await?
            .map_err(|e| ApiError::internal(format!("persist {FENCED_FILE}: {e}")))?;
        *self.fences.operator() = fenced;
        Ok(())
    }

    /// One confirmation round (D317 rule 3, as amended for #1760): this node asks every
    /// peer for its map at once, each within the push timeout, and installs the newest if
    /// it's newer than its own (a map that moved a partition on is then this node's, and
    /// can't confirm it here: the newer-version veto). It then pushes its map to each peer
    /// that answered with an older one, waiting for the ack, so a peer counts only once it
    /// holds the map, and an old owner always has a move's map before the new owner can
    /// count that peer (audit120's D3). A node that holds the newest map alone spreads it
    /// here (D4).
    ///
    /// **Leases (D326):** it then asks every node of the map that holds it, this one
    /// included, for a grant of the partitions this node serves at their epochs
    /// ([`Fences::askable`]), and counts the grants by partition. Only the nodes that
    /// answered are asked, so a node that's down costs the round one push timeout, not two.
    /// `t0`, the clock before the first request, is what a lease runs from.
    pub(crate) async fn round(self: &Arc<Self>) -> Result<Round, ApiError> {
        let t0 = self.grants.clock().now();
        let mine = self.get_map()?;
        let mut asks = tokio::task::JoinSet::new();
        for n in mine.nodes.iter().filter(|n| n.id != self.id) {
            let (node, peer, address) = (Arc::clone(self), n.id.clone(), n.address.clone());
            asks.spawn(async move {
                let theirs =
                    tokio::time::timeout(node.peers.push_timeout, node.fetch_map(&address))
                        .await
                        .ok()
                        .and_then(Result::ok);
                theirs.map(|m| (peer, address, m))
            });
        }
        let answers: Vec<(String, String, ClusterMap)> =
            asks.join_all().await.into_iter().flatten().collect();
        // Ordered by `(writer_term, map_version)` (D330, simp724's S1 on the PR 1 plan): an
        // old writer's minority map, at a higher version but a lower term, is behind.
        let mine_key = self
            .get_map()
            .map_or((0, 0), |m| (m.writer_term, m.map_version));
        if let Some((peer, _, newest)) = answers
            .iter()
            .max_by_key(|(_, _, m)| (m.writer_term, m.map_version))
            && (newest.writer_term, newest.map_version) > mine_key
            && let Err(e) = self
                .push_map_async(newest.clone(), Some(peer.clone()))
                .await
        {
            eprintln!("phdb serve: node {peer}'s newer cluster map: {}", e.message);
        }
        let map = self.get_map()?;
        let json = super::map::canonical_json(&map);
        // Counted only among the round's map's nodes: one that map removed doesn't vote
        // (audit120's F1 on #1899).
        let members: BTreeSet<&str> = map.nodes.iter().map(|n| n.id.as_str()).collect();
        let mut holders = usize::from(members.contains(self.id.as_str()));
        let mut divergent = Vec::new();
        let mut pushes = tokio::task::JoinSet::new();
        for (peer, address, theirs) in &answers {
            match (theirs.writer_term, theirs.map_version).cmp(&(map.writer_term, map.map_version))
            {
                std::cmp::Ordering::Equal if super::map::canonical_json(theirs) == json => {
                    holders += usize::from(members.contains(peer.as_str()));
                }
                std::cmp::Ordering::Equal => divergent.push(peer.clone()),
                std::cmp::Ordering::Less => {
                    let (node, peer, address, map) =
                        (Arc::clone(self), peer.clone(), address.clone(), map.clone());
                    pushes.spawn(async move {
                        let pushed = tokio::time::timeout(
                            node.peers.push_timeout,
                            node.push_to(&address, map),
                        )
                        .await
                        .unwrap_or_else(|_| Err("timed out".to_owned()));
                        (peer, pushed)
                    });
                }
                // Newer than the map this node now holds: its install was refused (a forced
                // map from another history, say). It holds a map that conflicts with this
                // node's, so it blocks the round as a divergent one does (audit120's F3).
                std::cmp::Ordering::Greater => divergent.push(peer.clone()),
            }
        }
        // An answerer that was behind counts once it took the map. One that didn't blocks
        // the round: it answered with an older map, and may be an old owner that hasn't taken
        // the move yet (audit120's F2). A peer that never answered doesn't block, and nor
        // does one whose term fence refused the map (#2073): it voted above the map's term,
        // and takes no map below it. It counts as one that never answered. That's safe by
        // D326, not by F2 (audit120's S1 on #2083): it can't renew a lease once a majority
        // holds a newer map, a lease it still holds is withheld from any other node by its
        // grantors until it runs out, and a round that leaves it out still needs a majority of
        // holders and of grants. Were it to block, a node stranded by a lost vote would stop
        // every node's leases, the writer's included, and the writer could never adopt the
        // term that lets it take the map.
        let mut behind = Vec::new();
        let mut holding: Vec<(String, String)> = answers
            .iter()
            .filter(|(peer, _, theirs)| {
                members.contains(peer.as_str())
                    && (theirs.writer_term, theirs.map_version)
                        == (map.writer_term, map.map_version)
                    && !divergent.contains(peer)
            })
            .map(|(peer, address, _)| (peer.clone(), address.clone()))
            .collect();
        let writer = super::schema::writer(&map) == self.id;
        for (peer, pushed) in pushes.join_all().await {
            if let Err(e) = &pushed
                && e.contains(super::VOTED_ABOVE)
            {
                // The writer's duties look at it (#2073).
                if writer {
                    let now = self.grants.clock().now();
                    self.stranded
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .refused(&peer, now, self.grants.lease());
                }
                continue;
            }
            if pushed.is_ok() {
                holders += usize::from(members.contains(peer.as_str()));
                if members.contains(peer.as_str())
                    && let Some((_, address, _)) = answers.iter().find(|(p, _, _)| *p == peer)
                {
                    holding.push((peer.clone(), address.clone()));
                }
            } else {
                behind.push(peer);
            }
        }
        let mut round = Round {
            map,
            holders,
            answered: answers.len(),
            divergent,
            behind,
            t0,
            granted: BTreeMap::new(),
            withheld: Vec::new(),
            grace_ms: 0,
        };
        self.ask_leases(&mut round, &holding).await;
        Ok(round)
    }

    /// Partition `id`'s in-doubt external shares here (D329), for its lease part (D330 PR
    /// 2's F7): their xids as UTF-8 (hex if not), as many as fit in
    /// [`super::map::IN_DOUBT_BYTES`], and how many there are. None while it isn't served
    /// here.
    fn in_doubt_of(&self, id: &str) -> (Vec<String>, u64) {
        let Some(shared) = self.server.get().and_then(|o| o.shared.upgrade()) else {
            return (Vec::new(), 0);
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return (Vec::new(), 0);
        };
        let parts = cluster.parts.load();
        let Some(Ok(part)) = parts.get(id).map(|s| s.part()) else {
            return (Vec::new(), 0);
        };
        let all = part.db.in_doubt();
        let mut xids = Vec::new();
        let mut bytes = 0;
        for (xid, _) in &all {
            // As the operator pastes it into `phdb cluster txn decide` (simp724's M1 on
            // #2069): serve's xids are UTF-8; another's bytes go as hex.
            let text = String::from_utf8(xid.clone()).unwrap_or_else(|_| {
                use std::fmt::Write as _;
                xid.iter()
                    .fold(String::with_capacity(xid.len() * 2), |mut s, b| {
                        let _ = write!(s, "{b:02x}");
                        s
                    })
            });
            bytes += text.len();
            if bytes > super::map::IN_DOUBT_BYTES {
                break;
            }
            xids.push(text);
        }
        (xids, all.len() as u64)
    }

    /// A round's lease requests (D326): to each node in `holding` (id, address), which hold
    /// the round's map, and to this node's own grants if the map names it; each bounded by
    /// the push timeout. Counts each partition's grants into `round`.
    async fn ask_leases(self: &Arc<Self>, round: &mut Round, holding: &[(String, String)]) {
        let mut parts = self.fences.askable(&round.map, &self.id);
        // Every part carries the partition's in-doubt count: the balancer never moves a
        // partition with shares in doubt (D332) and reads the count from these renewals, so
        // a grant from before it was switched on must have it too (audit120's M1 on 1.3.0,
        // simp724 on #2127). With failover on, the shares' ids too, so a writer that fails
        // it over can name them (D330 PR 2's F7).
        for p in &mut parts {
            let (xids, count) = self.in_doubt_of(&p.partition);
            p.in_doubt_count = count;
            if round.map.failover {
                p.in_doubt_xids = xids;
            }
        }
        // D335 PR 2b: a partition in its fallback asks the round to record where it began,
        // or that it ended; counted from the grantors' synced echoes below.
        let marks = self.lease_marks(&mut parts);
        // The writer lease (D330): the map's writer asks for it at the map's term; a
        // candidate, at the term it was voted, until its map lands.
        if let Some(term) = self.writer_term_to_ask(&round.map) {
            parts.push(LeasePart {
                partition: super::lease::WRITER_LEASE.to_owned(),
                epoch: 0,
                term,
                ..LeasePart::default()
            });
        }
        // Nothing to ask, and nothing asked last time: no grant here to end.
        if !self
            .fences
            .asked
            .swap(!parts.is_empty(), std::sync::atomic::Ordering::AcqRel)
            && parts.is_empty()
        {
            return;
        }
        let req = LeaseRequest {
            cluster_id: round.map.cluster_id.clone(),
            node_id: self.id.clone(),
            map_version: round.map.map_version,
            round: self
                .fences
                .round
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1,
            parts,
            lease_ms: self.grants.lease_ms(),
            start: self.grants.start(),
            writer_term: round.map.writer_term,
            pre_vote: false,
        };
        let mut asks = tokio::task::JoinSet::new();
        for (peer, address) in holding {
            let (node, peer, address, req) =
                (Arc::clone(self), peer.clone(), address.clone(), req.clone());
            asks.spawn(async move {
                let answer = tokio::time::timeout(
                    node.peers.push_timeout,
                    node.ask_lease(&address, req.clone()),
                )
                .await;
                match answer {
                    Ok(Ok(r)) => {
                        node.fences.pre_lease(&peer, false);
                        node.fences.unrouted(&peer, false);
                        Some((peer, r))
                    }
                    // `Unimplemented`: a build before D326 (1.2) in a rolling upgrade
                    // (#2047), or a 1.3 node behind something that doesn't route the call
                    // (an HTTP 404 reads as `Unimplemented`, audit120's S1 on #2048). Only the
                    // first counts, and only once its `Status`, asked now rather than from
                    // the cache, lacks `leases`: it holds the round's map (it answered with
                    // it, or took it) and grants as 1.2 serves, on that map alone, so a mixed
                    // cluster serves with D317's confirmation and the 1.3 nodes' grants. A
                    // 1.2 node has no grant table and no start grace, so nothing is withheld.
                    Ok(Err(s)) if s.code() == tonic::Code::Unimplemented => {
                        let build = node
                            .features_of(
                                std::iter::once((peer.clone(), address.clone())),
                                super::LEASES_FEATURE,
                                false,
                            )
                            .await
                            .pop()
                            .map(|(_, v)| v);
                        match build {
                            Some(super::PeerFeature::Lacks) => {
                                if node.fences.pre_lease(&peer, true) {
                                    eprintln!(
                                        "phdb serve: node {peer} runs a build without \
                                         partition leases (before 1.3): counted as granting \
                                         each lease this node asks, until it's upgraded"
                                    );
                                }
                                // Never the writer lease (D330, simp724's N1 on the PR 1
                                // plan): a 1.2 node grants no such thing, so a phantom
                                // grant mustn't count toward it.
                                // Nor a record of a fallback (D335 PR 2b): it echoes only
                                // what a grantor synced, and this one synced nothing.
                                let granted = req
                                    .parts
                                    .iter()
                                    .filter(|p| p.partition != super::lease::WRITER_LEASE)
                                    .map(|p| LeasePart {
                                        degraded_from: None,
                                        degraded_clear: false,
                                        ..p.clone()
                                    })
                                    .collect();
                                Some((
                                    peer,
                                    super::proto::LeaseResponse {
                                        granted,
                                        ..Default::default()
                                    },
                                ))
                            }
                            Some(super::PeerFeature::Has) => {
                                node.fences.pre_lease(&peer, false);
                                if node.fences.unrouted(&peer, true) {
                                    eprintln!(
                                        "phdb serve: node {peer} reports leases, but its \
                                         Cluster.Lease answered Unimplemented: check what \
                                         sits between the nodes (a proxy that doesn't route \
                                         /pigeonhole.v1.Cluster/Lease); counted as no grant"
                                    );
                                }
                                None
                            }
                            _ => None,
                        }
                    }
                    Ok(Err(s)) => {
                        eprintln!("phdb serve: node {peer}'s lease grant: {}", s.message());
                        None
                    }
                    Err(_) => None,
                }
            });
        }
        let mut answers: Vec<(String, super::proto::LeaseResponse)> = Vec::new();
        if round.map.nodes.iter().any(|n| n.id == self.id) {
            match self.grants.grant(&round.map, &req) {
                Ok(r) => answers.push((
                    self.id.clone(),
                    self.record_degraded(&round.map, &req, r).await,
                )),
                Err(e) => eprintln!("phdb serve: this node's own lease grant: {}", e.message),
            }
        }
        answers.extend(asks.join_all().await.into_iter().flatten());
        self.count_marks(&round.map, &marks, &answers);
        for (_, r) in answers {
            for g in r.granted {
                *round.granted.entry(g.partition).or_default() += 1;
            }
            round.withheld.extend(
                r.withheld
                    .into_iter()
                    .map(|w| (w.partition, w.holder, w.remaining_ms)),
            );
            round.grace_ms = round.grace_ms.max(r.grace_remaining_ms);
        }
    }

    /// The open partition `id`'s replica acknowledgements here, if it's open.
    pub(crate) fn acks_of(&self, id: &str) -> Option<Arc<crate::serve::acks::ReplicaAcks>> {
        let shared = self.server.get().and_then(|o| o.shared.upgrade())?;
        let cluster = shared.router.cluster.as_ref()?;
        let parts = cluster.parts.load();
        let service = parts.get(id)?;
        service.part_opt().map(|p| Arc::clone(&p.acks))
    }

    /// The open partition `id`'s visible seqno here, if it's open.
    fn visible_of(&self, id: &str) -> Option<u64> {
        let shared = self.server.get().and_then(|o| o.shared.upgrade())?;
        let cluster = shared.router.cluster.as_ref()?;
        let parts = cluster.parts.load();
        let service = parts.get(id)?;
        service.part_opt().map(|p| p.db.visible_seqno())
    }

    /// D335 PR 2b: each of `parts` in its fallback carries its mark (its start, or its end);
    /// the marks, for [`Node::count_marks`].
    fn lease_marks(
        &self,
        parts: &mut [LeasePart],
    ) -> Vec<(
        String,
        crate::serve::acks::Mark,
        Arc<crate::serve::acks::ReplicaAcks>,
    )> {
        use crate::serve::acks::Mark;
        let mut marks = Vec::new();
        for p in parts.iter_mut() {
            let Some(acks) = self.acks_of(&p.partition) else {
                continue;
            };
            let Some(mark) = acks.lease_mark() else {
                continue;
            };
            match mark {
                Mark::From(from) => p.degraded_from = Some(from.into()),
                Mark::Clear => p.degraded_clear = true,
            }
            marks.push((p.partition.clone(), mark, acks));
        }
        marks
    }

    /// D335 PR 2b: a mark a majority of `map`'s nodes echoed (each echo is of a synced
    /// record) is recorded, counted over `map`'s node set; a grantor that echoed an earlier
    /// start than the one asked (recorded before this owner lost its own) lowers it.
    fn count_marks(
        &self,
        map: &ClusterMap,
        marks: &[(
            String,
            crate::serve::acks::Mark,
            Arc<crate::serve::acks::ReplicaAcks>,
        )],
        answers: &[(String, super::proto::LeaseResponse)],
    ) {
        use crate::serve::acks::{Mark, Pos};
        // A grantor's FLAGGED record of a partition not in its fallback here: this owner
        // lost it (a restart without its own record), or left on a majority's clear that
        // grantor missed. It resumes, taking every write it has as possibly answered
        // uncovered, and clears it the usual way.
        let mut flagged: BTreeMap<&str, Pos> = BTreeMap::new();
        for g in answers.iter().flat_map(|(_, r)| r.granted.iter()) {
            if let Some(f) = g.degraded_from.as_ref().map(Pos::from)
                && !marks.iter().any(|(id, _, _)| *id == g.partition)
            {
                let e = flagged.entry(g.partition.as_str()).or_insert(f);
                *e = (*e).min(f);
            }
        }
        for (id, from) in flagged {
            if let (Some(acks), Some(visible)) = (self.acks_of(id), self.visible_of(id)) {
                acks.resume(from, visible);
            }
        }
        if marks.is_empty() {
            return;
        }
        let nodes = crate::serve::acks::node_set(map);
        for (id, mark, acks) in marks {
            let mut echoed = 0usize;
            let mut lower: Option<Pos> = None;
            for g in answers
                .iter()
                .flat_map(|(_, r)| r.granted.iter())
                .filter(|g| g.partition == *id)
            {
                match mark {
                    Mark::From(from) => match g.degraded_from.as_ref().map(Pos::from) {
                        Some(e) if e == *from => echoed += 1,
                        Some(e) if e < *from => lower = Some(lower.map_or(e, |l| l.min(e))),
                        _ => {}
                    },
                    Mark::Clear => echoed += usize::from(g.degraded_clear),
                }
            }
            if let Some(l) = lower {
                acks.lower_from(l);
            } else if echoed * 2 > map.nodes.len() {
                acks.marked(*mark, &nodes);
            }
        }
    }

    /// D335 PR 2b: grant `r`'s records ([`super::degraded::Records::record`]) on a blocking
    /// thread (the fsync), and its echoes, when it grants a partition whose map sets
    /// `replica_acks`. A failed write is logged, and `r` goes without echoes.
    pub(crate) async fn record_degraded(
        self: &Arc<Self>,
        mine: &ClusterMap,
        req: &super::proto::LeaseRequest,
        r: super::proto::LeaseResponse,
    ) -> super::proto::LeaseResponse {
        let acked = |id: &str| {
            mine.partitions
                .iter()
                .any(|p| p.id == id && p.replica_acks > 0)
        };
        if !r.granted.iter().any(|g| acked(&g.partition)) {
            return r;
        }
        let (node, mine, asked) = (Arc::clone(self), mine.clone(), req.parts.clone());
        let fallback = r.clone();
        let done = tokio::task::spawn_blocking(move || {
            let mut r = r;
            let res = node.degraded.record(&mine, &asked, &mut r.granted);
            (r, res)
        })
        .await;
        match done {
            Ok((r, Ok(()))) => r,
            Ok((_, Err(e))) => {
                eprintln!(
                    "phdb serve: recording replica-ack fallbacks in {}: {e}; this grant \
                     confirms none",
                    super::degraded::DEGRADED_FILE
                );
                fallback
            }
            Err(e) => {
                eprintln!("phdb serve: recording replica-ack fallbacks: {e}");
                fallback
            }
        }
    }

    /// Gives up partition `id` at `epoch`, which this node closed (D326 PR 2): every node of
    /// its map drops it from its grant to this node, this node's own grants included. In a
    /// task, best effort, each peer within the push timeout: a release that doesn't arrive
    /// costs the new owner a wait until the grant runs out, never safety. Numbered as a
    /// lease round, so an older round's request that arrives after it is ignored.
    pub(crate) fn release_soon(&self, id: &str, epoch: u64) {
        self.release_part_soon(LeasePart {
            partition: id.to_owned(),
            epoch,
            term: 0,
            ..LeasePart::default()
        });
    }

    /// The next lease round's number, for a request or a release.
    pub(super) fn next_round(&self) -> u64 {
        self.fences
            .round
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// [`Node::release_soon`] of any part: a partition at its epoch, or the writer lease at
    /// its term (D330, simp724's N2 on the PR 1 plan).
    pub(crate) fn release_part_soon(&self, part: LeasePart) {
        let (Some(node), Ok(rt), Ok(map)) = (
            self.me.upgrade(),
            tokio::runtime::Handle::try_current(),
            self.get_map(),
        ) else {
            return;
        };
        let req = super::proto::ReleaseRequest {
            cluster_id: map.cluster_id.clone(),
            node_id: self.id.clone(),
            round: self.next_round(),
            parts: vec![part],
            start: self.grants.start(),
        };
        self.grants.release(&req);
        #[cfg(test)]
        if drop_releases(&self.dir) {
            return;
        }
        for n in map.nodes.into_iter().filter(|n| n.id != self.id) {
            let (node, req) = (Arc::clone(&node), req.clone());
            rt.spawn(async move {
                let sent = tokio::time::timeout(
                    node.peers.push_timeout,
                    node.send_release(&n.address, req),
                )
                .await;
                // A build before D326 (1.2, #2047) holds no grant to release.
                if let Ok(Err(e)) = sent
                    && e.code() != tonic::Code::Unimplemented
                {
                    let e = e.message();
                    eprintln!(
                        "phdb serve: releasing a partition's lease at node {}: {e}",
                        n.id
                    );
                }
            });
        }
    }

    /// Sends a release to the node at `address`.
    async fn send_release(
        &self,
        address: &str,
        req: super::proto::ReleaseRequest,
    ) -> Result<(), tonic::Status> {
        let mut client = self
            .client(address)
            .await
            .map_err(tonic::Status::unavailable)?;
        let req = self.request(req).map_err(tonic::Status::internal)?;
        client.release(req).await.map(drop)
    }

    /// Asks the node at `address` for a lease grant: its answer, or its status (a build
    /// before D326 answers `Unimplemented`). A failed connect is `Unavailable` and a request
    /// that can't be built `Internal`, never `Unimplemented`, so neither can count as a 1.2
    /// peer's grant.
    pub(super) async fn ask_lease(
        &self,
        address: &str,
        req: LeaseRequest,
    ) -> Result<super::proto::LeaseResponse, tonic::Status> {
        let mut client = self
            .client(address)
            .await
            .map_err(tonic::Status::unavailable)?;
        let req = self.request(req).map_err(tonic::Status::internal)?;
        client.lease(req).await.map(tonic::Response::into_inner)
    }

    /// Gives each partition served here the lease `round` gives it (D326): until
    /// `t0 + L·(1−ρ)/(1+ρ)`. Only while the node's map is still the round's, checked and set
    /// under the state lock: an install publishes a newer map under it first and ends the
    /// leases that map no longer names after, so a renewal can't bring back a lease the
    /// newer map ended (audit120's S1).
    pub(crate) fn apply_leases(&self, round: &Round) {
        let end = super::lease::holder_end(round.t0, self.grants.lease());
        let st = self.state();
        // The round's map by `(writer_term, map_version)`, not the version alone (#2159 item
        // 4): a deposed writer's map and the elected writer's can share a version.
        let same = |m: &ClusterMap| (m.writer_term, m.map_version);
        if st.map.as_ref().map(|(m, _)| same(m)) != Some(same(&round.map)) {
            return;
        }
        self.fences.for_served(|id, s| {
            if !round.leased(&self.id, id, s.epoch) {
                // A renewal that failed while the lease still runs: said each round until it
                // runs out, so the log shows why a partition stopped serving.
                if names(&round.map, &self.id, id, s.epoch) && !s.gate.awaiting_peers() {
                    eprintln!(
                        "phdb serve: partition {id}'s lease wasn't renewed ({}); it ends in \
                         {} ms unless a renewal reaches a majority",
                        round.why_not_leased(id),
                        s.gate.lease_remaining().as_millis()
                    );
                }
                return;
            }
            if !s.gate.renew(end) {
                return;
            }
            if s.gate.operator_fenced() {
                eprintln!(
                    "phdb serve: partition {id} leased by {} of {} nodes at map version {}, \
                     and still fenced by the operator (phdb cluster unfence lifts it)",
                    round.granted.get(id).copied().unwrap_or(0),
                    round.map.nodes.len(),
                    round.map.map_version
                );
            } else {
                eprintln!(
                    "phdb serve: partition {id} serves: {} of {} nodes granted its lease at \
                     map version {}, which places it on this node",
                    round.granted.get(id).copied().unwrap_or(0),
                    round.map.nodes.len(),
                    round.map.map_version
                );
            }
        });
    }

    /// Pushes `map` to the peer at `address` (a round's catch-up of a peer that's behind).
    async fn push_to(&self, address: &str, map: ClusterMap) -> Result<(), String> {
        let mut client = self.client(address).await?;
        let req = self.request(super::proto::PushMapRequest { map: Some(map) })?;
        client
            .push_map(req)
            .await
            .map(drop)
            .map_err(|s| s.message().to_owned())
    }

    /// A lease round, then the leases it gives (D326): each partition its map names here at
    /// the file's epoch, with a majority of the map's nodes holding that map and granting
    /// it. An operator's fence stays (#1829).
    pub(super) async fn confirm_round(self: &Arc<Self>) -> Option<Round> {
        let round = match self.round().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("phdb serve: renewing partition leases: {}", e.message);
                return None;
            }
        };
        self.apply_leases(&round);
        self.note_writer_lease(&round);
        // A lease-less partition's feeds wait while the node reaches a majority, and end
        // once it doesn't (feed1248's F2).
        self.grants.clock().set_majority(round.majority());
        Some(round)
    }

    /// A round soon, for a map a peer pushed (it may give a partition its lease), rather
    /// than at the next renewal: one at a time per node, and a push that arrives during one
    /// runs one more after it, never dropped.
    pub(crate) fn confirm_soon(&self) {
        // Only for a partition a round could lease: not a copy the map moved away, which
        // would otherwise run a round on every push (audit120's nit on #1899).
        let Ok(map) = self.get_map() else { return };
        if self.fences.confirmable(&map, &self.id).is_empty() {
            return;
        }
        let Some(node) = self.me.upgrade() else {
            return;
        };
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            eprintln!(
                "phdb serve: a pushed map can't start a confirmation round here (no runtime); \
                 the start's retries confirm the partitions instead"
            );
            return;
        };
        {
            let mut r = self.fences.rounds();
            if r.running {
                r.again = true;
                return;
            }
            r.running = true;
        }
        rt.spawn(async move {
            loop {
                node.confirm_round().await;
                let mut r = node.fences.rounds();
                if r.again {
                    r.again = false;
                } else {
                    r.running = false;
                    return;
                }
            }
        });
    }

    /// The node's lease renewals (D326), for as long as it runs: a round every third of a
    /// lease, timed from the last round's start so a slow round doesn't let a lease run out;
    /// sooner, backing off from [`RETRY_FIRST`], while a partition its map names here has no
    /// lease (at the start, or after it opens while the node serves: a move's new owner).
    /// One loop per node, started once its partitions are open; it holds the node only
    /// while a round runs, and ends once the node is gone.
    pub(crate) fn renew_leases(self: &Arc<Self>) {
        // The node's device ceiling's readings start with its lease renewals (D332), and so
        // does its range rows' pruning (D331 PR 5).
        self.watch_device();
        self.watch_range_prunes();
        if self
            .fences
            .retrying
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            self.fences
                .retrying
                .store(false, std::sync::atomic::Ordering::Release);
            // Said, not silent (simp724's nit on D331 PR 2): a partition that asked for a
            // lease here would otherwise hold it only until its first grant runs out.
            eprintln!(
                "phdb serve: the lease renewals can't start here (no runtime); the next \
                 call from the node's runtime starts them"
            );
            return;
        };
        let me = Arc::downgrade(self);
        let period = self.grants.lease() / 3;
        rt.spawn(async move {
            let mut wait = RETRY_FIRST;
            loop {
                let Some(node) = me.upgrade() else { return };
                let started = tokio::time::Instant::now();
                let asked = node.fences.asked.load(std::sync::atomic::Ordering::Acquire);
                let missing = match node.get_map() {
                    Ok(map)
                        if asked
                            || !node.fences.askable(&map, &node.id).is_empty()
                            || node.writer_term_to_ask(&map).is_some() =>
                    {
                        match node.confirm_round().await {
                            Some(r) => !node.fences.confirmable(&r.map, &node.id).is_empty(),
                            None => true,
                        }
                    }
                    _ => false,
                };
                // An election of the map's writer, then failovers, if due (D330), off this
                // task.
                node.spawn_duties();
                drop(node);
                let next = if missing {
                    let w = wait.min(period);
                    wait = (wait * 2).min(RETRY_MAX);
                    started + w
                } else {
                    wait = RETRY_FIRST;
                    started + period
                };
                // Or at once for a partition entering its fallback (D335 PR 2b). Its
                // `notify_one` keeps a permit, so one that came during the round isn't lost.
                let kick = match me.upgrade() {
                    Some(node) => Arc::clone(&node.fences.kick),
                    None => return,
                };
                tokio::select! {
                    () = tokio::time::sleep_until(next) => {}
                    () = kick.notified() => {}
                }
            }
        });
    }
}

/// A fault seam (tests): a node in a listed cluster directory sends its releases to no
/// peer (its own grants still drop them), as if every one were lost: a planned move's new
/// node then waits out the old node's grants.
#[cfg(test)]
pub(crate) static DROP_RELEASES: Mutex<Vec<std::path::PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before D326 (1.2), by cluster directory:
/// their `Cluster.Lease` and `Cluster.Release` answer `Unimplemented`, and their `Status`
/// leaves `leases` out (#2047).
#[cfg(test)]
pub(crate) static PRE_LEASE: Mutex<Vec<std::path::PathBuf>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn pre_lease(dir: &Path) -> bool {
    PRE_LEASE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|d| d == dir)
}

/// A fault seam (tests): nodes whose `Cluster.Lease` and `Release` answer `Unimplemented`
/// while their `Status` still reports `leases`: a 1.3 node behind a proxy that doesn't
/// route the calls (audit120's S1 on #2048).
#[cfg(test)]
pub(crate) static LEASE_UNROUTED: Mutex<Vec<std::path::PathBuf>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn lease_unrouted(dir: &Path) -> bool {
    LEASE_UNROUTED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|d| d == dir)
}

#[cfg(test)]
fn drop_releases(dir: &Path) -> bool {
    DROP_RELEASES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|d| d == dir)
}

pub(super) fn not_here(id: &str) -> ApiError {
    ApiError::partition_unavailable(format!("partition {id:?} isn't open on this node"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_starts_retries_wait_only_for_partitions_the_map_still_names_here() {
        // audit120's S1 on #1897: an unconfirmed partition the node's own map no longer names
        // at its file's epoch (its copy was moved away) can't be confirmed again, so
        // `confirm_until_done` doesn't keep asking the peers for it.
        let dir = pigeonhole::doc_support::temp_dir();
        let n = super::super::tests::node(&dir, "a");
        let map = |node: &str, epoch: u64| ClusterMap {
            map_version: 2,
            partitions: vec![crate::serve::proto::Partition {
                id: "p1".into(),
                node: node.into(),
                epoch,
                ..Default::default()
            }],
            ..Default::default()
        };
        n.serve_gate("p1", &n.new_gate("p1"), 1);
        assert_eq!(n.fences.confirmable(&map("a", 1), "a"), ["p1"]);
        assert!(
            n.fences.confirmable(&map("b", 2), "a").is_empty(),
            "moved away"
        );
        assert!(
            n.fences.confirmable(&map("a", 2), "a").is_empty(),
            "another epoch"
        );
    }

    #[test]
    fn fenced_json_round_trips_and_a_missing_file_is_none() {
        let dir = pigeonhole::doc_support::temp_dir();
        assert!(read_fenced(&dir).unwrap().is_empty());
        let set: BTreeSet<String> = ["p2".to_owned(), "p1".to_owned()].into();
        write_fenced(&dir, &set).unwrap();
        assert_eq!(read_fenced(&dir).unwrap(), set);
        assert_eq!(
            std::fs::read_to_string(dir.join(FENCED_FILE)).unwrap(),
            r#"{"fenced":["p1","p2"]}"#
        );
        assert!(!dir.join(format!("{FENCED_FILE}.tmp")).exists());
        assert!(parse_fenced(r#"{"fenced":[1]}"#).is_err());
        assert!(parse_fenced("{}").is_err());
    }
}
