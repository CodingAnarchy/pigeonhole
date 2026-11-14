//! Fencing a cluster node's partitions (D317 ticket 6, rule 3): each partition served here
//! has a write gate ([`WriteGate`]), which `phdb cluster fence` and `unfence` set and
//! lift, and which a node's start sets until a peer's map confirms the partition is still
//! this node's (auto-unfence).
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
use super::proto::{ClusterMap, FenceReason, LocalPartitionState};
use crate::serve::errors::ApiError;
use crate::serve::gate::{Fence, WriteGate};

/// The operator's fences, in a node's directory.
pub(crate) const FENCED_FILE: &str = "fenced.json";

/// How long `Fence` waits for the writes admitted before it to finish. Past it, the
/// partition stays fenced and the answer says how many still run: `fence` again waits again.
const DRAIN_WAIT: Duration = Duration::from_secs(30);

/// How often a node still waiting to unfence a start-fenced partition asks its peers for
/// their maps again (cli692's S3): the first wait, doubled up to the last.
const RETRY_FIRST: Duration = Duration::from_millis(250);
const RETRY_MAX: Duration = Duration::from_secs(5);

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

/// A node's fences.
#[derive(Debug, Default)]
pub(crate) struct Fences {
    served: Mutex<BTreeMap<String, Served>>,
    /// The operator's fences as `fenced.json` has them, partitions not open here included.
    operator: Mutex<BTreeSet<String>>,
    /// One fence, unfence or close at a time, so the file follows the flags in order.
    pub(super) changing: tokio::sync::Mutex<()>,
    /// Partitions closed here ([`Node::close`]), with their files once hashed.
    pub(super) closed: Mutex<BTreeMap<String, super::close::Closed>>,
    /// Partitions being opened while the node serves ([`Node::open_part`]): `Status` shows
    /// them opening and doesn't read their files (#1770).
    pub(super) opening: Mutex<BTreeSet<String>>,
    /// [`Node::confirm_soon`]'s rounds: one running, and whether another is owed.
    rounds: Mutex<Rounds>,
    /// [`Node::confirm_until_done`]'s loop is running.
    retrying: std::sync::atomic::AtomicBool,
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
    /// Peers holding a different map at the same version (two forced maps): nothing is
    /// confirmed while there's one.
    pub(crate) divergent: Vec<String>,
}

impl Round {
    /// Whether a majority of `map`'s nodes hold it, with no divergent map among them.
    fn majority(&self) -> bool {
        self.divergent.is_empty() && self.holders * 2 > self.map.nodes.len()
    }

    /// Whether it confirms partition `id` at `epoch` on node `node`: a majority hold the
    /// map, and the map names `node` for it there (D317 rule 3, as amended for #1760).
    pub(crate) fn confirms(&self, node: &str, id: &str, epoch: u64) -> bool {
        self.majority() && names(&self.map, node, id, epoch)
    }
}

impl Fences {
    fn served(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Served>> {
        self.served.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn rounds(&self) -> std::sync::MutexGuard<'_, Rounds> {
        self.rounds.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Partitions open here that await their peers' confirmation, by id.
    fn unconfirmed_any(&self) -> Vec<String> {
        self.served()
            .iter()
            .filter(|(_, s)| s.gate.awaiting_peers())
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub(super) fn operator(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        self.operator.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn closed(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<String, super::close::Closed>> {
        self.closed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether partition `id` is being opened here.
    pub(super) fn is_opening(&self, id: &str) -> bool {
        self.opening
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(id)
    }

    /// Partition `id` no longer served here: its gate goes with it.
    pub(super) fn forget(&self, id: &str) {
        self.served().remove(id);
    }

    pub(super) fn gate(&self, id: &str) -> Option<(Arc<WriteGate>, u64)> {
        self.served()
            .get(id)
            .map(|s| (Arc::clone(&s.gate), s.epoch))
    }

    /// Partitions a peer's map can still confirm: unconfirmed since they opened here
    /// (operator-fenced or not, #1829), and named by `map`, this node's (`node`), at their
    /// file's epoch. One the map no longer names there can never be confirmed (a map's epochs
    /// only rise), so the start's retries don't wait for it (audit120's S1 on #1897: a move's
    /// old node, restarted with its copy fenced, would otherwise ask its peers for ever).
    pub(crate) fn confirmable(&self, map: &ClusterMap, node: &str) -> Vec<String> {
        self.served()
            .iter()
            .filter(|(id, s)| s.gate.awaiting_peers() && names(map, node, id, s.epoch))
            .map(|(id, _)| id.clone())
            .collect()
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

    /// Partition `id`'s gate, at its file's `epoch`, as it opens here: unconfirmed until
    /// the peers' maps confirm it (rule 3: a node that was down may have missed the
    /// partition's move), and fenced by the operator as well if `fenced.json` lists it. The
    /// two are separate flags (#1829), so an operator's fence doesn't hide the wait.
    pub(crate) fn open_gate(&self, id: &str, epoch: u64) -> Arc<WriteGate> {
        let operator = self.fences.operator().contains(id);
        let gate = WriteGate::opened(id, operator);
        self.fences.served().insert(
            id.to_owned(),
            Served {
                gate: Arc::clone(&gate),
                epoch,
            },
        );
        gate
    }

    /// Partition `id`'s state here, and why it's fenced when it is, for `Status`: `None`
    /// when it isn't open here.
    pub(crate) fn part_state(&self, id: &str) -> Option<(LocalPartitionState, FenceReason)> {
        let (gate, _) = self.fences.gate(id)?;
        Some(match gate.fenced() {
            Some(Fence::Operator) => (LocalPartitionState::Fenced, FenceReason::Operator),
            Some(Fence::Unconfirmed) => (LocalPartitionState::Fenced, FenceReason::AwaitingPeerMap),
            None => (LocalPartitionState::Serving, FenceReason::Unspecified),
        })
    }

    /// Partition `id`'s state as `Status` reports it, for `Admin.Info` (#1852): open
    /// ([`part_state`](Self::part_state)), else closed, else being opened. Unspecified for one
    /// the node hasn't opened (no file, or a failed open).
    pub(crate) fn local_state(&self, id: &str) -> (LocalPartitionState, FenceReason) {
        if let Some(state) = self.part_state(id) {
            state
        } else if self.fences.closed().contains_key(id) {
            (LocalPartitionState::Closed, FenceReason::Unspecified)
        } else if self.fences.is_opening(id) {
            (LocalPartitionState::Opening, FenceReason::Unspecified)
        } else {
            (LocalPartitionState::Unspecified, FenceReason::Unspecified)
        }
    }

    /// Why partition `id` is fenced here, for the startup line and the log.
    pub(crate) fn fence_of(&self, id: &str) -> Option<Fence> {
        self.fences.gate(id).and_then(|(g, _)| g.fenced())
    }

    /// Whether partition `id`, open here, awaits its peers' confirmation, whatever the
    /// operator's fence (`Status`'s `awaiting_peers`, #1829).
    pub(crate) fn awaiting_peers(&self, id: &str) -> bool {
        // Not for a copy the node's map no longer names at its epoch: no peer can confirm
        // that one (audit120's note on #1897).
        self.fences.gate(id).is_some_and(|(g, epoch)| {
            g.awaiting_peers() && self.get_map().is_ok_and(|m| names(&m, &self.id, id, epoch))
        })
    }

    /// `Cluster.Fence`: from now, P's writes answer `Fenced`; answers once every write
    /// admitted before has finished (within [`DRAIN_WAIT`]). Persisted, so it survives a
    /// restart. Fencing a fenced partition waits again.
    pub(crate) async fn fence(&self, id: &str) -> Result<(), ApiError> {
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
        })
    }

    /// `Cluster.Unfence`: P takes writes again. Refused unless this node's map names it for
    /// P at its file's epoch (a write would break rule 1 or 2 anyway), and unless a peer's
    /// map, at least as new, says the same (or `force`): this node's map alone may be
    /// behind. P first takes the map's schema it skipped while fenced.
    pub(crate) async fn unfence(self: &Arc<Self>, id: &str, force: bool) -> Result<(), ApiError> {
        let _one = self.fences.changing.lock().await;
        // The checks first, at the epoch of the open partition or of the closed one's file:
        // a refused unfence leaves a closed partition closed, its hashes kept (simp724's N1
        // on #1826).
        let closed = self.fences.closed().get(id).map(|c| c.epoch);
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
            if !round.confirms(&self.id, id, epoch) {
                let since = if unconfirmed {
                    "fenced, and unconfirmed since it opened here,"
                } else {
                    "fenced"
                };
                let why = if !round.divergent.is_empty() {
                    format!(
                        "node{} {} hold{} a different map at version {}",
                        if round.divergent.len() > 1 { "s" } else { "" },
                        round.divergent.join(", "),
                        if round.divergent.len() > 1 { "" } else { "s" },
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
                     peers to answer, or pass --force only if they can't and you know the \
                     partition hasn't moved"
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
        eprintln!("phdb serve: partition {id} unfenced: it takes writes");
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
    pub(crate) async fn round(self: &Arc<Self>) -> Result<Round, ApiError> {
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
        if let Some((peer, _, newest)) = answers.iter().max_by_key(|(_, _, m)| m.map_version)
            && newest.map_version > self.state().version()
            && let Err(e) = self
                .push_map_async(newest.clone(), Some(peer.clone()))
                .await
        {
            eprintln!("phdb serve: node {peer}'s newer cluster map: {}", e.message);
        }
        let map = self.get_map()?;
        let json = super::map::canonical_json(&map);
        let mut holders = 1;
        let mut divergent = Vec::new();
        let mut pushes = tokio::task::JoinSet::new();
        for (peer, address, theirs) in &answers {
            match theirs.map_version.cmp(&map.map_version) {
                std::cmp::Ordering::Equal if super::map::canonical_json(theirs) == json => {
                    holders += 1;
                }
                std::cmp::Ordering::Equal => divergent.push(peer.clone()),
                std::cmp::Ordering::Less => {
                    let (node, address, map) = (Arc::clone(self), address.clone(), map.clone());
                    pushes.spawn(async move {
                        tokio::time::timeout(node.peers.push_timeout, node.push_to(&address, map))
                            .await
                            .is_ok_and(|r| r.is_ok())
                    });
                }
                // Newer than the map this node now holds: its install was refused (a forced
                // map from another history, say). It doesn't hold this node's map.
                std::cmp::Ordering::Greater => {}
            }
        }
        holders += pushes.join_all().await.into_iter().filter(|ok| *ok).count();
        Ok(Round {
            map,
            holders,
            answered: answers.len(),
            divergent,
        })
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

    /// A round, then every unconfirmed partition it confirms: one its map names here at the
    /// file's epoch, with a majority of the map's nodes holding that map. An operator's
    /// fence stays (#1829).
    async fn confirm_round(self: &Arc<Self>) -> Option<Round> {
        let round = match self.round().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("phdb serve: confirming partitions: {}", e.message);
                return None;
            }
        };
        let served = self.fences.served();
        for (id, s) in served.iter() {
            if s.gate.awaiting_peers() && round.confirms(&self.id, id, s.epoch) && s.gate.confirm()
            {
                if s.gate.fenced().is_none() {
                    eprintln!(
                        "phdb serve: partition {id} unfenced: {} of {} nodes hold map version \
                         {}, which still places it on this node",
                        round.holders,
                        round.map.nodes.len(),
                        round.map.map_version
                    );
                } else {
                    eprintln!(
                        "phdb serve: partition {id} confirmed by {} of {} nodes at map version \
                         {}, and still fenced by the operator (phdb cluster unfence lifts it)",
                        round.holders,
                        round.map.nodes.len(),
                        round.map.map_version
                    );
                }
            }
        }
        drop(served);
        Some(round)
    }

    /// A round soon, for a map a peer pushed (it may confirm a partition): one at a time
    /// per node, and a push that arrives during one runs one more after it, never dropped.
    pub(crate) fn confirm_soon(&self) {
        if self.fences.unconfirmed_any().is_empty() {
            return;
        }
        let (Some(node), Ok(rt)) = (self.me.upgrade(), tokio::runtime::Handle::try_current())
        else {
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

    /// Rounds, backing off, while a partition the node's map names here at its file's epoch
    /// is unconfirmed: at the start, and after a partition opens while the node serves (a
    /// move's new owner, which serves once a majority holds the move's map). One such loop
    /// at a time per node; a cluster of one confirms in its first round.
    pub(crate) async fn confirm_until_done(self: &Arc<Self>) {
        if self
            .fences
            .retrying
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let mut wait = RETRY_FIRST;
        loop {
            let done = match self.confirm_round().await {
                Some(r) => self.fences.confirmable(&r.map, &self.id).is_empty(),
                None => self.get_map().is_err(),
            };
            if done {
                self.fences
                    .retrying
                    .store(false, std::sync::atomic::Ordering::Release);
                // A partition that opened while this loop was ending: go again.
                let Ok(map) = self.get_map() else { return };
                if self.fences.confirmable(&map, &self.id).is_empty()
                    || self
                        .fences
                        .retrying
                        .swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    return;
                }
                wait = RETRY_FIRST;
                continue;
            }
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(RETRY_MAX);
        }
    }
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
        let _gate = n.open_gate("p1", 1);
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
