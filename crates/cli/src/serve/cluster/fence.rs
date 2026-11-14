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
}

impl Fences {
    fn served(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Served>> {
        self.served.lock().unwrap_or_else(PoisonError::into_inner)
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

    /// Partitions still fenced since the node started.
    pub(crate) fn start_fenced(&self) -> Vec<String> {
        self.served()
            .iter()
            .filter(|(_, s)| s.gate.fenced() == Some(Fence::Start))
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

    /// Partition `id`'s gate, at its file's `epoch`, as it opens here: fenced, by the
    /// operator if `fenced.json` lists it, else until a peer's map confirms it (rule 3: a
    /// node that was down may have missed the partition's move).
    pub(crate) fn open_gate(&self, id: &str, epoch: u64) -> Arc<WriteGate> {
        let why = if self.fences.operator().contains(id) {
            Fence::Operator
        } else {
            Fence::Start
        };
        let gate = WriteGate::new(id, Some(why));
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
            Some(Fence::Start) => (LocalPartitionState::Fenced, FenceReason::AwaitingPeerMap),
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
        let (epoch, start_fenced) = match closed {
            Some(epoch) => (epoch, false),
            None => {
                let (gate, epoch) = self.fences.gate(id).ok_or_else(|| not_here(id))?;
                (epoch, gate.fenced() == Some(Fence::Start))
            }
        };
        let map = self.get_map()?;
        // This node's own map may be stale, whichever fence it is: it was down or missed a
        // push, and a move may have passed it by. So its peers' newest map must confirm the
        // partition first, as at start, unless the operator forces it (cli692's S1 on
        // #1755). An operator's fence needs that as much as a start fence does (audit120's
        // P2-F1): a move fences its old node as the operator, and that node is the one whose
        // map is behind when it misses the move's map.
        if !force && !self.peer_confirms(&map, id, epoch).await {
            let since = if start_fenced {
                "fenced since this node started"
            } else {
                "fenced"
            };
            return Err(ApiError::failed_precondition(format!(
                "partition {id:?} is {since}, and no peer's map confirms it's still this \
                 node's at epoch {epoch}: the newest map among the peers that answer must be \
                 at least as new as this node's and name it. A peer that holds a newer map \
                 may have moved the partition, and unfencing would give it two writers. \
                 Wait for a peer to answer, or pass --force only if no peer can and you \
                 know the partition hasn't moved"
            )));
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

    /// Auto-unfence (rule 3, cli692's S3): `map`, a peer's (or this node's own, in a
    /// cluster of one), confirms every start-fenced partition it names for this node at the
    /// partition's file's epoch, if it's at least as new as this node's map. Any later map
    /// that does counts: pushed, pulled, or fetched at start.
    pub(crate) fn confirm(&self, map: &ClusterMap) {
        if map.map_version < self.state().version() {
            return;
        }
        let served = self.fences.served();
        for (id, s) in served.iter() {
            if s.gate.fenced() == Some(Fence::Start)
                && names(map, &self.id, id, s.epoch)
                && s.gate.unfence_start()
            {
                eprintln!(
                    "phdb serve: partition {id} unfenced: map version {} still places it on \
                     this node",
                    map.map_version
                );
            }
        }
    }

    /// Whether partition `id` at `epoch` is still this node's by its peers' word: its own
    /// map, in a cluster of one; else the NEWEST map among the peers that answer, which
    /// must be at least as new as `map` and name this node. Every peer is asked, at once:
    /// the first one to agree isn't enough, since a peer that missed the same move holds the
    /// same stale map (simp724's S1 on #1826). The newest map is taken here if it's newer.
    /// A peer that doesn't answer can't be counted: with the one peer that knows of a move
    /// down, the others still confirm (a quorum is for 1.3.0, ICR 0117).
    async fn peer_confirms(self: &Arc<Self>, map: &ClusterMap, id: &str, epoch: u64) -> bool {
        if map.nodes.iter().all(|n| n.id == self.id) {
            return names(map, &self.id, id, epoch);
        }
        let mut asks = tokio::task::JoinSet::new();
        for n in map.nodes.iter().filter(|n| n.id != self.id) {
            let (node, peer, address) = (Arc::clone(self), n.id.clone(), n.address.clone());
            asks.spawn(async move {
                tokio::time::timeout(node.peers.push_timeout, node.fetch_map(&address))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .map(|theirs| (peer, theirs))
            });
        }
        let mut newest: Option<(String, ClusterMap)> = None;
        while let Some(answer) = asks.join_next().await {
            if let Ok(Some((peer, theirs))) = answer
                && newest
                    .as_ref()
                    .is_none_or(|(_, n)| theirs.map_version > n.map_version)
            {
                newest = Some((peer, theirs));
            }
        }
        let Some((peer, theirs)) = newest else {
            return false;
        };
        if theirs.map_version > self.state().version() {
            let _ = self.push_map_async(theirs.clone(), Some(peer)).await;
        }
        theirs.map_version >= map.map_version && names(&theirs, &self.id, id, epoch)
    }

    /// At start, after [`Node::sync_on_start`]'s round: while a partition is still fenced
    /// since the start, asks the peers for their maps again, backing off, until one
    /// confirms it. A cluster of one confirms on its own map.
    pub(crate) async fn confirm_after_start(self: &Arc<Self>) {
        let mut wait = RETRY_FIRST;
        loop {
            let Ok(map) = self.get_map() else {
                return;
            };
            if map.nodes.iter().all(|n| n.id == self.id) {
                self.confirm(&map);
            }
            if self.fences.start_fenced().is_empty() {
                return;
            }
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(RETRY_MAX);
            for n in map.nodes.iter().filter(|n| n.id != self.id) {
                let fetched =
                    tokio::time::timeout(self.peers.push_timeout, self.fetch_map(&n.address)).await;
                let Ok(Ok(theirs)) = fetched else {
                    continue;
                };
                if theirs.map_version > self.state().version()
                    && let Err(e) = self
                        .push_map_async(theirs.clone(), Some(n.id.clone()))
                        .await
                {
                    eprintln!(
                        "phdb serve: node {}'s newer cluster map: {}",
                        n.id, e.message
                    );
                }
                self.confirm(&theirs);
            }
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
