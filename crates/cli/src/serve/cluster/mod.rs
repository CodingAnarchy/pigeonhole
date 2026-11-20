//! `phdb serve --cluster DIR --node-id ID` (D317, ICR 0117): a node of a cluster of
//! statically partitioned databases. So far (ticket 1) the node keeps the partition map and
//! distributes it: `Cluster.SetMap` validates, persists and pushes a new map, nodes take a
//! newer map pushed (or pulled) from a peer, and `Cluster.Status` reports the node's map and
//! its partition files. Routing data calls to partitions is ticket 2.
//!
//! **One server per DIR:** `DIR/.lock` is held exclusively while the node runs, and
//! `phdb cluster init-partition` takes it too, so nothing else rewrites the map file or a
//! partition's identity under it.

mod backup;
pub(crate) mod balance;
pub(crate) mod balancing;
mod bulkfan;
pub(crate) mod cli;
pub(crate) mod close;
pub(crate) mod failover;
pub(crate) mod fanout;
pub(crate) mod feed;
pub(crate) mod fence;
pub(crate) mod forward;
pub(crate) mod hub;
pub(crate) mod identity;
mod indexfan;
pub(crate) mod lease;
pub(crate) mod load;
pub(crate) mod map;
mod memfan;
pub(crate) mod memwatch;
mod moves;
pub(crate) mod moving;
pub(crate) mod open;
pub(crate) mod position;
pub(crate) mod promote;
pub(crate) mod range_copy;
pub(crate) mod range_feed;
pub(crate) mod range_follow;
mod restore;
pub(crate) mod route;
pub(crate) mod schema;
pub(crate) mod spanning;
pub(crate) mod stamp;
#[cfg(test)]
mod tests;
pub(crate) mod token;
mod transfer;
pub(crate) mod upload;
pub(crate) mod writer;

mod api;
mod features;
mod install;
mod peers;
#[cfg(test)]
mod seams;
mod start;
mod status;

pub(crate) use api::*;
pub(crate) use features::*;
use install::*;
#[cfg(test)]
pub(crate) use seams::*;
pub(crate) use start::*;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use pigeonhole::ErrorCode;
use tonic::{Request, Status};

use super::auth::{Caller, Scope, Tokens};
use super::errors::ApiError;
use super::proto::{
    self, ClusterMap, GetMapRequest, LocalPartition, NodePush, NodeStatus, PushMapRequest,
    PushMapResponse, SetMapRequest, SetMapResponse, StatusRequest,
};
use super::{ServeArgs, config, tls};
use crate::ExitStatus;
use crate::exit::Failure;

/// The map file in a node's directory.
pub(crate) const MAP_FILE: &str = "cluster-map.json";
/// The highest writer term this node voted for (D330), in decimal: written and synced before
/// the vote is answered, so the term fence survives a restart (D330 amendment 1's M1).
pub(crate) const WRITER_TERM_FILE: &str = "cluster-writer-term";

/// The term in `dir`'s [`WRITER_TERM_FILE`]: 0 without one.
pub(crate) fn read_voted(dir: &Path) -> Result<u64, String> {
    match std::fs::read_to_string(dir.join(WRITER_TERM_FILE)) {
        Ok(s) => s
            .trim()
            .parse()
            .map_err(|e| format!("{WRITER_TERM_FILE} isn't a writer term: {e}")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(format!("read {WRITER_TERM_FILE}: {e}")),
    }
}

/// The lock file that keeps a directory to one server (or one `init-partition`).
pub(crate) const LOCK_FILE: &str = ".lock";
/// The header a node's calls to its peers carry its map version in.
const MAP_VERSION_HEADER: &str = "x-pigeonhole-map-version";

/// The header a node's calls to its peers carry its id in, so a pull knows whom to ask.
pub(crate) const NODE_ID_HEADER: &str = "x-pigeonhole-node-id";

/// Takes `dir`'s lock, or says another server or command has it.
pub(crate) fn lock_dir(dir: &Path) -> Result<std::fs::File, Failure> {
    let path = dir.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| Failure::io(&format!("open {}", path.display()), &e))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(Failure::usage(&format!(
            "another phdb serve or phdb cluster command has {}",
            dir.display()
        ))),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(Failure::io(&format!("lock {}", path.display()), &e))
        }
    }
}

/// Reads the map file in `dir`, if there is one.
pub(crate) fn read_map(dir: &Path) -> Result<Option<ClusterMap>, String> {
    let path = dir.join(MAP_FILE);
    match std::fs::read_to_string(&path) {
        Ok(json) => map::parse_json(&json)
            .map(Some)
            .map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("read {}: {e}", path.display())),
    }
}

/// Writes `json` as `dir`'s map file, so a crash leaves the old map or the new one, never a
/// torn file.
fn write_map(dir: &Path, json: &str) -> std::io::Result<()> {
    write_synced(dir, MAP_FILE, json.as_bytes())
}

/// `d` in whole milliseconds, for a proto field.
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Writes `bytes` as `dir`'s file `name`: a temporary file (`name.tmp`), synced, renamed
/// over the old one, then the directory synced, so a crash leaves the old file or the new
/// one whole.
pub(super) fn write_synced(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// What a node holds, behind one lock (never held across an await).
#[derive(Debug, Default)]
struct MapState {
    /// The newest map, in canonical form, and its canonical JSON.
    map: Option<(ClusterMap, String)>,
    /// Peers seen with this node's version but a different map (cli692's M3).
    divergent: BTreeSet<String>,
    /// Versions a pull was started for: once each.
    pulled: HashSet<u64>,
}

impl MapState {
    fn version(&self) -> u64 {
        self.map.as_ref().map_or(0, |(m, _)| m.map_version)
    }
}

/// How this node reaches its peers.
#[derive(Debug, Clone)]
struct Peers {
    /// The bearer token it presents (`cluster` scope there).
    token: Option<String>,
    /// `https://` unless the node runs without TLS.
    scheme: &'static str,
    ca: Option<tonic::transport::Certificate>,
    push_timeout: Duration,
    /// `--cluster-lease-ms` (D326): how long a lease this node grants runs, on its clock.
    lease: Duration,
    /// `--cluster-failover-after-ms` (D330): the margin a writer goes without its lease
    /// before an election, 2·L by default.
    failover_after: Duration,
    /// `--cluster-failover-max-lag-ms` (D330 PR 2): none for 0.
    failover_max_lag: Option<Duration>,
    /// `--balance-window` (D332): the load averages' and the device ceiling's window.
    balance_window: Duration,
    /// How often a partition's load averages take a reading ([`load::INTERVAL`]; shorter in
    /// tests).
    load_interval: Duration,
}

/// A cluster node.
#[derive(Debug)]
pub(crate) struct Node {
    dir: PathBuf,
    id: String,
    /// The node dials its peers without TLS, so it dials only loopback ones (cli692's M2
    /// and R3): otherwise its cluster token would cross a network in the clear.
    loopback_only: bool,
    tokens: Tokens,
    peers: Peers,
    /// Held briefly, never across an fsync or an await.
    state: Mutex<MapState>,
    /// Serializes installs (check, persist, publish), so the file and `state` take maps in
    /// one order; held only on blocking threads (cli692's R2).
    installing: Mutex<()>,
    /// The routes of the newest map, swapped whole with it: read on every routed call
    /// without a lock (D317 ticket 2; `load()`'s guard, never `load_full()`, perf287).
    routes: arc_swap::ArcSwapOption<route::Routes>,
    /// One awaited map pull at a time ([`Node::pull_now`]).
    pulling: tokio::sync::Mutex<()>,
    /// Each partition's last reconcile, by id: the map version, and its outcome, for
    /// `Status`.
    reconciled: Mutex<std::collections::BTreeMap<String, (u64, schema::Outcome)>>,
    /// The newest map version reconciled here, written under `installing`: an older map is
    /// never reconciled after it, so a stale one can't drop a table a newer one created
    /// (audit120's F6).
    reconciled_newest: std::sync::atomic::AtomicU64,
    /// Peers' features as they last answered `Status` (`map_features`), by node id, with
    /// when (`None`: it didn't answer): asked again after [`PEER_FEATURES_TTL`], or
    /// [`PEER_DOWN_TTL`] for no answer ([`Node::owners_keep`]).
    peer_features: Mutex<HashMap<String, PeerFeatures>>,
    /// Whether this node has taken its peers' newer maps since it started (cleared by
    /// [`run`] for a node that restarts with a map, set by [`Node::sync_on_start`]). The
    /// map's writer changes the map only once it has (D317 amendment 1): a writer back from
    /// a forced recovery finds the forced map first, which no longer names it.
    synced: std::sync::atomic::AtomicBool,
    /// One schema change at a time at the map's writer ([`schema`]'s `change_map`), so
    /// concurrent ones queue rather than retry each other's compare-and-set (D317
    /// amendment 1). Held across the set and its push, never inside an install.
    pub(crate) schema_changes: tokio::sync::Mutex<()>,
    /// The partitions served here: their write gates, and the operator's fences (D317
    /// ticket 6).
    fences: fence::Fences,
    /// The server this node serves through, set once it's built ([`open::attach`]): how the
    /// node reaches its partitions, to reconcile their schema on every install before the
    /// map's routes serve (D317 ticket 5) and to open and close them while it serves
    /// (ticket 6).
    server: std::sync::OnceLock<open::Server>,
    /// One open or close of a partition at a time (D317 ticket 6): the parts' swaps take
    /// it. Taken after `fences.changing` where both are.
    opening: tokio::sync::Mutex<()>,
    /// This node, for the tasks an install starts.
    me: std::sync::Weak<Node>,
    /// Its cluster feed hubs (#1798): one merge per subscription key.
    pub(crate) feed_hubs: hub::Hubs,
    /// The leases this node grants, and its lease clock (D326).
    grants: lease::Grants,
    /// When this writer first saw a majority of both the nodes before and the nodes after
    /// a node change hold it, by the change's map version, on its lease clock: a further
    /// change waits a lease past that (audit120's L1 on #1899's F4). In memory: a writer
    /// that restarts, or another that takes over, checks again and waits from its own
    /// check.
    joint_seen: Mutex<Option<(u64, u64)>>,
    /// This node's writer lease (D330), as its last round that granted it gave it: the term,
    /// and when it ends on the lease clock (`(0, 0)`: none).
    writer_lease: Mutex<(u64, u64)>,
    /// The term this node was voted writer at, until its elected map lands (0: none): its
    /// rounds renew that vote meanwhile.
    candidate: std::sync::atomic::AtomicU64,
    /// When this node first found an election due ([`Node::maybe_elect`]): it waits a margin
    /// per smaller live id from there.
    elect_since: Mutex<Option<tokio::time::Instant>>,
    /// Whether the writer's duties run ([`Node::spawn_duties`]): set (Acquire/Release swap)
    /// by the tick that claims them, cleared (Release) by their task's guard on every exit.
    /// Serve's shared state isn't in concurrency.md's inventory (engine and runtime only).
    duties: std::sync::atomic::AtomicBool,
    /// The writer's failover state (D330 PR 2): when it took office, the last survey, the
    /// promote in progress.
    failovers: failover::Failovers,
    /// The writer's watch for nodes a lost election stranded (#2073).
    stranded: Mutex<writer::Stranded>,
    /// D332's balancer, while this node is the map's writer.
    balancing: Mutex<balancing::Run>,
    /// The node's device ceiling (D332): the highest one-second rate of its partitions'
    /// device bytes within `--balance-window`, fed by [`Node::watch_device`].
    device_ceiling: load::DeviceCeiling,
    /// Whether [`Node::watch_device`]'s loop runs.
    watching_device: std::sync::atomic::AtomicBool,
}

impl Node {
    fn new(
        dir: &Path,
        id: &str,
        loopback_only: bool,
        tokens: Tokens,
        peers: Peers,
        state: MapState,
        voted: u64,
    ) -> Arc<Self> {
        let routes = state
            .map
            .as_ref()
            .map(|(m, _)| Arc::new(route::Routes::new(m)));
        let grants = lease::Grants::new(Arc::default(), peers.lease).with_voted(voted);
        let device_ceiling = load::DeviceCeiling::new(peers.balance_window);
        Arc::new_cyclic(|me| Self {
            device_ceiling,
            watching_device: std::sync::atomic::AtomicBool::new(false),
            grants,
            joint_seen: Mutex::new(None),
            writer_lease: Mutex::new((0, 0)),
            candidate: std::sync::atomic::AtomicU64::new(0),
            elect_since: Mutex::new(None),
            duties: std::sync::atomic::AtomicBool::new(false),
            failovers: failover::Failovers::default(),
            stranded: Mutex::new(writer::Stranded::default()),
            balancing: Mutex::new(balancing::Run::default()),
            dir: dir.to_path_buf(),
            id: id.to_owned(),
            loopback_only,
            tokens,
            peers,
            state: Mutex::new(state),
            installing: Mutex::new(()),
            routes: arc_swap::ArcSwapOption::new(routes),
            pulling: tokio::sync::Mutex::new(()),
            reconciled: Mutex::new(std::collections::BTreeMap::new()),
            reconciled_newest: std::sync::atomic::AtomicU64::new(0),
            peer_features: Mutex::new(HashMap::new()),
            synced: std::sync::atomic::AtomicBool::new(true),
            schema_changes: tokio::sync::Mutex::new(()),
            fences: fence::Fences::default(),
            server: std::sync::OnceLock::new(),
            opening: tokio::sync::Mutex::new(()),
            feed_hubs: hub::Hubs::default(),
            me: me.clone(),
        })
    }

    /// The newest map's routes, `None` before the node has a map. A guard, not a clone of
    /// the `Arc`: read on every routed call.
    pub(crate) fn routes(&self) -> arc_swap::Guard<Option<Arc<route::Routes>>> {
        self.routes.load()
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    /// The node's cluster directory (`--cluster`).
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    fn state(&self) -> std::sync::MutexGuard<'_, MapState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn caller(&self, header: Option<&str>) -> Result<Caller, ApiError> {
        self.tokens.caller_of(header)
    }

    /// Whether `caller` may make a call that any of `scopes` allows. Without tokens (a
    /// loopback cluster, `--insecure-no-tls-no-auth`) everyone may do everything, pushing a
    /// map included.
    fn allowed(&self, caller: &Caller, scopes: &[Scope], what: &str) -> Result<(), ApiError> {
        if caller.memory_only() {
            Err(super::service::namespaced_denied(what))
        } else if !self.tokens.any() || scopes.iter().any(|&s| caller.scope.allows(s)) {
            Ok(())
        } else {
            Err(ApiError::permission_denied(what))
        }
    }

    /// The node's map, or `NotFound` before it has one.
    pub(crate) fn get_map(&self) -> Result<ClusterMap, ApiError> {
        self.state()
            .map
            .as_ref()
            .map(|(m, _)| m.clone())
            .ok_or_else(|| ApiError::not_found("this node has no cluster map yet"))
    }

    /// The epoch partition `id`'s current history began at, as this node's map has it
    /// (`Partition.history_epoch`): `None` without a map or the partition.
    pub(crate) fn history_epoch(&self, id: &str) -> Option<u64> {
        let map = self.get_map().ok()?;
        map.partitions
            .iter()
            .find(|p| p.id == id)
            .map(|p| p.history_epoch)
    }

    /// Partition `id`'s owner in this node's map and its address, for a replica copy's
    /// follower (D328): an error when the map has no such partition, or names this node.
    pub(crate) fn owner_of(&self, id: &str) -> Result<(String, String), String> {
        let map = self.get_map().map_err(|e| e.message)?;
        let p =
            map.partitions.iter().find(|p| p.id == id).ok_or_else(|| {
                format!("partition {id} isn't in map version {}", map.map_version)
            })?;
        if p.node == self.id {
            return Err(format!(
                "map version {} gives partition {id} to this node: it isn't a replica here",
                map.map_version
            ));
        }
        let address = map
            .nodes
            .iter()
            .find(|n| n.id == p.node)
            .map(|n| n.address.clone())
            .ok_or_else(|| format!("partition {id}'s node {} isn't in the map", p.node))?;
        Ok((p.node.clone(), address))
    }
}
