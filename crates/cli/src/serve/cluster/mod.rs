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

/// The map fields this build keeps when it decodes and stores a map (`NodeStatus.map_features`,
/// ICR 0138 and 0139). A map's writer adds such a field only once every node reports it: a
/// node that drops it would store a different map at the same version, and read as divergent.
/// Request features its peers may rely on are listed here too (#1915 PR 3, ICR 0139's PR 3
/// section): [`NEAREST_QUALIFIERS_FEATURE`] and [`MEMORY_TOOL_FIELDS_FEATURE`].
pub(crate) const MAP_FEATURES: &[&str] = &[
    MEMORY_FEATURE,
    INDEXES_FEATURE,
    NEAREST_QUALIFIERS_FEATURE,
    MEMORY_TOOL_FIELDS_FEATURE,
    REPLICAS_FEATURE,
    INDEX_BULK_LOAD_FEATURE,
    LEASES_FEATURE,
    WRITER_TERMS_FEATURE,
    RANGE_MOVES_FEATURE,
    spanning::SPANNING_TXNS_FEATURE,
];

/// [`MAP_FEATURES`]' name for `ClusterMap.moves` (D331): a node without it would drop the
/// field from its copy, read as divergent, and gate no moving range.
pub(crate) const RANGE_MOVES_FEATURE: &str = "range_moves";

/// [`MAP_FEATURES`]' name for a build that serves `Cluster.Lease` (D326 amendment 1, #2048):
/// a peer whose `Lease` answers `Unimplemented` counts as granting only if its `Status`
/// lacks it (a 1.2 node), never when it reports it (a 1.3 node behind something that doesn't
/// route the call, audit120's S1).
pub(crate) const LEASES_FEATURE: &str = "leases";

/// [`MAP_FEATURES`]' name for `ClusterMap.memory` (#1915).
pub(crate) const MEMORY_FEATURE: &str = "memory";

/// [`MAP_FEATURES`]' name for `ClusterMap.indexes` (#1722, ICR 0138): a node without it
/// would drop the field from the map it stores, and read as divergent.
pub(crate) const INDEXES_FEATURE: &str = "indexes";

/// [`MAP_FEATURES`]' name for a build that serves `Cluster.ForwardBulkLoad` (#1722 PR 3, ICR
/// 0138): an entry node checks every node holding a part of the index for it before a
/// `Vectors.BulkLoad` reads anything, so an older one refuses the load up front, not
/// part way.
pub(crate) const INDEX_BULK_LOAD_FEATURE: &str = "index_bulk_load";

/// [`MAP_FEATURES`]' name for a build that applies `NearestRequest.qualifiers` (#1915 PR 3):
/// a node without it would rank every qualifier.
pub(crate) const NEAREST_QUALIFIERS_FEATURE: &str = "nearest_qualifiers";

/// [`MAP_FEATURES`]' name for a build that serves the memory calls' fields for `/mcp`'s
/// memory tools (#1915 PR 3b): `RememberRequest.supersedes`, `RememberResponse.
/// supersede_error`, `RecallResponse.truncated`, `MemoryHit.superseded_by` and the
/// `missing` ids of `MarkUsed` and `Forget`. A node without it would ignore `supersedes`
/// and leave the rest empty.
pub(crate) const MEMORY_TOOL_FIELDS_FEATURE: &str = "memory_tool_fields";

/// How long a peer's answered features ([`Node::owners_keep`]) are taken as its own: a
/// node's build changes only when it restarts.
const PEER_FEATURES_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a peer that didn't answer the features check is taken as unavailable without
/// asking again: so searches while it's down don't each wait a push timeout for it, yet one
/// back up serves again soon.
const PEER_DOWN_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// A peer's features as it last answered `Status`, and when (`None`: it didn't answer).
type PeerFeatures = (std::time::Instant, Option<Vec<String>>);

/// What a peer said about a feature ([`Node::features_of`]).
pub(crate) enum PeerFeature {
    Has,
    /// It answered without it: an older build.
    Lacks,
    /// It didn't answer (in time): why.
    NoAnswer(String),
}

/// [`MAP_FEATURES`]' name for `Partition.replicas` (D328, #1851).
pub(crate) const REPLICAS_FEATURE: &str = "replicas";

/// Whether `map` places a read replica of any partition (D328).
pub(crate) fn has_replicas(map: &ClusterMap) -> bool {
    map.partitions.iter().any(|p| !p.replicas.is_empty())
}

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

/// [`MAP_FEATURES`]' name for `ClusterMap.writer`, `.writer_term` and `.failover` (D330): a
/// node without it would drop them from its copy and read as divergent, so a map sets them
/// only once every node reports it.
pub(crate) const WRITER_TERMS_FEATURE: &str = "writer_terms";
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

/// A fault seam (tests): nodes cut off from their peers, each by its cluster directory and
/// its address. A cut node's dials of its peers fail, and so do its peers' dials of it, for
/// maps, pushes and leases; its process runs on and answers its own clients (D326's cut-off
/// owner). One entry per node, so tests running at once don't cut each other's.
#[cfg(test)]
pub(crate) static CUT_OFF: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

/// A fault seam (tests): the node in each directory can't reach the paired address, one way
/// (D330 PR 2's test 7: an owner cut off from the writer alone). List both ways to cut a
/// pair.
#[cfg(test)]
pub(crate) static CUT_BETWEEN: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before `nearest_qualifiers` (#1915 PR 3),
/// by cluster directory. `status`: its `Status` leaves the feature out. `parts`: its
/// partitions' nearest searches rank every qualifier, as such a build's would (with
/// `status` false, a peer that changed build within the entry node's features cache).
#[cfg(test)]
pub(crate) static OLD_NEAREST: Mutex<Vec<(PathBuf, OldNearest)>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before `memory_tool_fields` (#1915 PR 3b),
/// by cluster directory: their `Status` leaves the feature out.
#[cfg(test)]
pub(crate) static OLD_MEMORY: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before `ForwardBulkLoad` (#1722 PR 3), by
/// cluster directory: their `Status` leaves `index_bulk_load` out.
#[cfg(test)]
pub(crate) static OLD_BULK_LOAD: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests): nodes acting as a build before `ClusterMap.indexes` (#1722), by
/// cluster directory: their `Status` leaves `indexes` out.
#[cfg(test)]
pub(crate) static OLD_INDEXES: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A hold seam (tests): nodes whose `Status` doesn't answer while their cluster directory
/// is listed, as a stalled peer's wouldn't.
#[cfg(test)]
pub(crate) static HOLD_STATUS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests, #2073): nodes that hold no election while their cluster directory is
/// listed.
#[cfg(test)]
pub(crate) static NO_ELECTION: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests, #2073): candidates whose real vote requests reach no peer, past a
/// pre-vote that did: an election lost between the two rounds. Its own vote is recorded.
#[cfg(test)]
pub(crate) static DROP_VOTES: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A fault seam (tests, #2073): writers that don't adopt a stranded node's term, so a test
/// can watch the cluster with the node still stranded.
#[cfg(test)]
pub(crate) static NO_ADOPT: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// Whether the seam `list` names this node's directory `dir`.
#[cfg(test)]
pub(crate) fn seam(list: &Mutex<Vec<PathBuf>>, dir: &Path) -> bool {
    list.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|d| d == dir)
}

/// How [`OLD_NEAREST`] has a node act.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum OldNearest {
    /// `Status` leaves `nearest_qualifiers` out.
    Status,
    /// The node's partitions ignore `NearestRequest.qualifiers`.
    Parts,
}

/// Whether [`OLD_NEAREST`] has the node or partition at `path` act as `how`.
#[cfg(test)]
pub(crate) fn old_nearest(path: &Path, how: OldNearest) -> bool {
    OLD_NEAREST
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|(d, h)| *h == how && path.starts_with(d))
}

#[cfg(test)]
fn cut_off(dir: &Path, address: &str) -> bool {
    CUT_OFF
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|(d, a)| d == dir || a == address)
        || CUT_BETWEEN
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|(d, a)| d == dir && a == address)
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
    /// The node's device ceiling (D332): the highest one-second rate of its partitions'
    /// device bytes within `--balance-window`, fed by [`Node::watch_device`].
    device_ceiling: load::DeviceCeiling,
    /// Whether [`Node::watch_device`]'s loop runs.
    watching_device: std::sync::atomic::AtomicBool,
}

/// How long a node change's `SetMap` goes on pushing it, in push timeouts, for a majority of
/// both the nodes before it and the nodes after it to hold it (#1899's F4).
const JOINT_PUSHES: u32 = 3;

/// Who holds a node change ([`Node::joint`]): of the nodes before it and after it, how many
/// hold it and how many there are, this node included; and why each node that doesn't, as
/// "id: why".
struct Joint {
    before: (usize, usize),
    after: (usize, usize),
    missing: Vec<String>,
}

impl Joint {
    /// A majority of both sets.
    fn held(&self) -> bool {
        self.before.0 * 2 > self.before.1 && self.after.0 * 2 > self.after.1
    }

    /// The refusal of a change while `map`, a node change, isn't held.
    fn refusal(&self, map: &ClusterMap) -> String {
        format!(
            "map version {} changed the cluster's nodes, and a majority of both the nodes \
             before it ({} of {}) and the nodes after it ({} of {}) must hold it before the \
             map changes again (D317 rule 3): {}. Bring those nodes up, or set a map with \
             --force",
            map.map_version,
            self.before.0,
            self.before.1,
            self.after.0,
            self.after.1,
            self.missing.join("; ")
        )
    }
}

/// Where a map [`Node::install`] takes comes from.
#[derive(Clone, Copy)]
enum MapSource<'a> {
    /// Set here through the writer (`SetMap`): a valid change from the current map, with
    /// its partitions' `history_epoch` derived (`map::with_history`), keeping the history
    /// of each partition in `SetMapRequest.copies`.
    Change(&'a [String]),
    /// Set here by the forced recovery (`SetMap` with `force`): the same cluster's and newer
    /// (`map::validate_catch_up`), with no epoch going back and no move keeping one
    /// (`map::validate_epochs`), and `history_epoch` derived as for a change that copied
    /// nothing. That's only as new as this node's map: `phdb cluster set-map --force`
    /// checks the epochs against every node it reaches (simp724's F2 on #1729), so a forced
    /// epoch is above every one the cluster used and its history value is new.
    Forced,
    /// A peer's, pushed or pulled: only the same cluster's and newer (cli692's R1), taken
    /// as it arrives, derived where it was set.
    Peer,
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

    /// Validates and persists `map` as the node's newest; `check` decides, under the state
    /// lock, whether it may replace the current one. `from` says where the map comes from,
    /// which decides its checks and whether its partitions' `history_epoch` is derived here
    /// ([`MapSource`]). Returns the new version.
    ///
    /// **Blocking** (address resolution, an fsync): run it on a blocking thread
    /// ([`Node::install_async`]). The state lock is held only to check and to publish; the
    /// persist runs under `installing` alone.
    fn install(
        &self,
        map: &ClusterMap,
        from: MapSource<'_>,
        check: impl FnOnce(&mut MapState) -> Result<Option<u64>, ApiError>,
    ) -> Result<u64, ApiError> {
        map::validate(map, self.loopback_only).map_err(ApiError::invalid)?;
        // A map set here, forced or not, gets its partitions' `history_epoch` derived from
        // the current one, or from none for a first map (D317 ticket 6); a peer's arrives
        // derived.
        let mut derived = None;
        let _one = self
            .installing
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        {
            let mut st = self.state();
            if let Some(same) = check(&mut st)? {
                return Ok(same);
            }
            // The term fence (D330 amendment 1's M1): a map below a term this node voted for
            // is from a writer that has been replaced.
            let voted = self.grants.voted();
            if map.writer_term < voted {
                return Err(ApiError::failed_precondition(format!(
                    "the map is at writer term {}, but {VOTED_ABOVE} {voted}: it's from a writer \
                     that has been replaced (D330)",
                    map.writer_term
                )));
            }
            match &st.map {
                None if !map.nodes.iter().any(|n| n.id == self.id) => {
                    return Err(ApiError::invalid(format!(
                        "this node's first map must name it: node {:?} isn't in the map",
                        self.id
                    )));
                }
                None if matches!(from, MapSource::Change(c) if !c.is_empty()) => {
                    return Err(ApiError::invalid(
                        "a first map can't mark a partition as copied: it relocates nothing",
                    ));
                }
                // A first map set here (a new cluster, or a restore's forced map onto fresh
                // nodes) starts every history at its epoch, whatever the map sent says: a
                // backup's map carries its old values, which its files no longer hold
                // (simp724's S1 on #1729). A peer's arrives derived.
                None => {
                    if !matches!(from, MapSource::Peer) {
                        derived = Some(
                            map::with_history(&ClusterMap::default(), map, &[])
                                .map_err(ApiError::invalid)?,
                        );
                    }
                }
                Some((old, _)) => match from {
                    MapSource::Change(copies) => {
                        map::validate_change(old, map).map_err(ApiError::invalid)?;
                        derived =
                            Some(map::with_history(old, map, copies).map_err(ApiError::invalid)?);
                    }
                    MapSource::Forced => {
                        map::validate_catch_up(old, map).map_err(ApiError::invalid)?;
                        // No epoch goes back and no move keeps one, so no history value is
                        // used twice (simp724's F1 on #1729); the tables needn't step.
                        map::validate_epochs(old, map).map_err(ApiError::invalid)?;
                        derived =
                            Some(map::with_history(old, map, &[]).map_err(ApiError::invalid)?);
                    }
                    MapSource::Peer => {
                        map::validate_catch_up(old, map).map_err(ApiError::invalid)?;
                    }
                },
            }
        }
        let canonical = map::canonical(derived.as_ref().unwrap_or(map));
        let json = map::canonical_json(&canonical);
        write_map(&self.dir, &json)
            .map_err(|e| ApiError::internal(format!("persist the cluster map: {e}")))?;
        let version = canonical.map_version;
        // The partition's files take the map's schema before its routes serve (D317 ticket
        // 5): a call routed by this map finds its table, or `TableNotFound` while a node
        // that hasn't taken it yet catches up.
        self.reconcile_locked(&_one, &canonical, None);
        let routes = Arc::new(route::Routes::new(&canonical));
        let mut st = self.state();
        let moved = moved_partitions(st.map.as_ref().map(|(m, _)| m), &canonical);
        st.map = Some((canonical, json));
        // Under the state lock, so the routes never run behind the map `GetMap` answers.
        self.routes.store(Some(routes));
        st.divergent.clear();
        // Versions at or below this one need no pull again (cli692's N3).
        st.pulled.retain(|v| *v > version);
        // A lease the map no longer names this node for ends now (audit120's S1), still
        // under the state lock, so a round reads the new map only once it has (audit120's
        // M3): its request no longer asks for the partition, and frees its grants. The
        // state lock, then the fences' `local` one, as `apply_leases` takes them.
        if let Some((m, _)) = &st.map {
            self.fences.lapse_unnamed(m, &self.id);
        }
        let installed = st.map.as_ref().map(|(m, _)| m.clone());
        drop(st);
        if let Some(m) = &installed {
            self.release_writer_if_replaced(m);
        }
        // A partition the map now gives this node opens (D317 ticket 6, §3).
        self.open_missing_soon();
        self.end_moved_feeds(version);
        // D331: each partition's range gate, and a source's hand-over.
        if let Some(m) = &installed {
            self.apply_moves(m);
        }
        self.wake_copies(&moved);
        Ok(version)
    }

    /// Wakes this node's replica copies of `moved` (D330 amendment 2): their followers look
    /// the owner up in the map now rather than after their pause.
    fn wake_copies(&self, moved: &[String]) {
        if moved.is_empty() {
            return;
        }
        let Some(shared) = self.server.get().and_then(|o| o.shared.upgrade()) else {
            return;
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return;
        };
        let copies = cluster.replicas.load();
        for id in moved {
            if let Some(copy) = copies.get(id) {
                copy.follower.wake();
            }
        }
    }

    /// Ends the cluster feed components of every partition open here that the map,
    /// installed at `version`, gives another node: a relocation that didn't close it here
    /// (a fenced partition stays open) would otherwise leave them following this copy,
    /// which takes no more writes. Their subscriptions open them again where the map puts
    /// them (audit120's S-C5). After the routes are stored, so a component opened meanwhile
    /// finds the new map when it checks it again ([`feed::serve`]).
    fn end_moved_feeds(&self, version: u64) {
        let Some(shared) = self.server.get().and_then(|o| o.shared.upgrade()) else {
            return;
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return;
        };
        let routes = self.routes();
        let Some(routes) = routes.as_ref() else {
            return;
        };
        for (id, service) in cluster.parts.load().iter() {
            if routes.owner(id).is_some_and(|o| o.node == self.id) {
                continue;
            }
            if let Ok(part) = service.part() {
                part.feeds.end_parts(ApiError::wrong_partition(
                    version,
                    format!("map version {version} moved partition {id:?} off this node"),
                ));
            }
        }
    }

    /// [`Node::install`] on a blocking thread.
    async fn install_async(
        self: &Arc<Self>,
        map: ClusterMap,
        copies: Option<Vec<String>>,
        check: impl FnOnce(&mut MapState) -> Result<Option<u64>, ApiError> + Send + 'static,
    ) -> Result<u64, ApiError> {
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let from = copies
                .as_deref()
                .map_or(MapSource::Forced, MapSource::Change);
            node.install(&map, from, check)
        })
        .await?
    }

    /// `Cluster.SetMap`: compare-and-set on the version, then push to the others. Only the
    /// map's writer, the node with the smallest id in its current map, sets a map (D317
    /// amendment 1); another answers `WrongPartition` naming it, and `phdb cluster set-map`
    /// follows. `force` (the forced recovery, for a writer that's down) skips the writer and
    /// the compare-and-set: the map need only be the same cluster's, at a version above this
    /// node's.
    ///
    /// **A node change (#1899's F4):** a map that changes the nodes carries the nodes before
    /// it (`previous_nodes`), and answers once a majority of those and a majority of its own
    /// hold it, or after [`JOINT_PUSHES`] push timeouts of trying. Until a majority of both
    /// hold it, the writer takes no other change, a schema change included: a majority of
    /// one set confirms a partition's owner (D317 rule 3), so the set before must have moved
    /// on before a later map can count on the set after.
    async fn set_map(self: &Arc<Self>, req: SetMapRequest) -> Result<SetMapResponse, ApiError> {
        let mut map = req
            .map
            .ok_or_else(|| ApiError::invalid("SetMap needs a map"))?;
        if req.force {
            // A copy is a planned move, never part of a recovery: a forced map's raised
            // epochs all restart their partitions' histories.
            if !req.copies.is_empty() {
                return Err(ApiError::invalid(
                    "a forced map can't mark a partition as copied: relocate by copy through \
                     the map's writer",
                ));
            }
            // A recovery waits for no majority: it's what `--force` means.
            map.previous_nodes.clear();
            let version = self.install_async(map, None, |_| Ok(None)).await?;
            let pushed = self.push_all().await;
            return Ok(SetMapResponse {
                map_version: version,
                pushed,
            });
        }
        if let Ok(current) = self.get_map() {
            let w = schema::writer(&current);
            if w != self.id {
                let address = current
                    .nodes
                    .iter()
                    .find(|n| n.id == w)
                    .map(|n| n.address.as_str())
                    .unwrap_or_default();
                return Err(ApiError::wrong_partition(
                    current.map_version,
                    format!(
                        "node {w} (at {address}) is the cluster map's writer: set the map \
                         there, or with --force if it's down"
                    ),
                ));
            }
            if !self.synced.load(std::sync::atomic::Ordering::Acquire) {
                return Err(ApiError::partition_unavailable(
                    "this node is still taking its peers' maps after starting: try again",
                ));
            }
            // The writer and its term are the writer's to keep (D330): an operator's map
            // can't change them, and from term 1 a change needs the writer lease.
            map.writer = current.writer.clone();
            map.writer_term = current.writer_term;
            self.await_writer_lease(&current).await?;
            if !map::uses_failover(&current) && map::uses_failover(&map) {
                self.every_node_keeps(&map, WRITER_TERMS_FEATURE, "failover")
                    .await?;
            }
            if let Some(why) = map::moves_with_node_change(&current, &map) {
                return Err(ApiError::failed_precondition(why));
            }
            // A memory store goes into the map only once every node keeps it (#1915).
            if current.memory.is_none() && map.memory.is_some() {
                self.every_node_keeps(&map, MEMORY_FEATURE, "a memory store")
                    .await?;
            }
            // So do read replicas (D328).
            if !has_replicas(&current) && has_replicas(&map) {
                self.every_node_keeps(&map, REPLICAS_FEATURE, "a read replica")
                    .await?;
            }
            // So does the first index placement (#1722).
            if current.indexes.is_empty() && !map.indexes.is_empty() {
                self.every_node_keeps(&map, INDEXES_FEATURE, "an index placement")
                    .await?;
            }
            // So does the first range move (D331).
            if current.moves.is_empty() && !map.moves.is_empty() {
                self.every_node_keeps(&map, RANGE_MOVES_FEATURE, "a range move")
                    .await?;
            }
            if !current.previous_nodes.is_empty() {
                let (joint, _) = self.joint(&current).await;
                if !joint.held() {
                    return Err(ApiError::failed_precondition(joint.refusal(&current)));
                }
                self.joint_settled(&current).await;
            }
            map.previous_nodes = if map::same_nodes(&current, &map) {
                Vec::new()
            } else {
                current.nodes.clone()
            };
        } else {
            // The node's first map: its nodes keep a memory store too, or none goes in.
            if map.memory.is_some() {
                self.every_node_keeps(&map, MEMORY_FEATURE, "a memory store")
                    .await?;
            }
            if has_replicas(&map) {
                self.every_node_keeps(&map, REPLICAS_FEATURE, "a read replica")
                    .await?;
            }
            if !map.indexes.is_empty() {
                self.every_node_keeps(&map, INDEXES_FEATURE, "an index placement")
                    .await?;
            }
            if !map.moves.is_empty() {
                self.every_node_keeps(&map, RANGE_MOVES_FEATURE, "a range move")
                    .await?;
            }
            if map::uses_failover(&map) || map.writer_term > 0 {
                self.every_node_keeps(&map, WRITER_TERMS_FEATURE, "failover or a writer term")
                    .await?;
            }
            map.previous_nodes.clear();
        }
        let changes_nodes = !map.previous_nodes.is_empty();
        let expected = req.expected_map_version;
        let version = self
            .install_async(map, Some(req.copies), move |st| {
                let current = st.version();
                if expected != current {
                    return Err(ApiError::from_code(
                        ErrorCode::Conflict,
                        &format!(
                            "the cluster map is at version {current}, not {expected}: re-read it and set it again",
                        ),
                    ));
                }
                Ok(None)
            })
            .await?;
        // The lease again before the push (D330). Past it, the change is installed here and
        // not undone: it may still reach the cluster (this writer's next round, if it renews
        // at the same term, pushes it) or be replaced by an elected map. So its outcome is
        // unknown (simp724's S1 on #2056).
        if let Ok(set) = self.get_map()
            && let Err(e) = self.writer_may_change(&set)
        {
            return Err(ApiError::from_code(
                ErrorCode::OutcomeUnknown,
                &format!(
                    "map version {version} is installed on this node, the writer, but its \
                     writer lease ran out before the push ({}). It may still reach the \
                     cluster, or be replaced by an elected map: read the map (phdb cluster \
                     show-map) before setting it again",
                    e.message
                ),
            ));
        }
        let pushed = match self.get_map() {
            Ok(set) if changes_nodes && set.map_version == version => self.joint_wait(&set).await,
            _ => self.push_all().await,
        };
        Ok(SetMapResponse {
            map_version: version,
            pushed,
        })
    }

    /// A node change `map` that a majority of both its node sets hold: waits until a lease
    /// has passed since this writer first saw that (audit120's L1 on #1899's F4, D326). The
    /// nodes before the change may have granted leases at the old version, and the nodes
    /// after it never saw those grants: until they run out, a majority of the new set could
    /// grant a partition to a second node. Recomputed, never remembered across a restart or
    /// a change of writer: such a writer counts from its own check, at most one lease more.
    async fn joint_settled(&self, map: &ClusterMap) {
        let now = self.grants.clock().now();
        let seen = {
            let mut seen = self
                .joint_seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match *seen {
                Some((version, at)) if version == map.map_version => at,
                _ => {
                    *seen = Some((map.map_version, now));
                    now
                }
            }
        };
        let grace = u64::try_from(lease::grace(self.grants.lease()).as_nanos()).unwrap_or(u64::MAX);
        let left = seen.saturating_add(grace).saturating_sub(now);
        if left > 0 {
            tokio::time::sleep(Duration::from_nanos(left)).await;
        }
    }

    /// Records that this writer saw a majority of both of node change `map`'s sets hold it
    /// now, unless it saw that before ([`Node::joint_settled`]).
    fn joint_held(&self, map: &ClusterMap) {
        let now = self.grants.clock().now();
        let mut seen = self
            .joint_seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !matches!(*seen, Some((version, _)) if version == map.map_version) {
            *seen = Some((map.map_version, now));
        }
    }

    /// Pushes a node change `map` to the nodes before it and after it, until a majority of
    /// each holds it or [`JOINT_PUSHES`] push timeouts have passed: the last pushes.
    async fn joint_wait(self: &Arc<Self>, map: &ClusterMap) -> Vec<NodePush> {
        let until = tokio::time::Instant::now() + self.peers.push_timeout * JOINT_PUSHES;
        let mut pause = Duration::from_millis(100);
        loop {
            let (joint, pushed) = self.joint(map).await;
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            if joint.held() {
                self.joint_held(map);
                return pushed;
            }
            if left.is_zero() {
                return pushed;
            }
            tokio::time::sleep(pause.min(left)).await;
            pause = (pause * 2).min(Duration::from_secs(1));
        }
    }

    /// Pushes a node change `map` (one with `previous_nodes`) to every node before it and
    /// after it, and counts who holds it: those that took it, and this node.
    async fn joint(self: &Arc<Self>, map: &ClusterMap) -> (Joint, Vec<NodePush>) {
        let mut all = map.previous_nodes.clone();
        for n in &map.nodes {
            if !all.iter().any(|p| p.id == n.id) {
                all.push(n.clone());
            }
        }
        let pushed = self.push_each(map, &all).await;
        let holds = |id: &str| id == self.id || pushed.iter().any(|p| p.node == id && p.ok);
        let count = |set: &[proto::Node]| (set.iter().filter(|n| holds(&n.id)).count(), set.len());
        let joint = Joint {
            before: count(&map.previous_nodes),
            after: count(&map.nodes),
            missing: pushed
                .iter()
                .filter(|p| !p.ok)
                .map(|p| format!("{}: {}", p.node, p.error))
                .collect(),
        };
        (joint, pushed)
    }

    /// Pushes the node's map to every other node in it, in parallel, each within the push
    /// timeout. A node it misses pulls the map once a call from this node reaches it.
    async fn push_all(self: &Arc<Self>) -> Vec<NodePush> {
        let Ok(map) = self.get_map() else {
            return Vec::new();
        };
        self.push_each(&map, &map.nodes).await
    }

    /// Pushes `map` to each of `nodes` but this one, in parallel, each within the push
    /// timeout: each node's outcome, in id order.
    async fn push_each(self: &Arc<Self>, map: &ClusterMap, nodes: &[proto::Node]) -> Vec<NodePush> {
        let mut tasks = tokio::task::JoinSet::new();
        for n in nodes.iter().filter(|n| n.id != self.id) {
            let (node, id, address, map) = (
                Arc::clone(self),
                n.id.clone(),
                n.address.clone(),
                map.clone(),
            );
            tasks.spawn(async move {
                let result = tokio::time::timeout(node.peers.push_timeout, async {
                    let mut client = node.client(&address).await?;
                    let req = node.request(PushMapRequest { map: Some(map) })?;
                    client
                        .push_map(req)
                        .await
                        .map_err(|s| s.message().to_owned())
                })
                .await
                .unwrap_or_else(|_| Err("timed out".to_owned()));
                if let Err(e) = &result
                    && e.contains(DIVERGENT)
                {
                    node.state().divergent.insert(id.clone());
                }
                // A node the term fence keeps from this map: it voted in an election that
                // lost (#2073). The writer's duties look at it.
                if let Err(e) = &result
                    && e.contains(VOTED_ABOVE)
                {
                    let now = node.grants.clock().now();
                    node.stranded
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .refused(&id, now, node.grants.lease());
                }
                NodePush {
                    node: id,
                    ok: result.is_ok(),
                    error: result.err().unwrap_or_default(),
                }
            });
        }
        let mut out: Vec<NodePush> = tasks.join_all().await;
        out.sort_by(|a, b| a.node.cmp(&b.node));
        out
    }

    /// At startup with a map: fetches every peer's map and takes a newer one (through
    /// `PushMap`'s checks), then pushes the result to the peers. A node that was down while
    /// the map moved on catches up here; peers that are behind catch up from the push.
    async fn sync_on_start(self: &Arc<Self>) {
        let Ok(map) = self.get_map() else {
            return;
        };
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
        }
        self.push_all().await;
        self.synced
            .store(true, std::sync::atomic::Ordering::Release);
        // Its partitions' leases (D326): a round now, then renewed for the node's life, and
        // asked again sooner while one has none (cli692's S3).
        self.confirm_round().await;
        self.renew_leases();
    }

    /// `FailedPrecondition` unless every node `map` lists keeps its field `feature`
    /// ([`MAP_FEATURES`]), asked through `Status`: a node that would drop the field stores
    /// another map at the same version, reads as divergent and blocks confirms (#1899's
    /// rule), so `what` is added only once the whole cluster runs a build that keeps it. A
    /// node that doesn't answer counts as one that doesn't: the refusal names it, and a
    /// forced map skips this as it skips the majority.
    async fn every_node_keeps(
        self: &Arc<Self>,
        map: &ClusterMap,
        feature: &str,
        what: &str,
    ) -> Result<(), ApiError> {
        // Every node at once, as a confirm round asks them (simp724's n2 on #1920): k nodes
        // that don't answer cost one push timeout, not k.
        let peers = map
            .nodes
            .iter()
            .filter(|n| n.id != self.id)
            .map(|n| (n.id.clone(), n.address.clone()));
        let lacking = self.lacking(peers, feature).await;
        if lacking.is_empty() {
            Ok(())
        } else {
            Err(ApiError::failed_precondition(format!(
                "the map can't add {what} until every node runs a build that keeps it in its \
                 map, or that node would read as divergent: {}. Finish the upgrade first",
                lacking.join(", ")
            )))
        }
    }

    /// The peers of `peers` (id, address) that don't report `feature` in `Status`, each
    /// named with why: an older build, or no answer (in time). Every peer asked at once, as
    /// a confirm round asks them (simp724's n2 on #1920).
    async fn lacking(
        self: &Arc<Self>,
        peers: impl Iterator<Item = (String, String)>,
        feature: &str,
    ) -> Vec<String> {
        let mut lacking: Vec<String> = self
            .features_of(peers, feature, false)
            .await
            .into_iter()
            .filter_map(|(id, f)| match f {
                PeerFeature::Has => None,
                PeerFeature::Lacks => Some(format!("{id} (an older build)")),
                PeerFeature::NoAnswer(why) => Some(format!("{id} ({why})")),
            })
            .collect();
        lacking.sort();
        lacking
    }

    /// What each peer of `peers` (id, address) says about `feature` in `Status`, asked all
    /// at once. With `cached`, an answer from the last [`PEER_FEATURES_TTL`] stands, and a
    /// no-answer from the last [`PEER_DOWN_TTL`].
    pub(crate) async fn features_of(
        self: &Arc<Self>,
        peers: impl Iterator<Item = (String, String)>,
        feature: &str,
        cached: bool,
    ) -> Vec<(String, PeerFeature)> {
        let verdict = |fs: &Option<Vec<String>>| match fs {
            Some(fs) if fs.iter().any(|f| f == feature) => PeerFeature::Has,
            Some(_) => PeerFeature::Lacks,
            None => PeerFeature::NoAnswer("no answer lately".into()),
        };
        let mut asks = tokio::task::JoinSet::new();
        let mut out = Vec::new();
        for (id, address) in peers {
            if cached {
                let known = self
                    .peer_features
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&id)
                    .filter(|(at, fs)| {
                        let ttl = if fs.is_some() {
                            PEER_FEATURES_TTL
                        } else {
                            PEER_DOWN_TTL
                        };
                        at.elapsed() < ttl
                    })
                    .map(|(_, fs)| verdict(fs));
                if let Some(v) = known {
                    out.push((id, v));
                    continue;
                }
            }
            let node = Arc::clone(self);
            let feature = feature.to_owned();
            asks.spawn(async move {
                let answer = async {
                    let mut client = node.client(&address).await?;
                    let req = node.request(proto::StatusRequest {})?;
                    client
                        .status(req)
                        .await
                        .map(tonic::Response::into_inner)
                        .map_err(|s| s.message().to_owned())
                };
                let (seen, v) = match tokio::time::timeout(node.peers.push_timeout, answer).await {
                    Ok(Ok(st)) => {
                        let v = if st.map_features.contains(&feature) {
                            PeerFeature::Has
                        } else {
                            PeerFeature::Lacks
                        };
                        (Some(st.map_features), v)
                    }
                    Ok(Err(e)) => (None, PeerFeature::NoAnswer(format!("no answer: {e}"))),
                    Err(_) => (None, PeerFeature::NoAnswer("no answer in time".into())),
                };
                node.peer_features
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id.clone(), (std::time::Instant::now(), seen));
                (id, v)
            });
        }
        out.extend(asks.join_all().await);
        out
    }

    /// Whether every node among `owners` (other than this one) applies request feature
    /// `feature`, which `what` needs (#1915 PR 3): `FailedPrecondition` naming each that
    /// answers without it (a build before it would answer its part without it), else the
    /// nodes that didn't answer, which the caller treats as unavailable (the coordinator's
    /// call). Answers are cached per peer ([`Node::features_of`]), so a node that's down
    /// doesn't cost each request a push timeout.
    pub(crate) async fn owners_keep(
        self: &Arc<Self>,
        owners: impl Iterator<Item = (String, String)>,
        feature: &str,
        what: &str,
    ) -> Result<Vec<String>, ApiError> {
        let mut seen = std::collections::BTreeMap::new();
        for (id, address) in owners.filter(|(id, _)| *id != self.id) {
            seen.entry(id).or_insert(address);
        }
        let (mut lacking, mut down) = (Vec::new(), Vec::new());
        for (id, f) in self.features_of(seen.into_iter(), feature, true).await {
            match f {
                PeerFeature::Has => {}
                PeerFeature::Lacks => lacking.push(format!("{id} (an older build)")),
                PeerFeature::NoAnswer(_) => down.push(id),
            }
        }
        lacking.sort();
        if lacking.is_empty() {
            Ok(down)
        } else {
            Err(ApiError::failed_precondition(format!(
                "{what} needs every node that holds a part of it to run a build that supports \
                 it, and these don't: {}. Finish the upgrade first, or ask without it",
                lacking.join(", ")
            )))
        }
    }

    /// Peer `address`'s map.
    async fn fetch_map(&self, address: &str) -> Result<ClusterMap, String> {
        let mut client = self.client(address).await?;
        let req = self.request(GetMapRequest {})?;
        client
            .get_map(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|s| s.message().to_owned())
    }

    /// [`Node::push_map`] on a blocking thread.
    async fn push_map_async(
        self: &Arc<Self>,
        map: ClusterMap,
        from: Option<String>,
    ) -> Result<PushMapResponse, ApiError> {
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || node.push_map(map, from.as_deref())).await?
    }

    /// `Cluster.PushMap`: a peer's newer map. `from`: the pushing node's id, if it said.
    /// Blocking ([`Node::install`]).
    fn push_map(&self, map: ClusterMap, from: Option<&str>) -> Result<PushMapResponse, ApiError> {
        let pushed = from.is_some().then(|| map.clone());
        let version = self.push_map_install(map, from)?;
        // A peer's map, taken or the same as this node's: a round may now confirm this
        // node's partitions (D317 rule 3, #1760).
        if pushed.is_some() {
            self.confirm_soon();
        }
        Ok(PushMapResponse {
            map_version: version,
        })
    }

    fn push_map_install(&self, map: ClusterMap, from: Option<&str>) -> Result<u64, ApiError> {
        let version = self.install(&map, MapSource::Peer, |st| {
            let Some((old, json)) = &st.map else {
                return Ok(None);
            };
            if (map.writer_term, map.map_version) != (old.writer_term, old.map_version) {
                return Ok(None);
            }
            // The same `(term, version)` and contents: a no-op (simp724's N3), as rounds push
            // the same map again.
            if map::canonical_json(&map) == *json {
                return Ok(Some(old.map_version));
            }
            if let Some(f) = from {
                st.divergent.insert(f.to_owned());
            }
            Err(ApiError::invalid(format!(
                "{DIVERGENT}: this node has a different map at version {}: set a newer map",
                old.map_version
            )))
        })?;
        Ok(version)
    }

    /// `Cluster.Status`: the node, its map version, and its directory's partition files.
    fn status(&self) -> NodeStatus {
        let (version, cluster_id, divergent, map) = {
            let st = self.state();
            (
                st.version(),
                st.map
                    .as_ref()
                    .map(|(m, _)| m.cluster_id.clone())
                    .unwrap_or_default(),
                st.divergent.iter().cloned().collect(),
                st.map.as_ref().map(|(m, _)| m.clone()),
            )
        };
        let mut partitions = Vec::new();
        let mut files: Vec<PathBuf> = std::fs::read_dir(&self.dir)
            .map(|d| {
                d.filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| p.extension().is_some_and(|x| x == "phdb"))
                    .collect()
            })
            .unwrap_or_default();
        files.sort();
        // The partitions this node has open: their identity comes from their own handle,
        // since a read-only open of a file a writer holds is refused (#1767).
        let open: std::collections::BTreeMap<String, crate::serve::router::PartRef> = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .map(|shared| shared.router.partitions().into_iter().collect())
            .unwrap_or_default();
        let copies = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .and_then(|shared| {
                shared
                    .router
                    .cluster
                    .as_ref()
                    .map(|c| c.replicas.load_full())
            })
            .unwrap_or_default();
        let spanning = self
            .server
            .get()
            .and_then(|o| o.shared.upgrade())
            .and_then(|shared| {
                shared
                    .router
                    .cluster
                    .as_ref()
                    .map(|c| Arc::clone(&c.spanning))
            });
        for path in files {
            let file = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let named = path
                .file_stem()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let mut lp = LocalPartition {
                id: named.clone(),
                epoch: 0,
                file,
                ok: false,
                error: String::new(),
                ..Default::default()
            };
            // This node's replica copy (D328): its follower says how it is. Its file is held
            // open, and its identity row is its owner's, so it isn't read here.
            if let Some(copy) = copies.get(&named) {
                let c = copy.follower.copy_status();
                lp.role = proto::PartitionRole::Replica as i32;
                lp.set_replica_state(c.state);
                lp.replica_of = c.owner;
                lp.replica_applied_seqno = c.applied;
                lp.replica_primary_seqno = c.primary_seqno;
                lp.replica_lag_ms = c.lag_ms;
                // The copy's identity epoch: its owner's at the bootstrap (`e_b`), which a
                // promotion checks against P's history (D328 PR 3).
                lp.epoch = copy
                    .service
                    .part()
                    .ok()
                    .and_then(|part| identity::read(&part.db).ok().flatten())
                    .map_or(0, |id| id.epoch);
                lp.ok = c.state != proto::ReplicaState::Behind
                    && c.state != proto::ReplicaState::OwnerFeedOff;
                lp.error = if lp.ok { String::new() } else { c.last_error };
                partitions.push(lp);
                continue;
            }
            // A partition being opened: no read of its file, whose read-only open would refuse
            // the node's own open (#1770). It shows as opening.
            if self.fences.is_opening(&named) {
                lp.set_state(proto::LocalPartitionState::Opening);
                lp.ok = true;
                partitions.push(lp);
                continue;
            }
            let read = match open.get(&named) {
                Some(part) => identity::read(&part.db),
                None => identity::read_file(&path),
            };
            match read {
                Ok(None) => lp.error = "no identity row: run phdb cluster init-partition".into(),
                Err(e) => lp.error = e,
                Ok(Some(id)) => {
                    lp.epoch = id.epoch;
                    lp.error = if id.partition_id != named {
                        format!("the file says it's partition {:?}", id.partition_id)
                    } else if !cluster_id.is_empty() && id.cluster_id != cluster_id {
                        format!("the file is in cluster {:?}", id.cluster_id)
                    } else {
                        match map
                            .as_ref()
                            .and_then(|m| m.partitions.iter().find(|p| p.id == named))
                        {
                            Some(p) if p.node != self.id => {
                                format!("the map has it on node {:?}", p.node)
                            }
                            Some(p) if p.epoch != id.epoch => format!(
                                "the map has it at epoch {}, the file at {}",
                                p.epoch, id.epoch
                            ),
                            None if map.is_some() => "the map has no such partition".into(),
                            _ => String::new(),
                        }
                    };
                    lp.ok = lp.error.is_empty();
                }
            }
            // D329: its shares held in doubt, the oldest first named, and why each waits.
            if let Some(part) = open.get(&named) {
                in_doubt(&mut lp, &part.db, &named, spanning.as_deref());
            }
            partitions.push(lp);
        }
        // Partitions the map places here with no file in DIR: what an operator most needs
        // to see before the node serves them (cli692's N1).
        if let Some(m) = &map {
            for p in m.partitions.iter().filter(|p| p.node == self.id) {
                if !partitions.iter().any(|lp| lp.id == p.id) {
                    partitions.push(LocalPartition {
                        id: p.id.clone(),
                        epoch: p.epoch,
                        file: format!("{}.phdb", p.id),
                        ok: false,
                        error: format!(
                            "the map places it here, but the directory has no {}.phdb",
                            p.id
                        ),
                        ..Default::default()
                    });
                }
            }
        }
        // Each open partition's reconcile (D317 ticket 5), through its own service.
        {
            let reconciled = self
                .reconciled
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            for lp in &mut partitions {
                let here = map.as_ref().is_some_and(|m| {
                    m.partitions
                        .iter()
                        .any(|p| p.id == lp.id && p.node == self.id)
                });
                if here && let Some((v, o)) = reconciled.get(&lp.id) {
                    lp.reconciled_version = *v;
                    lp.reconcile_errors = o.errors.clone();
                    lp.unmapped_tables = o.unmapped.clone();
                    lp.adopted_tables = o.adopted.clone();
                }
            }
        }
        // An owner's file (D328): the map gives the partition to this node.
        for lp in &mut partitions {
            if lp.role == 0
                && map.as_ref().is_some_and(|m| {
                    m.partitions
                        .iter()
                        .any(|p| p.id == lp.id && p.node == self.id)
                })
            {
                lp.role = proto::PartitionRole::Owner as i32;
            }
        }
        // Each open partition's state, and a closed one's files (D317 ticket 6).
        for lp in &mut partitions {
            // One read (simp724's S1 on #1964): a reopen or a close between two would show
            // the partition as neither.
            let id = lp.id.clone();
            if let fence::Now::Served(gate) = self.fences.now(&id, |closed| closed.describe(lp)) {
                let (state, reason) = fence::gate_state(&gate);
                lp.set_state(state);
                lp.set_fence_reason(reason);
                // Both reasons at once (#1829): an operator-fenced partition may still await
                // its lease.
                lp.awaiting_peers = self.awaiting_peers(&id);
                lp.lease_remaining_ms = millis(self.lease_remaining(&id));
                // D332: its load, folded now if an interval has ended.
                if let Some(part) = open.get(&id) {
                    let l = self.partition_load(part);
                    (lp.load_cpu, lp.load_device, lp.load) = (l.cpu, l.device, l.load());
                }
            }
            // D331: the moves naming it (the follower's lag comes with PR 4's follower).
            if let Some(m) = &map {
                lp.moves = crate::serve::ranges::statuses(m, &id);
                // PR 4b: a source whose move's retention hold lapsed; a target's follower.
                if let Some(copies) = self.range_copies() {
                    for s in lp.moves.iter_mut().filter(|s| s.source) {
                        s.retention_lapsed = copies.lapsed(&s.id);
                    }
                }
                if let Some(follows) = self.range_follows() {
                    for s in lp.moves.iter_mut().filter(|s| !s.source) {
                        if let Some(p) = follows.progress(&s.id) {
                            s.copied_bytes =
                                p.copied_bytes.load(std::sync::atomic::Ordering::Relaxed);
                            s.copy_done = p.copy_done.load(std::sync::atomic::Ordering::Relaxed);
                            s.source_applied_seqno =
                                p.applied.load(std::sync::atomic::Ordering::Acquire);
                        }
                    }
                }
            }
        }
        let (writer, writer_term, writer_lease) = map.as_ref().map_or_else(
            || (String::new(), self.grants.voted(), Duration::ZERO),
            |m| {
                (
                    schema::writer(m).to_owned(),
                    self.grants.term_floor(m),
                    self.writer_lease_status(m),
                )
            },
        );
        NodeStatus {
            writer,
            writer_term,
            writer_lease_remaining_ms: millis(writer_lease),
            failover_blocked: self.failovers.blocked(),
            grants: self.grants.granted(),
            node_id: self.id.clone(),
            map_version: version,
            cluster_id,
            partitions,
            divergent,
            grace_remaining_ms: millis(self.grants.grace_remaining()),
            map_features: MAP_FEATURES
                .iter()
                .filter(|&&f| {
                    #[cfg(test)]
                    if f == NEAREST_QUALIFIERS_FEATURE && old_nearest(&self.dir, OldNearest::Status)
                    {
                        return false;
                    }
                    #[cfg(test)]
                    if f == INDEXES_FEATURE
                        && OLD_INDEXES
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .iter()
                            .any(|d| self.dir.starts_with(d))
                    {
                        return false;
                    }
                    #[cfg(test)]
                    if f == INDEX_BULK_LOAD_FEATURE
                        && OLD_BULK_LOAD
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .iter()
                            .any(|d| self.dir.starts_with(d))
                    {
                        return false;
                    }
                    #[cfg(test)]
                    if f == LEASES_FEATURE && fence::pre_lease(&self.dir) {
                        return false;
                    }
                    #[cfg(test)]
                    if f == MEMORY_TOOL_FIELDS_FEATURE
                        && OLD_MEMORY
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .iter()
                            .any(|d| self.dir.starts_with(d))
                    {
                        return false;
                    }
                    let _ = f;
                    true
                })
                .map(|&f| f.to_owned())
                .collect(),
        }
    }

    /// A peer at `address` (the map's `host:port`).
    async fn client(
        &self,
        address: &str,
    ) -> Result<proto::cluster_client::ClusterClient<tonic::transport::Channel>, String> {
        #[cfg(test)]
        if cut_off(&self.dir, address) {
            return Err(format!("connecting to {address}: cut off (a test's seam)"));
        }
        let channel = self
            .endpoint(address)?
            .connect()
            .await
            .map_err(|e| format!("connecting to {address}: {e}"))?;
        Ok(forward::peer_client(channel))
    }

    /// A channel to the peer at `address` (the map's `host:port`), as [`Node::client`] dials
    /// it: for a call other than `Cluster`'s, a replica copy's follower's (D328).
    pub(crate) async fn peer_channel(
        &self,
        address: &str,
    ) -> Result<tonic::transport::Channel, String> {
        #[cfg(test)]
        if cut_off(&self.dir, address) {
            return Err(format!("connecting to {address}: cut off (a test's seam)"));
        }
        self.endpoint(address)?
            .http2_keep_alive_interval(Duration::from_secs(30))
            .connect()
            .await
            .map_err(|e| format!("connecting to {address}: {e}"))
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

    /// How this node dials the peer at `address` (the map's `host:port`): its TLS settings,
    /// and a connect within the push timeout.
    pub(crate) fn endpoint(&self, address: &str) -> Result<tonic::transport::Endpoint, String> {
        let url = format!("{}://{address}", self.peers.scheme);
        let mut endpoint = tonic::transport::Endpoint::from_shared(url.clone())
            .map_err(|e| format!("{url}: {e}"))?
            .connect_timeout(self.peers.push_timeout);
        if self.peers.scheme == "https" {
            let mut tls = tonic::transport::ClientTlsConfig::new().with_native_roots();
            if let Some(ca) = &self.peers.ca {
                tls = tls.ca_certificate(ca.clone());
            }
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|e| format!("TLS to {url}: {e}"))?;
        }
        Ok(endpoint)
    }

    /// Pulls the map from node `from` at `address` now, and waits for it to be installed,
    /// unless this node's map is already past `past` (D317 ticket 2: a `WrongPartition`
    /// answer means the owner has a newer map). Single-flight: concurrent callers wait on
    /// one pull, and the ones after it find the map moved on.
    pub(crate) async fn pull_now(
        self: &Arc<Self>,
        from: &str,
        address: &str,
        past: u64,
    ) -> Result<(), String> {
        let _one = self.pulling.lock().await;
        if self.state().version() > past {
            return Ok(());
        }
        let map = tokio::time::timeout(self.peers.push_timeout, self.fetch_map(address))
            .await
            .map_err(|_| "timed out".to_owned())??;
        if map.map_version <= past {
            return Err(format!(
                "node {from}'s map is version {}, no newer than this node's",
                map.map_version
            ));
        }
        self.push_map_async(map, Some(from.to_owned()))
            .await
            .map(|_| ())
            .map_err(|e| e.message)
    }

    /// `message` as a call to a peer: the node's token, id and map version.
    pub(crate) fn request<T>(&self, message: T) -> Result<Request<T>, String> {
        let mut req = Request::new(message);
        let md = req.metadata_mut();
        if let Some(token) = &self.peers.token {
            md.insert(
                "authorization",
                format!("Bearer {token}")
                    .parse()
                    .map_err(|_| "the cluster token isn't a valid header value".to_owned())?,
            );
        }
        md.insert(
            MAP_VERSION_HEADER,
            self.state().version().to_string().parse().expect("digits"),
        );
        md.insert(
            NODE_ID_HEADER,
            self.id
                .parse()
                .map_err(|_| "the node id isn't a valid header value".to_owned())?,
        );
        Ok(req)
    }

    /// A forwarded call from a peer whose map is newer than this node's: take that map from
    /// the peer first, and wait for it, so the call is served under it (audit120's S-C3).
    /// Served under the older map, a write could land in a table the newer one dropped and
    /// made again, which this node's next reconcile drops with the write. A peer whose map
    /// can't be taken in time answers `PartitionUnavailable`: nothing ran, so a retry is
    /// safe. Anyone else's headers are ignored, as [`Node::maybe_pull`] ignores them.
    pub(crate) async fn catch_up(
        self: &Arc<Self>,
        caller: &Caller,
        md: &tonic::metadata::MetadataMap,
    ) -> Result<(), ApiError> {
        if caller.scope != Scope::Cluster && self.tokens.any() {
            return Ok(());
        }
        let header = |k: &str| md.get(k).and_then(|v| v.to_str().ok());
        let (Some(version), Some(from)) = (
            header(MAP_VERSION_HEADER).and_then(|v| v.parse::<u64>().ok()),
            header(NODE_ID_HEADER),
        ) else {
            return Ok(());
        };
        let (mine, address) = {
            let st = self.state();
            let address = st.map.as_ref().and_then(|(m, _)| {
                m.nodes
                    .iter()
                    .find(|n| n.id == from)
                    .map(|n| n.address.clone())
            });
            (st.version(), address)
        };
        if version <= mine {
            return Ok(());
        }
        let behind = |why: &str| {
            ApiError::partition_unavailable(format!(
                "this node's cluster map (version {mine}) is behind node {from}'s ({version}), \
                 which forwarded the call, and {why}: nothing ran"
            ))
        };
        let Some(address) = address else {
            return Err(behind(
                "this node's map doesn't list that node to take it from",
            ));
        };
        // At least the sender's version, not just newer than this node's: another pull may
        // have installed a map between the two (audit120's S1 on #1817).
        self.pull_now(from, &address, version - 1)
            .await
            .map_err(|e| behind(&format!("taking it failed: {e}")))
    }

    /// A call from a peer (a `cluster`-scope caller) that says its map is newer: fetch it
    /// from that peer, in the background, once per version (cli692's M4). Anyone else's
    /// headers are ignored.
    fn maybe_pull(self: &Arc<Self>, caller: &Caller, md: &tonic::metadata::MetadataMap) {
        if caller.scope != Scope::Cluster && self.tokens.any() {
            return;
        }
        let header = |k: &str| md.get(k).and_then(|v| v.to_str().ok());
        let (Some(version), Some(from)) = (
            header(MAP_VERSION_HEADER).and_then(|v| v.parse::<u64>().ok()),
            header(NODE_ID_HEADER),
        ) else {
            return;
        };
        let address = {
            let mut st = self.state();
            if version <= st.version() || !st.pulled.insert(version) {
                return;
            }
            let Some(addr) = st.map.as_ref().and_then(|(m, _)| {
                m.nodes
                    .iter()
                    .find(|n| n.id == from)
                    .map(|n| n.address.clone())
            }) else {
                // A node with no map, or one that doesn't list the caller, can't know where
                // to pull from: the caller's next push brings the map.
                st.pulled.remove(&version);
                return;
            };
            addr
        };
        let (node, from) = (Arc::clone(self), from.to_owned());
        tokio::spawn(async move {
            let pulled = node.fetch_map(&address);
            let result = match tokio::time::timeout(node.peers.push_timeout, pulled).await {
                Ok(Ok(map)) => node
                    .push_map_async(map, Some(from.clone()))
                    .await
                    .map(|_| ())
                    .map_err(|e| e.message),
                Ok(Err(e)) => Err(e),
                Err(_) => Err("timed out".to_owned()),
            };
            if let Err(e) = result {
                eprintln!("phdb serve: pulling the cluster map from node {from}: {e}");
                // A later call may try again.
                node.state().pulled.remove(&version);
            }
        });
    }
}

/// The marker a divergent push's refusal carries, so the pusher records the divergence too.
/// Matched in the refusal's message (cli692's N4): a field of its own (a status detail, say)
/// would be sturdier across versions, once a client other than a node needs it.
const DIVERGENT: &str = "divergent map";

/// The marker the term fence's refusal carries (D330), so a writer whose push it refuses
/// looks at the node (#2073). Matched in the message, as [`DIVERGENT`] is.
const VOTED_ABOVE: &str = "this node voted for term";

/// The `Cluster` service over gRPC.
#[derive(Debug, Clone)]
/// The `Cluster` service: the node, and on a node that serves data (not a bare node in a
/// test) the server, which a `Forward` is served through.
struct ClusterGrpc(Arc<Node>, Option<Arc<super::Shared>>);

impl ClusterGrpc {
    fn caller<T>(&self, req: &Request<T>, scopes: &[Scope], what: &str) -> Result<Caller, Status> {
        let caller = self.0.caller(super::grpc::auth_header(req))?;
        self.0.allowed(&caller, scopes, what)?;
        self.0.maybe_pull(&caller, req.metadata());
        Ok(caller)
    }
}

const READERS: &[Scope] = &[Scope::Read, Scope::Cluster];

impl ClusterGrpc {
    /// Partition `id`'s line of `Status`.
    async fn local(&self, id: &str) -> Result<LocalPartition, ApiError> {
        let node = Arc::clone(&self.0);
        let status = tokio::task::spawn_blocking(move || node.status()).await?;
        status
            .partitions
            .into_iter()
            .find(|p| p.id == id)
            .ok_or_else(|| ApiError::not_found(format!("no partition {id:?} on this node")))
    }
}

#[tonic::async_trait]
impl proto::cluster_server::Cluster for ClusterGrpc {
    /// `phdb cluster fence P --node N` (D317 ticket 6): admin scope, or a node's cluster token
    /// (a move the map's writer runs, D332).
    async fn fence(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        // An operator's, or the map's writer's in a move it runs (D332, D331 PR 5).
        self.caller(&req, &[Scope::Admin, Scope::Cluster], "fence a partition")?;
        let id = req.into_inner().partition;
        self.0.fence(&id).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster unfence P --node N` (D317 ticket 6): admin scope, or a node's cluster
    /// token (a move the map's writer runs lifts its own fence on abort, D332).
    async fn unfence(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        // A move the writer runs lifts its own fence on abort (the coordinator, #2094).
        let caller = self.caller(&req, &[Scope::Admin, Scope::Cluster], "unfence a partition")?;
        let req = req.into_inner();
        // `force` skips the check that the peers' newest map still names this node: never
        // a move's abort, so an admin's only (simp724's should on #2094).
        if req.force && caller.scope == Scope::Cluster {
            return Err(ApiError::permission_denied(
                "unfence with --force: a forced unfence is an admin's",
            )
            .into());
        }
        let id = req.partition;
        self.0.unfence(&id, req.force).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster txn decide` (D329 2b): decides a share held in doubt on this node's
    /// partition, for a transaction whose home will never answer. Admin or cluster scope.
    /// It can break the transaction's atomicity, so without `force` it's refused while the
    /// home answers `ResolveTxn` (its resolver settles the share then).
    async fn decide_txn(
        &self,
        req: Request<proto::DecideTxnRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(
            &req,
            &[Scope::Admin, Scope::Cluster],
            "decide a transaction",
        )?;
        let r = req.into_inner();
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        let Some(cluster) = shared.router.cluster.as_ref() else {
            return Err(ApiError::unsupported("this node isn't a cluster node").into());
        };
        let service = {
            let routes = cluster.node.routes();
            let Some(routes) = routes.as_ref() else {
                return Err(
                    ApiError::wrong_partition(0, "this node has no cluster map yet").into(),
                );
            };
            cluster.owned_here(routes, &r.partition)?
        };
        if !r.force {
            let asked = spanning::ask_home(
                &cluster.node,
                &cluster.parts,
                &cluster.forwarder,
                &cluster.spanning,
                &r.xid,
                None,
                shared.limits.request_timeout,
            )
            .await;
            // A home that answers UNKNOWN (its history changed) can't settle the share: the
            // operator's decide is what's left (audit120's S1 on #2076).
            if let Ok(home) = asked
                && home.state != proto::TxnOutcomeState::Unknown as i32
            {
                return Err(ApiError::failed_precondition(format!(
                    "the transaction's home answers ({:?}: {}), so its resolver settles this \
                     share; --force only for a home that will never answer, and it can break \
                     the transaction's atomicity",
                    proto::TxnOutcomeState::try_from(home.state)
                        .unwrap_or(proto::TxnOutcomeState::Unspecified),
                    home.reason
                ))
                .into());
            }
        }
        service
            .decide_branch(&r.xid, r.commit, 0, 0, &cluster.spanning, &r.partition)
            .await?;
        Ok(tonic::Response::new(self.local(&r.partition).await?))
    }

    /// `phdb cluster close P --node N` (D317 amendment 2): admin scope, or a node's cluster
    /// token (a move the map's writer runs, D332).
    async fn close(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(&req, &[Scope::Admin, Scope::Cluster], "close a partition")?;
        let id = req.into_inner().partition;
        self.0.close(&id).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster drop-partition P --node N` (D317 ticket 6): admin scope, or a node's
    /// cluster token (a move the map's writer runs, D332).
    async fn drop_partition(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(
            &req,
            &[Scope::Admin, Scope::Cluster],
            "drop a partition's files",
        )?;
        let id = req.into_inner().partition;
        Ok(tonic::Response::new(self.0.drop_partition(&id).await?))
    }

    /// `Cluster.Stamp` (D317 ticket 6): relocate's and restore's first step on the node the
    /// partition goes to. Cluster scope, as a peer's calls; `phdb cluster relocate` sends it
    /// with the cluster token. The reply's `files` is the set it installed, if any.
    async fn stamp(
        &self,
        req: Request<proto::StampRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(&req, &[Scope::Cluster, Scope::Admin], "stamp a partition")?;
        let req = req.into_inner();
        let id = req.partition.clone();
        let installed = self.0.stamp(req).await?;
        let mut lp = self.local(&id).await?;
        lp.files = installed;
        Ok(tonic::Response::new(lp))
    }

    /// Node to node (D326): a lease round's request for this node's grant. Cluster scope.
    async fn lease(
        &self,
        req: Request<proto::LeaseRequest>,
    ) -> Result<tonic::Response<proto::LeaseResponse>, Status> {
        self.caller(&req, &[Scope::Cluster], "ask for a partition lease")?;
        #[cfg(test)]
        if fence::pre_lease(&self.0.dir) || fence::lease_unrouted(&self.0.dir) {
            return Err(Status::unimplemented("a build before D326"));
        }
        let req = req.into_inner();
        let mine = self.0.get_map()?;
        if req.cluster_id != mine.cluster_id {
            return Err(ApiError::invalid(format!(
                "a lease request for cluster {:?} reached a node of cluster {:?}",
                req.cluster_id, mine.cluster_id
            ))
            .into());
        }
        // A pre-vote is answered as a ballot whatever its term: never as a lease request,
        // which could grant the writer lease it names.
        if req.pre_vote {
            return Ok(tonic::Response::new(self.0.grants.pre_vote(
                &mine,
                &req,
                self.0.peers.failover_after,
            )?));
        }
        if self.0.is_vote(&mine, &req) {
            return Ok(tonic::Response::new(self.0.vote_async(mine, req).await?));
        }
        let r = self.0.grants.grant(&mine, &req)?;
        // The writer notes an asker it answers with its newer map (#2073): one that stays
        // behind may be stranded by the term fence.
        if schema::writer(&mine) == self.0.id {
            let now = self.0.grants.clock().now();
            let mut s = self
                .0
                .stranded
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if r.newer_map_version != 0 {
                s.behind(&req.node_id, now);
            } else {
                s.caught_up(&req.node_id);
            }
        }
        Ok(tonic::Response::new(r))
    }

    /// Node to node (D326 PR 2): a node gave up partitions it closed. Cluster scope.
    async fn release(
        &self,
        req: Request<proto::ReleaseRequest>,
    ) -> Result<tonic::Response<proto::ReleaseResponse>, Status> {
        self.caller(&req, &[Scope::Cluster], "release a partition lease")?;
        #[cfg(test)]
        if fence::pre_lease(&self.0.dir) || fence::lease_unrouted(&self.0.dir) {
            return Err(Status::unimplemented("a build before D326"));
        }
        let req = req.into_inner();
        let mine = self.0.get_map()?;
        if req.cluster_id != mine.cluster_id {
            return Err(ApiError::invalid(format!(
                "a lease release for cluster {:?} reached a node of cluster {:?}",
                req.cluster_id, mine.cluster_id
            ))
            .into());
        }
        self.0.grants.release(&req);
        Ok(tonic::Response::new(proto::ReleaseResponse {}))
    }

    async fn get_map(
        &self,
        req: Request<GetMapRequest>,
    ) -> Result<tonic::Response<ClusterMap>, Status> {
        self.caller(&req, READERS, "read the cluster map")?;
        Ok(tonic::Response::new(self.0.get_map()?))
    }

    async fn set_map(
        &self,
        req: Request<SetMapRequest>,
    ) -> Result<tonic::Response<SetMapResponse>, Status> {
        self.caller(&req, &[Scope::Admin], "set the cluster map")?;
        Ok(tonic::Response::new(
            self.0.set_map(req.into_inner()).await?,
        ))
    }

    async fn push_map(
        &self,
        req: Request<PushMapRequest>,
    ) -> Result<tonic::Response<PushMapResponse>, Status> {
        // Only a node pushes (an admin sets the map with `SetMap`). Without tokens (a
        // loopback test cluster) everyone may.
        self.caller(&req, &[Scope::Cluster], "push a cluster map")?;
        let from = req
            .metadata()
            .get(NODE_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let map = req
            .into_inner()
            .map
            .ok_or_else(|| ApiError::invalid("PushMap needs a map"))?;
        Ok(tonic::Response::new(
            self.0.push_map_async(map, from).await?,
        ))
    }

    async fn status(
        &self,
        req: Request<StatusRequest>,
    ) -> Result<tonic::Response<NodeStatus>, Status> {
        self.caller(&req, READERS, "read the cluster status")?;
        #[cfg(test)]
        while HOLD_STATUS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&self.0.dir)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let node = Arc::clone(&self.0);
        let status = tokio::task::spawn_blocking(move || node.status())
            .await
            .map_err(ApiError::from)?;
        Ok(tonic::Response::new(status))
    }

    /// Node to node: a call another node routed here (D317 ticket 2). Cluster scope; the
    /// call is then checked for the caller it names, and served only if this node's map
    /// gives it the key.
    async fn forward(
        &self,
        req: Request<proto::ForwardRequest>,
    ) -> Result<tonic::Response<proto::ForwardResponse>, Status> {
        let peer = self.caller(&req, &[Scope::Cluster], "forward a call")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        self.0.catch_up(&peer, req.metadata()).await?;
        // The sender's deadline for the call (it sends what's left of its client's), for a
        // replica copy's read-your-writes wait (D328).
        let deadline = crate::serve::replica::ReadDeadline::new(
            std::time::Instant::now(),
            shared.limits.request_timeout,
            crate::serve::grpc::grpc_timeout(req.metadata().as_ref()),
        );
        let msg = req.into_inner();
        // A spanning transaction's branch call (D329): served on the partition it names.
        if spanning::is_branch(&msg) {
            let caller = forward::caller_of(msg.caller, &self.0.tokens)?;
            let Some(cluster) = shared.router.cluster.as_ref() else {
                return Err(ApiError::unsupported("this node isn't a cluster node").into());
            };
            let service = {
                let routes = cluster.node.routes();
                let Some(routes) = routes.as_ref() else {
                    return Err(
                        ApiError::wrong_partition(0, "this node has no cluster map yet").into(),
                    );
                };
                cluster.owned_here(routes, &msg.partition)?
            };
            let op = msg
                .op
                .ok_or_else(|| ApiError::invalid("a forward carries no call"))?;
            let reply = spanning::serve_branch(
                &cluster.node,
                &cluster.spanning,
                service,
                &msg.partition,
                op,
                &caller,
            )
            .await?;
            return Ok(tonic::Response::new(reply));
        }
        // D331 PR 4b: a range move's copy chunk, answered by the source's copy server.
        if let Some(proto::forward_request::Op::RangeCopy(r)) = msg.op {
            let Some(cluster) = shared.router.cluster.as_ref() else {
                return Err(ApiError::unsupported("this node isn't a cluster node").into());
            };
            let chunk = cluster
                .range_copies
                .serve(cluster, &msg.partition, r)
                .await?;
            return Ok(tonic::Response::new(proto::ForwardResponse {
                reply: Some(proto::forward_response::Reply::RangeCopy(chunk)),
                ..Default::default()
            }));
        }
        let op = forward::op_of(msg.op)?;
        // A schema change at the map's writer takes its caller's scope uncapped, from this
        // node's own token entry (D317 amendment 1).
        let caller = if crate::serve::router::map_schema_change(&op) {
            forward::schema_caller_of(msg.caller, &self.0.tokens)?
        } else {
            forward::caller_of(msg.caller, &self.0.tokens)?
        };
        // A read the entry node chose this node's replica copy for (D328).
        let replica = msg.to_replica.then_some(crate::serve::router::ReplicaRead {
            max_lag_ms: msg.replica_max_lag_ms,
            min_primary_seqno: (msg.min_primary_seqno > 0).then_some(msg.min_primary_seqno),
        });
        let (reply, served) = shared
            .router
            .serve_forwarded(
                op,
                &caller,
                msg.fanout_part,
                &msg.partition,
                replica.map(|r| (r, deadline)),
            )
            .await?;
        let (reply, charge) = forward::reply_to_forward(reply, served)?;
        let mut resp = tonic::Response::new(reply);
        // A read's charge lasts until the reply's body is written: `routes` holds it there.
        if let Some(c) = charge {
            resp.extensions_mut().insert(super::charge::Charged::new(c));
        }
        Ok(resp)
    }

    type ForwardSubscribeStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<proto::FeedEvent, Status>> + Send>,
    >;

    /// Served by a route of its own ([`feed::ForwardSubscribeGrpc`]), ahead of this one.
    async fn forward_subscribe(
        &self,
        _req: Request<proto::ForwardSubscribeRequest>,
    ) -> Result<tonic::Response<Self::ForwardSubscribeStream>, Status> {
        Err(Status::unimplemented(
            "this node doesn't serve data, so it has no partition to subscribe to",
        ))
    }

    type ForwardMemoryWatchStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<proto::MemoryEvent, Status>> + Send>,
    >;

    /// Node to node: one partition's component of a memory watch another node merges
    /// (#1915 PR 2b). Cluster scope; served for the caller it names, if this node's map
    /// gives it the partition. Mounted on a route of its own, outside the `Cluster` calls'
    /// small limit, since a watch lasts: it holds a `--max-streams` place on the partition.
    async fn forward_memory_watch(
        &self,
        req: Request<proto::ForwardMemoryWatchRequest>,
    ) -> Result<tonic::Response<Self::ForwardMemoryWatchStream>, Status> {
        let peer = self.caller(&req, &[Scope::Cluster], "forward a memory watch")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        self.0.maybe_pull(&peer, req.metadata());
        self.0.catch_up(&peer, req.metadata()).await?;
        let events = shared
            .router
            .serve_forwarded_memory_watch(req.into_inner())
            .await?;
        Ok(tonic::Response::new(Box::pin(
            crate::serve::grpc::GrpcStream(events),
        )))
    }

    /// Node to node: one partition's part of a `Vectors.BulkLoad` another node splits by id
    /// (#1722 PR 3). Cluster scope; served for the caller it names, as this node's own load
    /// of the partition, if this node's map gives it the partition. Mounted on a route of its
    /// own behind `prepay`, outside the `Cluster` calls' small limit (a load runs for
    /// minutes), so each message reserves its share of the in-flight budget as it arrives.
    async fn forward_bulk_load(
        &self,
        req: Request<tonic::Streaming<proto::ForwardBulkLoadRequest>>,
    ) -> Result<tonic::Response<proto::Empty>, Status> {
        let peer = self.caller(&req, &[Scope::Cluster], "forward a bulk load")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        self.0.catch_up(&peer, req.metadata()).await?;
        let (budget, meter) = crate::serve::grpc::stream_budget(&req);
        let limits = shared.limits;
        let wait = crate::serve::grpc::grpc_timeout(req.metadata().as_ref())
            .map_or(limits.request_timeout, |d| d.min(limits.request_timeout));
        let mut stream = req.into_inner();
        let Some(first) = stream.message().await? else {
            return Err(ApiError::invalid(
                "a forwarded BulkLoad's first message names its partition and index",
            )
            .into());
        };
        let caller = forward::caller_of(first.caller, &self.0.tokens)?;
        let load = first
            .load
            .ok_or_else(|| ApiError::invalid("a forwarded BulkLoad carries no load"))?;
        shared
            .router
            .serve_forwarded_bulk_load(
                &first.partition,
                load,
                budget,
                crate::serve::grpc::ForwardSource { stream, meter },
                &caller,
                wait,
            )
            .await?;
        Ok(tonic::Response::new(proto::Empty {}))
    }

    type ForwardScanStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<proto::ScanPage, Status>> + Send>>;

    /// Node to node: one partition's part of a scan another node fans out (D317 ticket 4a).
    /// Cluster scope; served for the caller it names, as this node's own scan of the
    /// partition, if this node's map gives it the partition.
    async fn forward_scan(
        &self,
        req: Request<proto::ForwardScanRequest>,
    ) -> Result<tonic::Response<Self::ForwardScanStream>, Status> {
        let peer = self.caller(&req, &[Scope::Cluster], "forward a scan")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        self.0.catch_up(&peer, req.metadata()).await?;
        let msg = req.into_inner();
        let caller = forward::caller_of(msg.caller, &self.0.tokens)?;
        let scan = msg
            .scan
            .ok_or_else(|| ApiError::invalid("a forwarded scan carries no scan"))?;
        let pages = shared
            .router
            .serve_forwarded_scan(
                &msg.partition,
                scan,
                &caller,
                msg.to_replica.then_some(msg.replica_max_lag_ms),
            )
            .await?;
        Ok(tonic::Response::new(Box::pin(super::grpc::GrpcStream(
            pages,
        ))))
    }
}

fn http_caller(
    node: &Node,
    headers: &HeaderMap,
    scopes: &[Scope],
    what: &str,
) -> Result<(), ApiError> {
    let caller = node.caller(crate::serve::http::auth_header(headers))?;
    node.allowed(&caller, scopes, what)
}

async fn http_get_map(State(node): State<Arc<Node>>, headers: HeaderMap) -> Response {
    match http_caller(&node, &headers, READERS, "read the cluster map")
        .and_then(|()| node.get_map())
    {
        Ok(m) => axum::Json(m).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn http_set_map(
    State(node): State<Arc<Node>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let run = async {
        http_caller(&node, &headers, &[Scope::Admin], "set the cluster map")?;
        let req: SetMapRequest = serde_json::from_slice(&body)
            .map_err(|e| ApiError::invalid(format!("not a SetMapRequest: {e}")))?;
        node.set_map(req).await
    };
    match run.await {
        Ok(r) => axum::Json(r).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn http_status(State(node): State<Arc<Node>>, headers: HeaderMap) -> Response {
    if let Err(e) = http_caller(&node, &headers, READERS, "read the cluster status") {
        return e.into_response();
    }
    match tokio::task::spawn_blocking(move || node.status()).await {
        Ok(s) => axum::Json(s).into_response(),
        Err(e) => ApiError::internal(e.to_string()).into_response(),
    }
}

/// The `Cluster` service's routes, gRPC and HTTP, which the data listener mounts beside the
/// data routes (D317 ticket 2). Outside the server's load shedding, with a small limit of
/// their own: a node busy with data must still take a map, or a re-forward's pull fails
/// exactly when it matters (cli692's 3). `Forward` itself is a data call: an owner that
/// sheds it answers `Overloaded`, which the forwarding node passes back.
pub(crate) fn routes(
    node: Arc<Node>,
    shared: Option<Arc<super::Shared>>,
    max_request_bytes: usize,
) -> axum::Router {
    let idle = shared.as_ref().map(|s| s.limits.stream_idle);
    // A cluster subscription's parts last as long as it does: outside the limit below, each
    // holding a `--max-streams` place instead (D317 ticket 7).
    // So do a memory watch's (#1915 PR 2b): the `Cluster` service again, on that one path.
    let streams = shared.as_ref().map(|shared| {
        let feeds = crate::serve::grpc::EventsGrpc(feed::ForwardSubscribeGrpc {
            node: Arc::clone(&node),
            shared: Arc::clone(shared),
            max_request_bytes,
        });
        let watches = proto::cluster_server::ClusterServer::new(ClusterGrpc(
            Arc::clone(&node),
            Some(Arc::clone(shared)),
        ))
        .max_decoding_message_size(max_request_bytes.saturating_add(forward::FORWARD_ENVELOPE));
        // A BulkLoad's part (#1722 PR 3): a client stream, metered by `prepay` as a client's
        // `Vectors.BulkLoad` is, and outside the limit below, since a load runs for minutes.
        let loads = tower::ServiceBuilder::new()
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(shared),
                crate::serve::grpc::prepay,
            ))
            .service(
                proto::cluster_server::ClusterServer::new(ClusterGrpc(
                    Arc::clone(&node),
                    Some(Arc::clone(shared)),
                ))
                .max_decoding_message_size(
                    max_request_bytes.saturating_add(forward::FORWARD_ENVELOPE),
                ),
            );
        (feeds, watches, loads)
    });
    // A forward carries a call the entry node took within its `--max-request-bytes`, inside
    // an envelope (audit120's S-C2).
    let grpc = proto::cluster_server::ClusterServer::new(ClusterGrpc(Arc::clone(&node), shared))
        .max_decoding_message_size(max_request_bytes.saturating_add(forward::FORWARD_ENVELOPE));
    let mut app = axum::Router::new()
        .route("/v1/cluster/map", get(http_get_map).post(http_set_map))
        .route("/v1/cluster/status", get(http_status))
        .with_state(node)
        .route_service("/pigeonhole.v1.Cluster/{*method}", grpc);
    // A forwarded read's charge (#1567) moves into its reply's body, as the data routes'
    // `prepay` does, so it's held until the reply is written (cli692's F1 on ticket 2).
    if let Some(idle) = idle {
        app = app.layer(axum::middleware::map_response(
            move |resp: axum::response::Response| async move { super::charge::hold(resp, idle) },
        ));
    }
    let app = app.layer(tower::limit::ConcurrencyLimitLayer::new(CLUSTER_CALLS));
    match streams {
        Some((feeds, watches, loads)) => app
            .route_service(feed::FORWARD_SUBSCRIBE, feeds)
            .route_service(memwatch::FORWARD_MEMORY_WATCH, watches)
            .route_service(crate::serve::grpc::FORWARD_BULK_LOAD, loads),
        None => app,
    }
}

/// `Cluster` calls a node runs at once: map reads and installs, pushes and pulls are few.
const CLUSTER_CALLS: usize = 16;

/// `phdb serve --cluster DIR`.
pub(crate) fn run(
    cli: &crate::args::Cli,
    args: &ServeArgs,
    dir: &Path,
    stderr: &mut dyn std::io::Write,
) -> Result<ExitStatus, Failure> {
    // Per-file flags have no meaning across partitions until a ticket defines them
    // (cli692's M1).
    let refused = [
        (
            args.replica_of.is_some(),
            "--replica-of",
            "#1436's promotion path in D317 §6",
        ),
        (
            args.create,
            "--create",
            "D317's ticket 2 (partitions are created by phdb cluster init-partition)",
        ),
    ];
    if let Some((_, flag, ticket)) = refused.iter().find(|(set, _, _)| *set) {
        return Err(Failure::usage(&format!(
            "{flag} can't be used with --cluster yet: it comes with {ticket}"
        )));
    }
    let id = args.node_id.clone().unwrap_or_default();
    if !map::id_ok(&id) {
        return Err(Failure::usage(
            "--node-id is 1-64 of [a-z0-9_-], not starting with -",
        ));
    }
    if !dir.is_dir() {
        return Err(Failure::usage(&format!(
            "--cluster {}: not a directory",
            dir.display()
        )));
    }
    let tokens = super::tokens(args)?;
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            Some(tls::acceptor(cert, key).map_err(|e| Failure::usage(&format!("TLS: {e}")))?)
        }
        _ => None,
    };
    config::check_bind(
        args.listen,
        tls.is_some(),
        tokens.any(),
        args.insecure_no_tls_no_auth,
    )
    .map_err(|e| Failure::usage(&e))?;
    let token = match &args.cluster_token_file {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?
                .trim()
                .to_owned(),
        ),
        None => std::env::var("PHDB_CLUSTER_TOKEN").ok(),
    }
    .filter(|t| !t.is_empty());
    let ca = match &args.cluster_ca {
        Some(path) => Some(tonic::transport::Certificate::from_pem(
            std::fs::read(path)
                .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?,
        )),
        None => None,
    };
    // A renewal round has to fit in a third of a lease, with a push timeout to spare for a
    // node that's down (D326).
    if args.cluster_lease_ms == 0
        || args.cluster_lease_ms < args.cluster_push_timeout_ms.max(1).saturating_mul(3)
    {
        return Err(Failure::usage(&format!(
            "--cluster-lease-ms ({}) must be at least 3 times --cluster-push-timeout-ms ({}): \
             a lease is renewed every third of it, and a renewal can wait a push timeout for \
             a node that's down",
            args.cluster_lease_ms, args.cluster_push_timeout_ms
        )));
    }
    let _lock = lock_dir(dir)?;
    // Without TLS the node dials its peers in the clear, with its cluster token: only on
    // loopback (cli692's R3), whether or not it requires tokens itself.
    let loopback_only = tls.is_none();
    let mut state = MapState::default();
    if let Some(m) = read_map(dir).map_err(|e| Failure::usage(&e))? {
        map::validate(&m, loopback_only)
            .map_err(|e| Failure::usage(&format!("{}: {e}", dir.join(MAP_FILE).display())))?;
        let json = map::canonical_json(&m);
        state.map = Some((map::canonical(&m), json));
    }
    let version = state.version();
    let voted = read_voted(dir).map_err(|e| Failure::usage(&format!("{}: {e}", dir.display())))?;
    let node = Node::new(
        dir,
        &id,
        loopback_only,
        tokens,
        Peers {
            token,
            scheme: if tls.is_some() { "https" } else { "http" },
            ca,
            push_timeout: Duration::from_millis(args.cluster_push_timeout_ms.max(1)),
            lease: Duration::from_millis(args.cluster_lease_ms),
            failover_after: Duration::from_millis(match args.cluster_failover_after_ms {
                0 => args.cluster_lease_ms.saturating_mul(2),
                ms => ms,
            }),
            failover_max_lag: (args.cluster_failover_max_lag_ms > 0)
                .then(|| Duration::from_millis(args.cluster_failover_max_lag_ms)),
            balance_window: Duration::from_secs(args.balance_window),
            load_interval: load::INTERVAL,
        },
        state,
        voted,
    );
    node.load_fences().map_err(|e| Failure::usage(&e))?;
    // The partitions the map gives this node, in id order, with their epochs: none before
    // a first map, or when the map gives none (the node only routes).
    let mine: Vec<(String, u64)> = {
        let routes = node.routes();
        match routes.as_ref() {
            None => Vec::new(),
            Some(r) => {
                let mut mine: Vec<(String, u64)> = r
                    .partitions_of(&id)
                    .map(|p| (p.to_owned(), r.owner(p).map_or(0, |o| o.epoch)))
                    .collect();
                mine.sort_unstable();
                mine
            }
        }
    };
    let cluster_id = node.state().map.as_ref().map(|(m, _)| m.cluster_id.clone());
    // The node's budget (D317 ticket 8a, §10, perf287's P1): `--shards` and
    // `--compaction-threads` are the node's totals, split across its partitions.
    let total_shards = args.shards.unwrap_or_else(super::placement::default_shards);
    let total_compaction = args
        .compaction_threads
        .unwrap_or_else(|| super::placement::default_compaction_threads(total_shards));
    let map_now = node.state().map.as_ref().map(|(m, _)| m.clone());
    let (shares, memories) = start_budget(
        map_now.as_ref(),
        &mine,
        total_shards,
        total_compaction,
        args.engine_memory,
    )?;
    // With several partitions, the node's compaction CPUs are reserved before any engine
    // opens: the CPUs right after all the partitions' shards in the pin order, and every
    // partition's compaction threads are confined to them (ICR 0134). Engines opened one
    // after another would otherwise pin shard, shard, compaction for each, interleaving the
    // shards' cores, so the network side's mask spanned both L3 domains of the AX102 and
    // its threads ran at 0.70 IPC against 0.91 with one partition (perf287's g75 on #1482).
    let reserved = (mine.len() > 1)
        .then(|| super::placement::compaction_reserve(&shares, total_compaction))
        .flatten();
    // What every partition's database takes, at the start and when one opens later.
    let served = super::served_options(cli, args);
    let opened = open_partitions(
        dir,
        cluster_id.as_deref(),
        &mine,
        false,
        Budget {
            shares: &shares,
            reserved: reserved.as_deref(),
            // Unset, the library's own defaults, exactly as before.
            memories: memories.as_deref(),
        },
        |share, memory| {
            let feed = args.change_feed.map(|t| {
                let all = shares.iter().map(|s| s.shards).sum();
                super::placement::feed_share(t, share.shards, all)
            });
            open::options_for(&served, share, memory, feed, false)
        },
        stderr,
    )?;
    if let Some(total) = args.change_feed {
        let parts: Vec<(&str, &pigeonhole::Pigeonhole)> =
            opened.iter().map(|o| (o.id.as_str(), &o.db)).collect();
        open::log_feed(total, &parts, "at start");
    }
    let dbs: Vec<&pigeonhole::Pigeonhole> = opened.iter().map(|o| &o.db).collect();
    let mut placement = if dbs.is_empty() {
        super::placement::Placement::routes_only(args.io_threads, args.blocking_threads)
    } else {
        super::placement::Placement::plan(
            &dbs,
            args.io_threads,
            args.blocking_threads,
            args.feed_threads,
            args.feed_hub_threads,
            args.affinity == super::config::Affinity::On,
        )
    };
    // Compaction threads confined to the node's reserved CPUs run there as pinned ones
    // would: the startup line counts those CPUs too (perf287's 8b run).
    if let Some(r) = reserved.as_deref()
        && opened.iter().any(|o| o.pinning.compaction_confined > 0)
    {
        placement.confined_compaction(r);
    }
    let placement = Arc::new(placement);
    let limits = args.limits();
    // D329: a decision record must outlive any transaction that could still ask for it
    // (audit120's S2 on the design).
    let floor =
        limits.leases.txn_lifetime + limits.request_timeout + std::time::Duration::from_secs(60);
    if limits.txn_outcome_retention < floor {
        return Err(Failure::usage(&format!(
            "--txn-outcome-retention {:?} is below --txn-lifetime-ms + the request timeout + 1 \
             minute ({floor:?}): a transaction's outcome must outlive its session",
            limits.txn_outcome_retention
        )));
    }
    let metrics = Arc::new(super::service::Metrics::default());
    // The node's service, which serves no partition itself: its budgets, pools, session
    // caps and metrics are every partition's (D317 ticket 8a), as is its one `Backups`
    // (simp724's N1 on #1711).
    let service = super::service::Service::routing_only(
        limits,
        placement.blocking_threads,
        Arc::clone(&metrics),
        Some(Arc::clone(&placement)),
    )
    .memory_opt(None, args.memory_calls.unwrap_or(0))
    .text_pack(args.text_pack);
    // `--memory` (#1915): each partition the map places the store on opens its own, with
    // this node's embedder and options, when a reconcile finds it placed. The map may place
    // it later; it can't place another store.
    let recipe = super::memory_recipe(args, &placement)?;
    if let (Some(r), Ok(map)) = (&recipe, node.get_map())
        && let Some(m) = &map.memory
        && m.store != r.name
    {
        return Err(Failure::usage(&format!(
            "--memory {}: the cluster map places memory store {:?}, not this one",
            r.name, m.store
        )));
    }
    let backups = super::backup::Backups::new(args.backup_dir.as_deref(), args.max_backups)
        .map_err(|e| Failure::usage(&e))?;
    let parts: Vec<(String, super::service::Service)> = opened
        .iter()
        .map(|o| -> Result<_, Failure> {
            // Fenced from the start (D317 ticket 6), until a peer's map confirms it.
            let epoch = mine.iter().find(|(p, _)| *p == o.id).map_or(0, |(_, e)| *e);
            let gate = node.open_gate(&o.id, epoch);
            // D331: its range gate, before it serves.
            node.arm_ranges(&gate, &o.id, &o.db, || ())
                .map_err(|e| Failure::usage(&format!("partition {}: {e}", o.id)))?;
            let config = super::service::PartitionConfig {
                backups: Arc::clone(&backups),
                placement: Some(Arc::clone(&placement)),
                pinning: o.pinning,
                engine_memory: Some(o.memory),
            };
            let part = service.for_partition(o.db.clone(), &o.id, gate, config);
            // A text index rebuild a fence or a shutdown stopped carries on once the
            // partition takes writes (#1756).
            part.resume_text_rebuilds();
            Ok((o.id.clone(), part))
        })
        .collect::<Result<_, _>>()?;
    let serving = if mine.is_empty() {
        format!(
            "no partition, routing only, as cluster node {id} of {}; map version {version}",
            dir.display()
        )
    } else {
        let list: Vec<String> = mine
            .iter()
            .map(
                |(p, epoch)| match (node.fence_of(p), node.awaiting_peers(p)) {
                    (Some(crate::serve::gate::Fence::Operator), true) => format!(
                        "{p} (epoch {epoch}, fenced: phdb cluster fence, and waiting for its \
                         lease)"
                    ),
                    (Some(crate::serve::gate::Fence::Operator), false) => {
                        format!("{p} (epoch {epoch}, fenced: phdb cluster fence)")
                    }
                    (Some(_), _) => {
                        format!("{p} (epoch {epoch}, waiting for its lease)")
                    }
                    (None, _) => format!("{p} (epoch {epoch})"),
                },
            )
            .collect();
        format!(
            "partition{} {} as cluster node {id} of {}; map version {version}",
            if mine.len() > 1 { "s" } else { "" },
            list.join(", "),
            dir.display()
        )
    };
    let shared = Arc::new(super::Shared {
        router: super::router::Router::cluster(service, Arc::clone(&node), parts)
            .max_uploads(args.max_uploads)
            .range_moves(
                args.move_rate_bytes,
                args.move_retention_cap,
                args.max_move_followers,
            )
            .memory_recipe(recipe),
        // The data calls' tokens: the same file the node's own calls read.
        tokens: super::tokens(args)?,
        limits,
        metrics,
        feed_runtimes: std::sync::OnceLock::new(),
    });
    // A partition a map gives the node while it serves opens as the start's do (D317
    // ticket 6), with its share of the same totals.
    // Its partitions' schema follows the map from here (D317 ticket 5): reconciled now, and
    // on every map it takes.
    open::attach(
        &node,
        open::Server {
            shared: Arc::downgrade(&shared),
            placement: Arc::clone(&placement),
            backups: Arc::clone(&backups),
            served,
            shards: total_shards,
            compaction: total_compaction,
            engine_memory: args.engine_memory,
            change_feed: args.change_feed,
            reserved: reserved.clone(),
            replica_run: args.replica_run.max(1),
        },
    );
    // A node that restarts with a map takes a newer one from its peers, then pushes; and
    // D329's resolver starts once it serves, as do the range moves it follows as a target
    // (D331 PR 4b), each from its progress rows.
    let resolver =
        spanning::start_resolver(Arc::downgrade(&shared), shared.limits.txn_resolve_delay);
    let sync = start_sync(&node, version);
    let follows = Arc::clone(&node);
    let start: Option<Box<dyn FnOnce() + Send>> = Some(Box::new(move || {
        if let Some(sync) = sync {
            sync();
        }
        resolver();
        follows.start_range_follows();
    }));
    super::serve_shared(
        super::Serving {
            args,
            shared,
            placement,
            tls,
            serving,
            replica: None,
            start,
        },
        stderr,
    )
}

/// The node's start split of its budget across `mine`, the partitions the map gives it
/// (D317 ticket 8a): each one's share of `shards` and `compaction`, and, when
/// `--engine-memory` or the map sets any, each one's memory: the map's `memory_bytes` off
/// the top, the rest of `engine_memory` in equal shares. A restart re-splits it across
/// every partition the map gives the node, which is how a node a run-time open refused
/// for memory takes that partition (D317 ticket 6).
pub(crate) fn start_budget(
    map: Option<&ClusterMap>,
    mine: &[(String, u64)],
    shards: usize,
    compaction: usize,
    engine_memory: Option<u64>,
) -> Result<
    (
        Vec<super::placement::Share>,
        Option<Vec<super::placement::Memory>>,
    ),
    Failure,
> {
    let shares = super::placement::split(shards, compaction, mine.len());
    let overrides: Vec<u64> = mine
        .iter()
        .map(|(p, _)| {
            map.and_then(|m| m.partitions.iter().find(|q| q.id == *p))
                .map_or(0, |q| q.memory_bytes)
        })
        .collect();
    let set_memory = engine_memory.is_some() || overrides.iter().any(|&m| m > 0);
    let memories = super::placement::memory(
        engine_memory,
        &shares
            .iter()
            .zip(&overrides)
            .map(|(s, &m)| (s.shards, m))
            .collect::<Vec<_>>(),
    )
    .map_err(|(i, why)| match i.and_then(|i| mine.get(i)) {
        Some((p, _)) => Failure::usage(&format!("partition {p}'s engine memory: {why}")),
        None => Failure::usage(&format!("--engine-memory: {why}")),
    })?;
    Ok((shares, set_memory.then_some(memories)))
}

/// How a node's partitions share its budget as they open (D317 ticket 8a).
#[derive(Clone, Copy)]
struct Budget<'a> {
    /// Each partition's threads, in the partitions' order.
    shares: &'a [super::placement::Share],
    /// The node's reserved compaction CPUs, with several partitions and pinning.
    reserved: Option<&'a [usize]>,
    /// Each partition's memory, when `--engine-memory` or the map sets any.
    memories: Option<&'a [super::placement::Memory]>,
}

/// One partition opened on this node (D317 ticket 8a).
struct Opened {
    id: String,
    db: pigeonhole::Pigeonhole,
    pinning: super::service::Pinning,
    memory: super::placement::Memory,
}

/// Opens the partitions `mine` (with their epochs) in order, each checked against the map
/// and given its share of the node's budget (D317 ticket 8a): partitions past the node's
/// compaction total share the compaction CPUs the earlier ones pinned (ICR 0134,
/// perf287's A1). Each one's thread placement is printed; an engine with threads unpinned
/// where pinning works is a warning. A partition that fails to open fails the node's start,
/// after closing those already open (cli692's M2).
fn open_partitions(
    dir: &Path,
    cluster_id: Option<&str>,
    mine: &[(String, u64)],
    replica: bool,
    budget: Budget<'_>,
    options: impl Fn(super::placement::Share, Option<super::placement::Memory>) -> pigeonhole::Options,
    stderr: &mut dyn std::io::Write,
) -> Result<Vec<Opened>, Failure> {
    let Some(cluster_id) = cluster_id else {
        return Ok(Vec::new());
    };
    let mut opened: Vec<Opened> = Vec::new();
    // A partition that fails to open fails the node, closing those already open (M2).
    let fail = |opened: Vec<Opened>, e: Failure| {
        for o in opened {
            let _ = o.db.close();
        }
        Err(e)
    };
    let mut compaction_cpus: Vec<usize> = Vec::new();
    let Budget {
        shares,
        reserved,
        memories,
    } = budget;
    for (i, ((id, epoch), share)) in mine.iter().zip(shares).enumerate() {
        let planned = memories.map(|m| m[i]);
        let open = |memory: Option<super::placement::Memory>, set: &[usize]| {
            let mut o = options(*share, memory);
            // The node's reserved compaction CPUs, for every partition; without them (one
            // partition, or no pinning), only the partitions past the node's compaction total
            // share the earlier ones' (perf287's A1).
            match reserved {
                Some(r) if share.compaction > 0 => o = o.compaction_cpu_set(r.to_vec()),
                Some(_) => {}
                None if share.confined && !set.is_empty() => {
                    o = o.compaction_cpu_set(set.to_vec());
                }
                None => {}
            }
            if replica {
                open_replica_copy(dir, cluster_id, id, o).map(Some)
            } else {
                open_partition(dir, cluster_id, id, *epoch, o)
            }
        };
        let mut db = match open(planned, &compaction_cpus) {
            Ok(Some(db)) => db,
            // Stamped past the map: a restore or relocation onto this node that stopped
            // before its map was set. Not this map's copy, so not served, but the node
            // starts: the rerun needs it, and the map that names that epoch opens it
            // (audit120's P2-C4). Status shows the epochs.
            Ok(None) => {
                let _ = writeln!(
                    stderr,
                    "phdb serve: partition {id}: its file is stamped at an epoch above the \
                     map's {epoch} (a restore or relocation that stopped before its map was \
                     set): left unopened until a map names that epoch. Run the restore or \
                     relocation again"
                );
                continue;
            }
            Err(e) => return fail(opened, e),
        };
        // A partition whose change feed recorded a shard count keeps it, whatever its share
        // (ICR 0132): its memory is then resized for the count it has, and reopened with it
        // (cli692's F2 on #1718), so its memtables stay within its share.
        let actual = db.info().map_or(share.shards, |i| i.shards);
        let memory = match planned {
            Some(m) if m.shards != actual => {
                let bytes = m.bytes();
                let resized = match super::placement::Memory::of(bytes, actual) {
                    Ok(r) => r,
                    Err(least) => {
                        let _ = db.close();
                        return fail(
                            opened,
                            Failure::usage(&format!(
                                "partition {id}: its change feed keeps {actual} shards, and its \
                                 {bytes} bytes of engine memory can't cover their memtables: it \
                                 needs at least {least}"
                            )),
                        );
                    }
                };
                if let Err(e) = db.close() {
                    return fail(opened, Failure::db("close the partition to resize it", &e));
                }
                db = match open(Some(resized), &compaction_cpus) {
                    Ok(Some(db)) => db,
                    Ok(None) => {
                        return fail(
                            opened,
                            Failure::usage(&format!(
                                "partition {id}'s file changed epoch while it reopened"
                            )),
                        );
                    }
                    Err(e) => return fail(opened, e),
                };
                resized
            }
            Some(m) => m,
            None => super::placement::Memory::defaults(actual),
        };
        compaction_cpus.extend(db.pinned_compaction_cpus());
        let pinning = super::service::Pinning::of(&db, share.compaction);
        let _ = writeln!(
            stderr,
            "phdb serve: partition {id}: {} shard threads ({} pinned), {} compaction threads \
             ({} pinned, {} on the node's compaction CPUs); {} bytes of memtables ({} per \
             shard), a {}-byte block cache and a {}-byte row cache",
            pinning.shards,
            pinning.shards_pinned,
            pinning.compaction,
            pinning.compaction_pinned,
            pinning.compaction_confined,
            memory.memtables(),
            memory.memtable_per_shard,
            memory.block_cache,
            memory.row_cache,
        );
        // Where pinning works at all (Linux), an engine left unpinned is worth saying: its
        // threads can land on another partition's shards or the network side's CPUs.
        if cfg!(target_os = "linux")
            && (pinning.shards_unpinned() > 0 || pinning.compaction_unpinned() > 0)
        {
            let _ = writeln!(
                stderr,
                "phdb serve: WARNING: partition {id}: {} shard and {} compaction threads \
                 run unpinned ({}): give the node fewer --shards or --compaction-threads, or \
                 more CPUs",
                pinning.shards_unpinned(),
                pinning.compaction_unpinned(),
                db.engine_metrics()
                    .unpinned_reason
                    .unwrap_or("the OS refused some pins"),
            );
        }
        opened.push(Opened {
            id: id.clone(),
            db,
            pinning,
            memory,
        });
    }
    Ok(opened)
}

/// For a node that starts with a map at `version`: what runs once it serves, taking its
/// peers' newer maps ([`Node::sync_on_start`]). Until that's done, the node changes no map
/// as the writer (D317 amendment 1), so a writer back after a forced recovery takes the
/// forced map, which no longer names it, before anything else. `None` for a node without a
/// map yet.
pub(crate) fn start_sync(node: &Arc<Node>, version: u64) -> Option<Box<dyn FnOnce() + Send>> {
    (version > 0).then(|| {
        node.synced
            .store(false, std::sync::atomic::Ordering::Release);
        let n = Arc::clone(node);
        Box::new(move || {
            tokio::spawn(async move {
                n.sync_on_start().await;
                // Its replica copies (D328) open as a running node opens them, now that it
                // serves and holds its peers' newest map.
                n.open_missing().await;
            });
        }) as Box<dyn FnOnce() + Send>
    })
}

/// The options a cluster node opens its partition with: `served`, the options every
/// database the server opens takes ([`served_options`](super::served_options): the shard
/// poll, WAL preallocation, `--shm-dir` and `--change-feed`, as a single node's), with its
/// share of the node's budget (D317 ticket 8a). A partition whose feed is on keeps it
/// without the flag (D317 ticket 7), and its feed's recorded shard count.
pub(crate) fn partition_options(
    served: pigeonhole::Options,
    share: super::placement::Share,
) -> pigeonhole::Options {
    // Its share of the node's budget (D317 ticket 8a): the shard count as a default, so a
    // partition whose feed recorded a count keeps it (ICR 0132), never resharding it.
    served
        .default_shards(share.shards)
        .compaction_cores(share.compaction)
}

/// Opens the database at `path` for writing, retrying a moment while a read-only open of
/// the file holds its writer lock (an operator's `phdb info`, another node's tooling): 50 ms,
/// doubling, about 1.5 s in all, then the refusal stands (#1770).
pub(crate) fn open_retrying(
    path: &Path,
    options: &pigeonhole::Options,
) -> pigeonhole::Result<pigeonhole::Pigeonhole> {
    let mut wait = std::time::Duration::from_millis(50);
    loop {
        match pigeonhole::Pigeonhole::open(path, options.clone()) {
            Err(e)
                if e.code() == pigeonhole::ErrorCode::WriterLocked && wait.as_millis() <= 800 =>
            {
                std::thread::sleep(wait);
                wait *= 2;
            }
            opened => return opened,
        }
    }
}

/// Opens this node's replica copy of `partition` in `dir` (D328): `DIR/P.phdb`, created
/// empty when missing, for its follower's bootstrap to fill. A file that's this cluster's
/// and partition's at any epoch (a copy keeps the epoch it bootstrapped at until its
/// follower records the owner's), or has no identity row yet, is the copy. One of another
/// cluster or partition is refused, and so is one that isn't a replica's and has tables: a
/// moved-away owner's file, which a bootstrap would replace, so `drop-partition` must move
/// it aside first (a single node's `--replica-of` refuses one so too).
fn open_replica_copy(
    dir: &Path,
    cluster_id: &str,
    partition: &str,
    options: pigeonhole::Options,
) -> Result<pigeonhole::Pigeonhole, Failure> {
    stamp::finish_install(dir, partition).map_err(|e| Failure::usage(&e.message))?;
    let path = dir.join(format!("{partition}.phdb"));
    let db = open_retrying(&path, &options)
        .map_err(|e| Failure::db("open the partition's replica copy", &e))?;
    let refuse = |db: pigeonhole::Pigeonhole, why: String| {
        let _ = db.close();
        Err(Failure::usage(&format!(
            "{} can't be this node's replica of partition {partition:?}: {why}",
            path.display()
        )))
    };
    match identity::read(&db) {
        Ok(Some(id)) if id.cluster_id != cluster_id || id.partition_id != partition => {
            return refuse(
                db,
                format!(
                    "it's partition {:?} of cluster {:?}",
                    id.partition_id, id.cluster_id
                ),
            );
        }
        Ok(_) => {}
        Err(e) => return refuse(db, e),
    }
    let is_replica = match db.replicated_seqno() {
        Ok(applied) => applied.is_some(),
        Err(e) => return refuse(db, format!("reading its replica state: {e}")),
    };
    if !is_replica && !db.tables().is_empty() {
        return refuse(
            db,
            "it isn't a replica's copy and has tables (an owner's file, kept after the \
             partition moved away): move it aside with phdb cluster drop-partition first"
                .to_owned(),
        );
    }
    Ok(db)
}

/// D331 PR 3: a restored file (its stamp left the clip marker) is clipped to the map in
/// `dir` before it serves ([`moves::clip_restored`]); the marker stays until the clip is done,
/// so a clip that stopped runs again at the next open.
fn clip_if_restored(
    dir: &Path,
    db: &pigeonhole::Pigeonhole,
    partition: &str,
) -> Result<(), String> {
    if !crate::serve::ranges::clip_pending(db)? {
        return Ok(());
    }
    let map = read_map(dir)?.ok_or("no cluster map to clip to")?;
    let c = moves::clip_restored(db, &map, partition)?;
    eprintln!(
        "phdb serve: partition {partition}: the restored file is clipped to the map: {} rows \
         outside its ranges deleted{}",
        c.deleted,
        if c.in_doubt == 0 {
            String::new()
        } else {
            format!(
                "; {} cluster transaction(s) in doubt here could still write outside them, so \
                 the clip runs again at the next open: decide them (phdb cluster txn decide), \
                 then reopen the partition",
                c.in_doubt
            )
        }
    );
    Ok(())
}

/// Opens partition `partition`'s file in `dir`, checking its identity row against the map:
/// this cluster's, this partition's, at the map's epoch. A file that says otherwise was
/// stamped for another cluster or partition, or the map moved past it: refused, not served.
/// `None` for this cluster's and partition's file at an epoch **above** the map's: a stamp
/// whose map didn't land yet, which isn't a reason to fail the node (audit120's P2-C4).
fn open_partition(
    dir: &Path,
    cluster_id: &str,
    partition: &str,
    epoch: u64,
    options: pigeonhole::Options,
) -> Result<Option<pigeonhole::Pigeonhole>, Failure> {
    // An install a crash interrupted is finished first: the partition never opens with its
    // marker there (D317 ticket 6, feed1248's F1).
    stamp::finish_install(dir, partition).map_err(|e| Failure::usage(&e.message))?;
    let path = dir.join(format!("{partition}.phdb"));
    if !path.exists() {
        return Err(Failure::usage(&format!(
            "the map gives this node partition {partition}, but {} doesn't exist: create it \
             with phdb cluster init-partition",
            path.display()
        )));
    }
    let db = open_retrying(&path, &options).map_err(|e| Failure::db("open the partition", &e))?;
    let want = identity::Identity {
        cluster_id: cluster_id.to_owned(),
        partition_id: partition.to_owned(),
        epoch,
    };
    match identity::read(&db) {
        Ok(Some(id)) if id == want => match clip_if_restored(dir, &db, partition) {
            Ok(()) => Ok(Some(db)),
            Err(e) => {
                let _ = db.close();
                Err(Failure::usage(&format!(
                    "partition {partition:?}: clipping the restored file to the map: {e}"
                )))
            }
        },
        Ok(Some(id))
            if id.cluster_id == want.cluster_id
                && id.partition_id == want.partition_id
                && id.epoch > epoch =>
        {
            let _ = db.close();
            Ok(None)
        }
        // D331 (simp724's B3): just below a map that hands a range of it over, at that epoch:
        // a crash after the map's persist and before the source's stamp. The open finishes
        // the hand-over (the moved-away row, then the stamp) before it serves. The map's
        // epoch is exactly the hand-over's from the file's: one above, or past a promote's
        // floor (#2070, ai1249's review), so a file staler than that isn't served.
        Ok(Some(id))
            if id.cluster_id == want.cluster_id
                && id.partition_id == want.partition_id
                && id.epoch < epoch
                && let Ok(Some(map)) = read_map(dir)
                && epoch == id.epoch.max(promote::promote_floor(&map, partition)) + 1
                && let Some(m) = moves::handing_over(&map, partition) =>
        {
            match moves::finish_hand_over(&db, m, epoch, dir) {
                Ok(_) => Ok(Some(db)),
                Err(e) => {
                    let _ = db.close();
                    Err(Failure::usage(&format!(
                        "partition {partition:?}: finishing move {:?}'s hand-over: {e}",
                        m.id
                    )))
                }
            }
        }
        found => {
            let _ = db.close();
            let found = match found {
                Ok(Some(id)) => format!(
                    "partition {:?} of cluster {:?} at epoch {}",
                    id.partition_id, id.cluster_id, id.epoch
                ),
                Ok(None) => "no identity row (run phdb cluster init-partition)".to_owned(),
                Err(e) => e,
            };
            Err(Failure::usage(&format!(
                "{} isn't partition {partition:?} of cluster {cluster_id:?} at the map's epoch \
                 {epoch}: it says {found}",
                path.display()
            )))
        }
    }
}

/// Fills `lp`'s in-doubt fields (D329, `LocalPartition` 21–25) from `db`'s shares held in
/// doubt, with each one's reason as the resolver last found it.
fn in_doubt(
    lp: &mut LocalPartition,
    db: &pigeonhole::Pigeonhole,
    partition: &str,
    spanning: Option<&spanning::Spanning>,
) {
    let mut shares = db.in_doubt_shares();
    if shares.is_empty() {
        return;
    }
    // The oldest first.
    shares.sort_by_key(|s| std::cmp::Reverse(s.1));
    let ms = |d: std::time::Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    lp.in_doubt = u32::try_from(shares.len()).unwrap_or(u32::MAX);
    lp.in_doubt_oldest_xid = String::from_utf8_lossy(&shares[0].0).into_owned();
    lp.in_doubt_oldest_age_ms = ms(shares[0].1);
    lp.in_doubt_wal_bytes = shares.iter().map(|s| s.2).sum();
    lp.in_doubt_shares = shares
        .iter()
        .map(|(xid, age, _)| proto::InDoubtShare {
            xid: String::from_utf8_lossy(xid).into_owned(),
            age_ms: ms(*age),
            reason: spanning
                .and_then(|s| s.reason(partition, xid))
                .unwrap_or_else(|| "its decision hasn't arrived yet".to_owned()),
        })
        .collect();
}

/// The partitions whose owner, epoch or history `new` changes from `old` (all of them for a
/// first map): what a copy's follower must look up again (D330 amendment 2).
fn moved_partitions(old: Option<&ClusterMap>, new: &ClusterMap) -> Vec<String> {
    // By id, not a scan per partition (perf2's nit on #2069): this runs under the state lock.
    let before: HashMap<&str, &crate::serve::proto::Partition> = old
        .map(|o| o.partitions.iter().map(|q| (q.id.as_str(), q)).collect())
        .unwrap_or_default();
    new.partitions
        .iter()
        .filter(|p| {
            before.get(p.id.as_str()).is_none_or(|q| {
                q.node != p.node || q.epoch != p.epoch || q.history_epoch != p.history_epoch
            })
        })
        .map(|p| p.id.clone())
        .collect()
}
