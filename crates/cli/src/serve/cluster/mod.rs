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
pub(crate) mod cli;
pub(crate) mod close;
pub(crate) mod fanout;
pub(crate) mod feed;
pub(crate) mod fence;
pub(crate) mod forward;
pub(crate) mod identity;
pub(crate) mod idle;
pub(crate) mod map;
pub(crate) mod moving;
pub(crate) mod open;
pub(crate) mod position;
mod restore;
pub(crate) mod route;
pub(crate) mod schema;
pub(crate) mod stamp;
#[cfg(test)]
mod tests;
pub(crate) mod token;
pub(crate) mod upload;

use std::collections::{BTreeSet, HashSet};
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
/// The lock file that keeps a directory to one server (or one `init-partition`).
pub(crate) const LOCK_FILE: &str = ".lock";
/// The header a node's calls to its peers carry its map version in.
const MAP_VERSION_HEADER: &str = "x-pigeonhole-map-version";
/// The header a node's calls to its peers carry its id in, so a pull knows whom to ask.
const NODE_ID_HEADER: &str = "x-pigeonhole-node-id";

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

/// Writes `json` as `dir`'s map file: a temporary file, synced, renamed over the old one,
/// then the directory synced, so a crash leaves the old map or the new one, never a torn
/// file.
fn write_map(dir: &Path, json: &str) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = dir.join(format!("{MAP_FILE}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(json.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(MAP_FILE))?;
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
    /// A serving node's schema reconcile (D317 ticket 5), run on every install before the
    /// map's routes serve: [`schema::attach`] sets it.
    reconciler: std::sync::OnceLock<Reconciler>,
    /// Each partition's last reconcile, by id: the map version, and its outcome, for
    /// `Status`.
    reconciled: Mutex<std::collections::BTreeMap<String, (u64, schema::Outcome)>>,
    /// The newest map version reconciled here, written under `installing`: an older map is
    /// never reconciled after it, so a stale one can't drop a table a newer one created
    /// (audit120's F6).
    reconciled_newest: std::sync::atomic::AtomicU64,
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
    /// How the node opens a partition while it serves ([`open::attach_opener`]).
    opener: std::sync::OnceLock<open::Opener>,
    /// One open or close of a partition at a time (D317 ticket 6): the parts' swaps take
    /// it. Taken after `fences.changing` where both are.
    opening: tokio::sync::Mutex<()>,
    /// This node, for the tasks an install starts.
    me: std::sync::Weak<Node>,
}

/// A node's partitions' reconcile to a map, each partition's outcome by id; `None` once the
/// server is gone.
/// The second argument names a partition whose operator's fence is being lifted, which
/// reconciles although its gate still says fenced.
pub(crate) type Reconcile =
    dyn Fn(&ClusterMap, Option<&str>) -> Option<Vec<(String, schema::Outcome)>> + Send + Sync;

/// [`Reconcile`], boxed for [`Node::reconciler`].
pub(crate) struct Reconciler(pub(crate) Box<Reconcile>);

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Reconciler")
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
    ) -> Arc<Self> {
        let routes = state
            .map
            .as_ref()
            .map(|(m, _)| Arc::new(route::Routes::new(m)));
        Arc::new_cyclic(|me| Self {
            dir: dir.to_path_buf(),
            id: id.to_owned(),
            loopback_only,
            tokens,
            peers,
            state: Mutex::new(state),
            installing: Mutex::new(()),
            routes: arc_swap::ArcSwapOption::new(routes),
            pulling: tokio::sync::Mutex::new(()),
            reconciler: std::sync::OnceLock::new(),
            reconciled: Mutex::new(std::collections::BTreeMap::new()),
            reconciled_newest: std::sync::atomic::AtomicU64::new(0),
            synced: std::sync::atomic::AtomicBool::new(true),
            schema_changes: tokio::sync::Mutex::new(()),
            fences: fence::Fences::default(),
            opener: std::sync::OnceLock::new(),
            opening: tokio::sync::Mutex::new(()),
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
        if !self.tokens.any() {
            return Ok(Caller::anonymous());
        }
        super::auth::bearer(header)
            .and_then(|t| self.tokens.caller(t))
            .ok_or_else(ApiError::unauthenticated)
    }

    /// Whether `caller` may make a call that any of `scopes` allows. Without tokens (a
    /// loopback cluster, `--insecure-no-tls-no-auth`) everyone may do everything, pushing a
    /// map included.
    fn allowed(&self, caller: &Caller, scopes: &[Scope], what: &str) -> Result<(), ApiError> {
        if !self.tokens.any() || scopes.iter().any(|&s| caller.scope.allows(s)) {
            Ok(())
        } else {
            Err(ApiError::permission_denied(what))
        }
    }

    /// The node's map, or `NotFound` before it has one.
    fn get_map(&self) -> Result<ClusterMap, ApiError> {
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
        st.map = Some((canonical, json));
        // Under the state lock, so the routes never run behind the map `GetMap` answers.
        self.routes.store(Some(routes));
        st.divergent.clear();
        // Versions at or below this one need no pull again (cli692's N3).
        st.pulled.retain(|v| *v > version);
        drop(st);
        // A partition the map now gives this node opens (D317 ticket 6, §3).
        self.open_missing_soon();
        self.end_moved_feeds(version);
        Ok(version)
    }

    /// Ends the cluster feed components of every partition open here that the map,
    /// installed at `version`, gives another node: a relocation that didn't close it here
    /// (a fenced partition stays open) would otherwise leave them following this copy,
    /// which takes no more writes. Their subscriptions open them again where the map puts
    /// them (audit120's S-C5). After the routes are stored, so a component opened meanwhile
    /// finds the new map when it checks it again ([`feed::serve`]).
    fn end_moved_feeds(&self, version: u64) {
        let Some(shared) = self.opener.get().and_then(|o| o.shared.upgrade()) else {
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
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?
    }

    /// `Cluster.SetMap`: compare-and-set on the version, then push to the others. Only the
    /// map's writer, the node with the smallest id in its current map, sets a map (D317
    /// amendment 1); another answers `WrongPartition` naming it, and `phdb cluster set-map`
    /// follows. `force` (the forced recovery, for a writer that's down) skips the writer and
    /// the compare-and-set: the map need only be the same cluster's, at a version above this
    /// node's.
    async fn set_map(self: &Arc<Self>, req: SetMapRequest) -> Result<SetMapResponse, ApiError> {
        let map = req
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
        }
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
        let pushed = self.push_all().await;
        Ok(SetMapResponse {
            map_version: version,
            pushed,
        })
    }

    /// Pushes the node's map to every other node in it, in parallel, each within the push
    /// timeout. A node it misses pulls the map once a call from this node reaches it.
    async fn push_all(self: &Arc<Self>) -> Vec<NodePush> {
        let Ok(map) = self.get_map() else {
            return Vec::new();
        };
        let mut tasks = tokio::task::JoinSet::new();
        for n in map.nodes.iter().filter(|n| n.id != self.id) {
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
            // Its partitions' start fences (D317 ticket 6).
            self.confirm(&theirs);
        }
        self.push_all().await;
        self.synced
            .store(true, std::sync::atomic::Ordering::Release);
        // Partitions no peer has confirmed yet: asked again until one does (cli692's S3).
        self.confirm_after_start().await;
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
        tokio::task::spawn_blocking(move || node.push_map(map, from.as_deref()))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?
    }

    /// `Cluster.PushMap`: a peer's newer map. `from`: the pushing node's id, if it said.
    /// Blocking ([`Node::install`]).
    fn push_map(&self, map: ClusterMap, from: Option<&str>) -> Result<PushMapResponse, ApiError> {
        let pushed = from.is_some().then(|| map.clone());
        let version = self.push_map_install(map, from)?;
        // A peer's map, taken or the same as this node's: its partitions' start fences
        // (D317 ticket 6).
        if let Some(theirs) = pushed {
            self.confirm(&theirs);
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
            if map.map_version != old.map_version {
                return Ok(None);
            }
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
            .opener
            .get()
            .and_then(|o| o.shared.upgrade())
            .map(|shared| shared.router.partitions().into_iter().collect())
            .unwrap_or_default();
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
        // Each open partition's state, and a closed one's files (D317 ticket 6).
        for lp in &mut partitions {
            if let Some((state, reason)) = self.part_state(&lp.id) {
                lp.set_state(state);
                lp.set_fence_reason(reason);
            } else if let Some(closed) = self.fences.closed().get(&lp.id) {
                closed.describe(lp);
            }
        }
        NodeStatus {
            node_id: self.id.clone(),
            map_version: version,
            cluster_id,
            partitions,
            divergent,
        }
    }

    /// A peer at `address` (the map's `host:port`).
    async fn client(
        &self,
        address: &str,
    ) -> Result<proto::cluster_client::ClusterClient<tonic::transport::Channel>, String> {
        let channel = self
            .endpoint(address)?
            .connect()
            .await
            .map_err(|e| format!("connecting to {address}: {e}"))?;
        Ok(proto::cluster_client::ClusterClient::new(channel))
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
        let pulled = async {
            let mut client = self.client(address).await?;
            let req = self.request(GetMapRequest {})?;
            client
                .get_map(req)
                .await
                .map(tonic::Response::into_inner)
                .map_err(|s| s.message().to_owned())
        };
        let map = tokio::time::timeout(self.peers.push_timeout, pulled)
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
            let pulled = async {
                let mut client = node.client(&address).await?;
                let req = node.request(GetMapRequest {})?;
                client
                    .get_map(req)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(|s| s.message().to_owned())
            };
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

/// The `Cluster` service over gRPC.
#[derive(Debug, Clone)]
/// The `Cluster` service: the node, and on a node that serves data (not a bare node in a
/// test) the server, which a `Forward` is served through.
struct ClusterGrpc(Arc<Node>, Option<Arc<super::Shared>>);

impl ClusterGrpc {
    fn caller<T>(&self, req: &Request<T>, scopes: &[Scope], what: &str) -> Result<Caller, Status> {
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        let caller = self.0.caller(header)?;
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
        let status = tokio::task::spawn_blocking(move || node.status())
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        status
            .partitions
            .into_iter()
            .find(|p| p.id == id)
            .ok_or_else(|| ApiError::not_found(format!("no partition {id:?} on this node")))
    }
}

#[tonic::async_trait]
impl proto::cluster_server::Cluster for ClusterGrpc {
    /// `phdb cluster fence P --node N` (D317 ticket 6): admin scope.
    async fn fence(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(&req, &[Scope::Admin], "fence a partition")?;
        let id = req.into_inner().partition;
        self.0.fence(&id).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster unfence P --node N` (D317 ticket 6): admin scope.
    async fn unfence(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(&req, &[Scope::Admin], "unfence a partition")?;
        let req = req.into_inner();
        let id = req.partition;
        self.0.unfence(&id, req.force).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster close P --node N` (D317 amendment 2): admin scope.
    async fn close(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(&req, &[Scope::Admin], "close a partition")?;
        let id = req.into_inner().partition;
        self.0.close(&id).await?;
        Ok(tonic::Response::new(self.local(&id).await?))
    }

    /// `phdb cluster drop-partition P --node N` (D317 ticket 6): admin scope.
    async fn drop_partition(
        &self,
        req: Request<proto::PartitionRequest>,
    ) -> Result<tonic::Response<proto::LocalPartition>, Status> {
        self.caller(&req, &[Scope::Admin], "drop a partition's files")?;
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
        let node = Arc::clone(&self.0);
        let status = tokio::task::spawn_blocking(move || node.status())
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        Ok(tonic::Response::new(status))
    }

    /// Node to node: a call another node routed here (D317 ticket 2). Cluster scope; the
    /// call is then checked for the caller it names, and served only if this node's map
    /// gives it the key.
    async fn forward(
        &self,
        req: Request<proto::ForwardRequest>,
    ) -> Result<tonic::Response<proto::ForwardResponse>, Status> {
        self.caller(&req, &[Scope::Cluster], "forward a call")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        let msg = req.into_inner();
        let op = forward::op_of(msg.op)?;
        // A schema change at the map's writer takes its caller's scope uncapped, from this
        // node's own token entry (D317 amendment 1).
        let caller = match op {
            crate::serve::service::ServiceOp::CreateTable(_)
            | crate::serve::service::ServiceOp::DropTable(_) => {
                forward::schema_caller_of(msg.caller, &self.0.tokens)?
            }
            _ => forward::caller_of(msg.caller, &self.0.tokens)?,
        };
        let reply = shared
            .router
            .serve_forwarded(op, &caller, msg.fanout_part)
            .await?;
        let (reply, charge) = forward::reply_to_forward(reply)?;
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

    type ForwardScanStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<proto::ScanPage, Status>> + Send>>;

    /// Node to node: one partition's part of a scan another node fans out (D317 ticket 4a).
    /// Cluster scope; served for the caller it names, as this node's own scan of the
    /// partition, if this node's map gives it the partition.
    async fn forward_scan(
        &self,
        req: Request<proto::ForwardScanRequest>,
    ) -> Result<tonic::Response<Self::ForwardScanStream>, Status> {
        self.caller(&req, &[Scope::Cluster], "forward a scan")?;
        let Some(shared) = &self.1 else {
            return Err(ApiError::unsupported("this node doesn't serve data").into());
        };
        let msg = req.into_inner();
        let caller = forward::caller_of(msg.caller, &self.0.tokens)?;
        let scan = msg
            .scan
            .ok_or_else(|| ApiError::invalid("a forwarded scan carries no scan"))?;
        let pages = shared
            .router
            .serve_forwarded_scan(&msg.partition, scan, &caller)
            .await?;
        Ok(tonic::Response::new(Box::pin(super::grpc::GrpcPages(
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
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let caller = node.caller(header)?;
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
    let streams = shared.as_ref().map(|shared| feed::ForwardSubscribeGrpc {
        node: Arc::clone(&node),
        shared: Arc::clone(shared),
        max_request_bytes,
    });
    let grpc = proto::cluster_server::ClusterServer::new(ClusterGrpc(Arc::clone(&node), shared))
        .max_decoding_message_size(max_request_bytes);
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
        Some(svc) => app.route_service(feed::FORWARD_SUBSCRIBE, svc),
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
            args.memory.memory.is_some(),
            "--memory",
            "a later D317 ticket",
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
        },
        state,
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
    let opened = open_partitions(
        dir,
        cluster_id.as_deref(),
        &mine,
        Budget {
            shares: &shares,
            reserved: reserved.as_deref(),
            // Unset, the library's own defaults, exactly as before.
            memories: memories.as_deref(),
        },
        |share, memory| {
            let options = partition_options(super::served_options(cli, args), share);
            match memory {
                Some(m) => m.apply(options),
                None => options,
            }
        },
        stderr,
    )?;
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
    let metrics = Arc::new(super::service::Metrics::default());
    // The node's service, which serves no partition itself: its budgets, pools, session
    // caps and metrics are every partition's (D317 ticket 8a), as is its one `Backups`
    // (simp724's N1 on #1711).
    let service = super::service::Service::routing_only(
        limits,
        placement.blocking_threads,
        Arc::clone(&metrics),
    )
    .placement(&placement);
    let backups = super::backup::Backups::new(args.backup_dir.as_deref(), args.max_backups)
        .map_err(|e| Failure::usage(&e))?;
    let parts: Vec<(String, super::service::Service)> = opened
        .iter()
        .map(|o| {
            // Fenced from the start (D317 ticket 6), until a peer's map confirms it.
            let epoch = mine.iter().find(|(p, _)| *p == o.id).map_or(0, |(_, e)| *e);
            let gate = node.open_gate(&o.id, epoch);
            let part = service
                .for_partition(o.db.clone(), &o.id, &backups, o.pinning, o.memory, gate)
                .placement(&placement);
            (o.id.clone(), part)
        })
        .collect();
    let serving = if mine.is_empty() {
        format!(
            "no partition, routing only, as cluster node {id} of {}; map version {version}",
            dir.display()
        )
    } else {
        let list: Vec<String> = mine
            .iter()
            .map(|(p, epoch)| match node.fence_of(p) {
                Some(crate::serve::gate::Fence::Operator) => {
                    format!("{p} (epoch {epoch}, fenced: phdb cluster fence)")
                }
                Some(crate::serve::gate::Fence::Start) => {
                    format!("{p} (epoch {epoch}, fenced: waiting for a peer's map)")
                }
                None => format!("{p} (epoch {epoch})"),
            })
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
            .max_uploads(args.max_uploads),
        // The data calls' tokens: the same file the node's own calls read.
        tokens: super::tokens(args)?,
        limits,
        metrics,
        feed_runtimes: std::sync::OnceLock::new(),
    });
    // A partition a map gives the node while it serves opens as the start's do (D317
    // ticket 6), with its share of the same totals.
    let served = super::served_options(cli, args);
    open::attach_opener(
        &node,
        open::Opener {
            shared: Arc::downgrade(&shared),
            placement: Arc::clone(&placement),
            backups: Arc::clone(&backups),
            options: Box::new(move |share, memory| {
                let options = partition_options(served.clone(), share);
                match memory {
                    Some(m) => m.apply(options),
                    None => options,
                }
            }),
            shards: total_shards,
            compaction: total_compaction,
            engine_memory: args.engine_memory,
        },
    );
    // Its partition's schema follows the map (D317 ticket 5): reconciled now, and on every
    // map it takes.
    schema::attach(&node, &shared);
    // A node that restarts with a map takes a newer one from its peers, then pushes.
    let start = start_sync(&node, version);
    super::serve_shared(
        super::Serving {
            args,
            shared,
            placement,
            tls,
            serving,
            dbs: opened.into_iter().map(|o| o.db).collect(),
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
            open_partition(dir, cluster_id, id, *epoch, o)
        };
        let mut db = match open(planned, &compaction_cpus) {
            Ok(db) => db,
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
                    Ok(db) => db,
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

/// Opens partition `partition`'s file in `dir`, checking its identity row against the map:
/// this cluster's, this partition's, at the map's epoch. A file that says otherwise was
/// stamped for another cluster or partition, or the map moved past it: refused, not served.
fn open_partition(
    dir: &Path,
    cluster_id: &str,
    partition: &str,
    epoch: u64,
    options: pigeonhole::Options,
) -> Result<pigeonhole::Pigeonhole, Failure> {
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
        Ok(Some(id)) if id == want => Ok(db),
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
