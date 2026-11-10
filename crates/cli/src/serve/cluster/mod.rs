//! `phdb serve --cluster DIR --node-id ID` (D317, ICR 0117): a node of a cluster of
//! statically partitioned databases. So far (ticket 1) the node keeps the partition map and
//! distributes it: `Cluster.SetMap` validates, persists and pushes a new map, nodes take a
//! newer map pushed (or pulled) from a peer, and `Cluster.Status` reports the node's map and
//! its partition files. Routing data calls to partitions is ticket 2.
//!
//! **One server per DIR:** `DIR/.lock` is held exclusively while the node runs, and
//! `phdb cluster init-partition` takes it too, so nothing else rewrites the map file or a
//! partition's identity under it.

pub(crate) mod cli;
pub(crate) mod forward;
pub(crate) mod identity;
pub(crate) mod map;
pub(crate) mod route;
#[cfg(test)]
mod tests;

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
        Arc::new(Self {
            dir: dir.to_path_buf(),
            id: id.to_owned(),
            loopback_only,
            tokens,
            peers,
            state: Mutex::new(state),
            installing: Mutex::new(()),
            routes: arc_swap::ArcSwapOption::new(routes),
            pulling: tokio::sync::Mutex::new(()),
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
    /// lock, whether it may replace the current one. `step`: a map being set here, which must
    /// be a valid change from the current one; otherwise a peer's, which only has to be the
    /// same cluster's and newer (`map::validate_catch_up`, cli692's R1). Returns the new
    /// version.
    ///
    /// **Blocking** (address resolution, an fsync): run it on a blocking thread
    /// ([`Node::install_async`]). The state lock is held only to check and to publish; the
    /// persist runs under `installing` alone.
    fn install(
        &self,
        map: &ClusterMap,
        step: bool,
        check: impl FnOnce(&mut MapState) -> Result<Option<u64>, ApiError>,
    ) -> Result<u64, ApiError> {
        map::validate(map, self.loopback_only).map_err(ApiError::invalid)?;
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
                None => {}
                Some((old, _)) if step => {
                    map::validate_change(old, map).map_err(ApiError::invalid)?;
                }
                Some((old, _)) => {
                    map::validate_catch_up(old, map).map_err(ApiError::invalid)?;
                }
            }
        }
        let canonical = map::canonical(map);
        let json = map::canonical_json(&canonical);
        write_map(&self.dir, &json)
            .map_err(|e| ApiError::internal(format!("persist the cluster map: {e}")))?;
        let version = canonical.map_version;
        let routes = Arc::new(route::Routes::new(&canonical));
        let mut st = self.state();
        st.map = Some((canonical, json));
        // Under the state lock, so the routes never run behind the map `GetMap` answers.
        self.routes.store(Some(routes));
        st.divergent.clear();
        // Versions at or below this one need no pull again (cli692's N3).
        st.pulled.retain(|v| *v > version);
        Ok(version)
    }

    /// [`Node::install`] on a blocking thread.
    async fn install_async(
        self: &Arc<Self>,
        map: ClusterMap,
        step: bool,
        check: impl FnOnce(&mut MapState) -> Result<Option<u64>, ApiError> + Send + 'static,
    ) -> Result<u64, ApiError> {
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || node.install(&map, step, check))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?
    }

    /// `Cluster.SetMap`: compare-and-set on the version, then push to the others.
    async fn set_map(self: &Arc<Self>, req: SetMapRequest) -> Result<SetMapResponse, ApiError> {
        let map = req
            .map
            .ok_or_else(|| ApiError::invalid("SetMap needs a map"))?;
        let expected = req.expected_map_version;
        let version = self
            .install_async(map, true, move |st| {
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
            let fetched = tokio::time::timeout(self.peers.push_timeout, async {
                let mut client = self.client(&n.address).await?;
                let req = self.request(GetMapRequest {})?;
                client
                    .get_map(req)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(|s| s.message().to_owned())
            })
            .await;
            if let Ok(Ok(theirs)) = fetched
                && theirs.map_version > self.state().version()
                && let Err(e) = self.push_map_async(theirs, Some(n.id.clone())).await
            {
                eprintln!(
                    "phdb serve: node {}'s newer cluster map: {}",
                    n.id, e.message
                );
            }
        }
        self.push_all().await;
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
        let version = self.install(&map, false, |st| {
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
        Ok(PushMapResponse {
            map_version: version,
        })
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
            };
            match identity::read_file(&path) {
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
                    });
                }
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

#[tonic::async_trait]
impl proto::cluster_server::Cluster for ClusterGrpc {
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

    /// Node to node: a call another node routed here (D317 ticket 2). Cluster scope only.
    /// The node serves it through its router once that lands; until then it's refused.
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
        let caller = forward::caller_of(msg.caller)?;
        let op = forward::op_of(msg.op)?;
        let reply = shared.router.serve_forwarded(op, &caller).await?;
        Ok(tonic::Response::new(forward::reply_to_forward(reply)?))
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
    let grpc = proto::cluster_server::ClusterServer::new(ClusterGrpc(Arc::clone(&node), shared))
        .max_decoding_message_size(max_request_bytes);
    axum::Router::new()
        .route("/v1/cluster/map", get(http_get_map).post(http_set_map))
        .route("/v1/cluster/status", get(http_status))
        .with_state(node)
        .route_service("/pigeonhole.v1.Cluster/{*method}", grpc)
        .layer(tower::limit::ConcurrencyLimitLayer::new(CLUSTER_CALLS))
}

/// `Cluster` calls a node runs at once: map reads and installs, pushes and pulls are few.
const CLUSTER_CALLS: usize = 16;

/// `phdb serve --cluster DIR`.
pub(crate) fn run(
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
            args.backup_dir.is_some(),
            "--backup-dir",
            "D317's ticket 8 (cluster backup)",
        ),
        (
            args.create,
            "--create",
            "D317's ticket 2 (partitions are created by phdb cluster init-partition)",
        ),
        (
            args.change_feed.is_some(),
            "--change-feed",
            "D317's ticket 7 (cluster feeds)",
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
    // The partition the map gives this node, opened and checked against the map; none
    // before a first map, or when the map gives it none (the node only routes). Several
    // per node come with D317's ticket 8.
    let local = {
        let routes = node.routes();
        match routes.as_ref() {
            None => None,
            Some(r) => {
                let mut mine: Vec<&str> = r.partitions_of(&id).collect();
                mine.sort_unstable();
                if mine.len() > 1 {
                    return Err(Failure::usage(&format!(
                        "the map gives node {id} {} partitions ({}): several per node come \
                         with D317's ticket 8",
                        mine.len(),
                        mine.join(", ")
                    )));
                }
                mine.first()
                    .map(|p| ((*p).to_owned(), r.owner(p).map_or(0, |o| o.epoch)))
            }
        }
    };
    let cluster_id = node.state().map.as_ref().map(|(m, _)| m.cluster_id.clone());
    let db = match (&local, &cluster_id) {
        (Some((p, epoch)), Some(c)) => Some(open_partition(dir, c, p, *epoch)?),
        _ => None,
    };
    let placement = Arc::new(match &db {
        Some(db) => super::placement::Placement::plan(
            db,
            args.io_threads,
            args.blocking_threads,
            args.feed_threads,
        ),
        None => super::placement::Placement::routes_only(args.io_threads, args.blocking_threads),
    });
    let limits = args.limits();
    let metrics = Arc::new(super::service::Metrics::default());
    let service = match &db {
        Some(db) => super::service::Service::new(
            db.clone(),
            limits,
            placement.blocking_threads,
            Arc::clone(&metrics),
        )
        .placement(&placement),
        None => super::service::Service::routing_only(
            limits,
            placement.blocking_threads,
            Arc::clone(&metrics),
        ),
    };
    let serving = match &local {
        Some((p, epoch)) => format!(
            "partition {p} (epoch {epoch}) as cluster node {id} of {}; map version {version}",
            dir.display()
        ),
        None => format!(
            "no partition, routing only, as cluster node {id} of {}; map version {version}",
            dir.display()
        ),
    };
    let shared = Arc::new(super::Shared {
        router: super::router::Router::cluster(service, Arc::clone(&node), local.map(|(p, _)| p)),
        // The data calls' tokens: the same file the node's own calls read.
        tokens: super::tokens(args)?,
        limits,
        metrics,
    });
    // A node that restarts with a map takes a newer one from its peers, then pushes.
    let start: Option<Box<dyn FnOnce() + Send>> = (version > 0).then(|| {
        let n = Arc::clone(&node);
        Box::new(move || {
            tokio::spawn(async move {
                n.sync_on_start().await;
            });
        }) as Box<dyn FnOnce() + Send>
    });
    super::serve_shared(
        super::Serving {
            args,
            shared,
            placement,
            tls,
            serving,
            db,
            replica: None,
            start,
        },
        stderr,
    )
}

/// Opens partition `partition`'s file in `dir`, checking its identity row against the map:
/// this cluster's, this partition's, at the map's epoch. A file that says otherwise was
/// stamped for another cluster or partition, or the map moved past it: refused, not served.
fn open_partition(
    dir: &Path,
    cluster_id: &str,
    partition: &str,
    epoch: u64,
) -> Result<pigeonhole::Pigeonhole, Failure> {
    let path = dir.join(format!("{partition}.phdb"));
    if !path.exists() {
        return Err(Failure::usage(&format!(
            "the map gives this node partition {partition}, but {} doesn't exist: create it \
             with phdb cluster init-partition",
            path.display()
        )));
    }
    let db = pigeonhole::Pigeonhole::open(&path, pigeonhole::Options::default())
        .map_err(|e| Failure::db("open the partition", &e))?;
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
