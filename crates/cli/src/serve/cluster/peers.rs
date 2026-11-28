use super::*;

impl Node {
    /// Pushes the node's map to every other node in it, in parallel, each within the push
    /// timeout. A node it misses pulls the map once a call from this node reaches it.
    pub(super) async fn push_all(self: &Arc<Self>) -> Vec<NodePush> {
        let Ok(map) = self.get_map() else {
            return Vec::new();
        };
        self.push_each(&map, &map.nodes).await
    }

    /// Pushes `map` to each of `nodes` but this one, in parallel, each within the push
    /// timeout: each node's outcome, in id order.
    pub(super) async fn push_each(
        self: &Arc<Self>,
        map: &ClusterMap,
        nodes: &[proto::Node],
    ) -> Vec<NodePush> {
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
    pub(super) async fn sync_on_start(self: &Arc<Self>) {
        #[cfg(test)]
        hold_rounds(&self.dir).await;
        let Ok(map) = self.get_map() else {
            return;
        };
        for n in map.nodes.iter().filter(|n| n.id != self.id) {
            let fetched =
                tokio::time::timeout(self.peers.push_timeout, self.fetch_map(&n.address)).await;
            let Ok(Ok(theirs)) = fetched else {
                continue;
            };
            // Newer by `(writer_term, map_version)` (#2159 item 4): an elected writer's map
            // can be at or below this node's version.
            if self
                .get_map()
                .ok()
                .is_none_or(|mine| map::newer(&theirs, &mine))
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

    /// Peer `address`'s map.
    pub(super) async fn fetch_map(&self, address: &str) -> Result<ClusterMap, String> {
        let mut client = self.client(address).await?;
        let req = self.request(GetMapRequest {})?;
        let map = client
            .get_map(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|s| s.message().to_owned())?;
        // The answer of a call the cut overtook isn't taken (#2259): a cut node's fetch in
        // flight would otherwise bring it a map its peers set after the cut.
        #[cfg(test)]
        if cut_off(&self.dir, address) {
            return Err(format!("{address}'s map: cut off (a test's seam)"));
        }
        Ok(map)
    }

    /// [`Node::push_map`] on a blocking thread.
    pub(super) async fn push_map_async(
        self: &Arc<Self>,
        map: ClusterMap,
        from: Option<String>,
    ) -> Result<PushMapResponse, ApiError> {
        let node = Arc::clone(self);
        tokio::task::spawn_blocking(move || node.push_map(map, from.as_deref())).await?
    }

    /// `Cluster.PushMap`: a peer's newer map. `from`: the pushing node's id, if it said.
    /// Blocking ([`Node::install`]).
    pub(super) fn push_map(
        &self,
        map: ClusterMap,
        from: Option<&str>,
    ) -> Result<PushMapResponse, ApiError> {
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

    /// A fault seam's check on receive (tests, #2259): whether node `from`, by its address
    /// in this node's map, is cut apart from this node ([`cut_apart`]). A sender this node's
    /// map doesn't list isn't.
    #[cfg(test)]
    pub(super) fn cut_from(&self, from: &str) -> bool {
        let address = self.state().map.as_ref().and_then(|(m, _)| {
            m.nodes
                .iter()
                .find(|n| n.id == from)
                .map(|n| n.address.clone())
        });
        address.is_some_and(|a| cut_apart(&self.dir, &a))
    }

    /// A peer at `address` (the map's `host:port`).
    pub(super) async fn client(
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
        // Again once connected (#2259): a dial that began before the cut and connected
        // after it would carry a call past the cut.
        #[cfg(test)]
        if cut_off(&self.dir, address) {
            return Err(format!("connecting to {address}: cut off (a test's seam)"));
        }
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
        let channel = self
            .endpoint(address)?
            .http2_keep_alive_interval(Duration::from_secs(30))
            .connect()
            .await
            .map_err(|e| format!("connecting to {address}: {e}"))?;
        // Again once connected, as `client` does (#2259).
        #[cfg(test)]
        if cut_off(&self.dir, address) {
            return Err(format!("connecting to {address}: cut off (a test's seam)"));
        }
        Ok(channel)
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
        // A higher writer term is newer at any version (#2159 item 4): the elected writer's
        // map can be at or below the version this node holds from the deposed one. The early
        // return above compares versions only, which is safe: a map at the same version and
        // another term is pulled, not skipped.
        let higher_term = self
            .get_map()
            .is_ok_and(|mine| map.writer_term > mine.writer_term);
        if map.map_version <= past && !higher_term {
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
        // Versions only: the header carries no term (#2159 item 4). A forwarder at the same
        // version under a higher term isn't pulled from here; the next lease round or push
        // brings that map, and a call routed on a deposed writer's map still meets the
        // owner's own lease and epoch checks (D326), so nothing is served wrongly. A term
        // header is a protocol addition, left for a release that changes the protocol.
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
    pub(super) fn maybe_pull(self: &Arc<Self>, caller: &Caller, md: &tonic::metadata::MetadataMap) {
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
            // Versions only, as `catch_up` compares them (#2159 item 4).
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
