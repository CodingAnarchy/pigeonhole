//! Schema in the map (D317 §5, ticket 5): a cluster's tables and families live in the
//! partition map, and each node reconciles its partitions' files to it.
//!
//! - **The calls:** `Schema.CreateTable` and `DropTable` on a cluster node are map changes,
//!   made by the map's one writer (D317 amendment 1): the node with the smallest id in the
//!   map. Any other node forwards the call to it, with the client's scope checked first
//!   (simp724's S3) and checked again there against the writer's own entry for the token.
//!   The writer applies the change to its current map and sets it, re-reading on a version
//!   conflict (S2). `ListTables` and `DescribeTable` answer from the entry node's map.
//! - **Reconcile** runs whenever the node takes a map, and at its start. It creates what the
//!   map places on its partitions and the files lack, and drops what the map dropped.
//! - **What the map dropped** is known from the file alone (simp724's S1): each table the map
//!   made is recorded in the partition's `__phdb_cluster` table with the map version that
//!   created it. A recorded table the map no longer places here, or places with another
//!   `created`, is dropped. An unrecorded one is a hand-made table: left alone if the map
//!   doesn't place it, adopted if it does. A crash between a table and its record just
//!   redoes the step.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use pigeonhole::{ErrorCode, Family, Pigeonhole};

use super::Node;
use super::identity;
use crate::serve::auth::Caller;
use crate::serve::errors::ApiError;
use crate::serve::proto::ForwardRequest;
use crate::serve::proto::{
    self, ClusterMap, FamilySpec, SetMapRequest, SplitKey, TablePlacement, split_key,
};
use crate::serve::router::Cluster;
use crate::serve::router::Router;
use crate::serve::service::{ServiceOp, ServiceReply};

/// The family of `__phdb_cluster` holding the schema records.
const FAMILY: &str = "schema";
/// A record's row: this prefix and the table's name.
const PREFIX: &[u8] = b"schema/";
/// A record's one cell: the map version that created the table.
const CREATED: &[u8] = b"created";
/// Compare-and-set tries for one schema call before it answers `Conflict`.
const TRIES: usize = 5;

/// What one reconcile did and found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub created: Vec<String>,
    pub dropped: Vec<String>,
    /// Hand-made tables a create through the map took over (simp724's nit): the map now owns
    /// them, a later drop included.
    pub adopted: Vec<String>,
    /// Tables in the file the map doesn't place: made by hand, left alone.
    pub unmapped: Vec<String>,
    pub errors: Vec<String>,
}

/// The schema records in `db`: each recorded table's `created`.
fn records(db: &Pigeonhole) -> Result<BTreeMap<String, u64>, String> {
    let t = match db.table(identity::TABLE).and_then(|t| t.open()) {
        Ok(t) => t,
        Err(e) if e.code() == ErrorCode::TableNotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(format!("read {}: {e}", identity::TABLE)),
    };
    if !t.families().contains(&FAMILY) {
        return Ok(BTreeMap::new());
    }
    let mut out = BTreeMap::new();
    let rows = t
        .scan_prefix(PREFIX)
        .family(FAMILY)
        .iter()
        .map_err(|e| format!("read the schema records: {e}"))?;
    for row in rows {
        let row = row.map_err(|e| format!("read the schema records: {e}"))?;
        let name = String::from_utf8_lossy(&row.key()[PREFIX.len()..]).into_owned();
        let created = row
            .get(FAMILY, CREATED)
            .and_then(|c| std::str::from_utf8(c.value()).ok()?.parse().ok())
            .ok_or_else(|| format!("the schema record of {name:?} is malformed"))?;
        out.insert(name, created);
    }
    Ok(out)
}

fn records_table(db: &Pigeonhole) -> Result<pigeonhole::Table, String> {
    db.table(identity::TABLE)
        .and_then(|b| b.family(FAMILY, Family::default()).create_if_missing())
        .map_err(|e| format!("open the schema records: {e}"))
}

fn record(db: &Pigeonhole, name: &str, created: u64) -> Result<(), String> {
    let row = [PREFIX, name.as_bytes()].concat();
    records_table(db)?
        .mutate(&row)
        .put(FAMILY, CREATED, created.to_string().as_bytes())
        .commit()
        .map(|_| ())
        .map_err(|e| format!("record table {name:?}: {e}"))
}

fn unrecord(db: &Pigeonhole, name: &str) -> Result<(), String> {
    let row = [PREFIX, name.as_bytes()].concat();
    records_table(db)?
        .mutate(&row)
        .delete_row()
        .commit()
        .map(|_| ())
        .map_err(|e| format!("drop the record of {name:?}: {e}"))
}

/// Whether `have` (a family as stored) agrees with `want` (the map's): every option the map
/// sets is the stored one.
fn agrees(want: &FamilySpec, have: &FamilySpec) -> bool {
    (want.kind.is_none() || want.kind == have.kind)
        && want
            .max_versions
            .is_none_or(|v| Some(v) == have.max_versions)
        && want.ttl_micros.is_none_or(|v| Some(v) == have.ttl_micros)
        && want.bloom_bits.is_none_or(|v| Some(v) == have.bloom_bits)
        && want
            .blob_threshold
            .is_none_or(|v| Some(v) == have.blob_threshold)
        && want.block_size.is_none_or(|v| Some(v) == have.block_size)
        && (want.compression.is_none() || want.compression == have.compression)
        && (want.cache_priority == 0 || want.cache_priority == have.cache_priority)
        && (want.compaction == 0 || want.compaction == have.compaction)
}

/// Brings partition `partition`'s file `db` to `map`'s schema. Never fails: what it couldn't
/// do is in the outcome's `errors`, for `Status` and the log.
pub(crate) fn reconcile(db: &Pigeonhole, partition: &str, map: &ClusterMap) -> Outcome {
    let mut o = Outcome::default();
    let mut recorded = match records(db) {
        Ok(r) => r,
        Err(e) => {
            o.errors.push(e);
            return o;
        }
    };
    let placed: BTreeMap<&str, &TablePlacement> = map
        .tables
        .iter()
        .filter(|t| t.partitions.iter().any(|p| p == partition))
        .map(|t| (t.name.as_str(), t))
        .collect();
    // What the map dropped: a recorded table it no longer places here, or places as another
    // incarnation. The table first, then its record.
    let gone: Vec<String> = recorded
        .iter()
        .filter(|(name, created)| {
            placed
                .get(name.as_str())
                .is_none_or(|t| t.created != **created)
        })
        .map(|(name, _)| name.clone())
        .collect();
    for name in gone {
        match db.drop_table(&name) {
            Ok(()) => o.dropped.push(name.clone()),
            Err(e) if e.code() == ErrorCode::TableNotFound => {}
            Err(e) => {
                o.errors.push(format!("drop table {name:?}: {e}"));
                continue;
            }
        }
        match unrecord(db, &name) {
            Ok(()) => {
                recorded.remove(&name);
            }
            Err(e) => o.errors.push(e),
        }
    }
    let have: BTreeSet<String> = db.tables().into_iter().collect();
    for (name, t) in &placed {
        let families: Result<Vec<(String, Family)>, String> = t
            .families
            .iter()
            .map(|f| {
                crate::serve::convert::family_in(f)
                    .map(|fam| (f.name.clone(), fam))
                    .map_err(|e| format!("table {name:?}, family {:?}: {}", f.name, e.message))
            })
            .collect();
        let families = match families {
            Ok(f) => f,
            Err(e) => {
                o.errors.push(e);
                continue;
            }
        };
        let exists = have.contains(*name);
        // A placement set by hand, before its families: nothing to create yet.
        if !exists && families.is_empty() {
            continue;
        }
        let mut b = match db.table(name) {
            Ok(b) => b,
            Err(e) => {
                o.errors.push(format!("table {name:?}: {e}"));
                continue;
            }
        };
        for (f, fam) in families {
            b = b.family(&f, fam);
        }
        let table = match b.create_if_missing() {
            Ok(t) => t,
            Err(e) => {
                o.errors.push(format!("create table {name:?}: {e}"));
                continue;
            }
        };
        for want in &t.families {
            if let Some(info) = table.family_info(&want.name)
                && !agrees(want, &crate::serve::convert::family_out(&info))
            {
                o.errors.push(format!(
                    "table {name:?}'s family {:?} has other options in this partition than in the map",
                    want.name
                ));
            }
        }
        match recorded.get(*name) {
            Some(_) => {}
            None => {
                if !exists {
                    o.created.push((*name).to_owned());
                } else if t.created > 0 {
                    o.adopted.push((*name).to_owned());
                }
                if let Err(e) = record(db, name, t.created) {
                    o.errors.push(e);
                }
            }
        }
    }
    let after: BTreeSet<String> = db.tables().into_iter().collect();
    o.unmapped = after
        .into_iter()
        .filter(|n| !n.starts_with(super::map::RESERVED_PREFIX))
        .filter(|n| !placed.contains_key(n.as_str()) && !recorded.contains_key(n))
        .collect();
    o
}

/// Wires a serving node's reconcile: whenever `node` takes a map, each of its partitions'
/// files is reconciled to it through that partition's own service (D317 ticket 8a, simp724's
/// N1 on #1720), on the installing thread, before the map's routes serve, and that service's
/// cached handles go. Runs once now, for the map the node starts with.
pub(crate) fn attach(node: &Arc<Node>, shared: &Arc<crate::serve::Shared>) {
    // Attached on a node that only routes too: a partition may open there later (ticket 6).
    if shared.router.cluster.is_none() {
        return;
    }
    let weak = Arc::downgrade(shared);
    let run = move |map: &ClusterMap, unfencing: Option<&str>| -> Option<Vec<(String, Outcome)>> {
        let shared = weak.upgrade()?;
        let parts = shared.router.cluster.as_ref()?.parts.load_full();
        let mut outcomes = Vec::with_capacity(parts.len());
        for (partition, service) in parts.iter() {
            let Ok(part) = service.part() else {
                continue;
            };
            // An operator's fence (D317 ticket 6) leaves the files as they are: the
            // partition takes the map's schema when it's unfenced or reopened, and Status
            // shows its reconcile behind the map meanwhile. A start fence doesn't: the
            // partition's tables must be there when it's unfenced. Counted on the gate
            // first, then the fence read, so a fence's drain waits for a reconcile it
            // didn't stop.
            let held = service.gate().map(|g| g.hold());
            if let Some((_, Some(crate::serve::gate::Fence::Operator))) = held
                && unfencing != Some(partition.as_str())
            {
                continue;
            }
            // The partition's schema lock, as a single node's create and drop take it: no
            // create or drop of its own runs across a reconcile.
            let _schema = part.schema.blocking_lock();
            let outcome = reconcile(&part.db, partition, map);
            if !outcome.created.is_empty() || !outcome.dropped.is_empty() {
                service.tables_changed();
            }
            for name in &outcome.adopted {
                eprintln!(
                    "phdb serve: cluster map version {}: table {name:?} in partition {partition} \
                     was made by hand; a create through the map took it over, and the map now \
                     owns it (a drop through the map drops it)",
                    map.map_version
                );
            }
            for e in &outcome.errors {
                eprintln!(
                    "phdb serve: reconciling partition {partition} to map version {}: {e}",
                    map.map_version
                );
            }
            outcomes.push((partition.clone(), outcome));
        }
        Some(outcomes)
    };
    let _ = node.reconciler.set(super::Reconciler(Box::new(run)));
    drop(node.reconcile_current(None));
}

impl Node {
    /// Reconciles this node's partitions to its current map, under the lock an install
    /// holds while it reconciles, so no install's reconcile runs between this one's map and
    /// its end (audit120's F6: an open that reconciled to the map it started with dropped a
    /// table created meanwhile, with its rows). `unfencing` names a partition whose
    /// operator's fence is being lifted: it reconciles too. Returns the lock, for a caller
    /// that must act before the next install's reconcile (`unfence` lifts the gate under
    /// it). Blocking: run it on a blocking thread.
    pub(crate) fn reconcile_current(
        &self,
        unfencing: Option<&str>,
    ) -> std::sync::MutexGuard<'_, ()> {
        let installing = self
            .installing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Ok(map) = self.get_map() {
            self.reconcile_locked(&installing, &map, unfencing);
        }
        installing
    }

    /// Reconciles this node's partitions to `map`, if a server attached them, and keeps each
    /// one's outcome for `Status`. The caller holds `installing` (`_installing`), so
    /// reconciles run one at a time, and a map older than the newest one reconciled is
    /// refused: a stale map would drop the tables created since (audit120's F6).
    pub(super) fn reconcile_locked(
        &self,
        _installing: &std::sync::MutexGuard<'_, ()>,
        map: &ClusterMap,
        unfencing: Option<&str>,
    ) {
        use std::sync::atomic::Ordering;
        let newest = self.reconciled_newest.load(Ordering::Relaxed);
        if map.map_version < newest {
            eprintln!(
                "phdb serve: not reconciling to cluster map version {}: version {newest} is \
                 reconciled already",
                map.map_version
            );
            return;
        }
        let Some(run) = self.reconciler.get() else {
            return;
        };
        self.reconciled_newest
            .store(map.map_version, Ordering::Relaxed);
        if let Some(outcomes) = (run.0)(map, unfencing) {
            let mut reconciled = self
                .reconciled
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (partition, outcome) in outcomes {
                reconciled.insert(partition, (map.map_version, outcome));
            }
        }
    }
}

/// The map's writer (D317 amendment 1): the node with the smallest id. It alone changes the
/// map, so two changes through two nodes can't both become the same version.
pub(crate) fn writer(map: &ClusterMap) -> &str {
    map.nodes
        .iter()
        .map(|n| n.id.as_str())
        .min()
        .unwrap_or_default()
}

/// A placement request's split keys as the map writes them (canonicalised when installed).
fn split_keys(splits: &[prost::bytes::Bytes]) -> Vec<SplitKey> {
    splits
        .iter()
        .map(|k| SplitKey {
            key: Some(split_key::Key::Raw(k.clone())),
        })
        .collect()
}

impl Router {
    /// `Schema.*` on a cluster node (D317 ticket 5), after the caller's scope is checked as a
    /// single node checks it.
    pub(crate) async fn cluster_schema(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        prepaid: Option<tokio::sync::OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        let _request = self.service.admit(&op.admit(), caller, prepaid).await?;
        let node = &cluster.node;
        match op {
            ServiceOp::ListTables(_) => {
                let map = node.get_map()?;
                let mut tables: Vec<String> = map.tables.iter().map(|t| t.name.clone()).collect();
                tables.sort();
                Ok(ServiceReply::Tables(proto::ListTablesResponse { tables }))
            }
            ServiceOp::DescribeTable(r) => {
                let map = node.get_map()?;
                let t = map
                    .tables
                    .iter()
                    .find(|t| t.name == r.name)
                    .ok_or_else(|| {
                        ApiError::from_code(
                            ErrorCode::TableNotFound,
                            &format!("no table {:?} in the cluster map", r.name),
                        )
                    })?;
                Ok(ServiceReply::Table(proto::TableResponse {
                    name: t.name.clone(),
                    families: t.families.clone(),
                    unreconciled: Vec::new(),
                }))
            }
            op @ (ServiceOp::CreateTable(_) | ServiceOp::DropTable(_)) => {
                self.at_writer(cluster, op, caller, deadline).await
            }
            _ => Err(ApiError::internal("cluster_schema takes a schema call")),
        }
    }

    /// A schema change at the map's writer (D317 amendment 1): made here if this node is
    /// the writer, otherwise forwarded to it. A writer that answers `WrongPartition` has a
    /// newer map naming another writer: this node pulls it and sends once more. A change
    /// whose answer is lost after it was sent answers `OutcomeUnknown` (B3): a create with
    /// `if_missing` is safe to send again, and a drop that went through answers
    /// `TableNotFound` the second time.
    async fn at_writer(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
        // From the request's arrival, as a data call's forward has it (simp724's N2 on
        // #1720): the send, and after a `WrongPartition` the pull and the second send.
        deadline: tokio::time::Instant,
    ) -> Result<ServiceReply, ApiError> {
        let node = &cluster.node;
        let left = || {
            deadline
                .checked_duration_since(tokio::time::Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(ApiError::deadline_exceeded)
        };
        let op = super::forward::op_to_forward(op)?;
        let mut pulled = false;
        loop {
            let map = node.get_map()?;
            let w = writer(&map);
            if w == node.id() {
                return self
                    .write_schema(node, super::forward::op_of(Some(op))?)
                    .await;
            }
            let address = map
                .nodes
                .iter()
                .find(|n| n.id == w)
                .map(|n| n.address.clone())
                .unwrap_or_default();
            let req = ForwardRequest {
                fanout_part: false,
                map_version: map.map_version,
                caller: Some(super::forward::forwarded_caller(caller)?),
                op: Some(op.clone()),
            };
            match cluster.forwarder.send(&address, req, left()?, true).await {
                Err(e) if e.code == "WrongPartition" && !pulled => {
                    pulled = true;
                    tokio::time::timeout(left()?, node.pull_now(w, &address, map.map_version))
                        .await
                        .map_err(|_| ApiError::deadline_exceeded())?
                        .map_err(|why| {
                            ApiError::partition_unavailable(format!(
                                "node {w} has a newer cluster map, which this node couldn't \
                                 take: {why}"
                            ))
                        })?;
                }
                reply => return super::forward::reply_of(reply?),
            }
        }
    }

    /// A schema change another node forwarded to this one as the map's writer (D317
    /// amendment 1). `caller` is this node's own entry for the token, uncapped
    /// ([`super::forward::schema_caller_of`]), so a drop's `admin` is checked here. Made
    /// only if this node's map names it the writer, otherwise `WrongPartition` with this
    /// node's map version: never forwarded on.
    pub(crate) async fn serve_forwarded_schema(
        &self,
        cluster: &Cluster,
        op: ServiceOp,
        caller: &Caller,
    ) -> Result<ServiceReply, ApiError> {
        let _request = self.service.admit(&op.admit(), caller, None).await?;
        let node = &cluster.node;
        let map = node.get_map()?;
        let w = writer(&map);
        if w != node.id() {
            return Err(ApiError::wrong_partition(
                map.map_version,
                format!("node {w} is the cluster map's writer, not this node"),
            ));
        }
        self.write_schema(node, op).await
    }

    /// A schema change made here, at the writer.
    async fn write_schema(
        &self,
        node: &Arc<Node>,
        op: ServiceOp,
    ) -> Result<ServiceReply, ApiError> {
        match op {
            ServiceOp::CreateTable(r) => {
                self.cluster_create(node, r).await.map(ServiceReply::Table)
            }
            ServiceOp::DropTable(r) => {
                self.cluster_drop(node, r.name).await?;
                Ok(ServiceReply::Empty(proto::Empty {}))
            }
            _ => Err(ApiError::internal("write_schema takes a schema change")),
        }
    }

    /// One schema change as a compare-and-set on the map: `change` makes the new map from the
    /// current one (`None`: nothing to change). A conflict re-reads the map and runs `change`
    /// again (simp724's S2). The new map and the nodes the push missed.
    async fn change_map(
        node: &Arc<Node>,
        mut change: impl FnMut(&ClusterMap) -> Result<Option<ClusterMap>, ApiError>,
    ) -> Result<(ClusterMap, Vec<String>), ApiError> {
        // Schema changes queue here; a conflict can then come only from an operator's
        // set-map at the same moment.
        let _one = node.schema_changes.lock().await;
        let mut last = None;
        for _ in 0..TRIES {
            let map = node.get_map()?;
            let Some(mut new) = change(&map)? else {
                return Ok((map, Vec::new()));
            };
            new.map_version = map.map_version + 1;
            match node
                .set_map(SetMapRequest {
                    copies: Vec::new(),
                    expected_map_version: map.map_version,
                    map: Some(new),
                    force: false,
                })
                .await
            {
                Ok(set) => {
                    let missed = set
                        .pushed
                        .iter()
                        .filter(|p| !p.ok)
                        .map(|p| p.node.clone())
                        .collect();
                    return Ok((node.get_map()?, missed));
                }
                Err(e) if e.code == "Conflict" => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last
            .unwrap_or_else(|| ApiError::from_code(ErrorCode::Conflict, "the map kept changing")))
    }

    async fn cluster_create(
        &self,
        node: &Arc<Node>,
        r: proto::CreateTableRequest,
    ) -> Result<proto::TableResponse, ApiError> {
        if r.name.is_empty() {
            return Err(ApiError::invalid("a table needs a name"));
        }
        if r.name.starts_with(super::map::RESERVED_PREFIX) {
            return Err(ApiError::invalid(format!(
                "table {:?}: the {} prefix is reserved",
                r.name,
                super::map::RESERVED_PREFIX
            )));
        }
        for f in &r.families {
            crate::serve::convert::family_in(f)?;
        }
        let (map, missed) = Self::change_map(node, |map| {
            let mut new = map.clone();
            match new.tables.iter_mut().find(|t| t.name == r.name) {
                Some(t) => {
                    if !r.if_missing && t.created > 0 {
                        return Err(ApiError::from_code(
                            ErrorCode::TableExists,
                            &format!("table {:?} exists", r.name),
                        ));
                    }
                    // Families it lacks are added; existing ones keep their options, as a
                    // single node's `if_missing` keeps them.
                    let before = t.families.len();
                    for f in &r.families {
                        if !t.families.iter().any(|g| g.name == f.name) {
                            t.families.push(f.clone());
                        }
                    }
                    Ok((t.families.len() != before).then_some(new))
                }
                None => {
                    let p = r.placement.as_ref().ok_or_else(|| {
                        ApiError::invalid(format!(
                            "table {:?} isn't in the cluster map: a create on a cluster needs a \
                             placement (its split keys and partitions, or a group)",
                            r.name
                        ))
                    })?;
                    if r.families.is_empty() {
                        return Err(ApiError::invalid("a table needs at least one family"));
                    }
                    let mut partitions = p.partitions.clone();
                    // A group's tables share its partition: one named by the group already.
                    if partitions.is_empty()
                        && !p.group.is_empty()
                        && let Some(g) = map.tables.iter().find(|t| t.group == p.group)
                    {
                        partitions = g.partitions.clone();
                    }
                    new.tables.push(TablePlacement {
                        name: r.name.clone(),
                        group: p.group.clone(),
                        families: r.families.clone(),
                        splits: split_keys(&p.splits),
                        partitions,
                        created: map.map_version + 1,
                    });
                    Ok(Some(new))
                }
            }
        })
        .await?;
        let t = map
            .tables
            .iter()
            .find(|t| t.name == r.name)
            .ok_or_else(|| ApiError::internal("the created table isn't in the new map"))?;
        let unreconciled = t
            .partitions
            .iter()
            .filter(|p| {
                map.partitions
                    .iter()
                    .any(|q| q.id == **p && missed.contains(&q.node))
            })
            .cloned()
            .collect();
        Ok(proto::TableResponse {
            name: t.name.clone(),
            families: t.families.clone(),
            unreconciled,
        })
    }

    async fn cluster_drop(&self, node: &Arc<Node>, name: String) -> Result<(), ApiError> {
        Self::change_map(node, |map| {
            if !map.tables.iter().any(|t| t.name == name) {
                return Err(ApiError::from_code(
                    ErrorCode::TableNotFound,
                    &format!("no table {name:?} in the cluster map"),
                ));
            }
            let mut new = map.clone();
            new.tables.retain(|t| t.name != name);
            Ok(Some(new))
        })
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::{Outcome, reconcile, records};
    use crate::serve::proto::{ClusterMap, FamilySpec, TablePlacement};
    use pigeonhole::{Family, Options, Pigeonhole};

    fn db() -> Pigeonhole {
        // A count as well as the time: tests run in parallel, and two in one clock tick
        // would otherwise open one file (`WriterLocked`).
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "phdb-schema-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Pigeonhole::open(
            dir.join("p1.phdb"),
            Options::default().create_if_missing(true).shards(1),
        )
        .unwrap()
    }

    fn family(name: &str) -> FamilySpec {
        FamilySpec {
            name: name.into(),
            ..Default::default()
        }
    }

    /// A map whose tables are placed on `p1` (or `p2`), each `(name, created, families,
    /// partition)`.
    fn map(version: u64, tables: &[(&str, u64, &[&str], &str)]) -> ClusterMap {
        ClusterMap {
            cluster_id: "c1".into(),
            map_version: version,
            tables: tables
                .iter()
                .map(|(name, created, fams, p)| TablePlacement {
                    name: (*name).into(),
                    families: fams.iter().map(|f| family(f)).collect(),
                    partitions: vec![(*p).into()],
                    created: *created,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn tables(db: &Pigeonhole) -> Vec<String> {
        let mut t: Vec<String> = db
            .tables()
            .into_iter()
            .filter(|n| !n.starts_with("__phdb_"))
            .collect();
        t.sort();
        t
    }

    #[test]
    fn a_partition_takes_the_maps_schema_and_drops_only_what_the_map_made() {
        let db = db();
        // A hand-made table, never in the map: left alone throughout.
        db.table("manual")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        // Created through the map.
        let o = reconcile(&db, "p1", &map(2, &[("orders", 2, &["f"], "p1")]));
        assert_eq!(o.created, ["orders"]);
        assert_eq!(o.unmapped, ["manual"]);
        assert_eq!(records(&db).unwrap().get("orders"), Some(&2));
        // Again: nothing to do. A table placed on another partition isn't made here.
        let again = reconcile(
            &db,
            "p1",
            &map(
                3,
                &[("orders", 2, &["f"], "p1"), ("other", 3, &["f"], "p2")],
            ),
        );
        assert_eq!(
            (again.created.len(), again.dropped.len()),
            (0, 0),
            "{again:?}"
        );
        assert_eq!(tables(&db), ["manual", "orders"]);
        // A family added through the map.
        let o = reconcile(&db, "p1", &map(4, &[("orders", 2, &["f", "g"], "p1")]));
        assert!(o.errors.is_empty(), "{o:?}");
        let t = db.table("orders").unwrap().open().unwrap();
        assert!(t.families().contains(&"g"));
        drop(t);
        // Dropped and created again while this node was away: another `created`, so the old
        // table goes and the new one is made (simp724's S1).
        db.table("orders")
            .unwrap()
            .open()
            .unwrap()
            .mutate(b"r")
            .put("f", b"q", b"old")
            .commit()
            .unwrap();
        let o = reconcile(&db, "p1", &map(9, &[("orders", 7, &["f"], "p1")]));
        assert_eq!(
            (o.dropped.as_slice(), o.created.as_slice()),
            (&["orders".to_owned()][..], &["orders".to_owned()][..])
        );
        let t = db.table("orders").unwrap().open().unwrap();
        assert!(
            t.get(b"r", "f", b"q").unwrap().is_none(),
            "the old incarnation's data went"
        );
        drop(t);
        // Dropped through the map: the recorded table goes, the hand-made one stays.
        let o = reconcile(&db, "p1", &map(10, &[]));
        assert_eq!(o.dropped, ["orders"]);
        assert_eq!(tables(&db), ["manual"]);
        assert!(records(&db).unwrap().is_empty());
        db.close().unwrap();
    }

    #[test]
    fn a_reconcile_cut_short_by_a_crash_finishes_on_the_next_one() {
        // A crash between a table and its record, either way, redoes the step: the map, the
        // file and the records are all a reconcile needs (simp724's S1).
        let db = db();
        let _ = reconcile(&db, "p1", &map(2, &[("orders", 2, &["f"], "p1")]));
        // The map dropped `orders`, and the node crashed after dropping the table, before
        // removing its record: the next reconcile removes the record.
        db.drop_table("orders").unwrap();
        let o = reconcile(&db, "p1", &map(3, &[]));
        assert!(o.errors.is_empty(), "{o:?}");
        assert!(records(&db).unwrap().is_empty());
        // The map placed `items` and the node crashed after creating it, before recording it:
        // the next reconcile records it, and doesn't call it hand-made.
        db.table("items")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        let o: Outcome = reconcile(&db, "p1", &map(5, &[("items", 4, &["f"], "p1")]));
        assert_eq!(records(&db).unwrap().get("items"), Some(&4));
        // It's reported as taken over: the map's create met an unrecorded table of its name.
        assert_eq!(o.adopted, ["items"]);
        // A map that dropped `items`, persisted before the crash and found at the restart: the
        // record says the map made it, so it goes.
        let o = reconcile(&db, "p1", &map(6, &[]));
        assert_eq!(o.dropped, ["items"]);
        assert!(tables(&db).is_empty());
        db.close().unwrap();
    }

    #[test]
    fn a_placement_set_by_hand_adopts_its_table_without_calling_it_taken_over() {
        // Before ticket 5, an operator placed a table with set-map (created 0) and made it in
        // each partition by hand: the map owns it from now on, quietly.
        let db = db();
        db.table("users")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        let o = reconcile(&db, "p1", &map(2, &[("users", 0, &[], "p1")]));
        assert!(
            o.adopted.is_empty() && o.unmapped.is_empty() && o.errors.is_empty(),
            "{o:?}"
        );
        assert_eq!(records(&db).unwrap().get("users"), Some(&0));
        db.close().unwrap();
    }
}
