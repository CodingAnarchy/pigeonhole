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
    self, ClusterMap, FamilySpec, IndexPlacement, SetMapRequest, SplitKey, TablePlacement,
    split_key,
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
    /// Tables already here that took a family the map added (a create with `if_missing`):
    /// a cached handle of one lists its families as they were (audit120's P2-C1).
    pub altered: Vec<String>,
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
    // D331: a table is a partition's too while a range move names it as the move's source
    // or target (the target makes it at COPYING, before the map gives it a range: design
    // §3's plan), and while the file records a range of it moved away (the source serves it
    // to a cluster feed up to the move's last seqno, PR 3b, and its cleanup is PR 5's):
    // never dropped from under either.
    // A failed read keeps every table this time (simp724's and cli692's R1): taken as no
    // rows, it would drop exactly the table this protects. The next install tries again.
    let (moved_away, drops) = match crate::serve::ranges::read_moved(db) {
        Ok(rows) => (rows, true),
        Err(e) => {
            o.errors.push(format!(
                "read the moved-away ranges, so no table is dropped this time: {e}"
            ));
            (Vec::new(), false)
        }
    };
    let placed: BTreeMap<&str, &TablePlacement> = map
        .tables
        .iter()
        .filter(|t| {
            t.partitions.iter().any(|p| p == partition)
                || map.moves.iter().any(|m| {
                    !super::map::node_move(m)
                        && m.table == t.name
                        && (m.from == partition || m.to == partition)
                })
                || moved_away.iter().any(|m| m.table == t.name)
        })
        .map(|t| (t.name.as_str(), t))
        .collect();
    // The indexes placed here (#1722): by the table each follows, or by their own ranges.
    let indexes: BTreeMap<&str, &IndexPlacement> = map
        .indexes
        .iter()
        .filter(|i| index_on(map, i, partition))
        .map(|i| (i.name.as_str(), i))
        .collect();
    // What the map dropped: a recorded table (or index) it no longer places here, or places
    // as another incarnation. The table first, then its record.
    let created_here = |name: &str| {
        placed
            .get(name)
            .map(|t| t.created)
            .or_else(|| indexes.get(name).map(|i| i.created))
    };
    let gone: Vec<String> = recorded
        .iter()
        .filter(|_| drops)
        .filter(|(name, created)| created_here(name).is_none_or(|c| c != **created))
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
        // An existing table's families before the create, to see whether it added one.
        let had: BTreeSet<String> = if exists {
            db.table(name)
                .and_then(|t| t.open())
                .map(|t| t.families().into_iter().map(str::to_owned).collect())
                .unwrap_or_default()
        } else {
            BTreeSet::new()
        };
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
        if exists && table.families().iter().any(|f| !had.contains(*f)) {
            o.altered.push((*name).to_owned());
        }
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
    // Indexes after tables: a text index reads its source table, created above.
    let have: BTreeSet<String> = db.tables().into_iter().collect();
    for (name, i) in &indexes {
        let exists = have.contains(*name);
        if recorded.contains_key(*name) {
            // Recorded before its create (below), so a crash between the two leaves it
            // recorded without its table: make it now. One there that doesn't open (a crash
            // part way through its create) is left for the operator, since a drop and create
            // through the API now reaches it: never made again here, as a full index that
            // stopped opening (a tokeniser change) would come back empty (audit120's R1).
            if !exists {
                create_recorded(db, name, i, &mut o);
            } else if let Err(e) = open_index(db, i) {
                o.errors.push(format!(
                    "index {name:?} doesn't open: {e}. Drop the index and create it again"
                ));
            }
            continue;
        }
        if !exists {
            // Recorded first: a create that fails part way is undone below, and one a crash
            // cuts short is the map's to finish or drop (audit120's R1).
            if let Err(e) = record(db, name, i.created) {
                o.errors.push(e);
                continue;
            }
            create_recorded(db, name, i, &mut o);
            continue;
        }
        // An unrecorded table of its name (made by hand, before the map placed it): adopted
        // only if it opens as the kind the map places, else left unrecorded with an error
        // for `Status` (audit120's S2).
        if let Err(e) = open_index(db, i) {
            o.errors.push(format!(
                "index {name:?}: an unrecorded table of its name doesn't open as the index: {e}"
            ));
            continue;
        }
        if i.created > 0 {
            o.adopted.push((*name).to_owned());
        }
        if let Err(e) = record(db, name, i.created) {
            o.errors.push(e);
        }
    }
    let after: BTreeSet<String> = db.tables().into_iter().collect();
    o.unmapped = after
        .into_iter()
        .filter(|n| !n.starts_with(super::map::RESERVED_PREFIX))
        .filter(|n| {
            !placed.contains_key(n.as_str())
                && !indexes.contains_key(n.as_str())
                && !recorded.contains_key(n)
        })
        .collect();
    o
}

/// Whether `map` places index `i` on `partition` (#1722): by the table it follows, or by
/// its own ranges.
fn index_on(map: &ClusterMap, i: &IndexPlacement, partition: &str) -> bool {
    if i.follows.is_empty() {
        return i.partitions.iter().any(|p| p == partition);
    }
    map.tables
        .iter()
        .find(|t| t.name == i.follows)
        .is_some_and(|t| t.partitions.iter().any(|p| p == partition))
}

/// Creates index `i` in this partition's file, with the options its create sent (#1722): a
/// vector index empty; a text index unfilled, with a rebuild pending that the partition's
/// background resume runs a document at a time under its gate (#1756), not this reconcile
/// on the installing thread.
fn create_index(db: &Pigeonhole, i: &IndexPlacement) -> Result<(), String> {
    use crate::serve::proto::index_placement::Kind;
    let made = match &i.kind {
        Some(Kind::Vector(r)) => {
            let (spec, options) = crate::serve::indexes::create_args(r).map_err(|e| e.message)?;
            pigeonhole_vector::Index::create(db, &i.name, spec, options)
                .map(drop)
                .map_err(|e| e.to_string())
        }
        Some(Kind::Text(r)) => {
            let options = crate::serve::texts::options(r).map_err(|e| e.message)?;
            let table = db
                .table(&r.source_table)
                .and_then(|t| t.open())
                .map_err(|e| e.to_string())?;
            let source =
                pigeonhole_text::Source::new(&table, &r.source_family, &r.source_qualifier);
            pigeonhole_text::Index::create_unfilled(db, &i.name, source, options)
                .map(drop)
                .map_err(|e| e.to_string())
        }
        None => Err("its placement has no create request".into()),
    };
    // A fault seam: the create fails once its table is made (audit120's R1).
    #[cfg(test)]
    if made.is_ok() && FAIL_CREATE.with(|f| f.replace(false)) {
        return Err("injected: the create failed after its table".into());
    }
    made
}

#[cfg(test)]
thread_local! {
    /// Makes this thread's next [`create_index`] fail after its table is made.
    static FAIL_CREATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Creates recorded index `name` (#1722). A create that fails part way (the text create
/// makes the table, then commits its config) drops what it made and the record, so the next
/// reconcile starts again: the table wasn't there before, so nothing is lost. If that drop
/// fails, the record stays, and the index's `DropTable` reaches the table.
fn create_recorded(db: &Pigeonhole, name: &str, i: &IndexPlacement, o: &mut Outcome) {
    let Err(e) = create_index(db, i) else {
        o.created.push(name.to_owned());
        return;
    };
    o.errors.push(format!("create index {name:?}: {e}"));
    match db.drop_table(name) {
        Ok(()) => {}
        Err(d) if d.code() == ErrorCode::TableNotFound => {}
        Err(d) => {
            o.errors
                .push(format!("drop the index {name:?} its create left: {d}"));
            return;
        }
    }
    if let Err(u) = unrecord(db, name) {
        o.errors.push(u);
    }
}

/// Opens index `i` as the kind its placement says, to check a table of its name is it.
fn open_index(db: &Pigeonhole, i: &IndexPlacement) -> Result<(), String> {
    use crate::serve::proto::index_placement::Kind;
    match &i.kind {
        Some(Kind::Vector(_)) => pigeonhole_vector::Index::open(db, &i.name)
            .map(drop)
            .map_err(|e| e.to_string()),
        Some(Kind::Text(_)) => pigeonhole_text::Index::open(db, &i.name)
            .map(drop)
            .map_err(|e| e.to_string()),
        None => Err("its placement has no create request".into()),
    }
}

/// Reconciles each partition `shared` has open to `map` (D317 ticket 5), through that
/// partition's own service (ticket 8a, simp724's N1 on #1720), and drops the service's cached
/// handles where the file changed: each partition's outcome by id. `None` when `shared` isn't
/// a cluster node's. `unfencing` names a partition whose operator's fence is being lifted,
/// which reconciles although its gate still says fenced. Run by
/// [`Node::reconcile_locked`], on the installing thread, on a node that only routes too: a
/// partition may open there later (ticket 6).
pub(crate) fn reconcile_parts(
    shared: &crate::serve::Shared,
    map: &ClusterMap,
    unfencing: Option<&str>,
) -> Option<Vec<(String, Outcome)>> {
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
        let mut outcome = reconcile(&part.db, partition, map);
        // A family added to a table here changes it as much as a create: the service's
        // cached handles list the old families (audit120's P2-C1).
        if !outcome.created.is_empty() || !outcome.dropped.is_empty() || !outcome.altered.is_empty()
        {
            service.tables_changed();
        }
        // A text index created here is unfilled (#1722): the partition's background resume
        // fills it.
        if !outcome.created.is_empty() {
            service.resume_text_rebuilds();
        }
        for name in &outcome.adopted {
            eprintln!(
                "phdb serve: cluster map version {}: table {name:?} in partition {partition} \
                 was made by hand; a create through the map took it over, and the map now \
                 owns it (a drop through the map drops it)",
                map.map_version
            );
        }
        if let Err(e) = memory(shared, service, partition, map) {
            outcome.errors.push(e);
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
}

/// Partition `partition`'s memory store, when `map` places the store there (#1915) and the
/// node was started with `--memory`: its slot is set the first time, and its store opened
/// again at every reconcile after (a reconcile that changed the partition's tables cleared
/// it), so the store's background loops keep running. A map can't move a placed store, so
/// a slot once set stays the partition's. `Err`: why the partition serves no store although
/// the map places one there, which its Status shows (`reconcile_errors`, simp724's ask 1 on
/// #1920).
fn memory(
    shared: &crate::serve::Shared,
    service: &crate::serve::service::Service,
    partition: &str,
    map: &ClusterMap,
) -> Result<(), String> {
    let Some(placed) = map
        .memory
        .as_ref()
        .filter(|m| m.partitions.iter().any(|p| p == partition))
    else {
        return Ok(());
    };
    let Some(recipe) = shared
        .router
        .cluster
        .as_ref()
        .and_then(|c| c.memory.as_ref())
    else {
        return Err(no_memory_flag(&placed.store));
    };
    // Refused at start; the placement is fixed once written.
    if placed.store != recipe.name {
        return Err(format!(
            "the map places memory store {:?}, but this node serves --memory {}",
            placed.store, recipe.name
        ));
    }
    if let Some(slot) = service.memory_slot() {
        return if slot.cached().is_none() {
            recipe.reopen(slot)
        } else {
            Ok(())
        };
    }
    let (Ok(part), Some(gate)) = (service.part(), service.gate()) else {
        return Ok(());
    };
    let loops = crate::serve::memslot::Loops {
        gate: Arc::clone(gate),
        placement: service.placement_of().cloned(),
        partition: partition.to_owned(),
    };
    let (slot, opened) = recipe.open(&part.db, loops);
    service.set_memory(slot);
    opened
}

/// Why a partition the map places memory store `store` on serves none: its node was started
/// without `--memory` (simp724's ask 2 on #1920).
fn no_memory_flag(store: &str) -> String {
    format!(
        "the cluster map places memory store {store} on this partition, but its node was \
         started without --memory: restart it with --memory {store} and the memory flags the \
         other nodes have"
    )
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
        let Some(server) = self.server.get() else {
            return;
        };
        self.reconciled_newest
            .store(map.map_version, Ordering::Relaxed);
        // `None` once the server is gone.
        let outcomes = server
            .shared
            .upgrade()
            .and_then(|shared| reconcile_parts(&shared, map, unfencing));
        let mut reconciled = self
            .reconciled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (partition, outcome) in outcomes.into_iter().flatten() {
            reconciled.insert(partition, (map.map_version, outcome));
        }
        // A partition the map no longer places here has no outcome to show: `Status` reads
        // one only for a partition its map gives this node, so this changes no answer, and
        // the entries stop at the node's own partitions (#1830).
        reconciled.retain(|id, _| {
            map.partitions
                .iter()
                .any(|p| p.id == *id && p.node == self.id)
        });
    }
}

/// The map's writer (D317 amendment 1): the node the map names (D330, from term 1), or else
/// the node with the smallest id. It alone changes the map, so two changes through two nodes
/// can't both become the same version.
pub(crate) fn writer(map: &ClusterMap) -> &str {
    if !map.writer.is_empty() {
        return &map.writer;
    }
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

/// An index create's request (#1722), of either kind.
enum IndexRequest {
    Vector(proto::CreateVectorIndexRequest),
    Text(proto::CreateTextIndexRequest),
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
            op @ (ServiceOp::CreateTable(_)
            | ServiceOp::DropTable(_)
            | ServiceOp::CreateIndex(_)
            | ServiceOp::TextCreateIndex(_)) => self.at_writer(cluster, op, caller, deadline).await,
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
                to_replica: false,
                replica_max_lag_ms: 0,
                min_primary_seqno: 0,
                partition: String::new(),
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
            ServiceOp::CreateIndex(r) => {
                Self::cluster_create_index(node, IndexRequest::Vector(r)).await?;
                Ok(ServiceReply::Empty(proto::Empty {}))
            }
            ServiceOp::TextCreateIndex(r) => {
                Self::cluster_create_index(node, IndexRequest::Text(r)).await?;
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
            // An index's name (audit120's S4): `TableExists`, as a single node answers, where
            // the map's own rule would say `InvalidArgument`. Even with `if_missing`: the
            // index's table isn't the map's to give families.
            if map.indexes.iter().any(|i| i.name == r.name) {
                return Err(ApiError::from_code(
                    ErrorCode::TableExists,
                    &format!("{:?} is an index's table", r.name),
                ));
            }
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
                        pinned_ranges: Default::default(),
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
            let mut new = map.clone();
            // An index is a table of its own (#1722): dropping it drops its placement.
            if map.indexes.iter().any(|i| i.name == name) {
                new.indexes.retain(|i| i.name != name);
                return Ok(Some(new));
            }
            if !map.tables.iter().any(|t| t.name == name) {
                return Err(ApiError::from_code(
                    ErrorCode::TableNotFound,
                    &format!("no table {name:?} in the cluster map"),
                ));
            }
            // A table an index follows stays while the index does (#1916's rule, #1722).
            let over: Vec<String> = map
                .indexes
                .iter()
                .filter(|i| i.follows == name)
                .map(|i| format!("{:?}", i.name))
                .collect();
            if !over.is_empty() {
                return Err(ApiError::failed_precondition(format!(
                    "table {name:?} is followed by the index{} {}: drop {} first",
                    if over.len() == 1 { "" } else { "es" },
                    over.join(", "),
                    if over.len() == 1 { "it" } else { "them" },
                )));
            }
            new.tables.retain(|t| t.name != name);
            Ok(Some(new))
        })
        .await
        .map(|_| ())
    }

    /// `Vectors.CreateIndex` or `Text.CreateIndex` on a cluster (#1722, ICR 0138), at the
    /// map's writer: an index placement, which every node owning one of its partitions then
    /// creates. A vector index follows a placed table or has ranges of its own; a text index
    /// follows its source table. `if_missing` answers an index the map places as it is, as a
    /// single node's opens one without comparing its options.
    async fn cluster_create_index(node: &Arc<Node>, r: IndexRequest) -> Result<(), ApiError> {
        use crate::serve::proto::index_placement::Kind;
        let (name, if_missing) = match &r {
            IndexRequest::Vector(r) => (r.name.clone(), r.if_missing),
            IndexRequest::Text(r) => (r.name.clone(), r.if_missing),
        };
        if name.is_empty() {
            return Err(ApiError::invalid("an index needs a name"));
        }
        if name.starts_with(super::map::RESERVED_PREFIX) {
            return Err(ApiError::invalid(format!(
                "index {name:?}: the {} prefix is reserved",
                super::map::RESERVED_PREFIX
            )));
        }
        // The options parse as each partition will parse them, before the map takes them.
        // A text index's source family, checked against the followed table's in the map.
        let source_family = match &r {
            IndexRequest::Text(t) => Some(t.source_family.clone()),
            IndexRequest::Vector(_) => None,
        };
        let (kind, follows, splits, partitions) = match r {
            IndexRequest::Vector(r) => {
                crate::serve::indexes::create_args(&r)?;
                let (follows, splits, partitions) = match (&r.placement, r.follows.is_empty()) {
                    (None, false) => (r.follows.clone(), Vec::new(), Vec::new()),
                    (Some(p), true) => {
                        if !p.group.is_empty() {
                            return Err(ApiError::invalid(format!(
                                "index {name:?}: an index takes no co-location group (follow \
                                 a table instead)"
                            )));
                        }
                        (String::new(), split_keys(&p.splits), p.partitions.clone())
                    }
                    (None, true) => {
                        return Err(ApiError::invalid(format!(
                            "index {name:?}: a create on a cluster needs follows (a placed \
                             table whose ranges it shares) or a placement of its own"
                        )));
                    }
                    (Some(_), false) => {
                        return Err(ApiError::invalid(format!(
                            "index {name:?}: give follows or a placement, not both"
                        )));
                    }
                };
                (Kind::Vector(r), follows, splits, partitions)
            }
            IndexRequest::Text(r) => {
                crate::serve::texts::options(&r)?;
                let follows = r.source_table.clone();
                (Kind::Text(r), follows, Vec::new(), Vec::new())
            }
        };
        Self::change_map(node, |map| {
            if map.indexes.iter().any(|i| i.name == name) {
                if if_missing {
                    return Ok(None);
                }
                return Err(ApiError::from_code(
                    ErrorCode::TableExists,
                    &format!("index {name:?} exists"),
                ));
            }
            if map.tables.iter().any(|t| t.name == name) {
                return Err(ApiError::from_code(
                    ErrorCode::TableExists,
                    &format!("table {name:?} exists, and an index is a table of its own"),
                ));
            }
            let followed = map.tables.iter().find(|t| t.name == follows);
            if !follows.is_empty() && followed.is_none() {
                return Err(ApiError::from_code(
                    ErrorCode::TableNotFound,
                    &format!(
                        "index {name:?} follows table {follows:?}, which isn't in the cluster map"
                    ),
                ));
            }
            // Every partition would refuse to make it, for ever, with the placement fixed
            // (audit120's M3). A placement set by hand with no families can't be checked.
            if let (Some(family), Some(t)) = (&source_family, followed)
                && !t.families.is_empty()
                && !t.families.iter().any(|f| f.name == *family)
            {
                return Err(ApiError::invalid(format!(
                    "text index {name:?}: table {follows:?} has no family {family:?}"
                )));
            }
            let mut new = map.clone();
            new.indexes.push(IndexPlacement {
                name: name.clone(),
                kind: Some(kind.clone()),
                follows: follows.clone(),
                splits: splits.clone(),
                partitions: partitions.clone(),
                created: map.map_version + 1,
            });
            // The rest of the rules (unique names, ranges, partitions) are the map's own.
            super::map::validate(&new, false).map_err(ApiError::invalid)?;
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
        let dir = crate::test_dir::fresh("phdb-schema-");
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

    /// D331: a table a move took from the partition stays while the file records a range of
    /// it moved away, and a failed read of those rows drops nothing (simp724's and cli692's
    /// R1 on PR 2), reported, for the next install to try again.
    #[test]
    fn a_moved_tables_row_keeps_it_and_a_failed_read_drops_nothing() {
        let db = db();
        reconcile(&db, "p1", &map(2, &[("orders", 2, &["f"], "p1")]));
        crate::serve::ranges::write_moved(
            &db,
            &crate::serve::ranges::Moved {
                id: "mv1".into(),
                table: "orders".into(),
                start: Vec::new(),
                end: None,
                term: 0,
                version: 3,
                seqno: 1,
                at_ms: 0,
                ..Default::default()
            },
        )
        .unwrap();
        // The map now places orders on p2 alone: the row keeps it here.
        let o = reconcile(&db, "p1", &map(3, &[("orders", 2, &["f"], "p2")]));
        assert!(o.dropped.is_empty() && o.errors.is_empty(), "{o:?}");
        assert!(tables(&db).contains(&"orders".to_owned()));
        // A malformed row (its seqno gone): the read fails, and nothing is dropped.
        let t = db
            .table(crate::serve::cluster::identity::TABLE)
            .unwrap()
            .open()
            .unwrap();
        t.mutate(b"moved/mv1")
            .delete_column("moved", b"seqno")
            .commit()
            .unwrap();
        drop(t);
        let o = reconcile(&db, "p1", &map(4, &[("orders", 2, &["f"], "p2")]));
        assert!(o.dropped.is_empty(), "{o:?}");
        assert!(
            o.errors.iter().any(|e| e.contains("no table is dropped")),
            "{o:?}"
        );
        assert!(tables(&db).contains(&"orders".to_owned()));
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
            (
                again.created.len(),
                again.dropped.len(),
                again.altered.len()
            ),
            (0, 0, 0),
            "{again:?}"
        );
        assert_eq!(tables(&db), ["manual", "orders"]);
        // A family added through the map.
        let o = reconcile(&db, "p1", &map(4, &[("orders", 2, &["f", "g"], "p1")]));
        assert!(o.errors.is_empty(), "{o:?}");
        // Reported, so the service forgets its handles of it (audit120's P2-C1); once.
        assert_eq!(o.altered, ["orders"], "{o:?}");
        let again = reconcile(&db, "p1", &map(5, &[("orders", 2, &["f", "g"], "p1")]));
        assert!(again.altered.is_empty(), "{again:?}");
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

    /// audit120's S2 on #1994: an unrecorded table of an index's name is adopted only if it
    /// opens as that index. One that doesn't (a crash between the index's create steps, or
    /// a plain table) stays unrecorded, with an error.
    #[test]
    fn an_unrecorded_index_table_is_adopted_only_if_it_opens_as_the_index() {
        use crate::serve::proto::{
            CreateVectorIndexRequest, Element, IndexPlacement, Metric, VectorSpec,
            index_placement::Kind,
        };
        let db = db();
        let request = |name: &str| CreateVectorIndexRequest {
            name: name.into(),
            spec: Some(VectorSpec {
                dim: 2,
                element: Element::F32 as i32,
                metric: Metric::L2 as i32,
            }),
            ..Default::default()
        };
        // `half` is a table and no index; `whole` is an index made outside the map.
        db.table("half")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        let (spec, options) = crate::serve::indexes::create_args(&request("whole")).unwrap();
        pigeonhole_vector::Index::create(&db, "whole", spec, options).unwrap();
        let mut m = map(2, &[]);
        for (name, created) in [("half", 2), ("whole", 2)] {
            m.indexes.push(IndexPlacement {
                name: name.into(),
                kind: Some(Kind::Vector(request(name))),
                partitions: vec!["p1".into()],
                created,
                ..Default::default()
            });
        }
        let o = reconcile(&db, "p1", &m);
        assert_eq!(o.adopted, vec!["whole".to_owned()], "{o:?}");
        assert!(
            o.errors.len() == 1 && o.errors[0].contains("\"half\""),
            "{o:?}"
        );
        let r = records(&db).unwrap();
        assert_eq!((r.get("whole"), r.get("half")), (Some(&2), None));
        db.close().unwrap();
    }

    /// audit120's R1 on #1994: an index is recorded before its create. A create that fails
    /// once its table is made leaves neither, and the next reconcile makes it. One recorded
    /// without its table (a crash between the two) is made; one recorded that doesn't open
    /// stays, with an error saying to drop and create it again, which its record lets reach it.
    #[test]
    fn an_index_is_recorded_before_its_create_and_a_failed_create_is_undone() {
        use crate::serve::proto::{
            CreateVectorIndexRequest, Element, IndexPlacement, Metric, VectorSpec,
            index_placement::Kind,
        };
        let db = db();
        let placement = |name: &str| IndexPlacement {
            name: name.into(),
            kind: Some(Kind::Vector(CreateVectorIndexRequest {
                name: name.into(),
                spec: Some(VectorSpec {
                    dim: 2,
                    element: Element::F32 as i32,
                    metric: Metric::L2 as i32,
                }),
                ..Default::default()
            })),
            partitions: vec!["p1".into()],
            created: 2,
            ..Default::default()
        };
        let mut m = map(2, &[]);
        m.indexes.push(placement("v"));
        super::FAIL_CREATE.with(|f| f.set(true));
        let o = reconcile(&db, "p1", &m);
        assert!(
            o.created.is_empty() && o.errors.len() == 1 && o.errors[0].contains("injected"),
            "{o:?}"
        );
        assert!(tables(&db).is_empty(), "{:?}", tables(&db));
        assert_eq!(records(&db).unwrap().get("v"), None);
        let o = reconcile(&db, "p1", &m);
        assert!(o.created == ["v"] && o.errors.is_empty(), "{o:?}");
        assert_eq!(records(&db).unwrap().get("v"), Some(&2));
        pigeonhole_vector::Index::open(&db, "v").unwrap();

        // Recorded, its table never made: made now.
        super::record(&db, "w", 2).unwrap();
        m.indexes.push(placement("w"));
        let o = reconcile(&db, "p1", &m);
        assert!(o.created == ["w"] && o.errors.is_empty(), "{o:?}");
        pigeonhole_vector::Index::open(&db, "w").unwrap();

        // Recorded, a table of its name that isn't the index: an error, nothing dropped.
        db.table("x")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        super::record(&db, "x", 2).unwrap();
        m.indexes.push(placement("x"));
        let o = reconcile(&db, "p1", &m);
        assert!(
            o.errors.len() == 1
                && o.errors[0].contains("\"x\" doesn't open")
                && o.errors[0].contains("Drop the index and create it again"),
            "{o:?}"
        );
        assert!(o.dropped.is_empty() && o.created.is_empty(), "{o:?}");
        assert_eq!(tables(&db), ["v", "w", "x"]);
        // Its drop through the map (the placement gone) reaches the table.
        m.indexes.retain(|i| i.name != "x");
        let o = reconcile(&db, "p1", &m);
        assert!(o.dropped == ["x"] && o.errors.is_empty(), "{o:?}");
        db.close().unwrap();
    }
}
