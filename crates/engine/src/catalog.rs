//! The catalog: tables, families, tablets, counters and everything else the manifest
//! persists, rebuilt from manifest edits at open and updated by every manifest commit.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use pigeonhole_compaction::{MergeOperator, MergeRegistry};
use pigeonhole_format::hash::{FastMap, FastSet};
use pigeonhole_format::manifest::{Edit, FamilyOptions, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{
    BlobFileId, FamilyId, Lsn, Seqno, SstId, StreamId, TableId, TabletId, Timestamp,
};
use pigeonhole_runtime::ShardId;

use crate::snapshot::TabletEntry;
use crate::{Error, FamilyInfo, Result, TableInfo};

/// Name of the built-in `i64` add operator (`pigeonhole_compaction::I64Add`).
pub(crate) const I64_ADD: &str = "pigeonhole.i64_add";

/// How merge operands of a family are resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeKind {
    /// No operator: merge operands are refused at write time.
    None,
    /// The built-in wrapping `i64` add.
    I64Add,
    /// An operator registered in `EngineOptions::merge_operators`.
    Registered,
    /// An operator this process has not registered: reads of merged cells fail.
    Unknown,
}

/// Everything a read or write needs to know about a family, by id.
#[derive(Debug, Clone)]
pub(crate) struct FamilyMeta {
    pub table: TableId,
    pub options: FamilyOptions,
    pub merge: MergeKind,
    /// The operator itself (`None` for no operator or an unregistered one).
    pub merge_op: Option<Arc<dyn MergeOperator>>,
}

/// Id allocation counters and floors (`Edit::Counters`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Counters {
    pub next_table: u32,
    pub next_family: u32,
    pub next_tablet: u64,
    pub next_sst: u64,
    pub next_blob_file: u32,
    pub seqno_ceiling: Seqno,
    pub ts_floor: Timestamp,
}

/// A blob file as the manifest records it (carried through snapshots; used from Milestone B).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobFile {
    pub family: FamilyId,
    pub extents: Vec<ExtentRef>,
    pub total_bytes: u64,
    pub live_bytes: u64,
}

/// The SSTs of one `(tablet, family)`: `(level, meta)` in manifest order.
pub(crate) type SstList = Vec<(u8, Arc<SstMeta>)>;

/// The persisted state of the database, as the manifest describes it. Immutable once built;
/// a manifest commit produces a new `Catalog`.
#[derive(Debug, Clone, Default)]
///
/// Its collections are shared copy-on-write (#499): cloning a catalog, which every manifest
/// commit does, copies no collection, and an edit copies only the collections it changes
/// (`Arc::make_mut`). `ssts` shares each slot's list too, so a commit copies the lists of the
/// slots it changes and no others, and two catalogs' slots compare with `Arc::ptr_eq`.
pub struct Catalog {
    tables: Arc<BTreeMap<TableId, Arc<TableInfo>>>,
    by_name: Arc<HashMap<String, TableId>>,
    families: Arc<FastMap<FamilyId, FamilyMeta>>,
    tablets: Arc<BTreeMap<TabletId, TabletEntry>>,
    pub(crate) counters: Counters,
    pub(crate) checkpoints: Arc<BTreeMap<StreamId, Lsn>>,
    pub(crate) flushed: Arc<BTreeMap<(TabletId, FamilyId), Seqno>>,
    /// `(tablet, family) -> [(level, meta)]`, in manifest order.
    pub(crate) ssts: Arc<BTreeMap<(TabletId, FamilyId), Arc<SstList>>>,
    pub(crate) blob_files: Arc<BTreeMap<BlobFileId, BlobFile>>,
    /// Per SST, the blob files its puts point into and the bytes they reference
    /// (`SstBlobRefs`, #240). An SST without an entry (written by an older build) may point
    /// into any blob file of its family. Entries of SSTs no tablet references any more are
    /// dropped by [`Catalog::prune_blob_refs`] after each batch of edits.
    pub(crate) blob_refs: Arc<HashMap<SstId, Vec<(BlobFileId, u64)>>>,
    /// SSTs given blob references since the last prune: with the SSTs a commit removed, the
    /// only entries [`Catalog::prune_blob_refs_since`] has to check.
    refs_touched: Vec<SstId>,
    /// Whether any family names a merge operator this process cannot run.
    pub(crate) has_unknown_merge: bool,
    /// The operators this process knows (`EngineOptions::merge_operators`).
    registry: Arc<MergeRegistry>,
}

impl Catalog {
    /// An empty catalog resolving operator names through `registry`.
    pub(crate) fn with_registry(registry: Arc<MergeRegistry>) -> Self {
        Self {
            registry,
            ..Self::default()
        }
    }

    /// The operator registry.
    pub(crate) fn registry(&self) -> &Arc<MergeRegistry> {
        &self.registry
    }

    /// Applies one edit. A tablet a `PutTablet` creates goes to shard `tablet % shards`
    /// (a split or merge then places it with `set_shard`).
    pub(crate) fn apply(&mut self, edit: &Edit, shards: usize) -> Result<()> {
        match edit {
            Edit::CreateTable { table, name } => {
                let info = Arc::new(TableInfo {
                    id: *table,
                    name: name.clone(),
                    families: Vec::new(),
                });
                Arc::make_mut(&mut self.by_name).insert(name.clone(), *table);
                Arc::make_mut(&mut self.tables).insert(*table, info);
            }
            Edit::DropTable { table } => {
                if let Some(info) = Arc::make_mut(&mut self.tables).remove(table) {
                    Arc::make_mut(&mut self.by_name).remove(&info.name);
                    let families = Arc::make_mut(&mut self.families);
                    for f in &info.families {
                        families.remove(&f.id);
                    }
                }
                let dropped: Vec<TabletId> = self
                    .tablets
                    .values()
                    .filter(|t| t.table == *table)
                    .map(|t| t.id)
                    .collect();
                for id in dropped {
                    self.drop_tablet(id);
                }
            }
            Edit::PutFamily {
                table,
                family,
                name,
                options,
            } => {
                let Some(info) = self.tables.get(table) else {
                    return Err(Error::Corruption(format!(
                        "manifest: family {} of unknown table {}",
                        family.0, table.0
                    )));
                };
                let mut info = (**info).clone();
                let new = FamilyInfo {
                    id: *family,
                    name: name.clone(),
                    options: options.clone(),
                };
                match info.families.iter_mut().find(|f| f.id == *family) {
                    Some(existing) => *existing = new,
                    None => info.families.push(new),
                }
                let name = options.merge_operator.as_str();
                let merge_op = if name.is_empty() {
                    None
                } else {
                    self.registry.get(name)
                };
                let merge = match (name, &merge_op) {
                    ("", _) => MergeKind::None,
                    (I64_ADD, _) => MergeKind::I64Add,
                    (_, Some(_)) => MergeKind::Registered,
                    (_, None) => {
                        self.has_unknown_merge = true;
                        MergeKind::Unknown
                    }
                };
                Arc::make_mut(&mut self.families).insert(
                    *family,
                    FamilyMeta {
                        table: *table,
                        options: options.clone(),
                        merge,
                        merge_op,
                    },
                );
                Arc::make_mut(&mut self.tables).insert(*table, Arc::new(info));
            }
            Edit::PutTablet {
                tablet,
                table,
                start,
                end,
            } => {
                Arc::make_mut(&mut self.tablets).insert(
                    *tablet,
                    TabletEntry {
                        id: *tablet,
                        table: *table,
                        start: start.clone(),
                        end: end.clone(),
                        shard: shard_for(*tablet, shards),
                    },
                );
            }
            Edit::DropTablet { tablet } => self.drop_tablet(*tablet),
            Edit::AddSst {
                tablet,
                family,
                level,
                meta,
            } => {
                let list = Arc::make_mut(&mut self.ssts)
                    .entry((*tablet, *family))
                    .or_default();
                Arc::make_mut(list).push((*level, Arc::new(meta.clone())));
            }
            Edit::RemoveSst {
                tablet,
                family,
                sst,
            } => {
                // Only a slot that holds the SST is copied.
                let key = (*tablet, *family);
                if self
                    .ssts
                    .get(&key)
                    .is_some_and(|l| l.iter().any(|(_, m)| m.id == *sst))
                    && let Some(list) = Arc::make_mut(&mut self.ssts).get_mut(&key)
                {
                    Arc::make_mut(list).retain(|(_, m)| m.id != *sst);
                }
            }
            Edit::SetFlushed {
                tablet,
                family,
                seqno,
            } => {
                let e = Arc::make_mut(&mut self.flushed)
                    .entry((*tablet, *family))
                    .or_default();
                *e = (*e).max(*seqno);
            }
            Edit::WalCheckpoint { stream, lsn } => {
                Arc::make_mut(&mut self.checkpoints).insert(*stream, *lsn);
            }
            Edit::PutBlobFile {
                blob_file,
                family,
                extents,
                total_bytes,
                live_bytes,
            } => {
                Arc::make_mut(&mut self.blob_files).insert(
                    *blob_file,
                    BlobFile {
                        family: *family,
                        extents: extents.clone(),
                        total_bytes: *total_bytes,
                        live_bytes: *live_bytes,
                    },
                );
            }
            Edit::DropBlobFile { blob_file } => {
                Arc::make_mut(&mut self.blob_files).remove(blob_file);
            }
            Edit::SstBlobRefs { sst, refs } => {
                Arc::make_mut(&mut self.blob_refs).insert(*sst, refs.clone());
                self.refs_touched.push(*sst);
            }
            Edit::Counters {
                next_table,
                next_family,
                next_tablet,
                next_sst,
                next_blob_file,
                seqno_ceiling,
                ts_floor,
            } => {
                self.counters = Counters {
                    next_table: *next_table,
                    next_family: *next_family,
                    next_tablet: *next_tablet,
                    next_sst: *next_sst,
                    next_blob_file: *next_blob_file,
                    seqno_ceiling: *seqno_ceiling,
                    ts_floor: *ts_floor,
                };
            }
            _ => {
                // A tag this build does not know; the length prefix let the decoder skip it.
            }
        }
        Ok(())
    }

    /// Removes a tablet and its slots (flushed seqnos and SST lists).
    fn drop_tablet(&mut self, id: TabletId) {
        Arc::make_mut(&mut self.tablets).remove(&id);
        if self.flushed.keys().any(|k| k.0 == id) {
            Arc::make_mut(&mut self.flushed).retain(|k, _| k.0 != id);
        }
        if self.ssts.keys().any(|k| k.0 == id) {
            Arc::make_mut(&mut self.ssts).retain(|k, _| k.0 != id);
        }
    }

    /// Every edit needed to rebuild this catalog from empty (the manifest snapshot block).
    pub(crate) fn snapshot_edits(&self) -> Vec<Edit> {
        let mut edits = vec![self.counters_edit()];
        for info in self.tables.values() {
            edits.push(Edit::CreateTable {
                table: info.id,
                name: info.name.clone(),
            });
            for f in &info.families {
                edits.push(Edit::PutFamily {
                    table: info.id,
                    family: f.id,
                    name: f.name.clone(),
                    options: f.options.clone(),
                });
            }
        }
        for t in self.tablets.values() {
            edits.push(Edit::PutTablet {
                tablet: t.id,
                table: t.table,
                start: t.start.clone(),
                end: t.end.clone(),
            });
        }
        for ((tablet, family), list) in self.ssts.iter() {
            for (level, meta) in list.iter() {
                edits.push(Edit::AddSst {
                    tablet: *tablet,
                    family: *family,
                    level: *level,
                    meta: (**meta).clone(),
                });
            }
        }
        for ((tablet, family), seqno) in self.flushed.iter() {
            edits.push(Edit::SetFlushed {
                tablet: *tablet,
                family: *family,
                seqno: *seqno,
            });
        }
        for (stream, lsn) in self.checkpoints.iter() {
            edits.push(Edit::WalCheckpoint {
                stream: *stream,
                lsn: *lsn,
            });
        }
        for (id, b) in self.blob_files.iter() {
            edits.push(Edit::PutBlobFile {
                blob_file: *id,
                family: b.family,
                extents: b.extents.clone(),
                total_bytes: b.total_bytes,
                live_bytes: b.live_bytes,
            });
        }
        let mut refs: Vec<(&SstId, &Vec<(BlobFileId, u64)>)> = self.blob_refs.iter().collect();
        refs.sort_by_key(|(id, _)| **id);
        for (sst, refs) in refs {
            edits.push(Edit::SstBlobRefs {
                sst: *sst,
                refs: refs.clone(),
            });
        }
        edits
    }

    /// Drops the blob references of SSTs no tablet references (call after a batch of edits:
    /// a trivial move removes and re-adds an SST within one batch).
    pub(crate) fn prune_blob_refs(&mut self) {
        self.refs_touched.clear();
        if self.blob_refs.is_empty() {
            return;
        }
        let live: HashSet<SstId> = self
            .ssts
            .values()
            .flat_map(|l| l.iter().map(|(_, m)| m.id))
            .collect();
        Arc::make_mut(&mut self.blob_refs).retain(|id, _| live.contains(id));
    }

    /// [`Catalog::prune_blob_refs`] for a catalog made from `old` (pruned) by a batch of
    /// edits, checking only what the batch can have made stale (#499): the SSTs it removed,
    /// and those it gave references. Equal to the full prune, without building a set of
    /// every live SST on each manifest commit.
    pub(crate) fn prune_blob_refs_since(&mut self, old: &Catalog) {
        let mut check: Vec<SstId> = self.removed_since(old).iter().map(|m| m.id).collect();
        check.append(&mut self.refs_touched);
        if check.is_empty() || self.blob_refs.is_empty() {
            return;
        }
        // An SST given references this batch is usually one it added, so in a changed slot.
        let dead: Vec<SstId> = self.unreferenced(old, check);
        if !dead.is_empty() {
            let refs = Arc::make_mut(&mut self.blob_refs);
            for id in dead {
                refs.remove(&id);
            }
        }
    }

    /// The ids among `ids` that no slot references, looking first in the slots that differ
    /// from `old`'s and over every slot only for those still unresolved.
    fn unreferenced(&self, old: &Catalog, mut ids: Vec<SstId>) -> Vec<SstId> {
        ids.sort_unstable();
        ids.dedup();
        let mut pending: FastSet<SstId> = ids.into_iter().collect();
        for (key, list) in self.ssts.iter() {
            if old.ssts.get(key).is_some_and(|o| Arc::ptr_eq(o, list)) {
                continue;
            }
            for (_, m) in list.iter() {
                pending.remove(&m.id);
            }
        }
        if !pending.is_empty() {
            for list in self.ssts.values() {
                for (_, m) in list.iter() {
                    pending.remove(&m.id);
                }
                if pending.is_empty() {
                    break;
                }
            }
        }
        let mut dead: Vec<SstId> = pending.into_iter().collect();
        dead.sort_unstable();
        dead
    }

    /// Whether `(tablet, family)` holds the same SST list here as in `other`: the same shared
    /// list, compared by pointer (#499). A catalog made from another by edits shares the
    /// lists of the slots they did not change, so this finds the changed slots cheaply.
    #[allow(dead_code)] // The view publish's first use is blob33's (#499 item 3).
    pub(crate) fn same_ssts(&self, other: &Catalog, key: (TabletId, FamilyId)) -> bool {
        match (self.ssts.get(&key), other.ssts.get(&key)) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }

    /// Whether SST `id` may hold a pointer into blob file `blob`: its references say so, or
    /// it has none recorded.
    pub(crate) fn may_reference(&self, id: SstId, blob: BlobFileId) -> bool {
        self.blob_refs
            .get(&id)
            .is_none_or(|refs| refs.iter().any(|(b, _)| *b == blob))
    }

    pub(crate) fn counters_edit(&self) -> Edit {
        let c = self.counters;
        Edit::Counters {
            next_table: c.next_table,
            next_family: c.next_family,
            next_tablet: c.next_tablet,
            next_sst: c.next_sst,
            next_blob_file: c.next_blob_file,
            seqno_ceiling: c.seqno_ceiling,
            ts_floor: c.ts_floor,
        }
    }

    /// Every extent the manifest references besides its own blocks (SSTs and blob extents).
    pub(crate) fn data_extents(&self) -> Vec<ExtentRef> {
        let mut out = Vec::new();
        for list in self.ssts.values() {
            out.extend(list.iter().map(|(_, m)| m.extent));
        }
        for b in self.blob_files.values() {
            out.extend(b.extents.iter().copied());
        }
        out
    }

    pub(crate) fn table(&self, id: TableId) -> Option<&Arc<TableInfo>> {
        self.tables.get(&id)
    }

    pub(crate) fn table_by_name(&self, name: &str) -> Option<&Arc<TableInfo>> {
        self.by_name.get(name).and_then(|id| self.tables.get(id))
    }

    pub(crate) fn tables(&self) -> impl Iterator<Item = &Arc<TableInfo>> {
        self.tables.values()
    }

    pub(crate) fn family(&self, id: FamilyId) -> Option<&FamilyMeta> {
        self.families.get(&id)
    }

    pub(crate) fn tablets(&self) -> Vec<TabletEntry> {
        self.tablets.values().cloned().collect()
    }

    pub(crate) fn tablet_ids_of(&self, table: TableId) -> Vec<TabletId> {
        self.tablets
            .values()
            .filter(|t| t.table == table)
            .map(|t| t.id)
            .collect()
    }

    /// Hands tablet `id` to `shard` (a move, or a split or merge placing its outputs). Not
    /// persisted: the manifest has no owner field, so an open re-derives every owner from the
    /// tablet id (see `docs/design/questions/engine.md`).
    pub(crate) fn set_shard(&mut self, id: TabletId, shard: ShardId) {
        if let Some(t) = Arc::make_mut(&mut self.tablets).get_mut(&id) {
            t.shard = shard;
        }
    }

    /// The flushed-through seqno of `(tablet, family)`, or `None` when the tablet no longer
    /// exists (dropped with its table, or retired by a split or merge after every write to it
    /// reached SSTs): nothing of it needs replaying.
    pub(crate) fn flushed_through(&self, tablet: TabletId, family: FamilyId) -> Option<Seqno> {
        self.tablets
            .contains_key(&tablet)
            .then(|| self.flushed.get(&(tablet, family)).copied().unwrap_or(0))
    }

    /// How many `(tablet, family)` lists name each SST (more than one after a split, D13).
    pub(crate) fn sst_refs(&self) -> HashMap<SstId, usize> {
        let mut refs = HashMap::new();
        for list in self.ssts.values() {
            for (_, m) in list.iter() {
                *refs.entry(m.id).or_insert(0) += 1;
            }
        }
        refs
    }

    /// Re-derives tablet ownership for `shards` shards (a reopen with another shard count).
    pub(crate) fn reassign(&mut self, shards: usize) {
        for t in Arc::make_mut(&mut self.tablets).values_mut() {
            t.shard = shard_for(t.id, shards);
        }
    }

    /// The most `(tablet, family)` slots any of `shards` shards holds with tablets where
    /// `shard_for` puts them (tablet changes off).
    pub(crate) fn max_slots_per_shard(&self, shards: usize) -> usize {
        let shards = shards.max(1);
        let mut slots = vec![0usize; shards];
        for t in self.tablets.values() {
            let width = self.tables.get(&t.table).map_or(0, |i| i.families.len());
            slots[usize::from(shard_for(t.id, shards).0)] += width;
        }
        slots.into_iter().max().unwrap_or(0)
    }

    /// Places every tablet for `shards` shards keeping each shard within `max_slots`
    /// `(tablet, family)` slots where it can (with tablet changes on; owners are not
    /// persisted, D130). In tablet id order, a tablet goes to shard `tablet % shards` when
    /// that fits, else to the shard holding the fewest slots. Returns the most slots any
    /// shard ends up holding, above `max_slots` only when the tablets fit no other way.
    pub(crate) fn place(&mut self, shards: usize, max_slots: usize) -> usize {
        let shards = shards.max(1);
        let mut slots = vec![0usize; shards];
        let widths: HashMap<TableId, usize> = self
            .tables
            .values()
            .map(|t| (t.id, t.families.len()))
            .collect();
        for t in Arc::make_mut(&mut self.tablets).values_mut() {
            let width = widths.get(&t.table).copied().unwrap_or(0);
            let preferred = usize::from(shard_for(t.id, shards).0);
            let shard = if slots[preferred] + width <= max_slots {
                preferred
            } else {
                (0..shards).min_by_key(|&s| slots[s]).unwrap_or(preferred)
            };
            slots[shard] += width;
            t.shard = ShardId(shard as u16);
        }
        slots.into_iter().max().unwrap_or(0)
    }

    pub(crate) fn alloc_table(&mut self) -> TableId {
        let id = TableId(self.counters.next_table.max(1));
        self.counters.next_table = id.0 + 1;
        id
    }

    pub(crate) fn alloc_family(&mut self) -> FamilyId {
        let id = FamilyId(self.counters.next_family.max(1));
        self.counters.next_family = id.0 + 1;
        id
    }

    pub(crate) fn alloc_tablet(&mut self) -> TabletId {
        let id = TabletId(self.counters.next_tablet.max(1));
        self.counters.next_tablet = id.0 + 1;
        id
    }
}

impl Catalog {
    /// Every SST id the catalog names (the full recompute the copy-on-write tests check
    /// against).
    #[cfg(test)]
    pub(crate) fn sst_ids(&self) -> HashSet<SstId> {
        self.ssts
            .values()
            .flat_map(|l| l.iter().map(|(_, m)| m.id))
            .collect()
    }

    /// The SSTs `old` names that this catalog no longer references from any tablet.
    pub(crate) fn removed_since(&self, old: &Catalog) -> Vec<Arc<SstMeta>> {
        if Arc::ptr_eq(&self.ssts, &old.ssts) {
            return Vec::new();
        }
        // Candidates: SSTs of `old`'s slots that changed or went, missing from the new slot.
        // Slots this catalog still shares with `old` cannot have lost one (#499).
        let mut candidates: Vec<Arc<SstMeta>> = Vec::new();
        let mut seen: FastSet<SstId> = FastSet::default();
        for (key, old_list) in old.ssts.iter() {
            let new_list = self.ssts.get(key);
            if new_list.is_some_and(|l| Arc::ptr_eq(l, old_list)) {
                continue;
            }
            for (_, m) in old_list.iter() {
                let kept = new_list.is_some_and(|l| l.iter().any(|(_, n)| n.id == m.id));
                if !kept && seen.insert(m.id) {
                    candidates.push(Arc::clone(m));
                }
            }
        }
        if candidates.is_empty() {
            return candidates;
        }
        // A candidate another slot still holds (shared after a split, decision D13) stays.
        let dead: FastSet<SstId> = self
            .unreferenced(old, candidates.iter().map(|m| m.id).collect())
            .into_iter()
            .collect();
        candidates.retain(|m| dead.contains(&m.id));
        candidates
    }

    /// Whether `id` is referenced by more than one `(tablet, family)` (shared after a split,
    /// decision D13).
    pub(crate) fn sst_shared(&self, id: SstId) -> bool {
        self.ssts
            .values()
            .filter(|l| l.iter().any(|(_, m)| m.id == id))
            .count()
            > 1
    }

    /// The level and meta of `id` within `(tablet, family)`.
    pub(crate) fn sst(
        &self,
        tablet: TabletId,
        family: FamilyId,
        id: SstId,
    ) -> Option<(u8, &Arc<SstMeta>)> {
        self.ssts
            .get(&(tablet, family))?
            .iter()
            .find(|(_, m)| m.id == id)
            .map(|(l, m)| (*l, m))
    }

    /// The tablet entry of `id`.
    pub(crate) fn tablet(&self, id: TabletId) -> Option<&TabletEntry> {
        self.tablets.get(&id)
    }

    /// The families of `table` in creation order.
    /// [`Catalog::family_ids_of`] without collecting them (#499): the scans over every slot
    /// that run after each flush allocate nothing per table.
    pub(crate) fn families_of(&self, table: TableId) -> impl Iterator<Item = FamilyId> + '_ {
        self.tables
            .get(&table)
            .into_iter()
            .flat_map(|t| t.families.iter().map(|f| f.id))
    }

    pub(crate) fn family_ids_of(&self, table: TableId) -> Vec<FamilyId> {
        self.tables
            .get(&table)
            .map(|t| t.families.iter().map(|f| f.id).collect())
            .unwrap_or_default()
    }
}

/// Deterministic tablet ownership: the same for every open with the same shard count.
pub(crate) fn shard_for(tablet: TabletId, shards: usize) -> ShardId {
    ShardId((tablet.0 % shards.max(1) as u64) as u16)
}

#[cfg(test)]
mod cow_tests {
    use super::*;
    use pigeonhole_format::manifest::FamilyOptions;
    use pigeonhole_format::superblock::ExtentRef;

    fn meta(id: u64) -> SstMeta {
        SstMeta {
            id: SstId(id),
            extent: ExtentRef {
                page: 16 * (id + 1),
                size_class: 0,
            },
            len: 4096,
            smallest_key: vec![1],
            largest_key: vec![2],
            seqno_range: (1, 2),
            ts_range: (1, 2),
            entries: 1,
            deletes: 0,
        }
    }

    /// The full recompute `removed_since` replaced: every SST `old` names that no slot of
    /// `new` references.
    fn removed_full(new: &Catalog, old: &Catalog) -> Vec<SstId> {
        let live = new.sst_ids();
        let mut out: Vec<SstId> = old
            .sst_ids()
            .into_iter()
            .filter(|id| !live.contains(id))
            .collect();
        out.sort_unstable();
        out
    }

    /// One slot and its list of `(level, id)`.
    type SlotContents = ((TabletId, FamilyId), Vec<(u8, SstId)>);

    /// Every slot's list, deep-copied (to check an edited clone leaves its source alone).
    fn contents(c: &Catalog) -> Vec<SlotContents> {
        c.ssts
            .iter()
            .map(|(k, l)| (*k, l.iter().map(|(lv, m)| (*lv, m.id)).collect()))
            .collect()
    }

    /// Random batches of edits (#499): the slot-level `removed_since` and
    /// `prune_blob_refs_since` equal the full recomputes, `same_ssts` only reports equal
    /// lists, and editing a clone leaves the catalog it came from unchanged. Prints the seed
    /// on failure.
    #[test]
    fn slot_diffs_equal_the_full_recompute() {
        for seed in 1..=200u64 {
            let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut rand = move |n: u64| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x % n
            };
            let mut cur = Catalog::default();
            for t in 1..=2u32 {
                cur.apply(
                    &Edit::CreateTable {
                        table: TableId(t),
                        name: format!("t{t}"),
                    },
                    4,
                )
                .unwrap();
                for f in 0..2u32 {
                    cur.apply(
                        &Edit::PutFamily {
                            table: TableId(t),
                            family: FamilyId(t * 10 + f),
                            name: format!("f{f}"),
                            options: FamilyOptions::default(),
                        },
                        4,
                    )
                    .unwrap();
                }
            }
            let tablet = |t: u64| Edit::PutTablet {
                tablet: TabletId(t),
                table: TableId(1 + (t % 2) as u32),
                start: vec![t as u8],
                end: None,
            };
            for t in 1..=6u64 {
                cur.apply(&tablet(t), 4).unwrap();
            }
            let mut next_sst = 1u64;
            for batch in 0..30 {
                let old = cur.clone();
                let before = contents(&old);
                let mut new = old.clone();
                for _ in 0..1 + rand(6) {
                    let t = 1 + rand(6);
                    let f = FamilyId((1 + (t % 2) as u32) * 10 + rand(2) as u32);
                    let ids: Vec<SstId> = new.sst_ids().into_iter().collect();
                    let edit = match rand(10) {
                        0..=2 => {
                            next_sst += 1;
                            Edit::AddSst {
                                tablet: TabletId(t),
                                family: f,
                                level: rand(3) as u8,
                                meta: meta(next_sst),
                            }
                        }
                        // An SST another slot holds too (shared after a split, D13).
                        3 if !ids.is_empty() => {
                            let id = ids[rand(ids.len() as u64) as usize];
                            Edit::AddSst {
                                tablet: TabletId(t),
                                family: f,
                                level: 0,
                                meta: meta(id.0),
                            }
                        }
                        4..=5 if !ids.is_empty() => {
                            let id = ids[rand(ids.len() as u64) as usize];
                            let (tablet, family) = *new
                                .ssts
                                .iter()
                                .find(|(_, l)| l.iter().any(|(_, m)| m.id == id))
                                .unwrap()
                                .0;
                            Edit::RemoveSst {
                                tablet,
                                family,
                                sst: id,
                            }
                        }
                        6..=7 => Edit::SstBlobRefs {
                            sst: SstId(1 + rand(next_sst + 2)),
                            refs: vec![(BlobFileId(1), rand(100))],
                        },
                        8 => Edit::DropTablet {
                            tablet: TabletId(t),
                        },
                        _ => tablet(t),
                    };
                    new.apply(&edit, 4).unwrap();
                }
                let mut got: Vec<SstId> = new.removed_since(&old).iter().map(|m| m.id).collect();
                got.sort_unstable();
                assert_eq!(
                    got,
                    removed_full(&new, &old),
                    "seed {seed}, batch {batch}: removed_since"
                );
                let mut full = new.clone();
                full.prune_blob_refs();
                let mut diffed = new.clone();
                diffed.prune_blob_refs_since(&old);
                assert_eq!(
                    diffed.blob_refs, full.blob_refs,
                    "seed {seed}, batch {batch}: prune"
                );
                for key in old.ssts.keys().chain(new.ssts.keys()) {
                    if new.same_ssts(&old, *key) {
                        assert_eq!(
                            new.ssts
                                .get(key)
                                .map(|l| l.iter().map(|(_, m)| m.id).collect::<Vec<_>>()),
                            old.ssts
                                .get(key)
                                .map(|l| l.iter().map(|(_, m)| m.id).collect::<Vec<_>>()),
                            "seed {seed}, batch {batch}: same_ssts on different lists"
                        );
                    }
                }
                assert_eq!(
                    contents(&old),
                    before,
                    "seed {seed}, batch {batch}: the source changed"
                );
                cur = diffed;
            }
        }
    }
}
