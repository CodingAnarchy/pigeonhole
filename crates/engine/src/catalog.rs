//! The catalog: tables, families, tablets, counters and everything else the manifest
//! persists, rebuilt from manifest edits at open and updated by every manifest commit.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use pigeonhole_compaction::{MergeOperator, MergeRegistry};
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
pub struct Catalog {
    tables: BTreeMap<TableId, Arc<TableInfo>>,
    by_name: HashMap<String, TableId>,
    families: HashMap<FamilyId, FamilyMeta>,
    tablets: BTreeMap<TabletId, TabletEntry>,
    pub(crate) counters: Counters,
    pub(crate) checkpoints: BTreeMap<StreamId, Lsn>,
    pub(crate) flushed: BTreeMap<(TabletId, FamilyId), Seqno>,
    /// `(tablet, family) -> [(level, meta)]`, in manifest order.
    pub(crate) ssts: BTreeMap<(TabletId, FamilyId), SstList>,
    pub(crate) blob_files: BTreeMap<BlobFileId, BlobFile>,
    /// Per SST, the blob files its puts point into and the bytes they reference
    /// (`SstBlobRefs`, #240). An SST without an entry (written by an older build) may point
    /// into any blob file of its family. Entries of SSTs no tablet references any more are
    /// dropped by [`Catalog::prune_blob_refs`] after each batch of edits.
    pub(crate) blob_refs: HashMap<SstId, Vec<(BlobFileId, u64)>>,
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
                self.by_name.insert(name.clone(), *table);
                self.tables.insert(*table, info);
            }
            Edit::DropTable { table } => {
                if let Some(info) = self.tables.remove(table) {
                    self.by_name.remove(&info.name);
                    for f in &info.families {
                        self.families.remove(&f.id);
                    }
                }
                let dropped: Vec<TabletId> = self
                    .tablets
                    .values()
                    .filter(|t| t.table == *table)
                    .map(|t| t.id)
                    .collect();
                for id in dropped {
                    self.tablets.remove(&id);
                    self.flushed.retain(|k, _| k.0 != id);
                    self.ssts.retain(|k, _| k.0 != id);
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
                self.families.insert(
                    *family,
                    FamilyMeta {
                        table: *table,
                        options: options.clone(),
                        merge,
                        merge_op,
                    },
                );
                self.tables.insert(*table, Arc::new(info));
            }
            Edit::PutTablet {
                tablet,
                table,
                start,
                end,
            } => {
                self.tablets.insert(
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
            Edit::DropTablet { tablet } => {
                self.tablets.remove(tablet);
                self.flushed.retain(|k, _| k.0 != *tablet);
                self.ssts.retain(|k, _| k.0 != *tablet);
            }
            Edit::AddSst {
                tablet,
                family,
                level,
                meta,
            } => {
                self.ssts
                    .entry((*tablet, *family))
                    .or_default()
                    .push((*level, Arc::new(meta.clone())));
            }
            Edit::RemoveSst {
                tablet,
                family,
                sst,
            } => {
                if let Some(list) = self.ssts.get_mut(&(*tablet, *family)) {
                    list.retain(|(_, m)| m.id != *sst);
                }
            }
            Edit::SetFlushed {
                tablet,
                family,
                seqno,
            } => {
                let e = self.flushed.entry((*tablet, *family)).or_default();
                *e = (*e).max(*seqno);
            }
            Edit::WalCheckpoint { stream, lsn } => {
                self.checkpoints.insert(*stream, *lsn);
            }
            Edit::PutBlobFile {
                blob_file,
                family,
                extents,
                total_bytes,
                live_bytes,
            } => {
                self.blob_files.insert(
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
                self.blob_files.remove(blob_file);
            }
            Edit::SstBlobRefs { sst, refs } => {
                self.blob_refs.insert(*sst, refs.clone());
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
        for ((tablet, family), list) in &self.ssts {
            for (level, meta) in list {
                edits.push(Edit::AddSst {
                    tablet: *tablet,
                    family: *family,
                    level: *level,
                    meta: (**meta).clone(),
                });
            }
        }
        for ((tablet, family), seqno) in &self.flushed {
            edits.push(Edit::SetFlushed {
                tablet: *tablet,
                family: *family,
                seqno: *seqno,
            });
        }
        for (stream, lsn) in &self.checkpoints {
            edits.push(Edit::WalCheckpoint {
                stream: *stream,
                lsn: *lsn,
            });
        }
        for (id, b) in &self.blob_files {
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
        if self.blob_refs.is_empty() {
            return;
        }
        let live: HashSet<SstId> = self
            .ssts
            .values()
            .flat_map(|l| l.iter().map(|(_, m)| m.id))
            .collect();
        self.blob_refs.retain(|id, _| live.contains(id));
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
        if let Some(t) = self.tablets.get_mut(&id) {
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
            for (_, m) in list {
                *refs.entry(m.id).or_insert(0) += 1;
            }
        }
        refs
    }

    /// Re-derives tablet ownership for `shards` shards (a reopen with another shard count).
    pub(crate) fn reassign(&mut self, shards: usize) {
        for t in self.tablets.values_mut() {
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
        for t in self.tablets.values_mut() {
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
    /// Every SST id the catalog names.
    pub(crate) fn sst_ids(&self) -> HashSet<SstId> {
        self.ssts
            .values()
            .flat_map(|l| l.iter().map(|(_, m)| m.id))
            .collect()
    }

    /// The SSTs `old` names that this catalog no longer references from any tablet.
    pub(crate) fn removed_since(&self, old: &Catalog) -> Vec<Arc<SstMeta>> {
        let live = self.sst_ids();
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for list in old.ssts.values() {
            for (_, m) in list {
                if !live.contains(&m.id) && seen.insert(m.id) {
                    out.push(Arc::clone(m));
                }
            }
        }
        out
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
