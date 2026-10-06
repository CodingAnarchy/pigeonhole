//! The catalog: tables, families, tablets, counters and everything else the manifest
//! persists, rebuilt from manifest edits at open and updated by every manifest commit.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use pigeonhole_format::manifest::{Edit, FamilyOptions, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{BlobFileId, FamilyId, Lsn, Seqno, StreamId, TableId, TabletId, Timestamp};
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
    /// An operator this process has not registered: reads of merged cells fail.
    Unknown,
}

/// Everything a read or write needs to know about a family, by id.
#[derive(Debug, Clone)]
pub(crate) struct FamilyMeta {
    pub table: TableId,
    pub options: FamilyOptions,
    pub merge: MergeKind,
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
    /// Whether any family names a merge operator this process cannot run.
    pub(crate) has_unknown_merge: bool,
}

impl Catalog {
    /// Applies one edit. Shard assignment of tablets is `tablet % shards`.
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
                let merge = match options.merge_operator.as_str() {
                    "" => MergeKind::None,
                    I64_ADD => MergeKind::I64Add,
                    _ => {
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
        edits
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

    /// Re-derives tablet ownership for `shards` shards (a reopen with another shard count).
    pub(crate) fn reassign(&mut self, shards: usize) {
        for t in self.tablets.values_mut() {
            t.shard = shard_for(t.id, shards);
        }
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

/// Deterministic tablet ownership: the same for every open with the same shard count.
pub(crate) fn shard_for(tablet: TabletId, shards: usize) -> ShardId {
    ShardId((tablet.0 % shards.max(1) as u64) as u16)
}
