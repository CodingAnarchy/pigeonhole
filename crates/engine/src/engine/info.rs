//! What the database holds, from the catalog both roles read (ICR 0047): tables, families
//! with their stored options, tablets, and the SSTs and blob files of each family.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use pigeonhole_format::{FamilyId, FormatVersion, SstId};

use super::{Engine, Role, TableInfo};

/// A snapshot of what the database holds and how it is stored ([`Engine::info`]).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DbInfo {
    /// The database's random id, chosen at creation.
    pub db_id: [u8; 16],
    /// The file format version: a writable open upgrades older files, so an open database
    /// is always at [`FormatVersion::CURRENT`].
    pub format_version: u32,
    /// The role this instance opened in.
    pub role: Role,
    /// Shard count (the writer's, for a reader).
    pub shards: usize,
    /// The main file's length in bytes.
    pub file_bytes: u64,
    /// Bytes of the main file no extent holds, allocated or awaiting reclamation (writer
    /// only: a reader does not track allocations).
    pub free_bytes: Option<u64>,
    /// Every table, in name order.
    pub tables: Vec<TableStorage>,
}

/// One table of [`DbInfo`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableStorage {
    /// The catalog entry: name and families with their options.
    pub info: Arc<TableInfo>,
    /// Tablets the table is split into.
    pub tablets: usize,
    /// Per family, in creation order (the order of `info.families`).
    pub families: Vec<FamilyStorage>,
}

/// One family's files ([`TableStorage::families`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FamilyStorage {
    /// The family.
    pub family: FamilyId,
    /// Per level holding SSTs, lowest level first.
    pub levels: Vec<LevelStorage>,
    /// Blob files of the family.
    pub blob_files: u64,
    /// Their bytes.
    pub blob_bytes: u64,
    /// Bytes of those that live SSTs still reference.
    pub blob_live_bytes: u64,
}

/// SSTs at one level of one family ([`FamilyStorage::levels`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LevelStorage {
    /// The level (0 is the newest).
    pub level: u8,
    /// SSTs, each counted once even when several tablets share it.
    pub ssts: u64,
    /// Their bytes.
    pub bytes: u64,
    /// Entries (cell versions and tombstones) they hold.
    pub entries: u64,
}

impl Engine {
    /// What the database holds and how it is stored (ICR 0047). Cheap: reads the current
    /// catalog, no file I/O beyond the file's length.
    pub fn info(&self) -> crate::Result<DbInfo> {
        let inner = &self.inner;
        let shared = &inner.shared;
        let catalog = inner.catalog();
        let pager = &shared.pager;
        let file_bytes = pager.data_file().len()?;
        let free_bytes = (inner.role == Role::Writer).then(|| {
            let s = pager.stats();
            file_bytes.saturating_sub(s.allocated_bytes + s.retired_bytes)
        });

        // Per (family, level): the distinct SSTs. A split leaves one SST shared by both
        // halves until compaction rewrites it.
        let mut levels: BTreeMap<(FamilyId, u8), (HashSet<SstId>, LevelStorage)> = BTreeMap::new();
        for ((_, family), list) in catalog.ssts.iter() {
            for (level, meta) in list.iter() {
                let (seen, l) = levels.entry((*family, *level)).or_insert_with(|| {
                    (
                        HashSet::new(),
                        LevelStorage {
                            level: *level,
                            ..LevelStorage::default()
                        },
                    )
                });
                if seen.insert(meta.id) {
                    l.ssts += 1;
                    l.bytes += meta.len;
                    l.entries += meta.entries;
                }
            }
        }
        let mut blobs: BTreeMap<FamilyId, (u64, u64, u64)> = BTreeMap::new();
        for b in catalog.blob_files.values() {
            let e = blobs.entry(b.family).or_default();
            e.0 += 1;
            e.1 += b.total_bytes;
            e.2 += b.live_bytes;
        }
        let mut tablets: BTreeMap<_, usize> = BTreeMap::new();
        for t in catalog.tablets() {
            *tablets.entry(t.table).or_default() += 1;
        }

        let mut tables: Vec<TableStorage> = catalog
            .tables()
            .map(|t| TableStorage {
                info: Arc::clone(t),
                tablets: tablets.get(&t.id).copied().unwrap_or(0),
                families: t
                    .families
                    .iter()
                    .map(|f| {
                        let (blob_files, blob_bytes, blob_live_bytes) =
                            blobs.get(&f.id).copied().unwrap_or_default();
                        FamilyStorage {
                            family: f.id,
                            levels: levels
                                .range((f.id, 0)..=(f.id, u8::MAX))
                                .map(|(_, (_, l))| *l)
                                .collect(),
                            blob_files,
                            blob_bytes,
                            blob_live_bytes,
                        }
                    })
                    .collect(),
            })
            .collect();
        tables.sort_by(|a, b| a.info.name.cmp(&b.info.name));

        Ok(DbInfo {
            db_id: pager.db_id(),
            format_version: FormatVersion::CURRENT.0,
            role: inner.role,
            shards: shared.shards,
            file_bytes,
            free_bytes,
            tables,
        })
    }
}
