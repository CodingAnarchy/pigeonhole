use std::collections::HashMap;
use std::sync::Arc;

use pigeonhole_format::shm::{ViewMemtable, ViewRecord, ViewTablet};
use pigeonhole_format::{FamilyId, ManifestVersion, Seqno, TableId, TabletId};
use pigeonhole_memtable::MemtableReader;
use pigeonhole_runtime::ShardId;

use crate::catalog::Catalog;

/// One tablet of the routing table: a contiguous row range of one table and its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TabletEntry {
    pub id: TabletId,
    pub table: TableId,
    /// Inclusive start row; empty means unbounded.
    pub start: Vec<u8>,
    /// Exclusive end row; `None` means unbounded.
    pub end: Option<Vec<u8>>,
    pub shard: ShardId,
}

/// An immutable routing table: for each table, its tablets' row ranges and owning shards.
/// Swapped atomically as a whole; read without locks.
///
/// Tablets are assigned to shards deterministically from their id (`tablet % shards`), so
/// the same database opened with the same shard count routes the same way every time, and a
/// different shard count only changes which shard applies a row, never the result.
#[derive(Debug, Default)]
pub struct TabletMap {
    version: u64,
    /// Per table, tablets sorted by start row.
    tables: HashMap<TableId, Vec<TabletEntry>>,
}

impl TabletMap {
    pub(crate) fn build(version: u64, tablets: &[TabletEntry]) -> Self {
        let mut tables: HashMap<TableId, Vec<TabletEntry>> = HashMap::new();
        for t in tablets {
            tables.entry(t.table).or_default().push(t.clone());
        }
        for list in tables.values_mut() {
            list.sort_by(|a, b| a.start.cmp(&b.start));
        }
        Self { version, tables }
    }

    /// Version (bumped by every split, merge or move).
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The tablet holding `row` of `table` and its owner (binary search).
    pub fn route(&self, table: TableId, row: &[u8]) -> Option<(TabletId, ShardId)> {
        let list = self.tables.get(&table)?;
        // The last tablet whose start is <= row.
        let idx = list.partition_point(|t| t.start.as_slice() <= row);
        let t = list.get(idx.checked_sub(1)?)?;
        match &t.end {
            Some(end) if row >= end.as_slice() => None,
            _ => Some((t.id, t.shard)),
        }
    }

    /// The tablets of `table` in row order.
    pub(crate) fn tablets_of(&self, table: TableId) -> &[TabletEntry] {
        self.tables.get(&table).map_or(&[], Vec::as_slice)
    }

    /// Every tablet, in table and row order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &TabletEntry> {
        let mut ids: Vec<&TableId> = self.tables.keys().collect();
        ids.sort();
        ids.into_iter().flat_map(|t| self.tables[t].iter())
    }

    pub(crate) fn to_view_tablets(&self) -> Vec<ViewTablet> {
        self.iter()
            .map(|t| ViewTablet {
                tablet: t.id,
                table: t.table,
                shard: t.shard.0,
                start: t.start.clone(),
                end: t.end.clone(),
            })
            .collect()
    }
}

/// The memtables of one `(tablet, family)` as a view sees them: the active one first, then
/// frozen ones newest first. Readers of any thread or process open them through
/// [`MemtableReader`]s.
#[derive(Debug, Clone)]
pub(crate) struct MemSet {
    pub shard: ShardId,
    /// Active first, then frozen newest first.
    pub readers: Vec<MemtableReader>,
    /// Header offsets within the shard arena, parallel to `readers`.
    pub roots: Vec<u32>,
}

/// One shard's memtable sets, as a view holds them. A shard publishes a new piece when one
/// of its memtables is created or frozen; the other shards' pieces are shared by reference.
#[derive(Debug, Default)]
pub(crate) struct ShardMems {
    pub map: HashMap<(TabletId, FamilyId), Arc<MemSet>>,
}

/// One immutable, consistent picture of the database: the tablet map, every tablet's active
/// and frozen memtables, and the SST set of one manifest version. Any change to any of these
/// publishes a new view. A frozen memtable stays in every new view until its flushed SST is
/// in the manifest.
#[derive(Debug)]
pub struct View {
    pub(crate) version: u64,
    pub(crate) manifest_version: ManifestVersion,
    pub(crate) tablets: Arc<TabletMap>,
    pub(crate) catalog: Arc<Catalog>,
    /// One piece per shard.
    pub(crate) mems: Vec<Arc<ShardMems>>,
}

impl View {
    /// View version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Manifest version of its SST set.
    pub fn manifest_version(&self) -> ManifestVersion {
        self.manifest_version
    }

    /// The tablet map.
    pub fn tablets(&self) -> &TabletMap {
        &self.tablets
    }

    pub(crate) fn memtables(
        &self,
        shard: ShardId,
        tablet: TabletId,
        family: FamilyId,
    ) -> Option<&Arc<MemSet>> {
        self.mems
            .get(usize::from(shard.0))?
            .map
            .get(&(tablet, family))
    }

    /// Every memtable set of every shard.
    pub(crate) fn all_memtables(
        &self,
    ) -> impl Iterator<Item = (&(TabletId, FamilyId), &Arc<MemSet>)> {
        self.mems.iter().flat_map(|m| m.map.iter())
    }

    /// The record published in shared memory for this view.
    pub(crate) fn to_record(&self) -> ViewRecord {
        let mut memtables = Vec::new();
        let mut entries: Vec<(&(TabletId, FamilyId), &Arc<MemSet>)> =
            self.all_memtables().collect();
        entries.sort_by_key(|(k, _)| **k);
        for (key, set) in entries {
            for (age, root) in set.roots.iter().enumerate() {
                memtables.push(ViewMemtable {
                    tablet: key.0,
                    family: key.1,
                    shard: set.shard.0,
                    age: age.min(u8::MAX as usize) as u8,
                    root: *root,
                });
            }
        }
        ViewRecord {
            view_version: self.version,
            manifest_version: self.manifest_version,
            tablets: self.tablets.to_view_tablets(),
            memtables,
        }
    }
}

/// Counts the live snapshots of a reader process: when it drops to zero the reader moves
/// its slot pin forward at the next snapshot, so a long-lived reader never blocks
/// reclamation for ever.
#[derive(Debug)]
pub(crate) struct LiveSnapshot {
    pub count: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for LiveSnapshot {
    fn drop(&mut self) {
        self.count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// A seqno plus the view current when it was taken. Reads through a snapshot ignore newer
/// commits and use only its view. Holding it pins the view's memtables and SST extents
/// (epoch-based; no locks). Cheap to clone.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) seqno: Seqno,
    pub(crate) view: Arc<View>,
    /// Reader processes only: counted while any clone of this snapshot lives (a drop guard).
    pub(crate) _live: Option<Arc<LiveSnapshot>>,
}

impl Snapshot {
    /// The snapshot seqno.
    pub fn seqno(&self) -> Seqno {
        self.seqno
    }

    /// The pinned view.
    pub fn view(&self) -> &Arc<View> {
        &self.view
    }

    /// The same view at an older seqno (a view covers every seqno at or below the one it was
    /// taken with). A test hook for recovery checks (the `test-hooks` feature); not part of
    /// the stable API.
    #[cfg(feature = "test-hooks")]
    pub fn at_seqno(&self, seqno: Seqno) -> Snapshot {
        Snapshot {
            seqno: seqno.min(self.seqno),
            view: Arc::clone(&self.view),
            _live: self._live.clone(),
        }
    }
}
