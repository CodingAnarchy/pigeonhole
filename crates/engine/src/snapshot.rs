//! Views and snapshots: the immutable picture of the database a read uses (tablet map,
//! memtables and the open SST set of one manifest version), and the registries that tell
//! flush and compaction which views and seqnos are still live in this process.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use pigeonhole_cache::{BlockCache, Cell, Priority};
use pigeonhole_compaction::{Levels, blob_pointer};
use pigeonhole_format::key::row_prefix_len;
use pigeonhole_format::manifest::{CachePriority, SstMeta};
use pigeonhole_format::shm::{ViewMemtable, ViewRecord, ViewTablet};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::value::BlobPointer;
use pigeonhole_format::{BlobFileId, FamilyId, ManifestVersion, Seqno, SstId, TableId, TabletId};
use pigeonhole_io::FileRef;
use pigeonhole_memtable::MemtableReader;
use pigeonhole_runtime::ShardId;
use pigeonhole_shm::ShmRegion;
use pigeonhole_sst::{BlobReader, SstReader};

use crate::catalog::Catalog;
use crate::{Error, Result};

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
/// At open every tablet goes to shard `tablet % shards` (with `EngineOptions::tablet_changes`
/// on, unless that shard's arena would serve too many slots). With it on, splits, merges and the balancer's moves hand tablets to other shards while the
/// database runs, and each change publishes a new map with a higher version. Which shard applies a row never changes the result.
///
/// ```
/// use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions};
/// use pigeonhole_io::sim::SimVfs;
///
/// # fn main() -> pigeonhole_engine::Result<()> {
/// let mut options = EngineOptions::new(SimVfs::new(1));
/// options.create_if_missing = true;
/// options.shards = 2;
/// options.memtable_budget = 4 << 20;
/// let db = Engine::open("/db/data.phdb".as_ref(), options)?;
/// let t = db.create_table("t", &[("f".into(), FamilyOptions::default())])?;
/// let snap = db.snapshot()?;
/// // A new table is one tablet covering every row.
/// let (tablet, shard) = snap.view().tablets().route(t.id, b"any row").unwrap();
/// assert_eq!(snap.view().tablets().route(t.id, b""), Some((tablet, shard)));
/// db.close()?;
/// # Ok(())
/// # }
/// ```
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

    /// The entry of tablet `id`, if it exists.
    pub(crate) fn entry(&self, id: TabletId) -> Option<&TabletEntry> {
        self.iter().find(|t| t.id == id)
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

impl MemSet {
    /// The same set without the memtables whose roots are in `drop`.
    pub(crate) fn without(&self, drop: &dyn Fn(u32) -> bool) -> Option<Arc<MemSet>> {
        if !self.roots.iter().any(|r| drop(*r)) {
            return None;
        }
        let mut readers = Vec::with_capacity(self.readers.len());
        let mut roots = Vec::with_capacity(self.roots.len());
        for (r, root) in self.readers.iter().zip(&self.roots) {
            if !drop(*root) {
                readers.push(r.clone());
                roots.push(*root);
            }
        }
        Some(Arc::new(MemSet {
            shard: self.shard,
            readers,
            roots,
        }))
    }
}

/// One shard's memtable sets, as a view holds them. A shard publishes a new piece when one
/// of its memtables is created or frozen; the other shards' pieces are shared by reference.
#[derive(Debug, Default)]
pub(crate) struct ShardMems {
    pub map: HashMap<(TabletId, FamilyId), Arc<MemSet>>,
}

impl ShardMems {
    /// The piece without the memtables `drop` names, or `None` if it holds none of them.
    pub(crate) fn without(&self, drop: &dyn Fn(u32) -> bool) -> Option<Arc<ShardMems>> {
        let mut changed = false;
        let mut map = HashMap::with_capacity(self.map.len());
        for (k, set) in &self.map {
            match set.without(drop) {
                Some(s) => {
                    changed = true;
                    map.insert(*k, s);
                }
                None => {
                    map.insert(*k, Arc::clone(set));
                }
            }
        }
        changed.then(|| Arc::new(ShardMems { map }))
    }
}

/// An SST the manifest names, with its reader opened on first use (or handed in by the
/// flush or compaction that wrote it, so a hot path never opens one).
pub(crate) struct OpenSst {
    pub meta: Arc<SstMeta>,
    reader: OnceLock<Arc<SstReader>>,
}

impl std::fmt::Debug for OpenSst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenSst")
            .field("id", &self.meta.id)
            .field("open", &self.reader.get().is_some())
            .finish()
    }
}

impl OpenSst {
    pub(crate) fn new(meta: Arc<SstMeta>, reader: Option<Arc<SstReader>>) -> Self {
        let slot = OnceLock::new();
        if let Some(r) = reader {
            let _ = slot.set(r);
        }
        Self { meta, reader: slot }
    }

    /// The row prefix of the smallest key.
    pub(crate) fn first_row(&self) -> &[u8] {
        row_of(&self.meta.smallest_key)
    }

    /// The row prefix of the largest key.
    pub(crate) fn last_row(&self) -> &[u8] {
        row_of(&self.meta.largest_key)
    }

    /// The reader, opened through `set` if this is its first use.
    pub(crate) fn reader(&self, set: &SstSet, priority: Priority) -> Result<Arc<SstReader>> {
        if let Some(r) = self.reader.get() {
            return Ok(Arc::clone(r));
        }
        let r = Arc::new(SstReader::open(
            set.file.clone(),
            &self.meta,
            Arc::clone(&set.cache),
            priority,
        )?);
        let _ = self.reader.set(Arc::clone(&r));
        Ok(self.reader.get().map_or(r, Arc::clone))
    }
}

/// A blob file the manifest names, with its reader opened on first use. A file's extents
/// never change once it is published, so versions share it by id.
pub(crate) struct OpenBlob {
    id: BlobFileId,
    extents: Vec<ExtentRef>,
    reader: OnceLock<Arc<BlobReader>>,
}

impl std::fmt::Debug for OpenBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenBlob")
            .field("id", &self.id)
            .field("open", &self.reader.get().is_some())
            .finish()
    }
}

impl OpenBlob {
    /// The reader, opened through `set` if this is its first use (no I/O: extent headers are
    /// verified by the first read that touches them).
    pub(crate) fn reader(&self, set: &SstSet) -> Arc<BlobReader> {
        Arc::clone(self.reader.get_or_init(|| {
            Arc::new(BlobReader::new(
                set.file.clone(),
                self.id,
                self.extents.clone(),
                Arc::clone(&set.cache),
            ))
        }))
    }
}

/// The row prefix of an internal key (the whole key if it has none).
pub(crate) fn row_of(key: &[u8]) -> &[u8] {
    &key[..row_prefix_len(key).unwrap_or(key.len())]
}

/// The SSTs of one `(tablet, family)` by level: level 0 newest first, deeper levels sorted
/// by key and disjoint.
#[derive(Debug, Default)]
pub(crate) struct FamilySsts {
    pub levels: Vec<Vec<Arc<OpenSst>>>,
}

impl FamilySsts {
    /// The picker's view of the levels.
    pub(crate) fn levels_meta(&self) -> Levels {
        Levels {
            levels: self
                .levels
                .iter()
                .map(|l| l.iter().map(|s| Arc::clone(&s.meta)).collect())
                .collect(),
        }
    }

    /// Every SST, newest level first.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Arc<OpenSst>> {
        self.levels.iter().flatten()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.levels.iter().all(Vec::is_empty)
    }

    pub(crate) fn find(&self, id: SstId) -> Option<(u8, &Arc<OpenSst>)> {
        self.levels
            .iter()
            .enumerate()
            .find_map(|(l, files)| files.iter().find(|s| s.meta.id == id).map(|s| (l as u8, s)))
    }
}

/// The open SST set of one manifest version: every `(tablet, family)`'s levels, with
/// readers shared across versions by SST id.
pub(crate) struct SstSet {
    pub file: FileRef,
    pub cache: Arc<BlockCache>,
    pub map: HashMap<(TabletId, FamilyId), Arc<FamilySsts>>,
    by_id: HashMap<SstId, Arc<OpenSst>>,
    /// The blob files of this manifest version.
    blobs: HashMap<BlobFileId, Arc<OpenBlob>>,
}

impl std::fmt::Debug for SstSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstSet")
            .field("families", &self.map.len())
            .field("ssts", &self.by_id.len())
            .field("blob_files", &self.blobs.len())
            .finish()
    }
}

impl SstSet {
    /// An empty set over `file` and `cache`.
    pub(crate) fn empty(file: FileRef, cache: Arc<BlockCache>) -> Self {
        Self {
            file,
            cache,
            map: HashMap::new(),
            by_id: HashMap::new(),
            blobs: HashMap::new(),
        }
    }

    /// Builds the set of `catalog`, reusing the open SSTs of `prev` and taking the readers
    /// in `readers` for SSTs just written.
    pub(crate) fn build(
        catalog: &Catalog,
        prev: Option<&SstSet>,
        readers: &mut HashMap<SstId, Arc<SstReader>>,
        file: FileRef,
        cache: Arc<BlockCache>,
    ) -> Self {
        let mut by_id: HashMap<SstId, Arc<OpenSst>> = HashMap::new();
        let mut map = HashMap::new();
        for (key, list) in &catalog.ssts {
            let mut levels: Vec<Vec<Arc<OpenSst>>> = Vec::new();
            for (level, meta) in list {
                let open = match by_id.get(&meta.id) {
                    Some(o) => Arc::clone(o),
                    None => {
                        let o = prev
                            .and_then(|p| p.by_id.get(&meta.id))
                            .map(Arc::clone)
                            .unwrap_or_else(|| {
                                Arc::new(OpenSst::new(Arc::clone(meta), readers.remove(&meta.id)))
                            });
                        by_id.insert(meta.id, Arc::clone(&o));
                        o
                    }
                };
                let l = usize::from(*level);
                if levels.len() <= l {
                    levels.resize_with(l + 1, Vec::new);
                }
                levels[l].push(open);
            }
            for (l, files) in levels.iter_mut().enumerate() {
                if l == 0 {
                    // Newest first: by largest seqno, then by id.
                    files.sort_by(|a, b| {
                        (b.meta.seqno_range.1, b.meta.id.0)
                            .cmp(&(a.meta.seqno_range.1, a.meta.id.0))
                    });
                } else {
                    files.sort_by(|a, b| a.meta.smallest_key.cmp(&b.meta.smallest_key));
                }
            }
            map.insert(*key, Arc::new(FamilySsts { levels }));
        }
        let blobs = catalog
            .blob_files
            .iter()
            .map(|(id, b)| {
                // A file whose extents a shrink replaced (#231) gets a new reader; views of
                // older versions keep the old one, which reads the retired extents they pin.
                let open = prev
                    .and_then(|p| p.blobs.get(id))
                    .filter(|o| o.extents == b.extents)
                    .map(Arc::clone)
                    .unwrap_or_else(|| {
                        Arc::new(OpenBlob {
                            id: *id,
                            extents: b.extents.clone(),
                            reader: OnceLock::new(),
                        })
                    });
                (*id, open)
            })
            .collect();
        Self {
            file,
            cache,
            map,
            by_id,
            blobs,
        }
    }

    /// The reader of blob file `id`, if this version names it.
    pub(crate) fn blob_reader(&self, id: BlobFileId) -> Option<Arc<BlobReader>> {
        self.blobs.get(&id).map(|b| b.reader(self))
    }

    /// The stored value a separated value names, or `None` if `stored` is not a blob pointer.
    /// A pointer into a blob file this version does not name is corruption.
    pub(crate) fn read_blob(&self, stored: &[u8]) -> Result<Option<Cell>> {
        match blob_pointer(stored) {
            Some(ptr) => self.read_pointer(&ptr).map(Some),
            None => Ok(None),
        }
    }

    /// The stored value `ptr` names.
    pub(crate) fn read_pointer(&self, ptr: &BlobPointer) -> Result<Cell> {
        let reader = self.blob_reader(ptr.blob_file).ok_or_else(|| {
            Error::Corruption(format!(
                "a blob pointer names blob file {}, which the manifest does not list",
                ptr.blob_file.0
            ))
        })?;
        Ok(reader.read(ptr)?)
    }

    /// Whether this version names any blob file.
    pub(crate) fn has_blobs(&self) -> bool {
        !self.blobs.is_empty()
    }

    pub(crate) fn family(&self, tablet: TabletId, family: FamilyId) -> Option<&Arc<FamilySsts>> {
        self.map.get(&(tablet, family))
    }

    /// The cache priority of a family option.
    pub(crate) fn priority(p: CachePriority) -> Priority {
        match p {
            CachePriority::Low => Priority::Low,
            CachePriority::Normal => Priority::Normal,
            CachePriority::High => Priority::High,
        }
    }
}

/// The manifest versions of every live in-process view, so the pager reclaims only extents
/// no view can reach (decision D61).
#[derive(Debug, Default)]
pub(crate) struct LiveViews {
    versions: Mutex<BTreeMap<ManifestVersion, u32>>,
}

impl LiveViews {
    fn register(&self, v: ManifestVersion) {
        *self
            .versions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(v)
            .or_insert(0) += 1;
    }

    fn unregister(&self, v: ManifestVersion) {
        let mut m = self.versions.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(n) = m.get_mut(&v) {
            *n -= 1;
            if *n == 0 {
                m.remove(&v);
            }
        }
    }

    /// The oldest manifest version any live view uses.
    pub(crate) fn oldest(&self) -> Option<ManifestVersion> {
        self.versions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .next()
            .copied()
    }
}

/// Keeps a view's manifest version registered while the view lives.
#[derive(Debug)]
pub(crate) struct ViewPin {
    registry: Arc<LiveViews>,
    version: ManifestVersion,
}

impl ViewPin {
    pub(crate) fn new(registry: &Arc<LiveViews>, version: ManifestVersion) -> Self {
        registry.register(version);
        Self {
            registry: Arc::clone(registry),
            version,
        }
    }
}

impl Drop for ViewPin {
    fn drop(&mut self) {
        self.registry.unregister(self.version);
    }
}

/// Every live in-process snapshot seqno, for compaction's `GcPolicy::snapshots`.
#[derive(Debug, Default)]
pub(crate) struct LiveSeqnos {
    seqnos: Mutex<BTreeMap<Seqno, u32>>,
}

impl LiveSeqnos {
    /// The live seqnos, ascending.
    pub(crate) fn list(&self) -> Vec<Seqno> {
        self.seqnos
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }
}

/// Keeps a snapshot's seqno registered while any clone of the snapshot lives.
#[derive(Debug)]
pub(crate) struct SeqnoPin {
    registry: Arc<LiveSeqnos>,
    seqno: Seqno,
}

impl SeqnoPin {
    /// Reads the visible seqno with `visible` and registers it, in one critical section with
    /// the GC's reading of the list (`compact::gc_snapshots`): a flush or compaction that read
    /// the list before saw only inputs at or below this seqno (reads at it equal latest for
    /// them), and one that reads it after keeps its versions (#315 review). The caller loads
    /// the view afterwards.
    pub(crate) fn pin_visible(registry: &Arc<LiveSeqnos>, visible: impl FnOnce() -> Seqno) -> Self {
        let mut seqnos = registry
            .seqnos
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let seqno = visible();
        *seqnos.entry(seqno).or_insert(0) += 1;
        drop(seqnos);
        Self {
            registry: Arc::clone(registry),
            seqno,
        }
    }

    /// The pinned seqno.
    pub(crate) fn seqno(&self) -> Seqno {
        self.seqno
    }
}

impl Drop for SeqnoPin {
    fn drop(&mut self) {
        let mut m = self
            .registry
            .seqnos
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(n) = m.get_mut(&self.seqno) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.seqno);
            }
        }
    }
}

/// One immutable, consistent picture of the database: the tablet map, every tablet's active
/// and frozen memtables, and the SST set of one manifest version. Any change to any of these
/// publishes a new view. A frozen memtable stays in every new view until its flushed SST is
/// in the manifest, and never appears together with that SST.
#[derive(Debug)]
pub struct View {
    pub(crate) version: u64,
    pub(crate) manifest_version: ManifestVersion,
    pub(crate) tablets: Arc<TabletMap>,
    pub(crate) catalog: Arc<Catalog>,
    /// One piece per shard.
    pub(crate) mems: Vec<Arc<ShardMems>>,
    pub(crate) ssts: Arc<SstSet>,
    /// Registered in the writer's live-view registry while the view lives (none in a
    /// reader process).
    pub(crate) _pin: Option<ViewPin>,
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
    pub count: Arc<AtomicUsize>,
    /// The region generation the snapshot was taken in: its pin lives there.
    pub shm: ShmRegion,
}

impl LiveSnapshot {
    /// Fails with [`Error::SnapshotExpired`] once a new writer generation exists. The pin
    /// lives in the abandoned region, so the new writer may have freed and reused the
    /// extents the snapshot names (issue #140). A read through the snapshot checks this
    /// **after** it read, seqlock style: a writer marks the old region abandoned and
    /// publishes its generation before it allocates anything, so a read that finished before
    /// either is visible read what the snapshot names.
    pub(crate) fn check_current(&self) -> Result<()> {
        // Orders the read's loads (shared-memory arenas included) before the state load.
        std::sync::atomic::fence(Ordering::Acquire);
        if self.shm.is_stale() {
            return Err(Error::SnapshotExpired);
        }
        Ok(())
    }
}

impl Drop for LiveSnapshot {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A seqno plus the view current when it was taken. Reads through a snapshot ignore newer
/// commits and use only its view. Holding it pins the view's memtables and SST extents
/// (epoch-based; no locks) and keeps compaction from dropping anything the snapshot can see.
/// Cheap to clone.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) seqno: Seqno,
    pub(crate) view: Arc<View>,
    /// Reader processes only: counted while any clone of this snapshot lives (a drop guard).
    pub(crate) _live: Option<Arc<LiveSnapshot>>,
    /// Writer process: the seqno stays in the live-snapshot list while any clone lives.
    pub(crate) _pin: Option<Arc<SeqnoPin>>,
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

    /// Reader processes: [`Error::SnapshotExpired`] if a writer restarted since the snapshot
    /// was taken; call after a read through it (see `LiveSnapshot::check_current`). Always
    /// `Ok` in the writer process.
    pub(crate) fn check_current(&self) -> Result<()> {
        self._live.as_ref().map_or(Ok(()), |l| l.check_current())
    }

    /// `result` of a read through this snapshot, or [`Error::SnapshotExpired`] in its place
    /// when the snapshot expired during the read (whatever the read returned).
    pub(crate) fn checked<T>(&self, result: Result<T>) -> Result<T> {
        self.check_current()?;
        result
    }

    /// The same view at an older seqno (a view covers every seqno at or below the one it was
    /// taken with). A test hook for recovery checks (the `test-hooks` feature); not part of
    /// the stable API. Compaction may have dropped versions no live snapshot needed, so only
    /// seqnos above every compaction's inputs read as the model does.
    #[cfg(feature = "test-hooks")]
    pub fn at_seqno(&self, seqno: Seqno) -> Snapshot {
        Snapshot {
            seqno: seqno.min(self.seqno),
            view: Arc::clone(&self.view),
            _live: self._live.clone(),
            _pin: self._pin.clone(),
        }
    }
}
