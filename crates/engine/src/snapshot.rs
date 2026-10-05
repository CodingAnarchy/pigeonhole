use std::sync::Arc;

use pigeonhole_format::{ManifestVersion, Seqno, TableId, TabletId};
use pigeonhole_runtime::ShardId;

/// An immutable routing table: for each table, its tablets' row ranges and owning shards.
/// Swapped atomically as a whole; read without locks.
#[derive(Debug)]
pub struct TabletMap {
    _priv: (),
}

impl TabletMap {
    /// Version (bumped by every split, merge or move).
    pub fn version(&self) -> u64 {
        todo!()
    }

    /// The tablet holding `row` of `table` and its owner (binary search).
    pub fn route(&self, table: TableId, row: &[u8]) -> Option<(TabletId, ShardId)> {
        todo!()
    }
}

/// One immutable, consistent picture of the database: the tablet map, every tablet's active
/// and frozen memtables, and the SST set of one manifest version. Any change to any of these
/// publishes a new view. A frozen memtable stays in every new view until its flushed SST is
/// in the manifest.
#[derive(Debug)]
pub struct View {
    _priv: (),
}

impl View {
    /// View version.
    pub fn version(&self) -> u64 {
        todo!()
    }

    /// Manifest version of its SST set.
    pub fn manifest_version(&self) -> ManifestVersion {
        todo!()
    }

    /// The tablet map.
    pub fn tablets(&self) -> &TabletMap {
        todo!()
    }
}

/// A seqno plus the view current when it was taken. Reads through a snapshot ignore newer
/// commits and use only its view. Holding it pins the view's memtables and SST extents
/// (epoch-based; no locks). Cheap to clone.
#[derive(Debug, Clone)]
pub struct Snapshot {
    _view: Arc<View>,
}

impl Snapshot {
    /// The snapshot seqno.
    pub fn seqno(&self) -> Seqno {
        todo!()
    }

    /// The pinned view.
    pub fn view(&self) -> &Arc<View> {
        todo!()
    }
}
