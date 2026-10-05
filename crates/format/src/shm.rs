//! Shared-memory region layout. See `FORMAT.md` §11.
//!
//! ```text
//! [header 4 KiB][watermarks: 64 B x shards][view area: 2 buffers][reader slots: 64 B x n][arenas]
//! ```
//!
//! Fields that change while the region is live are accessed as atomics at the offsets given
//! here; `pigeonhole-shm` does the atomic access, this module only fixes the numbers.

use crate::{FamilyId, ManifestVersion, ShmLayoutVersion, TableId, TabletId};

/// Magic of the directory region.
pub const DIRECTORY_MAGIC: [u8; 8] = *b"PHDBSHMD";

/// Size of the region header.
pub const HEADER_LEN: usize = 4096;
/// Size of one shard's watermark line.
pub const WATERMARK_STRIDE: usize = 64;
/// Size of one reader slot.
pub const READER_SLOT_LEN: usize = 64;
/// Alignment of each shard arena (so NUMA binding works on whole pages).
pub const ARENA_ALIGN: usize = 2 * 1024 * 1024;

/// Byte offsets of the header fields.
pub mod header {
    /// `[u8; 8]` [`SHM_MAGIC`](crate::version::SHM_MAGIC).
    pub const MAGIC: usize = 0;
    /// `u32` layout version.
    pub const LAYOUT_VERSION: usize = 8;
    /// `u32` header length (4096).
    pub const HEADER_LEN: usize = 12;
    /// `u64` total region length.
    pub const REGION_LEN: usize = 16;
    /// `[u8; 16]` database id from the superblock.
    pub const DB_ID: usize = 24;
    /// `u64` this region's generation (also in its name).
    pub const GENERATION: usize = 40;
    /// `u32` atomic: 0 initializing, 1 ready, 2 abandoned (a newer generation replaced it;
    /// attached processes must re-attach through the directory).
    pub const STATE: usize = 48;
    /// `u32` shard count.
    pub const SHARD_COUNT: usize = 52;
    /// `u32` reader slot count.
    pub const READER_SLOT_COUNT: usize = 56;
    /// `u32` length of one view buffer.
    pub const VIEW_BUFFER_LEN: usize = 60;
    /// `u64` atomic: current manifest version.
    pub const MANIFEST_VERSION: usize = 64;
    /// `u64` atomic: `(view_version << 1) | buffer_index` of the current view. View versions
    /// start at 1; 0 means none published yet.
    pub const VIEW_POINTER: usize = 72;
    /// `u32` writer process id.
    pub const WRITER_PID: usize = 80;
    /// `u64` writer process start time (opaque, platform-defined).
    pub const WRITER_START_TIME: usize = 88;
    /// `u64` offset of the watermark table.
    pub const WATERMARKS_OFF: usize = 96;
    /// `u64` offset of the view area.
    pub const VIEWS_OFF: usize = 104;
    /// `u64` offset of the reader-slot table.
    pub const READER_SLOTS_OFF: usize = 112;
    /// `u64` offset of shard 0's arena.
    pub const ARENAS_OFF: usize = 120;
    /// `u64` length of each shard arena.
    pub const ARENA_LEN: usize = 128;
    /// `u64` atomic: next global seqno to reserve.
    pub const NEXT_SEQNO: usize = 136;
    /// `u64` device number of the main file.
    pub const FILE_DEVICE: usize = 144;
    /// `u64` inode (file index on Windows) of the main file.
    pub const FILE_INODE: usize = 152;
}

/// Byte offsets within a watermark line.
pub mod watermark {
    /// `u64` atomic: the lowest seqno this shard holds unapplied: the minimum of its current
    /// group's lower bound and every cross-shard commit seqno it coordinates and has not yet
    /// released; `u64::MAX` when idle.
    pub const PENDING: usize = 0;
}

/// Byte offsets within a reader slot.
pub mod reader_slot {
    /// `u32` atomic: 0 free, 1 claiming, 2 active.
    pub const STATE: usize = 0;
    /// `u32` process id.
    pub const PID: usize = 4;
    /// `u64` process start time.
    pub const START_TIME: usize = 8;
    /// `u64` atomic: pinned snapshot seqno, 0 if none.
    pub const PINNED_SEQNO: usize = 16;
    /// `u64` atomic: pinned view version, 0 if none.
    pub const PINNED_VIEW: usize = 24;
    /// `u64` generation the slot was claimed under.
    pub const GENERATION: usize = 32;
}

/// The immutable part of the header, written once when the region is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShmHeader {
    /// Layout version.
    pub layout_version: ShmLayoutVersion,
    /// Total region length.
    pub region_len: u64,
    /// Database id.
    pub db_id: [u8; 16],
    /// Shards (one arena and one watermark line each).
    pub shard_count: u32,
    /// Reader slots.
    pub reader_slot_count: u32,
    /// Length of each of the two view buffers.
    pub view_buffer_len: u32,
    /// Offset of the watermark table.
    pub watermarks_off: u64,
    /// Offset of the view area.
    pub views_off: u64,
    /// Offset of the reader-slot table.
    pub reader_slots_off: u64,
    /// Offset of shard 0's arena.
    pub arenas_off: u64,
    /// Length of each arena.
    pub arena_len: u64,
    /// Device of the main file.
    pub file_device: u64,
    /// Inode of the main file.
    pub file_inode: u64,
}

impl ShmHeader {
    /// Computes a layout for the given sizes, aligning each area.
    pub fn layout(
        db_id: [u8; 16],
        shard_count: u32,
        reader_slot_count: u32,
        view_buffer_len: u32,
        arena_len: u64,
        file_device: u64,
        file_inode: u64,
    ) -> Self {
        todo!()
    }

    /// Encodes the immutable fields (atomic fields are left zero).
    pub fn encode(&self, out: &mut [u8; HEADER_LEN]) {
        todo!()
    }

    /// Decodes and validates magic, layout version and offsets against `region_len`.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        todo!()
    }
}

/// Name of the directory region: `phdb-` followed by 16 lowercase hex digits of
/// `xxh3_64(device LE ++ inode LE)` of the main file, identical on every process however it
/// spells the path. The directory is one page that records the current generation; its layout
/// never changes, so it is never rebuilt (decision D27).
pub fn directory_name(device: u64, inode: u64) -> String {
    todo!()
}

/// Name of the region for `generation`: the directory name, `-`, and the generation in
/// lowercase hex (at most 31 bytes, macOS's `shm_open` limit). A rebuilt region always has a
/// new name, so an old mapping that some process still holds (Windows keeps named mappings
/// alive while any handle is open) is never mistaken for the new one.
pub fn region_name(device: u64, inode: u64, generation: u64) -> String {
    todo!()
}

/// Byte offsets within the directory region (4096 bytes, layout fixed forever).
pub mod directory {
    /// `[u8; 8]` magic `PHDBSHMD`.
    pub const MAGIC: usize = 0;
    /// `u32` directory format (always 1).
    pub const FORMAT: usize = 8;
    /// `u64` atomic: generation of the current region (0 = none yet).
    pub const GENERATION: usize = 16;
    /// `u32` atomic: layout version of the current region.
    pub const LAYOUT_VERSION: usize = 24;
    /// Size of the directory region.
    pub const LEN: usize = 4096;
}

/// One tablet in a published view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewTablet {
    /// Tablet.
    pub tablet: TabletId,
    /// Table.
    pub table: TableId,
    /// Owning shard.
    pub shard: u16,
    /// Inclusive start row (unescaped); empty means unbounded.
    pub start: Vec<u8>,
    /// Exclusive end row; `None` means unbounded.
    pub end: Option<Vec<u8>>,
}

/// One memtable in a published view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewMemtable {
    /// Tablet.
    pub tablet: TabletId,
    /// Family.
    pub family: FamilyId,
    /// Shard whose arena holds it.
    pub shard: u16,
    /// 0 for the active memtable, then 1, 2, ... for frozen ones, newest first.
    pub age: u8,
    /// Offset of the memtable header within the shard arena.
    pub root: u32,
}

/// A view as published in shared memory: what a reader process needs to read the writer's
/// memtables and know which manifest version goes with them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ViewRecord {
    /// View version (strictly increasing, starting at 1).
    pub view_version: u64,
    /// Manifest version this view's SST set comes from.
    pub manifest_version: ManifestVersion,
    /// The tablet map.
    pub tablets: Vec<ViewTablet>,
    /// Every memtable a reader must consult.
    pub memtables: Vec<ViewMemtable>,
}

impl ViewRecord {
    /// Encoded length, so the writer can check it against the view buffer before publishing.
    pub fn encoded_len(&self) -> usize {
        todo!()
    }

    /// Encodes with a CRC32C so a reader can detect a torn copy.
    pub fn encode(&self, out: &mut Vec<u8>) {
        todo!()
    }

    /// Decodes and verifies a copied view buffer.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        todo!()
    }
}

/// Memtable layout inside a shard arena. Offsets are `u32`, relative to the arena base; `0` is
/// null (the first 64 bytes of every arena are reserved).
pub mod memtable {
    /// Maximum skiplist height.
    pub const MAX_HEIGHT: usize = 16;
    /// Size of the memtable header.
    pub const HEADER_LEN: usize = 64;
    /// Header: `u32` [`MEMTABLE_MAGIC`](crate::version::MEMTABLE_MAGIC).
    pub const H_MAGIC: usize = 0;
    /// Header: `u16` layout version (1).
    pub const H_VERSION: usize = 4;
    /// Header: `u8` flags (bit 0: frozen).
    pub const H_FLAGS: usize = 6;
    /// Header: `u32` offset of the head node (height [`MAX_HEIGHT`]).
    pub const H_HEAD: usize = 8;
    /// Header: `u32` atomic entry count.
    pub const H_COUNT: usize = 12;
    /// Header: `u64` atomic bytes allocated.
    pub const H_BYTES: usize = 16;
    /// Header: `u64` atomic largest seqno inserted.
    pub const H_MAX_SEQNO: usize = 24;
    /// Header: `u64` smallest seqno inserted.
    pub const H_MIN_SEQNO: usize = 32;
    /// Node: `u32` internal key length.
    pub const N_KEY_LEN: usize = 0;
    /// Node: `u32` stored value length.
    pub const N_VALUE_LEN: usize = 4;
    /// Node: `u8` height.
    pub const N_HEIGHT: usize = 8;
    /// Node: tower of `height` atomic `u32` next offsets, starting here; key bytes then value
    /// bytes follow the tower. Nodes are 4-byte aligned.
    pub const N_TOWER: usize = 12;
}
