//! Manifest blocks and edit records.
//!
//! The manifest is a chain of immutable blocks, each in its own extent: one *snapshot* block
//! (every edit needed to rebuild the state from empty) followed by zero or more *delta* blocks
//! (the edits of one manifest commit). Each block points to its predecessor; the superblock
//! points to the newest. Nothing is ever overwritten. When the chain grows past a bound the
//! manifest writer emits a fresh snapshot block and the old chain is retired. See `FORMAT.md`
//! §9 and decision D7.

use crate::compress::Compression;
use crate::superblock::ExtentRef;
use crate::{
    BlobFileId, FamilyId, FormatVersion, Lsn, ManifestVersion, Seqno, SstId, StreamId, TableId,
    TabletId, Timestamp,
};

/// Size of the fixed header in front of a manifest block's edits.
pub const MANIFEST_HEADER_LEN: usize = 64;

/// Whether a block restarts the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ManifestBlockKind {
    /// Full state; recovery stops walking back here.
    Snapshot = 1,
    /// Edits relative to the previous block.
    Delta = 2,
}

/// The fixed header of a manifest block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestHeader {
    /// Format version.
    pub version: FormatVersion,
    /// Snapshot or delta.
    pub kind: ManifestBlockKind,
    /// Version this block produces.
    pub manifest_version: ManifestVersion,
    /// The previous block; `None` for the first snapshot.
    pub prev: Option<ExtentRef>,
    /// Length of the previous block in bytes (0 if none).
    pub prev_len: u32,
    /// Number of edits.
    pub edit_count: u32,
    /// Length of the edit bytes following the header.
    pub body_len: u32,
}

/// Appends a complete manifest block (header, then edits, checksum over both) to `out`.
pub fn encode_block(header: &ManifestHeader, edits: &[Edit], out: &mut Vec<u8>) {
    todo!()
}

/// Decodes and verifies one manifest block, returning its header and edits.
pub fn decode_block(bytes: &[u8]) -> crate::Result<(ManifestHeader, Vec<Edit>)> {
    todo!()
}

/// Compaction strategy of a family. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum CompactionStyle {
    /// Leveled (Phase 1).
    #[default]
    Leveled = 0,
    /// Tiered/universal (Phase 2).
    Tiered = 1,
    /// FIFO by time: drop whole SSTs whose newest timestamp has expired (Phase 2).
    FifoByTime = 2,
}

/// Block-cache priority of a family's blocks. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
pub enum CachePriority {
    /// Evicted first.
    Low = 0,
    /// The default.
    #[default]
    Normal = 1,
    /// Evicted last.
    High = 2,
}

/// The persisted policy of one family. Stored in the manifest so another binary interprets
/// the data the same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyOptions {
    /// Block codec.
    pub compression: Compression,
    /// zstd level when `compression` is zstd.
    pub compression_level: i8,
    /// Target uncompressed data-block size.
    pub block_size: u32,
    /// Bloom bits per key; 0 disables both filters.
    pub bloom_bits: u8,
    /// Versions kept per column; 0 keeps all.
    pub max_versions: u32,
    /// Time to live in microseconds; 0 disables TTL.
    pub ttl_micros: u64,
    /// Values longer than this are separated into blob extents; `u32::MAX` disables it.
    pub blob_threshold: u32,
    /// Merge operator name; empty if none.
    pub merge_operator: String,
    /// Cache priority.
    pub cache_priority: CachePriority,
    /// Compaction strategy.
    pub compaction: CompactionStyle,
}

impl Default for FamilyOptions {
    fn default() -> Self {
        todo!()
    }
}

/// Everything the manifest records about one SST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstMeta {
    /// SST id.
    pub id: SstId,
    /// Extent holding it.
    pub extent: ExtentRef,
    /// Bytes used within the extent (the footer ends here).
    pub len: u64,
    /// Smallest internal key.
    pub smallest_key: Vec<u8>,
    /// Largest internal key.
    pub largest_key: Vec<u8>,
    /// Smallest and largest seqno.
    pub seqno_range: (Seqno, Seqno),
    /// Smallest and largest timestamp.
    pub ts_range: (Timestamp, Timestamp),
    /// Entries.
    pub entries: u64,
    /// Delete entries.
    pub deletes: u64,
}

/// One manifest edit. Tags (the `u8` before each edit) are frozen; see `FORMAT.md` §9.3.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Edit {
    /// Tag 1. A table was created.
    CreateTable {
        /// Id.
        table: TableId,
        /// Name (unique among live tables).
        name: String,
    },
    /// Tag 2. A table was dropped; its tablets, families and SSTs go with it.
    DropTable {
        /// Id.
        table: TableId,
    },
    /// Tag 3. A family was added to a table (or its options replaced).
    PutFamily {
        /// Table.
        table: TableId,
        /// Family id.
        family: FamilyId,
        /// Name (unique within the table).
        name: String,
        /// Persisted options.
        options: FamilyOptions,
    },
    /// Tag 4. A tablet was created covering `[start, end)` of the table's row space.
    PutTablet {
        /// Tablet id.
        tablet: TabletId,
        /// Table.
        table: TableId,
        /// Inclusive start row (unescaped); empty means unbounded.
        start: Vec<u8>,
        /// Exclusive end row (unescaped); `None` means unbounded.
        end: Option<Vec<u8>>,
    },
    /// Tag 5. A tablet was retired (after a split or merge).
    DropTablet {
        /// Tablet id.
        tablet: TabletId,
    },
    /// Tag 6. An SST was added to `(tablet, family)` at `level`.
    AddSst {
        /// Tablet.
        tablet: TabletId,
        /// Family.
        family: FamilyId,
        /// LSM level (0 = newest).
        level: u8,
        /// The SST.
        meta: SstMeta,
    },
    /// Tag 7. An SST was removed from `(tablet, family)`. Its extent is freed once no tablet
    /// references it and no live view uses it.
    RemoveSst {
        /// Tablet.
        tablet: TabletId,
        /// Family.
        family: FamilyId,
        /// The SST.
        sst: SstId,
    },
    /// Tag 8. Every write to `(tablet, family)` with seqno `<= seqno` is in SSTs.
    SetFlushed {
        /// Tablet.
        tablet: TabletId,
        /// Family.
        family: FamilyId,
        /// Flushed-through seqno.
        seqno: Seqno,
    },
    /// Tag 9. Replay of `stream` may start at `lsn`; earlier segments are recyclable.
    WalCheckpoint {
        /// Stream.
        stream: StreamId,
        /// First position that still matters.
        lsn: Lsn,
    },
    /// Tag 10. A blob file was created or grew.
    PutBlobFile {
        /// Blob file.
        blob_file: BlobFileId,
        /// Family whose values it holds.
        family: FamilyId,
        /// Its extents, in logical order.
        extents: Vec<ExtentRef>,
        /// Bytes of records written.
        total_bytes: u64,
        /// Bytes still referenced (maintained by compaction; drives blob GC).
        live_bytes: u64,
    },
    /// Tag 11. A blob file was deleted.
    DropBlobFile {
        /// Blob file.
        blob_file: BlobFileId,
    },
    /// Tag 12. Id allocation counters and the seqno floor, so ids are never reused and the
    /// seqno counter restarts above anything persisted.
    Counters {
        /// Next table id.
        next_table: u32,
        /// Next family id.
        next_family: u32,
        /// Next tablet id.
        next_tablet: u64,
        /// Next SST id.
        next_sst: u64,
        /// Next blob file id.
        next_blob_file: u32,
        /// Every assigned seqno is below this.
        seqno_ceiling: Seqno,
    },
}

impl Edit {
    /// Appends the tagged encoding of this edit.
    pub fn encode(&self, out: &mut Vec<u8>) {
        todo!()
    }

    /// Decodes one edit from the front of `input`; returns it and the bytes consumed.
    pub fn decode(input: &[u8]) -> crate::Result<(Self, usize)> {
        todo!()
    }
}
