//! Sorted string tables for Pigeonhole: blocks, filters, partitioned index, filtered
//! iteration.
//!
//! An SST is an immutable sorted run of one `(tablet, family)` inside one extent of the main
//! file (layout in `FORMAT.md` §4). [`SstWriter`] builds one from entries in key order;
//! [`SstReader`] opens one with its top-level index and filters pinned in memory, so a point
//! lookup is at most one cached index-partition lookup plus one data-block read.
//! [`SstIter`] is a zero-copy [`Cursor`] that applies the entry-level part of a
//! [`ScanFilter`] inside the block decoder and skips rows using the row-start table.
//!
//! Blob extents (separated values) are read and written here too ([`BlobWriter`],
//! [`BlobReader`]); deciding what to separate is compaction's job.
//!
//! The mock for the layer above is the real writer and reader over
//! [`SimVfs`](pigeonhole_io::sim::SimVfs).
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]
// Interface freeze: bodies are `todo!()`. Remove this allow when implementing.
#![allow(unused_variables, clippy::ptr_arg)]

use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_cache::{BlockCache, Cell, Priority};
use pigeonhole_format::compress::Compression;
use pigeonhole_format::manifest::{FamilyOptions, SstMeta};
use pigeonhole_format::sst::Properties;
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::value::BlobPointer;
use pigeonhole_format::{BlobFileId, Cursor, FamilyId, SstId, TableId, TabletId, Timestamp};
use pigeonhole_io::FileRef;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SST errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The file failed.
    Io(pigeonhole_io::Error),
    /// A block, footer or filter failed to decode or verify.
    Format(pigeonhole_format::Error),
    /// The SST would not fit in its extent; the caller should have cut earlier.
    ExtentFull,
    /// Keys were not added in strictly increasing order.
    OutOfOrder,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}

impl From<pigeonhole_io::Error> for Error {
    fn from(e: pigeonhole_io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<pigeonhole_format::Error> for Error {
    fn from(e: pigeonhole_format::Error) -> Self {
        Self::Format(e)
    }
}

/// How to build an SST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstWriterOptions {
    /// Target uncompressed data-block size.
    pub block_size: usize,
    /// Restart interval within data blocks.
    pub restart_interval: usize,
    /// Block codec.
    pub compression: Compression,
    /// Bloom bits per key; 0 writes no filters.
    pub bloom_bits: u8,
    /// Table (recorded in properties).
    pub table: TableId,
    /// Family (recorded in properties).
    pub family: FamilyId,
    /// Tablet (recorded in properties).
    pub tablet: TabletId,
    /// Merge operator name (recorded in properties).
    pub merge_operator: String,
}

impl SstWriterOptions {
    /// Options derived from a family's persisted policy.
    pub fn for_family(
        options: &FamilyOptions,
        table: TableId,
        family: FamilyId,
        tablet: TabletId,
    ) -> Self {
        todo!()
    }
}

/// Builds one SST into a pre-allocated extent. Entries must arrive in strictly increasing
/// internal-key order. Does not fsync: the pager's root commit syncs before publishing.
#[derive(Debug)]
pub struct SstWriter {
    _priv: (),
}

impl SstWriter {
    /// A writer for SST `id` into `extent` of `file`.
    pub fn new(file: FileRef, extent: ExtentRef, id: SstId, options: SstWriterOptions) -> Self {
        todo!()
    }

    /// Whether an entry of these sizes still fits, leaving room for index, filters and
    /// footer. Callers cut a new SST when it returns false.
    pub fn fits(&self, key_len: usize, value_len: usize) -> bool {
        todo!()
    }

    /// Adds an entry.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    /// Entries added.
    pub fn entries(&self) -> u64 {
        todo!()
    }

    /// Writes the remaining data block, index partitions, top index, filters, properties and
    /// footer, and returns the manifest record.
    pub fn finish(self) -> Result<SstMeta> {
        todo!()
    }

    /// Gives up; returns the extent for `Pager::abandon`.
    pub fn abandon(self) -> ExtentRef {
        todo!()
    }
}

/// Entry-level part of a scan filter, applied inside the block decoder.
///
/// Only conditions that are safe per entry are applied here: qualifier selection and the
/// time range on puts and merge operands. Delete entries and family markers always pass,
/// because hiding them could resurrect older versions. Per-column and per-row conditions
/// (version count, columns per row, value predicates) need snapshot visibility and are
/// applied by the resolver above (`pigeonhole_compaction::CellResolver`), still before any
/// cell is materialized.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScanFilter {
    /// Which qualifiers to keep.
    pub qualifiers: QualifierFilter,
    /// Keep puts and merges with `min <= ts < max`.
    pub time_range: Option<(Timestamp, Timestamp)>,
}

/// Qualifier selection.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum QualifierFilter {
    /// Every qualifier.
    #[default]
    All,
    /// Qualifiers starting with these bytes.
    Prefix(Vec<u8>),
    /// Qualifiers within the range.
    Range(Bound<Vec<u8>>, Bound<Vec<u8>>),
}

/// An open SST. Shared by every reader (`Send + Sync`); the engine keeps one per live SST.
#[derive(Debug)]
pub struct SstReader {
    _priv: (),
}

impl SstReader {
    /// Opens an SST: reads the footer, pins the top-level index and both filters, and reads
    /// the properties. Verifies checksums.
    pub fn open(
        file: FileRef,
        meta: &SstMeta,
        cache: Arc<BlockCache>,
        priority: Priority,
    ) -> Result<SstReader> {
        todo!()
    }

    /// The SST id.
    pub fn id(&self) -> SstId {
        todo!()
    }

    /// The properties block.
    pub fn properties(&self) -> &Properties {
        todo!()
    }

    /// Row filter check with a precomputed `pigeonhole_format::filter::row_hash`. `true` if
    /// the SST has no filter.
    pub fn may_contain_row(&self, row_hash: u64) -> bool {
        todo!()
    }

    /// Column filter check with a precomputed `pigeonhole_format::filter::column_hash`.
    pub fn may_contain_column(&self, column_hash: u64) -> bool {
        todo!()
    }

    /// A cursor applying `filter`; unpositioned until a seek.
    pub fn iter(&self, filter: ScanFilter, options: ReadOptions) -> SstIter<'_> {
        todo!()
    }
}

/// Per-read I/O policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOptions {
    /// Insert blocks read from disk into the cache (false for compaction inputs).
    pub fill_cache: bool,
    /// Cache priority for inserted blocks.
    pub priority: Priority,
    /// Data blocks to read ahead in one submission during forward scans (0 = none).
    pub readahead_blocks: u32,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            fill_cache: true,
            priority: Priority::Normal,
            readahead_blocks: 0,
        }
    }
}

/// A zero-copy cursor over one SST. Holds a pinned [`pigeonhole_cache::BlockHandle`] for the
/// current data block; keys and values borrow it.
#[derive(Debug)]
pub struct SstIter<'a> {
    _reader: &'a SstReader,
}

impl SstIter<'_> {
    /// The current value as a pinned [`Cell`] that outlives the cursor (no copy).
    pub fn value_cell(&self) -> Cell {
        todo!()
    }
}

impl Cursor for SstIter<'_> {
    type Error = Error;

    fn valid(&self) -> bool {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        todo!()
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        todo!()
    }

    fn next(&mut self) -> Result<()> {
        todo!()
    }

    /// Uses the block's row-start table; crosses into the next block only if the row
    /// continues there.
    fn skip_row(&mut self) -> Result<()> {
        todo!()
    }
}

/// Appends separated values to one logical blob file. The caller supplies extents (from the
/// pager) whenever [`BlobWriter::needs_extent`] says so.
#[derive(Debug)]
pub struct BlobWriter {
    _priv: (),
}

impl BlobWriter {
    /// A writer for `blob_file`, whose extents are all of `size_class`.
    pub fn new(file: FileRef, blob_file: BlobFileId, size_class: u8) -> Self {
        todo!()
    }

    /// Whether appending a value of `len` bytes needs more extents first.
    pub fn needs_extent(&self, len: usize) -> bool {
        todo!()
    }

    /// Adds the next extent (writes its header).
    pub fn add_extent(&mut self, extent: ExtentRef) -> Result<()> {
        todo!()
    }

    /// Appends a value and returns the pointer the LSM entry stores.
    pub fn append(&mut self, value: &[u8]) -> Result<BlobPointer> {
        todo!()
    }

    /// Finishes; returns the extents in logical order and total bytes written.
    pub fn finish(self) -> Result<(Vec<ExtentRef>, u64)> {
        todo!()
    }
}

/// Reads separated values of one logical blob file.
#[derive(Debug)]
pub struct BlobReader {
    _priv: (),
}

impl BlobReader {
    /// A reader over the blob file's extents.
    pub fn new(
        file: FileRef,
        blob_file: BlobFileId,
        extents: Vec<ExtentRef>,
        cache: Arc<BlockCache>,
    ) -> Self {
        todo!()
    }

    /// Reads and verifies the value `ptr` points to.
    pub fn read(&self, ptr: &BlobPointer) -> Result<Cell> {
        todo!()
    }
}
