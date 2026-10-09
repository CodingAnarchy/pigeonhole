//! Sorted string tables for Pigeonhole: blocks, filters, partitioned index, filtered
//! iteration.
//!
//! An SST is an immutable sorted run of one `(tablet, family)` inside one extent of the main
//! file (layout in `FORMAT.md` §4). [`SstWriter`] builds one from entries in key order;
//! [`SstReader`] opens one with its top-level index and filters pinned in memory, so a point
//! lookup is at most one cached index-partition lookup plus one data-block read.
//! [`SstIter`] is a zero-copy [`Cursor`](pigeonhole_format::Cursor) that applies a [`ScanFilter`] inside the block
//! decoder (the same `ScanFilter::admits` rule every source uses) and skips rows using the
//! row-start table. It owns an `Arc<SstReader>` and a pinned block, so it can be stored
//! anywhere (scan cursors, compaction jobs) without borrowing.
//!
//! Blob extents (separated values) are read and written here too ([`BlobWriter`],
//! [`BlobReader`]); deciding what to separate is compaction's job.
//!
//! The mock for the layer above is the real writer and reader over
//! [`SimVfs`](pigeonhole_io::sim::SimVfs).
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
//!
//! **Cache keys.** Blocks are cached under [`BlockKey`](pigeonhole_cache::BlockKey)s whose
//! `file` is [`sst_cache_file`] for SST blocks and [`blob_cache_file`] for blob records, so
//! the two id spaces never collide; pass the same values to `BlockCache::erase_files` when an
//! SST or blob file is deleted.
#![forbid(unsafe_code)]

mod blob;
mod iter;
mod reader;
mod writer;

use std::fmt;
use std::sync::Arc;

use pigeonhole_cache::{BlockCache, Cell, Priority};
use pigeonhole_format::compress::Compression;
use pigeonhole_format::manifest::{FamilyOptions, SstMeta};
use pigeonhole_format::sst::Properties;
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::value::BlobPointer;
use pigeonhole_format::{BlobFileId, FamilyId, SstId, TableId, TabletId};

pub use iter::SstIter;
pub use pigeonhole_format::scan::{QualifierFilter, ScanFilter};
use pigeonhole_io::FileRef;
pub use reader::Fetch;

/// Tag bit separating blob-file cache namespaces from SST ones.
const BLOB_NAMESPACE: u64 = 1 << 63;

/// The block-cache `file` namespace of an SST's blocks. SST ids must stay below `2^63` (the
/// top bit tags blob files); the engine's id counter never gets near it.
pub fn sst_cache_file(id: SstId) -> u64 {
    debug_assert!(
        id.0 < BLOB_NAMESPACE,
        "SST id {} collides with blob namespaces",
        id.0
    );
    id.0 & !BLOB_NAMESPACE
}

/// The block-cache `file` namespace of a blob file's records (blob ids are `u32`, so always
/// below `2^63`).
pub fn blob_cache_file(id: BlobFileId) -> u64 {
    const { assert!((u32::MAX as u64) < BLOB_NAMESPACE) };
    BLOB_NAMESPACE | u64::from(id.0)
}

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
    /// A cache-only read ([`ReadOptions::cache_only`], [`SstReader::open_cache_only`]) missed:
    /// the block it needs, to fetch asynchronously and admit before reading again (ICR 0014).
    WouldBlock(Box<Fetch>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "sst: {e}"),
            Self::Format(e) => write!(f, "sst: {e}"),
            Self::ExtentFull => write!(f, "sst: the extent is full"),
            Self::OutOfOrder => write!(f, "sst: keys added out of order"),
            Self::WouldBlock(fetch) => write!(f, "sst: a cache-only read missed ({fetch:?})"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Format(e) => Some(e),
            _ => None,
        }
    }
}

impl Error {
    /// Whether the error means stored bytes are bad (checksum, structure, truncation), as
    /// opposed to an I/O failure, a caller mistake or an unsupported feature.
    pub fn is_corruption(&self) -> bool {
        use pigeonhole_format::Error as F;
        matches!(
            self,
            Self::Format(
                F::Truncated { .. } | F::BadMagic { .. } | F::Checksum { .. } | F::Corrupt { .. }
            )
        )
    }

    /// Whether the bytes are intact but use a format version or codec this build does not
    /// support.
    pub fn is_unsupported(&self) -> bool {
        use pigeonhole_format::Error as F;
        matches!(
            self,
            Self::Format(F::UnsupportedVersion { .. } | F::UnsupportedCompression(_))
        )
    }
}

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
#[non_exhaustive]
pub struct SstWriterOptions {
    /// Target uncompressed data-block size.
    pub block_size: usize,
    /// Restart interval within data blocks.
    pub restart_interval: usize,
    /// Block codec.
    pub compression: Compression,
    /// zstd level (ignored by the other codecs).
    pub compression_level: i8,
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
    /// Creation time recorded in properties, microseconds since the Unix epoch (the engine
    /// passes `Vfs::now_micros`, so simulated runs stay deterministic). Default 0.
    pub created_micros: u64,
}

impl SstWriterOptions {
    /// Options derived from a family's persisted policy.
    pub fn for_family(
        options: &FamilyOptions,
        table: TableId,
        family: FamilyId,
        tablet: TabletId,
    ) -> Self {
        Self {
            block_size: options.block_size as usize,
            restart_interval: pigeonhole_format::block::DEFAULT_RESTART_INTERVAL,
            compression: options.compression,
            compression_level: options.compression_level,
            bloom_bits: options.bloom_bits,
            table,
            family,
            tablet,
            merge_operator: options.merge_operator.clone(),
            created_micros: 0,
        }
    }
}

/// Builds one SST into a pre-allocated extent. Entries must arrive in strictly increasing
/// internal-key order. Does not fsync: the pager's root commit syncs before publishing.
///
/// Data blocks are written to the extent as they fill (in 1 MiB batches); the index
/// partitions, top index, filters and properties follow at [`finish`](Self::finish), and the
/// footer is written last in its own write, so an interrupted build leaves no footer that
/// [`SstReader::open`] would accept in front of missing blocks.
///
/// ```
/// use pigeonhole_format::key::{Kind, encode_key, encode_marker_key};
/// use pigeonhole_format::manifest::FamilyOptions;
/// use pigeonhole_format::superblock::ExtentRef;
/// use pigeonhole_format::{FamilyId, SstId, TableId, TabletId};
/// use pigeonhole_io::{OpenOptions, Vfs};
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_sst::{SstWriter, SstWriterOptions};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let file = SimVfs::new(7).open("/db".as_ref(), OpenOptions::read_write_create())?;
/// let options = SstWriterOptions::for_family(&FamilyOptions::default(), TableId(1), FamilyId(2), TabletId(3));
/// let mut w = SstWriter::new(file, ExtentRef { page: 16, size_class: 0 }, SstId(9), options);
///
/// let mut key = Vec::new();
/// encode_marker_key(&mut key, b"row", 50, 4)?; // a family delete sorts before the row's cells
/// assert!(w.fits(key.len(), 0));
/// w.add(&key, b"")?;
/// key.clear();
/// encode_key(&mut key, b"row", b"q", 40, 3, Kind::Put)?;
/// w.add(&key, b"\x00value")?;
///
/// let meta = w.finish()?;
/// assert_eq!((meta.entries, meta.deletes), (2, 1));
/// assert_eq!(meta.seqno_range, (3, 4));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SstWriter {
    inner: writer::Writer,
}

impl SstWriter {
    /// A writer for SST `id` into `extent` of `file`.
    pub fn new(file: FileRef, extent: ExtentRef, id: SstId, options: SstWriterOptions) -> Self {
        Self {
            inner: writer::Writer::new(file, extent, id, options),
        }
    }

    /// Whether an entry of these sizes still fits, leaving room for index, filters and
    /// footer. Callers cut a new SST when it returns false. Conservative: if every `add` was
    /// preceded by a `true` answer, [`finish`](Self::finish) never fails with
    /// [`Error::ExtentFull`].
    pub fn fits(&self, key_len: usize, value_len: usize) -> bool {
        self.inner.fits(key_len, value_len)
    }

    /// Adds an entry: a valid internal key (else [`Error::Format`]) above the previous one
    /// (else [`Error::OutOfOrder`]) and its stored value. After an error, abandon the writer.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.add(key, value)
    }

    /// Data bytes written so far: sealed blocks and the open one, without the index, filters
    /// and footer still to come. A writer that cuts its outputs by size reads it.
    pub fn data_len(&self) -> u64 {
        self.inner.data_len()
    }

    /// Entries added.
    pub fn entries(&self) -> u64 {
        self.inner.entries()
    }

    /// Writes the remaining data block, index partitions, top index, filters, properties and
    /// footer, and returns the manifest record.
    pub fn finish(self) -> Result<SstMeta> {
        self.inner.finish()
    }

    /// Gives up; returns the extent for `Pager::abandon`.
    pub fn abandon(self) -> ExtentRef {
        self.inner.extent()
    }
}

/// An open SST. Shared by every reader (`Send + Sync`); the engine keeps one per live SST.
///
/// Opening reads the footer, then the top-level index and both filters through the block
/// cache and keeps them pinned, so a point lookup costs at most one cached index-partition
/// lookup and one data-block read, and a filter miss costs no I/O.
///
/// ```
/// use std::sync::Arc;
/// use pigeonhole_cache::{BlockCache, Priority};
/// use pigeonhole_format::filter::{column_hash, row_hash};
/// use pigeonhole_format::key::{Kind, encode_column_prefix, encode_key, encode_marker_prefix};
/// use pigeonhole_format::manifest::FamilyOptions;
/// use pigeonhole_format::superblock::ExtentRef;
/// use pigeonhole_format::{Cursor, FamilyId, SstId, TableId, TabletId};
/// use pigeonhole_io::{OpenOptions, Vfs};
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_sst::{ReadOptions, ScanFilter, SstReader, SstWriter, SstWriterOptions};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let file = SimVfs::new(3).open("/db".as_ref(), OpenOptions::read_write_create())?;
/// let options = SstWriterOptions::for_family(&FamilyOptions::default(), TableId(1), FamilyId(1), TabletId(1));
/// let mut w = SstWriter::new(file.clone(), ExtentRef { page: 16, size_class: 0 }, SstId(1), options);
/// let mut key = Vec::new();
/// encode_key(&mut key, b"alice", b"email", 10, 1, Kind::Put)?;
/// w.add(&key, b"\x00a@example.com")?;
/// let meta = w.finish()?;
///
/// let cache = Arc::new(BlockCache::new(1 << 20, 1));
/// let sst = Arc::new(SstReader::open(file, &meta, cache, Priority::Normal)?);
///
/// // A point get probes the column and the row's marker key (FORMAT §6).
/// let mut probe = Vec::new();
/// encode_column_prefix(&mut probe, b"alice", b"email")?;
/// assert!(sst.may_contain_column(column_hash(&probe)));
/// assert!(sst.may_contain_row(row_hash(b"alice")));
///
/// let mut it = sst.iter(ScanFilter::all(), ReadOptions::default());
/// it.seek(&probe)?;
/// assert_eq!(it.key(), &key[..]);
/// assert_eq!(&it.value_cell()[..], b"\x00a@example.com");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SstReader {
    inner: reader::Reader,
}

impl SstReader {
    /// Opens an SST: reads the footer, pins the top-level index and both filters, and reads
    /// the properties. Verifies their checksums, but not those of data blocks and index
    /// partitions, which are verified when first read (a damaged one is a corruption error,
    /// never wrong data). Reading the whole SST at open is unnecessary: nothing references an
    /// SST before its manifest commit, whose root commit syncs it first.
    pub fn open(
        file: FileRef,
        meta: &SstMeta,
        cache: Arc<BlockCache>,
        priority: Priority,
    ) -> Result<SstReader> {
        Ok(Self {
            inner: reader::Reader::open(file, meta, cache, priority, false)?,
        })
    }

    /// As [`SstReader::open`], reading only from the block cache: whatever the open needs that
    /// is not cached (the footer, the top-level index, the filters, the properties) fails it
    /// with [`Error::WouldBlock`], to fetch and admit before opening again (ICR 0014).
    pub fn open_cache_only(
        file: FileRef,
        meta: &SstMeta,
        cache: Arc<BlockCache>,
        priority: Priority,
    ) -> Result<Self> {
        Ok(Self {
            inner: reader::Reader::open(file, meta, cache, priority, true)?,
        })
    }

    /// The SST id.
    pub fn id(&self) -> SstId {
        self.inner.blocks.id
    }

    /// The SST's length in bytes (`SstMeta::len`).
    pub fn len_bytes(&self) -> u64 {
        self.inner.len_bytes()
    }

    /// The properties block.
    pub fn properties(&self) -> &Properties {
        &self.inner.properties
    }

    /// Row filter check with a precomputed `pigeonhole_format::filter::row_hash`. `true` if
    /// the SST has no filter.
    pub fn may_contain_row(&self, row_hash: u64) -> bool {
        self.inner.may_contain_row(row_hash)
    }

    /// Column filter check with a precomputed `pigeonhole_format::filter::column_hash`. For a
    /// row's family markers the column key is the marker prefix
    /// (`pigeonhole_format::key::encode_marker_prefix`), which every SST holding a marker
    /// adds; a point get probes both. `true` if the SST has no filter.
    pub fn may_contain_column(&self, column_hash: u64) -> bool {
        self.inner.may_contain_column(column_hash)
    }

    /// A cursor applying `filter`; unpositioned until a seek. Owns a clone of the `Arc`.
    /// Allocation-free.
    pub fn iter(self: &Arc<Self>, filter: ScanFilter, options: ReadOptions) -> SstIter {
        SstIter::new(Arc::clone(self), filter, options)
    }
}

/// Per-read I/O policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReadOptions {
    /// Insert blocks read from disk into the cache (false for compaction inputs).
    pub fill_cache: bool,
    /// Cache priority for inserted blocks.
    pub priority: Priority,
    /// Data blocks to read ahead in one submission during forward scans (0 = none).
    pub readahead_blocks: u32,
    /// Read only what is cached: a block that is not fails the read with
    /// [`Error::WouldBlock`] instead of being read from the file (async reads, ICR 0014).
    pub cache_only: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            fill_cache: true,
            priority: Priority::Normal,
            readahead_blocks: 0,
            cache_only: false,
        }
    }
}

/// Appends separated values to one logical blob file. The caller supplies extents (from the
/// pager) whenever [`BlobWriter::needs_extent`] says so. Like [`SstWriter`], it does not
/// fsync.
///
/// ```
/// use std::sync::Arc;
/// use pigeonhole_cache::BlockCache;
/// use pigeonhole_format::BlobFileId;
/// use pigeonhole_format::superblock::ExtentRef;
/// use pigeonhole_io::{OpenOptions, Vfs};
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_sst::{BlobReader, BlobWriter};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let file = SimVfs::new(5).open("/db".as_ref(), OpenOptions::read_write_create())?;
/// let mut w = BlobWriter::new(file.clone(), BlobFileId(4), 0);
/// let big = vec![7u8; 100_000]; // larger than one 64 KiB extent: the record spans two
/// let mut next_page = 16;
/// while w.needs_extent(big.len()) {
///     w.add_extent(ExtentRef { page: next_page, size_class: 0 })?;
///     next_page += 16;
/// }
/// let ptr = w.append(&big)?;
/// let (extents, bytes) = w.finish()?;
/// assert_eq!((extents.len(), bytes), (2, 100_016));
///
/// let r = BlobReader::new(file, BlobFileId(4), extents, Arc::new(BlockCache::new(1 << 20, 1)));
/// assert_eq!(&r.read(&ptr)?[..], &big[..]);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct BlobWriter {
    inner: blob::Writer,
}

impl BlobWriter {
    /// A writer for `blob_file`, whose extents are all of `size_class`.
    pub fn new(file: FileRef, blob_file: BlobFileId, size_class: u8) -> Self {
        Self {
            inner: blob::Writer::new(file, blob_file, size_class),
        }
    }

    /// Whether appending a value of `len` bytes needs more extents first.
    pub fn needs_extent(&self, len: usize) -> bool {
        self.inner.needs_extent(len)
    }

    /// Adds the next extent (writes its header). It must be of the writer's size class.
    pub fn add_extent(&mut self, extent: ExtentRef) -> Result<()> {
        self.inner.add_extent(extent)
    }

    /// Appends a value and returns the pointer the LSM entry stores. Fails with
    /// [`Error::ExtentFull`] if [`needs_extent`](Self::needs_extent) said more extents were
    /// needed and none were added.
    pub fn append(&mut self, value: &[u8]) -> Result<BlobPointer> {
        self.inner.append(value)
    }

    /// Finishes; returns the extents in logical order and total bytes written (the logical
    /// length: every record's 16-byte header and value).
    pub fn finish(self) -> Result<(Vec<ExtentRef>, u64)> {
        Ok(self.inner.finish())
    }
}

/// Reads separated values of one logical blob file. Records are cached (at low priority)
/// under [`blob_cache_file`]; see [`BlobWriter`] for an example.
#[derive(Debug)]
pub struct BlobReader {
    inner: blob::Reader,
}

impl BlobReader {
    /// A reader over the blob file's extents.
    pub fn new(
        file: FileRef,
        blob_file: BlobFileId,
        extents: Vec<ExtentRef>,
        cache: Arc<BlockCache>,
    ) -> Self {
        Self {
            inner: blob::Reader::new(file, blob_file, extents, cache),
        }
    }

    /// Reads and verifies the value `ptr` points to (verification is skipped on a cache
    /// hit). A pointer past the blob file or a checksum mismatch is a [`Error::Format`]
    /// corruption error.
    pub fn read(&self, ptr: &BlobPointer) -> Result<Cell> {
        self.inner.read(ptr)
    }

    /// The value `ptr` names if its record is cached: a lookup only, which never reads the
    /// file (async reads count the reads they make synchronously, ICR 0014).
    pub fn cached(&self, ptr: &BlobPointer) -> Option<Cell> {
        self.inner.cached(ptr)
    }
}
