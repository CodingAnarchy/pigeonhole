//! Manifest blocks and edit records.
//!
//! The manifest is one *snapshot* block (every edit needed to rebuild the state from empty)
//! in its own extent, plus a *delta log*: one extent (256 KiB) holding consecutive *delta*
//! blocks, one per manifest commit. The superblock names both and how many log bytes are
//! live. A commit appends a delta past the live end of the log (never touching live bytes) and
//! flips the superblock. When the log is full or outgrows the snapshot, the writer emits a new
//! snapshot and starts a new log. Open reads the superblocks, the snapshot and the log: three
//! reads. See `FORMAT.md` §9 and decision D7.
//!
//! ```
//! use pigeonhole_format::manifest::{Edit, ManifestBlockKind, ManifestHeader, decode_block, encode_block};
//! use pigeonhole_format::{FormatVersion, TableId};
//!
//! let header = ManifestHeader {
//!     version: FormatVersion::CURRENT,
//!     kind: ManifestBlockKind::Delta,
//!     manifest_version: 2,
//!     edit_count: 0, // filled in by encode_block
//!     body_len: 0,   // filled in by encode_block
//! };
//! let edits = [Edit::CreateTable { table: TableId(1), name: "pages".into() }];
//! let mut log = Vec::new();
//! encode_block(&header, &edits, &mut log);
//! let (decoded, back, len) = decode_block(&log).unwrap();
//! assert_eq!((decoded.manifest_version, back.as_slice(), len), (2, &edits[..], log.len()));
//! ```

use crate::Error;
use crate::bytes::{Reader, le_u32, le_u64};
use crate::compress::Compression;
use crate::superblock::ExtentRef;
use crate::version::MANIFEST_MAGIC;
use crate::{
    BlobFileId, FamilyId, FormatVersion, Lsn, ManifestVersion, Seqno, SstId, StreamId, TableId,
    TabletId, Timestamp,
};

/// Size of the fixed header in front of a manifest block's edits.
pub const MANIFEST_HEADER_LEN: usize = 64;

/// Size of a delta-log extent (size class 2).
pub const LOG_EXTENT_LEN: usize = 256 * 1024;

/// Whether a block is a full snapshot or one commit's delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ManifestBlockKind {
    /// Full state.
    Snapshot = 1,
    /// One commit's edits, relative to the block before it.
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
    /// Number of edits.
    pub edit_count: u32,
    /// Length of the edit bytes following the header.
    pub body_len: u32,
}

/// Appends a complete manifest block (header, then edits, checksum over both) to `out`.
///
/// `header.edit_count` and `header.body_len` are ignored and computed from `edits`.
pub fn encode_block(header: &ManifestHeader, edits: &[Edit], out: &mut Vec<u8>) {
    let start = out.len();
    out.resize(start + MANIFEST_HEADER_LEN, 0);
    for e in edits {
        e.encode(out);
    }
    let body_len = (out.len() - start - MANIFEST_HEADER_LEN) as u32;
    let h = &mut out[start..start + MANIFEST_HEADER_LEN];
    h[0..8].copy_from_slice(&MANIFEST_MAGIC);
    h[8..12].copy_from_slice(&header.version.0.to_le_bytes());
    h[12] = header.kind as u8;
    h[16..24].copy_from_slice(&header.manifest_version.to_le_bytes());
    h[40..44].copy_from_slice(&(edits.len() as u32).to_le_bytes());
    h[44..48].copy_from_slice(&body_len.to_le_bytes());
    let checksum = crate::checksum::xxh3_64_parts(&[
        &out[start..start + 48],
        &out[start + MANIFEST_HEADER_LEN..],
    ]);
    out[start + 48..start + 56].copy_from_slice(&checksum.to_le_bytes());
}

/// Decodes and verifies one manifest block from the front of `bytes`, returning its header,
/// edits and length. A delta log is decoded by calling this until the live length is used up;
/// deltas must carry consecutive versions following the snapshot's.
///
/// Edits with a tag this build does not know are skipped (their length prefix makes that
/// possible) and are not returned when the tag is 128 or more; below 128 the edit must be
/// understood and the block fails with [`Error::UnsupportedFeature`] (D269).
pub fn decode_block(bytes: &[u8]) -> crate::Result<(ManifestHeader, Vec<Edit>, usize)> {
    const WHAT: &str = "manifest block";
    let Some(h) = bytes.get(..MANIFEST_HEADER_LEN) else {
        return Err(Error::Truncated { what: WHAT });
    };
    if h[..8] != MANIFEST_MAGIC {
        return Err(Error::BadMagic { what: WHAT });
    }
    let edit_count = le_u32(h, 40);
    let body_len = le_u32(h, 44);
    let end = MANIFEST_HEADER_LEN.saturating_add(body_len as usize);
    let Some(body) = bytes.get(MANIFEST_HEADER_LEN..end) else {
        return Err(Error::Truncated { what: WHAT });
    };
    if crate::checksum::xxh3_64_parts(&[&h[..48], body]) != le_u64(h, 48) {
        return Err(Error::Checksum { what: WHAT });
    }
    let version = FormatVersion(le_u32(h, 8));
    version.check(WHAT)?;
    let kind = match h[12] {
        1 => ManifestBlockKind::Snapshot,
        2 => ManifestBlockKind::Delta,
        k => {
            return Err(Error::UnsupportedFeature {
                what: "manifest block kind",
                value: u64::from(k),
            });
        }
    };
    let header = ManifestHeader {
        version,
        kind,
        manifest_version: le_u64(h, 16),
        edit_count,
        body_len,
    };
    // Every edit takes at least two bytes.
    if edit_count as usize > body.len() / MIN_EDIT_BYTES {
        return Err(Error::Corrupt {
            what: "manifest edit count",
        });
    }
    // Reserve modestly: a corrupt-but-checksummed count must not reserve gigabytes.
    let mut edits = Vec::with_capacity((edit_count as usize).min(4096));
    let mut pos = 0;
    for _ in 0..edit_count {
        let rest = &body[pos..];
        match rest.first() {
            // No build writes tag 0: damage, not a newer build's edit.
            Some(0) => {
                return Err(Error::Corrupt {
                    what: "manifest edit tag 0",
                });
            }
            // An unknown tag below 128 is a must-understand edit from a newer build: skipping
            // it could change what the manifest means (D269; FORMAT §9.3).
            Some(&tag) if !KNOWN_TAGS.contains(&tag) && tag < SKIPPABLE_TAGS => {
                return Err(Error::UnsupportedFeature {
                    what: "manifest edit tag",
                    value: u64::from(tag),
                });
            }
            // From 128 an unknown edit is safe to skip; its length prefix allows it.
            Some(tag) if !KNOWN_TAGS.contains(tag) => {
                pos += skip_edit(rest)?;
                continue;
            }
            _ => {}
        }
        let (e, n) = Edit::decode(rest)?;
        edits.push(e);
        pos += n;
    }
    if pos != body.len() {
        return Err(Error::Corrupt {
            what: "manifest block body length",
        });
    }
    Ok((header, edits, end))
}

/// Smallest encoded edit: a tag and an empty body's length byte.
const MIN_EDIT_BYTES: usize = 2;

/// Tags this build decodes (FORMAT §9.3).
const KNOWN_TAGS: std::ops::RangeInclusive<u8> = 1..=13;

/// The first edit tag an older build may skip (D269): tags 1..=127 must be understood,
/// 128..=255 may be ignored. Tag 13 (`SstBlobRefs`), added before the ranges, is
/// understood by every build since and skipped by older ones.
pub const SKIPPABLE_TAGS: u8 = 128;

/// Length of the edit at the front of `input`, whatever its tag.
fn skip_edit(input: &[u8]) -> crate::Result<usize> {
    let mut r = Reader::new(input, "manifest edit");
    r.u8()?;
    r.bytes()?;
    Ok(r.pos())
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

/// What a family's columns hold, which decides how merge operands and deletes resolve
/// (decisions D179, D294). Numbers are frozen.
///
/// Every kind but `Standard` is an *aggregate* family (Bigtable's aggregate families):
/// operands combine with the kind's built-in operator only within one `(column, timestamp)`
/// bucket, each bucket is one version, and a delete hides only entries written before it
/// (lower seqno) within its timestamp scope. A kind added after 1.0 needs an `incompat`
/// feature bit ([`FamilyKind::required_incompat`]), set before the first family of the kind
/// is written, so a build that does not know the kind refuses the file at open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum FamilyKind {
    /// Plain cells. Merge operands (with a named operator) fold across timestamps (D41) and
    /// deletes hide by timestamp. Families written before the kind was stored read as this.
    #[default]
    Standard = 0,
    /// An `i64` sum counter family (D179): operands add, wrapping.
    Counter = 1,
    /// An `i64` minimum family (D294): a bucket holds the smallest value written to it.
    /// Needs [`INCOMPAT_AGGREGATES`].
    Min = 2,
    /// An `i64` maximum family (D294): a bucket holds the largest value written to it.
    /// Needs [`INCOMPAT_AGGREGATES`].
    Max = 3,
    /// A HyperLogLog family (D294): a bucket holds a sketch of the distinct `i64` elements
    /// written to it ([`crate::hll`]). Needs [`INCOMPAT_AGGREGATES`].
    Hll = 4,
    /// A bitwise-OR family over `u64` (D298): a bucket holds the OR of the values written to
    /// it (`ValueTag::I64`, the `u64`'s bits). Needs [`INCOMPAT_AGGREGATES`].
    BitOr = 5,
    /// A bitwise-AND family over `u64` (D298). Needs [`INCOMPAT_AGGREGATES`].
    BitAnd = 6,
    /// A bitwise-XOR family over `u64` (D298). Needs [`INCOMPAT_AGGREGATES`].
    BitXor = 7,
    /// An `f64` minimum family (D298): a bucket holds the smallest value written to it by
    /// `f64::total_cmp` (`-0.0 < +0.0`; NaN is refused at write). Needs
    /// [`INCOMPAT_AGGREGATES`].
    MinF64 = 8,
    /// An `f64` maximum family (D298), as [`FamilyKind::MinF64`]. Needs
    /// [`INCOMPAT_AGGREGATES`].
    MaxF64 = 9,
    /// An exact `f64` sum family (D298): inputs are finite `f64`s, and a bucket's value is
    /// the exact sum's state ([`crate::exact`], FORMAT §3.2). Needs [`INCOMPAT_AGGREGATES`].
    SumF64 = 10,
    /// A mean-and-variance family (D298): inputs are finite `f64`s, and a bucket's value
    /// holds their count, exact sum and exact sum of squares ([`crate::exact`], FORMAT
    /// §3.3). Needs [`INCOMPAT_AGGREGATES`].
    MeanVar = 11,
    /// A fixed-bucket histogram family (D298): inputs are `f64`s (NaN refused), counted in
    /// the buckets its `kind_params` bounds give ([`crate::buckets`], FORMAT §3.4). Needs
    /// [`INCOMPAT_AGGREGATES`].
    Histogram = 12,
    /// A log-linear quantile sketch family (D298): inputs are finite `f64`s, and its
    /// `kind_params` set the mantissa bits and bucket limit ([`crate::buckets`], FORMAT
    /// §3.5). Needs [`INCOMPAT_AGGREGATES`].
    Quantiles = 13,
    /// A bloom filter family (D298): inputs are `i64` or bytes elements, set in a filter of
    /// the bits and hashes its `kind_params` give ([`crate::bloom`], FORMAT §3.6). Needs
    /// [`INCOMPAT_AGGREGATES`].
    Bloom = 14,
    /// A vector family (D303, #490 B): every value is a vector (value tag `0x04`) of the
    /// element type, dimension and metric its `kind_params` give ([`crate::vector`]). Not an
    /// aggregate: cells version and delete as a `Standard` family's. Needs
    /// [`INCOMPAT_AGGREGATES`].
    Vector = 16,
}

impl FamilyKind {
    /// Whether the kind is an aggregate family: operands combine within one bucket, and the
    /// kind fixes the operator. An explicit list (D303): every kind but `Standard` and
    /// `Vector`.
    ///
    /// ```
    /// use pigeonhole_format::manifest::FamilyKind;
    ///
    /// assert!(!FamilyKind::Standard.is_aggregate());
    /// assert!(FamilyKind::Counter.is_aggregate() && FamilyKind::Min.is_aggregate());
    /// assert!(!FamilyKind::Vector.is_aggregate());
    /// ```
    pub fn is_aggregate(self) -> bool {
        matches!(
            self,
            FamilyKind::Counter
                | FamilyKind::Min
                | FamilyKind::Max
                | FamilyKind::Hll
                | FamilyKind::BitOr
                | FamilyKind::BitAnd
                | FamilyKind::BitXor
                | FamilyKind::MinF64
                | FamilyKind::MaxF64
                | FamilyKind::SumF64
                | FamilyKind::MeanVar
                | FamilyKind::Histogram
                | FamilyKind::Quantiles
                | FamilyKind::Bloom
        )
    }

    /// The `incompat` feature bits a file must carry, in both superblock pages, before a
    /// family of this kind is written (D294; FORMAT §8.2): 0 for the kinds 1.0 knows. Kinds
    /// that land together share one bit, and a kind's later parameters fall under its bit.
    ///
    /// ```
    /// use pigeonhole_format::manifest::{FamilyKind, INCOMPAT_AGGREGATES};
    ///
    /// assert_eq!(FamilyKind::Counter.required_incompat(), 0);
    /// assert_eq!(FamilyKind::Max.required_incompat(), INCOMPAT_AGGREGATES);
    /// ```
    pub fn required_incompat(self) -> u64 {
        match self {
            FamilyKind::Standard | FamilyKind::Counter => 0,
            FamilyKind::Min
            | FamilyKind::Max
            | FamilyKind::Hll
            | FamilyKind::BitOr
            | FamilyKind::BitAnd
            | FamilyKind::BitXor
            | FamilyKind::MinF64
            | FamilyKind::MaxF64
            | FamilyKind::SumF64
            | FamilyKind::MeanVar
            | FamilyKind::Histogram
            | FamilyKind::Quantiles
            | FamilyKind::Bloom
            | FamilyKind::Vector => INCOMPAT_AGGREGATES,
        }
    }

    /// Whether the kind's values must stay inline, never separated into blob files at commit,
    /// flush or compaction (D298): a value its built-in operator folds, or a reader must see
    /// whole. An explicit list (not "every aggregate kind"): #488 PR 5 takes `Bloom` out once
    /// its operator folds a separated base (§4.1), and never puts `Set` in. Vectors are in it
    /// (D303): typed values, at most 16 KiB, read in place.
    ///
    /// ```
    /// use pigeonhole_format::manifest::FamilyKind;
    ///
    /// assert!(FamilyKind::Hll.values_stay_inline());
    /// assert!(!FamilyKind::Standard.values_stay_inline());
    /// ```
    pub fn values_stay_inline(self) -> bool {
        matches!(
            self,
            FamilyKind::Counter
                | FamilyKind::Min
                | FamilyKind::Max
                | FamilyKind::Hll
                | FamilyKind::BitOr
                | FamilyKind::BitAnd
                | FamilyKind::BitXor
                | FamilyKind::MinF64
                | FamilyKind::MaxF64
                | FamilyKind::SumF64
                | FamilyKind::MeanVar
                | FamilyKind::Histogram
                | FamilyKind::Quantiles
                | FamilyKind::Bloom
                | FamilyKind::Vector
        )
    }
}

/// The `incompat` feature bit of the family kinds added after 1.0 (bit 0, D294, D298, D303):
/// every kind from Min on (the vector kind and value tag `0x04` included), and
/// `kind_params`; they ship together in 1.1.0.
pub const INCOMPAT_AGGREGATES: u64 = 1 << 0;

/// The persisted policy of one family. Stored in the manifest so another binary interprets
/// the data the same way.
///
/// `#[non_exhaustive]`, so fields can be added without a breaking release: build it from
/// [`FamilyOptions::default`] and the setters, one per field.
///
/// ```
/// use pigeonhole_format::manifest::{CompactionStyle, FamilyKind, FamilyOptions};
///
/// let counters = FamilyOptions::default()
///     .merge_operator("pigeonhole.i64_add")
///     .kind(FamilyKind::Counter)
///     .compaction(CompactionStyle::Tiered);
/// assert_eq!(counters.kind, FamilyKind::Counter);
/// assert_eq!(counters.max_versions, 0); // the default
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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
    /// What the family's columns hold (appended field; absent reads as `Standard`).
    pub kind: FamilyKind,
    /// The kind's parameters ([`crate::kind_params`], D298): a canonical tag-length-value
    /// list, empty for a kind that defines none (every kind so far). Appended to the
    /// encoding after `kind` only when non-empty.
    pub kind_params: Vec<u8>,
}

impl Default for FamilyOptions {
    /// LZ4, 16 KiB blocks, 10 bloom bits per key, every version kept, no TTL, values over
    /// 4 KiB separated (spec defaults), no merge operator, normal priority, leveled.
    fn default() -> Self {
        Self {
            compression: Compression::Lz4,
            compression_level: 3,
            block_size: crate::block::DEFAULT_BLOCK_SIZE as u32,
            bloom_bits: 10,
            max_versions: 0,
            ttl_micros: 0,
            blob_threshold: 4096,
            merge_operator: String::new(),
            cache_priority: CachePriority::Normal,
            compaction: CompactionStyle::Leveled,
            kind: FamilyKind::Standard,
            kind_params: Vec::new(),
        }
    }
}

impl FamilyOptions {
    /// Sets `compression`: block codec.
    #[must_use]
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets `compression_level`: zstd level when `compression` is zstd.
    #[must_use]
    pub fn compression_level(mut self, compression_level: i8) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Sets `block_size`: target uncompressed data-block size.
    #[must_use]
    pub fn block_size(mut self, block_size: u32) -> Self {
        self.block_size = block_size;
        self
    }

    /// Sets `bloom_bits`: bloom bits per key; 0 disables both filters.
    #[must_use]
    pub fn bloom_bits(mut self, bloom_bits: u8) -> Self {
        self.bloom_bits = bloom_bits;
        self
    }

    /// Sets `max_versions`: versions kept per column; 0 keeps all.
    #[must_use]
    pub fn max_versions(mut self, max_versions: u32) -> Self {
        self.max_versions = max_versions;
        self
    }

    /// Sets `ttl_micros`: time to live in microseconds; 0 disables TTL.
    #[must_use]
    pub fn ttl_micros(mut self, ttl_micros: u64) -> Self {
        self.ttl_micros = ttl_micros;
        self
    }

    /// Sets `blob_threshold`: values longer than this are separated into blob extents; `u32::MAX` disables it.
    #[must_use]
    pub fn blob_threshold(mut self, blob_threshold: u32) -> Self {
        self.blob_threshold = blob_threshold;
        self
    }

    /// Sets `cache_priority`: cache priority.
    #[must_use]
    pub fn cache_priority(mut self, cache_priority: CachePriority) -> Self {
        self.cache_priority = cache_priority;
        self
    }

    /// Sets `compaction`: compaction strategy.
    #[must_use]
    pub fn compaction(mut self, compaction: CompactionStyle) -> Self {
        self.compaction = compaction;
        self
    }

    /// Sets `kind`: what the family's columns hold.
    #[must_use]
    pub fn kind(mut self, kind: FamilyKind) -> Self {
        self.kind = kind;
        self
    }

    /// Sets `kind_params` (a canonical list, [`crate::kind_params::encode`]).
    pub fn kind_params(mut self, params: Vec<u8>) -> Self {
        self.kind_params = params;
        self
    }

    /// The payload length above which a flush or compaction separates a put's value into a
    /// blob file: [`blob_threshold`](Self::blob_threshold), or never (`u32::MAX`) for a kind
    /// whose values stay inline ([`FamilyKind::values_stay_inline`], D298; commit-time
    /// separation keeps them inline too).
    pub fn separation_threshold(&self) -> u32 {
        if self.kind.values_stay_inline() {
            u32::MAX
        } else {
            self.blob_threshold
        }
    }

    /// Sets `merge_operator`: the merge operator's name; empty for none.
    #[must_use]
    pub fn merge_operator(mut self, name: impl Into<String>) -> Self {
        self.merge_operator = name.into();
        self
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.compression as u8);
        out.push(self.compression_level as u8);
        out.extend_from_slice(&self.block_size.to_le_bytes());
        out.push(self.bloom_bits);
        out.extend_from_slice(&self.max_versions.to_le_bytes());
        out.extend_from_slice(&self.ttl_micros.to_le_bytes());
        out.extend_from_slice(&self.blob_threshold.to_le_bytes());
        crate::varint::put_bytes(out, self.merge_operator.as_bytes());
        out.push(self.cache_priority as u8);
        out.push(self.compaction as u8);
        out.push(self.kind as u8);
        // Appended for D298: present only when the kind has parameters.
        if !self.kind_params.is_empty() {
            crate::varint::put_bytes(out, &self.kind_params);
        }
    }

    fn decode(r: &mut Reader<'_>) -> crate::Result<Self> {
        Ok(Self {
            compression: {
                let c = r.u8()?;
                Compression::from_u8(c).map_err(|_| Error::UnsupportedFeature {
                    what: "family compression",
                    value: u64::from(c),
                })?
            },
            compression_level: r.i8()?,
            block_size: r.u32()?,
            bloom_bits: r.u8()?,
            max_versions: r.u32()?,
            ttl_micros: r.u64()?,
            blob_threshold: r.u32()?,
            merge_operator: r.string()?,
            cache_priority: match r.u8()? {
                0 => CachePriority::Low,
                1 => CachePriority::Normal,
                2 => CachePriority::High,
                p => {
                    return Err(Error::UnsupportedFeature {
                        what: "family cache priority",
                        value: u64::from(p),
                    });
                }
            },
            compaction: match r.u8()? {
                0 => CompactionStyle::Leveled,
                1 => CompactionStyle::Tiered,
                2 => CompactionStyle::FifoByTime,
                c => {
                    return Err(Error::UnsupportedFeature {
                        what: "family compaction style",
                        value: u64::from(c),
                    });
                }
            },
            // Appended after format 1's first release: absent means `Standard`.
            kind: if r.remaining() == 0 {
                FamilyKind::Standard
            } else {
                match r.u8()? {
                    0 => FamilyKind::Standard,
                    1 => FamilyKind::Counter,
                    2 => FamilyKind::Min,
                    3 => FamilyKind::Max,
                    4 => FamilyKind::Hll,
                    5 => FamilyKind::BitOr,
                    6 => FamilyKind::BitAnd,
                    7 => FamilyKind::BitXor,
                    8 => FamilyKind::MinF64,
                    9 => FamilyKind::MaxF64,
                    10 => FamilyKind::SumF64,
                    11 => FamilyKind::MeanVar,
                    12 => FamilyKind::Histogram,
                    13 => FamilyKind::Quantiles,
                    14 => FamilyKind::Bloom,
                    16 => FamilyKind::Vector,
                    k => {
                        return Err(Error::UnsupportedFeature {
                            what: "family kind",
                            value: u64::from(k),
                        });
                    }
                }
            },
            // Appended for D298: absent for a kind without parameters, and length 0 the same
            // (the slot a later appended field needs, perf287's S1 on #1213); else a
            // canonical list ([`crate::kind_params`]). Bytes after it are later fields.
            kind_params: if r.remaining() == 0 {
                Vec::new()
            } else {
                let params = r.bytes()?;
                crate::kind_params::parse(params)?;
                params.to_vec()
            },
        })
    }
}

/// Writes the length of the body that follows `at + 1` as a varint at `at`, where one byte
/// was reserved: a body of 128 bytes or more moves right to make room. The result is what
/// `varint::put_bytes` writes for the same body.
fn set_body_len(out: &mut Vec<u8>, at: usize) {
    let len = out.len() - at - 1;
    if len < 0x80 {
        out[at] = len as u8;
        return;
    }
    let mut buf = [0u8; 10];
    let n = {
        let mut v = len as u64;
        let mut i = 0;
        while v >= 0x80 {
            buf[i] = (v as u8) | 0x80;
            v >>= 7;
            i += 1;
        }
        buf[i] = v as u8;
        i + 1
    };
    let end = out.len();
    out.resize(end + n - 1, 0);
    out.copy_within(at + 1..end, at + n);
    out[at..at + n].copy_from_slice(&buf[..n]);
}

fn put_extent(out: &mut Vec<u8>, e: &ExtentRef) {
    out.extend_from_slice(&e.page.to_le_bytes());
    out.push(e.size_class);
}

fn get_extent(r: &mut Reader<'_>) -> crate::Result<ExtentRef> {
    ExtentRef {
        page: r.u64()?,
        size_class: r.u8()?,
    }
    .validate("manifest extent")
}

impl SstMeta {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.0.to_le_bytes());
        put_extent(out, &self.extent);
        out.extend_from_slice(&self.len.to_le_bytes());
        crate::varint::put_bytes(out, &self.smallest_key);
        crate::varint::put_bytes(out, &self.largest_key);
        for v in [
            self.seqno_range.0,
            self.seqno_range.1,
            self.ts_range.0,
            self.ts_range.1,
            self.entries,
            self.deletes,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
    }

    fn decode(r: &mut Reader<'_>) -> crate::Result<Self> {
        Ok(Self {
            id: SstId(r.u64()?),
            extent: get_extent(r)?,
            len: r.u64()?,
            smallest_key: r.bytes()?.to_vec(),
            largest_key: r.bytes()?.to_vec(),
            seqno_range: (r.u64()?, r.u64()?),
            ts_range: (r.u64()?, r.u64()?),
            entries: r.u64()?,
            deletes: r.u64()?,
        })
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
        /// Every timestamp assigned by default is at most this; default timestamps after
        /// open are greater (decision D11).
        ts_floor: Timestamp,
    },
    /// Tag 13. The blob files an SST's puts point into, with the bytes they reference in
    /// each (`16 + len` per pointer, FORMAT §7). Written with every new SST (an empty list:
    /// it points into none); an SST without one, written by an older build, may point into
    /// any blob file of its family.
    SstBlobRefs {
        /// SST.
        sst: SstId,
        /// `(blob file, referenced bytes)`, by blob file id.
        refs: Vec<(BlobFileId, u64)>,
    },
}

impl Edit {
    /// Appends the tagged encoding of this edit: the tag, the body's length as a varint, then
    /// the body. The body is written in place (#499): a one-byte length is reserved and
    /// widened afterwards if the body turned out longer, so no edit allocates a body of its
    /// own.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let tag_at = out.len();
        out.extend_from_slice(&[0, 0]);
        let b: &mut Vec<u8> = out;
        let tag = match self {
            Edit::CreateTable { table, name } => {
                b.extend_from_slice(&table.0.to_le_bytes());
                crate::varint::put_bytes(b, name.as_bytes());
                1
            }
            Edit::DropTable { table } => {
                b.extend_from_slice(&table.0.to_le_bytes());
                2
            }
            Edit::PutFamily {
                table,
                family,
                name,
                options,
            } => {
                b.extend_from_slice(&table.0.to_le_bytes());
                b.extend_from_slice(&family.0.to_le_bytes());
                crate::varint::put_bytes(b, name.as_bytes());
                options.encode(b);
                3
            }
            Edit::PutTablet {
                tablet,
                table,
                start,
                end,
            } => {
                b.extend_from_slice(&tablet.0.to_le_bytes());
                b.extend_from_slice(&table.0.to_le_bytes());
                crate::varint::put_bytes(b, start);
                match end {
                    None => b.push(0),
                    Some(end) => {
                        b.push(1);
                        crate::varint::put_bytes(b, end);
                    }
                }
                4
            }
            Edit::DropTablet { tablet } => {
                b.extend_from_slice(&tablet.0.to_le_bytes());
                5
            }
            Edit::AddSst {
                tablet,
                family,
                level,
                meta,
            } => {
                b.extend_from_slice(&tablet.0.to_le_bytes());
                b.extend_from_slice(&family.0.to_le_bytes());
                b.push(*level);
                meta.encode(b);
                6
            }
            Edit::RemoveSst {
                tablet,
                family,
                sst,
            } => {
                b.extend_from_slice(&tablet.0.to_le_bytes());
                b.extend_from_slice(&family.0.to_le_bytes());
                b.extend_from_slice(&sst.0.to_le_bytes());
                7
            }
            Edit::SetFlushed {
                tablet,
                family,
                seqno,
            } => {
                b.extend_from_slice(&tablet.0.to_le_bytes());
                b.extend_from_slice(&family.0.to_le_bytes());
                b.extend_from_slice(&seqno.to_le_bytes());
                8
            }
            Edit::WalCheckpoint { stream, lsn } => {
                b.extend_from_slice(&stream.0.to_le_bytes());
                b.extend_from_slice(&lsn.0.to_le_bytes());
                9
            }
            Edit::PutBlobFile {
                blob_file,
                family,
                extents,
                total_bytes,
                live_bytes,
            } => {
                b.extend_from_slice(&blob_file.0.to_le_bytes());
                b.extend_from_slice(&family.0.to_le_bytes());
                b.extend_from_slice(&(extents.len() as u32).to_le_bytes());
                for e in extents {
                    put_extent(b, e);
                }
                b.extend_from_slice(&total_bytes.to_le_bytes());
                b.extend_from_slice(&live_bytes.to_le_bytes());
                10
            }
            Edit::DropBlobFile { blob_file } => {
                b.extend_from_slice(&blob_file.0.to_le_bytes());
                11
            }
            Edit::SstBlobRefs { sst, refs } => {
                b.extend_from_slice(&sst.0.to_le_bytes());
                b.extend_from_slice(&(refs.len() as u32).to_le_bytes());
                for (blob_file, bytes) in refs {
                    b.extend_from_slice(&blob_file.0.to_le_bytes());
                    b.extend_from_slice(&bytes.to_le_bytes());
                }
                13
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
                b.extend_from_slice(&next_table.to_le_bytes());
                b.extend_from_slice(&next_family.to_le_bytes());
                b.extend_from_slice(&next_tablet.to_le_bytes());
                b.extend_from_slice(&next_sst.to_le_bytes());
                b.extend_from_slice(&next_blob_file.to_le_bytes());
                b.extend_from_slice(&seqno_ceiling.to_le_bytes());
                b.extend_from_slice(&ts_floor.to_le_bytes());
                12
            }
        };
        out[tag_at] = tag;
        set_body_len(out, tag_at + 1);
    }

    /// Decodes one edit from the front of `input`; returns it and the bytes consumed.
    ///
    /// Bytes after the known fields of a body are ignored (fields can be appended). An
    /// unknown tag fails with `Corrupt`; [`decode_block`] skips such edits instead.
    pub fn decode(input: &[u8]) -> crate::Result<(Self, usize)> {
        let mut outer = Reader::new(input, "manifest edit");
        let tag = outer.u8()?;
        let body = outer.bytes()?;
        let r = &mut Reader::new(body, "manifest edit");
        let edit = match tag {
            1 => Edit::CreateTable {
                table: TableId(r.u32()?),
                name: r.string()?,
            },
            2 => Edit::DropTable {
                table: TableId(r.u32()?),
            },
            3 => Edit::PutFamily {
                table: TableId(r.u32()?),
                family: FamilyId(r.u32()?),
                name: r.string()?,
                options: FamilyOptions::decode(r)?,
            },
            4 => Edit::PutTablet {
                tablet: TabletId(r.u64()?),
                table: TableId(r.u32()?),
                start: r.bytes()?.to_vec(),
                end: match r.u8()? {
                    0 => None,
                    1 => Some(r.bytes()?.to_vec()),
                    _ => {
                        return Err(Error::Corrupt {
                            what: "manifest opt-bytes",
                        });
                    }
                },
            },
            5 => Edit::DropTablet {
                tablet: TabletId(r.u64()?),
            },
            6 => Edit::AddSst {
                tablet: TabletId(r.u64()?),
                family: FamilyId(r.u32()?),
                level: r.u8()?,
                meta: SstMeta::decode(r)?,
            },
            7 => Edit::RemoveSst {
                tablet: TabletId(r.u64()?),
                family: FamilyId(r.u32()?),
                sst: SstId(r.u64()?),
            },
            8 => Edit::SetFlushed {
                tablet: TabletId(r.u64()?),
                family: FamilyId(r.u32()?),
                seqno: r.u64()?,
            },
            9 => Edit::WalCheckpoint {
                stream: StreamId(r.u32()?),
                lsn: Lsn(r.u64()?),
            },
            10 => {
                let blob_file = BlobFileId(r.u32()?);
                let family = FamilyId(r.u32()?);
                let count = r.u32()? as usize;
                // Each extent is 9 bytes, which bounds the allocation.
                if count > r.remaining() / 9 {
                    return Err(Error::Truncated {
                        what: "manifest edit",
                    });
                }
                let extents = (0..count)
                    .map(|_| get_extent(r))
                    .collect::<crate::Result<_>>()?;
                Edit::PutBlobFile {
                    blob_file,
                    family,
                    extents,
                    total_bytes: r.u64()?,
                    live_bytes: r.u64()?,
                }
            }
            11 => Edit::DropBlobFile {
                blob_file: BlobFileId(r.u32()?),
            },
            13 => {
                let sst = SstId(r.u64()?);
                let count = r.u32()? as usize;
                // Each reference is 12 bytes, which bounds the allocation.
                if count > r.remaining() / 12 {
                    return Err(Error::Truncated {
                        what: "manifest edit",
                    });
                }
                let refs = (0..count)
                    .map(|_| Ok((BlobFileId(r.u32()?), r.u64()?)))
                    .collect::<crate::Result<_>>()?;
                Edit::SstBlobRefs { sst, refs }
            }
            12 => Edit::Counters {
                next_table: r.u32()?,
                next_family: r.u32()?,
                next_tablet: r.u64()?,
                next_sst: r.u64()?,
                next_blob_file: r.u32()?,
                seqno_ceiling: r.u64()?,
                ts_floor: r.u64()?,
            },
            _ => {
                return Err(Error::Corrupt {
                    what: "manifest edit tag",
                });
            }
        };
        Ok((edit, outer.pos()))
    }
}

#[cfg(test)]
mod encode_tests {
    use super::*;

    /// The in-place length is what `varint::put_bytes` writes, at every varint width (#499).
    #[test]
    fn in_place_body_length_matches_put_bytes() {
        for len in [0usize, 1, 127, 128, 129, 300, 16_383, 16_384, 20_000] {
            let body: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let mut expected = vec![7u8];
            crate::varint::put_bytes(&mut expected, &body);
            let mut out = vec![7u8, 0];
            out.extend_from_slice(&body);
            set_body_len(&mut out, 1);
            assert_eq!(out, expected, "body of {len} bytes");
        }
    }

    fn put_family(options: FamilyOptions) -> Vec<u8> {
        let mut out = Vec::new();
        Edit::PutFamily {
            table: TableId(1),
            family: FamilyId(2),
            name: "f".into(),
            options,
        }
        .encode(&mut out);
        out
    }

    /// D298's kinds round-trip, and `kind_params` is appended only when present: a family
    /// without parameters encodes as before it existed.
    #[test]
    fn d298_kinds_and_kind_params_round_trip() {
        for kind in [
            FamilyKind::BitOr,
            FamilyKind::BitAnd,
            FamilyKind::BitXor,
            FamilyKind::MinF64,
            FamilyKind::MaxF64,
            FamilyKind::SumF64,
            FamilyKind::MeanVar,
            FamilyKind::Histogram,
            FamilyKind::Quantiles,
            FamilyKind::Bloom,
            FamilyKind::Vector,
        ] {
            let plain = FamilyOptions::default().kind(kind);
            let bytes = put_family(plain.clone());
            assert_eq!(
                bytes.last(),
                Some(&(kind as u8)),
                "{kind:?}: kind is the last field"
            );
            let (Edit::PutFamily { options, .. }, _) = Edit::decode(&bytes).unwrap() else {
                panic!("a family");
            };
            assert_eq!(options, plain);
            assert_eq!(
                kind.required_incompat(),
                crate::manifest::INCOMPAT_AGGREGATES
            );
            let params = crate::kind_params::encode(&[(1, &[3]), (200, &9u32.to_le_bytes())]);
            let with = plain.kind_params(params.clone());
            let bytes = put_family(with.clone());
            let (Edit::PutFamily { options, .. }, _) = Edit::decode(&bytes).unwrap() else {
                panic!("a family");
            };
            assert_eq!((options.kind_params, options.kind), (params, kind));
        }
    }

    /// A `BitOr` family's `PutFamily` with `tail` appended to its body by hand.
    fn with_tail(tail: &[u8]) -> Vec<u8> {
        let bytes = put_family(FamilyOptions::default().kind(FamilyKind::BitOr));
        let (body, _) = crate::varint::get_bytes(&bytes[1..]).unwrap();
        let mut body = body.to_vec();
        body.extend_from_slice(tail);
        let mut fixed = vec![bytes[0]];
        crate::varint::put_bytes(&mut fixed, &body);
        fixed
    }

    /// A present `kind_params` of length 0 is no parameters, and what follows it is a later
    /// appended field this build ignores (perf287's S1 on #1213): the slot a future field
    /// needs after a family without parameters.
    #[test]
    fn a_zero_length_kind_params_is_no_parameters() {
        for tail in [&[0u8][..], &[0, 0x42], &[0, 3, 1, 2, 3]] {
            let (Edit::PutFamily { options, .. }, _) = Edit::decode(&with_tail(tail)).unwrap()
            else {
                panic!("a family");
            };
            assert_eq!(
                options,
                FamilyOptions::default().kind(FamilyKind::BitOr),
                "{tail:?}"
            );
        }
    }

    /// A present `kind_params` that is not canonical is corruption.
    #[test]
    fn non_canonical_kind_params_are_corrupt() {
        for bad in [vec![2, 0, 1, 0], vec![0, 0], vec![1, 5, 0]] {
            let mut tail = Vec::new();
            crate::varint::put_bytes(&mut tail, &bad);
            let fixed = with_tail(&tail);
            assert!(
                matches!(Edit::decode(&fixed), Err(Error::Corrupt { .. })),
                "{bad:?}: {:?}",
                Edit::decode(&fixed)
            );
        }
    }

    /// A blob file of many extents (a body of about 18 KB, a 3-byte length) round-trips.
    #[test]
    fn a_long_edit_round_trips() {
        let edit = Edit::PutBlobFile {
            blob_file: BlobFileId(9),
            family: FamilyId(2),
            extents: (0..2_000)
                .map(|i| ExtentRef {
                    // 64 KiB extents (class 0, 16 pages), each page-aligned to its size.
                    page: crate::superblock::FIRST_DATA_PAGE + i * 16,
                    size_class: 0,
                })
                .collect(),
            total_bytes: 1 << 30,
            live_bytes: 1 << 29,
        };
        let mut out = vec![0xAA];
        edit.encode(&mut out);
        assert_eq!(Edit::decode(&out[1..]).unwrap(), (edit, out.len() - 1));
    }
}
