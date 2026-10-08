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
/// possible) and are not returned.
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
        _ => {
            return Err(Error::Corrupt {
                what: "manifest block kind",
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
        if rest.first().is_some_and(|tag| !KNOWN_TAGS.contains(tag)) {
            pos += skip_edit(rest)?;
            continue;
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
/// (decision D179). Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum FamilyKind {
    /// Plain cells. Merge operands (with a named operator) fold across timestamps (D41) and
    /// deletes hide by timestamp. Families written before the kind was stored read as this.
    #[default]
    Standard = 0,
    /// An `i64` sum counter family (Bigtable's aggregate families): operands combine only
    /// within one `(column, timestamp)` bucket, each bucket is one version, and a delete
    /// hides only entries written before it (lower seqno) within its timestamp scope.
    Counter = 1,
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
    /// What the family's columns hold (appended field; absent reads as `Standard`).
    pub kind: FamilyKind,
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
        }
    }
}

impl FamilyOptions {
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
    }

    fn decode(r: &mut Reader<'_>) -> crate::Result<Self> {
        let corrupt = |what| move |_| Error::Corrupt { what };
        Ok(Self {
            compression: Compression::from_u8(r.u8()?).map_err(corrupt("family compression"))?,
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
                _ => {
                    return Err(Error::Corrupt {
                        what: "family cache priority",
                    });
                }
            },
            compaction: match r.u8()? {
                0 => CompactionStyle::Leveled,
                1 => CompactionStyle::Tiered,
                2 => CompactionStyle::FifoByTime,
                _ => {
                    return Err(Error::Corrupt {
                        what: "family compaction style",
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
                    _ => {
                        return Err(Error::Corrupt {
                            what: "family kind",
                        });
                    }
                }
            },
        })
    }
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
    /// Appends the tagged encoding of this edit.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let mut body = Vec::new();
        let b = &mut body;
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
        out.push(tag);
        crate::varint::put_bytes(out, &body);
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
