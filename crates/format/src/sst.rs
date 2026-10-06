//! SST footer and properties. An SST is
//! `data blocks, index partitions, top index, row filter, column filter, properties, footer`
//! laid out from the start of its extent. See `FORMAT.md` §5.
//!
//! ```
//! use pigeonhole_format::block::BlockAddr;
//! use pigeonhole_format::sst::Footer;
//! use pigeonhole_format::FormatVersion;
//!
//! let footer = Footer {
//!     top_index: BlockAddr { offset: 4096, len: 200 },
//!     row_filter: BlockAddr::default(),
//!     column_filter: BlockAddr::default(),
//!     properties: BlockAddr { offset: 4296, len: 120 },
//!     compression_dict: BlockAddr::default(),
//!     version: FormatVersion::CURRENT,
//!     flags: 0,
//! };
//! assert_eq!(Footer::decode(&footer.encode()).unwrap(), footer);
//! ```

use crate::Error;
use crate::block::BlockAddr;
use crate::bytes::{Reader, le_u32, le_u64};
use crate::version::SST_MAGIC;
use crate::{FamilyId, FormatVersion, Seqno, TableId, TabletId, Timestamp};

/// Size of the fixed footer at the end of every SST.
pub const FOOTER_LEN: usize = 104;

/// The fixed-size footer. Its last 8 bytes are [`SST_MAGIC`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    /// Top-level index block.
    pub top_index: BlockAddr,
    /// Row-key filter block, or zero length if the family has no filter.
    pub row_filter: BlockAddr,
    /// Row+qualifier filter block, or zero length.
    pub column_filter: BlockAddr,
    /// Properties block.
    pub properties: BlockAddr,
    /// Compression dictionary block, or zero length (reserved for zstd dictionaries).
    pub compression_dict: BlockAddr,
    /// Format version of this SST.
    pub version: FormatVersion,
    /// Reserved flags; zero in version 1.
    pub flags: u32,
}

impl Footer {
    /// Encodes to exactly [`FOOTER_LEN`] bytes (checksum and magic included).
    pub fn encode(&self) -> [u8; FOOTER_LEN] {
        let mut out = [0; FOOTER_LEN];
        let addrs = [
            self.top_index,
            self.row_filter,
            self.column_filter,
            self.properties,
            self.compression_dict,
        ];
        for (i, a) in addrs.iter().enumerate() {
            out[16 * i..16 * i + 8].copy_from_slice(&a.offset.to_le_bytes());
            out[16 * i + 8..16 * i + 12].copy_from_slice(&a.len.to_le_bytes());
        }
        out[80..84].copy_from_slice(&self.version.0.to_le_bytes());
        out[84..88].copy_from_slice(&self.flags.to_le_bytes());
        let checksum = crate::checksum::xxh3_64(&out[..88]);
        out[88..96].copy_from_slice(&checksum.to_le_bytes());
        out[96..].copy_from_slice(&SST_MAGIC);
        out
    }

    /// Decodes and verifies magic, checksum and version. Reads the last [`FOOTER_LEN`] bytes
    /// of `bytes`, so the tail of an SST (or the whole SST) can be passed directly.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let Some(start) = bytes.len().checked_sub(FOOTER_LEN) else {
            return Err(Error::Truncated { what: "sst footer" });
        };
        let b = &bytes[start..];
        if b[96..] != SST_MAGIC {
            return Err(Error::BadMagic { what: "sst footer" });
        }
        if crate::checksum::xxh3_64(&b[..88]) != le_u64(b, 88) {
            return Err(Error::Checksum { what: "sst footer" });
        }
        let version = FormatVersion(le_u32(b, 80));
        version.check("sst footer")?;
        let addr = |i: usize| BlockAddr {
            offset: le_u64(b, 16 * i),
            len: le_u32(b, 16 * i + 8),
        };
        Ok(Self {
            top_index: addr(0),
            row_filter: addr(1),
            column_filter: addr(2),
            properties: addr(3),
            compression_dict: addr(4),
            version,
            flags: le_u32(b, 84),
        })
    }
}

/// Summary statistics stored in the properties block.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Properties {
    /// Table the SST belongs to.
    pub table: TableId,
    /// Family the SST belongs to.
    pub family: FamilyId,
    /// Tablet that wrote it.
    pub tablet: TabletId,
    /// Total entries.
    pub entries: u64,
    /// Distinct rows.
    pub rows: u64,
    /// Delete entries of any kind.
    pub deletes: u64,
    /// Merge operands.
    pub merges: u64,
    /// Sum of internal key lengths.
    pub raw_key_bytes: u64,
    /// Sum of stored value lengths.
    pub raw_value_bytes: u64,
    /// Data blocks.
    pub data_blocks: u32,
    /// Index partitions.
    pub index_partitions: u32,
    /// Smallest and largest seqno.
    pub seqno_range: (Seqno, Seqno),
    /// Smallest and largest timestamp (drives FIFO-by-time and TTL drops).
    pub ts_range: (Timestamp, Timestamp),
    /// Creation time, microseconds since the Unix epoch.
    pub created_micros: u64,
    /// Smallest internal key.
    pub smallest_key: Vec<u8>,
    /// Largest internal key.
    pub largest_key: Vec<u8>,
    /// Merge operator name in force when written, empty if none.
    pub merge_operator: String,
}

impl Properties {
    /// Appends the logical properties block.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.table.0.to_le_bytes());
        out.extend_from_slice(&self.family.0.to_le_bytes());
        out.extend_from_slice(&self.tablet.0.to_le_bytes());
        for v in [
            self.entries,
            self.rows,
            self.deletes,
            self.merges,
            self.raw_key_bytes,
            self.raw_value_bytes,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&self.data_blocks.to_le_bytes());
        out.extend_from_slice(&self.index_partitions.to_le_bytes());
        for v in [
            self.seqno_range.0,
            self.seqno_range.1,
            self.ts_range.0,
            self.ts_range.1,
            self.created_micros,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        crate::varint::put_bytes(out, &self.smallest_key);
        crate::varint::put_bytes(out, &self.largest_key);
        crate::varint::put_bytes(out, self.merge_operator.as_bytes());
    }

    /// Decodes a logical properties block. Trailing bytes after the last known field are
    /// ignored, so later versions can append fields.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let mut r = Reader::new(bytes, "sst properties");
        Ok(Self {
            table: TableId(r.u32()?),
            family: FamilyId(r.u32()?),
            tablet: TabletId(r.u64()?),
            entries: r.u64()?,
            rows: r.u64()?,
            deletes: r.u64()?,
            merges: r.u64()?,
            raw_key_bytes: r.u64()?,
            raw_value_bytes: r.u64()?,
            data_blocks: r.u32()?,
            index_partitions: r.u32()?,
            seqno_range: (r.u64()?, r.u64()?),
            ts_range: (r.u64()?, r.u64()?),
            created_micros: r.u64()?,
            smallest_key: r.bytes()?.to_vec(),
            largest_key: r.bytes()?.to_vec(),
            merge_operator: r.string()?,
        })
    }
}
