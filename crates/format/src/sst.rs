//! SST footer and properties. An SST is
//! `data blocks, index partitions, top index, row filter, column filter, properties, footer`
//! laid out from the start of its extent. See `FORMAT.md` §4.

use crate::block::BlockAddr;
use crate::{FamilyId, FormatVersion, Seqno, TableId, TabletId, Timestamp};

/// Size of the fixed footer at the end of every SST.
pub const FOOTER_LEN: usize = 104;

/// The fixed-size footer. Its last 8 bytes are [`SST_MAGIC`](crate::version::SST_MAGIC).
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
        todo!()
    }

    /// Decodes and verifies magic, checksum and version.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        todo!()
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
        todo!()
    }

    /// Decodes a logical properties block.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        todo!()
    }
}
