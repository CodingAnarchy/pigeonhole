//! Blob extents: separated large values (WiscKey-style). A logical blob file is a sequence of
//! equal-sized extents listed in the manifest; each extent starts with a 64-byte header and
//! the payload areas concatenate into one logical address space. See `FORMAT.md` §6.

use crate::{BlobFileId, FormatVersion};

/// Size of the header at the start of every blob extent.
pub const BLOB_EXTENT_HEADER_LEN: usize = 64;

/// Size of the header in front of each blob record: `len u64 LE`, `xxh3-64 of value u64 LE`.
pub const BLOB_RECORD_HEADER_LEN: usize = 16;

/// The header of one blob extent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobExtentHeader {
    /// Format version.
    pub version: FormatVersion,
    /// The logical blob file this extent belongs to.
    pub blob_file: BlobFileId,
    /// Position of this extent in the blob file's extent list.
    pub extent_index: u32,
}

impl BlobExtentHeader {
    /// Encodes to 64 bytes (magic and checksum included).
    pub fn encode(&self) -> [u8; BLOB_EXTENT_HEADER_LEN] {
        todo!()
    }

    /// Decodes and verifies magic, checksum and version.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        todo!()
    }
}

/// Encodes a blob record header for `value`.
pub fn encode_record_header(value: &[u8]) -> [u8; BLOB_RECORD_HEADER_LEN] {
    todo!()
}

/// Verifies `value` against an encoded record header and the pointer's expected length.
pub fn verify_record(header: &[u8], value: &[u8], expected_len: u32) -> crate::Result<()> {
    todo!()
}
