//! Blob extents: separated large values (WiscKey-style). A logical blob file is a sequence of
//! equal-sized extents listed in the manifest; each extent starts with a 64-byte header and
//! the payload areas concatenate into one logical address space. See `FORMAT.md` §7.
//!
//! ```
//! use pigeonhole_format::blob::{encode_record_header, verify_record};
//!
//! let header = encode_record_header(b"large value");
//! verify_record(&header, b"large value", 11).unwrap();
//! assert!(verify_record(&header, b"other value", 11).is_err());
//! ```

use crate::bytes::{le_u32, le_u64};
use crate::version::BLOB_MAGIC;
use crate::{BlobFileId, Error, FormatVersion};

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
        let mut out = [0; BLOB_EXTENT_HEADER_LEN];
        out[0..8].copy_from_slice(&BLOB_MAGIC);
        out[8..12].copy_from_slice(&self.version.0.to_le_bytes());
        out[12..16].copy_from_slice(&self.blob_file.0.to_le_bytes());
        out[16..20].copy_from_slice(&self.extent_index.to_le_bytes());
        let checksum = crate::checksum::xxh3_64(&out[..56]);
        out[56..64].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    /// Decodes and verifies magic, checksum and version.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let Some(b) = bytes.get(..BLOB_EXTENT_HEADER_LEN) else {
            return Err(Error::Truncated {
                what: "blob extent header",
            });
        };
        if b[..8] != BLOB_MAGIC {
            return Err(Error::BadMagic {
                what: "blob extent header",
            });
        }
        if crate::checksum::xxh3_64(&b[..56]) != le_u64(b, 56) {
            return Err(Error::Checksum {
                what: "blob extent header",
            });
        }
        let version = FormatVersion(le_u32(b, 8));
        version.check("blob extent header")?;
        Ok(Self {
            version,
            blob_file: BlobFileId(le_u32(b, 12)),
            extent_index: le_u32(b, 16),
        })
    }
}

/// Encodes a blob record header for `value`.
pub fn encode_record_header(value: &[u8]) -> [u8; BLOB_RECORD_HEADER_LEN] {
    let mut out = [0; BLOB_RECORD_HEADER_LEN];
    out[..8].copy_from_slice(&(value.len() as u64).to_le_bytes());
    out[8..].copy_from_slice(&crate::checksum::xxh3_64(value).to_le_bytes());
    out
}

/// Verifies `value` against an encoded record header and the pointer's expected length.
pub fn verify_record(header: &[u8], value: &[u8], expected_len: u32) -> crate::Result<()> {
    if header.len() < BLOB_RECORD_HEADER_LEN {
        return Err(Error::Truncated {
            what: "blob record header",
        });
    }
    let len = le_u64(header, 0);
    if len != u64::from(expected_len) || len != value.len() as u64 {
        return Err(Error::Corrupt {
            what: "blob record length",
        });
    }
    if crate::checksum::xxh3_64(value) != le_u64(header, 8) {
        return Err(Error::Checksum {
            what: "blob record",
        });
    }
    Ok(())
}
