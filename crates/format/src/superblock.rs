//! Superblocks (pages 0 and 1) and the lock page (page 2). See `FORMAT.md` §7 and §8.

use crate::{FormatVersion, ManifestVersion};

/// Page number of the first superblock.
pub const SUPERBLOCK_PAGE_A: u64 = 0;
/// Page number of the second superblock.
pub const SUPERBLOCK_PAGE_B: u64 = 1;
/// Page reserved for byte-range locks; never read or written.
pub const LOCK_PAGE: u64 = 2;
/// First page available to the extent allocator (pages 0-15 are reserved).
pub const FIRST_DATA_PAGE: u64 = 16;

/// Absolute file offset of the writer lock byte (held exclusive by the one writer).
pub const WRITER_LOCK_BYTE: u64 = LOCK_PAGE * crate::PAGE_SIZE as u64;
/// Absolute file offset of the presence lock byte (held shared by every open process).
pub const PRESENCE_LOCK_BYTE: u64 = WRITER_LOCK_BYTE + 1;
/// Absolute file offset of the shared-memory init byte (held exclusive while a process
/// creates, validates or rebuilds the shared-memory region).
pub const SHM_INIT_LOCK_BYTE: u64 = WRITER_LOCK_BYTE + 2;

/// Number of meaningful bytes at the start of a superblock page; the rest is zero.
pub const SUPERBLOCK_LEN: usize = 128;

/// A power-of-two run of pages: `64 KiB << size_class` bytes starting at `page`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExtentRef {
    /// First page.
    pub page: u64,
    /// Size class: 0 = 64 KiB, 1 = 128 KiB, ... 10 = 64 MiB.
    pub size_class: u8,
}

impl ExtentRef {
    /// Largest size class (64 MiB).
    pub const MAX_CLASS: u8 = 10;

    /// Absolute byte offset in the main file.
    pub fn offset(&self) -> u64 {
        todo!()
    }

    /// Length in bytes.
    pub fn len(&self) -> u64 {
        todo!()
    }

    /// Always false; extents are never empty. Present for API symmetry with `len`.
    pub fn is_empty(&self) -> bool {
        false
    }
}

/// One superblock. The valid copy with the higher `sequence` is current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Superblock {
    /// Format version of the file.
    pub version: FormatVersion,
    /// Page size; always 4096 in version 1.
    pub page_size: u32,
    /// Incremented by every root commit.
    pub sequence: u64,
    /// Random id chosen at creation; WAL segments and the shared-memory region carry it.
    pub db_id: [u8; 16],
    /// Extent holding the manifest snapshot block; `None` for an empty database.
    pub snapshot: Option<ExtentRef>,
    /// Length of the snapshot block in bytes.
    pub snapshot_len: u32,
    /// Extent holding the manifest delta log; `None` if no delta follows the snapshot.
    pub log: Option<ExtentRef>,
    /// Bytes of the delta log that belong to this root (later bytes are not live).
    pub log_len: u32,
    /// Manifest version this root represents (the last delta's, or the snapshot's).
    pub manifest_version: ManifestVersion,
    /// High-water mark of the file, in pages.
    pub file_pages: u64,
    /// Bit 0: the last writer closed cleanly. Other bits reserved.
    pub flags: u64,
}

impl Superblock {
    /// Encodes into a full page buffer (bytes past [`SUPERBLOCK_LEN`] are zero).
    pub fn encode(&self, page: &mut [u8; crate::PAGE_SIZE]) {
        todo!()
    }

    /// Decodes and verifies magic, checksum and version.
    pub fn decode(page: &[u8]) -> crate::Result<Self> {
        todo!()
    }

    /// Picks the current superblock from the two decode results, or fails if neither is valid.
    pub fn choose(a: crate::Result<Self>, b: crate::Result<Self>) -> crate::Result<(Self, u64)> {
        todo!()
    }
}
