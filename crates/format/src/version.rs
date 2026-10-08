//! Version numbers and magic constants. Every persisted structure carries one or the other.

/// Version of the main-file and WAL byte formats. Stored in the superblock, the SST footer,
/// manifest blocks and WAL segment headers.
///
/// ```
/// use pigeonhole_format::FormatVersion;
///
/// assert!(FormatVersion::CURRENT.is_readable());
/// assert!(FormatVersion(1).is_readable()); // 0.1.0 files
/// assert!(!FormatVersion(99).is_readable());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct FormatVersion(pub u32);

impl FormatVersion {
    /// The format this build writes. Version 2 adds blob files (FORMAT §7): a version 1
    /// build (0.1.0) would read a blob pointer as an empty value, so it must refuse a file
    /// this build has written.
    pub const CURRENT: Self = Self(2);
    /// The oldest format this build reads.
    pub const MIN_READABLE: Self = Self(1);

    /// Whether this build can read files written with `self`.
    pub fn is_readable(self) -> bool {
        Self::MIN_READABLE <= self && self <= Self::CURRENT
    }

    /// Fails with [`Error::UnsupportedVersion`](crate::Error::UnsupportedVersion) unless
    /// [`is_readable`](Self::is_readable).
    pub(crate) fn check(self, what: &'static str) -> crate::Result<()> {
        if self.is_readable() {
            Ok(())
        } else {
            Err(crate::Error::UnsupportedVersion {
                what,
                found: self.0,
            })
        }
    }
}

/// Version of the shared-memory region layout. Independent of [`FormatVersion`]: a process
/// whose layout version differs from a live region refuses to attach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ShmLayoutVersion(pub u32);

impl ShmLayoutVersion {
    /// The layout this build creates and attaches to (exact match required).
    pub const CURRENT: Self = Self(1);
}

/// Magic at offset 0 of each superblock (pages 0 and 1).
pub const SUPERBLOCK_MAGIC: [u8; 8] = *b"PHDBSUPR";
/// Magic at offset 0 of every manifest block.
pub const MANIFEST_MAGIC: [u8; 8] = *b"PHDBMANI";
/// Magic in the last 8 bytes of every SST.
pub const SST_MAGIC: [u8; 8] = *b"PHDBSST\x01";
/// Magic at offset 0 of every blob extent.
pub const BLOB_MAGIC: [u8; 8] = *b"PHDBBLOB";
/// Magic at offset 0 of every WAL segment.
pub const WAL_SEGMENT_MAGIC: [u8; 8] = *b"PHDBWALS";
/// Magic at offset 0 of the shared-memory region.
pub const SHM_MAGIC: [u8; 8] = *b"PHDBSHM\0";
/// Magic at offset 0 of a memtable header inside a shard arena.
pub const MEMTABLE_MAGIC: u32 = u32::from_le_bytes(*b"MEMT");
