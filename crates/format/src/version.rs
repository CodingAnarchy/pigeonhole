//! Version numbers and magic constants. Every persisted structure carries one or the other.

/// Version of the main-file and WAL byte formats. Stored in the superblock, the SST footer,
/// manifest blocks and WAL segment headers.
///
/// ```
/// use pigeonhole_format::FormatVersion;
///
/// assert!(FormatVersion::CURRENT.is_readable());
/// assert!(FormatVersion(1).is_readable()); // 0.1.0 files
/// assert!(FormatVersion(2).is_readable()); // 0.2.x and 0.3.x files
/// assert!(FormatVersion(3).is_readable()); // unreleased builds before format 4
/// assert!(!FormatVersion(99).is_readable());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct FormatVersion(pub u32);

impl FormatVersion {
    /// The format this build writes. Version 2 adds blob files (FORMAT §7): a version 1
    /// build (0.1.0) would read a blob pointer as an empty value, so it must refuse a file
    /// this build has written. Version 3 adds the WAL's DECISION record (FORMAT §10.3, D263,
    /// ICR 0046): a version 2 build (0.2.x, 0.3.x) would fail on it as corruption, or skip a
    /// version 3 segment as a torn header, so it must refuse the file by its version.
    /// Version 4 is the 1.x format (D269): the superblock's feature sets
    /// ([`Features`](crate::superblock::Features)), the 128-byte WAL segment header, the SST
    /// footer's incompatible-feature bits and the manifest's edit-tag ranges. 1.x never bumps
    /// it again: later 1.x features set feature bits instead (FORMAT §12).
    pub const CURRENT: Self = Self(4);
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
    /// The layout this build creates and attaches to (exact match required). Version 2 adds
    /// each shard's durable watermark (D227). Version 3 judges reader liveness by each
    /// slot's byte-range lock in the database file, not by process id (#693 PR 3): a
    /// version 2 reader takes no such lock and would look dead to a version 3 writer.
    /// Version 4 gives every memtable node a stale-tail offset (D277; the memtable header's
    /// version 2), so reader processes skip superseded versions as the writer does: a
    /// version 3 reader would misread every node's tower and key. Version 5 adds the
    /// row-epoch table the row caches of every process share (D281): a version 4 writer
    /// would not raise its slots, so a version 5 reader's cache would serve stale rows.
    pub const CURRENT: Self = Self(5);
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
