//! Superblocks (pages 0 and 1) and the lock page (page 2). See `FORMAT.md` §8.

use crate::bytes::{le_u32, le_u64};
use crate::version::SUPERBLOCK_MAGIC;
use crate::{Error, FormatVersion, ManifestVersion, PAGE_SIZE};

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
        self.page.saturating_mul(PAGE_SIZE as u64)
    }

    /// Length in bytes. Saturates for a size class above [`MAX_CLASS`](Self::MAX_CLASS),
    /// which decoders reject.
    pub fn len(&self) -> u64 {
        (64u64 * 1024)
            .checked_shl(u32::from(self.size_class))
            .unwrap_or(u64::MAX)
    }

    /// Validates a decoded extent: page past the reserved pages, known size class, aligned to
    /// its own size.
    pub(crate) fn validate(self, what: &'static str) -> crate::Result<Self> {
        let pages = self.len() / PAGE_SIZE as u64;
        if self.page < FIRST_DATA_PAGE
            || self.size_class > Self::MAX_CLASS
            || !self.page.is_multiple_of(pages)
        {
            return Err(Error::Corrupt { what });
        }
        Ok(self)
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
        page.fill(0);
        let p = &mut page[..];
        put(p, 0, &SUPERBLOCK_MAGIC);
        put(p, 8, &self.version.0.to_le_bytes());
        put(p, 12, &self.page_size.to_le_bytes());
        put(p, 16, &self.sequence.to_le_bytes());
        put(p, 24, &self.db_id);
        let snap = self.snapshot.map_or((0, 0), |e| (e.page, e.size_class));
        put(p, 40, &snap.0.to_le_bytes());
        p[48] = snap.1;
        put(p, 52, &self.snapshot_len.to_le_bytes());
        put(p, 56, &self.manifest_version.to_le_bytes());
        put(p, 64, &self.file_pages.to_le_bytes());
        put(p, 72, &self.flags.to_le_bytes());
        let log = self.log.map_or((0, 0), |e| (e.page, e.size_class));
        put(p, 80, &log.0.to_le_bytes());
        p[88] = log.1;
        put(p, 92, &self.log_len.to_le_bytes());
        let checksum = crate::checksum::xxh3_64(&p[..120]);
        put(p, 120, &checksum.to_le_bytes());
    }

    /// Decodes and verifies magic, checksum and version.
    pub fn decode(page: &[u8]) -> crate::Result<Self> {
        let Some(b) = page.get(..SUPERBLOCK_LEN) else {
            return Err(Error::Truncated { what: "superblock" });
        };
        if b[..8] != SUPERBLOCK_MAGIC {
            return Err(Error::BadMagic { what: "superblock" });
        }
        if crate::checksum::xxh3_64(&b[..120]) != le_u64(b, 120) {
            return Err(Error::Checksum { what: "superblock" });
        }
        let version = FormatVersion(le_u32(b, 8));
        version.check("superblock")?;
        let page_size = le_u32(b, 12);
        if page_size as usize != PAGE_SIZE {
            return Err(Error::Corrupt {
                what: "superblock page size",
            });
        }
        let extent = |page: u64, size_class: u8, what| match page {
            0 => Ok(None),
            _ => ExtentRef { page, size_class }.validate(what).map(Some),
        };
        Ok(Self {
            version,
            page_size,
            sequence: le_u64(b, 16),
            db_id: b[24..40].try_into().expect("16 bytes"),
            snapshot: extent(le_u64(b, 40), b[48], "superblock snapshot extent")?,
            snapshot_len: le_u32(b, 52),
            manifest_version: le_u64(b, 56),
            file_pages: le_u64(b, 64),
            flags: le_u64(b, 72),
            log: extent(le_u64(b, 80), b[88], "superblock log extent")?,
            log_len: le_u32(b, 92),
        })
    }

    /// Picks the current superblock from the two decode results, or fails if neither is valid.
    /// Returns it with its page number ([`SUPERBLOCK_PAGE_A`] or [`SUPERBLOCK_PAGE_B`]); the
    /// next root commit overwrites the other page. On a tie in `sequence`, A wins. If both
    /// fail, an [`Error::UnsupportedVersion`] is preferred (a newer file, not a corrupt one).
    ///
    /// ```
    /// use pigeonhole_format::superblock::Superblock;
    /// use pigeonhole_format::FormatVersion;
    ///
    /// let sb = |sequence| Superblock {
    ///     version: FormatVersion::CURRENT,
    ///     page_size: 4096,
    ///     sequence,
    ///     db_id: [7; 16],
    ///     snapshot: None,
    ///     snapshot_len: 0,
    ///     log: None,
    ///     log_len: 0,
    ///     manifest_version: 0,
    ///     file_pages: 16,
    ///     flags: 0,
    /// };
    /// let mut page = [0u8; 4096];
    /// sb(5).encode(&mut page);
    /// let a = Superblock::decode(&page);
    /// sb(6).encode(&mut page);
    /// let b = Superblock::decode(&page);
    /// assert_eq!(Superblock::choose(a, b).unwrap(), (sb(6), 1));
    /// ```
    pub fn choose(a: crate::Result<Self>, b: crate::Result<Self>) -> crate::Result<(Self, u64)> {
        match (a, b) {
            (Ok(a), Ok(b)) if b.sequence > a.sequence => Ok((b, SUPERBLOCK_PAGE_B)),
            (Ok(a), _) => Ok((a, SUPERBLOCK_PAGE_A)),
            (Err(_), Ok(b)) => Ok((b, SUPERBLOCK_PAGE_B)),
            (Err(ea), Err(eb)) => match (&ea, &eb) {
                (_, Error::UnsupportedVersion { .. })
                    if !matches!(ea, Error::UnsupportedVersion { .. }) =>
                {
                    Err(eb)
                }
                _ => Err(ea),
            },
        }
    }
}

fn put(buf: &mut [u8], off: usize, v: &[u8]) {
    crate::bytes::put_at(buf, off, v);
}
