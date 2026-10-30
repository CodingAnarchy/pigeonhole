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
/// Absolute file offset of reader slot 0's liveness lock byte (shared-memory layout 3,
/// #693 PR 3): slot `i` is byte `READER_LOCK_BASE + i`, held exclusive by the reader process
/// that owns the slot for as long as it does. The writer frees a slot only once it can take
/// that byte itself. 2^62 lies beyond any file this format can reach, so no read or write
/// ever touches these bytes (on Windows, where byte-range locks are mandatory, that matters).
pub const READER_LOCK_BASE: u64 = 1 << 62;

/// Number of meaningful bytes at the start of a superblock page; the rest is zero.
pub const SUPERBLOCK_LEN: usize = 128;
/// The superblock page's extension area (format 4, D269; FORMAT §8.2): reserved for later
/// 1.x features, zero until one uses it. The superblock checksum does not cover it, so a
/// feature that stores data here brings its own checksum. Every format 4 build carries the
/// area unchanged into each root commit (`pigeonhole-pager`), so an unaware build keeps a
/// compat feature's data here; data tied to the root goes stale when that build commits a
/// new root, and the feature must detect that. [`Superblock::encode`] zeroes it.
pub const EXTENSION_AREA: std::ops::Range<usize> = SUPERBLOCK_LEN..crate::PAGE_SIZE - 8;

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

/// The file's feature sets (format 4, D269; FORMAT §8.2), as ext4 has them. A file sets a
/// bit only once it uses the feature, so a file that uses none stays openable by every 1.x
/// build. A build that meets a bit it does not know:
/// - `incompat`: refuses the file ([`Error::UnsupportedFeature`]);
/// - `ro_compat`: may read it but not write it;
/// - `compat`: ignores it, and keeps it when it rewrites the superblock.
///
/// **A compat feature's data must stay correct, or detectably stale, after a build that
/// does not know the feature commits.** Such a build keeps the bit and the
/// [`EXTENSION_AREA`] bytes, but it may change the root they describe, and it drops manifest
/// edits with skippable tags (128..=255) at its next manifest snapshot. A feature that
/// cannot tolerate that is `ro_compat`, so builds that do not know it cannot write.
///
/// Setting or clearing an `incompat` or `ro_compat` bit commits both superblock pages before
/// the file relies on the change: otherwise the other page keeps the old bits, and a build
/// may refuse the file, or write to it, by the stale page.
///
/// ```
/// use pigeonhole_format::superblock::Features;
///
/// assert!(Features::default().check_read().is_ok());
/// assert!(Features { incompat: 1 << 40, ..Features::default() }.check_read().is_err());
/// let ro = Features { ro_compat: 1 << 5, ..Features::default() };
/// assert!(ro.check_read().is_ok() && ro.check_write().is_err());
/// assert!(Features { compat: 1, ..Features::default() }.check_write().is_ok());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Features {
    /// Features an older build may read and write, ignoring them.
    pub compat: u64,
    /// Features an older build may read but must not write.
    pub ro_compat: u64,
    /// Features an older build must refuse the file for.
    pub incompat: u64,
}

impl Features {
    /// The `compat` bits this build knows (none yet).
    pub const KNOWN_COMPAT: u64 = 0;
    /// The `ro_compat` bits this build knows (none yet).
    pub const KNOWN_RO_COMPAT: u64 = 0;
    /// The `incompat` bits this build knows: bit 0, the Min and Max family kinds
    /// ([`INCOMPAT_AGGREGATES`](crate::manifest::INCOMPAT_AGGREGATES), D294).
    pub const KNOWN_INCOMPAT: u64 = crate::manifest::INCOMPAT_AGGREGATES;

    /// `incompat` bits this build does not know.
    pub fn unknown_incompat(&self) -> u64 {
        self.incompat & !Self::KNOWN_INCOMPAT
    }

    /// `ro_compat` bits this build does not know.
    pub fn unknown_ro_compat(&self) -> u64 {
        self.ro_compat & !Self::KNOWN_RO_COMPAT
    }

    /// Fails with [`Error::UnsupportedFeature`] if the file uses an incompatible feature this
    /// build does not know: it must not even read it.
    pub fn check_read(&self) -> crate::Result<()> {
        match self.unknown_incompat() {
            0 => Ok(()),
            bits => Err(Error::UnsupportedFeature {
                what: "incompatible feature bits",
                value: bits,
            }),
        }
    }

    /// As [`check_read`](Self::check_read), and also fails if the file uses a
    /// read-only-compatible feature this build does not know: it may read it, not write it.
    pub fn check_write(&self) -> crate::Result<()> {
        self.check_read()?;
        match self.unknown_ro_compat() {
            0 => Ok(()),
            bits => Err(Error::UnsupportedFeature {
                what: "read-only-compatible feature bits (the file can be opened read-only)",
                value: bits,
            }),
        }
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
    /// The file's feature sets (format 4; zero in older versions, whose bytes 96..120 are
    /// reserved).
    pub features: Features,
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
        put(p, 96, &self.features.compat.to_le_bytes());
        put(p, 104, &self.features.ro_compat.to_le_bytes());
        put(p, 112, &self.features.incompat.to_le_bytes());
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
        let snapshot = extent(le_u64(b, 40), b[48], "superblock snapshot extent")?;
        let snapshot_len = le_u32(b, 52);
        let log = extent(le_u64(b, 80), b[88], "superblock log extent")?;
        let log_len = le_u32(b, 92);
        // A length must fit its extent; an absent extent has length zero.
        let fits =
            |e: Option<ExtentRef>, len: u32| e.map_or(len == 0, |e| u64::from(len) <= e.len());
        if !fits(snapshot, snapshot_len) || !fits(log, log_len) {
            return Err(Error::Corrupt {
                what: "superblock manifest length",
            });
        }
        Ok(Self {
            version,
            page_size,
            sequence: le_u64(b, 16),
            db_id: b[24..40].try_into().expect("16 bytes"),
            snapshot,
            snapshot_len,
            manifest_version: le_u64(b, 56),
            file_pages: le_u64(b, 64),
            flags: le_u64(b, 72),
            log,
            log_len,
            // Bytes 96..120 were reserved (written zero) before format 4.
            features: if version >= FormatVersion(4) {
                Features {
                    compat: le_u64(b, 96),
                    ro_compat: le_u64(b, 104),
                    incompat: le_u64(b, 112),
                }
            } else {
                Features::default()
            },
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
    ///     features: Default::default(),
    /// };
    /// let mut page = [0u8; 4096];
    /// sb(5).encode(&mut page);
    /// let a = Superblock::decode(&page);
    /// sb(6).encode(&mut page);
    /// let b = Superblock::decode(&page);
    /// assert_eq!(Superblock::choose(a, b).unwrap(), (sb(6), 1));
    /// ```
    pub fn choose(a: crate::Result<Self>, b: crate::Result<Self>) -> crate::Result<(Self, u64)> {
        // A slot using an incompatible feature this build does not know is refused like a
        // newer version: a newer build set the bit (in both slots, before using the feature),
        // and the other slot may be one it has not rewritten yet.
        let features = |r: crate::Result<Self>| r.and_then(|s| s.features.check_read().map(|()| s));
        let (a, b) = (features(a), features(b));
        match (a, b) {
            // A slot with a valid checksum but a newer version was written by a newer build
            // (an upgrade that may have finished only that slot): never pass it over for the
            // older slot, whose root that build may already have freed (#496).
            (Err(e @ (Error::UnsupportedVersion { .. } | Error::UnsupportedFeature { .. })), _)
            | (_, Err(e @ (Error::UnsupportedVersion { .. } | Error::UnsupportedFeature { .. }))) => {
                Err(e)
            }
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
