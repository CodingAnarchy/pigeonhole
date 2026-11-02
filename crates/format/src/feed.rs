//! The change feed's sidecar, `<db>-feed` (D310, FORMAT §10.4).
//!
//! An append-only log of the sessions that wrote the database while a change feed may have
//! history in its WAL. It is not part of the main file: builds that do not know the change
//! feed never open it. Each writable open of a build that knows it appends one entry: a
//! [`FeedEntry::Session`] when the open feeds tables, or a [`FeedEntry::Unfed`] when it
//! does not and the sidecar already exists. A feeding session then appends a
//! [`FeedEntry::Root`] after each root commit it makes, so a later feeding session can tell
//! whether any other session, of any build, committed a root in between: the superblock's
//! `sequence` at its open is then ahead of the last one recorded.
//!
//! The file is a 40-byte header, then entries, each `len` u32, `crc` u32 (CRC32C of the
//! payload), then `len` payload bytes whose first byte is the entry type. A crash can tear
//! the last entry: [`decode_entries`] stops at the first entry whose length or checksum
//! fails and reports where the valid entries end, and a writer truncates the file there
//! before appending.
//!
//! ```
//! use pigeonhole_format::feed::{FeedEntry, FeedHeader, FeedTables, decode_entries};
//!
//! let mut file = FeedHeader { db_id: [7; 16] }.encode().to_vec();
//! let entries = [
//!     FeedEntry::Session {
//!         session: 0xfeed,
//!         opened_at: 4,
//!         retention_bytes: 1 << 30,
//!         retention_age_secs: 0,
//!         streams: 4,
//!         tables: FeedTables::All,
//!     },
//!     FeedEntry::Root { session: 0xfeed, sequence: 5 },
//!     FeedEntry::Unfed { sequence: 7 },
//! ];
//! for e in &entries {
//!     e.encode(&mut file);
//! }
//! file.extend_from_slice(&[3, 0]); // a torn append
//!
//! let header = FeedHeader::decode(&file).unwrap();
//! assert_eq!(header.db_id, [7; 16]);
//! let (decoded, end) = decode_entries(&file[FeedHeader::LEN..]);
//! assert_eq!(decoded, entries);
//! assert_eq!(FeedHeader::LEN + end, file.len() - 2);
//! ```

use crate::bytes::{Reader, le_u32};
use crate::{Error, TableId};

/// The sidecar's magic.
pub const FEED_MAGIC: [u8; 8] = *b"PHDBFEED";

/// The sidecar layout this build writes and reads.
pub const FEED_VERSION: u32 = 1;

/// The sidecar's path suffix: the sidecar of `data.phdb` is `data.phdb-feed`.
pub const FEED_SUFFIX: &str = "-feed";

const WHAT: &str = "feed sidecar";

/// The sidecar's header (40 bytes): magic, version, reserved, the database id, reserved, and
/// a CRC32C of bytes 0..36.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedHeader {
    /// The database id from the superblock: a sidecar of another database is refused.
    pub db_id: [u8; 16],
}

impl FeedHeader {
    /// Length of the encoded header.
    pub const LEN: usize = 40;

    /// Encodes the header.
    pub fn encode(&self) -> [u8; Self::LEN] {
        let mut b = [0u8; Self::LEN];
        b[0..8].copy_from_slice(&FEED_MAGIC);
        b[8..12].copy_from_slice(&FEED_VERSION.to_le_bytes());
        b[16..32].copy_from_slice(&self.db_id);
        let crc = crate::checksum::crc32c(&b[..36]);
        b[36..40].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// Decodes and verifies magic, CRC and version.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let Some(b) = bytes.get(..Self::LEN) else {
            return Err(Error::Truncated { what: WHAT });
        };
        if b[..8] != FEED_MAGIC {
            return Err(Error::BadMagic { what: WHAT });
        }
        if crate::checksum::crc32c(&b[..36]) != le_u32(b, 36) {
            return Err(Error::Checksum { what: WHAT });
        }
        let version = le_u32(b, 8);
        if version != FEED_VERSION {
            return Err(Error::UnsupportedVersion {
                what: WHAT,
                found: version,
            });
        }
        Ok(Self {
            db_id: b[16..32].try_into().expect("16 bytes"),
        })
    }
}

/// Which tables a session feeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedTables {
    /// Every table, including those created during the session.
    All,
    /// Only these tables.
    Only(Vec<TableId>),
}

impl FeedTables {
    /// Whether `table` is fed.
    pub fn contains(&self, table: TableId) -> bool {
        match self {
            FeedTables::All => true,
            FeedTables::Only(t) => t.contains(&table),
        }
    }
}

/// One sidecar entry. Numbers (the type byte) are frozen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedEntry {
    /// Type 1: a feeding session's open, written and synced before the session writes any
    /// WAL segment (each of which carries `session` in its header, FORMAT §10.2).
    Session {
        /// The session's random id, nonzero.
        session: u64,
        /// The superblock's `sequence` when the open read it, before any root commit of its
        /// own.
        opened_at: u64,
        /// The session's feed retention in bytes, for the whole database.
        retention_bytes: u64,
        /// Its retention age in seconds, 0 for none.
        retention_age_secs: u64,
        /// The session's WAL streams (its shard count): a reader's history never spans
        /// sessions with different counts.
        streams: u32,
        /// The tables it feeds.
        tables: FeedTables,
    },
    /// Type 2: the superblock `sequence` after a root commit of a feeding session.
    Root {
        /// The session that committed.
        session: u64,
        /// The new root's sequence.
        sequence: u64,
    },
    /// Type 3: an open of a build that knows the feed, with the feed off: the feed's history
    /// ends here.
    Unfed {
        /// The superblock's `sequence` when the open read it.
        sequence: u64,
    },
    /// An entry type this build does not know, written by a newer one (its checksum
    /// passed). A reader treats it as the end of the history before it.
    Unknown(u8),
}

impl FeedEntry {
    /// Appends the entry, framed (`len`, `crc`, payload), to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let mut p = Vec::with_capacity(48);
        match self {
            FeedEntry::Session {
                session,
                opened_at,
                retention_bytes,
                retention_age_secs,
                streams,
                tables,
            } => {
                p.push(1);
                p.extend_from_slice(&session.to_le_bytes());
                p.extend_from_slice(&opened_at.to_le_bytes());
                p.extend_from_slice(&retention_bytes.to_le_bytes());
                p.extend_from_slice(&retention_age_secs.to_le_bytes());
                p.extend_from_slice(&streams.to_le_bytes());
                match tables {
                    FeedTables::All => {
                        p.push(1);
                        p.extend_from_slice(&0u32.to_le_bytes());
                    }
                    FeedTables::Only(t) => {
                        p.push(0);
                        p.extend_from_slice(&(t.len() as u32).to_le_bytes());
                        for id in t {
                            p.extend_from_slice(&id.0.to_le_bytes());
                        }
                    }
                }
            }
            FeedEntry::Root { session, sequence } => {
                p.push(2);
                p.extend_from_slice(&session.to_le_bytes());
                p.extend_from_slice(&sequence.to_le_bytes());
            }
            FeedEntry::Unfed { sequence } => {
                p.push(3);
                p.extend_from_slice(&sequence.to_le_bytes());
            }
            FeedEntry::Unknown(t) => p.push(*t),
        }
        out.extend_from_slice(&(p.len() as u32).to_le_bytes());
        out.extend_from_slice(&crate::checksum::crc32c(&p).to_le_bytes());
        out.extend_from_slice(&p);
    }

    /// Decodes one entry's payload (its checksum already verified).
    fn decode_payload(p: &[u8]) -> crate::Result<Self> {
        const WHAT: &str = "feed sidecar entry";
        let mut r = Reader::new(p, WHAT);
        let entry = match r.u8()? {
            1 => {
                let session = r.u64()?;
                let opened_at = r.u64()?;
                let retention_bytes = r.u64()?;
                let retention_age_secs = r.u64()?;
                let streams = r.u32()?;
                let all = r.u8()?;
                let n = r.u32()? as usize;
                let tables = match all {
                    1 if n == 0 => FeedTables::All,
                    0 => {
                        if r.remaining() != n * 4 {
                            return Err(Error::Corrupt { what: WHAT });
                        }
                        let mut t = Vec::with_capacity(n);
                        for _ in 0..n {
                            t.push(TableId(r.u32()?));
                        }
                        FeedTables::Only(t)
                    }
                    _ => return Err(Error::Corrupt { what: WHAT }),
                };
                FeedEntry::Session {
                    session,
                    opened_at,
                    retention_bytes,
                    retention_age_secs,
                    streams,
                    tables,
                }
            }
            2 => FeedEntry::Root {
                session: r.u64()?,
                sequence: r.u64()?,
            },
            3 => FeedEntry::Unfed { sequence: r.u64()? },
            t => return Ok(FeedEntry::Unknown(t)),
        };
        if r.remaining() != 0 {
            return Err(Error::Corrupt { what: WHAT });
        }
        Ok(entry)
    }
}

/// Decodes the entries after the header, up to the first torn or invalid one. Returns them
/// and the length of the valid prefix of `bytes` (where a writer truncates before
/// appending).
pub fn decode_entries(bytes: &[u8]) -> (Vec<FeedEntry>, usize) {
    let mut out = Vec::new();
    let mut pos = 0;
    while let Some(head) = bytes.get(pos..pos + 8) {
        let len = le_u32(head, 0) as usize;
        let Some(payload) = bytes.get(pos + 8..(pos + 8).saturating_add(len)) else {
            break;
        };
        if len == 0 || crate::checksum::crc32c(payload) != le_u32(head, 4) {
            break;
        }
        let Ok(entry) = FeedEntry::decode_payload(payload) else {
            break;
        };
        out.push(entry);
        pos += 8 + len;
    }
    (out, pos)
}
