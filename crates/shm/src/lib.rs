//! Shared-memory region and multi-process reader protocol for Pigeonhole.
//!
//! The region (layout in `FORMAT.md` §11) holds the header and writer generation, each
//! shard's published watermark, the current view (tablet map plus memtable roots), the
//! reader-slot table, and one memtable arena per shard. This crate owns the layout, the
//! lifecycle (create, attach, rebuild, remove), the locks on the main file's lock page, and
//! the reader protocol. What goes inside the arenas belongs to `pigeonhole-memtable`.
//!
//! All shared fields are accessed through [`SharedRegion`](pigeonhole_io::SharedRegion) atomics, so this crate needs no
//! `unsafe`. [`ShmRegion::in_memory`] is the heap-backed mock for engine tests.
//!
//! # Lifecycle
//!
//! Every process opens the main file **for writing** (even readers, which still write
//! nothing: byte-range locks that are exclusive, such as the shm-init byte and the
//! presence-byte upgrade at close, need a writable handle; decision D36).
//!
//! - The writer takes the [`WriterLock`], then calls [`ShmRegion::open`] with
//!   [`Role::Writer`], then takes [`Presence`], all on the same handle (decision D37). The
//!   open always builds a region under a new [`Generation`]: it creates the new region, marks
//!   the old one abandoned, records the new generation in the directory region and removes
//!   the old region's name. It takes the presence byte shared itself before publishing, so
//!   the later [`Presence::acquire`] only returns the guard.
//! - A reader takes [`Presence`], opens with [`Role::Reader`], claims a [`ReaderSlot`] and
//!   pins before each snapshot. When [`ShmRegion::is_stale`] reports that a new writer built
//!   a new generation, it calls [`ShmRegion::reattach`], re-claims a slot and re-pins.
//! - At close, the process that can [`Presence::try_become_last`] calls [`ShmRegion::remove`].
//!
//! ```
//! use pigeonhole_format::shm::ViewRecord;
//! use pigeonhole_io::ProcessId;
//! use pigeonhole_shm::{ShmConfig, ShmRegion};
//!
//! let mut config = ShmConfig::new(2);
//! config.arena_bytes = 2 << 20;
//! config.view_buffer_bytes = 64 << 10;
//! let shm = ShmRegion::in_memory([7; 16], &config);
//!
//! // Writer side: a group of three commits on shard 0.
//! shm.publish_pending(0, shm.visible_seqno() + 1);
//! let first = shm.reserve_seqnos(3);
//! shm.publish_pending(0, first);
//! // ... apply the group ...
//! shm.publish_pending(0, u64::MAX);
//! assert_eq!(shm.visible_seqno(), first + 2);
//!
//! shm.publish_view(&ViewRecord { view_version: 1, ..Default::default() }).unwrap();
//!
//! // Reader side: pin, then read through the pinned view.
//! let slot = shm.claim_reader_slot(ProcessId { pid: 42, start_time: 1 }).unwrap();
//! slot.pin(shm.visible_seqno(), shm.view_version());
//! let view = shm.read_view().unwrap();
//! assert_eq!(view.view_version, 1);
//! assert_eq!(shm.oldest_reader_pin(), Some((first + 2, 1)));
//! drop(slot);
//! assert_eq!(shm.oldest_reader_pin(), None);
//! ```
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

// The region's atomics are native-endian while FORMAT.md fixes its integers as
// little-endian; refuse to build where the two differ (decision D56).
#[cfg(not(target_endian = "little"))]
compile_error!("pigeonhole-shm supports little-endian targets only (FORMAT.md §11)");

mod lock;
mod region;

use std::fmt;
use std::sync::Arc;

use pigeonhole_format::{Seqno, ShmLayoutVersion};
use pigeonhole_io::FileRef;

pub use region::ShmRegion;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Shared-memory errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The underlying OS call failed.
    Io(pigeonhole_io::Error),
    /// Another process holds the writer lock.
    WriterLocked,
    /// A live region has a different layout version.
    VersionMismatch {
        /// Version in the region.
        found: u32,
        /// Version this build uses.
        expected: u32,
    },
    /// The region belongs to another database or its header is invalid.
    Corrupt(&'static str),
    /// Every reader slot is taken.
    NoReaderSlot,
    /// The region could not be allocated at the requested size.
    Unavailable,
    /// The encoded view does not fit in a view buffer. The writer refuses the change that
    /// would grow it (for example a tablet split) instead of publishing a truncated view.
    ViewTooLarge {
        /// Encoded view length.
        needed: usize,
        /// View buffer length.
        capacity: usize,
    },
    /// This mapping was replaced by a newer generation; call [`ShmRegion::reattach`].
    Stale,
    /// [`ShmRegion::publish_view`] was given a view version at or below the published one.
    /// Versions are strictly increasing (readers pin by version) and 0 means "no view".
    ViewVersionNotNewer {
        /// The version currently published.
        published: u64,
        /// The version offered.
        offered: u64,
    },
    /// An [`ShmConfig`] field is out of range (see [`ShmConfig::validate`]).
    InvalidConfig(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "shared memory: {e}"),
            Error::WriterLocked => f.write_str("another process holds the writer lock"),
            Error::VersionMismatch { found, expected } => write!(
                f,
                "shared-memory layout version {found} differs from this build's {expected}"
            ),
            Error::Corrupt(what) => write!(f, "shared-memory region is invalid: {what}"),
            Error::NoReaderSlot => f.write_str("every reader slot is taken"),
            Error::Unavailable => {
                f.write_str("the shared-memory region could not be allocated at the requested size")
            }
            Error::ViewTooLarge { needed, capacity } => write!(
                f,
                "encoded view ({needed} bytes) does not fit the view buffer ({capacity} bytes)"
            ),
            Error::Stale => {
                f.write_str("this mapping was replaced by a newer generation; re-attach")
            }
            Error::ViewVersionNotNewer { published, offered } => write!(
                f,
                "view version {offered} is not newer than the published version {published}"
            ),
            Error::InvalidConfig(what) => write!(f, "invalid shared-memory configuration: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<pigeonhole_io::Error> for Error {
    fn from(e: pigeonhole_io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<pigeonhole_format::Error> for Error {
    /// A header or view that fails to decode. A layout version other than this build's is
    /// [`Error::VersionMismatch`]; everything else is [`Error::Corrupt`].
    fn from(e: pigeonhole_format::Error) -> Self {
        match e {
            pigeonhole_format::Error::UnsupportedVersion { found, .. } => Error::VersionMismatch {
                found,
                expected: ShmLayoutVersion::CURRENT.0,
            },
            pigeonhole_format::Error::Truncated { what }
            | pigeonhole_format::Error::BadMagic { what }
            | pigeonhole_format::Error::Checksum { what }
            | pigeonhole_format::Error::Corrupt { what } => Error::Corrupt(what),
            _ => Error::Corrupt("shared-memory structure"),
        }
    }
}

/// Region generation: bumped each time a writer (re)builds the region, and part of the
/// region's name. The directory region records the current one; readers re-attach when it
/// changes.
///
/// ```
/// use pigeonhole_shm::Generation;
///
/// assert!(Generation(1) < Generation(2));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation(pub u64);

/// Region sizing and placement.
///
/// ```
/// use pigeonhole_shm::ShmConfig;
///
/// let config = ShmConfig::new(8);
/// assert_eq!(config.shards, 8);
/// assert_eq!(config.arena_bytes, 64 << 20);
/// assert_eq!(config.reader_slots, 126);
/// assert_eq!(config.view_buffer_bytes, 4 << 20);
/// assert_eq!(config.first_seqno, 1);
/// assert!(config.dir.is_none());
/// assert!(config.validate().is_ok());
/// assert!(ShmConfig::new(0).validate().is_err());
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ShmConfig {
    /// Shards (arenas and watermark lines).
    pub shards: u32,
    /// Bytes per shard arena (default 64 MiB; rounded up to 2 MiB).
    pub arena_bytes: u64,
    /// Reader slots (default 126).
    pub reader_slots: u32,
    /// Bytes per view buffer (default 4 MiB). Must hold the encoded view: about
    /// `40 + 2 * key bytes` per tablet plus 24 per memtable.
    pub view_buffer_bytes: u32,
    /// Directory for a file-backed region instead of the default memory-backed one.
    pub dir: Option<std::path::PathBuf>,
    /// The first seqno a region built with this config hands out (default 1). A writer
    /// passes the seqno ceiling it recovered, so visible seqnos never go backwards across a
    /// writer restart (ICR 0002). Values below 1 are raised to 1: 0 means "none" in reader
    /// slots.
    pub first_seqno: Seqno,
}

impl ShmConfig {
    /// Defaults for `shards` shards.
    pub fn new(shards: u32) -> Self {
        Self {
            shards,
            arena_bytes: 64 << 20,
            reader_slots: 126,
            view_buffer_bytes: 4 << 20,
            dir: None,
            first_seqno: 1,
        }
    }

    /// Checks the ranges a region can be built from: at least one shard, at least one
    /// reader slot, and a view buffer that holds at least a view header (32 bytes).
    pub fn validate(&self) -> Result<()> {
        if self.shards == 0 {
            return Err(Error::InvalidConfig("shards must be at least 1"));
        }
        if self.reader_slots == 0 {
            return Err(Error::InvalidConfig("reader_slots must be at least 1"));
        }
        if self.view_buffer_bytes < 32 {
            return Err(Error::InvalidConfig(
                "view_buffer_bytes must be at least 32 (one view header)",
            ));
        }
        Ok(())
    }
}

/// The writer lock: the exclusive lock on the writer byte of the lock page. Held for the life
/// of the writer; released on drop.
///
/// ```
/// use std::path::Path;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_io::{OpenOptions, Vfs};
/// use pigeonhole_shm::{Error, WriterLock};
///
/// let vfs = SimVfs::new(1);
/// let file = vfs.open(Path::new("/db/data.phdb"), OpenOptions::read_write_create()).unwrap();
/// let other = vfs.open(Path::new("/db/data.phdb"), OpenOptions::read_write_create()).unwrap();
///
/// let lock = WriterLock::acquire(&file).unwrap();
/// assert!(matches!(WriterLock::acquire(&other), Err(Error::WriterLocked)));
/// drop(lock);
/// assert!(WriterLock::acquire(&other).is_ok());
/// ```
#[derive(Debug)]
pub struct WriterLock {
    file: FileRef,
}

/// The shared presence lock every open process holds on the presence byte.
///
/// ```
/// use std::path::Path;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_io::{OpenOptions, Vfs};
/// use pigeonhole_shm::Presence;
///
/// let vfs = SimVfs::new(1);
/// let file = vfs.open(Path::new("/db/data.phdb"), OpenOptions::read_write_create()).unwrap();
/// let other = vfs.open(Path::new("/db/data.phdb"), OpenOptions::read_write_create()).unwrap();
///
/// let mine = Presence::acquire(&file).unwrap();
/// let theirs = Presence::acquire(&other).unwrap();
/// assert!(!mine.try_become_last().unwrap(), "another process is present");
/// drop(theirs);
/// assert!(mine.try_become_last().unwrap(), "now the last one: clean up");
/// ```
#[derive(Debug)]
pub struct Presence {
    file: FileRef,
}

/// How the calling process uses the region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The writer: creates the region if absent, rebuilds it under a new generation if it
    /// was left by a dead writer.
    Writer,
    /// A reader: attaches to an existing region; waits for `state == ready`.
    Reader,
}

/// One reader process's slot. Released on drop.
///
/// ```
/// use pigeonhole_io::ProcessId;
/// use pigeonhole_shm::{ShmConfig, ShmRegion};
///
/// let mut config = ShmConfig::new(1);
/// config.arena_bytes = 2 << 20;
/// config.reader_slots = 1;
/// let shm = ShmRegion::in_memory([1; 16], &config);
/// let me = ProcessId { pid: 7, start_time: 1 };
///
/// let slot = shm.claim_reader_slot(me).unwrap();
/// assert_eq!(slot.index(), 0);
/// assert!(shm.claim_reader_slot(me).is_err(), "one slot, already taken");
/// assert_eq!(slot.pin(10, 0), (10, 0));
/// assert_eq!(shm.oldest_reader_pin(), Some((10, 0)));
/// slot.unpin();
/// assert_eq!(shm.oldest_reader_pin(), None);
/// ```
#[derive(Debug)]
pub struct ReaderSlot {
    region: Arc<region::Inner>,
    index: u32,
}
