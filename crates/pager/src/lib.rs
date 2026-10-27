//! Page file for Pigeonhole: superblock pair, extent allocator, epoch-deferred freeing.
//!
//! The pager owns the main file's space, not its contents. It hands out power-of-two
//! [`Extent`]s, flips the superblock to publish a new root, and frees extents only when the
//! engine says no live view can reach them.
//!
//! The free-space bitmap is not persisted (decision D8): at open the engine reads the manifest
//! and passes every live extent to [`OpenedPager::finish`]; everything else is free. A crash
//! therefore never leaks space, and no bitmap can disagree with the manifest.
//!
//! The in-memory mock for the layers above is simply a `Pager` over
//! [`SimVfs`](pigeonhole_io::sim::SimVfs) ([`Pager::create`] with any path).
//!
//! # Threading
//!
//! `Pager` is shared by every shard (`&self` methods, internally synchronized). The calls
//! differ in what they may block on:
//!
//! - [`Pager::allocate`], [`Pager::abandon`], [`Pager::retire`], [`Pager::reclaim`],
//!   [`Pager::stats`], [`Pager::shrink_plan`] and [`Pager::clear_for`] only touch
//!   memory, except that `allocate` preallocates more file. Ahead of need, once no free
//!   block of the largest class (64 MiB) is left: one `fallocate` on the caller's thread,
//!   outside the allocator lock, with its `sync_all` submitted to the I/O backend. Inline,
//!   only when no free extent fits (a growth ahead still in flight, or a file under
//!   128 MiB): one `fallocate` and `sync_all` under the allocator lock. They run per flush
//!   or compaction output, never per write.
//! - [`Pager::read`] and [`Pager::write`] are positional I/O on the caller's thread.
//! - [`Pager::commit_root`] and [`Pager::mark_clean`] block on two fsyncs. They must not run
//!   on a shard's foreground loop (decision D30): the manifest task uses
//!   [`Pager::submit_commit_root`], whose fsyncs run on the I/O backend. Root commits must
//!   not overlap; an overlapping commit fails instead of racing.
//! - [`Pager::relocate`], [`Pager::relocate_below`], [`Pager::relocate_into`] and
//!   [`Pager::truncate_tail`] copy or truncate on the caller's thread (online shrink is a
//!   background job).
//!
//! # Example
//!
//! ```
//! use std::path::Path;
//! use pigeonhole_io::VfsRef;
//! use pigeonhole_io::sim::SimVfs;
//! use pigeonhole_pager::{Pager, Root};
//!
//! # fn main() -> pigeonhole_pager::Result<()> {
//! let vfs: VfsRef = SimVfs::new(7);
//! let path = Path::new("/db/data.phdb");
//! let pager = Pager::create(&vfs, path)?;
//!
//! // Write a "manifest snapshot" into a fresh extent and publish it.
//! let snapshot = pager.allocate(100)?;
//! pager.write(snapshot, 0, b"manifest bytes")?;
//! let root = Root { snapshot: Some(snapshot), snapshot_len: 14, manifest_version: 1, ..Root::default() };
//! pager.commit_root(root)?;
//! drop(pager);
//!
//! // Reopen: read the root, read the manifest, then name every live extent.
//! let opened = Pager::open(&vfs, path, true)?;
//! assert_eq!(opened.root(), root);
//! let pager = opened.finish([snapshot])?;
//! let mut buf = [0u8; 14];
//! pager.read(snapshot, 0, &mut buf)?;
//! assert_eq!(&buf, b"manifest bytes");
//! # Ok(())
//! # }
//! ```
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod alloc;

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hasher;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, TryLockError};

use pigeonhole_format::superblock::{
    EXTENSION_AREA, Features, SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock,
};
use pigeonhole_format::{FormatVersion, ManifestVersion, PAGE_SIZE};
use pigeonhole_io::{Completion, ErrorKind, FileRef, IoBuf, OpenOptions, VfsRef};

use crate::alloc::{Alloc, LoadError, UNIT_BYTES, UNIT_PAGES};

/// A region [`Pager::clear_for`] holds for a larger extent, and the live extents to move
/// out of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Clearing {
    /// The region, as an extent of the class it is cleared for.
    pub region: Extent,
    /// The region's live extents, largest first.
    pub occupants: Vec<Extent>,
}

/// An allocated extent: `64 KiB << size_class` bytes at `page`. The persisted form.
pub use pigeonhole_format::superblock::ExtentRef as Extent;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Pager errors.
///
/// ```
/// use pigeonhole_io::VfsRef;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_pager::{Error, Pager};
///
/// let vfs: VfsRef = SimVfs::new(1);
/// let pager = Pager::create(&vfs, "/db/data.phdb".as_ref()).unwrap();
/// assert!(matches!(pager.allocate(65 << 20), Err(Error::TooLarge)));
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The underlying file failed.
    Io(pigeonhole_io::Error),
    /// Neither superblock is valid, or a structure failed to decode.
    Format(pigeonhole_format::Error),
    /// No free extent of the requested size and the file cannot grow.
    NoSpace,
    /// The request is larger than the largest extent (64 MiB).
    TooLarge,
    /// The file was created by a newer, incompatible format.
    UnsupportedVersion(u32),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "page file I/O: {e}"),
            Error::Format(e) => write!(f, "page file format: {e}"),
            Error::NoSpace => f.write_str("no free extent and the file cannot grow"),
            Error::TooLarge => f.write_str("extent request larger than 64 MiB"),
            Error::UnsupportedVersion(v) => write!(f, "unsupported page file format version {v}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Format(e) => Some(e),
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
    fn from(e: pigeonhole_format::Error) -> Self {
        match e {
            pigeonhole_format::Error::UnsupportedVersion { found, .. } => {
                Self::UnsupportedVersion(found)
            }
            e => Self::Format(e),
        }
    }
}

fn io_err(kind: ErrorKind, context: &'static str) -> Error {
    Error::Io(pigeonhole_io::Error::new(kind, context))
}

/// What the superblock points at: the manifest snapshot block and the live part of the
/// delta log (decision D7).
///
/// ```
/// use pigeonhole_pager::{Extent, Root};
///
/// let empty = Root::default();
/// assert_eq!(empty.snapshot, None);
/// let root = Root {
///     snapshot: Some(Extent { page: 16, size_class: 0 }),
///     snapshot_len: 512,
///     manifest_version: 3,
///     ..Root::default()
/// };
/// assert_ne!(root, empty);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Root {
    /// Extent holding the snapshot block; `None` for an empty database.
    pub snapshot: Option<Extent>,
    /// Length of the snapshot block.
    pub snapshot_len: u32,
    /// Extent holding the delta log, if any delta follows the snapshot.
    pub log: Option<Extent>,
    /// Live bytes of the delta log.
    pub log_len: u32,
    /// Manifest version of this root.
    pub manifest_version: ManifestVersion,
}

impl Root {
    fn from_superblock(sb: &Superblock) -> Self {
        Self {
            snapshot: sb.snapshot,
            snapshot_len: sb.snapshot_len,
            log: sb.log,
            log_len: sb.log_len,
            manifest_version: sb.manifest_version,
        }
    }
}

/// Flag bit 0 of the superblock: the last writer closed cleanly.
const FLAG_CLEAN: u64 = 1;

/// A pager that has read its superblock but does not yet know which extents are live.
///
/// ```
/// use pigeonhole_io::VfsRef;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_pager::{Pager, Root};
///
/// # fn main() -> pigeonhole_pager::Result<()> {
/// let vfs: VfsRef = SimVfs::new(3);
/// let path = "/db/data.phdb".as_ref();
/// let id = Pager::create(&vfs, path)?.db_id();
///
/// let opened = Pager::open(&vfs, path, false)?;
/// assert_eq!(opened.db_id(), id);
/// assert_eq!(opened.root(), Root::default());
/// // The engine reads the manifest through `opened.file()` here, then names live extents.
/// let pager = opened.finish([])?;
/// assert_eq!(pager.stats().allocated_bytes, 0);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct OpenedPager {
    file: FileRef,
    writable: bool,
    superblock: Superblock,
    slot: u64,
    /// Either superblock page holds a valid superblock of an older format version, or the
    /// two valid pages' feature sets differ: `finish` rewrites both.
    older: bool,
    /// The current page's extension area.
    extension: Extension,
}

impl OpenedPager {
    /// The current root, from the newest valid superblock.
    pub fn root(&self) -> Root {
        Root::from_superblock(&self.superblock)
    }

    /// The database id.
    pub fn db_id(&self) -> [u8; 16] {
        self.superblock.db_id
    }

    /// Whether the last writer closed cleanly. The flag stays set until the next root
    /// commit, which clears it.
    pub fn clean_shutdown(&self) -> bool {
        self.superblock.flags & FLAG_CLEAN != 0
    }

    /// The file's feature sets, from the current superblock (D269).
    pub fn features(&self) -> Features {
        self.superblock.features
    }

    /// Turns a writable open into a read-only one after the manifest was read (a writer
    /// process that may not write this file, ICR 0059): `finish` then neither syncs nor
    /// upgrades, and the pager refuses root commits. The file handle stays as opened.
    pub fn into_read_only(mut self) -> Self {
        self.writable = false;
        self
    }

    /// The file, for reading the manifest before `finish`.
    pub fn file(&self) -> &FileRef {
        &self.file
    }

    /// Marks `live` extents allocated (everything else is free) and returns a usable pager.
    /// `live` must include the manifest snapshot and log extents, every SST and every blob
    /// extent. Exact duplicates are accepted (tablets may share an SST after a split);
    /// overlapping, misaligned or out-of-file extents fail with [`Error::Format`]. A writable
    /// pager first makes the file's current length durable (`sync_all`).
    ///
    /// If either superblock page holds an older format version, a writable pager then commits
    /// the current root twice, so both pages carry [`FormatVersion::CURRENT`] before its
    /// caller writes anything an older build could not read (ICR 0046): an older build
    /// refuses the file with an unsupported-version error instead of falling back to the
    /// page it can still decode. A crash between the two commits leaves one older page
    /// naming the same root, and the next writable open finishes the upgrade. Two pages
    /// whose feature sets differ (a crash inside [`Pager::set_features`]) are rewritten the
    /// same way, with the current page's sets.
    pub fn finish(self, live: impl IntoIterator<Item = Extent>) -> Result<Pager> {
        let upgrade = self.writable && self.older;
        let pager = self.load(live)?;
        if upgrade {
            let root = pager.root();
            pager.commit_root(root)?;
            pager.commit_root(root)?;
        }
        Ok(pager)
    }

    fn load(self, live: impl IntoIterator<Item = Extent>) -> Result<Pager> {
        // The allocator is sized from the length the page cache shows, which a process that
        // died between a growth's `fallocate` and its `sync_all` left longer than the disk's.
        // Make it durable before handing out that tail: root commits sync with `sync_data`.
        // Skipped when the file is exactly as long as the last committed root recorded and
        // that root came from a clean close (#158, D203): that length was made durable by the
        // growth or truncation that set it (each syncs before it is used) or by an earlier
        // open. A length that differs either way (a growth or a truncation interrupted by a
        // crash) is synced as before. A clean flag left stale by a crash in a session that
        // committed nothing is harmless here: such a session neither grew nor truncated
        // durably past the recorded length without the length differing.
        let recorded = self.superblock.file_pages * PAGE_SIZE as u64;
        let settled = self.clean_shutdown() && self.file.len()? == recorded;
        if self.writable && !settled {
            self.file.sync_all()?;
        }
        let frontier = self.file.len()?.div_ceil(UNIT_BYTES);
        let alloc = Alloc::load(frontier, live).map_err(|e| {
            let what = match e {
                LoadError::Invalid => "live extent is not a valid extent",
                LoadError::Overlap => "live extents overlap",
                LoadError::PastEnd => "live extent past the end of the file",
            };
            Error::Format(pigeonhole_format::Error::Corrupt { what })
        })?;
        let frontier = alloc.frontier();
        Ok(Pager {
            inner: Arc::new(Inner {
                file: self.file,
                data_file: OnceLock::new(),
                writable: self.writable,
                db_id: self.superblock.db_id,
                alloc: Mutex::new(alloc),
                state: Mutex::new(CommitState {
                    root: Root::from_superblock(&self.superblock),
                    sequence: self.superblock.sequence,
                    slot: self.slot,
                    version: self.superblock.version,
                    features: self.superblock.features,
                    extension: self.extension,
                    poisoned: false,
                }),
                committing: AtomicBool::new(false),
                growths: AtomicU64::new(0),
                growth_nanos: AtomicU64::new(0),
                ahead: AtomicBool::new(false),
                ahead_flight: Mutex::new(None),
                growths_ahead: AtomicU64::new(0),
                ahead_nanos: AtomicU64::new(0),
                side: SideSyncs::default(),
                frontier: AtomicU64::new(frontier),
            }),
        })
    }
}

/// The main page file. Shared by every shard (`&self` methods, internally synchronized;
/// allocation is per flush or compaction output, never per write). See the crate docs'
/// *Threading* section for what each call may block on.
///
/// Space is handed out as power-of-two [`Extent`]s from an in-memory buddy allocator
/// (lowest address first within the smallest fitting size). An extent is freed either at
/// once ([`abandon`](Pager::abandon), for output never published) or after epoch-deferred
/// reclamation ([`retire`](Pager::retire) then [`reclaim`](Pager::reclaim)), so a view that
/// can still reach an extent never sees it reused.
///
/// ```
/// use pigeonhole_io::VfsRef;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_pager::{Pager, Root};
///
/// # fn main() -> pigeonhole_pager::Result<()> {
/// let vfs: VfsRef = SimVfs::new(9);
/// let pager = Pager::create(&vfs, "/db/data.phdb".as_ref())?;
/// let old = pager.allocate(64 << 10)?;
/// pager.commit_root(Root { snapshot: Some(old), manifest_version: 1, ..Root::default() })?;
///
/// // Version 2 no longer references `old`; a snapshot of version 1 may still read it.
/// let new = pager.allocate(64 << 10)?;
/// pager.commit_root(Root { snapshot: Some(new), manifest_version: 2, ..Root::default() })?;
/// pager.retire(old, 2);
/// assert_eq!(pager.reclaim(1), 0); // a view at version 1 is still live
/// assert_eq!(pager.reclaim(2), 1); // every live view is at version 2 or later
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Pager {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    file: FileRef,
    /// A second handle on the same file for SST and blob extents, opened for direct I/O
    /// (#403); set once at open, `file` otherwise.
    data_file: OnceLock<FileRef>,
    writable: bool,
    db_id: [u8; 16],
    alloc: Mutex<Alloc>,
    state: Mutex<CommitState>,
    /// Set while a root commit runs; overlapping commits are refused.
    committing: AtomicBool,
    /// File growths, and the nanoseconds they held the allocator (ICR 0015).
    growths: AtomicU64,
    growth_nanos: AtomicU64,
    /// Set while a growth ahead of need is in flight (one at a time).
    ahead: AtomicBool,
    /// That growth's sync, with its continuation, until a blocking commit takes it to wait
    /// on: the wait drives I/O only its thread or a waiter completes (an io_uring ring the
    /// committing thread owns, the simulator's deferred device), which the side syncs'
    /// condvar alone would wait for for ever.
    ahead_flight: Mutex<Option<Completion<()>>>,
    /// Growths ahead of need that completed, and the nanoseconds their `fallocate` took
    /// on the allocating thread.
    growths_ahead: AtomicU64,
    ahead_nanos: AtomicU64,
    /// Growth and truncation syncs in flight, which a commit must see the outcome of before
    /// it publishes (D58), without holding the allocator lock (#182).
    side: SideSyncs,
    /// The allocator's frontier (units), kept beside it so a commit reads it for the
    /// superblock without taking the allocator lock, which a growth holds across its sync
    /// (#182). Updated under that lock whenever the frontier moves.
    frontier: AtomicU64,
}

/// Commit steps a finished side sync resumes; [`Parked::run`] them with no pager lock held.
#[must_use]
struct Parked(Vec<Box<dyn FnOnce() + Send>>);

impl Parked {
    fn run(self) {
        for step in self.0 {
            step();
        }
    }
}

impl fmt::Debug for SideSyncs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SideSyncs")
            .field("in_flight", &lock(&self.state).in_flight)
            .finish_non_exhaustive()
    }
}

/// How often a blocking commit waiting for side syncs looks again for a growth's sync to
/// drive ([`Inner::wait_side_idle`]).
const SIDE_RECHECK: std::time::Duration = std::time::Duration::from_millis(1);

/// The growths' and truncations' `sync_all`s in flight (#182). A commit's sync may succeed
/// only because one of them was handed the error for the same pages (D58), so before a
/// commit publishes, every side sync that may have overlapped it must have finished: a
/// failed one poisons the pager first. A submitted commit never waits for that on the I/O
/// backend's thread: it parks its next step, and the side sync that finishes last
/// resubmits it.
#[derive(Default)]
struct SideSyncs {
    state: Mutex<SideState>,
    /// Signalled when the count reaches zero (for a blocking commit).
    idle: Condvar,
}

#[derive(Default)]
struct SideState {
    in_flight: u32,
    /// Commits' next steps, parked until no side sync is in flight.
    parked: Vec<Box<dyn FnOnce() + Send>>,
}

impl SideSyncs {
    /// Counts a side sync in flight until [`SideSyncs::finish`].
    fn start(&self) {
        lock(&self.state).in_flight += 1;
    }

    /// Ends a side sync: `poisoned` has already been set if it failed. Returns the parked
    /// commit steps when it was the last one in flight, for the caller to run once it holds
    /// no pager lock (a step can end its commit, which takes the allocator lock). Their steps
    /// only submit I/O, so the thread running them (an allocating shard or compaction
    /// thread) does not wait for it.
    fn finish(&self) -> Parked {
        let mut st = lock(&self.state);
        st.in_flight -= 1;
        if st.in_flight != 0 {
            return Parked(Vec::new());
        }
        let parked = std::mem::take(&mut st.parked);
        drop(st);
        self.idle.notify_all();
        Parked(parked)
    }

    /// Runs `step` now if no side sync is in flight, else when the last one finishes.
    fn after(&self, step: impl FnOnce() + Send + 'static) {
        let mut st = lock(&self.state);
        if st.in_flight != 0 {
            st.parked.push(Box::new(step));
            return;
        }
        drop(st);
        step();
    }

    /// Waits until no side sync is in flight, or `timeout` passes; returns whether none is.
    fn wait_idle_for(&self, timeout: std::time::Duration) -> bool {
        let st = lock(&self.state);
        let (st, _) = self
            .idle
            .wait_timeout_while(st, timeout, |st| st.in_flight != 0)
            .unwrap_or_else(PoisonError::into_inner);
        st.in_flight == 0
    }
}

#[derive(Debug)]
struct CommitState {
    /// The root most recently committed (or reloaded).
    root: Root,
    /// Sequence of the current superblock.
    sequence: u64,
    /// Page of the current superblock; the next commit writes the other one.
    slot: u64,
    /// Format version of the current superblock: older until a writable pager's first
    /// commit (which `finish` makes at once for an older file), then the current one.
    version: FormatVersion,
    /// The file's feature sets (format 4, D269), carried into every root commit: compat
    /// bits a newer build set are kept, not cleared.
    features: Features,
    /// The extension area, carried likewise (D269).
    extension: Extension,
    /// A sync of the file failed: a commit's (the on-disk root is uncertain) or a growth's
    /// or truncation's. An fsync error may have dropped written data, and on Linux it is
    /// reported to one sync only, so a later sync succeeding proves nothing: no further
    /// commit is attempted (decision D58). Set under the allocator lock when a growth or
    /// truncation sync fails; see `Inner::check_syncs`.
    poisoned: bool,
}

/// A root commit in flight: the encoded superblock and where it goes.
struct PendingCommit {
    page: Box<[u8; PAGE_SIZE]>,
    slot: u64,
    sequence: u64,
    root: Root,
    /// The feature sets the page carries: the current ones once it is durable.
    features: Features,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The guard, or `None` while another thread holds `m`.
fn try_lock<T>(m: &Mutex<T>) -> Option<MutexGuard<'_, T>> {
    match m.try_lock() {
        Ok(g) => Some(g),
        Err(TryLockError::Poisoned(p)) => Some(p.into_inner()),
        Err(TryLockError::WouldBlock) => None,
    }
}

/// The smallest size class holding `bytes`.
fn class_for(bytes: u64) -> Result<u8> {
    let class = (0..=Extent::MAX_CLASS).find(|&c| UNIT_BYTES << c >= bytes);
    class.ok_or(Error::TooLarge)
}

fn poisoned_error() -> pigeonhole_io::Error {
    pigeonhole_io::Error::new(
        ErrorKind::Other,
        "an earlier root commit failed, or a sync of the file did; reopen the database",
    )
}

fn grow_error(e: pigeonhole_io::Error) -> Error {
    match e.kind {
        ErrorKind::NoSpace => Error::NoSpace,
        _ => Error::Io(e),
    }
}

/// A fresh database id: [`Vfs::random_u64`](pigeonhole_io::Vfs::random_u64) mixed with the
/// path, so a simulated `Vfs` replays the same id from its seed.
fn random_db_id(vfs: &VfsRef, path: &Path) -> [u8; 16] {
    let mut id = [0u8; 16];
    let (halves, _) = id.as_chunks_mut::<8>();
    for (i, half) in halves.iter_mut().enumerate() {
        let mut h = DefaultHasher::new();
        h.write_u64(vfs.random_u64());
        h.write(path.as_os_str().as_encoded_bytes());
        h.write_usize(i);
        *half = h.finish().to_le_bytes();
    }
    id
}

/// Reads one superblock slot. A short file reads as a truncated (invalid) superblock.
fn read_slot(
    file: &FileRef,
    slot: u64,
    page: &mut [u8; PAGE_SIZE],
) -> Result<pigeonhole_format::Result<Superblock>> {
    match file.read_at(page, slot * PAGE_SIZE as u64) {
        Ok(()) => Ok(Superblock::decode(page)),
        Err(e) if e.kind == ErrorKind::UnexpectedEof => {
            Ok(Err(pigeonhole_format::Error::Truncated {
                what: "superblock",
            }))
        }
        Err(e) => Err(e.into()),
    }
}

/// The current superblock and its page, and whether either page holds a valid superblock of
/// an older format version.
fn read_superblocks(file: &FileRef) -> Result<(Superblock, u64, bool)> {
    read_superblocks_with(file).map(|(sb, slot, older, _)| (sb, slot, older))
}

/// As [`read_superblocks`], with the current page's extension area (zero before format 4).
fn read_superblocks_with(file: &FileRef) -> Result<(Superblock, u64, bool, Extension)> {
    let mut pages = [[0u8; PAGE_SIZE]; 2];
    let [page_a, page_b] = &mut pages;
    let a = read_slot(file, SUPERBLOCK_PAGE_A, page_a)?;
    let b = read_slot(file, SUPERBLOCK_PAGE_B, page_b)?;
    // A page of an older format, or two pages whose feature sets differ (a crash between a
    // feature change's two commits, `Pager::set_features`): a writable open rewrites both.
    let older = [&a, &b]
        .iter()
        .any(|r| matches!(r, Ok(sb) if sb.version < FormatVersion::CURRENT))
        || matches!((&a, &b), (Ok(x), Ok(y)) if x.features != y.features);
    let (sb, slot) = Superblock::choose(a, b)?;
    let mut extension = Extension::default();
    if sb.version >= FormatVersion(4) {
        let page = &pages[usize::from(slot == SUPERBLOCK_PAGE_B)];
        extension.0.copy_from_slice(&page[EXTENSION_AREA]);
    }
    Ok((sb, slot, older, extension))
}

/// The superblock's extension area (FORMAT §8.2, D269), carried unchanged into every root
/// commit so a later 1.x feature's data there survives a build that does not know it.
#[derive(Clone)]
struct Extension(Box<[u8; EXTENSION_LEN]>);

const EXTENSION_LEN: usize = EXTENSION_AREA.end - EXTENSION_AREA.start;

impl Default for Extension {
    fn default() -> Self {
        Self(Box::new([0; EXTENSION_LEN]))
    }
}

impl fmt::Debug for Extension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let used = self.0.iter().filter(|&&b| b != 0).count();
        write!(f, "Extension({used} nonzero bytes)")
    }
}

impl Inner {
    /// Validates and encodes a commit of `root` and claims the commit slot.
    fn begin(&self, root: Root, clean: bool) -> pigeonhole_io::Result<PendingCommit> {
        self.begin_with(Some(root), clean, None)
    }

    /// [`Inner::begin`], with `root` and `features` defaulting to the current ones, read
    /// under the state lock as the commit slot is claimed (a feature change commits the root
    /// that is current then, never one read earlier; perf287's S1 on #858).
    fn begin_with(
        &self,
        root: Option<Root>,
        clean: bool,
        features: Option<Features>,
    ) -> pigeonhole_io::Result<PendingCommit> {
        use pigeonhole_io::Error as IoError;
        if !self.writable {
            return Err(IoError::new(
                ErrorKind::Unsupported,
                "root commit on a read-only pager",
            ));
        }
        if self.committing.swap(true, Ordering::AcqRel) {
            return Err(IoError::new(ErrorKind::Other, "root commits overlap"));
        }
        let st = lock(&self.state);
        if st.poisoned {
            drop(st);
            self.committing.store(false, Ordering::Release);
            return Err(poisoned_error());
        }
        let sequence = st.sequence + 1;
        let slot = st.slot ^ 1;
        let root = root.unwrap_or(st.root);
        let features = features.unwrap_or(st.features);
        let file_pages = self.frontier.load(Ordering::Acquire) * UNIT_PAGES;
        let sb = Superblock {
            version: FormatVersion::CURRENT,
            page_size: PAGE_SIZE as u32,
            sequence,
            db_id: self.db_id,
            snapshot: root.snapshot,
            snapshot_len: root.snapshot_len,
            log: root.log,
            log_len: root.log_len,
            manifest_version: root.manifest_version,
            file_pages,
            flags: if clean { FLAG_CLEAN } else { 0 },
            features,
        };
        let mut page = Box::new([0u8; PAGE_SIZE]);
        sb.encode(&mut page);
        page[EXTENSION_AREA].copy_from_slice(&st.extension.0[..]);
        drop(st);
        Ok(PendingCommit {
            page,
            slot,
            sequence,
            root,
            features,
        })
    }

    /// Waits until no side sync is in flight (a blocking commit, on its caller's thread),
    /// driving a growth ahead's sync if no other thread has taken it. A growth stores its
    /// sync just after submitting it, so the wait looks for it again while it waits.
    fn wait_side_idle(&self) {
        loop {
            let flight = lock(&self.ahead_flight).take();
            if let Some(c) = flight {
                // Its outcome reaches the pager through the continuation (a failure poisons).
                let _ = c.wait();
            }
            if self.side.wait_idle_for(SIDE_RECHECK) {
                return;
            }
        }
    }

    /// Fails if a growth's or truncation's sync failed. A commit calls it after each of its
    /// own syncs succeeded, once no side sync is in flight ([`SideSyncs`]): that sync may
    /// have succeeded only because a concurrent one was handed the error for the same pages,
    /// and a failed side sync poisons before it stops counting as in flight.
    fn check_syncs(&self) -> pigeonhole_io::Result<()> {
        if lock(&self.state).poisoned {
            return Err(poisoned_error());
        }
        Ok(())
    }

    /// Writes the superblock once the first sync succeeded and no other sync failed
    /// (blocking: waits for side syncs in flight).
    fn write_superblock(&self, p: &PendingCommit) -> pigeonhole_io::Result<()> {
        self.wait_side_idle();
        self.check_syncs()?;
        self.file.write_at(&p.page[..], p.slot * PAGE_SIZE as u64)
    }

    /// Ends a commit: on success the new root is current; on failure (or if another sync
    /// failed meanwhile) the pager is poisoned. No side sync may be in flight.
    fn end(
        &self,
        p: PendingCommit,
        result: pigeonhole_io::Result<()>,
    ) -> pigeonhole_io::Result<()> {
        let result = result.and_then(|()| self.check_syncs());
        let mut st = lock(&self.state);
        match &result {
            Ok(()) => {
                st.root = p.root;
                st.sequence = p.sequence;
                st.slot = p.slot;
                st.version = FormatVersion::CURRENT;
                st.features = p.features;
            }
            Err(_) => st.poisoned = true,
        }
        drop(st);
        if result.is_ok() {
            // The manifest extents the durable root names may no longer be abandoned.
            let mut alloc = lock(&self.alloc);
            for e in p.root.snapshot.into_iter().chain(p.root.log) {
                alloc.publish(e);
            }
        }
        self.committing.store(false, Ordering::Release);
        result
    }

    /// Sync, write the non-current superblock, sync; blocking.
    fn commit(&self, root: Root, clean: bool) -> pigeonhole_io::Result<()> {
        let p = self.begin(root, clean)?;
        self.commit_pending(p)
    }

    /// A blocking commit of the current root with `features` ([`Pager::set_features`]):
    /// the root and sets are taken as the commit slot is claimed, and `features` becomes
    /// current only once the page is durable.
    fn commit_features(&self, features: Features) -> pigeonhole_io::Result<()> {
        let p = self.begin_with(None, false, Some(features))?;
        self.commit_pending(p)
    }

    fn commit_pending(&self, p: PendingCommit) -> pigeonhole_io::Result<()> {
        let result = self
            .file
            .sync_data()
            .and_then(|()| self.write_superblock(&p))
            .and_then(|()| self.file.sync_data());
        self.wait_side_idle();
        self.end(p, result)
    }

    /// The same steps, all submitted to the I/O backend: sync, superblock write, sync. No
    /// step waits on the thread that resolves the one before (#182): one that must wait for a
    /// side sync in flight is parked ([`SideSyncs::after`]) and submitted when it finishes.
    fn submit_commit(self: Arc<Self>, root: Root) -> Completion<()> {
        let p = match self.begin(root, false) {
            Ok(p) => p,
            Err(e) => return Completion::ready(Err(e)),
        };
        let (done, resolver) = Completion::pair();
        let first = self.file.submit_sync_data();
        // The continuations' own completions are not needed: `resolver` reports the outcome.
        let _chained = first.map(move |synced| {
            let inner = Arc::clone(&self);
            self.side
                .after(move || inner.submit_superblock(p, synced, resolver));
            Ok(())
        });
        done
    }

    /// A submitted commit's second step, with no side sync in flight: writes the superblock
    /// (if the first sync and every side sync succeeded) and submits the second sync.
    fn submit_superblock(
        self: Arc<Self>,
        p: PendingCommit,
        synced: pigeonhole_io::Result<()>,
        resolver: pigeonhole_io::Resolver<()>,
    ) {
        if let Err(e) = synced.and_then(|()| self.check_syncs()) {
            resolver.resolve(self.end(p, Err(e)));
            return;
        }
        let mut page = IoBuf::zeroed(PAGE_SIZE);
        page.copy_from_slice(&p.page[..]);
        let written = self.file.submit_write(page, p.slot * PAGE_SIZE as u64);
        let _chained = written.map(move |r| {
            let r = r.map(drop);
            if let Err(e) = r {
                resolver.resolve(self.end(p, Err(e)));
                return Ok(());
            }
            let second = self.file.submit_sync_data();
            let _chained = second.map(move |synced| {
                let inner = Arc::clone(&self);
                self.side
                    .after(move || resolver.resolve(inner.end(p, synced)));
                Ok(())
            });
            Ok(())
        });
    }
}

impl Pager {
    /// Creates a new database file at `path` with an empty root (`Root::default()`) and a
    /// fresh random db id. Fails if the file exists.
    ///
    /// The file is usable once this returns (superblock A written and synced, directory
    /// entry synced). A crash during `create` can leave a file without a valid superblock;
    /// [`Pager::open`] then fails with [`Error::Format`], and the caller may remove it and
    /// create again since nothing was ever committed to it.
    pub fn create(vfs: &VfsRef, path: &Path) -> Result<Pager> {
        let mut opts = OpenOptions::read_write_create();
        opts.create_new = true;
        let file = vfs.open(path, opts)?;
        file.set_len(UNIT_BYTES).map_err(grow_error)?;
        let superblock = Superblock {
            version: FormatVersion::CURRENT,
            page_size: PAGE_SIZE as u32,
            sequence: 1,
            db_id: random_db_id(vfs, path),
            snapshot: None,
            snapshot_len: 0,
            log: None,
            log_len: 0,
            manifest_version: 0,
            file_pages: UNIT_PAGES,
            flags: 0,
            features: Default::default(),
        };
        let mut page = [0u8; PAGE_SIZE];
        superblock.encode(&mut page);
        file.write_at(&page, SUPERBLOCK_PAGE_A * PAGE_SIZE as u64)?;
        file.sync_all()?;
        let dir = match path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        vfs.sync_dir(dir)?;
        OpenedPager {
            file,
            writable: true,
            superblock,
            slot: SUPERBLOCK_PAGE_A,
            older: false,
            extension: Extension::default(),
        }
        .finish([])
    }

    /// Opens an existing file and reads both superblocks. No recovery scan. A read-only pager
    /// (reader processes) never allocates, retires or commits; it only reads and reloads.
    pub fn open(vfs: &VfsRef, path: &Path, writable: bool) -> Result<OpenedPager> {
        let mut opts = OpenOptions::read();
        opts.write = writable;
        let file = vfs.open(path, opts)?;
        let (superblock, slot, older, extension) = read_superblocks_with(&file)?;
        // `choose` refused unknown incompatible features; a writer also refuses unknown
        // read-only-compatible ones (a reader process may read the file).
        if writable {
            superblock.features.check_write()?;
        }
        Ok(OpenedPager {
            file,
            writable,
            superblock,
            slot,
            older,
            extension,
        })
    }

    /// The file handle: superblocks, the manifest, and the copies `shrink` and `backup` make.
    pub fn file(&self) -> &FileRef {
        &self.inner.file
    }

    /// The handle SST and blob extents are read and written through: the direct-I/O handle
    /// set by [`Pager::set_data_file`] (#403), or [`Pager::file`].
    pub fn data_file(&self) -> &FileRef {
        self.inner.data_file.get().unwrap_or(&self.inner.file)
    }

    /// Sets the handle for SST and blob extents: a second handle on the same file, opened
    /// for direct I/O (#403). Only the first call takes effect; call it at open, before any
    /// extent is read or written.
    pub fn set_data_file(&self, file: FileRef) {
        let _ = self.inner.data_file.set(file);
    }

    /// The database id.
    pub fn db_id(&self) -> [u8; 16] {
        self.inner.db_id
    }

    /// The root most recently committed.
    pub fn root(&self) -> Root {
        lock(&self.inner.state).root
    }

    /// The file's feature sets (D269): as of the last commit, or the last reload for a
    /// read-only pager.
    pub fn features(&self) -> Features {
        lock(&self.inner.state).features
    }

    /// The current superblock's format version: [`FormatVersion::CURRENT`] once a writable
    /// pager has committed (an older file is upgraded at `finish`), the file's own on a
    /// read-only pager, which never upgrades.
    pub fn format_version(&self) -> FormatVersion {
        lock(&self.inner.state).version
    }

    /// Replaces the file's feature sets (D269; FORMAT §8.2), committing twice so both
    /// superblock pages carry them before this returns. Call it before the first write that
    /// uses a newly set feature, and after the last write that used a cleared one: then no
    /// page a build may choose disagrees with what the file holds. Each commit names the
    /// root current when it starts, so a root committed between them is kept, and the new
    /// sets become current only once the first page is durable: a failed commit leaves them
    /// as they were (and poisons the pager, as any failed commit does). A crash between the
    /// two commits leaves the pages disagreeing, and the next writable open rewrites both
    /// with the current page's sets ([`OpenedPager::finish`]).
    ///
    /// The caller serializes it with root commits, as root commits are with each other: an
    /// overlapping one fails ("root commits overlap"). An engine caller (#487 the first)
    /// routes the change through its manifest writer as a request, so it never interleaves
    /// with a manifest commit. Equal sets commit nothing.
    pub fn set_features(&self, features: Features) -> Result<()> {
        if self.features() == features {
            return Ok(());
        }
        self.inner.commit_features(features)?;
        Ok(self.inner.commit_features(features)?)
    }

    /// Allocates the smallest extent of at least `bytes` (64 KiB minimum), growing the file
    /// if needed. Never returns an extent that is allocated or awaiting reclamation.
    pub fn allocate(&self, bytes: u64) -> Result<Extent> {
        self.allocate_from(bytes, false)
    }

    /// As [`Pager::allocate`], from the lowest-addressed free block that fits whatever its
    /// size (splitting a larger one) rather than the smallest free class. The manifest's
    /// snapshot rewrites use it, so the manifest settles low in the file instead of in
    /// whichever small hole is free, which `shrink` relies on to move it out of the way.
    pub fn allocate_lowest(&self, bytes: u64) -> Result<Extent> {
        self.allocate_from(bytes, true)
    }

    fn allocate_from(&self, bytes: u64, lowest: bool) -> Result<Extent> {
        let class = class_for(bytes)?;
        if !self.inner.writable {
            return Err(io_err(
                ErrorKind::Unsupported,
                "allocate on a read-only pager",
            ));
        }
        let mut alloc = lock(&self.inner.alloc);
        let free = if lowest {
            alloc.alloc_lowest(class)
        } else {
            alloc.alloc_free(class)
        };
        if let Some(e) = free {
            self.maybe_grow_ahead(alloc, lowest);
            return Ok(e);
        }
        // The allocator lock is held across this one `fallocate` and `sync_all` (at most
        // 64 MiB, once per file growth) so two growths cannot claim the same tail; other
        // allocations wait. Preallocate rather than extend sparsely: a full disk fails here,
        // as `NoSpace`. The `sync_all` makes the new length durable now, because root commits
        // sync with `sync_data`, which need not persist a length change: without it a
        // power loss after the commit could cut the file short of a published extent.
        let (_, end) = alloc.grow_target(class);
        let from = alloc.frontier() * UNIT_BYTES;
        let started = std::time::Instant::now();
        self.inner
            .file
            .allocate(from, end * UNIT_BYTES - from)
            .map_err(grow_error)?;
        // A failed sync poisons (decision D58): it may have been handed the error for pages
        // a flush wrote, which the next commit's sync would then not report. A commit whose
        // sync overlaps this one waits for its outcome before publishing (#182).
        self.inner.side.start();
        let synced = self.inner.file.sync_all();
        if synced.is_err() {
            lock(&self.inner.state).poisoned = true;
        }
        let parked = self.inner.side.finish();
        let grown = synced.map(|()| alloc.alloc_grown(class));
        self.inner
            .frontier
            .store(alloc.frontier(), Ordering::Release);
        drop(alloc);
        parked.run();
        let grown = grown?;
        self.inner.growths.fetch_add(1, Ordering::Relaxed);
        self.inner.growth_nanos.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.maybe_grow_ahead(lock(&self.inner.alloc), lowest);
        Ok(grown)
    }

    /// After an allocation (not a relocation's `allocate_lowest`, which packs the file
    /// toward its start): grows the file ahead of need if no free block of the largest class
    /// is left ([`Alloc::wants_ahead`]) and no such growth is in flight, so the next large
    /// allocation finds free space instead of growing the file inline, under the allocator
    /// lock, on the thread that needs it.
    ///
    /// The tail is claimed under the lock (placeholders, so nothing is allocated there yet);
    /// the `fallocate` runs on this thread after the lock is dropped, and the `sync_all`
    /// that makes the new length durable is submitted to the I/O backend. Its completion
    /// frees the claimed space, or poisons the pager if it failed (D58); root commits wait
    /// for its outcome as for any side sync (#182). A failed `fallocate` (a full disk) gives
    /// the claim back and reports nothing: the next inline growth reports it.
    fn maybe_grow_ahead(&self, mut alloc: MutexGuard<'_, Alloc>, lowest: bool) {
        if lowest || !alloc.wants_ahead() || self.inner.ahead.swap(true, Ordering::AcqRel) {
            return;
        }
        let (from, end) = alloc.claim_ahead();
        drop(alloc);
        let started = std::time::Instant::now();
        let allocated = self
            .inner
            .file
            .allocate(from * UNIT_BYTES, (end - from) * UNIT_BYTES);
        self.inner.ahead_nanos.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if allocated.is_err() {
            lock(&self.inner.alloc).release_ahead(from, end, false);
            self.inner.ahead.store(false, Ordering::Release);
            return;
        }
        self.inner.side.start();
        let inner = Arc::clone(&self.inner);
        let flight = self.inner.file.submit_sync_all().map(move |synced| {
            let mut alloc = lock(&inner.alloc);
            if synced.is_err() {
                // Before the sync stops counting as in flight, as in `allocate_from`.
                lock(&inner.state).poisoned = true;
            }
            alloc.release_ahead(from, end, synced.is_ok());
            inner.frontier.store(alloc.frontier(), Ordering::Release);
            let parked = inner.side.finish();
            drop(alloc);
            inner.ahead.store(false, Ordering::Release);
            if synced.is_ok() {
                inner.growths_ahead.fetch_add(1, Ordering::Relaxed);
            }
            parked.run();
            Ok(())
        });
        // Kept for a blocking commit to drive (`Inner::wait_side_idle`); dropped once done.
        if !flight.is_ready() {
            *lock(&self.inner.ahead_flight) = Some(flight);
        }
    }

    /// Sets the growth ahead of need ([`Pager::allocate`]): once no free block of at least
    /// `block` bytes (rounded up to a size class) is left and the file is at least `from`
    /// bytes long, an allocation grows the file by such a block outside the allocator lock.
    /// The defaults are 64 MiB from 128 MiB. A test hook: tests on the simulator, which
    /// keeps whole files in memory, set it small so they reach it.
    #[doc(hidden)]
    pub fn set_grow_ahead(&self, block: u64, from: u64) -> Result<()> {
        let class = class_for(block)?;
        lock(&self.inner.alloc).set_ahead(class, from.div_ceil(UNIT_BYTES));
        Ok(())
    }

    /// Returns an extent that was allocated but never published in a root (an abandoned
    /// flush or compaction output). Freed immediately.
    ///
    /// Never call it on a published extent: retire that instead. The pager refuses (and
    /// debug-asserts on) every extent it knows a durable root references — those loaded at
    /// open and the manifest snapshot and log of each committed root — and retired or
    /// unallocated ones. It cannot tell an SST or blob extent published in this session
    /// from pending output (it never reads the manifest), so that part is the caller's
    /// contract.
    pub fn abandon(&self, extent: Extent) {
        let freed = lock(&self.inner.alloc).release_live(extent);
        debug_assert!(
            freed,
            "abandon of an extent that is not pending output: {extent:?}"
        );
    }

    /// Shrinks an extent that was allocated but never published in a root to the smallest
    /// one holding `bytes` (64 KiB minimum), and returns it: the same first page, a size
    /// class no larger. The freed tail is reusable at once. Bytes already written below
    /// `bytes` stay where they are, so a finished flush or compaction output trims to its
    /// length before it is published. Unpublished space is never referenced by a durable
    /// root, so this needs no retirement (D8).
    ///
    /// Returns `extent` unchanged if `bytes` exceeds it. Never call it on a published
    /// extent; as with [`Pager::abandon`], the pager refuses (returns `extent` unchanged,
    /// and debug-asserts) any extent it knows is published, retired or unallocated.
    pub fn trim(&self, extent: Extent, bytes: u64) -> Extent {
        let Ok(class) = class_for(bytes) else {
            return extent;
        };
        if class >= extent.size_class {
            return extent;
        }
        let trimmed = lock(&self.inner.alloc).shrink_live(extent, class);
        debug_assert!(
            trimmed,
            "trim of an extent that is not pending output: {extent:?}"
        );
        if !trimmed {
            return extent;
        }
        Extent {
            page: extent.page,
            size_class: class,
        }
    }

    fn check_range(extent: Extent, offset: u64, len: usize) -> Result<u64> {
        match offset.checked_add(len as u64) {
            Some(end) if end <= extent.len() => Ok(extent.offset() + offset),
            _ => Err(io_err(ErrorKind::Other, "access past the end of an extent")),
        }
    }

    /// Writes `data` at `offset` within `extent`.
    pub fn write(&self, extent: Extent, offset: u64, data: &[u8]) -> Result<()> {
        let at = Self::check_range(extent, offset, data.len())?;
        debug_assert!(
            lock(&self.inner.alloc).is_used(extent),
            "write to an unallocated extent: {extent:?}"
        );
        Ok(self.inner.file.write_at(data, at)?)
    }

    /// Reads `buf.len()` bytes at `offset` within `extent`.
    pub fn read(&self, extent: Extent, offset: u64, buf: &mut [u8]) -> Result<()> {
        let at = Self::check_range(extent, offset, buf.len())?;
        Ok(self.inner.file.read_at(buf, at)?)
    }

    /// Publishes `root`: syncs data written so far, writes the non-current superblock slot
    /// with a higher sequence, and syncs again. The only in-place write of live data in the
    /// main file. When this returns, a crash recovers to `root`; before it returns, to the
    /// previous root or `root`. Blocks; use [`Pager::submit_commit_root`] on a shard thread.
    ///
    /// If a commit fails, or any sync of the file failed before (a growth in
    /// [`Pager::allocate`], a [`Pager::truncate_tail`]), the pager refuses every later commit
    /// (an fsync error may have dropped written data, so retrying could publish a root over
    /// lost bytes); reopen.
    pub fn commit_root(&self, root: Root) -> Result<()> {
        Ok(self.inner.commit(root, false)?)
    }

    /// [`Pager::commit_root`] run by the I/O backend: returns at once, so the manifest task
    /// never blocks a shard's foreground loop on the two fsyncs. Root commits must not
    /// overlap; submit the next only after this one resolves.
    pub fn submit_commit_root(&self, root: Root) -> Completion<()> {
        Arc::clone(&self.inner).submit_commit(root)
    }

    /// Re-reads both superblocks and returns the current root if it changed since the last
    /// call (read-only handles in reader processes, when the shared-memory header names a
    /// newer manifest version). Never writes. A writable pager already knows its root and
    /// returns `None`.
    pub fn reload_root(&self) -> Result<Option<Root>> {
        if self.inner.writable {
            return Ok(None);
        }
        let (sb, slot, _) = read_superblocks(&self.inner.file)?;
        let mut st = lock(&self.inner.state);
        if sb.sequence == st.sequence {
            return Ok(None);
        }
        st.sequence = sb.sequence;
        st.version = sb.version;
        st.slot = slot;
        st.root = Root::from_superblock(&sb);
        st.features = sb.features;
        Ok(Some(st.root))
    }

    /// Marks `extent` unreachable from the root committed at `superseded_at` and every later
    /// one. It is freed by [`Pager::reclaim`] once no view older than `superseded_at` lives.
    pub fn retire(&self, extent: Extent, superseded_at: ManifestVersion) {
        let retired = lock(&self.inner.alloc).retire(extent, superseded_at);
        debug_assert!(retired, "retire of an extent that is not live: {extent:?}");
    }

    /// Frees every retired extent whose `superseded_at <= oldest_live`, where `oldest_live`
    /// is the oldest manifest version any view (in-process or in a reader slot) still uses.
    ///
    /// `oldest_live` is clamped to the manifest version of the last *completed* root commit:
    /// until the root that drops an extent is durable, a crash recovers to a root that still
    /// references it, so it is not freed even if no view uses it (a retire and reclaim may
    /// run while [`Pager::submit_commit_root`] is still in flight).
    pub fn reclaim(&self, oldest_live: ManifestVersion) -> usize {
        let durable = lock(&self.inner.state).root.manifest_version;
        lock(&self.inner.alloc).reclaim(oldest_live.min(durable))
    }

    /// As [`Pager::reclaim`], but never waits: `None` (nothing done) if another thread holds
    /// the allocator or the root state, which it may hold across a growth's `fallocate` and
    /// `sync_all` or a truncation's sync. For reclaims nobody waits on (the last view of
    /// an old version going): the holder's own commit or shrink reclaims anyway.
    pub fn try_reclaim(&self, oldest_live: ManifestVersion) -> Option<usize> {
        let durable = try_lock(&self.inner.state)?.root.manifest_version;
        Some(try_lock(&self.inner.alloc)?.reclaim(oldest_live.min(durable)))
    }

    /// Holds the allocator's lock until the guard drops (test hook: makes
    /// [`Pager::try_reclaim`] skip, as another thread's growth or commit would).
    #[doc(hidden)]
    pub fn hold_alloc_for_test(&self) -> impl Sized + '_ {
        lock(&self.inner.alloc)
    }

    /// Extents past the shrink point that must move before the file can shrink.
    ///
    /// The shrink point is where the live (not retired) extents would end if packed toward
    /// the start of the file, largest first. The plan is best effort: relocating every
    /// extent it names, publishing the moves, retiring and reclaiming the old extents and
    /// then calling [`Pager::truncate_tail`] shrinks the file; if free space below the point
    /// is still held by retired extents, calling it again after they are reclaimed shrinks
    /// it further.
    pub fn shrink_plan(&self) -> Vec<Extent> {
        lock(&self.inner.alloc).shrink_plan()
    }

    /// Makes room for `big`, a live extent that [`Pager::relocate`] cannot move because
    /// small extents fragment every aligned hole of its class below it (#314).
    ///
    /// Picks a region of `big`'s class below it whose occupants are all live, smaller and
    /// `movable`, and can each be relocated outside the region and below `big` (by
    /// [`Pager::relocate_below`], or for the manifest's, a snapshot rewrite's
    /// [`Pager::allocate_lowest`]: both take the lowest free block); of the first few such
    /// regions (among the lowest few hundred), the one with the least to move.
    ///
    /// The region is then held: its free space, and the occupants' old extents once they
    /// are reclaimed, stay unallocatable (placeholders, never published, so free again at
    /// the next open). The caller moves the occupants out (`big` as the limit), publishes
    /// the moves and, once their old extents are reclaimed, moves `big` in with
    /// [`Pager::relocate_into`]; or gives up with [`Pager::release_region`]. `None` if no
    /// region qualifies; nothing is held then.
    pub fn clear_for(&self, big: Extent, movable: impl Fn(Extent) -> bool) -> Option<Clearing> {
        /// Candidate regions tried, and regions looked at, per call.
        const TRIES: usize = 16;
        const SCAN: usize = 512;
        let (region, occupants) = lock(&self.inner.alloc).clear_for(big, &movable, TRIES, SCAN)?;
        Some(Clearing { region, occupants })
    }

    /// Stops holding a region [`Pager::clear_for`] cleared: what it held becomes free
    /// space. Returns false if it was not held (taken by [`Pager::relocate_into`]).
    pub fn release_region(&self, region: Extent) -> bool {
        lock(&self.inner.alloc).release_region(region)
    }

    /// Copies `extent` into the held `region` ([`Pager::clear_for`]), which it takes whole,
    /// and returns the copy, as [`Pager::relocate`] does; the region is no longer held.
    /// Fails with [`Error::NoSpace`], changing nothing, while anything but held free space
    /// is still inside it (an occupant's old extent not reclaimed yet), and with
    /// [`Error::Io`] if `extent` is not live.
    pub fn relocate_into(&self, extent: Extent, region: Extent) -> Result<Extent> {
        if !self.inner.writable {
            return Err(io_err(
                ErrorKind::Unsupported,
                "relocate on a read-only pager",
            ));
        }
        let target = {
            let mut alloc = lock(&self.inner.alloc);
            if !alloc.is_live(extent) {
                return Err(io_err(
                    ErrorKind::Other,
                    "relocate of an extent that is not live (unallocated or retired)",
                ));
            }
            if region.size_class != extent.size_class || region.page >= extent.page {
                return Err(Error::NoSpace);
            }
            alloc.claim_region(region).ok_or(Error::NoSpace)?
        };
        if let Err(e) = self.copy_extent(extent, target) {
            self.abandon(target);
            return Err(e);
        }
        Ok(target)
    }

    /// Whether `extent` is exactly an allocated extent that is not retired. A caller that
    /// read the manifest earlier uses it to tell an extent retired since (a compaction
    /// replaced it) from a failure, for example after [`Pager::relocate`] refuses.
    pub fn is_live(&self, extent: Extent) -> bool {
        lock(&self.inner.alloc).is_live(extent)
    }

    /// Copies `extent` into a newly allocated extent nearer the start of the file and returns
    /// it. The caller publishes the move in the manifest, then retires the old extent.
    ///
    /// Fails with [`Error::NoSpace`] if no free extent of that size lies below `extent`, and
    /// with [`Error::Io`] if `extent` is not live (see [`Pager::is_live`]).
    pub fn relocate(&self, extent: Extent) -> Result<Extent> {
        self.relocate_below(extent, extent)
    }

    /// As [`Pager::relocate`], into the lowest free extent that lies below `limit` rather
    /// than below `extent` itself: an occupant of a region [`Pager::clear_for`] reserved may
    /// move up, as long as it stays below the extent the region is cleared for.
    pub fn relocate_below(&self, extent: Extent, limit: Extent) -> Result<Extent> {
        if !self.inner.writable {
            return Err(io_err(
                ErrorKind::Unsupported,
                "relocate on a read-only pager",
            ));
        }
        let target = {
            let mut alloc = lock(&self.inner.alloc);
            if !alloc.is_live(extent) {
                return Err(io_err(
                    ErrorKind::Other,
                    "relocate of an extent that is not live (unallocated or retired)",
                ));
            }
            match alloc.alloc_lowest(extent.size_class) {
                Some(t) if t.page < limit.page => t,
                Some(t) => {
                    alloc.release_live(t);
                    return Err(Error::NoSpace);
                }
                None => return Err(Error::NoSpace),
            }
        };
        if let Err(e) = self.copy_extent(extent, target) {
            self.abandon(target);
            return Err(e);
        }
        Ok(target)
    }

    fn copy_extent(&self, from: Extent, to: Extent) -> Result<()> {
        const CHUNK: u64 = 1 << 20;
        let mut buf = vec![0u8; CHUNK.min(from.len()) as usize];
        let mut at = 0;
        while at < from.len() {
            self.read(from, at, &mut buf)?;
            self.write(to, at, &buf)?;
            at += buf.len() as u64;
        }
        Ok(())
    }

    /// Truncates the file after its last allocated extent. Returns bytes released.
    ///
    /// Retired extents still count as allocated (an old view may read them), and free space
    /// only exists where the durable root references nothing (see [`Pager::reclaim`]). While
    /// a root commit is in flight this releases nothing and returns 0, so a truncation never
    /// races the commit's syncs; call it again afterwards. The truncation is synced before
    /// this returns. The superblock's `file_pages` is refreshed by the next root commit; open
    /// sizes the allocator from the file length, never from `file_pages`.
    pub fn truncate_tail(&self) -> Result<u64> {
        if !self.inner.writable {
            return Err(io_err(
                ErrorKind::Unsupported,
                "truncate on a read-only pager",
            ));
        }
        let released = {
            let mut alloc = lock(&self.inner.alloc);
            if self.inner.committing.load(Ordering::Acquire) {
                return Ok(0);
            }
            let (end, frontier) = (alloc.used_end(), alloc.frontier());
            if end >= frontier {
                return Ok(0);
            }
            self.inner.file.set_len(end * UNIT_BYTES)?;
            alloc.truncate(end);
            self.inner.frontier.store(end, Ordering::Release);
            // Synced under the allocator lock, poisoning on failure, as in `allocate`: every
            // allocation waits for this sync meanwhile, and a commit's next step after it.
            self.inner.side.start();
            let synced = self.inner.file.sync_all();
            if synced.is_err() {
                lock(&self.inner.state).poisoned = true;
            }
            let parked = self.inner.side.finish();
            drop(alloc);
            parked.run();
            synced?;
            (frontier - end) * UNIT_BYTES
        };
        Ok(released)
    }

    /// Records a clean close in the superblock (bit 0 of flags) via one more root commit.
    /// Blocks like [`Pager::commit_root`]. The next root commit clears the flag.
    pub fn mark_clean(&self) -> Result<()> {
        let root = self.root();
        Ok(self.inner.commit(root, true)?)
    }

    /// Allocation statistics.
    pub fn stats(&self) -> PagerStats {
        let alloc = lock(&self.inner.alloc);
        PagerStats {
            file_bytes: alloc.frontier() * UNIT_BYTES,
            allocated_bytes: (alloc.used_units() - alloc.retired_units()) * UNIT_BYTES,
            retired_bytes: alloc.retired_units() * UNIT_BYTES,
            growths: self.inner.growths.load(Ordering::Relaxed),
            growth_nanos: self.inner.growth_nanos.load(Ordering::Relaxed),
            growths_ahead: self.inner.growths_ahead.load(Ordering::Relaxed),
            ahead_nanos: self.inner.ahead_nanos.load(Ordering::Relaxed),
        }
    }
}

/// Space accounting. `allocated_bytes + retired_bytes` never exceeds `file_bytes`; the rest
/// of the file (less the 64 KiB of superblocks and reserved pages) is free.
///
/// ```
/// use pigeonhole_io::VfsRef;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_pager::Pager;
///
/// # fn main() -> pigeonhole_pager::Result<()> {
/// let vfs: VfsRef = SimVfs::new(5);
/// let pager = Pager::create(&vfs, "/db/data.phdb".as_ref())?;
/// let e = pager.allocate(100 << 10)?; // rounds up to 128 KiB
/// assert_eq!(pager.stats().allocated_bytes, 128 << 10);
/// pager.retire(e, 1);
/// assert_eq!(pager.stats().retired_bytes, 128 << 10);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PagerStats {
    /// File length in bytes.
    pub file_bytes: u64,
    /// Bytes in allocated extents that are not retired.
    pub allocated_bytes: u64,
    /// Bytes retired but not yet reclaimed.
    pub retired_bytes: u64,
    /// File growths since open: each held the allocator across a `fallocate` and a
    /// `sync_all` (#28, #182; ICR 0015).
    pub growths: u64,
    /// Wall-clock nanoseconds those growths held the allocator.
    pub growth_nanos: u64,
    /// File growths ahead of need since open (64 MiB each, outside the allocator lock, the
    /// sync submitted to the I/O backend), once no free block of that size was left.
    pub growths_ahead: u64,
    /// Wall-clock nanoseconds their `fallocate`s took on the allocating thread.
    pub ahead_nanos: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pigeonhole_io::sim::SimVfs;

    #[test]
    fn growths_are_counted() {
        let vfs: VfsRef = SimVfs::new(1);
        let pager = Pager::create(&vfs, "/db".as_ref()).unwrap();
        let before = pager.stats().growths;
        let e = pager.allocate(1 << 20).unwrap();
        let s = pager.stats();
        assert!(s.growths > before, "{s:?}");
        pager.abandon(e);
        // Freed space is reused without growing.
        let e = pager.allocate(1 << 20).unwrap();
        assert_eq!(pager.stats().growths, s.growths);
        pager.abandon(e);
    }

    #[test]
    fn try_reclaim_does_not_wait_for_a_held_allocator() {
        // A growth holds the allocator across a `fallocate` and a `sync_all`: a reclaim
        // nobody waits on gives up instead of blocking for them.
        let vfs: VfsRef = SimVfs::new(1);
        let pager = Arc::new(Pager::create(&vfs, "/db".as_ref()).unwrap());
        let e = pager.allocate(64 << 10).unwrap();
        pager.abandon(e);
        let held = lock(&pager.inner.alloc);
        let (tx, rx) = std::sync::mpsc::channel();
        let p = Arc::clone(&pager);
        let t = std::thread::spawn(move || tx.send(p.try_reclaim(u64::MAX)).unwrap());
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("try_reclaim blocked on the held allocator");
        assert_eq!(got, None);
        drop(held);
        t.join().unwrap();
        assert_eq!(pager.try_reclaim(u64::MAX), Some(0));
    }

    fn page_of(vfs: &VfsRef, page: u64) -> Superblock {
        let f = vfs.open("/db".as_ref(), OpenOptions::read()).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        f.read_at(&mut buf, page * PAGE_SIZE as u64).unwrap();
        Superblock::decode(&buf).unwrap()
    }

    #[test]
    fn a_feature_change_keeps_a_root_committed_between_its_two_commits() {
        // perf287's S1 on #858: each of `set_features`' commits names the root current when
        // it starts, so a root committed between them is never replaced by an older one.
        let vfs: VfsRef = SimVfs::new(3);
        let pager = Pager::create(&vfs, "/db".as_ref()).unwrap();
        let root = |manifest_version| Root {
            manifest_version,
            ..Root::default()
        };
        pager.commit_root(root(1)).unwrap();
        let f = Features {
            compat: 1 << 2,
            ..Features::default()
        };
        // `set_features`' two steps, with the manifest writer's R2 between them.
        pager.inner.commit_features(f).unwrap();
        assert_eq!(pager.features(), f);
        pager.commit_root(root(2)).unwrap();
        pager.inner.commit_features(f).unwrap();
        assert_eq!(pager.root().manifest_version, 2, "R2 was replaced");
        let pages = [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B].map(|p| page_of(&vfs, p));
        assert!(pages.iter().all(|sb| sb.features == f), "{pages:?}");
        let newest = pages.iter().max_by_key(|sb| sb.sequence).unwrap();
        assert_eq!(newest.manifest_version, 2, "the newest page names R2");
        assert!(
            pages.iter().all(|sb| sb.manifest_version == 2),
            "a page went back to R1: {pages:?}"
        );
    }

    #[test]
    fn a_failed_feature_commit_leaves_the_sets_as_they_were() {
        let sim = SimVfs::new(4);
        let vfs: VfsRef = sim.clone();
        let pager = Pager::create(&vfs, "/db".as_ref()).unwrap();
        // Every write from here on fails: the first commit's superblock write among them.
        let mut plan = pigeonhole_io::sim::FaultPlan::none();
        plan.enospc_after_bytes = Some(0);
        sim.set_faults(plan);
        let f = Features {
            compat: 1,
            ..Features::default()
        };
        assert!(pager.set_features(f).is_err());
        assert_eq!(pager.features(), Features::default());
    }
}
