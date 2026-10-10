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
//!   memory, except that `allocate` preallocates more file (one `fallocate` of at most
//!   64 MiB, under the allocator lock) when no free extent fits. They run per flush or
//!   compaction output, never per write.
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
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, TryLockError};

use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_format::{FormatVersion, ManifestVersion, PAGE_SIZE};
use pigeonhole_io::{Completion, ErrorKind, FileRef, OpenOptions, VfsRef};

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

    /// The file, for reading the manifest before `finish`.
    pub fn file(&self) -> &FileRef {
        &self.file
    }

    /// Marks `live` extents allocated (everything else is free) and returns a usable pager.
    /// `live` must include the manifest snapshot and log extents, every SST and every blob
    /// extent. Exact duplicates are accepted (tablets may share an SST after a split);
    /// overlapping, misaligned or out-of-file extents fail with [`Error::Format`]. A writable
    /// pager first makes the file's current length durable (`sync_all`).
    pub fn finish(self, live: impl IntoIterator<Item = Extent>) -> Result<Pager> {
        // The allocator is sized from the length the page cache shows, which a process that
        // died between a growth's `fallocate` and its `sync_all` left longer than the disk's.
        // Make it durable before handing out that tail: root commits sync with `sync_data`.
        if self.writable {
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
                    poisoned: false,
                }),
                committing: AtomicBool::new(false),
                growths: AtomicU64::new(0),
                growth_nanos: AtomicU64::new(0),
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
}

#[derive(Debug)]
struct CommitState {
    /// The root most recently committed (or reloaded).
    root: Root,
    /// Sequence of the current superblock.
    sequence: u64,
    /// Page of the current superblock; the next commit writes the other one.
    slot: u64,
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
fn read_slot(file: &FileRef, slot: u64) -> Result<pigeonhole_format::Result<Superblock>> {
    let mut page = [0u8; PAGE_SIZE];
    match file.read_at(&mut page, slot * PAGE_SIZE as u64) {
        Ok(()) => Ok(Superblock::decode(&page)),
        Err(e) if e.kind == ErrorKind::UnexpectedEof => {
            Ok(Err(pigeonhole_format::Error::Truncated {
                what: "superblock",
            }))
        }
        Err(e) => Err(e.into()),
    }
}

/// The current superblock and its page.
fn read_superblocks(file: &FileRef) -> Result<(Superblock, u64)> {
    let a = read_slot(file, SUPERBLOCK_PAGE_A)?;
    let b = read_slot(file, SUPERBLOCK_PAGE_B)?;
    Ok(Superblock::choose(a, b)?)
}

impl Inner {
    /// Validates and encodes a commit of `root` and claims the commit slot.
    fn begin(&self, root: Root, clean: bool) -> pigeonhole_io::Result<PendingCommit> {
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
        drop(st);
        let file_pages = lock(&self.alloc).frontier() * UNIT_PAGES;
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
        };
        let mut page = Box::new([0u8; PAGE_SIZE]);
        sb.encode(&mut page);
        Ok(PendingCommit {
            page,
            slot,
            sequence,
            root,
        })
    }

    /// Fails if a growth's or truncation's sync failed. A commit calls it after each of its
    /// own syncs succeeded: that sync may have succeeded only because a concurrent one was
    /// handed the error for the same pages. Those syncs run under the allocator lock and
    /// poison before releasing it, so taking the lock waits for one in flight. That wait can
    /// be a whole growth's `fallocate` and `sync_all` (up to 64 MiB), and for a submitted
    /// commit it happens on the I/O backend's thread, which then serves no reads meanwhile
    /// (bounded; issue #182).
    fn check_syncs(&self) -> pigeonhole_io::Result<()> {
        let _alloc = lock(&self.alloc);
        if lock(&self.state).poisoned {
            return Err(poisoned_error());
        }
        Ok(())
    }

    /// Writes the superblock once the first sync succeeded and no other sync failed.
    fn write_superblock(&self, p: &PendingCommit) -> pigeonhole_io::Result<()> {
        self.check_syncs()?;
        self.file.write_at(&p.page[..], p.slot * PAGE_SIZE as u64)
    }

    /// Ends a commit: on success the new root is current; on failure (or if another sync
    /// failed meanwhile) the pager is poisoned.
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
        let result = self
            .file
            .sync_data()
            .and_then(|()| self.write_superblock(&p))
            .and_then(|()| self.file.sync_data());
        self.end(p, result)
    }

    /// The same steps with both syncs submitted to the I/O backend. The superblock write (one
    /// 4 KiB page into the page cache) runs on the thread that resolves the first sync.
    fn submit_commit(self: Arc<Self>, root: Root) -> Completion<()> {
        let p = match self.begin(root, false) {
            Ok(p) => p,
            Err(e) => return Completion::ready(Err(e)),
        };
        let (done, resolver) = Completion::pair();
        let first = self.file.submit_sync_data();
        // The continuation's own completion is not needed: `resolver` reports the outcome.
        let _chained = first.map(move |synced| {
            match synced.and_then(|()| self.write_superblock(&p)) {
                Err(e) => resolver.resolve(self.end(p, Err(e))),
                Ok(()) => {
                    let second = self.file.submit_sync_data();
                    let _chained = second.map(move |synced| {
                        resolver.resolve(self.end(p, synced));
                        Ok(())
                    });
                }
            }
            Ok(())
        });
        done
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
        }
        .finish([])
    }

    /// Opens an existing file and reads both superblocks. No recovery scan. A read-only pager
    /// (reader processes) never allocates, retires or commits; it only reads and reloads.
    pub fn open(vfs: &VfsRef, path: &Path, writable: bool) -> Result<OpenedPager> {
        let mut opts = OpenOptions::read();
        opts.write = writable;
        let file = vfs.open(path, opts)?;
        let (superblock, slot) = read_superblocks(&file)?;
        Ok(OpenedPager {
            file,
            writable,
            superblock,
            slot,
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
        // a flush wrote, which the next commit's sync would then not report.
        if let Err(e) = self.inner.file.sync_all() {
            lock(&self.inner.state).poisoned = true;
            return Err(e.into());
        }
        self.inner.growths.fetch_add(1, Ordering::Relaxed);
        self.inner.growth_nanos.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(alloc.alloc_grown(class))
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
        let (sb, slot) = read_superblocks(&self.inner.file)?;
        let mut st = lock(&self.inner.state);
        if sb.sequence == st.sequence {
            return Ok(None);
        }
        st.sequence = sb.sequence;
        st.slot = slot;
        st.root = Root::from_superblock(&sb);
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
            // Synced under the allocator lock, poisoning on failure, as in `allocate`: every
            // allocation (and a commit's `check_syncs`) waits for this sync meanwhile.
            if let Err(e) = self.inner.file.sync_all() {
                lock(&self.inner.state).poisoned = true;
                return Err(e.into());
            }
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
}
