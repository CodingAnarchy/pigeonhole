use std::fmt::Debug;
use std::sync::Arc;

use crate::{Completion, FileIdentity, IoBuf, Result};

/// How to open a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct OpenOptions {
    /// Open for writing as well as reading.
    pub write: bool,
    /// Create the file if it does not exist.
    pub create: bool,
    /// Fail if the file exists (implies `create`).
    pub create_new: bool,
    /// Bypass the OS page cache (#403): `O_DIRECT` on Linux, `F_NOCACHE` on macOS,
    /// `FILE_FLAG_NO_BUFFERING` on Windows. Every read and write on such a handle must then
    /// be aligned ([`File::direct_align`]): its offset, its length and its buffer's address
    /// (an [`IoBuf`] is). A file system that refuses direct I/O fails the open with
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported); open without it instead.
    pub direct: bool,
}

impl OpenOptions {
    /// Read-only.
    pub fn read() -> Self {
        Self::default()
    }

    /// Read-write, creating if missing.
    pub fn read_write_create() -> Self {
        Self {
            write: true,
            create: true,
            ..Self::default()
        }
    }
}

/// Mode of a byte-range lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Any number of holders.
    Shared,
    /// One holder.
    Exclusive,
}

/// An open file. Shared across threads; all methods take `&self` and use positional I/O.
///
/// Locks are per handle (OFD locks on Linux, `fcntl` behind a per-process registry on macOS
/// and BSD so closing one handle never drops another's lock, `LockFileEx` on Windows).
///
/// A `File` that wraps another must forward [`File::count_into`], or the wrapped file's I/O
/// goes uncounted (D332).
pub trait File: Send + Sync + Debug {
    /// Reads exactly `buf.len()` bytes at `offset`; fails with `UnexpectedEof` if short.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;

    /// Writes all of `buf` at `offset`.
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()>;

    /// Counts this file's I/O into `counters` from now on (D332, ICR 0142): the buffer length
    /// of each read and write as it's submitted (`read_at`, `submit_read`,
    /// `submit_reads_ahead`, `write_at`, `submit_write`), a short or failed one in full. Not
    /// syncs, writeback hints, allocations or punches. Bytes moved before the call (an
    /// open's own reads) aren't counted. A second call with the same counters does nothing;
    /// with others it's a bug, caught in debug builds. The default counts nothing: a test's
    /// or an application's own `File` (a wrapper must forward it).
    fn count_into(&self, counters: std::sync::Arc<crate::IoCounters>) {
        let _ = counters;
    }

    /// For a handle opened with [`OpenOptions::direct`], the alignment its I/O needs: offsets,
    /// lengths and buffer addresses a multiple of it, else
    /// [`ErrorKind::Misaligned`](crate::ErrorKind::Misaligned). `None` for a buffered
    /// handle, whose I/O may start and end anywhere (the default).
    fn direct_align(&self) -> Option<usize> {
        None
    }

    /// A buffer of `len` bytes for a submitted read ([`File::submit_read`]): on a backend
    /// with registered buffers (io_uring, #402), a slot of the pool the read will use, so the
    /// kernel skips pinning its pages. Its contents are unspecified (old bytes of an earlier
    /// read may be there): fill it fully before anyone reads it, as an exact read does, or
    /// drop it. Keep a slot-backed buffer only for its I/O: one stored for longer goes through
    /// [`IoBuf::detached`]. The default is [`IoBuf::reused`].
    fn read_buf(&self, len: usize) -> IoBuf {
        IoBuf::reused(len)
    }

    /// Submits a read of `buf.len()` bytes at `offset` and returns at once.
    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion;

    /// Submits reads that nothing waits on yet (a scan's readahead) as one batch, each
    /// resolving as [`File::submit_read`] would, in the order given. A backend may hand the
    /// whole batch to the kernel at once and run each read off the submitting thread (io_uring:
    /// one enter; on the shared ring every read with `IOSQE_ASYNC`, ICR 0077). The default
    /// submits each in turn.
    ///
    /// ```
    /// use std::path::Path;
    /// use pigeonhole_io::{File, OpenOptions, Vfs};
    /// use pigeonhole_io::sim::SimVfs;
    ///
    /// # fn main() -> pigeonhole_io::Result<()> {
    /// let vfs = SimVfs::new(42);
    /// let f = vfs.open(Path::new("/db/f"), OpenOptions::read_write_create())?;
    /// f.write_at(&[7; 8192], 0)?;
    /// let reads = vec![(f.read_buf(4096), 0), (f.read_buf(4096), 4096)];
    /// for done in f.submit_reads_ahead(reads) {
    ///     assert!(done.wait()?.iter().all(|&b| b == 7));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    fn submit_reads_ahead(&self, reads: Vec<(IoBuf, u64)>) -> Vec<Completion> {
        reads
            .into_iter()
            .map(|(buf, offset)| self.submit_read(buf, offset))
            .collect()
    }

    /// Submits a write of `buf` at `offset` and returns at once.
    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion;

    /// Starts writing `len` bytes at `offset` back to the device, without waiting for them,
    /// as a hint that they are complete and a sync will want them (D334): a writer of a long
    /// output calls it per chunk, so the output's sync finds it mostly written back instead
    /// of sending it to the device in one burst that a small, latency-bound write (a WAL
    /// commit's) then waits behind inside the device (#1991).
    ///
    /// Never a durability step: [`File::sync_data`] still makes the bytes durable, and an
    /// implementation may do nothing. Linux uses `sync_file_range(SYNC_FILE_RANGE_WRITE)`;
    /// macOS and Windows have no range writeback that returns without waiting
    /// (`F_FULLFSYNC` and `FlushFileBuffers` flush the whole file), so there it is a no-op,
    /// as it is on a direct-I/O handle (no page cache) and in the simulator (which keeps
    /// the bytes unsynced, so a crash test sees the same states as without the hint).
    fn start_writeback(&self, _offset: u64, _len: u64) -> Result<()> {
        Ok(())
    }

    /// Makes written data durable (fdatasync / F_FULLFSYNC / FlushFileBuffers).
    ///
    /// Callers may not rely on it to make a length change durable: after `set_len`,
    /// `allocate` or a write past the end, call [`File::sync_all`] before depending on the
    /// new length surviving a power loss. `SimVfs` enforces this.
    fn sync_data(&self) -> Result<()>;

    /// Submits [`File::sync_data`] and returns at once; the backend runs it off the caller's
    /// thread (pread pool, io_uring in Phase 3) so a shard keeps working while it runs.
    fn submit_sync_data(&self) -> Completion<()>;

    /// Makes data and metadata (size) durable.
    fn sync_all(&self) -> Result<()>;

    /// Submits [`File::sync_all`] and returns at once. The pread backend runs it on its pool,
    /// so syncs of several files overlap; the default runs it inline (as `SimVfs` does, which
    /// keeps simulated runs deterministic).
    fn submit_sync_all(&self) -> Completion<()> {
        Completion::ready(self.sync_all())
    }

    /// Current length.
    fn len(&self) -> Result<u64>;

    /// Whether the file is empty.
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Truncates or extends (with zeros).
    fn set_len(&self, len: u64) -> Result<()>;

    /// Preallocates `[offset, offset + len)` so later writes there need no metadata update.
    fn allocate(&self, offset: u64, len: u64) -> Result<()>;

    /// Gives the filesystem back the blocks under `[offset, offset + len)`, keeping the
    /// file's length: the range then reads back as zeros (D333, ICR 0144). For space no
    /// reader can reach any more; the bytes there are gone, so never call it on data
    /// anything may still read or recover.
    ///
    /// A punch is not ordered with a later sync: after a power loss the range may read back
    /// as zeros or as its old bytes. A range past the end changes nothing.
    ///
    /// - Linux: `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`.
    /// - macOS: `fcntl(F_PUNCHHOLE)`, whose offset and length must be multiples of the
    ///   filesystem's block size (`EINVAL` otherwise, reported as an error, not as
    ///   unsupported).
    /// - Elsewhere, and on a filesystem that cannot punch: fails with
    ///   [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported), as does the default.
    ///
    /// ```
    /// use std::path::Path;
    /// use pigeonhole_io::{File, OpenOptions, Vfs};
    /// use pigeonhole_io::sim::SimVfs;
    ///
    /// # fn main() -> pigeonhole_io::Result<()> {
    /// let vfs = SimVfs::new(7);
    /// let f = vfs.open(Path::new("/db/f"), OpenOptions::read_write_create())?;
    /// f.write_at(&[9; 8192], 0)?;
    /// f.punch_hole(0, 4096)?;
    /// let mut buf = [1; 8192];
    /// f.read_at(&mut buf, 0)?;
    /// assert!(buf[..4096].iter().all(|&b| b == 0) && buf[4096..].iter().all(|&b| b == 9));
    /// assert_eq!(f.len()?, 8192);
    /// # Ok(())
    /// # }
    /// ```
    fn punch_hole(&self, offset: u64, len: u64) -> Result<()> {
        let _ = (offset, len);
        Err(crate::Error::new(
            crate::ErrorKind::Unsupported,
            "punch_hole: not supported here",
        ))
    }

    /// Takes a byte-range lock on the single byte at `byte`. Never blocks: fails with
    /// `ErrorKind::Locked` if a conflicting lock is held. Taking `Exclusive` while holding
    /// `Shared` is an upgrade attempt. `Exclusive` needs a handle opened for writing (POSIX
    /// refuses write locks on read-only descriptors); otherwise it fails with `Unsupported`.
    fn lock(&self, byte: u64, mode: LockMode) -> Result<()>;

    /// Releases this handle's lock on `byte` (a no-op if none is held).
    fn unlock(&self, byte: u64) -> Result<()>;

    /// The lock this handle holds on `byte`, if any. Locks never conflict within one handle
    /// (a second [`File::lock`] of a byte the handle holds succeeds), so a caller that would
    /// release a byte after a failed step checks this first, to never drop a lock it held
    /// before (reader slots, #693). The default reports none: a wrapper that forwards
    /// [`File::lock`] must forward this too.
    fn holds(&self, byte: u64) -> Option<LockMode> {
        let _ = byte;
        None
    }

    /// Device and inode (file index on Windows), used to name the shared-memory region.
    fn identity(&self) -> Result<FileIdentity>;

    /// Whether the file lives on a local filesystem. Network filesystems are refused at open.
    fn is_local(&self) -> Result<bool>;

    /// What kind of filesystem the file lives on: [`Locality::Fuse`] apart from other
    /// non-local ones, so a caller can accept FUSE on request (decision D173, #299). The
    /// default derives it from [`File::is_local`] (never `Fuse`).
    fn locality(&self) -> Result<Locality> {
        Ok(if self.is_local()? {
            Locality::Local
        } else {
            Locality::Network
        })
    }
}

/// The kind of filesystem a file lives on ([`File::locality`]).
///
/// ```
/// use pigeonhole_io::Locality;
///
/// assert_ne!(Locality::Fuse, Locality::Local);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Locality {
    /// A local filesystem.
    Local,
    /// A FUSE filesystem. It may be local (ntfs-3g, gocryptfs) or network-backed (sshfs,
    /// s3fs, gcsfuse): its byte-range locks may be local to one host, and a sync may not
    /// reach stable storage.
    Fuse,
    /// A network or cluster filesystem (NFS, SMB, Ceph, GPFS, ...).
    Network,
}

/// A shared handle to an open file.
pub type FileRef = Arc<dyn File>;
