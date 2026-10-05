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
    /// Bypass the OS page cache (O_DIRECT / F_NOCACHE / FILE_FLAG_NO_BUFFERING) where
    /// supported; ignored elsewhere. Phase 3.
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
pub trait File: Send + Sync + Debug {
    /// Reads exactly `buf.len()` bytes at `offset`; fails with `UnexpectedEof` if short.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;

    /// Writes all of `buf` at `offset`.
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()>;

    /// Submits a read of `buf.len()` bytes at `offset` and returns at once.
    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion;

    /// Submits a write of `buf` at `offset` and returns at once.
    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion;

    /// Makes written data durable (fdatasync / F_FULLFSYNC / FlushFileBuffers).
    fn sync_data(&self) -> Result<()>;

    /// Submits [`File::sync_data`] and returns at once; the backend runs it off the caller's
    /// thread (pread pool, io_uring in Phase 3) so a shard keeps working while it runs.
    fn submit_sync_data(&self) -> Completion<()>;

    /// Makes data and metadata (size) durable.
    fn sync_all(&self) -> Result<()>;

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

    /// Takes a byte-range lock on the single byte at `byte`. Never blocks: fails with
    /// `ErrorKind::Locked` if a conflicting lock is held. Taking `Exclusive` while holding
    /// `Shared` is an upgrade attempt.
    fn lock(&self, byte: u64, mode: LockMode) -> Result<()>;

    /// Releases this handle's lock on `byte` (a no-op if none is held).
    fn unlock(&self, byte: u64) -> Result<()>;

    /// Device and inode (file index on Windows), used to name the shared-memory region.
    fn identity(&self) -> Result<FileIdentity>;

    /// Whether the file lives on a local filesystem. Network filesystems are refused at open.
    fn is_local(&self) -> Result<bool>;
}

/// A shared handle to an open file.
pub type FileRef = Arc<dyn File>;
