use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{FileRef, OpenOptions, Result, SharedOpen, SharedRegion};

/// Device and inode of a file: the same on every process however the path is spelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileIdentity {
    /// Device number (volume serial number on Windows).
    pub device: u64,
    /// Inode (file index on Windows).
    pub inode: u64,
}

/// A process as recorded in a reader slot: its id plus its start time, so a recycled pid is
/// not mistaken for the original process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProcessId {
    /// Operating-system process id.
    pub pid: u32,
    /// Opaque, platform-defined start time.
    pub start_time: u64,
}

/// The environment: files, shared memory, clocks and process liveness.
///
/// Held as [`VfsRef`] (`Arc<dyn Vfs>`). Dynamic dispatch is fine here: every call is an I/O
/// or a clock read, never a per-cell operation.
pub trait Vfs: Send + Sync + Debug {
    /// Opens a file.
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef>;

    /// Removes a file.
    fn remove(&self, path: &Path) -> Result<()>;

    /// Whether a path exists.
    fn exists(&self, path: &Path) -> Result<bool>;

    /// Lists the entries of a directory (used to discover WAL streams).
    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>>;

    /// Makes directory entries (creations, removals) durable.
    fn sync_dir(&self, dir: &Path) -> Result<()>;

    /// Opens or creates a shared-memory region named `name` of exactly `len` bytes. `dir`
    /// overrides the default memory-backed location (`/dev/shm`, POSIX `shm_open`, or a
    /// pagefile-backed mapping) with a file in that directory.
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion>;

    /// Removes a shared-memory region's name (existing mappings stay valid).
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()>;

    /// Wall-clock time in microseconds since the Unix epoch (timestamps, TTL).
    fn now_micros(&self) -> u64;

    /// Monotonic time in nanoseconds from an arbitrary origin (scheduling, latencies).
    fn monotonic_nanos(&self) -> u64;

    /// The calling process.
    fn current_process(&self) -> ProcessId;

    /// Whether `process` is still running (pid present and start time unchanged).
    fn process_alive(&self, process: ProcessId) -> bool;
}

/// A shared handle to a [`Vfs`].
pub type VfsRef = Arc<dyn Vfs>;
