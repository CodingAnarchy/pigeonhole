use std::collections::hash_map::RandomState;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{FileRef, OpenOptions, Result, SharedOpen, SharedRegion};

/// A backend's io_uring rings now ([`Vfs::ring_stats`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct RingStats {
    /// Rings: the shared one, plus one per shard thread that got its own.
    pub rings: usize,
    /// Rings with a registered buffer pool (fixed buffers); the rest use plain buffers.
    pub pooled: usize,
}

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

    /// Whether the clocks are simulated: they move only when the program moves them, so a
    /// reading that stays the same means nothing will change until it does. The engine
    /// then applies its stopped-clock fallbacks (D126) once a timer sees the clock stand
    /// still. A real clock (the default) is never taken as stopped, however coarse its
    /// ticks (ICR 0012, #263). A wrapper around another `Vfs` that keeps its clocks forwards
    /// this.
    fn clock_is_simulated(&self) -> bool {
        false
    }

    /// The calling process.
    fn current_process(&self) -> ProcessId;

    /// Whether `process` is still running (pid present and start time unchanged).
    fn process_alive(&self, process: ProcessId) -> bool;

    /// Gives the calling thread an I/O queue of its own, if this backend has them (an
    /// io_uring ring per shard thread, #402): operations it submits from then on go there,
    /// and it completes them by reaping ([`reap_own_io`](crate::reap_own_io)), in its turns
    /// and in its waits. Called by threads that drive shards; calling it again does nothing.
    /// The default, for backends whose completions arrive on threads of their own, does
    /// nothing.
    fn attach_thread(&self) {}

    /// For a backend with io_uring rings (#402): how many rings it has now and how many got
    /// a registered buffer pool (a ring registers one on the first read that can use it). Rings past the locked-memory budget, and shard threads whose
    /// ring could not be created (they use the shared one), run with plain buffers; this
    /// shows when that happens. `None` for other backends (the default).
    fn ring_stats(&self) -> Option<RingStats> {
        None
    }

    /// A random 64-bit value, for identifiers that must be unique (a new database's id).
    ///
    /// The default mixes the standard library's per-process random hash keys (`RandomState`,
    /// seeded from the OS) with both clocks and the process id. It is not cryptographic:
    /// expect values unique across databases and processes for identification, never
    /// unpredictable to an adversary. Two calls in one process differ (each `RandomState`
    /// gets fresh keys). A simulated `Vfs` overrides it to derive values from its seed, so a
    /// run replays exactly (`SimVfs` does).
    fn random_u64(&self) -> u64 {
        let mut h = RandomState::new().build_hasher();
        h.write_u64(self.now_micros());
        h.write_u64(self.monotonic_nanos());
        h.write_u32(self.current_process().pid);
        h.finish()
    }
}

/// A shared handle to a [`Vfs`].
pub type VfsRef = Arc<dyn Vfs>;
