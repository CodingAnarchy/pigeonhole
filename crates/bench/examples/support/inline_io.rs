//! A filesystem for a measured shape whose count must not depend on thread timing (#794):
//! the pread backend's files with every submitted operation run on the submitting thread,
//! and a monotonic clock that moves a fixed step per reading.
//!
//! On the pread backend a submitted read runs on an I/O worker thread. Whether a read-ahead
//! block's completion (`Fetch::admit`, then decompressing the block) then ran on the measured
//! shard thread or on the worker depended on which got there first, and so did the measured
//! thread's heap, and with it which of the compaction's buffer reallocs copied: the
//! `compact` shape varied by about 0.4% between runs on one machine. And a background task
//! (a compaction) yields at a wall-clock deadline, so a slower machine ran it in more
//! slices, each counted: identical code measured 30-46 instructions per entry more on some
//! CI runners than on others. Inline I/O runs every completion on the thread that submitted
//! it, and the stepped clock ends each slice after the same work on every machine.
//!
//! Wall-clock time (`now_micros`: timestamps) is the real one.
//!
//! (Not an example of its own: examples include it with `#[path]`.)
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_io::{
    Completion, File, FileIdentity, FileRef, IoBuf, Locality, LockMode, OpenOptions, ProcessId,
    Result, SharedOpen, SharedRegion, Vfs, VfsRef,
};

/// How far the monotonic clock moves per reading: 1 µs, so a 500 µs task slice ends after
/// 500 readings.
const STEP_NANOS: u64 = 1_000;

/// The pread backend's files, with inline I/O and a stepped monotonic clock.
pub struct InlineVfs {
    inner: VfsRef,
    clock: AtomicU64,
}

impl InlineVfs {
    /// Real files through the pread backend.
    pub fn vfs() -> VfsRef {
        Arc::new(InlineVfs {
            inner: pigeonhole_io::pread::PreadVfs::new(0),
            clock: AtomicU64::new(0),
        })
    }
}

impl fmt::Debug for InlineVfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InlineVfs").finish_non_exhaustive()
    }
}

impl Vfs for InlineVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        let inner = self.inner.open(path, opts)?;
        Ok(Arc::new(InlineFile { inner }))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        self.inner.remove(path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        self.inner.exists(path)
    }

    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        self.inner.list_dir(dir)
    }

    fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.inner.sync_dir(dir)
    }

    fn submit_sync_dir(&self, dir: &Path) -> Completion<()> {
        Completion::ready(self.inner.sync_dir(dir))
    }

    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }

    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()> {
        self.inner.remove_shared(name, dir)
    }

    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }

    fn monotonic_nanos(&self) -> u64 {
        self.clock.fetch_add(STEP_NANOS, Ordering::Relaxed) + STEP_NANOS
    }

    fn current_process(&self) -> ProcessId {
        self.inner.current_process()
    }
}

/// A pread file whose submitted operations run on the calling thread.
#[derive(Debug)]
struct InlineFile {
    inner: FileRef,
}

impl File for InlineFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.inner.read_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.inner.write_at(buf, offset)
    }

    fn direct_align(&self) -> Option<usize> {
        self.inner.direct_align()
    }

    fn read_buf(&self, len: usize) -> IoBuf {
        self.inner.read_buf(len)
    }

    fn submit_read(&self, mut buf: IoBuf, offset: u64) -> Completion {
        Completion::ready(self.inner.read_at(&mut buf, offset).map(|()| buf))
    }

    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        Completion::ready(self.inner.write_at(&buf, offset).map(|()| buf))
    }

    fn sync_data(&self) -> Result<()> {
        self.inner.sync_data()
    }

    fn submit_sync_data(&self) -> Completion<()> {
        Completion::ready(self.inner.sync_data())
    }

    fn sync_all(&self) -> Result<()> {
        self.inner.sync_all()
    }

    fn submit_sync_all(&self) -> Completion<()> {
        Completion::ready(self.inner.sync_all())
    }

    fn len(&self) -> Result<u64> {
        self.inner.len()
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.inner.set_len(len)
    }

    fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        self.inner.allocate(offset, len)
    }

    fn lock(&self, byte: u64, mode: LockMode) -> Result<()> {
        self.inner.lock(byte, mode)
    }

    fn unlock(&self, byte: u64) -> Result<()> {
        self.inner.unlock(byte)
    }

    fn identity(&self) -> Result<FileIdentity> {
        self.inner.identity()
    }

    fn is_local(&self) -> Result<bool> {
        self.inner.is_local()
    }

    fn locality(&self) -> Result<Locality> {
        self.inner.locality()
    }
}
