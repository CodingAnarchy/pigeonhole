//! The `pread` thread-pool backend: real files on every platform, the safe reference.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{FileRef, OpenOptions, ProcessId, Result, SharedOpen, SharedRegion, Vfs};

/// Real files with a fixed pool of threads serving submitted I/O.
#[derive(Debug)]
pub struct PreadVfs {
    _priv: (),
}

impl PreadVfs {
    /// A backend with `threads` I/O worker threads (0 picks a default from core count).
    pub fn new(threads: usize) -> Arc<Self> {
        todo!()
    }
}

impl Vfs for PreadVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        todo!()
    }

    fn remove(&self, path: &Path) -> Result<()> {
        todo!()
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        todo!()
    }

    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        todo!()
    }

    fn sync_dir(&self, dir: &Path) -> Result<()> {
        todo!()
    }

    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion> {
        todo!()
    }

    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()> {
        todo!()
    }

    fn now_micros(&self) -> u64 {
        todo!()
    }

    fn monotonic_nanos(&self) -> u64 {
        todo!()
    }

    fn current_process(&self) -> ProcessId {
        todo!()
    }

    fn process_alive(&self, process: ProcessId) -> bool {
        todo!()
    }
}
