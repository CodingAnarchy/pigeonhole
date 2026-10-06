//! The three lock-page bytes (FORMAT §8.3, decisions D3 and D21): writer, presence and
//! shm-init. Locks never block; a conflicting holder fails the call at once.

use pigeonhole_io::{ErrorKind, FileRef, LockMode};

use crate::{Error, Presence, Result, WriterLock};

/// Writer byte: exclusive by the one writer.
pub(crate) const WRITER_BYTE: u64 = 8192;
/// Presence byte: shared by every process with the database open.
pub(crate) const PRESENCE_BYTE: u64 = 8193;
/// Shm-init byte: exclusive while a process creates, validates or rebuilds the region.
pub(crate) const SHM_INIT_BYTE: u64 = 8194;

impl WriterLock {
    /// Takes the writer byte; fails at once with [`Error::WriterLocked`].
    pub fn acquire(file: &FileRef) -> Result<WriterLock> {
        match file.lock(WRITER_BYTE, LockMode::Exclusive) {
            Ok(()) => Ok(WriterLock { file: file.clone() }),
            Err(e) if e.kind == ErrorKind::Locked => Err(Error::WriterLocked),
            Err(e) => Err(e.into()),
        }
    }
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock(WRITER_BYTE);
    }
}

impl Presence {
    /// Takes the presence byte shared.
    pub fn acquire(file: &FileRef) -> Result<Presence> {
        file.lock(PRESENCE_BYTE, LockMode::Shared)?;
        Ok(Presence { file: file.clone() })
    }

    /// Tries to upgrade to exclusive. Success means this is the last process: it may
    /// checkpoint, remove the WAL files and remove the region. Releases on drop either way.
    ///
    /// The upgrade needs a handle opened for writing; on a read-only handle it fails with
    /// `Unsupported` (decision Q1 (io)).
    pub fn try_become_last(&self) -> Result<bool> {
        match self.file.lock(PRESENCE_BYTE, LockMode::Exclusive) {
            Ok(()) => Ok(true),
            Err(e) if e.kind == ErrorKind::Locked => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

impl Drop for Presence {
    fn drop(&mut self) {
        let _ = self.file.unlock(PRESENCE_BYTE);
    }
}

/// The shm-init byte, held exclusive for the duration of a create, validate or rebuild.
/// Released on drop.
pub(crate) struct ShmInit {
    file: FileRef,
}

/// How often and how long to retry the shm-init byte. Another process holds it only while
/// building or validating a region, which takes milliseconds.
const INIT_RETRIES: u32 = 200;
const INIT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(5);

impl ShmInit {
    /// Takes the shm-init byte, retrying briefly while another process holds it.
    pub(crate) fn acquire(file: &FileRef) -> Result<Self> {
        let mut attempts = 0;
        loop {
            match file.lock(SHM_INIT_BYTE, LockMode::Exclusive) {
                Ok(()) => return Ok(Self { file: file.clone() }),
                Err(e) if e.kind == ErrorKind::Locked && attempts < INIT_RETRIES => {
                    attempts += 1;
                    std::thread::sleep(INIT_RETRY_DELAY);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

impl Drop for ShmInit {
    fn drop(&mut self) {
        let _ = self.file.unlock(SHM_INIT_BYTE);
    }
}

/// Whether no other process has the database open: tries to take the presence byte
/// exclusively through `file`. On success the byte is left **shared**, so a presence lock the
/// caller already holds on this handle is kept (converted back) and a process that held none
/// is now simply present, which it is while `file` stays open. `Locked` means another process
/// is present; any other failure (for example a read-only handle, which cannot try) is
/// returned.
pub(crate) fn alone(file: &FileRef) -> Result<bool> {
    match file.lock(PRESENCE_BYTE, LockMode::Exclusive) {
        Ok(()) => {
            file.lock(PRESENCE_BYTE, LockMode::Shared)?;
            Ok(true)
        }
        Err(e) if e.kind == ErrorKind::Locked => Ok(false),
        Err(e) => Err(e.into()),
    }
}
