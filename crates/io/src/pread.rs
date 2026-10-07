//! The `pread` thread-pool backend: real files on every platform, the safe reference.
//!
//! Positional reads and writes (`pread`/`pwrite`, `ReadFile`/`WriteFile` with an offset) run
//! on the caller's thread; submitted operations run on a fixed pool of worker threads and
//! resolve their [`Completion`].
//!
//! Byte-range locks belong to the handle that took them:
//!
//! - **Linux:** open-file-description locks (`F_OFD_SETLK`), per handle natively.
//! - **macOS and BSD:** classic `fcntl` locks are per *process*, and closing any descriptor
//!   of a file drops all of the process's locks on it. A per-process registry keyed by
//!   (device, inode) arbitrates between handles of the same process, and defers closing a
//!   handle's descriptor while other handles of that file still hold locks.
//! - **Windows:** `LockFileEx`, per handle natively. An upgrade from shared to exclusive is
//!   not atomic there (unlock, then lock): if it fails the shared lock is re-taken, and in
//!   the rare race where another process takes the byte in between, it is lost.
//!
//! POSIX grants write (exclusive) locks only on descriptors open for writing, so on every
//! platform an exclusive lock through a read-only handle fails with `Unsupported`.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{
    Completion, Error, ErrorKind, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions,
    ProcessId, Result, SharedOpen, SharedRegion, Vfs, os,
};

/// Real files with a fixed pool of threads serving submitted I/O.
///
/// ```
/// use pigeonhole_io::{File, IoBuf, OpenOptions, Vfs, pread::PreadVfs};
///
/// # fn main() -> pigeonhole_io::Result<()> {
/// # if cfg!(miri) { return Ok(()); } // real syscalls
/// let dir = std::env::temp_dir().join(format!("pigeonhole-io-doc-{}", std::process::id()));
/// std::fs::create_dir_all(&dir).unwrap();
/// let path = dir.join("example.bin");
///
/// let vfs = PreadVfs::new(2);
/// let file = vfs.open(&path, OpenOptions::read_write_create())?;
/// file.write_at(b"hello", 0)?;
///
/// // Submitted reads run on the pool; wait (or `.await`) for the buffer.
/// let buf = file.submit_read(IoBuf::zeroed(5), 0).wait()?;
/// assert_eq!(&buf[..], b"hello");
/// file.submit_sync_data().wait()?;
///
/// drop(file);
/// vfs.remove(&path)?;
/// # std::fs::remove_dir(&dir).unwrap();
/// # Ok(())
/// # }
/// ```
pub struct PreadVfs {
    pool: Arc<Pool>,
    origin: Instant,
    process: ProcessId,
}

impl PreadVfs {
    /// A backend with `threads` I/O worker threads (0 picks a default from core count).
    pub fn new(threads: usize) -> Arc<Self> {
        let threads = if threads == 0 {
            os::available_cpus().clamp(2, 16)
        } else {
            threads
        };
        Arc::new(Self {
            pool: Pool::start(threads),
            origin: Instant::now(),
            process: ProcessId {
                pid: std::process::id(),
                start_time: os::current_start_time(),
            },
        })
    }
}

impl fmt::Debug for PreadVfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreadVfs")
            .field("threads", &self.pool.threads)
            .field("process", &self.process)
            .finish()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

type Job = Box<dyn FnOnce() + Send>;

/// The worker pool. Workers drain the queue and exit once the pool is dropped, which only
/// happens after the vfs and every file opened through it are gone.
struct Pool {
    shared: Arc<PoolShared>,
    threads: usize,
}

struct PoolShared {
    queue: Mutex<PoolQueue>,
    cond: Condvar,
}

struct PoolQueue {
    jobs: VecDeque<Job>,
    shutdown: bool,
}

impl Pool {
    fn start(threads: usize) -> Arc<Self> {
        let shared = Arc::new(PoolShared {
            queue: Mutex::new(PoolQueue {
                jobs: VecDeque::new(),
                shutdown: false,
            }),
            cond: Condvar::new(),
        });
        for i in 0..threads {
            let shared = Arc::clone(&shared);
            std::thread::Builder::new()
                .name(format!("pigeonhole-io-{i}"))
                .spawn(move || worker(&shared))
                .expect("spawn I/O worker thread");
        }
        Arc::new(Self { shared, threads })
    }

    fn submit(&self, job: Job) {
        lock(&self.shared.queue).jobs.push_back(job);
        self.shared.cond.notify_one();
    }
}

fn worker(shared: &PoolShared) {
    loop {
        let job = {
            let mut q = lock(&shared.queue);
            loop {
                if let Some(job) = q.jobs.pop_front() {
                    break job;
                }
                if q.shutdown {
                    return;
                }
                q = shared.cond.wait(q).unwrap_or_else(PoisonError::into_inner);
            }
        };
        // A panicking job drops its `Resolver` while unwinding, which resolves the
        // completion with an error; the worker itself keeps serving.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        lock(&self.shared.queue).shutdown = true;
        self.shared.cond.notify_all();
    }
}

/// An open file. The state lives behind an `Arc` so submitted jobs can carry it.
#[derive(Debug)]
struct PreadFile {
    inner: Arc<FileInner>,
}

struct FileInner {
    /// Always `Some` until drop (taken there so the registry can defer the close).
    file: Option<fs::File>,
    pool: Arc<Pool>,
    writable: bool,
    /// Locks this handle holds, by byte.
    held: Mutex<HashMap<u64, LockMode>>,
    #[cfg(all(unix, not(target_os = "linux")))]
    key: FileIdentity,
}

impl fmt::Debug for FileInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreadFile")
            .field("file", &self.file)
            .finish_non_exhaustive()
    }
}

impl FileInner {
    fn file(&self) -> &fs::File {
        self.file.as_ref().expect("file is open until drop")
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        os::read_exact_at(self.file(), buf, offset).map_err(|e| Error::os("read", e))
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        os::write_all_at(self.file(), buf, offset).map_err(|e| Error::os("write", e))
    }

    fn sync_data(&self) -> Result<()> {
        self.file().sync_data().map_err(|e| Error::os("sync", e))
    }
}

impl Drop for FileInner {
    fn drop(&mut self) {
        let held = std::mem::take(self.held.get_mut().unwrap_or_else(PoisonError::into_inner));
        let Some(file) = self.file.take() else { return };
        #[cfg(all(unix, not(target_os = "linux")))]
        registry::close(self.key, file, held);
        #[cfg(not(all(unix, not(target_os = "linux"))))]
        for byte in held.into_keys() {
            // Release explicitly: Windows frees a closed handle's locks only eventually.
            let _ = platform_unlock(&file, byte);
        }
    }
}

#[cfg(target_os = "linux")]
fn platform_lock(
    file: &fs::File,
    held: &mut HashMap<u64, LockMode>,
    byte: u64,
    mode: LockMode,
) -> Result<()> {
    // OFD locks replace atomically: upgrades and downgrades are a single call.
    os::set_lock(file, byte, Some(mode))?;
    held.insert(byte, mode);
    Ok(())
}

#[cfg(target_os = "linux")]
fn platform_unlock(file: &fs::File, byte: u64) -> Result<()> {
    os::set_lock(file, byte, None)
}

#[cfg(windows)]
fn platform_lock(
    file: &fs::File,
    held: &mut HashMap<u64, LockMode>,
    byte: u64,
    mode: LockMode,
) -> Result<()> {
    match held.get(&byte) {
        None => os::lock_range(file, byte, mode)?,
        // A handle may stack a shared lock on its own exclusive one; the next unlock then
        // releases the exclusive lock first, leaving the shared one.
        Some(LockMode::Exclusive) => {
            os::lock_range(file, byte, LockMode::Shared)?;
            os::unlock_range(file, byte)?;
        }
        // An exclusive lock may not overlap any lock, even our own shared one.
        Some(LockMode::Shared) => {
            os::unlock_range(file, byte)?;
            if let Err(e) = os::lock_range(file, byte, LockMode::Exclusive) {
                if os::lock_range(file, byte, LockMode::Shared).is_err() {
                    held.remove(&byte);
                }
                return Err(e);
            }
        }
    }
    held.insert(byte, mode);
    Ok(())
}

#[cfg(windows)]
fn platform_unlock(file: &fs::File, byte: u64) -> Result<()> {
    os::unlock_range(file, byte)
}

/// Per-process lock registry for platforms whose `fcntl` locks are owned by the process.
#[cfg(all(unix, not(target_os = "linux")))]
mod registry {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::{LazyLock, Mutex};

    use super::lock;
    use crate::{Error, ErrorKind, FileIdentity, LockMode, Result, os};

    /// Every descriptor this registry owns is closed only while its lock is held, so no
    /// other thread can take a lock in the window between deciding a close is safe and the
    /// `close(2)` that would drop the process's locks.
    static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(Registry::default()));

    #[derive(Default)]
    struct Registry {
        inodes: HashMap<FileIdentity, Inode>,
        /// Descriptors whose identity could not be read: closing one might drop another
        /// handle's locks, so they stay open for the life of the process (never expected).
        orphans: Vec<fs::File>,
    }

    #[derive(Default)]
    struct Inode {
        handles: usize,
        bytes: HashMap<u64, ByteState>,
        /// Descriptors of closed handles, kept open because closing one would drop the
        /// process's locks that other handles hold.
        deferred: Vec<fs::File>,
    }

    /// Which handles of this process hold a byte.
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    struct ByteState {
        shared: usize,
        exclusive: bool,
    }

    impl ByteState {
        /// The lock the process must hold at the OS level.
        fn level(self) -> Option<LockMode> {
            if self.exclusive {
                Some(LockMode::Exclusive)
            } else if self.shared > 0 {
                Some(LockMode::Shared)
            } else {
                None
            }
        }

        fn without(mut self, mode: Option<LockMode>) -> Self {
            match mode {
                Some(LockMode::Shared) => self.shared -= 1,
                Some(LockMode::Exclusive) => self.exclusive = false,
                None => {}
            }
            self
        }
    }

    impl Inode {
        /// Records `state` for `byte`; called with the registry lock held, so deferred
        /// descriptors close under it.
        fn set(&mut self, byte: u64, state: ByteState) {
            if state == ByteState::default() {
                self.bytes.remove(&byte);
                if self.bytes.is_empty() {
                    // No locks left: deferred descriptors can close safely.
                    self.deferred.clear();
                }
            } else {
                self.bytes.insert(byte, state);
            }
        }

        /// Releases one handle's lock on `byte`, telling the OS first.
        fn release(&mut self, file: &fs::File, byte: u64, prev: LockMode) -> Result<()> {
            let before = self.bytes.get(&byte).copied().unwrap_or_default();
            let after = before.without(Some(prev));
            if after.level() != before.level() {
                os::set_lock(file, byte, after.level())?;
            }
            self.set(byte, after);
            Ok(())
        }
    }

    /// Registers a newly opened descriptor, returning its key. On failure the descriptor is
    /// parked, never closed (see `Registry::orphans`).
    pub(super) fn register(file: fs::File) -> Result<(fs::File, FileIdentity)> {
        let mut reg = lock(&REGISTRY);
        match os::identity(&file) {
            Ok(key) => {
                reg.inodes.entry(key).or_default().handles += 1;
                Ok((file, key))
            }
            Err(e) => {
                reg.orphans.push(file);
                Err(e)
            }
        }
    }

    pub(super) fn lock_byte(
        key: FileIdentity,
        file: &fs::File,
        held: &mut HashMap<u64, LockMode>,
        byte: u64,
        mode: LockMode,
    ) -> Result<()> {
        let mut reg = lock(&REGISTRY);
        let inode = reg.inodes.get_mut(&key).expect("handle is registered");
        let before = inode.bytes.get(&byte).copied().unwrap_or_default();
        let mut after = before.without(held.get(&byte).copied());
        let conflict = match mode {
            LockMode::Exclusive => after.exclusive || after.shared > 0,
            LockMode::Shared => after.exclusive,
        };
        if conflict {
            return Err(Error::new(ErrorKind::Locked, "lock"));
        }
        match mode {
            LockMode::Exclusive => after.exclusive = true,
            LockMode::Shared => after.shared += 1,
        }
        if after.level() != before.level() {
            os::set_lock(file, byte, after.level())?;
        }
        inode.set(byte, after);
        held.insert(byte, mode);
        Ok(())
    }

    pub(super) fn unlock_byte(
        key: FileIdentity,
        file: &fs::File,
        held: &mut HashMap<u64, LockMode>,
        byte: u64,
    ) -> Result<()> {
        let Some(&prev) = held.get(&byte) else {
            return Ok(());
        };
        let mut reg = lock(&REGISTRY);
        let inode = reg.inodes.get_mut(&key).expect("handle is registered");
        inode.release(file, byte, prev)?;
        held.remove(&byte);
        Ok(())
    }

    pub(super) fn close(key: FileIdentity, file: fs::File, held: HashMap<u64, LockMode>) {
        let mut reg = lock(&REGISTRY);
        let inode = reg.inodes.get_mut(&key).expect("handle is registered");
        for (byte, prev) in held {
            // Best effort: on failure the byte stays recorded as held, so the descriptor is
            // deferred below rather than closed under someone else's lock.
            let _ = inode.release(&file, byte, prev);
        }
        inode.handles -= 1;
        if inode.handles == 0 {
            reg.inodes.remove(&key);
            drop(file);
        } else if inode.bytes.is_empty() {
            drop(file);
        } else {
            inode.deferred.push(file);
        }
        // `reg` (the registry lock) is released only after the close above.
        drop(reg);
    }
}

impl File for PreadFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.inner.read_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.inner.write_at(buf, offset)
    }

    fn submit_read(&self, mut buf: IoBuf, offset: u64) -> Completion {
        let (done, resolver) = Completion::pair();
        let inner = Arc::clone(&self.inner);
        self.inner.pool.submit(Box::new(move || {
            resolver.resolve(inner.read_at(&mut buf, offset).map(|()| buf));
        }));
        done
    }

    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        let (done, resolver) = Completion::pair();
        let inner = Arc::clone(&self.inner);
        self.inner.pool.submit(Box::new(move || {
            resolver.resolve(inner.write_at(&buf, offset).map(|()| buf));
        }));
        done
    }

    fn sync_data(&self) -> Result<()> {
        self.inner.sync_data()
    }

    fn submit_sync_data(&self) -> Completion<()> {
        let (done, resolver) = Completion::pair();
        let inner = Arc::clone(&self.inner);
        self.inner
            .pool
            .submit(Box::new(move || resolver.resolve(inner.sync_data())));
        done
    }

    fn sync_all(&self) -> Result<()> {
        self.inner
            .file()
            .sync_all()
            .map_err(|e| Error::os("sync", e))
    }

    fn submit_sync_all(&self) -> Completion<()> {
        let (done, resolver) = Completion::pair();
        let inner = Arc::clone(&self.inner);
        self.inner.pool.submit(Box::new(move || {
            resolver.resolve(inner.file().sync_all().map_err(|e| Error::os("sync", e)))
        }));
        done
    }

    fn len(&self) -> Result<u64> {
        Ok(self
            .inner
            .file()
            .metadata()
            .map_err(|e| Error::os("stat", e))?
            .len())
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.inner
            .file()
            .set_len(len)
            .map_err(|e| Error::os("set_len", e))
    }

    fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        os::allocate(self.inner.file(), offset, len)
    }

    fn lock(&self, byte: u64, mode: LockMode) -> Result<()> {
        let mut held = lock(&self.inner.held);
        if held.get(&byte) == Some(&mode) {
            return Ok(());
        }
        if mode == LockMode::Exclusive && !self.inner.writable {
            return Err(exclusive_needs_write());
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        return registry::lock_byte(self.inner.key, self.inner.file(), &mut held, byte, mode);
        #[cfg(not(all(unix, not(target_os = "linux"))))]
        return platform_lock(self.inner.file(), &mut held, byte, mode);
    }

    fn unlock(&self, byte: u64) -> Result<()> {
        let mut held = lock(&self.inner.held);
        #[cfg(all(unix, not(target_os = "linux")))]
        return registry::unlock_byte(self.inner.key, self.inner.file(), &mut held, byte);
        #[cfg(not(all(unix, not(target_os = "linux"))))]
        match held.remove(&byte) {
            Some(_) => platform_unlock(self.inner.file(), byte),
            None => Ok(()),
        }
    }

    fn identity(&self) -> Result<FileIdentity> {
        os::identity(self.inner.file())
    }

    fn is_local(&self) -> Result<bool> {
        os::is_local(self.inner.file())
    }
}

/// POSIX refuses write locks on read-only descriptors; every backend refuses alike.
pub(crate) fn exclusive_needs_write() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "exclusive lock needs a handle opened for writing",
    )
}

/// Path of a region placed in an explicit directory (FORMAT §11).
fn region_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}.phdb-shm"))
}

impl Vfs for PreadVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        let mut o = fs::OpenOptions::new();
        o.read(true)
            .write(opts.write || opts.create || opts.create_new)
            .create(opts.create && !opts.create_new)
            .create_new(opts.create_new);
        let file = o.open(path).map_err(|e| Error::os("open", e))?;
        #[cfg(all(unix, not(target_os = "linux")))]
        let (file, key) = registry::register(file)?;
        Ok(Arc::new(PreadFile {
            inner: Arc::new(FileInner {
                file: Some(file),
                pool: Arc::clone(&self.pool),
                writable: opts.write || opts.create || opts.create_new,
                held: Mutex::new(HashMap::new()),
                #[cfg(all(unix, not(target_os = "linux")))]
                key,
            }),
        }))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        fs::remove_file(path).map_err(|e| Error::os("remove", e))
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        path.try_exists().map_err(|e| Error::os("exists", e))
    }

    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let mut paths = fs::read_dir(dir)
            .map_err(|e| Error::os("list directory", e))?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|e| Error::os("list directory", e))?;
        paths.sort();
        Ok(paths)
    }

    fn sync_dir(&self, dir: &Path) -> Result<()> {
        os::sync_dir(dir)
    }

    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion> {
        os::check_shared_name(name)?;
        if len == 0 {
            return Err(Error::new(ErrorKind::Other, "shared region is empty"));
        }
        let Some(dir) = dir else {
            return os::open_default_shared(name, len, mode).map(SharedRegion::mapped);
        };
        let path = region_path(dir, name);
        let file = os::open_region_file(&path, len, mode)?;
        let size =
            usize::try_from(len).map_err(|_| Error::new(ErrorKind::Other, "region too large"))?;
        let mapping = os::map_file(&file, size).inspect_err(|_| {
            if mode == SharedOpen::CreateNew {
                let _ = fs::remove_file(&path);
            }
        })?;
        Ok(SharedRegion::mapped(mapping))
    }

    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()> {
        os::check_shared_name(name)?;
        match dir {
            Some(dir) => fs::remove_file(region_path(dir, name))
                .map_err(|e| Error::os("remove shared region", e)),
            None => os::remove_default_shared(name),
        }
    }

    fn now_micros(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64)
    }

    fn monotonic_nanos(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }

    fn current_process(&self) -> ProcessId {
        self.process
    }

    fn process_alive(&self, process: ProcessId) -> bool {
        process == self.process || os::process_alive(process.pid, process.start_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panicking_job_fails_its_completion_and_spares_the_worker() {
        let pool = Pool::start(1);
        let (done, resolver) = Completion::<u8>::pair();
        pool.submit(Box::new(move || {
            let _owned = resolver;
            panic!("injected job panic");
        }));
        assert_eq!(done.wait().unwrap_err().kind, ErrorKind::Other);

        let (done, resolver) = Completion::<u8>::pair();
        pool.submit(Box::new(move || resolver.resolve(Ok(5))));
        assert_eq!(done.wait().unwrap(), 5, "the only worker survived");
    }
}
