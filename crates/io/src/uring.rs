//! The io_uring backend (Linux, #402): real files whose submitted I/O goes through an
//! io_uring ring instead of a thread pool.
//!
//! Files are opened, locked and read or written synchronously exactly as by
//! [`PreadVfs`]; only the submitted operations (`submit_read`, `submit_write`,
//! `submit_sync_data`, `submit_sync_all`) differ. They go to one shared ring, and one
//! reaper thread waits for their completions and resolves each operation's [`Completion`],
//! waking its waiter or task. Per-shard rings reaped on the shard thread come next (#402).
//!
//! Every `unsafe` block of the backend is here. The invariant they rely on: an operation in
//! flight owns its buffer and its file handle in the ring's table of operations until its
//! completion has been reaped, so the memory and descriptor the kernel uses stay valid even
//! when the caller drops its [`Completion`]; and the ring is torn down only after every
//! operation in flight has completed.

use std::cell::RefCell;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle, ThreadId};
use std::time::Duration;

use io_uring::{IoUring, Probe, opcode, squeue, types};

use crate::completion::Resolver;
use crate::pread::{PreadFile, PreadVfs};
use crate::{
    Completion, Error, ErrorKind, File, FileIdentity, FileRef, IoBuf, Locality, LockMode,
    OpenOptions, ProcessId, Result, SharedOpen, SharedRegion, Vfs,
};

/// Submission queue entries of the shared ring (completions get twice as many).
const RING_ENTRIES: u32 = 256;

/// The `user_data` of the no-op that wakes the reaper to shut down.
const SHUTDOWN: u64 = u64::MAX;

/// Real files with submitted I/O on io_uring.
///
/// [`UringVfs::new`] probes the kernel and fails with [`ErrorKind::Unsupported`] when
/// io_uring is unavailable (an old kernel, a seccomp profile, `kernel.io_uring_disabled`),
/// so the caller can use [`PreadVfs`] instead.
pub struct UringVfs {
    files: Arc<PreadVfs>,
    ring: Arc<RingHandle>,
}

impl UringVfs {
    /// Probes io_uring and starts the shared ring and its reaper thread.
    pub fn new() -> Result<Arc<Self>> {
        let ring = RingHandle::start()?;
        Ok(Arc::new(Self {
            files: PreadVfs::without_pool(),
            ring,
        }))
    }
}

impl fmt::Debug for UringVfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UringVfs").finish_non_exhaustive()
    }
}

impl Vfs for UringVfs {
    /// Gives the calling thread a ring of its own: what it submits from then on goes there,
    /// and it reaps the completions itself (in its turns and its waits). Where the kernel
    /// lacks what such a ring needs, the thread keeps using the shared ring.
    fn attach_thread(&self) {
        ThreadRing::attach(self.ring.id());
    }

    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        Ok(Arc::new(UringFile {
            file: self.files.open_file(path, opts)?,
            ring: Arc::clone(&self.ring),
        }))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        self.files.remove(path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        self.files.exists(path)
    }

    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        self.files.list_dir(dir)
    }

    fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.files.sync_dir(dir)
    }

    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion> {
        self.files.open_shared(name, dir, len, mode)
    }

    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()> {
        self.files.remove_shared(name, dir)
    }

    fn now_micros(&self) -> u64 {
        self.files.now_micros()
    }

    fn monotonic_nanos(&self) -> u64 {
        self.files.monotonic_nanos()
    }

    fn current_process(&self) -> ProcessId {
        self.files.current_process()
    }

    fn process_alive(&self, process: ProcessId) -> bool {
        self.files.process_alive(process)
    }
}

/// A file of [`UringVfs`]: synchronous calls as [`PreadFile`]'s, submitted ones on the ring.
struct UringFile {
    file: Arc<PreadFile>,
    ring: Arc<RingHandle>,
}

impl fmt::Debug for UringFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UringFile").finish_non_exhaustive()
    }
}

impl UringFile {
    /// Submits the operation `kind` builds: to the calling thread's own ring for this
    /// backend if it attached one (completed when it reaps), else to the shared ring.
    fn submit<T: Send + 'static>(&self, kind: impl FnOnce(Resolver<T>) -> Kind) -> Completion<T> {
        match ThreadRing::current(self.ring.id()) {
            Some(ring) => {
                let (done, resolver) = Completion::driven_pair(Some(ring.drive()));
                ring.submit(Op {
                    file: Arc::clone(&self.file),
                    kind: kind(resolver),
                });
                done
            }
            None => {
                let (done, resolver) = Completion::pair();
                self.ring.ring.submit(Op {
                    file: Arc::clone(&self.file),
                    kind: kind(resolver),
                });
                done
            }
        }
    }
}

impl File for UringFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.file.read_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.file.write_at(buf, offset)
    }

    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion {
        self.submit(|resolver| Kind::Read {
            buf,
            offset,
            done: 0,
            resolver,
        })
    }

    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        self.submit(|resolver| Kind::Write {
            buf,
            offset,
            done: 0,
            resolver,
        })
    }

    fn sync_data(&self) -> Result<()> {
        self.file.sync_data()
    }

    fn submit_sync_data(&self) -> Completion<()> {
        self.submit(|resolver| Kind::Sync {
            data_only: true,
            resolver,
        })
    }

    fn sync_all(&self) -> Result<()> {
        self.file.sync_all()
    }

    fn submit_sync_all(&self) -> Completion<()> {
        self.submit(|resolver| Kind::Sync {
            data_only: false,
            resolver,
        })
    }

    fn len(&self) -> Result<u64> {
        self.file.len()
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.file.set_len(len)
    }

    fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        self.file.allocate(offset, len)
    }

    fn lock(&self, byte: u64, mode: LockMode) -> Result<()> {
        self.file.lock(byte, mode)
    }

    fn unlock(&self, byte: u64) -> Result<()> {
        self.file.unlock(byte)
    }

    fn identity(&self) -> Result<FileIdentity> {
        self.file.identity()
    }

    fn is_local(&self) -> Result<bool> {
        self.file.is_local()
    }

    fn locality(&self) -> Result<Locality> {
        self.file.locality()
    }
}

/// An operation in flight: what the kernel uses (its buffer, its file's descriptor) stays
/// here until the operation's completion is reaped.
struct Op {
    file: Arc<PreadFile>,
    kind: Kind,
}

enum Kind {
    /// Reads `buf` in full at `offset`; `done` bytes are in so far (a short read continues).
    Read {
        buf: IoBuf,
        offset: u64,
        done: usize,
        resolver: Resolver<IoBuf>,
    },
    /// Writes `buf` in full at `offset`; `done` bytes are out so far.
    Write {
        buf: IoBuf,
        offset: u64,
        done: usize,
        resolver: Resolver<IoBuf>,
    },
    Sync {
        data_only: bool,
        resolver: Resolver<()>,
    },
}

impl Op {
    /// The submission entry for the rest of the operation, tagged `user_data`.
    fn entry(&mut self, user_data: u64) -> squeue::Entry {
        let fd = types::Fd(self.file.raw_fd());
        match &mut self.kind {
            Kind::Read {
                buf, offset, done, ..
            } => {
                let rest = &mut buf[*done..];
                opcode::Read::new(fd, rest.as_mut_ptr(), chunk(rest.len()))
                    .offset(*offset + *done as u64)
                    .build()
            }
            Kind::Write {
                buf, offset, done, ..
            } => {
                let rest = &buf[*done..];
                opcode::Write::new(fd, rest.as_ptr(), chunk(rest.len()))
                    .offset(*offset + *done as u64)
                    .build()
            }
            Kind::Sync { data_only, .. } => {
                let flags = if *data_only {
                    types::FsyncFlags::DATASYNC
                } else {
                    types::FsyncFlags::empty()
                };
                opcode::Fsync::new(fd).flags(flags).build()
            }
        }
        .user_data(user_data)
    }

    /// Applies a completion's result. Returns the operation back when it must continue (a
    /// short read or write), or the resolution to deliver.
    fn complete(mut self, res: i32) -> Step {
        if res < 0 {
            let e = io::Error::from_raw_os_error(-res);
            return Step::Done(match self.kind {
                Kind::Read { resolver, .. } => {
                    Box::new(move || resolver.resolve(Err(Error::os("read", e))))
                }
                Kind::Write { resolver, .. } => {
                    Box::new(move || resolver.resolve(Err(Error::os("write", e))))
                }
                Kind::Sync { resolver, .. } => {
                    Box::new(move || resolver.resolve(Err(Error::os("sync", e))))
                }
            });
        }
        let n = res as usize;
        match &mut self.kind {
            Kind::Read { buf, done, .. } | Kind::Write { buf, done, .. } => {
                if n == 0 && *done < buf.len() {
                    let (Kind::Read { resolver, .. } | Kind::Write { resolver, .. }) = self.kind
                    else {
                        unreachable!()
                    };
                    return Step::Done(Box::new(move || {
                        resolver.resolve(Err(Error::new(ErrorKind::UnexpectedEof, "read")))
                    }));
                }
                *done += n;
                if *done < buf.len() {
                    return Step::Again(self);
                }
                let (Kind::Read { buf, resolver, .. } | Kind::Write { buf, resolver, .. }) =
                    self.kind
                else {
                    unreachable!()
                };
                Step::Done(Box::new(move || resolver.resolve(Ok(buf))))
            }
            Kind::Sync { .. } => {
                let Kind::Sync { resolver, .. } = self.kind else {
                    unreachable!()
                };
                Step::Done(Box::new(move || resolver.resolve(Ok(()))))
            }
        }
    }
}

/// What a completion leads to.
enum Step {
    /// The operation continues with the rest of its buffer.
    Again(Op),
    /// It is over: resolve it (outside every lock: the completion's continuations may submit).
    Done(Box<dyn FnOnce() + Send>),
}

/// The length of one read or write: the kernel takes a `u32`.
fn chunk(len: usize) -> u32 {
    len.min(1 << 30) as u32
}

/// The operations in flight, by `user_data` (their index here).
#[derive(Default)]
struct Table {
    ops: Vec<Option<Op>>,
    free: Vec<usize>,
    live: usize,
}

impl Table {
    fn insert(&mut self, op: Op) -> u64 {
        self.live += 1;
        match self.free.pop() {
            Some(i) => {
                self.ops[i] = Some(op);
                i as u64
            }
            None => {
                self.ops.push(Some(op));
                (self.ops.len() - 1) as u64
            }
        }
    }

    fn take(&mut self, user_data: u64) -> Option<Op> {
        let op = self.ops.get_mut(user_data as usize)?.take()?;
        self.free.push(user_data as usize);
        self.live -= 1;
        Some(op)
    }
}

/// One ring and its table of operations in flight.
struct Ring {
    uring: IoUring,
    /// Held while pushing to the submission queue (one pusher at a time).
    sq: Mutex<()>,
    table: Mutex<Table>,
    /// Set when the last handle goes: the reaper ends once nothing is in flight.
    shutdown: AtomicBool,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Ring {
    /// A ring whose kernel supports every operation this backend submits.
    fn probe() -> Result<Self> {
        let unsupported = |e: io::Error| Error {
            kind: ErrorKind::Unsupported,
            context: "io_uring",
            source: Some(e),
        };
        let uring = IoUring::builder()
            .setup_cqsize(RING_ENTRIES * 2)
            .build(RING_ENTRIES)
            .map_err(unsupported)?;
        let mut probe = Probe::new();
        uring
            .submitter()
            .register_probe(&mut probe)
            .map_err(unsupported)?;
        let needed = [
            opcode::Nop::CODE,
            opcode::Read::CODE,
            opcode::Write::CODE,
            opcode::Fsync::CODE,
        ];
        if !needed.iter().all(|&op| probe.is_supported(op)) {
            return Err(Error::new(ErrorKind::Unsupported, "io_uring operations"));
        }
        Ok(Self {
            uring,
            sq: Mutex::new(()),
            table: Mutex::new(Table::default()),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Submits `op`. A failure to submit resolves it with that error.
    fn submit(&self, op: Op) {
        let mut table = lock(&self.table);
        let user_data = table.insert(op);
        let entry = table.ops[user_data as usize]
            .as_mut()
            .expect("inserted above")
            .entry(user_data);
        drop(table);
        if let Err(e) = self.push(&entry)
            && let Some(op) = lock(&self.table).take(user_data)
        {
            // Not submitted: the kernel never saw its buffer.
            fail(op, e);
        }
    }

    /// Pushes `entry` and tells the kernel, waiting for room in a full queue.
    fn push(&self, entry: &squeue::Entry) -> io::Result<()> {
        let _sq = lock(&self.sq);
        loop {
            // SAFETY: `self.sq` is held, so this is the only submission queue borrowed. The
            // entry's buffer and descriptor belong to an operation in `self.table`, which
            // keeps them until the entry's completion is reaped.
            let pushed = unsafe {
                let mut sq = self.uring.submission_shared();
                let r = sq.push(entry);
                sq.sync();
                r
            };
            match pushed {
                Ok(()) => break,
                // Full: hand the queued entries to the kernel to make room.
                Err(_) => {
                    self.uring.submit()?;
                }
            }
        }
        self.uring.submit().map(drop)
    }

    /// The reaper's loop: waits for completions and resolves them until shut down with
    /// nothing in flight.
    fn reap(&self) {
        loop {
            match self.uring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(_) => {
                    // The ring is unusable: fail what is in flight rather than hang.
                    self.fail_all();
                    return;
                }
            }
            let mut done = Vec::new();
            let mut again = Vec::new();
            {
                // SAFETY: only this thread (the reaper) ever borrows the completion queue.
                let mut cq = unsafe { self.uring.completion_shared() };
                cq.sync();
                let mut table = lock(&self.table);
                for cqe in &mut cq {
                    if cqe.user_data() == SHUTDOWN {
                        continue;
                    }
                    let Some(op) = table.take(cqe.user_data()) else {
                        continue;
                    };
                    match op.complete(cqe.result()) {
                        Step::Done(resolve) => done.push(resolve),
                        Step::Again(op) => again.push(op),
                    }
                }
            }
            for op in again {
                self.submit(op);
            }
            for resolve in done {
                resolve();
            }
            if self.shutdown.load(Ordering::Acquire) && lock(&self.table).live == 0 {
                return;
            }
        }
    }

    /// Fails every operation in flight (the ring can no longer complete them).
    fn fail_all(&self) {
        let ops: Vec<Op> = {
            let mut table = lock(&self.table);
            let n = table.ops.len() as u64;
            (0..n).filter_map(|i| table.take(i)).collect()
        };
        for op in ops {
            fail(op, io::Error::other("io_uring failed"));
        }
    }
}

/// Resolves `op` with `e`.
fn fail(op: Op, e: io::Error) {
    match op.kind {
        Kind::Read { resolver, .. } => resolver.resolve(Err(Error::os("read", e))),
        Kind::Write { resolver, .. } => resolver.resolve(Err(Error::os("write", e))),
        Kind::Sync { resolver, .. } => resolver.resolve(Err(Error::os("sync", e))),
    }
}

/// The shared ring and its reaper thread; dropping the last handle stops the reaper once
/// everything in flight has completed.
struct RingHandle {
    ring: Arc<Ring>,
    reaper: Mutex<Option<JoinHandle<()>>>,
    reaper_id: ThreadId,
}

impl RingHandle {
    /// Identifies the backend this handle belongs to (for a thread's own rings).
    fn id(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    fn start() -> Result<Arc<Self>> {
        let ring = Arc::new(Ring::probe()?);
        let r = Arc::clone(&ring);
        let reaper = thread::Builder::new()
            .name("pigeonhole-uring".into())
            .spawn(move || r.reap())
            .map_err(|e| Error::os("spawn the io_uring reaper", e))?;
        Ok(Arc::new(Self {
            reaper_id: reaper.thread().id(),
            ring,
            reaper: Mutex::new(Some(reaper)),
        }))
    }
}

impl Drop for RingHandle {
    fn drop(&mut self) {
        self.ring.shutdown.store(true, Ordering::Release);
        // Wake the reaper; it ends once nothing is in flight.
        let nop = opcode::Nop::new().build().user_data(SHUTDOWN);
        let _ = self.ring.push(&nop);
        let reaper = lock(&self.reaper).take();
        // The last handle can go on the reaper itself (a continuation holding a file): it
        // then ends on its own after this call.
        if thread::current().id() != self.reaper_id
            && let Some(r) = reaper
        {
            let _ = r.join();
        }
    }
}

/// The `user_data` of the poll that wakes a thread's ring wait.
const WAKE: u64 = u64::MAX - 1;

/// Longest a waiting [`Completion`] on a ring's owner thread reaps before it checks its
/// result again.
const DRIVE_SLICE: Duration = Duration::from_millis(1);

thread_local! {
    /// The rings this thread attached, one per backend.
    static THREAD_RINGS: RefCell<Vec<Arc<ThreadRing>>> = const { RefCell::new(Vec::new()) };
}

/// A ring owned by one thread (a shard's, #402): only that thread submits to it and reaps
/// it, in its turns ([`crate::reap_own_io`] without waiting), in its idle waits and while it
/// blocks on one of its completions. Other threads wake its waits through an eventfd the
/// ring polls ([`crate::OwnIoWaker`]).
struct ThreadRing {
    uring: IoUring,
    table: Mutex<Table>,
    owner: ThreadId,
    /// The backend ([`RingHandle::id`]) the ring belongs to.
    backend: usize,
    wake: Arc<EventFd>,
}

/// An eventfd, closed on drop.
struct EventFd(std::os::fd::OwnedFd);

impl EventFd {
    fn new() -> io::Result<Self> {
        use std::os::fd::FromRawFd;
        // SAFETY: plain syscall; a non-negative result is a descriptor we now own.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` was just returned by `eventfd` and is owned by nobody else.
        Ok(Self(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) }))
    }

    fn raw(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.0.as_raw_fd()
    }

    /// Adds one to the counter (wakes a poll on it).
    fn signal(&self) {
        let one = 1u64;
        // SAFETY: writes 8 bytes from a live `u64` to a descriptor this value owns. A full
        // counter (EAGAIN) means the poll is already due, which is all a wake needs.
        let _ = unsafe { libc::write(self.raw(), std::ptr::from_ref(&one).cast(), 8) };
    }

    /// Resets the counter.
    fn drain(&self) {
        let mut n = 0u64;
        // SAFETY: reads 8 bytes into a live `u64`; EAGAIN (already zero) is fine.
        let _ = unsafe { libc::read(self.raw(), std::ptr::from_mut(&mut n).cast(), 8) };
    }
}

impl ThreadRing {
    /// The calling thread's ring for `backend`, if it attached one.
    fn current(backend: usize) -> Option<Arc<Self>> {
        THREAD_RINGS
            .try_with(|r| r.borrow().iter().find(|t| t.backend == backend).cloned())
            .ok()
            .flatten()
    }

    /// Gives the calling thread a ring for `backend` (once). Without the kernel support it
    /// needs (timed waits), the thread keeps using the shared ring.
    fn attach(backend: usize) {
        if Self::current(backend).is_some() {
            return;
        }
        let Some(ring) = Self::new(backend) else {
            return;
        };
        let own: std::sync::Weak<dyn crate::own::OwnIo> = Arc::downgrade(&ring) as _;
        crate::own::register(own);
        let _ = THREAD_RINGS.try_with(|r| r.borrow_mut().push(ring));
    }

    fn new(backend: usize) -> Option<Arc<Self>> {
        // Completions run only when this thread enters the ring (no work on other threads),
        // where the kernel offers it (6.1); a plain ring otherwise. The task-run flag tells
        // a non-waiting reap that deferred completions are pending, so its enter carries
        // GETEVENTS and runs them (without it, only a waiting enter would: a busy shard, or
        // an application loop that never waits, would never see its completions).
        let uring = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_taskrun_flag()
            .setup_cqsize(RING_ENTRIES * 2)
            .build(RING_ENTRIES)
            .or_else(|_| {
                IoUring::builder()
                    .setup_cqsize(RING_ENTRIES * 2)
                    .build(RING_ENTRIES)
            })
            .ok()?;
        if !uring.params().is_feature_ext_arg() {
            return None;
        }
        let mut probe = Probe::new();
        uring.submitter().register_probe(&mut probe).ok()?;
        if !probe.is_supported(opcode::PollAdd::CODE) {
            return None;
        }
        let ring = Arc::new(Self {
            uring,
            table: Mutex::new(Table::default()),
            owner: thread::current().id(),
            backend,
            wake: Arc::new(EventFd::new().ok()?),
        });
        ring.arm_wake().ok()?;
        Some(ring)
    }

    /// Polls the eventfd, so a signal ends the ring's wait.
    fn arm_wake(&self) -> io::Result<()> {
        let poll = opcode::PollAdd::new(types::Fd(self.wake.raw()), libc::POLLIN as u32)
            .build()
            .user_data(WAKE);
        self.push(&poll)
    }

    /// Pushes `entry` and hands it to the kernel. Only the owner thread calls this.
    fn push(&self, entry: &squeue::Entry) -> io::Result<()> {
        debug_assert_eq!(thread::current().id(), self.owner);
        loop {
            // SAFETY: only the owner thread borrows this ring's submission queue, and never
            // re-entrantly (no callback runs during the borrow). The entry's buffer and
            // descriptor belong to an operation in `self.table` until its completion is
            // reaped.
            let pushed = unsafe {
                let mut sq = self.uring.submission_shared();
                let r = sq.push(entry);
                sq.sync();
                r
            };
            match pushed {
                Ok(()) => break,
                Err(_) => {
                    self.uring.submit()?;
                }
            }
        }
        self.uring.submit().map(drop)
    }

    fn submit(&self, op: Op) {
        let mut table = lock(&self.table);
        let user_data = table.insert(op);
        let entry = table.ops[user_data as usize]
            .as_mut()
            .expect("inserted above")
            .entry(user_data);
        drop(table);
        if let Err(e) = self.push(&entry)
            && let Some(op) = lock(&self.table).take(user_data)
        {
            fail(op, e);
        }
    }

    /// What a blocked [`Completion::wait`] on one of this ring's operations calls: reaps
    /// while on the owner thread, and leaves the wait to the condvar elsewhere.
    fn drive(self: &Arc<Self>) -> crate::completion::Drive {
        let ring = Arc::downgrade(self);
        let owner = self.owner;
        Arc::new(move || {
            if thread::current().id() != owner {
                return false;
            }
            match ring.upgrade() {
                Some(r) => {
                    r.reap(Some(DRIVE_SLICE));
                    true
                }
                None => false,
            }
        })
    }

    /// Takes the finished completions, waiting up to `wait` for one when none has (`None`:
    /// do not wait). Returns whether an operation completed.
    fn reap(&self, wait: Option<Duration>) -> bool {
        debug_assert_eq!(thread::current().id(), self.owner);
        let entered = match wait {
            Some(t) if lock(&self.table).live > 0 || !t.is_zero() => {
                let ts = types::Timespec::new()
                    .sec(t.as_secs())
                    .nsec(t.subsec_nanos());
                let args = types::SubmitArgs::new().timespec(&ts);
                self.uring.submitter().submit_with_args(1, &args)
            }
            // Runs this thread's deferred completion work without waiting.
            _ => self
                .uring
                .submitter()
                .submit_with_args(0, &types::SubmitArgs::new()),
        };
        if let Err(e) = entered
            && !matches!(
                e.raw_os_error(),
                Some(libc::ETIME | libc::EINTR | libc::EBUSY)
            )
        {
            // The ring is unusable: fail what is in flight rather than hang.
            let ops: Vec<Op> = {
                let mut table = lock(&self.table);
                let n = table.ops.len() as u64;
                (0..n).filter_map(|i| table.take(i)).collect()
            };
            let any = !ops.is_empty();
            for op in ops {
                fail(op, io::Error::other("io_uring failed"));
            }
            return any;
        }
        let mut done = Vec::new();
        let mut again = Vec::new();
        let mut woke = false;
        {
            // SAFETY: only the owner thread borrows this ring's completion queue, and the
            // borrow ends before any completion's continuation runs.
            let mut cq = unsafe { self.uring.completion_shared() };
            cq.sync();
            let mut table = lock(&self.table);
            for cqe in &mut cq {
                if cqe.user_data() == WAKE {
                    woke = true;
                    continue;
                }
                let Some(op) = table.take(cqe.user_data()) else {
                    continue;
                };
                match op.complete(cqe.result()) {
                    Step::Done(resolve) => done.push(resolve),
                    Step::Again(op) => again.push(op),
                }
            }
        }
        if woke {
            self.wake.drain();
            // Re-armed for the next wake; a failure leaves waits to their timeouts.
            let _ = self.arm_wake();
        }
        for op in again {
            self.submit(op);
        }
        let any = !done.is_empty();
        for resolve in done {
            resolve();
        }
        any
    }
}

impl crate::own::OwnIo for ThreadRing {
    fn in_flight_here(&self) -> bool {
        thread::current().id() == self.owner && lock(&self.table).live > 0
    }

    fn reap_here(&self, wait: Option<Duration>) -> bool {
        thread::current().id() == self.owner && self.reap(wait)
    }

    fn waker_here(&self) -> Option<crate::OwnIoWaker> {
        if thread::current().id() != self.owner {
            return None;
        }
        let wake = Arc::clone(&self.wake);
        Some(crate::OwnIoWaker(Arc::new(move || wake.signal())))
    }
}

impl Drop for ThreadRing {
    /// The owner thread is ending (its thread-local rings go): completes what is still in
    /// flight before the ring, and the buffers the kernel writes into, go.
    fn drop(&mut self) {
        if thread::current().id() != self.owner {
            // Only the owner can reap a single-issuer ring. The last strong handle is
            // the owner's thread-local one, so this is unreachable in practice.
            return;
        }
        while lock(&self.table).live > 0 {
            self.reap(Some(Duration::from_millis(10)));
        }
    }
}
