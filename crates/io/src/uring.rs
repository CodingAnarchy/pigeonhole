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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::{self, JoinHandle, ThreadId};
use std::time::Duration;

use io_uring::{IoUring, Probe, opcode, squeue, types};

use crate::buf::SlotPool;
use crate::completion::Resolver;
use crate::pread::{PreadFile, PreadVfs};
use crate::{
    Completion, Error, ErrorKind, File, FileIdentity, FileRef, IoBuf, Locality, LockMode,
    OpenOptions, ProcessId, Result, SharedOpen, SharedRegion, Vfs,
};

/// Submission queue entries of the shared ring (completions get twice as many).
const RING_ENTRIES: u32 = 256;

/// Registered buffer slots per ring (64 KiB each: an SST block or a run of them), at most:
/// 1 MiB a ring.
const POOL_SLOTS: u32 = 16;
const POOL_SLOT_LEN: usize = 64 << 10;

/// Bytes of registered pools this process holds now (see [`memlock_budget`]).
static REGISTERED: AtomicUsize = AtomicUsize::new(0);

/// The most this process registers as pools, in bytes: half its locked-memory limit.
///
/// Registered buffers are pinned pages, charged to the user's locked-memory limit
/// (RLIMIT_MEMLOCK, often 8 MiB, shared by every ring of every process of the user) unless the
/// process has CAP_IPC_LOCK. Since Linux 5.12 a ring's own memory is charged to the memory
/// cgroup instead, but kernels that allocate rings as accounted regions charge it to
/// RLIMIT_MEMLOCK again: pools that took the whole limit would leave no room to create a
/// ring at all. So pools take at most half, and the other half stays for rings.
fn memlock_budget() -> usize {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid `rlimit` to fill.
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) } != 0 {
        return 0;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return usize::MAX;
    }
    usize::try_from(limit.rlim_cur / 2).unwrap_or(usize::MAX)
}

/// A backend's rings now, and how many have a registered pool ([`Vfs::ring_stats`]).
#[derive(Default)]
struct RingCounts {
    rings: AtomicUsize,
    pooled: AtomicUsize,
}

/// One ring's place in its backend's [`RingCounts`], given back when the ring goes.
struct Counted {
    counts: Arc<RingCounts>,
}

impl Counted {
    fn new(counts: &Arc<RingCounts>) -> Self {
        counts.rings.fetch_add(1, Ordering::Relaxed);
        Self {
            counts: Arc::clone(counts),
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.counts.rings.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A ring's pool, registered on first use ([`File::read_buf`]): pinned memory only in
/// processes that read through it. Pinning at ring creation instead made every process with
/// a ring hold pools, and the user's processes together then left none of the shared
/// locked-memory limit for other processes to create rings in.
#[derive(Default)]
struct LazyPool(OnceLock<Option<Registered>>);

impl LazyPool {
    /// The pool, registering it with `uring` on the first call. On a thread's ring, called
    /// only by its owner (a single-issuer ring takes registrations from its owner alone).
    fn get_or_register(&self, uring: &IoUring, counts: &Arc<RingCounts>) -> Option<&Arc<SlotPool>> {
        self.0
            .get_or_init(|| register_pool(uring, counts))
            .as_ref()
            .map(|r| &r.pool)
    }

    /// The pool if it was registered, without registering it (to submit with).
    fn registered(&self) -> Option<&Arc<SlotPool>> {
        self.0.get().and_then(|r| r.as_ref()).map(|r| &r.pool)
    }
}

/// A pool registered with a ring, counted in [`REGISTERED`] and in its backend's
/// [`RingCounts`] until the ring holding it goes.
struct Registered {
    pool: Arc<SlotPool>,
    bytes: usize,
    counts: Arc<RingCounts>,
}

impl Drop for Registered {
    fn drop(&mut self) {
        REGISTERED.fetch_sub(self.bytes, Ordering::Relaxed);
        self.counts.pooled.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Takes up to `slots` slots' worth of the budget; returns how many it took.
fn reserve(slots: u32) -> u32 {
    let budget = memlock_budget();
    let mut held = REGISTERED.load(Ordering::Relaxed);
    loop {
        let room = budget.saturating_sub(held) / POOL_SLOT_LEN;
        let taken = slots.min(u32::try_from(room).unwrap_or(u32::MAX));
        // Fails only when another ring reserved meanwhile: retry with the new total.
        match REGISTERED.compare_exchange_weak(
            held,
            held + taken as usize * POOL_SLOT_LEN,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return taken,
            Err(now) => held = now,
        }
    }
}

/// A pool of buffer slots registered with `uring` (fixed buffers), as many as the budget
/// ([`memlock_budget`]) and the kernel allow up to [`POOL_SLOTS`], or `None` where they allow
/// none: reads and writes then use plain buffers.
fn register_pool(uring: &IoUring, counts: &Arc<RingCounts>) -> Option<Registered> {
    let mut slots = reserve(POOL_SLOTS);
    let reserved = slots;
    let registered = loop {
        if slots == 0 {
            break None;
        }
        let pool = SlotPool::new(slots, POOL_SLOT_LEN);
        let iovecs: Vec<libc::iovec> = (0..pool.slots())
            .map(|i| libc::iovec {
                iov_base: pool.slot_ptr(i).cast(),
                iov_len: pool.slot_len(),
            })
            .collect();
        // SAFETY: every iovec names a slot of `pool`'s region, which the ring holding `pool`
        // keeps alive until after the ring itself (and so the registration) is gone.
        match unsafe { uring.submitter().register_buffers(&iovecs) } {
            Ok(()) => break Some(pool),
            // The limit is shared with other processes of the user: fewer slots.
            Err(e) if e.raw_os_error() == Some(libc::ENOMEM) => slots /= 2,
            Err(_) => slots = 0,
        }
    };
    // Give back what the pool did not use.
    REGISTERED.fetch_sub(
        (reserved - slots) as usize * POOL_SLOT_LEN,
        Ordering::Relaxed,
    );
    registered.map(|pool| {
        counts.pooled.fetch_add(1, Ordering::Relaxed);
        Registered {
            pool,
            bytes: slots as usize * POOL_SLOT_LEN,
            counts: Arc::clone(counts),
        }
    })
}

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
        let ring = RingHandle::start(false)?;
        Ok(Arc::new(Self {
            files: PreadVfs::without_pool(),
            ring,
        }))
    }

    /// As [`UringVfs::new`], for application-owned mode (#408), where the engine starts no
    /// threads: the shared ring, for threads with no ring of their own, has no reaper. The
    /// threads that drive shards ([`Vfs::attach_thread`]) take its completions on every reap,
    /// and each one's completion fd ([`crate::own_io_fd`], an epoll descriptor over its own
    /// ring's eventfd and the shared ring's) turns readable for them too. A thread blocked on
    /// a completion nobody else reaps (one of the shared ring's, or one chained after them)
    /// reaps the shared ring itself.
    pub fn new_application_owned() -> Result<Arc<Self>> {
        let ring = RingHandle::start(true)?;
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
        let client = self
            .ring
            .reaper
            .is_none()
            .then(|| Arc::clone(&self.ring.ring));
        ThreadRing::attach(self.ring.id(), &self.ring.counts, client);
    }

    fn ring_stats(&self) -> Option<crate::RingStats> {
        let counts = &self.ring.counts;
        Some(crate::RingStats {
            rings: counts.rings.load(Ordering::Relaxed),
            pooled: counts.pooled.load(Ordering::Relaxed),
        })
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

    fn submit_sync_dir(&self, dir: &Path) -> Completion<()> {
        // An `fsync` of the directory's descriptor, through the ring (ICR 0021); the
        // operation keeps the descriptor open until it completes.
        match self.files.open_file(dir, OpenOptions::read()) {
            Ok(file) => UringFile {
                file,
                ring: Arc::clone(&self.ring),
            }
            .submit_sync_all(),
            Err(_) => Completion::ready(self.files.sync_dir(dir)),
        }
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
    /// The registered pool of the ring this thread's operations go to, if it has one
    /// (registering it on first use).
    fn pool(&self) -> Option<Arc<SlotPool>> {
        match ThreadRing::current(self.ring.id()) {
            Some(ring) => ring
                .pool
                .get_or_register(&ring.uring, &ring.counted.counts)
                .cloned(),
            None => {
                let ring = &self.ring.ring;
                ring.pool
                    .get_or_register(&ring.uring, &ring.counted.counts)
                    .cloned()
            }
        }
    }

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
                // With no reaper, a blocked wait reaps the shared ring itself.
                let drive = self.ring.reaper.is_none().then(|| self.ring.ring.drive());
                let (done, resolver) = Completion::driven_pair(drive);
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
    fn direct_align(&self) -> Option<usize> {
        self.file.direct_align()
    }

    /// A slot of the registered pool of the ring this thread submits to (a fixed buffer,
    /// read without pinning its pages), or a plain buffer when none is free.
    fn read_buf(&self, len: usize) -> IoBuf {
        if len > POOL_SLOT_LEN {
            return IoBuf::zeroed(len);
        }
        self.pool()
            .and_then(|p| p.take(len))
            .unwrap_or_else(|| IoBuf::zeroed(len))
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.file.read_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.file.write_at(buf, offset)
    }

    fn submit_read(&self, buf: IoBuf, offset: u64) -> Completion {
        if let Err(e) = self.file.check_aligned(buf.as_ptr(), buf.len(), offset) {
            return Completion::ready(Err(e));
        }
        self.submit(|resolver| Kind::Read {
            buf,
            offset,
            done: 0,
            resolver,
        })
    }

    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        if let Err(e) = self.file.check_aligned(buf.as_ptr(), buf.len(), offset) {
            return Completion::ready(Err(e));
        }
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
    /// The submission entry for the rest of the operation, tagged `user_data`. A buffer in a
    /// slot of `pool` (the submitting ring's registered pool) goes as a fixed buffer.
    fn entry(&mut self, user_data: u64, pool: Option<&Arc<SlotPool>>) -> squeue::Entry {
        let fd = types::Fd(self.file.raw_fd());
        let fixed = |buf: &IoBuf| match (buf.slot_of(), pool) {
            (Some((p, index)), Some(ring)) if Arc::ptr_eq(p, ring) => u16::try_from(index).ok(),
            _ => None,
        };
        match &mut self.kind {
            Kind::Read {
                buf, offset, done, ..
            } => {
                let index = fixed(buf);
                let rest = &mut buf[*done..];
                let at = *offset + *done as u64;
                match index {
                    Some(i) => opcode::ReadFixed::new(fd, rest.as_mut_ptr(), chunk(rest.len()), i)
                        .offset(at)
                        .build(),
                    None => opcode::Read::new(fd, rest.as_mut_ptr(), chunk(rest.len()))
                        .offset(at)
                        .build(),
                }
            }
            Kind::Write {
                buf, offset, done, ..
            } => {
                let index = fixed(buf);
                let rest = &buf[*done..];
                let at = *offset + *done as u64;
                match index {
                    Some(i) => opcode::WriteFixed::new(fd, rest.as_ptr(), chunk(rest.len()), i)
                        .offset(at)
                        .build(),
                    None => opcode::Write::new(fd, rest.as_ptr(), chunk(rest.len()))
                        .offset(at)
                        .build(),
                }
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
    /// Its registered buffer slots, once used (dropped after `uring`, which unregisters
    /// them).
    pool: LazyPool,
    /// Its place in the backend's ring counts.
    counted: Counted,
    /// Held while pushing to the submission queue (one pusher at a time).
    sq: Mutex<()>,
    /// Held while taking completions (the reaper, or the threads that reap a ring without
    /// one), never while they run.
    cq: Mutex<()>,
    table: Mutex<Table>,
    /// Set when the last handle goes: the reaper ends once nothing is in flight.
    shutdown: AtomicBool,
    /// Without a reaper (application-owned mode, #408): registered with the ring, so each
    /// completion signals it; the driving threads' completion fds (epoll) include it.
    notify: Option<EventFd>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Ring {
    /// A ring whose kernel supports every operation this backend submits.
    fn probe(counts: &Arc<RingCounts>, notify: bool) -> Result<Self> {
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
        let notify = if notify {
            let e = EventFd::new().map_err(unsupported)?;
            uring
                .submitter()
                .register_eventfd(e.raw())
                .map_err(unsupported)?;
            Some(e)
        } else {
            None
        };
        Ok(Self {
            uring,
            counted: Counted::new(counts),
            pool: LazyPool::default(),
            sq: Mutex::new(()),
            cq: Mutex::new(()),
            table: Mutex::new(Table::default()),
            shutdown: AtomicBool::new(false),
            notify,
        })
    }

    /// Submits `op`. A failure to submit resolves it with that error.
    fn submit(&self, op: Op) {
        let mut table = lock(&self.table);
        let user_data = table.insert(op);
        let entry = table.ops[user_data as usize]
            .as_mut()
            .expect("inserted above")
            .entry(user_data, self.pool.registered());
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
            self.take_completions();
            if self.shutdown.load(Ordering::Acquire) && lock(&self.table).live == 0 {
                return;
            }
        }
    }

    /// Reaps a ring without a reaper (#408), from any thread: takes what has completed,
    /// waiting up to `wait` for something to when nothing has (`None`: do not wait).
    /// Returns whether an operation completed.
    fn reap_now(&self, wait: Option<Duration>) -> bool {
        if let Some(notify) = &self.notify {
            notify.drain();
        }
        if let Some(t) = wait
            && lock(&self.table).live > 0
        {
            let ts = types::Timespec::new()
                .sec(t.as_secs())
                .nsec(t.subsec_nanos());
            let args = types::SubmitArgs::new().timespec(&ts);
            let _ = self.uring.submitter().submit_with_args(1, &args);
        }
        self.take_completions()
    }

    /// What a blocked [`Completion::wait`] on a ring without a reaper calls: reaps it, from
    /// whichever thread waits.
    fn drive(self: &Arc<Self>) -> crate::completion::Drive {
        let ring = Arc::downgrade(self);
        Arc::new(move || match ring.upgrade() {
            Some(r) => {
                r.reap_now(Some(DRIVE_SLICE));
                true
            }
            None => false,
        })
    }

    /// Whether the completion queue holds entries (a look, no system call).
    fn has_completions(&self) -> bool {
        let _cq = lock(&self.cq);
        // SAFETY: `self.cq` is held, so this is the only borrow of the completion queue.
        let mut cq = unsafe { self.uring.completion_shared() };
        cq.sync();
        !cq.is_empty()
    }

    /// Takes the completions the ring holds and resolves them (outside any lock, so their
    /// continuations may submit or reap again). Returns whether an operation completed.
    fn take_completions(&self) -> bool {
        let mut done = Vec::new();
        let mut again = Vec::new();
        {
            let _cq = lock(&self.cq);
            // SAFETY: `self.cq` is held, so this is the only borrow of the completion queue.
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
        let any = !done.is_empty();
        for resolve in done {
            resolve();
        }
        any
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

impl crate::own::OrphanIo for Ring {
    fn reap_orphan(&self, wait: Duration) -> bool {
        self.reap_now(Some(wait))
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

/// The shared ring and its reaper thread (none in application-owned mode, #408); dropping
/// the last handle stops the reaper once everything in flight has completed.
struct RingHandle {
    ring: Arc<Ring>,
    /// This backend's rings: the shared one and the threads' own.
    counts: Arc<RingCounts>,
    /// The reaper and its thread; `None` without one.
    reaper: Option<(Mutex<Option<JoinHandle<()>>>, ThreadId)>,
}

impl RingHandle {
    /// Identifies the backend this handle belongs to (for a thread's own rings).
    fn id(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    fn start(application_owned: bool) -> Result<Arc<Self>> {
        let counts = Arc::new(RingCounts::default());
        let ring = Arc::new(Ring::probe(&counts, application_owned)?);
        if application_owned {
            // No reaper: any thread blocked on a completion chained after this ring's
            // operations takes their completions (#408).
            let orphan: std::sync::Weak<dyn crate::own::OrphanIo> = Arc::downgrade(&ring) as _;
            crate::own::register_orphan(orphan);
            return Ok(Arc::new(Self {
                ring,
                counts,
                reaper: None,
            }));
        }
        let r = Arc::clone(&ring);
        let reaper = thread::Builder::new()
            .name("pigeonhole-uring".into())
            .spawn(move || r.reap())
            .map_err(|e| Error::os("spawn the io_uring reaper", e))?;
        let id = reaper.thread().id();
        Ok(Arc::new(Self {
            ring,
            counts,
            reaper: Some((Mutex::new(Some(reaper)), id)),
        }))
    }
}

impl Drop for RingHandle {
    fn drop(&mut self) {
        let Some((reaper, reaper_id)) = &self.reaper else {
            // No reaper (#408): complete what is in flight here, before the ring and the
            // buffers the kernel writes into go. Continuations run outside the ring's locks,
            // so this works from one too.
            while lock(&self.ring.table).live > 0 {
                self.ring.reap_now(Some(Duration::from_millis(10)));
            }
            return;
        };
        self.ring.shutdown.store(true, Ordering::Release);
        // Wake the reaper; it ends once nothing is in flight.
        let nop = opcode::Nop::new().build().user_data(SHUTDOWN);
        let _ = self.ring.push(&nop);
        let reaper = lock(reaper).take();
        // The last handle can go on the reaper itself (a continuation holding a file): it
        // then ends on its own after this call.
        if thread::current().id() != *reaper_id
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
    /// Its registered buffer slots, once used (dropped after `uring`).
    pool: LazyPool,
    /// Its place in the backend's ring counts.
    counted: Counted,
    table: Mutex<Table>,
    owner: ThreadId,
    /// The backend ([`RingHandle::id`]) the ring belongs to.
    backend: usize,
    wake: Arc<EventFd>,
    /// Registered with the ring (`IORING_REGISTER_EVENTFD`): signalled on each completion,
    /// for a thread that waits on it in its own event loop ([`crate::own_io_fd`], #408).
    /// `None` if the kernel refused the registration.
    notify: Option<EventFd>,
    /// The backend's shared ring when it has no reaper (application-owned mode, #408): this
    /// thread's reaps take its completions too.
    client: Option<Arc<Ring>>,
    /// With `client`: an epoll descriptor over `notify` and the shared ring's eventfd, the
    /// thread's completion fd (`own_io_fd`), readable when either ring has completions. Not
    /// an io_uring poll of the shared ring's eventfd: the kernel does not let an eventfd that
    /// io_uring signals wake another io_uring poll reliably (`EPOLL_URING_WAKE`), and #443's
    /// first version hung on that.
    epoll: Option<std::os::fd::OwnedFd>,
}

/// An epoll descriptor readable when any of `fds` is (`None` if `wanted` is false, an fd is
/// missing, or the kernel refuses).
fn epoll_over(fds: [Option<&EventFd>; 2], wanted: bool) -> Option<std::os::fd::OwnedFd> {
    use std::os::fd::FromRawFd;
    if !wanted {
        return None;
    }
    // SAFETY: plain syscall; a non-negative result is a descriptor we now own.
    let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if ep < 0 {
        return None;
    }
    // SAFETY: `ep` was just returned by `epoll_create1` and is owned by nobody else.
    let owned = unsafe { std::os::fd::OwnedFd::from_raw_fd(ep) };
    for fd in fds {
        let fd = fd?.raw();
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: fd as u64,
        };
        // SAFETY: a live epoll descriptor, a live eventfd and a live event.
        if unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_ADD, fd, &mut ev) } != 0 {
            return None;
        }
    }
    Some(owned)
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
    fn attach(backend: usize, counts: &Arc<RingCounts>, client: Option<Arc<Ring>>) {
        if Self::current(backend).is_some() {
            return;
        }
        let Some(ring) = Self::new(backend, counts, client) else {
            return;
        };
        let own: std::sync::Weak<dyn crate::own::OwnIo> = Arc::downgrade(&ring) as _;
        crate::own::register(own);
        let _ = THREAD_RINGS.try_with(|r| r.borrow_mut().push(ring));
    }

    fn new(
        backend: usize,
        counts: &Arc<RingCounts>,
        client: Option<Arc<Ring>>,
    ) -> Option<Arc<Self>> {
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
        // Signalled on each completion, for an application's own event loop (#408). Kept off
        // `wake`, which the ring itself polls: a registered eventfd the ring also polled
        // would signal itself with every poll completion.
        let notify = EventFd::new()
            .ok()
            .filter(|e| uring.submitter().register_eventfd(e.raw()).is_ok());
        let ring = Arc::new(Self {
            uring,
            counted: Counted::new(counts),
            pool: LazyPool::default(),
            table: Mutex::new(Table::default()),
            owner: thread::current().id(),
            backend,
            wake: Arc::new(EventFd::new().ok()?),
            epoll: epoll_over(
                [
                    notify.as_ref(),
                    client.as_ref().and_then(|c| c.notify.as_ref()),
                ],
                client.is_some(),
            ),
            notify,
            client,
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
            .entry(user_data, self.pool.registered());
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
        // Reset before taking completions: one that finishes after this signals it again.
        if let Some(notify) = &self.notify {
            notify.drain();
        }
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
        let mut any = !done.is_empty();
        for resolve in done {
            resolve();
        }
        // The reaper-less shared ring (#408): taken on every turn when it holds completions
        // (a look at its completion queue, no system call). Its eventfd is drained first, so
        // a completion that lands in between is seen now or signals again.
        if let Some(client) = &self.client {
            if let Some(n) = &client.notify {
                n.drain();
            }
            if client.has_completions() {
                any |= client.take_completions();
            }
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

    fn fd_here(&self) -> Option<i32> {
        use std::os::fd::AsRawFd;
        if thread::current().id() != self.owner {
            return None;
        }
        match &self.epoll {
            Some(e) => Some(e.as_raw_fd()),
            None => self.notify.as_ref().map(EventFd::raw),
        }
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
