//! The io_uring backend (Linux, #402): real files whose submitted I/O goes through an
//! io_uring ring instead of a thread pool.
//!
//! Files are opened, locked and read or written synchronously exactly as by
//! [`PreadVfs`]; only the submitted operations (`submit_read`, `submit_write`,
//! `submit_sync_data`, `submit_sync_all`) differ. A thread that attached a ring of its own
//! ([`Vfs::attach_thread`]: shard, compaction and pool threads, #402) submits there and
//! reaps it itself, in its turns and its waits. Any other thread submits to the backend's
//! shared ring, whose reaper thread resolves each operation's [`Completion`], waking its
//! waiter or task; in application-owned mode there is no reaper, and such a thread runs its
//! operations inline instead (#408, #929).
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
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::{self, JoinHandle, ThreadId};
use std::time::{Duration, Instant};

use io_uring::{IoUring, Probe, opcode, squeue, types};

use crate::buf::SlotPool;
use crate::completion::Resolver;
use crate::pread::{PreadFile, PreadVfs};
use crate::ring_wake::RingWake;
use crate::{
    Completion, Error, ErrorKind, File, FileIdentity, FileRef, IoBuf, Locality, LockMode,
    OpenOptions, ProcessId, Result, SharedOpen, SharedRegion, Vfs,
};

/// Submission queue entries of the shared ring (completions get twice as many).
const RING_ENTRIES: u32 = 256;

/// The most io-wq workers for bounded work (reads and writes of regular files, #974) of each
/// task that submits to the shared ring; `0` would leave the kernel's default. io-wq is per
/// task, and an entry's work runs on the io-wq of whichever task's enter submitted it: a
/// client's, or the reaper's (its wait submits what the queue holds), so a client's batches
/// use at most twice this. In 6.8 a task takes the cap when the ring registers it (the
/// registering task, and every task already on the ring), or when its task context is first
/// created, by its first io_uring submit of any ring: a thread that used another ring first
/// (its own ring, or another backend's shared ring) keeps that one's limits, or none.
const IOWQ_BOUNDED_WORKERS: u32 = 4;

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
    attach_failures: AtomicU64,
    inline: AtomicU64,
}

/// How long a thread whose ring could not be created waits before it tries again (#929):
/// a refusal for memory (`ENOMEM`) is often the previous rings' charge, which the kernel
/// gives back only once their teardown has run.
const ATTACH_RETRY: Duration = Duration::from_millis(10);

/// Attaches still to fail on purpose ([`fail_ring_attaches`]), and whether as a kernel
/// without the support (permanent) rather than out of memory (transient).
static FAIL_ATTACHES: AtomicUsize = AtomicUsize::new(0);
static FAIL_UNSUPPORTED: AtomicBool = AtomicBool::new(false);

/// Makes the next `n` attempts to give a thread its own ring fail as the kernel refusing it
/// for memory would (`ENOMEM`: the thread retries later; `usize::MAX`: every one). A test hook
/// (#929), not part of the API.
#[doc(hidden)]
pub fn fail_ring_attaches(n: usize) {
    FAIL_UNSUPPORTED.store(false, Ordering::SeqCst);
    FAIL_ATTACHES.store(n, Ordering::SeqCst);
}

/// As [`fail_ring_attaches`], failing as a kernel without the support does (permanent: the
/// thread never retries). A test hook (#929, perf287's S1 on #938), not part of the API.
#[doc(hidden)]
pub fn fail_ring_attaches_unsupported(n: usize) {
    FAIL_UNSUPPORTED.store(true, Ordering::SeqCst);
    FAIL_ATTACHES.store(n, Ordering::SeqCst);
}

/// Why a thread got no ring of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttachFailure {
    /// The kernel refused one for now (`ENOMEM`, `EAGAIN`): often the previous rings'
    /// charge, given back once their teardown has run. Retried after [`ATTACH_RETRY`].
    Transient,
    /// The kernel lacks what a thread's ring needs (timed waits, `PollAdd`), or refused it
    /// for a reason waiting will not change: never retried.
    Permanent,
}

impl AttachFailure {
    fn of(e: &io::Error) -> Self {
        match e.raw_os_error() {
            Some(libc::ENOMEM | libc::EAGAIN) => AttachFailure::Transient,
            _ => AttachFailure::Permanent,
        }
    }
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
    /// threads: the shared ring has no reaper, and nothing is submitted to it. The threads
    /// that drive shards ([`Vfs::attach_thread`]) submit to rings of their own and reap them,
    /// each one's completion fd ([`crate::own_io_fd`]) being its own ring's eventfd; any other
    /// thread runs its operations inline (#929, #939).
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
    /// lacks what such a ring needs, the thread keeps using the shared ring, or runs its
    /// operations inline on a backend without a reaper (#929).
    fn attach_thread(&self) {
        ThreadRing::attach(self.ring.id(), &self.ring.counts);
    }

    fn ring_stats(&self) -> Option<crate::RingStats> {
        let counts = &self.ring.counts;
        Some(crate::RingStats {
            rings: counts.rings.load(Ordering::Relaxed),
            pooled: counts.pooled.load(Ordering::Relaxed),
            attach_failures: counts.attach_failures.load(Ordering::Relaxed),
            inline: counts.inline.load(Ordering::Relaxed),
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
    /// (registering it on first use). None for a thread that runs them inline (#929).
    fn pool(&self) -> Option<Arc<SlotPool>> {
        if crate::own::in_inline_io() {
            return None;
        }
        match ThreadRing::current(self.ring.id()) {
            Some(ring) => ring
                .pool
                .get_or_register(&ring.uring, &ring.counted.counts)
                .cloned(),
            None if self.ring.reaper.is_none() => None,
            None => {
                let ring = &self.ring.ring;
                ring.pool
                    .get_or_register(&ring.uring, &ring.counted.counts)
                    .cloned()
            }
        }
    }

    /// Whether the calling thread runs this file's operations inline: inside an
    /// [`crate::inline_io`] scope (a background worker's slice, D284), always; otherwise when
    /// the backend has no reaper (application-owned mode) and the thread no ring of its own
    /// (#929). Nothing then
    /// waits in a ring for someone to reap, whatever the thread's waits look like. A thread
    /// whose attach failed tries again here once [`ATTACH_RETRY`] has passed.
    ///
    /// An inline sync covers only writes that completed, as any `fdatasync` does, not one
    /// still in flight on another thread's ring. The WAL never counts on more: a sync's
    /// target is the stream's written position, taken after its synchronous buffer write
    /// (`WalStream::submit_sync`: `let lsn = self.write()?`, `write_buf`'s `write_at`), and a
    /// held segment header's write completes before the sync chained after it.
    fn inline_here(&self) -> bool {
        // A background worker's slice (D284): inline whatever the thread has, and no attach.
        if crate::own::in_inline_io() {
            self.ring.counts.inline.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        if self.ring.reaper.is_some() {
            return false;
        }
        let backend = self.ring.id();
        if ThreadRing::current(backend).is_some() {
            return false;
        }
        let retry = ATTACH_AFTER
            .try_with(|a| {
                a.borrow()
                    .iter()
                    .any(|(b, at)| *b == backend && at.is_some_and(|at| Instant::now() >= at))
            })
            .unwrap_or(false);
        if retry {
            ThreadRing::attach(backend, &self.ring.counts);
            if ThreadRing::current(backend).is_some() {
                return false;
            }
        }
        self.ring.counts.inline.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Submits the operation `kind` builds: to the calling thread's own ring for this
    /// backend if it attached one (completed when it reaps), else to the shared ring and its
    /// reaper. Without a reaper, a thread with no ring of its own never gets here: it runs
    /// its operations inline ([`UringFile::inline_here`], #929).
    fn submit<T: Send + 'static>(&self, kind: impl FnOnce(Resolver<T>) -> Kind) -> Completion<T> {
        match ThreadRing::current(self.ring.id()) {
            Some(ring) => {
                let (done, resolver) = Completion::driven_pair(Some(ring.drive()));
                ring.submit(Op {
                    file: Arc::clone(&self.file),
                    kind: kind(resolver),
                    flags: squeue::Flags::empty(),
                });
                done
            }
            None => {
                debug_assert!(
                    self.ring.reaper.is_some(),
                    "an operation reached the shared ring of a backend with no reaper"
                );
                let (done, resolver) = Completion::driven_pair(None);
                self.ring.ring.submit(Op {
                    file: Arc::clone(&self.file),
                    kind: kind(resolver),
                    flags: squeue::Flags::empty(),
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

    fn submit_read(&self, mut buf: IoBuf, offset: u64) -> Completion {
        if let Err(e) = self.file.check_aligned(buf.as_ptr(), buf.len(), offset) {
            return Completion::ready(Err(e));
        }
        if self.inline_here() {
            return Completion::ready(self.file.read_at(&mut buf, offset).map(|()| buf));
        }
        self.submit(|resolver| Kind::Read {
            buf,
            offset,
            done: 0,
            resolver,
        })
    }

    /// One batch on the ring this thread submits to, with one enter for it (ICR 0077). On the
    /// shared ring every read's entry carries `IOSQE_ASYNC`, so an io-wq worker copies a
    /// page-cache hit rather than the submitter inside its enter, under the kernel's
    /// `uring_lock` that every submitter queues on (#974). A thread that runs its I/O inline
    /// reads each in turn.
    fn submit_reads_ahead(&self, reads: Vec<(IoBuf, u64)>) -> Vec<Completion> {
        if self.inline_here() {
            return reads
                .into_iter()
                .map(|(mut buf, offset)| {
                    let r = self.file.check_aligned(buf.as_ptr(), buf.len(), offset);
                    Completion::ready(
                        r.and_then(|()| self.file.read_at(&mut buf, offset))
                            .map(|()| buf),
                    )
                })
                .collect();
        }
        let own = ThreadRing::current(self.ring.id());
        // `IOSQE_ASYNC` only on the shared ring, where submitters queue on the kernel's
        // `uring_lock`: a thread's own ring has one submitter, so the copy is its own work
        // either way, and its io-wq is not capped (perf287 on #977).
        let flags = if own.is_some() {
            squeue::Flags::empty()
        } else {
            squeue::Flags::ASYNC
        };
        let mut out = Vec::with_capacity(reads.len());
        let mut ops = Vec::with_capacity(reads.len());
        for (buf, offset) in reads {
            if let Err(e) = self.file.check_aligned(buf.as_ptr(), buf.len(), offset) {
                out.push(Completion::ready(Err(e)));
                continue;
            }
            let (done, resolver) = Completion::driven_pair(own.as_ref().map(|r| r.drive()));
            out.push(done);
            ops.push(Op {
                file: Arc::clone(&self.file),
                kind: Kind::Read {
                    buf,
                    offset,
                    done: 0,
                    resolver,
                },
                flags,
            });
        }
        match own {
            Some(ring) => ring.submit_batch(ops),
            None => {
                debug_assert!(
                    self.ring.reaper.is_some(),
                    "an operation reached the shared ring of a backend with no reaper"
                );
                self.ring.ring.submit_batch(ops);
            }
        }
        out
    }

    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        if let Err(e) = self.file.check_aligned(buf.as_ptr(), buf.len(), offset) {
            return Completion::ready(Err(e));
        }
        if self.inline_here() {
            return Completion::ready(self.file.write_at(&buf, offset).map(|()| buf));
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
        if self.inline_here() {
            return Completion::ready(self.file.sync_data());
        }
        self.submit(|resolver| Kind::Sync {
            data_only: true,
            resolver,
        })
    }

    fn sync_all(&self) -> Result<()> {
        self.file.sync_all()
    }

    fn submit_sync_all(&self) -> Completion<()> {
        if self.inline_here() {
            return Completion::ready(self.file.sync_all());
        }
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

    fn holds(&self, byte: u64) -> Option<LockMode> {
        self.file.holds(byte)
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
    /// Flags of every entry it submits (a short read's rest too): `IOSQE_ASYNC` for a read
    /// nothing waits on yet ([`File::submit_reads_ahead`], ICR 0077).
    flags: squeue::Flags,
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
        .flags(self.flags)
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
}

/// Kernel enters still to fail on purpose ([`fail_ring_enters`]), and with which `errno`.
static FAIL_ENTERS: AtomicUsize = AtomicUsize::new(0);
static FAIL_ENTERS_ERRNO: AtomicI32 = AtomicI32::new(0);

/// Makes the next `n` kernel enters that hand a ring's submitted entries over fail with
/// `errno`, as the kernel refusing them would (`EAGAIN` under memory pressure). A test hook
/// (perf287's must-fix on #974), not part of the API.
#[doc(hidden)]
pub fn fail_ring_enters(n: usize, errno: i32) {
    FAIL_ENTERS_ERRNO.store(errno, Ordering::SeqCst);
    FAIL_ENTERS.store(n, Ordering::SeqCst);
}

/// Reaps' kernel enters still to fail on purpose ([`fail_ring_reaps`]), and with which `errno`.
static FAIL_REAPS: AtomicUsize = AtomicUsize::new(0);
static FAIL_REAPS_ERRNO: AtomicI32 = AtomicI32::new(0);

/// As [`fail_ring_enters`], for the next `n` enters of a reap (the shared ring's reaper,
/// or a thread reaping its own ring), which submit what the queue holds too. A test hook
/// (perf287's must-fix on #976), not part of the API.
#[doc(hidden)]
pub fn fail_ring_reaps(n: usize, errno: i32) {
    FAIL_REAPS_ERRNO.store(errno, Ordering::SeqCst);
    FAIL_REAPS.store(n, Ordering::SeqCst);
}

/// Takes one injected failure from `left` ([`fail_ring_enters`], [`fail_ring_reaps`]).
fn injected(left: &AtomicUsize, errno: &AtomicI32) -> Option<io::Error> {
    let mut n = left.load(Ordering::Relaxed);
    while n > 0 {
        match left.compare_exchange_weak(n, n - 1, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => return Some(io::Error::from_raw_os_error(errno.load(Ordering::SeqCst))),
            Err(now) => n = now,
        }
    }
    None
}

/// A reap's enter, `enter`, failing while [`fail_ring_reaps`] asks.
fn reap_enter(enter: impl FnOnce() -> io::Result<usize>) -> io::Result<usize> {
    match injected(&FAIL_REAPS, &FAIL_REAPS_ERRNO) {
        Some(e) => Err(e),
        None => enter(),
    }
}

/// The longest pause between enters the kernel refuses for now (`EAGAIN`).
const ENTER_RETRY_MAX: Duration = Duration::from_millis(1);

/// `io_uring_enter` handing over what the submission queue holds, failing while
/// [`fail_ring_enters`] asks.
fn enter(uring: &IoUring) -> io::Result<usize> {
    match injected(&FAIL_ENTERS, &FAIL_ENTERS_ERRNO) {
        Some(e) => Err(e),
        None => uring.submit(),
    }
}

/// Hands entries already published to the kernel. Whatever it returns, they stay the
/// ring's: an operation whose entry is published is never failed here, as some later enter
/// submits the entry and the kernel then reads into or from its buffer (perf287's must-fix
/// on #974). A refusal for now (`EAGAIN`, `EINTR`) is retried, backing off up to 1 ms: the
/// ring's waiter may be blocked in the kernel with nothing else in flight, and would never
/// enter again on its own. Any other error is left to the next enter: `EBUSY` means
/// completions wait to be taken, so the reaper (or the owner) is not blocked and enters
/// again soon, and a broken ring fails that enter too, which resolves everything in flight
/// (`fail_all`).
fn enter_published(uring: &IoUring) {
    let mut pause = Duration::from_micros(1);
    loop {
        match enter(uring) {
            Err(e) if matches!(e.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) => {
                thread::sleep(pause);
                pause = (pause * 2).min(ENTER_RETRY_MAX);
            }
            _ => return,
        }
    }
}

/// Resolves `op`, in flight on a ring that failed, with `e`, and leaks its buffer: the kernel
/// may still read into or write from it until the ring's teardown has cancelled everything,
/// which happens after the ring is dropped, so the buffer is never freed or given back to its
/// slot pool (perf287 on #976). A leaked slot keeps its pool, so the ring's whole registered
/// region stays allocated (no longer locked or budgeted once the ring drops): acceptable for a
/// ring that can no longer be entered, the only one this is for. An operation whose entry the
/// kernel never saw goes through [`fail`].
fn abandon(op: Op, e: io::Error) {
    match op.kind {
        Kind::Read { buf, resolver, .. } => {
            std::mem::forget(buf);
            resolver.resolve(Err(Error::os("read", e)));
        }
        Kind::Write { buf, resolver, .. } => {
            std::mem::forget(buf);
            resolver.resolve(Err(Error::os("write", e)));
        }
        Kind::Sync { resolver, .. } => resolver.resolve(Err(Error::os("sync", e))),
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Ring {
    /// A ring whose kernel supports every operation this backend submits.
    fn probe(counts: &Arc<RingCounts>) -> Result<Self> {
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
        // Bounds each submitting thread's io-wq (#974): in 6.8 io-wq is per task, so every
        // client thread gets its own pool, up to `min(entries, 4 x CPUs)` workers, each
        // created inside that thread's enter. The registering thread takes the cap now, and
        // every task whose first submit goes to this ring takes it then. Best effort: a
        // kernel without the call keeps its default.
        let _ = uring
            .submitter()
            .register_iowq_max_workers(&mut [IOWQ_BOUNDED_WORKERS, 0]);
        Ok(Self {
            uring,
            counted: Counted::new(counts),
            pool: LazyPool::default(),
            sq: Mutex::new(()),
            cq: Mutex::new(()),
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
            .entry(user_data, self.pool.registered());
        drop(table);
        if let Err(e) = self.push(&entry)
            && let Some(op) = lock(&self.table).take(user_data)
        {
            // Not published: the kernel never sees its buffer.
            fail(op, e);
        }
    }

    /// [`Ring::submit`] for several operations, with one enter for them all (ICR 0077).
    fn submit_batch(&self, ops: Vec<Op>) {
        let entries = insert_all(&self.table, ops, self.pool.registered());
        let published = {
            let _sq = lock(&self.sq);
            publish_all(&entries, |entry| self.publish(entry))
        };
        if published.is_ok() || published.as_ref().is_err_and(|(n, _)| *n > 0) {
            enter_published(&self.uring);
        }
        fail_unpublished(&self.table, &entries, published);
    }

    /// Pushes `entry` and tells the kernel, waiting for room in a full queue. An error means
    /// the entry was not published (the kernel never sees it); once it is, it is the ring's
    /// ([`enter_published`]). `sq` covers only the push and the tail's publication: the
    /// enter runs after it is released, as the reaper's always has (an enter hands over
    /// whatever tail it sees, so another thread's later enter finds less or nothing), and the
    /// kernel copies a page-cache hit inline under its own `uring_lock` during the enter
    /// (#974), which would otherwise hold every other pusher too, as would a retried enter's
    /// backoff (#976).
    fn push(&self, entry: &squeue::Entry) -> io::Result<()> {
        {
            let _sq = lock(&self.sq);
            self.publish(entry)?;
        }
        enter_published(&self.uring);
        Ok(())
    }

    /// Publishes `entry` in the submission queue, handing queued entries to the kernel while
    /// it is full. The caller holds `sq`.
    fn publish(&self, entry: &squeue::Entry) -> io::Result<()> {
        loop {
            // SAFETY: the caller holds `self.sq`, so this is the only submission queue
            // borrowed. The entry's buffer and descriptor belong to an operation in
            // `self.table`, which keeps them until the entry's completion is reaped.
            let pushed = unsafe {
                let mut sq = self.uring.submission_shared();
                let r = sq.push(entry);
                sq.sync();
                r
            };
            match pushed {
                Ok(()) => break,
                // Full: hand the queued entries to the kernel to make room. `entry` is not
                // published yet, so a failure here fails only its operation.
                Err(_) => {
                    enter(&self.uring)?;
                }
            }
        }
        Ok(())
    }

    /// The reaper's loop: waits for completions and resolves them until shut down with
    /// nothing in flight.
    fn reap(&self) {
        let mut pause = Duration::from_micros(1);
        loop {
            match reap_enter(|| self.uring.submit_and_wait(1)) {
                Ok(_) => pause = Duration::from_micros(1),
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {}
                // Refused for now (no request could be allocated; the enter also submits
                // what the queue holds, #976): what is published and in flight stays the
                // ring's. Take what has completed (which frees requests), back off, enter
                // again.
                Err(e) if e.raw_os_error() == Some(libc::EAGAIN) => {
                    thread::sleep(pause);
                    pause = (pause * 2).min(ENTER_RETRY_MAX);
                }
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
            abandon(op, io::Error::other("io_uring failed"));
        }
    }
}

/// Puts `ops` in `table` and builds their entries, tagged with their places there.
fn insert_all(
    table: &Mutex<Table>,
    ops: Vec<Op>,
    pool: Option<&Arc<SlotPool>>,
) -> Vec<(u64, squeue::Entry)> {
    let mut table = lock(table);
    ops.into_iter()
        .map(|op| {
            let user_data = table.insert(op);
            let entry = table.ops[user_data as usize]
                .as_mut()
                .expect("inserted above")
                .entry(user_data, pool);
            (user_data, entry)
        })
        .collect()
}

/// Publishes `entries` in order with `publish`, stopping at the first that fails: then how
/// many were published, and why the next was not.
fn publish_all(
    entries: &[(u64, squeue::Entry)],
    mut publish: impl FnMut(&squeue::Entry) -> io::Result<()>,
) -> std::result::Result<(), (usize, io::Error)> {
    for (n, (_, entry)) in entries.iter().enumerate() {
        publish(entry).map_err(|e| (n, e))?;
    }
    Ok(())
}

/// Fails the operations of `entries` that [`publish_all`] did not publish: the kernel never
/// sees their buffers. The published ones are the ring's ([`enter_published`]).
fn fail_unpublished(
    table: &Mutex<Table>,
    entries: &[(u64, squeue::Entry)],
    published: std::result::Result<(), (usize, io::Error)>,
) {
    let Err((n, e)) = published else { return };
    for (user_data, _) in &entries[n..] {
        if let Some(op) = lock(table).take(*user_data) {
            let e = match e.raw_os_error() {
                Some(code) => io::Error::from_raw_os_error(code),
                None => io::Error::new(e.kind(), e.to_string()),
            };
            fail(op, e);
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
        let ring = Arc::new(Ring::probe(&counts)?);
        if application_owned {
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
    /// Per backend this thread failed to attach a ring for, when it may try again (`None`:
    /// never, the failure was permanent).
    static ATTACH_AFTER: RefCell<Vec<(usize, Option<Instant>)>> =
        const { RefCell::new(Vec::new()) };
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
    /// The kernel flags deferred completion work in the submission queue
    /// (`IORING_SQ_TASKRUN`): without the flag, anything in flight may have completed.
    taskrun_flag: bool,
    /// The owner took the completion fd ([`crate::own_io_fd`]): reaps keep it current.
    fd_taken: AtomicBool,
    /// Times a reap entered the kernel ([`crate::own_io_enters`]); only the owner writes it.
    enters: AtomicU64,
    wake: Arc<EventFd>,
    /// Whether a wake must write `wake` (only while the owner waits in the ring).
    ring_wake: Arc<RingWake>,
    /// Registered with the ring (`IORING_REGISTER_EVENTFD`): signalled on each completion,
    /// for a thread that waits on it in its own event loop ([`crate::own_io_fd`], #408).
    /// `None` if the kernel refused the registration.
    notify: Option<EventFd>,
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
    /// needs (timed waits), or when the kernel refuses one (`ENOMEM`), the thread goes on
    /// without: on a backend with a reaper its operations use the shared ring, and on one
    /// without (application-owned mode) they run inline (#929).
    fn attach(backend: usize, counts: &Arc<RingCounts>) {
        if Self::current(backend).is_some() {
            return;
        }
        let ring = match Self::new(backend, counts) {
            Ok(ring) => ring,
            Err(why) => {
                // The thread goes on without a ring of its own; on a backend with no reaper
                // its operations run inline meanwhile (#929). It tries again later only if
                // the refusal may pass: a kernel without the support never gives one, and
                // building and dropping a probe ring every few ms would add to the very
                // locked-memory pressure a transient refusal comes from (perf287's S1).
                counts.attach_failures.fetch_add(1, Ordering::Relaxed);
                let retry = match why {
                    AttachFailure::Transient => Some(Instant::now() + ATTACH_RETRY),
                    AttachFailure::Permanent => None,
                };
                let _ = ATTACH_AFTER.try_with(|a| {
                    let mut a = a.borrow_mut();
                    a.retain(|(b, _)| *b != backend);
                    a.push((backend, retry));
                });
                return;
            }
        };
        let _ = ATTACH_AFTER.try_with(|a| a.borrow_mut().retain(|(b, _)| *b != backend));
        let own: std::sync::Weak<dyn crate::own::OwnIo> = Arc::downgrade(&ring) as _;
        crate::own::register(own);
        let _ = THREAD_RINGS.try_with(|r| r.borrow_mut().push(ring));
    }

    fn new(
        backend: usize,
        counts: &Arc<RingCounts>,
    ) -> std::result::Result<Arc<Self>, AttachFailure> {
        use AttachFailure::{Permanent, Transient};
        // The test hook fails here, on the same path as a refusal below.
        let mut n = FAIL_ATTACHES.load(Ordering::SeqCst);
        while n > 0 {
            let next = if n == usize::MAX { n } else { n - 1 };
            match FAIL_ATTACHES.compare_exchange_weak(n, next, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) if FAIL_UNSUPPORTED.load(Ordering::SeqCst) => return Err(Permanent),
                Ok(_) => return Err(Transient),
                Err(now) => n = now,
            }
        }
        // Completions run only when this thread enters the ring (no work on other threads),
        // where the kernel offers it (6.1); a plain ring otherwise. The task-run flag tells
        // a non-waiting reap that deferred completions are pending, so its enter carries
        // GETEVENTS and runs them (without it, only a waiting enter would: a busy shard, or
        // an application loop that never waits, would never see its completions).
        let (uring, taskrun_flag) = match IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_taskrun_flag()
            .setup_cqsize(RING_ENTRIES * 2)
            .build(RING_ENTRIES)
        {
            Ok(uring) => (uring, true),
            Err(_) => (
                IoUring::builder()
                    .setup_cqsize(RING_ENTRIES * 2)
                    .build(RING_ENTRIES)
                    .map_err(|e| AttachFailure::of(&e))?,
                false,
            ),
        };
        if !uring.params().is_feature_ext_arg() {
            return Err(Permanent);
        }
        let mut probe = Probe::new();
        uring
            .submitter()
            .register_probe(&mut probe)
            .map_err(|_| Permanent)?;
        if !probe.is_supported(opcode::PollAdd::CODE) {
            return Err(Permanent);
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
            taskrun_flag,
            fd_taken: AtomicBool::new(false),
            enters: AtomicU64::new(0),
            wake: Arc::new(EventFd::new().map_err(|e| AttachFailure::of(&e))?),
            ring_wake: Arc::default(),
            notify,
        });
        ring.arm_wake().map_err(|e| AttachFailure::of(&e))?;
        Ok(ring)
    }

    /// Polls the eventfd, so a signal ends the ring's wait.
    fn arm_wake(&self) -> io::Result<()> {
        let poll = opcode::PollAdd::new(types::Fd(self.wake.raw()), libc::POLLIN as u32)
            .build()
            .user_data(WAKE);
        self.push(&poll)
    }

    /// Pushes `entry` and hands it to the kernel. Only the owner thread calls this. An error
    /// means the entry was not published; once it is, it is the ring's ([`enter_published`]).
    fn push(&self, entry: &squeue::Entry) -> io::Result<()> {
        self.publish(entry)?;
        enter_published(&self.uring);
        Ok(())
    }

    /// Publishes `entry` in the submission queue, handing queued entries to the kernel while
    /// it is full. Only the owner thread calls this.
    fn publish(&self, entry: &squeue::Entry) -> io::Result<()> {
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
                    enter(&self.uring)?;
                }
            }
        }
        Ok(())
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

    /// [`ThreadRing::submit`] for several operations, with one enter for them all (ICR 0077).
    fn submit_batch(&self, ops: Vec<Op>) {
        let entries = insert_all(&self.table, ops, self.pool.registered());
        let published = publish_all(&entries, |entry| self.publish(entry));
        if published.is_ok() || published.as_ref().is_err_and(|(n, _)| *n > 0) {
            enter_published(&self.uring);
        }
        fail_unpublished(&self.table, &entries, published);
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

    /// Counts a kernel enter ([`crate::own_io_enters`]); only the owner calls it.
    fn count_enter(&self) {
        self.enters
            .store(self.enters.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    }

    /// Whether a non-waiting reap has anything to take: completions already queued, or
    /// completion work the kernel deferred to this thread (flagged in the submission queue;
    /// on a ring without the flag, whenever anything is in flight).
    fn has_work(&self) -> bool {
        // SAFETY: only the owner thread (the caller) borrows this ring's queues, and the
        // borrows end here.
        if !unsafe { self.uring.completion_shared() }.is_empty() {
            return true;
        }
        if self.taskrun_flag {
            // SAFETY: as above.
            unsafe { self.uring.submission_shared() }.taskrun()
        } else {
            lock(&self.table).live > 0
        }
    }

    /// Takes the finished completions, waiting up to `wait` for one when none has (`None`:
    /// do not wait). Returns whether an operation completed.
    fn reap(&self, wait: Option<Duration>) -> bool {
        debug_assert_eq!(thread::current().id(), self.owner);
        // A shard thread's turn, with nobody watching the completion fd: with nothing to
        // take, it stays out of the kernel (two system calls a turn otherwise, which cost
        // write-heavy workloads 15-20% on the AX102).
        let watched = self.fd_taken.load(Ordering::Relaxed);
        if wait.is_none() && !watched && !self.has_work() {
            return false;
        }
        // Reset before taking completions: one that finishes after this signals it again.
        if watched && let Some(notify) = &self.notify {
            notify.drain();
        }
        let entered = match wait {
            Some(t) if lock(&self.table).live > 0 || !t.is_zero() => {
                if self.ring_wake.begin_wait() {
                    let ts = types::Timespec::new()
                        .sec(t.as_secs())
                        .nsec(t.subsec_nanos());
                    let args = types::SubmitArgs::new().timespec(&ts);
                    self.count_enter();
                    let entered = reap_enter(|| self.uring.submitter().submit_with_args(1, &args));
                    self.ring_wake.end_wait();
                    entered
                } else {
                    // Woken since the turn: back to it, taking only what is already queued.
                    self.ring_wake.end_wait();
                    Ok(0)
                }
            }
            // Runs this thread's deferred completion work without waiting.
            _ => {
                self.count_enter();
                reap_enter(|| {
                    self.uring
                        .submitter()
                        .submit_with_args(0, &types::SubmitArgs::new())
                })
            }
        };
        if let Err(e) = &entered
            && e.raw_os_error() == Some(libc::EAGAIN)
        {
            // Refused for now (#976): what is published and in flight stays the ring's; this
            // reap takes what has completed, and the owner's next one enters again.
            thread::sleep(ENTER_RETRY_MAX / 20);
        }
        if let Err(e) = entered
            && !matches!(
                e.raw_os_error(),
                Some(libc::ETIME | libc::EINTR | libc::EBUSY | libc::EAGAIN)
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
                abandon(op, io::Error::other("io_uring failed"));
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
        let (wake, ring_wake) = (Arc::clone(&self.wake), Arc::clone(&self.ring_wake));
        // The eventfd only while the owner waits in the ring (`RingWake`): a parked owner is
        // woken by the unpark beside this, and a running one finds the wake pending.
        Some(crate::OwnIoWaker(Arc::new(move || {
            if ring_wake.wake() {
                wake.signal();
            }
        })))
    }

    fn fd_here(&self) -> Option<i32> {
        if thread::current().id() != self.owner {
            return None;
        }
        self.fd_taken.store(true, Ordering::Relaxed);
        self.notify.as_ref().map(EventFd::raw)
    }

    fn enters_here(&self) -> u64 {
        if thread::current().id() != self.owner {
            return 0;
        }
        self.enters.load(Ordering::Relaxed)
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
        // Unpins and uncharges the pool now, with nothing in flight: the kernel gives back
        // the ring's own memory only once its teardown has run, after the descriptor
        // closes, and the next thread's ring may meanwhile hit the locked-memory limit
        // (#929). The pool is the bulk of the charge (1 MiB against the ring's ~30 KiB).
        // Owner-only, as the check above makes it: a single-issuer ring refuses
        // registrations from other tasks (`EEXIST`). Its error is ignored by design: should
        // the unregister fail, the pool is uncharged at the ring's teardown, as before.
        if self.pool.registered().is_some() {
            let _ = self.uring.submitter().unregister_buffers();
        }
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    /// #974: the shared ring's setup caps the io-wq of the task that set it up (and of every
    /// task whose first submit goes to it) at `IOWQ_BOUNDED_WORKERS` bounded workers. Zeros
    /// read the calling task's limits back without changing them.
    #[test]
    fn the_shared_ring_caps_its_submitters_io_wq() {
        let ring = Ring::probe(&Arc::new(RingCounts::default())).expect("io_uring is available");
        let mut limits = [0, 0];
        ring.uring
            .submitter()
            .register_iowq_max_workers(&mut limits)
            .expect("the kernel reports io-wq limits");
        assert_eq!(limits[0], IOWQ_BOUNDED_WORKERS, "{limits:?}");
    }

    /// The case the cap is for (perf287 on #977): a client thread whose first io_uring submit
    /// goes to the shared ring takes the cap with the task context that submit creates.
    #[test]
    fn a_fresh_thread_submitting_to_the_shared_ring_takes_the_cap() {
        let ring =
            Arc::new(Ring::probe(&Arc::new(RingCounts::default())).expect("io_uring is available"));
        let r = Arc::clone(&ring);
        let limits = thread::spawn(move || {
            let mut limits = [0, 0];
            // Before any submit the thread has no task context: nothing to read back.
            r.uring
                .submitter()
                .register_iowq_max_workers(&mut limits)
                .expect("the kernel reports io-wq limits");
            assert_eq!(limits, [0, 0], "a thread that never submitted");
            r.push(&opcode::Nop::new().build().user_data(SHUTDOWN))
                .expect("pushed");
            r.uring
                .submitter()
                .register_iowq_max_workers(&mut limits)
                .expect("the kernel reports io-wq limits");
            limits
        })
        .join()
        .unwrap();
        assert_eq!(limits[0], IOWQ_BOUNDED_WORKERS, "{limits:?}");
    }
}
