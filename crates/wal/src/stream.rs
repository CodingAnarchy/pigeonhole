//! The sidecar-file stream: one file per stream, made of preallocated slots that are recycled
//! after checkpoint.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use pigeonhole_format::wal::{
    FRAGMENT_HEADER_LEN, FRAME_SIZE, FrameEncoder, SegmentHeader, WalRecord,
};
use pigeonhole_format::{Durability, FormatVersion, Lsn, StreamId};
use pigeonhole_io::{Completion, FileRef, OpenOptions, VfsRef};

use crate::{CommitTicket, Error, Result, Wal, WalOptions, stream_path, sync_parent};

/// Frame size as a file offset.
pub(crate) const FRAME: u64 = FRAME_SIZE as u64;

/// Largest segment: 4 GiB minus one frame, so a successor's `prev_end` always fits a `u32`
/// (decision D43).
pub(crate) const MAX_SEGMENT_SIZE: u64 = (1 << 32) - FRAME;

/// Bytes written per call when zero-filling a slot.
const ZERO_CHUNK: usize = 1 << 20;

/// Epoch recorded in the stream's slot table for a slot the spare pool owns (blank, never
/// held a segment the stream knows of). Real epochs never reach this value.
const POOLED: u32 = u32::MAX;

pub(crate) fn corrupt(what: &'static str) -> Error {
    Error::Format(pigeonhole_format::Error::Corrupt { what })
}

pub(crate) fn check_options(opts: &WalOptions) -> Result<()> {
    let size = opts.segment_size;
    if !size.is_multiple_of(FRAME) || !(2 * FRAME..=MAX_SEGMENT_SIZE).contains(&size) {
        return Err(Error::InvalidArgument {
            what: "wal segment size",
        });
    }
    Ok(())
}

/// Writable, no creation.
pub(crate) fn open_existing() -> OpenOptions {
    let mut o = OpenOptions::read();
    o.write = true;
    o
}

/// The slot grid of an existing stream file: its slot size and the valid header in each slot.
#[derive(Debug)]
pub(crate) struct Grid {
    pub(crate) segment_size: u64,
    /// `None` where the slot holds no valid header (blank, or torn and therefore unused).
    pub(crate) headers: Vec<Option<SegmentHeader>>,
}

impl Grid {
    /// Reads every slot header. Returns `None` if no valid header exists anywhere in the
    /// file, in which case its slot size is unknown and nothing in it is reachable.
    pub(crate) fn read(file: &FileRef, stream: StreamId, db_id: [u8; 16]) -> Result<Option<Self>> {
        let len = file.len()?;
        let mut probe = [0u8; 64];
        // Slot 0 normally holds a valid header. If its last header write was torn, find the
        // slot size from the first valid header at any frame boundary.
        let mut segment_size = None;
        let mut off = 0;
        while off + probe.len() as u64 <= len {
            file.read_at(&mut probe, off)?;
            if let Ok(h) = SegmentHeader::decode(&probe)
                && off.is_multiple_of(h.segment_size)
            {
                segment_size = Some(h.segment_size);
                break;
            }
            off += FRAME;
        }
        let Some(segment_size) = segment_size else {
            return Ok(None);
        };
        let slots = usize::try_from(len / segment_size).map_err(|_| corrupt("wal file size"))?;
        let mut headers = Vec::with_capacity(slots);
        for slot in 0..slots {
            file.read_at(&mut probe, slot as u64 * segment_size)?;
            headers.push(match SegmentHeader::decode(&probe) {
                Ok(h) => {
                    if h.db_id != db_id {
                        return Err(Error::ForeignSegment);
                    }
                    if h.stream != stream {
                        return Err(corrupt("wal segment stream id"));
                    }
                    if h.segment_size != segment_size {
                        return Err(corrupt("wal segment size"));
                    }
                    Some(h)
                }
                Err(_) => None,
            });
        }
        Ok(Some(Self {
            segment_size,
            headers,
        }))
    }

    /// The largest epoch in any header.
    pub(crate) fn max_epoch(&self) -> u32 {
        self.headers
            .iter()
            .flatten()
            .map(|h| h.epoch)
            .max()
            .unwrap_or(0)
    }

    /// The slot holding the segment with `epoch`.
    pub(crate) fn slot_of(&self, epoch: u32) -> Option<usize> {
        self.headers
            .iter()
            .position(|h| h.is_some_and(|h| h.epoch == epoch))
    }
}

/// State shared between a [`WalStream`] (shard thread), its sync completions (I/O threads)
/// and its [`SpareSegments`] handle (a background task).
struct Shared {
    /// Durable position, raised by sync completions.
    durable: AtomicU64,
    /// Set by the first failed write or sync; every later append, write or sync is refused.
    poisoned: AtomicBool,
    pool: Mutex<Pool>,
    counters: Arc<WalCounters>,
    /// Every sync of the stream file in flight (see [`Syncs`]).
    syncs: Mutex<Syncs>,
    /// Signalled whenever a sync finishes.
    sync_done: Condvar,
}

/// Told, once a durable sync settles, whether the stream is still unpoisoned.
type Settle = Box<dyn FnOnce(bool) + Send>;

/// The syncs of a stream file in flight. On Linux an fsync error is reported to one sync
/// only: a sync that overlapped a failing one may succeed although pages it covered were
/// lost (decision D58). So every sync of the file (a group sync, a rollover's, or a side
/// sync preparing or growing a slot) takes a ticket before it is issued and poisons the
/// stream if it fails, and a successful sync that makes records durable counts only once
/// every sync started before it finished has finished too, and none failed.
#[derive(Default)]
struct Syncs {
    /// The ticket the next sync gets.
    next: u64,
    /// Tickets of the syncs issued and not finished.
    running: BTreeSet<u64>,
    /// Durable syncs that succeeded, with the ticket every older sync is below, waiting for
    /// those to finish. Each is told whether the stream is still unpoisoned then.
    settling: Vec<(u64, Settle)>,
}

impl Syncs {
    /// Whether every sync with a ticket below `barrier` has finished.
    fn drained(&self, barrier: u64) -> bool {
        self.running.first().is_none_or(|&t| t >= barrier)
    }

    /// Takes the settling syncs whose older syncs have all finished.
    fn settled(&mut self) -> Vec<Settle> {
        let mut ready = Vec::new();
        let mut i = 0;
        while i < self.settling.len() {
            if self.drained(self.settling[i].0) {
                ready.push(self.settling.swap_remove(i).1);
            } else {
                i += 1;
            }
        }
        ready
    }
}

/// Blank slots and the file's slot count. The file is `total * segment_size` bytes long (or
/// longer, if a preparation failed after extending it).
#[derive(Debug, Default)]
struct Pool {
    total: usize,
    /// Allocated, zero-filled and synced: a segment started here needs no metadata update.
    ready: Vec<usize>,
    /// Allocated but not zero-filled (grown inline, or found blank at recovery).
    blank: Vec<usize>,
    /// Slots the stream can recycle (below its checkpoint), published by the stream.
    recyclable: usize,
    /// The last preparation failed (a full disk, say): the stream stops waiting for spares
    /// and rolls over inline, which reports the failure (#19).
    prepare_failed: bool,
    /// Called when a preparation ends: a stream held back waiting for a spare (#19).
    waiters: Waiters,
}

/// Wake-ups waiting for a spare slot.
#[derive(Default)]
struct Waiters(Vec<Box<dyn FnOnce() + Send>>);

impl fmt::Debug for Waiters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} waiting", self.0.len())
    }
}

/// A stream's shard-thread stall counters ([`WalStream::counters`], ICR 0015): shared, so a
/// metrics reader on another thread sees them without the stream or its locks.
///
/// ```
/// let c = pigeonhole_wal::WalCounters::default();
/// assert_eq!((c.inline_grows(), c.inline_rollover_syncs()), (0, 0));
/// ```
#[derive(Debug, Default)]
pub struct WalCounters {
    inline_grows: AtomicU64,
    inline_rollover_syncs: AtomicU64,
    rollover_blocks: AtomicU64,
    rollover_waits: AtomicU64,
}

impl WalCounters {
    /// Rollovers that found no recyclable or ready slot and grew the file inline.
    pub fn inline_grows(&self) -> u64 {
        self.inline_grows.load(Ordering::Relaxed)
    }

    /// Rollovers that synced the full segment on the shard thread (no slot was ready).
    pub fn inline_rollover_syncs(&self) -> u64 {
        self.inline_rollover_syncs.load(Ordering::Relaxed)
    }

    /// Times the engine held a group back because the stream was blocked ([`Wal::blocked`]):
    /// its segment was nearly full while that segment's own header still waited for the
    /// previous segment's sync (#19). Counted once per hold ([`Wal::notify_unblocked`]).
    pub fn rollover_blocks(&self) -> u64 {
        self.rollover_blocks.load(Ordering::Relaxed)
    }

    /// Rollovers that had to wait on the stream's thread for the previous rollover's sync (a
    /// segment filled while its own header was still held back; a group larger than what
    /// [`Wal::blocked`] leaves room for). Expected to stay 0 (#19).
    pub fn rollover_waits(&self) -> u64 {
        self.rollover_waits.load(Ordering::Relaxed)
    }
}

/// A rollover's submitted sync of the full segment, which the successor's header must wait
/// for (FORMAT §10.1 rule 1): settled by the sync's continuation, observed without blocking
/// by the stream, and followed by work queued on it (#19).
#[derive(Default)]
struct RolloverSync {
    state: Mutex<RolloverState>,
    settled: Condvar,
}

#[derive(Default)]
struct RolloverState {
    /// `None` while in flight; then whether it succeeded.
    done: Option<bool>,
    /// Its error, until the stream reports it (once: later calls see `Poisoned`).
    error: Option<pigeonhole_io::Error>,
    /// The kind of that error, for every sync chained after it.
    kind: Option<pigeonhole_io::ErrorKind>,
    then: Vec<Box<dyn FnOnce(bool) + Send>>,
}

impl RolloverSync {
    fn lock(&self) -> MutexGuard<'_, RolloverState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the sync finished, and how: `None` while in flight.
    fn done(&self) -> Option<bool> {
        self.lock().done
    }

    /// The failed sync's error, for a sync chained after it.
    fn chained_error(&self) -> pigeonhole_io::Error {
        let kind = self.lock().kind.unwrap_or(pigeonhole_io::ErrorKind::Other);
        pigeonhole_io::Error::new(kind, "wal rollover sync failed")
    }

    /// The failed sync's error, the first time it is asked for.
    fn take_error(&self) -> Option<pigeonhole_io::Error> {
        self.lock().error.take()
    }

    /// Records the outcome and runs what waited for it (outside the lock).
    fn settle(&self, r: &pigeonhole_io::Result<()>) {
        let ok = r.is_ok();
        let then = {
            let mut st = self.lock();
            st.done = Some(ok);
            if let Err(e) = r {
                st.error = Some(pigeonhole_io::Error::new(
                    e.kind,
                    "wal rollover sync failed",
                ));
                st.kind = Some(e.kind);
            }
            std::mem::take(&mut st.then)
        };
        self.settled.notify_all();
        for f in then {
            f(ok);
        }
    }

    /// Runs `f` with the outcome once the sync finished (now if it has).
    fn then(&self, f: impl FnOnce(bool) + Send + 'static) {
        let mut st = self.lock();
        match st.done {
            Some(ok) => {
                drop(st);
                f(ok);
            }
            None => st.then.push(Box::new(f)),
        }
    }

    /// Blocks until the sync finished (the rare wait [`WalCounters::rollover_waits`]
    /// counts), reaping I/O only this thread completes meanwhile (#207).
    fn wait(&self) -> bool {
        crate::foreground::may_block("a wait for a WAL rollover sync");
        let mut st = self.lock();
        loop {
            if let Some(ok) = st.done {
                return ok;
            }
            if pigeonhole_io::own_io_in_flight() {
                drop(st);
                pigeonhole_io::reap_own_io(Some(OWN_IO_SLICE));
                st = self.lock();
                continue;
            }
            st = self
                .settled
                .wait_timeout(st, OWN_IO_SLICE)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

/// A successor segment's header frame, held back until the previous segment's rollover sync
/// is durable (FORMAT §10.1 rule 1). The successor's records are written meanwhile: under the
/// slot's stale header (an older epoch, or zeros) replay never reads them (§10.2), and none is
/// acknowledged before the header is written (#19).
struct HeldHeader {
    frame: Vec<u8>,
    /// Absolute file offset of the header (the slot's start).
    at: u64,
    /// Where the previous, full segment ends: what [`Wal::written`] reports meanwhile.
    prev_end: Lsn,
    synced: Arc<RolloverSync>,
    /// The epoch of the stale segment a recycled slot still holds (its header is on disk
    /// until this one is written); `None` for a prepared or blank slot.
    stale: Option<u32>,
}

/// How long a thread with I/O only it completes waits on that I/O before it checks the
/// stream's syncs again (#207).
const OWN_IO_SLICE: std::time::Duration = std::time::Duration::from_millis(1);

impl Shared {
    fn pool(&self) -> MutexGuard<'_, Pool> {
        self.pool.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn syncs(&self) -> MutexGuard<'_, Syncs> {
        self.syncs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers a sync about to be issued.
    fn start_sync(&self) -> u64 {
        let mut syncs = self.syncs();
        let ticket = syncs.next;
        syncs.next += 1;
        syncs.running.insert(ticket);
        ticket
    }

    /// Records that sync `ticket` finished, poisoning the stream if it failed.
    fn end_sync(&self, syncs: &mut Syncs, ticket: u64, failed: bool) {
        syncs.running.remove(&ticket);
        if failed {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    /// Tells settled syncs whether the stream is unpoisoned and wakes blocked waiters. Called
    /// without the lock held.
    fn tell(&self, ready: Vec<Settle>) {
        self.sync_done.notify_all();
        let ok = !self.poisoned.load(Ordering::Acquire);
        for f in ready {
            f(ok);
        }
    }

    /// Runs `sync`, a sync of the stream file no commit waits on (growing or preparing a
    /// slot): a failure poisons the stream.
    fn side_sync(&self, sync: impl FnOnce() -> pigeonhole_io::Result<()>) -> Result<()> {
        crate::foreground::may_block("a blocking WAL sync");
        let ticket = self.start_sync();
        let r = sync();
        let ready = {
            let mut syncs = self.syncs();
            self.end_sync(&mut syncs, ticket, r.is_err());
            syncs.settled()
        };
        self.tell(ready);
        Ok(r?)
    }

    /// Runs `sync`, a blocking sync that makes records durable, and returns once it counts:
    /// after every sync started before it finished, and only if none failed.
    fn durable_sync(&self, sync: impl FnOnce() -> pigeonhole_io::Result<()>) -> Result<()> {
        crate::foreground::may_block("a blocking WAL sync");
        let ticket = self.start_sync();
        let r = sync();
        let (ready, barrier) = {
            let mut syncs = self.syncs();
            self.end_sync(&mut syncs, ticket, r.is_err());
            (syncs.settled(), syncs.next)
        };
        self.tell(ready);
        r?;
        let mut syncs = self.syncs();
        while !syncs.drained(barrier) {
            if pigeonhole_io::own_io_in_flight() {
                // An older sync may be I/O only this thread completes (a shard driver's
                // ring, #207): reap it, outside the lock its completion takes, rather than
                // wait for ever.
                drop(syncs);
                pigeonhole_io::reap_own_io(Some(OWN_IO_SLICE));
                syncs = self.syncs();
                continue;
            }
            syncs = self
                .sync_done
                .wait(syncs)
                .unwrap_or_else(PoisonError::into_inner);
        }
        drop(syncs);
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Error::Poisoned);
        }
        Ok(())
    }

    /// [`Shared::durable_sync`] with the sync submitted to the I/O backend. The returned
    /// completion resolves once the sync counts; no thread blocks meanwhile (the last older
    /// sync to finish resolves it).
    fn submit_durable_sync(self: &Arc<Self>, file: &FileRef) -> Completion<()> {
        self.submit_durable(|| file.submit_sync_data())
    }

    /// [`Shared::submit_durable_sync`] for any submitted sync of the stream file (`submit`
    /// issues it): the open-time segments' `sync_all` goes through here too.
    fn submit_durable(self: &Arc<Self>, submit: impl FnOnce() -> Completion<()>) -> Completion<()> {
        let ticket = self.start_sync();
        let (done, resolver) = Completion::pair();
        let shared = Arc::clone(self);
        // The continuation's own completion is not needed: `resolver` reports the outcome.
        let _chained = submit().map(move |r| {
            let ready = {
                let mut syncs = shared.syncs();
                shared.end_sync(&mut syncs, ticket, r.is_err());
                match r {
                    Err(e) => {
                        let ready = syncs.settled();
                        drop(syncs);
                        shared.tell(ready);
                        resolver.resolve(Err(e));
                        return Ok(());
                    }
                    Ok(()) => {
                        let barrier = syncs.next;
                        syncs.settling.push((
                            barrier,
                            Box::new(move |ok| {
                                resolver.resolve(if ok { Ok(()) } else { Err(poisoned_io()) });
                            }),
                        ));
                        syncs.settled()
                    }
                }
            };
            shared.tell(ready);
            Ok(())
        });
        done
    }
}

fn poisoned_io() -> pigeonhole_io::Error {
    pigeonhole_io::Error::new(
        pigeonhole_io::ErrorKind::Other,
        "wal stream poisoned by a failed sync",
    )
}

/// Zero-fills slot `slot` of a stream file.
fn zero_fill(file: &FileRef, segment_size: u64, slot: usize, zeros: &[u8]) -> Result<()> {
    let start = slot as u64 * segment_size;
    let mut off = 0;
    while off < segment_size {
        let n = zeros.len().min((segment_size - off) as usize);
        file.write_at(&zeros[..n], start + off)?;
        off += n as u64;
    }
    Ok(())
}

/// Prepares spare slots for a [`WalStream`] off the shard thread.
///
/// Obtained from [`WalStream::spares`] (or [`Wal::spares`]); `Send + Sync`, so the engine
/// runs [`SpareSegments::prepare`] on a background task. A prepared slot is allocated,
/// zero-filled and synced, so a segment started in it needs no metadata update at its
/// fdatasyncs. The stream's rollover takes a recyclable slot first, then a prepared one, and
/// only grows the file inline when it has neither ([`WalStream::inline_grows`] counts those).
///
/// ```
/// use std::path::Path;
/// use pigeonhole_format::StreamId;
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_wal::{Wal, WalOptions, WalStream};
///
/// # fn main() -> pigeonhole_wal::Result<()> {
/// let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
/// let mut opts = WalOptions::default();
/// opts.segment_size = 2 * 32 * 1024;
/// let wal = WalStream::create(&vfs, Path::new("/db/x.phdb"), StreamId(0), [0; 16], opts)?;
/// let spares = wal.spares();
/// // On a background task:
/// let worker = std::thread::spawn(move || spares.prepare(spares.target()));
/// assert_eq!(worker.join().unwrap()?, 2);
/// assert_eq!(wal.spares().ready(), 2);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SpareSegments {
    file: FileRef,
    segment_size: u64,
    target: u32,
    shared: Arc<Shared>,
}

impl fmt::Debug for SpareSegments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pool = self.shared.pool();
        f.debug_struct("SpareSegments")
            .field("segment_size", &self.segment_size)
            .field("target", &self.target)
            .field("total", &pool.total)
            .field("ready", &pool.ready.len())
            .field("blank", &pool.blank.len())
            .field("recyclable", &pool.recyclable)
            .finish()
    }
}

impl SpareSegments {
    /// The stream's configured `spare_segments`: what the engine normally passes to
    /// [`SpareSegments::prepare`].
    pub fn target(&self) -> u32 {
        self.target
    }

    /// The stream's slot size in bytes.
    pub fn segment_size(&self) -> u64 {
        self.segment_size
    }

    /// Prepared slots not yet taken by the stream.
    pub fn ready(&self) -> u32 {
        self.shared.pool().ready.len() as u32
    }

    /// Makes sure the stream has at least `n` free slots (recyclable, blank or prepared),
    /// zero-filling blank slots first and growing the file by whole slots after that, then
    /// syncs once. Returns how many slots it zero-filled. Blocking file I/O: run it on a
    /// background task, never on a shard thread.
    pub fn prepare(&self, n: u32) -> Result<u32> {
        let (mut to_fill, old_total, grown) = {
            let mut pool = self.shared.pool();
            // Blank slots are not ready (a segment there needs its zero-fill first), so they
            // count as needing preparation, not as spares.
            let have = pool.recyclable + pool.ready.len();
            let need = (n as usize).saturating_sub(have);
            // Blank slots are zero-filled first; the rest are new slots at the file's end.
            let from_blank = need.min(pool.blank.len());
            let mut to_fill: Vec<usize> = pool.blank.drain(..from_blank).collect();
            let grow = need - from_blank;
            let old_total = pool.total;
            to_fill.extend(old_total..old_total + grow);
            pool.total += grow;
            (to_fill, old_total, grow)
        };
        if to_fill.is_empty() {
            return Ok(0);
        }
        let zeros = vec![0u8; ZERO_CHUNK.min(self.segment_size as usize)];
        let result = (|| {
            for &slot in &to_fill {
                self.file
                    .allocate(slot as u64 * self.segment_size, self.segment_size)?;
                zero_fill(&self.file, self.segment_size, slot, &zeros)?;
            }
            // A failed allocation or write leaves the stream usable (the slots stay blank);
            // a failed sync poisons it.
            self.shared.side_sync(|| self.file.sync_all())
        })();
        let mut pool = self.shared.pool();
        pool.prepare_failed = result.is_err();
        let waiters = std::mem::take(&mut pool.waiters.0);
        let out = match result {
            Ok(()) => {
                let filled = to_fill.len() as u32;
                pool.ready.append(&mut to_fill);
                pool.ready.sort_unstable();
                Ok(filled)
            }
            Err(e) => {
                // Give back what was reserved: blank slots stay blank, and the new slots are
                // forgotten if nothing else extended the file meanwhile (otherwise they stay
                // as blank slots; the first write to one extends the file if it must).
                if pool.total == old_total + grown {
                    pool.total = old_total;
                    to_fill.truncate(to_fill.len() - grown);
                }
                pool.blank.append(&mut to_fill);
                pool.blank.sort_unstable();
                Err(e)
            }
        };
        drop(pool);
        for wake in waiters {
            wake();
        }
        out
    }
}

/// The sidecar-file implementation: one file per stream, made of preallocated segments that
/// are recycled after checkpoint.
///
/// Appends are buffered in memory and framed as they arrive; [`Wal::write`] hands the buffer
/// to the kernel with one positional write. When a segment fills, the stream writes what is
/// left of it, syncs it and starts the successor with `prev_end` set to where the full
/// segment ended. FORMAT §10.1 rule 1 (a full segment is durable before its successor's
/// header is written) holds either way. When a recyclable or prepared slot is ready, the old
/// segment's sync is submitted to the I/O backend and never waited for on this thread (D200):
/// the successor's header is held back until that sync completes, while its records are
/// written as usual (replay ignores them under the slot's stale header, and
/// [`Wal::written`] does not count them until the header is written). Only when no such slot
/// is ready does the stream sync inline ([`WalStream::inline_rollover_syncs`] counts those;
/// D30's remaining exception).
///
/// The successor goes into a slot whose epoch is below the latest checkpoint if there is one,
/// else into a slot prepared by [`SpareSegments::prepare`] (zero-filled and synced, so its
/// fdatasyncs update no metadata), and only otherwise into a slot allocated inline
/// ([`WalStream::inline_grows`] counts those). [`WalStream::create`] starts the first segment
/// in a slot allocated past the file's end, which reads as zeros and is not zero-filled, so
/// an open writes one frame rather than a segment (#143).
///
/// **Poisoning.** A failed write or sync leaves the kernel's view of the file unknown: a later
/// successful sync could acknowledge commits behind a hole that replay would drop. So the
/// first such failure poisons the stream: every later [`Wal::append`], [`Wal::write`],
/// [`Wal::sync`] and [`Wal::submit_sync`] fails with [`Error::Poisoned`] until the engine
/// reopens the stream through [`Recovery`](crate::Recovery). Argument errors
/// ([`Error::RecordTooLarge`], [`Error::InvalidArgument`]) do not poison.
///
/// ```
/// use std::path::Path;
/// use pigeonhole_format::wal::{StreamList, WalRecord};
/// use pigeonhole_format::{Durability, StreamId};
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_wal::{Wal, WalOptions, WalStream};
///
/// # fn main() -> pigeonhole_wal::Result<()> {
/// let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
/// let mut opts = WalOptions::default();
/// opts.segment_size = 2 * 32 * 1024;
/// let mut wal = WalStream::create(&vfs, Path::new("/db/x.phdb"), StreamId(0), [0; 16], opts)?;
/// let rec = WalRecord::Commit { seqno: 1, participants: StreamList::new(&[0, 0]).unwrap() };
/// let t = wal.append(&rec, Durability::Sync)?;
/// assert!(!wal.satisfies(&t));
/// wal.sync()?;
/// assert!(wal.satisfies(&t) && wal.durable() == t.end);
/// Box::new(wal).remove()?;
/// assert!(!vfs.exists(Path::new("/db/x.phdb-wal-0"))?);
/// # Ok(())
/// # }
/// ```
pub struct WalStream {
    vfs: VfsRef,
    file: FileRef,
    path: PathBuf,
    stream: StreamId,
    db_id: [u8; 16],
    segment_size: u64,
    spare: u32,
    /// Largest record payload a segment can hold.
    max_payload: usize,
    /// Epoch of the segment each slot holds; [`POOLED`] for a blank slot the pool owns.
    slots: Vec<u32>,
    /// Largest epoch in any header ever seen or written for this stream.
    max_epoch: u32,
    /// Slots with an epoch below this are recyclable.
    checkpoint_epoch: u32,
    /// Slot of the current segment.
    slot: usize,
    /// Epoch of the current segment.
    epoch: u32,
    enc: FrameEncoder,
    /// Segment offset past the last byte handed to the kernel; `buf` starts here.
    written_off: u64,
    /// Appended, not yet written bytes of the current segment.
    buf: Vec<u8>,
    /// Record encoding scratch, reused across appends.
    scratch: Vec<u8>,
    /// The current segment's header while it waits for the previous segment's submitted
    /// sync (FORMAT §10.1 rule 1).
    held: Option<HeldHeader>,
    /// The epoch the current segment's slot held before, if it was recycled.
    stale: Option<u32>,
    shared: Arc<Shared>,
}

impl fmt::Debug for WalStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalStream")
            .field("stream", &self.stream)
            .field("path", &self.path)
            .field("segment_size", &self.segment_size)
            .field("slots", &self.slots)
            .field("epoch", &self.epoch)
            .field("written", &self.written())
            .field("durable", &self.durable())
            .field("buffered", &self.buf.len())
            .field("poisoned", &self.shared.poisoned.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Where a new segment's slot came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotSource {
    /// A slot whose segment is below the checkpoint.
    Recycled,
    /// A zero-filled, synced spare.
    Prepared,
    /// An allocated slot never zero-filled.
    Blank,
    /// A slot the open-time segment added past the file's end: allocated, never written, so
    /// it reads as zeros.
    Extended,
}

impl WalStream {
    /// Creates stream `stream` for a new database, or after its recovery found nothing.
    ///
    /// An existing file at the stream's path is truncated first, so stale segments can never
    /// be mistaken for new ones. The first slot is allocated past the file's end (never
    /// written, so it reads as zeros and needs no zero-fill), its segment's header (epoch 1)
    /// written and synced with the file's length, and the directory entry synced, before
    /// this returns. Spare slots are not
    /// prepared here: the engine runs [`SpareSegments::prepare`] on a background task.
    pub fn create(
        vfs: &VfsRef,
        db_path: &Path,
        stream: StreamId,
        db_id: [u8; 16],
        opts: WalOptions,
    ) -> Result<WalStream> {
        let s = Self::create_file(vfs, db_path, stream, db_id, opts)?;
        sync_parent(&s.vfs, &s.path)?;
        Ok(s)
    }

    /// [`WalStream::create`] for several streams, one after the other, with one directory
    /// sync for all of them at the end (an open creating a stream per shard pays one rather
    /// than one per shard, #143).
    ///
    /// ```
    /// use std::path::Path;
    /// use pigeonhole_format::StreamId;
    /// use pigeonhole_io::sim::SimVfs;
    /// use pigeonhole_wal::{WalOptions, WalStream, discover_streams};
    ///
    /// # fn main() -> pigeonhole_wal::Result<()> {
    /// let vfs: pigeonhole_io::VfsRef = SimVfs::new(1);
    /// let db = Path::new("/db/data.phdb");
    /// let mut opts = WalOptions::default();
    /// opts.segment_size = 2 * 32 * 1024;
    /// let streams = WalStream::create_all(&vfs, db, &[StreamId(0), StreamId(1)], [1; 16], opts)?;
    /// assert_eq!(streams.len(), 2);
    /// assert_eq!(discover_streams(&vfs, db)?, [StreamId(0), StreamId(1)]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_all(
        vfs: &VfsRef,
        db_path: &Path,
        streams: &[StreamId],
        db_id: [u8; 16],
        opts: WalOptions,
    ) -> Result<Vec<WalStream>> {
        // Every header first, then every sync submitted before any is waited for: on a real
        // filesystem they overlap.
        let mut created = Vec::with_capacity(streams.len());
        for &s in streams {
            let mut w = Self::create_unsynced(vfs, db_path, s, db_id, opts)?;
            let lsn = w.begin_open_segment(0, 0)?;
            created.push((w, lsn));
        }
        let syncs: Vec<_> = created
            .iter()
            .map(|(w, _)| w.shared.submit_durable(|| w.file.submit_sync_all()))
            .collect();
        let mut out = Vec::with_capacity(created.len());
        for ((w, lsn), sync) in created.into_iter().zip(syncs) {
            w.finish_open_segment(lsn, sync.wait().map_err(Error::from))?;
            out.push(w);
        }
        if let Some(s) = out.first() {
            sync_parent(&s.vfs, &s.path)?;
        }
        Ok(out)
    }

    /// `create` without the directory sync.
    fn create_file(
        vfs: &VfsRef,
        db_path: &Path,
        stream: StreamId,
        db_id: [u8; 16],
        opts: WalOptions,
    ) -> Result<WalStream> {
        let mut s = Self::create_unsynced(vfs, db_path, stream, db_id, opts)?;
        s.open_segment(0, 0)?;
        Ok(s)
    }

    /// An empty stream file with no segment yet.
    fn create_unsynced(
        vfs: &VfsRef,
        db_path: &Path,
        stream: StreamId,
        db_id: [u8; 16],
        opts: WalOptions,
    ) -> Result<WalStream> {
        check_options(&opts)?;
        let path = stream_path(db_path, stream);
        let file = vfs.open(&path, OpenOptions::read_write_create())?;
        file.set_len(0)?;
        Ok(Self::blank(
            Arc::clone(vfs),
            file,
            path,
            stream,
            db_id,
            opts.segment_size,
            opts.spare_segments,
        ))
    }

    /// A stream over `file` with no current segment yet.
    pub(crate) fn blank(
        vfs: VfsRef,
        file: FileRef,
        path: PathBuf,
        stream: StreamId,
        db_id: [u8; 16],
        segment_size: u64,
        spare: u32,
    ) -> Self {
        let frames = segment_size / FRAME;
        let max_payload = (frames as usize - 1) * (FRAME_SIZE - FRAGMENT_HEADER_LEN);
        Self {
            vfs,
            file,
            path,
            stream,
            db_id,
            segment_size,
            spare,
            max_payload,
            slots: Vec::new(),
            max_epoch: 0,
            checkpoint_epoch: 0,
            slot: 0,
            epoch: 0,
            enc: FrameEncoder::new(0, FRAME),
            written_off: 0,
            buf: Vec::new(),
            scratch: Vec::new(),
            held: None,
            stale: None,
            shared: Arc::new(Shared {
                durable: AtomicU64::new(0),
                poisoned: AtomicBool::new(false),
                pool: Mutex::new(Pool::default()),
                counters: Arc::default(),
                syncs: Mutex::new(Syncs::default()),
                sync_done: Condvar::new(),
            }),
        }
    }

    /// Adopts an existing file's slots (from recovery): `epochs` has 0 for a blank slot.
    pub(crate) fn adopt(&mut self, epochs: &[u32], max_epoch: u32, checkpoint_epoch: u32) {
        let mut pool = self.shared.pool();
        pool.total = epochs.len();
        self.slots = epochs
            .iter()
            .enumerate()
            .map(|(i, &e)| {
                if e == 0 {
                    pool.blank.push(i);
                    POOLED
                } else {
                    e
                }
            })
            .collect();
        drop(pool);
        self.max_epoch = max_epoch;
        self.checkpoint_epoch = checkpoint_epoch;
        self.publish_recyclable();
    }

    /// Starts the first segment of a fresh or reopened stream at open time, then writes its
    /// header and syncs data and length together.
    ///
    /// Only a blank slot is zero-filled first: it may hold frames of a segment whose header
    /// write was torn, under the epoch the new segment takes. A recycled slot holds only
    /// lower epochs, and a slot allocated past the file's end was never written, so it reads
    /// as zeros. That last one is what every open after a clean close uses, so an open writes
    /// one frame per stream instead of a segment (#143). The price is that the first
    /// segment's syncs also mark its blocks written (an unwritten-extent conversion), which
    /// D35's zero-filled spares avoid for every later segment.
    pub(crate) fn open_segment(&mut self, prev_epoch: u32, prev_end: u32) -> Result<()> {
        let lsn = self.begin_open_segment(prev_epoch, prev_end)?;
        let synced = self.shared.durable_sync(|| self.file.sync_all());
        self.finish_open_segment(lsn, synced)
    }

    /// `open_segment` up to its sync: returns the position the sync will make durable.
    fn begin_open_segment(&mut self, prev_epoch: u32, prev_end: u32) -> Result<Lsn> {
        let source = self.start_segment(prev_epoch, prev_end, true)?;
        if source == SlotSource::Blank {
            let zeros = vec![0u8; ZERO_CHUNK.min(self.segment_size as usize)];
            zero_fill(&self.file, self.segment_size, self.slot, &zeros)?;
        }
        self.write()
    }

    /// `open_segment` after its sync (`synced`, which covers data and length).
    fn finish_open_segment(&self, lsn: Lsn, synced: Result<()>) -> Result<()> {
        self.poison_on_err(synced)?;
        self.shared.durable.fetch_max(lsn.0, Ordering::Release);
        Ok(())
    }

    /// While the current segment's header is held back (#19): its epoch, and the epoch of the
    /// stale segment its recycled slot still holds (`None` for a prepared slot). A test hook
    /// for crash sweeps that must crash inside that window.
    #[doc(hidden)]
    pub fn held_header(&self) -> Option<(u32, Option<u32>)> {
        self.held
            .as_ref()
            .filter(|h| h.synced.done().is_none())
            .map(|h| (self.epoch, h.stale))
    }

    /// A handle for preparing spare slots on a background task.
    pub fn spares(&self) -> SpareSegments {
        SpareSegments {
            file: Arc::clone(&self.file),
            segment_size: self.segment_size,
            target: self.spare,
            shared: Arc::clone(&self.shared),
        }
    }

    /// Rollovers that found neither a recyclable nor a prepared slot and allocated one
    /// inline, on the shard thread (a sign that spares are not being prepared fast enough).
    pub fn inline_grows(&self) -> u64 {
        self.shared.counters.inline_grows()
    }

    /// Rollovers that synced the full segment on the shard thread because neither a
    /// recyclable nor a prepared slot was ready (decision D30's one exception). With spares
    /// prepared in time this stays 0.
    pub fn inline_rollover_syncs(&self) -> u64 {
        self.shared.counters.inline_rollover_syncs()
    }

    /// The stream's stall counters, shared: a metrics reader keeps the handle and reads it
    /// from any thread while the stream runs on its shard (ICR 0015).
    pub fn counters(&self) -> Arc<WalCounters> {
        Arc::clone(&self.shared.counters)
    }

    /// Whether the current segment's header still waits for the previous rollover's sync.
    fn header_waits(&self) -> bool {
        self.held
            .as_ref()
            .is_some_and(|h| h.synced.done().is_none())
    }

    /// Whether the next segment can start in a recyclable or prepared slot.
    fn spare_ready(&self) -> bool {
        (0..self.slots.len()).any(|i| self.is_recyclable(i)) || !self.shared.pool().ready.is_empty()
    }

    fn is_recyclable(&self, slot: usize) -> bool {
        let epoch = self.slots[slot];
        epoch != POOLED && epoch < self.checkpoint_epoch
    }

    fn publish_recyclable(&self) {
        let n = (0..self.slots.len())
            .filter(|&i| self.is_recyclable(i))
            .count();
        self.shared.pool().recyclable = n;
    }

    /// Picks the slot for a new segment: recyclable, then prepared, then blank, else a new
    /// slot allocated inline (counted as such unless this is the open-time segment).
    fn take_slot(&mut self, at_open: bool) -> Result<(usize, SlotSource)> {
        if let Some(i) = (0..self.slots.len()).find(|&i| self.is_recyclable(i)) {
            return Ok((i, SlotSource::Recycled));
        }
        let (slot, source) = {
            let mut pool = self.shared.pool();
            if !pool.ready.is_empty() {
                (pool.ready.remove(0), SlotSource::Prepared)
            } else if !pool.blank.is_empty() {
                (pool.blank.remove(0), SlotSource::Blank)
            } else {
                let slot = pool.total;
                pool.total += 1;
                drop(pool);
                let end = (slot as u64 + 1) * self.segment_size;
                if at_open {
                    if self.file.len()? >= end {
                        // Bytes past the slots the stream knows of (a preparation that
                        // failed after growing the file): treat them as stale.
                        (slot, SlotSource::Blank)
                    } else {
                        // Never written, so it reads as zeros and needs no zero-fill.
                        // `open_segment` syncs the length with the header.
                        self.extend_at_open(slot)?;
                        (slot, SlotSource::Extended)
                    }
                } else {
                    self.shared
                        .counters
                        .inline_grows
                        .fetch_add(1, Ordering::Relaxed);
                    self.file
                        .allocate(slot as u64 * self.segment_size, self.segment_size)?;
                    crate::foreground::exempt(|| self.shared.side_sync(|| self.file.sync_all()))?;
                    (slot, SlotSource::Blank)
                }
            }
        };
        if slot >= self.slots.len() {
            self.slots.resize(slot + 1, POOLED);
        }
        Ok((slot, source))
    }

    /// Adds slot `slot` past the file's end for the open-time segment, without writing it.
    ///
    /// On Linux the slot is allocated (`fallocate`: unwritten extents, a metadata update), so
    /// its space is reserved and a full disk fails the open rather than a later append. That
    /// also keeps the zero-read guarantee on filesystems that could otherwise expose stale
    /// freed blocks in a delayed-allocation hole after a crash (ext4 `data=writeback`, some
    /// FUSE filesystems). Elsewhere it is extended sparsely: APFS has no unwritten extents, so
    /// preallocating there writes the whole slot (64 MiB per shard per open, measured), and
    /// its holes read as zeros by design. `SimVfs` records the same operation either way.
    fn extend_at_open(&self, slot: usize) -> Result<()> {
        let (start, end) = (
            slot as u64 * self.segment_size,
            (slot as u64 + 1) * self.segment_size,
        );
        if cfg!(any(target_os = "linux", target_os = "android")) {
            self.file.allocate(start, self.segment_size)?;
        } else {
            self.file.set_len(end)?;
        }
        Ok(())
    }

    /// Starts a new segment (epoch one above every epoch seen) in a free slot, buffering its
    /// header. `buf` must be empty: the previous segment has been written out.
    fn start_segment(
        &mut self,
        prev_epoch: u32,
        prev_end: u32,
        at_open: bool,
    ) -> Result<SlotSource> {
        debug_assert!(self.buf.is_empty());
        let epoch = self
            .max_epoch
            .checked_add(1)
            .filter(|&e| e != POOLED)
            .ok_or_else(|| corrupt("wal epochs exhausted"))?;
        let (slot, source) = self.take_slot(at_open)?;
        self.stale = (source == SlotSource::Recycled).then_some(self.slots[slot]);
        self.slots[slot] = epoch;
        self.max_epoch = epoch;
        self.slot = slot;
        self.epoch = epoch;
        self.written_off = 0;
        self.enc = FrameEncoder::new(epoch, FRAME);
        self.buf.resize(FRAME_SIZE, 0);
        let header = SegmentHeader {
            version: FormatVersion::CURRENT,
            stream: self.stream,
            epoch,
            prev_epoch,
            prev_end,
            db_id: self.db_id,
            segment_size: self.segment_size,
        };
        let frame: &mut [u8; FRAME_SIZE] = (&mut self.buf[..FRAME_SIZE])
            .try_into()
            .expect("buffer holds one frame");
        header.encode(frame);
        self.publish_recyclable();
        Ok(source)
    }

    /// The current segment is full: write and sync it, then chain a new one to its end
    /// (FORMAT §10.1 rule 1). With a spare slot ready the sync is submitted, and the
    /// successor's header is held back until it is durable while its records are written as
    /// usual (#19); otherwise the sync runs inline (D30).
    fn rollover(&mut self) -> Result<()> {
        if let Some(held) = &self.held
            && held.synced.done().is_none()
        {
            // The current segment filled while its own header still waits for the previous
            // rollover's sync: it cannot be chained before that header is written. Rare
            // (`blocked` holds groups back first); counted.
            self.shared
                .counters
                .rollover_waits
                .fetch_add(1, Ordering::Relaxed);
            crate::foreground::exempt(|| held.synced.wait());
        }
        self.write_buf()?;
        let end = Lsn::new(self.epoch, self.written_off as u32);
        if self.spare_ready() {
            let shared = Arc::clone(&self.shared);
            let synced = Arc::new(RolloverSync::default());
            let settle = Arc::clone(&synced);
            // A failure poisons the stream at once and surfaces from the next `write_buf`.
            drop(self.shared.submit_durable_sync(&self.file).map(move |r| {
                if r.is_ok() {
                    shared.durable.fetch_max(end.0, Ordering::Release);
                }
                settle.settle(&r);
                r
            }));
            self.start_segment(end.epoch(), end.offset(), false)?;
            // Hold the header back; the records after it are written as they come.
            let frame = self.buf.split_off(0);
            self.written_off = FRAME;
            self.held = Some(HeldHeader {
                frame,
                at: self.slot as u64 * self.segment_size,
                prev_end: end,
                synced,
                stale: self.stale,
            });
            return Ok(());
        } else {
            // D30's remaining fallback, counted: no recyclable or prepared slot was ready
            // although `blocked` held the engine back for one (a group larger than the
            // segment's last quarter, or a failed preparation).
            crate::foreground::exempt(|| self.shared.durable_sync(|| self.file.sync_data()))?;
            self.shared.durable.fetch_max(end.0, Ordering::Release);
            self.shared
                .counters
                .inline_rollover_syncs
                .fetch_add(1, Ordering::Relaxed);
        }
        self.start_segment(end.epoch(), end.offset(), false)?;
        Ok(())
    }

    fn append_pos(&self) -> u64 {
        self.written_off + self.buf.len() as u64
    }

    /// Refuses work on a poisoned stream. A rollover sync whose failure poisoned it reports
    /// its own error first (its caller learns why, e.g. `Crashed`).
    /// Refuses work on a poisoned stream. A rollover sync whose failure poisoned it reports
    /// its own error first (its caller learns why, e.g. `Crashed`).
    #[inline]
    fn check_poisoned(&mut self) -> Result<()> {
        if self.shared.poisoned.load(Ordering::Acquire) {
            return Err(self.poisoned_error());
        }
        Ok(())
    }

    /// The error a poisoned stream reports (out of line: the commit path never takes it).
    #[cold]
    #[inline(never)]
    fn poisoned_error(&mut self) -> Error {
        match self.held.as_ref().and_then(|h| h.synced.take_error()) {
            Some(e) => e.into(),
            None => Error::Poisoned,
        }
    }

    /// Writes the held header once the previous segment's sync is durable; leaves it held
    /// while that sync is in flight (never waits). A failed sync has poisoned the stream.
    #[cold]
    #[inline(never)]
    fn release_header(&mut self) -> Result<()> {
        let Some(held) = &self.held else {
            return Ok(());
        };
        match held.synced.done() {
            None => Ok(()),
            Some(false) => Err(held
                .synced
                .take_error()
                .map_or(Error::Poisoned, Error::from)),
            Some(true) => {
                self.file.write_at(&held.frame, held.at)?;
                self.held = None;
                Ok(())
            }
        }
    }

    /// Poisons the stream if `r` is an I/O or format failure (argument errors leave it usable).
    fn poison_on_err<T>(&self, r: Result<T>) -> Result<T> {
        if matches!(&r, Err(Error::Io(_) | Error::Format(_))) {
            self.shared.poisoned.store(true, Ordering::Release);
        }
        r
    }

    #[inline]
    fn write_buf(&mut self) -> Result<()> {
        if self.held.is_some() {
            self.release_header()?;
        }
        if !self.buf.is_empty() {
            let at = self.slot as u64 * self.segment_size + self.written_off;
            self.file.write_at(&self.buf, at)?;
            self.written_off += self.buf.len() as u64;
            self.buf.clear();
        }
        Ok(())
    }

    /// Frames `scratch` into the buffer, rolling over first if it does not fit.
    fn append_scratch(&mut self) -> Result<Lsn> {
        let need = self.enc.encoded_len(self.scratch.len()) as u64;
        if self.append_pos() + need > self.segment_size {
            self.rollover()?;
        }
        let end = self.enc.encode(&self.scratch, &mut self.buf);
        Ok(Lsn::new(self.epoch, end as u32))
    }
}

impl Wal for WalStream {
    fn stream(&self) -> StreamId {
        self.stream
    }

    fn append(&mut self, record: &WalRecord<'_>, durability: Durability) -> Result<CommitTicket> {
        let _fg = crate::foreground::Foreground::enter();
        self.check_poisoned()?;
        self.scratch.clear();
        record.encode(&mut self.scratch);
        if self.scratch.len() > self.max_payload {
            return Err(Error::RecordTooLarge);
        }
        let end = self.append_scratch();
        let end = self.poison_on_err(end)?;
        Ok(CommitTicket {
            stream: self.stream,
            end,
            durability,
        })
    }

    fn write(&mut self) -> Result<Lsn> {
        let _fg = crate::foreground::Foreground::enter();
        self.check_poisoned()?;
        let r = self.write_buf();
        self.poison_on_err(r)?;
        Ok(self.written())
    }

    fn sync(&mut self) -> Result<Lsn> {
        // Blocking by contract (tests, shutdown): a held header waits for its rollover sync.
        if let Some(held) = &self.held {
            held.synced.wait();
        }
        let lsn = self.write()?;
        let r = self.shared.durable_sync(|| self.file.sync_data());
        self.poison_on_err(r)?;
        self.shared.durable.fetch_max(lsn.0, Ordering::Release);
        Ok(lsn)
    }

    fn submit_sync(&mut self) -> Result<Completion<Lsn>> {
        let _fg = crate::foreground::Foreground::enter();
        let lsn = self.write()?;
        let Some(held) = &self.held else {
            let shared = Arc::clone(&self.shared);
            return Ok(self.shared.submit_durable_sync(&self.file).map(move |r| {
                r?;
                shared.durable.fetch_max(lsn.0, Ordering::Release);
                Ok(lsn)
            }));
        };
        // The header still waits for the previous segment's sync: chain this sync after it
        // and after the header's write, without waiting here (#19). Everything appended so
        // far is written already, up to `upto`.
        let upto = Lsn::new(self.epoch, self.written_off as u32);
        let (done, resolver) = Completion::pair();
        let (shared, file) = (Arc::clone(&self.shared), Arc::clone(&self.file));
        let (frame, at) = (held.frame.clone(), held.at);
        let rollover = Arc::clone(&held.synced);
        held.synced.then(move |ok| {
            if !ok {
                resolver.resolve(Err(rollover.chained_error()));
                return;
            }
            let mut header = pigeonhole_io::IoBuf::zeroed(frame.len());
            header.copy_from_slice(&frame);
            // The same bytes the stream writes when it releases the header itself.
            drop(file.submit_write(header, at).map(move |r| {
                if let Err(e) = r {
                    shared.poisoned.store(true, Ordering::Release);
                    resolver.resolve(Err(e));
                    return Ok(());
                }
                let synced = Arc::clone(&shared);
                drop(shared.submit_durable_sync(&file).map(move |r| {
                    let r = r.map(|()| {
                        synced.durable.fetch_max(upto.0, Ordering::Release);
                        upto
                    });
                    resolver.resolve(r);
                    Ok(())
                }));
                Ok(())
            }));
        });
        Ok(done)
    }

    fn written(&self) -> Lsn {
        // Records written under a held header are not handed over yet: a process crash
        // would lose them with the header (#19).
        match &self.held {
            Some(held) => held.prev_end,
            None => Lsn::new(self.epoch, self.written_off as u32),
        }
    }

    fn durable(&self) -> Lsn {
        Lsn(self.shared.durable.load(Ordering::Acquire))
    }

    fn satisfies(&self, ticket: &CommitTicket) -> bool {
        ticket.met_by(self.stream, self.written(), self.durable())
    }

    fn checkpoint(&mut self, upto: Lsn) -> Result<()> {
        // The current segment is never recycled, so the checkpoint epoch never exceeds it.
        let epoch = upto.epoch().min(self.epoch);
        self.checkpoint_epoch = self.checkpoint_epoch.max(epoch);
        self.publish_recyclable();
        Ok(())
    }

    fn spares(&self) -> Option<SpareSegments> {
        Some(WalStream::spares(self))
    }

    fn blocked(&self) -> bool {
        // A quarter of the segment or less is left, and the rollover that filling it brings
        // could not run off this thread (#19): the segment's own header still waits for the
        // previous rollover's sync, or no recyclable or prepared slot is ready for the next
        // segment (a rollover would grow the file and sync inline). A failed preparation
        // stops the wait for spares: the inline rollover then reports the failure.
        self.segment_size - self.append_pos() <= self.segment_size / 4
            && (self.header_waits() || !self.spare_ready() && !self.shared.pool().prepare_failed)
    }

    fn notify_unblocked(&self, wake: Box<dyn FnOnce() + Send>) {
        if let Some(h) = self.held.as_ref().filter(|_| self.header_waits()) {
            // One call per time the engine holds a group back (`blocked`): counted.
            self.shared
                .counters
                .rollover_blocks
                .fetch_add(1, Ordering::Relaxed);
            h.synced.then(move |_| wake());
            return;
        }
        if !self.spare_ready() {
            let mut pool = self.shared.pool();
            // Checked again under the pool lock: a preparation ending meanwhile wakes us.
            if pool.ready.is_empty() && !pool.prepare_failed {
                self.shared
                    .counters
                    .rollover_blocks
                    .fetch_add(1, Ordering::Relaxed);
                pool.waiters.0.push(wake);
                return;
            }
        }
        wake();
    }

    fn remove(self: Box<Self>) -> Result<()> {
        let WalStream {
            vfs, file, path, ..
        } = *self;
        drop(file);
        vfs.remove(&path)?;
        sync_parent(&vfs, &path)
    }
}
