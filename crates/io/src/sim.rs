//! The simulated backend: in-memory files and shared memory, deterministic from a seed, with
//! fault injection. `pigeonhole-sim` builds its scheduler, crash points and model on top.
//!
//! The simulator tracks, per file, the bytes the "disk" holds durably and the bytes written
//! since the last sync. A [`SimVfs::crash`] keeps the durable image and, depending on the
//! [`FaultPlan`], a seeded subset of the writes made since each file's last sync (possibly
//! torn at 512-byte sector granularity, possibly surviving out of order). Synced data is
//! never lost or reordered: a sync is a barrier.
//!
//! The model, precisely:
//!
//! - **Files.** Reads see every completed write (the page cache). `sync_all` makes
//!   everything written to that file so far durable, size included. `sync_data` makes the
//!   written bytes durable but not the file's length: a size change (`set_len`, `allocate`,
//!   or a write past the end) since the last `sync_all` is durable only after the next
//!   `sync_all`. A power loss before then reverts the length to the last `sync_all`'s (data
//!   synced past it is lost; a shrink that was not made durable reads back as zeros), unless
//!   a fault plan is active, in which case the pending length may survive.
//! - **Directory entries.** Creating or removing a file changes the visible namespace at
//!   once, but the change is durable only after [`Vfs::sync_dir`] on its parent directory. A
//!   power loss reverts unsynced creations and removals, like a real filesystem. Directories
//!   themselves are implicit: any path can hold files, and listing an empty directory
//!   returns nothing.
//! - **Power loss** ([`CrashKind::Power`]). Each file reverts to its durable image plus, per
//!   the plan: nothing (no faults); an in-order prefix of its unsynced writes whose last
//!   write may be torn (`torn_writes`); or an arbitrary subset of them, each possibly torn
//!   (`reorder_unsynced`). Shared memory is lost.
//! - **Process crash** ([`CrashKind::Process`]): every simulated process dies at once.
//!   Nothing is lost: written data stays in the (simulated) kernel, unsynced, and
//!   shared-memory regions survive, as `/dev/shm` and `shm_open` objects do. (A Windows named
//!   mapping would vanish once no process holds it; the simulator does not model that.)
//! - **One process crashing** ([`SimVfs::crash_process`]): only the handles opened by that
//!   process (see [`SimVfs::enter_process`]) die and release their locks; the process is
//!   marked dead for [`Vfs::process_alive`]. Data, other processes and shared memory are
//!   untouched.
//! - **Handles** killed by a crash fail with `Crashed`, and their locks are released.
//!   Reopening through the same `SimVfs` is the restart.
//! - **Size.** Files are limited to 4 GiB; a write or length past that fails with `Other`
//!   (like `EFBIG`).
//! - **Submitted I/O** completes before `submit_*` returns, unless deferred
//!   ([`SimVfs::set_deferred_io`]): then it stays in flight, and nothing about it happens
//!   (no bytes move, no crash point passes) until the simulator completes it
//!   ([`SimVfs::complete_io`], in an order the seed chooses) or a thread blocks on its
//!   [`Completion::wait`], which runs it. A crash before then fails it with `Crashed`: it
//!   never happened. A handle dropped with I/O in flight stays open (its locks released)
//!   until that I/O completes, as the kernel holds a file for its in-flight operations.
//!
//! Every random decision draws from one seeded generator in a fixed order, so a seed and
//! the same sequence of calls replay exactly. [`Vfs::random_u64`] is derived from the seed
//! with a counter of its own, so asking for values never shifts the fault decisions.
//! [`SimVfs::record_ops`] records every mutating operation, to check that a run replays.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::{
    Completion, Error, ErrorKind, File, FileIdentity, FileRef, IoBuf, LockMode, OpenOptions,
    ProcessId, Result, SharedOpen, SharedRegion, Vfs,
};

/// Which faults to inject. All decisions draw from the seeded RNG, so a seed replays exactly.
///
/// ```
/// use pigeonhole_io::sim::FaultPlan;
///
/// let mut plan = FaultPlan::none();
/// plan.torn_writes = true;
/// plan.crash_after_ops = Some(10);
/// assert_ne!(plan, FaultPlan::none());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct FaultPlan {
    /// On crash, unsynced writes may survive partially, torn at sector boundaries.
    pub torn_writes: bool,
    /// On crash, any subset of a file's unsynced writes may survive: a later one can survive
    /// while an earlier one is lost (a disk reordering its write cache). Writes before the
    /// file's last sync are always kept.
    pub reorder_unsynced: bool,
    /// Fail writes with `NoSpace` once this many bytes have been written in total.
    pub enospc_after_bytes: Option<u64>,
    /// Probability, per million operations, that a read or write fails with `Other`.
    pub io_error_ppm: u32,
    /// Crash (as [`CrashKind::Power`]) immediately after the n-th mutating operation (write,
    /// sync, set_len, remove). Iterating `n` over every value crashes at every write point.
    pub crash_after_ops: Option<u64>,
}

impl FaultPlan {
    /// No faults: the plain in-memory mock.
    pub fn none() -> Self {
        Self::default()
    }

    /// Every fault class enabled at moderate rates: torn and reordered unsynced writes,
    /// `NoSpace` after 64 MiB written, and one failed read or write per thousand. No
    /// scheduled crash (the caller picks crash points).
    pub fn all() -> Self {
        Self {
            torn_writes: true,
            reorder_unsynced: true,
            enospc_after_bytes: Some(64 << 20),
            io_error_ppm: 1_000,
            crash_after_ops: None,
        }
    }
}

/// One mutating operation, as [`SimVfs::record_ops`] records it. Files are named by their
/// simulated node number (assigned in creation order), and written bytes by a digest, so two
/// runs that did the same I/O in the same order record equal traces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimOp {
    /// `write_at` (or `submit_write`).
    Write {
        /// The file's node number.
        node: u64,
        /// Where the write started.
        offset: u64,
        /// Bytes written.
        len: u64,
        /// FNV-1a digest of the bytes written.
        digest: u64,
    },
    /// `set_len`, or an `allocate` that grew the file.
    SetLen {
        /// The file's node number.
        node: u64,
        /// The new length.
        len: u64,
    },
    /// `sync_data` (`metadata: false`) or `sync_all` (`metadata: true`).
    Sync {
        /// The file's node number.
        node: u64,
        /// Whether the length was made durable too.
        metadata: bool,
    },
    /// [`Vfs::remove`].
    Remove(PathBuf),
    /// [`Vfs::sync_dir`].
    SyncDir(PathBuf),
}

/// FNV-1a, for [`SimOp::Write`]'s digest.
fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// What a crash loses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashKind {
    /// Every simulated process dies: data handed to the kernel (`write_at` returned)
    /// survives, and shared-memory regions survive (as `/dev/shm` objects outlive their
    /// processes). To kill one process, use [`SimVfs::crash_process`].
    Process,
    /// Power loss: only synced data survives (subject to the fault plan), shared memory is
    /// lost.
    Power,
}

/// In-memory, seeded, fault-injecting [`Vfs`]. Time only moves when advanced.
///
/// ```
/// use std::path::Path;
/// use pigeonhole_io::{ErrorKind, File, OpenOptions, Vfs};
/// use pigeonhole_io::sim::{CrashKind, SimVfs};
///
/// # fn main() -> pigeonhole_io::Result<()> {
/// let vfs = SimVfs::new(42);
/// let path = Path::new("/db/data.phdb");
/// let file = vfs.open(path, OpenOptions::read_write_create())?;
/// vfs.sync_dir(Path::new("/db"))?;
/// file.write_at(b"durable", 0)?;
/// file.sync_all()?; // the write grew the file, so `sync_data` alone would not do
/// file.write_at(b"lost", 7)?;
///
/// vfs.crash(CrashKind::Power);
/// assert_eq!(file.len().unwrap_err().kind, ErrorKind::Crashed);
///
/// let file = vfs.open(path, OpenOptions::read())?;
/// assert_eq!(file.len()?, 7);
/// # Ok(())
/// # }
/// ```
pub struct SimVfs {
    id: u64,
    seed: u64,
    me: Weak<SimVfs>,
    state: Mutex<SimState>,
    /// Signalled when deferred I/O is submitted (for [`SimVfs::complete_io_in_background`]).
    io_submitted: Condvar,
}

/// Wall clock at simulated time zero: 2026-01-01T00:00:00Z, in microseconds.
const WALL_BASE_MICROS: u64 = 1_767_225_600_000_000;
/// Sector size for torn writes.
const SECTOR: usize = 512;
/// Largest simulated file.
const MAX_FILE_LEN: u64 = 1 << 32;
/// Device number every simulated file reports.
const SIM_DEVICE: u64 = 0x5137;
/// The process a thread is in until it calls [`SimVfs::enter_process`].
const DEFAULT_PROCESS: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};

static NEXT_SIM_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Simulated process of this thread, per `SimVfs`.
    static CURRENT_PROCESS: RefCell<HashMap<u64, ProcessId>> = RefCell::new(HashMap::new());
}

struct SimState {
    rng: Rng,
    plan: FaultPlan,
    names: BTreeMap<PathBuf, u64>,
    durable_names: BTreeMap<PathBuf, u64>,
    nodes: BTreeMap<u64, Node>,
    next_node: u64,
    next_handle: u64,
    bytes_written: u64,
    ops: u64,
    /// Values handed out by `Vfs::random_u64`. A counter of its own, not `rng`, so asking
    /// for one does not shift the fault decisions a seed makes.
    random_draws: u64,
    /// Mutating operations recorded since [`SimVfs::record_ops`], if recording.
    trace: Option<Vec<SimOp>>,
    shm: BTreeMap<(Option<PathBuf>, String), SharedRegion>,
    killed: HashSet<ProcessId>,
    nanos: u64,
    /// Whether `submit_*` defers its operation (see [`SimVfs::set_deferred_io`]).
    deferred: bool,
    /// Whether only the submitting thread completes a deferred operation (see
    /// [`SimVfs::set_owner_reaps`]).
    owner_reaps: bool,
    /// The thread that turned deferred I/O on, once the simulator registered as a backend
    /// no thread reaps ([`SimVfs::set_deferred_io`]): only its blocked waits run the device.
    orphan_thread: Option<std::thread::ThreadId>,
    /// Deferred operations not yet completed, in submission order.
    in_flight: Vec<InFlight>,
    next_io: u64,
    /// Picks which in-flight operation completes next. A generator of its own, so deferring
    /// I/O never shifts the fault decisions a seed makes.
    io_rng: Rng,
    /// Deferred operations per handle, counting one that is running.
    io_pins: HashMap<u64, u32>,
    /// Handles dropped while pinned by in-flight I/O: closed when it completes.
    closing: HashSet<u64>,
    /// Crashes so far (a deferred file-system operation submitted before one never runs).
    crashes: u64,
    /// Deferred file-system operations (directory syncs) are not picked by `complete_io`
    /// or the background device while set ([`SimVfs::hold_dir_syncs`]).
    hold_vfs: bool,
}

/// A deferred operation: `job` runs it on its file and resolves its completion.
struct InFlight {
    id: u64,
    /// The submitting thread, in owner-reaps mode: the only one that completes it.
    owner: Option<std::thread::ThreadId>,
    target: Target,
}

/// What a deferred operation runs on.
enum Target {
    /// A file's operation, run on a handle the operation pins open.
    File {
        node: u64,
        handle: u64,
        writable: bool,
        job: Box<dyn FnOnce(&SimFile) + Send>,
    },
    /// A file-system operation (a directory sync, ICR 0021), told whether the simulator
    /// crashed since it was submitted: then it never happened.
    Vfs { crashes: u64, job: VfsJob },
}

/// A deferred file-system operation, told whether the simulator is still alive.
type VfsJob = Box<dyn FnOnce(&SimVfs, bool) + Send>;

#[derive(Default)]
struct Node {
    /// What reads see.
    data: Vec<u8>,
    /// The data the disk holds, including bytes synced by `sync_data` past `durable_len`.
    durable: Vec<u8>,
    /// The file length the disk's metadata holds (as of the last `sync_all`).
    durable_len: usize,
    /// Changes since the last sync, in order.
    pending: Vec<Pending>,
    /// Live handles and the process that opened each.
    open: BTreeMap<u64, ProcessId>,
    /// Lock holders per byte: (handle, mode).
    locks: BTreeMap<u64, Vec<(u64, LockMode)>>,
}

/// An unsynced change; offsets and lengths were checked against `MAX_FILE_LEN`.
enum Pending {
    Write { offset: usize, data: Vec<u8> },
    SetLen(usize),
}

/// SplitMix64: tiny, fast and good enough for fault decisions.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next()) * u128::from(n)) >> 64) as u64
    }

    fn coin(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

fn crashed() -> Error {
    Error::new(ErrorKind::Crashed, "simulated crash")
}

/// `offset + len` as a file length, or an error past `MAX_FILE_LEN`.
fn file_end(offset: u64, len: usize, context: &'static str) -> Result<usize> {
    offset
        .checked_add(len as u64)
        .filter(|&end| end <= MAX_FILE_LEN)
        .and_then(|end| usize::try_from(end).ok())
        .ok_or(Error::new(ErrorKind::Other, context))
}

/// Writes `bytes` at `start` (a range already checked by `file_end`).
fn write_into(image: &mut Vec<u8>, start: usize, bytes: &[u8]) {
    let end = start + bytes.len();
    if image.len() < end {
        image.resize(end, 0);
    }
    image[start..end].copy_from_slice(bytes);
}

fn apply(image: &mut Vec<u8>, op: &Pending) {
    match op {
        Pending::Write { offset, data } => write_into(image, *offset, data),
        Pending::SetLen(len) => image.resize(*len, 0),
    }
}

/// Applies a random subset of `op`'s sectors (a length change survives whole or not at all).
fn apply_torn(image: &mut Vec<u8>, op: &Pending, rng: &mut Rng) {
    match op {
        Pending::Write { offset, data } => {
            let end = offset + data.len();
            let mut pos = *offset;
            while pos < end {
                let next = ((pos / SECTOR + 1) * SECTOR).min(end);
                if rng.coin() {
                    write_into(image, pos, &data[pos - offset..next - offset]);
                }
                pos = next;
            }
        }
        Pending::SetLen(_) => {
            if rng.coin() {
                apply(image, op);
            }
        }
    }
}

impl SimState {
    fn node(&mut self, id: u64) -> &mut Node {
        self.nodes.get_mut(&id).expect("live node")
    }

    /// Fails a read or write with probability `io_error_ppm / 10^6`.
    fn inject(&mut self, context: &'static str) -> Result<()> {
        let ppm = self.plan.io_error_ppm;
        if ppm > 0 && self.rng.below(1_000_000) < u64::from(ppm) {
            return Err(Error::new(ErrorKind::Other, context));
        }
        Ok(())
    }

    /// Counts (and, if recording, records) a completed mutating operation and fires a
    /// scheduled crash.
    fn mutated(&mut self, op: impl FnOnce() -> SimOp) {
        if let Some(trace) = &mut self.trace {
            trace.push(op());
        }
        self.ops += 1;
        if self.plan.crash_after_ops == Some(self.ops) {
            self.crash(CrashKind::Power);
        }
    }

    fn is_open(&self, node: u64, handle: u64) -> bool {
        self.nodes
            .get(&node)
            .is_some_and(|n| n.open.contains_key(&handle))
    }

    /// Closes a handle: it loses its locks, and its node goes once nothing reaches it.
    fn close(&mut self, node: u64, handle: u64) {
        if !self.is_open(node, handle) {
            return;
        }
        let node = self.node(node);
        node.open.remove(&handle);
        for holders in node.locks.values_mut() {
            holders.retain(|&(h, _)| h != handle);
        }
        node.locks.retain(|_, holders| !holders.is_empty());
        self.gc();
    }

    /// Whether an in-flight operation may be picked by the device (not a held directory sync).
    fn has_eligible_io(&self) -> bool {
        self.in_flight
            .iter()
            .any(|op| !(self.hold_vfs && matches!(op.target, Target::Vfs { .. })))
    }

    fn crash(&mut self, kind: CrashKind) {
        self.crashes += 1;
        for node in self.nodes.values_mut() {
            node.open.clear();
            node.locks.clear();
        }
        if kind == CrashKind::Power {
            self.names = self.durable_names.clone();
            self.shm.clear();
            let (torn, reorder) = (self.plan.torn_writes, self.plan.reorder_unsynced);
            for node in self.nodes.values_mut() {
                let pending = std::mem::take(&mut node.pending);
                let mut image = std::mem::take(&mut node.durable);
                // A length synced only by `sync_data` survives only by luck.
                if image.len() != node.durable_len && !((torn || reorder) && self.rng.coin()) {
                    image.resize(node.durable_len, 0);
                }
                if reorder {
                    for op in &pending {
                        if self.rng.coin() {
                            if torn && self.rng.coin() {
                                apply_torn(&mut image, op, &mut self.rng);
                            } else {
                                apply(&mut image, op);
                            }
                        }
                    }
                } else if torn {
                    let k = self.rng.below(pending.len() as u64 + 1) as usize;
                    for op in &pending[..k] {
                        apply(&mut image, op);
                    }
                    if let Some(op) = pending.get(k) {
                        apply_torn(&mut image, op, &mut self.rng);
                    }
                }
                node.data.clone_from(&image);
                node.durable_len = image.len();
                node.durable = image;
            }
        }
        self.gc();
    }

    /// Drops nodes no name (visible or durable) and no handle can reach.
    fn gc(&mut self) {
        let named: HashSet<u64> = self
            .names
            .values()
            .chain(self.durable_names.values())
            .copied()
            .collect();
        self.nodes
            .retain(|id, node| !node.open.is_empty() || named.contains(id));
    }
}

impl SimVfs {
    /// A fault-free simulated filesystem (the in-memory mock).
    pub fn new(seed: u64) -> Arc<Self> {
        Self::with_faults(seed, FaultPlan::none())
    }

    /// A simulated filesystem injecting `plan`.
    pub fn with_faults(seed: u64, plan: FaultPlan) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            id: NEXT_SIM_ID.fetch_add(1, Ordering::Relaxed),
            seed,
            me: me.clone(),
            state: Mutex::new(SimState {
                rng: Rng(seed),
                plan,
                names: BTreeMap::new(),
                durable_names: BTreeMap::new(),
                nodes: BTreeMap::new(),
                next_node: 1,
                next_handle: 1,
                bytes_written: 0,
                ops: 0,
                random_draws: 0,
                trace: None,
                shm: BTreeMap::new(),
                killed: HashSet::new(),
                nanos: 0,
                deferred: false,
                owner_reaps: false,
                orphan_thread: None,
                in_flight: Vec::new(),
                next_io: 0,
                io_rng: Rng(seed ^ 0x6A09_E667_F3BC_C908),
                io_pins: HashMap::new(),
                closing: HashSet::new(),
                crashes: 0,
                hold_vfs: false,
            }),
            io_submitted: Condvar::new(),
        })
    }

    fn state(&self) -> MutexGuard<'_, SimState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The seed, for printing on failure.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Replaces the fault plan.
    pub fn set_faults(&self, plan: FaultPlan) {
        self.state().plan = plan;
    }

    /// Simulates a crash: every open handle starts failing with `Crashed`, and the stored
    /// state is reduced to what `kind` and the fault plan allow. Reopen through this same
    /// `SimVfs` to recover.
    pub fn crash(&self, kind: CrashKind) {
        self.state().crash(kind);
    }

    /// Simulates one process crashing (as by `SIGKILL`): the handles it opened fail with
    /// `Crashed` and lose their locks, and [`Vfs::process_alive`] reports it dead. Written
    /// data, shared memory and every other process's handles are untouched.
    pub fn crash_process(&self, process: ProcessId) {
        let mut st = self.state();
        for node in st.nodes.values_mut() {
            let dead: Vec<u64> = node
                .open
                .iter()
                .filter(|&(_, &p)| p == process)
                .map(|(&h, _)| h)
                .collect();
            if dead.is_empty() {
                continue;
            }
            node.open.retain(|h, _| !dead.contains(h));
            for holders in node.locks.values_mut() {
                holders.retain(|(h, _)| !dead.contains(h));
            }
            node.locks.retain(|_, holders| !holders.is_empty());
        }
        st.killed.insert(process);
        st.gc();
    }

    /// Advances both clocks.
    pub fn advance(&self, nanos: u64) {
        let mut st = self.state();
        st.nanos = st.nanos.saturating_add(nanos);
    }

    /// Mutating operations performed so far (to size a crash-at-every-point sweep).
    pub fn mutating_ops(&self) -> u64 {
        self.state().ops
    }

    /// Starts recording every mutating operation (discarding anything recorded so far), to
    /// check that a seed replays the same I/O: see [`SimVfs::recorded_ops`].
    ///
    /// ```
    /// use std::path::Path;
    /// use pigeonhole_io::{File, OpenOptions, Vfs};
    /// use pigeonhole_io::sim::{SimOp, SimVfs};
    ///
    /// # fn main() -> pigeonhole_io::Result<()> {
    /// let vfs = SimVfs::new(1);
    /// vfs.record_ops();
    /// let file = vfs.open(Path::new("/db/f"), OpenOptions::read_write_create())?;
    /// file.write_at(b"x", 0)?;
    /// file.sync_all()?;
    /// let ops = vfs.recorded_ops();
    /// assert_eq!(ops.len(), 2);
    /// assert!(matches!(ops[1], SimOp::Sync { metadata: true, .. }));
    /// # Ok(())
    /// # }
    /// ```
    pub fn record_ops(&self) {
        self.state().trace = Some(Vec::new());
    }

    /// The mutating operations recorded since [`SimVfs::record_ops`], in the order they
    /// completed (empty if not recording). Recording continues.
    pub fn recorded_ops(&self) -> Vec<SimOp> {
        self.state().trace.clone().unwrap_or_default()
    }

    /// Marks a simulated process as dead for [`Vfs::process_alive`].
    pub fn kill_process(&self, process: ProcessId) {
        self.state().killed.insert(process);
    }

    /// Makes [`Vfs::current_process`] return `process` on the calling thread (to simulate
    /// several processes in one test).
    pub fn enter_process(&self, process: ProcessId) {
        CURRENT_PROCESS.with(|m| m.borrow_mut().insert(self.id, process));
    }

    /// Defers I/O submitted from now on (`submit_read`, `submit_write`, `submit_sync_data`,
    /// `submit_sync_all`, `submit_sync_dir`): each operation stays in flight until
    /// [`SimVfs::complete_io`] (or a blocking [`Completion::wait`] on it) runs it, so a
    /// simulated run has I/O in flight across its scheduling points, as on a real device. Off
    /// (the default), submitted I/O completes before `submit_*` returns. Turning it off
    /// leaves operations already in flight there.
    ///
    /// Someone must complete deferred I/O: the scheduler (`pigeonhole-sim`'s `Sim` does it
    /// as one more task), the harness, or [`SimVfs::complete_io_in_background`] for code
    /// that runs on threads of its own.
    ///
    /// ```
    /// use std::path::Path;
    /// use pigeonhole_io::{File, IoBuf, OpenOptions, Vfs};
    /// use pigeonhole_io::sim::SimVfs;
    ///
    /// # fn main() -> pigeonhole_io::Result<()> {
    /// let vfs = SimVfs::new(7);
    /// vfs.set_deferred_io(true);
    /// let file = vfs.open(Path::new("/db/f"), OpenOptions::read_write_create())?;
    /// let mut buf = IoBuf::zeroed(4);
    /// buf.copy_from_slice(b"data");
    /// let write = file.submit_write(buf, 0);
    /// let sync = file.submit_sync_data();
    /// assert_eq!((file.len()?, vfs.io_in_flight()), (0, 2));
    /// assert!(vfs.complete_io()); // one of the two, chosen by the seed
    /// vfs.complete_all_io();
    /// assert!(write.is_ready() && sync.is_ready());
    /// assert_eq!(file.len()?, 4);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Without [`SimVfs::set_owner_reaps`], the simulator is then a backend no thread reaps:
    /// the thread that turned deferred I/O on (the harness, which completes the device's
    /// operations between its steps) runs one deferred operation, chosen by the seed, each
    /// time it blocks in a wait that reaps such backends (`Completion::wait` with no drive
    /// of its own, or a WAL sync behind the stream's older syncs; ICR 0028), as a real
    /// device finishes I/O on its own. Without it, an engine wait inside the harness's own
    /// turn, behind I/O only the harness completes, would wait for ever. Other threads'
    /// waits never run it, so tests sharing a process stay independent and seeds replay.
    pub fn set_deferred_io(&self, on: bool) {
        let mut st = self.state();
        st.deferred = on;
        if on && st.orphan_thread.is_none() {
            st.orphan_thread = Some(std::thread::current().id());
            drop(st);
            let orphan: Weak<dyn crate::own::OrphanIo> = self.me.clone();
            crate::own::register_orphan(orphan);
        }
    }

    /// With deferred I/O, completes a deferred operation only on the thread that submitted
    /// it, as an io_uring ring owned by a shard thread does (#207): by
    /// [`reap_own_io`](crate::reap_own_io), or a blocking [`Completion::wait`] on that
    /// thread. A wait on another thread blocks until the owner reaps. Operations submitted
    /// before the switch keep the mode they were submitted in;
    /// [`SimVfs::complete_io`] still completes any of them (a harness's override).
    ///
    /// ```
    /// use std::path::Path;
    /// use pigeonhole_io::{File, IoBuf, OpenOptions, Vfs, own_io_in_flight, reap_own_io};
    /// use pigeonhole_io::sim::SimVfs;
    ///
    /// # fn main() -> pigeonhole_io::Result<()> {
    /// let vfs = SimVfs::new(7);
    /// vfs.set_deferred_io(true);
    /// vfs.set_owner_reaps(true);
    /// let file = vfs.open(Path::new("/db/f"), OpenOptions::read_write_create())?;
    /// let sync = file.submit_sync_data();
    /// assert!(own_io_in_flight());
    /// // Another thread has none of its own.
    /// assert!(!std::thread::spawn(own_io_in_flight).join().unwrap());
    /// // A thread that waits finds the simulated device done; one that only polls does not.
    /// assert!(!reap_own_io(None));
    /// assert!(reap_own_io(Some(std::time::Duration::ZERO)));
    /// assert!(sync.is_ready() && !own_io_in_flight());
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_owner_reaps(&self, on: bool) {
        self.state().owner_reaps = on;
    }

    /// Holds deferred directory syncs ([`Vfs::submit_sync_dir`]) back while `on`: neither
    /// [`SimVfs::complete_io`] nor the background device picks them (a thread blocked on one
    /// still runs it). A test hook for ordering after a directory sync (#158, D203).
    pub fn hold_dir_syncs(&self, on: bool) {
        self.state().hold_vfs = on;
        self.io_submitted.notify_all();
    }

    /// Deferred operations in flight.
    pub fn io_in_flight(&self) -> usize {
        self.state().in_flight.len()
    }

    /// Completes one in-flight deferred operation, chosen by the seed, on the calling thread
    /// (its completion's continuations run here too). Returns `false` if none was in flight.
    pub fn complete_io(&self) -> bool {
        self.complete(None)
    }

    /// Completes deferred operations until none is in flight, including any their
    /// continuations submit.
    pub fn complete_all_io(&self) {
        while self.complete(None) {}
    }

    /// Completes deferred I/O on a thread of its own, one operation at a time in an order
    /// the seed chooses, a few yields after each is found in flight, for code whose threads
    /// block on submitted I/O (an engine running its own shard threads). The order is
    /// seeded, but what is in flight when depends on the OS scheduler, so such runs do not
    /// replay exactly. The thread ends once the `SimVfs` is dropped.
    pub fn complete_io_in_background(self: &Arc<Self>) -> JoinHandle<()> {
        let me = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("sim-io".into())
            .spawn(move || {
                while let Some(vfs) = me.upgrade() {
                    let yields = {
                        let (st, _) = vfs
                            .io_submitted
                            .wait_timeout_while(vfs.state(), Duration::from_millis(1), |st| {
                                !st.has_eligible_io()
                            })
                            .unwrap_or_else(PoisonError::into_inner);
                        let mut st = st;
                        st.has_eligible_io().then(|| st.io_rng.below(8))
                    };
                    if let Some(yields) = yields {
                        for _ in 0..yields {
                            std::thread::yield_now();
                        }
                        vfs.complete(None);
                    }
                }
            })
            .expect("spawn the simulated I/O thread")
    }

    /// Runs `run` on `file` now, or defers it (see [`SimVfs::set_deferred_io`]).
    fn submit<T: Send + 'static>(
        &self,
        file: &SimFile,
        run: impl FnOnce(&SimFile) -> Result<T> + Send + 'static,
    ) -> Completion<T> {
        let mut st = self.state();
        if !st.deferred {
            drop(st);
            return Completion::ready(run(file));
        }
        let id = st.next_io;
        st.next_io += 1;
        *st.io_pins.entry(file.handle).or_default() += 1;
        let owner = st.owner_reaps.then(|| std::thread::current().id());
        if owner.is_some() {
            let own: Weak<dyn crate::own::OwnIo> = self.me.clone();
            crate::own::register(own);
        }
        let me = self.me.clone();
        let (done, resolver) = Completion::driven_pair(Some(Arc::new(move || {
            // An owner-reaped operation runs only when its own thread blocks on it.
            if owner.is_none_or(|o| o == std::thread::current().id())
                && let Some(vfs) = me.upgrade()
            {
                vfs.complete(Some(id));
            }
            // Ran, or another thread must: either way, once is enough.
            false
        })));
        st.in_flight.push(InFlight {
            id,
            owner,
            target: Target::File {
                node: file.node,
                handle: file.handle,
                writable: file.writable,
                job: Box::new(move |file| resolver.resolve(run(file))),
            },
        });
        drop(st);
        self.io_submitted.notify_all();
        done
    }

    /// A deferred file-system operation (no file handle): as [`SimVfs::submit`].
    fn submit_vfs<T: Send + 'static>(
        &self,
        run: impl FnOnce(&SimVfs) -> Result<T> + Send + 'static,
    ) -> Completion<T> {
        let mut st = self.state();
        if !st.deferred {
            drop(st);
            return Completion::ready(run(self));
        }
        let id = st.next_io;
        st.next_io += 1;
        let owner = st.owner_reaps.then(|| std::thread::current().id());
        if owner.is_some() {
            let own: Weak<dyn crate::own::OwnIo> = self.me.clone();
            crate::own::register(own);
        }
        let me = self.me.clone();
        let (done, resolver) = Completion::driven_pair(Some(Arc::new(move || {
            if owner.is_none_or(|o| o == std::thread::current().id())
                && let Some(vfs) = me.upgrade()
            {
                vfs.complete(Some(id));
            }
            false
        })));
        let crashes = st.crashes;
        st.in_flight.push(InFlight {
            id,
            owner,
            target: Target::Vfs {
                crashes,
                job: Box::new(move |vfs, alive| {
                    resolver.resolve(if alive { run(vfs) } else { Err(crashed()) });
                }),
            },
        });
        drop(st);
        self.io_submitted.notify_all();
        done
    }

    /// Completes the in-flight operation `id` (if still in flight), or one chosen by the
    /// seed. Returns whether one ran.
    fn complete(&self, id: Option<u64>) -> bool {
        let op = {
            let mut st = self.state();
            let i = match id {
                Some(id) => st.in_flight.iter().position(|op| op.id == id),
                None => {
                    // While directory syncs are held, the device picks among the rest.
                    let held = st.hold_vfs;
                    let eligible: Vec<usize> = (0..st.in_flight.len())
                        .filter(|&i| {
                            !(held && matches!(st.in_flight[i].target, Target::Vfs { .. }))
                        })
                        .collect();
                    if eligible.is_empty() {
                        None
                    } else {
                        let n = eligible.len() as u64;
                        Some(eligible[st.io_rng.below(n) as usize])
                    }
                }
            };
            match i {
                Some(i) => st.in_flight.remove(i),
                None => return false,
            }
        };
        let (node, handle, writable, job) = match op.target {
            Target::File {
                node,
                handle,
                writable,
                job,
            } => (node, handle, writable, job),
            Target::Vfs { crashes, job } => {
                let alive = self.state().crashes == crashes;
                job(self, alive);
                return true;
            }
        };
        let file = SimFile {
            vfs: self.me.upgrade().expect("SimVfs is alive while borrowed"),
            node,
            handle,
            writable,
            direct: false,
            owner: false,
        };
        job(&file);
        let mut st = self.state();
        let pins = st.io_pins.get_mut(&handle).expect("pinned by its I/O");
        *pins -= 1;
        if *pins == 0 {
            st.io_pins.remove(&handle);
            if st.closing.remove(&handle) {
                st.close(node, handle);
            }
        }
        true
    }
}

impl crate::own::OrphanIo for SimVfs {
    /// One deferred operation (the seed picks which; held directory syncs stay held), for
    /// the thread that turned deferred I/O on, while it blocks; nothing in owner-reaps mode.
    fn reap_orphan(&self, wait: Duration) -> bool {
        {
            let st = self.state();
            if !st.deferred
                || st.owner_reaps
                || st.orphan_thread != Some(std::thread::current().id())
            {
                return false;
            }
        }
        if self.complete(None) {
            return true;
        }
        // Nothing to run (all held, or none in flight): wait as a device would, rather than
        // spin the caller's loop.
        std::thread::sleep(wait.min(Duration::from_millis(1)));
        false
    }
}

impl crate::own::OwnIo for SimVfs {
    fn in_flight_here(&self) -> bool {
        let me = std::thread::current().id();
        self.state().in_flight.iter().any(|op| op.owner == Some(me))
    }

    /// Runs every operation this thread submitted in owner-reaps mode, including any their
    /// continuations submit, when the thread waits (`Some`): the simulated device finishes
    /// them for a thread that waits for it, never in a non-waiting reap (`None`), so a
    /// shard's turn leaves them in flight as a harness expects.
    fn reap_here(&self, wait: Option<Duration>) -> bool {
        if wait.is_none() {
            return false;
        }
        let me = std::thread::current().id();
        let mut any = false;
        loop {
            let next = {
                let st = self.state();
                st.in_flight
                    .iter()
                    .find(|op| op.owner == Some(me))
                    .map(|op| op.id)
            };
            match next {
                Some(id) => any |= self.complete(Some(id)),
                None => return any,
            }
        }
    }
}

impl fmt::Debug for SimVfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimVfs")
            .field("seed", &self.seed)
            .field("plan", &self.state().plan)
            .finish_non_exhaustive()
    }
}

/// A handle to a simulated file.
struct SimFile {
    vfs: Arc<SimVfs>,
    node: u64,
    handle: u64,
    writable: bool,
    /// Opened for direct I/O: every read and write must be aligned to [`SIM_DIRECT_ALIGN`],
    /// as the kernel demands of `O_DIRECT` (#403), so a misaligned path fails in simulation.
    direct: bool,
    /// Whether dropping this closes the handle (`false` for the view a deferred operation
    /// runs on).
    owner: bool,
}

impl fmt::Debug for SimFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimFile")
            .field("node", &self.node)
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl SimFile {
    /// Runs `f` on the shared state if this handle has not been crashed.
    fn with<R>(&self, f: impl FnOnce(&mut SimState) -> Result<R>) -> Result<R> {
        let mut st = self.vfs.state();
        if !st.is_open(self.node, self.handle) {
            return Err(crashed());
        }
        f(&mut st)
    }

    fn check_writable(&self, context: &'static str) -> Result<()> {
        if self.writable {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::Other, context))
        }
    }

    fn change_len(&self, len: u64) -> Result<()> {
        self.check_writable("set_len: file not opened for writing")?;
        let len = file_end(len, 0, "set_len: beyond the simulated file size limit")?;
        self.with(|st| {
            let node = st.node(self.node);
            node.data.resize(len, 0);
            node.pending.push(Pending::SetLen(len));
            st.mutated(|| SimOp::SetLen {
                node: self.node,
                len: len as u64,
            });
            Ok(())
        })
    }

    /// Makes the pending writes durable, and the length too when `metadata` is set.
    fn sync(&self, metadata: bool) -> Result<()> {
        self.with(|st| {
            let node = st.node(self.node);
            for op in std::mem::take(&mut node.pending) {
                apply(&mut node.durable, &op);
            }
            if metadata {
                node.durable_len = node.durable.len();
            }
            st.mutated(|| SimOp::Sync {
                node: self.node,
                metadata,
            });
            Ok(())
        })
    }
}

impl Drop for SimFile {
    fn drop(&mut self) {
        if !self.owner {
            return;
        }
        let mut st = self.vfs.state();
        if !st.io_pins.contains_key(&self.handle) {
            st.close(self.node, self.handle);
        } else if st.is_open(self.node, self.handle) {
            // Closed when its in-flight I/O completes; the locks go now, as with `close(2)`.
            st.closing.insert(self.handle);
            let node = st.node(self.node);
            for holders in node.locks.values_mut() {
                holders.retain(|&(h, _)| h != self.handle);
            }
            node.locks.retain(|_, holders| !holders.is_empty());
        }
    }
}

/// The direct-I/O alignment the simulator enforces (the common device and page size).
pub const SIM_DIRECT_ALIGN: usize = 4096;

impl SimFile {
    /// On a direct handle, fails an I/O whose offset, length or buffer is not aligned.
    fn check_aligned(&self, ptr: *const u8, len: usize, offset: u64) -> Result<()> {
        let a = SIM_DIRECT_ALIGN;
        if self.direct
            && (!offset.is_multiple_of(a as u64)
                || !len.is_multiple_of(a)
                || !(ptr as usize).is_multiple_of(a))
        {
            return Err(Error::new(ErrorKind::Misaligned, "direct I/O"));
        }
        Ok(())
    }
}

impl File for SimFile {
    fn direct_align(&self) -> Option<usize> {
        self.direct.then_some(SIM_DIRECT_ALIGN)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.check_aligned(buf.as_ptr(), buf.len(), offset)?;
        self.with(|st| {
            if buf.is_empty() {
                return Ok(());
            }
            st.inject("read: injected I/O error")?;
            let data = &st.node(self.node).data;
            let end = offset.checked_add(buf.len() as u64);
            match end {
                Some(end) if end <= data.len() as u64 => {
                    buf.copy_from_slice(&data[offset as usize..end as usize]);
                    Ok(())
                }
                _ => Err(Error::new(ErrorKind::UnexpectedEof, "read")),
            }
        })
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.check_writable("write: file not opened for writing")?;
        self.check_aligned(buf.as_ptr(), buf.len(), offset)?;
        self.with(|st| {
            if buf.is_empty() {
                return Ok(());
            }
            st.inject("write: injected I/O error")?;
            file_end(
                offset,
                buf.len(),
                "write: beyond the simulated file size limit",
            )?;
            let start = offset as usize; // in range: checked just above
            let len = buf.len() as u64;
            if let Some(limit) = st.plan.enospc_after_bytes
                && st.bytes_written + len > limit
            {
                return Err(Error::new(ErrorKind::NoSpace, "write"));
            }
            st.bytes_written += len;
            let node = st.node(self.node);
            write_into(&mut node.data, start, buf);
            node.pending.push(Pending::Write {
                offset: start,
                data: buf.to_vec(),
            });
            st.mutated(|| SimOp::Write {
                node: self.node,
                offset,
                len,
                digest: digest(buf),
            });
            Ok(())
        })
    }

    fn submit_read(&self, mut buf: IoBuf, offset: u64) -> Completion {
        if let Err(e) = self.check_aligned(buf.as_ptr(), buf.len(), offset) {
            return Completion::ready(Err(e));
        }
        self.vfs
            .submit(self, move |f| f.read_at(&mut buf, offset).map(|()| buf))
    }

    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        if let Err(e) = self.check_aligned(buf.as_ptr(), buf.len(), offset) {
            return Completion::ready(Err(e));
        }
        self.vfs
            .submit(self, move |f| f.write_at(&buf, offset).map(|()| buf))
    }

    fn sync_data(&self) -> Result<()> {
        self.sync(false)
    }

    fn submit_sync_data(&self) -> Completion<()> {
        self.vfs.submit(self, SimFile::sync_data)
    }

    fn sync_all(&self) -> Result<()> {
        self.sync(true)
    }

    fn submit_sync_all(&self) -> Completion<()> {
        self.vfs.submit(self, SimFile::sync_all)
    }

    fn len(&self) -> Result<u64> {
        self.with(|st| Ok(st.node(self.node).data.len() as u64))
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.change_len(len)
    }

    fn allocate(&self, offset: u64, len: u64) -> Result<()> {
        let end = offset
            .checked_add(len)
            .ok_or(Error::new(ErrorKind::Other, "allocate: range overflows"))?;
        let cur = self.len()?;
        if end > cur {
            self.change_len(end)
        } else {
            self.check_writable("allocate: file not opened for writing")
        }
    }

    fn lock(&self, byte: u64, mode: LockMode) -> Result<()> {
        if mode == LockMode::Exclusive && !self.writable {
            return Err(crate::pread::exclusive_needs_write());
        }
        self.with(|st| {
            let holders = st.node(self.node).locks.entry(byte).or_default();
            let conflict = holders.iter().any(|&(h, m)| {
                h != self.handle && (mode == LockMode::Exclusive || m == LockMode::Exclusive)
            });
            if conflict {
                return Err(Error::new(ErrorKind::Locked, "lock"));
            }
            holders.retain(|&(h, _)| h != self.handle);
            holders.push((self.handle, mode));
            Ok(())
        })
    }

    fn unlock(&self, byte: u64) -> Result<()> {
        self.with(|st| {
            let locks = &mut st.node(self.node).locks;
            if let Some(holders) = locks.get_mut(&byte) {
                holders.retain(|&(h, _)| h != self.handle);
                if holders.is_empty() {
                    locks.remove(&byte);
                }
            }
            Ok(())
        })
    }

    fn identity(&self) -> Result<FileIdentity> {
        self.with(|_| {
            Ok(FileIdentity {
                device: SIM_DEVICE,
                inode: self.node,
            })
        })
    }

    fn is_local(&self) -> Result<bool> {
        self.with(|_| Ok(true))
    }
}

impl Vfs for SimVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> Result<FileRef> {
        let process = self.current_process();
        let mut st = self.state();
        let id = match st.names.get(path) {
            Some(_) if opts.create_new => {
                return Err(Error::new(ErrorKind::AlreadyExists, "open"));
            }
            Some(&id) => id,
            None if opts.create || opts.create_new => {
                let id = st.next_node;
                st.next_node += 1;
                st.nodes.insert(id, Node::default());
                st.names.insert(path.to_path_buf(), id);
                id
            }
            None => return Err(Error::new(ErrorKind::NotFound, "open")),
        };
        let handle = st.next_handle;
        st.next_handle += 1;
        st.node(id).open.insert(handle, process);
        Ok(Arc::new(SimFile {
            vfs: self.me.upgrade().expect("SimVfs is alive while borrowed"),
            node: id,
            handle,
            writable: opts.write || opts.create || opts.create_new,
            direct: opts.direct,
            owner: true,
        }))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let mut st = self.state();
        if st.names.remove(path).is_none() {
            return Err(Error::new(ErrorKind::NotFound, "remove"));
        }
        st.gc();
        st.mutated(|| SimOp::Remove(path.to_path_buf()));
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        Ok(self.state().names.contains_key(path))
    }

    fn list_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        Ok(self
            .state()
            .names
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .cloned()
            .collect())
    }

    fn submit_sync_dir(&self, dir: &Path) -> Completion<()> {
        // Deferred like any submitted operation (ICR 0021): the directory's entries become
        // durable only when the simulated device completes it, and a crash before then
        // means it never happened.
        let dir = dir.to_path_buf();
        self.submit_vfs(move |vfs| vfs.sync_dir(&dir))
    }

    fn sync_dir(&self, dir: &Path) -> Result<()> {
        let mut st = self.state();
        st.durable_names.retain(|p, _| p.parent() != Some(dir));
        let entries: Vec<(PathBuf, u64)> = st
            .names
            .iter()
            .filter(|(p, _)| p.parent() == Some(dir))
            .map(|(p, &id)| (p.clone(), id))
            .collect();
        st.durable_names.extend(entries);
        st.gc();
        st.mutated(|| SimOp::SyncDir(dir.to_path_buf()));
        Ok(())
    }

    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: SharedOpen,
    ) -> Result<SharedRegion> {
        crate::os::check_shared_name(name)?;
        if len == 0 {
            return Err(Error::new(ErrorKind::Other, "shared region is empty"));
        }
        let size =
            usize::try_from(len).map_err(|_| Error::new(ErrorKind::Other, "region too large"))?;
        let key = (dir.map(Path::to_path_buf), name.to_owned());
        let mut st = self.state();
        match (mode, st.shm.get(&key)) {
            (SharedOpen::CreateNew, Some(_)) => {
                Err(Error::new(ErrorKind::AlreadyExists, "open shared region"))
            }
            (SharedOpen::CreateNew, None) => {
                let region = SharedRegion::heap(size);
                st.shm.insert(key, region.clone());
                Ok(region)
            }
            (SharedOpen::Attach, None) => {
                Err(Error::new(ErrorKind::NotFound, "open shared region"))
            }
            (SharedOpen::Attach, Some(region)) if region.len() < size => Err(Error::new(
                ErrorKind::Other,
                "shared region is smaller than requested",
            )),
            (SharedOpen::Attach, Some(region)) => Ok(region.clone()),
        }
    }

    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> Result<()> {
        crate::os::check_shared_name(name)?;
        let key = (dir.map(Path::to_path_buf), name.to_owned());
        match self.state().shm.remove(&key) {
            Some(_) => Ok(()),
            None => Err(Error::new(ErrorKind::NotFound, "remove shared region")),
        }
    }

    fn now_micros(&self) -> u64 {
        WALL_BASE_MICROS + self.state().nanos / 1_000
    }

    fn monotonic_nanos(&self) -> u64 {
        self.state().nanos
    }

    fn clock_is_simulated(&self) -> bool {
        true
    }

    fn current_process(&self) -> ProcessId {
        CURRENT_PROCESS.with(|m| m.borrow().get(&self.id).copied().unwrap_or(DEFAULT_PROCESS))
    }

    fn process_alive(&self, process: ProcessId) -> bool {
        !self.state().killed.contains(&process)
    }

    /// Derived from the seed and a counter of its own (not the fault generator), so a seed
    /// replays the same values and asking for them never shifts its fault decisions.
    fn random_u64(&self) -> u64 {
        let mut st = self.state();
        st.random_draws += 1;
        Rng(self.seed ^ st.random_draws.wrapping_mul(0xA076_1D64_78BD_642F)).next()
    }
}
