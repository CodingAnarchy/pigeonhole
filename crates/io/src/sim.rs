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
//! - **Files.** Reads see every completed write (the page cache). `sync_data` and `sync_all`
//!   make everything written to that file so far durable (size included).
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
//! - **Submitted I/O** completes before `submit_*` returns, so runs are deterministic.
//!
//! Every random decision draws from one seeded generator in a fixed order, so a seed and
//! the same sequence of calls replay exactly.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

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
/// file.sync_data()?;
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
    shm: BTreeMap<(Option<PathBuf>, String), SharedRegion>,
    killed: HashSet<ProcessId>,
    nanos: u64,
}

#[derive(Default)]
struct Node {
    /// What reads see.
    data: Vec<u8>,
    /// What the disk holds.
    durable: Vec<u8>,
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

    /// Counts a completed mutating operation and fires a scheduled crash.
    fn mutated(&mut self) {
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

    fn crash(&mut self, kind: CrashKind) {
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
                shm: BTreeMap::new(),
                killed: HashSet::new(),
                nanos: 0,
            }),
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

    /// Marks a simulated process as dead for [`Vfs::process_alive`].
    pub fn kill_process(&self, process: ProcessId) {
        self.state().killed.insert(process);
    }

    /// Makes [`Vfs::current_process`] return `process` on the calling thread (to simulate
    /// several processes in one test).
    pub fn enter_process(&self, process: ProcessId) {
        CURRENT_PROCESS.with(|m| m.borrow_mut().insert(self.id, process));
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
            st.mutated();
            Ok(())
        })
    }
}

impl Drop for SimFile {
    fn drop(&mut self) {
        let mut st = self.vfs.state();
        if !st.is_open(self.node, self.handle) {
            return;
        }
        let node = st.node(self.node);
        node.open.remove(&self.handle);
        for holders in node.locks.values_mut() {
            holders.retain(|&(h, _)| h != self.handle);
        }
        node.locks.retain(|_, holders| !holders.is_empty());
        st.gc();
    }
}

impl File for SimFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
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
            st.mutated();
            Ok(())
        })
    }

    fn submit_read(&self, mut buf: IoBuf, offset: u64) -> Completion {
        Completion::ready(self.read_at(&mut buf, offset).map(|()| buf))
    }

    fn submit_write(&self, buf: IoBuf, offset: u64) -> Completion {
        Completion::ready(self.write_at(&buf, offset).map(|()| buf))
    }

    fn sync_data(&self) -> Result<()> {
        self.with(|st| {
            let node = st.node(self.node);
            for op in std::mem::take(&mut node.pending) {
                apply(&mut node.durable, &op);
            }
            st.mutated();
            Ok(())
        })
    }

    fn submit_sync_data(&self) -> Completion<()> {
        Completion::ready(self.sync_data())
    }

    fn sync_all(&self) -> Result<()> {
        self.sync_data()
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
        }))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let mut st = self.state();
        if st.names.remove(path).is_none() {
            return Err(Error::new(ErrorKind::NotFound, "remove"));
        }
        st.gc();
        st.mutated();
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
        st.mutated();
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

    fn current_process(&self) -> ProcessId {
        CURRENT_PROCESS.with(|m| m.borrow().get(&self.id).copied().unwrap_or(DEFAULT_PROCESS))
    }

    fn process_alive(&self, process: ProcessId) -> bool {
        !self.state().killed.contains(&process)
    }
}
