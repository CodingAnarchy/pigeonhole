//! The simulated backend: in-memory files and shared memory, deterministic from a seed, with
//! fault injection. `pigeonhole-sim` builds its scheduler, crash points and model on top.
//!
//! The simulator tracks, per file, the bytes the "disk" holds durably and the bytes written
//! since the last sync. A [`SimVfs::crash`] keeps the durable image and, depending on the
//! [`FaultPlan`], a seeded subset of unsynced writes (possibly torn at 512-byte sector
//! granularity, possibly reordered across fsyncs).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{FileRef, OpenOptions, ProcessId, Result, SharedOpen, SharedRegion, Vfs};

/// Which faults to inject. All decisions draw from the seeded RNG, so a seed replays exactly.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FaultPlan {
    /// On crash, unsynced writes may survive partially, torn at sector boundaries.
    pub torn_writes: bool,
    /// On crash, writes from after an fsync may survive while earlier unsynced ones do not
    /// (models a disk that reorders without barriers).
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

    /// Every fault class enabled at moderate rates.
    pub fn all() -> Self {
        todo!()
    }
}

/// What a crash loses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashKind {
    /// The process dies: data handed to the kernel (`write_at` returned) survives, shared
    /// memory survives if another process holds it.
    Process,
    /// Power loss: only synced data survives (subject to the fault plan), shared memory is
    /// lost.
    Power,
}

/// In-memory, seeded, fault-injecting [`Vfs`]. Time only moves when advanced.
#[derive(Debug)]
pub struct SimVfs {
    _priv: (),
}

impl SimVfs {
    /// A fault-free simulated filesystem (the in-memory mock).
    pub fn new(seed: u64) -> Arc<Self> {
        todo!()
    }

    /// A simulated filesystem injecting `plan`.
    pub fn with_faults(seed: u64, plan: FaultPlan) -> Arc<Self> {
        todo!()
    }

    /// The seed, for printing on failure.
    pub fn seed(&self) -> u64 {
        todo!()
    }

    /// Replaces the fault plan.
    pub fn set_faults(&self, plan: FaultPlan) {
        todo!()
    }

    /// Simulates a crash: every open handle starts failing with `Crashed`, and the stored
    /// state is reduced to what `kind` and the fault plan allow. Reopen through this same
    /// `SimVfs` to recover.
    pub fn crash(&self, kind: CrashKind) {
        todo!()
    }

    /// Advances both clocks.
    pub fn advance(&self, nanos: u64) {
        todo!()
    }

    /// Mutating operations performed so far (to size a crash-at-every-point sweep).
    pub fn mutating_ops(&self) -> u64 {
        todo!()
    }

    /// Marks a simulated process as dead for [`Vfs::process_alive`].
    pub fn kill_process(&self, process: ProcessId) {
        todo!()
    }

    /// Makes [`Vfs::current_process`] return `process` on the calling thread (to simulate
    /// several processes in one test).
    pub fn enter_process(&self, process: ProcessId) {
        todo!()
    }
}

impl Vfs for SimVfs {
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
