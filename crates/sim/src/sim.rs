use std::sync::Arc;

use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};

/// What a simulated task wants after one step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Run me again.
    Ready,
    /// Nothing to do until simulated time reaches this many nanoseconds.
    SleepUntil(u64),
    /// Finished; drop me.
    Done,
}

/// Handle to a spawned task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

/// A deterministic RNG (a small PCG/xoshiro). Never seeded from the OS.
#[derive(Debug, Clone)]
pub struct Rng {
    _priv: (),
}

impl Rng {
    /// An RNG from a seed.
    pub fn new(seed: u64) -> Self {
        todo!()
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        todo!()
    }

    /// Uniform in `0..n` (`n > 0`).
    pub fn below(&mut self, n: u64) -> u64 {
        todo!()
    }

    /// `true` with probability `ppm / 1_000_000`.
    pub fn chance(&mut self, ppm: u32) -> bool {
        todo!()
    }

    /// A child RNG with an independent stream (for per-task randomness).
    pub fn fork(&mut self) -> Self {
        todo!()
    }
}

/// A seeded simulation: one thread, one RNG, simulated time, a simulated filesystem.
///
/// Tasks are closures stepped in an order chosen by the RNG, so interleavings vary by seed
/// and replay exactly. The runtime's application-owned mode lets shard loops run as tasks.
/// On failure, print [`Sim::seed`].
pub struct Sim {
    _priv: (),
}

impl std::fmt::Debug for Sim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Sim {
    /// A simulation with no faults.
    pub fn new(seed: u64) -> Self {
        todo!()
    }

    /// A simulation injecting `plan`.
    pub fn with_faults(seed: u64, plan: FaultPlan) -> Self {
        todo!()
    }

    /// The seed.
    pub fn seed(&self) -> u64 {
        todo!()
    }

    /// The simulated filesystem (also usable as a `VfsRef`).
    pub fn vfs(&self) -> Arc<SimVfs> {
        todo!()
    }

    /// The simulation's RNG.
    pub fn rng(&mut self) -> &mut Rng {
        todo!()
    }

    /// Current simulated time in nanoseconds.
    pub fn now_nanos(&self) -> u64 {
        todo!()
    }

    /// Adds a task.
    pub fn spawn(&mut self, name: &'static str, task: Box<dyn FnMut(&mut Rng) -> Step>) -> TaskId {
        todo!()
    }

    /// Steps one runnable task chosen by the RNG, advancing time to the next wakeup if none
    /// is runnable. Returns `false` when no tasks remain.
    pub fn step(&mut self) -> bool {
        todo!()
    }

    /// Steps until no task is runnable and none is sleeping.
    pub fn run_until_idle(&mut self) {
        todo!()
    }

    /// Steps until `cond` holds or `max_steps` pass; returns whether it held.
    pub fn run_until(&mut self, max_steps: u64, cond: &mut dyn FnMut() -> bool) -> bool {
        todo!()
    }

    /// Crashes the simulated machine (or process) now; all tasks are dropped.
    pub fn crash(&mut self, kind: CrashKind) {
        todo!()
    }
}
