use std::sync::Arc;

use pigeonhole_io::Vfs;
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

/// A deterministic RNG (xoshiro256** seeded through splitmix64). Never seeded from the OS.
///
/// ```
/// use pigeonhole_sim::Rng;
///
/// let mut a = Rng::new(7);
/// let mut b = Rng::new(7);
/// assert_eq!(a.next_u64(), b.next_u64());
/// assert!(a.below(10) < 10);
/// ```
#[derive(Debug, Clone)]
pub struct Rng {
    s: [u64; 4],
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Rng {
    /// An RNG from a seed.
    pub fn new(seed: u64) -> Self {
        let mut sm = seed;
        Self {
            s: [
                splitmix64(&mut sm),
                splitmix64(&mut sm),
                splitmix64(&mut sm),
                splitmix64(&mut sm),
            ],
        }
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Uniform in `0..n` (`n > 0`).
    ///
    /// # Panics
    /// If `n == 0`.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "Rng::below(0)");
        // Lemire's nearly-divisionless method with rejection, so the result is unbiased.
        let mut m = u128::from(self.next_u64()) * u128::from(n);
        if (m as u64) < n {
            let threshold = n.wrapping_neg() % n;
            while (m as u64) < threshold {
                m = u128::from(self.next_u64()) * u128::from(n);
            }
        }
        (m >> 64) as u64
    }

    /// `true` with probability `ppm / 1_000_000`.
    pub fn chance(&mut self, ppm: u32) -> bool {
        self.below(1_000_000) < u64::from(ppm)
    }

    /// A uniform `f64` in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A child RNG with an independent stream (for per-task randomness).
    pub fn fork(&mut self) -> Self {
        Self::new(self.next_u64())
    }
}

struct Task {
    name: &'static str,
    rng: Rng,
    /// Runnable once simulated time reaches this many nanoseconds.
    wake_at: u64,
    run: Box<dyn FnMut(&mut Rng) -> Step>,
}

/// A seeded simulation: one thread, one RNG, simulated time, a simulated filesystem.
///
/// Tasks are closures stepped in an order chosen by the RNG, so interleavings vary by seed
/// and replay exactly. The runtime's application-owned mode lets shard loops run as tasks.
/// On failure, print [`Sim::seed`].
///
/// Time moves only when no task is runnable: the clock jumps to the earliest wakeup, through
/// [`SimVfs::advance`], so file-system clocks and the scheduler always agree.
///
/// ```
/// use std::{cell::Cell, rc::Rc};
/// use pigeonhole_sim::{Sim, Step};
///
/// let mut sim = Sim::new(1);
/// let hits = Rc::new(Cell::new(0));
/// let h = hits.clone();
/// sim.spawn("counter", Box::new(move |_rng| {
///     h.set(h.get() + 1);
///     if h.get() < 3 { Step::SleepUntil(h.get() * 1_000) } else { Step::Done }
/// }));
/// sim.run_until_idle();
/// assert_eq!(hits.get(), 3);
/// assert_eq!(sim.now_nanos(), 2_000);
/// ```
pub struct Sim {
    seed: u64,
    rng: Rng,
    vfs: Arc<SimVfs>,
    tasks: Vec<Task>,
    next_task: u64,
    /// Scratch list of runnable task indices, reused across steps.
    runnable: Vec<usize>,
}

impl std::fmt::Debug for Sim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sim")
            .field("seed", &self.seed)
            .field("now_nanos", &self.now_nanos())
            .field(
                "tasks",
                &self.tasks.iter().map(|t| t.name).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl Sim {
    /// A simulation with no faults.
    pub fn new(seed: u64) -> Self {
        Self::with_faults(seed, FaultPlan::none())
    }

    /// A simulation injecting `plan`.
    pub fn with_faults(seed: u64, plan: FaultPlan) -> Self {
        let mut rng = Rng::new(seed);
        // The VFS gets its own stream so scheduler decisions and fault decisions are independent.
        let vfs = SimVfs::with_faults(rng.next_u64(), plan);
        Self {
            seed,
            rng,
            vfs,
            tasks: Vec::new(),
            next_task: 0,
            runnable: Vec::new(),
        }
    }

    /// The seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The simulated filesystem (also usable as a `VfsRef`).
    pub fn vfs(&self) -> Arc<SimVfs> {
        self.vfs.clone()
    }

    /// The simulation's RNG.
    pub fn rng(&mut self) -> &mut Rng {
        &mut self.rng
    }

    /// Current simulated time in nanoseconds.
    pub fn now_nanos(&self) -> u64 {
        self.vfs.monotonic_nanos()
    }

    /// Adds a task. It gets its own RNG stream, forked from the simulation's, and is runnable
    /// immediately.
    pub fn spawn(&mut self, name: &'static str, task: Box<dyn FnMut(&mut Rng) -> Step>) -> TaskId {
        let id = TaskId(self.next_task);
        self.next_task += 1;
        let rng = self.rng.fork();
        self.tasks.push(Task {
            name,
            rng,
            wake_at: 0,
            run: task,
        });
        id
    }

    /// Steps one runnable task chosen by the RNG, advancing time to the next wakeup if none
    /// is runnable. Returns `false` when no tasks remain.
    pub fn step(&mut self) -> bool {
        if self.tasks.is_empty() {
            return false;
        }
        let now = self.now_nanos();
        self.runnable.clear();
        self.runnable.extend(
            self.tasks
                .iter()
                .enumerate()
                .filter(|(_, t)| t.wake_at <= now)
                .map(|(i, _)| i),
        );
        if self.runnable.is_empty() {
            let wake = self.tasks.iter().map(|t| t.wake_at).min().unwrap_or(now);
            self.vfs.advance(wake - now);
            return self.step();
        }
        let pick = self.runnable[self.rng.below(self.runnable.len() as u64) as usize];
        let task = &mut self.tasks[pick];
        match (task.run)(&mut task.rng) {
            Step::Ready => {}
            Step::SleepUntil(t) => task.wake_at = t,
            Step::Done => {
                self.tasks.remove(pick);
            }
        }
        true
    }

    /// Steps until no task is runnable and none is sleeping.
    pub fn run_until_idle(&mut self) {
        while self.step() {}
    }

    /// Steps until `cond` holds or `max_steps` pass; returns whether it held.
    pub fn run_until(&mut self, max_steps: u64, cond: &mut dyn FnMut() -> bool) -> bool {
        for _ in 0..max_steps {
            if cond() {
                return true;
            }
            if !self.step() {
                break;
            }
        }
        cond()
    }

    /// Crashes the simulated machine (or process) now; all tasks are dropped.
    pub fn crash(&mut self, kind: CrashKind) {
        self.vfs.crash(kind);
        self.tasks.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn rng_is_deterministic_and_forks_diverge() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        let xs: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        assert_eq!(xs, (0..8).map(|_| b.next_u64()).collect::<Vec<_>>());
        assert_ne!(Rng::new(43).next_u64(), xs[0]);
        let mut f = a.fork();
        assert_ne!(f.next_u64(), a.next_u64());
    }

    #[test]
    fn below_is_in_range_and_roughly_uniform() {
        let mut r = Rng::new(1);
        let mut buckets = [0u32; 7];
        for _ in 0..70_000 {
            buckets[r.below(7) as usize] += 1;
        }
        assert!(
            buckets.iter().all(|&c| (9_000..11_000).contains(&c)),
            "{buckets:?}"
        );
        assert_eq!(r.below(1), 0);
        let hits = (0..100_000).filter(|_| r.chance(250_000)).count();
        assert!((24_000..26_000).contains(&hits), "{hits}");
        assert!((0..1000).all(|_| (0.0..1.0).contains(&r.unit())));
    }

    fn trace(seed: u64) -> Vec<&'static str> {
        let mut sim = Sim::new(seed);
        let log = Rc::new(RefCell::new(Vec::new()));
        for name in ["a", "b", "c"] {
            let log = log.clone();
            let mut n = 0;
            sim.spawn(
                name,
                Box::new(move |_| {
                    log.borrow_mut().push(name);
                    n += 1;
                    if n == 4 { Step::Done } else { Step::Ready }
                }),
            );
        }
        sim.run_until_idle();
        assert!(!sim.step());
        Rc::try_unwrap(log).unwrap().into_inner()
    }

    #[test]
    fn interleaving_replays_per_seed_and_varies_across_seeds() {
        assert_eq!(trace(5), trace(5));
        assert_eq!(trace(5).len(), 12);
        assert!((0..20).any(|s| trace(s) != trace(5)));
    }

    #[test]
    fn sleeping_advances_vfs_clocks() {
        let mut sim = Sim::new(3);
        let vfs = sim.vfs();
        let base = vfs.now_micros();
        let woke = Rc::new(RefCell::new(Vec::new()));
        let w = woke.clone();
        let v = vfs.clone();
        let mut phase = 0;
        sim.spawn(
            "sleeper",
            Box::new(move |_| {
                w.borrow_mut().push(v.monotonic_nanos());
                phase += 1;
                match phase {
                    1 => Step::SleepUntil(5_000_000),
                    2 => Step::SleepUntil(7_000_000),
                    _ => Step::Done,
                }
            }),
        );
        sim.run_until_idle();
        assert_eq!(*woke.borrow(), [0, 5_000_000, 7_000_000]);
        assert_eq!(vfs.now_micros(), base + 7_000);
    }

    #[test]
    fn run_until_stops_on_condition_or_budget() {
        let mut sim = Sim::new(9);
        let n = Rc::new(RefCell::new(0u32));
        let c = n.clone();
        sim.spawn(
            "loop",
            Box::new(move |_| {
                *c.borrow_mut() += 1;
                Step::Ready
            }),
        );
        let c = n.clone();
        assert!(sim.run_until(100, &mut || *c.borrow() >= 10));
        assert_eq!(*n.borrow(), 10);
        assert!(!sim.run_until(5, &mut || false));
        assert_eq!(*n.borrow(), 15);
    }

    #[test]
    fn crash_drops_tasks_and_kills_handles() {
        use pigeonhole_io::{ErrorKind, OpenOptions};
        let mut sim = Sim::new(2);
        let vfs = sim.vfs();
        let file = vfs
            .open(
                std::path::Path::new("/d/f"),
                OpenOptions::read_write_create(),
            )
            .unwrap();
        sim.spawn("t", Box::new(|_| Step::Ready));
        sim.crash(CrashKind::Process);
        assert!(!sim.step());
        assert_eq!(file.len().unwrap_err().kind, ErrorKind::Crashed);
    }
}
