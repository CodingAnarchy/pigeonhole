use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::Durability;
use pigeonhole_engine::{
    CachePriority, CompactionStyle, Compression, EngineOptions, FamilyKind, FamilyOptions,
};
use pigeonhole_io::VfsRef;

use crate::MergeOperator;

/// Name of the built-in `i64` add operator, which counter families sum with.
pub(crate) const I64_ADD: &str = "pigeonhole.i64_add";
/// The built-in `i64` minimum, the operator of a min family (D294).
const I64_MIN: &str = "pigeonhole.i64_min";
/// The built-in `i64` maximum, the operator of a max family (D294).
const I64_MAX: &str = "pigeonhole.i64_max";
/// The built-in HyperLogLog union, the operator of an HLL family (D294).
const HLL: &str = "pigeonhole.hll";

/// `n` days, for TTLs: `Family::default().ttl(days(30))`. Saturates instead of overflowing.
pub fn days(n: u64) -> Duration {
    Duration::from_secs(n.saturating_mul(86_400))
}

/// Database options. Process-local: nothing here is stored in the file, so reopening with
/// different options changes them. Zero config is valid.
///
/// ```
/// use pigeonhole::{Durability, Options};
///
/// let options = Options::default()
///     .durability(Durability::Buffered)
///     .shards(2)
///     .memtable_budget(8 << 20)
///     .block_cache(64 << 20);
/// # let _ = options;
/// ```
#[derive(Debug, Clone)]
pub struct Options {
    durability: Durability,
    shards: usize,
    compaction_cores: Option<usize>,
    pin_threads: bool,
    memtable_budget: u64,
    block_cache: Option<usize>,
    row_cache: usize,
    row_cache_max_row: Option<usize>,
    counter_checkpoint_steps: Option<u64>,
    row_cache_get_fill: Option<bool>,
    nowait_reads: Option<bool>,
    row_cache_families: Vec<(String, String)>,
    shm_dir: Option<PathBuf>,
    reader_slots: u32,
    create_if_missing: bool,
    merge_operators: Vec<Arc<dyn MergeOperator>>,
    read_only_fallback: bool,
    allow_fuse: bool,
    io_backend: Option<IoBackend>,
    direct_io: Option<bool>,
    vfs: Option<VfsRef>,
    wal_segment_size: Option<u64>,
    tablet_changes: bool,
    tablet_packing: bool,
    tablet_balance: Option<(Duration, u64, u64)>,
    write_stall_timeout: Option<Duration>,
    commit_spin: Option<Duration>,
    shard_spin: Option<Duration>,
    shard_spin_cap: Option<Duration>,
    shard_threads: Option<usize>,
    adaptive_shard_threads: bool,
    background_workers: usize,
    cpu_profile: CpuProfile,
    auto_recover: Recovery,
}

/// Who the machine's (or container's) CPUs are for: how much of them the engine's threads
/// take by default (D289, ICR 0081). See [`Options::cpu_profile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum CpuProfile {
    /// The application runs in the same process and needs CPU of its own: the engine's
    /// driver and compaction threads take a share of the physical cores and leave the rest,
    /// and every SMT sibling, to the application. A shard thread that handled a commit
    /// polls for the next one before it sleeps for a window sized from its recent gaps
    /// between commits: up to 1 ms when they come steadily, and never more poll per gap
    /// than a 200 µs window would cost (D198 as amended again on 2026-10-30; #1176). The
    /// default.
    #[default]
    Embedded,
    /// The database has the machine or container to itself, and its clients reach it from
    /// elsewhere (another container, over IPC or the network): flush and compaction take
    /// half the physical cores instead of a quarter, and a shard thread that handled a
    /// commit always polls 1 ms for the next one before it sleeps. Under a moderate steady
    /// load that costs up to about two cores more than `Embedded` and trims the commit
    /// tail further (D198 as amended on 2026-10-30). Shard threads are the same as with `Embedded`:
    /// up to one per shard, sized to the load by default
    /// ([`adaptive_shard_threads`](Options::adaptive_shard_threads)).
    Server,
}

/// Which failures [`Options::auto_recover`] recovers from in place (D265, #620).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Recovery {
    /// Never: a failed instance stays failed until it is closed and reopened (the recovery
    /// guide's procedure). The default.
    #[default]
    Off = 0,
    /// From an I/O failure: a shard that stopped after a write or sync to its log failed
    /// (`FailureCause::Io`), or a failed manifest commit to the main file.
    Io = 1,
    /// From any failure, a shard's panic or an internal error included. A panic is a bug
    /// the same input may repeat; the budget (three recoveries in ten minutes) bounds the
    /// loop, and then the instance stays failed.
    Any = 2,
}

/// The engine-owned threads the defaults give (D289, ICR 0081), from the physical cores the
/// process may run on (after its affinity mask and cgroup CPU quota).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThreadBudget {
    /// Shards: one per physical core (D220). Data partitions, not threads.
    pub(crate) shards: usize,
    /// The most driver threads the shards run on: the load controller's ceiling (D289 PR 2),
    /// or the fixed count `Options::shard_threads` set (`fixed`).
    pub(crate) drivers: usize,
    /// Whether `drivers` is fixed (`Options::shard_threads`), turning the controller off.
    pub(crate) fixed: bool,
    /// Flush and compaction threads (`Options::compaction_cores`).
    pub(crate) compaction: usize,
}

/// The flush and compaction threads' share of the physical cores, as a fraction (numerator,
/// denominator), per profile: the profiles differ mainly here (owner, 2026-10-27; #760
/// comment 6446722456). Set by D289 PR 2's compaction-share arms (c = 2, 4, 8 at 16 cores).
fn compaction_share(profile: CpuProfile) -> (usize, usize) {
    match profile {
        CpuProfile::Embedded => (1, 4),
        CpuProfile::Server => (1, 2),
    }
}

/// How long a shard thread that handled a commit polls for the next one before it sleeps,
/// per profile (D198, amended 2026-10-30; #760). `Embedded` keeps the engine's 200 µs: a
/// longer window costs up to about two cores under a moderate steady load, which an
/// application sharing the CPU (a laptop, a CPU-limited container) should not pay. `Server`
/// polls 1 ms, which took sparse-wide's `Buffered` put p99 from 49.8 to 27.4 µs on the
/// reference machine.
fn shard_spin_default(profile: CpuProfile) -> Option<Duration> {
    match profile {
        CpuProfile::Embedded => None,
        CpuProfile::Server => Some(Duration::from_millis(1)),
    }
}

/// The cap of `Embedded`'s adaptive window (D198's third amendment, #1176): each shard thread polls up to 1 ms when
/// its recent gaps between commits make that cost no more poll per gap than the fixed 200
/// µs window could. On the reference machine that took sparse-wide's default-durability put
/// p99 from 390 to 122 µs and its `Buffered` put p99 to `Server`'s, at `Embedded`'s CPU under
/// a paced 1K and 4K commits/s, and 0.3 cores more at 20K. `Server` keeps its fixed 1 ms.
fn shard_spin_cap_default(profile: CpuProfile) -> Option<Duration> {
    match profile {
        CpuProfile::Embedded => Some(Duration::from_millis(1)),
        CpuProfile::Server => None,
    }
}

/// D289's CPU budget for `cores` physical cores under `profile`.
/// - **Drivers:** the load controller's ceiling is one per physical core, so a store under full
///   load keeps a thread per shard's throughput and a light one shrinks to a few (owner,
///   2026-10-27). An explicit `drivers` fixes the count instead.
/// - **Compaction:** the profile's share of the cores, scaling with them (D252's fixed cap of
///   four is gone), at most one per shard: a shard runs one compaction at a time.
///
/// Explicit settings win and are kept as given; derived counts are at least 1.
pub(crate) fn thread_budget(
    cores: usize,
    profile: CpuProfile,
    shards: Option<usize>,
    drivers: Option<usize>,
    compaction: Option<usize>,
) -> ThreadBudget {
    let cores = cores.max(1);
    let shards = shards.unwrap_or(cores).max(1);
    let (num, den) = compaction_share(profile);
    ThreadBudget {
        shards,
        drivers: drivers.unwrap_or(shards),
        fixed: drivers.is_some(),
        compaction: compaction.unwrap_or_else(|| (cores * num / den).clamp(1, shards)),
    }
}

/// The row cache's default capacity (D216): a quarter of the default block cache. On ycsb-c
/// it answers about three gets in four from the cache; more memory buys few more hits.
const DEFAULT_ROW_CACHE: usize = 64 << 20;

/// Reader processes that can attach at once by default: 128 slots of 64 bytes, inside the
/// padding before the 2 MiB-aligned arenas, so they cost no space.
const DEFAULT_READER_SLOTS: u32 = 128;

/// The most reader slots: 4 MiB of slots in the shared-memory file, which the writer scans
/// for dead readers every 250 ms.
const MAX_READER_SLOTS: u32 = 65_536;

impl Default for Options {
    fn default() -> Self {
        Self {
            durability: Durability::GroupSync,
            shards: 0,
            compaction_cores: None,
            pin_threads: true,
            memtable_budget: 64 << 20,
            block_cache: None,
            row_cache: DEFAULT_ROW_CACHE,
            row_cache_max_row: None,
            counter_checkpoint_steps: None,
            row_cache_get_fill: None,
            nowait_reads: None,
            row_cache_families: Vec::new(),
            shm_dir: None,
            reader_slots: DEFAULT_READER_SLOTS,
            create_if_missing: true,
            merge_operators: Vec::new(),
            read_only_fallback: false,
            allow_fuse: false,
            io_backend: None,
            direct_io: None,
            vfs: None,
            wal_segment_size: None,
            tablet_changes: true,
            tablet_packing: true,
            tablet_balance: None,
            write_stall_timeout: None,
            commit_spin: None,
            shard_spin: None,
            shard_spin_cap: None,
            shard_threads: None,
            adaptive_shard_threads: true,
            background_workers: 0,
            cpu_profile: CpuProfile::Embedded,
            auto_recover: Recovery::Off,
        }
    }
}

impl Options {
    /// Writer default durability (default [`Durability::GroupSync`]).
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Number of shards, the data partitions each row belongs to (default: one per physical
    /// core the process may run on, D220; SMT siblings count once, and a cgroup CPU quota
    /// still caps it). Engine-owned shards run on up to a thread each, as few as the load
    /// needs ([`adaptive_shard_threads`](Options::adaptive_shard_threads), on by default),
    /// unless [`shard_threads`](Options::shard_threads) fixes the count (D289).
    /// `1` is a valid configuration.
    pub fn shards(mut self, n: usize) -> Self {
        self.shards = n;
        self
    }

    /// Dedicate `k` threads to flush and compaction (pinned when
    /// [`pin_threads`](Options::pin_threads) is on); `0` runs them on the shard threads, in
    /// time slices between commits.
    ///
    /// - **Default (D289, ICR 0081):** engine-owned mode starts a quarter of the physical
    ///   cores (half with [`CpuProfile::Server`]), at least one and at most one per shard
    ///   (16 cores: 4, or 8 with `Server`). On a shard thread a flush or
    ///   compaction slice holds up every commit queued behind it for up to half a
    ///   millisecond, which is most of the write tail on contended workloads.
    /// - **Pinning:** they take the CPUs after the shards' (the shards' SMT siblings first).
    ///   Where there are too few free CPUs for both, the shards are pinned and these run
    ///   unpinned.
    /// - **Application-owned mode** starts no threads, so its default is `0`, and
    ///   [`Pigeonhole::open_application_owned`](crate::Pigeonhole::open_application_owned)
    ///   fails with [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument) when
    ///   `k > 0` (decision D40); flush and compaction then run on the shards you drive, or
    ///   on background workers ([`Options::background_workers`], D284).
    pub fn compaction_cores(mut self, k: usize) -> Self {
        self.compaction_cores = Some(k);
        self
    }

    /// Pin each shard thread, and each [`compaction_cores`](Options::compaction_cores)
    /// thread, to a CPU of its own (default on, D235). Engine-owned mode only:
    /// [`Pigeonhole::open_application_owned`](crate::Pigeonhole::open_application_owned)
    /// runs shards on your threads, which you place yourself.
    ///
    /// - **Where:** the CPUs come from the set the opening thread may run on (its affinity
    ///   mask: `taskset`, a cpuset, a container's CPUs), one per physical core first, then
    ///   the cores' SMT siblings (D220). Threads are never pinned outside that set.
    /// - **When it doesn't pin:** threads run unpinned, and the open still succeeds, where
    ///   the platform has no hard thread affinity (macOS: affinity tags are only hints), a
    ///   CPU quota (a cgroup `cpu.max`, not a cpuset) is narrower than the affinity set,
    ///   there are fewer free CPUs in the set than threads (never two pinned threads on one
    ///   CPU; with room for the shards only, just the compaction threads run unpinned), or
    ///   the OS refuses a pin. [`Pigeonhole::engine_metrics`](crate::Pigeonhole::engine_metrics)
    ///   reports how many threads are pinned (`threads_started`, `threads_pinned`) and why
    ///   not (`unpinned_reason`).
    /// - **Several databases in one process** share the set: each pins to CPUs the others
    ///   don't hold, and one that finds too few free runs unpinned.
    /// - **Several processes on one host** don't see each other's pins and would pin onto
    ///   the same CPUs. Give each process its own CPUs (start it under an affinity mask or
    ///   in its own cpuset), or turn this off with `pin_threads(false)`. There is no option
    ///   to name cores: the affinity mask chooses them.
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// // Several processes sharing one host's CPUs: leave placement to the OS.
    /// let options = Options::default().pin_threads(false);
    /// # let _ = options;
    /// ```
    pub fn pin_threads(mut self, yes: bool) -> Self {
        self.pin_threads = yes;
        self
    }

    /// Memtable arena per shard, in bytes (default 64 MiB): the in-memory write buffer, also
    /// the size of the shared-memory arena. Data beyond it is flushed into the file, so it
    /// does not bound the database size; it bounds the largest value and batch. A batch
    /// whose cells need more than about half of it fails with
    /// [`ErrorCode::BatchTooLarge`](crate::ErrorCode::BatchTooLarge).
    ///
    /// The shared-memory region holds every shard's arena (each rounded up to 2 MiB) plus
    /// about 12 MiB of views, reader slots and row epochs, 268 MiB for the default budget on 4 CPUs: on
    /// Linux, memory in `/dev/shm` (or a file
    /// in [`shm_dir`](Options::shm_dir)), used only as the memtables fill. Opening checks
    /// that the filesystem has that much free and fails with
    /// [`ErrorCode::ShmUnavailable`](crate::ErrorCode::ShmUnavailable) if it does not.
    pub fn memtable_budget(mut self, bytes: u64) -> Self {
        self.memtable_budget = bytes;
        self
    }

    /// How long a write, `flush` or `compact` may wait before it fails with
    /// [`ErrorCode::Busy`](crate::ErrorCode::Busy) (default 30 s). Two waits count:
    /// - **Memtable room.** A commit that finds its shard's arena full, or a `flush` or
    ///   `compact` with no room for fresh memtables, waits while flushes free room
    ///   (snapshots or a slow disk can hold it). Each call's wait counts from when it began
    ///   waiting, however long the shard has been short of room for the calls ahead of it
    ///   (D240). The start is kept to the millisecond, so `Busy` can come up to 1 ms before
    ///   the timeout as the caller measures it.
    /// - **A tablet change.** A commit whose rows are being split, merged or moved between
    ///   shards waits for the change, and a cross-shard commit that a participant refuses
    ///   because of one is retried, as often as needed (D241). So is a cross-shard commit a
    ///   participant refuses because it overlaps another commit's undecided write (D249).
    ///   These count from when the commit was submitted.
    ///
    /// An L0 write stall (compaction falling behind) slows commits down but never fails
    /// them. A commit that fails with `Busy` was not committed: back off and retry it.
    /// Latency-sensitive applications can set a shorter timeout and retry later; bulk loaders
    /// can wait longer. `Duration::ZERO` refuses as soon as a wait would begin. On a clock
    /// that does not move (a simulator's), a wait nothing can end is refused at once whatever
    /// this says.
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::Options;
    ///
    /// let options = Options::default().write_stall_timeout(Duration::from_millis(500));
    /// # let _ = options;
    /// ```
    pub fn write_stall_timeout(mut self, timeout: Duration) -> Self {
        self.write_stall_timeout = Some(timeout);
        self
    }

    /// Recovers a failed instance in place (D265, #620): with [`Recovery::Io`] or
    /// [`Recovery::Any`], a supervisor thread waits for a failure and, on one the setting
    /// covers, closes the failed engine keeping its writer lock, reopens the same path on it
    /// and swaps the new engine in behind every handle. Tables keep working; a snapshot, a
    /// scan or a transaction from before fails with `SnapshotExpired`, and an operation that
    /// arrives during the pause waits up to [`write_stall_timeout`](Self::write_stall_timeout)
    /// for the swap (`Busy` past it: not committed, retry); a commit that was in flight when
    /// the shard failed comes back as `Busy` too. At most three recoveries in ten minutes,
    /// with a backoff of 0, 1 and 4 seconds before the reopen; past the budget, on a reopen
    /// that fails with `Corruption`, `UnsupportedFormat`, `NoSpace`, `WriterLocked` or
    /// `ShmVersionMismatch`, or when the failed engine's close has not returned within four
    /// write-stall timeouts (a device that never answers), the instance stays failed as with
    /// [`Recovery::Off`].
    /// [`Pigeonhole::recovery_stats`](crate::Pigeonhole::recovery_stats) reports it. Refused
    /// by [`Pigeonhole::open_application_owned`](crate::Pigeonhole::open_application_owned).
    /// See the user guide's "Recovering from a failed shard".
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, Recovery};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(
    ///     dir.join("app.phdb"),
    ///     Options::default()
    ///         .shards(1)
    ///         .auto_recover(Recovery::Io)
    ///         .write_stall_timeout(Duration::from_secs(5)),
    /// )?;
    /// let t = db.table("users")?.family("profile", Family::default()).create_if_missing()?;
    /// // A commit during a recovery waits for the swap; past the timeout, or if it was in
    /// // flight at the failure, it comes back as `Busy` and is simply retried.
    /// let mut tries = 0;
    /// loop {
    ///     match t.mutate(b"u1").put("profile", b"name", b"Ada").commit() {
    ///         Ok(_) => break,
    ///         Err(e) if e.code() == ErrorCode::Busy && tries < 3 => tries += 1,
    ///         Err(e) => return Err(e),
    ///     }
    /// }
    /// let stats = db.recovery_stats();
    /// assert_eq!((stats.generation, stats.recoveries, stats.gave_up), (1, 0, false));
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn auto_recover(mut self, recovery: Recovery) -> Self {
        self.auto_recover = recovery;
        self
    }

    /// A test and bench hook (ICR 0085), only with the `__unstable-hooks` feature and outside
    /// the API: the default suits applications.
    ///
    /// How long a thread waiting for its buffered (or non-durable) commit polls for the
    /// result before it sleeps (default 50 µs). A shard usually answers within a few
    /// microseconds, and putting the thread to sleep and waking it costs that again or more
    /// (D198). A cross-shard commit takes two rounds, so the default covers those too (D253). A durable commit waits for a disk sync and sleeps at once. A wait that keeps
    /// finding nothing polls less often. `Duration::ZERO` always sleeps at once: for
    /// battery-powered or CPU-constrained hosts.
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::Options;
    ///
    /// let options = Options::default().commit_spin(Duration::ZERO).shard_spin(Duration::ZERO);
    /// # let _ = options;
    /// ```
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn commit_spin(mut self, window: Duration) -> Self {
        self.commit_spin = Some(window);
        self
    }

    /// A test and bench hook (ICR 0085), only with the `__unstable-hooks` feature and outside
    /// the API: the default suits applications.
    ///
    /// How long a shard thread that just handled a commit keeps polling for the next one
    /// before it sleeps (default 200 µs, sized up to 1 ms from the shard's recent gaps; a fixed
    /// 1 ms with [`CpuProfile::Server`]; a window set here is fixed; D198 as amended
    /// on 2026-10-19 and 2026-10-30), so a steady stream
    /// of commits is taken without waking the thread each time, and a store whose clients
    /// wait on each other across shards does not settle into every message costing a wake. An idle database never polls, and polls that
    /// keep finding nothing poll less often. `Duration::ZERO` always sleeps at once. Ignored
    /// by application-owned shards, whose loop the application runs.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn shard_spin(mut self, window: Duration) -> Self {
        self.shard_spin = Some(window);
        self
    }

    /// A test and bench hook (#1176), only with the `__unstable-hooks` feature and outside
    /// the API.
    ///
    /// Sizes each shard thread's poll from its recent idle gaps, up to `cap`: the longest
    /// poll whose expected cost per gap is no more than the [`shard_spin`](Self::shard_spin)
    /// window's could be. A cap at or below that window keeps the window fixed, as with no
    /// cap (the default under [`CpuProfile::Server`] or with an explicit window).
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn shard_spin_cap(mut self, cap: Duration) -> Self {
        self.shard_spin_cap = Some(cap);
        self
    }

    /// Run the shards on `k` threads rather than one each (D289, ICR 0079): shard `i` runs
    /// on thread `i % k`, and each thread takes its shards' turns one after another.
    ///
    /// - **Why:** a shard thread that waits for work sleeps, and a commit that finds it
    ///   asleep waits for the operating system to wake and schedule it, up to a scheduler
    ///   slice (about 1 ms on Linux) when the application's threads keep the CPUs busy.
    ///   Fewer threads, each busier, sleep less.
    /// - **Default `0`:** up to one thread per shard, sized to the load
    ///   ([`adaptive_shard_threads`](Options::adaptive_shard_threads), on by default). A
    ///   set `k` fixes the count and turns the sizing off; `k` at or above the shard count
    ///   means a thread per shard.
    /// - **Pinning:** the `k` threads are pinned as the shard threads are
    ///   ([`pin_threads`](Options::pin_threads)).
    /// - Engine-owned mode only: application-owned shards run on threads you choose.
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// let options = Options::default().shards(16).shard_threads(4);
    /// # let _ = options;
    /// ```
    pub fn shard_threads(mut self, k: usize) -> Self {
        self.shard_threads = Some(k);
        self
    }

    /// Who this machine's (or container's) CPUs are for, which sets how many threads the
    /// engine starts by default (D289, ICR 0081). [`shard_threads`](Options::shard_threads)
    /// and [`compaction_cores`](Options::compaction_cores) set explicitly still win, and the
    /// profile fills in the rest.
    ///
    /// - **Shard threads, both profiles:** up to one per shard, so one per physical core at
    ///   most by default, and fewer while the load is light
    ///   ([`adaptive_shard_threads`](Options::adaptive_shard_threads)).
    /// - **[`CpuProfile::Embedded`] (the default):** your application runs in this process
    ///   and needs CPU too. Flush and compaction take a quarter of the physical cores, and a
    ///   shard thread polls for the next commit before it sleeps: up to 1 ms while commits
    ///   come steadily, at no more poll per gap than 200 µs would cost.
    /// - **[`CpuProfile::Server`]:** the database has the machine or container to itself,
    ///   and its clients reach it from elsewhere (another container, over IPC or the
    ///   network). Flush and compaction take half the physical cores, and a shard thread
    ///   always polls 1 ms for the next commit before it sleeps: a lower commit tail at a
    ///   moderate steady load, for up to about two more cores.
    /// - **CPU limits:** "physical cores" means those the process may use: its affinity
    ///   mask and a container's CPU quota both narrow them, so a 4-CPU container sizes the
    ///   engine for 4 whatever the host has.
    /// - Engine-owned mode only: application-owned shards and workers run on threads you
    ///   start yourself (D40).
    ///
    /// ```
    /// use pigeonhole::{CpuProfile, Options};
    ///
    /// // A database container the application talks to over the network.
    /// let options = Options::default().cpu_profile(CpuProfile::Server);
    /// # let _ = options;
    /// ```
    pub fn cpu_profile(mut self, profile: CpuProfile) -> Self {
        self.cpu_profile = profile;
        self
    }

    /// Size the shard threads to the load (D289's controller, ICR 0081). **On by default.**
    /// The engine starts a thread per shard, then moves the shards of a light load onto
    /// fewer threads, which sleep less, so a commit less often waits for a sleeping thread to
    /// be woken; under load it spreads them out again, up to one per shard.
    ///
    /// - **What it gains:** a light, many-shard load's commit p99 falls from hundreds of
    ///   microseconds to tens. In the 2026-10-28 gate, sparse-wide's put p99 at `Buffered`
    ///   was 66-107 µs with the controller against 545-909 µs without, at the same
    ///   throughput; transfer, ycsb-a and ycsb-f were level (D289, #1015).
    /// - **When to turn it off (`false`):** for a fixed thread per shard, which is the
    ///   behaviour before 1.0. An explicit [`shard_threads`](Options::shard_threads) also
    ///   turns it off: the count you set is kept.
    /// - Engine-owned mode only: application-owned shards run on threads you start (D40).
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// // A thread per shard, never resized.
    /// let options = Options::default().adaptive_shard_threads(false);
    /// # let _ = options;
    /// ```
    pub fn adaptive_shard_threads(mut self, on: bool) -> Self {
        self.adaptive_shard_threads = on;
        self
    }

    /// Application-owned mode only: `n` background workers for flush and compaction (D284),
    /// handed out by
    /// [`Pigeonhole::open_application_owned`](crate::Pigeonhole::open_application_owned)
    /// for you to drive from threads you choose, as you drive the shards.
    ///
    /// - **Why:** with none (the default `0`), flush and compaction run on the shards, in
    ///   slices of up to half a millisecond that hold up the commits queued behind them.
    /// - **How many:** a quarter of the shards is a good start. The engine-owned count
    ///   ([`compaction_cores`](Options::compaction_cores)) has its own default, which may
    ///   differ.
    /// - Engine-owned opens ignore it: they start
    ///   [`compaction_cores`](Options::compaction_cores) threads of their own.
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// let options = Options::default().shards(16).background_workers(4);
    /// # let _ = options;
    /// ```
    pub fn background_workers(mut self, n: usize) -> Self {
        self.background_workers = n;
        self
    }

    /// Block cache capacity in bytes.
    pub fn block_cache(mut self, bytes: usize) -> Self {
        self.block_cache = Some(bytes);
        self
    }

    /// Row cache capacity in bytes (default 64 MiB, D216; 0 turns it off), on top of the block
    /// cache, plus about 9% of it for per-row write watermarks allocated at open. The row
    /// cache keeps the newest version of each cell of small, hot family rows and serves
    /// latest row reads (newest version, no time range) and point gets from it, without
    /// merging the row's sources. A family row is stored the second time a read misses it,
    /// so rows read once are never stored. Results are identical to uncached reads. Point
    /// gets at a snapshot or in a transaction are served from it too (D290). A commit that
    /// only overwrites cached cells with puts moves the cached copy forward (commits of at
    /// most 16 rows and 64 mutations; the row's other families miss); any other write makes
    /// it miss at once (D201). Snapshot row reads, scans and multi-version reads bypass it. Reader processes have their own
    /// ([`ReaderOptions::row_cache`], D281).
    pub fn row_cache(mut self, bytes: usize) -> Self {
        self.row_cache = bytes;
        self
    }

    /// Largest family row the row cache stores, in encoded bytes (about the qualifiers and
    /// values plus 16 bytes a cell; default 4 KiB). Larger rows are read as usual.
    pub fn row_cache_max_row(mut self, bytes: usize) -> Self {
        self.row_cache_max_row = Some(bytes);
        self
    }

    /// A counter read that folds at least `steps` increments keeps its result as the
    /// counter's base, so its next reads stop there (D291; default 128, 0 turns it off). For
    /// tests and benchmarks.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn counter_checkpoint_steps(mut self, steps: u64) -> Self {
        self.counter_checkpoint_steps = Some(steps);
        self
    }

    /// Whether a point get that misses a row twice fills the row cache with the family row
    /// (D290; default off since the owner's 2026-10-29 amendment, #1059). Off, gets are
    /// served from cached rows, which row reads fill, but never fill them. For benchmarks
    /// comparing the two.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn row_cache_get_fill(mut self, on: bool) -> Self {
        self.row_cache_get_fill = Some(on);
        self
    }

    /// Whether a read the page cache holds is copied on the thread that submits it (D292,
    /// Linux; default on), set on the database's I/O backend at open (ICR 0083). A backend
    /// passed through [`Options::vfs`] is shared: every database on it sees the change. For
    /// benchmarks comparing the two.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn nowait_reads(mut self, on: bool) -> Self {
        self.nowait_reads = Some(on);
        self
    }

    /// Serve only these families from the row cache: call once per `(table, family)`.
    /// Without any call, every family is served. Process-local, not stored in the file.
    pub fn row_cache_family(mut self, table: &str, family: &str) -> Self {
        self.row_cache_families
            .push((table.to_owned(), family.to_owned()));
        self
    }

    /// Put the shared-memory file in `dir` (for example a tmpfs mount) instead of the
    /// memory-backed default.
    pub fn shm_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.shm_dir = Some(dir.into());
        self
    }

    /// How many reader processes ([`Pigeonhole::open_reader`](crate::Pigeonhole::open_reader))
    /// can attach at once: default 128, at least 1, at most 65,536 (ICR 0055).
    ///
    /// Each attached reader handle holds one 64-byte slot in the shared-memory file. One more
    /// fails `open_reader` with [`ErrorCode::NoReaderSlot`](crate::ErrorCode::NoReaderSlot)
    /// at once, without waiting. The writer frees the slots of reader processes that exited
    /// or were killed within about 250 ms. The count is fixed for a writer's lifetime: a
    /// writer opened with another count builds its shared-memory file with that many.
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// let options = Options::default().reader_slots(16);
    /// # let _ = options;
    /// ```
    pub fn reader_slots(mut self, n: u32) -> Self {
        self.reader_slots = n.clamp(1, MAX_READER_SLOTS);
        self
    }

    /// Create the file if missing (default true).
    pub fn create_if_missing(mut self, yes: bool) -> Self {
        self.create_if_missing = yes;
        self
    }

    /// Registers a merge operator, referenced by families through its name
    /// ([`Family::merge_operator`]). A family naming an operator that is not registered is
    /// refused at creation, and a database whose families name one opens only read-only, with
    /// [`read_only_fallback`](Self::read_only_fallback), and with
    /// [`ErrorCode::UnknownMergeOperator`](crate::ErrorCode::UnknownMergeOperator) otherwise.
    /// Registering an operator under the built-in name `pigeonhole.i64_add` replaces it.
    pub fn merge_operator(mut self, op: Arc<dyn MergeOperator>) -> Self {
        self.merge_operators.push(op);
        self
    }

    /// Open read-only instead of failing when this build cannot write the database (default
    /// off; ICR 0059). Two cases:
    /// - the file uses a feature a newer build added that older builds may read but not
    ///   write: otherwise [`ErrorCode::UnsupportedFeature`](crate::ErrorCode::UnsupportedFeature);
    /// - a family names a merge operator this process has not registered: otherwise
    ///   [`ErrorCode::UnknownMergeOperator`](crate::ErrorCode::UnknownMergeOperator), and reads
    ///   of that family's merged cells fail with it.
    ///
    /// A handle opened read-only refuses writes, schema changes, `flush`, `compact` and
    /// `shrink` with [`ErrorCode::ReadOnly`](crate::ErrorCode::ReadOnly), and writes nothing to
    /// the database's files: it does not upgrade an older file's format, flush, compact or
    /// mark a clean close. It replays the write-ahead log into memory, so an open whose log
    /// does not fit [`memtable_budget`](Self::memtable_budget) fails with `ReadOnly`; raise
    /// the budget. [`DatabaseInfo::read_only`](crate::DatabaseInfo::read_only) tells whether
    /// the fallback was taken. It still holds the writer lock while open. A file that opens
    /// writable is unaffected.
    pub fn read_only_fallback(mut self, yes: bool) -> Self {
        self.read_only_fallback = yes;
        self
    }

    /// Accept a database on a FUSE filesystem (default off; decision D173). Use it only for a
    /// **local** FUSE mount you trust, such as ntfs-3g or an encrypted home directory
    /// (gocryptfs). FUSE byte-range locks may be local to one host, so two hosts could
    /// both open the file as writers and corrupt it, and a sync may not reach stable
    /// storage, so a commit acknowledged as durable could be lost in a power failure. Never
    /// set it for sshfs, s3fs, gcsfuse or other network-backed FUSE mounts. Network and
    /// cluster filesystems (NFS, SMB, GPFS, ...) are refused with
    /// [`ErrorCode::NetworkFilesystem`](crate::ErrorCode::NetworkFilesystem) either way.
    pub fn allow_fuse(mut self, yes: bool) -> Self {
        self.allow_fuse = yes;
        self
    }

    /// The I/O backend (default [`IoBackend::Auto`]: io_uring where the kernel offers it,
    /// `pread` elsewhere, D218). With [`IoBackend::Uring`] the open fails with
    /// [`ErrorCode::Unsupported`](crate::ErrorCode::Unsupported) where io_uring is unavailable
    /// (not Linux, an old kernel, a container that blocks it); [`IoBackend::Pread`] forces the
    /// thread pool everywhere.
    ///
    /// ```
    /// use pigeonhole::{IoBackend, Options};
    ///
    /// let options = Options::default().io_backend(IoBackend::Auto);
    /// # let _ = options;
    /// ```
    pub fn io_backend(mut self, backend: IoBackend) -> Self {
        self.io_backend = Some(backend);
        self
    }

    /// Read and write SST and blob extents with direct I/O (#403): `O_DIRECT` on Linux,
    /// `F_NOCACHE` on macOS, `FILE_FLAG_NO_BUFFERING` on Windows, through a second handle on
    /// the file, so the block cache is their only cache. Superblocks, the manifest and the
    /// WAL stay buffered. A file system that refuses direct I/O keeps buffered I/O.
    ///
    /// Default off, which suits most databases: buffered reads use whatever memory is free
    /// as cache. Turn it on when the data is much larger than RAM and
    /// [`Options::block_cache`] holds at least about 1/64 of the data (its pinned SST indexes
    /// and filters plus the index partitions, D215): cold gets then cost one device read
    /// each without the page cache's copy. On the reference machine (64 GiB table, 1 GiB block
    /// cache) cold gets measured p99 205 µs direct against 264 µs buffered on one thread, and
    /// 293 against 447 µs on 16. With a block cache much smaller than the working set,
    /// direct I/O is far slower than buffered.
    pub fn direct_io(mut self, yes: bool) -> Self {
        self.direct_io = Some(yes);
        self
    }

    /// A test and bench hook (ICR 0085), only with the `__unstable-hooks` feature and outside
    /// the API: the default suits applications.
    ///
    /// Let a table's tablets split, merge and move between shards (default on), so the
    /// writes of one table spread over every shard. Off, each table is one tablet on one
    /// shard: writes to a single table use one shard thread whatever [`shards`](Self::shards)
    /// says, and only commits touching several tables run on several shards.
    ///
    /// On, each shard's balancer splits a tablet at 256 MiB of live data or under sustained
    /// write skew, moves tablets to colder shards and merges small cold neighbours. Reads are
    /// never blocked by a change. Known limits: tablet owners are not stored in the file, so
    /// a reopen places the tablets again; and two commits on one row that are in flight
    /// together may be applied in either order while that row's tablet moves (a commit
    /// submitted after an earlier one returned is always applied after it).
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// // Keep every table on one shard.
    /// let options = Options::default().shards(4).tablet_changes(false);
    /// # let _ = options;
    /// ```
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn tablet_changes(mut self, yes: bool) -> Self {
        self.tablet_changes = yes;
        self
    }

    /// A test and bench hook (ICR 0085), only with the `__unstable-hooks` feature and outside
    /// the API: the default suits applications.
    ///
    /// Pack a lightly written store's tablets onto few shards (default on; D217). While the
    /// shards' threads are mostly idle, the balancer keeps the tablets being written on as few
    /// shards as stay about half busy, so a commit seldom has to wake a sleeping shard
    /// thread (a single writer over 32 shards otherwise pays a wakeup per commit). It
    /// spreads them again within about a second once those shards pass 75% busy. A shard is
    /// busy while its thread works or while its group fsyncs are in flight (D248), so
    /// durable commits waiting on the disk keep enough shards active. Off,
    /// tablets spread over every shard whatever the load. Only read with
    /// [`tablet_changes`](Self::tablet_changes) on; shards the application drives
    /// ([`Pigeonhole::open_application_owned`](crate::Pigeonhole::open_application_owned))
    /// are never packed.
    ///
    /// ```
    /// use pigeonhole::Options;
    ///
    /// // Spread over every shard even under light writes.
    /// let options = Options::default().tablet_packing(false);
    /// # let _ = options;
    /// ```
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn tablet_packing(mut self, yes: bool) -> Self {
        self.tablet_packing = yes;
        self
    }

    /// Run on a custom filesystem implementation. Used by the deterministic simulation
    /// suites; applications never need it.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn vfs(mut self, vfs: pigeonhole_io::VfsRef) -> Self {
        self.vfs = Some(vfs);
        self
    }

    /// WAL segment size in bytes (default 64 MiB; a multiple of 32 KiB, decision D43). A test
    /// hook (ICR 0005): small segments keep simulated opens fast. It also lowers the largest
    /// value a commit may carry (decision D16). Applications never need it.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn wal_segment_size(mut self, bytes: u64) -> Self {
        self.wal_segment_size = Some(bytes);
        self
    }

    /// Balancer tuning, a test hook (ICR 0009): a pass every `interval`, write skew acted on
    /// once a shard writes `min_writes` rows in one, and size splits at `split_bytes`. The
    /// simulation suites use tiny values so tablets change during short runs. Only read with
    /// [`tablet_changes`](Self::tablet_changes) on. Applications never need it.
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn tablet_balance(mut self, interval: Duration, min_writes: u64, split_bytes: u64) -> Self {
        self.tablet_balance = Some((interval, min_writes, split_bytes));
        self
    }
}

/// Options for a read-only handle in another process.
///
/// ```
/// use pigeonhole::ReaderOptions;
///
/// let options = ReaderOptions::default().block_cache(32 << 20);
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReaderOptions {
    block_cache: Option<usize>,
    shm_dir: Option<PathBuf>,
    merge_operators: Vec<Arc<dyn MergeOperator>>,
    allow_fuse: bool,
    io_backend: Option<IoBackend>,
    direct_io: Option<bool>,
    vfs: Option<VfsRef>,
    row_cache: Option<usize>,
    row_cache_max_row: Option<usize>,
    row_cache_families: Vec<(String, String)>,
}

impl ReaderOptions {
    /// Row cache capacity in bytes (default 64 MiB, as the writer's; 0 turns it off; D281).
    /// Each reader process has its own, so N reader processes use up to N × 64 MiB by
    /// default, plus 16 bytes per row-epoch slot of the writer's table (4 MiB at the
    /// defaults). It serves latest row reads and point gets as
    /// [`Options::row_cache`] does in the writer, with results identical to uncached reads: a
    /// write the reader can see makes the row's cached copy miss at once, through the
    /// writer's write watermarks in shared memory. Emptied whenever the reader re-attaches to
    /// a new writer (a restart, a recovery).
    pub fn row_cache(mut self, bytes: usize) -> Self {
        self.row_cache = Some(bytes);
        self
    }

    /// Largest family row the reader's row cache stores, as
    /// [`Options::row_cache_max_row`] (default 4 KiB).
    pub fn row_cache_max_row(mut self, bytes: usize) -> Self {
        self.row_cache_max_row = Some(bytes);
        self
    }

    /// Serve only these families from the reader's row cache, as
    /// [`Options::row_cache_family`]: call once per `(table, family)`.
    pub fn row_cache_family(mut self, table: &str, family: &str) -> Self {
        self.row_cache_families
            .push((table.to_owned(), family.to_owned()));
        self
    }

    /// Block cache capacity in bytes (each reader process has its own).
    pub fn block_cache(mut self, bytes: usize) -> Self {
        self.block_cache = Some(bytes);
        self
    }

    /// Where the writer put the shared-memory file, if not the default.
    pub fn shm_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.shm_dir = Some(dir.into());
        self
    }

    /// Registers a merge operator.
    pub fn merge_operator(mut self, op: Arc<dyn MergeOperator>) -> Self {
        self.merge_operators.push(op);
        self
    }

    /// Accept a database on a FUSE filesystem, as [`Options::allow_fuse`] (the same risks
    /// apply; default off).
    pub fn allow_fuse(mut self, yes: bool) -> Self {
        self.allow_fuse = yes;
        self
    }

    /// The I/O backend, as [`Options::io_backend`] (default [`IoBackend::Auto`]).
    pub fn io_backend(mut self, backend: IoBackend) -> Self {
        self.io_backend = Some(backend);
        self
    }

    /// Direct I/O for SST and blob extents, as [`Options::direct_io`] (default off).
    pub fn direct_io(mut self, yes: bool) -> Self {
        self.direct_io = Some(yes);
        self
    }

    /// Custom filesystem (simulation).
    #[cfg(feature = "__unstable-hooks")]
    #[doc(hidden)]
    pub fn vfs(mut self, vfs: pigeonhole_io::VfsRef) -> Self {
        self.vfs = Some(vfs);
        self
    }
}

/// Block-cache priority of a family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[non_exhaustive]
pub enum Priority {
    /// Evicted first.
    Low,
    /// The default.
    #[default]
    Normal,
    /// Evicted last.
    High,
}

/// Compaction strategy of a family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Compaction {
    /// Lowest read and space amplification (the default).
    #[default]
    Leveled,
    /// Lowest write amplification: sorted runs merge when their sizes are similar or space
    /// amplification passes 200%.
    Tiered,
    /// Drop whole files, with no rewrite, once their newest timestamp expires; for TTL'd,
    /// time-ordered data. Without a TTL nothing expires (small files still merge).
    FifoByTime,
}

/// A column family's policy. Stored in the file with the family; a family's kind and
/// options are fixed when it is created.
///
/// The defaults: every version kept, no TTL, 10 bloom bits per key, LZ4 blocks of 16 KiB,
/// values over 4 KiB separated, no merge operator, normal cache priority, leveled
/// compaction.
///
/// Counters live in a **counter family**, declared with [`Family::counter`] (decision D179,
/// after Bigtable's aggregate families). `incr` on any other family fails with
/// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument).
///
/// ```
/// use pigeonhole::{days, Compression, Family, Priority};
///
/// let hot = Family::default().max_versions(1).cache_priority(Priority::High);
/// let expiring = Family::default().ttl(days(30)).compression(Compression::None);
/// assert_ne!(hot, expiring);
/// let hits = Family::counter();
/// let daily = Family::counter().ttl(days(90));
/// assert_ne!(hits, daily);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Family {
    options: FamilyOptions,
}

/// How an aggregate family combines the values written to a cell, fixed when the family is
/// created, as in Bigtable's aggregate families (decisions D179, D294, D298;
/// [`Family::aggregate`]).
///
/// Inputs are written with [`aggregate`](crate::RowMutation::aggregate) (`i64`),
/// [`aggregate_u64`](crate::RowMutation::aggregate_u64) (the bitwise kinds) or
/// [`aggregate_f64`](crate::RowMutation::aggregate_f64) (the `f64` kinds), and `_at` forms for
/// a bucket. They combine within one bucket: a column at one timestamp. A bucket nothing was
/// written to (or whose writes were all deleted) reads as absent, never as 0.
///
/// Non-exhaustive: later releases add aggregations, some carrying parameters.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Aggregation {
    /// The sum of the inputs, wrapping on overflow: a counter ([`Family::counter`]).
    Sum,
    /// The smallest input.
    Min,
    /// The largest input.
    Max,
    /// The distinct inputs, as a HyperLogLog sketch: read with
    /// [`CellRef::hll_estimate`](crate::CellRef::hll_estimate) (4096 registers, about 1.6%
    /// standard error). A bucket's sketch is up to 3 KiB; reads and the row cache carry it
    /// whole, so bound a wide row's buckets with a TTL or `max_versions`.
    Hll,
    /// The bitwise OR of `u64` inputs ([`aggregate_u64`](crate::RowMutation::aggregate_u64);
    /// read with [`CellRef::as_u64`](crate::CellRef::as_u64)): a set of flags.
    BitOr,
    /// The bitwise AND of `u64` inputs.
    BitAnd,
    /// The bitwise XOR of `u64` inputs.
    BitXor,
    /// The smallest `f64` input ([`aggregate_f64`](crate::RowMutation::aggregate_f64); read
    /// with [`CellRef::as_f64`](crate::CellRef::as_f64)), by `f64::total_cmp`: `-0.0` is
    /// below `+0.0`, infinities are allowed, NaN is refused.
    MinF64,
    /// The largest `f64` input, as [`Aggregation::MinF64`].
    MaxF64,
    /// The exact sum of finite `f64` inputs
    /// ([`aggregate_f64`](crate::RowMutation::aggregate_f64); read with
    /// [`CellRef::sum_f64`](crate::CellRef::sum_f64)), rounded once when read: the same
    /// inputs read the same sum in any order. A cell is a state of up to 274 bytes.
    SumF64,
    /// The count, mean and variance of finite `f64` inputs
    /// ([`aggregate_f64`](crate::RowMutation::aggregate_f64); read with
    /// [`CellRef::mean_var`](crate::CellRef::mean_var)), each computed exactly and rounded
    /// once. A cell is a state of up to 821 bytes.
    MeanVar,
    /// Counts of `f64` inputs (NaN refused) in fixed buckets between the bounds of
    /// [`HistogramParams`] ([`aggregate_f64`](crate::RowMutation::aggregate_f64); read with
    /// [`CellRef::histogram_counts`](crate::CellRef::histogram_counts)).
    Histogram(HistogramParams),
    /// A log-linear quantile sketch of finite `f64` inputs, within a relative error of
    /// 2^-p ([`QuantileParams`]; [`aggregate_f64`](crate::RowMutation::aggregate_f64); read
    /// with [`CellRef::quantile`](crate::CellRef::quantile)). The same inputs give the same
    /// sketch in any order.
    Quantiles(QuantileParams),
}

/// A histogram family's bucket bounds ([`Aggregation::Histogram`], D298): `n` bounds
/// `b0 < b1 < … < b(n-1)` make `n + 1` buckets, `(-∞, b0)`, `[b0, b1)`, …, `[b(n-1), +∞)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HistogramParams {
    bounds: Vec<u64>,
}

impl HistogramParams {
    /// Bounds that are finite, strictly increasing, 1 to 1023 of them, and never `-0.0`;
    /// `InvalidArgument` otherwise.
    pub fn new(bounds: impl Into<Vec<f64>>) -> crate::Result<Self> {
        let bounds: Vec<f64> = bounds.into();
        pigeonhole_format::buckets::histogram_bounds(
            &pigeonhole_format::buckets::histogram_params(&bounds),
        )
        .map_err(|e| crate::Error::new(crate::ErrorCode::InvalidArgument, e.0.to_owned()))?;
        Ok(HistogramParams {
            bounds: bounds.iter().map(|b| b.to_bits()).collect(),
        })
    }

    /// The bounds.
    pub fn bounds(&self) -> Vec<f64> {
        self.bounds.iter().map(|&b| f64::from_bits(b)).collect()
    }

    /// The bucket an input `x` falls in: how many bounds lie at or below it (IEEE `<=`, so
    /// `-0.0` and `0.0` share a bucket). Counts from
    /// [`CellRef::histogram_counts`](crate::CellRef::histogram_counts) are indexed the same
    /// way: `n` bounds give `n + 1` counts.
    pub fn bucket_of(&self, x: f64) -> usize {
        pigeonhole_format::buckets::bucket_of(&self.bounds(), x)
    }
}

/// A quantile family's parameters ([`Aggregation::Quantiles`], D298): the mantissa bits
/// `p` an input keeps (1 to 12; a relative error of at most 2^-p; 7 by default) and the
/// buckets each sign's store keeps (16 to 4096; 2048 by default) before its smallest
/// magnitudes collapse into one bucket. Out-of-range values are refused when the family is
/// created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuantileParams {
    mantissa_bits: u8,
    max_buckets: u16,
}

impl Default for QuantileParams {
    fn default() -> Self {
        let d = pigeonhole_format::buckets::QuantileParams::default();
        QuantileParams {
            mantissa_bits: d.mantissa_bits,
            max_buckets: d.max_buckets,
        }
    }
}

impl QuantileParams {
    /// With `p` mantissa bits.
    pub fn mantissa_bits(mut self, p: u8) -> Self {
        self.mantissa_bits = p;
        self
    }

    /// With `n` buckets per store.
    pub fn max_buckets(mut self, n: u16) -> Self {
        self.max_buckets = n;
        self
    }

    /// The mantissa bits.
    pub fn get_mantissa_bits(&self) -> u8 {
        self.mantissa_bits
    }

    /// The buckets per store.
    pub fn get_max_buckets(&self) -> u16 {
        self.max_buckets
    }

    fn format(&self) -> pigeonhole_format::buckets::QuantileParams {
        pigeonhole_format::buckets::QuantileParams {
            mantissa_bits: self.mantissa_bits,
            max_buckets: self.max_buckets,
        }
    }
}

impl Aggregation {
    fn kind(&self) -> FamilyKind {
        match self {
            Aggregation::Sum => FamilyKind::Counter,
            Aggregation::Min => FamilyKind::Min,
            Aggregation::Max => FamilyKind::Max,
            Aggregation::Hll => FamilyKind::Hll,
            Aggregation::BitOr => FamilyKind::BitOr,
            Aggregation::BitAnd => FamilyKind::BitAnd,
            Aggregation::BitXor => FamilyKind::BitXor,
            Aggregation::MinF64 => FamilyKind::MinF64,
            Aggregation::MaxF64 => FamilyKind::MaxF64,
            Aggregation::SumF64 => FamilyKind::SumF64,
            Aggregation::MeanVar => FamilyKind::MeanVar,
            Aggregation::Histogram(_) => FamilyKind::Histogram,
            Aggregation::Quantiles(_) => FamilyKind::Quantiles,
        }
    }

    /// The family's `kind_params` (D298): a histogram's bounds, a quantile sketch's
    /// parameters; none for the other aggregations.
    fn kind_params(&self) -> Vec<u8> {
        match self {
            Aggregation::Histogram(h) => pigeonhole_format::buckets::histogram_params(&h.bounds()),
            Aggregation::Quantiles(q) => q.format().encode(),
            _ => Vec::new(),
        }
    }

    /// The aggregation of an engine family's options (`None` for a standard family), its
    /// parameters read back from `kind_params`.
    pub(crate) fn of_options(o: &pigeonhole_format::manifest::FamilyOptions) -> Option<Self> {
        use pigeonhole_format::buckets;
        match o.kind {
            FamilyKind::Histogram => buckets::histogram_bounds(&o.kind_params).ok().map(|b| {
                Aggregation::Histogram(HistogramParams {
                    bounds: b.iter().map(|x| x.to_bits()).collect(),
                })
            }),
            FamilyKind::Quantiles => buckets::QuantileParams::parse(&o.kind_params)
                .ok()
                .map(|q| {
                    Aggregation::Quantiles(QuantileParams {
                        mantissa_bits: q.mantissa_bits,
                        max_buckets: q.max_buckets,
                    })
                }),
            kind => Self::of_kind(kind),
        }
    }

    /// The aggregation of an engine family kind without parameters (`None` for a standard
    /// family).
    fn of_kind(kind: FamilyKind) -> Option<Self> {
        match kind {
            FamilyKind::Standard => None,
            FamilyKind::Counter => Some(Aggregation::Sum),
            FamilyKind::Min => Some(Aggregation::Min),
            FamilyKind::Max => Some(Aggregation::Max),
            FamilyKind::Hll => Some(Aggregation::Hll),
            FamilyKind::BitOr => Some(Aggregation::BitOr),
            FamilyKind::BitAnd => Some(Aggregation::BitAnd),
            FamilyKind::BitXor => Some(Aggregation::BitXor),
            FamilyKind::MinF64 => Some(Aggregation::MinF64),
            FamilyKind::MaxF64 => Some(Aggregation::MaxF64),
            FamilyKind::SumF64 => Some(Aggregation::SumF64),
            FamilyKind::MeanVar => Some(Aggregation::MeanVar),
            FamilyKind::Histogram | FamilyKind::Quantiles => None,
        }
    }

    fn operator(&self) -> &'static str {
        match self {
            Aggregation::Sum => I64_ADD,
            Aggregation::Min => I64_MIN,
            Aggregation::Max => I64_MAX,
            Aggregation::Hll => HLL,
            Aggregation::BitOr => "pigeonhole.u64_or",
            Aggregation::BitAnd => "pigeonhole.u64_and",
            Aggregation::BitXor => "pigeonhole.u64_xor",
            Aggregation::MinF64 => "pigeonhole.f64_min",
            Aggregation::MaxF64 => "pigeonhole.f64_max",
            Aggregation::SumF64 => "pigeonhole.f64_sum",
            Aggregation::MeanVar => "pigeonhole.f64_mean_var",
            Aggregation::Histogram(_) => "pigeonhole.f64_histogram",
            Aggregation::Quantiles(_) => "pigeonhole.f64_quantiles",
        }
    }
}

impl Family {
    /// An aggregate family: every cell combines the `i64` inputs written to it with
    /// `aggregation` (decision D294). It works as a [counter](Family::counter) does, which
    /// is the `Sum` case: [`aggregate`](crate::RowMutation::aggregate) writes an input to
    /// the column's cell at one fixed timestamp, [`aggregate_at`] to a bucket at a
    /// timestamp you choose, and inputs combine when read and when compacted.
    ///
    /// - [`put_i64`](crate::RowMutation::put_i64) sets the cell (`put_i64_at` a bucket):
    ///   later inputs combine with the value it set. Other puts, and
    ///   [`incr`](crate::RowMutation::incr) outside a `Sum` family, are refused with
    ///   `InvalidArgument`.
    /// - An `Hll` family counts distinct inputs: its cells are sketches, read with
    ///   [`CellRef::hll_estimate`](crate::CellRef::hll_estimate). `put_i64` is refused there;
    ///   `put` sets a bucket only to a well-formed sketch (as `phdb load` writes one back).
    /// - A delete removes what earlier commits wrote, so the next input starts the cell
    ///   again: after `delete_column`, a `Min` cell holds the smallest input written since.
    ///   A bucket with no input reads as absent.
    /// - TTL and `max_versions` work as for counters.
    ///
    /// A family of an aggregation added after 1.0 (`Min`, `Max`, `Hll`) marks the file: from its
    /// creation on, pigeonhole 1.0.x refuses to open the file (`UnsupportedFeature`), even
    /// after the family is dropped.
    ///
    /// ```
    /// use pigeonhole::{Aggregation, Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
    /// let t = db
    ///     .table("sensors")?
    ///     .family("low", Family::aggregate(Aggregation::Min))
    ///     .family("high", Family::aggregate(Aggregation::Max))
    ///     .create_if_missing()?;
    /// for reading in [21, 17, 25] {
    ///     t.mutate(b"s1")
    ///         .aggregate("low", b"temp", reading)
    ///         .aggregate("high", b"temp", reading)
    ///         .commit()?;
    /// }
    /// assert_eq!(t.get(b"s1", "low", b"temp")?.unwrap().as_i64(), Some(17));
    /// assert_eq!(t.get(b"s1", "high", b"temp")?.unwrap().as_i64(), Some(25));
    /// assert!(t.get(b"s2", "low", b"temp")?.is_none()); // no input: absent, not 0
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`aggregate_at`]: crate::RowMutation::aggregate_at
    pub fn aggregate(aggregation: Aggregation) -> Self {
        Self {
            options: FamilyOptions::default()
                .merge_operator(aggregation.operator().to_owned())
                .kind(aggregation.kind())
                .kind_params(aggregation.kind_params()),
        }
    }

    /// A counter family: every cell is an `i64` sum (decision D179).
    ///
    /// - [`incr`](crate::RowMutation::incr) adds to the column's counter, which lives at
    ///   one fixed timestamp (0), so a counter is one cell however often it is incremented:
    ///   its increments combine when read and when compacted.
    /// - [`incr_at`](crate::RowMutation::incr_at) adds to a *bucket*: the version at a
    ///   timestamp you choose (an hour or a day, say). Each bucket is its own version. The
    ///   TTL expires each bucket; `max_versions` limits what reads return, but compaction
    ///   keeps older buckets (a later delete of a newer one shows them again), so bound
    ///   storage with a TTL.
    /// - [`put_i64`](crate::RowMutation::put_i64) sets the counter (`put_i64_at` a bucket);
    ///   later increments add to it. Other puts are refused with `InvalidArgument`.
    /// - A delete removes what earlier commits wrote: an `incr` after `delete_column`
    ///   starts the counter again from 0. A write in the same commit as the delete is not
    ///   hidden (`incr` then `delete_column` in one mutation leaves the increment). A
    ///   delete without a timestamp takes the commit timestamp, so a bucket at a later
    ///   timestamp survives it.
    ///
    /// With a TTL the fixed timestamp would expire at once, so such a family takes only
    /// buckets (`incr_at`, `put_i64_at`); `incr` and `put_i64` fail with `InvalidArgument`.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
    /// let t = db.table("pages")?.family("hits", Family::counter()).create_if_missing()?;
    /// t.mutate(b"home").incr("hits", b"total", 2).commit()?;
    /// t.mutate(b"home").incr("hits", b"total", 3).commit()?;
    /// assert_eq!(t.get(b"home", "hits", b"total")?.unwrap().as_i64(), Some(5));
    ///
    /// // Hourly buckets: one version per hour.
    /// let hour = 3_600_000_000; // microseconds
    /// t.mutate(b"home").incr_at("hits", b"hourly", 7 * hour, 1).commit()?;
    /// t.mutate(b"home").incr_at("hits", b"hourly", 8 * hour, 4).commit()?;
    /// t.mutate(b"home").incr_at("hits", b"hourly", 8 * hour, 1).commit()?;
    /// let row = t.row(b"home").qualifier_prefix(b"hourly").versions(0).read()?.unwrap();
    /// let hourly: Vec<_> = row
    ///     .iter()
    ///     .map(|e| (e.cell.timestamp(), e.cell.as_i64().unwrap()))
    ///     .collect();
    /// assert_eq!(hourly, [(8 * hour, 5), (7 * hour, 1)]);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn counter() -> Self {
        Self::aggregate(Aggregation::Sum)
    }

    /// Keep at most `n` versions per column (0 keeps all).
    pub fn max_versions(mut self, n: u32) -> Self {
        self.options.max_versions = n;
        self
    }

    /// Cells older than `ttl` (by timestamp) expire.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.options.ttl_micros = u64::try_from(ttl.as_micros()).unwrap_or(u64::MAX);
        self
    }

    /// Bloom filter bits per key (0 disables filters; default 10).
    pub fn bloom_bits(mut self, bits: u8) -> Self {
        self.options.bloom_bits = bits;
        self
    }

    /// Store values longer than `bytes` in blob files (default 4096; `u32::MAX` never). A
    /// flush or compaction moves such a value out of the family's tree, which keeps a 16-byte
    /// pointer, so compactions and scans of other columns do not copy it. Blob GC rewrites
    /// blob files once they are half garbage.
    pub fn blob_threshold(mut self, bytes: u32) -> Self {
        self.options.blob_threshold = bytes;
        self
    }

    /// Block compression (ICR 0086; default [`Compression::Lz4`](crate::Compression::Lz4)):
    /// - [`Compression::Lz4`](crate::Compression::Lz4): fast, the default;
    /// - [`Compression::Zstd(level)`](crate::Compression::Zstd): smaller files than LZ4 for most
    ///   data, at a higher CPU cost to write and read, at a libzstd level (1 to 22, higher is
    ///   smaller and slower to write; negative levels are faster; 0 means 3);
    /// - [`Compression::None`](crate::Compression::None): stored as is (hot, small families).
    ///
    /// ```
    /// use pigeonhole::{Compression, Family};
    ///
    /// let archive = Family::default().compression(Compression::Zstd(9));
    /// # let _ = archive;
    /// ```
    pub fn compression(mut self, compression: crate::Compression) -> Self {
        match compression {
            crate::Compression::None => self.options.compression = Compression::None,
            crate::Compression::Lz4 => self.options.compression = Compression::Lz4,
            crate::Compression::Zstd(level) => {
                self.options.compression = Compression::Zstd;
                self.options.compression_level = level;
            }
        }
        self
    }

    /// Target uncompressed data-block size in bytes (default 16 KiB).
    pub fn block_size(mut self, bytes: u32) -> Self {
        self.options.block_size = bytes;
        self
    }

    /// Merge operator for this family, by registered name (see
    /// [`Options::merge_operator`]): [`RowMutation::merge`](crate::RowMutation::merge)
    /// writes its operands. Counters need none: declare a [`Family::counter`]. A counter
    /// family sums with the built-in `pigeonhole.i64_add` and refuses any other name at
    /// creation.
    ///
    /// A family that names `pigeonhole.i64_add` without being a counter family is how
    /// families created by 0.1.0 read: `incr` there writes at the commit timestamp and
    /// runs of increments fold across timestamps (decision D41). New counters belong in a
    /// counter family; see the migration note in the changelog.
    pub fn merge_operator(mut self, name: &str) -> Self {
        name.clone_into(&mut self.options.merge_operator);
        self
    }

    /// Block-cache priority.
    pub fn cache_priority(mut self, priority: Priority) -> Self {
        self.options.cache_priority = match priority {
            Priority::Low => CachePriority::Low,
            Priority::Normal => CachePriority::Normal,
            Priority::High => CachePriority::High,
        };
        self
    }

    /// Compaction strategy (default `Leveled`).
    pub fn compaction(mut self, strategy: Compaction) -> Self {
        self.options.compaction = match strategy {
            Compaction::Leveled => CompactionStyle::Leveled,
            Compaction::Tiered => CompactionStyle::Tiered,
            Compaction::FifoByTime => CompactionStyle::FifoByTime,
        };
        self
    }
}

impl Options {
    /// The engine configuration these options describe.
    pub(crate) fn to_engine(&self) -> Result<EngineOptions, pigeonhole_engine::Error> {
        self.to_engine_for(false)
    }

    /// [`Options::auto_recover`].
    pub(crate) fn recovery(&self) -> Recovery {
        self.auto_recover
    }

    /// [`Options::write_stall_timeout`], or the engine's default.
    pub(crate) fn stall_timeout(&self) -> Duration {
        self.write_stall_timeout
            .unwrap_or_else(|| Duration::from_secs(30))
    }

    /// The engine configuration these options describe, for an application-owned open when
    /// `application_owned` (an io_uring backend then starts no reaper thread, #408).
    pub(crate) fn to_engine_for(
        &self,
        application_owned: bool,
    ) -> Result<EngineOptions, pigeonhole_engine::Error> {
        let vfs = match &self.vfs {
            Some(v) => Arc::clone(v),
            None => backend_vfs(self.io_backend, application_owned)?,
        };
        if let Some(on) = self.nowait_reads {
            vfs.set_nowait_reads(on);
        }
        let direct_io = self.direct_io.unwrap_or_else(env_direct_io);
        let mut o = EngineOptions::new(vfs);
        o.direct_io = direct_io;
        o.create_if_missing = self.create_if_missing;
        o.shards = self.shards;
        // D289's budget, from the physical cores within the affinity mask and CPU quota.
        let budget = thread_budget(
            pigeonhole_io::sys::default_shards(),
            self.cpu_profile,
            (self.shards != 0).then_some(self.shards),
            self.shard_threads,
            self.compaction_cores,
        );
        o.compaction_threads = match self.compaction_cores {
            Some(k) => k,
            // Application-owned mode starts no threads (D40).
            None if application_owned => 0,
            None => budget.compaction,
        };
        o.pin_threads = self.pin_threads;
        o.shard_threads = budget.drivers;
        // The load controller (D289 PR 2), on by default since #1015's tuning (the owner,
        // 2026-10-28), never with a fixed count; application-owned shards run on the
        // application's threads (D40).
        o.adaptive_drivers = self.adaptive_shard_threads && !budget.fixed && !application_owned;
        if application_owned {
            o.background_workers = self.background_workers;
        }
        o.durability = self.durability.engine();
        o.memtable_budget = self.memtable_budget;
        o.memtable_freeze_bytes = self.memtable_budget / 4;
        if let Some(bytes) = self.block_cache {
            o.block_cache_bytes = bytes;
        }
        o.row_cache_bytes = self.row_cache;
        if let Some(bytes) = self.row_cache_max_row {
            o.row_cache_max_row = bytes;
        }
        if let Some(on) = self.row_cache_get_fill {
            o.row_cache_get_fill = on;
        }
        if let Some(steps) = self.counter_checkpoint_steps {
            o.counter_checkpoint_steps = steps;
        }
        o.row_cache_families = self.row_cache_families.clone();
        o.shm_dir.clone_from(&self.shm_dir);
        o.reader_slots = self.reader_slots;
        o.read_only_fallback = self.read_only_fallback;
        o.allow_fuse = self.allow_fuse;
        if let Some(bytes) = self.wal_segment_size {
            o.wal.segment_size = bytes;
        }
        o.tablet_changes = self.tablet_changes;
        o.balance_pack = self.tablet_packing;
        if let Some(window) = self.commit_spin {
            o.commit_spin_nanos = u64::try_from(window.as_nanos()).unwrap_or(u64::MAX);
        }
        // An explicit window wins over the profile's (D198, amended 2026-10-30).
        if let Some(window) = self.shard_spin.or(shard_spin_default(self.cpu_profile)) {
            o.shard_spin_nanos = u64::try_from(window.as_nanos()).unwrap_or(u64::MAX);
        }
        // An explicit window is fixed unless a cap is set with it; otherwise the profile's
        // cap applies (#1176).
        let cap = match (self.shard_spin_cap, self.shard_spin) {
            (Some(cap), _) => Some(cap),
            (None, Some(_)) => None,
            (None, None) => shard_spin_cap_default(self.cpu_profile),
        };
        if let Some(cap) = cap {
            o.shard_spin_cap_nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
        }
        if let Some(timeout) = self.write_stall_timeout {
            o.write_stall_timeout_nanos = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
        }
        if let Some((interval, min_writes, split_bytes)) = self.tablet_balance {
            o.balance_interval_nanos = u64::try_from(interval.as_nanos()).unwrap_or(u64::MAX);
            o.balance_min_writes = min_writes;
            o.tablet_split_bytes = split_bytes;
        }
        for op in &self.merge_operators {
            o.merge_operators
                .register(Arc::new(crate::merge::EngineMergeOperator(Arc::clone(op))));
        }
        Ok(o)
    }
}

impl ReaderOptions {
    /// The engine configuration these options describe.
    pub(crate) fn to_engine(&self) -> Result<EngineOptions, pigeonhole_engine::Error> {
        let vfs = match &self.vfs {
            Some(v) => Arc::clone(v),
            None => backend_vfs(self.io_backend, false)?,
        };
        let direct_io = self.direct_io.unwrap_or_else(env_direct_io);
        let mut o = EngineOptions::new(vfs);
        o.direct_io = direct_io;
        if let Some(bytes) = self.block_cache {
            o.block_cache_bytes = bytes;
        }
        o.shm_dir.clone_from(&self.shm_dir);
        o.allow_fuse = self.allow_fuse;
        o.row_cache_bytes = self.row_cache.unwrap_or(DEFAULT_ROW_CACHE);
        if let Some(bytes) = self.row_cache_max_row {
            o.row_cache_max_row = bytes;
        }
        o.row_cache_families.clone_from(&self.row_cache_families);
        for op in &self.merge_operators {
            o.merge_operators
                .register(Arc::new(crate::merge::EngineMergeOperator(Arc::clone(op))));
        }
        Ok(o)
    }
}

impl Family {
    /// The persisted options.
    pub(crate) fn to_engine(&self) -> FamilyOptions {
        self.options.clone()
    }

    /// The builder for options read back from the catalog (ICR 0047).
    pub(crate) fn from_engine(options: FamilyOptions) -> Self {
        Self { options }
    }
}

/// Which I/O backend a database runs its file I/O on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum IoBackend {
    /// A pool of threads serving submitted I/O with `pread` and `pwrite`, on every platform.
    Pread,
    /// io_uring (Linux): submitted I/O goes through a ring instead of a thread pool. Opening
    /// fails where it is unavailable.
    Uring,
    /// io_uring where it is available, `pread` otherwise (the default, D218).
    #[default]
    Auto,
}

/// A database's io_uring rings now ([`Pigeonhole::io_rings`](crate::Pigeonhole::io_rings)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct IoRings {
    /// Rings: the shared one, plus one per shard thread and compaction thread that got its
    /// own.
    pub rings: usize,
    /// Times a thread could not get a ring of its own (the kernel refused one, `ENOMEM` or a
    /// missing feature), counted per attempt; the thread retries later (#929, ICR 0070).
    pub attach_failures: u64,
    /// I/O completed synchronously on the calling thread instead of through a ring: in
    /// application-owned mode, a thread with no ring of its own runs its I/O inline, as the
    /// pread backend does (#929, ICR 0070, ICR 0087). Nonzero: some I/O blocked a thread
    /// your event loop drives.
    pub inline: u64,
}

/// The backend `backend` names; unset, [`IoBackend::Auto`] (D218), or with the
/// `__unstable-hooks` feature the one `PIGEONHOLE_IO` names (a test variable: `pread`, `uring`
/// or `auto`). For an application-owned open, an io_uring backend without a reaper thread.
fn backend_vfs(
    backend: Option<IoBackend>,
    application_owned: bool,
) -> Result<VfsRef, pigeonhole_engine::Error> {
    let backend = backend.unwrap_or_else(env_io_backend);
    let pread = || -> VfsRef { pigeonhole_io::pread::PreadVfs::new(0) };
    match backend {
        IoBackend::Pread => Ok(pread()),
        IoBackend::Uring => uring_vfs(application_owned).ok_or(
            pigeonhole_engine::Error::Unsupported("io_uring is unavailable on this system"),
        ),
        IoBackend::Auto => Ok(uring_vfs(application_owned).unwrap_or_else(pread)),
    }
}

/// The backend `PIGEONHOLE_IO` names (a test variable, read only with the
/// `__unstable-hooks` feature), else [`IoBackend::Auto`].
fn env_io_backend() -> IoBackend {
    #[cfg(feature = "__unstable-hooks")]
    match std::env::var("PIGEONHOLE_IO").as_deref() {
        Ok("pread") => return IoBackend::Pread,
        Ok("uring") => return IoBackend::Uring,
        _ => {}
    }
    IoBackend::Auto
}

/// `PIGEONHOLE_DIRECT=1` (a test variable, read only with the `__unstable-hooks` feature)
/// turns direct I/O on where the options leave it unset.
fn env_direct_io() -> bool {
    cfg!(feature = "__unstable-hooks") && std::env::var("PIGEONHOLE_DIRECT").as_deref() == Ok("1")
}

/// An io_uring backend, if the kernel offers one.
#[cfg(target_os = "linux")]
fn uring_vfs(application_owned: bool) -> Option<VfsRef> {
    let vfs = if application_owned {
        pigeonhole_io::uring::UringVfs::new_application_owned()
    } else {
        pigeonhole_io::uring::UringVfs::new()
    };
    vfs.ok().map(|v| v as VfsRef)
}

#[cfg(not(target_os = "linux"))]
fn uring_vfs(_application_owned: bool) -> Option<VfsRef> {
    None
}

#[cfg(test)]
mod tests {
    use super::{CpuProfile, ThreadBudget, thread_budget};

    fn b(shards: usize, drivers: usize, fixed: bool, compaction: usize) -> ThreadBudget {
        ThreadBudget {
            shards,
            drivers,
            fixed,
            compaction,
        }
    }

    /// D289's load controller is on by default (the owner, 2026-10-28, after #1015's box
    /// rounds), off with `adaptive_shard_threads(false)`, and never with a fixed
    /// `shard_threads` or in application-owned mode (D40).
    #[test]
    fn the_load_controller_is_on_by_default() {
        use super::Options;
        let o = || Options::default().vfs(pigeonhole_io::sim::SimVfs::new(1));
        let adaptive = |o: Options, app: bool| o.to_engine_for(app).unwrap().adaptive_drivers;
        assert!(adaptive(o(), false));
        assert!(!adaptive(o().adaptive_shard_threads(false), false));
        assert!(adaptive(o().adaptive_shard_threads(true), false));
        assert!(!adaptive(
            o().adaptive_shard_threads(true).shard_threads(4),
            false
        ));
        assert!(!adaptive(o(), true));
        // The engine-owned default starts a thread per shard: the controller's ceiling.
        let e = o().shards(20).to_engine_for(false).unwrap();
        assert_eq!(e.shard_threads, 20);
    }

    /// The shard's spin window by profile (D198, amended 2026-10-30; #1176): `Embedded`
    /// polls 200 µs adaptive up to 1 ms, `Server` a fixed 1 ms, and an explicit window wins
    /// over either.
    #[test]
    fn the_shard_spin_follows_the_profile() {
        use super::Options;
        let o = || Options::default().vfs(pigeonhole_io::sim::SimVfs::new(1));
        let spin = |o: Options| {
            let e = o.to_engine_for(false).unwrap();
            (e.shard_spin_nanos, e.shard_spin_cap_nanos)
        };
        // `Embedded`: 200 µs, adaptive up to 1 ms (#1176); `Server`: a fixed 1 ms.
        assert_eq!(spin(o()), (200_000, 1_000_000));
        assert_eq!(
            spin(o().cpu_profile(CpuProfile::Embedded)),
            (200_000, 1_000_000)
        );
        assert_eq!(spin(o().cpu_profile(CpuProfile::Server)), (1_000_000, 0));
        #[cfg(feature = "__unstable-hooks")]
        {
            use std::time::Duration;
            let set = Duration::from_micros(300);
            // An explicit window is fixed, under either profile, unless a cap comes with it.
            assert_eq!(
                spin(o().cpu_profile(CpuProfile::Server).shard_spin(set)),
                (300_000, 0)
            );
            assert_eq!(spin(o().shard_spin(set)), (300_000, 0));
            assert_eq!(spin(o().shard_spin(Duration::ZERO)), (0, 0));
            assert_eq!(
                spin(o().shard_spin(set).shard_spin_cap(Duration::from_millis(2))),
                (300_000, 2_000_000)
            );
            // A cap alone keeps the profile's window.
            assert_eq!(
                spin(o().shard_spin_cap(Duration::from_micros(200))),
                (200_000, 200_000)
            );
        }
    }

    /// D289's tables (the decision's entry and the guide): both profiles from 1 to 128
    /// physical cores, nothing set explicitly. The driver ceiling is one per core in both;
    /// the profiles differ in the compaction share.
    #[test]
    fn the_budget_by_profile_and_core_count() {
        let table = [
            // cores, embedded compaction, server compaction
            (1, 1, 1),
            (2, 1, 1),
            (4, 1, 2),
            (8, 2, 4),
            (16, 4, 8),
            (32, 8, 16),
            (64, 16, 32),
            (128, 32, 64),
        ];
        for (cores, embedded, server) in table {
            assert_eq!(
                thread_budget(cores, CpuProfile::Embedded, None, None, None),
                b(cores, cores, false, embedded),
                "embedded, {cores} cores"
            );
            assert_eq!(
                thread_budget(cores, CpuProfile::Server, None, None, None),
                b(cores, cores, false, server),
                "server, {cores} cores"
            );
        }
    }

    /// Explicit settings win and are kept as given; derived counts stay within one per shard
    /// and at least one.
    #[test]
    fn explicit_settings_win_and_the_budget_fills_the_rest() {
        let e = CpuProfile::Embedded;
        // A fixed driver count turns the controller off.
        assert_eq!(thread_budget(16, e, None, Some(2), None), b(16, 2, true, 4));
        assert_eq!(thread_budget(16, e, None, Some(0), None), b(16, 0, true, 4));
        // Compaction set (0: on the shards), kept as given, even past the shard count.
        assert_eq!(
            thread_budget(16, e, None, None, Some(0)),
            b(16, 16, false, 0)
        );
        assert_eq!(
            thread_budget(16, e, None, None, Some(20)),
            b(16, 16, false, 20)
        );
        // Fewer shards than cores: the ceiling and compaction stay within one per shard.
        assert_eq!(thread_budget(16, e, Some(2), None, None), b(2, 2, false, 2));
        // A 4-CPU container quota on a big host is 4 cores here (`default_shards`).
        assert_eq!(
            thread_budget(4, CpuProfile::Server, None, None, None),
            b(4, 4, false, 2)
        );
    }
}
