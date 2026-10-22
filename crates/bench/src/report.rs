//! Machine-readable results, the environment fingerprint, markdown summaries and the
//! run-to-run comparison used by the reproducibility gate.

use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;

use serde::{Deserialize, Serialize};

/// Version of the JSON layout written by [`Suite`].
pub const SUITE_FORMAT: u32 = 1;

/// Where a run happened. Every result carries one; only reference hardware gates a
/// phase, and none exists yet (decision D5), so every run is labeled non-reference.
///
/// ```
/// let env = pigeonhole_bench::Environment::detect(&std::env::temp_dir());
/// assert!(!env.reference);
/// assert!(env.summary().contains("non-reference"));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Environment {
    /// CPU model.
    pub cpu: String,
    /// Logical CPUs available to the process.
    pub cores: usize,
    /// Total memory in bytes, when known.
    pub memory_bytes: Option<u64>,
    /// Operating system and version.
    pub os: String,
    /// CPU architecture.
    pub arch: String,
    /// Filesystem type of the benchmark directory, when known.
    pub filesystem: String,
    /// Whether this is the reference hardware (enterprise NVMe with power-loss
    /// protection, Linux 6.x, io_uring). Set only by an operator through
    /// `PHDB_BENCH_REFERENCE=1` on Linux; never inferred.
    pub reference: bool,
    /// `"reference"` or `"non-reference (D5)"`.
    pub label: String,
    /// `release` or `debug`. Debug numbers are meaningless; the CLI warns.
    pub profile: String,
    /// Git revision of the code under test, when known.
    pub git_rev: Option<String>,
    /// Seconds since the Unix epoch when the suite started.
    pub started_unix: u64,
    /// One-minute load average when the suite started, when known. Other work on the
    /// machine skews every number; [`compare`] warns when this is high.
    #[serde(default)]
    pub load_average: Option<f64>,
}

fn cmd(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (!s.is_empty()).then_some(s)
}

fn cpu_model() -> String {
    if cfg!(target_os = "macos")
        && let Some(s) = cmd("sysctl", &["-n", "machdep.cpu.brand_string"])
    {
        return s;
    }
    if let Ok(info) = std::fs::read_to_string("/proc/cpuinfo") {
        for key in ["model name", "Model", "Hardware", "CPU part"] {
            if let Some(line) = info.lines().find(|l| l.starts_with(key))
                && let Some((_, v)) = line.split_once(':')
            {
                return v.trim().to_owned();
            }
        }
    }
    "unknown".to_owned()
}

fn memory_bytes() -> Option<u64> {
    if cfg!(target_os = "macos") {
        return cmd("sysctl", &["-n", "hw.memsize"])?.parse().ok();
    }
    let info = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = info.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

fn load_average() -> Option<f64> {
    let s = if cfg!(target_os = "macos") {
        // "{ 1.23 4.56 7.89 }"
        cmd("sysctl", &["-n", "vm.loadavg"])?
    } else {
        std::fs::read_to_string("/proc/loadavg").ok()?
    };
    s.split_whitespace()
        .find(|w| w.chars().next().is_some_and(|c| c.is_ascii_digit()))?
        .parse()
        .ok()
}

fn os_version() -> String {
    let os = std::env::consts::OS;
    let version = if cfg!(target_os = "macos") {
        cmd("sw_vers", &["-productVersion"])
    } else if cfg!(unix) {
        cmd("uname", &["-r"])
    } else {
        None
    };
    match version {
        Some(v) => format!("{os} {v}"),
        None => os.to_owned(),
    }
}

fn filesystem(dir: &Path) -> String {
    let d = dir.to_string_lossy();
    if cfg!(target_os = "linux") {
        if let Some(t) = cmd("stat", &["-f", "-c", "%T", &d]) {
            return t;
        }
    } else if cfg!(unix) {
        // `df -P` names the mount point; `mount` lists "... on <mp> (<type>, ...)".
        let mp = cmd("df", &["-P", &d]).and_then(|out| {
            let last = out.lines().last()?.to_owned();
            last.split_whitespace().nth(5).map(str::to_owned)
        });
        if let (Some(mp), Some(mounts)) = (mp, cmd("mount", &[])) {
            let needle = format!(" on {mp} (");
            if let Some(line) = mounts.lines().find(|l| l.contains(&needle)) {
                let rest = &line[line.find(&needle).unwrap_or(0) + needle.len()..];
                if let Some(t) = rest.split([',', ')']).next() {
                    return t.trim().to_owned();
                }
            }
        }
    }
    "unknown".to_owned()
}

impl Environment {
    /// Fingerprints this machine, with the filesystem that holds `dir`.
    pub fn detect(dir: &Path) -> Self {
        let reference = cfg!(target_os = "linux")
            && std::env::var("PHDB_BENCH_REFERENCE").is_ok_and(|v| v == "1");
        Self {
            cpu: cpu_model(),
            cores: std::thread::available_parallelism().map_or(1, |n| n.get()),
            memory_bytes: memory_bytes(),
            os: os_version(),
            arch: std::env::consts::ARCH.to_owned(),
            filesystem: filesystem(dir),
            reference,
            label: if reference {
                "reference"
            } else {
                "non-reference (D5)"
            }
            .to_owned(),
            profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            }
            .to_owned(),
            git_rev: cmd("git", &["rev-parse", "--short=12", "HEAD"]),
            started_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
            load_average: load_average(),
        }
    }

    /// Whether other work was running when the suite started: a one-minute load
    /// average of two or more. (A suite started right after another still shows that
    /// run's one busy core, so 1.0 would flag every back-to-back pair.)
    pub fn busy(&self) -> bool {
        self.load_average.is_some_and(|l| l >= 2.0)
    }

    /// One line: CPU, cores, memory, OS, filesystem and the reference label.
    pub fn summary(&self) -> String {
        let mem = self
            .memory_bytes
            .map(|b| format!(", {} GiB", b >> 30))
            .unwrap_or_default();
        let load = self
            .load_average
            .map(|l| format!(", load {l:.2}"))
            .unwrap_or_default();
        format!(
            "{} ({} cores{mem}), {} {}, {}{load} [{}]",
            self.cpu, self.cores, self.os, self.arch, self.filesystem, self.label
        )
    }

    /// Whether two runs come from the same machine type, so comparing them means
    /// something.
    pub fn same_machine(&self, other: &Environment) -> bool {
        self.cpu == other.cpu
            && self.cores == other.cores
            && self.os == other.os
            && self.arch == other.arch
            && self.filesystem == other.filesystem
            && self.profile == other.profile
    }
}

/// The full result of one workload against one store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    /// Store name.
    pub store: String,
    /// Store settings that affect results (shards, durability, ...).
    pub store_config: String,
    /// Workload name, as the CLI spells it.
    pub workload: String,
    /// Seed.
    pub seed: u64,
    /// Records loaded.
    pub records: u64,
    /// Operations measured.
    pub operations: u64,
    /// Value size in bytes.
    pub value_len: usize,
    /// Client threads used.
    pub threads: usize,
    /// Wall time of the load phase (not measured for latency).
    pub load_secs: f64,
    /// Wall time of the measured phase.
    pub run_secs: f64,
    /// Measured operations per second.
    pub throughput: f64,
    /// Median latency in nanoseconds.
    pub p50_ns: u64,
    /// 99th percentile in nanoseconds.
    pub p99_ns: u64,
    /// 99.9th percentile in nanoseconds.
    pub p999_ns: u64,
    /// Mean latency in nanoseconds.
    pub mean_ns: u64,
    /// Largest latency in nanoseconds.
    pub max_ns: u64,
    /// Unrecorded warmup operations run before the measured ones.
    #[serde(default)]
    pub warmup_ops: u64,
    /// What the run measured beyond latency: store size, `Busy` retries, cold/hot gets.
    #[serde(default)]
    pub detail: RunDetail,
}

impl RunRecord {
    /// The measured phase's commit rounds (D246), for a run with cross-shard commits on a
    /// store that counts them; `None` otherwise.
    ///
    /// The wasted rounds are the engine's refused two-phase rounds (ICR 0043,
    /// `Metrics::cross_shard`, in the stall counters). The sequence numbers used beyond one
    /// per commit (perf2's inference, which D246 used before the counters existed) are kept
    /// as a cross-check: each commit uses one sequence number and each refused round one more;
    /// with tablet changes on, a split, merge or move may use some as well.
    ///
    /// ```
    /// use pigeonhole_bench::{OpTypeStats, RunRecord, LatencyStats, Stalls, CROSS_SHARD_COMMIT};
    ///
    /// let mut r: RunRecord = serde_json::from_str(r#"{"store":"pigeonhole","store_config":"",
    ///     "workload":"cross-shard-contention","seed":1,"records":1,"operations":300,
    ///     "value_len":100,"threads":32,"load_secs":0,"run_secs":10,"throughput":30,
    ///     "p50_ns":0,"p99_ns":0,"p999_ns":0,"mean_ns":0,"max_ns":0}"#).unwrap();
    /// assert!(r.rounds().is_none(), "no cross-shard commits, no counters");
    /// let stats = |count| LatencyStats { count, ..LatencyStats::default() };
    /// r.detail.by_type = vec![
    ///     OpTypeStats { op: CROSS_SHARD_COMMIT.into(), stats: stats(100), mean_ns: 0 },
    ///     OpTypeStats { op: "put".into(), stats: stats(200), mean_ns: 0 },
    /// ];
    /// r.detail.seqnos_used = Some(311);
    /// let x = r.rounds().unwrap();
    /// assert_eq!((x.commits, x.cross, x.refused, x.inferred()), (300, 100, None, Some(11)));
    /// assert_eq!(x.wasted(), 11, "inferred without the engine's counters");
    /// r.detail.stalls = Some(Stalls { xshard_rounds: 110, xshard_refused_rounds: 10, ..Default::default() });
    /// let x = r.rounds().unwrap();
    /// assert_eq!((x.wasted(), x.per_cross()), (10, 0.1));
    /// ```
    pub fn rounds(&self) -> Option<Rounds> {
        let count = |name: &str| {
            self.detail
                .by_type
                .iter()
                .find(|t| t.op == name)
                .map_or(0, |t| t.stats.count)
        };
        let cross = count(crate::CROSS_SHARD_COMMIT);
        let refused = self.detail.stalls.as_ref().map(|s| s.xshard_refused_rounds);
        let seqnos = self.detail.seqnos_used;
        (cross > 0 && (refused.is_some() || seqnos.is_some())).then(|| Rounds {
            commits: cross + count("put"),
            cross,
            refused,
            seqnos,
        })
    }
}

/// A measured phase's commits and their two-phase rounds ([`RunRecord::rounds`], D246).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rounds {
    /// Commits measured, single-row and cross-shard.
    pub commits: u64,
    /// Cross-shard commits measured.
    pub cross: u64,
    /// Two-phase rounds a participant refused (ICR 0043), for stores that count them.
    pub refused: Option<u64>,
    /// Sequence numbers used while measuring, for stores that number their commits.
    pub seqnos: Option<u64>,
}

impl Rounds {
    /// Rounds that did not commit: the engine's refused rounds, else the inference from
    /// sequence numbers.
    pub fn wasted(&self) -> u64 {
        self.refused.or_else(|| self.inferred()).unwrap_or(0)
    }

    /// The sequence numbers used beyond one per commit: perf2's inference of the wasted
    /// rounds, a cross-check on [`refused`](Self::refused).
    pub fn inferred(&self) -> Option<u64> {
        Some(self.seqnos?.saturating_sub(self.commits))
    }

    /// [`wasted`](Self::wasted) per cross-shard commit; D246 judges it at most 0.1.
    pub fn per_cross(&self) -> f64 {
        self.wasted() as f64 / self.cross.max(1) as f64
    }
}

/// Percentiles of one class of operations, in nanoseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyStats {
    /// Operations in the class.
    pub count: u64,
    /// Median.
    pub p50_ns: u64,
    /// 99th percentile.
    pub p99_ns: u64,
    /// 99.9th percentile.
    pub p999_ns: u64,
}

impl LatencyStats {
    pub(crate) fn of(h: &crate::Histogram) -> Self {
        Self {
            count: h.count(),
            p50_ns: h.percentile(0.50).as_nanos() as u64,
            p99_ns: h.percentile(0.99).as_nanos() as u64,
            p999_ns: h.percentile(0.999).as_nanos() as u64,
        }
    }
}

/// Point gets of a run, split by whether the row had been read before in the run.
///
/// *Cold* is the first get of a row (the warmup counts as earlier), so the engine had no
/// reason to have its block cached; with a data set far larger than the cache it is the
/// closest a store-neutral bench gets to the spec's "one I/O per get". *Hot* is every
/// later get of the same row. The OS page cache can still serve a cold get; see
/// `docs/bench.md`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSplit {
    /// First touch of a row.
    pub cold: LatencyStats,
    /// Repeat touch.
    pub hot: LatencyStats,
}

/// Latency of one operation type in a run's measured phase.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpTypeStats {
    /// The type ([`BenchOp::type_name`](crate::BenchOp::type_name)).
    pub op: String,
    /// Percentiles.
    pub stats: LatencyStats,
    /// Mean latency in nanoseconds.
    pub mean_ns: u64,
}

/// Extra facts about a run.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunDetail {
    /// Bytes of the store directory when the run ended (0 when not measured).
    pub store_bytes: u64,
    /// Writes retried after `Busy` (a stall that outlasted the engine's timeout).
    pub busy_retries: u64,
    /// Cold and hot gets; `None` when the workload has no gets.
    pub reads: Option<ReadSplit>,
    /// Latency per operation type, by type name; empty in results written before it existed.
    #[serde(default)]
    pub by_type: Vec<OpTypeStats>,
    /// How the measured phase's work fell on each shard, indexed by shard; empty for
    /// stores without shards.
    #[serde(default)]
    pub shards: Vec<ShardShare>,
    /// What the measured phase stalled on (Pigeonhole only); `None` for other stores and in
    /// results written before it existed.
    #[serde(default)]
    pub stalls: Option<Stalls>,
    /// The measured phase's engine-side commit latency (Pigeonhole's inline driver only,
    /// D213); `None` elsewhere and in results written before it existed.
    #[serde(default)]
    pub engine_commit: Option<EngineCommit>,
    /// The settle step between the load and the warmup (D233); `None` in results written
    /// before it and for runs without it (the inline drivers).
    #[serde(default)]
    pub settle: Option<Settle>,
    /// Background work (flushes and compactions) completed during the measured phase, for
    /// stores that count it (Pigeonhole); `None` elsewhere and in results written before it.
    #[serde(default)]
    pub background_work: Option<u64>,
    /// Seconds of warmup between the settle step and the measured phase (D242): the warmup
    /// writes repeated on the measured phase's own client threads until the tablet placement
    /// went [`WARMUP_SETTLED`](crate::WARMUP_SETTLED) without a change; `None` in results
    /// written before it.
    #[serde(default)]
    pub warmup_secs: Option<f64>,
    /// Whether the placement settled within [`WARMUP_CAP`](crate::WARMUP_CAP) (D242); false
    /// when the warmup ran out first, so the measured phase may include tablet changes.
    #[serde(default)]
    pub warmup_settled: Option<bool>,
    /// Background work riding on the measured phase (D247: snapshot and durable-snapshot
    /// readers, a durable backup and a compaction), for runs that asked for it
    /// (`--riders`, Pigeonhole); `None` elsewhere.
    #[serde(default)]
    pub riders: Option<Riders>,
    /// Reader processes that ran beside the measured phase (`--reader-processes`,
    /// Pigeonhole, #683); `None` elsewhere and in results written before it.
    #[serde(default)]
    pub reader_processes: Option<ReaderProcesses>,
    /// Whether the measured phase stopped at its bound (`RunOptions::max_measured`, D247)
    /// before its operations were done; `None` for runs without a bound and in results
    /// written before it.
    #[serde(default)]
    pub capped: Option<bool>,
    /// Milliseconds the store took to close after the measured phase (D247: the flush and
    /// drain of a store that was just under load); `None` in results written before it.
    #[serde(default)]
    pub close_ms: Option<f64>,
    /// Transactions retried after a conflict during the measured phase, for stores whose
    /// transactions can conflict ([`Runner::txn_conflicts`](crate::Runner::txn_conflicts):
    /// Pigeonhole's optimistic ones); `None` elsewhere. Reported per transfer (D247).
    #[serde(default)]
    pub txn_conflicts: Option<u64>,
    /// Sequence numbers the store used during the measured phase, for stores that number
    /// their commits ([`Runner::seqno`](crate::Runner::seqno)); `None` elsewhere and in
    /// results written before it. With one per commit, the excess over the measured commits
    /// is commit rounds that did not commit: D246 judges it per cross-shard commit.
    #[serde(default)]
    pub seqnos_used: Option<u64>,
}

/// Background work that ran beside a measured phase (D247's riders, reported).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Riders {
    /// Scans of 10 rows under a snapshot re-taken every 100 ms, from a thread of their own.
    pub snapshot_scans: LatencyStats,
    /// The same under a durable snapshot.
    pub durable_snapshot_scans: LatencyStats,
    /// Snapshots and durable snapshots taken.
    pub snapshots: u64,
    /// See [`Riders::snapshots`].
    pub durable_snapshots: u64,
    /// Milliseconds a durable backup took, started 2 s into the measured phase and run to the
    /// end; `None` if the phase ended before it started.
    pub backup_ms: Option<f64>,
    /// Milliseconds a full compaction took, started right after the backup and run to the end
    /// (the riders wait for it); `None` when the backup never started.
    pub compact_ms: Option<f64>,
}

/// Reader processes beside a measured phase (#683, D247 item K, reported): separate
/// processes attached with `Pigeonhole::open_reader`, each doing ycsb-c reads (Zipfian rows,
/// a whole row of the `ycsb` family) for as long as the phase runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReaderProcesses {
    /// Reader processes that ran.
    pub processes: u32,
    /// Every process's row reads together.
    pub reads: LatencyStats,
    /// Reads per second over every process, from each process's own reading time.
    pub reads_per_sec: f64,
    /// Reads that found no row (0 on a fully loaded table).
    pub misses: u64,
    /// Reads that failed, every one counted (`SnapshotExpired` or `Busy` while the writer
    /// restarts would show here; none are expected in a steady run).
    pub errors: u64,
    /// The most published views the oldest reader snapshot lagged behind the writer's
    /// current view, sampled from the writer's `engine_metrics()` every 100 ms while the
    /// readers ran (`reader_pin_view_lag`, #842); 0 in results written before it.
    #[serde(default)]
    pub max_view_lag: u64,
    /// The same in commits (`reader_pin_seqno_lag`).
    #[serde(default)]
    pub max_seqno_lag: u64,
    /// The most reader slots the writer saw in use (`reader_slots_active`): the processes,
    /// once they attached.
    #[serde(default)]
    pub max_slots_active: u64,
}

/// The settle step after the load (D233): the store flushed its write buffer, then the run
/// waited until its background work stopped, so the measured phase starts from a settled
/// store instead of a timing-dependent share of the load's flushes and compactions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settle {
    /// Milliseconds from the flush to settled (or to the bound).
    pub millis: u64,
    /// False when the bound ran out first: the measured phase may include the load's tail.
    pub settled: bool,
    /// Background work (flushes and compactions) completed while settling, for stores that
    /// count it.
    pub work: Option<u64>,
}

/// A measured phase's commit latency inside the engine, from its own histograms (D213):
/// submission to resolution on the commit's shard, for the durability level most of the
/// phase's commits used. Beside the run's latency (submit to the future's resolution), it
/// splits a commit into the engine's part and the hand-back to the committer. Bucketed in
/// half powers of two, so each value is its bucket's lower bound.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineCommit {
    /// `Durability as usize`: 0 buffered, 1 sync, 2 group sync, 3 none.
    pub durability: u8,
    /// Commits counted.
    pub commits: u64,
    /// Median, nanoseconds.
    pub p50_ns: u64,
    /// 99th percentile, nanoseconds.
    pub p99_ns: u64,
    /// 99.9th percentile, nanoseconds.
    pub p999_ns: u64,
}

impl EngineCommit {
    /// The phase between two readings of Pigeonhole's `commit_latency_buckets`, with
    /// `floor` giving each bucket's lower bound; `None` when no commit completed between
    /// them.
    ///
    /// ```
    /// use pigeonhole_bench::EngineCommit;
    ///
    /// let before = [[0u64; 64]; 4];
    /// let mut after = [[0u64; 64]; 4];
    /// after[2][10] = 98; // 98 group-sync commits in bucket 10, 2 in bucket 20
    /// after[2][20] = 2;
    /// let e = EngineCommit::between(&before, &after, |b| b as u64 * 100).unwrap();
    /// assert_eq!((e.durability, e.commits, e.p50_ns, e.p99_ns), (2, 100, 1000, 2000));
    /// ```
    pub fn between(
        before: &[[u64; 64]; 4],
        after: &[[u64; 64]; 4],
        floor: impl Fn(usize) -> u64,
    ) -> Option<Self> {
        let hist: Vec<[u64; 64]> = (0..4)
            .map(|d| std::array::from_fn(|b| after[d][b].saturating_sub(before[d][b])))
            .collect();
        let (d, h) = hist
            .iter()
            .enumerate()
            .max_by_key(|(_, h)| h.iter().sum::<u64>())?;
        let total: u64 = h.iter().sum();
        if total == 0 {
            return None;
        }
        let at = |q: f64| {
            let target = ((total as f64 * q).ceil() as u64).max(1);
            let mut seen = 0;
            for (b, n) in h.iter().enumerate() {
                seen += n;
                if seen >= target {
                    return floor(b);
                }
            }
            floor(63)
        };
        Some(Self {
            durability: d as u8,
            commits: total,
            p50_ns: at(0.5),
            p99_ns: at(0.99),
            p999_ns: at(0.999),
        })
    }
}

/// What a run's measured phase stalled on (Pigeonhole, from its engine metrics, ICR 0015):
/// the write path's stalls and the background work behind them.
///
/// ```
/// use pigeonhole_bench::Stalls;
///
/// let before = Stalls { flushes: 2, write_stalls: 1, ..Default::default() };
/// let after = Stalls { flushes: 5, write_stalls: 1, ..Default::default() };
/// let d = Stalls::between(&before, &after);
/// assert_eq!((d.flushes, d.write_stalls), (3, 0));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stalls {
    /// Commits that waited for memtable room or the L0 token bucket, and the time they
    /// waited: the sum of the two kinds below.
    pub write_stalls: u64,
    /// See [`Stalls::write_stalls`].
    pub write_stall_nanos: u64,
    /// Waits for memtable arena room (arena pressure), and their time (ICR 0054). Absent
    /// from results written before them.
    #[serde(default)]
    pub room_stalls: u64,
    /// See [`Stalls::room_stalls`].
    #[serde(default)]
    pub room_stall_nanos: u64,
    /// L0 pacing spans (compaction debt), and their time (ICR 0054). Absent from results
    /// written before them.
    #[serde(default)]
    pub pacing_stalls: u64,
    /// See [`Stalls::pacing_stalls`].
    #[serde(default)]
    pub pacing_stall_nanos: u64,
    /// Flushes and compactions completed.
    pub flushes: u64,
    /// See [`Stalls::flushes`].
    pub compactions: u64,
    /// WAL unpin passes, and the small memtables they froze (#175).
    pub unpin_passes: u64,
    /// See [`Stalls::unpin_passes`].
    pub unpin_flushes: u64,
    /// WAL rollovers synced on a shard thread (#19).
    pub wal_inline_syncs: u64,
    /// File growths, and the time they held the page allocator (#28, #182).
    pub file_growths: u64,
    /// See [`Stalls::file_growths`].
    pub file_growth_nanos: u64,
    /// File growths ahead of need, outside the page allocator's lock (gate analysis D1b).
    /// Absent from results written before them.
    #[serde(default)]
    pub file_growths_ahead: u64,
    /// Idle parks of the shards' threads after their spin, wakes of a parked shard, and
    /// blocking commit waits that parked (ICR 0027): per commit, how often the commit
    /// path pays a thread wakeup (D198). Absent from results written before them.
    #[serde(default)]
    pub shard_parks: u64,
    /// See [`Stalls::shard_parks`].
    #[serde(default)]
    pub shard_wakes: u64,
    /// See [`Stalls::shard_parks`].
    #[serde(default)]
    pub commit_parks: u64,
    /// Commit waits (blocking and async) on the global visibility watermark (D19, D227), and the
    /// waiter-list entries their registrations scanned (ICR 0023, #154).
    #[serde(default)]
    pub visibility_waits: u64,
    /// See [`Stalls::visibility_waits`].
    #[serde(default)]
    pub visibility_scanned: u64,
    /// Visibility wake passes, and the waiters they woke.
    #[serde(default)]
    pub visibility_wake_passes: u64,
    /// See [`Stalls::visibility_wake_passes`].
    #[serde(default)]
    pub visibility_woken: u64,
    /// Visibility registrations and wake passes that found the waiter list locked.
    #[serde(default)]
    pub visibility_contended: u64,
    /// Times a shard held a commit group back because its WAL segment was nearly full while
    /// the next segment's header still waited for a sync (D213 exports it; expected near 0).
    #[serde(default)]
    pub wal_rollover_blocks: u64,
    /// Tablet splits and moves refused because a shard's memtable arena serves no more
    /// slots (a size split found due counts once per balancer pass; #123).
    #[serde(default)]
    pub slot_refusals: u64,
    /// Row cache lookups answered from the cache, lookups that missed, and family rows
    /// stored (D201, D216); all 0 with the row cache off.
    #[serde(default)]
    pub row_cache_hits: u64,
    /// See [`Stalls::row_cache_hits`].
    #[serde(default)]
    pub row_cache_misses: u64,
    /// See [`Stalls::row_cache_hits`].
    #[serde(default)]
    pub row_cache_fills: u64,
    /// Cross-shard two-phase rounds decided, and the rounds a participant refused (ICR 0043,
    /// `Metrics::cross_shard`): the refused rounds over the cross-shard commits are D246's
    /// wasted rounds per commit.
    #[serde(default)]
    pub xshard_rounds: u64,
    /// See [`Stalls::xshard_rounds`].
    #[serde(default)]
    pub xshard_refused_rounds: u64,
    /// Refused PREPAREs by reason: below a timestamp the participant assigned (D11), below a
    /// tablet move's raised floor, a tablet being changed or moved (D145), no arena room
    /// (ICR 0042). One refused round can hold several.
    #[serde(default)]
    pub xshard_refused_below_floor: u64,
    /// See [`Stalls::xshard_refused_below_floor`].
    #[serde(default)]
    pub xshard_refused_move_raise: u64,
    /// See [`Stalls::xshard_refused_below_floor`].
    #[serde(default)]
    pub xshard_refused_moved: u64,
    /// See [`Stalls::xshard_refused_below_floor`].
    #[serde(default)]
    pub xshard_refused_room: u64,
    /// PREPAREs refused because they overlapped another commit's undecided share and retried
    /// inside the engine, not returned as a `Conflict` (D249, ICR 0045).
    #[serde(default)]
    pub xshard_refused_pending_overlap: u64,
    /// D263: singles held back behind an undecided cross-shard share (once
    /// per group that held them), and the time shards had one held, summed over shards.
    #[serde(default)]
    pub xshard_share_waits: u64,
    /// See [`Stalls::xshard_share_waits`].
    #[serde(default)]
    pub xshard_share_wait_nanos: u64,
    /// D267: PREPAREs admitted behind an undecided share or a waiting single, and younger
    /// undecided shares wounded for one.
    #[serde(default)]
    pub xshard_chained: u64,
    /// See [`Stalls::xshard_chained`].
    #[serde(default)]
    pub xshard_wounds: u64,
    /// The most attempts one committed cross-shard commit took, for commits that only write
    /// and for commits that read. Not counts: the largest since the store opened, load and
    /// warmup included. Committed commits only: read beside `busy_retries`.
    #[serde(default)]
    pub xshard_max_attempts_blind: u64,
    /// See [`Stalls::xshard_max_attempts_blind`].
    #[serde(default)]
    pub xshard_max_attempts_reading: u64,
}

impl Stalls {
    /// The counts between two cumulative readings.
    pub fn between(before: &Stalls, after: &Stalls) -> Stalls {
        let d = |a: u64, b: u64| b.saturating_sub(a);
        Stalls {
            write_stalls: d(before.write_stalls, after.write_stalls),
            write_stall_nanos: d(before.write_stall_nanos, after.write_stall_nanos),
            room_stalls: d(before.room_stalls, after.room_stalls),
            room_stall_nanos: d(before.room_stall_nanos, after.room_stall_nanos),
            pacing_stalls: d(before.pacing_stalls, after.pacing_stalls),
            pacing_stall_nanos: d(before.pacing_stall_nanos, after.pacing_stall_nanos),
            flushes: d(before.flushes, after.flushes),
            compactions: d(before.compactions, after.compactions),
            unpin_passes: d(before.unpin_passes, after.unpin_passes),
            unpin_flushes: d(before.unpin_flushes, after.unpin_flushes),
            wal_inline_syncs: d(before.wal_inline_syncs, after.wal_inline_syncs),
            file_growths: d(before.file_growths, after.file_growths),
            file_growth_nanos: d(before.file_growth_nanos, after.file_growth_nanos),
            file_growths_ahead: d(before.file_growths_ahead, after.file_growths_ahead),
            shard_parks: d(before.shard_parks, after.shard_parks),
            shard_wakes: d(before.shard_wakes, after.shard_wakes),
            commit_parks: d(before.commit_parks, after.commit_parks),
            visibility_waits: d(before.visibility_waits, after.visibility_waits),
            visibility_scanned: d(before.visibility_scanned, after.visibility_scanned),
            visibility_wake_passes: d(before.visibility_wake_passes, after.visibility_wake_passes),
            visibility_woken: d(before.visibility_woken, after.visibility_woken),
            visibility_contended: d(before.visibility_contended, after.visibility_contended),
            slot_refusals: d(before.slot_refusals, after.slot_refusals),
            wal_rollover_blocks: d(before.wal_rollover_blocks, after.wal_rollover_blocks),
            row_cache_hits: d(before.row_cache_hits, after.row_cache_hits),
            row_cache_misses: d(before.row_cache_misses, after.row_cache_misses),
            row_cache_fills: d(before.row_cache_fills, after.row_cache_fills),
            xshard_rounds: d(before.xshard_rounds, after.xshard_rounds),
            xshard_refused_rounds: d(before.xshard_refused_rounds, after.xshard_refused_rounds),
            xshard_refused_below_floor: d(
                before.xshard_refused_below_floor,
                after.xshard_refused_below_floor,
            ),
            xshard_refused_move_raise: d(
                before.xshard_refused_move_raise,
                after.xshard_refused_move_raise,
            ),
            xshard_refused_moved: d(before.xshard_refused_moved, after.xshard_refused_moved),
            xshard_refused_room: d(before.xshard_refused_room, after.xshard_refused_room),
            xshard_refused_pending_overlap: d(
                before.xshard_refused_pending_overlap,
                after.xshard_refused_pending_overlap,
            ),
            xshard_share_waits: d(before.xshard_share_waits, after.xshard_share_waits),
            xshard_share_wait_nanos: d(
                before.xshard_share_wait_nanos,
                after.xshard_share_wait_nanos,
            ),
            xshard_chained: d(before.xshard_chained, after.xshard_chained),
            xshard_wounds: d(before.xshard_wounds, after.xshard_wounds),
            // Not counts: the largest seen since open.
            xshard_max_attempts_blind: after.xshard_max_attempts_blind,
            xshard_max_attempts_reading: after.xshard_max_attempts_reading,
        }
    }
}

/// One shard's share of a run's measured phase (Pigeonhole only): whether the scaling
/// gate's writes really spread over the shards (issue #51).
///
/// ```
/// use pigeonhole_bench::ShardShare;
///
/// let before = ShardShare { commits: 10, tablets_start: 1, tablets_end: 1, splits: 1, ..Default::default() };
/// let after = ShardShare { commits: 25, tablets_start: 3, tablets_end: 3, splits: 2, ..Default::default() };
/// let d = ShardShare::between(&before, &after);
/// assert_eq!((d.commits, d.tablets_start, d.tablets_end, d.splits), (15, 1, 3, 1));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardShare {
    /// Commits the shard applied.
    pub commits: u64,
    /// Tablets the shard owned when the measured phase started, after the load and warmup.
    pub tablets_start: u64,
    /// Tablets the shard owned when it ended.
    pub tablets_end: u64,
    /// Tablet splits the shard completed.
    pub splits: u64,
    /// Tablet merges the shard completed.
    pub merges: u64,
    /// Tablet moves the shard completed.
    pub moves: u64,
    /// Nanoseconds the shard's engine-owned thread spent in work turns (D217); divided by the
    /// phase's length, the fraction of it the shard was busy. Zero for application-owned
    /// shards.
    #[serde(default)]
    pub busy_nanos: u64,
    /// Nanoseconds the commits this shard completed spent in flight, from submit to reply
    /// (D257), plus the cross-shard shares it decided, from submit to the decision (D262);
    /// divided by the phase's length, the mean number of commit rounds in flight at the
    /// shard, and summed over shards the store's concurrency, which sizes the active
    /// shards. Absent from results written before it.
    #[serde(default)]
    pub commit_inflight_nanos: u64,
}

impl ShardShare {
    /// The share between two cumulative readings of the same shard: counters subtract,
    /// tablet counts are taken from each end.
    pub fn between(before: &ShardShare, after: &ShardShare) -> ShardShare {
        ShardShare {
            commits: after.commits.saturating_sub(before.commits),
            tablets_start: before.tablets_end,
            tablets_end: after.tablets_end,
            splits: after.splits.saturating_sub(before.splits),
            merges: after.merges.saturating_sub(before.merges),
            moves: after.moves.saturating_sub(before.moves),
            busy_nanos: after.busy_nanos.saturating_sub(before.busy_nanos),
            commit_inflight_nanos: after
                .commit_inflight_nanos
                .saturating_sub(before.commit_inflight_nanos),
        }
    }
}

impl RunRecord {
    /// The key that identifies "the same measurement" across two suites.
    pub fn key(&self) -> String {
        format!(
            "{}/{} [{}] t={}",
            self.workload, self.store, self.store_config, self.threads
        )
    }
}

/// The scaling gate: write throughput at N shards against one shard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scaling {
    /// Shards in the scaled run.
    pub shards: usize,
    /// Throughput with one shard.
    pub single_shard_throughput: f64,
    /// Throughput with `shards` shards.
    pub multi_shard_throughput: f64,
    /// `multi / (shards * single)`; the gate needs at least 0.8.
    pub efficiency: f64,
    /// Single-shard p99 in nanoseconds (the gate also needs no regression here,
    /// checked by [`compare`] against a baseline).
    pub single_shard_p99_ns: u64,
    /// The largest share of the N-shard run's commits on one shard (0 when the run did not
    /// report shares). Reported only: the gate's run is pre-split at the writes' quantiles,
    /// and placement is judged on its own by [`Placement`] (D231).
    #[serde(default)]
    pub max_share: f64,
    /// The N-shard run's async commits that waited on the global visibility watermark, per
    /// operation (0 when the run did not report it): near 0 means the watermark is not what
    /// the shards wait on (D19, #154).
    #[serde(default)]
    pub visibility_waits_per_op: f64,
}

impl Scaling {
    /// Minimum efficiency for the scaling gate (spec, Goals).
    pub const GATE: f64 = 0.8;

    /// Builds the gate result from a one-shard and an N-shard record.
    pub fn new(shards: usize, single: &RunRecord, multi: &RunRecord) -> Self {
        let efficiency = multi.throughput / (shards as f64 * single.throughput);
        let total: u64 = multi.detail.shards.iter().map(|s| s.commits).sum();
        let top = multi
            .detail
            .shards
            .iter()
            .map(|s| s.commits)
            .max()
            .unwrap_or(0);
        Self {
            shards,
            single_shard_throughput: single.throughput,
            multi_shard_throughput: multi.throughput,
            efficiency,
            single_shard_p99_ns: single.p99_ns,
            max_share: if total == 0 {
                0.0
            } else {
                top as f64 / total as f64
            },
            visibility_waits_per_op: multi.detail.stalls.map_or(0.0, |st| {
                st.visibility_waits as f64 / multi.operations.max(1) as f64
            }),
        }
    }

    /// Whether the scaling target is met: efficiency at the gate (D231: the placement is
    /// fixed by the pre-split, so efficiency alone is judged).
    pub fn passes(&self) -> bool {
        self.efficiency >= Self::GATE
    }
}

/// The balancer placement check (D231): an open-loop write load at a fixed offered rate over
/// N engine-owned shards, its table spread by the balancer. Once settled, the busiest shard
/// may take at most 1.25/N of the writes, it must settle within a bound, and nothing may
/// split, merge or move while measuring.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Placement {
    /// Shards.
    pub shards: usize,
    /// Writes offered per second (the drivers' schedule, kept whatever the store does).
    pub offered_rate: f64,
    /// Writes completed per second over the measured window.
    pub achieved_rate: f64,
    /// Seconds from the first write until the table had spread and nothing had split,
    /// merged or moved for [`Placement::QUIET_SECS`]; `None` when that never happened
    /// within [`settle_bound_secs`](Self::settle_bound_secs).
    pub settle_secs: Option<f64>,
    /// The settle-time bound, in seconds.
    pub settle_bound_secs: f64,
    /// Seconds measured after settling (or after the bound): the requested window, or longer
    /// if no write landed in it (up to `WINDOW_GROWTH` times, #802).
    pub window_secs: f64,
    /// Splits, merges and moves while measuring.
    pub changes_measured: u64,
    /// The largest share of the window's writes on one shard.
    pub max_share: f64,
    /// Every shard's share of the window's writes.
    #[serde(default)]
    pub shares: Vec<f64>,
}

impl Placement {
    /// The busiest shard's share may be at most this times an even share (D223, D231). At
    /// equal per-shard rates, scaling efficiency is at most `1 / (shards * max_share)`, so
    /// 1.25 is the loosest bound that still allows 0.8.
    pub const MAX_SHARE: f64 = 1.25;

    /// Seconds without a split, merge or move that count as settled: longer than the
    /// balancer's longest dwell (the slow skew path's 10 s, D224).
    pub const QUIET_SECS: f64 = 12.0;

    /// The window must complete at least this fraction of the offered rate, or the store
    /// could not keep up and the load was not open-loop.
    pub const SUSTAINED: f64 = 0.95;

    /// The largest share one shard may take: [`MAX_SHARE`](Self::MAX_SHARE) times an even
    /// share, or everything with one shard.
    ///
    /// ```
    /// let p = pigeonhole_bench::Placement { shards: 8, ..Default::default() };
    /// assert!((p.max_share_allowed() - 0.15625).abs() < 1e-12);
    /// ```
    pub fn max_share_allowed(&self) -> f64 {
        (Self::MAX_SHARE / self.shards.max(1) as f64).min(1.0)
    }

    /// Whether the store kept up with the offered rate over the window.
    pub fn sustained(&self) -> bool {
        self.achieved_rate >= Self::SUSTAINED * self.offered_rate
    }

    /// Whether the check passes: settled within the bound, no tablet change while measuring,
    /// the busiest share within bound, and the offered rate sustained.
    pub fn passes(&self) -> bool {
        self.settle_secs.is_some()
            && self.changes_measured == 0
            && self.max_share <= self.max_share_allowed()
            && self.sustained()
    }
}

/// The durable group-commit target (D213, D226): 4-cell `GroupSync` commits at N shards (one
/// per core), the table split over every shard up front, each shard's thread keeping
/// [`INLINE_IN_FLIGHT`](crate::INLINE_IN_FLIGHT) commits in flight (the D204 driver). A run
/// at another [`in_flight`](GroupCommit::in_flight) is reported beside it, not judged.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GroupCommit {
    /// Shards in the run.
    pub shards: usize,
    /// Physical cores the run could use (the default shard count, D220); logical CPUs
    /// ([`Environment::cores`]) in runs before D220.
    pub cores: usize,
    /// Cells committed per second, over every shard.
    pub cells_per_sec: f64,
    /// Commit latency (submit to durable resolution), nanoseconds.
    pub p50_ns: u64,
    /// See [`GroupCommit::p50_ns`].
    pub p99_ns: u64,
    /// The largest share of the run's commits on one shard (as [`Scaling::max_share`]).
    pub max_share: f64,
    /// Tablet splits, merges and moves during the measured phase: 0, or the run did not
    /// measure the fixed placement D213 asks for (and `phdb-bench` exits non-zero).
    #[serde(default)]
    pub tablet_changes: u64,
    /// The engine's part of the commit latency (submission to resolution on the shard).
    #[serde(default)]
    pub engine: Option<EngineCommit>,
    /// WAL rollover blocks and inline WAL syncs during the measured phase.
    #[serde(default)]
    pub wal_rollover_blocks: u64,
    /// See [`GroupCommit::wal_rollover_blocks`].
    #[serde(default)]
    pub wal_inline_syncs: u64,
    /// Commits each shard's thread kept in flight ([`INLINE_IN_FLIGHT`](crate::INLINE_IN_FLIGHT)
    /// in results written before the flag existed).
    #[serde(default = "judged_in_flight")]
    pub in_flight: usize,
}

fn judged_in_flight() -> usize {
    crate::INLINE_IN_FLIGHT
}

impl GroupCommit {
    /// Cells per second the target asks for, across cores.
    pub const CELLS_PER_SEC: f64 = 1e6;
    /// The p99 commit latency the target allows, nanoseconds.
    pub const P99_NS: u64 = 200_000;

    /// Whether this run is the judged shape: [`INLINE_IN_FLIGHT`](crate::INLINE_IN_FLIGHT)
    /// commits in flight per shard (D226; fewer are reported beside it).
    pub fn judged(&self) -> bool {
        self.in_flight == crate::INLINE_IN_FLIGHT
    }

    /// Builds the result from the D213 run's record (`in_flight` commits per shard): its
    /// throughput times [`GROUP_COMMIT_CELLS`](crate::GROUP_COMMIT_CELLS) cells per commit.
    pub fn new(shards: usize, cores: usize, in_flight: usize, run: &RunRecord) -> Self {
        let total: u64 = run.detail.shards.iter().map(|s| s.commits).sum();
        let top = run
            .detail
            .shards
            .iter()
            .map(|s| s.commits)
            .max()
            .unwrap_or(0);
        let st = run.detail.stalls.unwrap_or_default();
        Self {
            shards,
            cores,
            cells_per_sec: run.throughput * crate::GROUP_COMMIT_CELLS as f64,
            p50_ns: run.p50_ns,
            p99_ns: run.p99_ns,
            max_share: if total == 0 {
                0.0
            } else {
                top as f64 / total as f64
            },
            tablet_changes: run
                .detail
                .shards
                .iter()
                .map(|s| s.splits + s.merges + s.moves)
                .sum(),
            engine: run.detail.engine_commit,
            wal_rollover_blocks: st.wal_rollover_blocks,
            wal_inline_syncs: st.wal_inline_syncs,
            in_flight,
        }
    }

    /// Cells per second per core.
    ///
    /// ```
    /// use pigeonhole_bench::GroupCommit;
    ///
    /// let gc = GroupCommit {
    ///     shards: 8,
    ///     cores: 8,
    ///     cells_per_sec: 1.2e6,
    ///     p50_ns: 80_000,
    ///     p99_ns: 150_000,
    ///     max_share: 0.13,
    ///     in_flight: pigeonhole_bench::INLINE_IN_FLIGHT,
    ///     ..Default::default()
    /// };
    /// assert_eq!(gc.cells_per_core(), 150_000.0);
    /// assert!(gc.passes());
    /// // At fewer commits in flight it is reported, not judged (D226).
    /// assert!(!GroupCommit { in_flight: 8, ..gc }.passes());
    /// ```
    pub fn cells_per_core(&self) -> f64 {
        self.cells_per_sec / self.cores.max(1) as f64
    }

    /// Whether the target is met: more than 1M cells/s and p99 under 200 µs, measured on a
    /// placement that did not change, at the judged commits in flight (D226).
    pub fn passes(&self) -> bool {
        self.judged()
            && self.tablet_changes == 0
            && self.cells_per_sec > Self::CELLS_PER_SEC
            && self.p99_ns < Self::P99_NS
    }
}

/// Every result of one invocation, as written to `--json`.
///
/// ```
/// use pigeonhole_bench::{Environment, Suite};
///
/// let suite = Suite::new(Environment::detect(&std::env::temp_dir()));
/// let json = suite.to_json();
/// assert_eq!(Suite::from_json(&json).unwrap(), suite);
/// assert!(suite.to_markdown().contains("non-reference"));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Suite {
    /// [`SUITE_FORMAT`].
    pub format: u32,
    /// Where it ran.
    pub environment: Environment,
    /// One record per (workload, store, settings).
    pub results: Vec<RunRecord>,
    /// The scaling gate, when measured: application-owned shards writing inline (D204).
    pub scaling: Option<Scaling>,
    /// The engine-owned check reported beside the gate, not gating (D204): synchronous
    /// clients, four per shard, at half the gate's shards.
    #[serde(default)]
    pub scaling_sync: Option<Scaling>,
    /// The durable group-commit target (D213), when `group-commit` ran on Pigeonhole.
    #[serde(default)]
    pub group_commit: Option<GroupCommit>,
    /// The scaling gate's runs with the shard threads unpinned, reported beside the pinned,
    /// judged ones (D235).
    #[serde(default)]
    pub scaling_unpinned: Option<Scaling>,
    /// D213's group-commit run with the shard threads unpinned, reported beside the pinned,
    /// judged one (D235).
    #[serde(default)]
    pub group_commit_unpinned: Option<GroupCommit>,
    /// The balancer placement check (D231), when `placement` ran.
    #[serde(default)]
    pub placement: Option<Placement>,
}

fn us(ns: u64) -> String {
    let us = ns as f64 / 1000.0;
    if us >= 100.0 {
        format!("{us:.0}")
    } else if us >= 10.0 {
        format!("{us:.1}")
    } else {
        format!("{us:.2}")
    }
}

fn mib(bytes: u64) -> String {
    format!("{:.0} MiB", bytes as f64 / (1u64 << 20) as f64)
}

fn ops(t: f64) -> String {
    if t >= 1e6 {
        format!("{:.2}M", t / 1e6)
    } else if t >= 1e3 {
        format!("{:.1}K", t / 1e3)
    } else {
        format!("{t:.0}")
    }
}

impl Suite {
    /// An empty suite.
    pub fn new(environment: Environment) -> Self {
        Self {
            format: SUITE_FORMAT,
            environment,
            results: Vec::new(),
            scaling: None,
            scaling_sync: None,
            group_commit: None,
            scaling_unpinned: None,
            group_commit_unpinned: None,
            placement: None,
        }
    }

    /// Pretty JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("suite serializes")
    }

    /// Parses JSON written by [`Suite::to_json`].
    pub fn from_json(s: &str) -> Result<Self, String> {
        let suite: Suite = serde_json::from_str(s).map_err(|e| e.to_string())?;
        if suite.format != SUITE_FORMAT {
            return Err(format!(
                "suite format {} (expected {SUITE_FORMAT})",
                suite.format
            ));
        }
        Ok(suite)
    }

    /// A markdown summary: environment line, one table row per result, and the
    /// scaling verdict.
    pub fn to_markdown(&self) -> String {
        let env = &self.environment;
        let mut s = String::new();
        let _ = writeln!(s, "**Environment:** {}", env.summary());
        let _ = writeln!(
            s,
            "Build: {}{}. {}",
            env.profile,
            env.git_rev
                .as_ref()
                .map(|r| format!(", rev {r}"))
                .unwrap_or_default(),
            if env.reference {
                "Reference hardware."
            } else {
                "Not reference hardware: reported, never gates a phase (D5)."
            }
        );
        s.push('\n');
        s.push_str("| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |\n");
        s.push_str("|---|---|---|--:|--:|--:|--:|--:|--:|--:|\n");
        for r in &self.results {
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                r.workload,
                r.store,
                r.store_config,
                r.records,
                r.operations,
                r.threads,
                ops(r.throughput),
                us(r.p50_ns),
                us(r.p99_ns),
                us(r.p999_ns)
            );
        }
        let split: Vec<&RunRecord> = self
            .results
            .iter()
            .filter(|r| r.detail.reads.is_some() || r.detail.busy_retries > 0)
            .collect();
        if !split.is_empty() {
            s.push_str(
                "\n| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |\n",
            );
            s.push_str("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n");
            for r in split {
                let d = &r.detail;
                let reads = d.reads.unwrap_or_default();
                let _ = writeln!(
                    s,
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    r.workload,
                    r.store,
                    mib(d.store_bytes),
                    d.busy_retries,
                    reads.cold.count,
                    us(reads.cold.p50_ns),
                    us(reads.cold.p99_ns),
                    us(reads.cold.p999_ns),
                    reads.hot.count,
                    us(reads.hot.p50_ns),
                    us(reads.hot.p99_ns),
                    us(reads.hot.p999_ns),
                );
            }
        }
        let typed: Vec<&RunRecord> = self
            .results
            .iter()
            .filter(|r| r.detail.by_type.len() > 1)
            .collect();
        if !typed.is_empty() {
            s.push_str(
                "\n| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |\n",
            );
            s.push_str("|---|---|---|--:|--:|--:|--:|--:|\n");
            for r in typed {
                for t in &r.detail.by_type {
                    let _ = writeln!(
                        s,
                        "| {} | {} | {} | {} | {} | {} | {} | {} |",
                        r.workload,
                        r.store,
                        t.op,
                        t.stats.count,
                        us(t.mean_ns),
                        us(t.stats.p50_ns),
                        us(t.stats.p99_ns),
                        us(t.stats.p999_ns),
                    );
                }
            }
        }
        let rounds: Vec<(&RunRecord, Rounds)> = self
            .results
            .iter()
            .filter_map(|r| r.rounds().map(|x| (r, x)))
            .collect();
        if !rounds.is_empty() {
            s.push_str("\n| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: below floor, move raise, moved, room, pending overlap | Seqnos used | Inferred wasted rounds | Share waits, ms held |\n");
            s.push_str("|---|---|---|--:|--:|--:|--:|---|--:|--:|---|\n");
            let opt = |v: Option<u64>| v.map_or_else(|| "-".to_owned(), |v| v.to_string());
            for (r, x) in rounds {
                let reasons = r.detail.stalls.as_ref().map_or_else(
                    || "-".to_owned(),
                    |st| {
                        format!(
                            "{}, {}, {}, {}, {}",
                            st.xshard_refused_below_floor,
                            st.xshard_refused_move_raise,
                            st.xshard_refused_moved,
                            st.xshard_refused_room,
                            st.xshard_refused_pending_overlap
                        )
                    },
                );
                let share_waits = r.detail.stalls.as_ref().map_or_else(
                    || "-".to_owned(),
                    |st| {
                        format!(
                            "{}, {:.1}",
                            st.xshard_share_waits,
                            st.xshard_share_wait_nanos as f64 / 1e6
                        )
                    },
                );
                let _ = writeln!(
                    s,
                    "| {} | {} | {} | {} | {} | {} | {:.3} | {} | {} | {} | {} |",
                    r.workload,
                    r.store,
                    r.store_config,
                    x.commits,
                    x.cross,
                    opt(x.refused),
                    x.per_cross(),
                    reasons,
                    opt(x.seqnos),
                    opt(x.inferred()),
                    share_waits,
                );
            }
        }
        let spread: Vec<&RunRecord> = self
            .results
            .iter()
            .filter(|r| r.detail.shards.len() > 1)
            .collect();
        if !spread.is_empty() {
            s.push_str("\n| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |\n");
            s.push_str("|---|---|---|--:|--:|--:|--:|--:|--:|--:|\n");
            for r in spread {
                let total: u64 = r.detail.shards.iter().map(|d| d.commits).sum();
                for (i, d) in r.detail.shards.iter().enumerate() {
                    let _ = writeln!(
                        s,
                        "| {} | {} | {} | {i} | {} | {:.1}% | {} → {} | {} | {} | {} |",
                        r.workload,
                        r.store,
                        r.store_config,
                        d.commits,
                        100.0 * d.commits as f64 / total.max(1) as f64,
                        d.tablets_start,
                        d.tablets_end,
                        d.splits,
                        d.merges,
                        d.moves,
                    );
                }
            }
        }
        let stalled: Vec<(&RunRecord, &Stalls)> = self
            .results
            .iter()
            .filter_map(|r| r.detail.stalls.as_ref().map(|st| (r, st)))
            .collect();
        if !stalled.is_empty() {
            s.push_str("\n| Workload | Store | Threads | Write stalls (room / pacing) | Stalled ms (room / pacing) | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |\n");
            s.push_str("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n");
            for (r, st) in stalled {
                let ms = |ns: u64| format!("{:.1}", ns as f64 / 1e6);
                let per_op = |n: u64| format!("{:.2}", n as f64 / r.operations.max(1) as f64);
                let _ = writeln!(
                    s,
                    "| {} | {} | {} | {} ({} / {}) | {} ({} / {}) | {} | {} | {} ({}) | {} | {} | {} | {} | {} | {} | {} | {} ({}) | {} |",
                    r.workload,
                    r.store,
                    r.threads,
                    st.write_stalls,
                    st.room_stalls,
                    st.pacing_stalls,
                    ms(st.write_stall_nanos),
                    ms(st.room_stall_nanos),
                    ms(st.pacing_stall_nanos),
                    st.flushes,
                    st.compactions,
                    st.unpin_passes,
                    st.unpin_flushes,
                    st.wal_inline_syncs,
                    st.wal_rollover_blocks,
                    st.file_growths,
                    ms(st.file_growth_nanos),
                    per_op(st.shard_parks),
                    per_op(st.shard_wakes),
                    per_op(st.commit_parks),
                    st.visibility_waits,
                    st.visibility_contended,
                    st.slot_refusals,
                );
            }
        }
        if let Some(sc) = &self.scaling {
            let _ = writeln!(
                s,
                "\n**Scaling gate:** {} shards reach {} ops/s against {} ops/s on one shard: \
                 efficiency {:.2} (gate ≥ {:.1}); the table pre-split at the writes' quantiles, the \
                 busiest shard took {:.0}% of the commits; {:.3} visibility waits per operation — {}.",
                sc.shards,
                ops(sc.multi_shard_throughput),
                ops(sc.single_shard_throughput),
                sc.efficiency,
                Scaling::GATE,
                100.0 * sc.max_share,
                sc.visibility_waits_per_op,
                if sc.passes() { "pass" } else { "fail" }
            );
        }
        if let Some(p) = &self.placement {
            let _ = writeln!(
                s,
                "\n**Balancer placement (D231):** {} shards, open-loop at {} writes/s offered ({} \
                 completed); {}; the busiest shard took {:.1}% of {:.0} s of writes (at most \
                 {:.1}%); {} tablet changes while measuring — {}.",
                p.shards,
                ops(p.offered_rate),
                ops(p.achieved_rate),
                match p.settle_secs {
                    Some(t) => format!(
                        "settled after {t:.0} s (bound {:.0} s)",
                        p.settle_bound_secs
                    ),
                    None => format!("did not settle within {:.0} s", p.settle_bound_secs),
                },
                100.0 * p.max_share,
                p.window_secs,
                100.0 * p.max_share_allowed(),
                p.changes_measured,
                if p.passes() {
                    "pass"
                } else if !p.sustained() {
                    "fail: the store did not keep up with the offered rate"
                } else {
                    "fail"
                }
            );
        }
        if let Some(gc) = &self.group_commit {
            let _ = writeln!(
                s,
                "\n**Durable group commit (D213):** {} shards on {} cores, the table split over \
                 every shard, {} commits in flight per shard: {} cells/s ({} per core), commit \
                 p50 {} µs, p99 {} µs (target > 1M cells/s, p99 < 200 µs); the busiest shard \
                 took {:.0}% of the commits; {} tablet changes while measuring{}; {} WAL \
                 rollover blocks, {} inline WAL syncs — {}.",
                gc.shards,
                gc.cores,
                gc.in_flight,
                ops(gc.cells_per_sec),
                ops(gc.cells_per_core()),
                us(gc.p50_ns),
                us(gc.p99_ns),
                100.0 * gc.max_share,
                gc.tablet_changes,
                gc.engine.map_or_else(String::new, |e| format!(
                    "; inside the engine p50 {} µs, p99 {} µs, p99.9 {} µs",
                    us(e.p50_ns),
                    us(e.p99_ns),
                    us(e.p999_ns)
                )),
                gc.wal_rollover_blocks,
                gc.wal_inline_syncs,
                if gc.tablet_changes > 0 {
                    "invalid: the placement changed while measuring"
                } else if !gc.judged() {
                    "reported, not judged (D226 judges 16 in flight)"
                } else if gc.passes() {
                    "pass"
                } else {
                    "fail"
                }
            );
        }
        if let Some(sc) = &self.scaling_unpinned {
            let _ = writeln!(
                s,
                "\n**Scaling with the shard threads unpinned (reported, not judged; D235):** \
                 {} shards reach {} ops/s against {} ops/s on one shard: efficiency {:.2}.",
                sc.shards,
                ops(sc.multi_shard_throughput),
                ops(sc.single_shard_throughput),
                sc.efficiency,
            );
        }
        if let Some(gc) = &self.group_commit_unpinned {
            let _ = writeln!(
                s,
                "\n**Durable group commit with the shard threads unpinned (reported, not \
                 judged; D235):** {} cells/s, commit p50 {} µs, p99 {} µs.",
                ops(gc.cells_per_sec),
                us(gc.p50_ns),
                us(gc.p99_ns),
            );
        }
        if let Some(sc) = &self.scaling_sync {
            let _ = writeln!(
                s,
                "\n**Engine-owned, synchronous clients (reported, not gating; D204):** {} \
                 shards with 4 clients each reach {} ops/s against {} ops/s on one shard: \
                 efficiency {:.2}.",
                sc.shards,
                ops(sc.multi_shard_throughput),
                ops(sc.single_shard_throughput),
                sc.efficiency,
            );
        }
        s
    }
}

/// How far a candidate run may drift from a baseline and still count as the same
/// result. Relative: 0.20 allows ±20%. A latency also passes within its absolute floor
/// (D259): it fails only past both.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tolerance {
    /// Throughput.
    pub throughput: f64,
    /// Median latency.
    pub p50: f64,
    /// 99th percentile. Tails are noisier, so this is looser.
    pub p99: f64,
    /// The p50's absolute floor, in nanoseconds (D259).
    pub p50_floor_ns: f64,
    /// The p99's absolute floor, in nanoseconds (D259).
    pub p99_floor_ns: f64,
}

impl Default for Tolerance {
    /// The documented reproducibility tolerance (docs/bench.md): ±20% throughput,
    /// p50 within ±20% or ±3 µs, p99 within ±40% or ±6 µs (D259). p99.9 and max are
    /// reported but not checked.
    fn default() -> Self {
        Self::uniform(0.20)
    }
}

/// The p50's absolute floor per unit of relative tolerance (D259): 3 µs at the default
/// 0.20. The p99's is twice this, as its relative tolerance is.
const P50_FLOOR_NS_PER_T: f64 = 15_000.0;

impl Tolerance {
    /// `t` for throughput and p50, `2t` for p99; floors of `t × 15 µs` for p50 and
    /// `t × 30 µs` for p99 (D259: 3 and 6 µs at 0.20).
    pub fn uniform(t: f64) -> Self {
        Self {
            throughput: t,
            p50: t,
            p99: 2.0 * t,
            p50_floor_ns: t * P50_FLOOR_NS_PER_T,
            p99_floor_ns: 2.0 * t * P50_FLOOR_NS_PER_T,
        }
    }
}

/// One metric of one record, baseline against candidate.
#[derive(Debug, Clone, PartialEq)]
pub struct Delta {
    /// Record key (see [`RunRecord::key`]).
    pub key: String,
    /// `throughput`, `p50`, `p99` or `p99.9`.
    pub metric: &'static str,
    /// Baseline value.
    pub baseline: f64,
    /// Candidate value.
    pub candidate: f64,
    /// `candidate / baseline - 1`.
    pub change: f64,
    /// Allowed `|change|`; `None` for informational metrics.
    pub allowed: Option<f64>,
    /// Allowed `|candidate - baseline|` in nanoseconds, for a judged latency (D259):
    /// within either this or `allowed` passes.
    pub floor_ns: Option<f64>,
}

impl Delta {
    /// Whether this metric is within tolerance (informational metrics always are): within
    /// its relative tolerance or, for a latency, its absolute floor (D259).
    pub fn ok(&self) -> bool {
        self.allowed.is_none_or(|a| {
            self.change.abs() <= a
                || self
                    .floor_ns
                    .is_some_and(|f| (self.candidate - self.baseline).abs() <= f)
        })
    }

    /// The Allowed column: `±20%`, or `±20% or ±3 µs` for a judged latency.
    fn allowed_text(&self) -> String {
        let Some(a) = self.allowed else {
            return "—".into();
        };
        match self.floor_ns {
            Some(f) => {
                let us = f / 1000.0;
                if us.fract() == 0.0 {
                    format!("±{:.0}% or ±{us:.0} µs", a * 100.0)
                } else {
                    format!("±{:.0}% or ±{us:.2} µs", a * 100.0)
                }
            }
            None => format!("±{:.0}%", a * 100.0),
        }
    }
}

/// The result of [`compare`].
#[derive(Debug, Clone, PartialEq)]
pub struct Comparison {
    /// Every compared metric.
    pub deltas: Vec<Delta>,
    /// Records present in only one suite.
    pub unmatched: Vec<String>,
    /// Set when the suites ran on different machines or build profiles, or on a busy
    /// machine.
    pub environment_warning: Option<String>,
    /// Matched records whose measured phases did different background work (D233): one ran
    /// flushes or compactions and the other none, or one did more than twice the other's.
    /// A throughput gap there may be that work, not the store. Reported, not judged.
    pub background_notes: Vec<String>,
    /// Matched records too short to judge (D243): a measured phase under
    /// [`JUDGED_MIN_SECS`] in either run, unless it is a group-commit run of at least
    /// [`JUDGED_MIN_COMMITS`]. Their metrics are reported with no tolerance.
    pub short: Vec<String>,
}

/// The shortest measured phase the reproducibility check judges (D243): a shorter one
/// holds too few flushes and compactions for two runs to agree on more than timing luck.
pub const JUDGED_MIN_SECS: f64 = 10.0;

/// A group-commit run of at least this many commits is judged however long it ran (D243,
/// as D219 judges the durable group-commit target).
pub const JUDGED_MIN_COMMITS: u64 = 2_000_000;

/// Whether the reproducibility check judges `r` (D243).
fn judged(r: &RunRecord) -> bool {
    r.run_secs >= JUDGED_MIN_SECS
        || (r.workload == crate::WorkloadKind::GroupCommit.name()
            && r.operations >= JUDGED_MIN_COMMITS)
}

impl Comparison {
    /// Whether every checked metric is within tolerance and every record matched.
    pub fn passes(&self) -> bool {
        self.unmatched.is_empty() && self.deltas.iter().all(Delta::ok)
    }

    /// A markdown report of the comparison.
    pub fn to_markdown(&self) -> String {
        let mut s = String::new();
        if let Some(w) = &self.environment_warning {
            let _ = writeln!(s, "**Warning:** {w}\n");
        }
        s.push_str("| Result | Metric | Baseline | Candidate | Change | Allowed | |\n");
        s.push_str("|---|---|--:|--:|--:|--:|---|\n");
        for d in &self.deltas {
            let fmt = |v: f64| {
                if d.metric == "throughput" {
                    ops(v)
                } else {
                    format!("{} µs", us(v as u64))
                }
            };
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} | {:+.1}% | {} | {} |",
                d.key,
                d.metric,
                fmt(d.baseline),
                fmt(d.candidate),
                d.change * 100.0,
                d.allowed_text(),
                if d.ok() { "ok" } else { "**FAIL**" }
            );
        }
        for u in &self.unmatched {
            let _ = writeln!(s, "\nOnly in one run: {u}");
        }
        for n in &self.background_notes {
            let _ = writeln!(s, "\n**Background work differs:** {n}");
        }
        for n in &self.short {
            let _ = writeln!(
                s,
                "\n**Not judged (D243, shorter than {JUDGED_MIN_SECS:.0} s):** {n}"
            );
        }
        let _ = writeln!(
            s,
            "\n**{}**",
            if self.passes() {
                "Runs agree within tolerance."
            } else {
                "Runs disagree beyond tolerance."
            }
        );
        s
    }
}

/// Compares `candidate` against `baseline`, record by record (matched by
/// [`RunRecord::key`]). This is the reproducibility gate: two runs of the same suite on
/// the same machine must agree within `tolerance`.
///
/// ```
/// use pigeonhole_bench::{Environment, RunRecord, Suite, Tolerance, compare};
///
/// let rec = |tput: f64| RunRecord {
///     store: "pigeonhole".into(), store_config: String::new(), workload: "ycsb-c".into(),
///     seed: 1, records: 10, operations: 10, value_len: 8, threads: 1, load_secs: 0.0,
///     run_secs: 10.0, throughput: tput, p50_ns: 1000, p99_ns: 2000, p999_ns: 3000,
///     mean_ns: 1100, max_ns: 5000, warmup_ops: 0, detail: Default::default(),
/// };
/// let env = Environment::detect(&std::env::temp_dir());
/// let mut a = Suite::new(env.clone());
/// a.results.push(rec(100_000.0));
/// let mut b = Suite::new(env);
/// b.results.push(rec(110_000.0));
/// assert!(compare(&a, &b, Tolerance::default()).passes());
/// b.results[0].throughput = 50_000.0;
/// assert!(!compare(&a, &b, Tolerance::default()).passes());
/// ```
pub fn compare(baseline: &Suite, candidate: &Suite, tolerance: Tolerance) -> Comparison {
    let mut deltas = Vec::new();
    let mut unmatched = Vec::new();
    let mut background_notes = Vec::new();
    let mut short = Vec::new();
    for b in &baseline.results {
        let Some(c) = candidate.results.iter().find(|c| c.key() == b.key()) else {
            unmatched.push(b.key());
            continue;
        };
        if let (Some(bw), Some(cw)) = (b.detail.background_work, c.detail.background_work)
            && background_differs(bw, cw)
        {
            background_notes.push(format!(
                "{}: {bw} flushes and compactions in the baseline's measured phase, {cw} in \
                 the candidate's",
                b.key()
            ));
        }
        let judge = judged(b) && judged(c);
        if !judge {
            short.push(format!(
                "{}: measured {:.2} s and {:.2} s",
                b.key(),
                b.run_secs,
                c.run_secs
            ));
        }
        let allow = |t: f64| judge.then_some(t);
        type Metric = (&'static str, f64, f64, Option<f64>, Option<f64>);
        let metrics: [Metric; 4] = [
            (
                "throughput",
                b.throughput,
                c.throughput,
                allow(tolerance.throughput),
                None,
            ),
            (
                "p50",
                b.p50_ns as f64,
                c.p50_ns as f64,
                allow(tolerance.p50),
                allow(tolerance.p50_floor_ns),
            ),
            (
                "p99",
                b.p99_ns as f64,
                c.p99_ns as f64,
                allow(tolerance.p99),
                allow(tolerance.p99_floor_ns),
            ),
            ("p99.9", b.p999_ns as f64, c.p999_ns as f64, None, None),
        ];
        for (metric, bv, cv, allowed, floor_ns) in metrics {
            let change = if bv > 0.0 { cv / bv - 1.0 } else { 0.0 };
            deltas.push(Delta {
                key: b.key(),
                metric,
                baseline: bv,
                candidate: cv,
                change,
                allowed,
                floor_ns,
            });
        }
    }
    for c in &candidate.results {
        if !baseline.results.iter().any(|b| b.key() == c.key()) {
            unmatched.push(c.key());
        }
    }
    let (b, c) = (&baseline.environment, &candidate.environment);
    let environment_warning = if !b.same_machine(c) {
        Some(format!(
            "the runs come from different machines or builds ({} vs {}); the \
             reproducibility tolerance only applies to one machine",
            b.summary(),
            c.summary()
        ))
    } else if b.busy() || c.busy() {
        Some(format!(
            "the machine was busy when a run started (load {:.2} and {:.2}); other work \
             skews every number",
            b.load_average.unwrap_or(0.0),
            c.load_average.unwrap_or(0.0)
        ))
    } else {
        None
    };
    Comparison {
        deltas,
        unmatched,
        environment_warning,
        background_notes,
        short,
    }
}

/// Whether two measured phases' background work counts differ enough to note (D233): one
/// did some and the other none, or one more than twice the other.
fn background_differs(a: u64, b: u64) -> bool {
    let (lo, hi) = (a.min(b), a.max(b));
    (lo == 0 && hi > 0) || hi > 2 * lo
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(workload: &str, tput: f64, p50: u64, p99: u64) -> RunRecord {
        RunRecord {
            store: "pigeonhole".into(),
            store_config: "shards=1".into(),
            workload: workload.into(),
            seed: 1,
            records: 100,
            operations: 100,
            value_len: 100,
            threads: 1,
            load_secs: 0.1,
            run_secs: 10.0,
            throughput: tput,
            p50_ns: p50,
            p99_ns: p99,
            p999_ns: p99 * 2,
            mean_ns: p50,
            max_ns: p99 * 3,
            warmup_ops: 0,
            detail: RunDetail::default(),
        }
    }

    fn suite(records: Vec<RunRecord>) -> Suite {
        let mut s = Suite::new(Environment::detect(Path::new(".")));
        s.results = records;
        s
    }

    #[test]
    fn compare_notes_pairs_whose_background_work_differs() {
        // D233: the 2026-10-12 gate's ycsb-b differed by 25% between runs; one measured
        // phase ran 7 flushes and 7 compactions, the other none.
        let with = |work: Option<u64>| {
            let mut r = rec("ycsb-b", 1e5, 1000, 5000);
            r.detail.background_work = work;
            r
        };
        let cmp = |a, b| {
            compare(
                &suite(vec![with(a)]),
                &suite(vec![with(b)]),
                Tolerance::default(),
            )
        };
        let c = cmp(Some(14), Some(0));
        assert_eq!(c.background_notes.len(), 1);
        assert!(c.background_notes[0].contains("14 flushes and compactions"));
        assert!(c.to_markdown().contains("Background work differs"));
        assert!(c.passes(), "noted, not judged");
        assert!(cmp(Some(3), Some(7)).background_notes.len() == 1);
        assert!(cmp(Some(4), Some(7)).background_notes.is_empty());
        assert!(cmp(Some(0), Some(0)).background_notes.is_empty());
        assert!(
            cmp(None, Some(5)).background_notes.is_empty(),
            "uncounted stores"
        );
    }

    #[test]
    fn compare_reports_short_runs_without_judging_them() {
        // D243: the 2026-10-13 pair-2 run's 0.16 s D213 shape differed by +53% throughput.
        let a = rec("ycsb-a", 1e5, 1000, 5000);
        let mut short = rec("ycsb-a", 2e5, 500, 5000);
        short.run_secs = 0.16;
        let c = compare(
            &suite(vec![a.clone()]),
            &suite(vec![short]),
            Tolerance::default(),
        );
        assert!(c.passes(), "reported, not judged");
        assert_eq!(c.short.len(), 1);
        assert!(c.short[0].contains("10.00 s and 0.16 s"));
        assert!(c.deltas.iter().all(|d| d.allowed.is_none()));
        assert!(c.to_markdown().contains("Not judged (D243"));
        // A group-commit run of 2M commits is judged however short (D219).
        let gc = |tput: f64| {
            let mut r = rec("group-commit", tput, 1000, 5000);
            r.run_secs = 0.8;
            r.operations = JUDGED_MIN_COMMITS;
            r
        };
        let c = compare(
            &suite(vec![gc(1e6)]),
            &suite(vec![gc(2e6)]),
            Tolerance::default(),
        );
        assert!(c.short.is_empty());
        assert!(!c.passes());
        let mut few = gc(2e6);
        few.operations = JUDGED_MIN_COMMITS - 1;
        assert!(
            compare(
                &suite(vec![gc(1e6)]),
                &suite(vec![few]),
                Tolerance::default()
            )
            .passes()
        );
    }

    #[test]
    fn identical_runs_pass() {
        let a = suite(vec![rec("ycsb-a", 1e5, 1000, 5000)]);
        let mut a = a;
        a.environment.load_average = Some(0.2);
        let cmp = compare(&a, &a, Tolerance::default());
        assert!(cmp.passes());
        assert!(cmp.environment_warning.is_none());
        let mut busy = a.clone();
        busy.environment.load_average = Some(3.5);
        assert!(busy.environment.summary().contains("load 3.50"));
        let cmp = compare(&a, &busy, Tolerance::default());
        assert!(
            cmp.passes(),
            "a busy machine warns but does not fail on its own"
        );
        assert!(cmp.environment_warning.unwrap().contains("busy"));
        assert_eq!(cmp.deltas.len(), 4);
    }

    #[test]
    fn each_checked_metric_can_fail() {
        let a = suite(vec![rec("ycsb-a", 1e5, 20_000, 50_000)]);
        for b in [
            rec("ycsb-a", 0.75e5, 20_000, 50_000),
            rec("ycsb-a", 1e5, 25_000, 50_000),
            rec("ycsb-a", 1e5, 20_000, 75_000),
        ] {
            let cmp = compare(&a, &suite(vec![b]), Tolerance::default());
            assert!(!cmp.passes());
            assert!(cmp.to_markdown().contains("FAIL"));
        }
        // p99.9 is informational only.
        let mut b = rec("ycsb-a", 1e5, 20_000, 50_000);
        b.p999_ns *= 10;
        assert!(compare(&a, &suite(vec![b]), Tolerance::default()).passes());
    }

    #[test]
    fn latency_fails_only_past_both_its_tolerance_and_its_floor() {
        // D259: the 2026-10-19 gate's ycsb-f p50 at one thread, 10.2 -> 12.7 µs (+25%,
        // 2.5 µs), is within the 3 µs floor.
        let pass = |base: (u64, u64), cand: (u64, u64), t: Tolerance| {
            compare(
                &suite(vec![rec("ycsb-f", 1e5, base.0, base.1)]),
                &suite(vec![rec("ycsb-f", 1e5, cand.0, cand.1)]),
                t,
            )
            .passes()
        };
        let t = Tolerance::default();
        assert!(pass((10_200, 26_400), (12_700, 28_800), t));
        assert!(pass((1000, 5000), (1250, 7500), t), "sub-µs drift");
        // Past both: +35% and 3.5 µs at p50, +65% and 6.5 µs at p99.
        assert!(!pass((10_000, 30_000), (13_500, 30_000), t));
        assert!(!pass((10_000, 10_000), (10_000, 16_500), t));
        // Within the relative tolerance however many µs.
        assert!(pass((100_000, 300_000), (115_000, 400_000), t));
        // The floors scale with --tolerance as the p99 tolerance does: 6 and 12 µs at 0.40.
        let wide = Tolerance::uniform(0.40);
        assert!((wide.p50_floor_ns - 6000.0).abs() < 1e-6);
        assert!((wide.p99_floor_ns - 12_000.0).abs() < 1e-6);
        assert!(pass((10_000, 10_000), (15_500, 21_500), wide));
        assert!(!pass((10_000, 10_000), (16_500, 10_000), wide));
        // Throughput has no floor.
        assert!(
            !compare(
                &suite(vec![rec("ycsb-f", 1e5, 1000, 5000)]),
                &suite(vec![rec("ycsb-f", 0.75e5, 1000, 5000)]),
                t,
            )
            .passes()
        );
        let md = compare(
            &suite(vec![rec("ycsb-f", 1e5, 10_200, 26_400)]),
            &suite(vec![rec("ycsb-f", 1e5, 12_700, 28_800)]),
            t,
        )
        .to_markdown();
        assert!(md.contains("| ±20% or ±3 µs | ok |"), "{md}");
        assert!(md.contains("| ±40% or ±6 µs | ok |"), "{md}");
        assert!(
            md.contains("| throughput |") && md.contains("| ±20% | ok |"),
            "{md}"
        );
    }

    #[test]
    fn unmatched_records_fail() {
        let a = suite(vec![rec("ycsb-a", 1e5, 1000, 5000)]);
        let b = suite(vec![rec("ycsb-b", 1e5, 1000, 5000)]);
        let cmp = compare(&a, &b, Tolerance::default());
        assert!(!cmp.passes());
        assert_eq!(cmp.unmatched.len(), 2);
    }

    #[test]
    fn different_machines_warn() {
        let a = suite(vec![rec("ycsb-a", 1e5, 1000, 5000)]);
        let mut b = a.clone();
        b.environment.cpu = "Some Other CPU".into();
        assert!(
            compare(&a, &b, Tolerance::default())
                .environment_warning
                .is_some()
        );
    }

    #[test]
    fn json_round_trip_and_markdown() {
        let mut a = suite(vec![rec("ycsb-a", 1.5e6, 1500, 25_000)]);
        a.scaling = Some(Scaling::new(
            4,
            &rec("skewed-multi-shard", 1e5, 1000, 5000),
            &rec("skewed-multi-shard", 3.6e5, 1000, 5000),
        ));
        let back = Suite::from_json(&a.to_json()).unwrap();
        assert_eq!(back, a);
        let md = a.to_markdown();
        assert!(md.contains("| ycsb-a | pigeonhole | shards=1 |"));
        assert!(md.contains("1.50M"));
        assert!(md.contains("efficiency 0.90"));
        assert!(md.contains("pass"));
        assert!(Suite::from_json("{}").is_err());
    }

    #[test]
    fn scaling_is_judged_on_efficiency_and_placement_on_its_own() {
        // D231: engine scaling passes on efficiency alone (its busiest share is reported).
        let mut a = suite(vec![rec("ycsb-a", 1e5, 1000, 5000)]);
        let mut sc = Scaling::new(
            8,
            &rec("skewed-multi-shard", 1e5, 1000, 5000),
            &rec("skewed-multi-shard", 7e5, 1000, 5000),
        );
        sc.max_share = 0.189;
        assert!(sc.passes(), "efficiency {}", sc.efficiency);
        a.scaling = Some(sc);
        // The placement check: at 8 shards the busiest may take 15.6%, after settling, with
        // no change while measuring, at the offered rate.
        let mut p = Placement {
            shards: 8,
            offered_rate: 400_000.0,
            achieved_rate: 399_000.0,
            settle_secs: Some(20.0),
            settle_bound_secs: 60.0,
            window_secs: 10.0,
            changes_measured: 0,
            max_share: 0.15,
            shares: Vec::new(),
        };
        assert!(p.passes());
        a.placement = Some(p.clone());
        assert!(a.to_markdown().contains("(at most 15.6%)"));
        assert!(a.to_markdown().contains("— pass."));
        // 18.9% (perf287's 8-shard run) passed the old 2/N bound but caps efficiency at 0.66.
        p.max_share = 0.189;
        assert!(!p.passes());
        p.max_share = 0.15;
        p.changes_measured = 1;
        assert!(!p.passes(), "a change while measuring fails it");
        p.changes_measured = 0;
        p.settle_secs = None;
        assert!(!p.passes(), "never settling fails it");
        p.settle_secs = Some(20.0);
        p.achieved_rate = 300_000.0;
        assert!(!p.sustained() && !p.passes());
        a.placement = Some(p);
        assert!(a.to_markdown().contains("did not keep up"));
        let back = Suite::from_json(&a.to_json()).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn environment_is_never_reference_by_default() {
        let env = Environment::detect(Path::new("."));
        if std::env::var("PHDB_BENCH_REFERENCE").is_err() {
            assert!(!env.reference);
            assert_eq!(env.label, "non-reference (D5)");
        }
        assert!(env.cores >= 1);
    }
}
