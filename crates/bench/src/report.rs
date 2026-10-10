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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// waited.
    pub write_stalls: u64,
    /// See [`Stalls::write_stalls`].
    pub write_stall_nanos: u64,
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
}

impl Stalls {
    /// The counts between two cumulative readings.
    pub fn between(before: &Stalls, after: &Stalls) -> Stalls {
        let d = |a: u64, b: u64| b.saturating_sub(a);
        Stalls {
            write_stalls: d(before.write_stalls, after.write_stalls),
            write_stall_nanos: d(before.write_stall_nanos, after.write_stall_nanos),
            flushes: d(before.flushes, after.flushes),
            compactions: d(before.compactions, after.compactions),
            unpin_passes: d(before.unpin_passes, after.unpin_passes),
            unpin_flushes: d(before.unpin_flushes, after.unpin_flushes),
            wal_inline_syncs: d(before.wal_inline_syncs, after.wal_inline_syncs),
            file_growths: d(before.file_growths, after.file_growths),
            file_growth_nanos: d(before.file_growth_nanos, after.file_growth_nanos),
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
    /// report shares). Above [`Scaling::max_share_allowed`] the table was not spread and
    /// the run fails as skewed whatever its efficiency (D204).
    #[serde(default)]
    pub max_share: f64,
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
        }
    }

    /// The largest share of commits one shard may take for the run to count: twice an even
    /// share (`2 / shards`), or everything with one shard.
    pub fn max_share_allowed(&self) -> f64 {
        (2.0 / self.shards.max(1) as f64).min(1.0)
    }

    /// Whether the N-shard run's commits were spread (no shard over
    /// [`max_share_allowed`](Self::max_share_allowed)).
    pub fn spread(&self) -> bool {
        self.max_share <= self.max_share_allowed()
    }

    /// Whether the scaling target is met: efficiency at the gate, on a spread table.
    pub fn passes(&self) -> bool {
        self.efficiency >= Self::GATE && self.spread()
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
            s.push_str("\n| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | File growths | Growth ms |\n");
            s.push_str("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n");
            for (r, st) in stalled {
                let ms = |ns: u64| format!("{:.1}", ns as f64 / 1e6);
                let _ = writeln!(
                    s,
                    "| {} | {} | {} | {} | {} | {} | {} | {} ({}) | {} | {} | {} |",
                    r.workload,
                    r.store,
                    r.threads,
                    st.write_stalls,
                    ms(st.write_stall_nanos),
                    st.flushes,
                    st.compactions,
                    st.unpin_passes,
                    st.unpin_flushes,
                    st.wal_inline_syncs,
                    st.file_growths,
                    ms(st.file_growth_nanos),
                );
            }
        }
        if let Some(sc) = &self.scaling {
            let _ = writeln!(
                s,
                "\n**Scaling gate:** {} shards reach {} ops/s against {} ops/s on one shard: \
                 efficiency {:.2} (gate ≥ {:.1}); the busiest shard took {:.0}% of the commits \
                 (at most {:.0}%) — {}.",
                sc.shards,
                ops(sc.multi_shard_throughput),
                ops(sc.single_shard_throughput),
                sc.efficiency,
                Scaling::GATE,
                100.0 * sc.max_share,
                100.0 * sc.max_share_allowed(),
                if !sc.spread() {
                    "fail: skewed, the table was not spread"
                } else if sc.passes() {
                    "pass"
                } else {
                    "fail"
                }
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
/// result. Relative: 0.20 allows ±20%.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tolerance {
    /// Throughput.
    pub throughput: f64,
    /// Median latency.
    pub p50: f64,
    /// 99th percentile. Tails are noisier, so this is looser.
    pub p99: f64,
}

impl Default for Tolerance {
    /// The documented reproducibility tolerance (docs/bench.md): ±20% throughput and
    /// p50, ±40% p99. p99.9 and max are reported but not checked.
    fn default() -> Self {
        Self::uniform(0.20)
    }
}

impl Tolerance {
    /// `t` for throughput and p50, `2t` for p99.
    pub fn uniform(t: f64) -> Self {
        Self {
            throughput: t,
            p50: t,
            p99: 2.0 * t,
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
}

impl Delta {
    /// Whether this metric is within tolerance (informational metrics always are).
    pub fn ok(&self) -> bool {
        self.allowed.is_none_or(|a| self.change.abs() <= a)
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
                d.allowed
                    .map(|a| format!("±{:.0}%", a * 100.0))
                    .unwrap_or_else(|| "—".into()),
                if d.ok() { "ok" } else { "**FAIL**" }
            );
        }
        for u in &self.unmatched {
            let _ = writeln!(s, "\nOnly in one run: {u}");
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
///     run_secs: 1.0, throughput: tput, p50_ns: 1000, p99_ns: 2000, p999_ns: 3000,
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
    for b in &baseline.results {
        let Some(c) = candidate.results.iter().find(|c| c.key() == b.key()) else {
            unmatched.push(b.key());
            continue;
        };
        let metrics: [(&'static str, f64, f64, Option<f64>); 4] = [
            (
                "throughput",
                b.throughput,
                c.throughput,
                Some(tolerance.throughput),
            ),
            ("p50", b.p50_ns as f64, c.p50_ns as f64, Some(tolerance.p50)),
            ("p99", b.p99_ns as f64, c.p99_ns as f64, Some(tolerance.p99)),
            ("p99.9", b.p999_ns as f64, c.p999_ns as f64, None),
        ];
        for (metric, bv, cv, allowed) in metrics {
            let change = if bv > 0.0 { cv / bv - 1.0 } else { 0.0 };
            deltas.push(Delta {
                key: b.key(),
                metric,
                baseline: bv,
                candidate: cv,
                change,
                allowed,
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
    }
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
            run_secs: 0.1,
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
        let a = suite(vec![rec("ycsb-a", 1e5, 1000, 5000)]);
        for b in [
            rec("ycsb-a", 0.75e5, 1000, 5000),
            rec("ycsb-a", 1e5, 1250, 5000),
            rec("ycsb-a", 1e5, 1000, 7500),
        ] {
            let cmp = compare(&a, &suite(vec![b]), Tolerance::default());
            assert!(!cmp.passes());
            assert!(cmp.to_markdown().contains("FAIL"));
        }
        // p99.9 is informational only.
        let mut b = rec("ycsb-a", 1e5, 1000, 5000);
        b.p999_ns *= 10;
        assert!(compare(&a, &suite(vec![b]), Tolerance::default()).passes());
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
    fn environment_is_never_reference_by_default() {
        let env = Environment::detect(Path::new("."));
        if std::env::var("PHDB_BENCH_REFERENCE").is_err() {
            assert!(!env.reference);
            assert_eq!(env.label, "non-reference (D5)");
        }
        assert!(env.cores >= 1);
    }
}
