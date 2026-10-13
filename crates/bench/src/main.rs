//! `phdb-bench`: runs the Pigeonhole benchmark suite and compares runs.
//!
//! ```text
//! phdb-bench <workload|all|scaling> [options]
//! phdb-bench compare <baseline.json> <candidate.json> [--tolerance 0.20]
//! ```
//!
//! See `docs/bench.md` or `phdb-bench --help`.
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use pigeonhole_bench::{
    Environment, GROUP_COMMIT_THREADS, GroupCommit, INLINE_IN_FLIGHT, MemoryBudget,
    PLACEMENT_RATE_PER_SHARD, PLACEMENT_SETTLE_BOUND, PigeonholeRunner, PlacementOptions,
    RunOptions, RunRecord, Runner, Scaling, Suite, Tolerance, WorkloadConfig, WorkloadKind,
    compare, run_detailed,
};

/// Default warmup of `scaling`, as a fraction of the measured ops: as long as the measured
/// writes, so flushes and compactions reach their steady state before measuring (the table
/// is pre-split, D231; before that the balancer also needed about a second of writes to
/// spread it, issue #51).
const SCALING_WARMUP: f64 = 1.0;

const HELP: &str = "\
phdb-bench: Pigeonhole benchmark suite

USAGE:
    phdb-bench <WORKLOAD|all|scaling> [OPTIONS]
    phdb-bench compare <BASELINE.json> <CANDIDATE.json> [--tolerance T]

WORKLOADS:
    ycsb-a ycsb-b ycsb-c ycsb-d ycsb-e ycsb-f sparse-wide time-series-ttl
    adjacency skewed-multi-shard group-commit cross-shard-contention
    all       every workload above
              (group-commit: durable commits, GroupSync on Pigeonhole and fsync on
              the others whatever --sync says; without --threads it runs at 1, 4
              and 16 client threads; cross-shard-contention: 32 clients, half the
              commits across 2-4 of 16 partitions, and on Pigeonhole the same run
              with tablet changes off beside the default, D246)
    scaling   the scaling gate (D204, D231): skewed-multi-shard on Pigeonhole at 1 and N
              shards, application-owned with each shard thread writing inline (16 in
              flight), the table pre-split at the writes' quantiles, the balancer off;
              then, reported only, synchronous clients (4 per shard) at N/2 shards
    placement the balancer placement check (D231): skewed-multi-shard writes offered
              open-loop at a fixed rate over N engine-owned shards; the balancer spreads
              the table, and once settled the busiest shard's share is judged

OPTIONS:
    --engine LIST          pigeonhole,rocksdb,sqlite,fjall or all [default: pigeonhole]
                           (comparison engines need the matching cargo feature)
    --scale smoke|small|full|larger-than-ram
                           preset sizes [default: small]; full is the spec's 1M rows;
                           larger-than-ram is full with an 8 MiB write buffer and a
                           16 MiB cache (unless --write-buffer/--cache say otherwise),
                           so the data set is tens of times the memory budget, and the
                           report splits gets into cold (first touch) and hot
    --records N            rows loaded before measuring
    --ops N                operations measured
    --value-len N          bytes per value
    --threads N            client threads
    --seed N               RNG seed
    --warmup F             unrecorded warmup, as a fraction of --ops, on the measured
                           client threads and repeated until the tablet placement has
                           not changed for 2 s, at most 30 s (D242) [default: 0.05;
                           scaling: 1.0]
    --min-secs S           measure at least S seconds: inline runs (scaling,
                           group-commit's D213 run) repeat the measured writes (D219),
                           client runs go on with the workload's next operations (D243)
                           [default: 0, the configured operations once]; placement: the
                           measured window [default: 10]
    --offered-rate R       placement: writes per second offered over every client
                           [default: 30,000 per shard]
    --inline-ops N         group-commit: the D213 run's commits (D219) [default: --ops]
    --in-flight N          inline runs: commits each shard's thread keeps in flight
                           [default: 16, the judged shape]; fewer trade throughput for
                           latency (the gate reports 4 and 8 beside 16, D226)
    --shards N             Pigeonhole shards (scaling: N; default one per physical core)
    --write-buffer B       every engine's write buffer (Pigeonhole: memtable bytes per
                           shard) [default: 64 MiB]
    --cache B              every engine's read cache (SQLite: page cache gets
                           write buffer + cache) [default: 256 MiB]
    --sync                 fsync every commit on every engine [default: buffered]
    --no-tablet-changes    Pigeonhole: keep each table one tablet on one shard (tablets
                           split, merge and move between shards by default)
    --no-packing           Pigeonhole: spread tablets over every shard even under light
                           writes (D217's packing is on by default)
    --row-cache B          Pigeonhole: row cache bytes; 0 turns it off (D201)
                           [default: the library's, on (D216)]
    --shard-spin US        Pigeonhole: a shard's idle spin before it parks, in µs (D198)
                           [default: the library's]
    --dir DIR              where stores are created [default: system temp dir]
    --json PATH            write results as JSON
    --markdown PATH        write the markdown summary
    --tolerance T          compare: relative tolerance for throughput and p50; p99
                           gets 2T [default: 0.20]
";

#[derive(Debug, Default, Clone)]
struct Args {
    command: String,
    positional: Vec<String>,
    engines: Vec<String>,
    scale: Option<String>,
    records: Option<u64>,
    ops: Option<u64>,
    value_len: Option<usize>,
    threads: Option<usize>,
    seed: Option<u64>,
    warmup: Option<f64>,
    min_secs: Option<f64>,
    inline_ops: Option<u64>,
    in_flight: Option<usize>,
    offered_rate: Option<f64>,
    shards: Option<usize>,
    write_buffer: Option<u64>,
    cache: Option<u64>,
    sync: bool,
    no_tablet_changes: bool,
    no_packing: bool,
    row_cache: Option<u64>,
    shard_spin: Option<u64>,
    dir: Option<PathBuf>,
    json: Option<PathBuf>,
    markdown: Option<PathBuf>,
    tolerance: Option<f64>,
}

fn parse<T: std::str::FromStr>(flag: &str, v: Option<String>) -> Result<T, String> {
    let v = v.ok_or_else(|| format!("{flag} needs a value"))?;
    v.parse().map_err(|_| format!("{flag}: cannot parse {v:?}"))
}

fn parse_args(raw: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut it = raw.into_iter();
    let mut a = Args::default();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                a.command = "help".into();
                return Ok(a);
            }
            "--engine" => {
                let v: String = parse(&arg, it.next())?;
                a.engines = v.split(',').map(str::to_owned).collect();
            }
            "--scale" => a.scale = Some(parse(&arg, it.next())?),
            "--records" => a.records = Some(parse(&arg, it.next())?),
            "--ops" => a.ops = Some(parse(&arg, it.next())?),
            "--value-len" => a.value_len = Some(parse(&arg, it.next())?),
            "--threads" => a.threads = Some(parse(&arg, it.next())?),
            "--seed" => a.seed = Some(parse(&arg, it.next())?),
            "--shards" => a.shards = Some(parse(&arg, it.next())?),
            "--warmup" => a.warmup = Some(parse(&arg, it.next())?),
            "--min-secs" => a.min_secs = Some(parse(&arg, it.next())?),
            "--inline-ops" => a.inline_ops = Some(parse(&arg, it.next())?),
            "--in-flight" => {
                let n: usize = parse(&arg, it.next())?;
                if n == 0 {
                    return Err("--in-flight must be at least 1".to_owned());
                }
                a.in_flight = Some(n);
            }
            "--offered-rate" => a.offered_rate = Some(parse(&arg, it.next())?),
            "--write-buffer" | "--memtable-budget" => {
                a.write_buffer = Some(parse(&arg, it.next())?)
            }
            "--cache" => a.cache = Some(parse(&arg, it.next())?),
            "--sync" => a.sync = true,
            "--no-tablet-changes" => a.no_tablet_changes = true,
            "--no-packing" => a.no_packing = true,
            "--row-cache" => a.row_cache = Some(parse(&arg, it.next())?),
            "--shard-spin" => a.shard_spin = Some(parse(&arg, it.next())?),
            // The default since tablet changes turned on (#38); still accepted.
            "--tablet-changes" => a.no_tablet_changes = false,
            "--dir" => a.dir = Some(parse(&arg, it.next())?),
            "--json" => a.json = Some(parse(&arg, it.next())?),
            "--markdown" => a.markdown = Some(parse(&arg, it.next())?),
            "--tolerance" => a.tolerance = Some(parse(&arg, it.next())?),
            s if s.starts_with("--") => return Err(format!("unknown option {s}")),
            _ if a.command.is_empty() => a.command = arg,
            _ => a.positional.push(arg),
        }
    }
    if a.command.is_empty() {
        a.command = "help".into();
    }
    if a.engines.is_empty() {
        a.engines = vec!["pigeonhole".into()];
    }
    if a.engines.iter().any(|e| e == "all") {
        a.engines = ["pigeonhole", "rocksdb", "sqlite", "fjall"]
            .map(String::from)
            .to_vec();
    }
    Ok(a)
}

fn config(a: &Args, kind: WorkloadKind) -> Result<WorkloadConfig, String> {
    let mut c = match a.scale.as_deref() {
        None | Some("small") => WorkloadConfig::small(kind),
        Some("smoke") => WorkloadConfig::smoke(kind),
        Some("full") => WorkloadConfig::full(kind),
        Some("larger-than-ram") => WorkloadConfig::larger_than_ram(kind),
        Some(s) => {
            return Err(format!(
                "unknown scale {s:?} (smoke, small, full or larger-than-ram)"
            ));
        }
    };
    c.records = a.records.unwrap_or(c.records);
    c.operations = a.ops.unwrap_or(c.operations);
    c.value_len = a.value_len.unwrap_or(c.value_len);
    c.threads = a.threads.unwrap_or(c.threads);
    c.seed = a.seed.unwrap_or(c.seed);
    Ok(c)
}

fn memory(a: &Args) -> MemoryBudget {
    let d = if a.scale.as_deref() == Some("larger-than-ram") {
        MemoryBudget::larger_than_ram()
    } else {
        MemoryBudget::default()
    };
    MemoryBudget {
        write_buffer: a.write_buffer.unwrap_or(d.write_buffer),
        cache: a.cache.unwrap_or(d.cache),
    }
}

fn pigeonhole(a: &Args, shards: Option<usize>) -> PigeonholeRunner {
    let mut r = PigeonholeRunner::default()
        .sync(a.sync)
        .memory(memory(a))
        .tablet_changes(!a.no_tablet_changes)
        .packing(!a.no_packing);
    if let Some(bytes) = a.row_cache {
        r = r.row_cache(bytes);
    }
    if let Some(n) = shards.or(a.shards) {
        r = r.shards(n);
    }
    if let Some(us) = a.shard_spin {
        r = r.shard_spin_micros(us);
    }
    r
}

#[cfg(not(all(feature = "rocksdb", feature = "sqlite", feature = "fjall")))]
fn missing(feature: &str) -> Result<Box<dyn Runner>, String> {
    Err(format!(
        "engine {feature} is not compiled in; rebuild with `--features {feature}`"
    ))
}

/// The runner for `engine`. [`WorkloadKind::GroupCommit`] commits durably: `GroupSync` on
/// Pigeonhole, fsync on every write elsewhere (RocksDB's concurrent writers then share a
/// write group, its group commit).
fn runner(a: &Args, engine: &str, kind: WorkloadKind) -> Result<Box<dyn Runner>, String> {
    let group = kind == WorkloadKind::GroupCommit;
    let a = &Args {
        sync: a.sync || group,
        ..a.clone()
    };
    match engine {
        "pigeonhole" => Ok(Box::new(pigeonhole(a, None).group_sync(group))),
        #[cfg(feature = "rocksdb")]
        "rocksdb" => Ok(Box::new(
            pigeonhole_bench::RocksDbRunner::default()
                .sync(a.sync)
                .memory(memory(a)),
        )),
        #[cfg(not(feature = "rocksdb"))]
        "rocksdb" => missing("rocksdb"),
        #[cfg(feature = "sqlite")]
        "sqlite" | "sqlite-eav" => Ok(Box::new(
            pigeonhole_bench::SqliteRunner::default()
                .sync(a.sync)
                .memory(memory(a)),
        )),
        #[cfg(not(feature = "sqlite"))]
        "sqlite" | "sqlite-eav" => missing("sqlite"),
        #[cfg(feature = "fjall")]
        "fjall" => Ok(Box::new(
            pigeonhole_bench::FjallRunner::default()
                .sync(a.sync)
                .memory(memory(a)),
        )),
        #[cfg(not(feature = "fjall"))]
        "fjall" => missing("fjall"),
        other => Err(format!("unknown engine {other:?}")),
    }
}

/// Runs one measurement in a fresh directory under `root`, removed afterwards.
/// One inline run of the scaling gate (D204) in a fresh directory.
fn one_inline(
    a: &Args,
    root: &Path,
    seq: &mut u32,
    runner: &PigeonholeRunner,
    config: &WorkloadConfig,
    pin: bool,
) -> Result<RunRecord, String> {
    *seq += 1;
    let dir = root.join(format!("{}-inline-{}", config.kind.name(), *seq));
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    eprintln!(
        "running {} inline on pigeonhole ({}) ...",
        config.kind.name(),
        runner.describe()
    );
    let options = RunOptions {
        warmup: a.warmup.unwrap_or(SCALING_WARMUP),
        min_measured: std::time::Duration::from_secs_f64(a.min_secs.unwrap_or(0.0).max(0.0)),
        in_flight: a.in_flight.unwrap_or(INLINE_IN_FLIGHT),
        pin,
    };
    let result = runner
        .run_inline(config, &dir, &options)
        .map_err(|e| format!("{} inline: {e}", config.kind.name()));
    let _ = std::fs::remove_dir_all(&dir);
    result
}

fn one(
    a: &Args,
    root: &Path,
    seq: &mut u32,
    runner: &mut dyn Runner,
    config: &WorkloadConfig,
) -> Result<RunRecord, String> {
    *seq += 1;
    let dir = root.join(format!("{}-{}-{}", config.kind.name(), runner.name(), *seq));
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    eprintln!(
        "running {} on {} ({}) ...",
        config.kind.name(),
        runner.name(),
        runner.describe()
    );
    let options = RunOptions {
        warmup: a.warmup.unwrap_or(if a.command == "scaling" {
            SCALING_WARMUP
        } else {
            RunOptions::default().warmup
        }),
        min_measured: std::time::Duration::from_secs_f64(a.min_secs.unwrap_or(0.0).max(0.0)),
        ..RunOptions::default()
    };
    let result = run_detailed(runner, config, &dir, &options)
        .map_err(|e| format!("{} on {}: {e}", config.kind.name(), runner.name()));
    let _ = std::fs::remove_dir_all(&dir);
    result
}

fn bench(a: &Args) -> Result<Suite, String> {
    let root = a
        .dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("phdb-bench-{}", std::process::id())));
    std::fs::create_dir_all(&root).map_err(|e| format!("{}: {e}", root.display()))?;
    let mut suite = Suite::new(Environment::detect(&root));
    if suite.environment.profile == "debug" {
        eprintln!("warning: debug build; numbers are meaningless. Use --release.");
    }
    let mut seq = 0;
    let result = (|| -> Result<(), String> {
        if a.command == "placement" {
            let n = a.shards.unwrap_or_else(pigeonhole_io::sys::default_shards);
            let c = config(a, WorkloadKind::SkewedMultiShard)?;
            let runner = pigeonhole(a, Some(n));
            seq += 1;
            let dir = root.join(format!("placement-{seq}"));
            std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
            let opts = PlacementOptions {
                offered_rate: a
                    .offered_rate
                    .unwrap_or(PLACEMENT_RATE_PER_SHARD * n as f64),
                clients: a.threads.unwrap_or(n.max(2)),
                settle_bound: PLACEMENT_SETTLE_BOUND,
                window: std::time::Duration::from_secs_f64(a.min_secs.unwrap_or(10.0).max(1.0)),
            };
            eprintln!(
                "running the placement check on pigeonhole ({}), {:.0} writes/s offered ...",
                runner.describe(),
                opts.offered_rate
            );
            let p = runner.run_placement(&c, &dir, &opts);
            let _ = std::fs::remove_dir_all(&dir);
            suite.placement = Some(p?);
            return Ok(());
        }
        if a.command == "scaling" {
            // The gate (D204): application-owned shards, each thread writing its share
            // inline, at 1 shard and at N (default: one per physical core, D220). D231: the
            // table is split at the measured writes' quantiles up front and the balancer stays
            // off, so the run measures per-shard throughput; placement is judged on its own
            // (`placement`).
            let n = a.shards.unwrap_or_else(pigeonhole_io::sys::default_shards);
            let c = config(a, WorkloadKind::SkewedMultiShard)?;
            let gate = |shards| pigeonhole(a, Some(shards)).presplit_writes(true);
            // D235: judged with each shard's thread pinned to a CPU of its own (D220's
            // order), as a thread-per-core application runs; the same runs unpinned are
            // reported beside it.
            let single = one_inline(a, &root, &mut seq, &gate(1), &c, true)?;
            let multi = one_inline(a, &root, &mut seq, &gate(n), &c, true)?;
            suite.scaling = Some(Scaling::new(n, &single, &multi));
            suite.results.extend([single, multi]);
            let single = one_inline(a, &root, &mut seq, &gate(1), &c, false)?;
            let multi = one_inline(a, &root, &mut seq, &gate(n), &c, false)?;
            suite.scaling_unpinned = Some(Scaling::new(n, &single, &multi));
            suite.results.extend([single, multi]);
            // Reported beside it, not gating (D204): engine-owned with synchronous clients,
            // four per shard, at half the shards, so the clients have cores of their own.
            let half = (n / 2).max(1);
            let mut c = c;
            c.threads = a.threads.unwrap_or(4 * half);
            let single = one(a, &root, &mut seq, &mut pigeonhole(a, Some(1)), &c)?;
            let multi = one(a, &root, &mut seq, &mut pigeonhole(a, Some(half)), &c)?;
            suite.scaling_sync = Some(Scaling::new(half, &single, &multi));
            suite.results.extend([single, multi]);
            return Ok(());
        }
        let kinds: Vec<WorkloadKind> = if a.command == "all" {
            WorkloadKind::ALL.to_vec()
        } else {
            vec![a.command.parse()?]
        };
        for kind in kinds {
            let c = config(a, kind)?;
            if kind == WorkloadKind::GroupCommit && a.engines.iter().any(|e| e == "pigeonhole") {
                // The target's shape (D213): one shard per core (physical, D220), the table
                // split over all of them, each shard's thread keeping commits in flight inline
                // (D204's driver). The client-thread sweep below stays as a secondary report.
                let cores = pigeonhole_io::sys::default_shards();
                let n = a.shards.unwrap_or(cores);
                let r = pigeonhole(a, Some(n)).group_sync(true).presplit(true);
                let inline = WorkloadConfig {
                    operations: a.inline_ops.unwrap_or(c.operations),
                    ..c.clone()
                };
                // D235: judged pinned, reported unpinned.
                let run = one_inline(a, &root, &mut seq, &r, &inline, true)?;
                let in_flight = a.in_flight.unwrap_or(INLINE_IN_FLIGHT);
                suite.group_commit = Some(GroupCommit::new(n, cores, in_flight, &run));
                suite.results.push(run);
                let run = one_inline(a, &root, &mut seq, &r, &inline, false)?;
                suite.group_commit_unpinned = Some(GroupCommit::new(n, cores, in_flight, &run));
                suite.results.push(run);
            }
            let threads = match (kind, a.threads) {
                (WorkloadKind::GroupCommit, None) => GROUP_COMMIT_THREADS.to_vec(),
                _ => vec![c.threads],
            };
            for engine in &a.engines {
                for &t in &threads {
                    let c = WorkloadConfig {
                        threads: t,
                        ..c.clone()
                    };
                    let mut r = runner(a, engine, kind)?;
                    suite.results.push(one(a, &root, &mut seq, r.as_mut(), &c)?);
                    // D246: the default (tablet changes on) is judged against the same build
                    // with tablet changes off, its partitions one per shard.
                    if kind == WorkloadKind::CrossShardContention
                        && engine == "pigeonhole"
                        && !a.no_tablet_changes
                    {
                        let mut off = pigeonhole(a, None).tablet_changes(false);
                        suite.results.push(one(a, &root, &mut seq, &mut off, &c)?);
                    }
                }
            }
        }
        Ok(())
    })();
    if a.dir.is_none() {
        let _ = std::fs::remove_dir_all(&root);
    }
    result.map(|()| suite)
}

fn write(path: &Option<PathBuf>, contents: &str) -> Result<(), String> {
    if let Some(p) = path {
        std::fs::write(p, contents).map_err(|e| format!("{}: {e}", p.display()))?;
        eprintln!("wrote {}", p.display());
    }
    Ok(())
}

fn read_suite(path: &str) -> Result<Suite, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    Suite::from_json(&s).map_err(|e| format!("{path}: {e}"))
}

fn main_inner() -> Result<ExitCode, String> {
    let a = parse_args(std::env::args().skip(1))?;
    match a.command.as_str() {
        "help" => {
            print!("{HELP}");
            Ok(ExitCode::SUCCESS)
        }
        "compare" => {
            let [base, cand] = a.positional.as_slice() else {
                return Err("compare needs two JSON files".into());
            };
            let tolerance = a
                .tolerance
                .map_or_else(Tolerance::default, Tolerance::uniform);
            let cmp = compare(&read_suite(base)?, &read_suite(cand)?, tolerance);
            let md = cmp.to_markdown();
            print!("{md}");
            write(&a.markdown, &md)?;
            Ok(if cmp.passes() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        _ => {
            let suite = bench(&a)?;
            let md = suite.to_markdown();
            print!("{md}");
            write(&a.json, &suite.to_json())?;
            write(&a.markdown, &md)?;
            // D213: a group-commit result measured while tablets changed is not the target's
            // measurement. The files are written; the run still fails.
            if let Some(gc) = suite.group_commit.as_ref().filter(|g| g.tablet_changes > 0) {
                eprintln!(
                    "phdb-bench: {} tablet changes during the D213 group-commit measurement; \
                     the placement must stay fixed",
                    gc.tablet_changes
                );
                return Ok(ExitCode::from(1));
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn main() -> ExitCode {
    match main_inner() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("phdb-bench: {e}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Args {
        parse_args(s.split_whitespace().map(String::from)).unwrap()
    }

    #[test]
    fn parses_options() {
        let a =
            args("ycsb-a --engine pigeonhole,sqlite --records 10 --ops 20 --sync --json o.json");
        assert_eq!(a.command, "ycsb-a");
        assert_eq!(a.engines, ["pigeonhole", "sqlite"]);
        let c = config(&a, WorkloadKind::YcsbA).unwrap();
        assert_eq!((c.records, c.operations), (10, 20));
        assert!(a.sync);
        assert!(!a.no_tablet_changes);
        assert!(!args("scaling --tablet-changes").no_tablet_changes);
        assert!(args("scaling --no-tablet-changes").no_tablet_changes);
        assert_eq!(args("all --engine all").engines.len(), 4);
        assert_eq!(args("").command, "help");
        let rc = args("ycsb-c --row-cache 1048576");
        assert_eq!(rc.row_cache, Some(1 << 20));
        assert_eq!(args("ycsb-a --shard-spin 200").shard_spin, Some(200));
        assert!(pigeonhole(&rc, None).describe().contains(" rowcache=1MiB"));
        assert!(
            !pigeonhole(&args("ycsb-c"), None)
                .describe()
                .contains("rowcache")
        );
        assert!(parse_args(["--bogus".to_owned()]).is_err());
        assert!(parse_args(["--records".to_owned()]).is_err());
        let c = config(&args("x --scale smoke"), WorkloadKind::YcsbC).unwrap();
        assert_eq!(c, WorkloadConfig::smoke(WorkloadKind::YcsbC));
        let c = config(&args("x --scale full"), WorkloadKind::SparseWide).unwrap();
        assert_eq!((c.records, c.operations), (1_000_000, 1_000_000));
        assert!(config(&args("x --scale huge"), WorkloadKind::YcsbC).is_err());
        // The larger-than-ram preset shrinks the memory budget unless flags override it.
        let a = args("ycsb-c --scale larger-than-ram");
        assert_eq!(memory(&a), MemoryBudget::larger_than_ram());
        let c = config(&a, WorkloadKind::YcsbC).unwrap();
        assert_eq!(c, WorkloadConfig::larger_than_ram(WorkloadKind::YcsbC));
        let a = args("ycsb-c --scale larger-than-ram --cache 1048576");
        assert_eq!(memory(&a).cache, 1 << 20);
        assert_eq!(memory(&a).write_buffer, 8 << 20);
        assert_eq!(memory(&args("ycsb-c")), MemoryBudget::default());
    }

    #[test]
    fn placement_smoke_reports_the_check() {
        // D231: the open-loop placement check runs end to end and reports its record.
        let dir = std::env::temp_dir().join(format!("phdb-bench-placement-{}", std::process::id()));
        let a = args(&format!(
            "placement --scale smoke --shards 2 --offered-rate 40000 --min-secs 1 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        let p = suite.placement.clone().expect("a placement record");
        assert_eq!(p.shards, 2);
        assert_eq!(p.shares.len(), 2);
        assert!(p.achieved_rate > 0.0);
        assert!(suite.to_markdown().contains("Balancer placement (D231)"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cross_shard_contention_runs_tablets_off_beside_the_default() {
        // D246: the default is judged against the same build with tablet changes off.
        let dir = std::env::temp_dir().join(format!("phdb-bench-xs-{}", std::process::id()));
        let a = args(&format!(
            "cross-shard-contention --scale smoke --shards 4 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        let configs: Vec<&str> = suite
            .results
            .iter()
            .map(|r| r.store_config.as_str())
            .collect();
        assert_eq!(configs.len(), 2, "{configs:?}");
        assert!(configs[0].contains("tablets=on") && !configs[1].contains("tablets=on"));
        assert!(suite.results.iter().all(|r| r.rounds().is_some()));
        assert!(suite.to_markdown().contains("Per cross-shard commit"));
        let a = args(&format!(
            "cross-shard-contention --scale smoke --shards 4 --no-tablet-changes --dir {}",
            dir.display()
        ));
        assert_eq!(
            bench(&a).unwrap().results.len(),
            1,
            "no second tablets-off run"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn smoke_suite_end_to_end() {
        let dir = std::env::temp_dir().join(format!("phdb-bench-cli-{}", std::process::id()));
        let a = args(&format!(
            "ycsb-c --scale smoke --shards 1 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        assert_eq!(suite.results.len(), 1);
        assert!(suite.to_markdown().contains("| ycsb-c | pigeonhole |"));
        // Pigeonhole reports what its measured phase stalled on (ICR 0015).
        assert!(suite.results[0].detail.stalls.is_some());
        assert!(suite.to_markdown().contains("| Inline WAL syncs |"));
        let a = args(&format!(
            "scaling --scale smoke --shards 2 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        // The gate's inline runs at 1 and 2 shards (D204), pinned and judged, then unpinned
        // and reported (D235), then the engine-owned synchronous runs it reports at 1 and
        // 2 / 2 = 1 shards.
        assert_eq!(suite.results.len(), 6);
        assert!(suite.scaling.is_some());
        assert!(suite.scaling_unpinned.is_some());
        assert!(suite.scaling_sync.is_some());
        assert!(
            suite.results[..4]
                .iter()
                .all(|r| r.store_config.contains("inline"))
        );
        // Pinned where the platform can (unpinned with the reason elsewhere), then unpinned.
        assert!(
            suite.results[..2]
                .iter()
                .all(|r| r.store_config.contains(" pinned")
                    || r.store_config.contains(" unpinned ("))
        );
        assert!(
            suite.results[2..4]
                .iter()
                .all(|r| r.store_config.ends_with(" unpinned"))
        );
        assert!(
            suite
                .to_markdown()
                .contains("shard threads unpinned (reported, not judged")
        );
        assert!(suite.to_markdown().contains("reported, not gating"));
        assert!(suite.to_markdown().contains("the busiest shard took"));
        assert!(
            suite
                .results
                .iter()
                .all(|r| r.store_config.contains("tablets=on"))
        );
        // Every measured put is one commit on one shard; the report shows where they went.
        for (r, shards) in suite.results.iter().zip([1, 2, 1, 2, 1, 1]) {
            assert_eq!(
                r.warmup_ops, r.operations,
                "scaling warms up as long as it measures"
            );
            assert_eq!(r.detail.shards.len(), shards);
            let commits: u64 = r.detail.shards.iter().map(|s| s.commits).sum();
            assert_eq!(commits, r.operations);
            assert!(r.detail.shards.iter().map(|s| s.tablets_end).sum::<u64>() >= 1);
            // The visibility counters (ICR 0023): a waiter is woken at most once.
            let st = r.detail.stalls.expect("pigeonhole reports its counters");
            assert!(st.visibility_woken <= st.visibility_waits, "{st:?}");
        }
        assert!(suite.to_markdown().contains("| Shard | Commits | Share |"));
        assert!(
            suite
                .to_markdown()
                .contains("visibility waits per operation")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn group_commit_sweeps_threads_durably() {
        let dir = std::env::temp_dir().join(format!("phdb-bench-gc-{}", std::process::id()));
        let a = args(&format!(
            "group-commit --scale smoke --ops 64 --shards 2 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        // First the D213 shape: the table split over both shards, commits in flight inline,
        // pinned (judged), then the same unpinned (reported, D235).
        let (gate, rest) = suite.results.split_first().unwrap();
        let (unpinned, clients) = rest.split_first().unwrap();
        assert!(
            unpinned
                .store_config
                .ends_with(" inline in-flight=16 unpinned")
        );
        assert!(suite.group_commit_unpinned.is_some());
        assert!(
            gate.store_config.contains(" presplit inline"),
            "{}",
            gate.store_config
        );
        assert_eq!(gate.threads, 2);
        // The placement stays fixed: one tablet per shard, and no change while measuring.
        assert!(
            gate.detail
                .shards
                .iter()
                .all(|s| (s.tablets_start, s.tablets_end) == (1, 1))
        );
        let gc = suite.group_commit.as_ref().expect("the D213 result");
        assert_eq!(gc.shards, 2);
        assert_eq!(gc.tablet_changes, 0);
        let engine = gc.engine.expect("the engine's commit latency");
        assert_eq!(engine.durability, 2, "group sync");
        assert!(engine.commits > 0 && engine.p50_ns <= engine.p99_ns);
        assert!(gc.cells_per_sec > 0.0);
        assert!(suite.to_markdown().contains("Durable group commit (D213)"));
        // Then the client-thread sweep.
        let threads: Vec<usize> = clients.iter().map(|r| r.threads).collect();
        assert_eq!(threads, GROUP_COMMIT_THREADS);
        assert!(
            suite
                .results
                .iter()
                .all(|r| r.store_config.contains(" group-sync"))
        );
        // `--threads` picks one count for the sweep.
        let a = args(&format!(
            "group-commit --scale smoke --ops 64 --threads 2 --shards 2 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        // The D213 run pinned and unpinned (D235), then the one client run.
        assert_eq!(bench(&a).unwrap().results.len(), 3);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn inline_runs_take_their_own_length() {
        // D219: `--inline-ops` sets the D213 run's commits apart from the client runs', and
        // `--min-secs` repeats an inline run's measured writes until that long is measured.
        let dir = std::env::temp_dir().join(format!("phdb-bench-len-{}", std::process::id()));
        let a = args(&format!(
            "group-commit --scale smoke --ops 64 --inline-ops 256 --threads 1 --shards 2 \
             --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        assert_eq!((a.inline_ops, a.min_secs), (Some(256), None));
        let suite = bench(&a).unwrap();
        let ops: Vec<u64> = suite.results.iter().map(|r| r.operations).collect();
        assert_eq!(
            ops,
            [256, 256, 64],
            "the D213 run pinned and unpinned, then one client run"
        );
        assert!(suite.group_commit.as_ref().is_some_and(|g| g.judged()));
        // D226: `--in-flight` sets the commits each shard keeps in flight; a run at fewer
        // than 16 is reported, not judged.
        let a = args(&format!(
            "group-commit --scale smoke --ops 64 --inline-ops 256 --threads 1 --shards 2 \
             --in-flight 4 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        let gc = suite.group_commit.as_ref().expect("the D213 result");
        assert_eq!(gc.in_flight, 4);
        assert!(!gc.judged() && !gc.passes());
        assert!(suite.results[0].store_config.contains("in-flight=4 "));
        assert!(
            suite
                .to_markdown()
                .contains("4 commits in flight per shard")
        );
        let a = args(&format!(
            "scaling --scale smoke --ops 64 --shards 2 --min-secs 0.2 --write-buffer 16777216 \
             --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        let inline: Vec<&RunRecord> = suite
            .results
            .iter()
            .filter(|r| r.store_config.contains("inline"))
            .collect();
        assert_eq!(
            inline.len(),
            4,
            "one shard, then two; pinned, then unpinned (D235)"
        );
        for r in inline {
            assert!(r.run_secs >= 0.2, "{} s measured", r.run_secs);
            assert!(
                r.operations > 64 && r.operations % 64 == 0,
                "{} ops",
                r.operations
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
