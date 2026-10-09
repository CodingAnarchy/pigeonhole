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
    Environment, GROUP_COMMIT_THREADS, MemoryBudget, PigeonholeRunner, RunOptions, RunRecord,
    Runner, Scaling, Suite, Tolerance, WorkloadConfig, WorkloadKind, compare, run_detailed,
};

/// Default warmup of `scaling`, as a fraction of the measured ops. The balancer needs about
/// a second of writes to split the table over the shards (issue #51); the 5% default
/// (10,000 ops at `small`) ends before the first split, so the run would measure one tablet.
const SCALING_WARMUP: f64 = 1.0;

const HELP: &str = "\
phdb-bench: Pigeonhole benchmark suite

USAGE:
    phdb-bench <WORKLOAD|all|scaling> [OPTIONS]
    phdb-bench compare <BASELINE.json> <CANDIDATE.json> [--tolerance T]

WORKLOADS:
    ycsb-a ycsb-b ycsb-c ycsb-d ycsb-e ycsb-f sparse-wide time-series-ttl
    adjacency skewed-multi-shard group-commit
    all       every workload above
              (group-commit: durable commits, GroupSync on Pigeonhole and fsync on
              the others whatever --sync says; without --threads it runs at 1, 4
              and 16 client threads)
    scaling   the scaling gate: skewed-multi-shard on Pigeonhole at 1 and N shards

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
    --warmup F             unrecorded warmup, as a fraction of --ops [default: 0.05;
                           scaling: 1.0, so the balancer spreads the table first]
    --shards N             Pigeonhole shards (scaling: N, default all cores)
    --write-buffer B       every engine's write buffer (Pigeonhole: memtable bytes per
                           shard) [default: 64 MiB]
    --cache B              every engine's read cache (SQLite: page cache gets
                           write buffer + cache) [default: 256 MiB]
    --sync                 fsync every commit on every engine [default: buffered]
    --no-tablet-changes    Pigeonhole: keep each table one tablet on one shard (tablets
                           split, merge and move between shards by default)
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
    shards: Option<usize>,
    write_buffer: Option<u64>,
    cache: Option<u64>,
    sync: bool,
    no_tablet_changes: bool,
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
            "--write-buffer" | "--memtable-budget" => {
                a.write_buffer = Some(parse(&arg, it.next())?)
            }
            "--cache" => a.cache = Some(parse(&arg, it.next())?),
            "--sync" => a.sync = true,
            "--no-tablet-changes" => a.no_tablet_changes = true,
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
        .tablet_changes(!a.no_tablet_changes);
    if let Some(n) = shards.or(a.shards) {
        r = r.shards(n);
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
        if a.command == "scaling" {
            let n = a.shards.unwrap_or(suite.environment.cores);
            let mut c = config(a, WorkloadKind::SkewedMultiShard)?;
            c.threads = a.threads.unwrap_or(n);
            let single = one(a, &root, &mut seq, &mut pigeonhole(a, Some(1)), &c)?;
            let multi = one(a, &root, &mut seq, &mut pigeonhole(a, Some(n)), &c)?;
            suite.scaling = Some(Scaling::new(n, &single, &multi));
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
    fn smoke_suite_end_to_end() {
        let dir = std::env::temp_dir().join(format!("phdb-bench-cli-{}", std::process::id()));
        let a = args(&format!(
            "ycsb-c --scale smoke --shards 1 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        assert_eq!(suite.results.len(), 1);
        assert!(suite.to_markdown().contains("| ycsb-c | pigeonhole |"));
        let a = args(&format!(
            "scaling --scale smoke --shards 2 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        assert_eq!(suite.results.len(), 2);
        assert!(suite.scaling.is_some());
        assert!(
            suite
                .results
                .iter()
                .all(|r| r.store_config.contains("tablets=on"))
        );
        // Every measured put is one commit on one shard; the report shows where they went.
        for (r, shards) in suite.results.iter().zip([1, 2]) {
            assert_eq!(
                r.warmup_ops, r.operations,
                "scaling warms up as long as it measures"
            );
            assert_eq!(r.detail.shards.len(), shards);
            let commits: u64 = r.detail.shards.iter().map(|s| s.commits).sum();
            assert_eq!(commits, r.operations);
            assert!(r.detail.shards.iter().map(|s| s.tablets_end).sum::<u64>() >= 1);
        }
        assert!(suite.to_markdown().contains("| Shard | Commits | Share |"));
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
        let threads: Vec<usize> = suite.results.iter().map(|r| r.threads).collect();
        assert_eq!(threads, GROUP_COMMIT_THREADS);
        assert!(
            suite
                .results
                .iter()
                .all(|r| r.store_config.contains(" group-sync"))
        );
        // `--threads` picks one count.
        let a = args(&format!(
            "group-commit --scale smoke --ops 64 --threads 2 --shards 2 --write-buffer 16777216 --dir {}",
            dir.display()
        ));
        assert_eq!(bench(&a).unwrap().results.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
