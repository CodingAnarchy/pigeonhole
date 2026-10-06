//! `phdb-bench`: runs the Pigeonhole benchmark suite and compares runs.
//!
//! ```text
//! phdb-bench <workload|all|scaling> [options]
//! phdb-bench compare <baseline.json> <candidate.json> [--tolerance 0.15]
//! ```
//!
//! See `docs/bench.md` or `phdb-bench --help`.
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use pigeonhole_bench::{
    Environment, PigeonholeRunner, RunRecord, Runner, Scaling, Suite, Tolerance, WorkloadConfig,
    WorkloadKind, compare, run_detailed,
};

const HELP: &str = "\
phdb-bench: Pigeonhole benchmark suite

USAGE:
    phdb-bench <WORKLOAD|all|scaling> [OPTIONS]
    phdb-bench compare <BASELINE.json> <CANDIDATE.json> [--tolerance T]

WORKLOADS:
    ycsb-a ycsb-b ycsb-c ycsb-d ycsb-e ycsb-f sparse-wide time-series-ttl
    adjacency skewed-multi-shard
    all       every workload above
    scaling   the scaling gate: skewed-multi-shard on Pigeonhole at 1 and N shards

OPTIONS:
    --engine LIST          pigeonhole,rocksdb,sqlite,fjall or all [default: pigeonhole]
                           (comparison engines need the matching cargo feature)
    --scale smoke|small    preset sizes [default: small]
    --records N            rows loaded before measuring
    --ops N                operations measured
    --value-len N          bytes per value
    --threads N            client threads
    --seed N               RNG seed
    --shards N             Pigeonhole shards (scaling: N, default all cores)
    --memtable-budget B    Pigeonhole memtable bytes per shard
    --sync                 fsync every commit on every engine [default: buffered]
    --dir DIR              where stores are created [default: system temp dir]
    --json PATH            write results as JSON
    --markdown PATH        write the markdown summary
    --tolerance T          compare: relative tolerance for throughput and p50; p99
                           gets 2T [default: 0.15]
";

#[derive(Debug, Default)]
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
    shards: Option<usize>,
    memtable_budget: Option<u64>,
    sync: bool,
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
            "--memtable-budget" => a.memtable_budget = Some(parse(&arg, it.next())?),
            "--sync" => a.sync = true,
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
        Some(s) => return Err(format!("unknown scale {s:?} (smoke or small)")),
    };
    c.records = a.records.unwrap_or(c.records);
    c.operations = a.ops.unwrap_or(c.operations);
    c.value_len = a.value_len.unwrap_or(c.value_len);
    c.threads = a.threads.unwrap_or(c.threads);
    c.seed = a.seed.unwrap_or(c.seed);
    Ok(c)
}

fn pigeonhole(a: &Args, shards: Option<usize>) -> PigeonholeRunner {
    let mut r = PigeonholeRunner::default().sync(a.sync);
    if let Some(n) = shards.or(a.shards) {
        r = r.shards(n);
    }
    if let Some(b) = a.memtable_budget {
        r = r.memtable_budget(b);
    }
    r
}

#[cfg(not(all(feature = "rocksdb", feature = "sqlite", feature = "fjall")))]
fn missing(feature: &str) -> Result<Box<dyn Runner>, String> {
    Err(format!(
        "engine {feature} is not compiled in; rebuild with `--features {feature}`"
    ))
}

fn runner(a: &Args, engine: &str) -> Result<Box<dyn Runner>, String> {
    match engine {
        "pigeonhole" => Ok(Box::new(pigeonhole(a, None))),
        #[cfg(feature = "rocksdb")]
        "rocksdb" => Ok(Box::new(
            pigeonhole_bench::RocksDbRunner::default().sync(a.sync),
        )),
        #[cfg(not(feature = "rocksdb"))]
        "rocksdb" => missing("rocksdb"),
        #[cfg(feature = "sqlite")]
        "sqlite" | "sqlite-eav" => Ok(Box::new(
            pigeonhole_bench::SqliteRunner::default().sync(a.sync),
        )),
        #[cfg(not(feature = "sqlite"))]
        "sqlite" | "sqlite-eav" => missing("sqlite"),
        #[cfg(feature = "fjall")]
        "fjall" => Ok(Box::new(
            pigeonhole_bench::FjallRunner::default().sync(a.sync),
        )),
        #[cfg(not(feature = "fjall"))]
        "fjall" => missing("fjall"),
        other => Err(format!("unknown engine {other:?}")),
    }
}

/// Runs one measurement in a fresh directory under `root`, removed afterwards.
fn one(
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
    let result = run_detailed(runner, config, &dir)
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
            let single = one(&root, &mut seq, &mut pigeonhole(a, Some(1)), &c)?;
            let multi = one(&root, &mut seq, &mut pigeonhole(a, Some(n)), &c)?;
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
            for engine in &a.engines {
                let mut r = runner(a, engine)?;
                suite.results.push(one(&root, &mut seq, r.as_mut(), &c)?);
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
        assert_eq!(args("all --engine all").engines.len(), 4);
        assert_eq!(args("").command, "help");
        assert!(parse_args(["--bogus".to_owned()]).is_err());
        assert!(parse_args(["--records".to_owned()]).is_err());
        let c = config(&args("x --scale smoke"), WorkloadKind::YcsbC).unwrap();
        assert_eq!(c, WorkloadConfig::smoke(WorkloadKind::YcsbC));
    }

    #[test]
    fn smoke_suite_end_to_end() {
        let dir = std::env::temp_dir().join(format!("phdb-bench-cli-{}", std::process::id()));
        let a = args(&format!(
            "ycsb-c --scale smoke --shards 1 --memtable-budget 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        assert_eq!(suite.results.len(), 1);
        assert!(suite.to_markdown().contains("| ycsb-c | pigeonhole |"));
        let a = args(&format!(
            "scaling --scale smoke --shards 2 --memtable-budget 16777216 --dir {}",
            dir.display()
        ));
        let suite = bench(&a).unwrap();
        assert_eq!(suite.results.len(), 2);
        assert!(suite.scaling.is_some());
        std::fs::remove_dir_all(&dir).ok();
    }
}
