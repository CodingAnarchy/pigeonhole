//! candle's process-wide pool for `--embed-local` (#2061, D318).
//!
//! candle 0.11 runs large element-wise steps on a pool of its own threads, sized by
//! `CANDLE_NUM_THREADS` (every core by default) and read once from the environment. Those
//! threads aren't the embedder's: `LoadOptions::threads` doesn't count them and
//! `on_thread_start` doesn't place them, so they could run on the shards' CPUs. With
//! `CANDLE_NUM_THREADS=1` the pool has no workers: its steps run on the calling thread, an
//! embedder thread, which is counted and placed. On Linux, candle also re-reads
//! `/proc/cpuinfo` on every matrix product unless `CANDLE_CPU_MASK` (or `RAYON_NUM_THREADS`)
//! is set (huggingface/candle#4070): the mask, the process's own CPUs, takes its cached path
//! and places nothing (with no workers there is nothing to place).
//!
//! The variables must be in the environment before the process starts any thread, and
//! setting them in a running process needs `unsafe` (`std::env::set_var`), which this crate
//! forbids. So `phdb`, run with `--embed-local` and either one unset, runs itself again at
//! once with them added ([`reexec`]): before any thread, file or database is opened. A
//! variable already in the environment is kept, and the startup line says which.
use std::ffi::OsString;

/// The marker the re-exec adds: the variables phdb set (`CANDLE_NUM_THREADS=1 ...`), so the
/// second run neither runs itself again nor reports them as the operator's.
pub const SET_BY_PHDB: &str = "PHDB_CANDLE_SET";

/// Whether `args` (without the program name) ask for the local embedder: `--embed-local`
/// before any `--`. A plain flag of `phdb serve` and `phdb mcp`, never given a value.
pub fn wants_local(args: &[OsString]) -> bool {
    args.iter()
        .take_while(|a| a.as_os_str() != "--")
        .any(|a| a.as_os_str() == "--embed-local")
}

/// The variables to add, each one `env` doesn't have: `CANDLE_NUM_THREADS=1`, and
/// `CANDLE_CPU_MASK` set to `cpus` (the process's CPUs as a list) when there is one.
pub fn missing(
    env: &dyn Fn(&str) -> Option<OsString>,
    cpus: Option<String>,
) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if env("CANDLE_NUM_THREADS").is_none() {
        out.push(("CANDLE_NUM_THREADS", "1".to_owned()));
    }
    if let Some(cpus) = cpus
        && env("CANDLE_CPU_MASK").is_none()
    {
        out.push(("CANDLE_CPU_MASK", cpus));
    }
    out
}

/// The startup line's report of candle's variables: each one's value, and whether phdb set
/// it or kept it from the environment.
pub fn report(env: &dyn Fn(&str) -> Option<OsString>) -> String {
    let set_by_phdb = env(SET_BY_PHDB)
        .map(|v| v.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut parts = Vec::new();
    for k in ["CANDLE_NUM_THREADS", "CANDLE_CPU_MASK"] {
        let Some(v) = env(k) else {
            continue;
        };
        let v = v.to_string_lossy();
        let whose = if set_by_phdb
            .split(' ')
            .any(|p| p.split_once('=').is_some_and(|(n, _)| n == k))
        {
            "set by phdb"
        } else {
            "kept from environment"
        };
        parts.push(format!("{k}={v} ({whose})"));
    }
    if parts.is_empty() {
        "candle's pool: CANDLE_NUM_THREADS unset (one thread per core, unplaced)".to_owned()
    } else {
        format!("candle's pool: {}", parts.join(", "))
    }
}

/// The process's CPUs as a list (`0-7,16`), on Linux with `serve`'s `nix`; else `None`.
fn process_cpu_list() -> Option<String> {
    #[cfg(all(target_os = "linux", feature = "serve"))]
    {
        use nix::sched::{CpuSet, sched_getaffinity};
        let set = sched_getaffinity(nix::unistd::Pid::from_raw(0)).ok()?;
        let cpus: Vec<usize> = (0..CpuSet::count())
            .filter(|&c| set.is_set(c).unwrap_or(false))
            .collect();
        (!cpus.is_empty()).then(|| cpu_list(&cpus))
    }
    #[cfg(not(all(target_os = "linux", feature = "serve")))]
    {
        None
    }
}

/// `cpus` (ascending) as a CPU list: runs of two or more as `a-b`.
pub fn cpu_list(cpus: &[usize]) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < cpus.len() {
        let start = cpus[i];
        let mut end = start;
        while i + 1 < cpus.len() && cpus[i + 1] == end + 1 {
            i += 1;
            end = cpus[i];
        }
        out.push(if end > start {
            format!("{start}-{end}")
        } else {
            start.to_string()
        });
        i += 1;
    }
    out.join(",")
}

/// With `--embed-local` in the arguments and either variable unset, runs this program again
/// with them added, replacing this process (`exec`: the same pid, arguments and standard
/// streams). Returns only when there's nothing to do (`Ok`), or if the `exec` failed (the
/// caller goes on without them). Call it first thing in `main`, before any thread starts.
#[cfg(unix)]
pub fn reexec() -> std::io::Result<()> {
    use std::os::unix::process::CommandExt;
    let mut args = std::env::args_os();
    let arg0 = args.next().unwrap_or_default();
    let rest: Vec<OsString> = args.collect();
    if !wants_local(&rest) || std::env::var_os(SET_BY_PHDB).is_some() {
        return Ok(());
    }
    let add = missing(&|k| std::env::var_os(k), process_cpu_list());
    if add.is_empty() {
        return Ok(());
    }
    let marker = add
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");
    let err = std::process::Command::new(std::env::current_exe()?)
        .arg0(arg0)
        .args(rest)
        .envs(add)
        .env(SET_BY_PHDB, marker)
        .exec();
    Err(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(set: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<OsString> {
        move |k| {
            set.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| OsString::from(v))
        }
    }

    #[test]
    fn only_an_embed_local_flag_before_the_separator_asks() {
        let a = |s: &[&str]| s.iter().map(OsString::from).collect::<Vec<_>>();
        assert!(wants_local(&a(&["serve", "--memory", "m", "--embed-local"])));
        assert!(!wants_local(&a(&["serve", "--memory", "m"])));
        assert!(!wants_local(&a(&["get", "--", "--embed-local"])));
        assert!(!wants_local(&a(&["serve", "--embed-local-x"])));
    }

    #[test]
    fn only_the_unset_variables_are_added() {
        assert_eq!(
            missing(&env(&[]), Some("0-7".into())),
            vec![
                ("CANDLE_NUM_THREADS", "1".to_owned()),
                ("CANDLE_CPU_MASK", "0-7".to_owned())
            ]
        );
        assert_eq!(
            missing(&env(&[("CANDLE_NUM_THREADS", "4")]), Some("0-7".into())),
            vec![("CANDLE_CPU_MASK", "0-7".to_owned())]
        );
        assert_eq!(
            missing(&env(&[]), None),
            vec![("CANDLE_NUM_THREADS", "1".to_owned())]
        );
        assert!(
            missing(
                &env(&[("CANDLE_NUM_THREADS", "2"), ("CANDLE_CPU_MASK", "3")]),
                Some("0-7".into())
            )
            .is_empty()
        );
    }

    #[test]
    fn the_report_says_whose_each_variable_is() {
        let r = report(&env(&[
            ("CANDLE_NUM_THREADS", "1"),
            ("CANDLE_CPU_MASK", "2-7"),
            (SET_BY_PHDB, "CANDLE_NUM_THREADS=1"),
        ]));
        assert_eq!(
            r,
            "candle's pool: CANDLE_NUM_THREADS=1 (set by phdb), CANDLE_CPU_MASK=2-7 (kept from environment)"
        );
        assert!(report(&env(&[])).contains("unset"));
    }

    #[test]
    fn a_cpu_list_joins_runs() {
        assert_eq!(cpu_list(&[0, 1, 2, 3, 8, 10, 11]), "0-3,8,10-11");
        assert_eq!(cpu_list(&[5]), "5");
    }
}
