//! Platform glue: every syscall the crate makes lives under this module.

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(crate) use unix::*;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::*;

/// What the OS says about a process id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Liveness {
    /// No such process (or a zombie).
    Dead,
    /// Running, with its start time if the platform reports one.
    Alive(Option<u64>),
}

/// Whether a recorded process (pid plus start time, 0 = unknown) is still running.
pub(crate) fn process_alive(pid: u32, start_time: u64) -> bool {
    match process_liveness(pid) {
        Liveness::Dead => false,
        Liveness::Alive(None) => true,
        Liveness::Alive(Some(now)) => start_time == 0 || now == start_time,
    }
}

/// Validates a shared-memory name: non-empty, no path separators or NULs.
pub(crate) fn check_shared_name(name: &str) -> crate::Result<()> {
    if name.is_empty() || name.contains(['/', '\\', '\0']) {
        return Err(crate::Error::new(
            crate::ErrorKind::Other,
            "invalid shared-memory name",
        ));
    }
    Ok(())
}

/// The `i`-th CPU to pin a thread to (modulo the CPUs), as an index into the allowed set:
/// the first CPU of every physical core in order, then the remaining CPUs (SMT siblings) in
/// order (D220). `first_of_core[k]` says whether allowed CPU `k` is the first of its core.
#[cfg_attr(not(any(target_os = "linux", windows, test)), allow(dead_code))]
pub(crate) fn spread(first_of_core: &[bool], i: usize) -> usize {
    if first_of_core.is_empty() {
        return i;
    }
    let i = i % first_of_core.len();
    let cores = first_of_core.iter().filter(|&&first| first).count();
    let (want, nth) = if i < cores {
        (true, i)
    } else {
        (false, i - cores)
    };
    first_of_core
        .iter()
        .enumerate()
        .filter(|&(_, &first)| first == want)
        .nth(nth)
        .map_or(i, |(k, _)| k)
}

/// The first CPU of a sysfs CPU list ("0,16", "0-1", "3", "8-9,24-25\n"): the name of the
/// core whose sibling list it is.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
pub(crate) fn first_cpu_of_list(list: &str) -> Option<usize> {
    let list = list.trim();
    let end = list
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(list.len());
    list[..end].parse().ok()
}

/// For each CPU's core name (in the allowed set's order), whether it is the first CPU of its
/// core.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
pub(crate) fn first_of_core(cores: &[usize]) -> Vec<bool> {
    cores
        .iter()
        .enumerate()
        .map(|(k, core)| !cores[..k].contains(core))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{first_cpu_of_list, first_of_core, spread};

    /// Each CPU's sibling list, read as the Linux topology reader reads it.
    fn cores(lists: &[&str]) -> Vec<bool> {
        let names: Vec<usize> = lists
            .iter()
            .map(|l| first_cpu_of_list(l).unwrap())
            .collect();
        first_of_core(&names)
    }

    #[test]
    fn sibling_lists_name_their_core() {
        assert_eq!(first_cpu_of_list("3\n"), Some(3));
        assert_eq!(first_cpu_of_list("0,16\n"), Some(0));
        assert_eq!(first_cpu_of_list("14-15\n"), Some(14));
        assert_eq!(first_cpu_of_list("8-9,24-25\n"), Some(8));
        assert_eq!(first_cpu_of_list(""), None);
        assert_eq!(first_cpu_of_list("-1\n"), None);
    }

    #[test]
    fn smt_siblings_count_once() {
        // 4 cores, 8 threads, siblings numbered after every core (AMD, most Intel).
        let split = ["0,4", "1,5", "2,6", "3,7", "0,4", "1,5", "2,6", "3,7"];
        assert_eq!(
            cores(&split),
            [true, true, true, true, false, false, false, false]
        );
        // Siblings numbered next to each other, written as ranges.
        let paired = ["0-1", "0-1", "2-3", "2-3"];
        assert_eq!(cores(&paired), [true, false, true, false]);
        // Two packages: core_id 0 exists on both, the sibling lists stay distinct.
        let packages = ["0,2", "1,3", "0,2", "1,3"];
        assert_eq!(cores(&packages), [true, true, false, false]);
        // No SMT.
        assert_eq!(cores(&["0", "1", "2"]), [true, true, true]);
        // An affinity mask with only the siblings of cores 0 and 1 (CPUs 4 and 5).
        assert_eq!(cores(&["0,4", "1,5"]), [true, true]);
    }

    #[test]
    fn spread_over_a_two_package_machine() {
        // Package 0: CPUs 0-3 = cores 0,1 with siblings; package 1: CPUs 4-7 likewise.
        let lists = ["0-1", "0-1", "2-3", "2-3", "4-5", "4-5", "6-7", "6-7"];
        let first = cores(&lists);
        let order: Vec<usize> = (0..8).map(|i| spread(&first, i)).collect();
        assert_eq!(order, [0, 2, 4, 6, 1, 3, 5, 7]);
    }

    #[test]
    fn threads_spread_over_cores_before_their_siblings() {
        // Siblings numbered after every core (0-3 are cores, 4-7 their siblings).
        let split = [true, true, true, true, false, false, false, false];
        assert_eq!(
            (0..8).map(|i| spread(&split, i)).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 5, 6, 7]
        );
        // Siblings numbered next to each other (0-1 one core, 2-3 the next).
        let paired = [true, false, true, false, true, false];
        assert_eq!(
            (0..7).map(|i| spread(&paired, i)).collect::<Vec<_>>(),
            [0, 2, 4, 1, 3, 5, 0]
        );
        // No SMT, and no topology.
        assert_eq!(spread(&[true, true], 3), 1);
        assert_eq!(spread(&[], 5), 5);
    }
}
