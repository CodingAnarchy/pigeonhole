//! D235: engine-owned threads pin to CPUs of their own inside the process's affinity set;
//! a runtime that finds none free runs unpinned instead of stacking or failing; claims are
//! released at shutdown; compaction threads that don't fit leave the shards pinned (D252).
//! Only a pinned shard thread whose runtime gave every thread a CPU spins without yielding
//! (D289).
//! Its own test binary, so no other runtime in the process holds CPUs.

use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::sys::Unpinned;
use pigeonhole_runtime::{Runtime, RuntimeConfig, ShardContext, ShardHandler};

struct Idle;

impl ShardHandler for Idle {
    type Msg = ();
    fn handle(&mut self, _: &mut ShardContext<'_, ()>, (): ()) {}
    fn end_batch(&mut self, _: &mut ShardContext<'_, ()>) {}
}

fn start(shards: usize) -> Runtime<Idle> {
    start_with(shards, 0)
}

fn start_with(shards: usize, compaction_threads: usize) -> Runtime<Idle> {
    let mut c = RuntimeConfig::new(PreadVfs::new(1));
    c.shards = shards;
    c.compaction_threads = compaction_threads;
    c.pin_threads = true;
    Runtime::start(c, (0..shards).map(|_| Idle).collect()).expect("pinning never fails a start")
}

#[test]
fn runtimes_pin_to_distinct_cpus_and_one_without_free_cpus_runs_unpinned() {
    let cpus = pigeonhole_io::sys::available_cpus();
    let all = start(cpus);
    let p = all.pinning();
    assert_eq!(p.threads, cpus);
    match p.unpinned {
        // macOS, or a CPU quota narrower than the affinity set (a CI container): nothing
        // pinned, and nothing to claim, so the next runtime finds the same.
        Some(why) => {
            assert_eq!(p.pinned, 0);
            assert_eq!(p.quiet_spins, 0, "an unpinned shard's spin yields");
            assert!(
                matches!(why, Unpinned::Unsupported | Unpinned::CpuQuota),
                "{why:?}"
            );
            return;
        }
        None => {
            assert_eq!(p.pinned + p.failed, cpus, "{p:?}");
            // A thread the OS refused to pin runs unpinned, and yields.
            assert_eq!(p.quiet_spins, p.pinned, "{p:?}");
        }
    }
    // Every CPU is held: a second runtime runs unpinned rather than stacking, and starts.
    let second = start(1);
    assert_eq!(second.pinning().unpinned, Some(Unpinned::TooFewCpus));
    assert_eq!(second.pinning().pinned, 0);
    assert_eq!(second.pinning().quiet_spins, 0);
    drop(second);
    // Shutdown frees the CPUs.
    all.shutdown().unwrap();
    let again = start(1);
    assert_eq!(again.pinning().unpinned, None);
    assert_eq!(again.pinning().pinned + again.pinning().failed, 1);
    assert_eq!(again.pinning().quiet_spins, again.pinning().pinned);
    again.shutdown().unwrap();
    // Shards on every CPU and two compaction threads: the shards are pinned, and only the
    // compaction threads run unpinned (D252).
    let over = start_with(cpus, 2);
    let p = over.pinning();
    assert_eq!(p.threads, cpus + 2);
    assert_eq!(p.unpinned, Some(Unpinned::TooFewCpus));
    assert_eq!(p.pinned + p.failed, cpus, "{p:?}");
    // The unpinned compaction threads may land on a shard's CPU: every shard's spin yields.
    assert_eq!(p.quiet_spins, 0, "{p:?}");
    over.shutdown().unwrap();
}
