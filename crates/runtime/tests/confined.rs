//! ICR 0134: compaction threads confined to a set of CPUs (`compaction_cpu_set`) instead of
//! pinned one each. Its own test binary, as `pinning.rs` is: a runtime here holds CPUs that
//! another test in the same process would otherwise find taken.

use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::sys::Unpinned;
use pigeonhole_runtime::{Runtime, RuntimeConfig, ShardContext, ShardHandler};

struct Idle;

impl ShardHandler for Idle {
    type Msg = ();
    fn handle(&mut self, _: &mut ShardContext<'_, ()>, (): ()) {}
    fn end_batch(&mut self, _: &mut ShardContext<'_, ()>) {}
}

/// ICR 0134: compaction threads confined to a set take no CPU of the plan, so a runtime
/// whose shards fill every free CPU still pins every shard, and its compaction threads run
/// on the set rather than unpinned.
#[test]
fn compaction_threads_confined_to_a_set_leave_the_plan_to_the_shards() {
    let cpus = pigeonhole_io::sys::available_cpus();
    if cpus < 2 {
        return;
    }
    let mut c = RuntimeConfig::new(PreadVfs::new(1));
    c.shards = cpus;
    c.compaction_threads = 2;
    c.pin_threads = true;
    // A set of one CPU, which the shards also hold: confining shares it, never claims it.
    let set: std::sync::Arc<[usize]> = vec![0].into();
    c.compaction_cpu_set = Some(set);
    let rt = Runtime::start(c, (0..cpus).map(|_| Idle).collect()).unwrap();
    let p = rt.pinning();
    assert_eq!(p.threads, cpus + 2, "{p:?}");
    match p.unpinned {
        // No hard affinity (macOS) or a quota: nothing pinned or confined.
        Some(why) => {
            assert!(
                matches!(why, Unpinned::Unsupported | Unpinned::CpuQuota),
                "{why:?}"
            );
            assert_eq!(p.pinned + p.confined, 0, "{p:?}");
        }
        // Only the shards were planned, so they all fit; the compaction threads are
        // confined (or, refused by the OS, counted in `failed`), and list no CPU of their own.
        None => {
            assert_eq!(p.pinned + p.confined + p.failed, cpus + 2, "{p:?}");
            assert!(rt.pinned_compaction_cpus().is_empty());
            assert_eq!(rt.pinned_driver_cpus().len(), p.pinned);
        }
    }
    rt.shutdown().unwrap();
}
