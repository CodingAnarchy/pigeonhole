//! `ShardDriver::work_count`: the messages a shard handled and the background slices it ran,
//! for tests that drive shards by hand. `run_once` returns whether work remains, so a turn
//! that handled everything queued returns `false` while its count moves.

use pigeonhole_io::sim::SimVfs;
use pigeonhole_runtime::{
    Runtime, RuntimeConfig, ShardContext, ShardHandler, ShardId, Task, TaskPoll, TaskWaker,
};

struct Handler;

/// Runs for two slices, then finishes.
struct TwoSlices(u32);

impl Task for TwoSlices {
    fn run(&mut self, _deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        self.0 += 1;
        if self.0 == 2 {
            TaskPoll::Done
        } else {
            TaskPoll::Pending
        }
    }

    fn name(&self) -> &'static str {
        "two-slices"
    }
}

impl ShardHandler for Handler {
    type Msg = bool;

    fn handle(&mut self, ctx: &mut ShardContext<'_, bool>, spawn: bool) {
        if spawn {
            ctx.spawn(Box::new(TwoSlices(0)));
        }
    }

    fn end_batch(&mut self, _ctx: &mut ShardContext<'_, bool>) {}
}

#[test]
fn work_count_counts_handled_messages_and_slices() {
    let mut config = RuntimeConfig::new(SimVfs::new(1));
    config.shards = 1;
    let mut d = Runtime::application_owned(config, vec![Handler])
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(d.work_count(), 0);
    let s = d.submitter(ShardId(0));
    s.submit(false).unwrap();
    s.submit(false).unwrap();
    s.submit(false).unwrap();
    assert!(!d.run_once(u64::MAX), "nothing remains after the turn");
    assert_eq!(d.work_count(), 3, "three messages handled");
    assert!(!d.run_once(u64::MAX));
    assert_eq!(d.work_count(), 3, "an idle turn does no work");
    s.submit(true).unwrap();
    while d.run_once(u64::MAX) {}
    assert_eq!(
        d.work_count(),
        4 + 2,
        "one message and the task's two slices"
    );
}
