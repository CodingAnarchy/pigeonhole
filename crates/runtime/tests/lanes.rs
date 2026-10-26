//! D285's priority lane: a drain takes it before the main lane, order holds within each
//! lane, and a handler may take it between groups (`ShardContext::take_priority`) without
//! losing what it leaves queued. Application-owned, driven from the test thread, so the order
//! is deterministic.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use pigeonhole_io::sim::SimVfs;
use pigeonhole_runtime::{
    Runtime, RuntimeConfig, ShardContext, ShardDriver, ShardHandler, ShardId,
};

/// `(priority, id)`; the handler records what it handled, with `"s"` for a take between
/// groups.
type Msg = (bool, u32);

struct Recorder {
    log: Arc<Mutex<Vec<String>>>,
    /// Messages sent to this shard's priority lane at the start of the first `end_batch`, as
    /// a peer's would land mid-batch.
    arrive: Vec<u32>,
    /// How many priority messages `end_batch` takes (its safe point's bound).
    take: usize,
}

impl ShardHandler for Recorder {
    type Msg = Msg;

    fn handle(&mut self, _ctx: &mut ShardContext<'_, Msg>, (p, id): Msg) {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}{id}", if p { "p" } else { "m" }));
    }

    fn end_batch(&mut self, ctx: &mut ShardContext<'_, Msg>) {
        for x in std::mem::take(&mut self.arrive) {
            ctx.submitter(ctx.shard()).submit((true, x)).unwrap();
        }
        let mut log = self.log.lock().unwrap();
        log.push("end".into());
        for _ in 0..self.take {
            match ctx.take_priority() {
                Some((_, id)) => log.push(format!("s{id}")),
                None => break,
            }
        }
    }

    fn priority(msg: &Msg) -> bool {
        msg.0
    }
}

fn start(arrive: Vec<u32>, take: usize) -> (ShardDriver<Recorder>, Arc<Mutex<Vec<String>>>) {
    let mut config = RuntimeConfig::new(SimVfs::new(1));
    config.shards = 1;
    let log = Arc::new(Mutex::new(Vec::new()));
    let handler = Recorder {
        log: Arc::clone(&log),
        arrive,
        take,
    };
    let mut drivers = Runtime::application_owned(config, vec![handler]).unwrap();
    (drivers.pop().unwrap(), log)
}

fn taken(log: &Mutex<Vec<String>>) -> Vec<String> {
    std::mem::take(&mut *log.lock().unwrap())
}

#[test]
fn a_drain_takes_the_priority_lane_first_and_keeps_order_within_each_lane() {
    let (mut d, log) = start(Vec::new(), 0);
    let s = d.submitter(ShardId(0));
    for m in [(false, 1), (false, 2), (true, 10), (false, 3), (true, 11)] {
        s.submit(m).unwrap();
    }
    while d.run_once(u64::MAX) {}
    assert_eq!(taken(&log), ["p10", "p11", "m1", "m2", "m3", "end"]);
}

#[test]
fn a_take_between_groups_gets_what_arrived_during_the_batch_and_leaves_the_rest_queued() {
    // Three priority messages arrive while the batch's group runs (sent from `end_batch`,
    // which is when a peer's message lands mid-batch); the safe point takes two (its bound),
    // and the third stays queued: the next turn handles it, and the shard was woken for it.
    let (mut d, log) = start(vec![40, 41, 42], 2);
    let woken = Arc::new(AtomicU32::new(0));
    let w = Arc::clone(&woken);
    d.set_wakeup(Box::new(move || {
        w.fetch_add(1, Ordering::SeqCst);
    }));
    d.submitter(ShardId(0)).submit((false, 1)).unwrap();
    assert!(d.run_once(u64::MAX), "work left after the first turn");
    assert_eq!(taken(&log), ["m1", "end", "s40", "s41"]);
    assert!(woken.load(Ordering::SeqCst) > 0);
    while d.run_once(u64::MAX) {}
    assert_eq!(taken(&log), ["p42", "end"]);
}
