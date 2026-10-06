//! Determinism under the simulator: application-owned shards driven from one thread by a
//! seeded scheduler over `SimVfs` time produce identical traces for the same seed.
//!
//! Each shard driver and a message injector run as `pigeonhole_sim::Sim` tasks, so the
//! interleaving of `run_once` calls, injections and clock jumps is chosen by the seed.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pigeonhole_io::Vfs;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_runtime::{
    Runtime, RuntimeConfig, ShardContext, ShardDriver, ShardHandler, ShardId, Task, TaskPoll,
    TaskWaker,
};
use pigeonhole_sim::{Rng, Sim, Step};
use proptest::prelude::*;

type Trace = Arc<Mutex<Vec<String>>>;
type Parked = Arc<Mutex<Vec<TaskWaker>>>;

enum Msg {
    /// Forward `hops` more times to shards the handler's RNG picks; spawn a task sometimes.
    Work { id: u32, hops: u32 },
    /// Wake every task parked on this shard.
    WakeAll,
}

struct Handler {
    sim: Arc<SimVfs>,
    rng: Rng,
    trace: Trace,
    parked: Parked,
    batch: u32,
}

impl Handler {
    fn log(&self, ctx: &ShardContext<'_, Msg>, what: String) {
        let line = format!("{} s{} {what}", ctx.now_nanos(), ctx.shard().0);
        self.trace.lock().unwrap().push(line);
    }
}

impl ShardHandler for Handler {
    type Msg = Msg;

    fn handle(&mut self, ctx: &mut ShardContext<'_, Msg>, msg: Msg) {
        self.batch += 1;
        self.sim.advance(1_000);
        match msg {
            Msg::Work { id, hops } => {
                self.log(ctx, format!("work {id} hops {hops}"));
                if hops > 0 {
                    let to = ShardId(self.rng.below(ctx.shard_count() as u64) as u16);
                    ctx.submitter(to)
                        .submit(Msg::Work { id, hops: hops - 1 })
                        .unwrap();
                }
                if self.rng.below(4) == 0 {
                    ctx.spawn(Box::new(Chunky {
                        id,
                        sim: Arc::clone(&self.sim),
                        trace: Arc::clone(&self.trace),
                        parked: Arc::clone(&self.parked),
                        units_left: 20 + self.rng.below(200) as u32,
                        block_every: 1 + self.rng.below(60) as u32,
                        units: 0,
                    }));
                }
            }
            Msg::WakeAll => {
                let wakers = std::mem::take(&mut *self.parked.lock().unwrap());
                self.log(ctx, format!("wake {}", wakers.len()));
                wakers.iter().for_each(TaskWaker::wake);
            }
        }
    }

    fn end_batch(&mut self, ctx: &mut ShardContext<'_, Msg>) {
        self.log(ctx, format!("batch {}", self.batch));
        self.batch = 0;
    }
}

/// Background work that costs simulated time and blocks now and then.
struct Chunky {
    id: u32,
    sim: Arc<SimVfs>,
    trace: Trace,
    parked: Parked,
    units_left: u32,
    block_every: u32,
    units: u32,
}

impl Task for Chunky {
    fn run(&mut self, deadline: u64, waker: &TaskWaker) -> TaskPoll {
        let start = self.sim.monotonic_nanos();
        let mut done = 0;
        let poll = loop {
            if self.units_left == 0 {
                break TaskPoll::Done;
            }
            if self.sim.monotonic_nanos() >= deadline {
                break TaskPoll::Pending;
            }
            self.sim.advance(25_000);
            self.units_left -= 1;
            self.units += 1;
            done += 1;
            if self.units.is_multiple_of(self.block_every) {
                self.parked.lock().unwrap().push(waker.clone());
                break TaskPoll::Blocked;
            }
        };
        self.trace
            .lock()
            .unwrap()
            .push(format!("{start} task {} ran {done} -> {poll:?}", self.id));
        poll
    }
    fn name(&self) -> &'static str {
        "chunky"
    }
}

const SHARDS: usize = 3;

/// One seeded run; returns the full trace.
fn run(seed: u64, injections: u32) -> Vec<String> {
    let mut sim = Sim::new(seed);
    let vfs = sim.vfs();
    let trace: Trace = Arc::default();
    let mut config = RuntimeConfig::new(vfs.clone());
    config.shards = SHARDS;
    config.time_slice = Duration::from_micros(300);
    let handlers = (0..SHARDS)
        .map(|i| Handler {
            sim: Arc::clone(&vfs),
            rng: Rng::new(seed ^ (i as u64 + 1).wrapping_mul(0xA24B_AED4_963E_E407)),
            trace: Arc::clone(&trace),
            parked: Arc::default(),
            batch: 0,
        })
        .collect();
    let drivers: Vec<ShardDriver<Handler>> = Runtime::application_owned(config, handlers).unwrap();
    let submitters: Vec<_> = (0..SHARDS)
        .map(|i| drivers[0].submitter(ShardId(i as u16)))
        .collect();
    let drivers = Rc::new(RefCell::new(drivers));

    // One sim task per shard: run_once with a random deadline, then poll again later.
    for i in 0..SHARDS {
        let drivers = Rc::clone(&drivers);
        let vfs = Arc::clone(&vfs);
        let trace = Arc::clone(&trace);
        sim.spawn(
            "shard",
            Box::new(move |rng| {
                let now = vfs.monotonic_nanos();
                let more = drivers.borrow_mut()[i].run_once(now + rng.below(2_000_000));
                trace.lock().unwrap().push(format!("run s{i} more={more}"));
                if more {
                    Step::Ready
                } else {
                    Step::SleepUntil(vfs.monotonic_nanos() + 1 + rng.below(700_000))
                }
            }),
        );
    }

    // The injector: new work and wakeups at random times.
    let injected = Rc::new(Cell::new(0));
    {
        let injected = Rc::clone(&injected);
        let vfs = Arc::clone(&vfs);
        sim.spawn(
            "inject",
            Box::new(move |rng| {
                let n = injected.get();
                if n == injections {
                    return Step::Done;
                }
                injected.set(n + 1);
                let to = &submitters[rng.below(SHARDS as u64) as usize];
                if rng.below(5) == 0 {
                    to.submit(Msg::WakeAll).unwrap();
                } else {
                    let hops = rng.below(5) as u32;
                    to.submit(Msg::Work { id: n, hops }).unwrap();
                }
                Step::SleepUntil(vfs.monotonic_nanos() + rng.below(300_000))
            }),
        );
    }
    assert!(
        sim.run_until(u64::MAX, &mut || injected.get() == injections),
        "seed {seed}: injector never finished"
    );

    // Quiesce: wake everything and run until no shard has work.
    let mut drivers = drivers.borrow_mut();
    loop {
        for d in drivers.iter() {
            d.submitter(d.shard()).submit(Msg::WakeAll).unwrap();
        }
        let mut more = false;
        for d in drivers.iter_mut() {
            while d.run_once(u64::MAX) {
                more = true;
            }
        }
        let parked = drivers
            .iter_mut()
            .any(|d| d.with_handler(|h, _| !h.parked.lock().unwrap().is_empty()));
        if !more && !parked {
            break;
        }
    }
    drivers.clear();
    drop(drivers);
    drop(sim);
    Arc::try_unwrap(trace).unwrap().into_inner().unwrap()
}

/// Every injected message is handled `hops + 1` times; every task finishes.
fn check_complete(trace: &[String]) {
    let started = trace.iter().filter(|l| l.contains(" task ")).count();
    assert!(started > 0, "no background task ran");
    let mut per_id: std::collections::BTreeMap<u32, Vec<u32>> = Default::default();
    for l in trace {
        if let Some(rest) = l.split(" work ").nth(1) {
            let mut it = rest.split(" hops ");
            let id: u32 = it.next().unwrap().parse().unwrap();
            let hops: u32 = it.next().unwrap().parse().unwrap();
            per_id.entry(id).or_default().push(hops);
        }
    }
    for (id, hops) in per_id {
        let first = hops[0];
        let want: Vec<u32> = (0..=first).rev().collect();
        assert_eq!(
            hops, want,
            "message {id} not forwarded exactly once per hop"
        );
    }
}

#[test]
fn same_seed_same_trace() {
    for seed in [1, 2, 0xDEAD_BEEF, 42] {
        let a = run(seed, 1200);
        let b = run(seed, 1200);
        assert!(a == b, "seed {seed}: traces differ");
        check_complete(&a);
    }
}

#[test]
fn different_seeds_differ() {
    assert_ne!(run(1, 500), run(2, 500));
}

proptest! {
    // Honors PROPTEST_CASES (CI sets it low); 24 cases otherwise.
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(24)
    ))]
    #[test]
    fn deterministic_for_any_seed(seed in any::<u64>()) {
        let a = run(seed, 300);
        let b = run(seed, 300);
        prop_assert!(a == b, "seed {seed}: traces differ");
        check_complete(&a);
    }
}
