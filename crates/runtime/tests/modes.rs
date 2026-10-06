//! One suite, run in both embedding modes: engine-owned (`Runtime::start`) and
//! application-owned (`ShardDriver::run_once` driven from threads the test owns).

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

use pigeonhole_io::VfsRef;
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_runtime::{
    Error, Notifier, Runtime, RuntimeConfig, ShardContext, ShardDriver, ShardHandler, ShardId,
    Submitter, Task, TaskPoll, TaskWaker, completion,
};

#[derive(Debug, Clone, Copy)]
enum Mode {
    Engine,
    App,
}

/// A running runtime in either mode, behind one interface.
enum Harness<H: ShardHandler> {
    Engine(Runtime<H>),
    App {
        submitters: Vec<Submitter<H::Msg>>,
        stop: Arc<AtomicBool>,
        threads: Vec<JoinHandle<ShardDriver<H>>>,
    },
}

impl<H: ShardHandler> Harness<H> {
    fn start(mode: Mode, config: RuntimeConfig, handlers: Vec<H>) -> Self {
        match mode {
            Mode::Engine => Harness::Engine(Runtime::start(config, handlers).unwrap()),
            Mode::App => {
                let slice = config.time_slice.as_nanos() as u64;
                let vfs = Arc::clone(&config.vfs);
                let drivers = Runtime::application_owned(config, handlers).unwrap();
                let submitters = (0..drivers.len())
                    .map(|i| drivers[0].submitter(ShardId(i as u16)))
                    .collect();
                let stop = Arc::new(AtomicBool::new(false));
                let threads = drivers
                    .into_iter()
                    .map(|mut d| {
                        let stop = Arc::clone(&stop);
                        let vfs = Arc::clone(&vfs);
                        thread::spawn(move || {
                            let me = thread::current();
                            d.set_wakeup(Box::new(move || me.unpark()));
                            loop {
                                let more = d.run_once(vfs.monotonic_nanos().saturating_add(slice));
                                if more {
                                    continue;
                                }
                                if stop.load(Ordering::SeqCst) {
                                    // Everything submitted before `stop` is visible now.
                                    while d.run_once(u64::MAX) {}
                                    return d;
                                }
                                thread::park();
                            }
                        })
                    })
                    .collect();
                Harness::App {
                    submitters,
                    stop,
                    threads,
                }
            }
        }
    }

    fn submitter(&self, shard: u16) -> Submitter<H::Msg> {
        match self {
            Harness::Engine(rt) => rt.submitter(ShardId(shard)),
            Harness::App { submitters, .. } => submitters[usize::from(shard)].clone(),
        }
    }

    /// Stops the runtime after it handles everything queued; inspects each handler.
    fn finish<R>(self, f: impl Fn(&mut H) -> R) -> Vec<R> {
        match self {
            Harness::Engine(rt) => rt.shutdown().unwrap().iter_mut().map(f).collect(),
            Harness::App { stop, threads, .. } => {
                stop.store(true, Ordering::SeqCst);
                threads.iter().for_each(|t| t.thread().unpark());
                threads
                    .into_iter()
                    .map(|t| f(&mut t.join().unwrap().shutdown()))
                    .collect()
            }
        }
    }
}

fn config(shards: usize, vfs: VfsRef) -> RuntimeConfig {
    let mut c = RuntimeConfig::new(vfs);
    c.shards = shards;
    c.pin_threads = true; // best-effort; exercises sys::pin_current_thread in engine mode
    c
}

fn sim_config(shards: usize) -> (RuntimeConfig, Arc<SimVfs>) {
    let sim = SimVfs::new(1);
    (config(shards, sim.clone()), sim)
}

// ---------------------------------------------------------------------------------------
// The test handler.

enum Msg {
    Echo(u64, Notifier<u64>),
    Record {
        producer: u32,
        seq: u32,
    },
    Forward {
        hops: u32,
        path: Vec<u16>,
        done: Notifier<Vec<u16>>,
    },
    WhoAmI(Notifier<(Option<ShardId>, ShardId)>),
    Spawn(Box<dyn Task>),
    Stamp {
        sent: u64,
    },
    /// Drops its notifier on the shard without resolving it.
    Discard(Notifier<u64>),
}

#[derive(Default)]
struct Handler {
    records: Vec<(u32, u32)>,
    batches: u64,
    in_batch: usize,
    max_batch: usize,
    latencies: Vec<u64>,
}

impl ShardHandler for Handler {
    type Msg = Msg;

    fn handle(&mut self, ctx: &mut ShardContext<'_, Msg>, msg: Msg) {
        self.in_batch += 1;
        match msg {
            Msg::Echo(v, n) => n.notify(v),
            Msg::Record { producer, seq } => self.records.push((producer, seq)),
            Msg::Forward {
                hops,
                mut path,
                done,
            } => {
                path.push(ctx.shard().0);
                if hops == 0 {
                    done.notify(path);
                } else {
                    let next =
                        ShardId(((usize::from(ctx.shard().0) + 1) % ctx.shard_count()) as u16);
                    ctx.submitter(next)
                        .submit(Msg::Forward {
                            hops: hops - 1,
                            path,
                            done,
                        })
                        .unwrap();
                }
            }
            Msg::WhoAmI(n) => n.notify((Runtime::<Handler>::current_shard(), ctx.shard())),
            Msg::Spawn(task) => ctx.spawn(task),
            Msg::Stamp { sent } => self.latencies.push(ctx.now_nanos() - sent),
            Msg::Discard(n) => drop(n),
        }
    }

    fn end_batch(&mut self, _ctx: &mut ShardContext<'_, Msg>) {
        self.batches += 1;
        self.max_batch = self.max_batch.max(self.in_batch);
        self.in_batch = 0;
    }
}

fn handlers(n: usize) -> Vec<Handler> {
    (0..n).map(|_| Handler::default()).collect()
}

// ---------------------------------------------------------------------------------------
// Tasks.

/// Runs `slices` slices, then reports the thread it finished on.
struct CountSlices {
    left: u32,
    done: Option<Notifier<Option<String>>>,
}

impl Task for CountSlices {
    fn run(&mut self, _deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        self.left -= 1;
        if self.left > 0 {
            return TaskPoll::Pending;
        }
        let name = thread::current().name().map(str::to_owned);
        self.done.take().unwrap().notify(name);
        TaskPoll::Done
    }
    fn name(&self) -> &'static str {
        "count"
    }
}

/// Blocks once, handing its waker out (or waking itself before returning `Blocked`).
struct BlockOnce {
    waker_out: Option<Notifier<TaskWaker>>,
    self_wake: bool,
    blocked: bool,
    done: Option<Notifier<()>>,
}

impl Task for BlockOnce {
    fn run(&mut self, _deadline: u64, waker: &TaskWaker) -> TaskPoll {
        if self.blocked {
            self.done.take().unwrap().notify(());
            return TaskPoll::Done;
        }
        self.blocked = true;
        if self.self_wake {
            waker.wake(); // a wake racing the Blocked return must not be lost
        }
        if let Some(out) = self.waker_out.take() {
            out.notify(waker.clone());
        }
        TaskPoll::Blocked
    }
    fn name(&self) -> &'static str {
        "block-once"
    }
}

/// Burns simulated time in `unit` steps until each deadline and, every `every` units,
/// submits a timestamped message to its own shard: foreground work arriving mid-slice.
struct SimBusy {
    sim: Arc<SimVfs>,
    me: Submitter<Msg>,
    unit: u64,
    every: u32,
    units: u32,
    total: u32,
    done: Option<Notifier<()>>,
}

impl Task for SimBusy {
    fn run(&mut self, deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        use pigeonhole_io::Vfs;
        while self.sim.monotonic_nanos() < deadline {
            self.sim.advance(self.unit);
            self.units += 1;
            if self.units.is_multiple_of(self.every) {
                let sent = self.sim.monotonic_nanos();
                self.me.submit(Msg::Stamp { sent }).unwrap();
            }
            if self.units == self.total {
                self.done.take().unwrap().notify(());
                return TaskPoll::Done;
            }
        }
        TaskPoll::Pending
    }
    fn name(&self) -> &'static str {
        "sim-busy"
    }
}

/// Spins on the real clock until each deadline, until stopped.
struct RealBusy {
    vfs: VfsRef,
    stop: Arc<AtomicBool>,
}

impl Task for RealBusy {
    fn run(&mut self, deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        while self.vfs.monotonic_nanos() < deadline {
            std::hint::spin_loop();
        }
        if self.stop.load(Ordering::Relaxed) {
            TaskPoll::Done
        } else {
            TaskPoll::Pending
        }
    }
    fn name(&self) -> &'static str {
        "real-busy"
    }
}

// ---------------------------------------------------------------------------------------
// A minimal executor: completions are runtime-agnostic, so a park-based waker suffices.

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        thread::park();
    }
}

// ---------------------------------------------------------------------------------------
// The suite.

fn echo_sync_and_async(mode: Mode) {
    let (cfg, _) = sim_config(2);
    let h = Harness::start(mode, cfg, handlers(2));
    for shard in 0..2 {
        let (n, w) = completion();
        h.submitter(shard)
            .submit(Msg::Echo(41 + u64::from(shard), n))
            .unwrap();
        assert_eq!(w.wait(), Some(41 + u64::from(shard)));

        let (n, w) = completion();
        h.submitter(shard).submit(Msg::Echo(7, n)).unwrap();
        assert_eq!(block_on(w), Some(7));
    }
    h.finish(|_| ());
}

fn per_producer_fifo_and_batching(mode: Mode) {
    const PRODUCERS: u32 = 4;
    const PER: u32 = 3000;
    let (cfg, _) = sim_config(2);
    let h = Harness::start(mode, cfg, handlers(2));
    thread::scope(|s| {
        for p in 0..PRODUCERS {
            let sub = h.submitter(0);
            s.spawn(move || {
                for seq in 0..PER {
                    sub.submit(Msg::Record { producer: p, seq }).unwrap();
                }
            });
        }
    });
    let out = h.finish(|h| (std::mem::take(&mut h.records), h.batches, h.max_batch));
    let (records, batches, max_batch) = &out[0];
    assert_eq!(records.len(), (PRODUCERS * PER) as usize);
    let mut next = vec![0; PRODUCERS as usize];
    for &(p, seq) in records {
        assert_eq!(seq, next[p as usize], "producer {p} out of order");
        next[p as usize] += 1;
    }
    assert!(*batches >= 1);
    assert!(*max_batch <= 1024, "batch of {max_batch} exceeds the cap");
    assert!(out[1].0.is_empty());
}

fn cross_shard_forwarding(mode: Mode) {
    let (cfg, _) = sim_config(3);
    let h = Harness::start(mode, cfg, handlers(3));
    let (n, w) = completion();
    h.submitter(1)
        .submit(Msg::Forward {
            hops: 7,
            path: Vec::new(),
            done: n,
        })
        .unwrap();
    assert_eq!(w.wait().unwrap(), [1, 2, 0, 1, 2, 0, 1, 2]);
    h.finish(|_| ());
}

fn current_shard_is_set_on_shard_threads(mode: Mode) {
    let (cfg, _) = sim_config(2);
    let h = Harness::start(mode, cfg, handlers(2));
    assert_eq!(Runtime::<Handler>::current_shard(), None);
    for shard in 0..2 {
        let (n, w) = completion();
        h.submitter(shard).submit(Msg::WhoAmI(n)).unwrap();
        let (current, ctx) = w.wait().unwrap();
        assert_eq!(current, Some(ShardId(shard)));
        assert_eq!(ctx, ShardId(shard));
    }
    h.finish(|_| ());
}

fn background_task_runs_to_completion(mode: Mode) {
    let (cfg, _) = sim_config(1);
    let h = Harness::start(mode, cfg, handlers(1));
    let (n, w) = completion();
    let task = CountSlices {
        left: 50,
        done: Some(n),
    };
    h.submitter(0).submit(Msg::Spawn(Box::new(task))).unwrap();
    assert!(w.wait().is_some());
    h.finish(|_| ());
}

fn blocked_task_is_woken(mode: Mode) {
    let (cfg, _) = sim_config(1);
    let h = Harness::start(mode, cfg, handlers(1));

    // Woken from another thread after it blocked.
    let (wn, ww) = completion();
    let (dn, dw) = completion();
    let task = BlockOnce {
        waker_out: Some(wn),
        self_wake: false,
        blocked: false,
        done: Some(dn),
    };
    h.submitter(0).submit(Msg::Spawn(Box::new(task))).unwrap();
    let waker = ww.wait().unwrap();
    thread::sleep(Duration::from_millis(5)); // let the shard go idle
    waker.wake();
    assert_eq!(dw.wait(), Some(()));
    waker.wake(); // waking a finished task is harmless

    // Woken during its own slice, before it reported Blocked.
    let (dn, dw) = completion();
    let task = BlockOnce {
        waker_out: None,
        self_wake: true,
        blocked: false,
        done: Some(dn),
    };
    h.submitter(0).submit(Msg::Spawn(Box::new(task))).unwrap();
    assert_eq!(dw.wait(), Some(()));
    h.finish(|_| ());
}

/// Deterministic latency bound: under the simulated clock, a message submitted mid-slice is
/// handled within one time slice (plus one unit of task overshoot).
fn foreground_latency_bounded_sim(mode: Mode) {
    let (mut cfg, sim) = sim_config(1);
    cfg.time_slice = Duration::from_micros(500);
    let unit = 20_000; // 20 µs per unit of background work
    let h = Harness::start(mode, cfg, handlers(1));
    let (en, ew) = completion();
    let task = SimBusy {
        sim,
        me: h.submitter(0),
        unit,
        every: 7,
        units: 0,
        total: 7 * 400,
        done: Some(en),
    };
    h.submitter(0).submit(Msg::Spawn(Box::new(task))).unwrap();
    // `finish` then drains the stamps the task sent.
    assert!(ew.wait().is_some());
    let lat = h.finish(|h| std::mem::take(&mut h.latencies)).remove(0);
    let slice = 500_000;
    let max = lat.iter().copied().max().unwrap_or(0);
    assert!(lat.len() >= 300, "only {} stamps handled", lat.len());
    assert!(
        max <= slice + unit,
        "max foreground latency {max} ns exceeds slice {slice} + unit {unit}"
    );
    assert!(max >= slice / 2, "stamps never waited: test is vacuous");
}

/// Real-clock latency: a background task that spins for whole slices never holds a queued
/// foreground message for long.
fn foreground_latency_bounded_real(mode: Mode) {
    let vfs: VfsRef = PreadVfs::new(1);
    let mut cfg = config(1, Arc::clone(&vfs));
    cfg.time_slice = Duration::from_millis(1);
    let h = Harness::start(mode, cfg, handlers(1));
    let stop = Arc::new(AtomicBool::new(false));
    let busy = RealBusy {
        vfs,
        stop: Arc::clone(&stop),
    };
    h.submitter(0).submit(Msg::Spawn(Box::new(busy))).unwrap();
    let mut lat = Vec::new();
    for i in 0..200 {
        let (n, w) = completion();
        let t = Instant::now();
        h.submitter(0).submit(Msg::Echo(i, n)).unwrap();
        assert_eq!(w.wait(), Some(i));
        lat.push(t.elapsed());
        thread::sleep(Duration::from_micros(300));
    }
    stop.store(true, Ordering::Relaxed);
    h.finish(|_| ());
    lat.sort();
    let p50 = lat[lat.len() / 2];
    let p99 = lat[lat.len() * 99 / 100];
    // One 1 ms slice bounds the wait; the margins absorb CI scheduling noise.
    assert!(p50 < Duration::from_millis(5), "p50 {p50:?}");
    assert!(p99 < Duration::from_millis(50), "p99 {p99:?}");
}

fn shutdown_handles_queued_then_refuses(mode: Mode) {
    let (cfg, _) = sim_config(2);
    let h = Harness::start(mode, cfg, handlers(2));
    let sub = h.submitter(1);
    for seq in 0..5000 {
        sub.submit(Msg::Record { producer: 0, seq }).unwrap();
    }
    let counts = h.finish(|h| h.records.len());
    assert_eq!(counts, [0, 5000]);
    assert!(matches!(
        sub.submit(Msg::Record {
            producer: 0,
            seq: 0
        }),
        Err(Error::Closed)
    ));
}

fn dropped_notifier_resolves_none(mode: Mode) {
    let (cfg, _) = sim_config(1);
    let h = Harness::start(mode, cfg, handlers(1));
    // The shard drops the notifier unresolved: sync and async waiters see `None`.
    let (n, w) = completion::<u64>();
    h.submitter(0).submit(Msg::Discard(n)).unwrap();
    assert_eq!(w.wait(), None);
    let (n, w) = completion::<u64>();
    h.submitter(0).submit(Msg::Discard(n)).unwrap();
    assert_eq!(block_on(w), None);
    // The shard is still healthy.
    let (n, w) = completion::<u64>();
    h.submitter(0).submit(Msg::Echo(1, n)).unwrap();
    assert_eq!(block_on(w), Some(1));
    h.finish(|_| ());
}

/// Thousands of sleep/wake transitions from several callers: a lost wakeup would hang here.
fn many_round_trips_no_lost_wakeup(mode: Mode) {
    let (cfg, _) = sim_config(2);
    let h = Harness::start(mode, cfg, handlers(2));
    thread::scope(|s| {
        for c in 0..3u64 {
            let subs = [h.submitter(0), h.submitter(1)];
            s.spawn(move || {
                for i in 0..5000u64 {
                    let (n, w) = completion();
                    subs[(i % 2) as usize].submit(Msg::Echo(c * i, n)).unwrap();
                    assert_eq!(w.wait(), Some(c * i));
                }
            });
        }
    });
    h.finish(|_| ());
}

macro_rules! both_modes {
    ($($name:ident),* $(,)?) => {
        mod engine_owned {
            $(#[test] fn $name() { super::$name(super::Mode::Engine) })*
        }
        mod application_owned {
            $(#[test] fn $name() { super::$name(super::Mode::App) })*
        }
    };
}

both_modes!(
    echo_sync_and_async,
    per_producer_fifo_and_batching,
    cross_shard_forwarding,
    current_shard_is_set_on_shard_threads,
    background_task_runs_to_completion,
    blocked_task_is_woken,
    foreground_latency_bounded_sim,
    foreground_latency_bounded_real,
    shutdown_handles_queued_then_refuses,
    dropped_notifier_resolves_none,
    many_round_trips_no_lost_wakeup,
);

// ---------------------------------------------------------------------------------------
// Engine-owned specifics.

#[test]
fn application_owned_refuses_compaction_threads() {
    let (mut cfg, _) = sim_config(2);
    cfg.compaction_threads = 1;
    match Runtime::application_owned(cfg, handlers(2)) {
        Err(Error::InvalidConfig(what)) => assert!(what.contains("compaction_threads")),
        Err(e) => panic!("expected InvalidConfig, got {e}"),
        Ok(_) => panic!("expected InvalidConfig, got drivers"),
    }
    // `pin_threads` (on in this config) is simply not applicable and stays accepted.
    let (cfg, _) = sim_config(2);
    assert!(Runtime::application_owned(cfg, handlers(2)).is_ok());
}

#[test]
fn compaction_threads_run_spawned_tasks() {
    let (mut cfg, _) = sim_config(2);
    cfg.compaction_threads = 2;
    let rt = Runtime::start(cfg, handlers(2)).unwrap();
    for i in 0..4 {
        let (n, w) = completion();
        let task = CountSlices {
            left: 3,
            done: Some(n),
        };
        rt.submitter(ShardId(i % 2))
            .submit(Msg::Spawn(Box::new(task)))
            .unwrap();
        let name = w.wait().unwrap().unwrap();
        assert!(name.starts_with("pigeonhole-compaction-"), "{name}");
    }
    rt.shutdown().unwrap();
}

#[test]
fn shard_threads_are_named_and_handlers_return_in_order() {
    let (cfg, _) = sim_config(3);
    let rt = Runtime::start(cfg, handlers(3)).unwrap();
    assert_eq!(rt.shard_count(), 3);
    let (n, w) = completion();
    let task = CountSlices {
        left: 1,
        done: Some(n),
    };
    rt.submitter(ShardId(2))
        .submit(Msg::Spawn(Box::new(task)))
        .unwrap();
    assert_eq!(w.wait().unwrap().as_deref(), Some("pigeonhole-shard-2"));
    for shard in 0..3 {
        rt.submitter(ShardId(shard))
            .submit(Msg::Record {
                producer: 0,
                seq: u32::from(shard),
            })
            .unwrap();
    }
    let hs = rt.shutdown().unwrap();
    for (i, h) in hs.iter().enumerate() {
        assert_eq!(h.records, [(0, i as u32)]);
    }
}

#[test]
fn drop_without_shutdown_joins_threads() {
    let (cfg, _) = sim_config(2);
    let rt = Runtime::start(cfg, handlers(2)).unwrap();
    let sub = rt.submitter(ShardId(0));
    drop(rt);
    assert!(matches!(
        sub.submit(Msg::Record {
            producer: 0,
            seq: 0
        }),
        Err(Error::Closed)
    ));
}

#[test]
#[should_panic(expected = "handlers.len() must equal config.shards")]
fn handler_count_must_match() {
    let (cfg, _) = sim_config(2);
    let _ = Runtime::start(cfg, handlers(3));
}

#[test]
fn unpinned_start_works() {
    let (mut cfg, _) = sim_config(1);
    cfg.pin_threads = false;
    let rt = Runtime::start(cfg, handlers(1)).unwrap();
    let (n, w) = completion();
    rt.submitter(ShardId(0)).submit(Msg::Echo(3, n)).unwrap();
    assert_eq!(w.wait(), Some(3));
}

#[test]
fn notifier_outlives_shutdown_waiter() {
    // A waiter dropped before notify is fine; a notify after the waiter is gone is fine.
    let (n, w) = completion::<Mutex<u8>>();
    drop(w);
    n.notify(Mutex::new(1));
}

// ---------------------------------------------------------------------------------------
// Application-owned specifics.

fn app_drivers(shards: usize) -> (Vec<ShardDriver<Handler>>, Arc<SimVfs>) {
    let (cfg, sim) = sim_config(shards);
    (
        Runtime::application_owned(cfg, handlers(shards)).unwrap(),
        sim,
    )
}

#[test]
fn set_wakeup_after_first_run_once_is_not_lost() {
    let (mut drivers, _) = app_drivers(1);
    let mut d = drivers.remove(0);
    // Idle before any wakeup is registered: the submit below finds the shard asleep and has
    // nobody to wake.
    assert!(!d.run_once(u64::MAX));
    let (n, w) = completion();
    d.submitter(ShardId(0)).submit(Msg::Echo(9, n)).unwrap();
    let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let f = Arc::clone(&fired);
    d.set_wakeup(Box::new(move || {
        f.fetch_add(1, Ordering::SeqCst);
    }));
    assert!(
        fired.load(Ordering::SeqCst) >= 1,
        "registration must wake once"
    );
    d.run_once(u64::MAX);
    assert_eq!(w.wait(), Some(9));

    // From here on the wakeup fires once per idle period.
    assert!(!d.run_once(u64::MAX));
    let before = fired.load(Ordering::SeqCst);
    let (n1, _w1) = completion();
    let (n2, _w2) = completion();
    d.submitter(ShardId(0)).submit(Msg::Echo(1, n1)).unwrap();
    d.submitter(ShardId(0)).submit(Msg::Echo(2, n2)).unwrap();
    assert_eq!(fired.load(Ordering::SeqCst), before + 1);
}

#[test]
fn dropped_driver_discards_queued_messages() {
    let (mut drivers, _) = app_drivers(2);
    let (n, w) = completion();
    let sub = drivers[0].submitter(ShardId(1));
    sub.submit(Msg::Echo(5, n)).unwrap();
    drop(drivers.remove(1)); // queued Echo dropped on the floor, notifier with it
    assert_eq!(w.wait(), None);
    assert!(matches!(
        sub.submit(Msg::Record {
            producer: 0,
            seq: 0
        }),
        Err(Error::Closed)
    ));
    // Other shards are unaffected.
    let (n, w) = completion();
    drivers[0]
        .submitter(ShardId(0))
        .submit(Msg::Echo(6, n))
        .unwrap();
    drivers[0].run_once(u64::MAX);
    assert_eq!(w.wait(), Some(6));
}

#[test]
fn driver_shutdown_handles_queued_messages() {
    let (mut drivers, _) = app_drivers(1);
    let d = drivers.remove(0);
    let sub = d.submitter(ShardId(0));
    let (n, w) = completion();
    for seq in 0..3000 {
        sub.submit(Msg::Record { producer: 0, seq }).unwrap();
    }
    sub.submit(Msg::Echo(4, n)).unwrap();
    let h = d.shutdown();
    assert_eq!(h.records.len(), 3000);
    assert!(
        h.batches >= 3,
        "a 3001-message backlog spans several capped batches"
    );
    assert_eq!(w.wait(), Some(4));
    assert!(matches!(
        sub.submit(Msg::Record {
            producer: 0,
            seq: 0
        }),
        Err(Error::Closed)
    ));
}

/// `run_once(u64::MAX)` with a long-running task: one call runs the task across many slices
/// and still handles each mid-slice message within one slice.
#[test]
fn run_once_unbounded_deadline_keeps_latency_bounded() {
    use pigeonhole_io::Vfs;
    let (mut drivers, sim) = app_drivers(1);
    let mut d = drivers.remove(0);
    let unit = 20_000;
    let (en, ew) = completion();
    let task = SimBusy {
        sim: Arc::clone(&sim),
        me: d.submitter(ShardId(0)),
        unit,
        every: 5,
        units: 0,
        total: 5 * 2000, // 200 ms of simulated work: 400 slices of 500 µs
        done: Some(en),
    };
    d.submitter(ShardId(0))
        .submit(Msg::Spawn(Box::new(task)))
        .unwrap();
    let start = sim.monotonic_nanos();
    assert!(
        !d.run_once(u64::MAX),
        "one call runs the task to completion"
    );
    assert!(ew.wait().is_some());
    assert!(sim.monotonic_nanos() - start >= 200_000_000);
    let h = d.shutdown();
    let max = h.latencies.iter().copied().max().unwrap();
    assert_eq!(h.latencies.len(), 2000);
    assert!(max <= 500_000 + unit, "max latency {max} ns over one slice");
    assert!(
        h.batches >= 300,
        "only {} batches: foreground did not interleave",
        h.batches
    );
}
