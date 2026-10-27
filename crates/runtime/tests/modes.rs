//! One suite, run in both embedding modes: engine-owned (`Runtime::start`) and
//! application-owned (`ShardDriver::run_once` driven from threads the test owns).

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{Vfs, VfsRef};
use pigeonhole_runtime::{
    Error, Notifier, Runtime, RuntimeConfig, ShardContext, ShardDriver, ShardHandler, ShardId,
    Submitter, Task, TaskPoll, TaskWaker, WorkerPool, completion,
};

#[derive(Debug, Clone, Copy)]
enum Mode {
    Engine,
    /// Engine-owned with the shards on this many driver threads (D289).
    EngineDrivers(usize),
    /// Engine-owned with drivers sized by the controller (D289 PR 2), and a thread forcing
    /// the target through 1, 2, 3 and back every millisecond, so shards keep moving between
    /// drivers under the test's traffic.
    EngineChurn,
    App,
    /// Application-owned with this many background workers (D284), each driven by a thread
    /// of the test's.
    AppWorkers(usize),
}

/// A running runtime in either mode, behind one interface.
enum Harness<H: ShardHandler> {
    Engine(Runtime<H>),
    Churn {
        rt: Runtime<H>,
        stop: Arc<AtomicBool>,
        churn: JoinHandle<()>,
    },
    App {
        submitters: Vec<Submitter<H::Msg>>,
        stop: Arc<AtomicBool>,
        threads: Vec<JoinHandle<ShardDriver<H>>>,
        /// Background workers (D284): their pool and their threads.
        workers: Option<(WorkerPool, Vec<JoinHandle<()>>)>,
    },
}

impl<H: ShardHandler> Harness<H> {
    fn start(mode: Mode, config: RuntimeConfig, handlers: Vec<H>) -> Self {
        match mode {
            Mode::Engine => Harness::Engine(Runtime::start(config, handlers).unwrap()),
            Mode::EngineDrivers(k) => {
                let mut config = config;
                config.shard_threads = k;
                Harness::Engine(Runtime::start(config, handlers).unwrap())
            }
            Mode::EngineChurn => {
                let mut config = config;
                config.adaptive_drivers = true;
                let rt = Runtime::start(config, handlers).unwrap();
                let force = rt.driver_control();
                let stop = Arc::new(AtomicBool::new(false));
                let churn = {
                    let stop = Arc::clone(&stop);
                    thread::spawn(move || {
                        let mut k = 1;
                        while !stop.load(Ordering::Acquire) {
                            force(Some(k));
                            k = k % 3 + 1;
                            thread::sleep(Duration::from_millis(1));
                        }
                    })
                };
                Harness::Churn { rt, stop, churn }
            }
            Mode::App | Mode::AppWorkers(_) => {
                let slice = config.time_slice.as_nanos() as u64;
                let vfs = Arc::clone(&config.vfs);
                let mut config = config;
                config.compaction_threads = match mode {
                    Mode::AppWorkers(k) => k,
                    _ => 0,
                };
                let owned = Runtime::application_owned_with_workers(config, handlers).unwrap();
                let workers = owned.pool.map(|pool| {
                    let threads = owned
                        .workers
                        .into_iter()
                        .map(|mut w| {
                            thread::spawn(move || {
                                while !w.closed() {
                                    if !w.run_once(u64::MAX) {
                                        w.wait(Duration::from_millis(10));
                                    }
                                }
                            })
                        })
                        .collect();
                    (pool, threads)
                });
                let drivers = owned.shards;
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
                                // Until a wakeup, or the earliest sleeping task's deadline
                                // (re-checked every 10 ms: the simulated clock moves without
                                // waking anyone).
                                match d.next_deadline() {
                                    None => thread::park(),
                                    Some(at) => thread::park_timeout(
                                        Duration::from_nanos(
                                            at.saturating_sub(vfs.monotonic_nanos()),
                                        )
                                        .min(Duration::from_millis(10)),
                                    ),
                                }
                            }
                        })
                    })
                    .collect();
                Harness::App {
                    submitters,
                    stop,
                    threads,
                    workers,
                }
            }
        }
    }

    fn submitter(&self, shard: u16) -> Submitter<H::Msg> {
        match self {
            Harness::Engine(rt) | Harness::Churn { rt, .. } => rt.submitter(ShardId(shard)),
            Harness::App { submitters, .. } => submitters[usize::from(shard)].clone(),
        }
    }

    /// Stops the runtime after it handles everything queued; inspects each handler.
    fn finish<R>(self, f: impl Fn(&mut H) -> R) -> Vec<R> {
        match self {
            Harness::Engine(rt) => rt.shutdown().unwrap().iter_mut().map(f).collect(),
            Harness::Churn { rt, stop, churn } => {
                stop.store(true, Ordering::Release);
                churn.join().unwrap();
                rt.shutdown().unwrap().iter_mut().map(f).collect()
            }
            Harness::App {
                stop,
                threads,
                workers,
                ..
            } => {
                // The workers' tasks go back to the shards, which finish them below.
                if let Some((pool, workers)) = workers {
                    pool.close();
                    workers.into_iter().for_each(|w| w.join().unwrap());
                }
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
    /// Replies with the counter's value when the shard handles it (#960: how many background
    /// slices finished while the message was queued).
    SliceProbe(Arc<AtomicU64>, Notifier<u64>),
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
            Msg::SliceProbe(slices, n) => n.notify(slices.load(Ordering::SeqCst)),
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
    /// Slices finished.
    slices: Arc<AtomicU64>,
}

impl Task for RealBusy {
    fn run(&mut self, deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        while self.vfs.monotonic_nanos() < deadline {
            std::hint::spin_loop();
        }
        self.slices.fetch_add(1, Ordering::SeqCst);
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
/// foreground message for more than the slice it is in. Judged in slices, not wall time
/// (#960): a loaded runner can keep the shard thread, which spins, off its CPU for tens of
/// milliseconds (a p99 of 69 ms on macOS CI against a plain thread's 5 ms), which is the
/// machine's doing, not the shard holding a message behind its background task. The slice
/// count is what the runtime controls: it checks its queue between slices, so a message
/// waits for at most the one in progress, however long the OS stretches it: it drains after
/// every slice, and a sent message is visible to the next drain (`std::sync::mpsc` claims its
/// slot before writing, and a receive that sees the claim waits for the write).
fn foreground_latency_bounded_real(mode: Mode) {
    let vfs: VfsRef = PreadVfs::new(1);
    let mut cfg = config(1, Arc::clone(&vfs));
    cfg.time_slice = Duration::from_millis(1);
    let h = Harness::start(mode, cfg, handlers(1));
    let stop = Arc::new(AtomicBool::new(false));
    let slices = Arc::new(AtomicU64::new(0));
    let busy = RealBusy {
        vfs,
        stop: Arc::clone(&stop),
        slices: Arc::clone(&slices),
    };
    h.submitter(0).submit(Msg::Spawn(Box::new(busy))).unwrap();
    let mut waited = Vec::new();
    for _ in 0..200 {
        let (n, w) = completion();
        h.submitter(0)
            .submit(Msg::SliceProbe(Arc::clone(&slices), n))
            .unwrap();
        // Counted from once the message is in the queue (`submit` returned): read before
        // the submit, a preemption between the read and the send let the shard finish one
        // slice and start the next with the queue still empty, and the message's one slice
        // of waiting counted as two (#960, macOS CI). Read after, the count can only fall
        // short of the wait (the shard may run on before this read), never exceed it.
        let enqueued = slices.load(Ordering::SeqCst);
        let handled = w.wait().expect("handled");
        waited.push(handled.saturating_sub(enqueued));
        thread::sleep(Duration::from_micros(300));
    }
    stop.store(true, Ordering::Relaxed);
    h.finish(|_| ());
    let max = waited.iter().copied().max().unwrap_or(0);
    assert!(
        max <= 1,
        "a foreground message waited {max} background slices (at most the one in progress)"
    );
    // Not vacuous: the background task ran, and messages landed mid-slice and waited for it.
    assert!(
        slices.load(Ordering::SeqCst) > 0,
        "the background task never ran"
    );
    let mid_slice = waited.iter().filter(|&&w| w == 1).count();
    assert!(
        mid_slice >= 10,
        "only {mid_slice} of 200 messages waited a slice: the background task was not busy"
    );
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

/// Sleeps until `at` on the VFS clock (returning to sleep if run early), counting its runs;
/// reports the clock when it fired. Woken through its waker, it finishes at once.
struct Alarm {
    vfs: VfsRef,
    at: u64,
    runs: Arc<AtomicU32>,
    waker_out: Option<Notifier<TaskWaker>>,
    done: Option<Notifier<u64>>,
}

impl Task for Alarm {
    fn run(&mut self, _deadline: u64, waker: &TaskWaker) -> TaskPoll {
        let runs = self.runs.fetch_add(1, Ordering::Relaxed);
        if let Some(out) = self.waker_out.take() {
            out.notify(waker.clone());
        } else if runs > 0 && self.at == u64::MAX {
            // Woken early on purpose (the deadline never comes).
            if let Some(d) = self.done.take() {
                d.notify(self.vfs.monotonic_nanos());
            }
            return TaskPoll::Done;
        }
        let now = self.vfs.monotonic_nanos();
        if now >= self.at {
            if let Some(d) = self.done.take() {
                d.notify(now);
            }
            return TaskPoll::Done;
        }
        TaskPoll::SleepUntil(self.at)
    }

    fn name(&self) -> &'static str {
        "alarm"
    }
}

/// On a real clock a sleeping task runs once its deadline passes, and not in between: the
/// shard parks instead of polling (issue #89).
fn sleeping_task_fires_on_time_real(mode: Mode) {
    let vfs: VfsRef = PreadVfs::new(1);
    let h = Harness::start(mode, config(1, Arc::clone(&vfs)), handlers(1));
    let wait = Duration::from_millis(300);
    let at = vfs.monotonic_nanos() + wait.as_nanos() as u64;
    let runs = Arc::new(AtomicU32::new(0));
    let (n, w) = completion();
    let task = Alarm {
        vfs: Arc::clone(&vfs),
        at,
        runs: Arc::clone(&runs),
        waker_out: None,
        done: Some(n),
    };
    h.submitter(0).submit(Msg::Spawn(Box::new(task))).unwrap();
    // A plain thread sleeping to the same deadline: how late this machine wakes a parked
    // thread right now. A loaded runner can wake any thread hundreds of milliseconds late
    // (main's macOS CI: 263 ms), which is not the shard's lateness.
    let reference = {
        let vfs = Arc::clone(&vfs);
        thread::spawn(move || {
            loop {
                let now = vfs.monotonic_nanos();
                if now >= at {
                    return now;
                }
                thread::sleep(Duration::from_nanos(at - now));
            }
        })
    };
    let fired = w.wait().unwrap();
    let reference_late = Duration::from_nanos(reference.join().unwrap() - at);
    assert!(fired >= at, "fired before its deadline");
    let late = Duration::from_nanos(fired - at);
    assert!(
        late < Duration::from_millis(200) + reference_late,
        "fired {late:?} late (a plain sleeping thread woke {reference_late:?} late)"
    );
    // A polling timer runs millions of times in 300 ms; a parked one a handful (the 10 ms
    // re-check runs it only while the clock has not moved).
    let runs = runs.load(Ordering::Relaxed);
    assert!(runs < 100, "the sleeping task ran {runs} times");
    h.finish(|_| ());
}

/// A sleeping task's waker ends the sleep before its deadline.
fn sleeping_task_is_woken_early(mode: Mode) {
    let (cfg, sim) = sim_config(1);
    let h = Harness::start(mode, cfg, handlers(1));
    let (wn, ww) = completion();
    let (dn, dw) = completion();
    let task = Alarm {
        vfs: sim.clone(),
        at: u64::MAX,
        runs: Arc::new(AtomicU32::new(0)),
        waker_out: Some(wn),
        done: Some(dn),
    };
    h.submitter(0).submit(Msg::Spawn(Box::new(task))).unwrap();
    let waker = ww.wait().unwrap();
    thread::sleep(Duration::from_millis(5)); // let it go to sleep
    waker.wake();
    assert!(dw.wait().is_some());
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

/// Application-owned with 1 and 2 background workers (D284): the suite's tests whose
/// background tasks may run anywhere. The foreground-latency tests are left out: they
/// measure how long a message waits behind background slices on the shard, and with
/// workers none run there.
macro_rules! with_workers {
    ($($name:ident),* $(,)?) => {
        mod application_owned_one_worker {
            $(#[test] fn $name() { super::$name(super::Mode::AppWorkers(1)) })*
        }
        mod application_owned_two_workers {
            $(#[test] fn $name() { super::$name(super::Mode::AppWorkers(2)) })*
        }
    };
}

with_workers!(
    echo_sync_and_async,
    per_producer_fifo_and_batching,
    cross_shard_forwarding,
    current_shard_is_set_on_shard_threads,
    background_task_runs_to_completion,
    blocked_task_is_woken,
    shutdown_handles_queued_then_refuses,
    dropped_notifier_resolves_none,
    many_round_trips_no_lost_wakeup,
    sleeping_task_fires_on_time_real,
    sleeping_task_is_woken_early,
);

/// Engine-owned with every shard on one driver thread, and with two (D289): the whole
/// suite, since each test's shards then share a thread's turns, spin and park.
macro_rules! with_drivers {
    ($($name:ident),* $(,)?) => {
        mod engine_owned_one_driver {
            $(#[test] fn $name() { super::$name(super::Mode::EngineDrivers(1)) })*
        }
        mod engine_owned_two_drivers {
            $(#[test] fn $name() { super::$name(super::Mode::EngineDrivers(2)) })*
        }
    };
}

/// The suite with shards moving between drivers throughout (D289 PR 2's moves).
macro_rules! with_churn {
    ($($name:ident),* $(,)?) => {
        mod engine_owned_churning_drivers {
            $(#[test] fn $name() { super::$name(super::Mode::EngineChurn) })*
        }
    };
}

with_churn!(
    echo_sync_and_async,
    per_producer_fifo_and_batching,
    cross_shard_forwarding,
    current_shard_is_set_on_shard_threads,
    background_task_runs_to_completion,
    blocked_task_is_woken,
    shutdown_handles_queued_then_refuses,
    dropped_notifier_resolves_none,
    many_round_trips_no_lost_wakeup,
    sleeping_task_fires_on_time_real,
    sleeping_task_is_woken_early,
);

with_drivers!(
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
    sleeping_task_fires_on_time_real,
    sleeping_task_is_woken_early,
);

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
    sleeping_task_fires_on_time_real,
    sleeping_task_is_woken_early,
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

/// Advances the simulated clock by `unit` on each of `left` slices (a pool task whose time
/// shows on the clock), then reports. Until `go` is set it blocks, leaving its waker in
/// `waker` for the test to release it: the clock is shared, so a slice that ran while the
/// spawning shard's turn was still open would show in that turn's time too (the Windows
/// runner's 3 ms, #742).
struct SimSlices {
    sim: Arc<SimVfs>,
    unit: u64,
    left: u32,
    done: Option<Notifier<()>>,
    go: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<TaskWaker>>>,
}

impl Task for SimSlices {
    fn run(&mut self, _deadline: u64, waker: &TaskWaker) -> TaskPoll {
        if !self.go.load(Ordering::Acquire) {
            *self.waker.lock().unwrap() = Some(waker.clone());
            return TaskPoll::Blocked;
        }
        self.sim.advance(self.unit);
        self.left -= 1;
        if self.left > 0 {
            return TaskPoll::Pending;
        }
        self.done.take().unwrap().notify(());
        TaskPoll::Done
    }
    fn name(&self) -> &'static str {
        "sim-slices"
    }
}

/// A pool thread counts each slice's time to the shard that spawned the task (D256): three
/// 1 ms slices spawned from shard 0 show as at least 3 ms of shard 0's pool busy time, and
/// none of shard 1's; the shard's own turn time is not what grew. The slices run only once
/// shard 0 has parked after the turn that spawned them: a turn is the clock's rise across
/// it, and on the shared simulated clock a slice advancing it inside an open turn would
/// count there as well (on a real clock a turn is the thread's own wall time, which the
/// pool's concurrent work does not lengthen).
#[test]
fn pool_slices_count_to_the_spawning_shard() {
    let (mut cfg, sim) = sim_config(2);
    cfg.compaction_threads = 1;
    let rt = Runtime::start(cfg, handlers(2)).unwrap();
    let turns_before = rt.submitter(ShardId(0)).busy_nanos();
    let parks_before = rt.submitter(ShardId(0)).idle_counts().0;
    let go = Arc::new(AtomicBool::new(false));
    let waker = Arc::new(Mutex::new(None));
    let (n, w) = completion();
    let task = SimSlices {
        sim: sim.clone(),
        unit: 1_000_000,
        left: 3,
        done: Some(n),
        go: go.clone(),
        waker: waker.clone(),
    };
    rt.submitter(ShardId(0))
        .submit(Msg::Spawn(Box::new(task)))
        .unwrap();
    // The spawning turn has closed (its time counted) once shard 0 parks again, and the
    // task has blocked once its waker is in place: then let the slices advance the clock.
    let deadline = Instant::now() + Duration::from_secs(5);
    while (rt.submitter(ShardId(0)).idle_counts().0 == parks_before
        || waker.lock().unwrap().is_none())
        && Instant::now() < deadline
    {
        thread::yield_now();
    }
    assert!(
        rt.submitter(ShardId(0)).idle_counts().0 > parks_before,
        "shard 0 did not park after the spawn"
    );
    go.store(true, Ordering::Release);
    waker
        .lock()
        .unwrap()
        .take()
        .expect("the task blocked")
        .wake();
    w.wait().unwrap();
    // The last slice's time is added after the task's notify: wait for it to land.
    let deadline = Instant::now() + Duration::from_secs(5);
    while rt.submitter(ShardId(0)).pool_busy_nanos() < 3_000_000 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert!(
        rt.submitter(ShardId(0)).pool_busy_nanos() >= 3_000_000,
        "{}",
        rt.submitter(ShardId(0)).pool_busy_nanos()
    );
    assert_eq!(rt.submitter(ShardId(1)).pool_busy_nanos(), 0);
    // The shard's own turn that handled the spawn did not advance the clock.
    assert_eq!(rt.submitter(ShardId(0)).busy_nanos(), turns_before);
    rt.shutdown().unwrap();
}

/// A shard thread counts its spin before parking (D198) beside its turns (D256): with the
/// engine's 50 µs window, after a handled message the thread polls on the real clock, and
/// that time shows in `spin_nanos`, not in `busy_nanos`. With the runtime's default window
/// of zero no poll runs and nothing is counted: a skipped poll adds nothing (on Windows
/// two consecutive clock reads can be equal, so timing the skip read zero there and a few
/// nanoseconds elsewhere).
#[test]
fn a_shard_counts_its_spin_before_parking() {
    for (window, spins) in [(Duration::ZERO, false), (Duration::from_micros(50), true)] {
        let mut cfg = config(1, PreadVfs::new(1));
        cfg.idle_spin = window;
        let rt = Runtime::start(cfg, handlers(1)).unwrap();
        for i in 0..8 {
            let (n, w) = completion();
            rt.submitter(ShardId(0)).submit(Msg::Echo(i, n)).unwrap();
            assert_eq!(w.wait().unwrap(), i);
            thread::sleep(Duration::from_millis(1));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while spins && rt.submitter(ShardId(0)).spin_nanos() == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        let spun = rt.submitter(ShardId(0)).spin_nanos();
        assert_eq!(
            spun > 0,
            spins,
            "window {window:?}: spin counted {spun} ns after eight handled messages"
        );
        rt.shutdown().unwrap();
    }
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

/// D289: with `shard_threads = 2`, five shards run on two driver threads, shard `i` on
/// driver `i % 2`, and shutdown still returns the handlers in shard order. A value above
/// the shard count means a thread per shard, as before.
#[test]
fn drivers_hold_shards_round_robin_and_handlers_return_in_order() {
    for (k, names) in [
        (2, ["pigeonhole-driver-0", "pigeonhole-driver-1"].as_slice()),
        (
            9,
            [
                "pigeonhole-shard-0",
                "pigeonhole-shard-1",
                "pigeonhole-shard-2",
                "pigeonhole-shard-3",
                "pigeonhole-shard-4",
            ]
            .as_slice(),
        ),
    ] {
        let (mut cfg, _) = sim_config(5);
        cfg.shard_threads = k;
        let rt = Runtime::start(cfg, handlers(5)).unwrap();
        for shard in 0..5u16 {
            let (n, w) = completion();
            let task = CountSlices {
                left: 1,
                done: Some(n),
            };
            rt.submitter(ShardId(shard))
                .submit(Msg::Spawn(Box::new(task)))
                .unwrap();
            assert_eq!(
                w.wait().unwrap().as_deref(),
                Some(names[usize::from(shard) % names.len()]),
                "shard_threads {k}, shard {shard}"
            );
            rt.submitter(ShardId(shard))
                .submit(Msg::Record {
                    producer: 0,
                    seq: u32::from(shard),
                })
                .unwrap();
        }
        let hs = rt.shutdown().unwrap();
        assert_eq!(hs.len(), 5);
        for (i, h) in hs.iter().enumerate() {
            assert_eq!(h.records, [(0, i as u32)], "shard_threads {k}");
        }
    }
}

/// D289: one driver spins once for all its shards and charges the spin to the shards
/// that handled a message since its last park (D256), so a shard nobody sent to counts
/// none.
#[test]
fn a_driver_charges_its_spin_to_the_shards_that_worked() {
    let mut cfg = config(2, PreadVfs::new(1));
    cfg.idle_spin = Duration::from_micros(50);
    cfg.shard_threads = 1;
    let rt = Runtime::start(cfg, handlers(2)).unwrap();
    for i in 0..8 {
        let (n, w) = completion();
        rt.submitter(ShardId(0)).submit(Msg::Echo(i, n)).unwrap();
        assert_eq!(w.wait().unwrap(), i);
        thread::sleep(Duration::from_millis(1));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while rt.submitter(ShardId(0)).spin_nanos() == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        rt.submitter(ShardId(0)).spin_nanos() > 0,
        "shard 0 worked and spun"
    );
    assert_eq!(
        rt.submitter(ShardId(1)).spin_nanos(),
        0,
        "shard 1 never worked"
    );
    rt.shutdown().unwrap();
}

/// D289 PR 2: shards move between drivers under traffic. With the target forced through
/// 1, 2 and 4 drivers while every shard echoes round trips, shards are handed over many
/// times, every reply arrives, and shutdown returns every handler in shard order.
#[test]
fn shards_move_between_drivers_under_traffic() {
    let (mut cfg, _) = sim_config(4);
    cfg.adaptive_drivers = true;
    let rt = Runtime::start(cfg, handlers(4)).unwrap();
    let force = rt.driver_control();
    for round in 0..300u64 {
        force(Some([1, 2, 4][(round % 3) as usize]));
        for shard in 0..4u16 {
            let (n, w) = completion();
            rt.submitter(ShardId(shard))
                .submit(Msg::Echo(round, n))
                .unwrap();
            assert_eq!(w.wait().unwrap(), round, "shard {shard}, round {round}");
        }
    }
    assert!(rt.driver_moves() > 100, "only {} moves", rt.driver_moves());
    force(None);
    for shard in 0..4u16 {
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

#[test]
fn run_once_is_idle_with_only_sleeping_tasks() {
    // Application-owned: a sleeping task is not work that remains; `next_deadline` reports
    // it, and it runs once the clock reaches it.
    let (mut drivers, sim) = app_drivers(1);
    let d = &mut drivers[0];
    let at = sim.monotonic_nanos() + 1_000_000;
    let runs = Arc::new(AtomicU32::new(0));
    let (n, w) = completion();
    let task = Alarm {
        vfs: sim.clone(),
        at,
        runs: Arc::clone(&runs),
        waker_out: None,
        done: Some(n),
    };
    d.with_handler(|_, ctx| ctx.spawn(Box::new(task)));
    assert!(!d.run_once(u64::MAX));
    assert_eq!(d.next_deadline(), Some(at));
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    sim.advance(400_000);
    assert!(!d.run_once(u64::MAX));
    assert_eq!(runs.load(Ordering::Relaxed), 1, "ran before its deadline");
    sim.advance(600_000);
    assert!(!d.run_once(u64::MAX));
    assert_eq!(w.wait(), Some(at));
    assert_eq!(d.next_deadline(), None);
}

#[test]
fn a_clock_that_does_not_move_runs_sleepers_early() {
    // A simulated clock nobody advances: the next `run_once` at the same reading runs the
    // sleeping task early, so it can notice the clock is stuck (decision D126's timers).
    let (mut drivers, sim) = app_drivers(1);
    let d = &mut drivers[0];
    let runs = Arc::new(AtomicU32::new(0));
    let task = Alarm {
        vfs: sim.clone(),
        at: sim.monotonic_nanos() + 1_000_000,
        runs: Arc::clone(&runs),
        waker_out: None,
        done: None,
    };
    d.with_handler(|_, ctx| ctx.spawn(Box::new(task)));
    assert!(!d.run_once(u64::MAX));
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert!(!d.run_once(u64::MAX));
    assert_eq!(runs.load(Ordering::Relaxed), 2);
    // A moving clock does not: the task sleeps until its deadline.
    sim.advance(1_000);
    assert!(!d.run_once(u64::MAX));
    assert_eq!(runs.load(Ordering::Relaxed), 2);
}

// ---------------------------------------------------------------------------------------
// `ShardDriver::wait` (#553, ICR 0035).

/// An idle shard whose thread waits; `wake` (on another thread, after 50 ms) must end the
/// wait long before its 30 s timeout.
fn wait_is_ended_by(wake: impl FnOnce(&ShardDriver<Handler>) -> Box<dyn FnOnce() + Send>) {
    let (mut drivers, _) = app_drivers(1);
    let mut d = drivers.remove(0);
    assert!(!d.run_once(u64::MAX));
    let fire = wake(&d);
    let other = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        fire();
    });
    let started = Instant::now();
    d.wait(Duration::from_secs(30));
    let waited = started.elapsed();
    other.join().unwrap();
    assert!(waited < Duration::from_secs(5), "the wait ran {waited:?}");
}

#[test]
fn a_message_ends_the_wait() {
    wait_is_ended_by(|d| {
        let s = d.submitter(ShardId(0));
        Box::new(move || {
            let (n, _w) = completion();
            s.submit(Msg::Echo(1, n)).unwrap();
        })
    });
}

#[test]
fn a_waker_ends_the_wait() {
    wait_is_ended_by(|d| {
        let w = d.waker();
        Box::new(move || w.wake())
    });
}

/// A background task that wakes the application's waker once, as a shard resolving a commit
/// the driving thread polls does, during that thread's own turn.
struct WakeTheDriver(Option<std::task::Waker>);

impl Task for WakeTheDriver {
    fn run(&mut self, _deadline_nanos: u64, _waker: &TaskWaker) -> TaskPoll {
        if let Some(w) = self.0.take() {
            w.wake();
        }
        TaskPoll::Done
    }

    fn name(&self) -> &'static str {
        "wake-the-driver"
    }
}

#[test]
fn a_waker_wake_during_a_turn_ends_the_next_wait() {
    // #592: the wake arrives inside `run_once`, which then collects woken background tasks
    // (clearing their flag). The application's wake must survive that: the next wait returns
    // at once, rather than sleeping until a completion or its timeout.
    let (mut drivers, _) = app_drivers(1);
    let mut d = drivers.remove(0);
    assert!(!d.run_once(u64::MAX));
    let w = d.waker();
    d.with_handler(|_, ctx| ctx.spawn(Box::new(WakeTheDriver(Some(w)))));
    d.run_once(u64::MAX);
    let started = Instant::now();
    d.wait(Duration::from_secs(30));
    let waited = started.elapsed();
    assert!(waited < Duration::from_secs(5), "the wait ran {waited:?}");
    // The wake is spent. A wait may return early or spuriously (the OS timer, 15.6 ms on
    // Windows), so wait out 100 ms in a loop: a wake left set would end every wait at once,
    // thousands of times; a spent one lets each sleep, a handful of times.
    assert!(!d.run_once(u64::MAX));
    let deadline = Instant::now() + Duration::from_millis(100);
    let mut waits = 0;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        d.wait(left);
        waits += 1;
    }
    assert!(
        waits < 100,
        "{waits} waits in 100 ms: the wake was not spent"
    );
}

/// Background work that is always runnable: each slice counts itself and moves the simulated
/// clock to the slice's deadline.
struct AlwaysRunnable {
    sim: Arc<SimVfs>,
    slices: Arc<AtomicU32>,
}

impl Task for AlwaysRunnable {
    fn run(&mut self, deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        self.slices.fetch_add(1, Ordering::Relaxed);
        let now = self.sim.monotonic_nanos();
        self.sim.advance(deadline.saturating_sub(now).max(1));
        TaskPoll::Pending
    }

    fn name(&self) -> &'static str {
        "count-slices"
    }
}

#[test]
fn a_turn_that_settles_a_driver_future_skips_its_slices_at_most_eight_in_a_row() {
    // D237: a message that wakes a future the driving thread awaits (a commit it submitted
    // settling) ends the turn before its background slices, but never more than eight turns
    // in a row: under a steady stream of settles, background work still runs every ninth.
    let (mut drivers, sim) = app_drivers(1);
    let mut d = drivers.remove(0);
    let slices = Arc::new(AtomicU32::new(0));
    d.with_handler(|_, ctx| {
        ctx.spawn(Box::new(AlwaysRunnable {
            sim: sim.clone(),
            slices: Arc::clone(&slices),
        }));
    });
    let waker = d.waker();
    let mut cx = Context::from_waker(&waker);
    let mut turn = |d: &mut ShardDriver<Handler>, settle: bool| -> u32 {
        let before = slices.load(Ordering::Relaxed);
        let mut pending = None;
        if settle {
            let (n, w) = completion();
            let mut w = Box::pin(w);
            assert!(w.as_mut().poll(&mut cx).is_pending());
            d.submitter(ShardId(0)).submit(Msg::Echo(7, n)).unwrap();
            pending = Some(w);
        }
        assert!(
            d.run_once(sim.monotonic_nanos() + 1_000_000),
            "always runnable"
        );
        if let Some(mut w) = pending {
            assert_eq!(w.as_mut().poll(&mut cx), Poll::Ready(Some(7)));
        }
        slices.load(Ordering::Relaxed) - before
    };
    for round in 0..5 {
        for i in 0..8 {
            assert_eq!(turn(&mut d, true), 0, "round {round}, turn {i}: skipped");
        }
        assert!(
            turn(&mut d, true) > 0,
            "round {round}, ninth turn: slices ran"
        );
    }
    // A turn that settles nothing runs its slices, even right after one that skipped them.
    assert_eq!(turn(&mut d, true), 0);
    assert!(turn(&mut d, false) > 0);
    // A skipped turn leaves the application wake for the next wait (#592): it returns at once.
    assert_eq!(turn(&mut d, true), 0);
    let started = Instant::now();
    d.wait(Duration::from_secs(30));
    let waited = started.elapsed();
    assert!(waited < Duration::from_secs(5), "the wait ran {waited:?}");
}

#[test]
fn work_queued_before_the_wait_returns_it_at_once() {
    let (mut drivers, _) = app_drivers(1);
    let mut d = drivers.remove(0);
    assert!(!d.run_once(u64::MAX));
    let (n, w) = completion();
    // Submitted while the shard was idle with nobody to wake: only the wait's re-check
    // finds it.
    d.submitter(ShardId(0)).submit(Msg::Echo(4, n)).unwrap();
    let started = Instant::now();
    d.wait(Duration::from_secs(30));
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(!d.run_once(u64::MAX));
    assert_eq!(w.wait(), Some(4));
}

#[test]
fn the_wakeup_callback_still_fires_beside_a_wait() {
    // A submit while the driver waits fires the wakeup callback as well as ending the wait.
    // `wait` may return spuriously (its contract; `park_timeout` does on Windows, #867), and
    // a submit that then finds the driver awake rightly calls nothing: the message waits for
    // the next turn. So the test waits again until the submit has happened, and repeats the
    // scenario a few times: a callback that never fires still fails every attempt, while a
    // spurious return just before the submit (a window of a few instructions) fails one.
    let (mut drivers, _) = app_drivers(1);
    let mut d = drivers.remove(0);
    let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let f = Arc::clone(&fired);
    d.set_wakeup(Box::new(move || {
        f.fetch_add(1, Ordering::SeqCst);
    }));
    let mut called = false;
    for _attempt in 0..5 {
        while d.run_once(u64::MAX) {}
        let before = fired.load(Ordering::SeqCst);
        let s = d.submitter(ShardId(0));
        let submitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let other = {
            let submitted = Arc::clone(&submitted);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                let (n, _w) = completion();
                s.submit(Msg::Echo(2, n)).unwrap();
                submitted.store(true, Ordering::SeqCst);
            })
        };
        while !submitted.load(Ordering::SeqCst) {
            d.wait(Duration::from_secs(30));
        }
        other.join().unwrap();
        if fired.load(Ordering::SeqCst) > before {
            called = true;
            break;
        }
    }
    assert!(called, "the callback was not called in any of 5 attempts");
}
