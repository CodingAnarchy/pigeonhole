//! Background workers in application-owned mode (D284): who runs a task, and how tasks come
//! back to their shard when the application stops driving a worker.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use pigeonhole_io::Vfs;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_runtime::{
    AppOwned, PoolDriver, Runtime, RuntimeConfig, ShardContext, ShardDriver, ShardHandler, Task,
    TaskPoll, TaskWaker,
};

struct Idle;

impl ShardHandler for Idle {
    type Msg = ();
    fn handle(&mut self, _ctx: &mut ShardContext<'_, ()>, _msg: ()) {}
    fn end_batch(&mut self, _ctx: &mut ShardContext<'_, ()>) {}
}

/// What a test task saw.
#[derive(Default)]
struct Seen {
    /// Slices run on a worker (inside its inline scope) and on the shard.
    on_worker: AtomicU32,
    on_shard: AtomicU32,
    /// `reclaim` calls, and whether one said the task failed.
    reclaims: AtomicU32,
    failed: AtomicBool,
    done: AtomicBool,
}

/// Runs `steps` slices, each `Pending` but the last.
struct Steps {
    left: u32,
    room: bool,
    offload: bool,
    /// Panics in this slice (counting down from `left`).
    panic_at: Option<u32>,
    /// Waits here twice in its first slice: once to say it runs, once to be let go.
    gate: Option<Arc<Barrier>>,
    seen: Arc<Seen>,
}

impl Steps {
    fn new(left: u32, seen: &Arc<Seen>) -> Self {
        Self {
            left,
            room: false,
            offload: true,
            panic_at: None,
            gate: None,
            seen: Arc::clone(seen),
        }
    }
}

impl Task for Steps {
    fn run(&mut self, _deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        if pigeonhole_io::in_inline_io() {
            self.seen.on_worker.fetch_add(1, Ordering::SeqCst);
        } else {
            self.seen.on_shard.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(gate) = self.gate.take() {
            gate.wait();
            gate.wait();
        }
        if self.panic_at == Some(self.left) {
            panic!("test task panics in its slice");
        }
        self.left -= 1;
        if self.left == 0 {
            self.seen.done.store(true, Ordering::SeqCst);
            TaskPoll::Done
        } else {
            TaskPoll::Pending
        }
    }

    fn name(&self) -> &'static str {
        "steps"
    }

    fn commit_path(&self) -> bool {
        self.room
    }

    fn offload(&self) -> bool {
        self.offload
    }

    fn reclaim(&mut self, failed: bool) -> bool {
        self.seen.reclaims.fetch_add(1, Ordering::SeqCst);
        if failed {
            self.seen.failed.store(true, Ordering::SeqCst);
            return false;
        }
        true
    }
}

fn open(workers: usize) -> (Arc<SimVfs>, AppOwned<Idle>) {
    let vfs = SimVfs::new(7);
    let mut config = RuntimeConfig::new(vfs.clone());
    config.shards = 1;
    config.compaction_threads = workers;
    let owned = Runtime::application_owned_with_workers(config, vec![Idle]).unwrap();
    (vfs, owned)
}

fn spawn(shard: &mut ShardDriver<Idle>, task: Steps) {
    shard.with_handler(|_, ctx| ctx.spawn(Box::new(task)));
}

/// One worker slice (a deadline already due ends the turn after one slice).
fn one_slice(worker: &mut PoolDriver, vfs: &SimVfs) -> bool {
    worker.run_once(vfs.monotonic_nanos())
}

#[test]
fn workers_run_offloaded_tasks_and_shards_keep_the_rest() {
    let (vfs, mut owned) = open(2);
    let shard = &mut owned.shards[0];
    let (a, b) = (Arc::new(Seen::default()), Arc::new(Seen::default()));
    spawn(shard, Steps::new(3, &a));
    let mut timer = Steps::new(1, &b);
    timer.offload = false;
    spawn(shard, timer);
    // The shard runs only what does not offload.
    assert!(!shard.run_once(u64::MAX));
    assert_eq!(b.on_shard.load(Ordering::SeqCst), 1);
    assert_eq!(a.on_shard.load(Ordering::SeqCst), 0);
    let worker = &mut owned.workers[0];
    while worker.run_once(u64::MAX) {}
    assert!(a.done.load(Ordering::SeqCst));
    assert_eq!(a.on_worker.load(Ordering::SeqCst), 3);
    assert!(!pigeonhole_io::in_inline_io());
    let _ = vfs;
}

#[test]
fn room_freeing_tasks_are_taken_and_run_first() {
    let (vfs, mut owned) = open(1);
    let (slow, flush) = (Arc::new(Seen::default()), Arc::new(Seen::default()));
    spawn(&mut owned.shards[0], Steps::new(5, &slow));
    let worker = &mut owned.workers[0];
    one_slice(worker, &vfs);
    let mut f = Steps::new(1, &flush);
    f.room = true;
    spawn(&mut owned.shards[0], f);
    // The next slice is the flush's, ahead of the compaction-like task already runnable.
    one_slice(&mut owned.workers[0], &vfs);
    assert!(flush.done.load(Ordering::SeqCst));
    assert_eq!(slow.on_worker.load(Ordering::SeqCst), 1);
}

#[test]
fn close_takes_back_a_stopped_workers_tasks_without_driving_it() {
    // The owner's case: a worker ran part of a task, then nobody drives it; the close takes
    // the task back to its shard and completes without the application.
    let (vfs, mut owned) = open(1);
    let seen = Arc::new(Seen::default());
    spawn(&mut owned.shards[0], Steps::new(4, &seen));
    assert!(one_slice(&mut owned.workers[0], &vfs));
    assert_eq!(seen.on_worker.load(Ordering::SeqCst), 1);
    let pool = owned.pool.clone().unwrap();
    // From another thread, as an engine's close would; the worker stays alive, undriven.
    thread::spawn(move || pool.close()).join().unwrap();
    assert!(owned.workers[0].closed());
    assert!(!owned.workers[0].run_once(u64::MAX));
    assert_eq!(
        seen.reclaims.load(Ordering::SeqCst),
        0,
        "reclaim runs on the shard"
    );
    while owned.shards[0].run_once(u64::MAX) {}
    assert_eq!(seen.reclaims.load(Ordering::SeqCst), 1);
    assert!(seen.done.load(Ordering::SeqCst));
    assert_eq!(seen.on_shard.load(Ordering::SeqCst), 3);
    // Later spawns stay on the shard.
    let later = Arc::new(Seen::default());
    spawn(&mut owned.shards[0], Steps::new(1, &later));
    while owned.shards[0].run_once(u64::MAX) {}
    assert!(later.done.load(Ordering::SeqCst));
}

#[test]
fn close_waits_only_for_a_slice_running_now() {
    let (_vfs, mut owned) = open(1);
    let seen = Arc::new(Seen::default());
    let gate = Arc::new(Barrier::new(2));
    let mut t = Steps::new(3, &seen);
    t.gate = Some(Arc::clone(&gate));
    spawn(&mut owned.shards[0], t);
    let mut worker = owned.workers.pop().unwrap();
    let driving = thread::spawn(move || {
        worker.run_once(0);
        worker
    });
    gate.wait(); // the slice runs
    let pool = owned.pool.clone().unwrap();
    let closed = Arc::new(AtomicBool::new(false));
    let closer = {
        let closed = Arc::clone(&closed);
        thread::spawn(move || {
            pool.close();
            closed.store(true, Ordering::SeqCst);
        })
    };
    thread::sleep(Duration::from_millis(50));
    assert!(
        !closed.load(Ordering::SeqCst),
        "the close waits for the running slice"
    );
    gate.wait(); // the slice ends
    closer.join().unwrap();
    let worker = driving.join().unwrap();
    assert!(worker.closed());
    while owned.shards[0].run_once(u64::MAX) {}
    assert!(seen.done.load(Ordering::SeqCst));
}

#[test]
fn a_panicking_slice_is_taken_back_as_failed_and_leaves_no_inline_scope() {
    let (vfs, mut owned) = open(1);
    let seen = Arc::new(Seen::default());
    let mut t = Steps::new(3, &seen);
    t.panic_at = Some(2);
    spawn(&mut owned.shards[0], t);
    assert!(one_slice(&mut owned.workers[0], &vfs));
    let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| {
        one_slice(&mut owned.workers[0], &vfs);
    }));
    assert!(unwound.is_err());
    assert!(
        !pigeonhole_io::in_inline_io(),
        "the scope ends with the unwind"
    );
    // The close does not wait for it (it is not running), and the shard is told it failed.
    owned.pool.clone().unwrap().close();
    while owned.shards[0].run_once(u64::MAX) {}
    assert!(seen.failed.load(Ordering::SeqCst));
    assert!(
        !seen.done.load(Ordering::SeqCst),
        "a failed task is dropped"
    );
}

#[test]
fn a_dropped_worker_hands_its_tasks_back() {
    let (vfs, mut owned) = open(1);
    let seen = Arc::new(Seen::default());
    spawn(&mut owned.shards[0], Steps::new(3, &seen));
    one_slice(&mut owned.workers[0], &vfs);
    drop(owned.workers.pop());
    while owned.shards[0].run_once(u64::MAX) {}
    assert!(seen.done.load(Ordering::SeqCst));
    assert_eq!(seen.on_shard.load(Ordering::SeqCst), 2);
    // No worker left: later spawns stay on the shard.
    let later = Arc::new(Seen::default());
    spawn(&mut owned.shards[0], Steps::new(1, &later));
    while owned.shards[0].run_once(u64::MAX) {}
    assert!(later.done.load(Ordering::SeqCst));
}

#[test]
fn a_room_wait_takes_back_a_flush_its_worker_stopped_running() {
    let (vfs, mut owned) = open(1);
    let seen = Arc::new(Seen::default());
    let mut f = Steps::new(3, &seen);
    f.room = true;
    spawn(&mut owned.shards[0], f);
    one_slice(&mut owned.workers[0], &vfs);
    let shard = &mut owned.shards[0];
    // Not idle long enough yet: it stays, and the shard looks again by itself.
    assert_eq!(shard.with_handler(|_, ctx| ctx.reclaim_room()), 0);
    let again = shard.next_deadline().expect("a look again is due");
    vfs.advance(again - vfs.monotonic_nanos());
    while shard.run_once(u64::MAX) {}
    assert!(seen.done.load(Ordering::SeqCst));
    assert_eq!(seen.on_shard.load(Ordering::SeqCst), 2);
    assert_eq!(
        shard.submitter(shard.shard()).background_take_backs(),
        (0, 1)
    );
}

#[test]
fn a_room_wait_leaves_a_running_flush_where_it_is() {
    let (vfs, mut owned) = open(1);
    let seen = Arc::new(Seen::default());
    let gate = Arc::new(Barrier::new(2));
    let mut f = Steps::new(2, &seen);
    f.room = true;
    f.gate = Some(Arc::clone(&gate));
    spawn(&mut owned.shards[0], f);
    let mut worker = owned.workers.pop().unwrap();
    let driving = thread::spawn(move || {
        worker.run_once(0);
        worker
    });
    gate.wait();
    vfs.advance(10_000_000);
    // Running: the room wait neither waits nor takes it.
    assert_eq!(owned.shards[0].with_handler(|_, ctx| ctx.reclaim_room()), 0);
    gate.wait();
    let mut worker = driving.join().unwrap();
    while worker.run_once(u64::MAX) {}
    assert!(seen.done.load(Ordering::SeqCst));
    assert_eq!(seen.on_shard.load(Ordering::SeqCst), 0);
}

#[test]
fn tasks_fall_back_to_their_shard_when_no_worker_is_driven() {
    let (vfs, mut owned) = open(2);
    let seen = Arc::new(Seen::default());
    let shard = &mut owned.shards[0];
    spawn(shard, Steps::new(1, &seen));
    assert!(!shard.run_once(u64::MAX));
    assert_eq!(
        seen.on_shard.load(Ordering::SeqCst),
        0,
        "queued for the workers"
    );
    let at = shard.next_deadline().expect("the fallback is due later");
    vfs.advance(at - vfs.monotonic_nanos());
    while shard.run_once(u64::MAX) {}
    assert!(seen.done.load(Ordering::SeqCst));
    assert_eq!(
        seen.reclaims.load(Ordering::SeqCst),
        0,
        "a task that only waited in the queue is not told it was taken back"
    );
    assert_eq!(
        shard.submitter(shard.shard()).background_take_backs(),
        (1, 0)
    );
}

#[test]
fn a_worker_with_runnable_tasks_leaves_new_ones_for_others() {
    let (vfs, mut owned) = open(2);
    let seen: Vec<Arc<Seen>> = (0..3).map(|_| Arc::new(Seen::default())).collect();
    for s in &seen {
        spawn(&mut owned.shards[0], Steps::new(10, s));
    }
    // The first worker takes two (fewer than two held, then one runnable), not all three.
    for _ in 0..4 {
        one_slice(&mut owned.workers[0], &vfs);
    }
    one_slice(&mut owned.workers[1], &vfs);
    let worker1: u32 = seen
        .iter()
        .map(|s| s.on_worker.load(Ordering::SeqCst))
        .sum();
    assert_eq!(worker1, 5);
    assert!(seen.iter().all(|s| s.on_worker.load(Ordering::SeqCst) > 0));
}

#[test]
fn a_worker_waits_until_work_arrives() {
    let (_vfs, mut owned) = open(1);
    let mut worker = owned.workers.pop().unwrap();
    let woken = Arc::new(Mutex::new(0u32));
    {
        let woken = Arc::clone(&woken);
        worker.set_wakeup(Box::new(move || *woken.lock().unwrap() += 1));
    }
    assert!(!worker.run_once(u64::MAX));
    let seen = Arc::new(Seen::default());
    let driving = thread::spawn(move || {
        while !worker.closed() {
            if !worker.run_once(u64::MAX) {
                worker.wait(Duration::from_secs(5));
            }
        }
    });
    thread::sleep(Duration::from_millis(20));
    spawn(&mut owned.shards[0], Steps::new(2, &seen));
    while !seen.done.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    owned.pool.clone().unwrap().close();
    driving.join().unwrap();
    assert!(*woken.lock().unwrap() >= 2);
}

#[test]
fn a_stopped_worker_gives_its_held_tasks_back_without_a_close() {
    // perf287's should-fix on #983: a worker the application stops driving (without dropping
    // it) holds a compaction-like task. Once that worker has made no pass for 100 ms, the
    // shard takes the task back (telling it, so a compaction is dropped and picked again)
    // rather than keep it until the close.
    let (vfs, mut owned) = open(2);
    let seen = Arc::new(Seen::default());
    spawn(&mut owned.shards[0], Steps::new(5, &seen));
    vfs.advance(1_000_000);
    assert!(one_slice(&mut owned.workers[0], &vfs));
    // The other worker keeps passing: only the stopped one's tasks come back.
    let shard = &mut owned.shards[0];
    let mut waited = 0;
    while seen.reclaims.load(Ordering::SeqCst) == 0 {
        vfs.advance(10_000_000);
        owned.workers[1].run_once(vfs.monotonic_nanos());
        shard.run_once(u64::MAX);
        waited += 1;
        assert!(waited < 16, "the stopped worker's task never came back");
    }
    assert!(
        waited >= 10,
        "taken back before the worker was stale ({waited} steps)"
    );
    while shard.run_once(u64::MAX) {}
    assert!(seen.done.load(Ordering::SeqCst));
    assert_eq!(seen.on_worker.load(Ordering::SeqCst), 1);
    assert_eq!(seen.on_shard.load(Ordering::SeqCst), 4);
    assert_eq!(
        shard.submitter(shard.shard()).background_take_backs(),
        (1, 0)
    );
}
