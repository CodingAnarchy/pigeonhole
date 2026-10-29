//! D289 under io_uring: two shards on one driver thread share that thread's ring (#402).
//! A completion for shard B can be reaped in shard A's turn, or inside a blocking wait A
//! makes in its turn; B's task must still be woken and run, with B announced asleep or
//! not. Linux only; fails where io_uring is unavailable, so a run that should cover the
//! backend cannot pass without it.
#![cfg(all(target_os = "linux", not(miri)))]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use pigeonhole_io::uring::UringVfs;
use pigeonhole_io::{Completion, FileRef, IoBuf, OpenOptions, Vfs};
use pigeonhole_runtime::{
    Runtime, RuntimeConfig, ShardContext, ShardHandler, ShardId, Task, TaskPoll, TaskWaker,
};

enum Msg {
    Spawn(Box<dyn Task>),
    /// A blocking read inside the turn (it reaps the thread's ring until it completes),
    /// reporting the thread it ran on.
    BlockingRead(FileRef, mpsc::Sender<String>),
}

struct Handler;

impl ShardHandler for Handler {
    type Msg = Msg;

    fn handle(&mut self, ctx: &mut ShardContext<'_, Msg>, msg: Msg) {
        match msg {
            Msg::Spawn(task) => ctx.spawn(task),
            Msg::BlockingRead(file, done) => {
                let buf = file
                    .submit_read(IoBuf::zeroed(4096), 0)
                    .wait()
                    .expect("the blocking read completes");
                assert!(buf.iter().all(|&b| b == 7));
                let _ = done.send(thread_name());
            }
        }
    }

    fn end_batch(&mut self, _ctx: &mut ShardContext<'_, Msg>) {}
}

fn thread_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("<unnamed>")
        .to_owned()
}

/// What shard B's task saw.
#[derive(Debug)]
enum Report {
    /// The read is submitted, on the thread's own ring or not, from this thread.
    Submitted { own: bool, thread: String },
    /// The read's completion woke the task, which then ran on this thread.
    Done { thread: String },
}

/// Submits a read and blocks until its completion wakes it.
struct ReadAndWait {
    file: FileRef,
    read: Option<Completion<IoBuf>>,
    report: mpsc::Sender<Report>,
}

impl Task for ReadAndWait {
    fn run(&mut self, _deadline: u64, waker: &TaskWaker) -> TaskPoll {
        let Some(read) = self.read.take() else {
            let wake = waker.clone();
            let read = self.file.submit_read(IoBuf::zeroed(4096), 0).map(move |r| {
                wake.wake();
                r
            });
            self.read = Some(read);
            let _ = self.report.send(Report::Submitted {
                own: pigeonhole_io::own_io_in_flight(),
                thread: thread_name(),
            });
            return TaskPoll::Blocked;
        };
        let buf = read.wait().expect("the read completes");
        assert!(buf.iter().all(|&b| b == 7));
        let _ = self.report.send(Report::Done {
            thread: thread_name(),
        });
        TaskPoll::Done
    }

    fn name(&self) -> &'static str {
        "read-and-wait"
    }
}

/// Stays runnable until told to stop: shard A taking turn after turn, each of which reaps
/// the driver's ring first.
struct Busy(Arc<AtomicBool>);

impl Task for Busy {
    fn run(&mut self, _deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        if self.0.load(Ordering::Acquire) {
            TaskPoll::Done
        } else {
            TaskPoll::Pending
        }
    }

    fn name(&self) -> &'static str {
        "busy"
    }
}

/// Two shards on one driver over io_uring, and a 4 KiB file of 7s.
fn start(tag: &str) -> (Runtime<Handler>, FileRef, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-runtime-driver-ring-{tag}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let vfs = UringVfs::new().expect("io_uring is available");
    // Every read goes to a ring, even one the page cache holds (D292): these cases are
    // about whose ring reaps it.
    vfs.set_nowait_reads(false);
    let file = vfs
        .open(&dir.join("f"), OpenOptions::read_write_create())
        .unwrap();
    let mut buf = IoBuf::zeroed(4096);
    buf.fill(7);
    file.write_at(&buf, 0).unwrap();
    let mut c = RuntimeConfig::new(vfs);
    c.shards = 2;
    c.shard_threads = 1;
    c.pin_threads = false;
    let rt = Runtime::start(c, vec![Handler, Handler]).unwrap();
    (rt, file, dir)
}

/// Waits for B's two reports: submitted on the driver's own ring, then done on the driver.
fn check_b(reports: &mpsc::Receiver<Report>, what: &str) {
    let wait = Duration::from_secs(10);
    match reports.recv_timeout(wait) {
        Ok(Report::Submitted { own, thread }) => {
            assert_eq!(thread, "pigeonhole-driver-0", "{what}");
            assert!(
                own,
                "{what}: B's read went to the shared ring, not the driver's own"
            );
        }
        other => panic!("{what}: expected B's submit, got {other:?}"),
    }
    match reports.recv_timeout(wait) {
        Ok(Report::Done { thread }) => assert_eq!(thread, "pigeonhole-driver-0", "{what}"),
        other => panic!("{what}: B's completion did not wake its task (a lost wake): {other:?}"),
    }
}

/// B's read completes while A keeps the driver busy: A's turns reap the shared ring, so
/// B's completion is resolved in one of them, and B's task must still run.
#[test]
fn a_siblings_completion_reaped_in_a_turn_wakes_its_task() {
    let (rt, file, dir) = start("turn");
    let stop = Arc::new(AtomicBool::new(false));
    rt.submitter(ShardId(0))
        .submit(Msg::Spawn(Box::new(Busy(Arc::clone(&stop)))))
        .unwrap();
    let (report, reports) = mpsc::channel();
    rt.submitter(ShardId(1))
        .submit(Msg::Spawn(Box::new(ReadAndWait {
            file,
            read: None,
            report,
        })))
        .unwrap();
    check_b(&reports, "A busy");
    stop.store(true, Ordering::Release);
    rt.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// B's read is in flight when A blocks inside its turn on a read of its own: A's wait reaps
/// the shared ring until its read completes, which may resolve B's too. Neither waits on
/// the other's turn (D289's invariant), so both finish.
#[test]
fn a_blocking_wait_in_a_turn_leaves_a_siblings_io_to_complete() {
    let (rt, file, dir) = start("wait");
    let (report, reports) = mpsc::channel();
    rt.submitter(ShardId(1))
        .submit(Msg::Spawn(Box::new(ReadAndWait {
            file: Arc::clone(&file),
            read: None,
            report,
        })))
        .unwrap();
    let (done, a_done) = mpsc::channel();
    rt.submitter(ShardId(0))
        .submit(Msg::BlockingRead(file, done))
        .unwrap();
    assert_eq!(
        a_done.recv_timeout(Duration::from_secs(10)).unwrap(),
        "pigeonhole-driver-0"
    );
    check_b(&reports, "A blocked in its turn");
    rt.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// D289 PR 2: shards move between drivers while their reads are in flight on io_uring. A
/// shard moves only once its driver's ring is quiet, so every read completes on the ring
/// that submitted it and every task finishes, under a resize every millisecond. Rounds of
/// reads go on until a move lands while a round's reads are outstanding (at most 10 s): one
/// round can finish before the churn thread's first move does.
#[test]
fn shards_move_between_drivers_with_reads_in_flight() {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-runtime-driver-ring-moves-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let vfs = UringVfs::new().expect("io_uring is available");
    // Every read goes to a ring, even one the page cache holds (D292): a read copied on the
    // submitting thread is never in flight when its shard moves.
    vfs.set_nowait_reads(false);
    let file = vfs
        .open(&dir.join("f"), OpenOptions::read_write_create())
        .unwrap();
    let mut buf = IoBuf::zeroed(4096);
    buf.fill(7);
    file.write_at(&buf, 0).unwrap();
    let mut c = RuntimeConfig::new(vfs);
    c.shards = 4;
    c.adaptive_drivers = true;
    c.pin_threads = false;
    let rt = Runtime::start(c, (0..4).map(|_| Handler).collect()).unwrap();
    let force = rt.driver_control();
    let stop = Arc::new(AtomicBool::new(false));
    let churn = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut k = 1;
            while !stop.load(Ordering::Acquire) {
                force(Some(k));
                k = if k == 1 { 2 } else { 1 };
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };
    let (report, reports) = mpsc::channel();
    let tasks = 400;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let (mut rounds, mut moved_mid_round) = (0, false);
    while !moved_mid_round && std::time::Instant::now() < deadline {
        let moves = rt.driver_moves();
        for i in 0..tasks {
            rt.submitter(ShardId((i % 4) as u16))
                .submit(Msg::Spawn(Box::new(ReadAndWait {
                    file: Arc::clone(&file),
                    read: None,
                    report: report.clone(),
                })))
                .unwrap();
        }
        let mut done = 0;
        while done < tasks {
            match reports.recv_timeout(Duration::from_secs(10)) {
                Ok(Report::Done { .. }) => done += 1,
                Ok(Report::Submitted { .. }) => {
                    // Read before the round's last read reports done: a move seen here landed
                    // while some of the round's reads were outstanding.
                    moved_mid_round |= rt.driver_moves() > moves;
                }
                Err(e) => panic!("round {rounds}: {done} of {tasks} reads finished: {e}"),
            }
        }
        rounds += 1;
    }
    stop.store(true, Ordering::Release);
    churn.join().unwrap();
    assert!(
        moved_mid_round,
        "no shard moved while reads were in flight in {rounds} rounds ({} moves)",
        rt.driver_moves()
    );
    rt.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}
