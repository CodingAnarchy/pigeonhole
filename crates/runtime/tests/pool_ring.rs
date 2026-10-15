//! D252 under io_uring: a compaction-pool thread completes its own I/O on a ring of its
//! own, as a shard thread does (#402), and reaps that ring while it idles. Without the ring,
//! everything a pool task submits goes to the backend's shared ring and its one reaper
//! thread, which every other thread's reads queue on too; the pool's per-block waits then
//! serialize behind that reaper's decode work and flushes take seconds (the stall on the
//! AX102 after #721). Linux only; fails where io_uring is unavailable, so a run that should
//! cover the backend cannot pass without it.
#![cfg(all(target_os = "linux", not(miri)))]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use pigeonhole_io::uring::UringVfs;
use pigeonhole_io::{Completion, FileRef, IoBuf, OpenOptions, Vfs};
use pigeonhole_runtime::{
    Runtime, RuntimeConfig, ShardContext, ShardHandler, ShardId, Task, TaskPoll, TaskWaker,
};

enum Msg {
    Spawn(Box<dyn Task>),
}

struct Spawner;

impl ShardHandler for Spawner {
    type Msg = Msg;

    fn handle(&mut self, ctx: &mut ShardContext<'_, Msg>, msg: Msg) {
        match msg {
            Msg::Spawn(task) => ctx.spawn(task),
        }
    }

    fn end_batch(&mut self, _ctx: &mut ShardContext<'_, Msg>) {}
}

/// What the task saw at each step.
enum Report {
    /// The write is submitted: whether the submitting thread has I/O only it completes, and
    /// that thread's name.
    Submitted { own: bool, thread: String },
    /// The write's completion woke the task; a blocking read then resolved on `resolved_on`.
    Done { thread: String, resolved_on: String },
}

fn thread_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("<unnamed>")
        .to_owned()
}

/// Submits a write, blocks until its completion wakes it, then blocks on a read.
struct WriteThenRead {
    file: FileRef,
    write: Option<Completion>,
    report: mpsc::Sender<Report>,
}

impl Task for WriteThenRead {
    fn run(&mut self, _deadline: u64, waker: &TaskWaker) -> TaskPoll {
        let Some(write) = self.write.take() else {
            let mut buf = IoBuf::zeroed(4096);
            buf.fill(7);
            let write = self.file.submit_write(buf, 0);
            // Whether that write is this thread's own to complete (a ring of its own).
            let own = pigeonhole_io::own_io_in_flight();
            let wake = waker.clone();
            self.write = Some(write.map(move |r| {
                wake.wake();
                r
            }));
            let _ = self.report.send(Report::Submitted {
                own,
                thread: thread_name(),
            });
            // Woken only by the completion: on a ring only this thread reaps, that takes the
            // pool's idle path entering the ring.
            return TaskPoll::Blocked;
        };
        write.wait().expect("the write completes");
        // A blocking wait on this thread's own ring reaps it here; on the shared ring the
        // reaper thread resolves it.
        let resolved_on = self
            .file
            .submit_read(IoBuf::zeroed(4096), 0)
            .map(|r| r.map(|buf| (buf, thread_name())))
            .wait()
            .expect("the read completes");
        assert!(resolved_on.0.iter().all(|&b| b == 7));
        let _ = self.report.send(Report::Done {
            thread: thread_name(),
            resolved_on: resolved_on.1,
        });
        TaskPoll::Done
    }

    fn name(&self) -> &'static str {
        "write-then-read"
    }
}

/// Stays runnable (one `Pending` slice after another) until told to stop: a compaction
/// that keeps the pool thread busy, so the thread never reaches its idle wait.
struct Busy {
    stop: Arc<AtomicBool>,
    slices: Arc<AtomicU64>,
}

impl Task for Busy {
    fn run(&mut self, _deadline: u64, _waker: &TaskWaker) -> TaskPoll {
        self.slices.fetch_add(1, Ordering::Relaxed);
        if self.stop.load(Ordering::Acquire) {
            TaskPoll::Done
        } else {
            TaskPoll::Pending
        }
    }

    fn name(&self) -> &'static str {
        "busy"
    }
}

struct Started {
    rt: Runtime<Spawner>,
    reports: mpsc::Receiver<Report>,
    dir: std::path::PathBuf,
}

/// A runtime with one shard and one pool thread, with `WriteThenRead` spawned to the pool
/// (after `before`, if any).
fn start(tag: &str, before: Option<Box<dyn Task>>) -> Started {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-runtime-pool-ring-{tag}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let vfs = UringVfs::new().expect("io_uring is available");
    let file = vfs
        .open(&dir.join("f"), OpenOptions::read_write_create())
        .unwrap();
    let mut c = RuntimeConfig::new(vfs);
    c.shards = 1;
    c.compaction_threads = 1;
    c.pin_threads = false;
    let rt = Runtime::start(c, vec![Spawner]).unwrap();
    let (report, reports) = mpsc::channel();
    if let Some(task) = before {
        rt.submitter(ShardId(0)).submit(Msg::Spawn(task)).unwrap();
    }
    rt.submitter(ShardId(0))
        .submit(Msg::Spawn(Box::new(WriteThenRead {
            file,
            write: None,
            report,
        })))
        .unwrap();
    Started { rt, reports, dir }
}

/// Reads the task's two reports and checks the I/O shape they describe.
fn check_reports(reports: &mpsc::Receiver<Report>, what: &str) {
    let wait = Duration::from_secs(10);
    match reports
        .recv_timeout(wait)
        .unwrap_or_else(|e| panic!("{what}: the task did not run on the pool: {e}"))
    {
        Report::Submitted { own, thread } => {
            assert!(
                thread.starts_with("pigeonhole-compaction-"),
                "{what}: the task ran on {thread}, not a pool thread"
            );
            assert!(
                own,
                "{what}: the pool thread's write went to the shared ring, not a ring of its own"
            );
        }
        Report::Done { .. } => panic!("{what}: the task finished before it reported its submit"),
    }
    match reports
        .recv_timeout(wait)
        .unwrap_or_else(|e| panic!("{what}: the write's completion did not wake the task: {e}"))
    {
        Report::Done {
            thread,
            resolved_on,
        } => assert_eq!(
            resolved_on, thread,
            "{what}: the pool thread's blocking read resolved on another thread"
        ),
        Report::Submitted { .. } => panic!("{what}: reported twice"),
    }
}

#[test]
fn a_pool_thread_completes_its_own_io_on_its_own_ring() {
    let s = start("idle", None);
    check_reports(&s.reports, "idle pool thread");
    s.rt.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&s.dir);
}

/// The busy path: another task keeps the pool thread running slices, so it never reaches
/// its idle wait. The write's completion must still reach the blocked task, through the
/// reap at the top of every pass (a ring that completes only when its owner enters it
/// posts nothing while the thread stays in user space).
#[test]
fn a_busy_pool_thread_still_completes_its_own_io_between_slices() {
    let stop = Arc::new(AtomicBool::new(false));
    let slices = Arc::new(AtomicU64::new(0));
    let s = start(
        "busy",
        Some(Box::new(Busy {
            stop: Arc::clone(&stop),
            slices: Arc::clone(&slices),
        })),
    );
    check_reports(&s.reports, "busy pool thread");
    stop.store(true, Ordering::Release);
    assert!(
        slices.load(Ordering::Relaxed) > 1,
        "the busy task did not keep the pool thread running"
    );
    s.rt.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&s.dir);
}
