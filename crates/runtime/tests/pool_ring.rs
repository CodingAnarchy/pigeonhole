//! D252 under io_uring: a compaction-pool thread completes its own I/O on a ring of its
//! own, as a shard thread does (#402), and reaps that ring while it idles. Without the ring,
//! everything a pool task submits goes to the backend's shared ring and its one reaper
//! thread, which every other thread's reads queue on too; the pool's per-block waits then
//! serialize behind that reaper's decode work and flushes take seconds (the stall on the
//! AX102 after #721). Linux only; fails where io_uring is unavailable, so a run that should
//! cover the backend cannot pass without it.
#![cfg(all(target_os = "linux", not(miri)))]

use std::sync::mpsc;
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

#[test]
fn a_pool_thread_completes_its_own_io_on_its_own_ring() {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-runtime-pool-ring-{}",
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
    rt.submitter(ShardId(0))
        .submit(Msg::Spawn(Box::new(WriteThenRead {
            file,
            write: None,
            report,
        })))
        .unwrap();
    let wait = Duration::from_secs(10);
    match reports
        .recv_timeout(wait)
        .expect("the task ran on the pool")
    {
        Report::Submitted { own, thread } => {
            assert!(
                thread.starts_with("pigeonhole-compaction-"),
                "the task ran on {thread}, not a pool thread"
            );
            assert!(
                own,
                "the pool thread's write went to the shared ring, not a ring of its own"
            );
        }
        Report::Done { .. } => panic!("the task finished before it reported its submit"),
    }
    match reports
        .recv_timeout(wait)
        .expect("the write's completion woke the task while the pool thread idled")
    {
        Report::Done {
            thread,
            resolved_on,
        } => assert_eq!(
            resolved_on, thread,
            "the pool thread's blocking read resolved on another thread"
        ),
        Report::Submitted { .. } => panic!("reported twice"),
    }
    rt.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
