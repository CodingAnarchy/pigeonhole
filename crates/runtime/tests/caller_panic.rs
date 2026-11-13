//! Caller-runs (D321): a panic in a caller's turn fails its shard, and shutdown still
//! returns, reporting the panic (the 1.2.0 audit's S-E3).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use pigeonhole_io::pread::PreadVfs;
use pigeonhole_runtime::{Runtime, RuntimeConfig, ShardContext, ShardHandler, ShardId};

/// Panics on every message a caller's turn handles; counts those its driver handled.
struct PanicsInCallersTurn {
    driver_handled: Arc<AtomicU32>,
}

impl ShardHandler for PanicsInCallersTurn {
    type Msg = ();

    fn handle(&mut self, _ctx: &mut ShardContext<'_, ()>, _msg: ()) {
        // Driver threads are named `pigeonhole-shard-N` or `pigeonhole-driver-N`; the test's
        // own thread, which runs the callers' turns, is named for the test.
        if thread::current()
            .name()
            .is_some_and(|n| n.starts_with("pigeonhole-"))
        {
            self.driver_handled.fetch_add(1, Ordering::Relaxed);
        } else {
            panic!("a caller's turn panics");
        }
    }

    fn end_batch(&mut self, _ctx: &mut ShardContext<'_, ()>) {}

    fn caller_may_take(&self, _msg: &()) -> bool {
        true
    }

    fn caller_may_run(&self) -> bool {
        true
    }
}

/// With caller-runs on, a client whose turn on a shard panics (caught by the application,
/// as a server's per-request `catch_unwind` would) leaves that shard failed. Shutdown used
/// to hang there for ever: the shard's driver kept the failed shard, never found itself
/// empty, and never exited, and shutdown joins every driver. It now returns, and reports
/// the panic as a shard's, as it does for one on a driver thread (the engine's
/// `blocking_waits::a_panicked_shard_thread_does_not_hang_close` covers that one, through
/// `close`'s `shard_panicked`).
#[test]
fn a_panic_in_a_callers_turn_never_hangs_shutdown() {
    let mut config = RuntimeConfig::new(PreadVfs::new(1));
    config.shards = 1;
    config.caller_runs = true;
    let driver_handled = Arc::new(AtomicU32::new(0));
    let rt = Runtime::start(
        config,
        vec![PanicsInCallersTurn {
            driver_handled: Arc::clone(&driver_handled),
        }],
    )
    .unwrap();
    let caller = rt.callers()[0].clone();
    let submitter = rt.submitter(ShardId(0));
    // The driver may handle a message before this thread's turn does: submit until a
    // caller's turn takes one.
    let mut panicked = false;
    for _ in 0..10_000 {
        submitter.submit(()).unwrap();
        caller.note_sent();
        if catch_unwind(AssertUnwindSafe(|| caller.try_run(&mut || false))).is_err() {
            panicked = true;
            break;
        }
    }
    assert!(
        panicked,
        "no caller's turn ran a message in 10,000 tries (the driver handled {})",
        driver_handled.load(Ordering::Relaxed)
    );
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let outcome = match catch_unwind(AssertUnwindSafe(|| rt.shutdown())) {
            Ok(Ok(_)) => "returned Ok".to_owned(),
            Ok(Err(e)) => format!("returned {e}"),
            Err(p) => p
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "a panic with no message".to_owned()),
        };
        let _ = tx.send(outcome);
    });
    let outcome = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("shutdown hung after a caller's turn panicked");
    assert_eq!(outcome, "shard 0 panicked in a caller's turn");
}
