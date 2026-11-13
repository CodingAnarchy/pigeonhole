//! Caller-runs (D321): a panic in a caller's turn fails its shard, and shutdown still
//! returns, reporting the panic (the 1.2.0 audit's S-E3).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use pigeonhole_io::pread::PreadVfs;
use pigeonhole_runtime::{Runtime, RuntimeConfig, ShardContext, ShardHandler, ShardId};

enum Msg {
    /// Shard 0: panics in whichever turn handles it.
    Boom,
    /// Shard 1: holds its driver inside this turn until the test lets it go.
    Hold,
}

/// Shard 0's handler panics on `Boom`; shard 1's holds the driver on `Hold`, at the first
/// barrier once it's inside the turn and at the second until the test releases it.
struct Handler {
    hold: Option<(Arc<Barrier>, Arc<Barrier>)>,
}

impl ShardHandler for Handler {
    type Msg = Msg;

    fn handle(&mut self, _ctx: &mut ShardContext<'_, Msg>, msg: Msg) {
        match msg {
            Msg::Boom => panic!("a caller's turn panics"),
            Msg::Hold => {
                let (inside, release) = self.hold.as_ref().expect("shard 1 holds");
                inside.wait();
                release.wait();
            }
        }
    }

    fn end_batch(&mut self, _ctx: &mut ShardContext<'_, Msg>) {}

    fn caller_may_take(&self, _msg: &Msg) -> bool {
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
///
/// Both shards run on one driver thread. Shard 1's turn holds that driver while the test
/// submits to shard 0 and runs a caller's turn there, so the caller's turn, not the
/// driver, takes the message: a race between the two (#1820) lost on a loaded runner,
/// where the driver handled nearly every message first.
#[test]
fn a_panic_in_a_callers_turn_never_hangs_shutdown() {
    let mut config = RuntimeConfig::new(PreadVfs::new(1));
    config.shards = 2;
    config.shard_threads = 1;
    config.caller_runs = true;
    let (inside, release) = (Arc::new(Barrier::new(2)), Arc::new(Barrier::new(2)));
    let rt = Runtime::start(
        config,
        vec![
            Handler { hold: None },
            Handler {
                hold: Some((Arc::clone(&inside), Arc::clone(&release))),
            },
        ],
    )
    .unwrap();
    rt.submitter(ShardId(1)).submit(Msg::Hold).unwrap();
    inside.wait();
    // The driver is inside shard 1's turn: nothing but this thread can run shard 0.
    rt.submitter(ShardId(0)).submit(Msg::Boom).unwrap();
    let caller = rt.callers()[0].clone();
    caller.note_sent();
    let ran = catch_unwind(AssertUnwindSafe(|| caller.try_run(&mut || false)));
    release.wait();
    assert!(
        ran.is_err(),
        "the caller's turn didn't take the message (it returned {ran:?})"
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
