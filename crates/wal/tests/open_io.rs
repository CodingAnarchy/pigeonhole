//! New streams' open-time syncs (#158, D203) are submitted, not waited for. A blocking sync
//! must still finish where nothing else completes I/O: on the simulator's deferred device,
//! which only a waiter's drive runs. (A single-threaded harness that closed a shard right
//! after an open hung in the shard's final sync.)

mod common;

use std::sync::mpsc;
use std::time::Duration;

use common::*;
use pigeonhole_format::StreamId;
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_wal::{Wal, WalStream};

/// Runs `f` on its own thread and fails, rather than hanging, if it does not return.
fn within(what: &str, f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(()) => {}
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("{what} hung"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{what} failed"),
    }
}

#[test]
fn a_blocking_sync_right_after_create_all_drives_the_open_time_syncs() {
    // Each stream in turn first: the directory sync is one operation the streams share.
    for first in [0, 1] {
        within("a blocking sync after create_all", move || {
            let sim = SimVfs::new(3);
            sim.set_deferred_io(true);
            let vfs: VfsRef = sim.clone();
            let mut streams =
                WalStream::create_all(&vfs, db(), &[StreamId(0), StreamId(1)], DB_ID, opts(4, 1))
                    .unwrap();
            assert_eq!(
                sim.io_in_flight(),
                3,
                "two file syncs and one directory sync"
            );
            for i in [first, 1 - first] {
                streams[i].sync().unwrap();
            }
            assert_eq!(sim.io_in_flight(), 0);
        });
    }
}
