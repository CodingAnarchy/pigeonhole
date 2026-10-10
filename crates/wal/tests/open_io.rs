//! New streams' open-time syncs (#158, D203) are submitted, not waited for. A blocking sync
//! must still finish where nothing else completes I/O: on the simulator's deferred device,
//! which only a waiter's drive runs. (A single-threaded harness that closed a shard right
//! after an open hung in the shard's final sync.)

mod common;

use std::sync::mpsc;
use std::time::Duration;

use common::*;
use pigeonhole_format::{Durability, Lsn, StreamId};
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::{CrashKind, SimVfs};
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

#[test]
fn an_adopted_stream_whose_creating_open_died_before_its_directory_sync_survives_power_loss() {
    // The creating open: the stream file is made, its syncs (the directory's too) left in
    // flight, and the process dies before the directory sync runs.
    let sim = SimVfs::new(4);
    sim.set_deferred_io(true);
    let vfs: VfsRef = sim.clone();
    drop(WalStream::create_all(&vfs, db(), &[STREAM], DB_ID, opts(4, 0)).unwrap());
    sim.crash(CrashKind::Process);
    // The next open adopts the file and acknowledges a durable commit in it.
    let (_, r) = replay(&vfs, Lsn::default()).unwrap();
    let mut wal = r.into_stream(opts(4, 0)).unwrap();
    wal.append(&batch(1, 100).record(), Durability::GroupSync)
        .unwrap();
    wal.sync().unwrap();
    drop(wal);
    // A power loss keeps only durable directory entries: the adopted file's name must be one.
    sim.crash(CrashKind::Power);
    let (got, _) = replay(&vfs, Lsn::default()).expect("the stream file survives");
    assert_eq!(got.seqnos(), [1]);
}
