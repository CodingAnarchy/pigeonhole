//! The WAL on io_uring in application-owned mode (#408): a stream's blocking waits must
//! complete I/O that no thread reaps (the shared ring has no reaper there), or they wait for
//! ever (ICR 0028; the #473 scaling hang). Linux only; it fails where io_uring is
//! unavailable, so a run meant to cover it cannot pass without it.
#![cfg(target_os = "linux")]

mod common;

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use common::*;
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;
use pigeonhole_wal::{Wal, WalStream};

/// Runs `f` on its own thread and fails, rather than hanging, if it does not return (a
/// panic in it fails the test too).
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

/// A temporary directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("phdb-wal-uring-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn a_blocking_sync_behind_a_sync_on_a_ring_no_thread_reaps_completes() {
    within("a blocking sync behind an orphan ring's sync", || {
        let dir = TempDir::new("orphan");
        let vfs: VfsRef =
            pigeonhole_io::uring::UringVfs::new_application_owned().expect("io_uring");
        let path = dir.0.join("data.phdb");
        let mut wal = WalStream::create(&vfs, &path, STREAM, DB_ID, opts(4, 0)).unwrap();
        wal.append(&batch(1, 100).record(), Durability::GroupSync)
            .unwrap();
        wal.write().unwrap();
        // This thread drives no shard, so it has no ring: the sync goes to the shared ring,
        // which no thread reaps in application-owned mode.
        let pending = wal.submit_sync().unwrap();
        // A blocking sync counts only once every older sync finished (D58): it must reap
        // that one itself.
        wal.sync().unwrap();
        assert!(pending.is_ready());
    });
}
