//! perf287's review of #898 (S1, F1): a process forked from a reader process can't use or
//! free the parent's reader. In the child, the reader refuses snapshots (and so never claims
//! a slot through the inherited handle, which on Linux shares the parent's open file
//! description and its locks), reads through the parent's snapshots expire, and closing it
//! releases nothing. The parent's slot, pin and lock are untouched. Real files and a real
//! `fork`, so Unix only.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, Error, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

fn options(vfs: VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 4 << 20;
    o
}

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn put(db: &Engine, t: &pigeonhole_engine::TableInfo, row: &[u8], value: &[u8]) {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(value),
    )
    .unwrap();
    db.commit(wb, Some(Durability::Buffered)).unwrap();
}

/// Child exit-code bits, one per expectation that failed.
const SNAPSHOT_NOT_REFUSED: i32 = 1;
const OLD_SNAPSHOT_READ: i32 = 2;
const CLOSE_FAILED: i32 = 4;

#[test]
fn a_forked_childs_reader_refuses_and_leaves_the_parents_slot_alone() {
    let dir = TempDir(std::env::temp_dir().join(format!("phdb-fork-{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&dir.0);
    std::fs::create_dir_all(&dir.0).unwrap();
    let path = dir.0.join("f.phdb");
    let vfs: VfsRef = pigeonhole_io::pread::PreadVfs::new(2);

    let db = Engine::open(&path, options(Arc::clone(&vfs))).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    put(&db, &t, b"k", b"before");
    let reader = Engine::open_reader(Path::new(&path), options(Arc::clone(&vfs))).unwrap();
    let held = reader.snapshot().unwrap();
    let (tid, fid) = (t.id, t.families[0].id);
    let read = |s: &pigeonhole_engine::Snapshot| reader.get(s, tid, fid, b"k", b"q");
    assert!(read(&held).unwrap().is_some());
    let (pin, _) = db.reader_pin_and_view();
    assert!(pin.is_some());
    assert_eq!(db.metrics().reader_slots.0, 1);

    // SAFETY: the child calls into the reader only (no writer state, whose threads are not
    // in the child), then `_exit`s without running destructors.
    let child = unsafe { libc::fork() };
    assert!(child >= 0, "fork failed");
    if child == 0 {
        let mut failed = 0;
        match reader.snapshot() {
            Err(Error::Unsupported(msg)) if msg.contains("fork") => {}
            _ => failed |= SNAPSHOT_NOT_REFUSED,
        }
        if !matches!(read(&held), Err(Error::SnapshotExpired)) {
            failed |= OLD_SNAPSHOT_READ;
        }
        if reader.close().is_err() {
            failed |= CLOSE_FAILED;
        }
        // SAFETY: ends the child at once.
        unsafe { libc::_exit(failed) };
    }
    let mut status = 0;
    // SAFETY: waits for the child forked above.
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);

    // The parent's reader is untouched: same slot, same pin, still reading; no slot was
    // claimed for the child.
    assert_eq!(db.metrics().reader_slots.0, 1, "the child claimed a slot");
    assert_eq!(
        db.reader_pin_and_view().0,
        pin,
        "the child moved the parent's pin"
    );
    // Past the liveness interval, a flush runs the writer's reclamation: the parent's lock
    // is still held, so its slot stays.
    std::thread::sleep(std::time::Duration::from_millis(300));
    put(&db, &t, b"k", b"after");
    db.flush().unwrap();
    assert_eq!(
        db.metrics().reader_slots.0,
        1,
        "the parent's slot was freed"
    );
    assert_eq!(db.reader_pin_and_view().0, pin);
    assert_eq!(
        read(&held).unwrap().map(|c| match c.value() {
            ValueRef::Bytes(b) => b.to_vec(),
            other => panic!("unexpected value {other:?}"),
        }),
        Some(b"before".to_vec())
    );
    let fresh = reader.snapshot().unwrap();
    assert!(read(&fresh).unwrap().is_some());
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "the child's reader misbehaved: exit bits {} (1 snapshot not refused, 2 old snapshot \
         read, 4 close failed)",
        libc::WEXITSTATUS(status)
    );
    drop(fresh);
    drop(held);
    reader.close().unwrap();
    db.close().unwrap();
}
