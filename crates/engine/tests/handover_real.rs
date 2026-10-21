//! The writer-lock handover (D265) on real files, under the platform's lock model (OFD locks
//! on Linux, the per-process `fcntl` registry on macOS; `PIGEONHOLE_IO=uring` picks the
//! io_uring backend on Linux): both lock bytes are probed from a fresh handle at every step.
//! Written by perf287 for the review of #788; the simulated VFS of `handover.rs` has its own
//! lock table, so these are the tests that exercise the registry's deferred close and OFD's
//! per-description ownership.

#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, Error, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::{OpenOptions, VfsRef};
use pigeonhole_shm::{Presence, WriterLock};

fn real_vfs() -> VfsRef {
    match std::env::var("PIGEONHOLE_IO").as_deref() {
        #[cfg(target_os = "linux")]
        Ok("uring") => pigeonhole_io::uring::UringVfs::new().expect("io_uring"),
        _ => pigeonhole_io::pread::PreadVfs::new(2),
    }
}

fn options(vfs: VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 4 << 20;
    o
}

/// A temporary directory of its own per test, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let d = std::env::temp_dir().join(format!("phdb-handover-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }

    fn db(&self) -> PathBuf {
        self.0.join("h.phdb")
    }

    /// The names of the files in the directory, sorted.
    fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A fresh handle (another open file description, or another registry handle) probes both
/// bytes: whether the writer byte is free, and whether a presence taken now is the last one.
fn probe(vfs: &VfsRef, path: &Path) -> (bool, bool) {
    let mut oo = OpenOptions::read();
    oo.write = true;
    let f = vfs.open(path, oo).unwrap();
    let writer_free = match WriterLock::acquire(&f) {
        Ok(l) => {
            drop(l);
            true
        }
        Err(pigeonhole_shm::Error::WriterLocked) => false,
        Err(e) => panic!("{e:?}"),
    };
    let p = Presence::acquire(&f).unwrap();
    let last = p.try_become_last().unwrap();
    drop(p);
    drop(f);
    (writer_free, last)
}

fn put_sync(db: &Engine, t: &pigeonhole_engine::TableInfo, row: &str) {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    db.commit(wb, Some(Durability::Sync)).unwrap();
}

/// Both bytes stay held after `close_for_reopen`, after a refused second open (whose handle
/// closed at once), after dropping the old engine (which closes its pager and data handles
/// of the same inode) and after `open_with`. After the reopened instance closes cleanly,
/// both are free, only the `.phdb` file remains, and the data reads back.
#[test]
fn a_real_file_handover_keeps_both_bytes_at_every_step() {
    let vfs = real_vfs();
    let dir = TempDir::new("keep");
    let path = dir.db();
    let db = Engine::open(&path, options(Arc::clone(&vfs))).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    put_sync(&db, &t, "a");
    assert_eq!(probe(&vfs, &path), (false, false), "open: both held");
    let (closed, h) = db.close_for_reopen();
    closed.unwrap();
    let h = h.expect("handover");
    assert_eq!(probe(&vfs, &path), (false, false), "after close_for_reopen");
    assert!(matches!(
        Engine::open(&path, options(Arc::clone(&vfs))),
        Err(Error::WriterLocked)
    ));
    assert_eq!(
        probe(&vfs, &path),
        (false, false),
        "after a refused open's handle closed"
    );
    drop(t);
    drop(db);
    assert_eq!(
        probe(&vfs, &path),
        (false, false),
        "after dropping the old engine"
    );
    let db = Engine::open_with(&path, options(Arc::clone(&vfs)), h)
        .map_err(|(e, _)| e)
        .unwrap();
    assert_eq!(probe(&vfs, &path), (false, false), "after open_with");
    let t = db.table("t").unwrap();
    put_sync(&db, &t, "b");
    db.close().unwrap();
    drop(t);
    drop(db);
    assert_eq!(probe(&vfs, &path), (true, true), "after the reopened close");
    assert_eq!(
        dir.files(),
        ["h.phdb"],
        "a clean close leaves the main file alone"
    );
    let db = Engine::open(&path, options(Arc::clone(&vfs))).unwrap();
    let t = db.table("t").unwrap();
    let snap = db.snapshot().unwrap();
    for r in ["a", "b"] {
        assert!(
            db.get(&snap, t.id, t.families[0].id, r.as_bytes(), b"q")
                .unwrap()
                .is_some(),
            "{r}"
        );
    }
    drop(snap);
    db.close().unwrap();
}

/// Dropping a handover frees both bytes and runs no cleanup: the WAL files stay, as after a
/// crashed writer, and the next open replays them.
#[test]
fn a_dropped_real_file_handover_releases_both_bytes_and_keeps_the_wal() {
    let vfs = real_vfs();
    let dir = TempDir::new("drop");
    let path = dir.db();
    let db = Engine::open(&path, options(Arc::clone(&vfs))).unwrap();
    let (closed, h) = db.close_for_reopen();
    closed.unwrap();
    drop(db);
    assert_eq!(probe(&vfs, &path), (false, false));
    drop(h.unwrap());
    assert_eq!(
        probe(&vfs, &path),
        (true, true),
        "a dropped handover releases both"
    );
    assert!(
        dir.files().iter().any(|f| f.contains("wal")),
        "the WAL files stay: {:?}",
        dir.files()
    );
    Engine::open(&path, options(vfs)).unwrap().close().unwrap();
}

/// A reader process that closes inside the window is not the last one out: the handover's
/// presence keeps it from removing the WAL files and the region.
#[test]
fn a_reader_closing_during_a_real_file_handover_is_not_last() {
    let vfs = real_vfs();
    let dir = TempDir::new("reader");
    let path = dir.db();
    let db = Engine::open(&path, options(Arc::clone(&vfs))).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    put_sync(&db, &t, "a");
    let reader = Engine::open_reader(&path, options(Arc::clone(&vfs))).unwrap();
    let (closed, h) = db.close_for_reopen();
    closed.unwrap();
    reader.close().unwrap();
    drop(reader);
    assert_eq!(
        probe(&vfs, &path),
        (false, false),
        "after the reader closed in the window"
    );
    let db2 = Engine::open_with(&path, options(Arc::clone(&vfs)), h.unwrap())
        .map_err(|(e, _)| e)
        .unwrap();
    let t2 = db2.table("t").unwrap();
    put_sync(&db2, &t2, "b");
    drop(db);
    db2.close().unwrap();
}
