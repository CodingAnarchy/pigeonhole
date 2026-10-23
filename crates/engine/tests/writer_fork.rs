//! #901: a process forked from a writer process can't use or tear down the parent's engine.
//! After `fork` only the forking thread exists, so the child has none of the engine's shard,
//! compaction or I/O threads, and it shares the parent's descriptors and shared-memory
//! arenas. Every entry point fails with `Unsupported` at once (never a hang), close and drop
//! do nothing, and the parent's database is untouched. Real files and a real `fork`, so
//! Unix only.
#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, ScanSpec, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

fn options(vfs: VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.compaction_threads = 1;
    o.memtable_budget = 4 << 20;
    o
}

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn batch(t: &TableInfo, row: &[u8], value: &[u8]) -> WriteBatch {
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
    wb
}

fn count_rows(db: &Engine, t: &TableInfo) -> u64 {
    let snap = db.snapshot().unwrap();
    let mut cursor = db
        .scan(
            &snap,
            t.id,
            ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        )
        .unwrap();
    let mut n = 0;
    while cursor.next_row().unwrap() {
        n += 1;
    }
    n
}

/// The child's exit code when the watchdog fired: something hung.
const HUNG: i32 = 99;

#[test]
fn a_forked_childs_writer_refuses_every_entry_point_and_leaves_the_parent_alone() {
    let dir = TempDir(std::env::temp_dir().join(format!("phdb-wfork-{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&dir.0);
    std::fs::create_dir_all(&dir.0).unwrap();
    let path = dir.0.join("w.phdb");
    let vfs: VfsRef = pigeonhole_io::pread::PreadVfs::new(2);

    let db = Engine::open(&path, options(Arc::clone(&vfs))).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for i in 0..100u32 {
        db.commit(
            batch(&t, format!("r{i:03}").as_bytes(), b"v"),
            Some(Durability::Buffered),
        )
        .unwrap();
    }
    db.flush().unwrap();
    let held = db.snapshot().unwrap();
    let (tid, fid) = (t.id, t.families[0].id);

    // SAFETY: the child only calls the engine (which must refuse before touching any
    // thread, queue or lock) from the forking thread and a watchdog thread it starts, then
    // `_exit`s without running the parent's destructors.
    let child = unsafe { libc::fork() };
    assert!(child >= 0, "fork failed");
    if child == 0 {
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(20));
            // SAFETY: ends the child at once.
            unsafe { libc::_exit(HUNG) };
        });
        let mut failed: Vec<&str> = Vec::new();
        let mut expect = |what: &'static str, r: Result<(), Error>| match r {
            Err(Error::Unsupported(msg)) if msg.contains("fork") => {}
            other => {
                eprintln!("child: {what}: {other:?}");
                failed.push(what);
            }
        };
        expect(
            "commit",
            db.commit(batch(&t, b"child", b"x"), Some(Durability::Sync))
                .map(drop),
        );
        expect("flush", db.flush());
        expect("compact", db.compact(None));
        expect("snapshot", db.snapshot().map(drop));
        expect("durable_snapshot", db.durable_snapshot().map(drop));
        expect("get", db.get(&held, tid, fid, b"r001", b"q").map(drop));
        expect(
            "get_latest",
            db.get_latest(tid, fid, b"r001", b"q").map(drop),
        );
        expect(
            "scan",
            db.scan(
                &held,
                tid,
                ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            )
            .and_then(|mut c| c.next_row())
            .map(drop),
        );
        expect("begin", db.begin().map(drop));
        expect(
            "create_table",
            db.create_table("u", &[("f".into(), FamilyOptions::default())])
                .map(drop),
        );
        expect("backup", db.backup(&dir.0.join("copy.phdb")));
        let mut fut = db.get_latest_async(tid, fid, b"r001", b"q");
        let polled = std::pin::Pin::new(&mut fut).poll(&mut Context::from_waker(Waker::noop()));
        expect(
            "get_latest_async",
            match polled {
                Poll::Ready(r) => r.map(drop),
                Poll::Pending => Ok(()),
            },
        );
        if db.close().is_err() {
            failed.push("close");
        }
        drop(held);
        drop(t);
        drop(db);
        // SAFETY: ends the child at once.
        unsafe { libc::_exit(i32::try_from(failed.len()).unwrap_or(98)) };
    }
    let mut status = 0;
    // SAFETY: waits for the child forked above.
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status), "the child died: status {status}");
    let code = libc::WEXITSTATUS(status);
    assert_ne!(code, HUNG, "an entry point hung in the forked child");
    assert_eq!(
        code, 0,
        "{code} entry points did not refuse in the forked child (see its stderr)"
    );

    // The parent is untouched: its snapshot reads, it commits, and a reopen after its close
    // sees exactly its own commits.
    assert!(db.get(&held, tid, fid, b"r001", b"q").unwrap().is_some());
    db.commit(batch(&t, b"r100", b"v"), Some(Durability::Sync))
        .unwrap();
    assert_eq!(count_rows(&db, &t), 101);
    drop(held);
    db.close().unwrap();
    drop(db);
    let db = Engine::open(&path, options(Arc::clone(&vfs))).unwrap();
    let t = db.table("t").unwrap();
    assert!(db.table("u").is_none(), "the child created a table");
    assert_eq!(
        count_rows(&db, &t),
        101,
        "the reopened database lost or gained rows"
    );
    db.close().unwrap();
}
