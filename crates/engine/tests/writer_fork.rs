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
    // A cursor created before the fork, not yet stepped.
    let mut pre_fork_cursor = db
        .scan(
            &held,
            tid,
            ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        )
        .unwrap();

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
        expect(
            "a cursor created before the fork",
            pre_fork_cursor.next_row().map(drop),
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

    // The parent is untouched: its cursor and snapshot read, it commits, and a reopen after
    // its close sees exactly its own commits.
    let mut parent_rows = 0;
    while pre_fork_cursor.next_row().unwrap() {
        parent_rows += 1;
    }
    assert_eq!(parent_rows, 100);
    drop(pre_fork_cursor);
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

/// Every file in `dir` with its bytes, sorted by name.
fn files_of(dir: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (
                e.file_name().to_string_lossy().into_owned(),
                std::fs::read(e.path()).unwrap_or_default(),
            )
        })
        .collect();
    out.sort();
    out
}

/// Polls `f` once.
fn poll_once<F: std::future::Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    std::pin::Pin::new(f).poll(&mut Context::from_waker(Waker::noop()))
}

/// perf287's M1 and M2 on #919: in application-owned mode a forked child that keeps its
/// event loop going drives its copy of each shard. Its `run_once` must run nothing (the
/// state machine would append to the parent's log and apply into the parent's arenas), and
/// commits, checks and maintenance submitted before the fork must fail `Unsupported` on
/// their first poll instead of waiting for ever. The parent's files are byte-identical after
/// the child ran, and the parent then resolves everything itself.
#[test]
fn a_forked_childs_application_owned_shards_run_nothing_and_its_futures_refuse() {
    let dir = TempDir(std::env::temp_dir().join(format!("phdb-afork-{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&dir.0);
    std::fs::create_dir_all(&dir.0).unwrap();
    let path = dir.0.join("a.phdb");
    let vfs: VfsRef = pigeonhole_io::pread::PreadVfs::new(2);
    let mut o = options(Arc::clone(&vfs));
    o.compaction_threads = 0;
    let (db, mut shards) = Engine::open_application_owned(&path, o).unwrap();
    let drive = |shards: &mut Vec<pigeonhole_engine::EngineShard>| {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
    };
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for i in 0..20u32 {
        let mut pc = db
            .submit_async(
                batch(&t, format!("r{i:03}").as_bytes(), b"v"),
                Some(Durability::Buffered),
            )
            .unwrap();
        loop {
            drive(&mut shards);
            if let Poll::Ready(r) = poll_once(&mut pc) {
                r.unwrap();
                break;
            }
        }
    }
    // Submitted, never driven: they wait in the shards' queues across the fork.
    let mut commit = db
        .submit_async(batch(&t, b"pending", b"v"), Some(Durability::Buffered))
        .unwrap();
    let absent = pigeonhole_engine::Predicate::Absent {
        family: t.families[0].id,
        qualifier: b"q".to_vec(),
    };
    let mut check = db
        .submit_check_and_mutate_async(
            t.id,
            b"checked",
            &absent,
            batch(&t, b"checked", b"v"),
            Some(Durability::Buffered),
        )
        .unwrap();
    let mut flush = db.submit_flush().unwrap();
    let before = files_of(&dir.0);

    // SAFETY: the child calls the engine from the forking thread and a watchdog thread it
    // starts, then `_exit`s without running the parent's destructors.
    let child = unsafe { libc::fork() };
    assert!(child >= 0, "fork failed");
    if child == 0 {
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(20));
            // SAFETY: ends the child at once.
            unsafe { libc::_exit(HUNG) };
        });
        let mut failed = 0;
        let refused = |r: Result<(), Error>| matches!(r, Err(Error::Unsupported(msg)) if msg.contains("fork"));
        for _ in 0..50 {
            for s in shards.iter_mut() {
                if s.run_once(u64::MAX) {
                    failed |= 1;
                }
                s.wait(std::time::Duration::from_millis(1));
                if s.next_deadline().is_some() {
                    failed |= 1;
                }
            }
        }
        let first = |p: Poll<Result<(), Error>>| match p {
            Poll::Ready(r) => refused(r),
            Poll::Pending => false,
        };
        if !first(poll_once(&mut commit).map(|r| r.map(drop))) {
            failed |= 2;
        }
        if !first(poll_once(&mut check).map(|r| r.map(drop))) {
            failed |= 4;
        }
        if !first(poll_once(&mut flush)) {
            failed |= 8;
        }
        let local = shards[0].commit_local(batch(&t, b"local", b"x"), Some(Durability::Sync));
        if !refused(local.map(drop)) {
            failed |= 16;
        }
        // Inert, taking no lock.
        if format!("{:?}", db.metrics()) != format!("{:?}", pigeonhole_engine::Metrics::default())
            || db.table("t").is_some()
        {
            failed |= 32;
        }
        drop(commit);
        drop(check);
        drop(flush);
        drop(shards);
        drop(t);
        drop(db);
        // SAFETY: ends the child at once.
        unsafe { libc::_exit(failed) };
    }
    let mut status = 0;
    // SAFETY: waits for the child forked above.
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status), "the child died: status {status}");
    let code = libc::WEXITSTATUS(status);
    assert_ne!(code, HUNG, "the forked child hung");
    assert_eq!(
        code, 0,
        "exit bits {code}: 1 a shard ran, 2 commit, 4 check, 8 flush did not refuse, \
         16 commit_local did not refuse, 32 an accessor was not inert"
    );
    assert!(
        before == files_of(&dir.0),
        "the forked child changed the parent's files"
    );

    // The parent resolves what was pending, on its own shards.
    let mut done = (false, false, false);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        drive(&mut shards);
        if !done.0
            && let Poll::Ready(r) = poll_once(&mut commit)
        {
            r.unwrap();
            done.0 = true;
        }
        if !done.1
            && let Poll::Ready(r) = poll_once(&mut check)
        {
            assert!(r.unwrap().0, "the check found the row");
            done.1 = true;
        }
        if !done.2
            && let Poll::Ready(r) = poll_once(&mut flush)
        {
            r.unwrap();
            done.2 = true;
        }
        if done == (true, true, true) {
            break;
        }
    }
    assert_eq!(
        done,
        (true, true, true),
        "the parent's commits never resolved"
    );
    assert!(
        db.get_latest(t.id, t.families[0].id, b"pending", b"q")
            .unwrap()
            .is_some()
    );
    drop(t);
    let closing = std::thread::spawn({
        let db = Arc::clone(&db);
        move || db.close()
    });
    while !closing.is_finished() {
        drive(&mut shards);
    }
    closing.join().unwrap().unwrap();
}
