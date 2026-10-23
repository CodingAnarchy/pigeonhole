//! #901: a process forked from one that opened a database can't use or close the parent's
//! handle. With `auto_recover` on, the handle also owns a supervisor thread, which runs only
//! in the parent: the child's close must not wait for a recovery or join that thread. Every
//! operation fails with `ErrorCode::Unsupported` at once, and close returns. Real `fork`, so
//! Unix only.
#![cfg(unix)]

use pigeonhole::{ErrorCode, Options, Pigeonhole, Recovery};

const HUNG: i32 = 99;

#[test]
fn a_forked_child_cannot_use_or_close_the_parents_handle() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("fork.phdb");
    let db = Pigeonhole::open(&path, Options::default().shards(2).auto_recover(Recovery::Io))
        .unwrap();
    let t = pigeonhole::doc_support::table(&db, "t", &["f"]).unwrap();
    t.mutate(b"r").put("f", b"q", b"v").commit().unwrap();

    // SAFETY: the child uses the handle from the forking thread and a watchdog thread it
    // starts, then `_exit`s without running the parent's destructors.
    let child = unsafe { libc::fork() };
    assert!(child >= 0, "fork failed");
    if child == 0 {
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(20));
            // SAFETY: ends the child at once.
            unsafe { libc::_exit(HUNG) };
        });
        let mut bad = 0;
        let unsupported = |code: Option<ErrorCode>| code == Some(ErrorCode::Unsupported);
        if !unsupported(t.get(b"r", "f", b"q").err().map(|e| e.code())) {
            bad |= 1;
        }
        if !unsupported(
            t.mutate(b"child")
                .put("f", b"q", b"x")
                .commit()
                .err()
                .map(|e| e.code()),
        ) {
            bad |= 2;
        }
        if !unsupported(db.snapshot().err().map(|e| e.code())) {
            bad |= 4;
        }
        // Returns at once: no recovery wait, no supervisor join, no engine close.
        drop(t);
        if db.close().is_err() {
            bad |= 8;
        }
        // SAFETY: ends the child at once.
        unsafe { libc::_exit(bad) };
    }
    let mut status = 0;
    // SAFETY: waits for the child forked above.
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status), "the child died: status {status}");
    let code = libc::WEXITSTATUS(status);
    assert_ne!(code, HUNG, "the forked child hung");
    assert_eq!(
        code, 0,
        "exit bits {code}: 1 get, 2 commit, 4 snapshot did not fail Unsupported; 8 close failed"
    );

    // The parent goes on, and its close joins its own supervisor.
    assert!(t.get(b"r", "f", b"q").unwrap().is_some());
    t.mutate(b"r2").put("f", b"q", b"v").commit().unwrap();
    assert!(t.get(b"child", "f", b"q").unwrap().is_none());
    drop(t);
    db.close().unwrap();
}
