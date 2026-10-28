//! perf287's S3 on #968: the registry of databases opened with `Options::auto_recover` is
//! locked at such an open. A fork while another thread holds that lock copies it held into
//! the child, whose own supervised open would wait for ever; the child gets a registry of its
//! own instead. Real `fork`, and a test binary of its own: a forked child of a process whose
//! earlier databases left threads behind may block on a lock those threads held (POSIX
//! allows only async-signal-safe calls there), whatever the registry does. Linux only: on
//! macOS a forked child can't open a database at all, since std's thread parking uses
//! libdispatch semaphores there, which abort on the child side of a fork.
#![cfg(target_os = "linux")]

use pigeonhole::{Options, Pigeonhole, Recovery};

const HUNG: i32 = 99;

#[test]
fn a_forked_child_opens_a_supervised_database_while_the_parent_held_the_registry() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("child.phdb");
    // SAFETY: the fork runs while another thread, which the child does not have, holds the
    // registry's lock; the child opens, closes and `_exit`s without running the parent's
    // destructors.
    let child = pigeonhole::hooks::with_watch_registry_locked_elsewhere(|| unsafe { libc::fork() });
    assert!(child >= 0, "fork failed");
    if child == 0 {
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(20));
            // SAFETY: ends the child at once.
            unsafe { libc::_exit(HUNG) };
        });
        let opened = Pigeonhole::open(
            &path,
            Options::default().shards(1).auto_recover(Recovery::Io),
        );
        let code = match opened {
            Ok(db) => i32::from(db.close().is_err()),
            Err(_) => 2,
        };
        // SAFETY: ends the child at once.
        unsafe { libc::_exit(code) };
    }
    let mut status = 0;
    // SAFETY: waits for the child forked above.
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status), "the child died: status {status}");
    let code = libc::WEXITSTATUS(status);
    assert_ne!(code, HUNG, "the forked child hung on the registry's lock");
    assert_eq!(code, 0, "the child's open (2) or close (1) failed");
}
