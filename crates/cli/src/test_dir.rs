//! Directories for the unit tests (#2015): each one new, under a name no other run has
//! used, and removed when the test ends.
//!
//! Named only by the process id and a counter, a directory could be one an earlier run
//! left. macOS reuses process ids, and every worktree's runs share one `$TMPDIR`, so a test
//! could start among another run's files and fail. That run's partition file, `.replaced-*`
//! files or staging marker were enough to fail 4 stamp and upload tests once. `tempfile`
//! picks a random name and creates it only if it's new.

use std::path::PathBuf;
use std::time::Duration;

/// The directories [`fresh`] made on this thread, removed when the thread ends.
///
/// A test's thread ends after its servers, runtimes and databases are dropped, so its files
/// are closed by then. Each partition's WAL files are 64 MiB (a shard's first slot). Kept,
/// a full run left ~100 GB of them. They're sparse on macOS and Linux, but not on Windows,
/// whose CI disk filled partway through the cli tests ("no space left on device").
struct Made(std::cell::RefCell<Vec<PathBuf>>);

impl Drop for Made {
    fn drop(&mut self) {
        for dir in self.0.take() {
            // A stopped server's thread may still be closing a handle, and Windows refuses
            // to remove an open file: a few tries.
            for _ in 0..20 {
                if std::fs::remove_dir_all(&dir).is_ok() || !dir.exists() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

thread_local! {
    static MADE: Made = const { Made(std::cell::RefCell::new(Vec::new())) };
}

/// A new, empty directory under the system temp dir, named `prefix` and a random suffix.
/// It's removed when the calling thread ends, which for a test is when the test ends.
/// So call it from the test's own thread: one made on a spawned thread goes when that
/// thread ends, while the test may still use it (audit120's N4 on #2038).
pub(crate) fn fresh(prefix: &str) -> PathBuf {
    let dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create a test directory")
        .keep();
    MADE.with(|m| m.0.borrow_mut().push(dir.clone()));
    dir
}
