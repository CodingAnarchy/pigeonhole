//! Helpers for the crate's documentation examples and the user guide's doctests. Not part of
//! the API: hidden, unstable, and never needed by applications.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A database path in a fresh temporary directory, removed (with everything in it) on drop.
#[derive(Debug)]
pub struct TempDb {
    dir: PathBuf,
    path: PathBuf,
}

/// A new [`TempDb`] whose file is named `name`. Unique per process and call.
pub fn temp_db(name: &str) -> TempDb {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-doc-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create a temporary directory");
    let path = dir.join(name);
    TempDb { dir, path }
}

impl TempDb {
    /// The database file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for TempDb {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
