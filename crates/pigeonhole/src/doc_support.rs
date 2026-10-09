//! Helpers for the crate's documentation examples and the user guide's doctests. Not part of
//! the API: hidden, unstable, and never needed by applications.

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Family, Pigeonhole, Result, Table};

/// A fresh temporary directory, removed with everything in it on drop. Derefs to its path,
/// so examples write `dir.join("app.phdb")`.
#[derive(Debug)]
pub struct TempDir(PathBuf);

/// A new [`TempDir`], unique per process and call.
pub fn temp_dir() -> TempDir {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-doc-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create a temporary directory");
    TempDir(dir)
}

impl Deref for TempDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Opens (creating if needed) table `name` with `families`: counter families
/// ([`Family::counter`]) for the names `stats` and `hits`, default ones otherwise.
pub fn table(db: &Pigeonhole, name: &str, families: &[&str]) -> Result<Table> {
    families
        .iter()
        .fold(db.table(name)?, |b, f| {
            let family = if matches!(*f, "stats" | "hits") {
                Family::counter()
            } else {
                Family::default()
            };
            b.family(f, family)
        })
        .create_if_missing()
}

/// Runs `fut` to completion on the calling thread: a minimal executor for examples and
/// tests (the thread parks until the future's waker unparks it). Applications use their own
/// executor.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
        std::thread::park();
    }
}
