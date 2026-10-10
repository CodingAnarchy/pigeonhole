//! Helpers shared by the integration tests.
// Shared by several test binaries, each using a subset of it.
#![allow(dead_code)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, VfsRef};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A name unique to this process and call (short: macOS limits POSIX shm names to 31 bytes).
pub fn unique(tag: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ph{}{}{}", std::process::id(), tag, n)
}

/// A temporary directory removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("pigeonhole-io-{}", unique(tag)));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One backend under test, rooted at a directory it owns.
pub struct Backend {
    pub name: &'static str,
    pub vfs: VfsRef,
    pub root: PathBuf,
    _dir: Option<TempDir>,
}

impl Backend {
    pub fn pread(tag: &str) -> Self {
        let dir = TempDir::new(tag);
        Self {
            name: "pread",
            vfs: PreadVfs::new(2),
            root: dir.0.clone(),
            _dir: Some(dir),
        }
    }

    /// Real files with submitted I/O on io_uring. Fails where io_uring is unavailable, so a
    /// run that should cover it cannot pass on `pread` instead.
    #[cfg(target_os = "linux")]
    pub fn uring(tag: &str) -> Self {
        let dir = TempDir::new(tag);
        Self {
            name: "uring",
            vfs: pigeonhole_io::uring::UringVfs::new().expect("io_uring is available"),
            root: dir.0.clone(),
            _dir: Some(dir),
        }
    }

    /// [`Backend::uring`] for application-owned mode: no reaper thread (#408).
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub fn uring_application_owned(tag: &str) -> Self {
        let dir = TempDir::new(tag);
        Self {
            name: "uring-app",
            vfs: pigeonhole_io::uring::UringVfs::new_application_owned()
                .expect("io_uring is available"),
            root: dir.0.clone(),
            _dir: Some(dir),
        }
    }

    pub fn sim(seed: u64) -> Self {
        Self::sim_from(SimVfs::new(seed))
    }

    pub fn sim_from(vfs: Arc<SimVfs>) -> Self {
        Self {
            name: "sim",
            vfs,
            root: PathBuf::from("/sim/root"),
            _dir: None,
        }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    pub fn create(&self, name: &str) -> pigeonhole_io::FileRef {
        self.vfs
            .open(&self.path(name), OpenOptions::read_write_create())
            .expect("create file")
    }
}

/// Whether two paths name the same entry (temp dirs may be reached through symlinks).
pub fn same_name(a: &Path, b: &Path) -> bool {
    a.file_name() == b.file_name()
}

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// A minimal executor: polls `fut` on this thread, parking until woken.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => std::thread::park(),
        }
    }
}
