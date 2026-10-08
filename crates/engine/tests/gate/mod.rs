//! A VFS that holds, fails or panics on chosen files' asynchronous syncs, so a test can keep
//! I/O in flight across scheduling points (a WAL group unresolved, a manifest root commit
//! blocked) and decide when it completes. It also fails reads, and writes to the main
//! file, whose bytes contain a marker (one table's SST blocks, stored uncompressed), so a
//! test can make one table's flushes or compactions fail. Storage is `SimVfs` on a real
//! clock.

// Shared by several test binaries, each using a subset of it (as `tests/common` is).
#![allow(dead_code)]

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{Completion, FileRef, Resolver, Vfs, VfsRef};

/// What to do with a file's syncs.
#[derive(Debug, Default)]
struct Rules {
    hold: HashSet<PathBuf>,
    fail: HashSet<PathBuf>,
    panic: HashSet<PathBuf>,
}

/// A held sync: the file, its completion's resolver, and the sync's real outcome.
type Held = (PathBuf, Resolver<()>, pigeonhole_io::Result<()>);

/// The test's handle on the VFS's rules and held completions.
#[derive(Debug, Default)]
pub struct Gate {
    rules: Mutex<Rules>,
    held: Mutex<Vec<Held>>,
    /// Reads whose bytes contain this marker fail (one table's SST blocks, say).
    read_marker: Mutex<Option<Vec<u8>>>,
    /// When each injected read failure happened.
    read_failures: Mutex<Vec<Instant>>,
    /// Writes to the main `.phdb` file whose bytes contain this marker fail (one table's
    /// SST blocks; WAL records go to other files).
    write_marker: Mutex<Option<Vec<u8>>>,
    /// When each injected write failure happened.
    write_failures: Mutex<Vec<Instant>>,
}

impl Gate {
    /// Holds every later asynchronous sync of `path` until [`Gate::release`].
    pub fn hold(&self, path: &Path) {
        self.rules.lock().unwrap().hold.insert(path.to_path_buf());
    }

    /// Fails every later sync (blocking or not) of `path`.
    pub fn fail(&self, path: &Path) {
        self.rules.lock().unwrap().fail.insert(path.to_path_buf());
    }

    /// Panics in the next asynchronous sync of `path` (on whatever thread submits it).
    pub fn panic(&self, path: &Path) {
        self.rules.lock().unwrap().panic.insert(path.to_path_buf());
    }

    /// Fails every later read (blocking) whose bytes contain `marker`, or none (`None`).
    pub fn fail_reads_containing(&self, marker: Option<&[u8]>) {
        *self.read_marker.lock().unwrap() = marker.map(<[u8]>::to_vec);
    }

    /// When each injected read failure happened.
    pub fn read_failures(&self) -> Vec<Instant> {
        self.read_failures.lock().unwrap().clone()
    }

    /// Fails every later write to the main file whose bytes contain `marker`, or none.
    pub fn fail_writes_containing(&self, marker: Option<&[u8]>) {
        *self.write_marker.lock().unwrap() = marker.map(<[u8]>::to_vec);
    }

    /// When each injected write failure happened.
    pub fn write_failures(&self) -> Vec<Instant> {
        self.write_failures.lock().unwrap().clone()
    }

    /// Paths with a sync held now.
    pub fn held(&self) -> Vec<PathBuf> {
        self.held
            .lock()
            .unwrap()
            .iter()
            .map(|(p, _, _)| p.clone())
            .collect()
    }

    /// Waits (up to 10 s) until a sync of `path` is held.
    pub fn wait_held(&self, path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.held().iter().any(|p| p == path) {
            assert!(Instant::now() < deadline, "no sync of {path:?} was held");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Stops holding and completes every held sync (on this thread), including syncs the
    /// completions submit in turn.
    pub fn release(&self) {
        self.rules.lock().unwrap().hold.clear();
        loop {
            let held = std::mem::take(&mut *self.held.lock().unwrap());
            if held.is_empty() {
                return;
            }
            for (_, resolver, result) in held {
                resolver.resolve(result);
            }
        }
    }
}

/// The VFS: `SimVfs` storage, a real clock, and the gate's rules.
#[derive(Debug)]
pub struct GateVfs {
    inner: Arc<SimVfs>,
    /// `None`: the simulator's own clock, frozen unless the test advances it.
    start: Option<Instant>,
    gate: Arc<Gate>,
}

/// A new gated VFS and its gate.
pub fn vfs(seed: u64) -> (VfsRef, Arc<Gate>) {
    gated(seed, Some(Instant::now()))
}

/// As [`vfs`], on the simulator's frozen clock.
pub fn frozen_vfs(seed: u64) -> (VfsRef, Arc<Gate>) {
    gated(seed, None)
}

fn gated(seed: u64, start: Option<Instant>) -> (VfsRef, Arc<Gate>) {
    let gate = Arc::new(Gate::default());
    let vfs = Arc::new(GateVfs {
        inner: SimVfs::new(seed),
        start,
        gate: Arc::clone(&gate),
    });
    (vfs, gate)
}

#[derive(Debug)]
struct GateFile {
    path: PathBuf,
    inner: FileRef,
    gate: Arc<Gate>,
}

impl GateFile {
    fn failing(&self) -> bool {
        self.gate.rules.lock().unwrap().fail.contains(&self.path)
    }

    /// Whether a write of `buf` to this file must fail.
    fn write_fails(&self, buf: &[u8]) -> bool {
        if !self.path.to_string_lossy().ends_with(".phdb") {
            return false;
        }
        let failing = match &*self.gate.write_marker.lock().unwrap() {
            Some(marker) => buf.windows(marker.len()).any(|w| w == &marker[..]),
            None => false,
        };
        if failing {
            self.gate
                .write_failures
                .lock()
                .unwrap()
                .push(Instant::now());
        }
        failing
    }

    fn injected() -> pigeonhole_io::Error {
        pigeonhole_io::Error::new(pigeonhole_io::ErrorKind::Other, "injected sync failure")
    }
}

impl Vfs for GateVfs {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<FileRef> {
        Ok(Arc::new(GateFile {
            path: path.to_path_buf(),
            inner: self.inner.open(path, opts)?,
            gate: Arc::clone(&self.gate),
        }))
    }
    fn remove(&self, path: &Path) -> pigeonhole_io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> pigeonhole_io::Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> pigeonhole_io::Result<Vec<PathBuf>> {
        self.inner.list_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> pigeonhole_io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: pigeonhole_io::SharedOpen,
    ) -> pigeonhole_io::Result<pigeonhole_io::SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> pigeonhole_io::Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros() + self.start.map_or(0, |s| s.elapsed().as_micros() as u64)
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos() + self.start.map_or(0, |s| s.elapsed().as_nanos() as u64)
    }
    fn clock_is_simulated(&self) -> bool {
        self.start.is_none() && self.inner.clock_is_simulated()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}

impl pigeonhole_io::File for GateFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.read_at(buf, offset)?;
        if let Some(marker) = &*self.gate.read_marker.lock().unwrap()
            && buf.windows(marker.len()).any(|w| w == &marker[..])
        {
            self.gate.read_failures.lock().unwrap().push(Instant::now());
            return Err(pigeonhole_io::Error::new(
                pigeonhole_io::ErrorKind::Other,
                "injected read failure",
            ));
        }
        Ok(())
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> pigeonhole_io::Result<()> {
        if self.write_fails(buf) {
            return Err(pigeonhole_io::Error::new(
                pigeonhole_io::ErrorKind::Other,
                "injected write failure",
            ));
        }
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> Completion {
        self.inner.submit_read(buf, offset)
    }
    fn submit_write(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> Completion {
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> pigeonhole_io::Result<()> {
        if self.failing() {
            return Err(Self::injected());
        }
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> Completion<()> {
        let (hold, panic) = {
            let mut rules = self.gate.rules.lock().unwrap();
            (
                rules.hold.contains(&self.path),
                rules.panic.remove(&self.path),
            )
        };
        assert!(!panic, "injected panic in a sync of {:?}", self.path);
        let result = if self.failing() {
            Err(Self::injected())
        } else {
            self.inner.sync_data()
        };
        if !hold {
            return Completion::ready(result);
        }
        let (completion, resolver) = Completion::pair();
        self.gate
            .held
            .lock()
            .unwrap()
            .push((self.path.clone(), resolver, result));
        completion
    }
    fn sync_all(&self) -> pigeonhole_io::Result<()> {
        if self.failing() {
            return Err(Self::injected());
        }
        self.inner.sync_all()
    }
    fn len(&self) -> pigeonhole_io::Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> pigeonhole_io::Result<()> {
        self.inner.set_len(len)
    }
    fn allocate(&self, offset: u64, len: u64) -> pigeonhole_io::Result<()> {
        self.inner.allocate(offset, len)
    }
    fn lock(&self, byte: u64, mode: pigeonhole_io::LockMode) -> pigeonhole_io::Result<()> {
        self.inner.lock(byte, mode)
    }
    fn unlock(&self, byte: u64) -> pigeonhole_io::Result<()> {
        self.inner.unlock(byte)
    }
    fn identity(&self) -> pigeonhole_io::Result<pigeonhole_io::FileIdentity> {
        self.inner.identity()
    }
    fn is_local(&self) -> pigeonhole_io::Result<bool> {
        self.inner.is_local()
    }
}

/// Runs `f` on a new thread and returns its result, or `None` if it has not finished within
/// `secs` (the thread is left behind: a hang is the failure being tested for).
pub fn within<T: Send + 'static>(secs: u64, f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(secs)).ok()
}
