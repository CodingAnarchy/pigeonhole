//! Issue #147: a database on a network filesystem is refused with `NetworkFilesystem`,
//! including one whose byte-range locks fail (NFS mounted `nolock`, or lockd down): the
//! local-filesystem check runs before the writer lock is taken.

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, Error};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{FileRef, OpenOptions, Vfs, VfsRef};

const DB: &str = "/db/remote.phdb";

/// `SimVfs` storage that reports every file as remote; with `locks_fail`, every lock call
/// fails as `F_OFD_SETLK` does with `ENOLCK`.
#[derive(Debug)]
struct RemoteVfs {
    inner: Arc<SimVfs>,
    locks_fail: bool,
}

#[derive(Debug)]
struct RemoteFile {
    inner: FileRef,
    locks_fail: bool,
}

impl Vfs for RemoteVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> pigeonhole_io::Result<FileRef> {
        Ok(Arc::new(RemoteFile {
            inner: self.inner.open(path, opts)?,
            locks_fail: self.locks_fail,
        }))
    }
    fn remove(&self, path: &Path) -> pigeonhole_io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> pigeonhole_io::Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> pigeonhole_io::Result<Vec<std::path::PathBuf>> {
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
        self.inner.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos()
    }
    fn clock_is_simulated(&self) -> bool {
        self.inner.clock_is_simulated()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}

impl pigeonhole_io::File for RemoteFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> pigeonhole_io::Completion {
        self.inner.submit_read(buf, offset)
    }
    fn submit_write(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> pigeonhole_io::Completion {
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> pigeonhole_io::Result<()> {
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> pigeonhole_io::Completion<()> {
        self.inner.submit_sync_data()
    }
    fn sync_all(&self) -> pigeonhole_io::Result<()> {
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
        if self.locks_fail {
            return Err(pigeonhole_io::Error::new(
                pigeonhole_io::ErrorKind::Other,
                "lock: no locks available (ENOLCK)",
            ));
        }
        self.inner.lock(byte, mode)
    }
    fn unlock(&self, byte: u64) -> pigeonhole_io::Result<()> {
        self.inner.unlock(byte)
    }
    fn identity(&self) -> pigeonhole_io::Result<pigeonhole_io::FileIdentity> {
        self.inner.identity()
    }
    fn is_local(&self) -> pigeonhole_io::Result<bool> {
        Ok(false)
    }
}

fn open(locks_fail: bool) -> Result<Arc<Engine>, Error> {
    let vfs: VfsRef = Arc::new(RemoteVfs {
        inner: SimVfs::new(147),
        locks_fail,
    });
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 1 << 20;
    o.wal.segment_size = 256 << 10;
    Engine::open(Path::new(DB), o)
}

#[test]
fn a_network_filesystem_is_refused_even_when_its_locks_fail() {
    for locks_fail in [false, true] {
        match open(locks_fail) {
            Err(Error::NetworkFilesystem) => {}
            Err(e) => panic!("locks fail: {locks_fail}: {e:?}"),
            Ok(_) => panic!("locks fail: {locks_fail}: opened on a network filesystem"),
        }
    }
}
