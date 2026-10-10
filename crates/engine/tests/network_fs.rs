//! Issue #147: a database on a network filesystem is refused with `NetworkFilesystem`,
//! including one whose byte-range locks fail (NFS mounted `nolock`, or lockd down): the
//! local-filesystem check runs before the writer lock is taken. #299 (D173): FUSE is refused
//! the same way unless `EngineOptions::allow_fuse`, which accepts FUSE only.

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, Error};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{FileRef, Locality, OpenOptions, Vfs, VfsRef};

const DB: &str = "/db/remote.phdb";

/// `SimVfs` storage that reports every file on a filesystem of kind `locality`; with
/// `locks_fail`, every lock call fails as `F_OFD_SETLK` does with `ENOLCK`.
#[derive(Debug)]
struct RemoteVfs {
    inner: Arc<SimVfs>,
    locality: Locality,
    locks_fail: bool,
}

#[derive(Debug)]
struct RemoteFile {
    inner: FileRef,
    locality: Locality,
    locks_fail: bool,
}

impl Vfs for RemoteVfs {
    fn open(&self, path: &Path, opts: OpenOptions) -> pigeonhole_io::Result<FileRef> {
        Ok(Arc::new(RemoteFile {
            inner: self.inner.open(path, opts)?,
            locality: self.locality,
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
    fn direct_align(&self) -> Option<usize> {
        self.inner.direct_align()
    }

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
        Ok(self.locality == Locality::Local)
    }
    fn locality(&self) -> pigeonhole_io::Result<Locality> {
        Ok(self.locality)
    }
}

fn options(sim: &Arc<SimVfs>, locality: Locality, locks_fail: bool, fuse: bool) -> EngineOptions {
    let vfs: VfsRef = Arc::new(RemoteVfs {
        inner: Arc::clone(sim),
        locality,
        locks_fail,
    });
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 1 << 20;
    o.wal.segment_size = 256 << 10;
    o.allow_fuse = fuse;
    o
}

fn open(locality: Locality, locks_fail: bool, fuse: bool) -> Result<Arc<Engine>, Error> {
    Engine::open(
        Path::new(DB),
        options(&SimVfs::new(147), locality, locks_fail, fuse),
    )
}

#[test]
fn a_network_filesystem_is_refused_even_when_its_locks_fail() {
    for locks_fail in [false, true] {
        // The FUSE opt-in does not admit a network filesystem.
        for fuse in [false, true] {
            match open(Locality::Network, locks_fail, fuse) {
                Err(Error::NetworkFilesystem) => {}
                Err(e) => panic!("locks fail: {locks_fail}, fuse {fuse}: {e:?}"),
                Ok(_) => panic!("locks fail: {locks_fail}: opened on a network filesystem"),
            }
        }
    }
}

/// #299 (D173): FUSE is refused by default, before the writer lock, and accepted with
/// `allow_fuse`, by a writer and by a reader process alike.
#[test]
fn fuse_is_refused_unless_allowed() {
    for locks_fail in [false, true] {
        match open(Locality::Fuse, locks_fail, false) {
            Err(Error::NetworkFilesystem) => {}
            Err(e) => panic!("locks fail: {locks_fail}: {e:?}"),
            Ok(_) => panic!("opened on FUSE without the opt-in"),
        }
    }
    let sim = SimVfs::new(299);
    let db = Engine::open(Path::new(DB), options(&sim, Locality::Fuse, false, true)).unwrap();
    db.create_table(
        "t",
        &[("f".into(), pigeonhole_engine::FamilyOptions::default())],
    )
    .unwrap();
    match Engine::open_reader(Path::new(DB), options(&sim, Locality::Fuse, false, false)) {
        Err(Error::NetworkFilesystem) => {}
        Err(e) => panic!("reader without the opt-in: {e:?}"),
        Ok(_) => panic!("a reader opened on FUSE without the opt-in"),
    }
    let reader =
        Engine::open_reader(Path::new(DB), options(&sim, Locality::Fuse, false, true)).unwrap();
    assert!(reader.table("t").is_some());
    reader.close().unwrap();
    db.close().unwrap();
}
