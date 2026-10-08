//! Issue #263: a real clock that ticks coarsely (every 4 ms here, like
//! `CLOCK_MONOTONIC_COARSE`) is never taken as stopped. Before, a timer that polled an
//! unchanged reading 1024 times decided the clock had stopped, and a commit waiting for
//! arena room that snapshots hold was refused with `Busy` at once (D126's simulator
//! fallback) instead of after the stall timeout.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, Error, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{FileRef, OpenOptions, Vfs, VfsRef};

const DB: &str = "/db/coarse.phdb";
const TICK: Duration = Duration::from_millis(4);

/// `SimVfs` storage on a real clock that only moves in `TICK` steps.
#[derive(Debug)]
struct CoarseClock {
    inner: Arc<SimVfs>,
    start: Instant,
}

impl CoarseClock {
    fn elapsed_nanos(&self) -> u64 {
        let tick = TICK.as_nanos() as u64;
        (self.start.elapsed().as_nanos() as u64) / tick * tick
    }
}

impl Vfs for CoarseClock {
    fn open(&self, path: &Path, opts: OpenOptions) -> pigeonhole_io::Result<FileRef> {
        self.inner.open(path, opts)
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
        self.inner.now_micros() + self.elapsed_nanos() / 1_000
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos() + self.elapsed_nanos()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}

#[test]
fn a_coarse_real_clock_waits_for_the_stall_timeout() {
    let vfs: VfsRef = Arc::new(CoarseClock {
        inner: SimVfs::new(263),
        start: Instant::now(),
    });
    let timeout = Duration::from_millis(300);
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 1 << 20;
    o.write_stall_timeout_nanos = timeout.as_nanos() as u64;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let value = vec![7u8; 16 << 10];
    let mut snaps = Vec::new();
    for i in 0..5_000u32 {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            format!("r{i:05}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        let started = Instant::now();
        match db.commit(wb, Some(Durability::Buffered)) {
            Ok(_) => snaps.push(db.snapshot().unwrap()),
            Err(Error::Busy) => {
                let waited = started.elapsed();
                assert!(
                    waited >= timeout,
                    "refused after {waited:?} on a moving (coarse) clock: taken as stopped"
                );
                drop(snaps);
                db.close().unwrap();
                return;
            }
            Err(e) => panic!("commit {i}: {e}"),
        }
    }
    panic!("snapshots holding the arena never stalled a write");
}
