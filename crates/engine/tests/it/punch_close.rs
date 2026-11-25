//! #2181 (1): in application-owned mode a punch (D333) must not land after the close
//! released the file's locks: another writer may then have the file, and a block a pass
//! took is free in its allocator. Before the fix it could: the last shard's `final_close`
//! runs `release_after_io` (the locks go) before `try_final_close` sets `closed`, which was
//! all a pass checked, and a shard that already reported its part of the close keeps being
//! driven until `EngineShard::closed`. Each of this run's punches during the close came
//! after the writer byte's release, with the byte free to another handle.
//!
//! The run: two application-owned shards, one table (on shard 0). Space a compaction frees
//! is kept retired by a snapshot, so no pass can punch it early. Shard 1 is not driven
//! while shard 0 closes and reports; then the snapshot goes (its reclaim frees the space
//! without starting a pass) and shard 1 is driven again. Its close's commits start a pass,
//! which runs on shard 0's driver. A VFS wrapper holds that pass's punch, past the
//! `closed` check, until the data file's locks are released (or 10 s pass) and records
//! the order.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{Vfs, VfsRef};

const DB: &str = "/db/data.phdb";

#[derive(Debug, Default)]
struct Events {
    /// Punches are held from now on.
    armed: bool,
    /// The writer byte unlocked since arming.
    unlocked: bool,
    /// Punches since arming: whether each came after the writer byte's release, and
    /// whether another handle could take the writer byte just before it.
    punches: Vec<(bool, bool)>,
}

#[derive(Debug, Default)]
struct Watch {
    ev: Mutex<Events>,
    cv: Condvar,
    /// For a second handle on the data file, as another opener's.
    vfs: Mutex<Option<Arc<SimVfs>>>,
}

/// The writer byte (FORMAT §8.3).
const WRITER_BYTE: u64 = 8192;

#[derive(Debug)]
struct WatchVfs {
    inner: Arc<SimVfs>,
    watch: Arc<Watch>,
}

#[derive(Debug)]
struct WatchFile {
    inner: pigeonhole_io::FileRef,
    /// `Some` for the data file.
    watch: Option<Arc<Watch>>,
}

impl Vfs for WatchVfs {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<pigeonhole_io::FileRef> {
        let inner = self.inner.open(path, opts)?;
        let watch = (path == Path::new(DB)).then(|| Arc::clone(&self.watch));
        Ok(Arc::new(WatchFile { inner, watch }))
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
}

impl pigeonhole_io::File for WatchFile {
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
    fn punch_hole(&self, offset: u64, len: u64) -> pigeonhole_io::Result<()> {
        if let Some(w) = &self.watch {
            let mut ev = w.ev.lock().unwrap();
            if ev.armed {
                let until = Instant::now() + Duration::from_secs(10);
                while !ev.unlocked && Instant::now() < until {
                    ev = w.cv.wait_timeout(ev, Duration::from_millis(10)).unwrap().0;
                }
                let after = ev.unlocked;
                // Could another writer open the file now? A second handle takes the
                // writer byte, as an opener would, and lets it go again.
                let vfs = w.vfs.lock().unwrap().clone().expect("vfs");
                let other = vfs
                    .open(
                        Path::new(DB),
                        pigeonhole_io::OpenOptions::read_write_create(),
                    )
                    .expect("second handle");
                let free = other
                    .lock(WRITER_BYTE, pigeonhole_io::LockMode::Exclusive)
                    .is_ok();
                if free {
                    other.unlock(WRITER_BYTE).unwrap();
                }
                ev.punches.push((after, free));
            }
        }
        self.inner.punch_hole(offset, len)
    }
    fn lock(&self, byte: u64, mode: pigeonhole_io::LockMode) -> pigeonhole_io::Result<()> {
        self.inner.lock(byte, mode)
    }
    fn unlock(&self, byte: u64) -> pigeonhole_io::Result<()> {
        let r = self.inner.unlock(byte);
        if let Some(w) = &self.watch {
            let mut ev = w.ev.lock().unwrap();
            if ev.armed && byte == WRITER_BYTE {
                ev.unlocked = true;
                w.cv.notify_all();
            }
        }
        r
    }
    fn holds(&self, byte: u64) -> Option<pigeonhole_io::LockMode> {
        self.inner.holds(byte)
    }
    fn identity(&self) -> pigeonhole_io::Result<pigeonhole_io::FileIdentity> {
        self.inner.identity()
    }
    fn is_local(&self) -> pigeonhole_io::Result<bool> {
        self.inner.is_local()
    }
}

#[test]
fn a_punch_after_an_application_owned_close_released_the_locks() {
    let watch = Arc::new(Watch::default());
    let sim = SimVfs::new(2181);
    *watch.vfs.lock().unwrap() = Some(Arc::clone(&sim));
    let vfs: VfsRef = Arc::new(WatchVfs {
        inner: sim,
        watch: Arc::clone(&watch),
    });
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.memtable_budget = 1 << 20;
    o.memtable_freeze_bytes = 256 << 10;
    o.reader_slots = 8;
    o.wal.segment_size = 8 * 32 * 1024;
    o.wal.spare_segments = 1;
    o.grow_ahead = Some((4 << 20, 4 << 20));
    o.punch_freed = true;
    let mut tuning = pigeonhole_engine::PunchTuning::default();
    tuning.min_reserve = 0;
    tuning.trigger_bytes = 64 << 10;
    o.punch_tuning = Some(tuning);
    let pigeonhole_engine::AppOwned {
        engine: db,
        shards,
        workers,
        ..
    } = Engine::open_application_owned_with(Path::new(DB), o).unwrap();
    assert!(workers.is_empty());

    // Shard 1's driver stops while `pause` is set (and says so in `paused`).
    let pause = Arc::new(AtomicBool::new(false));
    let paused = Arc::new(AtomicBool::new(false));
    let drivers: Vec<_> = shards
        .into_iter()
        .map(|mut s| {
            let (pause, paused) = (Arc::clone(&pause), Arc::clone(&paused));
            std::thread::spawn(move || {
                let one = s.index() == 1;
                loop {
                    if one && pause.load(Ordering::Acquire) {
                        paused.store(true, Ordering::Release);
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    if let Some(r) = s.closed() {
                        return r;
                    }
                    if !s.run_once(u64::MAX) {
                        s.wait(Duration::from_millis(1));
                    }
                }
            })
        })
        .collect();

    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let (tid, fid) = (t.id, t.families[0].id);
    let mut snap = None;
    for round in 1..=3u8 {
        if round == 3 {
            // Keeps what the last compaction frees retired until the snapshot goes.
            snap = Some(db.snapshot().unwrap());
        }
        for i in 0..1024u32 {
            let mut wb = WriteBatch::new();
            let v = vec![round; 4 << 10];
            wb.put(tid, fid, &i.to_be_bytes(), b"q", None, ValueRef::Bytes(&v))
                .unwrap();
            db.commit(wb, Some(Durability::Buffered)).unwrap();
        }
        db.flush().unwrap();
        db.compact(None).unwrap();
    }

    // Shard 1 stops; shard 0 closes and reports.
    pause.store(true, Ordering::Release);
    while !paused.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    watch.ev.lock().unwrap().armed = true;
    let closer = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.close())
    };
    // Shard 0's part: give it time to report (nothing observable says so).
    std::thread::sleep(Duration::from_millis(500));
    assert!(!db.close_finished(), "the close needs shard 1");
    let before = db.metrics();
    drop(snap);
    pause.store(false, Ordering::Release);

    let outcomes: Vec<_> = drivers.into_iter().map(|d| d.join().unwrap()).collect();
    closer.join().unwrap().unwrap();
    for r in outcomes {
        r.unwrap();
    }
    let ev = watch.ev.lock().unwrap();
    // Punching was on: the earlier rounds' freed space was punched.
    assert!(before.file_punches.0 > 0, "{before:?}");
    assert!(
        !ev.punches.iter().any(|&(after, free)| after || free),
        "a punch ran after the close released the file's locks: {:?}",
        ev.punches
    );
}
