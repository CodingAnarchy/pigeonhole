//! #805 (owner decision, 2026-10-21): a full compaction leaves nothing purgeable. A lone SST
//! used to be moved to the last level, or left there, as it was, with tablet changes off;
//! one written while a snapshot or a memtable held its purge back kept every version and
//! delete marker it held. A lone SST whose metadata shows it may hold purgeable entries
//! (delete markers, expired cells, more entries than rows in a family that keeps
//! `max_versions`) is rewritten instead, once per `compact()`.

use crate::common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Durability, Engine, EngineShard, FamilyId, FamilyOptions, TableId, ValueRef, WriteBatch,
};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{FileRef, OpenOptions, ProcessId, SharedOpen, SharedRegion, Vfs, VfsRef};

const DB: &str = "/db/data.phdb";

fn poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    Pin::new(f).poll(&mut Context::from_waker(Waker::noop()))
}

/// Runs the shards until they go idle: a pass in which no shard has work left or did any.
/// `run_once` says whether work remains after its turn, not whether it did any, so a pass is quiet
/// only if no shard did any work either (`EngineShard::work_count`): then it handled
/// nothing, sent nothing, and left nothing queued.
fn run(shards: &mut [EngineShard]) {
    for _ in 0..100_000 {
        let work: u64 = shards.iter().map(EngineShard::work_count).sum();
        let mut more = false;
        for s in shards.iter_mut() {
            more |= s.run_once(u64::MAX);
        }
        if !more && shards.iter().map(EngineShard::work_count).sum::<u64>() == work {
            return;
        }
    }
    panic!("the shards never went idle");
}

fn settle<F: Future + Unpin>(shards: &mut [EngineShard], f: &mut F) -> F::Output {
    for _ in 0..1_000 {
        if let Poll::Ready(r) = poll(f) {
            return r;
        }
        run(shards);
    }
    panic!("never settled");
}

struct Rig {
    db: Arc<Engine>,
    shards: Vec<EngineShard>,
    t: TableId,
    f: FamilyId,
    clock: VfsRef,
}

/// A clock that moves a nanosecond per read (`SimVfs`'s moves only when a test advances it),
/// so a shard pass with a deadline ends even while a full compaction keeps finding work.
#[derive(Debug)]
struct Ticking {
    inner: VfsRef,
    ticks: AtomicU64,
}

impl Vfs for Ticking {
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
        mode: SharedOpen,
    ) -> pigeonhole_io::Result<SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> pigeonhole_io::Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos() + self.ticks.fetch_add(1, Ordering::Relaxed)
    }
    fn clock_is_simulated(&self) -> bool {
        self.inner.clock_is_simulated()
    }
    fn current_process(&self) -> ProcessId {
        self.inner.current_process()
    }
}

impl Rig {
    /// One shard, tablet changes off, a family keeping one version; the fourth L0 SST
    /// triggers a compaction into L1.
    fn open(seed: u64) -> Self {
        Self::open_with(seed, FamilyOptions::default().max_versions(1), None)
    }

    /// As [`Rig::open`], with `family` for the table's family and, if given, a target SST
    /// size (so a rewrite writes a run of several SSTs).
    fn open_with(seed: u64, family: FamilyOptions, target: Option<u64>) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o = common::options(Arc::clone(&vfs), 1, 4 << 20);
        let clock: VfsRef = Arc::new(Ticking {
            inner: Arc::clone(&o.vfs),
            ticks: AtomicU64::new(0),
        });
        o.vfs = Arc::clone(&clock);
        o.tablet_changes = false;
        o.compaction_threads = 0;
        o.compaction.l0_trigger = 4;
        o.compaction.level_base_bytes = u64::MAX;
        if let Some(target) = target {
            o.compaction.target_sst_bytes = target;
        }
        let (db, shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
        let t = db.create_table("t", &[("f".into(), family)]).unwrap();
        let (t, f) = (t.id, t.families[0].id);
        Rig {
            db,
            shards,
            t,
            f,
            clock,
        }
    }

    fn commit(&mut self, build: impl FnOnce(&mut WriteBatch, TableId, FamilyId)) {
        let mut wb = WriteBatch::new();
        build(&mut wb, self.t, self.f);
        let mut pc = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
        settle(&mut self.shards, &mut pc).unwrap();
    }

    fn flush(&mut self) {
        let mut f = self.db.flush_pending().unwrap();
        settle(&mut self.shards, &mut f).unwrap();
    }

    /// A full compaction, which must end within a bounded number of shard passes (one that
    /// kept finding work, rewriting the same SST again and again, would not).
    fn compact(&mut self) {
        let mut c = self.db.compact_pending(Some(self.t)).unwrap();
        for _ in 0..500 {
            if let Poll::Ready(r) = poll(&mut c) {
                r.unwrap();
                run(&mut self.shards);
                return;
            }
            let deadline = self.clock.monotonic_nanos() + 10_000;
            for s in &mut self.shards {
                s.run_once(deadline);
            }
        }
        panic!("the full compaction never ended");
    }

    /// The family's raw entries (every version and marker) in the current view.
    fn entries(&self) -> usize {
        let snap = self.db.snapshot().unwrap();
        self.db
            .raw_entries(&snap)
            .unwrap()
            .into_iter()
            .filter(|e| e.table == self.t && e.family == self.f)
            .count()
    }

    /// The family's levels holding SSTs: `(level, SSTs, entries)`.
    fn levels(&self) -> Vec<(u8, u64, u64)> {
        let info = self.db.info().unwrap();
        info.tables
            .iter()
            .filter(|t| t.info.id == self.t)
            .flat_map(|t| t.families.iter().filter(|f| f.family == self.f))
            .flat_map(|f| f.levels.iter().map(|l| (l.level, l.ssts, l.entries)))
            .collect()
    }

    /// `rows` rows of `cols` columns, each cell one version at `ts` of 100 bytes that do
    /// not compress.
    fn put_rows(&mut self, rows: u32, cols: u32, ts: u64) {
        for chunk in (0..rows).step_by(100) {
            self.commit(|wb, t, f| {
                for i in chunk..(chunk + 100).min(rows) {
                    for c in 0..cols {
                        let (row, q) = (format!("r{i:05}"), format!("q{c}"));
                        let mut x = (u64::from(i) << 32 | u64::from(c)) ^ ts;
                        let value: Vec<u8> = (0..100)
                            .map(|_| {
                                x = x
                                    .wrapping_mul(6364136223846793005)
                                    .wrapping_add(1442695040888963407);
                                (x >> 56) as u8
                            })
                            .collect();
                        wb.put(
                            t,
                            f,
                            row.as_bytes(),
                            q.as_bytes(),
                            Some(ts),
                            ValueRef::Bytes(&value),
                        )
                        .unwrap();
                    }
                }
            });
        }
    }

    fn close(mut self) {
        self.db.close().unwrap();
        run(&mut self.shards);
    }
}

#[test]
fn a_full_compaction_purges_a_lone_sst_written_under_a_snapshot() {
    let mut rig = Rig::open(805);
    // Two rows, four versions each; row `b` deleted after its second version.
    let mut snap = None;
    for (round, ts) in [10u64, 20, 30, 40].into_iter().enumerate() {
        rig.commit(|wb, t, f| {
            for row in [b"a".as_slice(), b"b"] {
                wb.put(t, f, row, b"q", Some(ts), ValueRef::Bytes(b"v"))
                    .unwrap();
            }
        });
        if round == 1 {
            rig.commit(|wb, t, _| wb.delete_row(t, b"b", None).unwrap());
            // A snapshot here keeps the versions and the marker it sees through the
            // background compaction the fourth flush triggers.
            snap = Some(rig.db.snapshot().unwrap());
        }
        rig.flush();
    }
    run(&mut rig.shards);
    let kept = rig.entries();
    assert!(kept > 1, "the snapshot kept versions: {kept}");
    drop(snap);
    // One lone SST in L1 now; tablet changes off: moved as it was, until #805.
    rig.compact();
    // Only row `a`'s newest version: the marker (at the clock's time, above every put of
    // `b`) hides all of `b`, and nothing needs it any more.
    let after = rig.entries();
    assert!(after < kept, "{kept} -> {after}");
    assert_eq!(after, 1, "one version of `a`; `b` and its marker purged");
    rig.close();
}

#[test]
fn a_lone_sst_that_still_looks_purgeable_is_rewritten_once_per_full_compaction() {
    let mut rig = Rig::open(806);
    // Rows of two columns: more entries than rows whatever is purged. A snapshot taken
    // before the last flush is live through the background compaction it triggers, and
    // through the full compactions below, so no output carries the fully-purged property
    // (D268), and the SST still looks as if it might hold extra versions.
    let mut snap = None;
    for ts in [10u64, 20, 30, 40] {
        rig.commit(|wb, t, f| {
            for q in [b"q1".as_slice(), b"q2"] {
                wb.put(t, f, b"r", q, Some(ts), ValueRef::Bytes(b"v"))
                    .unwrap();
            }
        });
        if ts == 40 {
            snap = Some(rig.db.snapshot().unwrap());
        }
        rig.flush();
    }
    run(&mut rig.shards);
    rig.db.record_history(true);
    // Ends (a full compaction runs until nothing is left to do) after one rewrite.
    rig.compact();
    let rewrites = rig.db.take_compactions().len();
    assert_eq!(rewrites, 1, "one rewrite of the lone SST");
    assert_eq!(rig.entries(), 2, "one version per column");
    // A second request rewrites it once more, and ends too.
    rig.compact();
    assert_eq!(rig.db.take_compactions().len(), 1);
    // With the snapshot gone, a rewrite carries the property, and then none follows.
    drop(snap);
    rig.compact();
    assert_eq!(rig.db.take_compactions().len(), 1);
    rig.compact();
    assert_eq!(
        rig.db.take_compactions().len(),
        0,
        "fully purged: left alone"
    );
    rig.close();
}

/// #820: a last-level run of several SSTs a full compaction wrote while a snapshot held its
/// purge back keeps every version; once the snapshot goes, the next full compaction purges
/// it (#814 left a run as it was). Its output, written with nothing held back, carries the
/// fully-purged property (D268), and a third full compaction rewrites nothing.
#[test]
fn a_full_compaction_purges_a_last_level_run_written_under_a_snapshot() {
    let mut rig = Rig::open_with(
        820,
        FamilyOptions::default().max_versions(1),
        Some(32 << 10),
    );
    rig.put_rows(2000, 1, 10);
    rig.flush();
    let snap = rig.db.snapshot().unwrap();
    rig.put_rows(2000, 1, 20);
    rig.flush();
    rig.compact();
    let held = rig.levels();
    assert!(
        matches!(held[..], [(_, ssts, 4000)] if ssts > 1),
        "one run of several SSTs keeping both versions: {held:?}"
    );
    drop(snap);
    rig.compact();
    let purged = rig.levels();
    assert!(
        matches!(purged[..], [(_, _, 2000)]),
        "one version per cell: {purged:?}"
    );
    rig.db.record_history(true);
    rig.compact();
    assert_eq!(
        rig.db.take_compactions().len(),
        0,
        "fully purged: left alone"
    );
    rig.close();
}

/// D268: rows of several columns always have more entries than rows, so without the
/// property a family keeping `max_versions` had its whole last level rewritten by every
/// full compaction. A run a full compaction wrote with nothing held back is left alone.
#[test]
fn a_run_of_several_columns_purged_with_nothing_held_back_is_not_rewritten_again() {
    let mut rig = Rig::open_with(
        821,
        FamilyOptions::default().max_versions(1),
        Some(32 << 10),
    );
    rig.put_rows(1000, 3, 10);
    rig.flush();
    rig.put_rows(1000, 3, 20);
    rig.flush();
    rig.compact();
    let run = rig.levels();
    assert!(
        matches!(run[..], [(_, ssts, 3000)] if ssts > 1),
        "one run, one version per cell: {run:?}"
    );
    rig.db.record_history(true);
    for _ in 0..2 {
        rig.compact();
        assert_eq!(rig.db.take_compactions().len(), 0, "rewritten again");
    }
    assert_eq!(rig.levels(), run);
    rig.close();
}

/// #820 (ctr274's review): a run of several SSTs under a live snapshot still looks
/// purgeable after each rewrite (the snapshot keeps the versions, and no output carries the
/// fully-purged property), so each full compaction rewrites it exactly once and returns.
/// Once the snapshot goes, one more rewrite purges it and marks it, and then none follows.
#[test]
fn a_run_under_a_live_snapshot_is_rewritten_once_per_full_compaction() {
    let mut rig = Rig::open_with(
        822,
        FamilyOptions::default().max_versions(1),
        Some(32 << 10),
    );
    rig.put_rows(2000, 1, 10);
    rig.flush();
    let snap = rig.db.snapshot().unwrap();
    rig.put_rows(2000, 1, 20);
    rig.flush();
    rig.compact();
    let held = rig.levels();
    assert!(
        matches!(held[..], [(_, ssts, 4000)] if ssts > 1),
        "one run of several SSTs keeping both versions: {held:?}"
    );
    rig.db.record_history(true);
    for _ in 0..2 {
        rig.compact();
        assert_eq!(rig.db.take_compactions().len(), 1, "one rewrite of the run");
        assert_eq!(rig.levels(), held, "the snapshot keeps both versions");
    }
    drop(snap);
    rig.compact();
    assert_eq!(rig.db.take_compactions().len(), 1);
    assert!(matches!(rig.levels()[..], [(_, _, 2000)]));
    rig.compact();
    assert_eq!(
        rig.db.take_compactions().len(),
        0,
        "fully purged: left alone"
    );
    rig.close();
}
