//! `shrink` (issue #138): it gives back a tail that holds nothing live, reports the bytes
//! the file shrank by, and commits its moves against the catalog at commit time, so a
//! compaction, a trivial move or a `drop_table` that commits while it copies is neither an
//! error nor undone.
//!
//! Application-owned with the one shard driven by the test, so nothing flushes, compacts or
//! publishes a view behind its back: an engine-owned shard thread still holding an older
//! view made a reclaim miss the dropped table's extent (shrink then had nowhere to move the
//! SST) or the abandoned copy (issue #138's follow-up flaked on both).

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PickerOptions, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_format::{Durability, TableId};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/data.phdb";

/// One shard and only explicit compactions, so nothing allocates between measurements. A
/// 4 MiB freeze size makes each flush one SST of about the bytes written since the last.
fn options(vfs: Arc<SimVfs>) -> EngineOptions {
    let mut o = common::options(vfs, 1, 16 << 20);
    o.memtable_freeze_bytes = 4 << 20;
    o.compaction.l0_trigger = u32::MAX;
    o.compaction.level_base_bytes = u64::MAX;
    o
}

fn file_len(vfs: &SimVfs) -> u64 {
    vfs.open(Path::new(DB), OpenOptions::read())
        .unwrap()
        .len()
        .unwrap()
}

/// The test's handle on the engine's one shard. Clones share it, so a shrink hook can run
/// the shard for a compaction it starts.
#[derive(Clone)]
struct Driver(Arc<Mutex<EngineShard>>);

impl Driver {
    /// Runs the shard once; returns whether it has work left.
    fn step(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .run_once(u64::MAX)
    }

    /// Runs the shard until `f` resolves.
    fn wait<F: Future + Unpin>(&self, mut f: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..100_000 {
            if let Poll::Ready(r) = Pin::new(&mut f).poll(&mut cx) {
                return r;
            }
            self.step();
        }
        panic!("the shard never resolved the request");
    }
}

/// An application-owned engine and the driver of its one shard.
struct Rig {
    db: Arc<Engine>,
    shard: Driver,
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>) -> Self {
        let (db, mut shards) =
            Engine::open_application_owned(Path::new(DB), options(Arc::clone(vfs))).unwrap();
        assert_eq!(shards.len(), 1);
        let shard = Driver(Arc::new(Mutex::new(shards.remove(0))));
        // Until it first runs, the shard has no driver and the open's work is pending.
        while shard.step() {}
        Self { db, shard }
    }

    fn commit(&self, wb: WriteBatch) {
        let pending = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
        self.shard.wait(pending).unwrap();
    }

    fn flush(&self) {
        self.shard.wait(self.db.flush_pending().unwrap()).unwrap();
    }

    fn compact(&self, t: Option<TableId>) {
        self.shard
            .wait(self.db.compact_pending(t).unwrap())
            .unwrap();
    }

    /// Closes, running the shard until it has finished.
    fn close(self) {
        self.db.close().unwrap();
        for _ in 0..100_000 {
            self.shard.step();
            let closed = self
                .shard
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .closed();
            if let Some(r) = closed {
                r.unwrap();
                return;
            }
        }
        panic!("the close never finished");
    }
}

/// Writes rows `range` of 1 KiB incompressible values (below the blob threshold), so an
/// SST's size tracks the bytes written.
fn write_rows(rig: &Rig, t: &TableInfo, range: std::ops::Range<u32>) {
    let f = t.families[0].id;
    let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ u64::from(range.start);
    for i in range {
        let mut v = vec![0u8; 1024];
        for b in &mut v {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            f,
            format!("row{i:06}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&v),
        )
        .unwrap();
        rig.commit(wb);
    }
}

fn delete_rows(rig: &Rig, t: TableId, range: std::ops::Range<u32>) {
    for i in range {
        let mut wb = WriteBatch::new();
        wb.delete_row(t, format!("row{i:06}").as_bytes(), None)
            .unwrap();
        rig.commit(wb);
    }
}

fn row_count(db: &Engine, t: &TableInfo) -> usize {
    let snap = db.snapshot().unwrap();
    let mut cursor = db
        .scan(
            &snap,
            t.id,
            pigeonhole_engine::ScanSpec::new(
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
            ),
        )
        .unwrap();
    let mut n = 0;
    while cursor.next_row().unwrap() {
        n += 1;
    }
    n
}

/// The ids of the SSTs of `t`'s tablets, with their levels.
fn ssts_of(db: &Engine, t: TableId) -> Vec<(u8, u64)> {
    let mut out: Vec<(u8, u64)> = db
        .sst_levels()
        .into_iter()
        .filter(|(table, ..)| *table == t)
        .map(|(_, level, id)| (level, id))
        .collect();
    out.sort_unstable();
    out
}

/// A database where `t`'s SSTs sit past the shrink point: a table written first (`junk`,
/// one SST of the same size class as each of `t`'s, 2 MiB at 1500 `rows`) was dropped,
/// leaving a free extent of their class below them. Returns `t`.
fn tail_behind_a_dropped_table(rig: &Rig, flushes: u32, rows: u32) -> Arc<TableInfo> {
    let db = &rig.db;
    let junk = db
        .create_table("junk", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    write_rows(rig, &junk, 0..rows);
    rig.flush();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for n in 0..flushes {
        write_rows(rig, &t, n * rows..(n + 1) * rows);
        rig.flush();
    }
    db.drop_table(junk.id).unwrap();
    // The shard takes the drop's message now, not in the middle of a shrink.
    while rig.shard.step() {}
    t
}

#[test]
fn shrink_gives_back_the_space_of_deleted_rows() {
    // 8-9 F4: delete every row, compact, shrink. The compaction left nothing live past the
    // manifest, so shrink had nothing to relocate and returned before truncating: the file
    // kept its full length (128 MiB in the review's probe) and shrink returned 0, even
    // after a reopen.
    let vfs = SimVfs::new(138);
    let rig = Rig::open(&vfs);
    let db = &rig.db;
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    write_rows(&rig, &t, 0..3000);
    rig.flush();
    rig.compact(None);
    delete_rows(&rig, t.id, 0..3000);
    rig.flush();
    rig.compact(None);
    assert_eq!(row_count(db, &t), 0);
    assert!(
        ssts_of(db, t.id).is_empty(),
        "the compaction dropped every row"
    );

    let before = file_len(&vfs);
    assert!(before >= 4 << 20, "the load grew the file: {before}");
    let released = db.shrink().unwrap();
    let after = file_len(&vfs);
    assert_eq!(
        before - after,
        released,
        "shrink reports the bytes released"
    );
    assert!(
        after <= 512 << 10,
        "only the manifest is live, yet the file is {after} bytes"
    );
    assert_eq!(db.shrink().unwrap(), 0, "nothing more to release");
    assert_eq!(db.unreferenced_bytes(), 0);
    rig.close();
    let rig = Rig::open(&vfs);
    assert_eq!(row_count(&rig.db, &rig.db.table("t").unwrap()), 0);
    rig.close();
    // The open rewrote the manifest (one more small extent), nothing else.
    assert!(file_len(&vfs) <= 1 << 20, "{}", file_len(&vfs));
}

#[test]
fn shrink_skips_an_sst_a_compaction_retired_under_it() {
    // 7 F7-5: a compaction that committed between shrink's catalog read and its relocation
    // retired the input, and `relocate` refused it: shrink failed with "relocate of an
    // extent that is not live".
    let vfs = SimVfs::new(1381);
    let rig = Rig::open(&vfs);
    let db = &rig.db;
    let t = tail_behind_a_dropped_table(&rig, 2, 1500);
    let old = ssts_of(db, t.id);
    assert_eq!(old.len(), 2, "{old:?}");

    let ran = Arc::new(AtomicBool::new(false));
    let (db2, shard, ran2, id) = (Arc::clone(db), rig.shard.clone(), Arc::clone(&ran), t.id);
    db.before_shrink_relocates(Box::new(move || {
        shard.wait(db2.compact_pending(Some(id)).unwrap()).unwrap();
        ran2.store(true, Ordering::Release);
    }));
    db.shrink().unwrap();
    assert!(ran.load(Ordering::Acquire), "the hook ran inside shrink");
    let now = ssts_of(db, t.id);
    assert!(
        now.iter().all(|s| !old.iter().any(|o| o.1 == s.1)),
        "the compaction replaced both inputs: {old:?} -> {now:?}"
    );
    assert_eq!(row_count(db, &t), 3000);
    assert_eq!(db.shrink().unwrap(), 0);
    assert_eq!(db.unreferenced_bytes(), 0, "no copy leaked");
    // The compaction's output (3 MiB of rows, one 4 MiB extent) grew the file under the
    // shrink; the shrink then moved it down into the space its inputs and `junk` left.
    assert!(file_len(&vfs) <= 8 << 20, "{}", file_len(&vfs));
    rig.close();

    let rig = Rig::open(&vfs);
    assert_eq!(row_count(&rig.db, &rig.db.table("t").unwrap()), 3000);
    rig.close();
}

#[test]
fn shrink_keeps_the_level_a_trivial_move_gave_an_sst() {
    // 7 F7-5: a full compaction of a lone L0 SST moves it to the last level without
    // rewriting it (same id, same extent, still live), so shrink's relocation succeeded,
    // and its edits re-added the copy at L0, the level it had read.
    let vfs = SimVfs::new(1382);
    let rig = Rig::open(&vfs);
    let db = &rig.db;
    // Small enough to be one output piece (#185): a larger lone SST is re-cut, not moved.
    let t = tail_behind_a_dropped_table(&rig, 1, 40);
    let old = ssts_of(db, t.id);
    assert_eq!(old.len(), 1, "{old:?}");
    assert_eq!(old[0].0, 0, "flushed to L0");

    let (db2, shard, id) = (Arc::clone(db), rig.shard.clone(), t.id);
    db.before_shrink_relocates(Box::new(move || {
        shard.wait(db2.compact_pending(Some(id)).unwrap()).unwrap();
    }));
    let released = db.shrink().unwrap();
    assert!(released > 0);
    let now = ssts_of(db, t.id);
    assert_eq!(now.len(), 1, "{now:?}");
    assert_ne!(now[0].1, old[0].1, "shrink relocated the SST");
    let last = PickerOptions::default().max_levels - 1;
    assert_eq!(
        now[0].0, last,
        "the copy stays at the level the trivial move gave the SST"
    );
    assert_eq!(row_count(db, &t), 40);
    rig.close();
}

#[test]
fn shrink_abandons_the_copy_of_a_table_dropped_under_it() {
    // 7 F7-5: a `drop_table` that committed between shrink's catalog read and its commit
    // made the whole shrink fail with `TableNotFound`.
    let vfs = SimVfs::new(1383);
    let rig = Rig::open(&vfs);
    let db = &rig.db;
    let t = tail_behind_a_dropped_table(&rig, 1, 1500);
    assert_eq!(ssts_of(db, t.id).len(), 1);

    let (db2, id) = (Arc::clone(db), t.id);
    db.before_shrink_relocates(Box::new(move || {
        db2.drop_table(id).unwrap();
    }));
    db.shrink().unwrap();
    assert!(db.table("t").is_none());
    assert_eq!(db.shrink().unwrap(), 0);
    assert_eq!(db.unreferenced_bytes(), 0, "the copy was abandoned");
    assert!(
        file_len(&vfs) <= 512 << 10,
        "nothing is live past the manifest: {}",
        file_len(&vfs)
    );
    rig.close();
}

#[test]
fn shrink_erases_the_cached_blocks_of_a_copy_it_abandons() {
    // A `drop_table` that commits after shrink copied an SST: the copy is abandoned at
    // commit, and its reader, which cached its top index under the copy's id, is dropped
    // and that entry erased. Before, the entry stayed in the cache until evicted.
    let vfs = SimVfs::new(1384);
    let rig = Rig::open(&vfs);
    let db = &rig.db;
    let t = tail_behind_a_dropped_table(&rig, 1, 1500);
    assert_eq!(ssts_of(db, t.id).len(), 1);
    let cached = db.block_cache_usage();

    let ran = Arc::new(AtomicBool::new(false));
    let (db2, ran2, id) = (Arc::clone(db), Arc::clone(&ran), t.id);
    db.before_shrink_commits(Box::new(move || {
        db2.drop_table(id).unwrap();
        ran2.store(true, Ordering::Release);
    }));
    db.shrink().unwrap();
    assert!(
        ran.load(Ordering::Acquire),
        "shrink copied the SST and reached its commit"
    );
    assert!(db.table("t").is_none());
    assert_eq!(
        db.unreferenced_bytes(),
        0,
        "the copy's extent was abandoned"
    );
    // `t`'s own reader stays pinned by the view shrink held while the drop committed, so
    // its entries stay too: nothing more and nothing less than before is cached.
    assert_eq!(
        db.block_cache_usage(),
        cached,
        "the copy's cached index was erased"
    );
    rig.close();
}
