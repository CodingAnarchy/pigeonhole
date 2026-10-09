//! Retired extents are reclaimed as soon as the last view that could read them goes, not at
//! the next manifest commit: a view held for a moment (a shard scoring its slots while
//! `shrink` reclaimed) otherwise left them retired on an idle database, and a test that
//! checked `unreferenced_bytes` right after `shrink` failed now and then.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineOptions, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

/// One application-owned shard driven by the test: nothing runs unless the test drives it.
struct Rig {
    db: Arc<Engine>,
    shard: EngineShard,
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>) -> Self {
        let vfs: Arc<SimVfs> = Arc::clone(vfs);
        let mut o = EngineOptions::new(vfs);
        o.create_if_missing = true;
        o.shards = 1;
        o.pin_threads = false;
        o.memtable_budget = 16 << 20;
        o.wal.segment_size = 256 << 10;
        o.compaction.l0_trigger = u32::MAX;
        o.compaction.level_base_bytes = u64::MAX;
        let (db, mut shards) =
            Engine::open_application_owned(Path::new("/db/reclaim.phdb"), o).unwrap();
        let mut rig = Self {
            db,
            shard: shards.remove(0),
        };
        rig.idle();
        rig
    }

    fn idle(&mut self) {
        while self.shard.run_once(u64::MAX) {}
    }

    fn wait<F: Future + Unpin>(&mut self, mut f: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(r) = Pin::new(&mut f).poll(&mut cx) {
                return r;
            }
            self.shard.run_once(u64::MAX);
        }
    }

    fn close(mut self) {
        self.db.close().unwrap();
        loop {
            self.shard.run_once(u64::MAX);
            if let Some(r) = self.shard.closed() {
                r.unwrap();
                return;
            }
        }
    }
}

#[test]
fn dropping_the_last_old_view_reclaims_what_it_kept_retired() {
    let vfs = SimVfs::new(46);
    let mut rig = Rig::open(&vfs);
    let t = rig
        .db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for i in 0..200u32 {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            format!("row{i:04}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[7u8; 200]),
        )
        .unwrap();
        let p = rig.db.submit(wb, Some(Durability::Buffered)).unwrap();
        rig.wait(p).unwrap();
    }
    let p = rig.db.flush_pending().unwrap();
    rig.wait(p).unwrap();
    rig.idle();
    assert_eq!(rig.db.unreferenced_bytes(), 0);

    // A view older than the compaction keeps its input SSTs: they are retired, not freed.
    let snapshot = rig.db.snapshot().unwrap();
    let p = rig.db.compact_pending(None).unwrap();
    rig.wait(p).unwrap();
    rig.idle();
    assert!(
        rig.db.unreferenced_bytes() > 0,
        "the snapshot's view keeps the compaction's inputs"
    );

    // Its last view going frees them at once: nothing else runs (the shard is idle and
    // driven only by this test).
    drop(snapshot);
    assert_eq!(
        rig.db.unreferenced_bytes(),
        0,
        "retired extents wait for the next manifest commit"
    );
    rig.close();
}

#[test]
fn a_point_get_keeps_no_memtable_alive_on_its_thread() {
    // A point get keeps its resolver on the thread for the next one (#46), refilled in
    // place: its sources must go after each get. One left there would pin its memtable, whose
    // chunks then never come back after the flush.
    let vfs = SimVfs::new(47);
    let mut rig = Rig::open(&vfs);
    let t = rig
        .db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let put = |rig: &mut Rig, row: &[u8], len: usize| {
        let mut wb = WriteBatch::new();
        wb.put(t.id, f, row, b"q", None, ValueRef::Bytes(&vec![7u8; len]))
            .unwrap();
        let p = rig.db.submit(wb, Some(Durability::Buffered)).unwrap();
        rig.wait(p).unwrap();
    };
    put(&mut rig, b"first", 1);
    let (free_before, _, _) = rig.db.arena_free(0);
    // About 1 MiB: many chunks.
    for i in 0..1000u32 {
        put(&mut rig, format!("row{i:04}").as_bytes(), 1000);
    }
    assert!(rig.db.arena_free(0).0 < free_before);
    let got = rig.db.get_latest(t.id, f, b"row0007", b"q").unwrap();
    assert!(got.is_some());
    drop(got);
    let p = rig.db.flush_pending().unwrap();
    rig.wait(p).unwrap();
    rig.idle();
    // The arena counters are published after a batch.
    put(&mut rig, b"last", 1);
    assert_eq!(
        rig.db.arena_free(0).0,
        free_before,
        "the flushed memtable's chunks did not come back"
    );
    rig.close();
}
