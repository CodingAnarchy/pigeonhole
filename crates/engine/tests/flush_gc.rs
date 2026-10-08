//! Flush-time version GC (#287): a flush runs its memtable through compaction's GC as a
//! non-bottommost job, and purges versions beyond `max_versions` only when no other source of
//! the slot can hold a delete. Every live snapshot still reads what it read, and a delete
//! outside the memtable keeps its versions. Each test also runs the engine with the GC broken
//! on purpose (`Engine::mutate_flush_gc`) and checks that the reads then go wrong, so the
//! assertions have teeth. One application-owned shard over `SimVfs`.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineShard, FamilyOptions, FlushGcMutation, Snapshot, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, Timestamp};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/flush_gc.phdb";

struct Rig {
    db: Arc<Engine>,
    shard: EngineShard,
    t: Arc<TableInfo>,
}

impl Rig {
    fn open(seed: u64, mutation: FlushGcMutation) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o = common::options(vfs, 1, 16 << 20);
        o.tablet_changes = false;
        // No compaction runs: what the reads see is the flush's doing.
        o.compaction.l0_trigger = u32::MAX;
        let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
        let mut shard = shards.remove(0);
        while shard.run_once(u64::MAX) {}
        db.mutate_flush_gc(mutation);
        let family = FamilyOptions {
            max_versions: 1,
            ..FamilyOptions::default()
        };
        let t = db.create_table("t", &[("f".into(), family)]).unwrap();
        Self { db, shard, t }
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

    fn commit(&mut self, wb: WriteBatch) {
        let pending = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
        self.wait(pending).unwrap();
    }

    fn put(&mut self, ts: Option<Timestamp>, value: &[u8]) {
        let mut wb = WriteBatch::new();
        let (t, f) = (self.t.id, self.t.families[0].id);
        wb.put(t, f, b"row", b"q", ts, ValueRef::Bytes(value))
            .unwrap();
        self.commit(wb);
    }

    fn delete_cell(&mut self, ts: Timestamp) {
        let mut wb = WriteBatch::new();
        let (t, f) = (self.t.id, self.t.families[0].id);
        wb.delete_cell(t, f, b"row", b"q", ts).unwrap();
        self.commit(wb);
    }

    fn flush(&mut self) {
        let flush = self.db.flush_pending().unwrap();
        self.wait(flush).unwrap();
        while self.shard.run_once(u64::MAX) {}
    }

    fn get(&self, snap: &Snapshot) -> Option<Vec<u8>> {
        let (t, f) = (self.t.id, self.t.families[0].id);
        self.db
            .get(snap, t, f, b"row", b"q")
            .unwrap()
            .map(|c| common::value_bytes(c.value()))
    }

    /// Stored versions of the cell, newest first, whatever their visibility.
    fn stored(&self) -> usize {
        let snap = self.db.snapshot().unwrap();
        self.db
            .raw_entries(&snap)
            .unwrap()
            .into_iter()
            .filter(|e| e.table == self.t.id)
            .count()
    }

    fn close(self) {
        let Self { db, mut shard, .. } = self;
        db.close().unwrap();
        while shard.closed().is_none() {
            shard.run_once(u64::MAX);
        }
    }
}

/// Four versions, snapshots after the second and the third, then a flush: the reads at the
/// two snapshots' seqnos through the view published by the flush (a reader process pins its
/// seqno before it loads the view, so it can read an older seqno through a newer view: why
/// the GC keeps every live snapshot's versions, D61), the read at latest, and how many
/// versions are stored.
fn snapshots_across_a_flush(mutation: FlushGcMutation) -> (Vec<Option<Vec<u8>>>, usize) {
    let mut rig = Rig::open(287, mutation);
    rig.put(None, b"v0");
    rig.put(None, b"v1");
    let s1 = rig.db.snapshot().unwrap();
    rig.put(None, b"v2");
    let s2 = rig.db.snapshot().unwrap();
    rig.put(None, b"v3");
    rig.flush();
    let latest = rig.db.snapshot().unwrap();
    // The snapshots' own views still list the memtable.
    assert_eq!(rig.get(&s1), Some(b"v1".to_vec()));
    assert_eq!(rig.get(&s2), Some(b"v2".to_vec()));
    let reads = vec![
        rig.get(&latest.at_seqno(s1.seqno())),
        rig.get(&latest.at_seqno(s2.seqno())),
        rig.get(&latest),
    ];
    let stored = rig.stored();
    drop((s1, s2, latest));
    rig.close();
    (reads, stored)
}

#[test]
fn a_flush_keeps_what_live_snapshots_read_and_purges_the_rest() {
    let want = vec![
        Some(b"v1".to_vec()),
        Some(b"v2".to_vec()),
        Some(b"v3".to_vec()),
    ];
    let (reads, stored) = snapshots_across_a_flush(FlushGcMutation::None);
    assert_eq!(reads, want);
    // v0 is beyond `max_versions` at every read point (both snapshots and latest).
    assert_eq!(stored, 3, "v1, v2 and v3 stay; v0 is purged");

    // Without the snapshot floor the flush keeps only what latest reads: the snapshots lose
    // their versions, which is what the floor is for.
    let (reads, stored) = snapshots_across_a_flush(FlushGcMutation::DropSnapshotFloor);
    assert_ne!(reads, want, "the mutation must change a snapshot's read");
    assert_eq!(stored, 1);
}

/// An SST holds a cell delete at timestamp 20; the memtable holds puts at 10 and 20. The
/// older delete hides the put at 20 (D9: by timestamp, whatever the seqno), so the put at 10
/// is the visible version, and a flush must keep it. Returns the read after the flush.
fn outside_delete(mutation: FlushGcMutation) -> Option<Vec<u8>> {
    let mut rig = Rig::open(2870, mutation);
    rig.delete_cell(20);
    rig.flush();
    rig.put(Some(10), b"ten");
    rig.put(Some(20), b"twenty");
    let before = rig.get(&rig.db.snapshot().unwrap());
    assert_eq!(
        before,
        Some(b"ten".to_vec()),
        "the delete hides the put at 20"
    );
    rig.flush();
    let after = rig.get(&rig.db.snapshot().unwrap());
    rig.close();
    after
}

#[test]
fn a_delete_outside_the_memtable_keeps_its_versions() {
    assert_eq!(outside_delete(FlushGcMutation::None), Some(b"ten".to_vec()));
    // Without the guard the flush counts the put at 20 as the newest version and purges the
    // one at 10, which the delete in the SST had made the visible one.
    assert_eq!(outside_delete(FlushGcMutation::DropGuard), None);
}
