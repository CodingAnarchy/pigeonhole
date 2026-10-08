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

/// Review of #315: a delete committed after the memtable was queued for a guarded flush,
/// before the flush installs. The delete hides the put at 20, so reads see the put at 10
/// while the frozen memtable is still in the view; the flush must not then drop it (that would
/// change a read with no write). The delete voids the purge and the memtable is flushed again
/// without it. Returns the read at a snapshot taken right after the delete, through the
/// view the flush publishes, and the read at latest after the flush.
fn delete_during_flush(mutation: FlushGcMutation) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let mut rig = Rig::open(2871, mutation);
    rig.put(Some(10), b"ten");
    rig.put(Some(20), b"twenty");
    // Queue the flush (a deadline already passed: the shard handles the request, freezing and
    // queueing the memtable, but runs no task slice), then admit the delete the same way, so
    // it arrives while the flush is in flight.
    let flush = rig.db.flush_pending().unwrap();
    rig.shard.run_once(0);
    let mut wb = WriteBatch::new();
    let (t, f) = (rig.t.id, rig.t.families[0].id);
    wb.delete_cell(t, f, b"row", b"q", 20).unwrap();
    let del = rig.db.submit(wb, Some(Durability::Buffered)).unwrap();
    rig.shard.run_once(0);
    rig.wait(del).unwrap();
    let s = rig.db.snapshot().unwrap();
    assert_eq!(
        rig.get(&s),
        Some(b"ten".to_vec()),
        "the delete shows the put at 10"
    );
    rig.wait(flush).unwrap();
    while rig.shard.run_once(u64::MAX) {}
    let latest = rig.db.snapshot().unwrap();
    let reads = (rig.get(&latest.at_seqno(s.seqno())), rig.get(&latest));
    drop((s, latest));
    rig.close();
    reads
}

#[test]
fn a_delete_during_a_guarded_flush_voids_its_purge() {
    let ten = Some(b"ten".to_vec());
    assert_eq!(
        delete_during_flush(FlushGcMutation::None),
        (ten.clone(), ten.clone())
    );
    // Without the void the flush installs its purge over the delete: the put at 10 is gone.
    let (at_s, latest) = delete_during_flush(FlushGcMutation::IgnoreVoids);
    assert_ne!(latest, ten, "the mutation must lose the visible version");
    assert_ne!(at_s, ten);
}

/// Review of #315: a snapshot pins its seqno before it loads the view, so a flush published
/// in between keeps the versions the snapshot reads. The hook runs between the two: it writes
/// a newer version and flushes, so the flush's GC (max_versions 1) would drop the older one
/// if the snapshot's seqno were not yet live.
#[test]
fn a_snapshot_pins_its_seqno_before_it_loads_the_view() {
    use std::sync::Mutex;
    let rig = Rig::open(2872, FlushGcMutation::None);
    let Rig { db, shard, t } = rig;
    let shard = Arc::new(Mutex::new(shard));
    let drive = {
        let shard = Arc::clone(&shard);
        move |mut f: Pin<Box<dyn Future<Output = Result<(), pigeonhole_engine::Error>> + Send>>| {
            let mut cx = Context::from_waker(Waker::noop());
            loop {
                if let Poll::Ready(r) = f.as_mut().poll(&mut cx) {
                    return r;
                }
                shard.lock().unwrap().run_once(u64::MAX);
            }
        }
    };
    let (tid, fid) = (t.id, t.families[0].id);
    let put = {
        let db = Arc::clone(&db);
        let drive = drive.clone();
        move |v: &'static [u8]| {
            let mut wb = WriteBatch::new();
            wb.put(tid, fid, b"row", b"q", None, ValueRef::Bytes(v))
                .unwrap();
            let pending = db.submit(wb, Some(Durability::Buffered)).unwrap();
            drive(Box::pin(async move { pending.await.map(drop) })).unwrap();
        }
    };
    put(b"v1");
    {
        let (db2, drive, put) = (Arc::clone(&db), drive.clone(), put.clone());
        db.before_snapshot_view_load(Box::new(move || {
            put(b"v2");
            let flush = db2.flush_pending().unwrap();
            drive(Box::pin(flush)).unwrap();
        }));
    }
    let snap = db.snapshot().unwrap();
    let read = db
        .get(&snap, tid, fid, b"row", b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()));
    assert_eq!(
        read,
        Some(b"v1".to_vec()),
        "the flush kept the pinned snapshot's version"
    );
    let latest = db.snapshot().unwrap();
    let read = db
        .get(&latest, tid, fid, b"row", b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()));
    assert_eq!(read, Some(b"v2".to_vec()));
    drop((snap, latest));
    db.close().unwrap();
    let mut shard = shard.lock().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
}

/// #301 put large values in blob files at commit time, so a memtable holds their pointers. A
/// flush that drops such a pointer (a version purged under the guard, a put hidden by a delete
/// in the same memtable) lowers its file's live bytes in the same commit: the accounting
/// check passes after the flush, and the live bytes fell by exactly the dropped records.
#[test]
fn a_flush_that_drops_a_blob_pointer_lowers_its_live_bytes() {
    let mut rig = Rig::open(2873, FlushGcMutation::None);
    rig.db.set_inline_value_limit(200);
    let (t, f) = (rig.t.id, rig.t.families[0].id);
    let large = |b: u8| vec![b; 400];
    rig.put(None, &large(b'1'));
    rig.put(None, &large(b'2'));
    // A large put at 7 and a cell delete of it: hidden at every read point.
    let mut wb = WriteBatch::new();
    wb.put(
        t,
        f,
        b"row",
        b"gone",
        Some(7),
        ValueRef::Bytes(&large(b'g')),
    )
    .unwrap();
    rig.commit(wb);
    let mut wb = WriteBatch::new();
    wb.delete_cell(t, f, b"row", b"gone", 7).unwrap();
    rig.commit(wb);
    let live = |db: &Engine| db.blob_files().iter().map(|b| b.3).sum::<u64>();
    let total = |db: &Engine| db.blob_files().iter().map(|b| b.2).sum::<u64>();
    rig.db.check_blob_accounting().unwrap();
    let before = live(&rig.db);
    assert_eq!(before, total(&rig.db), "three large values, all live");
    rig.flush();
    rig.db.check_blob_accounting().unwrap();
    // Two of the three records (all the same size) are no longer referenced.
    assert_eq!(live(&rig.db) * 3, before);
    assert_eq!(rig.get(&rig.db.snapshot().unwrap()), Some(large(b'2')));
    rig.close();
}
