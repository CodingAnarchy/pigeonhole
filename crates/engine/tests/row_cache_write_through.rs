//! D290's write-through against a late lower seqno (simp724's review of #1011): a cross-shard
//! commit's share is decided at seqno `d`, and a plain single to the same row, reserved after
//! the decision at `e > d`, applies on the row's shard before the share does. The single's
//! write-through moves the cached row to `e` without the share's write; the share's apply then
//! finds the slot above `d` and drops the row, so no read is served the row without it.
//! Three application-owned shards over `SimVfs`, run by hand to force that order.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyId, FamilyOptions, ReadSpec, TableId, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/row_cache_write_through.phdb";

fn poll<F: Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// Table `t`, family `f`, split so rows `a..` are on shard 0, `b..` on 1 and `c..` on 2.
struct Shards {
    vfs: Arc<SimVfs>,
    db: Arc<Engine>,
    shards: Vec<EngineShard>,
    t: TableId,
    f: FamilyId,
}

impl Shards {
    fn open(seed: u64) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o: EngineOptions = common::options(Arc::clone(&vfs), 3, 16 << 20);
        o.pin_threads = false;
        o.tablet_changes = true;
        o.balance_interval_nanos = 0;
        o.compaction.l0_trigger = u32::MAX;
        o.row_cache_bytes = 1 << 20;
        let (db, shards) = Engine::open_application_owned(Path::new(DB), o).expect("open");
        let info = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let mut s = Self {
            vfs,
            f: info.family("f").unwrap().id,
            t: info.id,
            db,
            shards,
        };
        for at in [&b"b"[..], b"c"] {
            let m = s.db.split_tablet_pending(s.t, at).unwrap();
            s.drive(m).unwrap();
        }
        for (row, to) in [(&b"a"[..], 0u16), (b"b", 1), (b"c", 2)] {
            let owner =
                s.db.snapshot()
                    .unwrap()
                    .view()
                    .tablets()
                    .ranges(s.t)
                    .iter()
                    .rfind(|r| r.2.as_slice() <= row)
                    .map(|r| r.1)
                    .unwrap();
            if owner != to {
                let m = s.db.move_tablet_pending(s.t, row, to).unwrap();
                s.drive(m).unwrap();
            }
        }
        s
    }

    fn run(&mut self, i: usize) {
        let now = pigeonhole_io::Vfs::monotonic_nanos(&*self.vfs);
        self.shards[i].run_once(now + 1_000);
    }

    fn drive<F: Future + Unpin>(&mut self, mut f: F) -> F::Output {
        for _ in 0..5_000 {
            if let Some(v) = poll(&mut f) {
                return v;
            }
            for i in 0..3 {
                self.run(i);
            }
            self.vfs.complete_all_io();
        }
        panic!("never resolved");
    }

    fn put(
        &self,
        level: Durability,
        puts: &[(&[u8], &[u8], &[u8])],
    ) -> pigeonhole_engine::PendingCommit {
        let mut wb = WriteBatch::new();
        for (row, q, v) in puts {
            wb.put(self.t, self.f, row, q, None, ValueRef::Bytes(v))
                .unwrap();
        }
        self.db.submit(wb, Some(level)).unwrap()
    }

    /// `b-r`'s cell `q`, through the row cache (a latest get).
    fn get(&self, q: &[u8]) -> Vec<u8> {
        let cell = self.db.get_latest(self.t, self.f, b"b-r", q).unwrap();
        match cell.expect("the cell exists").value() {
            ValueRef::Bytes(b) => b.to_vec(),
            v => panic!("not bytes: {v:?}"),
        }
    }

    /// `b-r` as a snapshot row read (never through the cache): `(qualifier, value)`.
    fn uncached(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut spec = ReadSpec::default();
        spec.families = vec![self.f];
        spec.versions = 1;
        let row = self
            .db
            .read_row(&self.db.snapshot().unwrap(), self.t, b"b-r", &spec)
            .unwrap()
            .expect("the row exists");
        row.cells
            .iter()
            .map(|c| {
                let v = match c.data.value() {
                    ValueRef::Bytes(b) => b.to_vec(),
                    v => panic!("not bytes: {v:?}"),
                };
                (row.qualifier(c).to_vec(), v)
            })
            .collect()
    }
}

#[test]
fn a_share_applied_after_a_later_single_drops_the_rows_write_through() {
    let mut s = Shards::open(290);
    let pc = s.put(
        Durability::Buffered,
        &[(b"b-r", b"q1", b"0"), (b"b-r", b"q2", b"0")],
    );
    s.drive(pc).unwrap();
    // Two gets: the first marks the row, the second fills it (D290).
    assert_eq!(s.get(b"q2"), b"0");
    assert_eq!(s.get(b"q2"), b"0");
    assert_eq!(s.db.row_cache_stats().fills, 1, "the row is cached");

    // From here the test thread is the device: syncs complete only when it says so.
    s.vfs.set_deferred_io(true);
    // X writes b-r's q2 (shard 1), coordinated by shard 0, at Sync: its
    // Decide goes out only once its DECISION is durable (D286's early Decide is Buffered's).
    let mut x = s.put(
        Durability::Sync,
        &[(b"a-x", b"q", b"x"), (b"b-r", b"q2", b"X")],
    );
    let parked = s.db.parked_decisions(1);
    // Complete I/O between turns (the PREPAREs' syncs) until shard 0 has decided X, which
    // reserves its seqno; then complete nothing more, so the DECISION's sync stays in flight
    // and X's Decide with it.
    let db = Arc::clone(&s.db);
    let rounds = || db.metrics().cross_shard.rounds;
    let decided = rounds();
    let mut turns = 0;
    'decide: loop {
        for i in [0, 1] {
            s.run(i);
            if rounds() > decided {
                break 'decide;
            }
        }
        s.vfs.complete_all_io();
        turns += 1;
        assert!(turns < 64, "X never decided");
    }
    // S writes b-r's q1 at Buffered. Shard 1 reserves its seqno now, above X's, and applies
    // it before X's share: S's write-through moves the cached row forward without X's q2.
    let before = s.db.row_cache_write_throughs();
    let mut single = s.put(Durability::Buffered, &[(b"b-r", b"q1", b"S")]);
    let mut turns = 0;
    while s.db.row_cache_write_throughs() == before {
        s.run(1);
        turns += 1;
        assert!(turns < 64, "S never wrote through");
    }
    assert!(
        poll(&mut x).is_none(),
        "X answered before its DECISION was durable"
    );
    // S is applied but not visible: X's seqno, below it, has not applied.
    assert!(poll(&mut single).is_none(), "S answered before X applied");
    assert_eq!(s.db.parked_decisions(1), parked);
    s.drive(&mut x).unwrap();
    s.drive(&mut single).unwrap();

    // X's share found the slot above its seqno and dropped the row: the reads miss and see
    // both writes. With the stale entry they'd return q2 = 0.
    let want = vec![
        (b"q1".to_vec(), b"S".to_vec()),
        (b"q2".to_vec(), b"X".to_vec()),
    ];
    assert_eq!(s.uncached(), want);
    assert_eq!(s.get(b"q1"), b"S");
    assert_eq!(
        s.get(b"q2"),
        b"X",
        "a get served the write-through that lacks X"
    );
    let snap = s.db.snapshot().unwrap();
    let cell = s.db.get(&snap, s.t, s.f, b"b-r", b"q2").unwrap().unwrap();
    assert_eq!(cell.value(), ValueRef::Bytes(b"X"), "a snapshot get");
    drop(snap);

    s.db.close().unwrap();
    while s.shards.iter_mut().any(|sh| sh.closed().is_none()) {
        for i in 0..3 {
            s.run(i);
        }
        s.vfs.complete_all_io();
    }
}
