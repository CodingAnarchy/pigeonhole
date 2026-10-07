//! A freeze deferred until the watermark passes its memtable (D117) is woken when the
//! watermark gets there, however many times it deferred, and a shard waiting for it never
//! wakes itself in a loop. Both shards are driven by hand so another shard's cross-shard
//! commit can hold the watermark back.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

struct Db {
    vfs: Arc<SimVfs>,
    engine: Arc<Engine>,
    shards: Vec<EngineShard>,
    /// A table owned by shard 0 and one owned by shard 1.
    tables: [Arc<TableInfo>; 2],
}

fn open(tweak: impl FnOnce(&mut EngineOptions)) -> Db {
    let vfs = SimVfs::new(11);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.reader_slots = 4;
    tweak(&mut o);
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let mut owned: [Option<Arc<TableInfo>>; 2] = [None, None];
    for i in 0.. {
        let t = engine
            .create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
            .expect("table");
        let snap = engine.snapshot().unwrap();
        let (_, shard) = snap.view().tablets().route(t.id, b"row").unwrap();
        let slot = &mut owned[usize::from(shard.0)];
        if slot.is_none() {
            *slot = Some(t);
        }
        if owned.iter().all(Option::is_some) {
            break;
        }
    }
    let [a, b] = owned;
    Db {
        vfs,
        engine,
        shards,
        tables: [a.unwrap(), b.unwrap()],
    }
}

impl Db {
    fn now(&self) -> u64 {
        pigeonhole_io::Vfs::monotonic_nanos(&*self.vfs)
    }

    /// Runs shard `i` once; returns whether it has work left.
    fn step(&mut self, i: usize) -> bool {
        let now = self.now();
        self.shards[i].run_once(now + 1_000)
    }

    fn put(&self, batch: &mut WriteBatch, shard: usize, row: &[u8], value: &[u8]) {
        let t = &self.tables[shard];
        batch
            .put(
                t.id,
                t.families[0].id,
                row,
                b"q",
                None,
                ValueRef::Bytes(value),
            )
            .unwrap();
    }

    fn submit(&self, batch: WriteBatch) -> PendingCommit {
        self.engine
            .submit(batch, Some(Durability::Buffered))
            .expect("submit")
    }

    /// Starts a cross-shard commit coordinated by shard 1 and prepares it on both shards,
    /// leaving shard 1 holding its seqno: nothing past it is visible until shard 1 runs.
    fn hold_watermark(&mut self) -> PendingCommit {
        let mut b = WriteBatch::new();
        self.put(&mut b, 1, b"held", b"x");
        self.put(&mut b, 0, b"held", b"x");
        let pc = self.submit(b);
        self.step(1);
        self.step(0);
        pc
    }

    fn close(mut self) {
        self.engine.close().unwrap();
        for _ in 0..64 {
            self.step(0);
            self.step(1);
        }
    }
}

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

#[test]
fn a_freeze_that_defers_again_after_a_wake_is_woken_when_the_watermark_passes() {
    // Main registered a deferred freeze for the watermark kick only while its flag was clear:
    // a publish that took the registration and kicked the shard too early (the watermark had
    // not passed) left the flag set, the freeze deferred again without registering, and the
    // publish that finally passed the watermark woke nobody. The flush then waited for
    // unrelated traffic.
    let mut db = open(|o| {
        // 16 KiB arena chunks: the freeze threshold is two of them.
        o.memtable_budget = 1 << 20;
        o.memtable_freeze_bytes = 32 << 10;
    });
    let mut held = db.hold_watermark();
    // Shard 0 fills its memtable past the freeze threshold above the held seqno: the freeze
    // defers and registers.
    let mut b = WriteBatch::new();
    for i in 0..8u8 {
        db.put(&mut b, 0, &[b'b', i], &vec![1u8; 8 << 10]);
    }
    let mut big = db.submit(b);
    for _ in 0..4 {
        db.step(0);
    }
    // More writes on shard 0 publish watermarks that do not pass the held seqno.
    let mut small = Vec::new();
    for i in 0..4u8 {
        let mut b = WriteBatch::new();
        db.put(&mut b, 0, &[b's', i], b"v");
        small.push(db.submit(b));
        for _ in 0..4 {
            db.step(0);
        }
    }
    assert_eq!(db.engine.metrics().flushes, 0, "nothing can freeze yet");
    // Shard 1 finishes the cross-shard commit (shard 0 applies the decision on the way): the
    // watermark passes everything.
    for _ in 0..64 {
        db.step(1);
        db.step(0);
        if ready(&mut held).is_some() {
            break;
        }
    }
    ready(&mut held).expect("cross-shard commit done").unwrap();
    // No new writes: only the watermark's kick can start the freeze and flush.
    for _ in 0..256 {
        db.step(0);
        db.step(1);
        if db.engine.metrics().flushes > 0 {
            break;
        }
    }
    assert!(
        db.engine.metrics().flushes > 0,
        "the deferred freeze was never woken"
    );
    ready(&mut big).expect("big commit").unwrap();
    for mut pc in small {
        ready(&mut pc).expect("small commit").unwrap();
    }
    db.close();
}

#[test]
fn a_room_wait_with_a_deferred_freeze_does_not_spin() {
    // Shard 0 runs out of arena room while every memtable it holds is above a seqno shard 1
    // holds: its freeze defers on every retry. The retry must not wake itself (each retry
    // registering for the watermark kick, and each publish kicking every registered shard,
    // was a busy loop until shard 1 moved). Once shard 1 finishes, the wait ends.
    let mut db = open(|o| {
        o.memtable_budget = 1 << 20;
        o.memtable_freeze_bytes = 64 << 10;
    });
    let mut held = db.hold_watermark();
    let mut commits = Vec::new();
    let mut i = 0u32;
    while db.engine.metrics().stalls.0 == 0 {
        assert!(i < 1_000, "no room wait after {i} commits");
        let mut b = WriteBatch::new();
        db.put(&mut b, 0, &i.to_be_bytes(), &vec![2u8; 8 << 10]);
        commits.push(db.submit(b));
        for _ in 0..4 {
            db.step(0);
        }
        i += 1;
    }
    // Waiting: shard 0 goes idle (its stall timers give up on the frozen clock) instead of
    // kicking itself for ever.
    let mut idle = false;
    for _ in 0..20_000 {
        if !db.step(0) {
            idle = true;
            break;
        }
    }
    assert!(idle, "shard 0 kept running while waiting for room");
    assert!(ready(&mut held).is_none());
    // Shard 1 releases the watermark: the freeze runs, a flush frees room, and every commit
    // either lands or (D126, frozen clock) was refused with `Busy`, never left hanging.
    for _ in 0..64 {
        db.step(1);
        db.step(0);
        if ready(&mut held).is_some() {
            break;
        }
    }
    ready(&mut held).expect("cross-shard commit done").unwrap();
    for _ in 0..2_000 {
        db.step(0);
        db.step(1);
    }
    for (n, mut pc) in commits.into_iter().enumerate() {
        match ready(&mut pc) {
            Some(Ok(_) | Err(pigeonhole_engine::Error::Busy)) => {}
            other => panic!("commit {n}: {other:?}"),
        }
    }
    db.close();
}
