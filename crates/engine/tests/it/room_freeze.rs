//! A wait for arena room freezes the active memtables only when no flush is in flight: a
//! frozen memtable already queued or flushing frees room when its flush ends. Freezing
//! every small active memtable on each wait instead filled a backlogged load's L0 with SSTs
//! a fraction of the freeze size (603 of 641 freezes in a 1 GiB single-shard load).

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

#[test]
fn a_room_wait_during_a_flush_leaves_the_active_memtable_open() {
    let vfs = SimVfs::new(616);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.reader_slots = 4;
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let mut shard: EngineShard = shards.remove(0);
    let step = |shard: &mut EngineShard| {
        for _ in 0..64 {
            let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
            if !shard.run_once(now + 1_000_000) {
                break;
            }
        }
    };
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    let f = t.families[0].id;
    // One commit group of a batch per row; with `room_wait`, every member after the first
    // waits for room as if the arena were full.
    let group = |shard: &mut EngineShard, rows: &[&[u8]], room_wait: bool| {
        if room_wait {
            engine.inject(pigeonhole_engine::Fault::NextRoomWait, true);
        }
        let pcs: Vec<_> = rows
            .iter()
            .map(|row| {
                let mut b = WriteBatch::new();
                b.put(t.id, f, row, b"q", None, ValueRef::Bytes(b"v"))
                    .unwrap();
                engine.submit(b, Some(Durability::Buffered)).unwrap()
            })
            .collect();
        step(shard);
        pcs
    };
    let committed = |pcs: Vec<PendingCommit>| {
        for mut pc in pcs {
            assert!(ready(&mut pc).expect("committed").is_ok());
        }
    };
    let l0 = || engine.sst_levels().iter().filter(|s| s.1 == 0).count();

    committed(group(&mut shard, &[b"a"], false));
    // The memtable holding `a` freezes and flushes; its manifest commit waits for the
    // exclusion this thread holds, so the flush stays in flight.
    assert!(engine.hold_manifest());
    let mut flush = engine.flush_pending().unwrap();
    step(&mut shard);
    // `d` waits for room while that flush is in flight: the memtable holding `b` (and `c`,
    // admitted with `d` but applied after the freeze) stays open.
    committed(group(&mut shard, &[b"b"], false));
    let waiting = group(&mut shard, &[b"c", b"d"], true);
    engine.release_manifest();
    step(&mut shard);
    assert!(ready(&mut flush).expect("flushed").is_ok());
    committed(waiting);
    assert_eq!(l0(), 1, "only the memtable already flushing became an SST");

    // With no flush in flight, a wait for room still freezes what is open (and flushes it).
    let waiting = group(&mut shard, &[b"e", b"f"], true);
    step(&mut shard);
    committed(waiting);
    assert_eq!(l0(), 2, "the open memtable froze and flushed for room");

    let snap = engine.snapshot().unwrap();
    for row in [&b"a"[..], b"b", b"c", b"d", b"e", b"f"] {
        assert!(engine.get(&snap, t.id, f, row, b"q").unwrap().is_some());
    }
    drop(snap);
    engine.close().unwrap();
    step(&mut shard);
}

/// A load from several threads that outruns its flushes, on a real clock and file system:
/// few of the L0 SSTs it leaves are small, not one small SST per wait for room.
#[test]
fn a_backlogged_load_leaves_few_small_l0_ssts() {
    let dir = std::env::temp_dir().join(format!("pigeonhole-room-freeze-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(2));
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 4 << 20;
    o.memtable_freeze_bytes = 1 << 20;
    // L0 stays as flushed: no compaction merges its SSTs.
    o.compaction.l0_trigger = 1 << 20;
    let engine = Engine::open(&dir.join("data.phdb"), o).expect("open");
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    let f = t.families[0].id;
    std::thread::scope(|s| {
        for w in 0..4u64 {
            let (engine, t) = (&engine, &t);
            s.spawn(move || {
                // Incompressible values, so SST sizes follow memtable sizes.
                let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ w;
                let mut value = vec![0u8; 1024];
                for batch in 0..256u64 {
                    let mut b = WriteBatch::new();
                    for i in 0..32u64 {
                        for v in value.iter_mut() {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            *v = x as u8;
                        }
                        let row = (w << 32 | batch << 8 | i).to_be_bytes();
                        b.put(t.id, f, &row, b"q", None, ValueRef::Bytes(&value))
                            .unwrap();
                    }
                    engine
                        .commit(b, Some(Durability::Buffered))
                        .expect("commit");
                }
            });
        }
    });
    let l0: Vec<u64> = engine
        .sst_lens()
        .iter()
        .filter(|s| s.0 == 0)
        .map(|s| s.1)
        .collect();
    // Before, 16-38% of them were under a quarter of the freeze size (none since).
    let small = l0.iter().filter(|&&n| n < 256 << 10).count();
    assert!(
        small * 8 <= l0.len(),
        "{small} of {} L0 SSTs are under a quarter of the 1 MiB freeze size: {l0:?}",
        l0.len()
    );
    engine.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A wait for room with no flush in flight, on a shard holding many slots, freezes the
/// largest memtables until they hold about half the arena, not every one (D124): three
/// large tablets beside thirteen small ones flush three SSTs, not sixteen.
#[test]
fn a_room_wait_on_a_crowded_shard_freezes_the_largest_memtables() {
    let vfs = SimVfs::new(618);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.reader_slots = 4;
    o.tablet_changes = true;
    o.balance_interval_nanos = 0;
    o.memtable_budget = 8 << 20;
    // No memtable is due by size: only the wait for room freezes.
    o.memtable_freeze_bytes = 8 << 20;
    o.compaction.l0_trigger = 1 << 20;
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let mut shard: EngineShard = shards.remove(0);
    let mut settle = |shard: &mut EngineShard| {
        for _ in 0..100_000 {
            let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
            if !shard.run_once(now + 1_000_000) {
                return;
            }
        }
        panic!("the shard did not settle");
    };
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    let f = t.families[0].id;
    let drive = |shard: &mut EngineShard, settle: &mut dyn FnMut(&mut EngineShard), mut m| {
        for _ in 0..1000 {
            if let Some(r) = ready(&mut m) {
                return r;
            }
            settle(shard);
        }
        panic!("did not finish")
    };
    for n in 1..16 {
        let m = engine
            .split_tablet_pending(t.id, format!("t{n:02}").as_bytes())
            .unwrap();
        drive(&mut shard, &mut settle, m).unwrap();
    }
    let tablets = engine
        .snapshot()
        .unwrap()
        .view()
        .tablets()
        .ranges(t.id)
        .len();
    assert_eq!(tablets, 16);
    let l0 = || engine.sst_levels().iter().filter(|s| s.1 == 0).count();
    let before = l0();
    // Tablets 0-2 hold 1.5 MiB each (4.5 MiB together, past half the 8 MiB arena; two hold
    // less than half), the other thirteen 32 KiB each.
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut value = vec![0u8; 1024];
    for n in 0..16u32 {
        let rows = if n < 3 { 1536 } else { 32 };
        for chunk in 0..rows / 32 {
            let mut b = WriteBatch::new();
            for i in 0..32 {
                for v in value.iter_mut() {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    *v = x as u8;
                }
                let row = format!("t{n:02}-{:05}", chunk * 32 + i);
                b.put(t.id, f, row.as_bytes(), b"q", None, ValueRef::Bytes(&value))
                    .unwrap();
            }
            let mut pc = engine.submit(b, Some(Durability::Buffered)).unwrap();
            settle(&mut shard);
            assert!(ready(&mut pc).expect("committed").is_ok());
        }
    }
    assert_eq!(l0(), before, "nothing froze by size");
    // A group whose second member waits for room, with no flush in flight.
    engine.inject(pigeonhole_engine::Fault::NextRoomWait, true);
    let pcs: Vec<PendingCommit> = [&b"t05-x"[..], b"t06-x"]
        .iter()
        .map(|row| {
            let mut b = WriteBatch::new();
            b.put(t.id, f, row, b"q", None, ValueRef::Bytes(b"v"))
                .unwrap();
            engine.submit(b, Some(Durability::Buffered)).unwrap()
        })
        .collect();
    settle(&mut shard);
    for mut pc in pcs {
        assert!(ready(&mut pc).expect("committed").is_ok());
    }
    assert_eq!(
        l0() - before,
        3,
        "the three largest memtables froze and flushed, the small ones stayed open"
    );
    engine.close().unwrap();
    settle(&mut shard);
}
