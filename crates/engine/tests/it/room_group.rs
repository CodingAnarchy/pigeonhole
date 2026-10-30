//! A group's singles reserve each memtable slot's chunks once between them, not once per
//! member (#749). Each single used to reserve its slot's run tail plus a fresh-memtable and a
//! freeze chunk, three chunks for a few hundred bytes: the arena always has 256 chunks, so a
//! group of about 85 single-row commits on one shard filled the reservation with the arena
//! nearly empty, and the rest waited for room that a flush of near-empty memtables then
//! "freed" (the box's #749 collapse: thousands of tiny flushes, then L0 pacing).

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineOptions, EngineShard, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

fn settle(shard: &mut EngineShard, vfs: &Arc<SimVfs>) {
    for _ in 0..100_000 {
        let now = pigeonhole_io::Vfs::monotonic_nanos(&**vfs);
        if !shard.run_once(now + 1_000_000) {
            return;
        }
    }
    panic!("the shard did not settle");
}

/// `n` single-row commits on rows `r{tablet:02}-{i}`, spread round-robin over `tablets`
/// tablets, all submitted before the shard runs, so they meet in one group; returns the
/// write stalls for room.
fn singles_in_one_group(n: usize, tablets: usize) -> u64 {
    let vfs = SimVfs::new(749);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.reader_slots = 4;
    o.tablet_changes = true;
    o.balance_interval_nanos = 0;
    // 256 chunks of 16 KiB.
    o.memtable_budget = 4 << 20;
    o.memtable_freeze_bytes = 4 << 20;
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let mut shard = shards.remove(0);
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    for n in 1..tablets {
        let mut m = engine
            .split_tablet_pending(t.id, format!("r{n:02}").as_bytes())
            .unwrap();
        for _ in 0..1000 {
            if ready(&mut m).is_some() {
                break;
            }
            settle(&mut shard, &vfs);
        }
    }
    let (table, family) = (t.id, t.families[0].id);
    let before = engine.metrics().room_stalls.0;
    let mut pcs: Vec<_> = (0..n)
        .map(|i| {
            let mut b = WriteBatch::new();
            let row = format!("r{:02}-{i:05}", i % tablets);
            b.put(
                table,
                family,
                row.as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(&[7; 100]),
            )
            .unwrap();
            engine.submit(b, Some(Durability::Buffered)).unwrap()
        })
        .collect();
    for _ in 0..1000 {
        settle(&mut shard, &vfs);
        if pcs.iter_mut().all(|pc| ready(pc).is_some()) {
            break;
        }
    }
    for pc in &mut pcs {
        assert!(
            matches!(ready(pc), Some(Ok(_))),
            "a single did not commit (a poisoned shard fails them: an arena miscount): {:?}",
            ready(pc)
        );
    }
    let stalls = engine.metrics().room_stalls.0 - before;
    engine.close().unwrap();
    stalls
}

/// 200 single-row commits on one memtable slot fit one group with room to spare (about
/// 30 KiB of entries in a 4 MiB arena): no wait for room. Each reserved three chunks
/// before, so the 86th waited.
#[test]
fn many_singles_on_one_slot_share_its_chunks_in_one_group() {
    assert_eq!(singles_in_one_group(200, 1), 0);
}

/// The same over 16 slots, each new to the group: each slot's chunks are still reserved
/// (once), and every single commits with the shard healthy.
#[test]
fn singles_over_many_slots_reserve_each_slots_chunks_once() {
    assert_eq!(singles_in_one_group(400, 16), 0);
}

/// Each single of a group on a different new slot still reserves that slot's chunks (#749,
/// simp724's review): crediting a later single for slots no earlier one touched would admit
/// more new memtables than the arena has chunks for, and the apply would run out (an arena
/// miscount, which poisons the shard). One tablet's memtable fills most of the arena first;
/// then `n` single-row commits, each to an empty tablet of its own, meet in one group. Each
/// needs two chunks (its new memtable's first and its run tail): the ones that fit are
/// admitted, the rest wait for room until the full memtable flushes, and every one commits.
#[test]
fn singles_on_new_slots_each_reserve_their_slots_chunks() {
    const TABLETS: usize = 56;
    let vfs = SimVfs::new(985);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.reader_slots = 4;
    o.tablet_changes = true;
    o.balance_interval_nanos = 0;
    // 256 chunks of 16 KiB; nothing freezes by size.
    o.memtable_budget = 4 << 20;
    o.memtable_freeze_bytes = 4 << 20;
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new("/db/data.phdb"), o).expect("open");
    let mut shard = shards.remove(0);
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    for n in 1..TABLETS {
        let mut m = engine
            .split_tablet_pending(t.id, format!("r{n:02}").as_bytes())
            .unwrap();
        for _ in 0..1000 {
            if ready(&mut m).is_some() {
                break;
            }
            settle(&mut shard, &vfs);
        }
    }
    let (table, family) = (t.id, t.families[0].id);
    // Tablet 0 ("" .. "r01") takes most of the arena, one commit at a time.
    let value = vec![5u8; 3000];
    for i in 0..PREFILL {
        let mut b = WriteBatch::new();
        for q in 0..16u32 {
            b.put(
                table,
                family,
                format!("a{i:05}").as_bytes(),
                format!("q{q:02}").as_bytes(),
                None,
                ValueRef::Bytes(&value),
            )
            .unwrap();
        }
        let mut pc = engine.submit(b, Some(Durability::Buffered)).unwrap();
        settle(&mut shard, &vfs);
        assert!(matches!(ready(&mut pc), Some(Ok(_))), "prefill {i}");
    }
    // One single on each other (empty) tablet, all in one group.
    let mut pcs: Vec<_> = (1..TABLETS)
        .map(|n| {
            let mut b = WriteBatch::new();
            b.put(
                table,
                family,
                format!("r{n:02}-x").as_bytes(),
                b"q",
                None,
                ValueRef::Bytes(b"v"),
            )
            .unwrap();
            engine.submit(b, Some(Durability::Buffered)).unwrap()
        })
        .collect();
    for _ in 0..1000 {
        settle(&mut shard, &vfs);
        if pcs.iter_mut().all(|pc| ready(pc).is_some()) {
            break;
        }
    }
    for (n, pc) in pcs.iter_mut().enumerate() {
        assert!(
            matches!(ready(pc), Some(Ok(_))),
            "the single on tablet {} did not commit (an arena miscount poisons the shard): {:?}",
            n + 1,
            ready(pc)
        );
    }
    // The group really outgrew the arena: some singles waited for room.
    assert!(
        engine.metrics().room_stalls.0 > 0,
        "no single waited for room: the prefill no longer fills the arena"
    );
    engine.close().unwrap();
}

/// 48 KiB commits on tablet 0 before the singles: enough that the singles' new memtables
/// don't all fit (crediting their slots is caught: the 33rd ran out of arena), not so many
/// that tablet 0 itself waits for room.
const PREFILL: usize = 70;
