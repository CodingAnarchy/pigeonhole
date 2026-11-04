//! #1497: one arena chunk per shard is kept back for freezes. A wait for room freezes open
//! memtables (D124), and each frozen one needs a fresh active memtable, so a free chunk.
//! When commits could take every chunk, the freezes found none and kept writing in place:
//! nothing was queued to flush, nothing freed room, and the wait ended only at the
//! write-stall timeout (30 s on the AX102, every writer at once). Admission now leaves one
//! chunk free beyond what admitted commits reserved, so a room wait's first freeze always
//! starts a fresh memtable and its flush frees room.
//!
//! What this doesn't cover is a legitimate wait: a frozen memtable that a snapshot pins
//! keeps its chunks after its flush, so a later freeze can starve again. That wait is bounded
//! by the room re-check timer (D161) and each waiter's own timeout (D240), not by this rule.
//!
//! Application-owned shard on `SimVfs`, the clock moved by hand.

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

const TIMEOUT: u64 = 30_000_000_000;
const CHUNK: usize = 32 << 10;
const BUDGET: u64 = 1 << 20;

struct Db {
    vfs: Arc<SimVfs>,
    engine: Arc<Engine>,
    shards: Vec<EngineShard>,
    table: Arc<TableInfo>,
}

fn open(seed: u64, tablets: bool) -> Db {
    let vfs = SimVfs::new(seed);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = tablets;
    o.balance_interval_nanos = 0;
    o.memtable_budget = BUDGET;
    // Never due by size: only a wait for room freezes, so the arena fills with open memtables.
    o.memtable_freeze_bytes = BUDGET;
    o.arena_chunk_bytes = Some(CHUNK);
    o.write_stall_timeout_nanos = TIMEOUT;
    o.compaction.l0_trigger = 1 << 20;
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/starved.phdb"), o).expect("open");
    let table = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    Db {
        vfs,
        engine,
        shards,
        table,
    }
}

impl Db {
    fn step(&mut self, nanos: u64) {
        self.vfs.advance(nanos);
        let now = pigeonhole_io::Vfs::monotonic_nanos(&*self.vfs);
        for s in &mut self.shards {
            for _ in 0..64 {
                if !s.run_once(now + 1_000) {
                    break;
                }
            }
        }
    }

    fn submit(&self, row: &[u8], value: &[u8]) -> PendingCommit {
        let mut wb = WriteBatch::new();
        wb.put(
            self.table.id,
            self.table.families[0].id,
            row,
            b"q",
            None,
            ValueRef::Bytes(value),
        )
        .unwrap();
        self.engine
            .submit(wb, Some(Durability::Buffered))
            .expect("submit")
    }

    /// Steps 1 ms at a time until `pc` resolves; the simulated time it took.
    fn wait(&mut self, pc: &mut PendingCommit, limit: u64) -> u64 {
        let mut cx = Context::from_waker(Waker::noop());
        let mut waited = 0;
        loop {
            if let Poll::Ready(r) = Pin::new(&mut *pc).poll(&mut cx) {
                r.expect("commit");
                return waited;
            }
            assert!(
                waited < limit,
                "a commit waited {} ms for room: the room wait's freezes found no chunk",
                waited / 1_000_000
            );
            self.step(1_000_000);
            waited += 1_000_000;
        }
    }

    fn close(mut self) {
        self.engine.close().unwrap();
        self.step(1_000);
    }
}

/// Writes four arenas' worth of 2 KiB rows, sixteen commits at a time, with nothing freezing by
/// size: every freeze is a wait for room's, made when the open memtable holds every chunk.
/// Each commit must resolve within a second of simulated time, nowhere near the 30 s stall
/// timeout.
fn fills_the_arena_without_a_stall(seed: u64, tablets: bool) {
    let mut db = open(seed, tablets);
    let value = vec![0x5Au8; 2048];
    let mut longest = 0;
    // Sixteen commits per group, as sixteen writers would send them: the members admitted
    // ahead of a waiting one hold reservations, so free room can be a chunk or more while
    // free less reserved is under a chunk.
    for g in 0..128u32 {
        let mut pcs: Vec<PendingCommit> = (0..16)
            .map(|i| db.submit(format!("row{:05}", g * 16 + i).as_bytes(), &value))
            .collect();
        for pc in &mut pcs {
            longest = longest.max(db.wait(pc, 1_000_000_000));
        }
    }
    let snap = db.engine.snapshot().unwrap();
    for i in [0u32, 511, 1024, 2047] {
        let row = format!("row{i:05}");
        assert!(
            db.engine
                .get(
                    &snap,
                    db.table.id,
                    db.table.families[0].id,
                    row.as_bytes(),
                    b"q"
                )
                .unwrap()
                .is_some(),
            "{row} lost"
        );
    }
    drop(snap);
    assert!(longest < 1_000_000_000, "longest wait {longest} ns");
    db.close();
}

#[test]
fn a_room_wait_with_every_chunk_taken_still_starts_a_flush() {
    fills_the_arena_without_a_stall(1497, true);
}

#[test]
fn a_room_wait_with_every_chunk_taken_still_starts_a_flush_without_tablets() {
    fills_the_arena_without_a_stall(1498, false);
}

/// The kept-back chunk never makes a batch wait for ever: a batch is refused as too large
/// only when it needs more than the arena less two chunks (`Room::Never`), and on an empty
/// arena one that needs exactly that much is admitted with the freeze chunk still free.
#[test]
fn a_batch_of_the_arena_less_two_chunks_commits_on_an_empty_arena() {
    let mut db = open(1499, true);
    // One entry, its overhead included, filling the arena less two chunks less a margin for
    // the key and the entry's header.
    let value = vec![0x5Au8; BUDGET as usize - 2 * CHUNK - 1024];
    let mut pc = db.submit(b"big", &value);
    db.wait(&mut pc, 1_000_000_000);
    db.close();
}

/// Two shards, every commit writing a row on each (a cross-shard commit, whose PREPARE
/// shares hold their room until the decision), sixteen at a time: the shape of a memory
/// store's `remember`, which writes several tables at once. Reservations held by shares
/// can leave free room above a chunk while free less reserved is under one.
#[test]
fn cross_shard_commits_that_fill_both_arenas_never_wait_out_the_timeout() {
    let vfs = SimVfs::new(1500);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.memtable_budget = BUDGET;
    o.memtable_freeze_bytes = BUDGET;
    o.arena_chunk_bytes = Some(CHUNK);
    o.write_stall_timeout_nanos = TIMEOUT;
    o.compaction.l0_trigger = 1 << 20;
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/starved2.phdb"), o).expect("open");
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
    let (a, b) = (a.unwrap(), b.unwrap());
    let mut db = Db {
        vfs,
        engine,
        shards,
        table: Arc::clone(&a),
    };
    let value = vec![0x5Au8; 2048];
    let mut longest = 0;
    for g in 0..128u32 {
        let mut pcs: Vec<PendingCommit> = (0..16)
            .map(|i| {
                let row = format!("row{:05}", g * 16 + i);
                let mut wb = WriteBatch::new();
                for t in [&a, &b] {
                    wb.put(
                        t.id,
                        t.families[0].id,
                        row.as_bytes(),
                        b"q",
                        None,
                        ValueRef::Bytes(&value),
                    )
                    .unwrap();
                }
                db.engine
                    .submit(wb, Some(Durability::Buffered))
                    .expect("submit")
            })
            .collect();
        for pc in &mut pcs {
            longest = longest.max(db.wait(pc, 1_000_000_000));
        }
    }
    assert!(
        db.engine.metrics().cross_shard.rounds > 0,
        "the commits were cross-shard"
    );
    assert!(longest < 1_000_000_000, "longest wait {longest} ns");
    db.close();
}

/// The holder that made #1497's stall on the AX102: undecided cross-shard shares keep
/// their room reserved until their decisions (D245). With the arena nearly full of open
/// memtables, a share admitted into the last free room left less than a chunk beyond its
/// reservation; the single that then waited for room froze the open memtable, found no
/// chunk for a fresh one, and nothing flushed until the stall timeout.
///
/// Shard 1's arena is filled with singles to `target` chunks free, a cross-shard commit's
/// share lands there (its decision held by `Fault::HoldDecides`, so its room stays
/// reserved), then one more single arrives. The singles all precede the share, so their
/// memtable is durable and may freeze (D227). Within a second, the open memtable must
/// freeze and flush; the share's decision is released after.
#[test]
fn a_room_wait_behind_an_undecided_share_still_starts_a_flush() {
    for target in 1..=8u64 {
        behind_an_undecided_share(target);
    }
}

fn behind_an_undecided_share(target: u64) {
    let vfs = SimVfs::new(1501 + target);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.memtable_budget = BUDGET;
    o.memtable_freeze_bytes = BUDGET;
    o.arena_chunk_bytes = Some(CHUNK);
    o.write_stall_timeout_nanos = TIMEOUT;
    o.compaction.l0_trigger = 1 << 20;
    let (engine, shards) =
        Engine::open_application_owned(Path::new("/db/starved3.phdb"), o).expect("open");
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
    let (a, b) = (a.unwrap(), b.unwrap());
    let mut db = Db {
        vfs,
        engine,
        shards,
        table: Arc::clone(&b),
    };
    // Singles on shard 1 until at most `target` chunks are free, or until they've written
    // more than the arena holds (the kept-back chunk then makes a wait for room flush first,
    // so free room may never get that low).
    let value = vec![0x5Au8; 2048];
    let mut i = 0u32;
    while db.engine.arena_free(1).0 > target * CHUNK as u64 && i < 512 {
        let mut pc = db.submit(format!("row{i:05}").as_bytes(), &value);
        db.wait(&mut pc, 1_000_000_000);
        i += 1;
    }
    // The share: a few kilobytes, held undecided.
    db.engine
        .inject(pigeonhole_engine::Fault::HoldDecides, true);
    let mut wb = WriteBatch::new();
    wb.put(
        a.id,
        a.families[0].id,
        b"x",
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    for j in 0..4u32 {
        wb.put(
            b.id,
            b.families[0].id,
            format!("share{j}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
    }
    let mut cross = db
        .engine
        .submit(wb, Some(Durability::Buffered))
        .expect("submit");
    for _ in 0..20 {
        db.step(1_000_000);
    }
    // One single after the share. If it waits for room, the wait must end within a second:
    // its freeze of the open memtable (only data from before the share, so durable) must
    // find a fresh chunk and flush.
    let (waits, waited) = db.engine.metrics().room_stalls;
    let mut late = db.submit(b"late", &value);
    for _ in 0..1000 {
        db.step(1_000_000);
        let (w, n) = db.engine.metrics().room_stalls;
        if w == waits || n > waited {
            break;
        }
    }
    let (w, n) = db.engine.metrics().room_stalls;
    assert!(
        w == waits || n > waited,
        "target {target}: a single waited a second for room behind an undecided share, with nothing freeing it"
    );
    db.engine
        .inject(pigeonhole_engine::Fault::HoldDecides, false);
    db.engine.release_held_decides(usize::MAX);
    db.wait(&mut cross, 2_000_000_000);
    db.wait(&mut late, 2_000_000_000);
    db.close();
}

/// simp724's deterministic shape of the AX102 state (#1497): two cross-shard commits, each
/// with its large share refused for room on its coordinator's shard while its small share
/// holds the peer shard's last room. Neither commit can decide, so neither small share
/// releases its room. A single on each shard then waits for room, and its freeze needs a
/// fresh chunk: on main none is free beyond the shares' reservations, and nothing is in
/// flight to flush, so every wait runs to the stall timeout. With one chunk kept back for
/// freezes, the shares can't take the last chunk, the singles' freezes flush, room frees,
/// and the refused shares retry and decide. Nothing holds the durable watermark here (no
/// decision seqno is reserved), so the freezes starve rather than defer.
#[test]
fn two_cross_shard_commits_holding_each_others_last_room_still_commit() {
    // The smallest arena the engine takes (a smaller budget is raised to it).
    const SMALL_BUDGET: u64 = 2 << 20;
    let vfs = SimVfs::new(1502);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.memtable_budget = SMALL_BUDGET;
    o.memtable_freeze_bytes = SMALL_BUDGET;
    o.arena_chunk_bytes = Some(CHUNK);
    o.write_stall_timeout_nanos = TIMEOUT;
    o.compaction.l0_trigger = 1 << 20;
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new("/db/cycle.phdb"), o).expect("open");
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
    let [ta, tb] = owned;
    let tables = [ta.unwrap(), tb.unwrap()];
    // One turn of shard `s`, the clock moved 1 ms first.
    let turn = |shards: &mut Vec<EngineShard>, s: usize| {
        vfs.advance(1_000_000);
        let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
        shards[s].run_once(now + 1_000);
    };
    let both = |shards: &mut Vec<EngineShard>| {
        for s in 0..2 {
            turn(shards, s);
        }
    };
    let put = |wb: &mut WriteBatch, s: usize, row: &[u8], len: usize| {
        let t = &tables[s];
        wb.put(
            t.id,
            t.families[0].id,
            row,
            b"q",
            None,
            ValueRef::Bytes(&vec![0x5A; len]),
        )
        .unwrap();
    };
    let small = 1024;
    let large = 2 * CHUNK;
    // 1. Fill each arena with singles until at most one chunk is free (or more than the
    // arena has been written: with a chunk kept back, a wait for room flushes first).
    for s in 0..2 {
        let mut i = 0u32;
        while engine.arena_free(s).0 > CHUNK as u64 && i < 4096 {
            let mut wb = WriteBatch::new();
            put(&mut wb, s, format!("fill{i:05}").as_bytes(), 2048);
            let mut pc = engine.submit(wb, Some(Durability::Buffered)).unwrap();
            let mut cx = Context::from_waker(Waker::noop());
            for _ in 0..1000 {
                both(&mut shards);
                if let Poll::Ready(r) = Pin::new(&mut pc).poll(&mut cx) {
                    r.expect("fill");
                    break;
                }
            }
            i += 1;
        }
    }
    // 2. Y, coordinated by shard 1 (its first row's): large on 1, small on 0.
    let mut wb = WriteBatch::new();
    put(&mut wb, 1, b"y-large", large);
    put(&mut wb, 0, b"y-small", small);
    let mut y = engine.submit(wb, Some(Durability::Buffered)).unwrap();
    turn(&mut shards, 1);
    // 3. X, coordinated by shard 0: large on 0, small on 1.
    let mut wb = WriteBatch::new();
    put(&mut wb, 0, b"x-large", large);
    put(&mut wb, 1, b"x-small", small);
    let mut x = engine.submit(wb, Some(Durability::Buffered)).unwrap();
    turn(&mut shards, 0);
    // 4. Shard 1 takes X's small share.
    turn(&mut shards, 1);
    // 5. A single on each shard, then turns: within a second of simulated time both
    // commits and both singles resolve.
    let mut singles: Vec<PendingCommit> = (0..2)
        .map(|s| {
            let mut wb = WriteBatch::new();
            put(&mut wb, s, b"late", 2048);
            engine.submit(wb, Some(Durability::Buffered)).unwrap()
        })
        .collect();
    let mut cx = Context::from_waker(Waker::noop());
    let mut pending: Vec<&mut PendingCommit> = vec![&mut x, &mut y];
    pending.extend(singles.iter_mut());
    let mut done = vec![false; pending.len()];
    for _ in 0..1000 {
        both(&mut shards);
        for (k, pc) in pending.iter_mut().enumerate() {
            if !done[k]
                && let Poll::Ready(r) = Pin::new(&mut **pc).poll(&mut cx)
            {
                r.unwrap_or_else(|e| panic!("commit {k}: {e}"));
                done[k] = true;
            }
        }
        if done.iter().all(|d| *d) {
            break;
        }
    }
    assert!(
        done.iter().all(|d| *d),
        "commits still waiting after a second (X, Y, single 0, single 1): {done:?}; room \
         refusals {}",
        engine.room_refusals()
    );
    engine.close().unwrap();
    for _ in 0..100 {
        both(&mut shards);
    }
}

/// The kept chunk as an invariant (#1497, and simp724's point that it must hold against
/// every chunk consumer, not just admission): on a seeded mixed load, after every turn of
/// each shard, a shard with no flush running or queued has at least one chunk free beyond
/// its reservations, so its next freeze can start a fresh memtable.
/// - The load: singles and cross-shard commits of 512 B to 8 KiB, in groups of 1 to 16,
///   on two small arenas with tablet changes on.
/// - Every so often: a tablet split, a flush and a full compaction.
/// - Every commit must also resolve within a second of simulated time.
///
/// Only a freeze's fresh memtable takes a chunk outside admission, and it queues a flush.
/// Everything else takes chunks from room admission reserved (apply's new slots, the
/// memtable's own growth) or keeps its own margin (`replay_fits`, the counter checkpoint).
/// Snapshots aren't held here: a pinned memtable's chunks stay taken after its flush, a
/// legitimate wait this rule doesn't cover.
#[test]
fn a_chunk_stays_free_beyond_reservations_whenever_no_flush_is_in_flight() {
    for seed in [1497u64, 1498, 1499] {
        mixed_load_keeps_the_freeze_chunk(seed);
    }
}

fn mixed_load_keeps_the_freeze_chunk(seed: u64) {
    let vfs = SimVfs::new(seed);
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut rnd = move |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    };
    // Two lives of the same file: the second opens by replaying the first's WAL, then runs
    // the same load.
    for life in 0..2u32 {
        let mut o = EngineOptions::new(vfs.clone());
        o.create_if_missing = true;
        o.shards = 2;
        o.pin_threads = false;
        o.tablet_changes = true;
        o.balance_interval_nanos = 0;
        o.memtable_budget = 2 << 20;
        // Never due by size: every freeze is a wait for room's, the case the rule is for.
        o.memtable_freeze_bytes = 2 << 20;
        o.arena_chunk_bytes = Some(CHUNK);
        o.write_stall_timeout_nanos = TIMEOUT;
        o.compaction.l0_trigger = 1 << 20;
        let (engine, mut shards) =
            Engine::open_application_owned(Path::new("/db/mixed.phdb"), o).expect("open");
        let check = |engine: &Engine, at: &str| {
            for s in 0..2 {
                let (free, reserved, flushing) = engine.arena_reserved(s);
                assert!(
                    flushing || free >= reserved + CHUNK as u64,
                    "seed {seed}, life {life}, {at}, shard {s}: {free} bytes free, {reserved} \
                     reserved, and no flush in flight: a freeze now would find no chunk"
                );
            }
        };
        check(&engine, "after open");
        let mut owned: [Option<Arc<TableInfo>>; 2] = [None, None];
        for i in 0.. {
            let name = format!("t{i}");
            let t = match engine.table(&name) {
                Some(t) => t,
                None => engine
                    .create_table(&name, &[("f".into(), FamilyOptions::default())])
                    .expect("table"),
            };
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
        let [ta, tb] = owned;
        let tables = [ta.unwrap(), tb.unwrap()];
        let mut cx = Context::from_waker(Waker::noop());
        let mut pending: Vec<(PendingCommit, u64)> = Vec::new();
        let mut maintenance = Vec::new();
        let mut now_ms = 0u64;
        let mut splits = 0u32;
        for round in 0..1200u32 {
            for _ in 0..1 + rnd(16) {
                let mut wb = WriteBatch::new();
                let cross = rnd(3) == 0;
                let first = rnd(2) as usize;
                for (k, s) in [first, 1 - first].into_iter().enumerate() {
                    if k == 1 && !cross {
                        break;
                    }
                    let t = &tables[s];
                    let len = 512 + rnd(8 * 1024 - 512) as usize;
                    let row = format!("l{life}r{round:04}-{}", rnd(1 << 20));
                    wb.put(
                        t.id,
                        t.families[0].id,
                        row.as_bytes(),
                        b"q",
                        None,
                        ValueRef::Bytes(&vec![0x5A; len]),
                    )
                    .unwrap();
                }
                pending.push((
                    engine.submit(wb, Some(Durability::Buffered)).unwrap(),
                    now_ms,
                ));
            }
            // The other chunk consumers: a tablet split (its receiver's slots), a flush() and
            // a full compaction (D138's waits).
            if round % 97 == 50 && splits < 3 {
                splits += 1;
                let t = &tables[rnd(2) as usize];
                if let Ok(m) =
                    engine.split_tablet_pending(t.id, format!("l{life}r{round:04}").as_bytes())
                {
                    maintenance.push((m, true));
                }
            }
            if round % 131 == 70 {
                maintenance.push((engine.flush_pending().unwrap(), false));
            }
            if round % 173 == 90 {
                maintenance.push((engine.compact_pending(None).unwrap(), false));
            }
            for _ in 0..4 {
                vfs.advance(1_000_000);
                now_ms += 1;
                let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
                for s in 0..2 {
                    shards[s].run_once(now + 1_000);
                    check(&engine, &format!("round {round}"));
                }
            }
            maintenance.retain_mut(|(m, split)| match Pin::new(m).poll(&mut cx) {
                // A split the small arena can't serve is refused (`Unsupported`): fine.
                Poll::Ready(r) => {
                    if !*split {
                        r.expect("maintenance");
                    }
                    false
                }
                Poll::Pending => true,
            });
            pending.retain_mut(|(pc, since)| match Pin::new(pc).poll(&mut cx) {
                Poll::Ready(r) => {
                    r.expect("commit");
                    false
                }
                Poll::Pending => {
                    assert!(
                        now_ms - *since < 1_000,
                        "seed {seed}, life {life}: a commit has waited {} ms",
                        now_ms - *since
                    );
                    true
                }
            });
        }
        for _ in 0..5000 {
            if pending.is_empty() && maintenance.is_empty() {
                break;
            }
            vfs.advance(1_000_000);
            let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
            for s in 0..2 {
                shards[s].run_once(now + 1_000);
            }
            pending.retain_mut(|(pc, _)| match Pin::new(pc).poll(&mut cx) {
                Poll::Ready(r) => {
                    r.expect("commit");
                    false
                }
                Poll::Pending => true,
            });
            maintenance.retain_mut(|(m, split)| match Pin::new(m).poll(&mut cx) {
                // A split the small arena can't serve is refused (`Unsupported`): fine.
                Poll::Ready(r) => {
                    if !*split {
                        r.expect("maintenance");
                    }
                    false
                }
                Poll::Pending => true,
            });
        }
        assert!(
            pending.is_empty(),
            "seed {seed}: {} commits never resolved",
            pending.len()
        );
        assert!(
            maintenance.is_empty(),
            "seed {seed}: maintenance never finished"
        );
        // The second life replays what the first left in its WAL: crash rather than close.
        if life == 0 {
            drop(engine);
            drop(shards);
        } else {
            engine.close().unwrap();
            for _ in 0..100 {
                let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
                for s in 0..2 {
                    shards[s].run_once(now + 1_000);
                }
            }
        }
    }
}
