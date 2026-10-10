//! A slot written once and never again must not pin its stream's checkpoint (#137): once a
//! stream holds more than its limit of bytes the checkpoint cannot pass, the shard flushes
//! the slots its oldest records wrote (and asks the participants of its oldest cross-shard
//! COMMITs to flush their shares), so the WAL, the shard's log and the next open's replay
//! stay bounded.
//!
//! Application-owned with every shard driven by the test, so each run is deterministic.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/data.phdb";
const BUDGET: u64 = 4 << 20;
const HOT_COMMITS: u32 = 20_000;
const VALUE: usize = 1000;

fn options(vfs: &Arc<SimVfs>, shards: usize) -> EngineOptions {
    common::options(Arc::clone(vfs), shards, BUDGET)
}

/// Bytes in every WAL stream file.
fn wal_bytes(vfs: &SimVfs) -> u64 {
    let mut total = 0;
    for p in vfs.list_dir(Path::new("/db")).unwrap() {
        if p.to_string_lossy().contains("-wal-") {
            total += vfs.open(&p, OpenOptions::read()).unwrap().len().unwrap();
        }
    }
    total
}

/// Runs every shard until `pending` resolves.
fn drive(shards: &mut [EngineShard], mut pending: PendingCommit) {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        match Pin::new(&mut pending).poll(&mut cx) {
            Poll::Ready(r) => {
                r.unwrap();
                return;
            }
            Poll::Pending => {}
        }
    }
    panic!("the commit never resolved");
}

fn put(
    batch: &mut WriteBatch,
    table: pigeonhole_format::TableId,
    family: u32,
    row: &[u8],
    v: &[u8],
) {
    batch
        .put(
            table,
            pigeonhole_format::FamilyId(family),
            row,
            b"q",
            None,
            ValueRef::Bytes(v),
        )
        .unwrap();
}

/// One shard: a single write to family `cold`, then `HOT_COMMITS` 1 KB commits to family
/// `hot`. Returns the WAL bytes at the end.
fn single_shard(cold: bool, seed: u64) -> u64 {
    let vfs = SimVfs::new(seed);
    let db = Engine::open(Path::new(DB), options(&vfs, 1)).unwrap();
    let t = db
        .create_table(
            "t",
            &[
                ("cold".into(), FamilyOptions::default()),
                ("hot".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    db.close().unwrap();
    drop(db);

    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(&vfs, 1)).unwrap();
    let cold_f = t.family("cold").unwrap().id.0;
    let hot_f = t.family("hot").unwrap().id.0;
    if cold {
        let mut wb = WriteBatch::new();
        put(&mut wb, t.id, cold_f, b"cold-row", b"cold-value");
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::GroupSync)).unwrap(),
        );
    }
    let v = vec![7u8; VALUE];
    let mut max_wal = 0;
    for i in 0..HOT_COMMITS {
        let mut wb = WriteBatch::new();
        put(&mut wb, t.id, hot_f, &i.to_be_bytes(), &v);
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::GroupSync)).unwrap(),
        );
        if i % 1000 == 0 {
            max_wal = max_wal.max(wal_bytes(&vfs));
        }
    }
    max_wal = max_wal.max(wal_bytes(&vfs));

    // The forced flush loses nothing: a power loss now keeps the cold write.
    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    let db = Engine::open(Path::new(DB), options(&vfs, 1)).unwrap();
    let got = db
        .get_latest(t.id, pigeonhole_format::FamilyId(cold_f), b"cold-row", b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()));
    assert_eq!(got.as_deref(), cold.then_some(&b"cold-value"[..]));
    db.close().unwrap();
    max_wal
}

#[test]
fn a_cold_slot_does_not_pin_the_wal() {
    let without = single_shard(false, 137);
    let with = single_shard(true, 137);
    // Before #137: ~24 MB with the cold write (every hot byte kept), ~1 MB without. The
    // limit is twice the memtable budget; allow a budget more for spares and the segment
    // in progress.
    assert!(
        with <= 3 * BUDGET,
        "one cold write pinned the WAL: {with} bytes (without it: {without})"
    );
}

#[test]
fn an_idle_participant_does_not_pin_the_coordinators_wal() {
    let vfs = SimVfs::new(1371);
    let db = Engine::open(Path::new(DB), options(&vfs, 2)).unwrap();
    // Tablet 1 → shard 1, tablet 2 → shard 0.
    let a = db
        .create_table("a", &[("cold".into(), FamilyOptions::default())])
        .unwrap();
    let b = db
        .create_table(
            "b",
            &[
                ("cold".into(), FamilyOptions::default()),
                ("hot".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    db.close().unwrap();
    drop(db);

    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(&vfs, 2)).unwrap();
    // One cross-shard commit: shard 0 coordinates, shard 1 holds a share and then idles.
    let mut wb = WriteBatch::new();
    put(
        &mut wb,
        a.id,
        a.family("cold").unwrap().id.0,
        b"x",
        b"cold-a",
    );
    put(
        &mut wb,
        b.id,
        b.family("cold").unwrap().id.0,
        b"x",
        b"cold-b",
    );
    drive(
        &mut shards,
        db.submit(wb, Some(Durability::GroupSync)).unwrap(),
    );
    let hot_f = b.family("hot").unwrap().id.0;
    let v = vec![7u8; VALUE];
    for i in 0..HOT_COMMITS {
        let mut wb = WriteBatch::new();
        put(&mut wb, b.id, hot_f, &i.to_be_bytes(), &v);
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::GroupSync)).unwrap(),
        );
    }
    let wal = wal_bytes(&vfs);
    assert!(
        wal <= 4 * BUDGET,
        "an idle participant pinned the coordinator's WAL: {wal} bytes"
    );

    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    let db = Engine::open(Path::new(DB), options(&vfs, 2)).unwrap();
    for (t, want) in [(&a, b"cold-a"), (&b, b"cold-b")] {
        let got = db
            .get_latest(t.id, t.family("cold").unwrap().id, b"x", b"q")
            .unwrap()
            .map(|c| common::value_bytes(c.value()));
        assert_eq!(got.as_deref(), Some(&want[..]));
    }
    db.close().unwrap();
}

/// Runs the shards whose index is in `which` until they go idle.
fn run_only(shards: &mut [EngineShard], which: &[u16]) {
    for _ in 0..100_000 {
        let mut more = false;
        for s in shards.iter_mut().filter(|s| which.contains(&s.index())) {
            more |= s.run_once(u64::MAX);
        }
        if !more {
            return;
        }
    }
    panic!("the shards never went idle");
}

/// The WAL bytes of stream `stream`.
fn stream_bytes(vfs: &SimVfs, stream: u32) -> u64 {
    vfs.open(
        Path::new(&format!("{DB}-wal-{stream}")),
        OpenOptions::read(),
    )
    .unwrap()
    .len()
    .unwrap()
}

#[test]
fn a_participant_serves_an_unpin_for_a_lower_seqno_decided_later() {
    // Three shards: coordinator A (shard 0), coordinator B (shard 1), participant P
    // (shard 2). Commit Y (B and P) takes its seqno first but is decided last; commit X
    // (A and P) is decided first, and A's pass sends P `Unpin { through: X }`. Then B
    // writes alone. Its pass sends P `Unpin { through: Y }` with Y < X: P must still flush
    // its share of Y, or B's checkpoint stays behind Y's COMMIT for good.
    let vfs = SimVfs::new(1372);
    let mut o = options(&vfs, 3);
    o.wal_pin_bytes = 1 << 20;
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    let family = || vec![("f".into(), FamilyOptions::default())];
    // Tablet n is on shard n % 3.
    let b = db.create_table("b", &family()).unwrap();
    let px = db.create_table("px", &family()).unwrap();
    let a = db.create_table("a", &family()).unwrap();
    let _filler = db.create_table("filler", &family()).unwrap();
    let py = db.create_table("py", &family()).unwrap();
    db.close().unwrap();
    drop(db);

    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let f = |t: &pigeonhole_engine::TableInfo| t.families[0].id.0;
    // Y: B coordinates; P prepares; B never hears back yet.
    let mut wb = WriteBatch::new();
    put(&mut wb, b.id, f(&b), b"y", b"y-b");
    put(&mut wb, py.id, f(&py), b"y", b"y-p");
    let y = db.submit(wb, Some(Durability::GroupSync)).unwrap();
    run_only(&mut shards, &[1]);
    run_only(&mut shards, &[2]);
    // X: A coordinates, P participates; decided while Y is not.
    let mut wb = WriteBatch::new();
    put(&mut wb, a.id, f(&a), b"x", b"x-a");
    put(&mut wb, px.id, f(&px), b"x", b"x-p");
    let x = db.submit(wb, Some(Durability::GroupSync)).unwrap();
    run_only(&mut shards, &[0, 2]);
    // A writes past its limit: its pass asks P to flush its share of X.
    let v = vec![7u8; VALUE];
    let mut held = Vec::new();
    for i in 0..1500u32 {
        let mut wb = WriteBatch::new();
        put(&mut wb, a.id, f(&a), &i.to_be_bytes(), &v);
        held.push(db.submit(wb, Some(Durability::GroupSync)).unwrap());
        run_only(&mut shards, &[0, 2]);
    }
    assert!(
        db.metrics().unpin.0 >= 2,
        "A's pass and P's answer: {:?}",
        db.metrics()
    );
    // Now Y is decided, and B writes alone.
    drive(&mut shards, y);
    drive(&mut shards, x);
    for p in held {
        drive(&mut shards, p);
    }
    for i in 0..HOT_COMMITS {
        let mut wb = WriteBatch::new();
        put(&mut wb, b.id, f(&b), &i.to_be_bytes(), &v);
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::GroupSync)).unwrap(),
        );
    }
    let wal = stream_bytes(&vfs, 1);
    assert!(
        wal <= 4 << 20,
        "P ignored B's Unpin for the lower seqno: B's WAL holds {wal} bytes (unpin passes \
         and forced flushes: {:?})",
        db.metrics().unpin
    );
}

/// What [`mixed`] measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MixStats {
    /// Unpin passes (own and served) and the memtables they froze below the threshold.
    passes: u64,
    small_flushes: u64,
    /// The largest WAL seen.
    max_wal: u64,
}

/// Slots of mixed temperature on four shards (#175): 8 tables (one tablet each, tablet `n` on
/// shard `n % 4`) of 4 families, so 32 slots. Each commit writes a 1 KB value to a slot drawn
/// with weight `1 / (rank + 1)`, and one commit in 10 also writes a slot of another table,
/// most often on another shard (a cross-shard commit). Deterministic: one commit at a time,
/// every shard driven by the test. Debug builds check every pass's slots and requests against
/// a scan of the whole log.
fn mixed(seed: u64, commits: u32) -> MixStats {
    const TABLES: usize = 8;
    const FAMILIES: usize = 4;
    let vfs = SimVfs::new(seed);
    let db = Engine::open(Path::new(DB), options(&vfs, 4)).unwrap();
    let fams: Vec<(String, FamilyOptions)> = (0..FAMILIES)
        .map(|f| (format!("f{f}"), FamilyOptions::default()))
        .collect();
    let tables: Vec<_> = (0..TABLES)
        .map(|t| db.create_table(&format!("t{t}"), &fams).unwrap())
        .collect();
    db.close().unwrap();
    drop(db);

    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(&vfs, 4)).unwrap();
    let slots: Vec<(pigeonhole_format::TableId, u32)> = tables
        .iter()
        .flat_map(|t| (0..FAMILIES).map(move |f| (t.id, t.family(&format!("f{f}")).unwrap().id.0)))
        .collect();
    // Weights 1 / (rank + 1) over a seeded shuffle of the slots.
    let mut rng = pigeonhole_sim::Rng::new(seed);
    let mut order: Vec<usize> = (0..slots.len()).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let weights: Vec<f64> = (0..slots.len()).map(|r| 1.0 / (r as f64 + 1.0)).collect();
    let total: f64 = weights.iter().sum();
    let pick = |rng: &mut pigeonhole_sim::Rng| {
        let mut x = rng.below(1 << 30) as f64 / f64::from(1 << 30) * total;
        for (r, w) in weights.iter().enumerate() {
            if x < *w {
                return order[r];
            }
            x -= w;
        }
        order[slots.len() - 1]
    };
    let v = vec![7u8; VALUE];
    let mut max_wal = 0;
    for i in 0..commits {
        let mut wb = WriteBatch::new();
        let a = pick(&mut rng);
        put(&mut wb, slots[a].0, slots[a].1, &i.to_be_bytes(), &v);
        if rng.below(10) == 0 {
            let b = pick(&mut rng);
            if slots[b].0 != slots[a].0 {
                put(&mut wb, slots[b].0, slots[b].1, &i.to_be_bytes(), &v);
            }
        }
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::Buffered)).unwrap(),
        );
        if i % 1000 == 0 {
            max_wal = max_wal.max(wal_bytes(&vfs));
        }
    }
    max_wal = max_wal.max(wal_bytes(&vfs));
    let m = db.metrics();
    drop(shards);
    drop(db);
    MixStats {
        passes: m.unpin.0,
        small_flushes: m.unpin.1,
        max_wal,
    }
}

/// The WAL stays bounded with slots of every temperature and cross-shard commits (#175).
/// `-- --nocapture` prints passes, the small memtables they froze, and the largest WAL.
#[test]
fn mixed_temperatures_keep_the_wal_bounded() {
    for seed in [175, 176] {
        let s = mixed(seed, if cfg!(miri) { 500 } else { 40_000 });
        println!("seed {seed}: {s:?}");
        // Four streams, each bounded as in `a_cold_slot_does_not_pin_the_wal`.
        assert!(s.max_wal <= 4 * 3 * BUDGET, "seed {seed}: {s:?}");
        assert!(
            s.passes > 0,
            "seed {seed}: the limit was never reached: {s:?}"
        );
    }
}
