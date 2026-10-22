//! #693 PR 2 (#39): a reader process's slot pins exactly what its live snapshots need. The
//! seqno pin is the oldest live snapshot's seqno and the view pin its oldest view; with no
//! snapshot alive the slot pins nothing. D87's pin used to stay put while any snapshot of the
//! reader lived, and stay at the last snapshot after every snapshot was dropped, so an idle
//! reader held the writer's memtables for ever.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, FamilyOptions, Snapshot, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const WRITER: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};
const READER: ProcessId = ProcessId {
    pid: 2,
    start_time: 1,
};

fn options_with(vfs: &Arc<SimVfs>, budget: u64) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 1, budget);
    o.memtable_freeze_bytes = 256 << 10;
    o.tablet_changes = false;
    // A commit or flush waiting for room behind the reader's pins gives up quickly: the
    // simulator's clock moves slowly, so the 30 s default would take for ever.
    o.write_stall_timeout_nanos = 5_000_000;
    o
}

/// Writes `row` = `value` (and a 300-byte filler per row of `fill`), Buffered.
fn put(db: &Engine, t: &TableInfo, row: &[u8], value: &[u8]) {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(value),
    )
    .unwrap();
    db.commit(wb, Some(Durability::Buffered)).unwrap();
}

/// Writes rows `range` with 300-byte values: enough of them fills the 2 MiB arena.
fn fill(db: &Engine, t: &TableInfo, range: std::ops::Range<u32>) -> pigeonhole_engine::Result<()> {
    for i in range {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            format!("fill{i:06}").as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&[(i % 251) as u8; 300]),
        )?;
        db.commit(wb, Some(Durability::Buffered))?;
    }
    Ok(())
}

fn open(seed: u64) -> (Arc<SimVfs>, Arc<Engine>, Arc<TableInfo>, Arc<Engine>) {
    open_with(seed, 2 << 20)
}

fn open_with(seed: u64, budget: u64) -> (Arc<SimVfs>, Arc<Engine>, Arc<TableInfo>, Arc<Engine>) {
    let vfs = SimVfs::new(seed);
    vfs.enter_process(WRITER);
    let db = Engine::open(Path::new(DB), options_with(&vfs, budget)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    put(&db, &t, b"k", b"0");
    vfs.enter_process(READER);
    let reader = Engine::open_reader(Path::new(DB), options_with(&vfs, budget)).unwrap();
    vfs.enter_process(WRITER);
    (vfs, db, t, reader)
}

fn pin(vfs: &SimVfs, db: &Engine) -> Option<(u64, u64)> {
    vfs.enter_process(WRITER);
    db.reader_pin_and_view().0
}

fn snap(vfs: &SimVfs, reader: &Engine, durable: bool) -> Snapshot {
    vfs.enter_process(READER);
    let s = if durable {
        reader.durable_snapshot()
    } else {
        reader.snapshot()
    }
    .unwrap();
    vfs.enter_process(WRITER);
    s
}

fn read_k(vfs: &SimVfs, reader: &Engine, t: &TableInfo, s: &Snapshot) -> Vec<u8> {
    vfs.enter_process(READER);
    let v = reader
        .get(s, t.id, t.families[0].id, b"k", b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
        .expect("k is present");
    vfs.enter_process(WRITER);
    v
}

#[test]
fn an_idle_reader_pins_nothing_and_the_writer_reclaims_under_it() {
    let (vfs, db, t, reader) = open(6930);
    let s = snap(&vfs, &reader, false);
    assert!(pin(&vfs, &db).is_some(), "a live snapshot pins");
    vfs.enter_process(READER);
    drop(s);
    assert_eq!(
        pin(&vfs, &db),
        None,
        "a reader with no snapshot pins nothing"
    );
    // Several arenas' worth of writes, flushed as they go: the retired memtables are freed.
    // (The simulator's clock is frozen, so a writer waiting for room behind a stale pin
    // fails `Busy` at once instead of waiting.)
    for round in 0..4u32 {
        fill(&db, &t, round * 8000..(round + 1) * 8000)
            .expect("the writer stalled behind an idle reader");
        db.flush().unwrap();
    }
    vfs.enter_process(READER);
    drop(reader);
    vfs.enter_process(WRITER);
    db.close().unwrap();
}

#[test]
fn the_pin_is_the_oldest_live_snapshot_and_rises_as_it_drops() {
    let (vfs, db, t, reader) = open(6931);
    let s1 = snap(&vfs, &reader, false);
    let p1 = pin(&vfs, &db).unwrap();
    put(&db, &t, b"k", b"1");
    fill(&db, &t, 0..1000).unwrap();
    db.flush().unwrap();
    let s2 = snap(&vfs, &reader, false);
    put(&db, &t, b"k", b"2");
    db.flush().unwrap();
    let s3 = snap(&vfs, &reader, false);
    assert!(s1.seqno() < s2.seqno() && s2.seqno() < s3.seqno());
    // The oldest snapshot's pins hold while it lives.
    assert_eq!(pin(&vfs, &db), Some(p1));
    assert_eq!(p1.0, s1.seqno());

    // Dropping the oldest raises both pins to the next one's.
    vfs.enter_process(READER);
    drop(s1);
    let p2 = pin(&vfs, &db).unwrap();
    assert_eq!(p2.0, s2.seqno(), "the seqno pin did not rise");
    assert!(p2.1 > p1.1, "the view pin did not rise: {p1:?} -> {p2:?}");
    // Dropping a newer one leaves the oldest's pins.
    vfs.enter_process(READER);
    drop(s3);
    assert_eq!(pin(&vfs, &db), Some(p2));
    // What the remaining snapshot reads is still there.
    assert_eq!(read_k(&vfs, &reader, &t, &s2), b"1");
    vfs.enter_process(READER);
    drop(s2);
    assert_eq!(pin(&vfs, &db), None);
    vfs.enter_process(READER);
    drop(reader);
    vfs.enter_process(WRITER);
    db.close().unwrap();
}

#[test]
fn a_durable_snapshot_below_the_pin_lowers_it_until_dropped() {
    let (vfs, db, t, reader) = open(6932);
    // A synced commit, then a Buffered one that is visible but not durable.
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        b"k",
        b"q",
        None,
        ValueRef::Bytes(b"s"),
    )
    .unwrap();
    db.commit(wb, Some(Durability::Sync)).unwrap();
    put(&db, &t, b"k", b"b");
    let visible = snap(&vfs, &reader, false);
    let durable = snap(&vfs, &reader, true);
    if durable.seqno() < visible.seqno() {
        assert_eq!(
            pin(&vfs, &db).unwrap().0,
            durable.seqno(),
            "a durable snapshot lowers the pin"
        );
        vfs.enter_process(READER);
        drop(durable);
        assert_eq!(
            pin(&vfs, &db).unwrap().0,
            visible.seqno(),
            "dropping it raises the pin back"
        );
    } else {
        // The Buffered commit was already durable: nothing to lower.
        vfs.enter_process(READER);
        drop(durable);
    }
    vfs.enter_process(READER);
    drop(visible);
    assert_eq!(pin(&vfs, &db), None);
    vfs.enter_process(READER);
    drop(reader);
    vfs.enter_process(WRITER);
    db.close().unwrap();
}

/// xorshift64*.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

#[test]
fn seeded_snapshot_churn_keeps_the_pin_exact_and_every_read_right() {
    let seeds: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let base: u64 = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    for seed in base..base + seeds {
        let r = std::panic::catch_unwind(|| churn(seed));
        if let Err(e) = r {
            eprintln!("failed at seed {seed}");
            std::panic::resume_unwind(e);
        }
    }
}

fn churn(seed: u64) {
    // A budget the churn's writes never fill (a parked shard thread never sees the
    // simulator's clock move, so a commit waiting for room behind old pins would wait for
    // ever); memtables still freeze every 256 KiB, flush and are reclaimed past the pins.
    let (vfs, db, t, reader) = open_with(10_000 + seed, 64 << 20);
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    // Live snapshots and the value of `k` each read when taken.
    let mut live: Vec<(Snapshot, Vec<u8>)> = Vec::new();
    let mut next_fill = 0u32;
    for step in 0..400u32 {
        match rng.below(10) {
            0..=2 if live.len() < 6 => {
                let durable = rng.below(4) == 0;
                let s = snap(&vfs, &reader, durable);
                let v = read_k(&vfs, &reader, &t, &s);
                live.push((s, v));
            }
            3..=4 if !live.is_empty() => {
                let i = rng.below(live.len() as u64) as usize;
                let (s, _) = live.swap_remove(i);
                vfs.enter_process(READER);
                drop(s);
                vfs.enter_process(WRITER);
            }
            // A flush may find no room for fresh memtables behind the pins: `Busy`.
            5 => {
                if let Err(e) = db.flush() {
                    assert!(
                        matches!(e, pigeonhole_engine::Error::Busy),
                        "seed {seed} step {step}: {e:?}"
                    );
                }
            }
            _ => {
                put(&db, &t, b"k", format!("v{step}").as_bytes());
                let n = 1 + rng.below(200) as u32;
                // Room may run out while old snapshots pin the arena: that is `Busy`, not a
                // wrong read, and the churn goes on.
                let _ = fill(&db, &t, next_fill..next_fill + n);
                next_fill += n;
            }
        }
        // The pin is the oldest live snapshot's seqno, or nothing.
        let want = live.iter().map(|(s, _)| s.seqno()).min();
        assert_eq!(
            pin(&vfs, &db).map(|(s, _)| s),
            want,
            "seed {seed} step {step}: the seqno pin is not the oldest live snapshot's"
        );
        // Every live snapshot still reads what it read when taken.
        for (s, v) in &live {
            assert_eq!(
                &read_k(&vfs, &reader, &t, s),
                v,
                "seed {seed} step {step}: a snapshot's read changed"
            );
        }
    }
    vfs.enter_process(READER);
    drop(live);
    drop(reader);
    vfs.enter_process(WRITER);
    db.close().unwrap();
}
