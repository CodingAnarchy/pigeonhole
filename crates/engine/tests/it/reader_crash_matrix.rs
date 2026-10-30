//! #693's seeded crash matrix for reader processes (SimVfs): a writer committing batches,
//! flushing and compacting, reader processes taking, scanning and dropping snapshots, and
//! seeded kills of either side: readers by `crash_process` (their handles forgotten, as a
//! `SIGKILL` runs no destructors) and the writer by `crash_process` and a reopen.
//!
//! Each commit is one batch of [`BATCH`] rows named after its batch number. Invariants:
//! - **Whole and in order:** a snapshot sees batches `0..n`, each with all its rows.
//! - **Stable:** rescanning a snapshot gives the same `n`, unless it fails as expired.
//! - **Typed failures only:** a read fails with `SnapshotExpired` (a writer restarted, or the
//!   writer freed the slot) or `Busy` (a view the reader could not build), never anything
//!   else, and never with rows from a newer or partial state.
//! - **Recovery:** a reopened writer sees every batch it committed (`Buffered` survives a
//!   process kill), and so does a reader's next snapshot.
//! - **Reclamation:** after the last reader is killed and the writer's liveness interval
//!   passes, no slot pins anything.
//! - **Row cache (D281):** every commit also overwrites one hot row with its batch number,
//!   and readers, with their row cache on, read it latest: a reader's reads of it never go
//!   back and never pass the last batch committed, across writer kills and reopens (a new
//!   generation each, its cache emptied at re-attach) and reader kills.
//!
//! `PIGEONHOLE_SEED`/`PIGEONHOLE_SEEDS` choose the seeds (default 1..=8); a failure names
//! its seed and step.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, ReadSpec, RowData, ScanSpec, Snapshot, TableInfo,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";
const BATCH: u64 = 4;
const STEPS: u32 = 300;
/// More than the writer's 250 ms liveness interval.
const PAST_INTERVAL: u64 = 300_000_000;

fn seeds() -> Vec<u64> {
    let n: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let base: u64 = std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    (base..base + n).collect()
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 1, 2 << 20);
    o.memtable_freeze_bytes = 256 << 10;
    o.tablet_changes = false;
    // The writer's row cache on, so it raises the slots and readers cache (D281).
    o.row_cache_bytes = 1 << 20;
    o
}

/// A reader process's options: its row cache on (D281).
fn reader_options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = options(vfs);
    o.row_cache_bytes = 1 << 20;
    o
}

fn writer_process(incarnation: u32) -> ProcessId {
    ProcessId {
        pid: 1,
        start_time: u64::from(incarnation) + 1,
    }
}

fn row(batch: u64, j: u64) -> Vec<u8> {
    format!("b{batch:06}-{j}").into_bytes()
}

fn commit(db: &Engine, t: &TableInfo, h: &TableInfo, batch: u64) -> Result<(), Error> {
    let mut wb = WriteBatch::new();
    wb.put(
        h.id,
        h.families[0].id,
        b"hot",
        b"q",
        None,
        ValueRef::Bytes(&batch.to_le_bytes()),
    )?;
    for j in 0..BATCH {
        wb.put(
            t.id,
            t.families[0].id,
            &row(batch, j),
            b"q",
            None,
            ValueRef::Bytes(&[batch as u8; 200]),
        )?;
    }
    db.commit(wb, Some(Durability::Buffered)).map(|_| ())
}

/// The batches `snap` sees, checked whole and in order: `Ok(n)` for batches `0..n`.
fn scan(db: &Engine, t: &TableInfo, snap: &Snapshot) -> Result<u64, Error> {
    let mut cursor = db.scan(
        snap,
        t.id,
        ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
    )?;
    let mut seen = 0u64;
    while cursor.next_row()? {
        let key = std::str::from_utf8(cursor.row())
            .expect("utf-8 keys")
            .to_owned();
        let (b, j) = key[1..].split_once('-').expect("b<batch>-<j>");
        let (b, j): (u64, u64) = (b.parse().unwrap(), j.parse().unwrap());
        assert_eq!(
            (b, j),
            (seen / BATCH, seen % BATCH),
            "a snapshot saw row {key} after {seen} rows: a batch out of order or partial"
        );
        seen += 1;
    }
    assert_eq!(
        seen % BATCH,
        0,
        "a snapshot saw part of a batch ({seen} rows)"
    );
    Ok(seen / BATCH)
}

/// The hot row's batch number as a latest row read sees it (`None` before the first
/// commit). Row reads fill the row cache (point gets only consult it, D201).
fn hot(db: &Engine, h: &TableInfo) -> Result<Option<u64>, Error> {
    let mut spec = ReadSpec::default();
    spec.versions = 1;
    let mut data = RowData::default();
    if !db.read_row_latest_into(h.id, b"hot", &[h.families[0].id], &spec, &mut data)? {
        return Ok(None);
    }
    let [cell] = &data.cells[..] else {
        panic!("hot row cells {:?}", data.cells.len());
    };
    Ok(Some(match cell.data.value() {
        ValueRef::Bytes(b) => u64::from_le_bytes(b.try_into().expect("8 bytes")),
        other => panic!("hot row value {other:?}"),
    }))
}

/// A reader failure must be typed: expired or busy. Returns 1, for the count.
fn typed(e: &Error, what: &str) -> u32 {
    assert!(
        matches!(e, Error::SnapshotExpired | Error::Busy),
        "{what}: {e:?}"
    );
    1
}

struct Reader {
    process: ProcessId,
    db: Arc<Engine>,
    /// A held snapshot and the batch count it saw.
    held: Option<(Snapshot, u64)>,
    /// The newest hot-row batch its latest reads saw.
    hot: Option<u64>,
}

fn run(seed: u64) {
    let vfs = SimVfs::new(seed);
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut incarnation = 0;
    vfs.enter_process(writer_process(incarnation));
    let mut db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let mut t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut h = db
        .create_table("h", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut committed = 0u64;
    let mut readers: Vec<Reader> = Vec::new();
    let mut next_pid = 100u32;
    // What the seed exercised: reader snapshots scanned, expired or busy reads, reader kills,
    // writer kills.
    let (mut scans, mut expired, mut reader_kills, mut writer_kills) = (0u32, 0u32, 0u32, 0u32);
    let mut no_slot = 0u32;
    let mut hot_reads = 0u32;

    for step in 0..STEPS {
        let at = format!("seed {seed} step {step}");
        let writer = writer_process(incarnation);
        match rng.below(100) {
            0..=44 => {
                vfs.enter_process(writer);
                commit(&db, &t, &h, committed).unwrap_or_else(|e| panic!("{at}: commit: {e:?}"));
                committed += 1;
            }
            45..=49 => {
                vfs.enter_process(writer);
                db.flush().unwrap_or_else(|e| panic!("{at}: flush: {e:?}"));
            }
            50..=51 => {
                vfs.enter_process(writer);
                db.compact(None)
                    .unwrap_or_else(|e| panic!("{at}: compact: {e:?}"));
            }
            52..=57 if readers.len() < 4 => {
                let process = ProcessId {
                    pid: next_pid,
                    start_time: 1,
                };
                next_pid += 1;
                vfs.enter_process(process);
                let mut opened = Engine::open_reader(Path::new(DB), reader_options(&vfs));
                if matches!(opened, Err(Error::NoReaderSlot)) {
                    // Killed readers hold their slots until the writer's next reclamation (a
                    // flush, a compaction or a room stall past the interval): with every slot
                    // held, a new reader is refused at once, as documented. The slots must
                    // come back: past the interval, a flush frees the dead readers' slots
                    // (at most 4 readers live, of 8 slots), and the retry attaches.
                    no_slot += 1;
                    vfs.advance(PAST_INTERVAL);
                    vfs.enter_process(writer);
                    commit(&db, &t, &h, committed)
                        .unwrap_or_else(|e| panic!("{at}: commit: {e:?}"));
                    committed += 1;
                    db.flush().unwrap_or_else(|e| panic!("{at}: flush: {e:?}"));
                    vfs.enter_process(process);
                    opened = Engine::open_reader(Path::new(DB), reader_options(&vfs));
                    assert!(
                        !matches!(opened, Err(Error::NoReaderSlot)),
                        "{at}: killed readers' slots were not reclaimed after a flush"
                    );
                }
                match opened {
                    Ok(db) => readers.push(Reader {
                        process,
                        db,
                        held: None,
                        hot: None,
                    }),
                    Err(e) => expired += typed(&e, &format!("{at}: open_reader")),
                }
            }
            58..=79 if !readers.is_empty() => {
                let i = rng.below(readers.len() as u64) as usize;
                let r = &mut readers[i];
                vfs.enter_process(r.process);
                let rt = r.db.table("t").expect("the reader sees the table");
                match rng.below(4) {
                    // A new snapshot: everything committed so far, whole.
                    0 => match r.db.snapshot() {
                        Ok(snap) => match scan(&r.db, &rt, &snap) {
                            Ok(n) => {
                                assert!(n <= committed, "{at}: saw {n} of {committed}");
                                r.held = Some((snap, n));
                                scans += 1;
                            }
                            Err(e) => expired += typed(&e, &format!("{at}: scan")),
                        },
                        Err(e) => expired += typed(&e, &format!("{at}: snapshot")),
                    },
                    // Rescan the held snapshot: the same batches, or expired.
                    1 => {
                        if let Some((snap, n)) = &r.held {
                            match scan(&r.db, &rt, snap) {
                                Ok(m) => {
                                    assert_eq!(m, *n, "{at}: a held snapshot moved");
                                    scans += 1;
                                }
                                Err(e) => expired += typed(&e, &format!("{at}: rescan")),
                            }
                        }
                    }
                    // The hot row, latest, three times (a miss, a fill, a hit): never older than the
                    // last read, never past the last commit.
                    2 => {
                        let rh = r.db.table("h").expect("the reader sees the table");
                        // A fresh snapshot's batches first: a latest read after it sees at
                        // least its newest batch (a stale cached copy would not).
                        let floor = match r.db.snapshot().and_then(|snap| scan(&r.db, &rt, &snap)) {
                            Ok(n) => n.checked_sub(1),
                            Err(e) => {
                                expired += typed(&e, &format!("{at}: hot floor"));
                                None
                            }
                        };
                        r.hot = r.hot.max(floor);
                        for _ in 0..3 {
                            match hot(&r.db, &rh) {
                                Ok(seen) => {
                                    assert!(
                                        seen >= r.hot,
                                        "{at}: hot row went back from {:?} to {seen:?}",
                                        r.hot
                                    );
                                    assert!(
                                        seen.is_none_or(|b| b < committed),
                                        "{at}: hot row {seen:?} past {committed} batches"
                                    );
                                    r.hot = seen;
                                    hot_reads += 1;
                                }
                                Err(e) => expired += typed(&e, &format!("{at}: hot row")),
                            }
                        }
                    }
                    _ => r.held = None,
                }
            }
            80..=84 if !readers.is_empty() => {
                // Kill a reader process: its handles are forgotten, never dropped.
                let i = rng.below(readers.len() as u64) as usize;
                let r = readers.swap_remove(i);
                vfs.crash_process(r.process);
                std::mem::forget(r);
                reader_kills += 1;
            }
            85..=89 => vfs.advance(PAST_INTERVAL),
            90..=92 => {
                // Kill the writer process and start a new one.
                writer_kills += 1;
                vfs.crash_process(writer);
                drop(t);
                drop(db);
                incarnation += 1;
                vfs.enter_process(writer_process(incarnation));
                db = Engine::open(Path::new(DB), options(&vfs))
                    .unwrap_or_else(|e| panic!("{at}: reopen: {e:?}"));
                t = db.table("t").unwrap();
                h = db.table("h").unwrap();
                let snap = db.snapshot().unwrap();
                let n = scan(&db, &t, &snap).unwrap_or_else(|e| panic!("{at}: {e:?}"));
                assert_eq!(n, committed, "{at}: the reopened writer lost batches");
            }
            _ => {}
        }
    }

    // Every surviving reader's next snapshot sees every batch, and its row cache served
    // some of the hot-row reads.
    let hits: u64 = readers
        .iter()
        .map(|r| {
            vfs.enter_process(r.process);
            r.db.row_cache_stats().hits
        })
        .sum();
    for r in &mut readers {
        vfs.enter_process(r.process);
        let rt = r.db.table("t").unwrap();
        let n =
            r.db.snapshot()
                .and_then(|s| scan(&r.db, &rt, &s))
                .unwrap_or_else(|e| panic!("seed {seed}: final snapshot: {e:?}"));
        assert_eq!(n, committed, "seed {seed}: a reader's final snapshot");
    }
    // Kill them all: past the interval, the writer's next reclamation frees every slot.
    for r in readers.drain(..) {
        vfs.crash_process(r.process);
        std::mem::forget(r);
    }
    vfs.enter_process(writer_process(incarnation));
    vfs.advance(PAST_INTERVAL);
    commit(&db, &t, &h, committed).unwrap();
    db.flush().unwrap();
    assert_eq!(
        db.reader_pin_and_view().0,
        None,
        "seed {seed}: a dead reader still pins"
    );
    drop(t);
    drop(h);
    db.close().unwrap();
    eprintln!(
        "seed {seed}: {committed} batches, {scans} reader scans, {expired} expired or busy, \
         {reader_kills} reader kills, {writer_kills} writer kills, {no_slot} refused for slots, \
         {hot_reads} hot-row reads ({hits} row cache hits by the surviving readers)"
    );
}

#[test]
fn readers_and_writer_killed_at_seeded_points_keep_every_invariant() {
    for seed in seeds() {
        run(seed);
    }
}
