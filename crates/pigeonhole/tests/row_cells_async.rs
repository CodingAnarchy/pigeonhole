//! #1560 PR 1: an async walk over one wide row (`RowCells::next_async`, `poll_next`,
//! `poll_seek`) returns exactly what the sync walk does, from a cold block cache, and fetches
//! the blocks it reads instead of reading them synchronously (D196 within a row). The row is
//! a text index's hub posting row in miniature: many SST blocks across levels, versions,
//! deletes, a memtable on top, and a second family.
#![cfg(feature = "async")]

use std::task::Poll;

use pigeonhole::doc_support::block_on;
use pigeonhole::{Family, Options, Pigeonhole, RowCells, Snapshot, Table};

/// (family, qualifier, timestamp, value) per cell, in walk order.
type Cell = (String, Vec<u8>, u64, Vec<u8>);

const HUB: &[u8] = b"hub";
const POSTINGS: u32 = 20_000;

fn qual(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// The hub row in four flushed rounds (compacted under a fifth), with overwrites, deletes and
/// a second family, then a memtable on top; reopened, so the block cache starts cold.
fn build() -> (pigeonhole::doc_support::TempDir, Pigeonhole, Table) {
    let dir = pigeonhole::doc_support::temp_dir();
    let opts = || Options::default().shards(1);
    let db = Pigeonhole::open(dir.join("r.phdb"), opts()).expect("open");
    let t = db
        .table("t")
        .expect("table")
        .family("post", Family::default().max_versions(0))
        .family("side", Family::default())
        .create_if_missing()
        .expect("create");
    for round in 0u32..4 {
        let mut m = t.mutate(HUB);
        for i in (round..POSTINGS).step_by(4) {
            m = m.put("post", &qual(i), &(u64::from(i) * 7).to_le_bytes());
        }
        m.commit().expect("commit");
        db.flush().expect("flush");
    }
    db.compact().expect("compact");
    // Newer versions and deletes over the compacted level, flushed.
    let mut m = t.mutate(HUB);
    for i in (0..POSTINGS).step_by(97) {
        m = m.put("post", &qual(i), b"newer");
    }
    for i in (5..POSTINGS).step_by(131) {
        m = m.delete_column("post", &qual(i));
    }
    for i in (0..2_000u32).step_by(3) {
        m = m.put("side", &qual(i), b"s");
    }
    m.commit().expect("commit");
    db.flush().expect("flush");
    // A memtable on top.
    t.mutate(HUB)
        .put("post", &qual(POSTINGS + 1), b"mem")
        .put("side", &qual(1), b"mem")
        .commit()
        .expect("commit");
    db.close().expect("close");
    let db = Pigeonhole::open(dir.join("r.phdb"), opts()).expect("reopen");
    let t = db.table("t").expect("table").open().expect("open table");
    (dir, db, t)
}

fn entry(e: &pigeonhole::CellEntry<'_>) -> Cell {
    (
        e.family.to_owned(),
        e.qualifier.to_vec(),
        e.cell.timestamp(),
        e.cell.value().to_vec(),
    )
}

fn sync_walk(mut cells: RowCells<'_>) -> Vec<Cell> {
    let mut out = Vec::new();
    while let Some(e) = cells.next().expect("next") {
        out.push(entry(&e));
    }
    out
}

/// Polls `f` to completion, counting the `Pending`s it returned.
fn drive<T>(mut f: impl FnMut(&mut std::task::Context<'_>) -> Poll<T>) -> (T, u32) {
    let mut pending = 0;
    let out = block_on(std::future::poll_fn(|cx| match f(cx) {
        Poll::Pending => {
            pending += 1;
            Poll::Pending
        }
        Poll::Ready(v) => Poll::Ready(v),
    }));
    (out, pending)
}

fn sync_reads(db: &Pigeonhole) -> u64 {
    db.engine_metrics().async_sync_reads
}

fn walk_all(s: &Snapshot, t: &Table, versions: u32) -> Vec<Cell> {
    sync_walk(
        t.row(HUB)
            .snapshot(s)
            .versions(versions)
            .cells()
            .expect("cells"),
    )
}

/// A cold walk by `poll_next` returns every cell the sync walk does, once, in order, with
/// its blocks fetched ahead: `Pending`s, and no synchronous read.
#[test]
fn a_cold_async_walk_returns_what_the_sync_one_returns() {
    let (_dir, db, t) = build();
    let s = db.snapshot().expect("snapshot");
    for versions in [1, 0] {
        let before = sync_reads(&db);
        let mut cells = t
            .row(HUB)
            .snapshot(&s)
            .versions(versions)
            .cells()
            .expect("cells");
        let mut got = Vec::new();
        let mut pendings = 0;
        loop {
            let (step, pending) = drive(|cx| {
                cells
                    .poll_next(cx)
                    .map(|r| r.expect("poll_next").map(|e| entry(&e)))
            });
            pendings += pending;
            match step {
                Some(c) => got.push(c),
                None => break,
            }
        }
        let reads = sync_reads(&db) - before;
        assert_eq!(got, walk_all(&s, &t, versions), "versions {versions}");
        assert_eq!(reads, 0, "synchronous reads in an async walk");
        if versions == 1 {
            // The first walk started cold: its blocks were fetched, so it was `Pending`.
            assert!(pendings > 0);
        }
    }
    // `next_async`, warm now.
    let mut cells = t.row(HUB).snapshot(&s).cells().expect("cells");
    let mut got = Vec::new();
    block_on(async {
        while let Some(e) = cells.next_async().await.expect("next_async") {
            got.push(entry(&e));
        }
    });
    assert_eq!(got, walk_all(&s, &t, 1));
    db.close().expect("close");
}

/// What a seek to (`family`, `q`) returns from the walk's position `at` (the index of its next
/// cell in `all`): the first cell at or after the column, in the walk's family order, never
/// before `at`.
fn expected_seek(all: &[Cell], at: usize, family: &str, q: &[u8], order: &[&str]) -> usize {
    let rank = |f: &str| order.iter().position(|o| *o == f).expect("a walked family");
    let want = (rank(family), q);
    (at..all.len())
        .find(|&j| (rank(&all[j].0), all[j].1.as_slice()) >= want)
        .unwrap_or(all.len())
}

/// Seeds for the seek mixes: printed on failure.
const SEEDS: [u64; 3] = [1, 0x5eed, 1560];

/// Seeks mixed with steps, from a cold cache: each lands where the sync walk's cells say,
/// never back, and no block is read synchronously.
#[test]
fn cold_seeks_land_where_the_sync_walk_says() {
    for seed in SEEDS {
        let (dir, db, t) = build();
        let all = walk_all(&db.snapshot().expect("snapshot"), &t, 1);
        // That walk warmed the cache: reopen, so the seeks start cold.
        drop(t);
        db.close().expect("close");
        let db =
            Pigeonhole::open(dir.join("r.phdb"), Options::default().shards(1)).expect("reopen");
        let t = db.table("t").expect("table").open().expect("open table");
        let s = db.snapshot().expect("snapshot");
        let order = ["post", "side"];
        let before = sync_reads(&db);
        let mut cells = t.row(HUB).snapshot(&s).cells().expect("cells");
        let mut rng = seed;
        let mut next = || {
            rng = rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            rng >> 33
        };
        let mut at = 0;
        let mut q = 0u32;
        let mut pendings = 0;
        while at < all.len() {
            let r = next();
            let (got, pending) = if r % 4 == 0 {
                drive(|cx| {
                    cells
                        .poll_next(cx)
                        .map(|r| r.expect("poll_next").map(|e| entry(&e)))
                })
            } else {
                // Mostly forward skips through `post`, sometimes into `side`, sometimes back
                // (which must not move back).
                let (family, target) = match r % 16 {
                    1 => ("side", (r as u32 >> 8) % 2_100),
                    2 => ("post", q.saturating_sub(500)),
                    _ => {
                        q += (r as u32 >> 8) % 600;
                        ("post", q)
                    }
                };
                let want = expected_seek(&all, at, family, &qual(target), &order);
                let got = drive(|cx| {
                    cells
                        .poll_seek(cx, family, &qual(target))
                        .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
                });
                assert_eq!(
                    got.0.as_ref(),
                    all.get(want),
                    "seed {seed}: seek to {family} {target} from cell {at}"
                );
                at = want;
                got
            };
            pendings += pending;
            match got {
                Some(c) => {
                    assert_eq!(Some(&c), all.get(at), "seed {seed}: cell {at}");
                    at += 1;
                }
                None => {
                    assert_eq!(at, all.len(), "seed {seed}: ended early at {at}");
                }
            }
        }
        assert_eq!(
            sync_reads(&db) - before,
            0,
            "seed {seed}: synchronous reads"
        );
        assert!(pendings > 0, "seed {seed}: a cold walk fetched nothing");
        db.close().expect("close");
    }
}

/// A walk resumed after a position runs two scans (the position's family, then the later
/// ones): a seek into the later family crosses to the second scan, a seek back into the
/// first family's lands on the next cell, and a family the table lacks is refused.
#[test]
fn seeks_cross_the_scans_of_a_resumed_walk() {
    let (_dir, db, t) = build();
    let s = db.snapshot().expect("snapshot");
    let all = walk_all(&s, &t, 1);
    // Resume after the 100th cell, which is in `post`.
    let mut sync = t.row(HUB).snapshot(&s).cells().expect("cells");
    for _ in 0..100 {
        sync.next().expect("next").expect("a cell");
    }
    let pos = sync.position().expect("position");
    let rest = &all[100..];
    let mut cells = t
        .row(HUB)
        .snapshot(&s)
        .resume_after(&pos)
        .cells()
        .expect("cells");
    // A seek into `side` (read by the second scan).
    let (got, _) = drive(|cx| {
        cells
            .poll_seek(cx, "side", &qual(900))
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    let at = expected_seek(rest, 0, "side", &qual(900), &["post", "side"]);
    assert_eq!(got.as_ref(), rest.get(at));
    // Back into `post`: the next cell, not a move back.
    let (got, _) = drive(|cx| {
        cells
            .poll_seek(cx, "post", &qual(0))
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    assert_eq!(got.as_ref(), rest.get(at + 1));
    let (err, _) = drive(|cx| cells.poll_seek(cx, "nope", b"q").map(|r| r.err()));
    assert_eq!(
        err.expect("refused").code(),
        pigeonhole::ErrorCode::FamilyNotFound
    );
    db.close().expect("close");
}

/// A walk that starts sync and goes on async reads on from where it is.
#[test]
fn a_walk_goes_on_async_after_sync_steps() {
    let (_dir, db, t) = build();
    let s = db.snapshot().expect("snapshot");
    let all = walk_all(&s, &t, 1);
    let mut cells = t.row(HUB).snapshot(&s).cells().expect("cells");
    let mut got = Vec::new();
    for _ in 0..1_000 {
        got.push(entry(&cells.next().expect("next").expect("a cell")));
    }
    block_on(async {
        while let Some(e) = cells.next_async().await.expect("next_async") {
            got.push(entry(&e));
        }
    });
    assert_eq!(got, all);
    db.close().expect("close");
}
