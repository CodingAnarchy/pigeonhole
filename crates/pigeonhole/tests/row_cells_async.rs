//! #1560 PR 1: an async walk over one wide row (`RowCells::next_async`, `poll_next`,
//! `poll_seek`) returns exactly what the sync walk does, from a cold block cache, and fetches
//! the blocks it reads instead of reading them synchronously (D196 within a row). The row is
//! a text index's hub posting row in miniature: many SST blocks across levels, versions,
//! deletes, a memtable on top, and a second family.
//!
//! The store is on the simulated device. An async walk runs with its I/O deferred (completed
//! after each `Pending`), so no read completes at submit: on real files a cache-only miss
//! whose read is done at submit (a D292 page-cache copy, or a fast backend) is read in place
//! and counted, which made "no synchronous read" depend on timing (#1625's macOS run).
#![cfg(feature = "async")]

use std::sync::Arc;
use std::task::Poll;

use pigeonhole::doc_support::block_on;
use pigeonhole::{Family, Options, Pigeonhole, RowCells, Snapshot, Table};
use pigeonhole_io::sim::SimVfs;

/// (family, qualifier, timestamp, value) per cell, in walk order.
type Cell = (String, Vec<u8>, u64, Vec<u8>);

const HUB: &[u8] = b"hub";
const POSTINGS: u32 = 20_000;

fn qual(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

const PATH: &str = "/db/r.phdb";

fn opts(vfs: &Arc<SimVfs>) -> Options {
    Options::default().vfs(Arc::clone(vfs) as _).shards(1)
}

/// Closes `db` once the I/O a test left in flight is done.
fn close(vfs: &SimVfs, db: Pigeonhole) {
    vfs.set_deferred_io(false);
    vfs.complete_all_io();
    db.close().expect("close");
}

/// The hub row in four flushed rounds (compacted under a fifth), with overwrites, deletes and
/// a second family, then a memtable on top; reopened, so the block cache starts cold.
fn build() -> (Arc<SimVfs>, Pigeonhole, Table) {
    let vfs = SimVfs::new(1560);
    let db = Pigeonhole::open(PATH, opts(&vfs)).expect("open");
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
    let (db, t) = reopen(&vfs, db);
    (vfs, db, t)
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

/// Polls `f` to completion with the device's I/O deferred, completing it after each
/// `Pending`; returns how many `Pending`s there were.
fn drive<T>(vfs: &SimVfs, mut f: impl FnMut(&mut std::task::Context<'_>) -> Poll<T>) -> (T, u32) {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    vfs.set_deferred_io(true);
    let mut pending = 0;
    let out = loop {
        match f(&mut cx) {
            Poll::Ready(v) => break v,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 100_000, "never resolved");
                assert!(vfs.io_in_flight() > 0, "pending with no I/O in flight");
                vfs.complete_all_io();
            }
        }
    };
    vfs.set_deferred_io(false);
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
    let (vfs, db, t) = build();
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
            let (step, pending) = drive(&vfs, |cx| {
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
    close(&vfs, db);
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
        let (vfs, db, t) = build();
        let all = walk_all(&db.snapshot().expect("snapshot"), &t, 1);
        // That walk warmed the cache: reopen, so the seeks start cold.
        drop(t);
        close(&vfs, db);
        let db = Pigeonhole::open(PATH, opts(&vfs)).expect("reopen");
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
                drive(&vfs, |cx| {
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
                let got = drive(&vfs, |cx| {
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
        close(&vfs, db);
    }
}

/// A walk resumed after a position runs two scans (the position's family, then the later
/// ones): a seek into the later family crosses to the second scan, a seek back into the
/// first family's lands on the next cell, and a family the table lacks is refused.
#[test]
fn seeks_cross_the_scans_of_a_resumed_walk() {
    let (vfs, db, t) = build();
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
    let (got, _) = drive(&vfs, |cx| {
        cells
            .poll_seek(cx, "side", &qual(900))
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    let at = expected_seek(rest, 0, "side", &qual(900), &["post", "side"]);
    assert_eq!(got.as_ref(), rest.get(at));
    // Back into `post`: the next cell, not a move back.
    let (got, _) = drive(&vfs, |cx| {
        cells
            .poll_seek(cx, "post", &qual(0))
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    assert_eq!(got.as_ref(), rest.get(at + 1));
    let (err, _) = drive(&vfs, |cx| {
        cells.poll_seek(cx, "nope", b"q").map(|r| r.err())
    });
    assert_eq!(
        err.expect("refused").code(),
        pigeonhole::ErrorCode::FamilyNotFound
    );
    close(&vfs, db);
}

/// A walk that starts sync and goes on async reads on from where it is.
#[test]
fn a_walk_goes_on_async_after_sync_steps() {
    let (vfs, db, t) = build();
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
    close(&vfs, db);
}

/// Polls `f` once with a waker that does nothing: whether it was `Pending`, or its value.
/// With its I/O deferred, so a fetch it starts stays in flight.
fn poll_once<T>(vfs: &SimVfs, f: impl FnOnce(&mut std::task::Context<'_>) -> Poll<T>) -> Option<T> {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    vfs.set_deferred_io(true);
    let r = match f(&mut cx) {
        Poll::Pending => None,
        Poll::Ready(v) => Some(v),
    };
    vfs.set_deferred_io(false);
    r
}

fn reopen(vfs: &Arc<SimVfs>, db: Pigeonhole) -> (Pigeonhole, Table) {
    close(vfs, db);
    let db = Pigeonhole::open(PATH, opts(vfs)).expect("reopen");
    let t = db.table("t").expect("table").open().expect("open table");
    (db, t)
}

/// ai1249's R1 on #1625: a sync `next` after a seek left `Pending` finishes the seek and
/// returns its cell, whether the seek was waiting on the row's positioning (before the scan
/// held the row) or inside the row.
#[test]
fn a_sync_step_finishes_a_pending_seek() {
    let (vfs, db, t) = build();
    let all = walk_all(&db.snapshot().expect("snapshot"), &t, 1);
    let order = ["post", "side"];
    // Waiting on the row's positioning: the first poll of a cold walk.
    drop(t);
    let (db, t) = reopen(&vfs, db);
    let mut cells = t.row(HUB).cells().expect("cells");
    let target = qual(12_345);
    assert!(
        poll_once(&vfs, |cx| cells
            .poll_seek(cx, "post", &target)
            .map(|r| r.is_ok()))
        .is_none(),
        "a cold walk's first poll fetches"
    );
    let want = expected_seek(&all, 0, "post", &target, &order);
    assert_eq!(
        cells.next().expect("next").map(|e| entry(&e)).as_ref(),
        all.get(want)
    );
    // Inside the row: one cell read first, then a seek far into it.
    drop(cells);
    drop(t);
    let (db, t) = reopen(&vfs, db);
    let mut cells = t.row(HUB).cells().expect("cells");
    let (first, _) = drive(&vfs, |cx| {
        cells
            .poll_next(cx)
            .map(|r| r.expect("poll_next").map(|e| entry(&e)))
    });
    assert_eq!(first.as_ref(), all.first());
    let target = qual(15_000);
    assert!(
        poll_once(&vfs, |cx| cells
            .poll_seek(cx, "post", &target)
            .map(|r| r.is_ok()))
        .is_none(),
        "a seek into an uncached block fetches"
    );
    let want = expected_seek(&all, 1, "post", &target, &order);
    assert_eq!(
        cells.next().expect("next").map(|e| entry(&e)).as_ref(),
        all.get(want)
    );
    assert_eq!(
        cells.next().expect("next").map(|e| entry(&e)).as_ref(),
        all.get(want + 1)
    );
    drop(cells);
    drop(t);
    close(&vfs, db);
}

/// ai1249's R2 on #1625: a seek replacing one left `Pending` keeps the later column, so the
/// walk never moves back: an earlier qualifier, or an earlier family, lands where the pending
/// seek would; a later family replaces it.
#[test]
fn a_replacing_seek_never_moves_back() {
    let (vfs, db, t) = build();
    let all = walk_all(&db.snapshot().expect("snapshot"), &t, 1);
    let order = ["post", "side"];
    drop(t);
    let (db, t) = reopen(&vfs, db);
    let mut cells = t.row(HUB).cells().expect("cells");
    let (first, _) = drive(&vfs, |cx| {
        cells
            .poll_next(cx)
            .map(|r| r.expect("poll_next").map(|e| entry(&e)))
    });
    assert_eq!(first.as_ref(), all.first());
    let far = qual(16_000);
    assert!(
        poll_once(&vfs, |cx| cells
            .poll_seek(cx, "post", &far)
            .map(|r| r.is_ok()))
        .is_none()
    );
    // An earlier qualifier: the pending seek's cell.
    let (got, _) = drive(&vfs, |cx| {
        cells
            .poll_seek(cx, "post", &qual(10))
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    let at = expected_seek(&all, 1, "post", &far, &order);
    assert_eq!(got.as_ref(), all.get(at));
    // A later family replaces a pending seek; an earlier family then keeps it.
    let farther = qual(18_000);
    let pending = poll_once(&vfs, |cx| {
        cells.poll_seek(cx, "post", &farther).map(|r| r.is_ok())
    });
    let (got, _) = drive(&vfs, |cx| {
        cells
            .poll_seek(cx, "side", &qual(600))
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    let side = expected_seek(&all, at + 1, "side", &qual(600), &order);
    assert_eq!(
        got.as_ref(),
        all.get(side),
        "pending: {}",
        pending.is_none()
    );
    let (got, _) = drive(&vfs, |cx| {
        cells
            .poll_seek(cx, "post", &qual(19_000))
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    assert_eq!(
        got.as_ref(),
        all.get(side + 1),
        "never back into an earlier family"
    );
    drop(cells);
    drop(t);
    close(&vfs, db);
}

/// ai1249's N1 on #1625: a seek to a family of the table the walk doesn't read is the next
/// cell.
#[test]
fn a_seek_to_an_unread_family_is_the_next_cell() {
    let (vfs, db, t) = build();
    let s = db.snapshot().expect("snapshot");
    let post = sync_walk(
        t.row(HUB)
            .snapshot(&s)
            .family("post")
            .cells()
            .expect("cells"),
    );
    let mut cells = t
        .row(HUB)
        .snapshot(&s)
        .family("post")
        .cells()
        .expect("cells");
    let (got, _) = drive(&vfs, |cx| {
        cells
            .poll_next(cx)
            .map(|r| r.expect("poll_next").map(|e| entry(&e)))
    });
    assert_eq!(got.as_ref(), post.first());
    let (got, _) = drive(&vfs, |cx| {
        cells
            .poll_seek(cx, "side", &qual(0))
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    assert_eq!(got.as_ref(), post.get(1));
    drop(cells);
    close(&vfs, db);
}

/// ai1249's N2 on #1625: `next_async`'s future is `Send` (the Python bridge polls it on its
/// own thread, PR 4).
#[test]
fn next_async_is_send() {
    fn send<T: Send>(_: &T) {}
    let (vfs, db, t) = build();
    let mut cells = t.row(HUB).cells().expect("cells");
    let fut = cells.next_async();
    send(&fut);
    drop(fut);
    drop(cells);
    close(&vfs, db);
}

/// ai1249's R3 on #1625: a seek to an unread family is a plain step, so it never replaces a
/// pending seek: a sync step after both finishes the pending one.
#[test]
fn an_unread_family_seek_keeps_a_pending_one() {
    let (vfs, db, t) = build();
    let s = db.snapshot().expect("snapshot");
    let post = sync_walk(
        t.row(HUB)
            .snapshot(&s)
            .family("post")
            .cells()
            .expect("cells"),
    );
    drop(s);
    drop(t);
    let (db, t) = reopen(&vfs, db);
    let mut cells = t.row(HUB).family("post").cells().expect("cells");
    let (first, _) = drive(&vfs, |cx| {
        cells
            .poll_next(cx)
            .map(|r| r.expect("poll_next").map(|e| entry(&e)))
    });
    assert_eq!(first.as_ref(), post.first());
    let target = qual(15_000);
    assert!(
        poll_once(&vfs, |cx| cells
            .poll_seek(cx, "post", &target)
            .map(|r| r.is_ok()))
        .is_none(),
        "a seek into an uncached block fetches"
    );
    // `side` isn't read by this walk; a pending poll of it leaves `post`'s seek in place.
    let _ = poll_once(&vfs, |cx| {
        cells.poll_seek(cx, "side", &qual(0)).map(|r| r.is_ok())
    });
    let want = expected_seek(&post, 1, "post", &target, &["post"]);
    assert_eq!(
        cells.next().expect("next").map(|e| entry(&e)).as_ref(),
        post.get(want)
    );
    drop(cells);
    drop(t);
    close(&vfs, db);
}

/// ai1249's N5 on #1625, the reverse order: a seek to an unread family left `Pending` is a
/// plain step, so a seek after it lands at or after its own column.
#[test]
fn a_pending_unread_family_seek_is_a_plain_step() {
    let (vfs, db, t) = build();
    let s = db.snapshot().expect("snapshot");
    let post = sync_walk(
        t.row(HUB)
            .snapshot(&s)
            .family("post")
            .cells()
            .expect("cells"),
    );
    drop(s);
    drop(t);
    let (db, t) = reopen(&vfs, db);
    let mut cells = t.row(HUB).family("post").cells().expect("cells");
    // `side` isn't read by this walk; a cold walk's first poll fetches.
    assert!(
        poll_once(&vfs, |cx| cells
            .poll_seek(cx, "side", &qual(0))
            .map(|r| r.is_ok()))
        .is_none(),
        "a cold walk's first poll fetches"
    );
    let target = qual(15_000);
    let (got, _) = drive(&vfs, |cx| {
        cells
            .poll_seek(cx, "post", &target)
            .map(|r| r.expect("poll_seek").map(|e| entry(&e)))
    });
    let want = expected_seek(&post, 0, "post", &target, &["post"]);
    assert_eq!(got.as_ref(), post.get(want));
    drop(cells);
    drop(t);
    close(&vfs, db);
}
