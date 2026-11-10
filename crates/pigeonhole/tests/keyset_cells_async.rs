//! #1652: an async read over a key set, or a scan, that walks a row longer than a block reads
//! that row's later blocks asynchronously too (D196 within a row, `poll_cell_ready`), and
//! returns what its sync form returns. Each of the six async readers that step a row's cells
//! is covered, from a cold block cache:
//!
//! - the multi-get (`KeyRows::next_async`);
//! - a traversal (`Traverse::run_async`) through a hub;
//! - a nearest-neighbour search over keys and over a scan (`Nearest::run_async`);
//! - a bounded read over keys and over a scan (`read_within_async`, the async reduce).
//!
//! The long rows span many blocks across levels, with newer versions flushed over them and a
//! newer partial copy in the memtable, so the merge's shadowed versions in older SSTs step
//! too (perf287's point 1). The store is on the simulated device with its I/O deferred, so no
//! read completes at submit, and the count of synchronous reads is exact: 0.
#![cfg(feature = "async")]

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole::{Element, Family, KeySet, Metric, Options, Pigeonhole, Table, VectorSpec};
use pigeonhole_io::sim::SimVfs;

const PATH: &str = "/db/k.phdb";
const HUB: &[u8] = b"hub";
/// Edges of the hub, across many blocks.
const EDGES: u32 = 6_000;
/// List rows of vectors, as an IVF index's are, and vectors per list.
const LISTS: u32 = 4;
const PER_LIST: u32 = 1_500;
const DIM: usize = 8;

fn opts(vfs: &Arc<SimVfs>) -> Options {
    Options::default().vfs(Arc::clone(vfs) as _).shards(1)
}

fn edge(i: u32) -> Vec<u8> {
    format!("v:{i:06}").into_bytes()
}

fn list(l: u32) -> Vec<u8> {
    format!("list:{l}").into_bytes()
}

fn vector(l: u32, i: u32) -> Vec<f32> {
    (0..DIM)
        .map(|d| ((l * 31 + i * 7 + d as u32 * 13) % 97) as f32 / 97.0 - 0.5)
        .collect()
}

/// The hub's edges and the list rows, in two flushed rounds compacted under newer versions,
/// then reopened (a cold cache) with a newer partial copy of each long row in the memtable.
fn build(seed: u64) -> (Arc<SimVfs>, Pigeonhole, Table) {
    let vfs = SimVfs::new(seed);
    let db = Pigeonhole::open(PATH, opts(&vfs)).expect("open");
    let spec = VectorSpec::new(DIM as u32, Element::F32, Metric::L2).expect("spec");
    let t = db
        .table("g")
        .expect("table")
        .family("out", Family::default())
        .family("vec", Family::vector(spec))
        .create_if_missing()
        .expect("create");
    for round in 0u32..2 {
        let mut m = t.mutate(HUB);
        for i in (round..EDGES).step_by(2) {
            m = m.put("out", &edge(i), &i.to_le_bytes());
        }
        m.commit().expect("commit");
        for l in 0..LISTS {
            let mut m = t.mutate(&list(l));
            for i in (round..PER_LIST).step_by(2) {
                m = m.put_vector_f32("vec", &i.to_be_bytes(), &vector(l, i));
            }
            m.commit().expect("commit");
        }
        // A few small rows the traversal reaches.
        for i in (0..EDGES).step_by(500) {
            t.mutate(&edge(i))
                .put("out", &edge(i + 1), b"")
                .commit()
                .expect("commit");
        }
        db.flush().expect("flush");
    }
    db.compact().expect("compact");
    // Newer versions over the compacted level, flushed.
    let mut m = t.mutate(HUB);
    for i in (0..EDGES).step_by(37) {
        m = m.put("out", &edge(i), b"newer");
    }
    m.commit().expect("commit");
    db.flush().expect("flush");
    drop(t);
    db.close().expect("close");
    let db = Pigeonhole::open(PATH, opts(&vfs)).expect("reopen");
    let t = db.table("g").expect("table").open().expect("open table");
    // A newer partial copy of each long row in the memtable: the merge steps the SSTs'
    // shadowed versions past it.
    let mut m = t.mutate(HUB);
    for i in (3..EDGES).step_by(53) {
        m = m.put("out", &edge(i), b"mem");
    }
    m.commit().expect("commit");
    for l in 0..LISTS {
        let mut m = t.mutate(&list(l));
        for i in (5..PER_LIST).step_by(61) {
            m = m.put_vector_f32("vec", &i.to_be_bytes(), &vector(l + 1, i));
        }
        m.commit().expect("commit");
    }
    (vfs, db, t)
}

fn close(vfs: &SimVfs, db: Pigeonhole) {
    vfs.set_deferred_io(false);
    vfs.complete_all_io();
    db.close().expect("close");
}

/// Polls `fut` to completion with the device's I/O deferred, completing it after each
/// `Pending`: its output and how many `Pending`s there were.
fn drive<F: Future>(vfs: &SimVfs, fut: F) -> (F::Output, u32) {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    vfs.set_deferred_io(true);
    let mut pending = 0;
    let out = loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => break v,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 1_000_000, "never resolved");
                vfs.complete_all_io();
            }
        }
    };
    vfs.set_deferred_io(false);
    (out, pending)
}

/// Runs `fut` cold and checks it read nothing synchronously and fetched something.
fn cold<F: Future>(vfs: &SimVfs, db: &Pigeonhole, what: &str, fut: F) -> F::Output {
    let before = db.engine_metrics().async_sync_reads;
    let (out, pending) = drive(vfs, fut);
    let reads = db.engine_metrics().async_sync_reads - before;
    assert_eq!(reads, 0, "{what}: synchronous reads");
    assert!(pending > 0, "{what}: nothing was fetched");
    out
}

#[test]
fn a_multi_get_of_long_rows_reads_them_asynchronously() {
    let (vfs, db, t) = build(1);
    let keys = KeySet::from_unsorted([HUB.to_vec(), list(1), edge(500), list(3)]);
    let got = cold(&vfs, &db, "multi-get", async {
        let mut rows = t.keys(&keys).read().expect("read");
        let mut out = Vec::new();
        while let Some((i, row)) = rows.next_async().await.expect("next_async") {
            out.push((i, format!("{:?}", row.to_owned())));
        }
        out
    });
    let mut want = Vec::new();
    let mut rows = t.keys(&keys).read().expect("read");
    while let Some((i, row)) = rows.next().expect("next") {
        want.push((i, format!("{:?}", row.to_owned())));
    }
    assert_eq!(got, want);
    assert!(got.iter().any(|(i, _)| keys.get(*i) == HUB));
    drop(t);
    close(&vfs, db);
}

#[test]
fn a_traversal_through_a_hub_reads_its_row_asynchronously() {
    let (vfs, db, t) = build(2);
    let seeds = KeySet::from_unsorted([HUB]);
    let traverse = || t.traverse(&seeds).family("out").hops(2);
    let got = cold(&vfs, &db, "traversal", traverse().run_async()).expect("run_async");
    let want = traverse().run().expect("run");
    assert_eq!(format!("{got:?}"), format!("{want:?}"));
    assert!(got.hops()[0].nodes().len() > 1_000);
    drop(t);
    close(&vfs, db);
}

#[test]
fn a_nearest_search_over_list_keys_reads_them_asynchronously() {
    let (vfs, db, t) = build(3);
    let keys = KeySet::from_unsorted((0..LISTS).map(list));
    let q = vector(2, 700);
    let search = || t.keys(&keys).family("vec").nearest_f32(&q, 25);
    let got = cold(&vfs, &db, "nearest over keys", search().run_async()).expect("run_async");
    assert_eq!(got, search().run().expect("run"));
    drop(t);
    close(&vfs, db);
}

#[test]
fn a_nearest_search_over_a_scan_reads_long_rows_asynchronously() {
    let (vfs, db, t) = build(4);
    let q = vector(1, 42);
    let search = || t.scan_prefix(b"list:").family("vec").nearest_f32(&q, 25);
    let got = cold(&vfs, &db, "nearest over a scan", search().run_async()).expect("run_async");
    assert_eq!(got, search().run().expect("run"));
    drop(t);
    close(&vfs, db);
}

#[test]
fn a_bounded_read_over_keys_reads_long_rows_asynchronously() {
    let (vfs, db, t) = build(5);
    let keys = KeySet::from_unsorted([HUB.to_vec(), list(0), list(2)]);
    let read = || t.keys(&keys);
    let got = cold(
        &vfs,
        &db,
        "read_within over keys",
        read().read_within_async(1 << 30),
    )
    .expect("read_within_async");
    let want = read().read_within(1 << 30).expect("read_within");
    assert_eq!(format!("{got:?}"), format!("{want:?}"));
    drop(t);
    close(&vfs, db);
}

#[test]
fn a_bounded_read_over_a_scan_reads_long_rows_asynchronously() {
    let (vfs, db, t) = build(6);
    let read = || t.scan_prefix(b"");
    let got = cold(
        &vfs,
        &db,
        "read_within over a scan",
        read().read_within_async(1 << 30),
    )
    .expect("read_within_async");
    let want = read().read_within(1 << 30).expect("read_within");
    assert_eq!(format!("{got:?}"), format!("{want:?}"));
    drop(t);
    close(&vfs, db);
}

/// perf287's point 4: a sync `next` after an async fill stopped mid-row finishes that row
/// (no key is lost), and the rest go on.
#[test]
fn a_sync_next_after_an_async_fill_stopped_mid_row_finishes_it() {
    let (vfs, db, t) = build(7);
    let keys = KeySet::from_unsorted([HUB.to_vec(), list(1)]);
    let mut want = Vec::new();
    let mut rows = t.keys(&keys).read().expect("read");
    while let Some((i, row)) = rows.next().expect("next") {
        want.push((i, format!("{:?}", row.to_owned())));
    }
    // A fresh cold store: poll once (the fill stops at a fetch), then step synchronously.
    drop(rows);
    drop(t);
    close(&vfs, db);
    let (vfs, db, t) = build(7);
    let mut rows = t.keys(&keys).read().expect("read");
    let mut got = Vec::new();
    // A few polls, each fetching one of the hub's blocks: the fill stops mid-row (the hub
    // spans far more blocks than this).
    vfs.set_deferred_io(true);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..3 {
        match rows.poll_next(&mut cx) {
            Poll::Pending => vfs.complete_all_io(),
            Poll::Ready(r) => {
                let (i, row) = r.expect("poll_next").expect("a row");
                got.push((i, format!("{:?}", row.to_owned())));
                break;
            }
        }
    }
    vfs.set_deferred_io(false);
    vfs.complete_all_io();
    while let Some((i, row)) = rows.next().expect("next") {
        got.push((i, format!("{:?}", row.to_owned())));
    }
    assert_eq!(got, want);
    drop(rows);
    drop(t);
    close(&vfs, db);
}
