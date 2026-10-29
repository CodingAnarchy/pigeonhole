//! An async read that misses the block cache reads exactly what a fresh synchronous read
//! reads (#709). A cache-only cursor reads a missing block in place when its read completes
//! at once (the simulator's reads do, as a page-cache hit does on a real file); otherwise it
//! fails with `WouldBlock` carrying the read in flight, which the future resumes. Both paths
//! are compared with the sync read, row by row, over random data in memtables and several
//! levels of SSTs, through a block cache too small to hold them.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole::{Family, Options, Pigeonhole, RowRef, Table};
use pigeonhole_io::sim::SimVfs;
use proptest::prelude::*;

const ROWS: u32 = 40;

/// One write: row, family (0 small blocks, 1 default), qualifier, timestamp, value length;
/// a value length of 0 deletes the cell's column instead.
type Write = (u32, u8, u8, u64, usize);

fn options(vfs: &Arc<SimVfs>) -> Options {
    Options::default()
        .vfs(Arc::clone(vfs) as _)
        .shards(2)
        .pin_threads(false)
        // Too small for the SSTs' blocks, and no row cache in front of them: most reads miss.
        .block_cache(16 << 10)
        .row_cache(0)
}

fn row_key(r: u32) -> Vec<u8> {
    format!("row{r:03}").into_bytes()
}

/// Writes `writes` in `batches`, flushing after each but the last (so later batches shadow
/// earlier SSTs and the last stays in the memtable), and reopens with a cold cache.
fn build(vfs: &Arc<SimVfs>, writes: &[Write], batches: usize) -> Pigeonhole {
    let path = "/db/inplace.phdb";
    {
        let db = Pigeonhole::open(path, options(vfs)).unwrap();
        let t = db
            .table("t")
            .unwrap()
            .family("s", Family::default().block_size(256).max_versions(3))
            .family("d", Family::default().max_versions(3))
            .create()
            .unwrap();
        let per = writes.len().div_ceil(batches.max(1)).max(1);
        for (i, chunk) in writes.chunks(per).enumerate() {
            for &(row, fam, q, ts, len) in chunk {
                let family = if fam == 0 { "s" } else { "d" };
                let m = t.mutate(&row_key(row));
                let m = if len == 0 {
                    m.delete_column(family, &[q])
                } else {
                    m.put_at(family, &[q], ts, &vec![(row as u8) ^ q; len])
                };
                m.commit().unwrap();
            }
            if i + 1 < writes.len().div_ceil(per) {
                db.flush().unwrap();
            }
        }
        db.close().unwrap();
    }
    Pigeonhole::open(path, options(vfs)).unwrap()
}

/// One cell: family, qualifier, timestamp, value.
type CellRow = (String, Vec<u8>, u64, Vec<u8>);

/// Every cell of a row.
fn cells(r: &RowRef<'_>) -> Vec<CellRow> {
    r.iter()
        .map(|e| {
            (
                e.family.to_owned(),
                e.qualifier.to_vec(),
                e.cell.timestamp(),
                e.cell.value().to_vec(),
            )
        })
        .collect()
}

/// Polls `fut` to completion, completing the simulator's deferred I/O whenever it waits.
fn drive<F: Future + Unpin>(vfs: &SimVfs, mut fut: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..10_000 {
        if let Poll::Ready(out) = Pin::new(&mut fut).poll(&mut cx) {
            return out;
        }
        vfs.complete_all_io();
    }
    panic!("the read never resolved");
}

fn sync_row(t: &Table, row: &[u8], versions: u32) -> Option<Vec<CellRow>> {
    t.row(row)
        .versions(versions)
        .read()
        .unwrap()
        .map(|r| cells(&r))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn async_reads_that_miss_read_what_a_fresh_sync_read_reads(
        seed in any::<u64>(),
        writes in prop::collection::vec(
            (0..ROWS, 0..2u8, 0..4u8, 1..6u64, prop_oneof![Just(0usize), 1..90usize]),
            1..300,
        ),
        batches in 1..5usize,
        // Per row: read its async read through deferred I/O (the future, resuming the read
        // the cursor submitted) rather than with reads that complete at once (in place).
        deferred in prop::collection::vec(any::<bool>(), ROWS as usize),
        versions in 1..4u32,
    ) {
        let vfs = SimVfs::new(seed);
        let db = build(&vfs, &writes, batches);
        let t = db.table("t").unwrap().open().unwrap();
        for r in 0..ROWS {
            let row = row_key(r);
            vfs.set_deferred_io(deferred[r as usize]);
            let got = drive(&vfs, t.row(&row).versions(versions).read_async())
                .unwrap()
                .map(|r| cells(&r.view()));
            vfs.set_deferred_io(false);
            let want = sync_row(&t, &row, versions);
            prop_assert_eq!(got, want, "row {} deferred {} seed {}", r, deferred[r as usize], seed);
        }
        drop(t);
        db.close().unwrap();
    }
}

/// Polls `fut` to completion like [`drive`], returning how often it waited on I/O.
fn drive_counting<F: Future + Unpin>(vfs: &SimVfs, mut fut: F) -> (F::Output, usize) {
    let mut cx = Context::from_waker(Waker::noop());
    for waits in 0..10_000 {
        if let Poll::Ready(out) = Pin::new(&mut fut).poll(&mut cx) {
            return (out, waits);
        }
        vfs.complete_all_io();
    }
    panic!("the read never resolved");
}

#[test]
fn both_paths_are_exercised() {
    // The property above is only as good as its paths: with reads that complete at once, async
    // reads miss the block cache and resolve without waiting or falling back to a sync read
    // (the cursor reads in place; `pigeonhole-sst`'s tests check that mechanism); with
    // deferred reads, they wait on the read the cursor submitted.
    let vfs = SimVfs::new(709);
    let writes: Vec<Write> = (0..400u32)
        .map(|i| {
            (
                i % ROWS,
                (i % 2) as u8,
                (i % 4) as u8,
                1 + u64::from(i % 5),
                60,
            )
        })
        .collect();
    let db = build(&vfs, &writes, 4);
    let t = db.table("t").unwrap().open().unwrap();
    let before = db.engine_metrics();
    for r in 0..ROWS / 2 {
        let (got, waits) = drive_counting(&vfs, t.row(&row_key(r)).read_async());
        assert_eq!(waits, 0, "row {r} resolved without waiting");
        assert_eq!(
            got.unwrap().map(|r| cells(&r.view())),
            sync_row(&t, &row_key(r), 1)
        );
    }
    let after = db.engine_metrics();
    assert!(
        after.block_cache_misses > before.block_cache_misses,
        "the in-place reads missed the cache"
    );
    assert_eq!(
        after.async_sync_reads, before.async_sync_reads,
        "no sync fallback"
    );
    let mut waited = 0;
    vfs.set_deferred_io(true);
    for r in ROWS / 2..ROWS {
        let (got, waits) = drive_counting(&vfs, t.row(&row_key(r)).read_async());
        waited += waits;
        vfs.set_deferred_io(false);
        assert_eq!(
            got.unwrap().map(|r| cells(&r.view())),
            sync_row(&t, &row_key(r), 1)
        );
        vfs.set_deferred_io(true);
    }
    vfs.set_deferred_io(false);
    assert!(waited > 0, "the deferred reads waited on their reads");
    drop(t);
    db.close().unwrap();
}
