//! A change-feed reader's read-ahead counts what it really holds (#1637): the memory its kept
//! records take, measured by a counting global allocator, stays within 1.25× of what the
//! read-ahead cap counts (D320's `read_ahead_bytes`).
//!
//! The counting allocator needs `unsafe` (`GlobalAlloc`); it lives in this test binary only,
//! as in `alloc_budget.rs`. It tracks live bytes per thread, so only the test thread's own
//! allocations count: the reader reads the streams on the thread that polls it.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, FamilyOptions, FeedOptions, FeedSpec, FeedStart, Next, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

struct Counting;

thread_local! {
    static LIVE: Cell<i64> = const { Cell::new(0) };
}

fn bump(bytes: i64) {
    // `try_with`: the allocator can run during thread-local teardown.
    let _ = LIVE.try_with(|n| n.set(n.get() + bytes));
}

// SAFETY: forwards every call to `System` unchanged; only counts calls on this thread.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump(layout.size() as i64);
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract, which we pass through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump(layout.size() as i64);
        // SAFETY: as for `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump(new_size as i64 - layout.size() as i64);
        // SAFETY: as for `alloc`; `ptr` and `layout` come from this allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        bump(-(layout.size() as i64));
        // SAFETY: as for `alloc`; `ptr` and `layout` come from this allocator.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> i64 {
    LIVE.with(Cell::get)
}

const DB: &str = "/db/data.phdb";

/// Two tables on different shards, so a batch over both is a cross-shard commit.
fn tables(db: &Engine) -> (Arc<TableInfo>, Arc<TableInfo>) {
    let f = || vec![("f".into(), FamilyOptions::default())];
    (
        db.create_table("a", &f()).unwrap(),
        db.create_table("b", &f()).unwrap(),
    )
}

fn put(wb: &mut WriteBatch, t: &TableInfo, row: &[u8], value: &[u8]) {
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(value),
    )
    .unwrap();
}

#[test]
fn a_readers_kept_records_take_about_what_its_read_ahead_counts() {
    let vfs = SimVfs::new(3301);
    let mut o = common::options(Arc::clone(&vfs), 2, 16 << 20);
    let mut feed = FeedOptions::default();
    feed.retention_bytes = 64 << 20;
    o.feed = Some(feed);
    let db = Engine::open(Path::new(DB), o).unwrap();
    let (a, b) = tables(&db);
    let mut r = db
        .feed(FeedSpec {
            tables: vec![a.id, b.id],
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
        })
        .unwrap();
    let value = [7u8; 100];
    let commit = |i: u32, cross: bool| {
        let mut wb = WriteBatch::new();
        let row = format!("r{i:05}");
        put(&mut wb, &a, row.as_bytes(), &value);
        if cross {
            put(&mut wb, &b, row.as_bytes(), &value);
        }
        db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno
    };
    // Warm up: the reader's cursors, buffers and scratch exist before the measured poll.
    for i in 0..20 {
        commit(i, i % 4 == 0);
    }
    while let Next::Commit(_) = r.wait(std::time::Duration::from_secs(10)).unwrap() {
        if r.delivered() >= db.visible_seqno() {
            break;
        }
    }
    // The backlog: small single-mutation commits (the heap profile's load, #1637) and some
    // cross-shard rounds.
    for i in 20..3020 {
        commit(i, i % 10 == 0);
    }
    let before = live();
    // One poll reads the whole backlog (well under the 64 MiB budget) and delivers its first
    // commit; the rest stay kept.
    assert!(matches!(r.try_next().unwrap(), Next::Commit(_)));
    let held = live() - before;
    let counted = r.read_ahead_bytes() as i64;
    eprintln!("kept: {held} bytes allocated, {counted} counted");
    assert!(counted > 0);
    assert!(
        held * 4 <= counted * 5,
        "the kept records take {held} bytes, over 1.25x the {counted} the read-ahead counts"
    );
    // And the count isn't loose either: what it counts is mostly there.
    assert!(
        held * 2 >= counted,
        "the read-ahead counts {counted} bytes for {held} allocated"
    );
    drop(r);
    db.close().unwrap();
}
