//! `Options::wal_preallocate` (D157 amendment, #1621, ICR 0130): a writable open gives each
//! WAL stream the free slots it holds in steady state (⌈2 × memtable budget / slot⌉ for the
//! checkpoint's lag, two spares, the change feed's retention), zero-filled and synced, before
//! it takes writes. So a busy stream's rollovers take prepared slots and its file doesn't
//! grow while it serves. Off, a new stream starts with one slot (D157).

use std::path::Path;

use pigeonhole::{Family, FeedRetention, Options, Pigeonhole, Table};

/// The slot size: small, so a test fills several.
const SEG: u64 = 256 << 10;
/// The memtable budget: twice this is the log a stream may hold unflushed (D229).
const BUDGET: u64 = 1 << 20;
/// A stream's ring after a pre-sizing open without the feed: the free slots plus the one in
/// use.
const RING: u64 = (2 * BUDGET).div_ceil(SEG) + 2 + 1;

fn options(preallocate: bool) -> Options {
    Options::default()
        .create_if_missing(true)
        .shards(1)
        .memtable_budget(BUDGET)
        .wal_segment_size(SEG)
        .wal_preallocate(preallocate)
}

/// The length of the database's only WAL stream file.
fn wal_len(db: &Path) -> u64 {
    std::fs::metadata(format!("{}-wal-0", db.display()))
        .expect("the stream file")
        .len()
}

fn table(db: &Pigeonhole) -> Table {
    db.table("t")
        .expect("table")
        .family("f", Family::default())
        .create_if_missing()
        .expect("create")
}

/// About `slots` slots' worth of 4 KiB values, one commit each.
fn write(t: &Table, slots: u64) {
    let v = [7u8; 4096];
    for i in 0..(slots * SEG / 4096) as u32 {
        t.mutate(&i.to_be_bytes())
            .put("f", b"q", &v)
            .commit()
            .expect("commit");
    }
}

fn check(t: &Table, slots: u64) {
    for i in [0, (slots * SEG / 4096) as u32 - 1] {
        let got = t.get(&i.to_be_bytes(), "f", b"q").expect("get");
        assert_eq!(got.expect("the row").value(), [7u8; 4096]);
    }
}

#[test]
fn without_it_a_new_stream_starts_with_one_slot() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("off.phdb");
    let db = Pigeonhole::open(&path, options(false)).expect("open");
    assert_eq!(wal_len(&path), SEG);
    db.close().expect("close");
}

#[test]
fn a_preallocating_open_sizes_the_ring_and_writes_take_its_slots() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("on.phdb");
    let db = Pigeonhole::open(&path, options(true)).expect("open");
    assert_eq!(wal_len(&path), RING * SEG, "the ring at the open");
    let t = table(&db);
    // Six slots of records: each rollover takes a prepared slot (or a recycled one once a
    // flush checkpoints), never a new one.
    write(&t, 6);
    assert_eq!(wal_len(&path), RING * SEG, "the ring didn't grow");
    check(&t, 6);
    drop(t);
    db.close().expect("close");
    // Without the feed the last clean close removes the streams, and the next open sizes
    // the ring again.
    let db = Pigeonhole::open(&path, options(true)).expect("reopen");
    assert_eq!(wal_len(&path), RING * SEG);
    check(&table(&db), 6);
    db.close().expect("close");
}

#[test]
fn the_ring_counts_the_change_feeds_retention_and_a_reopen_fills_only_whats_missing() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("feed.phdb");
    // Four slots of retention for the one stream.
    let feed = |o: Options| o.change_feed(FeedRetention::bytes(4 * SEG));
    let db = Pigeonhole::open(&path, feed(options(true))).expect("open");
    let ring = RING + 4;
    assert_eq!(wal_len(&path), ring * SEG, "the ring with retention");
    let t = table(&db);
    write(&t, 6);
    drop(t);
    db.close().expect("close");
    // With the feed on, a clean close keeps the stream, and recovery's reopen finds its
    // slots: recycled or prepared, so the open adds none.
    let db = Pigeonhole::open(&path, feed(options(true))).expect("reopen");
    assert_eq!(wal_len(&path), ring * SEG, "the reopen added no slot");
    check(&table(&db), 6);
    db.close().expect("close");
}
