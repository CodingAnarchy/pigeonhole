//! The row cache (#404, D201, D290): what it serves, what makes a cached row miss, what a
//! commit moves forward, and that every result equals an uncached read. A snapshot row read
//! never uses the cache, so a latest read and a snapshot row read taken with no write in
//! between must return the same cells.

use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole::{Family, Options, Pigeonhole, RowCacheStats, Table, ValueFilter};
use pigeonhole_io::sim::SimVfs;

fn options(vfs: &Arc<SimVfs>) -> Options {
    Options::default()
        .vfs(Arc::clone(vfs) as _)
        .shards(2)
        .memtable_budget(4 << 20)
        .wal_segment_size(256 << 10)
        .row_cache(1 << 20)
}

fn table(db: &Pigeonhole) -> Table {
    db.table("t")
        .unwrap()
        .family("f", Family::default().max_versions(3))
        .family("g", Family::default())
        .family("short", Family::default().ttl(Duration::from_secs(10)))
        .family("n", Family::counter())
        .create_if_missing()
        .unwrap()
}

/// `(family, qualifier, ts, value)` of every cell of a read.
type Cells = Vec<(String, Vec<u8>, u64, Vec<u8>)>;

fn cells(r: Option<pigeonhole::Row>) -> Cells {
    r.map(|r| {
        (0..r.len())
            .filter_map(|i| r.entry(i))
            .map(|(f, q, c)| (f.to_owned(), q.to_vec(), c.timestamp(), c.value().to_vec()))
            .collect()
    })
    .unwrap_or_default()
}

fn delta(db: &Pigeonhole, before: RowCacheStats) -> (u64, u64, u64) {
    let now = db.row_cache_stats();
    (
        now.hits - before.hits,
        now.misses - before.misses,
        now.fills - before.fills,
    )
}

#[test]
fn a_cached_row_is_served_until_the_row_is_written() {
    let vfs = SimVfs::new(404);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r")
        .put("f", b"a", b"1")
        .put("f", b"b", b"2")
        .commit()
        .unwrap();
    let read = || {
        cells(
            t.row(b"r")
                .family("f")
                .read()
                .unwrap()
                .map(|r| r.to_owned()),
        )
    };
    let s = db.row_cache_stats();
    let first = read();
    assert_eq!(delta(&db, s), (0, 1, 0), "a first miss only marks the row");
    let s = db.row_cache_stats();
    assert_eq!(read(), first);
    assert_eq!(delta(&db, s), (0, 1, 1), "a second miss fills");
    let s = db.row_cache_stats();
    assert_eq!(read(), first);
    assert_eq!(delta(&db, s), (1, 0, 0), "then hits");
    // A put to a cell of the cached row: the commit moves the row forward (D290), so the
    // next read hits and sees it.
    let w = db.row_cache_stats().write_throughs;
    t.mutate(b"r").put("f", b"a", b"3").commit().unwrap();
    assert_eq!(db.row_cache_stats().write_throughs, w + 1);
    let s = db.row_cache_stats();
    let after = read();
    assert_eq!(delta(&db, s), (1, 0, 0));
    assert_eq!(after[0].3, b"3");
    assert_eq!(after[1].3, b"2", "the other cell is unchanged");
    // A put to a new cell is not overlaid: the next read misses, sees it, and refills at
    // once (the row was read before); the one after hits.
    t.mutate(b"r").put("f", b"c", b"4").commit().unwrap();
    assert_eq!(db.row_cache_stats().write_throughs, w + 1);
    let s = db.row_cache_stats();
    let after = read();
    assert_eq!(delta(&db, s), (0, 1, 1));
    assert_eq!(after.len(), 3);
    let s = db.row_cache_stats();
    assert_eq!(read(), after);
    assert_eq!(delta(&db, s), (1, 0, 0));
    // A write to another row leaves it cached, unless the two rows share a write-watermark
    // slot (D201: a shared slot costs a false miss, never a wrong row). The slots are hashed
    // with a per-engine random seed, so any one other row shares `r`'s with probability about
    // 1 in 4,096 (#918): such a write makes the next read miss and refill, and the test tries
    // another row.
    let left_cached = (0..3).any(|i| {
        t.mutate(format!("other{i}").as_bytes())
            .put("f", b"a", b"x")
            .commit()
            .unwrap();
        let s = db.row_cache_stats();
        assert_eq!(read(), after);
        match delta(&db, s) {
            (1, 0, 0) => true,
            d => {
                assert_eq!(
                    d,
                    (0, 1, 1),
                    "only a shared slot makes it miss, and it refills"
                );
                false
            }
        }
    });
    assert!(left_cached, "three other rows all shared r's slot");
    // Flushes and compactions write no row: it stays cached.
    db.flush().unwrap();
    db.compact().unwrap();
    let s = db.row_cache_stats();
    assert_eq!(read(), after);
    assert_eq!(delta(&db, s), (1, 0, 0));
    // A row delete makes it miss.
    t.mutate(b"r").delete_row().commit().unwrap();
    assert_eq!(read(), Vec::new());
    drop(t);
    db.close().unwrap();
}

#[test]
fn projections_of_a_cached_row_equal_uncached_reads() {
    let vfs = SimVfs::new(405);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    let mut m = t.mutate(b"r");
    for q in [&b"a1"[..], b"a2", b"b1", b"b2", b"c"] {
        m = m.put("f", q, q);
    }
    m.put("g", b"x", b"y").commit().unwrap();
    // Older versions too: a cached row holds only the newest.
    t.mutate(b"r").put("f", b"a1", b"new").commit().unwrap();
    // Fill both families (a family row is stored on its second miss).
    let _ = t.row(b"r").read().unwrap();
    let _ = t.row(b"r").read().unwrap();
    let snap = db.snapshot().unwrap();
    type Shape = fn(pigeonhole::RowRead<'_>) -> pigeonhole::RowRead<'_>;
    let shapes: [Shape; 8] = [
        |r| r,
        |r| r.family("f"),
        |r| r.family("g").family("f"),
        |r| r.family("f").qualifier_prefix(b"a"),
        |r| {
            r.family("f")
                .qualifier_bounds(Bound::Excluded(&b"a1"[..]), Bound::Included(&b"b2"[..]))
        },
        |r| r.family("f").column_limit(2),
        |r| {
            r.family("f")
                .value_filter(ValueFilter::Prefix(b"b".to_vec()))
        },
        |r| {
            r.family("f")
                .qualifier_prefix(b"b")
                .column_limit(1)
                .value_filter(ValueFilter::Equals(b"b2".to_vec()))
        },
    ];
    for (i, shape) in shapes.iter().enumerate() {
        let s = db.row_cache_stats();
        let cached = cells(shape(t.row(b"r")).read().unwrap().map(|r| r.to_owned()));
        let (hits, _, _) = delta(&db, s);
        assert!(hits > 0, "shape {i} was not served from the cache");
        let s = db.row_cache_stats();
        let uncached = cells(
            shape(t.row(b"r"))
                .snapshot(&snap)
                .read()
                .unwrap()
                .map(|r| r.to_owned()),
        );
        assert_eq!(
            delta(&db, s),
            (0, 0, 0),
            "a snapshot read bypasses the cache"
        );
        assert_eq!(cached, uncached, "shape {i}");
    }
    // Multi-version and time-range reads bypass it.
    let s = db.row_cache_stats();
    let _ = t.row(b"r").versions(0).read().unwrap();
    let _ = t.row(b"r").time_range(0..u64::MAX).read().unwrap();
    assert_eq!(delta(&db, s), (0, 0, 0));
    drop(t);
    db.close().unwrap();
}

#[test]
fn gets_are_answered_from_a_cached_row_and_fill_on_a_second_miss() {
    let vfs = SimVfs::new(406);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r")
        .put("f", b"a", b"1")
        .put("f", b"b", b"2")
        .commit()
        .unwrap();
    let get = |q: &[u8]| t.get(b"r", "f", q).unwrap().map(|c| c.value().to_vec());
    let s = db.row_cache_stats();
    assert_eq!(get(b"a").unwrap(), b"1");
    assert_eq!(delta(&db, s), (0, 1, 0), "a first miss only marks the row");
    let s = db.row_cache_stats();
    assert_eq!(get(b"a").unwrap(), b"1");
    assert_eq!(
        delta(&db, s),
        (0, 1, 1),
        "a second miss fills the family row (D290)"
    );
    let s = db.row_cache_stats();
    assert_eq!(get(b"b").unwrap(), b"2");
    assert!(get(b"missing").is_none());
    assert_eq!(delta(&db, s), (2, 0, 0));
    // A put to a cached cell moves the row forward: still a hit.
    t.mutate(b"r").put("f", b"a", b"3").commit().unwrap();
    let s = db.row_cache_stats();
    assert_eq!(get(b"a").unwrap(), b"3");
    assert_eq!(delta(&db, s), (1, 0, 0));
    drop(t);
    db.close().unwrap();
}

#[test]
fn snapshot_and_transaction_gets_are_served_at_their_read_point() {
    let vfs = SimVfs::new(410);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r").put("f", b"a", b"1").commit().unwrap();
    let _ = t.get(b"r", "f", b"a").unwrap();
    let _ = t.get(b"r", "f", b"a").unwrap();
    let snap = db.snapshot().unwrap();
    let s = db.row_cache_stats();
    let at = |snap: &pigeonhole::Snapshot| {
        t.get_at(snap, b"r", "f", b"a")
            .unwrap()
            .map(|c| c.value().to_vec())
    };
    assert_eq!(at(&snap).unwrap(), b"1");
    assert_eq!(delta(&db, s), (1, 0, 0), "a snapshot get hits (D290)");
    let mut txn = db.transaction().unwrap();
    let s = db.row_cache_stats();
    assert_eq!(txn.get(&t, b"r", "f", b"a").unwrap().unwrap().value(), b"1");
    assert_eq!(delta(&db, s), (1, 0, 0), "so does a transaction's");
    // A later write moves the cached row past both read points: they miss and read the row
    // as of their snapshot, not the cached one.
    t.mutate(b"r").put("f", b"a", b"2").commit().unwrap();
    let s = db.row_cache_stats();
    assert_eq!(at(&snap).unwrap(), b"1");
    assert_eq!(txn.get(&t, b"r", "f", b"a").unwrap().unwrap().value(), b"1");
    assert_eq!(delta(&db, s).0, 0);
    // The read is in the read set whichever way it was answered: the write conflicts.
    txn.put(&t, b"r", "f", b"a", b"9");
    assert!(txn.commit().is_err());
    let mut txn = db.transaction().unwrap();
    let s = db.row_cache_stats();
    assert_eq!(txn.get(&t, b"r", "f", b"a").unwrap().unwrap().value(), b"2");
    assert_eq!(delta(&db, s), (1, 0, 0));
    drop(txn);
    drop(snap);
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_put_a_delete_could_hide_is_not_written_through() {
    let vfs = SimVfs::new(411);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    // `a` at 5 is newest once the version at 10 is deleted.
    t.mutate(b"r")
        .put_at("f", b"a", 5, b"old")
        .put_at("f", b"a", 10, b"gone")
        .commit()
        .unwrap();
    t.mutate(b"r").delete_cell("f", b"a", 10).commit().unwrap();
    let get = || {
        t.get(b"r", "f", b"a")
            .unwrap()
            .map(|c| (c.timestamp(), c.value().to_vec()))
    };
    assert_eq!(get().unwrap(), (5, b"old".to_vec()));
    assert_eq!(get().unwrap(), (5, b"old".to_vec()));
    // A put exactly at the deleted timestamp is hidden by the delete whatever its seqno. The
    // cached row doesn't record the delete, so the commit must not overlay it.
    let w = db.row_cache_stats().write_throughs;
    t.mutate(b"r")
        .put_at("f", b"a", 10, b"hidden")
        .commit()
        .unwrap();
    assert_eq!(get().unwrap(), (5, b"old".to_vec()));
    assert_eq!(db.row_cache_stats().write_throughs, w);
    // A delete or an increment isn't overlaid either.
    t.mutate(b"r").delete_cell("f", b"a", 5).commit().unwrap();
    assert!(get().is_none());
    t.mutate(b"r").incr("n", b"c", 2).commit().unwrap();
    assert_eq!(db.row_cache_stats().write_throughs, w);
    drop(t);
    db.close().unwrap();
}

#[test]
fn gets_refill_a_row_written_between_its_reads_at_most_every_other_get() {
    // D290's get-fill safeguard: a row that changes between its reads by writes the
    // write-through can't follow (here a new column each time) would otherwise be read whole
    // and stored again by every get. A get that finds its cached copy stale and unread skips
    // the fill and drops the copy; the next miss fills under the ghost rule. Reads stay
    // correct throughout.
    let vfs = SimVfs::new(413);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r").put("g", b"a", b"1").commit().unwrap();
    let get = || t.get(b"r", "g", b"a").unwrap().map(|c| c.value().to_vec());
    assert_eq!(get().unwrap(), b"1");
    assert_eq!(get().unwrap(), b"1");
    assert_eq!(db.row_cache_stats().fills, 1, "the second miss fills");
    for i in 0..10u32 {
        t.mutate(b"r")
            .put("g", format!("q{i}").as_bytes(), b"x")
            .commit()
            .unwrap();
        assert_eq!(get().unwrap(), b"1");
    }
    // Without the skip every one of the ten gets refills (11 in all).
    assert_eq!(
        db.row_cache_stats().fills,
        6,
        "one fill for every other get of a row written between its reads"
    );
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_row_refilled_and_written_before_a_hit_is_cached_again() {
    // simp724's review of aca874d4 (seed 416): a fill is not a read, so a copy refilled and
    // then written before any hit is stale and unread for good, and a skip that kept it
    // refused every later fill: the row stayed uncached until evicted. With the skip
    // one-shot, the gets after it fill and hit again.
    let vfs = SimVfs::new(416);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    let get = || t.get(b"r", "g", b"a").unwrap().map(|c| c.value().to_vec());
    t.mutate(b"r").put("g", b"a", b"1").commit().unwrap();
    for _ in 0..3 {
        assert_eq!(get().unwrap(), b"1"); // fill, then a hit: read
    }
    t.mutate(b"r").put("g", b"q1", b"x").commit().unwrap(); // refused; stale but read
    assert_eq!(get().unwrap(), b"1"); // refills, unread
    t.mutate(b"r").put("g", b"q2", b"x").commit().unwrap(); // refused before any hit
    let s = db.row_cache_stats();
    for _ in 0..5 {
        assert_eq!(get().unwrap(), b"1");
    }
    let now = db.row_cache_stats();
    assert!(
        now.fills > s.fills,
        "the row is stored again: {s:?} -> {now:?}"
    );
    assert!(
        now.hits > s.hits,
        "and served from the cache again: {s:?} -> {now:?}"
    );
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_commit_writing_one_cached_column_twice_is_sized_in_order() {
    // simp724's review of #1011: two puts to one cached column in one commit (two
    // timestamps, both kept) were each sized against the cached cell. With a 100-byte cell
    // and two empty puts that underflowed and panicked the shard thread; with growing puts
    // it under-sized the row and let it past `row_cache_max_row`.
    let vfs = SimVfs::new(415);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs).row_cache_max_row(200)).unwrap();
    let t = table(&db);
    let get = || {
        t.get(b"r", "g", b"a")
            .unwrap()
            .map(|c| (c.timestamp(), c.value().to_vec()))
            .unwrap()
    };
    t.mutate(b"r").put("g", b"a", &[7u8; 100]).commit().unwrap();
    let _ = get();
    let _ = get();
    assert_eq!(db.row_cache_stats().fills, 1);
    let t1 = u64::MAX / 4;
    let w = db.row_cache_stats().write_throughs;
    t.mutate(b"r")
        .put_at("g", b"a", t1, b"")
        .put_at("g", b"a", t1 + 1, b"")
        .commit()
        .unwrap();
    assert_eq!(get(), (t1 + 1, Vec::new()));
    assert_eq!(
        db.row_cache_stats().write_throughs,
        w + 1,
        "it fits: written through"
    );
    // 10 bytes, then 190 at a later timestamp: the result (190 bytes) is over the 200-byte
    // cap with the encoding's overhead, so the overlay is refused and the read is uncached.
    t.mutate(b"r")
        .put_at("g", b"a", t1 + 2, &[1u8; 10])
        .put_at("g", b"a", t1 + 3, &[2u8; 190])
        .commit()
        .unwrap();
    assert_eq!(
        db.row_cache_stats().write_throughs,
        w + 1,
        "over the cap: refused"
    );
    assert_eq!(get(), (t1 + 3, vec![2u8; 190]));
    drop(t);
    db.close().unwrap();
}

#[test]
fn gets_never_fill_a_counter_family() {
    let vfs = SimVfs::new(412);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r").incr("n", b"c", 1).commit().unwrap();
    for _ in 0..3 {
        assert!(t.get(b"r", "n", b"c").unwrap().is_some());
    }
    assert_eq!(db.row_cache_stats().fills, 0);
    drop(t);
    db.close().unwrap();
}

#[test]
fn a_cached_row_misses_once_a_cell_expires() {
    let vfs = SimVfs::new(407);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r").put("short", b"a", b"1").commit().unwrap();
    vfs.advance(5_000_000_000);
    t.mutate(b"r").put("short", b"b", b"2").commit().unwrap();
    let read = || {
        cells(
            t.row(b"r")
                .family("short")
                .read()
                .unwrap()
                .map(|r| r.to_owned()),
        )
    };
    assert_eq!(read().len(), 2);
    assert_eq!(read().len(), 2);
    let s = db.row_cache_stats();
    assert_eq!(read().len(), 2);
    assert_eq!(delta(&db, s), (1, 0, 0));
    // `a` expires 10 s after it was written: the cached row stops being served.
    vfs.advance(6_000_000_000);
    let s = db.row_cache_stats();
    let left = read();
    assert_eq!(delta(&db, s).0, 0);
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].1, b"b");
    drop(t);
    db.close().unwrap();
}

#[test]
fn only_listed_families_and_rows_under_the_cap_are_cached() {
    let vfs = SimVfs::new(408);
    let db = Pigeonhole::open(
        "/db/rc.phdb",
        options(&vfs)
            .row_cache_family("t", "f")
            .row_cache_max_row(256),
    )
    .unwrap();
    let t = table(&db);
    t.mutate(b"small")
        .put("f", b"a", b"1")
        .put("g", b"a", b"1")
        .commit()
        .unwrap();
    t.mutate(b"big").put("f", b"a", &[7; 300]).commit().unwrap();
    for _ in 0..2 {
        let _ = t.row(b"small").family("g").read().unwrap();
        let _ = t.row(b"big").family("f").read().unwrap();
    }
    assert_eq!(
        db.row_cache_stats().fills,
        0,
        "g is not listed; big is over the cap"
    );
    for _ in 0..3 {
        let _ = t.row(b"small").family("f").read().unwrap();
    }
    let s = db.row_cache_stats();
    assert_eq!((s.fills, s.hits), (1, 1));
    drop(t);
    db.close().unwrap();
}

#[test]
fn the_row_cache_is_off_by_default() {
    let vfs = SimVfs::new(409);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs).row_cache(0)).unwrap();
    let t = table(&db);
    t.mutate(b"r").put("f", b"a", b"1").commit().unwrap();
    let _ = t.row(b"r").read().unwrap();
    let _ = t.row(b"r").read().unwrap();
    assert_eq!(db.row_cache_stats(), RowCacheStats::default());
    drop(t);
    db.close().unwrap();
}
