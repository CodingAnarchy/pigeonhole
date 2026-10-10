//! The row cache (#404, D201): what it serves, what makes a cached row miss, and that every
//! result equals an uncached read. A snapshot read never uses the cache, so a latest read and
//! a snapshot read taken with no write in between must return the same cells.

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
    // A write to the row: the next read misses, sees it, and refills at once (the row was
    // read before); the one after hits.
    t.mutate(b"r").put("f", b"a", b"3").commit().unwrap();
    let s = db.row_cache_stats();
    let after = read();
    assert_eq!(delta(&db, s), (0, 1, 1));
    let s = db.row_cache_stats();
    assert_eq!(read(), after);
    assert_eq!(delta(&db, s), (1, 0, 0));
    assert_eq!(after[0].3, b"3");
    // A write to another row leaves it cached; flushes and compactions too.
    t.mutate(b"other").put("f", b"a", b"x").commit().unwrap();
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
fn gets_are_answered_from_a_cached_row_but_never_fill() {
    let vfs = SimVfs::new(406);
    let db = Pigeonhole::open("/db/rc.phdb", options(&vfs)).unwrap();
    let t = table(&db);
    t.mutate(b"r").put("f", b"a", b"1").commit().unwrap();
    let s = db.row_cache_stats();
    assert_eq!(t.get(b"r", "f", b"a").unwrap().unwrap().value(), b"1");
    assert_eq!(delta(&db, s), (0, 1, 0), "a get never fills");
    let _ = t.row(b"r").family("f").read().unwrap();
    let _ = t.row(b"r").family("f").read().unwrap();
    let s = db.row_cache_stats();
    assert_eq!(t.get(b"r", "f", b"a").unwrap().unwrap().value(), b"1");
    assert!(t.get(b"r", "f", b"missing").unwrap().is_none());
    assert_eq!(delta(&db, s), (2, 0, 0));
    t.mutate(b"r").put("f", b"a", b"2").commit().unwrap();
    assert_eq!(t.get(b"r", "f", b"a").unwrap().unwrap().value(), b"2");
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
