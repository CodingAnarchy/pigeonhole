//! `RowMutation::return_previous` (D314 amendment 1, ICR 0113): get-and-set, a column's
//! newest visible version just before the commit that writes it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole::{Condition, ErrorCode, Family, Options, Pigeonhole, Table};
use pigeonhole_io::sim::SimVfs;

fn open_with(seed: u64, options: impl FnOnce(Options) -> Options) -> (Arc<Pigeonhole>, Table) {
    let vfs = SimVfs::new(seed);
    let db = Arc::new(
        Pigeonhole::open(
            "/db/t.phdb",
            options(
                Options::default()
                    .vfs(vfs as _)
                    .shards(2)
                    .memtable_budget(4 << 20)
                    .wal_segment_size(256 << 10),
            ),
        )
        .unwrap(),
    );
    let t = db
        .table("t")
        .unwrap()
        .family("s", Family::default())
        .family("c", Family::counter())
        .family("b", Family::default().blob_threshold(64))
        .create()
        .unwrap();
    (db, t)
}

fn open(seed: u64) -> (Arc<Pigeonhole>, Table) {
    open_with(seed, |o| o)
}

/// Puts `value` in `s:q` of `row` and returns what it replaced.
fn swap(t: &Table, row: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    t.mutate(row)
        .put("s", b"q", value)
        .return_previous("s", b"q")
        .commit_returning()
        .unwrap()
        .previous("s", b"q")
        .unwrap()
        .map(|c| c.value().to_vec())
}

/// N concurrent swaps of one cell (default timestamps) form a chain: every value written,
/// and the initial one, is returned exactly once, except the last written, which is the
/// cell's value after. Across flushes, so some reads come from SSTs.
#[test]
fn concurrent_swaps_form_a_chain() {
    let (db, t) = open(1445);
    t.mutate(b"lease").put("s", b"q", b"init").commit().unwrap();
    const THREADS: usize = 6;
    const EACH: usize = 150;
    let mut handles = Vec::new();
    for i in 0..THREADS {
        let t = t.clone();
        handles.push(std::thread::spawn(move || {
            (0..EACH)
                .map(|j| swap(&t, b"lease", format!("{i}:{j}").as_bytes()).expect("a holder"))
                .collect::<Vec<Vec<u8>>>()
        }));
    }
    for _ in 0..4 {
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    let mut seen: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    for h in handles {
        for v in h.join().unwrap() {
            *seen.entry(v).or_default() += 1;
        }
    }
    let last = t
        .get(b"lease", "s", b"q")
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let mut want: Vec<Vec<u8>> = (0..THREADS)
        .flat_map(|i| (0..EACH).map(move |j| format!("{i}:{j}").into_bytes()))
        .filter(|v| *v != last)
        .collect();
    want.push(b"init".to_vec());
    want.sort();
    assert!(
        seen.values().all(|&n| n == 1),
        "a previous value returned twice"
    );
    assert_eq!(seen.into_keys().collect::<Vec<_>>(), want);
}

/// Several writes to one cell in a commit return the value from before the commit, not an
/// intermediate one.
#[test]
fn several_writes_return_the_value_before_the_commit() {
    let (_db, t) = open(1);
    t.mutate(b"r").put("s", b"q", b"a").commit().unwrap();
    let r = t
        .mutate(b"r")
        .put("s", b"q", b"b")
        .delete_column("s", b"q")
        .put("s", b"q", b"c")
        .return_previous("s", b"q")
        .commit_returning()
        .unwrap();
    assert_eq!(r.previous("s", b"q").unwrap().unwrap().value(), b"a");
    assert_eq!(swap(&t, b"r", b"d").as_deref(), Some(&b"c"[..]));
}

/// No visible version: never written, deleted by a column delete, or by a row delete.
#[test]
fn deleted_or_never_written_is_none() {
    let (_db, t) = open(2);
    assert_eq!(swap(&t, b"r", b"a"), None);
    t.mutate(b"r").delete_column("s", b"q").commit().unwrap();
    assert_eq!(swap(&t, b"r", b"b"), None);
    t.mutate(b"r").delete_row().commit().unwrap();
    assert_eq!(swap(&t, b"r", b"c"), None);
    // A delete in the returning commit itself: the value before it.
    let r = t
        .mutate(b"r")
        .delete_column("s", b"q")
        .return_previous("s", b"q")
        .commit_returning()
        .unwrap();
    assert_eq!(r.previous("s", b"q").unwrap().unwrap().value(), b"c");
}

/// simp724's C1: a put at an explicit timestamp below the column's newest version returns
/// that newest version, which stays newest, so the next swap returns it again.
#[test]
fn an_older_explicit_timestamp_returns_the_newest_version_again() {
    let (_db, t) = open(3);
    t.mutate(b"r")
        .put_at("s", b"q", 1_000, b"new")
        .commit()
        .unwrap();
    let r = t
        .mutate(b"r")
        .put_at("s", b"q", 10, b"old")
        .return_previous("s", b"q")
        .commit_returning()
        .unwrap();
    let prev = r.previous("s", b"q").unwrap().unwrap();
    assert_eq!((prev.value(), prev.timestamp()), (&b"new"[..], 1_000));
    let r = t
        .mutate(b"r")
        .put_at("s", b"q", 20, b"older")
        .return_previous("s", b"q")
        .commit_returning()
        .unwrap();
    assert_eq!(r.previous("s", b"q").unwrap().unwrap().value(), b"new");
}

/// A counter's previous value is its folded value: `put_i64(0)` drains it.
#[test]
fn a_counter_drains() {
    let (db, t) = open(4);
    for _ in 0..5 {
        t.mutate(b"r").incr("c", b"n", 3).commit().unwrap();
    }
    db.flush().unwrap();
    t.mutate(b"r").incr("c", b"n", 1).commit().unwrap();
    let r = t
        .mutate(b"r")
        .put_i64("c", b"n", 0)
        .return_previous("c", b"n")
        .commit_returning()
        .unwrap();
    assert_eq!(r.previous("c", b"n").unwrap().unwrap().as_i64(), Some(16));
    assert_eq!(r.value("c", b"n").unwrap(), Some(0));
}

/// simp724's C3: the read at the seqno below the commit does not see the D290 row cache,
/// which the commit's write-through has already moved forward. The row is flushed and
/// read until it is cached (a hit), and each swap moves the cached row forward (a
/// write-through), so a read that consulted the cache would see the swap's own write.
#[test]
fn a_cached_row_returns_the_value_before_the_commit() {
    let (db, t) = open_with(5, |o| o.row_cache(1 << 20));
    t.mutate(b"r").put("s", b"q", b"a").commit().unwrap();
    db.flush().unwrap();
    let read = || {
        t.row(b"r")
            .family("s")
            .read()
            .unwrap()
            .map(|r| r.to_owned())
            .is_some()
    };
    for _ in 0..3 {
        assert!(read());
    }
    let s = db.row_cache_stats();
    assert!(read());
    assert_eq!(db.row_cache_stats().hits, s.hits + 1, "the row is cached");
    for (put, was) in [(&b"b"[..], &b"a"[..]), (b"c", b"b")] {
        let w = db.row_cache_stats().write_throughs;
        assert_eq!(swap(&t, b"r", put).as_deref(), Some(was));
        assert_eq!(
            db.row_cache_stats().write_throughs,
            w + 1,
            "the swap moved the cached row forward"
        );
    }
}

/// A separated (blob) value is read out.
#[test]
fn a_blob_value_is_read_out() {
    let (db, t) = open(6);
    let big = vec![7u8; 10_000];
    t.mutate(b"r").put("b", b"q", &big).commit().unwrap();
    db.flush().unwrap();
    let r = t
        .mutate(b"r")
        .put("b", b"q", b"small")
        .return_previous("b", b"q")
        .commit_returning()
        .unwrap();
    assert_eq!(r.previous("b", b"q").unwrap().unwrap().value(), &big[..]);
}

/// A request-only mutation, a request on `commit` or `commit_if`, and a column not asked
/// for are refused; asking twice is asking once.
#[test]
fn refusals_and_repeats() {
    let (_db, t) = open(7);
    let e = t
        .mutate(b"r")
        .return_previous("s", b"q")
        .commit_returning()
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    let e = t
        .mutate(b"r")
        .put("s", b"q", b"a")
        .return_previous("s", b"q")
        .commit()
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    let cond = Condition::Absent {
        family: "s".into(),
        qualifier: b"q".to_vec(),
    };
    let e = t
        .mutate(b"r")
        .put("s", b"q", b"a")
        .return_previous("s", b"q")
        .commit_if(&cond)
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    // Nothing above committed.
    assert!(t.get(b"r", "s", b"q").unwrap().is_none());
    let e = t
        .mutate(b"r")
        .put("s", b"q", b"a")
        .return_previous("nope", b"q")
        .commit_returning()
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::FamilyNotFound);
    let r = t
        .mutate(b"r")
        .put("s", b"q", b"b")
        .return_previous("s", b"q")
        .return_previous("s", b"q")
        .return_previous("s", b"other")
        .commit_returning()
        .unwrap();
    assert!(r.previous("s", b"q").unwrap().is_none());
    assert!(r.previous("s", b"other").unwrap().is_none());
    assert_eq!(
        r.previous("s", b"x").unwrap_err().code(),
        ErrorCode::InvalidArgument
    );
}

#[cfg(feature = "async")]
#[test]
fn the_async_form_returns_the_same() {
    let (_db, t) = open(8);
    let mut prev: Option<Vec<u8>> = None;
    for v in [&b"a"[..], b"b", b"c"] {
        let r = pigeonhole::doc_support::block_on(
            t.mutate(b"r")
                .put("s", b"q", v)
                .return_previous("s", b"q")
                .commit_returning_async(),
        )
        .unwrap();
        assert_eq!(
            r.previous("s", b"q").unwrap().map(|c| c.value().to_vec()),
            prev
        );
        prev = Some(v.to_vec());
    }
}
