//! `RowMutation::commit_returning` (D314, ICR 0113): a mutation's counters with their values
//! after its commit, as a get at the commit's seqno sees them.

use std::sync::Arc;
use std::time::Duration;

use pigeonhole::{ErrorCode, Family, Options, Pigeonhole, Table};
use pigeonhole_io::sim::SimVfs;

fn open(seed: u64) -> (Arc<Pigeonhole>, Table) {
    let vfs = SimVfs::new(seed);
    let db = Arc::new(
        Pigeonhole::open(
            "/db/t.phdb",
            Options::default()
                .vfs(vfs as _)
                .shards(2)
                .memtable_budget(4 << 20)
                .wal_segment_size(256 << 10),
        )
        .unwrap(),
    );
    let t = db
        .table("t")
        .unwrap()
        .family("c", Family::counter())
        .family("b", Family::counter().ttl(Duration::from_secs(3_600)))
        .create()
        .unwrap();
    (db, t)
}

fn next(t: &Table, row: &[u8]) -> i64 {
    t.mutate(row)
        .incr("c", b"n", 1)
        .commit_returning()
        .unwrap()
        .value("c", b"n")
        .unwrap()
        .unwrap()
}

/// N callers adding 1 from zero get exactly 1..=N, across flushes (so some folds start
/// from an SST, the full read, and later ones from the checkpoint it leaves).
#[test]
fn concurrent_increments_return_one_to_n() {
    let (db, t) = open(1440);
    const THREADS: usize = 6;
    const EACH: usize = 200;
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let t = t.clone();
        handles.push(std::thread::spawn(move || {
            (0..EACH).map(|_| next(&t, b"seq")).collect::<Vec<i64>>()
        }));
    }
    for _ in 0..4 {
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    let mut got: Vec<i64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    got.sort_unstable();
    let want: Vec<i64> = (1..=(THREADS * EACH) as i64).collect();
    assert_eq!(got, want);
}

/// Beside cross-shard batches incrementing the same counter, no increment is lost and no
/// value repeats.
#[test]
fn returning_increments_beside_cross_shard_batches_lose_nothing() {
    let (db, t) = open(1441);
    let mut handles = Vec::new();
    for _ in 0..4 {
        let t = t.clone();
        handles.push(std::thread::spawn(move || {
            (0..150).map(|_| next(&t, b"r")).collect::<Vec<i64>>()
        }));
    }
    let (db2, t2) = (Arc::clone(&db), t.clone());
    let batches = std::thread::spawn(move || {
        for i in 0..60u8 {
            let mut b = db2.write_batch();
            b.incr(&t2, b"r", "c", b"n", 1_000);
            for k in 0..6u8 {
                b.incr(&t2, &[b'o', k, i], "c", b"n", 1);
            }
            b.commit().unwrap();
        }
    });
    let mut got: Vec<i64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    batches.join().unwrap();
    let total = t.get(b"r", "c", b"n").unwrap().unwrap().as_i64().unwrap();
    assert_eq!(total, 600 + 60 * 1_000);
    let mut ones: Vec<i64> = got.iter().map(|v| v % 1_000).collect();
    ones.sort_unstable();
    let mut want: Vec<i64> = (1..=600).map(|v| v % 1_000).collect();
    want.sort_unstable();
    assert_eq!(ones, want);
    got.sort_unstable();
    got.dedup();
    assert_eq!(got.len(), 600, "a value was returned twice");
}

/// D186: a mutation's writes of one counter apply in order, and the value is theirs.
#[test]
fn writes_of_one_counter_in_a_mutation_combine() {
    let (_db, t) = open(1442);
    let v =
        |m: pigeonhole::RowMutation<'_>| m.commit_returning().unwrap().value("c", b"n").unwrap();
    assert_eq!(
        v(t.mutate(b"a").incr("c", b"n", 1).incr("c", b"n", 2)),
        Some(3)
    );
    assert_eq!(
        v(t.mutate(b"a").put_i64("c", b"n", 5).incr("c", b"n", 1)),
        Some(6)
    );
    assert_eq!(
        v(t.mutate(b"a").incr("c", b"n", 1).put_i64("c", b"n", 5)),
        Some(5)
    );
    // Deleted last in the mutation: nothing to read.
    assert_eq!(
        v(t.mutate(b"a").incr("c", b"n", 1).delete_cell("c", b"n", 0)),
        None
    );
    // The next increment starts from 0 again.
    assert_eq!(v(t.mutate(b"a").incr("c", b"n", 4)), Some(4));
}

/// A bucket returns its own value; one already past the family's TTL at the commit returns
/// `None` (its operand is still written).
#[test]
fn buckets_return_their_own_value() {
    // A real clock: bucket timestamps are wall-clock microseconds.
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("b.phdb"), Options::default()).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("b", Family::counter().ttl(Duration::from_secs(3_600)))
        .create()
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let hour = 3_600_000_000u64;
    for want in [2, 5] {
        let r = t
            .mutate(b"h")
            .incr_at("b", b"n", now - hour / 2, if want == 2 { 2 } else { 3 })
            .incr_at("b", b"n", now - hour / 4, 1)
            .commit_returning()
            .unwrap();
        assert_eq!(r.value_at("b", b"n", now - hour / 2).unwrap(), Some(want));
        assert_eq!(r.iter().count(), 2);
    }
    let r = t
        .mutate(b"h")
        .incr_at("b", b"n", now - 2 * hour, 7)
        .commit_returning()
        .unwrap();
    assert_eq!(r.value_at("b", b"n", now - 2 * hour).unwrap(), None);
}

/// A cell the commit didn't write is an error, not a silent `None`.
#[test]
fn a_cell_not_written_is_an_error() {
    let (_db, t) = open(1444);
    let r = t
        .mutate(b"x")
        .incr("c", b"n", 1)
        .commit_returning()
        .unwrap();
    assert_eq!(
        r.value("c", b"other").unwrap_err().code(),
        ErrorCode::InvalidArgument
    );
    assert_eq!(
        r.value("nope", b"n").unwrap_err().code(),
        ErrorCode::InvalidArgument
    );
    assert!(r.info.seqno > 0);
}

#[cfg(feature = "async")]
#[test]
fn the_async_form_returns_the_same() {
    let (_db, t) = open(1445);
    for want in 1..=3 {
        let r = pigeonhole::doc_support::block_on(
            t.mutate(b"y").incr("c", b"n", 1).commit_returning_async(),
        )
        .unwrap();
        assert_eq!(r.value("c", b"n").unwrap(), Some(want));
    }
}
