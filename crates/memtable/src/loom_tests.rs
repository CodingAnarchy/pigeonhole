//! Loom model checks of the publication protocol: one writer, up to two readers, every
//! interleaving within the preemption bound. Under `cfg(loom)` the arena is a vector of loom
//! atomics (see `mem.rs`), so a reader that reads a node without the acquire that pairs with
//! the writer's release would observe stale bytes and fail these assertions.
//!
//! Run with `RUSTFLAGS="--cfg loom" cargo test --release -p pigeonhole-memtable --lib loom`.

use loom::thread;

use super::*;
use pigeonhole_format::{Kind, encode_key};

fn key(row: u8, seqno: u64) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(&mut k, &[row], b"", 0, seqno, Kind::Put).unwrap();
    k
}

fn model() -> loom::model::Builder {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b
}

type Entry = (Vec<u8>, Vec<u8>);

fn scan(reader: &MemtableReader) -> Vec<Entry> {
    let mut it = reader.iter();
    it.seek_to_first().unwrap();
    let mut out = Vec::new();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    out
}

/// Every scanned entry is a real one, in strictly ascending order, and no fewer than the
/// count the reader observed before scanning.
fn check_scan(seen: &[Entry], expected: &[Entry], at_least: usize) {
    assert!(
        seen.len() >= at_least,
        "count promised {at_least}, saw {}",
        seen.len()
    );
    assert!(
        seen.windows(2).all(|w| w[0].0 < w[1].0),
        "not sorted: {seen:?}"
    );
    for e in seen {
        assert!(expected.contains(e), "phantom entry {e:?}");
    }
}

#[test]
fn loom_writer_with_scanning_and_seeking_readers() {
    model().check(|| {
        let mut arena = ShardArena::new(ArenaRegion::heap(2048), 1024);
        let mut mt = Memtable::create(&mut arena).unwrap();
        let reader = mt.reader();
        // Inserted out of order so the second insert links in front of the first.
        let entries: Vec<Entry> = vec![
            (key(2, 1), b"\x00b".to_vec()),
            (key(1, 2), b"\x00a".to_vec()),
        ];
        let expected = entries.clone();

        let writer = thread::spawn(move || {
            for (k, v) in &entries {
                mt.insert(&mut arena, k, v).unwrap();
            }
            (mt, arena)
        });

        let scanner = {
            let reader = reader.clone();
            let expected = expected.clone();
            thread::spawn(move || {
                let n = reader.len();
                check_scan(&scan(&reader), &expected, n);
            })
        };

        let seeker = {
            let expected = expected.clone();
            thread::spawn(move || {
                // Strictly between the two keys (seqno 1 sorts after seqno 2 at equal
                // row and timestamp), so the lower bound is always the first insert, and
                // a seek that re-reads a link after deciding could land on the second.
                let target = key(1, 1);
                let mut it = reader.iter();
                it.seek(&target).unwrap();
                if it.valid() {
                    assert!(it.key() >= &target[..], "landed before the target");
                    assert_eq!(it.key(), &expected[0].0[..]);
                    let e = (it.key().to_vec(), it.value().to_vec());
                    assert!(expected.contains(&e), "phantom entry {e:?}");
                    let big = it.value_slice();
                    assert_eq!(&*big, &e.1[..]);
                }
            })
        };

        let (mt, _arena) = writer.join().unwrap();
        scanner.join().unwrap();
        seeker.join().unwrap();
        let mut sorted = expected.clone();
        sorted.sort();
        assert_eq!(scan(&mt.reader()), sorted);
    });
}

#[test]
fn loom_count_publishes_every_counted_entry() {
    model().check(|| {
        let mut arena = ShardArena::new(ArenaRegion::heap(2048), 1024);
        let mut mt = Memtable::create(&mut arena).unwrap();
        let reader = mt.reader();
        let k = key(5, 1);
        let v = b"\x00five".to_vec();
        let (k2, v2) = (k.clone(), v.clone());

        let writer = thread::spawn(move || {
            mt.insert(&mut arena, &k2, &v2).unwrap();
            mt.freeze();
            mt
        });
        let probe = thread::spawn(move || {
            // A count of one promises the entry is reachable, with its bytes intact.
            if reader.len() == 1 {
                let mut it = reader.iter();
                it.seek(&k).unwrap();
                assert!(it.valid());
                assert_eq!(it.key(), &k[..]);
                assert_eq!(it.value(), &v[..]);
                it.next().unwrap();
                assert!(!it.valid());
            }
        });
        let mt = writer.join().unwrap();
        probe.join().unwrap();
        assert!(mt.is_frozen());
        assert_eq!(mt.reader().len(), 1);
    });
}

#[test]
fn loom_forward_seek_sees_an_entry_linked_after_the_finger() {
    model().check(|| {
        let mut arena = ShardArena::new(ArenaRegion::heap(2048), 1024);
        let mut mt = Memtable::create(&mut arena).unwrap();
        let (a, c, d) = (key(1, 1), key(3, 1), key(4, 1));
        mt.insert(&mut arena, &a, b"\x00a").unwrap();
        mt.insert(&mut arena, &d, b"\x00d").unwrap();
        let reader = mt.reader();
        let c2 = c.clone();

        let writer = thread::spawn(move || {
            // Linked between the finger (`a`) and the forward seek's lower bound (`d`).
            mt.insert(&mut arena, &c2, b"\x00c").unwrap();
            mt
        });
        let seeker = thread::spawn(move || {
            let mut it = reader.iter();
            // Lands on `d` and records `a` as the level-0 finger.
            it.seek(&key(2, 1)).unwrap();
            assert!(it.valid());
            // A count of three promises `c` is reachable.
            let n = reader.len();
            it.seek_forward(&c).unwrap();
            assert!(it.valid());
            if n == 3 {
                assert_eq!(
                    it.key(),
                    &c[..],
                    "the forward seek missed a published entry"
                );
            } else {
                assert!(
                    it.key() == &c[..] || it.key() == &d[..],
                    "landed on {:?}",
                    it.key()
                );
            }
        });
        let _mt = writer.join().unwrap();
        seeker.join().unwrap();
    });
}

/// The has-markers flag (ICR 0020): the writer notes a marker (Release) before linking it,
/// then publishes (the engine's visibility watermark, modelled as one flag). A reader that
/// takes its read point (the publish) or finds the marker itself, then loads the flag, sees
/// it set: a marker a read can see is never skipped.
#[test]
fn loom_a_visible_marker_is_never_missed() {
    use loom::sync::atomic::{AtomicBool, Ordering};

    model().check(|| {
        let mut arena = ShardArena::new(ArenaRegion::heap(2048), 1024);
        let mut mt = Memtable::create(&mut arena).unwrap();
        let reader = mt.reader();
        assert!(!reader.may_have_markers());
        let published = Arc::new(AtomicBool::new(false));
        let marker = key(1, 1);

        let writer = {
            let (published, marker) = (Arc::clone(&published), marker.clone());
            thread::spawn(move || {
                mt.note_marker();
                mt.insert(&mut arena, &marker, b"").unwrap();
                published.store(true, Ordering::Release);
                (mt, arena)
            })
        };
        let by_read_point = {
            let (reader, published) = (reader.clone(), Arc::clone(&published));
            thread::spawn(move || {
                if published.load(Ordering::Acquire) {
                    assert!(reader.may_have_markers(), "a published marker was skipped");
                }
            })
        };
        let by_search = {
            let reader = reader.clone();
            thread::spawn(move || {
                let mut it = reader.iter();
                it.seek(&marker).unwrap();
                if it.valid() {
                    assert!(reader.may_have_markers(), "a found marker was skipped");
                }
            })
        };
        by_read_point.join().unwrap();
        by_search.join().unwrap();
        let _ = writer.join().unwrap();
        assert!(reader.may_have_markers());
    });
}

/// D194: a reader skipping a column while the writer links newer versions of it (each with
/// a stale-tail entry) lands on the next column, never on a phantom or a version of the
/// skipped column, and sees every tail entry's target fully written.
#[test]
fn loom_skip_column_with_a_concurrent_writer() {
    fn version(q: u8, ts: u64) -> Vec<u8> {
        let mut k = Vec::new();
        encode_key(&mut k, b"r", &[q], ts, ts, Kind::Put).unwrap();
        k
    }
    model().check(|| {
        let mut arena = ShardArena::new(ArenaRegion::heap(4096), 1024);
        let mut mt = Memtable::create(&mut arena).unwrap().with_tail_index();
        mt.insert(&mut arena, &version(b'a', 1), b"1").unwrap();
        mt.insert(&mut arena, &version(b'b', 1), b"b").unwrap();
        let reader = mt.reader();
        let column = {
            let k = version(b'a', 0);
            k[..k.len() - pigeonhole_format::key::SUFFIX_LEN].to_vec()
        };
        let r = thread::spawn(move || {
            let mut it = reader.iter();
            it.seek_to_first().unwrap();
            assert_eq!(&it.key()[..column.len()], &column[..]);
            if it.skip_column(&column).unwrap() {
                assert_eq!(it.key(), &version(b'b', 1)[..]);
                assert_eq!(it.value(), b"b");
            }
        });
        mt.insert(&mut arena, &version(b'a', 2), b"2").unwrap();
        mt.insert(&mut arena, &version(b'a', 3), b"3").unwrap();
        r.join().unwrap();
    });
}
