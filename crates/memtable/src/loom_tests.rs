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
