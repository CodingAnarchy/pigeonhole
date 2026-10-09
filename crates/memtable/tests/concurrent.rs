//! Linearizability-style check of concurrent readers against a locked `BTreeMap`.
//!
//! The writer records each entry in the locked model as pending, inserts it into the
//! memtable, then marks it committed. A reader snapshots the committed entries (`before`),
//! reads the memtable, then snapshots every entry the writer has started (`after`), and
//! checks what it read against both. A scan must be strictly sorted, contain every entry of
//! `before` with its value, and contain nothing outside `after`. A seek for a key in
//! `before` must hit; a seek for a random target must land between the target's successor
//! in `after` (the newest possible) and its successor in `before` (the oldest possible).

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use pigeonhole_format::Cursor;
use pigeonhole_memtable::{ArenaRegion, Memtable, MemtableReader, ShardArena};

use common::{Entry, Rng};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// The locked model: every entry the writer has started, and whether its insert returned.
#[derive(Default)]
struct Ledger {
    entries: BTreeMap<Vec<u8>, (Vec<u8>, bool)>,
}

impl Ledger {
    fn snapshot(&self, committed_only: bool) -> Model {
        self.entries
            .iter()
            .filter(|(_, (_, committed))| *committed || !committed_only)
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect()
    }
}

fn check_scan(scan: &[Entry], before: &Model, after: &Model) {
    if let Some(w) = scan.windows(2).find(|w| w[0].0 >= w[1].0) {
        panic!(
            "scan is not strictly sorted at index {}: {:?} then {:?}",
            scan.iter().position(|e| e == &w[0]).unwrap(),
            w[0],
            w[1]
        );
    }
    for (k, v) in before {
        let i = scan
            .binary_search_by(|(sk, _)| sk.cmp(k))
            .unwrap_or_else(|_| panic!("entry present before the scan is missing"));
        assert_eq!(&scan[i].1, v, "value differs");
    }
    for (k, v) in scan {
        assert_eq!(
            after.get(k),
            Some(v),
            "scanned entry was never inserted (or differs)"
        );
    }
}

/// Seeks every `before` key (must hit) and some random targets, returning where each random
/// target landed for `check_lower_bounds` once the `after` snapshot exists.
fn run_seeks(
    reader: &MemtableReader,
    before: &Model,
    rng: &mut Rng,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut it = reader.iter();
    let keys: Vec<&Vec<u8>> = before.keys().collect();
    let mut landed = Vec::new();
    for _ in 0..20 {
        if !keys.is_empty() {
            let k = keys[rng.below(keys.len())];
            it.seek(k).unwrap();
            assert!(it.valid(), "seek missed a key present before the seek");
            assert_eq!(it.key(), &k[..]);
            assert_eq!(it.value(), &before[k][..]);
        }
        let (target, _) = common::random_entry(rng, 0);
        it.seek(&target).unwrap();
        let at = it.valid().then(|| it.key().to_vec());
        landed.push((target, at));
    }
    landed
}

fn check_lower_bounds(landed: &[(Vec<u8>, Option<Vec<u8>>)], before: &Model, after: &Model) {
    for (target, at) in landed {
        let succ_before = before.range(target.clone()..).next().map(|(k, _)| k);
        let succ_after = after.range(target.clone()..).next().map(|(k, _)| k);
        match (at, succ_before, succ_after) {
            (None, None, _) => {}
            (None, Some(_), _) => panic!("seek found nothing but a successor existed"),
            (Some(_), _, None) => panic!("seek landed on an entry that was never inserted"),
            (Some(l), sb, Some(sa)) => {
                assert!(l >= target, "landed before the target");
                assert!(l >= sa, "landed before the successor in `after`");
                if let Some(sb) = sb {
                    assert!(l <= sb, "landed past the successor in `before`");
                }
            }
        }
    }
}

#[test]
fn readers_see_consistent_prefixes_of_the_writer() {
    readers_see_consistent_prefixes(None);
}

/// The same with the writer inserting in ascending runs of 64 (a row's cells, rising keys):
/// most inserts take the insert splice (no search) while readers scan and seek, and each
/// run's first key searches.
#[test]
fn readers_see_consistent_prefixes_of_a_writer_in_key_order() {
    readers_see_consistent_prefixes(Some(64));
}

/// Readers race one writer inserting the seed's entries, in their random order or sorted in
/// runs of `sorted_runs`.
fn readers_see_consistent_prefixes(sorted_runs: Option<usize>) {
    let seed = common::seed();
    let (n, readers, arena_len, chunk) = if cfg!(miri) {
        (60, 2, 256 * 1024, 16 * 1024)
    } else {
        (20_000, 3, 32 << 20, ShardArena::DEFAULT_CHUNK)
    };
    let mut arena = ShardArena::new(ArenaRegion::heap(arena_len), chunk);
    let mut mt = Memtable::create(&mut arena).unwrap();
    let reader = mt.reader();
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let done = Arc::new(AtomicBool::new(false));
    let mut entries = common::entries(seed, n);
    if let Some(run) = sorted_runs {
        for chunk in entries.chunks_mut(run) {
            chunk.sort();
        }
    }
    let mut final_entries: Vec<Entry> = entries.clone();
    final_entries.sort();

    let writer = {
        let ledger = Arc::clone(&ledger);
        let done = Arc::clone(&done);
        thread::spawn(move || {
            for (k, v) in &entries {
                ledger
                    .lock()
                    .unwrap()
                    .entries
                    .insert(k.clone(), (v.clone(), false));
                mt.insert(&mut arena, k, v).unwrap();
                ledger.lock().unwrap().entries.get_mut(k).unwrap().1 = true;
            }
            done.store(true, Ordering::Release);
            mt
        })
    };

    let handles: Vec<_> = (0..readers)
        .map(|i| {
            let reader = reader.clone();
            let ledger = Arc::clone(&ledger);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                let mut rng = Rng::new(seed ^ (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
                let committed = || ledger.lock().unwrap().snapshot(true);
                let started = || ledger.lock().unwrap().snapshot(false);
                let mut rounds = 0;
                loop {
                    let finished = done.load(Ordering::Acquire);

                    let before = committed();
                    let scan = common::scan(&reader);
                    let after = started();
                    check_scan(&scan, &before, &after);

                    let before = committed();
                    let landed = run_seeks(&reader, &before, &mut rng);
                    let after = started();
                    check_lower_bounds(&landed, &before, &after);

                    rounds += 1;
                    if finished {
                        break;
                    }
                }
                rounds
            })
        })
        .collect();

    let mt = writer.join().unwrap();
    for h in handles {
        assert!(h.join().unwrap() >= 1);
    }
    assert_eq!(mt.len(), n);
    assert_eq!(reader.len(), n);
    assert!(common::scan(&reader) == final_entries);
}
