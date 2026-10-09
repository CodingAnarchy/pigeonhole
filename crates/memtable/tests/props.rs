//! Property test: a memtable agrees with a `BTreeMap` on scans and lower-bound seeks for
//! arbitrary entries (proptest prints its seed on failure).

use std::collections::BTreeMap;

use pigeonhole_format::{Cursor, Kind, encode_key};
use pigeonhole_memtable::{ArenaRegion, Memtable, ShardArena};
use proptest::collection::vec;
use proptest::prelude::*;

/// Fewer, smaller cases under Miri (interpreted; each case costs seconds).
const MAX_ENTRIES: usize = if cfg!(miri) { 40 } else { 150 };
const MAX_TARGETS: usize = if cfg!(miri) { 6 } else { 20 };

fn kind(k: u8) -> Kind {
    match k % 4 {
        0 => Kind::Put,
        1 => Kind::Merge,
        2 => Kind::CellDelete,
        _ => Kind::ColumnDelete,
    }
}

proptest! {
    // `PROPTEST_CASES` is honored (the default config reads it); Miri caps it so a local
    // run without the variable stays short.
    #![proptest_config({
        let default = ProptestConfig::default();
        ProptestConfig {
            cases: if cfg!(miri) { default.cases.min(4) } else { default.cases },
            ..default
        }
    })]

    #[test]
    fn matches_a_btreemap(
        entries in vec(
            (vec(any::<u8>(), 0..6), vec(any::<u8>(), 0..4), 0u64..4, any::<u8>(), vec(any::<u8>(), 0..40)),
            0..MAX_ENTRIES,
        ),
        targets in vec(vec(any::<u8>(), 0..26), 0..MAX_TARGETS),
    ) {
        let mut arena = ShardArena::new(ArenaRegion::heap(128 * 1024), 4096);
        let mut mt = Memtable::create(&mut arena).unwrap();
        let mut model = BTreeMap::new();
        for (i, (row, qual, ts, k, value)) in entries.iter().enumerate() {
            let mut key = Vec::new();
            encode_key(&mut key, row, qual, *ts, i as u64 + 1, kind(*k)).unwrap();
            mt.insert(&mut arena, &key, value).unwrap();
            model.insert(key, value.clone());
        }
        prop_assert_eq!(mt.len(), model.len());

        let reader = mt.reader();
        let mut it = reader.iter();
        it.seek_to_first().unwrap();
        for (k, v) in &model {
            prop_assert!(it.valid());
            prop_assert_eq!(it.key(), &k[..]);
            prop_assert_eq!(it.value(), &v[..]);
            it.next().unwrap();
        }
        prop_assert!(!it.valid());

        let mut probes: Vec<Vec<u8>> = targets;
        probes.extend(model.keys().cloned());
        for target in probes {
            it.seek(&target).unwrap();
            match model.range(target..).next() {
                Some((k, v)) => {
                    prop_assert!(it.valid());
                    prop_assert_eq!(it.key(), &k[..]);
                    prop_assert_eq!(it.value(), &v[..]);
                }
                None => prop_assert!(!it.valid()),
            }
        }
    }
}

proptest! {
    #![proptest_config({
        let default = ProptestConfig::default();
        ProptestConfig {
            cases: if cfg!(miri) { default.cases.min(4) } else { default.cases },
            ..default
        }
    })]

    /// Inserts in ascending runs (a row's cells, rising keys) take the insert splice, and
    /// a run's first key may fall anywhere; a repeated internal key, inserted right after its
    /// first copy, must take the search (the splice needs the last key strictly below). The
    /// memtable still agrees with a multimap (a repeated key's copies in any order), and its
    /// towers do too (a seek to every key lands on it).
    #[test]
    fn matches_a_btreemap_inserted_in_runs(
        entries in vec(
            (vec(any::<u8>(), 0..6), vec(any::<u8>(), 0..4), 0u64..4, any::<u8>(), vec(any::<u8>(), 0..40)),
            0..MAX_ENTRIES,
        ),
        runs in vec(1usize..20, 1..20),
        repeats in vec(any::<prop::sample::Index>(), 0..8),
    ) {
        let mut keyed: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .enumerate()
            .map(|(i, (row, qual, ts, k, value))| {
                let mut key = Vec::new();
                encode_key(&mut key, row, qual, *ts, i as u64 + 1, kind(*k)).unwrap();
                (key, value.clone())
            })
            .collect();
        // Sort consecutive stretches of the input, of the lengths `runs` gives (cycling).
        let mut at = 0;
        for len in runs.iter().cycle() {
            if at >= keyed.len() {
                break;
            }
            let end = (at + len).min(keyed.len());
            keyed[at..end].sort();
            at = end;
        }
        // Repeat some keys, each right after its first copy, with a value of their own.
        if !keyed.is_empty() {
            for (n, r) in repeats.iter().enumerate() {
                let i = r.index(keyed.len());
                let (key, _) = keyed[i].clone();
                keyed.insert(i + 1, (key, vec![0xEE, n as u8]));
            }
        }
        let mut arena = ShardArena::new(ArenaRegion::heap(128 * 1024), 4096);
        let mut mt = Memtable::create(&mut arena).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
        for (key, value) in &keyed {
            mt.insert(&mut arena, key, value).unwrap();
            model.entry(key.clone()).or_default().push(value.clone());
        }
        prop_assert_eq!(mt.len(), keyed.len());
        let reader = mt.reader();
        let mut it = reader.iter();
        it.seek_to_first().unwrap();
        for (k, values) in &model {
            let mut seen = Vec::new();
            for _ in values {
                prop_assert!(it.valid());
                prop_assert_eq!(it.key(), &k[..]);
                seen.push(it.value().to_vec());
                it.next().unwrap();
            }
            let mut want = values.clone();
            want.sort();
            seen.sort();
            prop_assert_eq!(seen, want);
        }
        prop_assert!(!it.valid());
        for k in model.keys() {
            it.seek(k).unwrap();
            prop_assert!(it.valid());
            prop_assert_eq!(it.key(), &k[..]);
        }
    }
}

/// The arena bound the engine admits a batch by (issue #141): every entry (`e`, an upper
/// bound on its node: 84 bytes of header and tower plus key and value) costs `e + min(e,
/// chunk)` (the run tail it may leave behind), and each memtable touched one chunk more (its
/// last run's unused tail).
fn batch_bound(entries: &[(usize, usize)], touched: usize, chunk: usize) -> usize {
    entries
        .iter()
        .map(|&(_, e)| e + e.min(chunk))
        .sum::<usize>()
        + touched * chunk
}

proptest! {
    #![proptest_config({
        let default = ProptestConfig::default();
        ProptestConfig {
            cases: if cfg!(miri) { default.cases.min(4) } else { default.cases },
            ..default
        }
    })]

    /// Whatever the memtables already hold and wherever their runs end, a batch uses at most
    /// `batch_bound` of the free arena, and it never fails for want of space when the bound
    /// fits the free bytes (every entry within a chunk) or the largest free run (otherwise).
    #[test]
    fn a_batch_stays_within_the_engine_bound(
        before in vec((0usize..4, 0usize..1500, 0u8..12), 0..200),
        batch in vec((0usize..4, 0usize..8000), 1..8),
    ) {
        const CHUNK: usize = 1024;
        let mut arena = ShardArena::new(ArenaRegion::heap(128 * CHUNK), CHUNK);
        let mut mts: Vec<Memtable> = (0..4).map(|_| Memtable::create(&mut arena).unwrap()).collect();
        let mut seq = 1u64;
        let mut put = |arena: &mut ShardArena, mts: &mut [Memtable], m: usize, len: usize| {
            let mut k = Vec::new();
            encode_key(&mut k, &seq.to_be_bytes(), b"q", 0, seq, Kind::Put).unwrap();
            seq += 1;
            let r = mts[m].insert(arena, &k, &vec![7u8; len]);
            (r, 84 + k.len() + len)
        };
        // Inserts, and memtables retired and replaced (freeing their chunks wherever they
        // were), so the free space is fragmented.
        for &(m, len, op) in &before {
            if op == 0 {
                let fresh = match Memtable::create(&mut arena) {
                    Ok(fresh) => fresh,
                    Err(_) => continue,
                };
                let old = std::mem::replace(&mut mts[m], fresh);
                arena.reclaim(old.retire());
            } else {
                let _ = put(&mut arena, &mut mts, m, len);
            }
        }
        let (free, run) = (arena.free_bytes(), arena.largest_free_run());
        // Every key here has the same length (an 8-byte row).
        let mut k = Vec::new();
        encode_key(&mut k, &0u64.to_be_bytes(), b"q", 0, 0, Kind::Put).unwrap();
        let sized: Vec<(usize, usize)> = batch
            .iter()
            .map(|&(m, len)| (m, 84 + k.len() + len))
            .collect();
        let mut touched: Vec<usize> = batch.iter().map(|&(m, _)| m).collect();
        touched.sort_unstable();
        touched.dedup();
        let bound = batch_bound(&sized, touched.len(), CHUNK);
        // The engine's admission (issue #141): the bound fits the free bytes, and when an
        // entry spans chunks, a block of the largest entry's size (plus the 64-byte prefix of
        // the arena's first chunk) is sure to be left for each such entry, whatever the
        // batch's other allocations (at most each entry's own chunks) take first.
        let sizes: Vec<usize> = sized.iter().map(|&(_, e)| (e + 64).div_ceil(CHUNK)).collect();
        let k = sizes.iter().copied().max().unwrap_or(1);
        let small = k <= 1;
        let min_large = sizes.iter().copied().filter(|&c| c > 1).min().unwrap_or(0);
        let blocks = arena.blocks_left(k, sizes.iter().sum::<usize>() - min_large);
        let admitted = bound <= free && (small || blocks >= 1);
        let _ = run;
        let mut failed = false;
        for &(m, len) in &batch {
            if put(&mut arena, &mut mts, m, len).0.is_err() {
                failed = true;
                break;
            }
        }
        if admitted {
            prop_assert!(!failed, "an admitted batch ran out of arena (bound {bound}, free {free}, run {run}, {blocks} blocks of {k})");
        }
        if !failed {
            let used = free - arena.free_bytes();
            prop_assert!(used <= bound, "used {used} > bound {bound}");
        }
    }
}
