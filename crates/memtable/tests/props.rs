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
