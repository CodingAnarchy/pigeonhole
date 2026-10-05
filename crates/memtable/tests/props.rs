//! Property test: a memtable agrees with a `BTreeMap` on scans and lower-bound seeks for
//! arbitrary entries (proptest prints its seed on failure).

use std::collections::BTreeMap;

use pigeonhole_format::{Cursor, Kind, encode_key};
use pigeonhole_memtable::{ArenaRegion, Memtable, ShardArena};
use proptest::collection::vec;
use proptest::prelude::*;

fn kind(k: u8) -> Kind {
    match k % 4 {
        0 => Kind::Put,
        1 => Kind::Merge,
        2 => Kind::CellDelete,
        _ => Kind::ColumnDelete,
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 3 } else { 256 },
        ..ProptestConfig::default()
    })]

    #[test]
    fn matches_a_btreemap(
        entries in vec(
            (vec(any::<u8>(), 0..6), vec(any::<u8>(), 0..4), 0u64..4, any::<u8>(), vec(any::<u8>(), 0..40)),
            0..150,
        ),
        targets in vec(vec(any::<u8>(), 0..26), 0..20),
    ) {
        let mut arena = ShardArena::new(ArenaRegion::heap(1 << 20), 4096);
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
