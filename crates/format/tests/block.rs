//! Data and index blocks against a `BTreeMap` model: iteration, seek, row skipping,
//! sealing and the scan filter's pushdown rules.

mod common;

use std::collections::BTreeMap;
use std::ops::Bound;

use common::{Cell, cell, config, sized};
use pigeonhole_format::Cursor;
use pigeonhole_format::block::{Block, BlockBuilder, BlockKind, seal, verify};
use pigeonhole_format::compress::{Compression, decompress};
use pigeonhole_format::key::{decode_key, row_prefix_len};
use pigeonhole_format::scan::{QualifierFilter, ScanFilter};
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;

fn build_data(entries: &BTreeMap<Vec<u8>, Vec<u8>>, interval: usize) -> Vec<u8> {
    let mut b = BlockBuilder::data(interval);
    for (k, v) in entries {
        b.add(k, v).unwrap();
    }
    let est = b.estimated_len();
    let out = b.finish().to_vec();
    assert_eq!(est, out.len());
    out
}

fn entries() -> impl Strategy<Value = BTreeMap<Vec<u8>, Vec<u8>>> {
    btree_map(
        cell().prop_map(|c: Cell| c.encode()),
        vec(any::<u8>(), 0..20),
        0..sized(80, 6),
    )
}

fn collect<B: std::ops::Deref<Target = [u8]>>(
    it: &mut pigeonhole_format::block::BlockIter<B>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    out
}

proptest! {
    #![proptest_config(config(400))]

    #[test]
    fn data_block_matches_model(
        model in entries(),
        interval in 1usize..20,
        targets in vec(cell().prop_map(|c| c.encode()), 0..sized(10, 3)),
    ) {
        let bytes = build_data(&model, interval);
        let block = Block::new(bytes.as_slice()).unwrap();
        prop_assert_eq!(block.restart_count(), model.len().div_ceil(interval));
        let rows: std::collections::BTreeSet<_> =
            model.keys().map(|k| k[..row_prefix_len(k).unwrap()].to_vec()).collect();
        prop_assert_eq!(block.row_start_count(), rows.len());

        let mut it = block.into_cursor();
        prop_assert!(!it.valid());
        it.seek_to_first().unwrap();
        let all = collect(&mut it);
        let expect: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        prop_assert_eq!(&all, &expect);

        // Seek to every existing key, and to random targets.
        let existing = model.keys().cloned();
        for t in existing.chain(targets) {
            it.seek(&t).unwrap();
            let want = model.range(t.clone()..).next();
            match want {
                None => prop_assert!(!it.valid()),
                Some((k, v)) => {
                    prop_assert_eq!(it.key(), k.as_slice());
                    prop_assert_eq!(it.value(), v.as_slice());
                    let r = it.value_range();
                    prop_assert_eq!(&it.bytes()[r.start as usize..r.end as usize], v.as_slice());
                }
            }
        }

        // skip_row from every position lands on the next row's first entry.
        let keys: Vec<_> = model.keys().cloned().collect();
        for (i, k) in keys.iter().enumerate() {
            it.seek(k).unwrap();
            it.skip_row().unwrap();
            let row = &k[..row_prefix_len(k).unwrap()];
            match keys[i..].iter().find(|n| !n.starts_with(row)) {
                None => prop_assert!(!it.valid()),
                Some(n) => {
                    prop_assert_eq!(it.key(), n.as_slice());
                    // ...and iteration continues correctly from there.
                    let rest = collect(&mut it);
                    let pos = keys.iter().position(|x| x == n).unwrap();
                    prop_assert_eq!(rest.len(), keys.len() - pos);
                }
            }
        }
    }

    #[test]
    fn index_block_matches_model(model in btree_map(vec(any::<u8>(), 0..12), vec(any::<u8>(), 0..10), 0..sized(60, 10)), targets in vec(vec(any::<u8>(), 0..12), 0..sized(10, 3))) {
        let mut b = BlockBuilder::index();
        for (k, v) in &model {
            b.add(k, v).unwrap();
        }
        let bytes = b.finish().to_vec();
        let block = Block::new(bytes).unwrap();
        prop_assert_eq!(block.restart_count(), model.len());
        prop_assert_eq!(block.row_start_count(), 0);
        let mut it = block.into_cursor();
        it.seek_to_first().unwrap();
        let expect: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        prop_assert_eq!(collect(&mut it), expect);
        for t in targets {
            it.seek(&t).unwrap();
            prop_assert_eq!(it.valid().then(|| it.key().to_vec()), model.range(t..).next().map(|(k, _)| k.clone()));
        }
    }

    #[test]
    fn seal_verify_roundtrip(model in entries(), lz4 in any::<bool>()) {
        let logical = build_data(&model, 16);
        let codec = if lz4 { Compression::Lz4 } else { Compression::None };
        let mut physical = vec![0xAA; 3]; // seal appends
        let trailer = seal(BlockKind::Data, codec, &logical, &mut physical).unwrap();
        let (t, payload) = verify(&physical[3..]).unwrap();
        prop_assert_eq!(t, trailer);
        prop_assert_eq!(t.uncompressed_len as usize, logical.len());
        let mut back = vec![0; logical.len()];
        decompress(t.compression, payload, &mut back).unwrap();
        prop_assert_eq!(back, logical);
        // Any single flipped bit is caught.
        let mut bad = physical[3..].to_vec();
        let i = bad.len() / 2;
        bad[i] ^= 1;
        prop_assert!(verify(&bad).is_err());
    }

    /// `next_admissible` never skips an admitted entry, and `admits` follows D22.
    #[test]
    fn scan_filter_hints_are_safe(
        model in entries(),
        filter in scan_filter(),
    ) {
        let keys: Vec<_> = model.keys().cloned().collect();
        for (i, k) in keys.iter().enumerate() {
            let parts = decode_key(k).unwrap();
            let admitted = filter.admits(k);
            if parts.qualifier.is_none() {
                prop_assert!(admitted, "markers always pass");
            }
            let mut hint = Vec::new();
            let row = &k[..row_prefix_len(k).unwrap()];
            if filter.next_admissible(k, &mut hint) {
                prop_assert!(hint.as_slice() >= k.as_slice());
                if admitted {
                    prop_assert_eq!(&hint, k);
                }
                for skipped in keys[i..].iter().filter(|x| x.as_slice() < hint.as_slice()) {
                    prop_assert!(!filter.admits(skipped));
                }
            } else {
                prop_assert!(!admitted);
                for same_row in keys[i..].iter().filter(|x| x.starts_with(row)) {
                    prop_assert!(!filter.admits(same_row));
                }
            }
        }
    }
}

fn scan_filter() -> impl Strategy<Value = ScanFilter> {
    let q = || {
        vec(
            prop_oneof![Just(0u8), Just(1), Just(0xFF), Just(b'a')],
            0..3,
        )
    };
    let bound = move || {
        prop_oneof![
            q().prop_map(Bound::Included),
            q().prop_map(Bound::Excluded),
            Just(Bound::Unbounded)
        ]
    };
    (
        prop_oneof![
            Just(QualifierFilter::All),
            q().prop_map(QualifierFilter::Prefix),
            (bound(), bound()).prop_map(|(a, b)| QualifierFilter::Range(a, b)),
        ],
        proptest::option::of((0u64..4, 0u64..6)),
    )
        .prop_map(|(qualifiers, time_range)| {
            let mut f = ScanFilter::all();
            f.qualifiers = qualifiers;
            f.time_range = time_range;
            f
        })
}

#[test]
fn time_range_applies_to_puts_only() {
    use pigeonhole_format::key::{Kind, encode_key, encode_marker_key};
    let mut f = ScanFilter::all();
    f.time_range = Some((10, 20));
    let key = |ts, kind| {
        let mut k = Vec::new();
        encode_key(&mut k, b"r", b"q", ts, 1, kind).unwrap();
        k
    };
    assert!(f.admits(&key(10, Kind::Put)));
    assert!(!f.admits(&key(20, Kind::Put)));
    assert!(!f.admits(&key(5, Kind::Put)));
    for kind in [Kind::Merge, Kind::CellDelete, Kind::ColumnDelete] {
        assert!(f.admits(&key(5, kind)));
    }
    let mut m = Vec::new();
    encode_marker_key(&mut m, b"r", 5, 1).unwrap();
    assert!(f.admits(&m));
    assert!(!f.is_all());
    assert!(ScanFilter::all().is_all());
}

#[test]
fn builder_rejects_misuse_and_resets() {
    let mut b = BlockBuilder::data(4);
    assert!(b.is_empty());
    assert!(b.add(b"no terminator", b"").is_err());
    let k1 = Cell {
        row: b"a".to_vec(),
        qual: b"q".to_vec(),
        ts: 1,
        seqno: 1,
        kind: pigeonhole_format::Kind::Put,
    }
    .encode();
    let k2 = Cell {
        row: b"b".to_vec(),
        ..Cell {
            row: vec![],
            qual: b"q".to_vec(),
            ts: 1,
            seqno: 1,
            kind: pigeonhole_format::Kind::Put,
        }
    }
    .encode();
    b.add(&k2, b"").unwrap();
    assert!(b.add(&k1, b"").is_err(), "out of order");
    assert!(b.add(&k2, b"").is_err(), "duplicate");
    assert_eq!(b.last_key(), k2.as_slice());
    b.finish();
    assert!(b.add(&k1, b"").is_err(), "add after finish");
    b.reset();
    assert!(b.is_empty());
    // The first entry continues row b from the previous block: not a row start.
    let k3 = Cell {
        row: b"b".to_vec(),
        qual: b"r".to_vec(),
        ts: 1,
        seqno: 1,
        kind: pigeonhole_format::Kind::Put,
    }
    .encode();
    b.add(&k3, b"").unwrap();
    let block = Block::new(b.finish().to_vec()).unwrap();
    assert_eq!(block.row_start_count(), 0);
    let mut it = block.into_cursor();
    it.seek_to_first().unwrap();
    it.skip_row().unwrap();
    assert!(!it.valid());
    // An empty block is valid and empty.
    let mut empty = BlockBuilder::data(16);
    let mut it = Block::new(empty.finish().to_vec()).unwrap().into_cursor();
    it.seek_to_first().unwrap();
    assert!(!it.valid());
    it.seek(b"x").unwrap();
    assert!(!it.valid());
}
