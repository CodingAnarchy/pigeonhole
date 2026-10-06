//! Acceptance tests: write-then-read equals the input, filters have no false negatives, and
//! pushed-down filtering equals filtering afterwards (decision D22), for random data and
//! random writer layouts. Case counts honor `PROPTEST_CASES`.

mod common;

use std::sync::Arc;

use common::*;
use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_format::filter::{column_hash, row_hash};
use pigeonhole_format::key::{Kind, SUFFIX_LEN, decode_key, encode_key};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{Cursor, SstId};
use pigeonhole_sst::{Error, ReadOptions, ScanFilter, SstReader, SstWriter};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    First,
    Seek(Vec<u8>),
    Next,
    SkipRow,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        1 => Just(Op::First),
        3 => prop_oneof![
            raw_cell().prop_map(|c| encode(&c)),
            part(6),
        ]
        .prop_map(Op::Seek),
        6 => Just(Op::Next),
        3 => Just(Op::SkipRow),
    ]
}

/// Position in `entries` after `op` from position `at` (`entries.len()` = invalid).
fn model_step(entries: &[(Vec<u8>, Vec<u8>)], at: usize, op: &Op) -> usize {
    let n = entries.len();
    match op {
        Op::First => 0,
        Op::Seek(t) => entries.partition_point(|(k, _)| k.as_slice() < t.as_slice()),
        Op::Next => (at + 1).min(n),
        Op::SkipRow if at < n => {
            let row = row_of(&entries[at].0);
            (at + 1..n)
                .find(|&j| row_of(&entries[j].0) != row)
                .unwrap_or(n)
        }
        Op::SkipRow => n,
    }
}

proptest! {
    #[test]
    fn write_then_read_equals_input(
        cells in prop::collection::vec(raw_cell(), 0..300),
        l in layout(),
        targets in prop::collection::vec(part(5), 0..20),
    ) {
        let m = model(&cells);
        let (_vfs, file) = sim_file(1, EXTENT);
        let meta = write_sst(&file, EXTENT, &m, &l);
        let r = open(&file, &meta);

        let want: Vec<_> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        prop_assert_eq!(scan(&mut r.iter(ScanFilter::all(), ReadOptions::default())), want.clone());

        // Readahead, cache bypass and low priority read the same bytes.
        let mut opts = ReadOptions::default();
        opts.readahead_blocks = 3;
        opts.fill_cache = false;
        opts.priority = Priority::Low;
        prop_assert_eq!(scan(&mut r.iter(ScanFilter::all(), opts)), want.clone());

        let mut it = r.iter(ScanFilter::all(), ReadOptions::default());
        for t in targets.iter().chain(m.keys()) {
            it.seek(t).unwrap();
            let expect = m.range(t.clone()..).next();
            prop_assert_eq!(it.valid().then(|| (it.key(), it.value())),
                expect.map(|(k, v)| (k.as_slice(), v.as_slice())));
        }

        // Metadata and properties.
        let p = r.properties();
        prop_assert_eq!(meta.entries, m.len() as u64);
        prop_assert_eq!(p.entries, m.len() as u64);
        let parts: Vec<_> = m.keys().map(|k| decode_key(k).unwrap()).collect();
        let deletes = parts.iter().filter(|p| p.kind.is_delete()).count() as u64;
        prop_assert_eq!(meta.deletes, deletes);
        prop_assert_eq!(p.deletes, deletes);
        prop_assert_eq!(p.merges, parts.iter().filter(|p| p.kind == Kind::Merge).count() as u64);
        let mut rows: Vec<_> = m.keys().map(|k| row_of(k)).collect();
        rows.dedup();
        prop_assert_eq!(p.rows, rows.len() as u64);
        prop_assert_eq!(&meta.smallest_key, &m.keys().next().cloned().unwrap_or_default());
        prop_assert_eq!(&meta.largest_key, &m.keys().last().cloned().unwrap_or_default());
        prop_assert_eq!(&p.smallest_key, &meta.smallest_key);
        prop_assert_eq!(&p.largest_key, &meta.largest_key);
        if !parts.is_empty() {
            let range = |f: fn(&pigeonhole_format::key::KeyParts<'_>) -> u64| {
                (parts.iter().map(f).min().unwrap(), parts.iter().map(f).max().unwrap())
            };
            prop_assert_eq!(meta.seqno_range, range(|p| p.seqno));
            prop_assert_eq!(meta.ts_range, range(|p| p.ts));
        }
        prop_assert_eq!(p.raw_key_bytes, m.keys().map(|k| k.len() as u64).sum::<u64>());
        prop_assert_eq!(p.raw_value_bytes, m.values().map(|v| v.len() as u64).sum::<u64>());
        prop_assert_eq!(p.merge_operator.as_str(), "pigeonhole.i64_add");
        prop_assert_eq!(p.created_micros, 1_700_000_000_000_000);
        prop_assert_eq!(r.id(), SstId(77));
        prop_assert!(meta.len <= EXTENT.len());
    }

    /// Every cursor move on a filtered SST lands where the same move lands on the unfiltered
    /// entries filtered afterwards with `ScanFilter::admits`.
    #[test]
    fn pushdown_equals_filter_after(
        cells in prop::collection::vec(raw_cell(), 0..300),
        l in layout(),
        filter in scan_filter(),
        ops in prop::collection::vec(op(), 0..60),
    ) {
        let m = model(&cells);
        let (_vfs, file) = sim_file(2, EXTENT);
        let meta = write_sst(&file, EXTENT, &m, &l);
        let r = open(&file, &meta);

        let admitted: Vec<_> = m
            .iter()
            .filter(|(k, _)| filter.admits(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        prop_assert_eq!(scan(&mut r.iter(filter.clone(), ReadOptions::default())), admitted.clone());

        // Plain reads, and readahead into the cursor (no cache fill), where backward seeks
        // must drop stale read-ahead blocks.
        let mut readahead = ReadOptions::default();
        readahead.readahead_blocks = 3;
        readahead.fill_cache = false;
        for opts in [ReadOptions::default(), readahead] {
            let mut it = r.iter(filter.clone(), opts);
            let mut at = admitted.len();
            for op in &ops {
                match op {
                    Op::First => it.seek_to_first().unwrap(),
                    Op::Seek(t) => it.seek(t).unwrap(),
                    Op::Next => it.next().unwrap(),
                    Op::SkipRow => it.skip_row().unwrap(),
                }
                at = model_step(&admitted, at, op);
                let got = it.valid().then(|| (it.key().to_vec(), it.value().to_vec()));
                prop_assert_eq!(got.as_ref(), admitted.get(at), "after {:?} with {:?}", op, opts);
            }
        }
    }

    /// Filters never give a false negative, for rows, columns and family-marker keys.
    #[test]
    fn filters_have_no_false_negatives(
        cells in prop::collection::vec(raw_cell(), 1..300),
        mut l in layout(),
        bits in 1u8..16,
    ) {
        l.bloom_bits = bits;
        let m = model(&cells);
        let (_vfs, file) = sim_file(3, EXTENT);
        let meta = write_sst(&file, EXTENT, &m, &l);
        let r = open(&file, &meta);
        for k in m.keys() {
            let p = decode_key(k).unwrap();
            prop_assert!(r.may_contain_row(row_hash(p.row.as_escaped())));
            // The column prefix; for a family marker this is `row 00 01 00 00`.
            prop_assert!(r.may_contain_column(column_hash(&k[..k.len() - SUFFIX_LEN])));
            if p.kind == Kind::FamilyDelete {
                let mut raw = Vec::new();
                p.row.unescape_into(&mut raw);
                let mut marker = Vec::new();
                pigeonhole_format::key::encode_marker_prefix(&mut marker, &raw).unwrap();
                prop_assert!(r.may_contain_column(column_hash(&marker)));
            }
        }
    }

    /// `fits` is conservative: filling an extent while it says yes always finishes.
    #[test]
    fn fits_never_overflows_the_extent(
        cells in prop::collection::vec(raw_cell(), 1..400),
        l in layout(),
    ) {
        let extent = ExtentRef { page: 16, size_class: 0 };
        let (_vfs, file) = sim_file(4, extent);
        let mut w = SstWriter::new(file.clone(), extent, SstId(1), options(&l));
        let m = model(&cells);
        let mut added = Model::new();
        let mut cut = false;
        for (k, v) in &m {
            if !w.fits(k.len(), v.len()) {
                cut = true;
                break;
            }
            w.add(k, v).unwrap();
            added.insert(k.clone(), v.clone());
        }
        let meta = w.finish().unwrap();
        prop_assert!(meta.len <= extent.len());
        if cut {
            // Conservative, but not wasteful: a cut SST uses most of its extent.
            prop_assert!(meta.len * 2 > extent.len(), "cut at {} of {}", meta.len, extent.len());
        }
        let r = open(&file, &meta);
        let got = scan(&mut r.iter(ScanFilter::all(), ReadOptions::default()));
        prop_assert_eq!(got, added.into_iter().collect::<Vec<_>>());
    }
}

#[test]
fn empty_sst_opens_and_is_empty() {
    let (_vfs, file) = sim_file(5, EXTENT);
    for bloom_bits in [0, 10] {
        let l = Layout {
            block_size: 4096,
            restart_interval: 16,
            compression: Default::default(),
            bloom_bits,
        };
        let meta = write_sst(&file, EXTENT, &Model::new(), &l);
        let r = open(&file, &meta);
        assert!(scan(&mut r.iter(ScanFilter::all(), ReadOptions::default())).is_empty());
        let mut it = r.iter(ScanFilter::all(), ReadOptions::default());
        it.seek(b"anything").unwrap();
        assert!(!it.valid());
        it.next().unwrap();
        it.skip_row().unwrap();
        assert!(!it.valid());
        assert_eq!(meta.entries, 0);
        assert_eq!(r.may_contain_row(row_hash(b"x")), bloom_bits == 0);
    }
}

#[test]
fn rejects_out_of_order_and_malformed_keys() {
    let (_vfs, file) = sim_file(6, EXTENT);
    let l = Layout {
        block_size: 4096,
        restart_interval: 16,
        compression: Default::default(),
        bloom_bits: 10,
    };
    let mut w = SstWriter::new(file.clone(), EXTENT, SstId(1), options(&l));
    let mut a = Vec::new();
    encode_key(&mut a, b"r", b"b", 1, 1, Kind::Put).unwrap();
    let mut b = Vec::new();
    encode_key(&mut b, b"r", b"a", 1, 1, Kind::Put).unwrap();
    w.add(&a, b"\x00").unwrap();
    assert!(matches!(w.add(&a, b"\x00"), Err(Error::OutOfOrder)));
    assert!(matches!(w.add(&b, b"\x00"), Err(Error::OutOfOrder)));
    assert!(matches!(
        w.add(b"\xff not a key", b""),
        Err(Error::Format(_))
    ));
    assert_eq!(w.abandon(), EXTENT);
}

#[test]
fn a_full_extent_is_refused() {
    let extent = ExtentRef {
        page: 16,
        size_class: 0,
    };
    let (_vfs, file) = sim_file(7, extent);
    let l = Layout {
        block_size: 4096,
        restart_interval: 16,
        compression: pigeonhole_format::compress::Compression::None,
        bloom_bits: 10,
    };
    let mut w = SstWriter::new(file, extent, SstId(1), options(&l));
    let mut key = Vec::new();
    let value = vec![0u8; 1000];
    let mut err = None;
    for i in 0u32..200 {
        key.clear();
        encode_key(&mut key, &i.to_be_bytes(), b"q", 1, 1, Kind::Put).unwrap();
        if let Err(e) = w.add(&key, &value) {
            err = Some(e);
            break;
        }
    }
    let err = err.map_or_else(|| w.finish().map(|_| ()), Err);
    assert!(matches!(err, Err(Error::ExtentFull)), "{err:?}");
}

/// Long keys (beyond the cursor's inline key buffer) and values larger than a block.
#[test]
fn long_keys_and_huge_values_round_trip() {
    let mut m = Model::new();
    for i in 0u32..50 {
        let row = vec![b'r'; 300 + i as usize];
        let mut k = Vec::new();
        encode_key(
            &mut k,
            &row,
            &i.to_be_bytes(),
            9,
            u64::from(i) + 1,
            Kind::Put,
        )
        .unwrap();
        m.insert(k, vec![i as u8; if i % 7 == 0 { 70_000 } else { 10 }]);
    }
    let l = Layout {
        block_size: 1024,
        restart_interval: 4,
        compression: Default::default(),
        bloom_bits: 10,
    };
    let (_vfs, file) = sim_file(8, EXTENT);
    let meta = write_sst(&file, EXTENT, &m, &l);
    let r = open(&file, &meta);
    let want: Vec<_> = m.clone().into_iter().collect();
    assert_eq!(
        scan(&mut r.iter(ScanFilter::all(), ReadOptions::default())),
        want
    );
    let mut it = r.iter(ScanFilter::all(), ReadOptions::default());
    for (k, v) in &m {
        it.seek(k).unwrap();
        assert_eq!((it.key(), it.value()), (&k[..], &v[..]));
    }
}

/// A value cell pins its block: it stays readable after the cursor, the reader and the cache
/// entry are gone.
#[test]
fn value_cells_outlive_everything() {
    let mut m = Model::new();
    let mut k = Vec::new();
    encode_key(&mut k, b"row", b"q", 1, 1, Kind::Put).unwrap();
    m.insert(k.clone(), b"\x00pinned".to_vec());
    let (_vfs, file) = sim_file(9, EXTENT);
    let l = Layout {
        block_size: 4096,
        restart_interval: 16,
        compression: Default::default(),
        bloom_bits: 10,
    };
    let meta = write_sst(&file, EXTENT, &m, &l);
    let cache = Arc::new(BlockCache::new(1 << 20, 1));
    let r = Arc::new(SstReader::open(file, &meta, cache.clone(), Priority::Normal).unwrap());
    let mut it = r.iter(ScanFilter::all(), ReadOptions::default());
    it.seek(&k).unwrap();
    let cell = it.value_cell();
    drop((it, r));
    cache.erase_files(&[pigeonhole_sst::sst_cache_file(SstId(77))]);
    assert_eq!(&cell[..], b"\x00pinned");
}

/// A row spanning many 64-byte blocks: `skip_row` leaves the first block, finds the row
/// continuing in the next one with no later row start there, and seeks past the row with
/// `escaped row ++ 00 02` through the index.
#[test]
fn skip_row_seeks_past_a_row_spanning_many_blocks() {
    let mut m = Model::new();
    let mut put = |row: &[u8], q: u32| {
        let mut k = Vec::new();
        encode_key(&mut k, row, &q.to_be_bytes(), 1, 1, Kind::Put).unwrap();
        m.insert(k, b"\x00value".to_vec());
    };
    put(b"a", 0);
    for q in 0..20 {
        put(b"wide", q);
    }
    // A later row that extends the wide row's bytes, so a sloppy "past the row" key would
    // land inside or beyond it.
    put(b"wide\x00", 0);
    put(b"wide\x01", 0);
    put(b"z", 0);
    let l = Layout {
        block_size: 64,
        restart_interval: 2,
        compression: pigeonhole_format::compress::Compression::None,
        bloom_bits: 0,
    };
    let (_vfs, file) = sim_file(10, EXTENT);
    let meta = write_sst(&file, EXTENT, &m, &l);
    let r = open(&file, &meta);
    assert!(
        r.properties().data_blocks >= 20,
        "{} blocks",
        r.properties().data_blocks
    );
    let rows: Vec<Vec<u8>> = {
        let mut v: Vec<Vec<u8>> = m.keys().map(|k| row_of(k).to_vec()).collect();
        v.dedup();
        v
    };
    let mut readahead = ReadOptions::default();
    readahead.readahead_blocks = 4;
    for opts in [ReadOptions::default(), readahead] {
        let mut it = r.iter(ScanFilter::all(), opts);
        it.seek_to_first().unwrap();
        let mut seen = Vec::new();
        while it.valid() {
            seen.push(row_of(it.key()).to_vec());
            it.skip_row().unwrap();
        }
        assert_eq!(seen, rows);
        // From the middle of the wide row too.
        let mut mid = Vec::new();
        encode_key(&mut mid, b"wide", &10u32.to_be_bytes(), 1, 1, Kind::Put).unwrap();
        it.seek(&mid).unwrap();
        it.skip_row().unwrap();
        assert_eq!(row_of(it.key()), &rows[2][..]);
    }
}
