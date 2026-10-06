//! Shared helpers: random cell data, building an SST on `SimVfs`, and a reference model.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_format::compress::Compression;
use pigeonhole_format::key::{Kind, encode_key, encode_marker_key, row_prefix_len};
use pigeonhole_format::manifest::{FamilyOptions, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{Cursor, FamilyId, SstId, TableId, TabletId};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{FileRef, OpenOptions, Vfs};
use pigeonhole_sst::{
    QualifierFilter, ScanFilter, SstIter, SstReader, SstWriter, SstWriterOptions,
};
use proptest::prelude::*;

/// Sorted internal key -> stored value.
pub type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// One generated cell before encoding.
#[derive(Debug, Clone)]
pub struct RawCell {
    pub row: Vec<u8>,
    pub qualifier: Vec<u8>,
    pub ts: u64,
    pub seqno: u64,
    pub kind: Kind,
    pub value: Vec<u8>,
}

/// Bytes from a small alphabet that includes the escape-relevant 0x00, 0x01 and 0xFF.
pub fn part(max: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        prop::sample::select(vec![0u8, 1, b'a', b'b', b'c', 0xFF]),
        0..=max,
    )
}

pub fn raw_cell() -> impl Strategy<Value = RawCell> {
    let kind = prop::sample::select(vec![
        Kind::Put,
        Kind::Put,
        Kind::Put,
        Kind::Merge,
        Kind::CellDelete,
        Kind::ColumnDelete,
        Kind::FamilyDelete,
    ]);
    let value = prop_oneof![
        8 => prop::collection::vec(any::<u8>(), 0..24),
        1 => prop::collection::vec(any::<u8>(), 200..1500),
    ];
    (part(3), part(3), 0u64..6, 1u64..1000, kind, value).prop_map(
        |(row, qualifier, ts, seqno, kind, value)| RawCell {
            row,
            qualifier,
            ts,
            seqno,
            kind,
            value: if kind.is_delete() { Vec::new() } else { value },
        },
    )
}

pub fn encode(c: &RawCell) -> Vec<u8> {
    let mut k = Vec::new();
    if c.kind == Kind::FamilyDelete {
        encode_marker_key(&mut k, &c.row, c.ts, c.seqno).unwrap();
    } else {
        encode_key(&mut k, &c.row, &c.qualifier, c.ts, c.seqno, c.kind).unwrap();
    }
    k
}

pub fn model(cells: &[RawCell]) -> Model {
    cells.iter().map(|c| (encode(c), c.value.clone())).collect()
}

/// Writer options exercising many blocks, partitions and restart layouts.
#[derive(Debug, Clone)]
pub struct Layout {
    pub block_size: usize,
    pub restart_interval: usize,
    pub compression: Compression,
    pub bloom_bits: u8,
}

pub fn layout() -> impl Strategy<Value = Layout> {
    (
        prop::sample::select(vec![64usize, 256, 1024, 16 * 1024]),
        prop::sample::select(vec![1usize, 2, 16]),
        prop::sample::select(vec![Compression::None, Compression::Lz4]),
        prop::sample::select(vec![0u8, 4, 10]),
    )
        .prop_map(
            |(block_size, restart_interval, compression, bloom_bits)| Layout {
                block_size,
                restart_interval,
                compression,
                bloom_bits,
            },
        )
}

pub fn options(l: &Layout) -> SstWriterOptions {
    let mut o = SstWriterOptions::for_family(
        &FamilyOptions::default(),
        TableId(1),
        FamilyId(2),
        TabletId(3),
    );
    o.block_size = l.block_size;
    o.restart_interval = l.restart_interval;
    o.compression = l.compression;
    o.bloom_bits = l.bloom_bits;
    o.merge_operator = "pigeonhole.i64_add".into();
    o.created_micros = 1_700_000_000_000_000;
    o
}

/// The extent every test SST is written to (4 MiB at page 1024).
pub const EXTENT: ExtentRef = ExtentRef {
    page: 1024,
    size_class: 6,
};

/// A fresh simulated file, sized to hold `extent`, with the size made durable (as the pager
/// does when it allocates).
pub fn sim_file(seed: u64, extent: ExtentRef) -> (Arc<SimVfs>, FileRef) {
    let vfs = SimVfs::new(seed);
    let file = vfs
        .open("/db/data.phdb".as_ref(), OpenOptions::read_write_create())
        .unwrap();
    vfs.sync_dir("/db".as_ref()).unwrap();
    file.set_len(extent.offset() + extent.len()).unwrap();
    file.sync_all().unwrap();
    (vfs, file)
}

pub fn write_sst(file: &FileRef, extent: ExtentRef, m: &Model, l: &Layout) -> SstMeta {
    let mut w = SstWriter::new(file.clone(), extent, SstId(77), options(l));
    for (k, v) in m {
        w.add(k, v).unwrap();
    }
    assert_eq!(w.entries(), m.len() as u64);
    w.finish().unwrap()
}

pub fn open(file: &FileRef, meta: &SstMeta) -> Arc<SstReader> {
    let cache = Arc::new(BlockCache::new(8 << 20, 2));
    Arc::new(SstReader::open(file.clone(), meta, cache, Priority::Normal).unwrap())
}

pub fn scan(it: &mut SstIter) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    it.seek_to_first().unwrap();
    while it.valid() {
        assert_eq!(&it.value_cell()[..], it.value());
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    out
}

pub fn scan_filter() -> impl Strategy<Value = ScanFilter> {
    let bound = || {
        prop_oneof![
            Just(Bound::Unbounded),
            part(2).prop_map(Bound::Included),
            part(2).prop_map(Bound::Excluded),
        ]
    };
    let qualifiers = prop_oneof![
        1 => Just(QualifierFilter::All),
        2 => part(2).prop_map(QualifierFilter::Prefix),
        2 => (bound(), bound()).prop_map(|(lo, hi)| QualifierFilter::Range(lo, hi)),
    ];
    let time = prop_oneof![Just(None), (0u64..6, 0u64..7).prop_map(Some)];
    (qualifiers, time).prop_map(|(qualifiers, time_range)| {
        let mut f = ScanFilter::all();
        f.qualifiers = qualifiers;
        f.time_range = time_range;
        f
    })
}

/// The row prefix (escaped row plus terminator) of an internal key.
pub fn row_of(key: &[u8]) -> &[u8] {
    &key[..row_prefix_len(key).unwrap()]
}
