//! Shared test helpers: seeded proptest configs and strategies biased towards the bytes and
//! numbers that break order-preserving encodings (0x00, 0xFF, prefixes, extremes).
// Shared by several test binaries, each using a subset of it.
#![allow(dead_code)]

pub mod harness;

use std::cmp::Reverse;

use pigeonhole_format::compress::Compression;
use pigeonhole_format::key::{Kind, encode_key, encode_marker_key};
use pigeonhole_format::manifest::{
    CachePriority, CompactionStyle, Edit, FamilyKind, FamilyOptions, SstMeta,
};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{BlobFileId, FamilyId, Lsn, SstId, StreamId, TableId, TabletId};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::{Config, RngSeed};

/// A proptest config with a fixed seed: `PROPTEST_RNG_SEED` if set, otherwise a fresh random
/// one. The seed is printed so a failing run (whose output cargo shows) can be replayed.
pub fn config(cases: u32) -> Config {
    let seed = std::env::var("PROPTEST_RNG_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            // Miri isolates the clock; a fixed seed is fine for its handful of cases.
            if cfg!(miri) {
                return 0x5EED;
            }
            use std::hash::{BuildHasher, Hasher};
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u128(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos()),
            );
            h.finish()
        });
    println!("proptest seed {seed}; replay with PROPTEST_RNG_SEED={seed}");
    // `PROPTEST_CASES` wins when set (CI's Miri job sets it). Otherwise Miri, which is about
    // 1000x slower, runs a handful of cases: enough to check the code paths for UB, while
    // inputs are also shrunk with `sized`.
    let cases = match std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None if cfg!(miri) => cases.min(4),
        None => cases,
    };
    Config {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..Config::default()
    }
}

/// `normal` in ordinary runs, `miri` under Miri: input sizes are shrunk there so the
/// interpreter finishes in minutes, while normal runs keep full coverage.
pub const fn sized(normal: usize, miri: usize) -> usize {
    if cfg!(miri) { miri } else { normal }
}

/// A row key or qualifier: usually short strings over {00, 01, FF, 'a'} so escapes and prefix
/// relationships are common; sometimes arbitrary bytes.
pub fn part() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        3 => vec(prop_oneof![Just(0u8), Just(0xFF), Just(1u8), Just(b'a')], 0..5),
        1 => vec(any::<u8>(), 0..12),
    ]
}

/// A timestamp or seqno, biased to the extremes and to collisions.
pub fn num() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0u64),
        Just(1),
        Just(u64::MAX),
        Just(u64::MAX - 1),
        0u64..4,
        any::<u64>(),
    ]
}

pub fn kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::Put),
        Just(Kind::Merge),
        Just(Kind::CellDelete),
        Just(Kind::ColumnDelete),
        Just(Kind::FamilyDelete),
    ]
}

/// One logical entry; `FamilyDelete` is a family marker and ignores `qual`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    pub row: Vec<u8>,
    pub qual: Vec<u8>,
    pub ts: u64,
    pub seqno: u64,
    pub kind: Kind,
}

pub fn cell() -> impl Strategy<Value = Cell> {
    (part(), part(), num(), num(), kind()).prop_map(|(row, qual, ts, seqno, kind)| {
        let qual = if kind == Kind::FamilyDelete {
            Vec::new()
        } else {
            qual
        };
        Cell {
            row,
            qual,
            ts,
            seqno,
            kind,
        }
    })
}

/// The logical sort key: row, markers before cells, qualifier, newest timestamp first,
/// newest seqno first, kind.
pub type Logical = (Vec<u8>, Option<Vec<u8>>, Reverse<u64>, Reverse<u64>, u8);

impl Cell {
    pub fn encode(&self) -> Vec<u8> {
        let mut k = Vec::new();
        if self.kind == Kind::FamilyDelete {
            encode_marker_key(&mut k, &self.row, self.ts, self.seqno).unwrap();
        } else {
            encode_key(
                &mut k, &self.row, &self.qual, self.ts, self.seqno, self.kind,
            )
            .unwrap();
        }
        k
    }

    pub fn logical(&self) -> Logical {
        let q = (self.kind != Kind::FamilyDelete).then(|| self.qual.clone());
        (
            self.row.clone(),
            q,
            Reverse(self.ts),
            Reverse(self.seqno),
            self.kind as u8,
        )
    }
}

pub fn extent() -> impl Strategy<Value = ExtentRef> {
    (0u8..=ExtentRef::MAX_CLASS, 1u64..1000).prop_map(|(size_class, k)| ExtentRef {
        page: k * (16 << size_class),
        size_class,
    })
}

pub fn bytes(max: usize) -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 0..max)
}

pub fn family_options() -> impl Strategy<Value = FamilyOptions> {
    (
        prop_oneof![
            Just(Compression::None),
            Just(Compression::Lz4),
            Just(Compression::Zstd)
        ],
        any::<i8>(),
        any::<u32>(),
        any::<u8>(),
        any::<u32>(),
        any::<u64>(),
        any::<u32>(),
        "[a-z._]{0,12}",
        prop_oneof![
            Just(CachePriority::Low),
            Just(CachePriority::Normal),
            Just(CachePriority::High)
        ],
        prop_oneof![
            Just(CompactionStyle::Leveled),
            Just(CompactionStyle::Tiered),
            Just(CompactionStyle::FifoByTime)
        ],
        prop_oneof![Just(FamilyKind::Standard), Just(FamilyKind::Counter)],
    )
        .prop_map(
            |(
                compression,
                compression_level,
                block_size,
                bloom_bits,
                max_versions,
                ttl_micros,
                blob_threshold,
                merge_operator,
                cache_priority,
                compaction,
                kind,
            )| {
                FamilyOptions::default()
                    .compression(compression)
                    .compression_level(compression_level)
                    .block_size(block_size)
                    .bloom_bits(bloom_bits)
                    .max_versions(max_versions)
                    .ttl_micros(ttl_micros)
                    .blob_threshold(blob_threshold)
                    .merge_operator(merge_operator)
                    .cache_priority(cache_priority)
                    .compaction(compaction)
                    .kind(kind)
            },
        )
}

pub fn sst_meta() -> impl Strategy<Value = SstMeta> {
    (
        any::<u64>(),
        extent(),
        any::<u64>(),
        bytes(40),
        bytes(40),
        any::<[u64; 6]>(),
    )
        .prop_map(|(id, extent, len, smallest_key, largest_key, n)| SstMeta {
            id: SstId(id),
            extent,
            len,
            smallest_key,
            largest_key,
            seqno_range: (n[0], n[1]),
            ts_range: (n[2], n[3]),
            entries: n[4],
            deletes: n[5],
        })
}

pub fn edit() -> impl Strategy<Value = Edit> {
    prop_oneof![
        (any::<u32>(), "\\PC{0,10}").prop_map(|(t, name)| Edit::CreateTable {
            table: TableId(t),
            name
        }),
        any::<u32>().prop_map(|t| Edit::DropTable { table: TableId(t) }),
        (any::<u32>(), any::<u32>(), "\\PC{0,10}", family_options()).prop_map(
            |(t, f, name, options)| Edit::PutFamily {
                table: TableId(t),
                family: FamilyId(f),
                name,
                options
            }
        ),
        (
            any::<u64>(),
            any::<u32>(),
            bytes(20),
            proptest::option::of(bytes(20))
        )
            .prop_map(|(tablet, t, start, end)| Edit::PutTablet {
                tablet: TabletId(tablet),
                table: TableId(t),
                start,
                end
            }),
        any::<u64>().prop_map(|t| Edit::DropTablet {
            tablet: TabletId(t)
        }),
        (any::<u64>(), any::<u32>(), any::<u8>(), sst_meta()).prop_map(|(t, f, level, meta)| {
            Edit::AddSst {
                tablet: TabletId(t),
                family: FamilyId(f),
                level,
                meta,
            }
        }),
        (any::<u64>(), any::<u32>(), any::<u64>()).prop_map(|(t, f, s)| Edit::RemoveSst {
            tablet: TabletId(t),
            family: FamilyId(f),
            sst: SstId(s)
        }),
        (any::<u64>(), any::<u32>(), any::<u64>()).prop_map(|(t, f, seqno)| Edit::SetFlushed {
            tablet: TabletId(t),
            family: FamilyId(f),
            seqno
        }),
        (any::<u32>(), any::<u64>()).prop_map(|(s, l)| Edit::WalCheckpoint {
            stream: StreamId(s),
            lsn: Lsn(l)
        }),
        (
            any::<u32>(),
            any::<u32>(),
            vec(extent(), 0..5),
            any::<u64>(),
            any::<u64>()
        )
            .prop_map(
                |(b, f, extents, total_bytes, live_bytes)| Edit::PutBlobFile {
                    blob_file: BlobFileId(b),
                    family: FamilyId(f),
                    extents,
                    total_bytes,
                    live_bytes
                }
            ),
        any::<u32>().prop_map(|b| Edit::DropBlobFile {
            blob_file: BlobFileId(b)
        }),
        (any::<u64>(), vec((any::<u32>(), any::<u64>()), 0..5)).prop_map(|(s, refs)| {
            Edit::SstBlobRefs {
                sst: SstId(s),
                refs: refs.into_iter().map(|(b, n)| (BlobFileId(b), n)).collect(),
            }
        }),
        (
            any::<u32>(),
            any::<u32>(),
            any::<u64>(),
            any::<u64>(),
            any::<u32>(),
            any::<u64>(),
            any::<u64>()
        )
            .prop_map(|(a, b, c, d, e, f, g)| Edit::Counters {
                next_table: a,
                next_family: b,
                next_tablet: c,
                next_sst: d,
                next_blob_file: e,
                seqno_ceiling: f,
                ts_floor: g
            }),
    ]
}
