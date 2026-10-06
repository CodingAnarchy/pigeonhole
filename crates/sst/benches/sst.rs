//! SST throughput and latency: build (MB/s of raw keys and values), point gets with a hot
//! cache and with no cache (every block read, verified and decompressed), a full scan from
//! cache (decoded MB/s; the spec's target is > 1 GB/s per core), and a scan with a qualifier
//! filter pushed down. Files live on the in-memory `SimVfs`, so "cold" measures the decode
//! path, not a disk.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_format::key::{Kind, SUFFIX_LEN, encode_key};
use pigeonhole_format::manifest::{FamilyOptions, SstMeta};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{Cursor, FamilyId, SstId, TableId, TabletId};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{FileRef, OpenOptions, Vfs};
use pigeonhole_sst::{
    QualifierFilter, ReadOptions, ScanFilter, SstReader, SstWriter, SstWriterOptions,
};

const ROWS: u32 = 50_000;
const COLUMNS: u32 = 4;
const EXTENT: ExtentRef = ExtentRef {
    page: 16384,
    size_class: 10,
};

/// Single-family rows of four columns (`meta:a`, `meta:b`, `v:0`, `v:1`), 64-byte
/// incompressible values.
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

fn entries() -> Entries {
    let quals: [&[u8]; COLUMNS as usize] = [b"meta:a", b"meta:b", b"v:0", b"v:1"];
    let mut out = Vec::new();
    for r in 0..ROWS {
        let row = format!("user:{r:010}");
        for (c, q) in quals.iter().enumerate() {
            let mut k = Vec::new();
            encode_key(
                &mut k,
                row.as_bytes(),
                q,
                1_000,
                u64::from(r) + 1,
                Kind::Put,
            )
            .unwrap();
            // Pseudo-random bytes: LZ4 cannot shrink them, so throughput is not inflated
            // by compression (the ratio is printed at startup).
            let mut x = (u64::from(r) << 2 | c as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut v = vec![0u8];
            v.extend((0..63).map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            }));
            out.push((k, v));
        }
    }
    out
}

fn raw_bytes(e: &[(Vec<u8>, Vec<u8>)]) -> u64 {
    e.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
}

fn build(file: &FileRef, e: &[(Vec<u8>, Vec<u8>)]) -> SstMeta {
    let o = SstWriterOptions::for_family(
        &FamilyOptions::default(),
        TableId(1),
        FamilyId(1),
        TabletId(1),
    );
    let mut w = SstWriter::new(file.clone(), EXTENT, SstId(1), o);
    for (k, v) in e {
        w.add(k, v).unwrap();
    }
    w.finish().unwrap()
}

fn setup() -> (FileRef, SstMeta, Entries) {
    let vfs = SimVfs::new(1);
    let file = vfs
        .open("/db".as_ref(), OpenOptions::read_write_create())
        .unwrap();
    let e = entries();
    let meta = build(&file, &e);
    (file, meta, e)
}

fn bench(c: &mut Criterion) {
    let (file, meta, e) = setup();
    let bytes = raw_bytes(&e);
    eprintln!(
        "{} cells, {} raw bytes, SST {} bytes (ratio {:.2})",
        e.len(),
        bytes,
        meta.len,
        meta.len as f64 / bytes as f64
    );

    let mut g = c.benchmark_group("build");
    g.throughput(Throughput::Bytes(bytes));
    g.sample_size(10);
    g.bench_function("lz4_16k", |b| b.iter(|| black_box(build(&file, &e))));
    g.finish();

    let hot = Arc::new(
        SstReader::open(
            file.clone(),
            &meta,
            Arc::new(BlockCache::new(256 << 20, 0)),
            Priority::Normal,
        )
        .unwrap(),
    );
    let cold = Arc::new(
        SstReader::open(
            file.clone(),
            &meta,
            Arc::new(BlockCache::disabled()),
            Priority::Normal,
        )
        .unwrap(),
    );
    let targets: Vec<Vec<u8>> = e
        .iter()
        .step_by(97)
        .map(|(k, _)| k[..k.len() - SUFFIX_LEN].to_vec())
        .collect();
    let get = |r: &Arc<SstReader>, t: &[u8]| {
        let mut it = r.iter(ScanFilter::all(), ReadOptions::default());
        it.seek(t).unwrap();
        it.value_cell()
    };
    for t in &targets {
        get(&hot, t); // warm
    }
    let mut g = c.benchmark_group("point_get");
    let mut i = 0;
    g.bench_function("hot", |b| {
        b.iter(|| {
            i = (i + 1) % targets.len();
            black_box(get(&hot, &targets[i]))
        })
    });
    g.bench_function("cold", |b| {
        b.iter(|| {
            i = (i + 1) % targets.len();
            black_box(get(&cold, &targets[i]))
        })
    });
    g.finish();

    let scan = |filter: ScanFilter| {
        let mut it = hot.iter(filter, ReadOptions::default());
        it.seek_to_first().unwrap();
        let mut n = 0usize;
        while it.valid() {
            n += it.key().len() + it.value().len();
            it.next().unwrap();
        }
        n
    };
    scan(ScanFilter::all()); // warm every block
    let mut g = c.benchmark_group("scan");
    g.throughput(Throughput::Bytes(bytes));
    g.sample_size(20);
    g.bench_function("full_from_cache", |b| {
        b.iter(|| black_box(scan(ScanFilter::all())))
    });
    let mut meta_only = ScanFilter::all();
    meta_only.qualifiers = QualifierFilter::Prefix(b"meta:".to_vec());
    g.bench_function("qualifier_prefix_from_cache", |b| {
        b.iter_batched(
            || meta_only.clone(),
            |f| black_box(scan(f)),
            BatchSize::SmallInput,
        )
    });
    let mut readahead = ReadOptions::default();
    readahead.readahead_blocks = 8;
    readahead.fill_cache = false;
    g.bench_function("full_uncached_readahead8", |b| {
        b.iter(|| {
            let mut it = cold.iter(ScanFilter::all(), readahead);
            it.seek_to_first().unwrap();
            let mut n = 0usize;
            while it.valid() {
                n += it.value().len();
                it.next().unwrap();
            }
            black_box(n)
        })
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
