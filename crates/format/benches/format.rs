//! Hot-path benchmarks: key encode/decode, block build/seek/iterate, WAL frames.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole_format::Cursor;
use pigeonhole_format::block::{Block, BlockBuilder};
use pigeonhole_format::key::{Kind, decode_key, encode_key};
use pigeonhole_format::wal::{
    BatchBuilder, Decoded, FRAME_SIZE, FrameDecoder, FrameEncoder, WalRecord,
};
use pigeonhole_format::{FamilyId, TableId};

fn keys(n: usize) -> Vec<Vec<u8>> {
    let mut keys: Vec<_> = (0..n)
        .map(|i| {
            let mut k = Vec::new();
            let row = format!("com.example/{:06}", i / 8);
            let qual = format!("links:{:03}", i % 8);
            encode_key(
                &mut k,
                row.as_bytes(),
                qual.as_bytes(),
                1_700_000_000_000_000,
                i as u64 + 1,
                Kind::Put,
            )
            .unwrap();
            k
        })
        .collect();
    keys.sort();
    keys
}

fn bench_keys(c: &mut Criterion) {
    let mut g = c.benchmark_group("key");
    let mut buf = Vec::with_capacity(128);
    g.bench_function("encode_key", |b| {
        b.iter(|| {
            buf.clear();
            encode_key(
                &mut buf,
                black_box(b"com.example/page\x00x"),
                black_box(b"links:42"),
                1_700_000_000_000_000,
                99,
                Kind::Put,
            )
            .unwrap();
            black_box(&buf);
        })
    });
    let key = buf.clone();
    g.bench_function("decode_key", |b| {
        b.iter(|| black_box(decode_key(black_box(&key)).unwrap()))
    });
    g.finish();
}

fn bench_blocks(c: &mut Criterion) {
    // About one 16 KiB data block: 64-byte keys, 32-byte values.
    let keys = keys(170);
    let value = [7u8; 32];
    let mut g = c.benchmark_group("block");
    let mut builder = BlockBuilder::data(16);
    g.throughput(Throughput::Elements(keys.len() as u64));
    g.bench_function("build", |b| {
        b.iter(|| {
            builder.reset();
            for k in &keys {
                builder.add(k, &value).unwrap();
            }
            black_box(builder.finish().len());
        })
    });
    builder.reset();
    for k in &keys {
        builder.add(k, &value).unwrap();
    }
    let bytes = builder.finish().to_vec();
    g.bench_function("iterate", |b| {
        b.iter(|| {
            let mut it = Block::new(bytes.as_slice()).unwrap().into_cursor();
            it.seek_to_first().unwrap();
            let mut n = 0;
            while it.valid() {
                n += it.key().len() + it.value().len();
                it.next().unwrap();
            }
            black_box(n)
        })
    });
    g.throughput(Throughput::Elements(1));
    let target = &keys[keys.len() * 2 / 3];
    let mut it = Block::new(bytes.as_slice()).unwrap().into_cursor();
    g.bench_function("seek", |b| {
        b.iter(|| {
            it.seek(black_box(target)).unwrap();
            black_box(it.value().len())
        })
    });
    g.bench_function("skip_row", |b| {
        b.iter(|| {
            it.seek_to_first().unwrap();
            it.skip_row().unwrap();
            black_box(it.key().len())
        })
    });
    g.finish();
}

fn bench_wal(c: &mut Criterion) {
    let mut batch = BatchBuilder::new();
    for k in keys(16) {
        batch
            .push(
                TableId(1),
                FamilyId(2),
                Kind::Put,
                &k[..20],
                b"links:1",
                None,
                &[0u8; 100],
            )
            .unwrap();
    }
    let mut record = Vec::new();
    WalRecord::Batch {
        seqno: 1,
        commit_ts: 2,
        batch: batch.batch(),
    }
    .encode(&mut record);
    let mut g = c.benchmark_group("wal");
    g.throughput(Throughput::Bytes(record.len() as u64));
    let mut out = Vec::with_capacity(4 * FRAME_SIZE);
    g.bench_function("frame_encode", |b| {
        b.iter(|| {
            out.clear();
            let mut enc = FrameEncoder::new(1, FRAME_SIZE as u64);
            black_box(enc.encode(black_box(&record), &mut out));
        })
    });
    let mut frame = vec![0u8; FRAME_SIZE];
    frame[..out.len()].copy_from_slice(&out);
    g.bench_function("frame_decode", |b| {
        let mut dec = FrameDecoder::new(1, FRAME_SIZE as u64);
        b.iter(|| {
            dec = FrameDecoder::new(1, FRAME_SIZE as u64);
            match dec.decode(black_box(&frame)).unwrap() {
                Some(Decoded::Record { .. }) => black_box(WalRecord::decode(dec.record()).unwrap()),
                other => panic!("{other:?}"),
            };
        })
    });
    g.finish();
}

criterion_group!(benches, bench_keys, bench_blocks, bench_wal);
criterion_main!(benches);
