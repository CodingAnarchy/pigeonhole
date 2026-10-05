//! Insert and lookup latency on a memtable of one million entries (spec target: a memtable
//! lookup typically under 300 ns), plus a 10k-entry memtable that fits in cache.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole_format::{Cursor, Kind, encode_key};
use pigeonhole_memtable::{ArenaRegion, Memtable, ShardArena};

const MILLION: usize = 1_000_000;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// `n` cell keys with random 16-byte rows (so insertion order is random in key space) and
/// their 9-byte `I64` stored values.
fn entries(n: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut rng = Rng(0x5EED);
    let mut keys = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    for i in 0..n {
        let row = [rng.next().to_le_bytes(), rng.next().to_le_bytes()].concat();
        let mut key = Vec::new();
        encode_key(&mut key, &row, b"q", 1, i as u64 + 1, Kind::Put).unwrap();
        keys.push(key);
        let mut value = vec![1u8];
        value.extend((i as i64).to_le_bytes());
        values.push(value);
    }
    (keys, values)
}

fn build(keys: &[Vec<u8>], values: &[Vec<u8>]) -> (ShardArena, Memtable) {
    let mut arena = ShardArena::new(ArenaRegion::heap(256 << 20), ShardArena::DEFAULT_CHUNK);
    let mut mt = Memtable::create(&mut arena).unwrap();
    for (k, v) in keys.iter().zip(values) {
        mt.insert(&mut arena, k, v).unwrap();
    }
    (arena, mt)
}

fn bench(c: &mut Criterion) {
    let (keys, values) = entries(MILLION);

    let mut group = c.benchmark_group("insert");
    group.sample_size(10);
    group.throughput(Throughput::Elements(MILLION as u64));
    group.bench_function("1M random keys", |b| {
        b.iter_batched(
            || {
                let mut arena =
                    ShardArena::new(ArenaRegion::heap(256 << 20), ShardArena::DEFAULT_CHUNK);
                let mt = Memtable::create(&mut arena).unwrap();
                (arena, mt)
            },
            |(mut arena, mut mt)| {
                for (k, v) in keys.iter().zip(&values) {
                    mt.insert(&mut arena, k, v).unwrap();
                }
                black_box(mt.len())
            },
            BatchSize::PerIteration,
        )
    });
    group.finish();

    for (label, n) in [("1M entries", MILLION), ("10k entries", 10_000)] {
        let (_arena, mt) = build(&keys[..n], &values[..n]);
        let reader = mt.reader();
        // Probe keys in an order unrelated to insertion order.
        let mut rng = Rng(7);
        let hits: Vec<&Vec<u8>> = (0..4096).map(|_| &keys[rng.next() as usize % n]).collect();
        let misses: Vec<Vec<u8>> = (0..4096)
            .map(|_| {
                let mut k = hits[rng.next() as usize % 4096].clone();
                k[8] ^= 0x80; // somewhere inside the row: a key between real ones
                k
            })
            .collect();

        let mut group = c.benchmark_group(format!("seek/{label}"));
        let mut it = reader.iter();
        let mut i = 0;
        group.bench_function("hit (get)", |b| {
            b.iter(|| {
                i = (i + 1) & 4095;
                it.seek(black_box(hits[i])).unwrap();
                assert!(it.valid());
                black_box(it.value().len())
            })
        });
        group.bench_function("miss (lower bound)", |b| {
            b.iter(|| {
                i = (i + 1) & 4095;
                it.seek(black_box(&misses[i])).unwrap();
                black_box(it.valid())
            })
        });
        group.finish();

        let mut group = c.benchmark_group(format!("scan/{label}"));
        group.sample_size(10);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function("seek_to_first + next", |b| {
            b.iter(|| {
                let mut it = reader.iter();
                it.seek_to_first().unwrap();
                let mut count = 0usize;
                while it.valid() {
                    count += black_box(it.key()).len() & 1;
                    it.next().unwrap();
                }
                black_box(count)
            })
        });
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
