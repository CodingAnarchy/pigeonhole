//! Latency of the commit-path and snapshot-path operations on the heap-backed region:
//! `visible_seqno` (one cache line per shard), `reserve_seqnos`, `publish_pending`, the pin
//! handshake, `oldest_reader_pin` over a full slot table, and view publish/read.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use pigeonhole_format::shm::{ViewMemtable, ViewRecord, ViewTablet};
use pigeonhole_format::{FamilyId, TableId, TabletId};
use pigeonhole_io::ProcessId;
use pigeonhole_shm::{ShmConfig, ShmRegion};

fn region(shards: u32) -> ShmRegion {
    let mut config = ShmConfig::new(shards);
    config.arena_bytes = 2 << 20;
    ShmRegion::in_memory([9; 16], &config)
}

fn view(tablets: usize, memtables: usize) -> ViewRecord {
    ViewRecord {
        view_version: 1,
        manifest_version: 1,
        tablets: (0..tablets)
            .map(|i| ViewTablet {
                tablet: TabletId(i as u64),
                table: TableId(1),
                shard: (i % 8) as u16,
                start: vec![i as u8; 16],
                end: Some(vec![i as u8 + 1; 16]),
            })
            .collect(),
        memtables: (0..memtables)
            .map(|i| ViewMemtable {
                tablet: TabletId((i / 2) as u64),
                family: FamilyId((i % 2) as u32),
                shard: (i % 8) as u16,
                age: 0,
                root: 64,
            })
            .collect(),
    }
}

fn seqnos(c: &mut Criterion) {
    for shards in [8u32, 64] {
        let shm = region(shards);
        c.bench_function(&format!("visible_seqno/{shards}_shards"), |b| {
            b.iter(|| black_box(shm.visible_seqno()))
        });
    }
    let shm = region(8);
    c.bench_function("reserve_seqnos/1", |b| {
        b.iter(|| black_box(shm.reserve_seqnos(black_box(1))))
    });
    c.bench_function("publish_pending", |b| {
        b.iter(|| shm.publish_pending(black_box(3), black_box(42)))
    });
}

fn slots(c: &mut Criterion) {
    let shm = region(8);
    let slot = shm
        .claim_reader_slot(ProcessId {
            pid: 1,
            start_time: 1,
        })
        .unwrap();
    shm.publish_view(&view(1, 1)).unwrap();
    c.bench_function("pin_unpin", |b| {
        b.iter(|| {
            slot.pin(black_box(7), black_box(1));
            slot.unpin();
        })
    });
    // A full slot table (126 slots, all pinned).
    let others: Vec<_> = (0..125)
        .map(|i| {
            let s = shm
                .claim_reader_slot(ProcessId {
                    pid: 100 + i,
                    start_time: 1,
                })
                .unwrap();
            s.pin(u64::from(i) + 10, 1);
            s
        })
        .collect();
    c.bench_function("oldest_reader_pin/126_slots", |b| {
        b.iter(|| black_box(shm.oldest_reader_pin()))
    });
    drop(others);
}

fn views(c: &mut Criterion) {
    let shm = region(8);
    let v = view(64, 128);
    c.bench_function("publish_view/64_tablets_128_memtables", |b| {
        b.iter(|| shm.publish_view(black_box(&v)).unwrap())
    });
    c.bench_function("read_view/64_tablets_128_memtables", |b| {
        b.iter(|| black_box(shm.read_view().unwrap()))
    });
    c.bench_function("view_version", |b| b.iter(|| black_box(shm.view_version())));
}

criterion_group!(benches, seqnos, slots, views);
criterion_main!(benches);
