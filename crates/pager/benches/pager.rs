//! Pager hot paths on real files (`PreadVfs`): extent allocation and epoch-deferred freeing
//! (per flush or compaction output) and root commit latency (two fsyncs per manifest commit).

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use pigeonhole_io::VfsRef;
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_pager::{Pager, Root};

fn bench(c: &mut Criterion) {
    let dir = std::env::temp_dir().join(format!("pigeonhole-pager-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bench.phdb");
    let _ = std::fs::remove_file(&path);
    let vfs: VfsRef = PreadVfs::new(2);
    let pager = Pager::create(&vfs, &path).unwrap();

    // Warm the file to 256 MiB so the loops measure the allocator, not file growth.
    let warm: Vec<_> = (0..4).map(|_| pager.allocate(64 << 20).unwrap()).collect();
    for e in warm {
        pager.abandon(e);
    }

    c.bench_function("allocate+abandon 64KiB", |b| {
        b.iter(|| {
            let e = pager.allocate(black_box(64 << 10)).unwrap();
            pager.abandon(e);
        })
    });

    // A compaction-shaped cycle: allocate 16 outputs of mixed size, retire them at one
    // version, reclaim. Reported per extent.
    let mut version = 0u64;
    let mut group = c.benchmark_group("allocate+retire+reclaim");
    group.throughput(criterion::Throughput::Elements(16));
    group.bench_function("16 extents, 64KiB-2MiB", |b| {
        let mut extents = Vec::with_capacity(16);
        b.iter(|| {
            for i in 0..16u64 {
                extents.push(pager.allocate((64 << 10) << (i % 6)).unwrap());
            }
            version += 1;
            for e in extents.drain(..) {
                pager.retire(e, version);
            }
            black_box(pager.reclaim(version));
        })
    });
    group.finish();

    let manifest = pager.allocate(256 << 10).unwrap();
    let mut root = Root {
        snapshot: Some(manifest),
        snapshot_len: 4096,
        ..Root::default()
    };
    let block = vec![0x5Au8; 4096];
    let mut group = c.benchmark_group("commit_root");
    group.sample_size(20);
    group.bench_function("write 4KiB delta + commit_root", |b| {
        b.iter(|| {
            root.manifest_version += 1;
            pager.write(manifest, 0, &block).unwrap();
            pager.commit_root(root).unwrap();
        })
    });
    group.bench_function("write 4KiB delta + submit_commit_root.wait", |b| {
        b.iter(|| {
            root.manifest_version += 1;
            pager.write(manifest, 0, &block).unwrap();
            pager.submit_commit_root(root).wait().unwrap();
        })
    });
    group.finish();

    drop(pager);
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(benches, bench);
criterion_main!(benches);
