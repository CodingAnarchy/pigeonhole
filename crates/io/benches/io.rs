//! Latency of the I/O primitives a cold point get pays for: one 4 KiB positional read,
//! the same read submitted to the pool, and the completion round trip itself.

use std::hint::black_box;
use std::path::Path;

use criterion::{Criterion, criterion_group, criterion_main};
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{Completion, IoBuf, OpenOptions, Vfs};

fn bench(c: &mut Criterion) {
    let dir = std::env::temp_dir().join(format!("pigeonhole-io-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let vfs = PreadVfs::new(2);
    let file = vfs
        .open(&dir.join("bench.bin"), OpenOptions::read_write_create())
        .unwrap();
    file.write_at(&vec![7u8; 1 << 20], 0).unwrap();
    file.sync_data().unwrap();

    let mut buf = vec![0u8; 4096];
    c.bench_function("pread/read_at 4KiB (page cache)", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i = (i + 1) % 256;
            file.read_at(&mut buf, i * 4096).unwrap();
        })
    });

    let mut pooled = Some(IoBuf::zeroed(4096));
    c.bench_function("pread/submit_read+wait 4KiB", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i = (i + 1) % 256;
            let buf = pooled.take().unwrap();
            pooled = Some(file.submit_read(buf, i * 4096).wait().unwrap());
        })
    });

    let sim = SimVfs::new(1);
    let sfile = sim
        .open(Path::new("/b/bench.bin"), OpenOptions::read_write_create())
        .unwrap();
    sfile.write_at(&vec![7u8; 1 << 20], 0).unwrap();
    c.bench_function("sim/read_at 4KiB", |b| {
        b.iter(|| sfile.read_at(&mut buf, black_box(4096)).unwrap())
    });

    c.bench_function("completion/pair+resolve+wait", |b| {
        b.iter(|| {
            let (done, resolver) = Completion::<u64>::pair();
            resolver.resolve(Ok(1));
            black_box(done.wait().unwrap())
        })
    });

    drop(file);
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(benches, bench);
criterion_main!(benches);
