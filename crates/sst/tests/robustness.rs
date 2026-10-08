//! Bad bytes and interrupted builds: a corrupt or truncated SST returns corruption errors and
//! never panics, and a crash while writing leaves nothing `SstReader::open` accepts unless the
//! SST is whole.

mod common;

use std::sync::Arc;

use common::*;
use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_format::Cursor;
use pigeonhole_format::compress::Compression;
use pigeonhole_format::key::{Kind, encode_key};
use pigeonhole_format::manifest::SstMeta;
use pigeonhole_format::value::BlobPointer;
use pigeonhole_format::{BlobFileId, SstId};
use pigeonhole_io::sim::{CrashKind, FaultPlan};
use pigeonhole_io::{FileRef, OpenOptions, Vfs};
use pigeonhole_sst::{
    BlobReader, BlobWriter, Error, ReadOptions, ScanFilter, SstReader, SstWriter,
};
use proptest::prelude::*;

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

fn assert_corruption(e: &Error) {
    assert!(e.is_corruption(), "expected a corruption error, got {e:?}");
}

/// Opens and reads everything: a full scan, a seek per key, row skips. Returns the scan, or
/// the first error.
fn read_all(file: &FileRef, meta: &SstMeta, keys: &[Vec<u8>]) -> Result<Entries, Error> {
    let cache = Arc::new(BlockCache::new(8 << 20, 1));
    let r = Arc::new(SstReader::open(
        file.clone(),
        meta,
        cache,
        Priority::Normal,
    )?);
    let mut it = r.iter(ScanFilter::all(), ReadOptions::default());
    let mut out = Vec::new();
    it.seek_to_first()?;
    while it.valid() {
        out.push((it.key().to_vec(), it.value_cell().to_vec()));
        it.next()?;
    }
    for k in keys {
        it.seek(k)?;
        it.skip_row()?;
    }
    Ok(out)
}

fn sample(n: u32, value_len: usize) -> Model {
    let mut m = Model::new();
    for i in 0..n {
        let mut k = Vec::new();
        let row = format!("row{:05}", i / 3);
        encode_key(
            &mut k,
            row.as_bytes(),
            &[b'q', (i % 3) as u8],
            5,
            u64::from(i) + 1,
            Kind::Put,
        )
        .unwrap();
        let mut v = vec![0u8];
        // Pseudo-random, so LZ4 cannot shrink the SST below several write batches.
        let mut x = u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        v.extend((0..value_len).map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        }));
        m.insert(k, v);
    }
    m
}

const LAYOUT: Layout = Layout {
    block_size: 512,
    restart_interval: 4,
    compression: Compression::Lz4,
    compression_level: 3,
    bloom_bits: 10,
};

#[derive(Debug, Clone)]
enum Damage {
    /// XOR one byte with a nonzero mask.
    Flip(usize, u8),
    /// Overwrite a span with arbitrary bytes.
    Overwrite(usize, Vec<u8>),
    /// The manifest records a shorter length.
    Truncate(usize),
    /// The manifest records a longer length (garbage after the footer).
    Extend(usize),
}

fn damage() -> impl Strategy<Value = Damage> {
    prop_oneof![
        (any::<usize>(), 1u8..=255).prop_map(|(at, m)| Damage::Flip(at, m)),
        (any::<usize>(), prop::collection::vec(any::<u8>(), 1..64))
            .prop_map(|(at, b)| Damage::Overwrite(at, b)),
        (1usize..2000).prop_map(Damage::Truncate),
        (1usize..2000).prop_map(Damage::Extend),
    ]
}

proptest! {
    /// Any damage to the bytes of an SST (footer, index, filters, properties, data) is
    /// reported as corruption when read, never as a panic or as wrong data.
    #[test]
    fn damaged_ssts_are_corrupt_never_wrong(d in damage(), n in 1u32..400) {
        let m = sample(n, 20);
        let keys: Vec<_> = m.keys().cloned().collect();
        let (_vfs, file) = sim_file(10, EXTENT);
        let mut meta = write_sst(&file, EXTENT, &m, &LAYOUT);
        let len = meta.len as usize;
        let base = EXTENT.offset();
        let mut changed = true;
        match &d {
            Damage::Flip(at, mask) => {
                let at = (at % len) as u64;
                let mut b = [0u8];
                file.read_at(&mut b, base + at).unwrap();
                b[0] ^= mask;
                file.write_at(&b, base + at).unwrap();
            }
            Damage::Overwrite(at, bytes) => {
                let at = at % len;
                let bytes = &bytes[..bytes.len().min(len - at)];
                let mut old = vec![0; bytes.len()];
                file.read_at(&mut old, base + at as u64).unwrap();
                changed = old != bytes;
                file.write_at(bytes, base + at as u64).unwrap();
            }
            Damage::Truncate(k) => meta.len = meta.len.saturating_sub(*k as u64),
            Damage::Extend(k) => meta.len += *k as u64,
        }
        match read_all(&file, &meta, &keys) {
            Err(e) => assert_corruption(&e),
            Ok(got) => {
                prop_assert!(!changed, "damage {:?} went unnoticed", d);
                prop_assert_eq!(got, m.into_iter().collect::<Vec<_>>());
            }
        }
    }

    /// Arbitrary bytes where an SST should be never panic and never open as a valid SST.
    #[test]
    fn arbitrary_bytes_are_corrupt(bytes in prop::collection::vec(any::<u8>(), 0..4096), keep_footer in any::<bool>()) {
        let m = sample(50, 10);
        let (_vfs, file) = sim_file(11, EXTENT);
        let mut meta = write_sst(&file, EXTENT, &m, &LAYOUT);
        // Garbage blocks, optionally behind the real (now meaningless) footer.
        if keep_footer {
            let at = meta.len.saturating_sub(104 + bytes.len() as u64);
            file.write_at(&bytes, EXTENT.offset() + at).unwrap();
        } else {
            file.write_at(&bytes, EXTENT.offset()).unwrap();
            meta.len = bytes.len() as u64;
        }
        let keys: Vec<_> = m.keys().cloned().collect();
        if let Err(e) = read_all(&file, &meta, &keys) {
            assert_corruption(&e);
        }
    }

    /// Damaged blob extents and bad pointers are corruption errors.
    #[test]
    fn damaged_blobs_are_corrupt(at in any::<usize>(), mask in 1u8..=255, bad_ptr in any::<(u32, u64)>()) {
        let (_vfs, file) = sim_file(12, EXTENT);
        let mut w = BlobWriter::new(file.clone(), BlobFileId(3), 0);
        let mut page = 1024;
        let mut ptrs = Vec::new();
        for i in 0..20usize {
            let v = vec![i as u8; 1 + i * 997];
            while w.needs_extent(v.len()) {
                w.add_extent(pigeonhole_format::superblock::ExtentRef { page, size_class: 0 }).unwrap();
                page += 16;
            }
            ptrs.push((w.append(&v).unwrap(), v));
        }
        let (extents, total) = w.finish().unwrap();
        // Damage one payload byte somewhere in the logical blob file.
        let logical = (at as u64) % total;
        let payload = 65536 - 64;
        let e = extents[(logical / payload) as usize];
        let abs = e.offset() + 64 + logical % payload;
        let mut b = [0u8];
        file.read_at(&mut b, abs).unwrap();
        b[0] ^= mask;
        file.write_at(&b, abs).unwrap();

        let r = BlobReader::new(file, BlobFileId(3), extents, Arc::new(BlockCache::new(1 << 20, 1)));
        let mut errors = 0;
        for (p, v) in &ptrs {
            match r.read(p) {
                Ok(cell) => prop_assert_eq!(&cell[..], &v[..]),
                Err(e) => { assert_corruption(&e); errors += 1; }
            }
        }
        prop_assert_eq!(errors, 1);
        let bogus = BlobPointer { blob_file: BlobFileId(3), len: bad_ptr.0, offset: bad_ptr.1 };
        if let Err(e) = r.read(&bogus) {
            assert_corruption(&e);
        }
    }
}

/// A damaged blob extent header (magic, version, blob file, position or checksum) is caught
/// the first time a read touches that extent; values in intact extents still read.
#[test]
fn damaged_blob_extent_headers_are_corrupt() {
    use pigeonhole_format::superblock::ExtentRef;
    // Header bytes: magic, version, blob_file, extent_index, reserved, checksum.
    for at in [0usize, 9, 13, 17, 30, 60] {
        let (_vfs, file) = sim_file(13, EXTENT);
        let mut w = BlobWriter::new(file.clone(), BlobFileId(3), 0);
        let v = vec![5u8; 40_000];
        let mut page = 1024;
        let mut ptrs = Vec::new();
        for _ in 0..4 {
            while w.needs_extent(v.len()) {
                w.add_extent(ExtentRef {
                    page,
                    size_class: 0,
                })
                .unwrap();
                page += 16;
            }
            ptrs.push(w.append(&v).unwrap());
        }
        let (extents, _) = w.finish().unwrap();
        assert_eq!(extents.len(), 3);
        // Damage the second extent's header.
        let abs = extents[1].offset() + at as u64;
        let mut b = [0u8];
        file.read_at(&mut b, abs).unwrap();
        b[0] ^= 0x40;
        file.write_at(&b, abs).unwrap();
        let r = BlobReader::new(
            file,
            BlobFileId(3),
            extents,
            Arc::new(BlockCache::new(1 << 20, 1)),
        );
        // 65,472-byte payloads, 40,016-byte records: record 0 lies in extent 0, records 1 and
        // 3 span into extent 1, record 2 lies in it.
        assert_eq!(&r.read(&ptrs[0]).unwrap()[..], &v[..], "byte {at}");
        for p in &ptrs[1..] {
            assert_corruption(&r.read(p).unwrap_err());
        }
        // A failed check is not remembered as passed.
        assert_corruption(&r.read(&ptrs[2]).unwrap_err());
    }
    // A header naming the wrong extent position (swapped extents) is caught too.
    let (_vfs, file) = sim_file(14, EXTENT);
    let mut w = BlobWriter::new(file.clone(), BlobFileId(3), 0);
    w.add_extent(ExtentRef {
        page: 1024,
        size_class: 0,
    })
    .unwrap();
    w.add_extent(ExtentRef {
        page: 1040,
        size_class: 0,
    })
    .unwrap();
    let p = w.append(&vec![1u8; 70_000]).unwrap();
    let (mut extents, _) = w.finish().unwrap();
    extents.swap(0, 1);
    let r = BlobReader::new(
        file,
        BlobFileId(3),
        extents,
        Arc::new(BlockCache::new(1 << 20, 1)),
    );
    assert_corruption(&r.read(&p).unwrap_err());
}

/// Records above `min(1 MiB, capacity / 8)` are returned pinned but not cached.
#[test]
fn large_blob_records_bypass_the_cache() {
    use pigeonhole_format::superblock::ExtentRef;
    let (_vfs, file) = sim_file(15, EXTENT);
    let mut w = BlobWriter::new(file.clone(), BlobFileId(1), 6);
    w.add_extent(ExtentRef {
        page: 1024,
        size_class: 6,
    })
    .unwrap();
    let small = w.append(&[1u8; 1000]).unwrap();
    let big = w.append(&vec![2u8; 200_000]).unwrap();
    let (extents, _) = w.finish().unwrap();
    let cache = Arc::new(BlockCache::new(1 << 20, 1)); // limit: 128 KiB
    let r = BlobReader::new(file, BlobFileId(1), extents, cache.clone());
    assert_eq!(r.read(&big).unwrap().len(), 200_000);
    assert_eq!(cache.usage(), 0);
    assert_eq!(r.read(&small).unwrap().len(), 1000);
    assert!(cache.usage() > 0);
}

/// Writes `m` with a crash after the `n`-th mutating operation of the build. Returns whether
/// the build finished before the crash.
fn build_with_crash(
    seed: u64,
    plan: FaultPlan,
    n: u64,
    m: &Model,
) -> (Arc<pigeonhole_io::sim::SimVfs>, bool) {
    let (vfs, file) = sim_file(seed, EXTENT);
    let mut plan = plan;
    plan.crash_after_ops = Some(vfs.mutating_ops() + n);
    vfs.set_faults(plan);
    let mut w = SstWriter::new(file, EXTENT, SstId(77), options(&LAYOUT));
    let mut done = true;
    for (k, v) in m {
        if w.add(k, v).is_err() {
            done = false;
            break;
        }
    }
    if done {
        done = w.finish().is_ok();
    }
    // Writers never sync (the pager's root commit does), so power loss always follows.
    vfs.crash(CrashKind::Power);
    (vfs, done)
}

/// A crash at every write of a build, under torn and reordered writes: `open` accepts the
/// SST only if it is whole, and a reordered disk that keeps the footer but drops blocks
/// yields corruption errors, never wrong data.
#[test]
fn crash_during_write_leaves_no_half_sst() {
    // Several 1 MiB batches, so the build is several writes.
    let m = sample(if cfg!(miri) { 300 } else { 8_000 }, 300);
    let keys: Vec<_> = m.keys().cloned().collect();
    let want: Vec<_> = m.clone().into_iter().collect();
    let (vfs, file) = sim_file(0, EXTENT);
    let before = vfs.mutating_ops();
    let meta = write_sst(&file, EXTENT, &m, &LAYOUT);
    let writes = vfs.mutating_ops() - before;
    assert!(writes >= 3, "only {writes} writes");

    let mut plans = Vec::new();
    let mut torn = FaultPlan::none();
    torn.torn_writes = true;
    plans.push(("none", FaultPlan::none()));
    plans.push(("torn", torn));
    let mut reorder = FaultPlan::none();
    reorder.torn_writes = true;
    reorder.reorder_unsynced = true;
    plans.push(("reorder", reorder));

    let seeds = if cfg!(miri) { 1 } else { 8 };
    for (name, plan) in plans {
        for seed in 0..seeds {
            for n in 1..=writes + 1 {
                let (vfs, done) = build_with_crash(seed, plan.clone(), n, &m);
                assert_eq!(done, n >= writes, "plan {name} seed {seed} crash after {n}");
                let file = vfs
                    .open("/db/data.phdb".as_ref(), OpenOptions::read_write_create())
                    .unwrap();
                match read_all(&file, &meta, &keys) {
                    Err(e) => assert!(
                        e.is_corruption(),
                        "plan {name} seed {seed} crash after {n}: {e:?}"
                    ),
                    Ok(got) => {
                        assert_eq!(got, want, "plan {name} seed {seed} crash after {n}");
                        // Only a reordering disk can keep the last write without the rest,
                        // and then every block must have survived intact to get here.
                        if name != "reorder" {
                            assert!(
                                n >= writes,
                                "plan {name} seed {seed}: half SST accepted at {n}"
                            );
                        }
                    }
                }
            }
        }
    }
}
