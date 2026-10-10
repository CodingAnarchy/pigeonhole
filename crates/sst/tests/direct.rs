//! SST reads through a direct-I/O handle (#403): `SimVfs` fails any read on a direct handle
//! whose offset, length or buffer is not aligned, so every read path (blocks, contiguous
//! runs, the footer, the cache-only open's fetches) must round out to aligned pages and keep
//! just the bytes it wanted; and every write (SST pages, the padded last page and footer,
//! blob records) must too.

mod common;

use std::sync::Arc;

use common::*;
use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_format::BlobFileId;
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_io::{OpenOptions, Vfs};
use pigeonhole_sst::{BlobReader, BlobWriter, Error, ReadOptions, ScanFilter, SstReader};
use proptest::prelude::*;

proptest! {
    #[test]
    fn reads_through_a_direct_handle_match_the_model(
        cells in prop::collection::vec(raw_cell(), 0..300),
        l in layout(),
    ) {
        let extent = ExtentRef { page: 16, size_class: 4 };
        let (vfs, file) = sim_file(9, extent);
        let m = model(&cells);
        let meta = write_sst(&file, extent, &m, &l);
        let mut opts = OpenOptions::read();
        opts.direct = true;
        let direct = vfs.open("/db/data.phdb".as_ref(), opts).unwrap();
        prop_assert_eq!(direct.direct_align(), Some(4096));
        let want: Vec<_> = m.into_iter().collect();

        // Block by block, and in contiguous runs (readahead).
        let r = open(&direct, &meta);
        prop_assert_eq!(scan(&mut r.iter(ScanFilter::all(), ReadOptions::default())), want.clone());
        // Readahead fetches (one per block, or merged runs of adjacent blocks): a fetch that
        // failed its alignment would be dropped and its block read synchronously, so every
        // fetch issued must be used.
        for merge in [false, true] {
            let mut ahead = ReadOptions::default();
            ahead.readahead_blocks = 4;
            ahead.readahead_merge = merge;
            // A fresh reader and cache: the blocks are not cached yet.
            let cold = open(&direct, &meta);
            let (got, counts) = pigeonhole_sst::counting_readahead(|| {
                scan(&mut cold.iter(ScanFilter::all(), ahead))
            });
            prop_assert_eq!(got, want.clone());
            prop_assert_eq!(counts.used, counts.issued, "merge {}: {:?}", merge, counts);
        }

        // A cache-only open fetches what it misses (footer, index, filters) asynchronously.
        let cache = Arc::new(BlockCache::new(8 << 20, 2));
        let mut fetches = 0;
        let r = loop {
            match SstReader::open_cache_only(direct.clone(), &meta, Arc::clone(&cache), Priority::Normal) {
                Ok(r) => break Arc::new(r),
                Err(Error::WouldBlock(f)) => {
                    let buf = f.submit().wait().unwrap();
                    f.admit(buf).unwrap();
                    fetches += 1;
                    prop_assert!(fetches < 100, "the open keeps missing");
                }
                Err(e) => panic!("{e}"),
            }
        };
        prop_assert!(fetches > 0);
        prop_assert_eq!(scan(&mut r.iter(ScanFilter::all(), ReadOptions::default())), want);
    }

    #[test]
    fn ssts_written_through_a_direct_handle_read_back(
        cells in prop::collection::vec(raw_cell(), 0..300),
        l in layout(),
    ) {
        let extent = ExtentRef { page: 16, size_class: 4 };
        let other = ExtentRef { page: 16 + (extent.len() / 4096), size_class: 4 };
        let (vfs, file) = sim_file(11, extent);
        file.set_len(other.offset() + other.len()).unwrap();
        let mut opts = OpenOptions::read_write_create();
        opts.direct = true;
        let direct = vfs.open("/db/data.phdb".as_ref(), opts).unwrap();
        let m = model(&cells);
        let meta = write_sst(&direct, extent, &m, &l);
        // The same SST written buffered has the same layout: padding is past `len`.
        let plain = write_sst(&file, other, &m, &l);
        prop_assert_eq!(meta.len, plain.len);
        let want: Vec<_> = m.into_iter().collect();
        for handle in [&direct, &file] {
            let r = open(handle, &meta);
            prop_assert_eq!(scan(&mut r.iter(ScanFilter::all(), ReadOptions::default())), want.clone());
        }
    }

    #[test]
    fn blob_records_written_through_a_direct_handle_read_back(
        lens in prop::collection::vec(0usize..150_000, 1..12),
    ) {
        let vfs = pigeonhole_io::sim::SimVfs::new(13);
        let mut opts = OpenOptions::read_write_create();
        opts.direct = true;
        let direct = vfs.open("/db".as_ref(), opts).unwrap();
        direct.set_len(64 << 20).unwrap();
        let mut w = BlobWriter::new(direct.clone(), BlobFileId(4), 0);
        let mut next_page = 16;
        let mut written = Vec::new();
        for (i, len) in lens.iter().enumerate() {
            while w.needs_extent(*len) {
                w.add_extent(ExtentRef { page: next_page, size_class: 0 }).unwrap();
                next_page += 16;
            }
            let value: Vec<u8> = (0..*len).map(|j| (i * 31 + j) as u8).collect();
            let ptr = w.append(&value).unwrap();
            written.push((ptr, value));
        }
        let (extents, _) = w.finish().unwrap();
        let buffered = vfs.open("/db".as_ref(), OpenOptions::read()).unwrap();
        for handle in [direct, buffered] {
            let r = BlobReader::new(
                handle,
                BlobFileId(4),
                extents.clone(),
                Arc::new(BlockCache::new(1 << 20, 1)),
            );
            for (ptr, value) in &written {
                prop_assert_eq!(&r.read(ptr).unwrap()[..], &value[..]);
            }
        }
    }
}
