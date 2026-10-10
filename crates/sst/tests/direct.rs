//! SST reads through a direct-I/O handle (#403): `SimVfs` fails any read on a direct handle
//! whose offset, length or buffer is not aligned, so every read path (blocks, contiguous
//! runs, the footer, the cache-only open's fetches) must round out to aligned pages and keep
//! just the bytes it wanted. Written through a buffered handle; aligned writes are next.

mod common;

use std::sync::Arc;

use common::*;
use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_io::{OpenOptions, Vfs};
use pigeonhole_sst::{Error, ReadOptions, ScanFilter, SstReader};
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
        let mut ahead = ReadOptions::default();
        ahead.readahead_blocks = 4;
        prop_assert_eq!(scan(&mut r.iter(ScanFilter::all(), ahead)), want.clone());

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
}
