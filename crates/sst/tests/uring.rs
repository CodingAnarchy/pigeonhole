//! SST scans with readahead on the io_uring backend (#974, ICR 0077): a readahead's blocks and
//! merged runs go to the kernel as one batch of reads nothing waits on yet, each with
//! `IOSQE_ASYNC`, through the shared ring or a thread's own. Linux only; fails where io_uring
//! is unavailable.
#![cfg(all(target_os = "linux", not(miri)))]

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use common::*;
use pigeonhole_format::key::{Kind, encode_key};
use pigeonhole_io::{FileRef, OpenOptions, Vfs};
use pigeonhole_sst::{ReadOptions, ScanFilter};

/// A directory of its own under the system temp directory, removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("phdb-sst-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 800 rows in 256-byte blocks (50+ data blocks), as `sst.rs`'s readahead store.
fn rows() -> Model {
    let mut m = Model::new();
    for i in 0..800u32 {
        let mut k = Vec::new();
        encode_key(
            &mut k,
            format!("row{i:05}").as_bytes(),
            b"q",
            1,
            1,
            Kind::Put,
        )
        .unwrap();
        m.insert(k, b"\x00value-value-value".to_vec());
    }
    m
}

/// Scans `file`'s SST cold (a fresh reader and cache) with readahead in every combination of
/// filling the cache and merging adjacent blocks, checking every entry arrives and the
/// fetches went ahead of the scan.
fn scan_ahead(
    file: &FileRef,
    meta: &pigeonhole_format::manifest::SstMeta,
    want: &[(Vec<u8>, Vec<u8>)],
) {
    for (fill, merge) in [(true, false), (false, false), (true, true), (false, true)] {
        let mut o = ReadOptions::default();
        o.readahead_blocks = 4;
        o.fill_cache = fill;
        o.readahead_merge = merge;
        let r = open(file, meta);
        let (got, counts) =
            pigeonhole_sst::counting_readahead(|| scan(&mut r.iter(ScanFilter::all(), o)));
        assert_eq!(got, want, "fill {fill} merge {merge}");
        assert!(
            counts.issued > 0 && counts.used > 0,
            "fill {fill} merge {merge}: {counts:?}"
        );
    }
}

#[test]
fn readahead_scans_read_every_entry_through_io_uring() {
    let dir = TempDir::new("uring-readahead");
    let vfs = pigeonhole_io::uring::UringVfs::new().expect("io_uring is available");
    let path = dir.0.join("data.phdb");
    let file = vfs.open(&path, OpenOptions::read_write_create()).unwrap();
    file.set_len(EXTENT.offset() + EXTENT.len()).unwrap();
    let m = rows();
    let l = Layout {
        block_size: 256,
        restart_interval: 2,
        compression: pigeonhole_format::compress::Compression::None,
        compression_level: 3,
        bloom_bits: 0,
    };
    let meta = write_sst(&file, EXTENT, &m, &l);
    let want: Vec<_> = m.into_iter().collect();

    // The shared ring (this thread has none of its own).
    scan_ahead(&file, &meta, &want);

    // A direct handle, where the file system takes one: every read rounds out to aligned
    // pages, a merged run's too.
    let mut direct = OpenOptions::read();
    direct.direct = true;
    if let Ok(d) = vfs.open(&path, direct)
        && d.direct_align().is_some()
    {
        scan_ahead(&d, &meta, &want);
    }

    // A thread's own ring.
    let (vfs, file, meta, want) = (Arc::clone(&vfs), file.clone(), meta.clone(), want.clone());
    std::thread::spawn(move || {
        vfs.attach_thread();
        scan_ahead(&file, &meta, &want);
    })
    .join()
    .unwrap();
}
