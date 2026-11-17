//! Punching free space (D333, #2013): only space no recoverable root and no live view names
//! is given back, and what is given back reads as zeros.

use std::path::Path;

use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{ErrorKind, VfsRef};
use pigeonhole_pager::{Extent, Pager, Root};

const PATH: &str = "/db/data.phdb";
const MIB: u64 = 1 << 20;

fn root(version: u64) -> Root {
    Root {
        manifest_version: version,
        ..Root::default()
    }
}

fn filled(pager: &Pager, bytes: u64, byte: u8) -> Extent {
    let e = pager.allocate(bytes).unwrap();
    pager.write(e, 0, &vec![byte; bytes as usize]).unwrap();
    e
}

fn read_all(pager: &Pager, e: Extent) -> Vec<u8> {
    let mut buf = vec![0xAA; e.len() as usize];
    pager.data_file().read_at(&mut buf, e.offset()).unwrap();
    buf
}

fn punch_all(pager: &Pager, reserve: u64) -> u64 {
    let mut total = 0;
    while let Some(bytes) = pager.punch_free(reserve).unwrap() {
        total += bytes;
    }
    total
}

#[test]
fn a_reclaimed_extent_is_punched_and_live_ones_are_untouched() {
    let vfs: VfsRef = SimVfs::new(11);
    let pager = Pager::create(&vfs, Path::new(PATH)).unwrap();
    let live = filled(&pager, 4 * MIB, 1);
    let old = filled(&pager, 4 * MIB, 2);
    pager.commit_root(root(1)).unwrap();
    pager.retire(old, 2);
    pager.commit_root(root(2)).unwrap();
    assert_eq!(pager.reclaim(2), 1);

    let file_bytes = pager.stats().file_bytes;
    let punched = punch_all(&pager, 0);
    assert!(punched >= old.len(), "{punched}");
    assert!(read_all(&pager, old).iter().all(|&b| b == 0), "a hole now");
    assert!(read_all(&pager, live).iter().all(|&b| b == 1));
    let stats = pager.stats();
    assert_eq!(stats.file_bytes, file_bytes, "the length is kept");
    assert_eq!(stats.punched_bytes, punched);
    assert_eq!(stats.punch_bytes, punched);
    assert!(stats.punches >= 1);
    assert_eq!(punch_all(&pager, 0), 0, "nothing left to punch");

    // Allocating there again fills the hole: it leaves the punched set and holds data.
    let again = filled(&pager, 4 * MIB, 3);
    assert!(read_all(&pager, again).iter().all(|&b| b == 3));
    assert!(pager.stats().punched_bytes <= punched - again.len());
}

#[test]
fn an_extent_whose_dropping_root_is_not_durable_is_never_punched() {
    let vfs: VfsRef = SimVfs::new(12);
    let pager = Pager::create(&vfs, Path::new(PATH)).unwrap();
    let e = filled(&pager, MIB, 7);
    pager.commit_root(root(1)).unwrap();
    // Retired at version 2, whose root has not committed: a crash would recover root 1,
    // which still names it, so the reclaim (clamped to the durable root) keeps it.
    pager.retire(e, 2);
    assert_eq!(pager.reclaim(u64::MAX), 0);
    punch_all(&pager, 0);
    assert!(read_all(&pager, e).iter().all(|&b| b == 7));
}

#[test]
fn the_reserve_keeps_low_free_space_written() {
    let vfs: VfsRef = SimVfs::new(13);
    let pager = Pager::create(&vfs, Path::new(PATH)).unwrap();
    let low = filled(&pager, MIB, 4);
    let keep = filled(&pager, 16 * MIB, 5);
    let high = filled(&pager, 16 * MIB, 6);
    pager.commit_root(root(1)).unwrap();
    pager.retire(low, 2);
    pager.retire(high, 2);
    pager.commit_root(root(2)).unwrap();
    pager.reclaim(2);
    // A reserve at least as large as the low block keeps it written; the high one goes.
    assert!(low.offset() < keep.offset() && keep.offset() < high.offset());
    punch_all(&pager, MIB);
    assert!(
        read_all(&pager, low).iter().all(|&b| b == 4),
        "in the reserve"
    );
    assert!(read_all(&pager, high).iter().all(|&b| b == 0), "past it");
    assert!(read_all(&pager, keep).iter().all(|&b| b == 5), "live");
}

#[test]
fn a_read_only_pager_does_not_punch() {
    let vfs: VfsRef = SimVfs::new(14);
    drop(Pager::create(&vfs, Path::new(PATH)).unwrap());
    let pager = Pager::open(&vfs, Path::new(PATH), false)
        .unwrap()
        .finish([])
        .unwrap();
    let e = pager.punch_free(0).unwrap_err();
    assert!(
        matches!(e, pigeonhole_pager::Error::Io(ref io) if io.kind == ErrorKind::Unsupported),
        "{e:?}"
    );
}
