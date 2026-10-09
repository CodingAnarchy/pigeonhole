//! Superblock handling, reader reloads, refusals and error paths.

mod common;

use std::path::Path;

use pigeonhole_format::superblock::Superblock;
use pigeonhole_format::{FormatVersion, PAGE_SIZE};
use pigeonhole_io::sim::{FaultPlan, SimVfs};
use pigeonhole_io::{ErrorKind, OpenOptions, VfsRef};
use pigeonhole_pager::{Error, Extent, Pager, Root};

const PATH: &str = "/db/data.phdb";

fn path() -> &'static Path {
    Path::new(PATH)
}

fn sim() -> VfsRef {
    SimVfs::new(common::seed())
}

fn root(version: u64) -> Root {
    Root {
        manifest_version: version,
        ..Root::default()
    }
}

fn read_superblock(vfs: &VfsRef, slot: u64) -> pigeonhole_format::Result<Superblock> {
    let file = vfs.open(path(), OpenOptions::read()).unwrap();
    let mut page = [0u8; PAGE_SIZE];
    file.read_at(&mut page, slot * PAGE_SIZE as u64).unwrap();
    Superblock::decode(&page)
}

#[test]
fn create_then_open_empty() {
    let vfs = sim();
    let pager = Pager::create(&vfs, path()).unwrap();
    assert_eq!(pager.root(), Root::default());
    assert_eq!(pager.file().len().unwrap(), 64 << 10);
    let id = pager.db_id();
    assert_ne!(id, [0; 16]);
    drop(pager);
    assert!(matches!(
        Pager::create(&vfs, path()),
        Err(Error::Io(e)) if e.kind == ErrorKind::AlreadyExists
    ));
    let opened = Pager::open(&vfs, path(), true).unwrap();
    assert_eq!(opened.db_id(), id);
    assert_eq!(opened.root(), Root::default());
    assert!(!opened.clean_shutdown());
}

#[test]
fn db_ids_differ() {
    let a = Pager::create(&sim(), path()).unwrap().db_id();
    let b = Pager::create(&sim(), path()).unwrap().db_id();
    assert_ne!(a, b);
}

#[test]
fn commits_alternate_slots_with_rising_sequence() {
    let vfs = sim();
    let pager = Pager::create(&vfs, path()).unwrap();
    assert_eq!(read_superblock(&vfs, 0).unwrap().sequence, 1);
    assert!(read_superblock(&vfs, 1).is_err(), "slot B starts empty");
    for v in 1..=5u64 {
        let snapshot = pager.allocate(1).unwrap();
        let r = Root {
            snapshot: Some(snapshot),
            snapshot_len: 10,
            log: None,
            log_len: 0,
            manifest_version: v,
        };
        pager.commit_root(r).unwrap();
        assert_eq!(pager.root(), r);
        let written = read_superblock(&vfs, v % 2).unwrap();
        assert_eq!(written.sequence, v + 1);
        assert_eq!(written.manifest_version, v);
        assert_eq!(written.snapshot, Some(snapshot));
        assert_eq!(written.file_pages * 4096, pager.stats().file_bytes);
        let other = read_superblock(&vfs, 1 - v % 2).unwrap();
        assert_eq!(other.sequence, v, "the previous root is untouched");
    }
}

#[test]
fn corrupt_current_superblock_falls_back_to_previous_root() {
    let vfs = sim();
    let pager = Pager::create(&vfs, path()).unwrap();
    pager.commit_root(root(1)).unwrap(); // slot B, sequence 2
    pager.commit_root(root(2)).unwrap(); // slot A, sequence 3
    drop(pager);
    let file = vfs.open(path(), OpenOptions::read_write_create()).unwrap();
    file.write_at(&[0xFF; 8], 16).unwrap(); // sequence field of slot A
    let opened = Pager::open(&vfs, path(), true).unwrap();
    assert_eq!(opened.root(), root(1));
    // The next commit overwrites the corrupt slot, not the surviving one.
    let pager = opened.finish([]).unwrap();
    pager.commit_root(root(3)).unwrap();
    assert_eq!(read_superblock(&vfs, 0).unwrap().manifest_version, 3);
    assert_eq!(read_superblock(&vfs, 1).unwrap().manifest_version, 1);
}

#[test]
fn unopenable_files_are_reported() {
    let vfs = sim();
    assert!(matches!(
        Pager::open(&vfs, path(), false),
        Err(Error::Io(e)) if e.kind == ErrorKind::NotFound
    ));
    let file = vfs.open(path(), OpenOptions::read_write_create()).unwrap();
    assert!(matches!(
        Pager::open(&vfs, path(), false),
        Err(Error::Format(_))
    ));
    file.write_at(&[0x42; 3 * PAGE_SIZE], 0).unwrap();
    assert!(matches!(
        Pager::open(&vfs, path(), false),
        Err(Error::Format(_))
    ));
}

#[test]
fn newer_format_is_unsupported() {
    let vfs = sim();
    drop(Pager::create(&vfs, path()).unwrap());
    let mut sb = read_superblock(&vfs, 0).unwrap();
    sb.version = FormatVersion(99);
    let mut page = [0u8; PAGE_SIZE];
    sb.encode(&mut page);
    let file = vfs.open(path(), OpenOptions::read_write_create()).unwrap();
    file.write_at(&page, 0).unwrap();
    assert!(matches!(
        Pager::open(&vfs, path(), false),
        Err(Error::UnsupportedVersion(99))
    ));
}

#[test]
fn reader_reloads_new_roots_and_never_writes() {
    let vfs = sim();
    let writer = Pager::create(&vfs, path()).unwrap();
    let reader = Pager::open(&vfs, path(), false)
        .unwrap()
        .finish([])
        .unwrap();
    assert_eq!(reader.reload_root().unwrap(), None);
    let data = writer.allocate(1).unwrap();
    writer.write(data, 0, b"hello").unwrap();
    writer
        .commit_root(Root {
            snapshot: Some(data),
            snapshot_len: 5,
            ..root(1)
        })
        .unwrap();
    let r = reader.reload_root().unwrap().expect("a new root");
    assert_eq!(r, writer.root());
    assert_eq!(reader.root(), r);
    assert_eq!(
        reader.reload_root().unwrap(),
        None,
        "unchanged since the last call"
    );
    let mut buf = [0u8; 5];
    reader.read(data, 0, &mut buf).unwrap();
    assert_eq!(&buf, b"hello");

    let unsupported =
        |r: Result<_, Error>| matches!(r, Err(Error::Io(e)) if e.kind == ErrorKind::Unsupported);
    assert!(unsupported(reader.allocate(1).map(drop)));
    assert!(unsupported(reader.commit_root(root(9))));
    assert!(unsupported(
        reader.submit_commit_root(root(9)).wait().map_err(Error::Io)
    ));
    assert!(unsupported(reader.truncate_tail().map(drop)));
    assert!(unsupported(reader.relocate(data).map(drop)));
    assert_eq!(writer.reload_root().unwrap(), None);
}

#[test]
fn submit_commit_root_on_sim() {
    let vfs = sim();
    let pager = Pager::create(&vfs, path()).unwrap();
    pager.submit_commit_root(root(1)).wait().unwrap();
    assert_eq!(pager.root(), root(1));
    assert_eq!(Pager::open(&vfs, path(), false).unwrap().root(), root(1));
}

#[test]
#[cfg_attr(
    miri,
    ignore = "real files: fallocate and fsync are foreign calls Miri cannot run"
)]
fn submit_commit_root_on_real_files() {
    let dir = std::env::temp_dir().join(format!("pigeonhole-pager-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("data.phdb");
    let _ = std::fs::remove_file(&file);
    let vfs: VfsRef = pigeonhole_io::pread::PreadVfs::new(1);
    let pager = Pager::create(&vfs, &file).unwrap();
    let e = pager.allocate(1 << 20).unwrap();
    pager.write(e, 0, &[7; 4096]).unwrap();
    for v in 1..=4 {
        let r = Root {
            snapshot: Some(e),
            snapshot_len: 4096,
            ..root(v)
        };
        // One pool thread: the continuation submits the second sync from that thread.
        pager.submit_commit_root(r).wait().unwrap();
        assert_eq!(pager.root(), r);
    }
    drop(pager);
    let opened = Pager::open(&vfs, &file, true).unwrap();
    assert_eq!(opened.root().manifest_version, 4);
    let pager = opened.finish([e]).unwrap();
    pager.mark_clean().unwrap();
    drop(pager);
    assert!(Pager::open(&vfs, &file, false).unwrap().clean_shutdown());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn failed_commit_poisons_later_commits() {
    let sim = SimVfs::new(common::seed());
    let vfs: VfsRef = sim.clone();
    let pager = Pager::create(&vfs, path()).unwrap();
    pager.commit_root(root(1)).unwrap();
    let mut plan = FaultPlan::none();
    plan.io_error_ppm = 1_000_000;
    sim.set_faults(plan);
    assert!(pager.commit_root(root(2)).is_err());
    sim.set_faults(FaultPlan::none());
    let err = pager.commit_root(root(3)).unwrap_err();
    assert!(
        err.to_string().contains("earlier root commit failed"),
        "{err}"
    );
    assert!(pager.submit_commit_root(root(3)).wait().is_err());
    drop(pager);
    let r = Pager::open(&vfs, path(), true).unwrap().root();
    assert!(r == root(1) || r == root(2), "{r:?}");
}

#[test]
fn extent_bounds_are_enforced() {
    let pager = Pager::create(&sim(), path()).unwrap();
    let e = pager.allocate(64 << 10).unwrap();
    assert_eq!(e.len(), 64 << 10);
    pager.write(e, (64 << 10) - 4, b"abcd").unwrap();
    assert!(pager.write(e, (64 << 10) - 3, b"abcd").is_err());
    assert!(pager.write(e, u64::MAX, b"a").is_err());
    let mut buf = [0u8; 4];
    pager.read(e, (64 << 10) - 4, &mut buf).unwrap();
    assert_eq!(&buf, b"abcd");
    assert!(pager.read(e, 64 << 10, &mut buf).is_err());
}

#[test]
fn allocation_sizes() {
    let pager = Pager::create(&sim(), path()).unwrap();
    for (bytes, class) in [
        (0, 0),
        (1, 0),
        (64 << 10, 0),
        ((64 << 10) + 1, 1),
        // A 64 MiB extent is 64 MiB of simulated file; Miri stops at 512 KiB.
        if cfg!(miri) {
            (512 << 10, 3)
        } else {
            (64 << 20, 10)
        },
    ] {
        let e = pager.allocate(bytes).unwrap();
        assert_eq!(e.size_class, class, "{bytes} bytes");
        assert!(
            e.page.is_multiple_of(e.len() / 4096),
            "{e:?} aligned to its size"
        );
    }
    assert!(matches!(
        pager.allocate((64 << 20) + 1),
        Err(Error::TooLarge)
    ));
}

#[test]
fn trim_keeps_the_head_and_frees_the_tail() {
    let pager = Pager::create(&sim(), path()).unwrap();
    let e = pager.allocate(1 << 20).unwrap();
    assert_eq!(e.size_class, 4);
    let data = vec![7u8; 100 << 10];
    pager.write(e, 0, &data).unwrap();
    let before = pager.stats();
    let t = pager.trim(e, data.len() as u64);
    assert_eq!((t.page, t.size_class), (e.page, 1));
    let mut back = vec![0u8; data.len()];
    pager.read(t, 0, &mut back).unwrap();
    assert_eq!(back, data);
    let after = pager.stats();
    assert_eq!(
        after.allocated_bytes,
        before.allocated_bytes - (1 << 20) + (128 << 10)
    );
    assert_eq!(after.file_bytes, before.file_bytes);
    // Larger than the extent, or no smaller class: unchanged.
    assert_eq!(pager.trim(t, 1 << 20), t);
    assert_eq!(pager.trim(t, (64 << 10) + 1), t);
    // The freed tail is reused before the file grows: one 512 KiB block is the alignment
    // gap below `e`, the other is the tail of `e`.
    let a = pager.allocate(512 << 10).unwrap();
    let b = pager.allocate(512 << 10).unwrap();
    assert!([a.page, b.page].contains(&(e.page + (512 << 10) / 4096)));
    assert_eq!(pager.stats().file_bytes, before.file_bytes);
    // The trimmed extent is an ordinary pending extent.
    for x in [t, a, b] {
        pager.abandon(x);
    }
    assert_eq!(pager.stats().allocated_bytes, 0);
}

/// A database whose committed root names a manifest log, reopened with one live data
/// extent: both are known published.
fn published() -> (Pager, Extent, Extent) {
    let vfs = sim();
    let pager = Pager::create(&vfs, path()).unwrap();
    let data = pager.allocate(1 << 20).unwrap();
    let log = pager.allocate(256 << 10).unwrap();
    pager
        .commit_root(Root {
            log: Some(log),
            ..root(1)
        })
        .unwrap();
    drop(pager);
    let pager = Pager::open(&vfs, path(), true)
        .unwrap()
        .finish([data, log])
        .unwrap();
    // A root committed in this session publishes the log it names.
    let log2 = pager.allocate(256 << 10).unwrap();
    pager
        .commit_root(Root {
            log: Some(log2),
            ..root(2)
        })
        .unwrap();
    (pager, data, log2)
}

#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "not pending output"))]
fn trim_refuses_a_published_extent() {
    let (pager, data, log) = published();
    let before = pager.stats();
    assert_eq!(pager.trim(data, 64 << 10), data);
    assert_eq!(pager.trim(log, 64 << 10), log);
    assert_eq!(pager.stats(), before);
}

#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "not pending output"))]
fn abandon_refuses_a_published_extent() {
    let (pager, data, log) = published();
    let before = pager.stats();
    pager.abandon(data);
    pager.abandon(log);
    assert_eq!(pager.stats(), before);
}

#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "not pending output"))]
fn abandon_refuses_the_log_of_a_root_committed_in_this_session() {
    let (pager, _, log) = published();
    let before = pager.stats();
    pager.abandon(log);
    assert_eq!(pager.stats(), before);
}

#[test]
fn finish_rejects_inconsistent_live_sets() {
    let vfs = sim();
    let pager = Pager::create(&vfs, path()).unwrap();
    let big = pager.allocate(256 << 10).unwrap();
    drop(pager);
    let open = || Pager::open(&vfs, path(), true).unwrap();
    let inside = Extent {
        page: big.page + 16,
        size_class: 0,
    };
    assert!(matches!(
        open().finish([big, inside]),
        Err(Error::Format(_))
    ));
    let past_end = Extent {
        page: 1 << 20,
        size_class: 0,
    };
    assert!(matches!(open().finish([past_end]), Err(Error::Format(_))));
    let misaligned = Extent {
        page: 16,
        size_class: 1,
    };
    assert!(matches!(open().finish([misaligned]), Err(Error::Format(_))));
    let pager = open().finish([big, big]).unwrap();
    assert_eq!(pager.stats().allocated_bytes, 256 << 10);
}

#[test]
fn relocate_moves_bytes_toward_the_start() {
    let pager = Pager::create(&sim(), path()).unwrap();
    let low = pager.allocate(128 << 10).unwrap();
    let high = pager.allocate(128 << 10).unwrap();
    pager.write(high, 0, b"payload").unwrap();
    pager.write(high, (128 << 10) - 3, b"end").unwrap();
    // Nothing free below `high` yet.
    assert!(matches!(pager.relocate(high), Err(Error::NoSpace)));
    pager.abandon(low);
    let moved = pager.relocate(high).unwrap();
    assert_eq!(moved, low);
    let mut buf = [0u8; 7];
    pager.read(moved, 0, &mut buf).unwrap();
    assert_eq!(&buf, b"payload");
    pager.read(moved, (128 << 10) - 3, &mut buf[..3]).unwrap();
    assert_eq!(&buf[..3], b"end");
    // Publish the move (version 1 names `moved`), then retire the old copy.
    pager
        .commit_root(Root {
            snapshot: Some(moved),
            ..root(1)
        })
        .unwrap();
    pager.retire(high, 1);
    assert_eq!(
        pager.truncate_tail().unwrap(),
        0,
        "retired extents are kept"
    );
    assert_eq!(pager.reclaim(1), 1);
    assert_eq!(pager.truncate_tail().unwrap(), 128 << 10);
    assert_eq!(pager.file().len().unwrap(), pager.stats().file_bytes);
}

#[test]
fn errors_display() {
    let msgs = [
        Error::NoSpace.to_string(),
        Error::TooLarge.to_string(),
        Error::UnsupportedVersion(2).to_string(),
        Error::Format(pigeonhole_format::Error::Corrupt { what: "x" }).to_string(),
        Error::Io(pigeonhole_io::Error::new(ErrorKind::Other, "y")).to_string(),
    ];
    assert!(msgs.iter().all(|m| !m.is_empty()));
    assert!(msgs[2].contains('2'));
}

#[test]
fn relocate_refuses_retired_and_unallocated_extents() {
    let pager = Pager::create(&sim(), path()).unwrap();
    let low = pager.allocate(1).unwrap();
    let high = pager.allocate(1).unwrap();
    pager.abandon(low); // a free extent below `high`
    pager.retire(high, 1);
    assert!(matches!(pager.relocate(high), Err(Error::Io(_))), "retired");
    assert!(
        matches!(pager.relocate(low), Err(Error::Io(_))),
        "unallocated"
    );
    assert_eq!(pager.stats().allocated_bytes, 0, "nothing was allocated");
}

#[test]
fn a_region_cleared_for_a_large_extent_takes_it_whole() {
    // #314: units 1..48 hold 64 KiB extents, a 1 MiB extent sits at 48. Freeing most of
    // 16..32 and 40..47 leaves no free 16-unit hole below it, and no hole below 16.
    let vfs = sim();
    let pager = Pager::create(&vfs, path()).unwrap();
    let unit = |e: &Extent| e.page / 16;
    let small: Vec<Extent> = (1..48).map(|_| pager.allocate(64 << 10).unwrap()).collect();
    let big = pager.allocate(1 << 20).unwrap();
    assert_eq!(unit(&big), 48);
    let fill = |e: Extent, b: u8| pager.write(e, 0, &vec![b; e.len() as usize]).unwrap();
    let holds = |e: Extent, b: u8| {
        let mut got = vec![0u8; e.len() as usize];
        pager.read(e, 0, &mut got).unwrap();
        got.iter().all(|&x| x == b)
    };
    for e in &small {
        let u = unit(e);
        if ((17..32).contains(&u) && u != 18) || (40..47).contains(&u) {
            pager.abandon(*e);
        } else {
            fill(*e, u as u8);
        }
    }
    fill(big, 0xbb);
    assert!(matches!(pager.relocate(big), Err(Error::NoSpace)));

    // 16..32 holds the least (units 16 and 18); 32..48 could be cleared too.
    let clearing = pager.clear_for(big, |_| true).unwrap();
    assert_eq!(unit(&clearing.region), 16);
    assert_eq!(clearing.region.size_class, 4);
    let occupants: Vec<u64> = clearing.occupants.iter().map(unit).collect();
    assert_eq!(occupants, vec![16, 18]);
    // The only holes outside the region and below the large extent are above both: they
    // move up, which `relocate` (below itself) would refuse.
    for &o in &clearing.occupants {
        assert!(matches!(pager.relocate(o), Err(Error::NoSpace)));
        let to = pager.relocate_below(o, big).unwrap();
        assert!(unit(&to) > unit(&o) && unit(&to) < 48, "{o:?} -> {to:?}");
        assert!(holds(to, unit(&o) as u8));
        pager.abandon(o);
    }
    // Held: the occupants' old units and the region's free space go to no one else.
    for _ in 0..4 {
        let e = pager.allocate(64 << 10).unwrap();
        assert!(!(16..32).contains(&unit(&e)), "{e:?}");
    }
    let moved = pager.relocate_into(big, clearing.region).unwrap();
    assert_eq!(moved, clearing.region);
    assert!(holds(moved, 0xbb));
    assert!(!pager.release_region(clearing.region), "taken");
}

#[test]
fn a_released_region_frees_what_it_held() {
    let vfs = sim();
    let pager = Pager::create(&vfs, path()).unwrap();
    let small: Vec<Extent> = (1..32).map(|_| pager.allocate(64 << 10).unwrap()).collect();
    let big = pager.allocate(1 << 20).unwrap();
    for e in small.iter().filter(|e| e.page / 16 % 2 == 1) {
        pager.abandon(*e);
    }
    let clearing = pager.clear_for(big, |_| true).unwrap();
    let held = pager.stats().allocated_bytes;
    assert!(pager.release_region(clearing.region));
    assert!(pager.stats().allocated_bytes < held);
    assert!(matches!(
        pager.relocate_into(big, clearing.region),
        Err(Error::NoSpace)
    ));
}
