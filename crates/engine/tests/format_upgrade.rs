//! A writable open of a file an older build wrote (format version 2: 0.2.x, 0.3.x) rewrites
//! both superblock pages at the current version before it writes any WAL record (ICR 0046):
//! the WAL's DECISION record (D263) is one an older build cannot read, and an older build
//! skips a newer WAL segment header as torn. With both pages at version 3 it refuses the
//! file (`UnsupportedFormat(3)`) instead.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_format::{Durability, FormatVersion};
use pigeonhole_io::sim::{SimOp, SimVfs};
use pigeonhole_io::{FileRef, OpenOptions, Vfs};

const DB: &str = "/db/data.phdb";
const PAGE: u64 = 4096;

fn main_file(vfs: &SimVfs) -> FileRef {
    let mut opts = OpenOptions::read();
    opts.write = true;
    vfs.open(Path::new(DB), opts).unwrap()
}

fn page(f: &FileRef, page: u64) -> Superblock {
    let mut buf = [0u8; PAGE as usize];
    f.read_at(&mut buf, page * PAGE).unwrap();
    Superblock::decode(&buf).unwrap()
}

/// Both pages as a version 2 build left them.
fn downgrade(vfs: &SimVfs) {
    let f = main_file(vfs);
    for p in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        let mut sb = page(&f, p);
        sb.version = FormatVersion(2);
        let mut buf = [0u8; PAGE as usize];
        sb.encode(&mut buf);
        f.write_at(&buf, p * PAGE).unwrap();
    }
    f.sync_all().unwrap();
}

fn put(row: &[u8], value: &[u8], t: &pigeonhole_engine::TableInfo) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(value),
    )
    .unwrap();
    wb
}

#[test]
fn an_older_file_is_upgraded_on_both_pages_before_any_wal_write() {
    let vfs = SimVfs::new(46);
    let o = common::options(Arc::clone(&vfs), 2, 64 << 20);
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for i in 0..64u32 {
        db.commit(put(&i.to_be_bytes(), b"v", &t), Some(Durability::Sync))
            .unwrap();
    }
    db.close().unwrap();
    drop(db);
    // Both pages decode, and carry version 2 as 0.3.x wrote them.
    downgrade(&vfs);

    vfs.record_ops();
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    let ops = vfs.recorded_ops();
    let main = main_file(&vfs);
    for p in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        assert_eq!(page(&main, p).version, FormatVersion::CURRENT, "page {p}");
    }
    // The superblock writes: one page each, on the main file. Every write before the
    // second one is to the main file: no WAL record or segment header precedes it.
    let writes: Vec<(u64, u64, u64)> = ops
        .iter()
        .filter_map(|op| match op {
            SimOp::Write {
                node, offset, len, ..
            } => Some((*node, *offset, *len)),
            _ => None,
        })
        .collect();
    let superblock = |&(_, offset, len): &(u64, u64, u64)| {
        len == PAGE && (offset == SUPERBLOCK_PAGE_A * PAGE || offset == SUPERBLOCK_PAGE_B * PAGE)
    };
    let pages: Vec<usize> = (0..writes.len())
        .filter(|&i| superblock(&writes[i]))
        .take(2)
        .collect();
    assert_eq!(pages.len(), 2, "writes at open: {writes:?}");
    let main_node = writes[pages[0]].0;
    assert_eq!(writes[pages[1]].0, main_node);
    assert_ne!(
        writes[pages[0]].1, writes[pages[1]].1,
        "one page written twice"
    );
    assert!(
        writes[..pages[1]].iter().all(|w| w.0 == main_node),
        "a write to another file before the upgrade finished: {writes:?}"
    );

    // Everything committed before is there, and commits go on.
    let snap = db.snapshot().unwrap();
    for i in 0..64u32 {
        let cell = db
            .get(&snap, t.id, t.families[0].id, &i.to_be_bytes(), b"q")
            .unwrap();
        assert!(cell.is_some(), "row {i}");
    }
    drop(snap);
    db.commit(put(b"after", b"v", &t), Some(Durability::Sync))
        .unwrap();
    db.close().unwrap();
}

/// A merge operator the reopen will not register.
#[derive(Debug)]
struct Concat;

impl pigeonhole_engine::MergeOperator for Concat {
    fn name(&self) -> &str {
        "test.concat"
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), pigeonhole_engine::MergeError> {
        let mut out = older.to_vec();
        out.extend_from_slice(acc);
        *acc = out;
        Ok(())
    }

    fn finish(
        &self,
        base: Option<&[u8]>,
        acc: &mut Vec<u8>,
    ) -> Result<(), pigeonhole_engine::MergeError> {
        if let Some(b) = base {
            let mut out = b.to_vec();
            out.extend_from_slice(acc);
            *acc = out;
        }
        Ok(())
    }
}

/// #496 PR 2: an open refused for an unregistered merge operator is refused before the
/// pager upgrades the superblocks: an older file is left as it was, for the build it came
/// from.
#[test]
fn a_refused_open_leaves_an_older_file_unupgraded() {
    let vfs = SimVfs::new(4962);
    let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    o.merge_operators.register(Arc::new(Concat));
    let db = Engine::open(Path::new(DB), o).unwrap();
    db.create_table(
        "t",
        &[(
            "f".into(),
            FamilyOptions::default().merge_operator("test.concat"),
        )],
    )
    .unwrap();
    db.close().unwrap();
    drop(db);
    downgrade(&vfs);

    let o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    let e = Engine::open(Path::new(DB), o).unwrap_err();
    assert!(
        matches!(e, pigeonhole_engine::Error::UnknownMergeOperator(ref n) if n == "test.concat"),
        "{e:?}"
    );
    let main = main_file(&vfs);
    for p in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        assert_eq!(
            page(&main, p).version,
            FormatVersion(2),
            "page {p} was upgraded"
        );
    }
}

/// #496 PR 2 (the #774 hazard): a WAL segment header whose checksum passes but whose version
/// is newer was written by a newer build. The open refuses with `UnsupportedFormat`; it
/// used to read the slot as blank and replay without the segment's commits.
#[test]
fn a_newer_wal_segment_is_refused_not_skipped() {
    // From slot 0, every header is newer: the probe for the segment size meets one first.
    // From slot 1, slot 0 is current and the slot scan meets the newer ones (perf287's nit
    // on #851). The open and the offline check both refuse in each case.
    for first in [0, 1] {
        let vfs = SimVfs::new(4963);
        let o = common::options(Arc::clone(&vfs), 1, 16 << 20);
        let db = Engine::open(Path::new(DB), o.clone()).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        // 4 KiB values across more than one 256 KiB segment.
        for i in 0..128u32 {
            db.commit(
                put(&i.to_be_bytes(), &[7; 4096], &t),
                Some(Durability::Buffered),
            )
            .unwrap();
        }
        // The process dies with the commits only in the WAL.
        vfs.crash(pigeonhole_io::sim::CrashKind::Process);
        drop(db);

        let patched = make_segments_newer(&vfs, first);
        assert!(patched > 0, "no segment header from slot {first}");

        let v: pigeonhole_io::VfsRef = Arc::clone(&vfs) as _;
        let newer = FormatVersion::CURRENT.0 + 1;
        match Engine::check(
            &v,
            Path::new(DB),
            &pigeonhole_engine::CheckOptions::default(),
        ) {
            Err(pigeonhole_engine::Error::UnsupportedFormat(found)) => {
                assert_eq!(found, newer, "check, from slot {first}");
            }
            other => panic!("check, from slot {first}: {other:?}"),
        }
        match Engine::open(Path::new(DB), o) {
            Err(pigeonhole_engine::Error::UnsupportedFormat(found)) => {
                assert_eq!(found, newer, "open, from slot {first}");
            }
            Ok(_) => panic!("from slot {first}: a newer segment was skipped and the open went on"),
            Err(e) => panic!("open, from slot {first}: {e:?}"),
        }
    }
}

/// Rewrites every segment header in stream 0 from slot `first` on at a newer version, its
/// CRC fixed; returns how many it rewrote.
fn make_segments_newer(vfs: &SimVfs, first: u64) -> usize {
    use pigeonhole_format::checksum::crc32c;
    let mut opts = OpenOptions::read();
    opts.write = true;
    let wal = vfs.open(Path::new("/db/data.phdb-wal-0"), opts).unwrap();
    let mut header = [0u8; 60];
    wal.read_at(&mut header, 0).unwrap();
    assert_eq!(&header[..8], b"PHDBWALS", "slot 0 holds a segment");
    let segment = u64::from_le_bytes(header[40..48].try_into().unwrap());
    let slots = wal.len().unwrap() / segment;
    let mut patched = 0;
    for slot in first..slots {
        let at = slot * segment;
        wal.read_at(&mut header, at).unwrap();
        if &header[..8] != b"PHDBWALS" {
            continue;
        }
        let newer = FormatVersion::CURRENT.0 + 1;
        header[8..12].copy_from_slice(&newer.to_le_bytes());
        let crc = crc32c(&header[..56]);
        header[56..60].copy_from_slice(&crc.to_le_bytes());
        wal.write_at(&header, at).unwrap();
        patched += 1;
    }
    wal.sync_all().unwrap();
    patched
}
