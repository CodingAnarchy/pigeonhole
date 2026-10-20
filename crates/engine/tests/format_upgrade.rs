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
