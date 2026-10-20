//! The format upgrade at a writable open (ICR 0046): if either superblock page holds an older
//! format version, `OpenedPager::finish` commits the root twice, so both pages carry the
//! current version before the engine writes anything an older build could not read. An
//! older build then refuses the file by its version instead of falling back to the page it
//! can still decode.

use std::path::Path;
use std::sync::Arc;

use pigeonhole_format::FormatVersion;
use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_io::sim::{SimOp, SimVfs};
use pigeonhole_io::{FileRef, OpenOptions, Vfs, VfsRef};
use pigeonhole_pager::{Pager, Root};

const PATH: &str = "/db/data.phdb";
const PAGE: usize = 4096;

fn path() -> &'static Path {
    Path::new(PATH)
}

fn file(vfs: &Arc<SimVfs>) -> FileRef {
    let mut opts = OpenOptions::read();
    opts.write = true;
    vfs.open(path(), opts).unwrap()
}

/// A pager file whose two superblock pages both decode: roots at manifest versions 1 and 2.
fn prepare() -> Arc<SimVfs> {
    let vfs = SimVfs::new(7);
    let v: VfsRef = vfs.clone();
    let pager = Pager::create(&v, path()).unwrap();
    for manifest_version in [1, 2] {
        pager
            .commit_root(Root {
                manifest_version,
                ..Root::default()
            })
            .unwrap();
    }
    drop(pager);
    vfs
}

fn read_page(f: &FileRef, page: u64) -> pigeonhole_format::Result<Superblock> {
    let mut buf = [0u8; PAGE];
    f.read_at(&mut buf, page * PAGE as u64).unwrap();
    Superblock::decode(&buf)
}

/// Rewrites `page` as written by a build of format `version`, keeping everything else.
fn set_version(vfs: &Arc<SimVfs>, page: u64, version: u32) {
    let f = file(vfs);
    let mut sb = read_page(&f, page).unwrap();
    sb.version = FormatVersion(version);
    let mut buf = [0u8; PAGE];
    sb.encode(&mut buf);
    f.write_at(&buf, page * PAGE as u64).unwrap();
    f.sync_all().unwrap();
}

fn versions(vfs: &Arc<SimVfs>) -> [u32; 2] {
    let f = file(vfs);
    [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B].map(|p| read_page(&f, p).unwrap().version.0)
}

/// Opens and finishes the file; returns its root and whether it wrote a superblock page.
fn open(vfs: &Arc<SimVfs>, writable: bool) -> (Root, bool) {
    let v: VfsRef = vfs.clone();
    vfs.record_ops();
    let opened = Pager::open(&v, path(), writable).unwrap();
    let pager = opened.finish(Vec::new()).unwrap();
    let wrote = vfs.recorded_ops().iter().any(|op| {
        matches!(op, SimOp::Write { offset, .. }
            if *offset == SUPERBLOCK_PAGE_A * PAGE as u64 || *offset == SUPERBLOCK_PAGE_B * PAGE as u64)
    });
    (pager.root(), wrote)
}

#[test]
fn an_older_file_opened_for_writing_carries_the_current_version_on_both_pages() {
    let vfs = prepare();
    let current = FormatVersion::CURRENT.0;
    assert_eq!(versions(&vfs), [current; 2]);
    // As 0.3.x left it: version 2 on both pages.
    set_version(&vfs, SUPERBLOCK_PAGE_A, 2);
    set_version(&vfs, SUPERBLOCK_PAGE_B, 2);
    // A reader never writes.
    let (root, wrote) = open(&vfs, false);
    assert_eq!(root.manifest_version, 2);
    assert!(!wrote);
    assert_eq!(versions(&vfs), [2, 2]);
    // A writer upgrades both pages, naming the same root.
    let (root, wrote) = open(&vfs, true);
    assert_eq!(root.manifest_version, 2);
    assert!(wrote);
    assert_eq!(versions(&vfs), [current; 2]);
    // Once both are current, opening writes nothing.
    let (root, wrote) = open(&vfs, true);
    assert_eq!(root.manifest_version, 2);
    assert!(!wrote, "an open of a current file wrote a superblock");
}

#[test]
fn one_older_page_left_by_a_crash_mid_upgrade_is_upgraded_at_the_next_open() {
    // A crash between the upgrade's two commits leaves one page older, whichever page is
    // current: the next writable open finishes the upgrade.
    for page in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        let vfs = prepare();
        set_version(&vfs, page, 2);
        let (root, wrote) = open(&vfs, true);
        assert_eq!(root.manifest_version, 2, "page {page}");
        assert!(wrote, "page {page}");
        assert_eq!(versions(&vfs), [FormatVersion::CURRENT.0; 2], "page {page}");
    }
}

#[test]
fn a_new_file_has_nothing_to_upgrade() {
    // Page B of a new file is blank, not older.
    let vfs = SimVfs::new(8);
    let v: VfsRef = vfs.clone();
    drop(Pager::create(&v, path()).unwrap());
    let (_, wrote) = open(&vfs, true);
    assert!(!wrote);
}
