//! The open-time length sync (#158, D203): `OpenedPager::finish` syncs the file's length
//! unless the last close was clean and the file is exactly as long as that root recorded. A
//! growth or a truncation a crash interrupted leaves it longer or shorter, and is synced.

use std::path::Path;
use std::sync::Arc;

use pigeonhole_io::sim::{CrashKind, SimOp, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs, VfsRef};
use pigeonhole_pager::{Pager, Root};

const PATH: &str = "/db/data.phdb";

fn path() -> &'static Path {
    Path::new(PATH)
}

/// A pager file with one committed root, its last close clean or not.
fn prepare(clean: bool) -> Arc<SimVfs> {
    let vfs = SimVfs::new(5);
    let v: VfsRef = vfs.clone();
    let pager = Pager::create(&v, path()).unwrap();
    pager
        .commit_root(Root {
            manifest_version: 1,
            ..Root::default()
        })
        .unwrap();
    if clean {
        pager.mark_clean().unwrap();
    }
    drop(pager);
    vfs
}

/// Opens the file for writing and reports whether `finish` synced the length (`sync_all`).
fn open_syncs_length(vfs: &Arc<SimVfs>) -> bool {
    let v: VfsRef = vfs.clone();
    vfs.record_ops();
    let opened = Pager::open(&v, path(), true).unwrap();
    let before = vfs.recorded_ops().len();
    let _pager = opened.finish(Vec::new()).unwrap();
    vfs.recorded_ops()[before..]
        .iter()
        .any(|op| matches!(op, SimOp::Sync { metadata: true, .. }))
}

/// Changes the file's length behind the pager's back and loses the process (the page cache
/// keeps the length; nothing synced it): a growth or a truncation a crash interrupted.
fn interrupted_resize(vfs: &Arc<SimVfs>, by: i64) {
    let f = vfs.open(path(), OpenOptions::read_write_create()).unwrap();
    let len = f.len().unwrap();
    f.set_len(len.checked_add_signed(by).unwrap()).unwrap();
    drop(f);
    vfs.crash(CrashKind::Process);
}

#[test]
fn a_clean_close_with_the_recorded_length_skips_the_length_sync() {
    let vfs = prepare(true);
    assert!(!open_syncs_length(&vfs), "clean, exact length: no sync");
}

#[test]
fn an_unclean_close_syncs_the_length() {
    let vfs = prepare(false);
    assert!(open_syncs_length(&vfs), "unclean: synced");
}

#[test]
fn a_length_longer_or_shorter_than_recorded_is_synced() {
    // Longer: a growth's `fallocate` landed, its `sync_all` did not.
    let vfs = prepare(true);
    interrupted_resize(&vfs, 64 << 10);
    assert!(open_syncs_length(&vfs), "longer than recorded: synced");
    // Shorter: a truncation's `set_len` landed, its `sync_all` did not.
    let vfs = prepare(true);
    interrupted_resize(&vfs, -4096);
    assert!(open_syncs_length(&vfs), "shorter than recorded: synced");
}
