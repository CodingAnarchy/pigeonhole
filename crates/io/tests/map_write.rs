//! D312: [`File::map_write`] on real files. Linux maps; elsewhere it declines and callers
//! write. Real syscalls, so not run under Miri (the store itself is checked there, `map.rs`).
#![cfg(not(miri))]

mod common;

use common::{TempDir, unique};
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::{OpenOptions, Vfs};

#[test]
fn stores_through_a_mapping_are_what_reads_see() {
    let dir = TempDir::new(&unique("map-write"));
    let vfs = PreadVfs::new(0);
    let path = dir.0.join("f");
    let file = vfs.open(&path, OpenOptions::read_write_create()).unwrap();
    let page = 64 * 1024; // a multiple of every kernel page size
    file.set_len(4 * page as u64).unwrap();
    // Past the end: declined, never a mapping a store would fault through.
    assert!(file.map_write(3 * page as u64, 2 * page).unwrap().is_none());
    // A misaligned offset: declined.
    assert!(file.map_write(1, page).unwrap().is_none());
    let Some(mut map) = file.map_write(page as u64, 2 * page).unwrap() else {
        // Only Linux maps for now (a sync is known to cover mapped stores there).
        #[cfg(target_os = "linux")]
        panic!("Linux maps a writable file");
        #[cfg(not(target_os = "linux"))]
        return;
    };
    assert_eq!(map.len(), 2 * page);
    map.write(0, b"first").unwrap();
    map.write(2 * page - 4, b"last").unwrap();
    // Refused past the mapping's end, without touching it.
    assert!(map.write(2 * page - 3, b"last").is_err());
    // Reads through the file see the stores at once (the same page cache), before and after
    // the mapping is gone, and after a sync.
    let mut buf = [0u8; 5];
    file.read_at(&mut buf, page as u64).unwrap();
    assert_eq!(&buf, b"first");
    drop(map);
    file.sync_data().unwrap();
    let mut buf = [0u8; 4];
    file.read_at(&mut buf, 3 * page as u64 - 4).unwrap();
    assert_eq!(&buf, b"last");
    // Another handle sees them too.
    let other = vfs.open(&path, OpenOptions::read()).unwrap();
    other.read_at(&mut buf, 3 * page as u64 - 4).unwrap();
    assert_eq!(&buf, b"last");
    // A read-only handle never maps.
    assert!(other.map_write(0, page).unwrap().is_none());
}
