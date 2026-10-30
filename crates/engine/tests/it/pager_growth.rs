//! Gate analysis D1b: the page file grows ahead of need, outside the allocator lock, so a
//! flush or compaction that needs space finds it free instead of growing the file inline.
//! The engine tests' options turn it on from 4 MiB in 4 MiB blocks, so the crash and
//! model runs cover these growths; this checks that they happen there.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

#[test]
fn flushes_of_a_growing_file_find_space_grown_ahead() {
    let vfs = SimVfs::new(281);
    let db = Engine::open(
        Path::new("/db/data.phdb"),
        common::options(Arc::clone(&vfs), 1, 4 << 20),
    )
    .unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let value = vec![5u8; 4 << 10];
    for i in 0..6144u32 {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            &i.to_be_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        db.commit(wb, Some(Durability::Buffered)).unwrap();
    }
    db.flush().unwrap();
    let m = db.metrics();
    assert!(m.flushes > 1, "{m:?}");
    assert!(m.file_growths_ahead.0 > 0, "no growth ahead: {m:?}");
    // Every growth past the threshold is ahead of need: inline ones only below it.
    let below = (4u64 << 20) / (64 << 10);
    assert!(
        m.file_growths.0 <= below,
        "inline growths: {:?}",
        m.file_growths
    );
    db.close().unwrap();
}
