//! D336 (ICR 0147): changes pushed as logged beside a batch's ordinary writes
//! (`WriteBatch::push_logged_beside`) go only where routing appends them, a plain commit or a
//! transaction. Every other path refuses them (simp724's review of #2235).

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, Predicate, ValueRef, WriteBatch,
};
use pigeonhole_format::key::Kind;
use pigeonhole_format::{FamilyId, TableId};

fn open(
    name: &str,
) -> (
    std::path::PathBuf,
    std::sync::Arc<Engine>,
    TableId,
    FamilyId,
) {
    let dir =
        std::env::temp_dir().join(format!("phdb-logged-beside-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut o = EngineOptions::new(pigeonhole_io::pread::PreadVfs::new(0));
    o.create_if_missing = true;
    o.shards = 2;
    let db = Engine::open(&dir.join("t.phdb"), o).unwrap();
    let info = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    (dir, db, info.id, info.families[0].id)
}

fn beside(t: TableId, f: FamilyId, row: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.push_logged_beside(t, f, Kind::Put, row, b"q", 7, &[0x00, b'v'])
        .unwrap();
    wb
}

fn invalid(r: Result<impl std::fmt::Debug, Error>) -> String {
    match r {
        Err(Error::InvalidArgument(m)) => m,
        r => panic!("expected InvalidArgument: {r:?}"),
    }
}

/// `commit_logged` takes a logged batch only: one with changes beside an ordinary part,
/// or beside nothing, isn't one.
#[test]
fn commit_logged_refuses_changes_pushed_beside() {
    let (dir, db, t, f) = open("commit-logged");
    let m = invalid(db.commit_logged(beside(t, f, b"r"), None));
    assert!(m.contains("logged commit"), "{m}");
    let mut wb = beside(t, f, b"r");
    wb.put(t, f, b"o", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    invalid(db.commit_logged(wb, None));
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A batch `push_logged` marked takes nothing beside it.
#[test]
fn a_logged_batch_refuses_changes_beside_it() {
    let (dir, db, t, f) = open("marked");
    let mut wb = beside(t, f, b"r");
    wb.push_logged(t, f, Kind::Put, b"s", b"q", 8, &[0x00, b'w'])
        .unwrap();
    let m = invalid(db.commit_logged(wb, None));
    assert!(m.contains("push_logged alone"), "{m}");
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// simp724's P1 on #2235: `check_and_mutate` checks its batch touches only the checked
/// row, and refuses changes pushed beside, which it would otherwise not see.
#[test]
fn check_and_mutate_refuses_changes_pushed_beside() {
    let (dir, db, t, f) = open("check");
    let pred = Predicate::Absent {
        family: f,
        qualifier: b"lock".to_vec(),
    };
    let m = invalid(db.check_and_mutate(t, b"r", &pred, beside(t, f, b"other"), None));
    assert!(m.contains("pushed as logged"), "{m}");
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A plain commit of changes pushed beside nothing appends them as logged.
#[test]
fn a_plain_commit_appends_changes_pushed_beside() {
    let (dir, db, t, f) = open("plain");
    db.commit(beside(t, f, b"r"), None).unwrap();
    let snap = db.snapshot().unwrap();
    let cells = db
        .copy_range(&snap, t, b"r", Some(b"r\0"), usize::MAX)
        .unwrap()
        .cells;
    assert_eq!(cells.len(), 1, "{cells:?}");
    assert_eq!((cells[0].ts, &cells[0].stored[..]), (7, &[0x00, b'v'][..]));
    drop(snap);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
