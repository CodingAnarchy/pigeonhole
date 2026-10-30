//! ICR 0059 (#496 PR 4, D269): `read_only_fallback`. A writer-process open of a file this
//! build may not write (a read-only-compatible feature bit it does not know, or a family
//! naming a merge operator this process has not registered) opens read-only instead of
//! failing, and writes nothing to the database's files: the main file and every WAL file
//! are byte for byte as they were, through the open, reads and the close. What the WAL
//! held is replayed into memory, so commits a crash left only in the WAL are visible.

use crate::common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, Error, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::superblock::{Features, SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_format::{Durability, FormatVersion};
use pigeonhole_io::sim::{CrashKind, SimVfs};
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/data.phdb";
const PAGE: u64 = 4096;

/// Fails if anything wrote, grew, synced or removed a file since `record_ops` (no sync of
/// an unchanged file either: a read-only open may run on a file it cannot sync).
#[track_caller]
fn no_io(vfs: &SimVfs) {
    let ops = vfs.recorded_ops();
    assert!(ops.is_empty(), "a read-only open did I/O: {ops:?}");
}

/// The database's files (the main file and its WAL streams), by name, with their bytes.
fn files(vfs: &SimVfs) -> BTreeMap<String, Vec<u8>> {
    vfs.list_dir(Path::new("/db"))
        .unwrap()
        .into_iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?.to_owned();
            (name == "data.phdb" || name.starts_with("data.phdb-wal-")).then_some((p, name))
        })
        .map(|(p, name)| {
            let f = vfs.open(&p, OpenOptions::read()).unwrap();
            let mut bytes = vec![0u8; f.len().unwrap() as usize];
            f.read_at(&mut bytes, 0).unwrap();
            (name, bytes)
        })
        .collect()
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

/// A merge operator a later open does not register.
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

/// A database with a plain family `t` and a family `m` using `test.concat`: `flushed` rows
/// flushed to SSTs, then `logged` rows left only in the WAL by a process crash. Written with
/// `shards` shards.
fn crashed_db(seed: u64, shards: usize, flushed: u32, logged: u32) -> Arc<SimVfs> {
    let vfs = SimVfs::new(seed);
    let mut o = common::options(Arc::clone(&vfs), shards, 16 << 20);
    o.merge_operators.register(Arc::new(Concat));
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.create_table(
        "m",
        &[(
            "f".into(),
            FamilyOptions::default().merge_operator("test.concat"),
        )],
    )
    .unwrap();
    for i in 0..flushed {
        db.commit(
            put(&i.to_be_bytes(), b"flushed", &t),
            Some(Durability::Sync),
        )
        .unwrap();
    }
    db.flush().unwrap();
    for i in flushed..flushed + logged {
        db.commit(put(&i.to_be_bytes(), b"logged", &t), Some(Durability::Sync))
            .unwrap();
    }
    vfs.crash(CrashKind::Process);
    drop(db);
    vfs
}

/// Opens with `read_only_fallback` (no merge operator registered), reads every row,
/// tries each kind of write, closes, and checks the files never changed.
fn open_read_only_and_check(vfs: &Arc<SimVfs>, shards: usize, rows: u32) {
    let before = files(vfs);
    let mut o = common::options(Arc::clone(vfs), shards, 16 << 20);
    o.read_only_fallback = true;
    vfs.record_ops();
    let db = Engine::open(Path::new(DB), o).unwrap();
    let info = db.info().unwrap();
    assert!(info.read_only);
    let t = db.table("t").unwrap();
    let snap = db.snapshot().unwrap();
    for i in 0..rows {
        let cell = db
            .get(&snap, t.id, t.families[0].id, &i.to_be_bytes(), b"q")
            .unwrap();
        assert!(cell.is_some(), "row {i}");
    }
    drop(snap);
    assert!(matches!(
        db.commit(put(b"new", b"v", &t), Some(Durability::Sync)),
        Err(Error::ReadOnly)
    ));
    assert!(matches!(db.flush(), Err(Error::ReadOnly)));
    assert!(matches!(db.compact(None), Err(Error::ReadOnly)));
    assert!(matches!(db.shrink(), Err(Error::ReadOnly)));
    assert!(matches!(
        db.create_table("u", &[("f".into(), FamilyOptions::default())]),
        Err(Error::ReadOnly)
    ));
    db.close().unwrap();
    drop(db);
    no_io(vfs);
    assert!(files(vfs) == before, "a read-only open changed the files");
}

#[test]
fn an_unregistered_merge_operator_opens_read_only_and_writes_nothing() {
    let vfs = crashed_db(5901, 2, 40, 40);
    // Without the fallback: refused, as before.
    let o = common::options(Arc::clone(&vfs), 2, 16 << 20);
    assert!(matches!(
        Engine::open(Path::new(DB), o),
        Err(Error::UnknownMergeOperator(ref n)) if n == "test.concat"
    ));
    open_read_only_and_check(&vfs, 2, 80);
    // A later writable open (the operator registered) still replays the WAL: nothing the
    // read-only open did consumed it.
    let mut o = common::options(Arc::clone(&vfs), 2, 16 << 20);
    o.merge_operators.register(Arc::new(Concat));
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db.table("t").unwrap();
    let snap = db.snapshot().unwrap();
    for i in 0..80u32 {
        assert!(
            db.get(&snap, t.id, t.families[0].id, &i.to_be_bytes(), b"q")
                .unwrap()
                .is_some(),
            "row {i} after the read-only open"
        );
    }
    drop(snap);
    db.close().unwrap();
}

#[test]
fn a_read_only_open_takes_any_shard_count() {
    // Replay routes each record to the shard that owns its rows, so the read-only open
    // needs no particular layout (D-a): written with 2 shards, opened with 1 and with 3.
    for shards in [1, 3] {
        let vfs = crashed_db(5902, 2, 20, 30);
        open_read_only_and_check(&vfs, shards, 50);
    }
}

#[test]
fn a_replay_that_does_not_fit_fails_read_only_open() {
    let vfs = crashed_db(5903, 1, 0, 0);
    // More WAL than a small budget's arena holds.
    let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    o.merge_operators.register(Arc::new(Concat));
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db.table("t").unwrap();
    for i in 0..400u32 {
        db.commit(
            put(&i.to_be_bytes(), &[9; 8192], &t),
            Some(Durability::Sync),
        )
        .unwrap();
    }
    vfs.crash(CrashKind::Process);
    drop(db);
    let before = files(&vfs);
    let mut o = common::options(Arc::clone(&vfs), 1, 1 << 20);
    o.read_only_fallback = true;
    vfs.record_ops();
    match Engine::open(Path::new(DB), o) {
        Err(Error::ReadOnlyOpen(why)) => assert!(why.contains("memtable_budget"), "{why}"),
        Ok(_) => panic!("opened"),
        Err(e) => panic!("{e:?}"),
    }
    no_io(&vfs);
    assert!(files(&vfs) == before, "the refused open changed the files");
}

/// Rewrites both superblock pages, keeping everything but what `edit` changes.
fn edit_superblocks(vfs: &SimVfs, edit: impl Fn(&mut Superblock)) {
    let mut opts = OpenOptions::read();
    opts.write = true;
    let f = vfs.open(Path::new(DB), opts).unwrap();
    for p in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        let mut buf = [0u8; PAGE as usize];
        f.read_at(&mut buf, p * PAGE).unwrap();
        let mut sb = Superblock::decode(&buf).unwrap();
        edit(&mut sb);
        sb.encode(&mut buf);
        f.write_at(&buf, p * PAGE).unwrap();
    }
    f.sync_all().unwrap();
}

#[test]
fn an_unknown_ro_compat_bit_refuses_a_writer_unless_it_falls_back() {
    let vfs = crashed_db(5904, 1, 10, 10);
    // As a newer build would leave it: a ro_compat feature this build does not know, set
    // in both pages (D269's registry semantics), with the bit this build will never define.
    let bit = 1 << 63;
    edit_superblocks(&vfs, |sb| {
        sb.features = Features {
            ro_compat: bit,
            ..Features::default()
        }
    });
    let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    o.merge_operators.register(Arc::new(Concat));
    match Engine::open(Path::new(DB), o.clone()) {
        Err(Error::UnsupportedFeature(what)) => assert!(what.contains("read-only"), "{what}"),
        Ok(_) => panic!("a writer opened a file with an unknown ro_compat bit"),
        Err(e) => panic!("{e:?}"),
    }
    let before = files(&vfs);
    o.read_only_fallback = true;
    vfs.record_ops();
    let db = Engine::open(Path::new(DB), o).unwrap();
    let info = db.info().unwrap();
    assert!(info.read_only);
    assert_eq!(info.features.ro_compat, bit);
    let t = db.table("t").unwrap();
    let snap = db.snapshot().unwrap();
    for i in 0..20u32 {
        assert!(
            db.get(&snap, t.id, t.families[0].id, &i.to_be_bytes(), b"q")
                .unwrap()
                .is_some(),
            "row {i}"
        );
    }
    drop(snap);
    db.close().unwrap();
    drop(db);
    no_io(&vfs);
    assert!(files(&vfs) == before, "the fallback open changed the files");
}

#[test]
fn a_read_only_open_leaves_an_older_format_unupgraded() {
    // nit 3 on #851: an older file opened read-only keeps its version (an older build can
    // still open it), and info reports the file's version.
    let vfs = crashed_db(5905, 1, 10, 0);
    edit_superblocks(&vfs, |sb| sb.version = FormatVersion(2));
    let before = files(&vfs);
    let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    o.read_only_fallback = true;
    vfs.record_ops();
    let db = Engine::open(Path::new(DB), o).unwrap();
    assert_eq!(db.info().unwrap().format_version, 2);
    db.close().unwrap();
    drop(db);
    no_io(&vfs);
    assert!(
        files(&vfs) == before,
        "the read-only open upgraded the file"
    );
}

#[test]
fn a_read_only_open_never_compacts_even_with_compaction_debt() {
    // D-e: the unregistered-merge open used to compact the database's other families.
    // Eight L0 SSTs written under a high trigger, then opened read-only under a low one:
    // nothing compacts (nor tries to, and fails on the read-only pager).
    let vfs = SimVfs::new(5906);
    let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    o.merge_operators.register(Arc::new(Concat));
    o.compaction.l0_trigger = 100;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.create_table(
        "m",
        &[(
            "f".into(),
            FamilyOptions::default().merge_operator("test.concat"),
        )],
    )
    .unwrap();
    for round in 0..8u32 {
        for i in 0..20u32 {
            let row = (round * 100 + i).to_be_bytes();
            db.commit(put(&row, b"v", &t), Some(Durability::Sync))
                .unwrap();
        }
        db.flush().unwrap();
    }
    db.close().unwrap();
    drop(db);

    let before = files(&vfs);
    let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    o.read_only_fallback = true;
    o.compaction.l0_trigger = 2;
    vfs.record_ops();
    let db = Engine::open(Path::new(DB), o).unwrap();
    // Give the shard thread time to pick work, as it would at once with a writable open.
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(5));
        let m = db.metrics();
        assert_eq!(
            (m.compactions, m.compaction_failures),
            (0, 0),
            "a compaction ran"
        );
        assert_eq!((m.flushes, m.flush_failures), (0, 0), "a flush ran");
    }
    db.close().unwrap();
    drop(db);
    no_io(&vfs);
    assert!(files(&vfs) == before, "a read-only open changed the files");
}
