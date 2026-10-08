//! Blob separation and blob GC (issue #33): compaction moves values above a family's
//! `blob_threshold` into blob files, every read path returns the values (gets, row reads,
//! scans, value predicates, conditional writes, snapshots, reopens), blob GC empties files
//! that are mostly garbage, and a file's extents go once nothing references it.
//!
//! Application-owned with one shard driven by the test, so flushes and compactions run
//! exactly when the test says.

mod common;

use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, MergeError, MergeOperator, Predicate,
    ReadSpec, ScanSpec, TableInfo, ValuePredicate, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, TableId};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

/// One shard; only explicit flushes and compactions unless `background`.
fn options(vfs: Arc<SimVfs>, background: bool) -> EngineOptions {
    let mut o = common::options(vfs, 1, 16 << 20);
    o.memtable_freeze_bytes = 4 << 20;
    o.compaction.target_sst_bytes = 64 << 10;
    if background {
        o.compaction.l0_trigger = 1;
        o.compaction.level_base_bytes = 64 << 10;
    } else {
        o.compaction.l0_trigger = u32::MAX;
        o.compaction.level_base_bytes = u64::MAX;
    }
    o
}

struct Rig {
    db: Arc<Engine>,
    shard: EngineShard,
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>, background: bool) -> Self {
        Self::open_at(vfs, DB, background)
    }

    fn open_at(vfs: &Arc<SimVfs>, path: &str, background: bool) -> Self {
        let (db, mut shards) =
            Engine::open_application_owned(Path::new(path), options(Arc::clone(vfs), background))
                .unwrap();
        let mut rig = Self {
            db,
            shard: shards.remove(0),
        };
        rig.idle();
        rig
    }

    /// Runs the shard until it has nothing left to do (background work included).
    fn idle(&mut self) {
        for _ in 0..1_000_000 {
            if !self.shard.run_once(u64::MAX) {
                return;
            }
        }
        panic!("the shard never went idle");
    }

    fn wait<F: Future + Unpin>(&mut self, mut f: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..1_000_000 {
            if let Poll::Ready(r) = Pin::new(&mut f).poll(&mut cx) {
                return r;
            }
            self.shard.run_once(u64::MAX);
        }
        panic!("the shard never resolved the request");
    }

    fn commit(&mut self, wb: WriteBatch) {
        let pending = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
        self.wait(pending).unwrap();
    }

    fn flush(&mut self) {
        let p = self.db.flush_pending().unwrap();
        self.wait(p).unwrap();
        self.idle();
    }

    fn compact(&mut self) {
        let p = self.db.compact_pending(None).unwrap();
        self.wait(p).unwrap();
        self.idle();
    }

    fn close(mut self) {
        self.db.close().unwrap();
        for _ in 0..1_000_000 {
            self.shard.run_once(u64::MAX);
            if let Some(r) = self.shard.closed() {
                r.unwrap();
                return;
            }
        }
        panic!("the close never finished");
    }

    fn check(&self) {
        self.db.check_blob_accounting().unwrap();
    }
}

/// A family separating values longer than 100 bytes.
fn family() -> FamilyOptions {
    FamilyOptions {
        blob_threshold: 100,
        ..FamilyOptions::default()
    }
}

/// Row `i`'s value in generation `generation`: large (separated) for most rows, small
/// (inline) for every fifth.
fn value(i: u32, generation: u8) -> Vec<u8> {
    let len = if i.is_multiple_of(5) {
        20
    } else {
        300 + (i as usize % 7) * 50
    };
    let mut v = vec![b'a' + generation; len];
    v[..4].copy_from_slice(&i.to_le_bytes());
    v
}

fn row(i: u32) -> Vec<u8> {
    format!("row{i:04}").into_bytes()
}

fn write(rig: &mut Rig, t: &TableInfo, rows: std::ops::Range<u32>, generation: u8) {
    for i in rows {
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            t.families[0].id,
            &row(i),
            b"q",
            None,
            ValueRef::Bytes(&value(i, generation)),
        )
        .unwrap();
        rig.commit(wb);
    }
}

/// Reads every row through gets, row reads and a scan; each must hold `expected(i)`.
fn assert_reads(db: &Engine, t: &TableInfo, rows: u32, expected: impl Fn(u32) -> Vec<u8>) {
    let f = t.families[0].id;
    let snap = db.snapshot().unwrap();
    for i in 0..rows {
        let want = expected(i);
        let got = db.get(&snap, t.id, f, &row(i), b"q").unwrap().unwrap();
        assert_eq!(got.value(), ValueRef::Bytes(&want), "get row {i}");
        let latest = db.get_latest(t.id, f, &row(i), b"q").unwrap().unwrap();
        assert_eq!(latest.value(), ValueRef::Bytes(&want), "get_latest row {i}");
        let r = db
            .read_row(&snap, t.id, &row(i), &ReadSpec::default())
            .unwrap()
            .unwrap();
        assert_eq!(
            r.cells[0].data.value(),
            ValueRef::Bytes(&want),
            "read_row {i}"
        );
    }
    let mut cursor = db
        .scan(
            &snap,
            t.id,
            ScanSpec::new(Bound::Unbounded, Bound::Unbounded),
        )
        .unwrap();
    let mut n = 0;
    while cursor.next_row().unwrap() {
        let i = n;
        assert_eq!(cursor.row(), row(i));
        let cell = cursor.next_cell().unwrap().unwrap();
        let mut want = vec![0u8];
        want.extend_from_slice(&expected(i));
        assert_eq!(cell.stored, &want[..], "scan row {i}");
        assert_eq!(cursor.current_data().value(), ValueRef::Bytes(&expected(i)));
        n += 1;
    }
    assert_eq!(n, rows);
}

/// Rows a scan with a value predicate returns.
fn rows_matching(db: &Engine, t: TableId, predicate: ValuePredicate) -> Vec<Vec<u8>> {
    let snap = db.snapshot().unwrap();
    let mut spec = ScanSpec::new(Bound::Unbounded, Bound::Unbounded);
    spec.read.value = Some(predicate);
    let mut cursor = db.scan(&snap, t, spec).unwrap();
    let mut out = Vec::new();
    while cursor.next_row().unwrap() {
        out.push(cursor.row().to_vec());
    }
    out
}

#[test]
fn separated_values_read_back_through_every_path() {
    let vfs = SimVfs::new(33);
    let mut rig = Rig::open(&vfs, false);
    let t = rig.db.create_table("t", &[("f".into(), family())]).unwrap();
    write(&mut rig, &t, 0..60, 0);
    rig.flush();
    let files = rig.db.blob_files();
    assert!(!files.is_empty(), "the flush separated the large values");
    rig.compact();
    assert_eq!(rig.db.blob_files(), files, "values are separated once");
    assert!(
        files.iter().all(|f| f.2 == f.3),
        "every new file is all live"
    );
    rig.check();
    assert_reads(&rig.db, &t, 60, |i| value(i, 0));

    // Value predicates see the value, not the pointer (row 7's value is separated).
    let mut prefix = 7u32.to_le_bytes().to_vec();
    prefix.push(b'a');
    assert_eq!(
        rows_matching(&rig.db, t.id, ValuePredicate::Prefix(prefix)),
        vec![row(7)]
    );
    let all_a = ValuePredicate::Range(Bound::Unbounded, Bound::Unbounded);
    assert_eq!(rows_matching(&rig.db, t.id, all_a).len(), 60);

    rig.close();
    let rig = Rig::open(&vfs, false);
    let t = rig.db.table("t").unwrap();
    rig.check();
    assert_reads(&rig.db, &t, 60, |i| value(i, 0));
    rig.close();
}

#[test]
fn a_conditional_write_compares_a_separated_value() {
    let vfs = SimVfs::new(34);
    let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    o.compaction.l0_trigger = u32::MAX;
    o.compaction.level_base_bytes = u64::MAX;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db.create_table("t", &[("f".into(), family())]).unwrap();
    let f = t.families[0].id;
    let big = vec![b'z'; 500];
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(&big))
        .unwrap();
    db.commit(wb, None).unwrap();
    db.flush().unwrap();
    db.compact(None).unwrap();
    assert_eq!(db.blob_files().len(), 1);
    let cas = |expected: &[u8]| {
        let mut wb = WriteBatch::new();
        wb.put(t.id, f, b"r", b"other", None, ValueRef::Bytes(b"x"))
            .unwrap();
        let predicate = Predicate::Value {
            family: f,
            qualifier: b"q".to_vec(),
            predicate: ValuePredicate::Equals(expected.to_vec()),
        };
        db.check_and_mutate(t.id, b"r", &predicate, wb, None)
            .unwrap()
            .0
    };
    assert!(!cas(b"something else"));
    assert!(
        cas(&big),
        "the predicate compares the separated value itself"
    );
    db.close().unwrap();
}

#[test]
fn blob_gc_reclaims_overwritten_values() {
    let vfs = SimVfs::new(35);
    let mut rig = Rig::open(&vfs, false);
    let one_version = FamilyOptions {
        max_versions: 1,
        ..family()
    };
    let t = rig
        .db
        .create_table("t", &[("f".into(), one_version)])
        .unwrap();
    let f = t.families[0].id;
    write(&mut rig, &t, 0..80, 0);
    rig.flush();
    let first: Vec<u32> = rig.db.blob_files().iter().map(|f| f.1).collect();
    assert!(!first.is_empty(), "the flush separated the large values");

    // A snapshot from before the overwrite keeps the old versions, and their values, live.
    let old = rig.db.snapshot().unwrap();
    write(&mut rig, &t, 0..80, 1);
    rig.flush();
    rig.compact();
    rig.check();
    assert!(
        first
            .iter()
            .all(|id| rig.db.blob_files().iter().any(|f| f.1 == *id && f.2 == f.3)),
        "the snapshot keeps every old value: {:?}",
        rig.db.blob_files()
    );
    for i in [1u32, 2, 33, 79] {
        let v = rig.db.get(&old, t.id, f, &row(i), b"q").unwrap().unwrap();
        assert_eq!(
            v.value(),
            ValueRef::Bytes(&value(i, 0)),
            "row {i} at the snapshot"
        );
    }
    assert_reads(&rig.db, &t, 80, |i| value(i, 1));

    // Released, the old versions go at the next compaction, and the first files with them.
    // Three quarters of the second generation become garbage too, so blob GC empties its
    // files in the background.
    drop(old);
    write(&mut rig, &t, 0..60, 2);
    rig.flush();
    rig.compact();
    rig.check();
    let garbage = |rig: &Rig| -> u64 { rig.db.blob_files().iter().map(|f| f.2 - f.3).sum() };
    assert!(
        rig.db.blob_files().iter().all(|f| !first.contains(&f.1)),
        "files with no live value left are dropped: {:?}",
        rig.db.blob_files()
    );
    assert_eq!(garbage(&rig), 0, "{:?}", rig.db.blob_files());

    // A little garbage (below blob GC's ratio) stays until a full compaction rewrites it.
    write(&mut rig, &t, 70..72, 3);
    rig.flush();
    rig.compact();
    rig.check();
    assert!(garbage(&rig) > 0, "{:?}", rig.db.blob_files());
    write(&mut rig, &t, 79..80, 3);
    rig.flush();
    rig.compact();
    rig.check();
    assert_eq!(garbage(&rig), 0, "{:?}", rig.db.blob_files());
    assert_reads(&rig.db, &t, 80, |i| match i {
        0..60 => value(i, 2),
        70..72 | 79 => value(i, 3),
        _ => value(i, 1),
    });
    // Retired extents are reclaimed once no view can reach them (shrink reclaims first).
    rig.db.shrink().unwrap();
    assert_eq!(
        rig.db.unreferenced_bytes(),
        0,
        "every dropped extent was reclaimed"
    );
    rig.close();
}

#[test]
fn background_blob_gc_empties_mostly_garbage_files() {
    let vfs = SimVfs::new(36);
    let mut rig = Rig::open(&vfs, true);
    let t = rig.db.create_table("t", &[("f".into(), family())]).unwrap();
    write(&mut rig, &t, 0..100, 0);
    rig.flush();
    let first: Vec<u32> = rig.db.blob_files().iter().map(|f| f.1).collect();
    assert!(!first.is_empty(), "the flush separated values");
    // Deleting most rows turns most of the first files into garbage; compactions drop the
    // dead pointers and blob GC rewrites what is left of those files.
    for i in 0..100u32 {
        if !i.is_multiple_of(10) {
            let mut wb = WriteBatch::new();
            wb.delete_row(t.id, &row(i), None).unwrap();
            rig.commit(wb);
        }
    }
    rig.flush();
    for _ in 0..4 {
        rig.flush();
    }
    rig.check();
    let files = rig.db.blob_files();
    assert!(
        files.iter().all(|f| !first.contains(&f.1)),
        "blob GC left a mostly-garbage file: {files:?}"
    );
    let snap = rig.db.snapshot().unwrap();
    let f = t.families[0].id;
    for i in 0..100u32 {
        let got = rig.db.get(&snap, t.id, f, &row(i), b"q").unwrap();
        if i.is_multiple_of(10) {
            assert_eq!(got.unwrap().value(), ValueRef::Bytes(&value(i, 0)));
        } else {
            assert!(got.is_none(), "row {i} was deleted");
        }
    }
    drop(snap);
    rig.close();
}

#[test]
fn dropping_a_table_drops_its_blob_files() {
    let vfs = SimVfs::new(37);
    let mut rig = Rig::open(&vfs, false);
    let t = rig.db.create_table("t", &[("f".into(), family())]).unwrap();
    let keep = rig
        .db
        .create_table("keep", &[("f".into(), family())])
        .unwrap();
    write(&mut rig, &t, 0..30, 0);
    write(&mut rig, &keep, 0..30, 2);
    rig.flush();
    rig.compact();
    let kept_family = keep.families[0].id;
    assert!(rig.db.blob_files().iter().any(|f| f.0 != kept_family));
    rig.db.drop_table(t.id).unwrap();
    rig.idle();
    assert!(rig.db.blob_files().iter().all(|f| f.0 == kept_family));
    rig.check();
    assert_reads(&rig.db, &keep, 30, |i| value(i, 2));
    rig.db.shrink().unwrap();
    assert_eq!(rig.db.unreferenced_bytes(), 0);
    rig.close();
}

#[test]
fn a_snapshot_reads_a_blob_file_that_blob_gc_dropped() {
    let vfs = SimVfs::new(38);
    let mut rig = Rig::open(&vfs, false);
    let one_version = FamilyOptions {
        max_versions: 1,
        ..family()
    };
    let t = rig
        .db
        .create_table("t", &[("f".into(), one_version)])
        .unwrap();
    let f = t.families[0].id;
    write(&mut rig, &t, 0..80, 0);
    rig.flush();
    let first: Vec<u32> = rig.db.blob_files().iter().map(|f| f.1).collect();
    // Overwriting three quarters of the rows makes the first files mostly garbage once a
    // compaction drops the old versions; blob GC then empties them. A snapshot taken before
    // all that (but after the overwrite, so the old versions are not its) has a view that
    // still names the first files.
    write(&mut rig, &t, 0..60, 1);
    rig.flush();
    let snap = rig.db.snapshot().unwrap();
    rig.compact();
    rig.check();
    assert!(
        rig.db.blob_files().iter().all(|f| !first.contains(&f.1)),
        "blob GC dropped the first files: {:?}",
        rig.db.blob_files()
    );
    // Their extents are retired, not reused, while the snapshot's view lives.
    for i in [60u32, 61, 79] {
        let v = rig.db.get(&snap, t.id, f, &row(i), b"q").unwrap().unwrap();
        assert_eq!(v.value(), ValueRef::Bytes(&value(i, 0)), "row {i}");
    }
    write(&mut rig, &t, 0..80, 2);
    rig.flush();
    rig.compact();
    for i in [60u32, 61, 79] {
        let v = rig.db.get(&snap, t.id, f, &row(i), b"q").unwrap().unwrap();
        assert_eq!(
            v.value(),
            ValueRef::Bytes(&value(i, 0)),
            "row {i} after more writes"
        );
    }
    drop(snap);
    rig.db.shrink().unwrap();
    assert_eq!(rig.db.unreferenced_bytes(), 0);
    rig.close();
}

/// Appends `Bytes` payloads: the result is the base, then each operand oldest first.
#[derive(Debug)]
struct Append;

impl Append {
    fn bytes(stored: &[u8]) -> Result<&[u8], MergeError> {
        match stored.split_first() {
            Some((0, payload)) => Ok(payload),
            _ => Err(MergeError {
                operator: "test.append".into(),
                message: "not bytes".into(),
            }),
        }
    }
}

impl MergeOperator for Append {
    fn name(&self) -> &str {
        "test.append"
    }

    fn merge(&self, acc: &mut Vec<u8>, older: &[u8]) -> Result<(), MergeError> {
        let mut out = vec![0u8];
        out.extend_from_slice(Self::bytes(older)?);
        out.extend_from_slice(Self::bytes(acc)?);
        *acc = out;
        Ok(())
    }

    fn finish(&self, base: Option<&[u8]>, acc: &mut Vec<u8>) -> Result<(), MergeError> {
        let mut out = vec![0u8];
        if let Some(b) = base {
            out.extend_from_slice(Self::bytes(b)?);
        }
        out.extend_from_slice(Self::bytes(acc)?);
        *acc = out;
        Ok(())
    }
}

#[test]
fn operands_fold_onto_a_separated_base_on_every_read_path() {
    let vfs = SimVfs::new(40);
    let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
    o.compaction.l0_trigger = u32::MAX;
    o.compaction.level_base_bytes = u64::MAX;
    o.merge_operators.register(Arc::new(Append));
    let db = Engine::open(Path::new(DB), o).unwrap();
    let appended = FamilyOptions {
        merge_operator: "test.append".into(),
        ..family()
    };
    let counter = FamilyOptions {
        merge_operator: "pigeonhole.i64_add".into(),
        ..family()
    };
    let t = db
        .create_table("t", &[("a".into(), appended), ("c".into(), counter)])
        .unwrap();
    let (a, c) = (t.families[0].id, t.families[1].id);
    let base = vec![b'b'; 500];
    let mut wb = WriteBatch::new();
    wb.put(t.id, a, b"r", b"q", None, ValueRef::Bytes(&base))
        .unwrap();
    wb.put(t.id, c, b"r", b"n", None, ValueRef::Bytes(&base))
        .unwrap();
    db.commit(wb, None).unwrap();
    db.flush().unwrap();
    assert_eq!(db.blob_files().len(), 2, "both bases are separated");
    let mut wb = WriteBatch::new();
    wb.merge(t.id, a, b"r", b"q", ValueRef::Bytes(b"xy"))
        .unwrap();
    wb.merge(t.id, c, b"r", b"n", ValueRef::I64(1)).unwrap();
    db.commit(wb, None).unwrap();
    let mut want = base.clone();
    want.extend_from_slice(b"xy");

    let check = |what: &str| {
        let snap = db.snapshot().unwrap();
        let got = db.get(&snap, t.id, a, b"r", b"q").unwrap().unwrap();
        assert_eq!(got.value(), ValueRef::Bytes(&want), "{what}: get");
        let got = db.get_latest(t.id, a, b"r", b"q").unwrap().unwrap();
        assert_eq!(got.value(), ValueRef::Bytes(&want), "{what}: get_latest");
        let mut spec = ReadSpec::default();
        spec.families = vec![a];
        let row = db.read_row(&snap, t.id, b"r", &spec).unwrap().unwrap();
        assert_eq!(
            row.cells[0].data.value(),
            ValueRef::Bytes(&want),
            "{what}: read_row"
        );
        let mut scan = ScanSpec::new(Bound::Unbounded, Bound::Unbounded);
        scan.read.families = vec![a];
        let mut cursor = db.scan(&snap, t.id, scan).unwrap();
        assert!(cursor.next_row().unwrap());
        let cell = cursor.next_cell().unwrap().unwrap();
        assert_eq!(&cell.stored[1..], &want[..], "{what}: scan");
        // The built-in i64 add rejects a bytes base whether it is separated or not.
        assert!(
            matches!(
                db.get(&snap, t.id, c, b"r", b"n"),
                Err(pigeonhole_engine::Error::Merge(_))
            ),
            "{what}: i64 add over a bytes base"
        );
    };
    check("operand in a memtable");
    db.flush().unwrap();
    check("operand in an SST");
    db.compact(None).unwrap();
    check("after a compaction");

    let cas = |expected: &[u8]| {
        let mut wb = WriteBatch::new();
        wb.put(t.id, a, b"r", b"other", None, ValueRef::Bytes(b"x"))
            .unwrap();
        let predicate = Predicate::Value {
            family: a,
            qualifier: b"q".to_vec(),
            predicate: ValuePredicate::Equals(expected.to_vec()),
        };
        db.check_and_mutate(t.id, b"r", &predicate, wb, None)
            .unwrap()
            .0
    };
    assert!(!cas(&base), "the folded value is not the base");
    assert!(cas(&want), "check_and_mutate compares the folded value");
    db.close().unwrap();
}

#[test]
fn a_fifo_drop_releases_the_blob_bytes_of_the_ssts_it_drops() {
    // A FIFO-by-time `Drop` removes whole expired SSTs without a job (#32), so the engine
    // reads their blob pointers itself: the blob files they alone referenced go with them.
    let vfs = SimVfs::new(41);
    let mut rig = Rig::open(&vfs, false);
    let fifo = FamilyOptions {
        compaction: pigeonhole_engine::CompactionStyle::FifoByTime,
        ttl_micros: 1_000_000,
        ..family()
    };
    let t = rig.db.create_table("t", &[("f".into(), fifo)]).unwrap();
    write(&mut rig, &t, 0..30, 0);
    rig.flush();
    let first: Vec<u32> = rig.db.blob_files().iter().map(|f| f.1).collect();
    assert!(!first.is_empty(), "the flush separated the values");
    rig.check();
    // Past the TTL, the next flush's maintenance drops the expired SST.
    vfs.advance(2_000_000_000);
    write(&mut rig, &t, 100..101, 1); // row 100 is small (inline)
    rig.flush();
    rig.check();
    assert!(
        rig.db.blob_files().is_empty(),
        "the dropped SST's blob file is dropped: {:?}",
        rig.db.blob_files()
    );
    let snap = rig.db.snapshot().unwrap();
    let f = t.families[0].id;
    assert!(rig.db.get(&snap, t.id, f, &row(3), b"q").unwrap().is_none());
    assert_eq!(
        rig.db
            .get(&snap, t.id, f, &row(100), b"q")
            .unwrap()
            .unwrap()
            .value(),
        ValueRef::Bytes(&value(100, 1))
    );
    drop(snap);
    rig.db.shrink().unwrap();
    assert_eq!(rig.db.unreferenced_bytes(), 0);
    rig.close();
}

#[test]
fn backup_copies_the_values_it_references() {
    // Issue #58: the copy gets its own blob files, holding the separated values the
    // snapshot references (from SSTs) and the large values still in the active memtable at
    // backup time. Those go through the backup's temporary SSTs inline (#268's first phase)
    // and are separated only by its merge.
    let vfs = SimVfs::new(39);
    let mut rig = Rig::open(&vfs, false);
    let one_version = FamilyOptions {
        max_versions: 1,
        ..family()
    };
    let t = rig
        .db
        .create_table("t", &[("f".into(), one_version)])
        .unwrap();
    write(&mut rig, &t, 0..60, 0);
    rig.flush();
    rig.compact();
    write(&mut rig, &t, 0..20, 1);
    rig.flush();
    write(&mut rig, &t, 50..60, 2); // large values, left in the active memtable
    assert!(!rig.db.blob_files().is_empty());
    let source_files = rig.db.blob_files().len();
    let expected = |i: u32| match i {
        0..20 => value(i, 1),
        50..60 => value(i, 2),
        _ => value(i, 0),
    };
    rig.db.backup(Path::new("/db/copy.phdb")).unwrap();
    rig.close();

    let copy = Rig::open_at(&vfs, "/db/copy.phdb", false);
    let t = copy.db.table("t").unwrap();
    copy.check();
    assert_eq!(
        copy.db.unreferenced_bytes(),
        0,
        "the copy holds nothing unreferenced"
    );
    let files = copy.db.blob_files();
    assert!(!files.is_empty(), "the copy has blob files");
    assert!(files.len() <= source_files + 1, "{files:?}");
    assert!(
        files.iter().all(|f| f.2 == f.3),
        "only referenced values: {files:?}"
    );
    assert_reads(&copy.db, &t, 60, expected);
    copy.close();
    let mut copy = Rig::open_at(&vfs, "/db/copy.phdb", false);
    let t = copy.db.table("t").unwrap();
    // The copy keeps working: new separated values get fresh blob ids.
    write(&mut copy, &t, 20..30, 3);
    copy.flush();
    copy.compact();
    copy.check();
    assert_reads(&copy.db, &t, 60, |i| match i {
        20..30 => value(i, 3),
        _ => expected(i),
    });
    copy.close();
}

#[test]
fn shrink_moves_blob_extents_down() {
    // #231: blob extents past the shrink point move into free space below them like SST
    // extents: the file shrinks, reads (and a snapshot taken before) are unchanged. About
    // 2 MiB of 8 KiB values per table, so `junk`'s blob extents (written first) lie below
    // `t`'s and dropping `junk` frees room for them.
    let big = |i: u32, generation: u8| {
        let mut v = vec![b'a' + generation; 8 << 10];
        v[..4].copy_from_slice(&i.to_le_bytes());
        v
    };
    let write_big = |rig: &mut Rig, t: &TableInfo, generation: u8| {
        for i in 0..250u32 {
            let mut wb = WriteBatch::new();
            wb.put(
                t.id,
                t.families[0].id,
                &row(i),
                b"q",
                None,
                ValueRef::Bytes(&big(i, generation)),
            )
            .unwrap();
            rig.commit(wb);
        }
        rig.flush();
    };
    let vfs = SimVfs::new(42);
    let mut rig = Rig::open(&vfs, false);
    let junk = rig
        .db
        .create_table("junk", &[("f".into(), family())])
        .unwrap();
    write_big(&mut rig, &junk, 0);
    let t = rig.db.create_table("t", &[("f".into(), family())]).unwrap();
    write_big(&mut rig, &t, 1);
    let t_files: Vec<u32> = rig
        .db
        .blob_files()
        .iter()
        .filter(|f| f.0 == t.families[0].id)
        .map(|f| f.1)
        .collect();
    assert!(!t_files.is_empty());
    rig.db.drop_table(junk.id).unwrap();
    rig.idle();
    let before = rig.db.snapshot().unwrap();
    let len = |vfs: &SimVfs| {
        pigeonhole_io::Vfs::open(vfs, Path::new(DB), pigeonhole_io::OpenOptions::read())
            .unwrap()
            .len()
            .unwrap()
    };
    let start = len(&vfs);
    let released = rig.db.shrink().unwrap();
    rig.check();
    assert_eq!(start - len(&vfs), released);
    assert_eq!(
        rig.db.blob_files().iter().map(|f| f.1).collect::<Vec<_>>(),
        t_files,
        "the same blob files, moved"
    );
    let f = t.families[0].id;
    let snap = rig.db.snapshot().unwrap();
    for i in [0u32, 1, 99, 249] {
        for (s, what) in [(&before, "before"), (&snap, "after")] {
            let v = rig.db.get(s, t.id, f, &row(i), b"q").unwrap().unwrap();
            assert_eq!(
                v.value(),
                ValueRef::Bytes(&big(i, 1)),
                "row {i}, snapshot {what}"
            );
        }
    }
    // The snapshot taken before pinned the old extents; released, they are reclaimed and
    // the tail is cut. `junk` held as much as `t`: with `t`'s blob extents left at the tail
    // the file could not end below them.
    drop((before, snap));
    rig.db.shrink().unwrap();
    assert_eq!(rig.db.unreferenced_bytes(), 0);
    assert!(
        len(&vfs) * 4 < start * 3,
        "the file only shrank from {start} to {}",
        len(&vfs)
    );
    rig.close();
    let rig = Rig::open(&vfs, false);
    rig.check();
    let t = rig.db.table("t").unwrap();
    let snap = rig.db.snapshot().unwrap();
    let v = rig.db.get(&snap, t.id, f, &row(7), b"q").unwrap().unwrap();
    assert_eq!(v.value(), ValueRef::Bytes(&big(7, 1)));
    drop(snap);
    rig.close();
}
