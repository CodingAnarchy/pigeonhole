//! D294 (#487): min and max aggregate families, and the `incompat` feature bit they need.
//! Inputs combine per bucket like a counter's (D179), with the kind's operator; a put sets a
//! bucket; a delete starts it again. Creating the first such family sets bit 0 in both
//! superblock pages first; a refused creation sets nothing; a file whose family lacks its
//! bit is corrupt. One engine-owned shard over `SimVfs`.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyId, FamilyOptions, ReadSpec, TableId, ValueRef, WriteBatch,
};
use pigeonhole_format::manifest::{FamilyKind, INCOMPAT_AGGREGATES};
use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/aggregates.phdb";
const PAGE: u64 = 4096;

fn options(vfs: &Arc<SimVfs>, steps: u64) -> EngineOptions {
    let mut o: EngineOptions = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.pin_threads = false;
    o.counter_checkpoint_steps = steps;
    o
}

fn family(kind: FamilyKind) -> FamilyOptions {
    let op = match kind {
        FamilyKind::Min => "pigeonhole.i64_min",
        FamilyKind::Max => "pigeonhole.i64_max",
        _ => "pigeonhole.i64_add",
    };
    FamilyOptions::default().merge_operator(op).kind(kind)
}

/// Both superblock pages, as the file holds them.
fn pages(vfs: &SimVfs) -> [Superblock; 2] {
    let f = vfs.open(Path::new(DB), OpenOptions::read()).unwrap();
    [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B].map(|p| {
        let mut buf = [0u8; PAGE as usize];
        f.read_at(&mut buf, p * PAGE).unwrap();
        Superblock::decode(&buf).unwrap()
    })
}

/// The `incompat` sets of both pages.
fn incompat(vfs: &SimVfs) -> [u64; 2] {
    pages(vfs).map(|sb| sb.features.incompat)
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

struct Rig {
    db: Arc<Engine>,
    t: TableId,
    min: FamilyId,
    max: FamilyId,
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>, steps: u64) -> Self {
        let db = Engine::open(Path::new(DB), options(vfs, steps)).unwrap();
        let info = db
            .create_table(
                "t",
                &[
                    ("min".into(), family(FamilyKind::Min)),
                    ("max".into(), family(FamilyKind::Max)),
                ],
            )
            .unwrap();
        Self {
            t: info.id,
            min: info.family("min").unwrap().id,
            max: info.family("max").unwrap().id,
            db,
        }
    }

    fn commit(&self, f: impl FnOnce(&mut WriteBatch)) {
        let mut wb = WriteBatch::new();
        f(&mut wb);
        self.db.commit(wb, None).unwrap();
    }

    /// An input to both families' cell `q` of `row`.
    fn input(&self, row: &[u8], q: &[u8], v: i64) {
        self.commit(|wb| {
            for f in [self.min, self.max] {
                wb.merge(self.t, f, row, q, ValueRef::I64(v)).unwrap();
            }
        });
    }

    fn get(&self, f: FamilyId, row: &[u8], q: &[u8]) -> Option<i64> {
        let cell = self.db.get_latest(self.t, f, row, q).unwrap()?;
        match cell.value() {
            ValueRef::I64(v) => Some(v),
            v => panic!("not an i64: {v:?}"),
        }
    }

    /// Every version of `row` in family `f`, newest first, as `(timestamp, value)`.
    fn versions(&self, f: FamilyId, row: &[u8]) -> Vec<(u64, i64)> {
        let snap = self.db.snapshot().unwrap();
        let mut spec = ReadSpec::default();
        spec.families = vec![f];
        self.db
            .read_row(&snap, self.t, row, &spec)
            .unwrap()
            .map(|r| {
                r.cells
                    .iter()
                    .map(|c| match c.data.value() {
                        ValueRef::I64(v) => (c.data.timestamp(), v),
                        v => panic!("not an i64: {v:?}"),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `(min, max)` of cell `q` of `row`.
    fn both(&self, row: &[u8], q: &[u8]) -> (Option<i64>, Option<i64>) {
        (self.get(self.min, row, q), self.get(self.max, row, q))
    }
}

#[test]
fn min_and_max_fold_their_inputs_puts_set_and_deletes_start_again() {
    let vfs = SimVfs::new(1);
    let r = Rig::open(&vfs, 128);
    // No input: absent, never 0 (nor i64::MAX).
    assert_eq!(r.both(b"r", b"q"), (None, None));
    for v in [7, -3, 12, 0] {
        r.input(b"r", b"q", v);
    }
    assert_eq!(r.both(b"r", b"q"), (Some(-3), Some(12)));
    // A put sets the cell; later inputs combine with it.
    r.commit(|wb| {
        wb.put(r.t, r.min, b"r", b"q", None, ValueRef::I64(100))
            .unwrap();
        wb.put(r.t, r.max, b"r", b"q", None, ValueRef::I64(-100))
            .unwrap();
    });
    assert_eq!(r.both(b"r", b"q"), (Some(100), Some(-100)));
    r.input(b"r", b"q", 50);
    assert_eq!(r.both(b"r", b"q"), (Some(50), Some(50)));
    // Through a flush (operands over a base, combined by the flush's GC).
    r.db.flush().unwrap();
    assert_eq!(r.both(b"r", b"q"), (Some(50), Some(50)));
    r.input(b"r", b"q", 60);
    assert_eq!(r.both(b"r", b"q"), (Some(50), Some(60)));
    // A delete removes what earlier commits wrote: the next input starts the cell again.
    r.commit(|wb| {
        for f in [r.min, r.max] {
            wb.delete_column(r.t, f, b"r", b"q", None).unwrap();
        }
    });
    assert_eq!(r.both(b"r", b"q"), (None, None));
    r.input(b"r", b"q", 70);
    assert_eq!(r.both(b"r", b"q"), (Some(70), Some(70)));
    // Buckets: one version per timestamp, each combined on its own.
    r.commit(|wb| {
        for (ts, v) in [(8, 4), (8, 9), (16, -1)] {
            wb.merge_at(r.t, r.min, b"b", b"q", ts, ValueRef::I64(v))
                .unwrap();
        }
    });
    assert_eq!(r.versions(r.min, b"b"), [(16, -1), (8, 4)]);
    r.db.close().unwrap();
}

#[test]
fn writes_of_one_cell_in_one_commit_combine_in_order() {
    let vfs = SimVfs::new(2);
    let r = Rig::open(&vfs, 128);
    // put then input: the put's value combined with the input (D186's order).
    r.commit(|wb| {
        for f in [r.min, r.max] {
            wb.put(r.t, f, b"a", b"q", None, ValueRef::I64(5)).unwrap();
            wb.merge(r.t, f, b"a", b"q", ValueRef::I64(9)).unwrap();
        }
    });
    assert_eq!(r.both(b"a", b"q"), (Some(5), Some(9)));
    // input then put: the put wins.
    r.commit(|wb| {
        for f in [r.min, r.max] {
            wb.merge(r.t, f, b"b", b"q", ValueRef::I64(-4)).unwrap();
            wb.put(r.t, f, b"b", b"q", None, ValueRef::I64(1)).unwrap();
        }
    });
    assert_eq!(r.both(b"b", b"q"), (Some(1), Some(1)));
    // Inputs only.
    r.commit(|wb| {
        for v in [3, -8, 6] {
            for f in [r.min, r.max] {
                wb.merge(r.t, f, b"c", b"q", ValueRef::I64(v)).unwrap();
            }
        }
    });
    assert_eq!(r.both(b"c", b"q"), (Some(-8), Some(6)));
    r.db.close().unwrap();
}

#[test]
fn checkpoints_keep_min_and_max_reads_unchanged() {
    // D291 with every fold checkpointed: the result is a valid base for min and max too.
    let vfs = SimVfs::new(3);
    let r = Rig::open(&vfs, 1);
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    let mut x: i64 = 17;
    for i in 0..300 {
        x = (x * 31 + 7) % 1009 - 504;
        lo = lo.min(x);
        hi = hi.max(x);
        r.input(b"r", b"q", x);
        if i % 37 == 0 {
            assert_eq!(r.both(b"r", b"q"), (Some(lo), Some(hi)));
        }
        if i == 150 {
            r.db.flush().unwrap();
        }
    }
    assert_eq!(r.both(b"r", b"q"), (Some(lo), Some(hi)));
    r.db.flush().unwrap();
    assert_eq!(r.both(b"r", b"q"), (Some(lo), Some(hi)));
    r.db.close().unwrap();
}

#[test]
fn values_other_than_i64_are_refused() {
    let vfs = SimVfs::new(4);
    let r = Rig::open(&vfs, 128);
    let mut wb = WriteBatch::new();
    wb.put(r.t, r.min, b"r", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    match r.db.commit(wb, None) {
        Err(Error::InvalidArgument(why)) => assert!(why.contains("min family"), "{why}"),
        other => panic!("{other:?}"),
    }
    // A min family takes no other operator.
    let wrong = FamilyOptions::default()
        .merge_operator("pigeonhole.i64_add")
        .kind(FamilyKind::Min);
    match r.db.add_family(r.t, "bad", wrong) {
        Err(Error::InvalidArgument(why)) => assert!(why.contains("i64_min"), "{why}"),
        other => panic!("{other:?}"),
    }
    r.db.close().unwrap();
}

#[test]
fn the_min_and_max_operators_belong_to_their_kinds() {
    // perf287's S1 on #1194: a standard family naming pigeonhole.i64_min or i64_max would
    // fold across timestamps, with no feature bit, so 1.0.x would open the file and fail its
    // reads instead of refusing it. Refused at creation, as a table or an added family.
    let vfs = SimVfs::new(9);
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    for op in ["pigeonhole.i64_min", "pigeonhole.i64_max"] {
        let standard = FamilyOptions::default().merge_operator(op);
        match db.create_table("s", &[("f".into(), standard.clone())]) {
            Err(Error::InvalidArgument(why)) => {
                assert!(why.contains("create it with that kind"), "{why}")
            }
            other => panic!("{op}: {other:?}"),
        }
        let t = match db.table("plain") {
            Some(t) => t,
            None => db
                .create_table("plain", &[("f".into(), FamilyOptions::default())])
                .unwrap(),
        };
        match db.add_family(t.id, "g", standard) {
            Err(Error::InvalidArgument(why)) => assert!(why.contains(op), "{why}"),
            other => panic!("{op}: {other:?}"),
        }
    }
    // The add keeps its 0.1.0 meaning on a standard family (D41).
    db.create_table(
        "legacy",
        &[(
            "c".into(),
            FamilyOptions::default().merge_operator("pigeonhole.i64_add"),
        )],
    )
    .unwrap();
    assert_eq!(incompat(&vfs), [0, 0]);
    db.close().unwrap();
}

#[test]
fn the_first_min_or_max_family_flags_both_superblock_pages() {
    let vfs = SimVfs::new(5);
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    // Families 1.0 knows set nothing.
    let t = db
        .create_table(
            "plain",
            &[
                ("f".into(), FamilyOptions::default()),
                ("sum".into(), family(FamilyKind::Counter)),
            ],
        )
        .unwrap();
    assert_eq!(incompat(&vfs), [0, 0]);
    // The first max family, added to an existing table, sets bit 0 in both pages.
    db.add_family(t.id, "max", family(FamilyKind::Max)).unwrap();
    assert_eq!(incompat(&vfs), [INCOMPAT_AGGREGATES; 2]);
    // Both pages name the root that holds the family: it's durable, under the bit.
    let root_version = pages(&vfs).map(|sb| sb.manifest_version);
    assert!(root_version.iter().all(|v| *v > 0), "{root_version:?}");
    // Later ones change nothing, and dropping every such family keeps the bit (sticky).
    let m = db
        .create_table("m", &[("min".into(), family(FamilyKind::Min))])
        .unwrap();
    db.drop_table(m.id).unwrap();
    db.close().unwrap();
    assert_eq!(incompat(&vfs), [INCOMPAT_AGGREGATES; 2]);
    // This build opens its own flagged file.
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    assert!(db.table("plain").unwrap().family("max").is_some());
    db.close().unwrap();
}

#[test]
fn a_refused_creation_leaves_the_file_unflagged() {
    // perf287's S1 on D294: every check that refuses a creation runs before the feature
    // commits, so a refused call never flags a file that has no new family.
    let vfs = SimVfs::new(6);
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    db.create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    // The name exists: refused in the manifest writer's catalog closure.
    match db.create_table("t", &[("min".into(), family(FamilyKind::Min))]) {
        Err(Error::TableExists(_)) => {}
        other => panic!("{other:?}"),
    }
    // The family exists.
    let t = db.table("t").unwrap();
    assert!(db.add_family(t.id, "f", family(FamilyKind::Max)).is_err());
    // The wrong operator: refused before the request.
    let wrong = FamilyOptions::default().kind(FamilyKind::Max);
    assert!(db.add_family(t.id, "max", wrong).is_err());
    assert_eq!(incompat(&vfs), [0, 0]);
    db.close().unwrap();
    assert_eq!(incompat(&vfs), [0, 0]);
}

#[test]
fn a_min_family_without_its_bit_is_corruption() {
    let vfs = SimVfs::new(7);
    let r = Rig::open(&vfs, 128);
    r.input(b"r", b"q", 1);
    r.db.close().unwrap();
    // As only a writer bug (or a forged file) leaves it.
    edit_superblocks(&vfs, |sb| sb.features.incompat = 0);
    match Engine::open(Path::new(DB), options(&vfs, 128)) {
        Err(Error::Corruption(why)) => {
            assert!(why.contains("lacks its incompat feature bits"), "{why}")
        }
        Ok(_) => panic!("opened a min family without its bit"),
        Err(e) => panic!("{e:?}"),
    }
}

#[test]
fn an_unknown_incompat_bit_refuses_the_file_before_its_families_are_read() {
    // A later kind's bit, as a newer build would set it: refused at the superblock, so no
    // family is decoded (FORMAT §13: unsupported, never corrupt).
    let vfs = SimVfs::new(8);
    let r = Rig::open(&vfs, 128);
    r.db.close().unwrap();
    edit_superblocks(&vfs, |sb| sb.features.incompat |= 1 << 1);
    match Engine::open(Path::new(DB), options(&vfs, 128)) {
        Err(Error::UnsupportedFeature(what)) => {
            assert!(what.contains("incompatible feature bits 2"), "{what}")
        }
        Ok(_) => panic!("opened a file with an unknown incompat bit"),
        Err(e) => panic!("{e:?}"),
    }
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..20_000 {
        if cond() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn a_creation_queued_behind_a_background_commit_flags_the_file() {
    // perf287's S2 on #1194: a creation that finds the manifest writer's exclusion held by a
    // background commit queues behind it. Whoever finishes that commit drains the queue,
    // the pump on its shard thread or the waiting thread (`take_done`), and runs the
    // blocking feature commits inside `begin` (once in the file's life). Which one wins is a
    // race the engine leaves open by design; either way the feature commits complete, with
    // no root commit in flight, and both pages carry the bit before the family is visible.
    let vfs = SimVfs::new(10);
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    for round in 0..2u8 {
        let mut wb = WriteBatch::new();
        for i in 0..50u8 {
            wb.put(t.id, f, &[round, i], b"q", None, ValueRef::Bytes(b"v"))
                .unwrap();
        }
        db.commit(wb, None).unwrap();
        db.flush().unwrap();
    }
    // A compaction's commit holds the exclusion, parked before it publishes.
    db.park_manifest_commits(true);
    let compact = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.compact(None))
    };
    wait_for("the compaction's commit to park", || {
        db.manifest_commit_parked()
    });
    let create = {
        let (db, table) = (Arc::clone(&db), t.id);
        std::thread::spawn(move || db.add_family(table, "min", family(FamilyKind::Min)))
    };
    wait_for("the creation to queue behind the pump", || {
        db.manifest_queue_len() > 0
    });
    assert_eq!(incompat(&vfs), [0, 0], "flagged before its turn");
    db.park_manifest_commits(false);
    let info = create.join().unwrap().unwrap();
    compact.join().unwrap().unwrap();
    assert!(info.family("min").is_some());
    assert_eq!(incompat(&vfs), [INCOMPAT_AGGREGATES; 2]);
    // Writable and readable on, and after a reopen.
    let min = info.family("min").unwrap().id;
    let mut wb = WriteBatch::new();
    wb.merge(t.id, min, b"r", b"q", ValueRef::I64(4)).unwrap();
    db.commit(wb, None).unwrap();
    db.close().unwrap();
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    let cell = db.get_latest(t.id, min, b"r", b"q").unwrap().unwrap();
    assert_eq!(cell.value(), ValueRef::I64(4));
    db.close().unwrap();
}
