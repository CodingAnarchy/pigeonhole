//! D298 (#488 PR 5b): roaring set families. An input adds or removes one element, as an
//! operand `(A, R)`; operands combine in commit order and apply onto the base, through
//! flushes, checkpoints (D291) and compactions, so a cell reads as a `BTreeSet` replaying
//! the same writes. A large set separates into a blob file at flush, compaction or commit,
//! and reads and compactions fold new inputs onto it there (§4.1, §4.2). One engine-owned
//! shard over `SimVfs`.

use crate::common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyId, FamilyOptions, TableId, ValueRef, WriteBatch,
};
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_format::roaring::{self, SetRef};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/set_aggregates.phdb";

fn options(vfs: &Arc<SimVfs>, steps: u64) -> EngineOptions {
    let mut o: EngineOptions = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.pin_threads = false;
    o.counter_checkpoint_steps = steps;
    o
}

fn set_family() -> FamilyOptions {
    FamilyOptions::default()
        .merge_operator("pigeonhole.set")
        .kind(FamilyKind::Set)
}

fn operand(add: bool, x: u32) -> Vec<u8> {
    let mut v = Vec::new();
    if add {
        roaring::encode_operand(&mut v, &[x], &[]);
    } else {
        roaring::encode_operand(&mut v, &[], &[x]);
    }
    v
}

fn encode(s: &BTreeSet<u32>) -> Vec<u8> {
    let mut v = Vec::new();
    roaring::encode_sorted(&mut v, s.iter().copied());
    v
}

fn live_blob_files(db: &Engine, f: FamilyId) -> Vec<u32> {
    db.blob_files()
        .into_iter()
        .filter(|b| b.0 == f && b.3 > 0)
        .map(|b| b.1)
        .collect()
}

struct Rig {
    db: Arc<Engine>,
    t: TableId,
    s: FamilyId,
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>, steps: u64, blob_threshold: u32) -> Self {
        let db = Engine::open(Path::new(DB), options(vfs, steps)).unwrap();
        let info = db
            .create_table(
                "t",
                &[("s".into(), set_family().blob_threshold(blob_threshold))],
            )
            .unwrap();
        Self {
            t: info.id,
            s: info.family("s").unwrap().id,
            db,
        }
    }

    fn state(&self, row: &[u8]) -> Option<Vec<u8>> {
        let cell = self.db.get_latest(self.t, self.s, row, b"q").unwrap()?;
        match cell.value() {
            ValueRef::Bytes(b) => Some(b.to_vec()),
            v => panic!("not a set: {v:?}"),
        }
    }
}

#[test]
fn adds_and_removes_apply_in_commit_order_through_flushes_checkpoints_and_compactions() {
    for steps in [128, 1] {
        let vfs = SimVfs::new(1);
        let r = Rig::open(&vfs, steps, 64);
        let mut want = BTreeSet::new();
        let mut x = 7u64;
        for i in 0..400u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Adds and removes of a few elements across two containers, sometimes two to the
            // same element in one commit (they combine in order there too).
            let e = (x % 11) as u32 * 9000;
            let add = !x.is_multiple_of(3);
            let mut wb = WriteBatch::new();
            wb.merge(r.t, r.s, b"r", b"q", ValueRef::Bytes(&operand(add, e)))
                .unwrap();
            if i % 5 == 0 {
                wb.merge(r.t, r.s, b"r", b"q", ValueRef::Bytes(&operand(!add, e)))
                    .unwrap();
            }
            r.db.commit(wb, None).unwrap();
            let apply = |want: &mut BTreeSet<u32>, add| {
                if add {
                    want.insert(e);
                } else {
                    want.remove(&e);
                }
            };
            apply(&mut want, add);
            if i % 5 == 0 {
                apply(&mut want, !add);
            }
            match i {
                100 => r.db.flush().unwrap(),
                250 => {
                    r.db.flush().unwrap();
                    r.db.compact(None).unwrap();
                }
                _ => {}
            }
            assert_eq!(
                r.state(b"r"),
                Some(encode(&want)),
                "steps {steps}, input {i}"
            );
        }
        r.db.flush().unwrap();
        r.db.compact(None).unwrap();
        assert_eq!(r.state(b"r"), Some(encode(&want)), "steps {steps}");
        r.db.close().unwrap();
    }
}

#[test]
fn a_large_set_separates_and_folds_its_inputs_where_it_is() {
    // A 2^20-element set (every even number below 2^21: 32 bitmap containers, 256 KiB) put
    // whole, past a WAL record, goes to a blob file at commit; inputs fold onto it at read; a
    // compaction folds them into a new separated base; and a reopen reads the same.
    let vfs = SimVfs::new(2);
    let r = Rig::open(&vfs, 128, 4096);
    let mut want: BTreeSet<u32> = (0..1 << 20).map(|i| i * 2).collect();
    let mut wb = WriteBatch::new();
    wb.put(
        r.t,
        r.s,
        b"hub",
        b"q",
        None,
        ValueRef::Bytes(&encode(&want)),
    )
    .unwrap();
    r.db.commit(wb, None).unwrap();
    let base = live_blob_files(&r.db, r.s);
    assert_eq!(base.len(), 1, "separated at commit");
    for (add, e) in [(true, 1), (false, 2), (true, 3_000_001), (false, 4)] {
        let mut wb = WriteBatch::new();
        wb.merge(r.t, r.s, b"hub", b"q", ValueRef::Bytes(&operand(add, e)))
            .unwrap();
        r.db.commit(wb, None).unwrap();
        if add {
            want.insert(e);
        } else {
            want.remove(&e);
        }
    }
    let got = r.state(b"hub").unwrap();
    assert!(
        got == encode(&want),
        "the inputs fold onto the separated base"
    );
    let s = SetRef::parse(&got).unwrap();
    assert_eq!(s.len(), want.len() as u64);
    assert!(s.contains(1) && !s.contains(2) && s.contains(3_000_001));
    r.db.flush().unwrap();
    r.db.compact(None).unwrap();
    let after = live_blob_files(&r.db, r.s);
    assert_eq!(after.len(), 1, "{after:?}");
    assert_ne!(after, base, "folded into a new base");
    r.db.check_blob_accounting().unwrap();
    assert!(r.state(b"hub").unwrap() == encode(&want));
    r.db.close().unwrap();
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    let cell = db.get_latest(r.t, r.s, b"hub", b"q").unwrap().unwrap();
    assert!(cell.value() == ValueRef::Bytes(&encode(&want)), "reopened");
    db.close().unwrap();
}

#[test]
fn only_operands_and_canonical_sets_are_taken() {
    let vfs = SimVfs::new(3);
    let r = Rig::open(&vfs, 128, 4096);
    let refused = |kind_put: bool, v: ValueRef<'_>| {
        let mut wb = WriteBatch::new();
        if kind_put {
            wb.put(r.t, r.s, b"r", b"q", None, v).unwrap();
        } else {
            wb.merge(r.t, r.s, b"r", b"q", v).unwrap();
        }
        match r.db.commit(wb, None) {
            Err(Error::InvalidArgument(why)) => assert!(why.contains("set family"), "{why}"),
            other => panic!("{v:?}: {other:?}"),
        }
    };
    // Operands: an i64, a set (not an operand), an element added and removed at once.
    refused(false, ValueRef::I64(3));
    refused(false, ValueRef::Bytes(&encode(&[1].into())));
    let mut both = Vec::new();
    roaring::encode_operand(&mut both, &[4], &[4]);
    refused(false, ValueRef::Bytes(&both));
    // Puts: an operand, a set that is not canonical (a trailing byte), an i64.
    refused(true, ValueRef::Bytes(&operand(true, 1)));
    let mut bad = encode(&[1, 2].into());
    bad.push(0);
    refused(true, ValueRef::Bytes(&bad));
    refused(true, ValueRef::I64(1));
    // A put of a set, then a remove onto it.
    let mut wb = WriteBatch::new();
    wb.put(
        r.t,
        r.s,
        b"r",
        b"q",
        None,
        ValueRef::Bytes(&encode(&[1, 2].into())),
    )
    .unwrap();
    wb.merge(r.t, r.s, b"r", b"q", ValueRef::Bytes(&operand(false, 1)))
        .unwrap();
    r.db.commit(wb, None).unwrap();
    assert_eq!(r.state(b"r"), Some(encode(&[2].into())));
    // The operator belongs to the kind.
    match r.db.create_table(
        "x",
        &[(
            "f".into(),
            FamilyOptions::default().merge_operator("pigeonhole.set"),
        )],
    ) {
        Err(Error::InvalidArgument(why)) => assert!(why.contains("pigeonhole.set"), "{why}"),
        other => panic!("{other:?}"),
    }
    r.db.close().unwrap();
}

#[test]
fn combined_inputs_are_settled_before_the_wal_and_a_large_operand_is_refused() {
    // A commit's several inputs to one cell combine into one operand before the WAL (settled,
    // never the operator's working form): a reopen replays it and reads the same. Operands
    // are never separated, so one past a WAL record is refused.
    let vfs = SimVfs::new(4);
    let r = Rig::open(&vfs, 128, 4096);
    let mut wb = WriteBatch::new();
    for (add, x) in [(true, 1), (true, 70_000), (false, 1), (true, 5)] {
        wb.merge(r.t, r.s, b"c", b"q", ValueRef::Bytes(&operand(add, x)))
            .unwrap();
    }
    r.db.commit(wb, None).unwrap();
    let want = encode(&[5, 70_000].into());
    assert_eq!(r.state(b"c"), Some(want.clone()));
    r.db.close().unwrap();
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    let cell = db.get_latest(r.t, r.s, b"c", b"q").unwrap().unwrap();
    assert!(cell.value() == ValueRef::Bytes(&want), "replayed");
    let adds: Vec<u32> = (0..300_000).map(|i| i * 7).collect();
    let mut big = Vec::new();
    roaring::encode_operand(&mut big, &adds, &[]);
    let mut wb = WriteBatch::new();
    wb.merge(r.t, r.s, b"c", b"q", ValueRef::Bytes(&big))
        .unwrap();
    assert!(matches!(db.commit(wb, None), Err(Error::ValueTooLarge)));
    db.close().unwrap();
}

#[test]
fn a_set_keeps_checkpoints_up_to_256_kib() {
    // The owner's stopgap (D298 PR 5b): a set is exempt from the 4 KiB checkpoint cap up to
    // 256 KiB. With checkpoints after every step, a get of an 8 KB set leaves one; a get of a
    // 400 KB set never does.
    let vfs = SimVfs::new(5);
    let db = Engine::open(Path::new(DB), options(&vfs, 1)).unwrap();
    let info = db
        .create_table("t", &[("s".into(), set_family().blob_threshold(1 << 30))])
        .unwrap();
    let (t, s) = (info.id, info.family("s").unwrap().id);
    for (row, n, kept) in [
        (&b"small"[..], 4_000u32, true),
        (&b"large"[..], 200_000, false),
    ] {
        // Scattered, so every container is an array: about 2 bytes per element.
        let set: BTreeSet<u32> = (0..n).map(|i| i.wrapping_mul(2_654_435_761)).collect();
        let mut wb = WriteBatch::new();
        wb.put(t, s, row, b"q", None, ValueRef::Bytes(&encode(&set)))
            .unwrap();
        wb.merge(t, s, row, b"q", ValueRef::Bytes(&operand(true, 7)))
            .unwrap();
        db.commit(wb, None).unwrap();
        let mut wb = WriteBatch::new();
        wb.merge(t, s, row, b"q", ValueRef::Bytes(&operand(false, 7)))
            .unwrap();
        db.commit(wb, None).unwrap();
        let before = db.counter_checkpoints();
        db.get_latest(t, s, row, b"q").unwrap().unwrap();
        db.get_latest(t, s, row, b"q").unwrap().unwrap();
        // The shard keeps a checkpoint the get sent a moment later, on its own thread.
        let after = settled_checkpoints(&db, before, kept);
        if kept {
            assert!(after.0 > before.0, "{n}: {before:?} -> {after:?}");
        } else {
            assert_eq!(after, before, "{n}: none sent");
        }
    }
    db.close().unwrap();
}

/// The checkpoint counts once the shard has kept one more than `before` (when `kept`), or
/// after a quiet second (when not): the get sends its checkpoint, and the shard thread keeps
/// it a moment later.
fn settled_checkpoints(db: &Engine, before: (u64, u64), kept: bool) -> (u64, u64) {
    for _ in 0..200 {
        let now = db.counter_checkpoints();
        if kept && now.0 > before.0 {
            return now;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    db.counter_checkpoints()
}
