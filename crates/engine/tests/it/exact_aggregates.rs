//! D298 (#488 PR 2): exact `f64` sum and mean-variance families. Inputs are finite `f64`
//! operands, folded by the kind's operator into canonical states
//! (`pigeonhole_format::exact`) through flushes, checkpoints (D291) and compactions, so a
//! bucket reads the same bytes however its inputs were grouped; a put sets a bucket (an
//! `f64`, or a well-formed state as `phdb load` writes one back). One engine-owned shard
//! over `SimVfs`.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyId, FamilyOptions, TableId, ValueRef, WriteBatch,
};
use pigeonhole_format::exact;
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/exact_aggregates.phdb";

fn options(vfs: &Arc<SimVfs>, steps: u64) -> EngineOptions {
    let mut o: EngineOptions = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.pin_threads = false;
    o.counter_checkpoint_steps = steps;
    o
}

fn family(kind: FamilyKind) -> FamilyOptions {
    let op = match kind {
        FamilyKind::SumF64 => "pigeonhole.f64_sum",
        _ => "pigeonhole.f64_mean_var",
    };
    FamilyOptions::default().merge_operator(op).kind(kind)
}

struct Rig {
    db: Arc<Engine>,
    t: TableId,
    sum: FamilyId,
    mv: FamilyId,
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>, steps: u64) -> Self {
        let db = Engine::open(Path::new(DB), options(vfs, steps)).unwrap();
        let info = db
            .create_table(
                "t",
                &[
                    ("sum".into(), family(FamilyKind::SumF64)),
                    ("mv".into(), family(FamilyKind::MeanVar)),
                ],
            )
            .unwrap();
        Self {
            t: info.id,
            sum: info.family("sum").unwrap().id,
            mv: info.family("mv").unwrap().id,
            db,
        }
    }

    fn commit(&self, f: impl FnOnce(&mut WriteBatch)) -> Result<(), Error> {
        let mut wb = WriteBatch::new();
        f(&mut wb);
        self.db.commit(wb, None).map(|_| ())
    }

    fn input(&self, row: &[u8], x: f64) {
        self.commit(|wb| {
            for f in [self.sum, self.mv] {
                wb.merge(self.t, f, row, b"q", ValueRef::F64(x)).unwrap();
            }
        })
        .unwrap();
    }

    /// A family's cell, as its stored state.
    fn state(&self, f: FamilyId, row: &[u8]) -> Option<Vec<u8>> {
        let cell = self.db.get_latest(self.t, f, row, b"q").unwrap()?;
        match cell.value() {
            ValueRef::Bytes(b) => Some(b.to_vec()),
            v => panic!("not a state: {v:?}"),
        }
    }
}

fn sum_of(xs: &[f64]) -> Vec<u8> {
    let mut acc = Vec::new();
    for &x in xs {
        exact::sum_add(&mut acc, x).unwrap();
    }
    acc
}

fn mean_var_of(xs: &[f64]) -> Vec<u8> {
    let mut acc = Vec::new();
    for &x in xs {
        exact::mean_var_add(&mut acc, x).unwrap();
    }
    acc
}

#[test]
fn inputs_fold_into_the_same_state_through_flushes_checkpoints_and_compactions() {
    for steps in [128, 1] {
        let vfs = SimVfs::new(1);
        let r = Rig::open(&vfs, steps);
        assert_eq!(r.state(r.sum, b"r"), None, "no input: absent");
        let xs: Vec<f64> = (0..60)
            .map(|i| match i % 6 {
                0 => 1e300,
                1 => -1e300,
                2 => 5e-324,
                3 => -0.0,
                _ => f64::from(i) / 8.0 - 3.0,
            })
            .collect();
        for (i, &x) in xs.iter().enumerate() {
            r.input(b"r", x);
            match i {
                20 => r.db.flush().unwrap(),
                40 => {
                    r.db.flush().unwrap();
                    r.db.compact(None).unwrap();
                }
                _ => {}
            }
            let seen = &xs[..=i];
            assert_eq!(
                r.state(r.sum, b"r"),
                Some(sum_of(seen)),
                "steps {steps}, input {i}"
            );
            assert_eq!(
                r.state(r.mv, b"r"),
                Some(mean_var_of(seen)),
                "steps {steps}, input {i}"
            );
        }
        // The exact sum keeps the terms floating-point addition loses.
        let small: f64 = xs.iter().filter(|x| x.abs() < 1e200).sum();
        let read = exact::sum_read(&r.state(r.sum, b"r").unwrap()).unwrap();
        assert_eq!(read, small);
        r.db.close().unwrap();
    }
}

#[test]
fn a_put_sets_the_bucket_and_later_inputs_fold_onto_it() {
    let vfs = SimVfs::new(2);
    let r = Rig::open(&vfs, 128);
    // An `f64` put is the state of that one input; in one commit, an input folds onto it.
    r.commit(|wb| {
        for f in [r.sum, r.mv] {
            wb.put(r.t, f, b"a", b"q", None, ValueRef::F64(2.5))
                .unwrap();
            wb.merge(r.t, f, b"a", b"q", ValueRef::F64(-1.0)).unwrap();
        }
    })
    .unwrap();
    assert_eq!(r.state(r.sum, b"a"), Some(sum_of(&[2.5, -1.0])));
    assert_eq!(r.state(r.mv, b"a"), Some(mean_var_of(&[2.5, -1.0])));
    // A state put (as `phdb load` writes one back) is the base too, across a flush.
    let (s, mv) = (sum_of(&[1e10, 3.0]), mean_var_of(&[1e10, 3.0]));
    r.commit(|wb| {
        wb.put(r.t, r.sum, b"b", b"q", None, ValueRef::Bytes(&s))
            .unwrap();
        wb.put(r.t, r.mv, b"b", b"q", None, ValueRef::Bytes(&mv))
            .unwrap();
    })
    .unwrap();
    r.db.flush().unwrap();
    r.input(b"b", -1e10);
    assert_eq!(r.state(r.sum, b"b"), Some(sum_of(&[1e10, 3.0, -1e10])));
    let read = exact::mean_var_read(&r.state(r.mv, b"b").unwrap()).unwrap();
    assert_eq!((read.count, read.mean), (3, 1.0));
    r.db.close().unwrap();
}

#[test]
fn a_lone_f64_put_at_a_bucket_is_stored_as_its_state() {
    // An `f64` put alone in its commit, at an explicit timestamp, still becomes the state of
    // its one input before the WAL, as at the fixed timestamp: a read, a flush and a reopen
    // all see a state, never the raw `f64` (found with D298 PR 4's bloom inputs). The
    // histogram and quantile kinds: `bucket_aggregates.rs`.
    let vfs = SimVfs::new(5);
    let r = Rig::open(&vfs, 128);
    r.commit(|wb| {
        wb.put(r.t, r.sum, b"b", b"q", Some(3_600), ValueRef::F64(2.5))
            .unwrap();
    })
    .unwrap();
    r.commit(|wb| {
        wb.put(r.t, r.mv, b"b", b"q", Some(3_600), ValueRef::F64(2.5))
            .unwrap();
    })
    .unwrap();
    let at = |r: &Rig, f: FamilyId| {
        let snap = r.db.snapshot().unwrap();
        let c =
            r.db.get(&snap, r.t, f, b"b", b"q")
                .unwrap()
                .expect("the bucket");
        assert_eq!(c.timestamp(), 3_600);
        match c.value() {
            ValueRef::Bytes(b) => b.to_vec(),
            v => panic!("not a state: {v:?}"),
        }
    };
    assert_eq!(at(&r, r.sum), sum_of(&[2.5]));
    assert_eq!(at(&r, r.mv), mean_var_of(&[2.5]));
    r.db.flush().unwrap();
    assert_eq!(at(&r, r.sum), sum_of(&[2.5]));
    r.db.close().unwrap();
    let r = Rig {
        db: Engine::open(Path::new(DB), options(&vfs, 128)).unwrap(),
        ..r
    };
    assert_eq!(at(&r, r.sum), sum_of(&[2.5]));
    assert_eq!(at(&r, r.mv), mean_var_of(&[2.5]));
    r.db.close().unwrap();
}

#[test]
fn only_finite_f64s_and_well_formed_states_are_taken() {
    let vfs = SimVfs::new(3);
    let r = Rig::open(&vfs, 128);
    let refused = |v: ValueRef<'_>, put: bool| {
        for f in [r.sum, r.mv] {
            let got = r.commit(|wb| {
                if put {
                    wb.put(r.t, f, b"r", b"q", None, v).unwrap();
                } else {
                    wb.merge(r.t, f, b"r", b"q", v).unwrap();
                }
            });
            match got {
                Err(Error::InvalidArgument(why)) => assert!(why.contains("finite f64"), "{why}"),
                other => panic!("{v:?}: {other:?}"),
            }
        }
    };
    for put in [false, true] {
        refused(ValueRef::F64(f64::NAN), put);
        refused(ValueRef::F64(f64::INFINITY), put);
        refused(ValueRef::F64(f64::NEG_INFINITY), put);
        refused(ValueRef::I64(1), put);
    }
    // A state is a put, never an input; a malformed or another kind's state is refused.
    refused(ValueRef::Bytes(&sum_of(&[1.0])), false);
    refused(ValueRef::Bytes(&[2, 1, 0, 0, 0, 0, 0, 0, 0, 0]), true); // not canonical
    let mut wb = WriteBatch::new();
    wb.put(
        r.t,
        r.sum,
        b"r",
        b"q",
        None,
        ValueRef::Bytes(&mean_var_of(&[1.0])),
    )
    .unwrap();
    assert!(matches!(
        r.db.commit(wb, None),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(r.state(r.sum, b"r"), None, "nothing refused was written");
    r.db.close().unwrap();
}

#[test]
fn states_stay_inline_and_the_operators_belong_to_their_kinds() {
    let vfs = SimVfs::new(4);
    let r = Rig::open(&vfs, 128);
    // Commit-time separation (#230) leaves an aggregate family's values inline: a state is
    // folded by a built-in operator, which never reads a blob pointer.
    r.db.set_inline_value_limit(16);
    let big = mean_var_of(&[1e300, -1e300, 1e-300]);
    assert!(big.len() > 16);
    r.commit(|wb| {
        wb.put(r.t, r.mv, b"big", b"q", None, ValueRef::Bytes(&big))
            .unwrap();
    })
    .unwrap();
    r.input(b"big", 2.0);
    let want = mean_var_of(&[1e300, -1e300, 1e-300, 2.0]);
    assert_eq!(r.state(r.mv, b"big"), Some(want));
    // A blob threshold below the largest state is refused, as are the operators on a
    // standard family.
    for (kind, op, floor) in [
        (FamilyKind::SumF64, "pigeonhole.f64_sum", exact::SUM_MAX_LEN),
        (
            FamilyKind::MeanVar,
            "pigeonhole.f64_mean_var",
            exact::MEAN_VAR_MAX_LEN,
        ),
    ] {
        let low = family(kind).blob_threshold(floor as u32 - 1);
        match r.db.add_family(r.t, "low", low) {
            Err(Error::InvalidArgument(why)) => assert!(why.contains("inline"), "{why}"),
            other => panic!("{kind:?}: {other:?}"),
        }
        let standard = FamilyOptions::default().merge_operator(op);
        match r.db.add_family(r.t, "std", standard) {
            Err(Error::InvalidArgument(why)) => assert!(why.contains(op), "{why}"),
            other => panic!("{op}: {other:?}"),
        }
    }
    r.db.close().unwrap();
}
