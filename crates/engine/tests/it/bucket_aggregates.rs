//! D298 (#488 PR 3): histogram and quantile families, the first kinds with `kind_params`.
//! Each family's operator is built from its parameters (a histogram's bounds; a quantile
//! sketch's mantissa bits and bucket limit); inputs are `f64` operands folded into
//! canonical states through flushes, checkpoints and compactions; parameters are checked at
//! creation; and an aggregate family's values are never separated into blob files, however
//! small its `blob_threshold`. One engine-owned shard over `SimVfs`.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyId, FamilyOptions, TableId, ValueRef, WriteBatch,
};
use pigeonhole_format::buckets::{self, QuantileParams};
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/bucket_aggregates.phdb";
const BOUNDS: [f64; 4] = [-10.0, 0.0, 1.0, 100.0];
const Q: QuantileParams = QuantileParams {
    mantissa_bits: 4,
    max_buckets: 16,
};

fn options(vfs: &Arc<SimVfs>, steps: u64) -> EngineOptions {
    let mut o: EngineOptions = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.pin_threads = false;
    o.counter_checkpoint_steps = steps;
    o
}

fn histogram() -> FamilyOptions {
    FamilyOptions::default()
        .merge_operator("pigeonhole.f64_histogram")
        .kind(FamilyKind::Histogram)
        .kind_params(buckets::histogram_params(&BOUNDS))
}

fn quantiles(q: QuantileParams) -> FamilyOptions {
    FamilyOptions::default()
        .merge_operator("pigeonhole.f64_quantiles")
        .kind(FamilyKind::Quantiles)
        .kind_params(q.encode())
}

struct Rig {
    db: Arc<Engine>,
    t: TableId,
    h: FamilyId,
    q: FamilyId,
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>, steps: u64) -> Self {
        let db = Engine::open(Path::new(DB), options(vfs, steps)).unwrap();
        // A tiny blob threshold: these families' values must stay inline anyway.
        let info = db
            .create_table(
                "t",
                &[
                    ("h".into(), histogram().blob_threshold(8)),
                    ("q".into(), quantiles(Q).blob_threshold(8)),
                ],
            )
            .unwrap();
        Self {
            t: info.id,
            h: info.family("h").unwrap().id,
            q: info.family("q").unwrap().id,
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
            wb.merge(self.t, self.h, row, b"q", ValueRef::F64(x))
                .unwrap();
            if x.is_finite() {
                wb.merge(self.t, self.q, row, b"q", ValueRef::F64(x))
                    .unwrap();
            }
        })
        .unwrap();
    }

    fn state(&self, f: FamilyId, row: &[u8]) -> Option<Vec<u8>> {
        let cell = self.db.get_latest(self.t, f, row, b"q").unwrap()?;
        match cell.value() {
            ValueRef::Bytes(b) => Some(b.to_vec()),
            v => panic!("not a state: {v:?}"),
        }
    }
}

#[test]
fn inputs_fold_into_the_same_state_through_flushes_checkpoints_and_compactions() {
    for steps in [128, 1] {
        let vfs = SimVfs::new(1);
        let r = Rig::open(&vfs, steps);
        let (mut h, mut q) = (Vec::new(), Vec::new());
        for i in 0..80u32 {
            let x = match i % 7 {
                0 => f64::INFINITY,
                1 => -0.0,
                2 => 1e300,
                3 => -1e-300,
                _ => f64::from(i) * 3.5 - 100.0,
            };
            r.input(b"r", x);
            buckets::histogram_add(&mut h, &BOUNDS, x).unwrap();
            if x.is_finite() {
                buckets::quantiles_add(&mut q, Q, x).unwrap();
            }
            match i {
                25 => r.db.flush().unwrap(),
                55 => {
                    r.db.flush().unwrap();
                    r.db.compact(None).unwrap();
                }
                _ => {}
            }
            assert_eq!(
                r.state(r.h, b"r"),
                Some(h.clone()),
                "steps {steps}, input {i}"
            );
            // Absent until the first finite input.
            let want_q = (!q.is_empty()).then(|| q.clone());
            assert_eq!(r.state(r.q, b"r"), want_q, "steps {steps}, input {i}");
        }
        // Wide magnitudes with 16 buckets per store: the sketch collapsed, and stayed inline
        // (past its blob threshold of 8) through the flushes and compaction.
        assert!(q.len() > 8);
        r.db.flush().unwrap();
        r.db.compact(None).unwrap();
        assert_eq!(r.state(r.q, b"r"), Some(q));
        r.db.close().unwrap();
    }
}

#[test]
fn puts_take_an_f64_or_the_familys_own_state() {
    let vfs = SimVfs::new(2);
    let r = Rig::open(&vfs, 128);
    // An `f64` put is the state of that one input; an input folds onto it.
    r.commit(|wb| {
        wb.put(r.t, r.h, b"a", b"q", None, ValueRef::F64(0.5))
            .unwrap();
        wb.merge(r.t, r.h, b"a", b"q", ValueRef::F64(-50.0))
            .unwrap();
    })
    .unwrap();
    let mut want = Vec::new();
    for x in [0.5, -50.0] {
        buckets::histogram_add(&mut want, &BOUNDS, x).unwrap();
    }
    assert_eq!(r.state(r.h, b"a"), Some(want));
    // A state of the family's parameters is a put; one of other parameters is refused.
    let mut own = Vec::new();
    buckets::quantiles_add(&mut own, Q, 3.0).unwrap();
    r.commit(|wb| {
        wb.put(r.t, r.q, b"b", b"q", None, ValueRef::Bytes(&own))
            .unwrap();
    })
    .unwrap();
    assert_eq!(r.state(r.q, b"b"), Some(own));
    let mut other = Vec::new();
    let p5 = QuantileParams {
        mantissa_bits: 5,
        ..Q
    };
    buckets::quantiles_add(&mut other, p5, 3.0).unwrap();
    let mut wrong_buckets = Vec::new();
    buckets::histogram_add(&mut wrong_buckets, &[0.0], 3.0).unwrap();
    for (f, v) in [(r.q, &other), (r.h, &wrong_buckets)] {
        let got = r.commit(|wb| {
            wb.put(r.t, f, b"c", b"q", None, ValueRef::Bytes(v))
                .unwrap();
        });
        assert!(matches!(got, Err(Error::InvalidArgument(_))), "{got:?}");
    }
    // Refused inputs: NaN in both; infinities in the quantile family only.
    for (f, x) in [(r.h, f64::NAN), (r.q, f64::NAN), (r.q, f64::INFINITY)] {
        let got = r.commit(|wb| {
            wb.merge(r.t, f, b"c", b"q", ValueRef::F64(x)).unwrap();
        });
        match got {
            Err(Error::InvalidArgument(why)) => assert!(why.contains("f64 inputs"), "{why}"),
            other => panic!("{x}: {other:?}"),
        }
    }
    r.db.close().unwrap();
}

#[test]
fn parameters_are_checked_at_creation() {
    let vfs = SimVfs::new(3);
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    let refused = |o: FamilyOptions, what: &str| match db.create_table("x", &[("f".into(), o)]) {
        Err(Error::InvalidArgument(why)) => assert!(why.contains(what), "{why}"),
        other => panic!("{what}: {other:?}"),
    };
    refused(histogram().kind_params(Vec::new()), "bounds");
    refused(
        histogram().kind_params(buckets::histogram_params(&[1.0, 1.0])),
        "increasing",
    );
    refused(
        histogram().kind_params(buckets::histogram_params(&[f64::NAN])),
        "finite",
    );
    refused(
        quantiles(QuantileParams {
            mantissa_bits: 13,
            ..Q
        }),
        "mantissa bits",
    );
    refused(
        quantiles(QuantileParams {
            max_buckets: 8,
            ..Q
        }),
        "buckets",
    );
    let unknown = pigeonhole_format::kind_params::encode(&[(3, &[1])]);
    refused(quantiles(Q).kind_params(unknown), "parameter tag 3");
    // Defaults: a quantile family without parameters takes 7 bits and 2048 buckets.
    db.create_table("d", &[("q".into(), quantiles(Q).kind_params(Vec::new()))])
        .unwrap();
    // The operators belong to their kinds.
    for op in ["pigeonhole.f64_histogram", "pigeonhole.f64_quantiles"] {
        let standard = FamilyOptions::default().merge_operator(op);
        refused(standard, op);
    }
    db.close().unwrap();
}
