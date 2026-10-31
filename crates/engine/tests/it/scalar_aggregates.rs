//! D298 (#488 PR 1): the bitwise (`u64` OR, AND, XOR over the `i64` tag's bits) and `f64`
//! minimum and maximum aggregate families, and a family's `kind_params`. Inputs combine per
//! bucket like a counter's (D179) with the kind's built-in operator, through flushes,
//! checkpoints (D291) and compactions; an `f64` family never holds a NaN and orders by
//! `total_cmp` (`-0.0 < +0.0`). The kinds share `incompat` bit 0 with D294's. One
//! engine-owned shard over `SimVfs`.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyId, FamilyOptions, TableId, ValueRef, WriteBatch,
};
use pigeonhole_format::manifest::{FamilyKind, INCOMPAT_AGGREGATES};
use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/scalar_aggregates.phdb";
const PAGE: u64 = 4096;

fn options(vfs: &Arc<SimVfs>, steps: u64) -> EngineOptions {
    let mut o: EngineOptions = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.pin_threads = false;
    o.counter_checkpoint_steps = steps;
    o
}

const KINDS: [(FamilyKind, &str); 5] = [
    (FamilyKind::BitOr, "pigeonhole.u64_or"),
    (FamilyKind::BitAnd, "pigeonhole.u64_and"),
    (FamilyKind::BitXor, "pigeonhole.u64_xor"),
    (FamilyKind::MinF64, "pigeonhole.f64_min"),
    (FamilyKind::MaxF64, "pigeonhole.f64_max"),
];

fn family(kind: FamilyKind) -> FamilyOptions {
    let op = KINDS.iter().find(|k| k.0 == kind).expect("a D298 kind").1;
    FamilyOptions::default().merge_operator(op).kind(kind)
}

fn incompat(vfs: &SimVfs) -> [u64; 2] {
    let f = vfs.open(Path::new(DB), OpenOptions::read()).unwrap();
    [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B].map(|p| {
        let mut buf = [0u8; PAGE as usize];
        f.read_at(&mut buf, p * PAGE).unwrap();
        Superblock::decode(&buf).unwrap().features.incompat
    })
}

struct Rig {
    db: Arc<Engine>,
    t: TableId,
    /// OR, AND, XOR, f64 min, f64 max, in [`KINDS`] order.
    f: [FamilyId; 5],
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>, steps: u64) -> Self {
        let db = Engine::open(Path::new(DB), options(vfs, steps)).unwrap();
        let families: Vec<_> = KINDS
            .iter()
            .enumerate()
            .map(|(i, k)| (format!("k{i}"), family(k.0)))
            .collect();
        let info = db.create_table("t", &families).unwrap();
        let f = std::array::from_fn(|i| info.family(&format!("k{i}")).unwrap().id);
        Self { t: info.id, f, db }
    }

    fn commit(&self, f: impl FnOnce(&mut WriteBatch)) {
        let mut wb = WriteBatch::new();
        f(&mut wb);
        self.db.commit(wb, None).unwrap();
    }

    /// The input `bits` to each bitwise family and `x` to both `f64` families.
    fn input(&self, row: &[u8], bits: u64, x: f64) {
        self.commit(|wb| {
            for (i, f) in self.f.into_iter().enumerate() {
                let v = if i < 3 {
                    ValueRef::I64(bits as i64)
                } else {
                    ValueRef::F64(x)
                };
                wb.merge(self.t, f, row, b"q", v).unwrap();
            }
        });
    }

    fn bits(&self, i: usize, row: &[u8]) -> Option<u64> {
        let cell = self.db.get_latest(self.t, self.f[i], row, b"q").unwrap()?;
        match cell.value() {
            ValueRef::I64(v) => Some(v as u64),
            v => panic!("not an i64: {v:?}"),
        }
    }

    fn float(&self, i: usize, row: &[u8]) -> Option<f64> {
        let cell = self.db.get_latest(self.t, self.f[i], row, b"q").unwrap()?;
        match cell.value() {
            ValueRef::F64(x) => Some(x),
            v => panic!("not an f64: {v:?}"),
        }
    }

    /// `[or, and, xor]` and the `f64` `[min, max]` bits of `row` (bits, so `-0.0` and `+0.0`
    /// differ).
    fn all(&self, row: &[u8]) -> ([Option<u64>; 3], [Option<u64>; 2]) {
        (
            std::array::from_fn(|i| self.bits(i, row)),
            std::array::from_fn(|i| self.float(3 + i, row).map(f64::to_bits)),
        )
    }
}

/// The expected `all` after `inputs` (none yet: every cell absent).
fn expected(inputs: &[(u64, f64)]) -> ([Option<u64>; 3], [Option<u64>; 2]) {
    let fold = |f: fn(u64, u64) -> u64| inputs.iter().map(|i| i.0).reduce(f);
    let pick = |keep: std::cmp::Ordering| {
        inputs
            .iter()
            .map(|i| i.1)
            .reduce(|a, b| if b.total_cmp(&a) == keep { b } else { a })
            .map(f64::to_bits)
    };
    (
        [fold(|a, b| a | b), fold(|a, b| a & b), fold(|a, b| a ^ b)],
        [
            pick(std::cmp::Ordering::Less),
            pick(std::cmp::Ordering::Greater),
        ],
    )
}

#[test]
fn inputs_fold_through_flushes_checkpoints_and_compactions() {
    for steps in [128, 1] {
        let vfs = SimVfs::new(1);
        let r = Rig::open(&vfs, steps);
        assert_eq!(r.all(b"r"), expected(&[]));
        // Signed zeros and infinities: total order, -0.0 below +0.0.
        let xs = [
            0.0,
            -0.0,
            3.5,
            f64::INFINITY,
            -2.25,
            f64::NEG_INFINITY,
            1e-300,
        ];
        let mut inputs = Vec::new();
        for (i, x) in xs.into_iter().cycle().take(60).enumerate() {
            let bits = (0x9e37_79b9_7f4a_7c15u64).rotate_left(i as u32 * 7) | (1 << (i % 64));
            inputs.push((bits, x));
            r.input(b"r", bits, x);
            match i {
                20 => r.db.flush().unwrap(),
                40 => {
                    r.db.flush().unwrap();
                    r.db.compact(None).unwrap();
                }
                _ => {}
            }
            assert_eq!(r.all(b"r"), expected(&inputs), "steps {steps}, input {i}");
        }
        r.db.flush().unwrap();
        r.db.compact(None).unwrap();
        assert_eq!(r.all(b"r"), expected(&inputs));
        // Only negative and positive zero: the minimum is -0.0, the maximum +0.0.
        r.input(b"z", 0, 0.0);
        r.input(b"z", 0, -0.0);
        assert_eq!(
            r.float(3, b"z").map(f64::to_bits),
            Some((-0.0f64).to_bits())
        );
        assert_eq!(r.float(4, b"z").map(f64::to_bits), Some(0.0f64.to_bits()));
        r.db.close().unwrap();
        // The kinds share D294's feature bit.
        assert_eq!(incompat(&vfs), [INCOMPAT_AGGREGATES; 2]);
    }
}

#[test]
fn a_put_is_the_base_later_inputs_fold_onto() {
    let vfs = SimVfs::new(2);
    let r = Rig::open(&vfs, 128);
    // In one commit: a put, then an input combined onto it (D186's order).
    r.commit(|wb| {
        for (i, f) in r.f.into_iter().enumerate() {
            let (put, input) = if i < 3 {
                (ValueRef::I64(0b1100), ValueRef::I64(0b1010))
            } else {
                (ValueRef::F64(1.5), ValueRef::F64(-7.0))
            };
            wb.put(r.t, f, b"a", b"q", None, put).unwrap();
            wb.merge(r.t, f, b"a", b"q", input).unwrap();
        }
    });
    let ab = |a: u64, b: u64| [Some(a | b), Some(a & b), Some(a ^ b)];
    let one = |min: f64, max: f64| [Some(min.to_bits()), Some(max.to_bits())];
    assert_eq!(r.all(b"a"), (ab(0b1100, 0b1010), one(-7.0, 1.5)));
    // Across commits and a flush: the put is the base, the later input folds onto it.
    r.commit(|wb| {
        for (i, f) in r.f.into_iter().enumerate() {
            let put = if i < 3 {
                ValueRef::I64(0b0110)
            } else {
                ValueRef::F64(2.0)
            };
            wb.put(r.t, f, b"b", b"q", None, put).unwrap();
        }
    });
    r.db.flush().unwrap();
    r.input(b"b", 0b0011, 9.0);
    assert_eq!(r.all(b"b"), (ab(0b0110, 0b0011), one(2.0, 9.0)));
    r.db.close().unwrap();
}

#[test]
fn f64_families_take_finite_or_infinite_f64s_only() {
    let vfs = SimVfs::new(3);
    let r = Rig::open(&vfs, 128);
    let refused = |v: ValueRef<'_>, put: bool| {
        for f in [r.f[3], r.f[4]] {
            let mut wb = WriteBatch::new();
            if put {
                wb.put(r.t, f, b"r", b"q", None, v).unwrap();
            } else {
                wb.merge(r.t, f, b"r", b"q", v).unwrap();
            }
            match r.db.commit(wb, None) {
                Err(Error::InvalidArgument(why)) => assert!(why.contains("never NaN"), "{why}"),
                other => panic!("{v:?}: {other:?}"),
            }
        }
    };
    for put in [false, true] {
        refused(ValueRef::F64(f64::NAN), put);
        refused(ValueRef::F64(-f64::NAN), put);
        refused(ValueRef::I64(1), put);
        refused(ValueRef::Bytes(b"12345678"), put);
    }
    // A bitwise family holds i64s (a u64's bits), not f64s.
    let mut wb = WriteBatch::new();
    wb.merge(r.t, r.f[0], b"r", b"q", ValueRef::F64(1.0))
        .unwrap();
    match r.db.commit(wb, None) {
        Err(Error::InvalidArgument(why)) => assert!(why.contains("aggregate_u64"), "{why}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(r.all(b"r"), expected(&[]));
    r.db.close().unwrap();
}

#[test]
fn the_operators_belong_to_their_kinds() {
    let vfs = SimVfs::new(4);
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    for (kind, op) in KINDS {
        // A standard family naming the operator would fold across timestamps with no
        // feature bit (perf287's S1 on #1194): refused.
        let standard = FamilyOptions::default().merge_operator(op);
        match db.create_table("s", &[("f".into(), standard)]) {
            Err(Error::InvalidArgument(why)) => assert!(why.contains(op), "{why}"),
            other => panic!("{op}: {other:?}"),
        }
        // The kind takes no other operator.
        let wrong = FamilyOptions::default()
            .merge_operator("pigeonhole.i64_add")
            .kind(kind);
        match db.create_table("w", &[("f".into(), wrong)]) {
            Err(Error::InvalidArgument(why)) => assert!(why.contains(op), "{why}"),
            other => panic!("{op}: {other:?}"),
        }
    }
    assert_eq!(incompat(&vfs), [0, 0]);
    db.close().unwrap();
}

#[test]
fn kind_params_name_only_the_kinds_tags() {
    // No PR 1 kind defines a tag: any must-understand or skippable tag is refused at
    // creation, so no file holds parameters an older 1.1 build would misread.
    let vfs = SimVfs::new(5);
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    for tag in [1u8, 127, 128, 255] {
        let params = pigeonhole_format::kind_params::encode(&[(tag, &[1])]);
        let o = family(FamilyKind::BitOr).kind_params(params);
        match db.create_table("p", &[("f".into(), o)]) {
            Err(Error::InvalidArgument(why)) => assert!(why.contains("parameter"), "{why}"),
            other => panic!("tag {tag}: {other:?}"),
        }
    }
    db.close().unwrap();
}
