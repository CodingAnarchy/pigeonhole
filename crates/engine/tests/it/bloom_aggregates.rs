//! D298 (#488 PR 4): bloom filter families and bytes elements. A bloom family's operator is
//! built from its parameters (bits, hashes); an input is an `i64` or bytes element, written as
//! its one-element filter before the WAL, and folded by OR through flushes, checkpoints and
//! compactions; an HLL family takes bytes elements too, behind the same domain tag. One
//! engine-owned shard over `SimVfs`.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyId, FamilyOptions, TableId, ValueRef, WriteBatch,
};
use pigeonhole_format::bloom::{self, BloomParams};
use pigeonhole_format::hll;
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/bloom_aggregates.phdb";
const P: BloomParams = BloomParams {
    log2_bits: 9,
    hashes: 3,
};

fn options(vfs: &Arc<SimVfs>, steps: u64) -> EngineOptions {
    let mut o: EngineOptions = common::options(Arc::clone(vfs), 1, 16 << 20);
    o.pin_threads = false;
    o.counter_checkpoint_steps = steps;
    o
}

fn bloom_family(p: BloomParams) -> FamilyOptions {
    FamilyOptions::default()
        .merge_operator("pigeonhole.bloom")
        .kind(FamilyKind::Bloom)
        .kind_params(p.encode())
}

struct Rig {
    db: Arc<Engine>,
    t: TableId,
    b: FamilyId,
    h: FamilyId,
}

impl Rig {
    fn open(vfs: &Arc<SimVfs>, steps: u64) -> Self {
        let db = Engine::open(Path::new(DB), options(vfs, steps)).unwrap();
        let hll = FamilyOptions::default()
            .merge_operator("pigeonhole.hll")
            .kind(FamilyKind::Hll);
        let info = db
            .create_table(
                "t",
                &[
                    ("b".into(), bloom_family(P).blob_threshold(8)),
                    ("h".into(), hll),
                ],
            )
            .unwrap();
        Self {
            t: info.id,
            b: info.family("b").unwrap().id,
            h: info.family("h").unwrap().id,
            db,
        }
    }

    fn commit(&self, f: impl FnOnce(&mut WriteBatch)) -> Result<(), Error> {
        let mut wb = WriteBatch::new();
        f(&mut wb);
        self.db.commit(wb, None).map(|_| ())
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
fn elements_fold_through_flushes_checkpoints_and_compactions() {
    for steps in [128, 1] {
        let vfs = SimVfs::new(1);
        let r = Rig::open(&vfs, steps);
        let (mut want, mut hashes) = (Vec::new(), Vec::new());
        for i in 0..120i64 {
            // Half `i64` elements, half bytes elements; some at a bucket timestamp.
            let key = format!("user-{i}");
            let h = if i % 2 == 0 {
                hll::hash_i64(i)
            } else {
                hll::hash_bytes(key.as_bytes())
            };
            r.commit(|wb| {
                let v = if i % 2 == 0 {
                    ValueRef::I64(i)
                } else {
                    ValueRef::Bytes(key.as_bytes())
                };
                wb.merge(r.t, r.b, b"r", b"q", v).unwrap();
            })
            .unwrap();
            bloom::add(&mut want, P, h).unwrap();
            hashes.push(h);
            match i {
                40 => r.db.flush().unwrap(),
                80 => {
                    r.db.flush().unwrap();
                    r.db.compact(None).unwrap();
                }
                _ => {}
            }
            assert_eq!(
                r.state(r.b, b"r"),
                Some(want.clone()),
                "steps {steps}, input {i}"
            );
        }
        // Dense by now (past its blob threshold of 8), and still inline after a compaction.
        assert_eq!(want[3], 1, "dense");
        r.db.flush().unwrap();
        r.db.compact(None).unwrap();
        let s = r.state(r.b, b"r").unwrap();
        assert!(
            hashes.iter().all(|&h| bloom::contains(&s, h).unwrap()),
            "no false negatives"
        );
        r.db.close().unwrap();
    }
}

#[test]
fn an_hll_family_takes_bytes_elements_behind_their_domain_tag() {
    let vfs = SimVfs::new(2);
    let r = Rig::open(&vfs, 128);
    r.commit(|wb| {
        wb.merge(r.t, r.h, b"r", b"q", ValueRef::Bytes(b"alice"))
            .unwrap();
        wb.merge(r.t, r.h, b"r", b"q", ValueRef::Bytes(b"bob"))
            .unwrap();
        wb.merge(r.t, r.h, b"r", b"q", ValueRef::Bytes(b"alice"))
            .unwrap();
        wb.merge(r.t, r.h, b"r", b"q", ValueRef::I64(7)).unwrap();
    })
    .unwrap();
    let mut want = Vec::new();
    for h in [
        hll::hash_bytes(b"alice"),
        hll::hash_bytes(b"bob"),
        hll::hash_i64(7),
    ] {
        let mut one = Vec::new();
        hll::append_hashed(&mut one, h);
        if want.is_empty() {
            want = one;
        } else {
            hll::merge(&mut want, &one).unwrap();
        }
    }
    assert_eq!(r.state(r.h, b"r"), Some(want.clone()));
    assert_eq!(hll::estimate(&want).unwrap(), 3);
    // The bytes of an i64's little-endian form are another element.
    let mut wb = WriteBatch::new();
    wb.merge(r.t, r.h, b"s", b"q", ValueRef::Bytes(&7i64.to_le_bytes()))
        .unwrap();
    wb.merge(r.t, r.h, b"s", b"q", ValueRef::I64(7)).unwrap();
    r.db.commit(wb, None).unwrap();
    assert_eq!(hll::estimate(&r.state(r.h, b"s").unwrap()).unwrap(), 2);
    r.db.close().unwrap();
}

#[test]
fn puts_take_the_familys_own_filter_and_parameters_are_checked() {
    let vfs = SimVfs::new(3);
    let r = Rig::open(&vfs, 128);
    let mut own = Vec::new();
    bloom::add(&mut own, P, hll::hash_i64(5)).unwrap();
    r.commit(|wb| {
        wb.put(r.t, r.b, b"p", b"q", Some(3_600), ValueRef::Bytes(&own))
            .unwrap();
    })
    .unwrap();
    let mut other = Vec::new();
    bloom::add(&mut other, BloomParams { hashes: 4, ..P }, hll::hash_i64(5)).unwrap();
    for bad in [&other[..], &[6, 9, 3, 0], &own[..own.len() - 1]] {
        let got = r.commit(|wb| {
            wb.put(r.t, r.b, b"p", b"q", None, ValueRef::Bytes(bad))
                .unwrap();
        });
        match got {
            Err(Error::InvalidArgument(why)) => assert!(why.contains("bloom family"), "{why}"),
            other => panic!("{bad:?}: {other:?}"),
        }
    }
    // An f64 is no element.
    let got = r.commit(|wb| {
        wb.merge(r.t, r.b, b"p", b"q", ValueRef::F64(1.0)).unwrap();
    });
    assert!(matches!(got, Err(Error::InvalidArgument(_))), "{got:?}");
    // Parameters at creation: both required; bits a power of two in range; hashes in range.
    let refused = |o: FamilyOptions| match r.db.create_table("x", &[("f".into(), o)]) {
        Err(Error::InvalidArgument(why)) => assert!(why.contains("bloom"), "{why}"),
        other => panic!("{other:?}"),
    };
    refused(bloom_family(P).kind_params(Vec::new()));
    refused(bloom_family(BloomParams { log2_bits: 5, ..P }));
    refused(bloom_family(BloomParams { log2_bits: 25, ..P }));
    refused(bloom_family(BloomParams { hashes: 0, ..P }));
    refused(bloom_family(BloomParams { hashes: 17, ..P }));
    match r.db.create_table(
        "s",
        &[(
            "f".into(),
            FamilyOptions::default().merge_operator("pigeonhole.bloom"),
        )],
    ) {
        Err(Error::InvalidArgument(why)) => assert!(why.contains("pigeonhole.bloom"), "{why}"),
        other => panic!("{other:?}"),
    }
    r.db.close().unwrap();
}
