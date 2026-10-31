//! D298 (#488 PR 4): bloom filter families and bytes elements. A bloom family's operator is
//! built from its parameters (bits, hashes); an input is an `i64` or bytes element, written as
//! its one-element filter before the WAL, and folded by OR through flushes, checkpoints and
//! compactions; an HLL family takes bytes elements too, behind the same domain tag. A
//! filter's value may be separated into a blob file (PR 5): reads fold inputs onto the
//! separated base, compaction folds them into a new one, and a counter checkpoint keeps only
//! a folded value of at most 4 KiB. One engine-owned shard over `SimVfs`.

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
        // Dense by now, so past its blob threshold of 8 and separated since the first flush:
        // later reads fold onto the separated base, and compactions fold into a new one.
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

/// A dense filter of `p` with every other bit set.
fn dense_filter(p: BloomParams) -> Vec<u8> {
    let mut s = vec![bloom::ENCODING, p.log2_bits, p.hashes, 1];
    s.resize(4 + (1usize << p.log2_bits) / 8, 0x55);
    bloom::validate(&s, p).unwrap();
    s
}

fn live_blob_files(db: &Engine, f: FamilyId) -> Vec<(u32, u64)> {
    db.blob_files()
        .into_iter()
        .filter(|b| b.0 == f && b.3 > 0)
        .map(|b| (b.1, b.3))
        .collect()
}

#[test]
fn a_separated_base_folds_its_inputs_at_reads_and_compactions() {
    // D298 §4.1 and §4.2: a dense filter past the family's blob threshold is separated at
    // flush; inputs above it fold onto the value behind the pointer at read, and a
    // compaction folds them into a new separated base, leaving the old one unreferenced.
    for steps in [128, 1] {
        let vfs = SimVfs::new(4);
        let r = Rig::open(&vfs, steps);
        let mut want = Vec::new();
        for v in 0..100 {
            bloom::add(&mut want, P, hll::hash_i64(v)).unwrap();
        }
        r.commit(|wb| {
            wb.put(r.t, r.b, b"r", b"q", Some(3_600), ValueRef::Bytes(&want))
                .unwrap();
        })
        .unwrap();
        r.db.flush().unwrap();
        let base = live_blob_files(&r.db, r.b);
        assert_eq!(base.len(), 1, "steps {steps}: the base is separated");
        for v in 100..140i64 {
            r.commit(|wb| {
                wb.merge_at(r.t, r.b, b"r", b"q", 3_600, ValueRef::I64(v))
                    .unwrap();
            })
            .unwrap();
            bloom::add(&mut want, P, hll::hash_i64(v)).unwrap();
            let got = r.db.get_latest(r.t, r.b, b"r", b"q").unwrap().unwrap();
            assert_eq!(
                got.value(),
                ValueRef::Bytes(&want),
                "steps {steps}, input {v}"
            );
        }
        r.db.flush().unwrap();
        r.db.compact(None).unwrap();
        let after = live_blob_files(&r.db, r.b);
        assert_eq!(after.len(), 1, "steps {steps}: {after:?}");
        assert_ne!(
            after[0].0, base[0].0,
            "steps {steps}: folded into a new base"
        );
        r.db.check_blob_accounting().unwrap();
        let got = r.db.get_latest(r.t, r.b, b"r", b"q").unwrap().unwrap();
        assert_eq!(got.value(), ValueRef::Bytes(&want), "steps {steps}");
        assert_eq!(got.timestamp(), 3_600);
        r.db.close().unwrap();
        // And after a reopen.
        let db = Engine::open(Path::new(DB), options(&vfs, steps)).unwrap();
        let got = db.get_latest(r.t, r.b, b"r", b"q").unwrap().unwrap();
        assert_eq!(
            got.value(),
            ValueRef::Bytes(&want),
            "steps {steps}: reopened"
        );
        db.close().unwrap();
    }
}

#[test]
fn a_whole_filter_larger_than_a_wal_record_is_separated_at_commit() {
    // 2^21 bits: a 256 KiB dense filter, refused as too large while bloom values stayed
    // inline (PR 4), now goes to a blob file at commit (`phdb load`'s write back).
    let vfs = SimVfs::new(5);
    let db = Engine::open(Path::new(DB), options(&vfs, 128)).unwrap();
    let p = BloomParams {
        log2_bits: 21,
        hashes: 3,
    };
    let info = db
        .create_table("t", &[("b".into(), bloom_family(p))])
        .unwrap();
    let (t, b) = (info.id, info.family("b").unwrap().id);
    let mut want = dense_filter(p);
    let mut wb = WriteBatch::new();
    wb.put(t, b, b"r", b"q", None, ValueRef::Bytes(&want))
        .unwrap();
    db.commit(wb, None).unwrap();
    assert_eq!(live_blob_files(&db, b).len(), 1);
    let mut wb = WriteBatch::new();
    wb.merge(t, b, b"r", b"q", ValueRef::Bytes(b"carol"))
        .unwrap();
    db.commit(wb, None).unwrap();
    bloom::add(&mut want, p, hll::hash_bytes(b"carol")).unwrap();
    let got = db.get_latest(t, b, b"r", b"q").unwrap().unwrap();
    assert!(
        got.value() == ValueRef::Bytes(&want),
        "the input folds onto the blob"
    );
    db.flush().unwrap();
    db.compact(None).unwrap();
    let got = db.get_latest(t, b, b"r", b"q").unwrap().unwrap();
    assert!(got.value() == ValueRef::Bytes(&want), "and into a new base");
    db.check_blob_accounting().unwrap();
    db.close().unwrap();
}

#[test]
fn a_counter_checkpoint_keeps_only_a_folded_value_of_at_most_4_kib() {
    // D298 §4.3: with checkpoints after every step, a get of a filter of 2^14 bits (a 2 KiB
    // value) leaves a checkpoint; one of 2^15 bits (just over 4 KiB) never does.
    let vfs = SimVfs::new(6);
    let db = Engine::open(Path::new(DB), options(&vfs, 1)).unwrap();
    let (small, large) = (
        BloomParams {
            log2_bits: 14,
            hashes: 3,
        },
        BloomParams {
            log2_bits: 15,
            hashes: 3,
        },
    );
    let info = db
        .create_table(
            "t",
            &[
                ("s".into(), bloom_family(small).blob_threshold(1 << 20)),
                ("l".into(), bloom_family(large).blob_threshold(1 << 20)),
            ],
        )
        .unwrap();
    let t = info.id;
    for (name, p, kept) in [("s", small, true), ("l", large, false)] {
        let f = info.family(name).unwrap().id;
        let mut wb = WriteBatch::new();
        wb.put(
            t,
            f,
            b"r",
            b"q",
            Some(3_600),
            ValueRef::Bytes(&dense_filter(p)),
        )
        .unwrap();
        wb.merge_at(t, f, b"r", b"q", 3_600, ValueRef::I64(1))
            .unwrap();
        db.commit(wb, None).unwrap();
        let mut wb = WriteBatch::new();
        wb.merge_at(t, f, b"r", b"q", 3_600, ValueRef::I64(2))
            .unwrap();
        db.commit(wb, None).unwrap();
        let before = db.counter_checkpoints();
        db.get_latest(t, f, b"r", b"q").unwrap().unwrap();
        db.get_latest(t, f, b"r", b"q").unwrap().unwrap();
        let after = db.counter_checkpoints();
        if kept {
            assert!(after.0 > before.0, "{name}: {before:?} -> {after:?}");
        } else {
            assert_eq!(after, before, "{name}: none sent");
        }
    }
    db.close().unwrap();
}
