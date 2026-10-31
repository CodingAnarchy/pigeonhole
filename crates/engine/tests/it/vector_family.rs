//! D303 (#490 B): vector families. Their parameters are checked at creation
//! (`InvalidArgument` for a malformed list, a tag the kind doesn't define, values out of range
//! or mispaired, or any merge operator). The commit gate takes a vector put only into a vector
//! family, of its element type and dimension, with finite components and (under cosine) not
//! all zeros; a vector family takes no other put and no merge, and no other family takes a
//! vector. A vector family is not an aggregate: a put takes the commit timestamp and keeps
//! its versions, and two puts never combine.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, Error, FamilyOptions, ReadSpec, ValueRef, WriteBatch};
use pigeonhole_format::kind_params;
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_format::vector::{Element, Metric, Params, VectorRef};
use pigeonhole_io::sim::SimVfs;

fn vector(params: Vec<u8>) -> FamilyOptions {
    FamilyOptions::default()
        .kind(FamilyKind::Vector)
        .kind_params(params)
}

fn le(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

fn open(seed: u64) -> Arc<Engine> {
    let vfs = SimVfs::new(seed);
    Engine::open(
        Path::new("/db/vector_family.phdb"),
        common::options(Arc::clone(&vfs), 1, 16 << 20),
    )
    .unwrap()
}

#[test]
fn vector_families_check_their_parameters_and_take_no_operator() {
    let db = open(4931);
    let raw =
        |e: u8, d: u32, m: u8| kind_params::encode(&[(1, &[e]), (2, &d.to_le_bytes()), (3, &[m])]);
    for (bad, why) in [
        (Vec::new(), "needs its element"),
        (raw(1, 0, 1), "dimension"),
        (raw(1, 4097, 1), "dimension"),
        (raw(3, 12, 4), "multiple of 8"),
        (raw(3, 64, 1), "Hamming takes bit"),
        (raw(1, 64, 4), "Hamming takes bit"),
        (raw(5, 64, 1), "element"),
        (
            kind_params::encode(&[(1, &[1]), (2, &8u32.to_le_bytes()), (3, &[1]), (4, &[0])]),
            "no parameter tag 4",
        ),
    ] {
        match db.create_table("t", &[("v".into(), vector(bad.clone()))]) {
            Err(Error::InvalidArgument(msg)) => assert!(msg.contains(why), "{bad:?}: {msg}"),
            other => panic!("{bad:?}: {other:?}"),
        }
    }
    let ok = Params::new(Element::F32, 4, Metric::Cosine)
        .unwrap()
        .encode();
    for op in ["pigeonhole.i64_add", "app.concat"] {
        match db.create_table("t", &[("v".into(), vector(ok.clone()).merge_operator(op))]) {
            Err(Error::InvalidArgument(msg)) => assert!(msg.contains("no merge operator"), "{msg}"),
            other => panic!("{op}: {other:?}"),
        }
    }
    assert!(
        db.table("t").is_none(),
        "nothing was created by a refused family"
    );
    db.create_table("t", &[("v".into(), vector(ok))]).unwrap();
    assert!(!FamilyKind::Vector.is_aggregate() && FamilyKind::Vector.values_stay_inline());
}

#[test]
fn the_commit_gate_keeps_vectors_in_vector_families_of_their_spec() {
    let db = open(4932);
    let spec = |e, d, m| Params::new(e, d, m).unwrap().encode();
    let t = db
        .create_table(
            "t",
            &[
                ("cos".into(), vector(spec(Element::F32, 2, Metric::Cosine))),
                ("i8".into(), vector(spec(Element::I8, 2, Metric::L2))),
                ("std".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    let (cos, i8f, std) = (
        t.family("cos").unwrap().id,
        t.family("i8").unwrap().id,
        t.family("std").unwrap().id,
    );
    let put = |f, value: ValueRef<'_>| {
        let mut wb = WriteBatch::new();
        wb.put(t.id, f, b"r", b"q", None, value)?;
        db.commit(wb, None).map(|_| ())
    };
    let refused = |r: Result<(), Error>, why: &str| match r {
        Err(Error::InvalidArgument(msg)) => assert!(msg.contains(why), "{why}: {msg}"),
        other => panic!("{why}: {other:?}"),
    };
    let ok = le(&[0.6, 0.8]);
    put(
        cos,
        ValueRef::Vector(VectorRef::new(Element::F32, 2, &ok).unwrap()),
    )
    .unwrap();
    let three = le(&[1.0, 2.0, 3.0]);
    refused(
        put(
            cos,
            ValueRef::Vector(VectorRef::new(Element::F32, 3, &three).unwrap()),
        ),
        "takes no vector",
    );
    refused(
        put(
            cos,
            ValueRef::Vector(VectorRef::new(Element::I8, 2, &[1, 2]).unwrap()),
        ),
        "takes no vector",
    );
    let zero = le(&[0.0, -0.0]);
    refused(
        put(
            cos,
            ValueRef::Vector(VectorRef::new(Element::F32, 2, &zero).unwrap()),
        ),
        "all zeros",
    );
    let nan = le(&[f32::NAN, 1.0]);
    refused(
        put(
            cos,
            ValueRef::Vector(VectorRef::new(Element::F32, 2, &nan).unwrap()),
        ),
        "finite",
    );
    refused(put(cos, ValueRef::Bytes(b"bytes")), "takes vectors");
    refused(put(cos, ValueRef::I64(7)), "takes vectors");
    // An i8 L2 family takes the zero vector; no other family takes a vector.
    put(
        i8f,
        ValueRef::Vector(VectorRef::new(Element::I8, 2, &[0, 0]).unwrap()),
    )
    .unwrap();
    refused(
        put(
            std,
            ValueRef::Vector(VectorRef::new(Element::F32, 2, &ok).unwrap()),
        ),
        "not a vector family",
    );
    // Never a merge operand, in any family.
    let mut wb = WriteBatch::new();
    let v = ValueRef::Vector(VectorRef::new(Element::F32, 2, &ok).unwrap());
    assert!(matches!(
        wb.merge(t.id, cos, b"r", b"q", v),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        wb.merge(t.id, std, b"r", b"q", v),
        Err(Error::InvalidArgument(_))
    ));
    // A merge of anything into a vector family: it has no operator.
    let mut wb = WriteBatch::new();
    wb.merge(t.id, cos, b"r", b"q", ValueRef::I64(1)).unwrap();
    assert!(matches!(
        db.commit(wb, None),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn vector_cells_version_like_a_standard_familys() {
    let db = open(4933);
    let t = db
        .create_table(
            "t",
            &[(
                "v".into(),
                vector(Params::new(Element::F32, 2, Metric::L2).unwrap().encode()),
            )],
        )
        .unwrap();
    let f = t.family("v").unwrap().id;
    let mut ts = Vec::new();
    for v in [[1.0f32, 2.0], [3.0, 4.0]] {
        let bytes = le(&v);
        let mut wb = WriteBatch::new();
        wb.put(
            t.id,
            f,
            b"r",
            b"q",
            None,
            ValueRef::Vector(VectorRef::new(Element::F32, 2, &bytes).unwrap()),
        )
        .unwrap();
        db.commit(wb, None).unwrap();
        ts.push(
            db.get_latest(t.id, f, b"r", b"q")
                .unwrap()
                .unwrap()
                .timestamp(),
        );
    }
    assert!(
        ts[0] > 0 && ts[1] > ts[0],
        "commit timestamps, not a fixed one: {ts:?}"
    );
    let cell = db.get_latest(t.id, f, b"r", b"q").unwrap().unwrap();
    let ValueRef::Vector(v) = cell.value() else {
        panic!("a vector");
    };
    assert_eq!(v.as_bytes(), le(&[3.0, 4.0]), "the newest put, not a sum");
    let snap = db.snapshot().unwrap();
    let mut spec = ReadSpec::default();
    spec.versions = 0;
    let row = db.read_row(&snap, t.id, b"r", &spec).unwrap().unwrap();
    assert_eq!(row.cells.len(), 2, "both versions kept (max_versions 0)");
}
