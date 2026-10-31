//! D303 PR 1 (#490 B): the engine knows the vector family kind. Its parameters are checked
//! at creation (`InvalidArgument` for a malformed list, a tag the kind doesn't define, or
//! values out of range or mispaired), and a well-formed vector family is refused as
//! `Unsupported` until its write checks land (PR 2), and so is a vector value put or merged
//! into any family (tag 0x04 only ever lands in a vector family). A vector family is not an
//! aggregate.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, Error, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::kind_params;
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_format::vector::{Element, Metric, Params, VectorRef};
use pigeonhole_io::sim::SimVfs;

fn vector(params: Vec<u8>) -> FamilyOptions {
    FamilyOptions::default()
        .kind(FamilyKind::Vector)
        .kind_params(params)
}

#[test]
fn vector_families_check_their_parameters_and_wait_for_pr_2() {
    let vfs = SimVfs::new(4931);
    let db = Engine::open(
        Path::new("/db/vector_family.phdb"),
        common::options(Arc::clone(&vfs), 1, 16 << 20),
    )
    .unwrap();
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
    let ok = Params::new(Element::F32, 768, Metric::Cosine).unwrap();
    assert!(matches!(
        db.create_table("t", &[("v".into(), vector(ok.encode()))]),
        Err(Error::Unsupported(_))
    ));
    assert!(
        db.table("t").is_none(),
        "nothing was created by a refused family"
    );
    assert!(!FamilyKind::Vector.is_aggregate() && FamilyKind::Vector.values_stay_inline());

    // A vector value into a standard family: refused at the batch, put and merge alike, so
    // tag 0x04 never reaches a file without the vector kind's feature bit.
    let t = db
        .create_table("s", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let payload = 1.0f32.to_le_bytes();
    let v = ValueRef::Vector(VectorRef::new(Element::F32, 1, &payload).unwrap());
    let mut wb = WriteBatch::new();
    assert!(matches!(
        wb.put(t.id, f, b"r", b"q", None, v),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        wb.merge(t.id, f, b"r", b"q", v),
        Err(Error::Unsupported(_))
    ));
    assert!(wb.is_empty(), "nothing was added to the batch");
}
