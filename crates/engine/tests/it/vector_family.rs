//! D303 PR 1 (#490 B): the engine knows the vector family kind. Its parameters are checked
//! at creation (`InvalidArgument` for a malformed list, a tag the kind doesn't define, or
//! values out of range or mispaired), and a well-formed vector family is refused as
//! `Unsupported` until its write checks land (PR 2). A vector family is not an aggregate.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, Error, FamilyOptions};
use pigeonhole_format::kind_params;
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_format::vector::{Element, Metric, Params};
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
}
