//! D303 (#490 B, ICR 0098): vector families through the public API. Vectors written with
//! `put_vector_f32`, `put_vector_i8` and `put_vector_bits` (on a mutation, a write batch and a
//! transaction) read back as `Value::Vector`, through flushes and compactions and at a
//! blob threshold every vector is above (they stay inline). Each write is refused when it's
//! built if it doesn't fit the family's spec, and a vector family refuses plain puts, merges
//! and aggregate inputs. `VectorSpec::distance_*` scores as the kernels do.

use pigeonhole::{
    Element, ErrorCode, Family, Metric, Options, Pigeonhole, Table, Value, VectorSpec,
};

fn open() -> (pigeonhole::doc_support::TempDir, Pigeonhole, Table) {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("v.phdb"), Options::default().shards(2)).expect("open");
    let spec = |d, e, m| VectorSpec::new(d, e, m).expect("spec");
    let t = db
        .table("v")
        .expect("table")
        // A blob threshold below every vector: vectors are never separated (D303).
        .family(
            "f",
            Family::vector(spec(8, Element::F32, Metric::Cosine)).blob_threshold(16),
        )
        .family("q", Family::vector(spec(4, Element::I8, Metric::L2)))
        .family("b", Family::vector(spec(16, Element::Bit, Metric::Hamming)))
        .family("plain", Family::default())
        .create_if_missing()
        .expect("create");
    (dir, db, t)
}

fn f32s(i: u32) -> Vec<f32> {
    (0..8)
        .map(|j| ((i * 8 + j) % 13) as f32 - 6.0 + 0.5)
        .collect()
}

#[test]
fn vectors_round_trip_through_every_writer_and_storage() {
    let (_dir, db, t) = open();
    for i in 0..200u32 {
        let row = format!("r{i:04}");
        match i % 3 {
            0 => t
                .mutate(row.as_bytes())
                .put_vector_f32("f", b"", &f32s(i))
                .put_vector_i8("q", b"", &[i as i8, (i as i8).wrapping_neg(), 0, 1])
                .put_vector_bits("b", b"", &(i as u16).to_le_bytes())
                .commit()
                .map(drop)
                .expect("mutation"),
            1 => {
                let mut wb = db.write_batch();
                wb.put_vector_f32(&t, row.as_bytes(), "f", b"", &f32s(i))
                    .put_vector_i8(
                        &t,
                        row.as_bytes(),
                        "q",
                        b"",
                        &[i as i8, (i as i8).wrapping_neg(), 0, 1],
                    )
                    .put_vector_bits(&t, row.as_bytes(), "b", b"", &(i as u16).to_le_bytes());
                wb.commit().expect("batch");
            }
            _ => {
                let mut txn = db.transaction().expect("txn");
                txn.put_vector_f32(&t, row.as_bytes(), "f", b"", &f32s(i))
                    .put_vector_i8(
                        &t,
                        row.as_bytes(),
                        "q",
                        b"",
                        &[i as i8, (i as i8).wrapping_neg(), 0, 1],
                    )
                    .put_vector_bits(&t, row.as_bytes(), "b", b"", &(i as u16).to_le_bytes());
                txn.commit().expect("txn commit");
            }
        }
        if i == 100 {
            db.flush().expect("flush");
        }
    }
    db.compact().expect("compact");
    let fam = |n: &str| t.family_info(n).expect("family");
    let spec = fam("f").vector.expect("a vector family");
    assert_eq!(
        (spec.dim(), spec.element(), spec.metric()),
        (8, Element::F32, Metric::Cosine)
    );
    assert!(fam("plain").vector.is_none() && fam("f").aggregation.is_none());
    for i in 0..200u32 {
        let row = format!("r{i:04}");
        let cell = t
            .get(row.as_bytes(), "f", b"")
            .expect("get")
            .expect("written");
        let Value::Vector(v) = cell.typed() else {
            panic!("row {i}: not a vector");
        };
        assert_eq!(v.to_f32_vec(), Some(f32s(i)), "row {i}");
        assert!(spec.distance_f32(&f32s(i), v).expect("distance").abs() < 1e-6);
        let q = t
            .get(row.as_bytes(), "q", b"")
            .expect("get")
            .expect("written");
        let qv = q.as_vector().expect("i8 vector");
        assert_eq!(
            qv.to_f32_vec(),
            Some(vec![
                f32::from(i as i8),
                f32::from((i as i8).wrapping_neg()),
                0.0,
                1.0
            ])
        );
        let qspec = fam("q").vector.expect("spec");
        // Exact integers: (i - 0)² + (-i - 0)² + 0 + 1.
        let ii = f64::from(i as i8);
        assert_eq!(
            qspec.distance_i8(&[0, 0, 0, 0], qv).expect("distance"),
            2.0 * ii * ii + 1.0
        );
        let b = t
            .get(row.as_bytes(), "b", b"")
            .expect("get")
            .expect("written");
        let bv = b.as_vector().expect("bits");
        assert_eq!(bv.as_bytes(), (i as u16).to_le_bytes());
        let bspec = fam("b").vector.expect("spec");
        assert_eq!(
            bspec.distance_bits(&[0, 0], bv).expect("distance"),
            f64::from((i as u16).count_ones())
        );
    }
    db.close().expect("close");
}

#[test]
fn writes_that_dont_fit_the_spec_are_refused_when_built() {
    let (_dir, db, t) = open();
    let invalid = |r: pigeonhole::Result<pigeonhole::CommitInfo>, why: &str| {
        let e = r.expect_err(why);
        assert_eq!(e.code(), ErrorCode::InvalidArgument, "{why}: {e}");
        assert!(e.message().contains(why), "{why}: {}", e.message());
    };
    invalid(
        t.mutate(b"r").put_vector_f32("f", b"", &[1.0; 7]).commit(),
        "dimension",
    );
    invalid(
        t.mutate(b"r").put_vector_i8("f", b"", &[1; 8]).commit(),
        "element type",
    );
    let mut nan = [1.0f32; 8];
    nan[3] = f32::NAN;
    invalid(
        t.mutate(b"r").put_vector_f32("f", b"", &nan).commit(),
        "finite",
    );
    invalid(
        t.mutate(b"r").put_vector_f32("f", b"", &[0.0; 8]).commit(),
        "all zeros",
    );
    invalid(
        t.mutate(b"r")
            .put_vector_f32("plain", b"", &[1.0; 8])
            .commit(),
        "not a vector family",
    );
    // perf287's B1: components that would overflow a kernel, and a cosine norm that
    // underflows, are refused, so every stored vector has a finite distance to every query.
    invalid(
        t.mutate(b"r").put_vector_f32("f", b"", &[1e30; 8]).commit(),
        "2^56",
    );
    let mut tiny = [0.0f32; 8];
    tiny[0] = 1e-45;
    invalid(
        t.mutate(b"r").put_vector_f32("f", b"", &tiny).commit(),
        "nonzero norm",
    );
    let spec = t.family_info("f").and_then(|f| f.vector).expect("spec");
    t.mutate(b"ok")
        .put_vector_f32("f", b"", &[1.0; 8])
        .commit()
        .expect("put");
    let ok = t.get(b"ok", "f", b"").expect("get").expect("written");
    let e = spec
        .distance_f32(&tiny, ok.as_vector().expect("vector"))
        .expect_err("the same rule for a query");
    assert_eq!(e.code(), ErrorCode::InvalidArgument);
    // An i8 L2 family takes the zero vector.
    t.mutate(b"r")
        .put_vector_i8("q", b"", &[0; 4])
        .commit()
        .expect("zero under L2");
    // Nothing but vectors: a plain put, a merge, an aggregate input.
    for r in [
        t.mutate(b"r").put("f", b"", b"bytes").commit(),
        t.mutate(b"r").put_i64("f", b"", 3).commit(),
        t.mutate(b"r").merge("f", b"", b"op").commit(),
        t.mutate(b"r").aggregate("f", b"", 1).commit(),
    ] {
        assert_eq!(r.expect_err("refused").code(), ErrorCode::InvalidArgument);
    }
    // A refused write in a batch fails the whole batch; nothing lands.
    let mut wb = db.write_batch();
    wb.put_vector_f32(&t, b"s", "f", b"", &[1.0; 8])
        .put_vector_f32(&t, b"s", "f", b"x", &[1.0; 3]);
    assert!(wb.commit().is_err());
    assert!(t.get(b"s", "f", b"").expect("get").is_none());
    // Spec arguments.
    for (d, e, m) in [
        (0, Element::F32, Metric::L2),
        (4097, Element::F32, Metric::L2),
        (12, Element::Bit, Metric::Hamming),
        (16, Element::Bit, Metric::Cosine),
        (16, Element::F32, Metric::Hamming),
    ] {
        assert_eq!(
            VectorSpec::new(d, e, m).expect_err("refused").code(),
            ErrorCode::InvalidArgument,
            "{d} {e:?} {m:?}"
        );
    }
    db.close().expect("close");
}
