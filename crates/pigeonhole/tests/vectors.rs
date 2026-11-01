//! D303 (#490 B, ICR 0098): vector families through the public API. Vectors written with
//! `put_vector_f32`, `put_vector_i8` and `put_vector_bits` (on a mutation, a write batch and a
//! transaction) read back as `Value::Vector`, through flushes and compactions and at a
//! blob threshold every vector is above (they stay inline). Each write is refused when it's
//! built if it doesn't fit the family's spec, and a vector family refuses plain puts, merges
//! and aggregate inputs. `VectorSpec::distance_*` scores as the kernels do, and D296 PR 4's
//! `nearest_*` equals a brute force ranked by it, ties by row, at any fan-out.

use pigeonhole::{
    Element, ErrorCode, Family, KeySet, Metric, Neighbour, Options, Pigeonhole, Table, Value,
    ValueFilter, Vector, VectorSpec,
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

/// Every row's vector in `family`, scored by `score` and ranked as `nearest_*` ranks:
/// distance (`total_cmp`), then row key, then qualifier; the first `k`.
fn brute(
    t: &Table,
    family: &str,
    rows: &[Vec<u8>],
    score: impl Fn(pigeonhole::VectorRef<'_>) -> f64,
    k: usize,
) -> Vec<(Vec<u8>, f64)> {
    let mut all: Vec<(f64, Vec<u8>)> = rows
        .iter()
        .filter_map(|r| {
            let cell = t.get(r, family, b"").expect("get")?;
            Some((score(cell.as_vector().expect("vector")), r.clone()))
        })
        .collect();
    all.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    all.into_iter().take(k).map(|(d, r)| (r, d)).collect()
}

fn got(n: Vec<Neighbour>) -> Vec<(Vec<u8>, f64)> {
    assert!(n.iter().all(|x| x.qualifier.is_empty() && x.ts > 0));
    n.into_iter().map(|x| (x.row, x.distance)).collect()
}

#[test]
fn nearest_equals_a_brute_force_ranked_by_the_spec_for_every_element() {
    let (_dir, db, t) = open();
    let rows: Vec<Vec<u8>> = (0..300u32)
        .map(|i| format!("r{i:04}").into_bytes())
        .collect();
    for (i, row) in rows.iter().enumerate() {
        let i = i as u32;
        t.mutate(row)
            .put_vector_f32("f", b"", &f32s(i))
            .put_vector_i8("q", b"", &[(i % 7) as i8, -((i % 5) as i8), 3, 1])
            .put_vector_bits("b", b"", &((i % 37) as u16).to_le_bytes())
            .commit()
            .expect("put");
        if i == 150 {
            db.flush().expect("flush");
        }
    }
    let spec = |f: &str| t.family_info(f).and_then(|i| i.vector).expect("spec");
    let (fs, qs, bs) = (spec("f"), spec("q"), spec("b"));
    let qf = [0.5f32, -1.0, 2.0, 0.25, 3.0, -2.5, 1.5, 0.75];
    let qi = [2i8, -1, 3, 0];
    let qb = 0b1010_0101_0011_1100u16.to_le_bytes();
    let some: Vec<Vec<u8>> = rows.iter().step_by(3).cloned().collect();
    let some_set = KeySet::from_unsorted(&some);
    for k in [1usize, 7, 50, 1000] {
        let want = brute(&t, "f", &rows, |v| fs.distance_f32(&qf, v).unwrap(), k);
        assert_eq!(
            got(t
                .scan_prefix(b"")
                .family("f")
                .nearest_f32(&qf, k)
                .run()
                .unwrap()),
            want,
            "f32 k={k}"
        );
        assert_eq!(
            got(t
                .scan_prefix(b"")
                .family("f")
                .nearest_f32(&qf, k)
                .parallel(4)
                .run()
                .unwrap()),
            want,
            "f32 k={k}, fanned out"
        );
        // Integer distances tie often: the order is by row then.
        let want = brute(&t, "q", &rows, |v| qs.distance_i8(&qi, v).unwrap(), k);
        assert_eq!(
            got(t
                .scan_prefix(b"")
                .family("q")
                .nearest_i8(&qi, k)
                .run()
                .unwrap()),
            want,
            "i8 k={k}"
        );
        let want = brute(&t, "b", &rows, |v| bs.distance_bits(&qb, v).unwrap(), k);
        assert_eq!(
            got(t
                .scan_prefix(b"")
                .family("b")
                .nearest_bits(&qb, k)
                .run()
                .unwrap()),
            want,
            "bits k={k}"
        );
        // Over a key set.
        let want = brute(&t, "f", &some, |v| fs.distance_f32(&qf, v).unwrap(), k);
        assert_eq!(
            got(t
                .keys(&some_set)
                .family("f")
                .nearest_f32(&qf, k)
                .parallel(4)
                .run()
                .unwrap()),
            want,
            "keys k={k}"
        );
    }
    // A range narrows the candidates.
    let want = brute(
        &t,
        "f",
        &rows[100..200],
        |v| fs.distance_f32(&qf, v).unwrap(),
        5,
    );
    let near = t
        .scan_bounds(
            std::ops::Bound::Included(b"r0100"),
            std::ops::Bound::Excluded(b"r0200"),
        )
        .family("f")
        .nearest_f32(&qf, 5)
        .run()
        .unwrap();
    assert_eq!(got(near), want);
    assert!(
        t.scan_prefix(b"")
            .family("f")
            .nearest_f32(&qf, 0)
            .run()
            .unwrap()
            .is_empty()
    );
    db.close().expect("close");
}

#[test]
fn nearest_refuses_what_doesnt_fit() {
    let (_dir, db, t) = open();
    t.mutate(b"r")
        .put_vector_f32("f", b"", &[1.0; 8])
        .commit()
        .expect("put");
    let code = |r: pigeonhole::Result<Vec<Neighbour>>| r.expect_err("refused").code();
    for r in [
        t.scan_prefix(b"")
            .family("f")
            .nearest_f32(&[1.0; 7], 3)
            .run(),
        t.scan_prefix(b"").family("f").nearest_i8(&[1; 8], 3).run(),
        t.scan_prefix(b"")
            .family("f")
            .nearest_f32(&[0.0; 8], 3)
            .run(),
        t.scan_prefix(b"")
            .family("f")
            .nearest_f32(&[f32::NAN; 8], 3)
            .run(),
        t.scan_prefix(b"")
            .family("plain")
            .nearest_f32(&[1.0; 8], 3)
            .run(),
        t.scan_prefix(b"")
            .families(["f", "q"])
            .nearest_f32(&[1.0; 8], 3)
            .run(),
        t.scan_prefix(b"").nearest_f32(&[1.0; 8], 3).run(),
    ] {
        assert_eq!(code(r), ErrorCode::InvalidArgument);
    }
    db.close().expect("close");
}

/// An owned `Vector` (#490 D: centroids and means aren't cells) scores as a stored vector
/// does, and widening an `i8` vector to `f32` against an `f32` spec is exact.
#[test]
fn owned_vectors_score_as_stored_ones() {
    let (_dir, db, t) = open();
    t.mutate(b"r")
        .put_vector_f32("f", b"", &f32s(3))
        .put_vector_i8("q", b"", &[4, -2, 0, 1])
        .commit()
        .expect("put");
    let fs = t.family_info("f").and_then(|f| f.vector).expect("spec");
    let stored = t.get(b"r", "f", b"").expect("get").expect("written");
    let owned = Vector::from_f32(&f32s(3)).expect("vector");
    assert_eq!((owned.element(), owned.dim()), (Element::F32, 8));
    let q = [0.5f32, -1.0, 2.0, 0.25, 3.0, -2.5, 1.5, 0.75];
    assert_eq!(
        fs.distance_f32(&q, owned.as_ref()).unwrap().to_bits(),
        fs.distance_f32(&q, stored.as_vector().unwrap())
            .unwrap()
            .to_bits(),
    );
    assert_eq!(owned.as_ref(), stored.as_vector().unwrap());
    // An i8 vector widened, against an f32 centroid of an f32 L2 spec.
    let centroid = Vector::from_f32(&[1.0, 1.0, 1.0, 1.0]).expect("vector");
    let spec = VectorSpec::new(4, Element::F32, Metric::L2).unwrap();
    let i8v = t.get(b"r", "q", b"").expect("get").expect("written");
    let widened = i8v.as_vector().unwrap().to_f32_vec().unwrap();
    assert_eq!(
        spec.distance_f32(&widened, centroid.as_ref()).unwrap(),
        9.0 + 9.0 + 1.0
    );
    let bits = Vector::from_bits(16, &[0xff, 0x00]).expect("bits");
    assert_eq!(bits.dim(), 16);
    for bad in [
        Vector::from_f32(&[]),
        Vector::from_f32(&[0.0; 4097]),
        Vector::from_i8(&[]),
        Vector::from_bits(12, &[0, 0]),
        Vector::from_bits(16, &[0]),
    ] {
        assert_eq!(bad.expect_err("refused").code(), ErrorCode::InvalidArgument);
    }
    db.close().expect("close");
}

/// #1263: `f16` and `bf16` families. Their vectors are stored and read back bit for bit;
/// `f32` vectors written to them round to nearest-even, with an overflow, a non-finite
/// component or a vector that rounds to zero under cosine refused when built; and
/// `nearest_f32` over them gives the same neighbours and distances as the same vectors
/// widened into an `f32` family. At 4096 dimensions a half vector's value is half the bytes.
#[test]
fn half_precision_families_store_bits_and_rank_as_widened_f32() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("h.phdb"), Options::default()).expect("open");
    let spec = |e, m| VectorSpec::new(8, e, m).expect("spec");
    let t = db
        .table("h")
        .expect("table")
        .family("h16", Family::vector(spec(Element::F16, Metric::Cosine)))
        .family("hb", Family::vector(spec(Element::BF16, Metric::L2)))
        .family("w16", Family::vector(spec(Element::F32, Metric::Cosine)))
        .family("wb", Family::vector(spec(Element::F32, Metric::L2)))
        .create_if_missing()
        .expect("create");
    assert!(
        db.info().expect("info").features.incompat & 1 == 1,
        "a half-precision family marks the file, as every 1.1.0 vector family does"
    );
    let bits16 = |i: u32| -> Vec<u16> {
        (0..8)
            .map(|j| pigeonhole_format::half::f32_to_f16(f32s(i)[j] / 3.0).expect("fits"))
            .collect()
    };
    let bitsb = |i: u32| -> Vec<u16> {
        (0..8)
            .map(|j| pigeonhole_format::half::f32_to_bf16(f32s(i)[j] * 1e3).expect("fits"))
            .collect()
    };
    let widen16 = |b: &[u16]| -> Vec<f32> {
        b.iter()
            .map(|h| pigeonhole_format::half::f16_to_f32(*h))
            .collect()
    };
    let widenb = |b: &[u16]| -> Vec<f32> {
        b.iter()
            .map(|h| pigeonhole_format::half::bf16_to_f32(*h))
            .collect()
    };
    for i in 0..200u32 {
        let row = format!("r{i:04}");
        let (h, b) = (bits16(i), bitsb(i));
        t.mutate(row.as_bytes())
            .put_vector_f16("h16", b"", &h)
            .put_vector_bf16("hb", b"", &b)
            .put_vector_f32("w16", b"", &widen16(&h))
            .put_vector_f32("wb", b"", &widenb(&b))
            .commit()
            .expect("put");
    }
    db.flush().expect("flush");
    // Bit for bit, and widened exactly.
    let cell = t.get(b"r0007", "h16", b"").expect("get").expect("written");
    let v = cell.as_vector().expect("vector");
    assert_eq!(v.element(), Element::F16);
    let stored: Vec<u16> = v
        .as_bytes()
        .chunks(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    assert_eq!(stored, bits16(7));
    assert_eq!(v.to_f32_vec(), Some(widen16(&bits16(7))));
    // nearest_f32 over halves equals nearest_f32 over the widened f32 vectors.
    let q = [0.5f32, -1.0, 2.0, 0.25, 3.0, -2.5, 1.5, 0.75];
    for (half, wide) in [("h16", "w16"), ("hb", "wb")] {
        let a = t
            .scan_prefix(b"")
            .family(half)
            .nearest_f32(&q, 20)
            .run()
            .unwrap();
        let b = t
            .scan_prefix(b"")
            .family(wide)
            .nearest_f32(&q, 20)
            .run()
            .unwrap();
        let strip = |v: Vec<Neighbour>| -> Vec<(Vec<u8>, u64)> {
            v.into_iter()
                .map(|n| (n.row, n.distance.to_bits()))
                .collect()
        };
        assert_eq!(strip(a), strip(b), "{half} ranks as {wide}");
    }
    // f32 writes into a half family: rounded to nearest-even, refusals at build time.
    t.mutate(b"conv")
        .put_vector_f32(
            "h16",
            b"",
            &[1.0, -2.0, 0.333_333, 1e-3, 4.0, 5.0, 6.0, 7.0],
        )
        .commit()
        .expect("put f32 into f16");
    let c = t.get(b"conv", "h16", b"").expect("get").expect("written");
    let want: Vec<f32> = [1.0f32, -2.0, 0.333_333, 1e-3, 4.0, 5.0, 6.0, 7.0]
        .iter()
        .map(|x| {
            pigeonhole_format::half::f16_to_f32(pigeonhole_format::half::f32_to_f16(*x).unwrap())
        })
        .collect();
    assert_eq!(c.as_vector().unwrap().to_f32_vec(), Some(want));
    let invalid = |r: pigeonhole::Result<pigeonhole::CommitInfo>, why: &str| {
        let e = r.expect_err(why);
        assert_eq!(e.code(), ErrorCode::InvalidArgument, "{why}: {e}");
        assert!(e.message().contains(why), "{why}: {}", e.message());
    };
    invalid(
        t.mutate(b"x")
            .put_vector_f32("h16", b"", &[70_000.0; 8])
            .commit(),
        "overflows",
    );
    invalid(
        t.mutate(b"x")
            .put_vector_f32("h16", b"", &[1e-9; 8])
            .commit(),
        "rounds to a zero vector",
    );
    invalid(
        t.mutate(b"x")
            .put_vector_f16("h16", b"", &[0x7c00; 8])
            .commit(),
        "finite",
    );
    invalid(
        t.mutate(b"x")
            .put_vector_f16("hb", b"", &[0x3c00; 8])
            .commit(),
        "element type",
    );
    invalid(
        t.mutate(b"x").put_vector_i8("h16", b"", &[1; 8]).commit(),
        "element type",
    );
    // Half the bytes at 4096 dimensions (the payload after the element and dimension).
    let big = db
        .table("big")
        .expect("table")
        .family(
            "h",
            Family::vector(VectorSpec::new(4096, Element::F16, Metric::L2).unwrap()),
        )
        .family(
            "f",
            Family::vector(VectorSpec::new(4096, Element::F32, Metric::L2).unwrap()),
        )
        .create_if_missing()
        .expect("create");
    big.mutate(b"r")
        .put_vector_f32("h", b"", &[0.5; 4096])
        .put_vector_f32("f", b"", &[0.5; 4096])
        .commit()
        .expect("put");
    let h = big.get(b"r", "h", b"").unwrap().unwrap();
    let f = big.get(b"r", "f", b"").unwrap().unwrap();
    assert_eq!(
        h.as_vector().unwrap().as_bytes().len() * 2,
        f.as_vector().unwrap().as_bytes().len()
    );
    drop((t, big));
    db.close().expect("close");
}

/// Polls `fut` to completion with a no-op waker: its output, and how often it was `Pending`.
#[cfg(feature = "async")]
fn poll_counting<F: std::future::Future>(fut: F) -> (F::Output, usize) {
    use std::task::{Context, Poll, Waker};
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    let mut pending = 0;
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(r) => return (r, pending),
            Poll::Pending => pending += 1,
        }
        assert!(pending < 1_000_000, "never finished");
    }
}

/// `run_async` gives `run`'s neighbours from a cold block cache (a reopen, so its rows'
/// blocks are fetched), for every element, over a scan and a key set.
#[cfg(feature = "async")]
#[test]
fn an_async_search_returns_what_the_sync_one_returns() {
    let (dir, db, t) = open();
    let rows: Vec<Vec<u8>> = (0..400u32)
        .map(|i| format!("r{i:04}").into_bytes())
        .collect();
    for (i, row) in rows.iter().enumerate() {
        let i = i as u32;
        t.mutate(row)
            .put_vector_f32("f", b"", &f32s(i))
            .put_vector_i8("q", b"", &[(i % 7) as i8, -((i % 5) as i8), 3, 1])
            .put_vector_bits("b", b"", &((i % 37) as u16).to_le_bytes())
            .commit()
            .expect("put");
    }
    db.flush().expect("flush");
    drop(t);
    db.close().expect("close");
    let qf = [0.5f32, -1.0, 2.0, 0.25, 3.0, -2.5, 1.5, 0.75];
    let qi = [2i8, -1, 3, 0];
    let qb = 0b1010_0101_0011_1100u16.to_le_bytes();
    let some = KeySet::from_unsorted(rows.iter().step_by(3));
    for k in [1usize, 25, 1000] {
        let db =
            Pigeonhole::open(dir.join("v.phdb"), Options::default().shards(2)).expect("reopen");
        let t = db.table("v").expect("table").open().expect("open");
        // First, from the cold cache. Whether a poll returns `Pending` depends on the backend
        // (io_uring can complete a fetch before the poll returns), so the proof that the
        // async path ran is in the counters: the search missed the cache, and fewer of its
        // reads were synchronous than it missed, so the rest were fetched asynchronously.
        let before = db.engine_metrics();
        let (got, pending) = poll_counting(
            t.scan_prefix(b"")
                .family("f")
                .nearest_f32(&qf, k)
                .run_async(),
        );
        let got = got.unwrap();
        let after = db.engine_metrics();
        let misses = after.block_cache_misses - before.block_cache_misses;
        let sync_reads = after.async_sync_reads - before.async_sync_reads;
        eprintln!("k={k}: {misses} misses, {sync_reads} sync reads, {pending} pending");
        assert!(misses > 0, "k={k}: the search started from a cold cache");
        assert!(
            sync_reads < misses,
            "k={k}: some of the {misses} missed blocks were fetched asynchronously \
             ({sync_reads} read synchronously, {pending} Pending)"
        );
        let want = t
            .scan_prefix(b"")
            .family("f")
            .nearest_f32(&qf, k)
            .run()
            .unwrap();
        assert_eq!(got, want, "f32 k={k}");
        let want = t
            .scan_prefix(b"")
            .family("q")
            .nearest_i8(&qi, k)
            .run()
            .unwrap();
        let got = pigeonhole::doc_support::block_on(
            t.scan_prefix(b"")
                .family("q")
                .nearest_i8(&qi, k)
                .run_async(),
        )
        .unwrap();
        assert_eq!(got, want, "i8 k={k}");
        let want = t
            .keys(&some)
            .family("b")
            .nearest_bits(&qb, k)
            .run()
            .unwrap();
        let got = pigeonhole::doc_support::block_on(
            t.keys(&some).family("b").nearest_bits(&qb, k).run_async(),
        )
        .unwrap();
        assert_eq!(got, want, "bits over keys k={k}");
        drop(t);
        db.close().expect("close");
    }
}

/// A poll reads at most 4,096 cells, then yields: one row of 10,000 vectors (an IVF list's
/// shape) takes at least three polls, every block cached, and gives `run`'s result.
#[cfg(feature = "async")]
#[test]
fn an_async_search_yields_within_a_wide_row() {
    let (_dir, db, t) = open();
    for chunk in (0..10_000u32).collect::<Vec<_>>().chunks(1_000) {
        let mut m = t.mutate(b"list");
        for &i in chunk {
            m = m.put_vector_f32("f", format!("id{i:05}").as_bytes(), &f32s(i));
        }
        m.commit().expect("put");
    }
    db.flush().expect("flush");
    let q = [1.0f32; 8];
    let list = KeySet::from_unsorted([b"list"]);
    let want = t.keys(&list).family("f").nearest_f32(&q, 10).run().unwrap();
    // Warm the cache, then count polls.
    let _ = pigeonhole::doc_support::block_on(
        t.keys(&list).family("f").nearest_f32(&q, 10).run_async(),
    );
    let (got, pending) = poll_counting(t.keys(&list).family("f").nearest_f32(&q, 10).run_async());
    assert_eq!(got.unwrap(), want);
    assert!(pending >= 2, "{pending} yields over 10,000 cells");
    db.close().expect("close");
}

/// `run_async`'s future is `Send`, so a multi-threaded executor can run it (compile-time).
#[cfg(feature = "async")]
#[allow(dead_code)]
fn run_async_is_send(t: &Table, keys: &KeySet) {
    fn send<T: Send>(_: T) {}
    send(
        t.keys(keys)
            .family("f")
            .nearest_f32(&[1.0; 8], 3)
            .run_async(),
    );
    send(
        t.scan_prefix(b"")
            .family("f")
            .nearest_f32(&[1.0; 8], 3)
            .run_async(),
    );
}

/// Dropping an async search between a `Pending` and its next poll releases what it holds:
/// the database flushes, compacts, searches and closes after.
#[cfg(feature = "async")]
#[test]
fn a_dropped_async_search_releases_what_it_holds() {
    use std::task::{Context, Poll, Waker};
    let (_dir, db, t) = open();
    for chunk in (0..10_000u32).collect::<Vec<_>>().chunks(1_000) {
        let mut m = t.mutate(b"list");
        for &i in chunk {
            m = m.put_vector_f32("f", format!("id{i:05}").as_bytes(), &f32s(i));
        }
        m.commit().expect("put");
    }
    db.flush().expect("flush");
    let list = KeySet::from_unsorted([b"list"]);
    {
        let mut fut = std::pin::pin!(
            t.keys(&list)
                .family("f")
                .nearest_f32(&[1.0; 8], 5)
                .run_async()
        );
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
            "a 10,000-cell row yields"
        );
    }
    db.flush().expect("flush after the drop");
    db.compact().expect("compact after the drop");
    assert_eq!(
        t.keys(&list)
            .family("f")
            .nearest_f32(&[1.0; 8], 5)
            .run()
            .unwrap()
            .len(),
        5
    );
    drop(t);
    db.close().expect("close after the drop");
}

/// perf287's S1 on #1269 (#1237's rule): a value filter's misses count toward the poll
/// budget. One row of 10,000 vectors, of which 1% pass the filter (a value-equality on the
/// vector's bytes), still takes several polls, and gives `run`'s result.
#[cfg(feature = "async")]
#[test]
fn an_async_search_yields_over_filtered_cells() {
    let (_dir, db, t) = open();
    let kept = [1.0f32; 8];
    for chunk in (0..10_000u32).collect::<Vec<_>>().chunks(1_000) {
        let mut m = t.mutate(b"list");
        for &i in chunk {
            let v = if i % 100 == 0 { kept.to_vec() } else { f32s(i) };
            m = m.put_vector_f32("f", format!("id{i:05}").as_bytes(), &v);
        }
        m.commit().expect("put");
    }
    db.flush().expect("flush");
    // The filter compares a vector cell's value: element, dimension, payload.
    let value = t
        .get(b"list", "f", b"id00000")
        .expect("get")
        .expect("written");
    let only_kept = || ValueFilter::Equals(value.value().to_vec());
    let list = KeySet::from_unsorted([b"list"]);
    let search = || {
        t.keys(&list)
            .family("f")
            .value_filter(only_kept())
            .nearest_f32(&[0.5; 8], 5)
    };
    let want = search().run().unwrap();
    assert_eq!(want.len(), 5);
    assert!(want.iter().all(|n| n.qualifier.ends_with(b"00")));
    let (got, pending) = poll_counting(search().run_async());
    assert_eq!(got.unwrap(), want);
    assert!(
        pending >= 2,
        "{pending} yields over 10,000 cells, 9,900 filtered out"
    );
    db.close().expect("close");
}
