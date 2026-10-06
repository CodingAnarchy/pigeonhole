//! The resolver against the reference model, plus focused cases (#25, #21, limits).
#![allow(clippy::field_reassign_with_default)]

mod common;

use std::sync::Arc;

use common::*;
use pigeonhole_compaction::{
    CellResolver, Error, FilteredCursor, I64Add, MergeOperator, MergingCursor, ResolveOptions,
    ValuePredicate, VecCursor,
};
use pigeonhole_format::key::{Kind, encode_key, encode_marker_key};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::{Cursor, Seqno, Timestamp};
use pigeonhole_sim::Rng;
use proptest::prelude::*;

fn check_history(seed: u64, commits: usize) {
    let h = random_history(seed, commits);
    let mut rng = Rng::new(seed ^ 0x5eed);
    let max = h.model.snapshot();
    for _ in 0..4 {
        let snapshot = 1 + rng.below(max.max(1));
        let now = h.last_ts + rng.below(400);
        let expected = model_reads(&h, snapshot, now);
        let n = 1 + rng.below(4) as usize;
        let parts_seed = rng.next_u64();
        let actual = resolver_reads(&h, snapshot, now, |o| {
            let mut r = Rng::new(parts_seed);
            CellResolver::new(MergingCursor::new(vec_sources(&h.entries, n, &mut r)), o)
        });
        assert_same(
            &format!("seed {seed} snapshot {snapshot} now {now}"),
            &expected,
            &actual,
        );
        // Wrapping the sources in a pass-everything filter changes nothing.
        let actual = resolver_reads(&h, snapshot, now, |o| {
            let mut r = Rng::new(parts_seed);
            let sources = vec_sources(&h.entries, n, &mut r)
                .into_iter()
                .map(|s| FilteredCursor::new(s, ScanFilter::all()))
                .collect();
            CellResolver::new(MergingCursor::new(sources), o)
        });
        assert_same(&format!("filtered, seed {seed}"), &expected, &actual);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases(256)))]

    /// Done-when (1): the resolver over a merge of random sources equals the model at random
    /// snapshots.
    #[test]
    fn resolver_matches_model(seed in any::<u64>(), commits in 1usize..40) {
        check_history(seed, commits);
    }
}

#[test]
fn resolver_matches_model_fixed_seeds() {
    for seed in 0..if cfg!(miri) { 2 } else { 64 } {
        check_history(seed, 30);
    }
}

// ---------------------------------------------------------------------------------------
// Focused cases
// ---------------------------------------------------------------------------------------

fn key(row: &[u8], q: &[u8], ts: Timestamp, seqno: Seqno, kind: Kind) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(&mut k, row, q, ts, seqno, kind).unwrap();
    k
}

fn marker(row: &[u8], ts: Timestamp, seqno: Seqno) -> Vec<u8> {
    let mut k = Vec::new();
    encode_marker_key(&mut k, row, ts, seqno).unwrap();
    k
}

fn i64v(v: i64) -> Vec<u8> {
    stored(&v.to_le_bytes())
}

fn resolve(entries: Vec<(Vec<u8>, Vec<u8>)>, o: ResolveOptions) -> Vec<(Timestamp, Vec<u8>)> {
    try_resolve(entries, o).unwrap()
}

fn try_resolve(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    o: ResolveOptions,
) -> Result<Vec<(Timestamp, Vec<u8>)>, Error> {
    let mut r = CellResolver::new(VecCursor::new(entries), o);
    r.seek(b"")?;
    let mut out = Vec::new();
    while let Some(c) = r.next_cell()? {
        out.push((c.ts, c.value.to_vec()));
    }
    Ok(out)
}

fn all_versions(snapshot: Seqno) -> ResolveOptions {
    let mut o = ResolveOptions::new(snapshot, 1000);
    o.versions = 0;
    o.merge = Some(Arc::new(I64Add));
    o
}

/// Mirrors the model's `cell_delete_hits_exact_timestamp_only` (#25, D38).
#[test]
fn cell_delete_hits_exact_timestamp_only() {
    let mut e = vec![
        (key(b"r", b"q", 5, 1, Kind::Put), stored(b"a")),
        (key(b"r", b"q", 6, 1, Kind::Put), stored(b"b")),
        (key(b"r", b"q", 6, 2, Kind::CellDelete), vec![]),
    ];
    assert_eq!(resolve(e.clone(), all_versions(2)), [(5, stored(b"a"))]);
    assert_eq!(resolve(e.clone(), all_versions(1)).len(), 2);
    // A later put at the deleted timestamp stays hidden.
    e.push((key(b"r", b"q", 6, 3, Kind::Put), stored(b"c")));
    assert_eq!(resolve(e.clone(), all_versions(3)), [(5, stored(b"a"))]);
    // One at another timestamp is visible; a snapshot before the delete sees the original.
    e.push((key(b"r", b"q", 7, 4, Kind::Put), stored(b"d")));
    assert_eq!(
        resolve(e.clone(), all_versions(4)),
        [(7, stored(b"d")), (5, stored(b"a"))]
    );
    assert_eq!(resolve(e, all_versions(1))[0], (6, stored(b"b")));
}

/// Mirrors the model's `cell_delete_hides_operands_at_its_timestamp` (#25).
#[test]
fn cell_delete_hides_operands_at_its_timestamp() {
    let e = vec![
        (key(b"r", b"n", 10, 1, Kind::Merge), i64v(1)),
        (key(b"r", b"n", 20, 2, Kind::CellDelete), vec![]),
        (key(b"r", b"n", 20, 3, Kind::Merge), i64v(5)),
    ];
    let mut o = ResolveOptions::new(3, 100);
    o.merge = Some(Arc::new(I64Add));
    assert_eq!(resolve(e, o), [(10, i64v(1))]);
}

/// A large put followed (in key order) by an older delete at its timestamp: the resolver
/// reads the whole group first, and returns a surviving large value zero-copy.
#[test]
fn large_values_are_returned_from_the_source() {
    let big = stored(&vec![7u8; 10_000]);
    let e = vec![
        (key(b"r", b"q", 9, 5, Kind::Put), big.clone()),
        (key(b"r", b"q", 9, 2, Kind::CellDelete), vec![]),
        (key(b"r", b"x", 9, 5, Kind::Put), big.clone()),
        (key(b"r", b"x", 8, 4, Kind::Put), stored(b"small")),
    ];
    let mut r = CellResolver::new(VecCursor::new(e), all_versions(9));
    r.seek(b"").unwrap();
    let c = r.next_cell().unwrap().unwrap();
    assert!(c.from_source);
    assert_eq!(
        (c.key, c.value),
        (&key(b"r", b"x", 9, 5, Kind::Put)[..], &big[..])
    );
    assert_eq!(r.cursor().key(), &key(b"r", b"x", 9, 5, Kind::Put)[..]);
    let c = r.next_cell().unwrap().unwrap();
    assert!(!c.from_source);
    assert_eq!(c.value, stored(b"small"));
    assert!(r.next_cell().unwrap().is_none());
}

/// #21: an `i64` fold onto a non-`i64` base is a `MergeError`, never 0; the sum works.
#[test]
fn i64_add_rejects_a_non_i64_base() {
    let mut acc = i64v(2);
    I64Add.merge(&mut acc, &i64v(i64::MAX)).unwrap();
    assert_eq!(acc, i64v(i64::MAX.wrapping_add(2)));
    let mut acc = i64v(3);
    I64Add.finish(None, &mut acc).unwrap();
    assert_eq!(acc, i64v(3));
    let mut acc = i64v(3);
    I64Add.finish(Some(&i64v(4)), &mut acc).unwrap();
    assert_eq!(acc, i64v(7));
    let mut acc = i64v(3);
    assert!(I64Add.finish(Some(&stored(b"abc")), &mut acc).is_err());
    let mut acc = stored(b"abc");
    assert!(I64Add.merge(&mut acc, &i64v(1)).is_err());

    // Through the resolver: the read fails; an older version beyond the limit does not.
    let e = vec![
        (key(b"r", b"n", 10, 1, Kind::Put), stored(b"abc")),
        (key(b"r", b"n", 20, 2, Kind::Merge), i64v(1)),
    ];
    let mut o = ResolveOptions::new(2, 100);
    o.merge = Some(Arc::new(I64Add));
    assert!(matches!(
        try_resolve(e.clone(), o.clone()),
        Err(Error::Merge(_))
    ));
    // At snapshot 1 the base is returned as written.
    o.snapshot = 1;
    assert_eq!(resolve(e, o), [(10, stored(b"abc"))]);
}

#[test]
fn family_markers_and_seek_column() {
    let e = vec![
        (marker(b"r", 50, 3), vec![]),
        (key(b"r", b"a", 40, 1, Kind::Put), stored(b"old")),
        (key(b"r", b"a", 60, 4, Kind::Put), stored(b"new")),
        (key(b"s", b"a", 10, 2, Kind::Put), stored(b"other row")),
    ];
    let get = |snapshot, q: &[u8]| {
        let mut r = CellResolver::new(VecCursor::new(e.clone()), ResolveOptions::new(snapshot, 99));
        r.seek_column(b"r", q).unwrap();
        r.next_cell().unwrap().map(|c| (c.ts, c.value.to_vec()))
    };
    assert_eq!(get(4, b"a"), Some((60, stored(b"new"))));
    assert_eq!(get(3, b"a"), None); // hidden by the marker
    assert_eq!(get(2, b"a"), Some((40, stored(b"old"))));
    // A point get never runs into the next column or row.
    assert_eq!(get(4, b"b"), None);
}

#[test]
fn columns_per_row_versions_and_predicates() {
    let e = vec![
        (key(b"r", b"a", 3, 1, Kind::Put), stored(b"a3")),
        (key(b"r", b"a", 2, 1, Kind::Put), stored(b"a2")),
        (key(b"r", b"b", 3, 1, Kind::Put), stored(b"b3")),
        (key(b"r", b"c", 3, 1, Kind::Put), stored(b"c3")),
        (key(b"s", b"a", 3, 1, Kind::Put), stored(b"s3")),
        (key(b"s", b"b", 3, 1, Kind::Put), i64v(42)),
    ];
    let mut o = ResolveOptions::new(9, 9);
    o.columns_per_row = 2;
    o.versions = 0;
    let got: Vec<_> = resolve(e.clone(), o).into_iter().map(|c| c.1).collect();
    assert_eq!(
        got,
        [
            stored(b"a3"),
            stored(b"a2"),
            stored(b"b3"),
            stored(b"s3"),
            i64v(42)
        ]
    );

    let mut o = ResolveOptions::new(9, 9);
    o.value = Some(ValuePredicate::Prefix(b"b".to_vec()));
    assert_eq!(resolve(e.clone(), o), [(3, stored(b"b3"))]);
    let mut o = ResolveOptions::new(9, 9);
    o.value = Some(ValuePredicate::I64(std::cmp::Ordering::Greater, 40));
    assert_eq!(resolve(e.clone(), o), [(3, i64v(42))]);

    // `skip_row` from the caller.
    let mut r = CellResolver::new(VecCursor::new(e), ResolveOptions::new(9, 9));
    r.seek(b"").unwrap();
    assert_eq!(r.next_cell().unwrap().unwrap().value, stored(b"a3"));
    r.skip_row().unwrap();
    assert_eq!(r.next_cell().unwrap().unwrap().value, stored(b"s3"));
}

#[test]
fn merging_cursor_skip_row_spans_sources() {
    let a = VecCursor::new(vec![
        (key(b"r", b"a", 1, 1, Kind::Put), vec![0]),
        (key(b"s", b"a", 1, 1, Kind::Put), vec![0]),
    ]);
    let b = VecCursor::new(vec![
        (key(b"r", b"b", 1, 2, Kind::Put), vec![0]),
        (key(b"t", b"a", 1, 2, Kind::Put), vec![0]),
    ]);
    let mut m = MergingCursor::new(vec![a, b]);
    m.seek_to_first().unwrap();
    m.skip_row().unwrap();
    assert_eq!(m.key(), &key(b"s", b"a", 1, 1, Kind::Put)[..]);
    assert_eq!(m.current().unwrap().key(), m.key());
    m.skip_row().unwrap();
    assert_eq!(m.key(), &key(b"t", b"a", 1, 2, Kind::Put)[..]);
    m.next().unwrap();
    assert!(!m.valid());
}
