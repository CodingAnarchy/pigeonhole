//! The resolver against the reference model, plus focused cases (#25, #21, limits).
#![allow(clippy::field_reassign_with_default)]

mod common;

use std::sync::Arc;

use common::*;
use pigeonhole_compaction::{
    CellResolver, Error, FilteredCursor, I64Add, MergeBuffers, MergeOperator, MergingCursor,
    ResolveOptions, ResolverBuffers, ValuePredicate, VecCursor,
};
use pigeonhole_format::key::{Kind, encode_key, encode_marker_key};
use pigeonhole_format::scan::ScanFilter;
use pigeonhole_format::{Cursor, Seqno, Timestamp};
use pigeonhole_sim::Rng;
use proptest::prelude::*;

fn check_history(seed: u64, n: usize) {
    let h = random_history(seed, commits(n));
    let mut rng = Rng::new(seed ^ 0x5eed);
    let max = h.model.snapshot();
    for _ in 0..if cfg!(miri) { 1 } else { 4 } {
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
        check_extras(&h, snapshot, now, &mut rng, |o| {
            let mut r = Rng::new(parts_seed);
            CellResolver::new(MergingCursor::new(vec_sources(&h.entries, n, &mut r)), o)
        });
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
    for seed in 0..if cfg!(miri) { 1 } else { 64 } {
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

/// `current` repeats the cell `next_cell` returned (from the source or from the buffers)
/// until the resolver moves.
#[test]
fn current_repeats_the_last_cell_until_the_resolver_moves() {
    let big = stored(&vec![7u8; 10_000]);
    let e = vec![
        (key(b"r", b"x", 9, 5, Kind::Put), big.clone()),
        (key(b"r", b"x", 8, 4, Kind::Put), stored(b"small")),
        (key(b"r", b"y", 9, 6, Kind::Put), stored(b"y")),
        (key(b"s", b"z", 9, 7, Kind::Put), stored(b"z")),
    ];
    let mut r = CellResolver::new(VecCursor::new(e), all_versions(9));
    assert!(r.current().is_none());
    r.seek(b"").unwrap();
    assert!(r.current().is_none());
    let mut seen = Vec::new();
    while let Some(c) = r.next_cell().unwrap() {
        let returned = (c.key.to_vec(), c.ts, c.value.to_vec(), c.from_source);
        let again = r.current().unwrap();
        assert_eq!(
            (
                again.key.to_vec(),
                again.ts,
                again.value.to_vec(),
                again.from_source
            ),
            returned
        );
        seen.push(returned);
        if seen.len() == 3 {
            // Skipping the rest of the row moves the resolver: nothing is current.
            r.skip_row().unwrap();
            assert!(r.current().is_none());
        }
    }
    assert!(r.current().is_none());
    // Large from the source, then the small older version and `y` from the buffers, then `z`
    // in the next row.
    assert_eq!(
        seen.iter().map(|s| (s.1, s.3)).collect::<Vec<_>>(),
        [(9, true), (8, false), (9, false), (9, false)]
    );
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

/// Proposed D22 amendment: for a merge family the time range applies to resolved versions,
/// so a counter whose base is outside the range still sums it; pushing the range down to
/// puts would drop the base and keep the operands.
#[test]
fn counter_time_range_applies_to_resolved_versions() {
    let e = vec![
        (key(b"r", b"n", 10, 1, Kind::Put), i64v(100)),
        (key(b"r", b"n", 20, 2, Kind::Merge), i64v(1)),
        (key(b"r", b"n", 30, 3, Kind::Merge), i64v(2)),
    ];
    let mut filter = ScanFilter::all();
    let mut o = ResolveOptions::new(3, 100);
    o.merge = Some(Arc::new(I64Add));
    o.route_time_range(&mut filter, Some((25, 40)));
    let src = FilteredCursor::new(VecCursor::new(e.clone()), filter);
    let mut r = CellResolver::new(src, o.clone());
    r.seek(b"").unwrap();
    let c = r.next_cell().unwrap().unwrap();
    assert_eq!((c.ts, c.value), (30, &i64v(103)[..]));
    // Outside the range the folded version is not returned at all.
    o.time_range = Some((0, 25));
    assert_eq!(resolve(e.clone(), o), []);

    // Pushed down (the old D22 rule), the base is dropped and the sum is wrong.
    let mut pushed = ScanFilter::all();
    pushed.time_range = Some((25, 40));
    let mut o = ResolveOptions::new(3, 100);
    o.merge = Some(Arc::new(I64Add));
    let mut r = CellResolver::new(FilteredCursor::new(VecCursor::new(e), pushed), o);
    r.seek(b"").unwrap();
    assert_eq!(r.next_cell().unwrap().unwrap().value, i64v(3));
}

/// #287: a column with many versions is passed with a seek once the resolver has stepped
/// over a few of its skipped entries. Neighbouring columns whose qualifiers extend it (`a`,
/// `a\0`, `ab`), the empty qualifier and the next row are all still returned, at every
/// version limit.
#[test]
fn a_column_with_many_versions_is_skipped_by_seeking() {
    let mut e = Vec::new();
    for (row, q) in [
        (&b"r"[..], &b""[..]),
        (b"r", b"a"),
        (b"r", b"a\0"),
        (b"r", b"a\0b"),
        (b"r", b"ab"),
        (b"r\0", b"a"),
        (b"s", b"a"),
    ] {
        for ts in 1..=50u64 {
            let v = [row, b"/", q, b"/", ts.to_string().as_bytes()].concat();
            e.push((key(row, q, ts, ts, Kind::Put), stored(&v)));
        }
    }
    // A column delete under the skipped versions changes nothing above it.
    e.push((key(b"r", b"ab", 10, 60, Kind::ColumnDelete), Vec::new()));
    for versions in [1u32, 2, 8, 9, 20, 0] {
        let mut o = ResolveOptions::new(100, 100);
        o.versions = versions;
        let got = resolve(e.clone(), o);
        let mut want = Vec::new();
        for (row, q) in [
            (&b"r"[..], &b""[..]),
            (b"r", b"a"),
            (b"r", b"a\0"),
            (b"r", b"a\0b"),
            (b"r", b"ab"),
            (b"r\0", b"a"),
            (b"s", b"a"),
        ] {
            let floor = if q == b"ab" { 10 } else { 0 };
            let n = if versions == 0 {
                50
            } else {
                u64::from(versions)
            };
            for ts in (floor + 1..=50u64).rev().take(n as usize) {
                let v = [row, b"/", q, b"/", ts.to_string().as_bytes()].concat();
                want.push((ts, stored(&v)));
            }
        }
        assert_eq!(got, want, "versions {versions}");
    }
}

/// A resolved cell: key, timestamp and stored value.
type Resolved = (Vec<u8>, Timestamp, Vec<u8>);

/// Every cell `r` returns; `None` on a merge error.
fn cells<C: Cursor<Error = Error>>(r: &mut CellResolver<C>) -> Option<Vec<Resolved>> {
    let mut out = Vec::new();
    loop {
        match r.next_cell() {
            Ok(Some(c)) => out.push((c.key.to_vec(), c.ts, c.value.to_vec())),
            Ok(None) => return Some(out),
            Err(Error::Merge(_)) => return None,
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

#[test]
fn a_reused_resolver_reads_as_a_new_one() {
    // `MergingCursor::reuse` and `CellResolver::reuse` (#46): one set of buffers carried
    // through point gets and row reads of random histories, each read compared with a
    // resolver built by `new`. Whatever a read leaves in the buffers must not change the
    // next one.
    let mut merge: MergeBuffers<VecCursor> = MergeBuffers::default();
    let mut scratch = ResolverBuffers::default();
    for seed in 0..if cfg!(miri) { 2 } else { 24 } {
        let h = random_history(seed, commits(40));
        let snapshot = h.model.snapshot().max(1);
        let now = h.last_ts + 50;
        // Two sources take the merging cursor's two-way path (#335), three the heap.
        for (versions, n) in [(1, 2), (1, 3), (0, 2), (2, 3)] {
            for row in ROWS {
                for (point, q) in QUALS.iter().map(|q| (true, *q)).chain([(false, &b""[..])]) {
                    let sources = || vec_sources(&h.entries, n, &mut Rng::new(seed));
                    let read = |r: &mut CellResolver<MergingCursor<VecCursor>>| {
                        if point {
                            r.seek_column(row, q).unwrap();
                        } else {
                            r.set_upper_bound(Some(&row_end(row)));
                            r.seek(&row_prefix(row)).unwrap();
                        }
                        cells(r)
                    };
                    let mut fresh = CellResolver::new(
                        MergingCursor::new(sources()),
                        options(&h, snapshot, now, versions),
                    );
                    let want = read(&mut fresh);
                    let mut reused = CellResolver::reuse(
                        MergingCursor::reuse(sources(), merge),
                        options(&h, snapshot, now, versions),
                        scratch,
                    );
                    let got = read(&mut reused);
                    assert_eq!(
                        got, want,
                        "seed {seed}, versions {versions}, {n} sources, row {row:?}, point {point} {q:?}"
                    );
                    let (cursor, buffers) = reused.into_parts();
                    scratch = buffers;
                    merge = cursor.into_buffers();
                }
            }
        }
    }
}
