//! `CompiledFilter::check` (ICR 0030), compiled by reference or by value, decides exactly as `ScanFilter::admits` and
//! `ScanFilter::next_admissible` do together, on valid, marker and malformed keys.

mod common;

use std::ops::Bound;

use common::{config, num, part};
use pigeonhole_format::key::{Kind, encode_key, encode_marker_key};
use pigeonhole_format::scan::{QualifierFilter, ScanFilter, Verdict};
use proptest::prelude::*;

fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
    prop_oneof![
        part().prop_map(Bound::Included),
        part().prop_map(Bound::Excluded),
        Just(Bound::Unbounded),
    ]
}

fn filter() -> impl Strategy<Value = ScanFilter> {
    let qualifiers = prop_oneof![
        Just(QualifierFilter::All),
        part().prop_map(QualifierFilter::Prefix),
        (bound(), bound()).prop_map(|(lo, hi)| QualifierFilter::Range(lo, hi)),
    ];
    let time = prop_oneof![
        Just(None),
        (num(), num()).prop_map(|(a, b)| Some((a.min(b), a.max(b)))),
    ];
    (qualifiers, time).prop_map(|(qualifiers, time_range)| {
        let mut f = ScanFilter::all();
        f.qualifiers = qualifiers;
        f.time_range = time_range;
        f
    })
}

/// A cell kind (markers come from `encode_marker_key`).
fn kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::Put),
        Just(Kind::Merge),
        Just(Kind::CellDelete),
        Just(Kind::ColumnDelete),
    ]
}

/// A valid key, a marker, a valid key with one byte changed, or random bytes.
fn key() -> impl Strategy<Value = Vec<u8>> {
    let valid = || {
        (part(), part(), num(), num(), kind()).prop_map(|(row, q, ts, seqno, kind)| {
            let mut k = Vec::new();
            encode_key(&mut k, &row, &q, ts, seqno, kind).unwrap();
            k
        })
    };
    let marker = (part(), num(), num()).prop_map(|(row, ts, seqno)| {
        let mut k = Vec::new();
        encode_marker_key(&mut k, &row, ts, seqno).unwrap();
        k
    });
    prop_oneof![
        6 => valid(),
        1 => marker,
        2 => (valid(), any::<prop::sample::Index>(), any::<u8>()).prop_map(|(mut k, i, b)| {
            let i = i.index(k.len());
            k[i] = b;
            k
        }),
        1 => prop::collection::vec(any::<u8>(), 0..40),
    ]
}

/// What `SstIter`/`FilteredCursor` did before ICR 0030, from the two `ScanFilter` calls.
fn expected(f: &ScanFilter, key: &[u8]) -> (Verdict, Vec<u8>) {
    if f.admits(key) {
        return (Verdict::Admit, Vec::new());
    }
    let mut hint = Vec::new();
    if !f.next_admissible(key, &mut hint) {
        (Verdict::SkipRow, Vec::new())
    } else if hint.as_slice() > key {
        (Verdict::Seek, hint)
    } else {
        (Verdict::Next, Vec::new())
    }
}

proptest! {
    #![proptest_config(config(20_000))]

    #[test]
    fn compiled_check_equals_admits_and_next_admissible(f in filter(), keys in prop::collection::vec(key(), 1..8)) {
        // Compiled by reference and by value (bounds reused, escaped in place).
        for c in [f.compile(), f.clone().into_compiled()] {
            prop_assert_eq!(c.is_all(), f.is_all());
            let mut out = Vec::new();
            for k in &keys {
                let v = c.check(k, &mut out);
                prop_assert_eq!((v, out.clone()), expected(&f, k), "{:?} on {:?}", f, k);
                prop_assert_eq!(c.admits(k), f.admits(k), "{:?} on {:?}", f, k);
            }
        }
    }
}
