//! `CompiledFilter::check` (ICR 0030), compiled by reference or by value, decides exactly as
//! D22's rules say, on valid, marker and malformed keys. The rules are written out below
//! ([`admits`], [`next_admissible`]) on unescaped qualifiers, independently of the crate's
//! escaped-byte comparisons.

mod common;

use std::ops::Bound;

use common::{config, num, part};
use pigeonhole_format::key::{
    Kind, TERMINATOR, decode_key, encode_key, encode_marker_key, escape_into, row_prefix_len,
};
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

/// Where an (unescaped) qualifier stands relative to the selection.
#[derive(PartialEq)]
enum Place {
    Before,
    In,
    After,
}

fn place(f: &QualifierFilter, q: &[u8]) -> Place {
    let (below, above) = match f {
        QualifierFilter::All => (false, false),
        QualifierFilter::Prefix(p) => {
            if q.starts_with(p) {
                (false, false)
            } else {
                (q < p.as_slice(), q > p.as_slice())
            }
        }
        QualifierFilter::Range(lo, hi) => (
            match lo {
                Bound::Included(l) => q < l.as_slice(),
                Bound::Excluded(l) => q <= l.as_slice(),
                Bound::Unbounded => false,
            },
            match hi {
                Bound::Included(h) => q > h.as_slice(),
                Bound::Excluded(h) => q >= h.as_slice(),
                Bound::Unbounded => false,
            },
        ),
    };
    if below {
        Place::Before
    } else if above {
        Place::After
    } else {
        Place::In
    }
}

/// D22: whether to keep the entry. Markers and malformed keys pass; the qualifier
/// selection applies to every cell entry, and the time range to puts only.
fn admits(f: &ScanFilter, key: &[u8]) -> bool {
    let Ok(parts) = decode_key(key) else {
        return true;
    };
    let Some(q) = parts.qualifier else {
        return true;
    };
    let mut raw = Vec::new();
    q.unescape_into(&mut raw);
    if place(&f.qualifiers, &raw) != Place::In {
        return false;
    }
    match f.time_range {
        Some((min, max)) if parts.kind == Kind::Put => min <= parts.ts && parts.ts < max,
        _ => true,
    }
}

/// The first key at or after `key` that could be admitted within its row, appended to
/// `out`; false if none. Only the qualifier selection skips: inside an admitted column the
/// hint is `key` itself.
fn next_admissible(f: &ScanFilter, key: &[u8], out: &mut Vec<u8>) -> bool {
    let Ok(parts) = decode_key(key) else {
        out.extend_from_slice(key);
        return true;
    };
    let Some(q) = parts.qualifier else {
        out.extend_from_slice(key);
        return true;
    };
    let mut raw = Vec::new();
    q.unescape_into(&mut raw);
    match place(&f.qualifiers, &raw) {
        Place::In => {
            out.extend_from_slice(key);
            true
        }
        Place::After => false,
        Place::Before => {
            let Ok(row_len) = row_prefix_len(key) else {
                out.extend_from_slice(key);
                return true;
            };
            out.extend_from_slice(&key[..row_len]);
            match &f.qualifiers {
                QualifierFilter::Prefix(p) => escape_into(out, p),
                QualifierFilter::Range(Bound::Included(l), _) => escape_into(out, l),
                QualifierFilter::Range(Bound::Excluded(l), _) => {
                    // After every key of qualifier `l`, before any longer qualifier.
                    escape_into(out, l);
                    out.extend_from_slice(&[TERMINATOR[0], TERMINATOR[1] + 1]);
                }
                _ => out.extend_from_slice(&key[row_len..]),
            }
            true
        }
    }
}

/// What `SstIter`/`FilteredCursor` did before ICR 0030, from the two rules.
fn expected(f: &ScanFilter, key: &[u8]) -> (Verdict, Vec<u8>) {
    if f.is_all() || admits(f, key) {
        return (Verdict::Admit, Vec::new());
    }
    let mut hint = Vec::new();
    if !next_admissible(f, key, &mut hint) {
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
    fn compiled_check_follows_the_rules(f in filter(), keys in prop::collection::vec(key(), 1..8)) {
        // Compiled by reference and by value (bounds reused, escaped in place).
        for c in [f.compile(), f.clone().into_compiled()] {
            prop_assert_eq!(c.is_all(), f.is_all());
            let mut out = Vec::new();
            for k in &keys {
                let v = c.check(k, &mut out);
                prop_assert_eq!((v, out.clone()), expected(&f, k), "{:?} on {:?}", f, k);
            }
        }
    }
}
