//! Encoded key order equals logical order (FORMAT §2), and the key helpers agree with it.

mod common;

use common::{Cell, cell, config, num, part, sized};
use pigeonhole_format::key::{
    Kind, column_prefix_len, common_prefix_len, compare, decode_key, decode_key_in_row,
    encode_column_prefix, encode_key, encode_marker_prefix, encode_row_prefix, encode_seek_key,
    row_prefix_len, split_suffix,
};
use proptest::collection::vec;
use proptest::prelude::*;

proptest! {
    #![proptest_config(config(2000))]

    /// Byte order of encoded keys equals the logical order, pairwise.
    #[test]
    fn byte_order_is_logical_order(cells in vec(cell(), 2..sized(24, 4))) {
        let encoded: Vec<Vec<u8>> = cells.iter().map(Cell::encode).collect();
        for (a, ea) in cells.iter().zip(&encoded) {
            for (b, eb) in cells.iter().zip(&encoded) {
                prop_assert_eq!(ea.cmp(eb), a.logical().cmp(&b.logical()), "{:?} vs {:?}", a, b);
                prop_assert_eq!(compare(ea, eb), ea.cmp(eb), "{:?} vs {:?}", a, b);
            }
        }
    }

    /// A row that is a strict prefix of another sorts first, whatever follows.
    #[test]
    fn prefix_rows_sort_first(
        row in part(),
        ext in vec(prop_oneof![Just(0u8), Just(1), Just(0xFF), any::<u8>()], 1..4),
        q1 in part(), q2 in part(), ts1 in num(), ts2 in num(),
    ) {
        let long = [row.as_slice(), &ext].concat();
        let a = Cell { row, qual: q1, ts: ts1, seqno: 1, kind: Kind::Put }.encode();
        let b = Cell { row: long, qual: q2, ts: ts2, seqno: 1, kind: Kind::Put }.encode();
        prop_assert!(a < b);
    }

    /// Decoding recovers every field; the prefix helpers agree with the encoders.
    #[test]
    fn decode_and_prefixes(c in cell()) {
        let key = c.encode();
        let parts = decode_key(&key).unwrap();
        prop_assert!(parts.row.eq_raw(&c.row));
        let mut row = Vec::new();
        parts.row.unescape_into(&mut row);
        prop_assert_eq!(&row, &c.row);
        prop_assert_eq!((parts.ts, parts.seqno, parts.kind), (c.ts, c.seqno, c.kind));
        match parts.qualifier {
            None => prop_assert_eq!(c.kind, Kind::FamilyDelete),
            Some(q) => prop_assert!(q.eq_raw(&c.qual)),
        }
        let (_, ts, seqno, kind) = split_suffix(&key).unwrap();
        prop_assert_eq!((ts, seqno, kind), (c.ts, c.seqno, c.kind));

        let mut rp = Vec::new();
        encode_row_prefix(&mut rp, &c.row).unwrap();
        prop_assert_eq!(row_prefix_len(&key).unwrap(), rp.len());
        prop_assert!(key.starts_with(&rp));

        let mut cp = Vec::new();
        if c.kind == Kind::FamilyDelete {
            encode_marker_prefix(&mut cp, &c.row).unwrap();
        } else {
            encode_column_prefix(&mut cp, &c.row, &c.qual).unwrap();
        }
        prop_assert_eq!(column_prefix_len(&key).unwrap(), cp.len());
        prop_assert!(key.starts_with(&cp));
    }

    /// Two keys share a row prefix iff they have the same row, and a column prefix iff they
    /// address the same column.
    #[test]
    fn prefixes_identify_rows_and_columns(a in cell(), b in cell()) {
        let (ka, kb) = (a.encode(), b.encode());
        let ra = &ka[..row_prefix_len(&ka).unwrap()];
        let rb = &kb[..row_prefix_len(&kb).unwrap()];
        prop_assert_eq!(ra == rb, a.row == b.row);
        let ca = &ka[..column_prefix_len(&ka).unwrap()];
        let cb = &kb[..column_prefix_len(&kb).unwrap()];
        let col = |c: &Cell| (c.row.clone(), (c.kind != Kind::FamilyDelete).then(|| c.qual.clone()));
        prop_assert_eq!(ca == cb, col(&a) == col(&b));
    }

    /// A seek key sorts after exactly the versions newer than (ts, seqno) and before the rest.
    #[test]
    fn seek_key_bounds(row in part(), qual in part(), t in num(), s in num(), ts in num(), seqno in num(), kind in prop_oneof![Just(Kind::Put), Just(Kind::Merge), Just(Kind::CellDelete), Just(Kind::ColumnDelete)]) {
        let mut seek = Vec::new();
        encode_seek_key(&mut seek, &row, &qual, t, s).unwrap();
        let mut key = Vec::new();
        encode_key(&mut key, &row, &qual, ts, seqno, kind).unwrap();
        let visible = ts < t || (ts == t && seqno <= s);
        prop_assert_eq!(seek < key, visible);
    }
}

#[test]
#[cfg_attr(miri, ignore = "64 KiB keys are too slow under Miri")]
fn limits_and_kinds() {
    let big = vec![0u8; pigeonhole_format::key::MAX_KEY_PART + 1];
    let ok = vec![0u8; pigeonhole_format::key::MAX_KEY_PART];
    let mut out = Vec::new();
    assert!(encode_key(&mut out, &ok, &ok, 0, 0, Kind::Put).is_ok());
    assert!(decode_key(&out).is_ok());
    assert_eq!(
        encode_key(&mut Vec::new(), &big, b"", 0, 0, Kind::Put),
        Err(pigeonhole_format::Error::KeyTooLarge)
    );
    assert_eq!(
        encode_key(&mut Vec::new(), b"", &big, 0, 0, Kind::Put),
        Err(pigeonhole_format::Error::KeyTooLarge)
    );
    assert!(encode_key(&mut Vec::new(), b"r", b"q", 0, 0, Kind::FamilyDelete).is_err());
    // A seek key carries kind 0x00, which is not a real kind.
    let mut seek = Vec::new();
    encode_seek_key(&mut seek, b"r", b"q", 5, 5).unwrap();
    assert!(decode_key(&seek).is_err());
    for b in 0..=255u8 {
        assert_eq!(Kind::from_u8(b).is_ok(), (1..=5).contains(&b));
    }
}

/// Two byte strings from a small alphabet that often share a long prefix (as two keys of one
/// column do) and differ in length.
fn shared_prefix_pair() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
    (vec(0u8..3, 0..40), vec(0u8..3, 0..20), vec(0u8..3, 0..20)).prop_map(|(shared, a, b)| {
        let mut x = shared.clone();
        x.extend(a);
        let mut y = shared;
        y.extend(b);
        (x, y)
    })
}

proptest! {
    #![proptest_config(config(5000))]

    /// `compare` is exactly `<[u8]>::cmp`, both ways round.
    #[test]
    fn compare_is_byte_order((a, b) in shared_prefix_pair()) {
        prop_assert_eq!(compare(&a, &b), a.cmp(&b));
        prop_assert_eq!(compare(&b, &a), b.cmp(&a));
        prop_assert_eq!(compare(&a, &a), std::cmp::Ordering::Equal);
    }

    /// `common_prefix_len` counts the leading bytes `a` and `b` share.
    #[test]
    fn common_prefix_len_counts_shared_bytes((a, b) in shared_prefix_pair()) {
        let naive = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
        prop_assert_eq!(common_prefix_len(&a, &b), naive);
        prop_assert_eq!(common_prefix_len(&b, &a), naive);
        prop_assert_eq!(common_prefix_len(&a, &a), a.len());
    }
}

proptest! {
    #![proptest_config(config(2000))]

    /// A key of a row already decoded decodes the same without its row scanned again, cells
    /// and family markers alike (rows and qualifiers may hold zero bytes, which escape).
    #[test]
    fn decode_key_in_row_matches_decode_key(
        row in vec(0u8..3, 0..12),
        first in vec(0u8..3, 0..8),
        second in prop::option::of(vec(0u8..3, 0..8)),
        ts in any::<u64>(),
        seqno in 0u64..1 << 40,
    ) {
        let mut a = Vec::new();
        encode_key(&mut a, &row, &first, ts, seqno, Kind::Put).unwrap();
        let row_len = decode_key(&a).unwrap().row.as_escaped().len();
        let mut b = Vec::new();
        match &second {
            Some(q) => encode_key(&mut b, &row, q, ts, seqno, Kind::Put).unwrap(),
            None => pigeonhole_format::key::encode_marker_key(&mut b, &row, ts, seqno).unwrap(),
        }
        prop_assert_eq!(decode_key_in_row(&b, row_len).unwrap(), decode_key(&b).unwrap());
    }
}
