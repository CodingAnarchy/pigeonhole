//! The default `Cursor::skip_row` (for cursors with no row-start table, a memtable's among
//! them): it passes the rest of the current row and stops on the next row's first key, for row
//! prefixes that fit its stack copy and ones that don't.

use pigeonhole_format::Cursor;
use pigeonhole_format::key::{Kind, encode_key};

/// Sorted keys over a `Vec`, with no `skip_row` of its own.
struct Keys {
    keys: Vec<Vec<u8>>,
    at: usize,
}

impl Cursor for Keys {
    type Error = std::convert::Infallible;

    fn valid(&self) -> bool {
        self.at < self.keys.len()
    }

    fn key(&self) -> &[u8] {
        &self.keys[self.at]
    }

    fn value(&self) -> &[u8] {
        &[]
    }

    fn seek_to_first(&mut self) -> Result<(), Self::Error> {
        self.at = 0;
        Ok(())
    }

    fn seek(&mut self, target: &[u8]) -> Result<(), Self::Error> {
        self.at = self.keys.partition_point(|k| k.as_slice() < target);
        Ok(())
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        self.at += 1;
        Ok(())
    }
}

fn key(row: &[u8], qualifier: &[u8], ts: u64) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(&mut k, row, qualifier, ts, 1, Kind::Put).unwrap();
    k
}

#[test]
fn the_default_skip_row_stops_on_the_next_rows_first_key_for_short_and_long_rows() {
    // A short row (inline copy), a row whose prefix is past the 64-byte stack copy (heap), a
    // row that extends the long one (same leading bytes, a different row), and a last row.
    let long = vec![b'L'; 100];
    let mut longer = long.clone();
    longer.push(b'x');
    let rows: [&[u8]; 4] = [b"a", &long, &longer, b"z"];
    let mut keys = Vec::new();
    for row in rows {
        for q in [&b"q1"[..], b"q2", b"q3"] {
            keys.push(key(row, q, 5));
        }
    }
    keys.sort();
    let mut c = Keys { keys, at: 0 };
    let first_of = |row: &[u8]| key(row, b"q1", 5);
    c.seek_to_first().unwrap();
    // Sorted: the long row, the longer one, then `a` and `z` (`L` sorts before `a`).
    for next in [&longer[..], b"a", b"z"] {
        c.skip_row().unwrap();
        assert_eq!(
            c.key(),
            first_of(next).as_slice(),
            "skip to {:?}",
            &next[..next.len().min(4)]
        );
    }
    // From the middle of a row too.
    c.seek(&key(&long, b"q2", 5)).unwrap();
    c.skip_row().unwrap();
    assert_eq!(c.key(), first_of(&longer).as_slice());
    // The last row: past the end.
    c.seek(&key(b"z", b"q2", 5)).unwrap();
    c.skip_row().unwrap();
    assert!(!c.valid());
}
