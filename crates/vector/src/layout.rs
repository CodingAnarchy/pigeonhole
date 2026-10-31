//! The index's rows (D306 §1): their keys, and the options stored in `G`.

/// The family of a list row's vectors: one column per id.
pub(crate) const VEC: &str = "vec";
/// The family of a centroid: one column, the empty qualifier.
pub(crate) const CEN: &str = "cen";
/// Plain cells: a list's version (`C{list}:meta:v`) and the global row's fields.
pub(crate) const META: &str = "meta";
/// An id's list (`M{id}:map:l`).
pub(crate) const MAP: &str = "map";
/// A list's length, a counter (`N{list}:cnt:""`).
pub(crate) const CNT: &str = "cnt";

/// `C{list}:meta:v`, the list's version.
pub(crate) const VERSION: &[u8] = b"v";
/// `M{id}:map:l`, the id's list.
pub(crate) const LIST: &[u8] = b"l";
/// The global row.
pub(crate) const GLOBAL: &[u8] = b"G";
/// `G:meta:layout`, bumped by every split and merge.
pub(crate) const LAYOUT: &[u8] = b"layout";
/// `G:meta:next`, the next list id.
pub(crate) const NEXT: &[u8] = b"next";
/// `G:meta:opts`, the index's options.
pub(crate) const OPTS: &[u8] = b"opts";

/// A fixed bijection on `u32` (rev 1): consecutive list ids land far apart in the key
/// space, so tablet splits by size spread the lists a query reads across tablets, and a
/// parallel search fans out over them.
pub(crate) fn scramble(list: u32) -> u32 {
    let mut x = list.wrapping_mul(0x9E37_79B1);
    x ^= x >> 16;
    x.wrapping_mul(0x85EB_CA6B)
}

/// `L{scramble(list)}`: the list's vectors.
pub(crate) fn list_row(list: u32) -> [u8; 5] {
    keyed(b'L', scramble(list))
}

/// `C{list}`: the list's centroid and version.
pub(crate) fn centroid_row(list: u32) -> [u8; 5] {
    keyed(b'C', list)
}

/// `N{list}`: the list's length.
pub(crate) fn count_row(list: u32) -> [u8; 5] {
    keyed(b'N', list)
}

/// `M{id}`: the id's list.
pub(crate) fn map_row(id: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + id.len());
    k.push(b'M');
    k.extend_from_slice(id);
    k
}

fn keyed(tag: u8, n: u32) -> [u8; 5] {
    let b = n.to_be_bytes();
    [tag, b[0], b[1], b[2], b[3]]
}

/// The list id of a `C{list}` or `N{list}` row key.
pub(crate) fn list_of(key: &[u8]) -> Option<u32> {
    match key {
        [_, a, b, c, d] => Some(u32::from_be_bytes([*a, *b, *c, *d])),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scramble_is_a_bijection_that_spreads_consecutive_lists() {
        let mut seen = std::collections::HashSet::new();
        for l in 0..100_000u32 {
            assert!(seen.insert(scramble(l)), "list {l}");
        }
        // Neighbouring ids are not neighbours in the key space.
        let (a, b) = (scramble(7), scramble(8));
        assert!(a.abs_diff(b) > 1 << 20, "{a:#x} {b:#x}");
    }

    #[test]
    fn keys_round_trip() {
        assert_eq!(list_of(&centroid_row(0xDEAD_BEEF)), Some(0xDEAD_BEEF));
        assert_eq!(list_of(&count_row(3)), Some(3));
        assert_eq!(list_of(b"C12"), None);
        assert_eq!(map_row(b"id"), b"Mid");
    }
}
