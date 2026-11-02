//! The store's layout (D311 §2, ICR 0107): row keys, families, the `MemoryId` forms, the
//! dedup normalisation and hash.

use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

/// The memory rows' tag after the namespace: `{ns}\0m{rid}`.
pub(crate) const MEMORY_TAG: &[u8] = b"\0m";
/// The dedup rows' tag: `{ns}\0h{hash}`.
pub(crate) const DEDUP_TAG: &[u8] = b"\0h";
/// The store's own row: the layout version, the embedding model, the text index's name.
pub(crate) const GLOBAL: &[u8] = b"G";

/// `content:text`, the text index's source column.
pub(crate) const CONTENT: &str = "content";
pub(crate) const TEXT: &[u8] = b"text";
/// `emb:v`, the embedding.
pub(crate) const EMB: &str = "emb";
pub(crate) const V: &[u8] = b"v";
/// `meta`: kind, session, pinned, consolidated, superseded_by, valid_to; and `rid` in a
/// dedup row; and `G`'s columns.
pub(crate) const META: &str = "meta";
pub(crate) const KIND: &[u8] = b"kind";
pub(crate) const SESSION: &[u8] = b"session";
pub(crate) const PINNED: &[u8] = b"pinned";
pub(crate) const SUPERSEDED_BY: &[u8] = b"superseded_by";
pub(crate) const VALID_TO: &[u8] = b"valid_to";
pub(crate) const RID: &[u8] = b"rid";
/// `sources`: one column per provenance entry, the value its first-seen time (µs).
pub(crate) const SOURCES: &str = "sources";
/// `stats` (a counter): `uses`.
pub(crate) const STATS: &str = "stats";
pub(crate) const USES: &[u8] = b"uses";
/// `peak` (a `max` aggregate): `importance` (parts per million) and `last_used` (µs).
pub(crate) const PEAK: &str = "peak";
pub(crate) const IMPORTANCE: &[u8] = b"importance";
pub(crate) const LAST_USED: &[u8] = b"last_used";
/// `links` and `backlinks`: one column per link, the qualifier the other end's row key, the
/// value `(type u8, weight f32 LE)`.
pub(crate) const LINKS: &str = "links";
pub(crate) const BACKLINKS: &str = "backlinks";

/// `G`'s columns.
pub(crate) const LAYOUT: &[u8] = b"layout";
pub(crate) const MODEL_ID: &[u8] = b"model_id";
pub(crate) const MODEL_DIM: &[u8] = b"model_dim";
pub(crate) const MODEL_ELEMENT: &[u8] = b"model_element";
pub(crate) const TEXT_INDEX: &[u8] = b"text_index";
/// `G`'s forget policy (T2), when the store has one.
pub(crate) const FORGET: &[u8] = b"forget";

pub(crate) const LAYOUT_VERSION: u64 = 1;

/// Importance is stored in parts per million of 0–1, so the `max` aggregate is exact.
pub(crate) const PPM: f64 = 1_000_000.0;

/// A namespace: non-empty, without `\0` (D311 §5, exact and flat).
pub(crate) fn check_namespace(ns: &str) -> Result<()> {
    if ns.is_empty() {
        return Err(Error::InvalidArgument("a namespace can't be empty".into()));
    }
    if ns.as_bytes().contains(&0) {
        return Err(Error::InvalidArgument(format!(
            "a namespace can't contain \\0: {ns:?}"
        )));
    }
    if ns.len() > u16::MAX as usize {
        return Err(Error::InvalidArgument(format!(
            "a namespace is at most {} bytes, and this one is {}",
            u16::MAX,
            ns.len()
        )));
    }
    Ok(())
}

/// A memory's row key: `{ns}\0m{rid}`.
pub(crate) fn memory_row(ns: &str, rid: &[u8; 16]) -> Vec<u8> {
    [ns.as_bytes(), MEMORY_TAG, rid].concat()
}

/// A dedup row's key: `{ns}\0h{hash}`.
pub(crate) fn dedup_row(ns: &str, hash: &[u8; 16]) -> Vec<u8> {
    [ns.as_bytes(), DEDUP_TAG, hash].concat()
}

/// A time-ordered `rid` (D311 §2): `(u64::MAX − created_µs)` big-endian, then 8 random
/// bytes. A forward scan of a namespace's memory rows is newest first.
pub(crate) fn new_rid(created_us: u64) -> Result<[u8; 16]> {
    let mut rid = [0u8; 16];
    rid[..8].copy_from_slice(&(u64::MAX - created_us).to_be_bytes());
    getrandom::fill(&mut rid[8..])
        .map_err(|e| Error::InvalidArgument(format!("no randomness for a memory id: {e}")))?;
    Ok(rid)
}

/// The wall clock, in µs since the Unix epoch.
pub(crate) fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

/// Dedup's normalisation (D311 §4, cli692's 7, frozen in ICR 0107): trim, then collapse
/// every run of Unicode whitespace to one space. No case folding, no Unicode normalisation.
pub fn normalize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for word in text.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    out
}

/// `xxh3_128` of the normalised UTF-8, big-endian.
pub(crate) fn content_hash(normalized: &str) -> [u8; 16] {
    twox_hash::XxHash3_128::oneshot(normalized.as_bytes()).to_be_bytes()
}

/// A memory's id: its namespace and its `rid` (D311 §2, frozen in ICR 0107).
///
/// - **The byte form** (`to_bytes`/`from_bytes`): the namespace's length (`u16`, little
///   endian), its bytes, then the 16-byte `rid`.
/// - **The text form** (`Display`/`FromStr`): RFC 4648 base32 of the byte form, lowercase,
///   without padding. That's what MCP tools and Python pass.
///
/// Both round-trip, and both refuse anything malformed (`InvalidArgument`).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemoryId {
    ns: String,
    rid: [u8; 16],
}

impl MemoryId {
    pub(crate) fn new(ns: &str, rid: [u8; 16]) -> Self {
        MemoryId {
            ns: ns.to_owned(),
            rid,
        }
    }

    /// The memory's id from its row key, if the key is a memory row's.
    pub(crate) fn from_row(key: &[u8]) -> Option<Self> {
        let at = key.windows(2).position(|w| w == MEMORY_TAG)?;
        let rid: [u8; 16] = key.get(at + 2..)?.try_into().ok()?;
        let ns = std::str::from_utf8(&key[..at]).ok()?;
        Some(MemoryId::new(ns, rid))
    }

    /// The namespace.
    pub fn namespace(&self) -> &str {
        &self.ns
    }

    /// When the memory was created, in µs since the Unix epoch: the `rid`'s time.
    pub fn created_us(&self) -> u64 {
        let t = u64::from_be_bytes(self.rid[..8].try_into().expect("8 bytes"));
        u64::MAX - t
    }

    /// The memory's row key.
    pub(crate) fn row(&self) -> Vec<u8> {
        memory_row(&self.ns, &self.rid)
    }

    /// The byte form.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + self.ns.len() + 16);
        out.extend_from_slice(&(self.ns.len() as u16).to_le_bytes());
        out.extend_from_slice(self.ns.as_bytes());
        out.extend_from_slice(&self.rid);
        out
    }

    /// The id the byte form names; anything else is `InvalidArgument`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bad = || Error::InvalidArgument("not a memory id".into());
        let len = u16::from_le_bytes(bytes.get(..2).ok_or_else(bad)?.try_into().expect("2"));
        let len = len as usize;
        if bytes.len() != 2 + len + 16 {
            return Err(bad());
        }
        let ns = std::str::from_utf8(&bytes[2..2 + len]).map_err(|_| bad())?;
        check_namespace(ns).map_err(|_| bad())?;
        let rid: [u8; 16] = bytes[2 + len..].try_into().expect("16");
        Ok(MemoryId::new(ns, rid))
    }
}

impl fmt::Display for MemoryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&base32_encode(&self.to_bytes()))
    }
}

impl fmt::Debug for MemoryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryId({:?}, {})", self.ns, self)
    }
}

impl FromStr for MemoryId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let bytes =
            base32_decode(s).ok_or_else(|| Error::InvalidArgument("not a memory id".into()))?;
        MemoryId::from_bytes(&bytes)
    }
}

const BASE32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// RFC 4648 base32, lowercase, without padding.
fn base32_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for &b in bytes {
        acc = (acc << 8) | u32::from(b);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(BASE32[((acc >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(BASE32[((acc << (5 - bits)) & 31) as usize] as char);
    }
    out
}

/// The bytes `base32_encode` wrote: lowercase, no padding, and zero trailing bits.
fn base32_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes() {
        let v = BASE32.iter().position(|&x| x == c)? as u32;
        acc = (acc << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    // A canonical encoding leaves fewer than 5 bits, all zero.
    if bits >= 5 || acc & ((1 << bits) - 1) != 0 {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base32_matches_rfc_4648_vectors_lowercase_unpadded() {
        for (raw, enc) in [
            ("", ""),
            ("f", "my"),
            ("fo", "mzxq"),
            ("foo", "mzxw6"),
            ("foob", "mzxw6yq"),
            ("fooba", "mzxw6ytb"),
            ("foobar", "mzxw6ytboi"),
        ] {
            assert_eq!(base32_encode(raw.as_bytes()), enc);
            assert_eq!(base32_decode(enc).unwrap(), raw.as_bytes());
        }
        // Not canonical: uppercase, padding, a stray bit, an impossible length.
        for bad in ["MY", "my==", "mz", "m"] {
            assert_eq!(base32_decode(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_memory_id_round_trips_and_refuses_the_malformed() {
        let id = MemoryId::new("agent:alice", new_rid(1_700_000_000_000_000).unwrap());
        assert_eq!(MemoryId::from_bytes(&id.to_bytes()).unwrap(), id);
        assert_eq!(id.to_string().parse::<MemoryId>().unwrap(), id);
        assert_eq!(id.created_us(), 1_700_000_000_000_000);
        assert_eq!(MemoryId::from_row(&id.row()).unwrap(), id);
        let b = id.to_bytes();
        for bad in [&b[..b.len() - 1], &b[..1], &[0, 0][..]] {
            assert!(MemoryId::from_bytes(bad).is_err());
        }
        assert!("not!base32".parse::<MemoryId>().is_err());
        // An empty namespace isn't an id.
        let mut empty = vec![0, 0];
        empty.extend_from_slice(&[7; 16]);
        assert!(MemoryId::from_bytes(&empty).is_err());
    }

    #[test]
    fn newer_rids_sort_first() {
        let old = new_rid(1_000).unwrap();
        let new = new_rid(2_000).unwrap();
        assert!(new < old);
    }

    #[test]
    fn normalisation_trims_and_collapses_whitespace_only() {
        assert_eq!(normalize("  a \t b\n\nc  "), "a b c");
        assert_eq!(normalize("Café\u{00a0}au  lait"), "Café au lait");
        assert_ne!(normalize("A"), normalize("a"));
        assert_eq!(normalize(" \n "), "");
        assert_eq!(content_hash("a b"), content_hash(&normalize(" a  b ")));
    }

    #[test]
    fn namespaces_refuse_empty_and_nul() {
        assert!(check_namespace("").is_err());
        assert!(check_namespace("a\0b").is_err());
        assert!(check_namespace("agent:alice/x").is_ok());
    }
}
