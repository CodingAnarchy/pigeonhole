//! The index table's rows and values (D309 §2).

use crate::error::{Error, Result};

/// Posting lists: `P{term}`.
pub(crate) const POST_PREFIX: &[u8] = b"P";
/// Forward rows: `F{doc}`.
pub(crate) const FWD_PREFIX: &[u8] = b"F";
/// The corpus statistics.
pub(crate) const STATS_ROW: &[u8] = b"S";
/// The layout, options, source and tokeniser.
pub(crate) const GLOBAL: &[u8] = b"G";

/// `P{t}`: one column per document, the qualifier its id, the value [`posting`].
pub(crate) const POST: &str = "post";
/// `P{t}`: one column per document, its positions as ascending LEB128 deltas.
pub(crate) const POS: &str = "pos";
/// `P{t}`: a counter, the term's document frequency, at [`N`].
pub(crate) const DF: &str = "df";
/// `P{t}`: a `max` aggregate, the largest term frequency any posting had (MaxScore, M1).
pub(crate) const MAXTF: &str = "maxtf";
/// `P{t}`: a `min` aggregate, the smallest document length any posting had (MaxScore, M1).
pub(crate) const MINLEN: &str = "minlen";
/// `F{doc}`: one column per distinct term, the value its frequency (`u64`).
pub(crate) const TERMS: &str = "terms";
/// `F{doc}` and `G`: plain columns.
pub(crate) const META: &str = "meta";
/// `S`: counters, [`DOCS`] and [`LEN`].
pub(crate) const STATS: &str = "stats";

pub(crate) const N: &[u8] = b"n";
pub(crate) const BOUND: &[u8] = b"";
pub(crate) const DOCS: &[u8] = b"docs";
pub(crate) const LEN: &[u8] = b"len";
/// `F{doc}`'s marker, written for every indexed document (an empty one included).
pub(crate) const PRESENT: &[u8] = b"present";
/// `F{doc}`'s length in terms (`u64`).
pub(crate) const DOC_LEN: &[u8] = b"len";
/// `G`'s columns.
pub(crate) const LAYOUT: &[u8] = b"layout";
pub(crate) const OPTS: &[u8] = b"opts";
pub(crate) const SOURCE: &[u8] = b"source";
pub(crate) const TOKENIZER: &[u8] = b"tokenizer";
pub(crate) const FINGERPRINT: &[u8] = b"fingerprint";
/// The normaliser's Unicode version, for diagnostics (D309 rev 6 §A.9).
pub(crate) const NORMALIZER_UNICODE: &[u8] = b"normalizer-unicode";
/// Present while [`Index::repair`](crate::Index::repair) runs.
pub(crate) const REPAIR: &[u8] = b"repair";

pub(crate) const LAYOUT_VERSION: u64 = 1;

pub(crate) fn post_row(term: &str) -> Vec<u8> {
    [POST_PREFIX, term.as_bytes()].concat()
}

pub(crate) fn fwd_row(doc: &[u8]) -> Vec<u8> {
    [FWD_PREFIX, doc].concat()
}

/// A posting's value: the term frequency (high 32 bits) and the document's length (low).
pub(crate) fn posting(tf: u32, len: u32) -> u64 {
    (u64::from(tf) << 32) | u64::from(len)
}

pub(crate) fn unposting(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

/// Ascending positions as LEB128 deltas.
pub(crate) fn encode_positions(positions: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(positions.len() + 4);
    let mut last = 0u32;
    for &p in positions {
        let mut d = p - last;
        last = p;
        loop {
            let byte = (d & 0x7f) as u8;
            d >>= 7;
            if d == 0 {
                out.push(byte);
                break;
            }
            out.push(byte | 0x80);
        }
    }
    out
}

pub(crate) fn decode_positions(bytes: &[u8], out: &mut Vec<u32>) -> Result<()> {
    out.clear();
    let (mut last, mut d, mut shift) = (0u32, 0u32, 0u32);
    for &b in bytes {
        if shift >= 32 {
            return Err(Error::Corruption("a position delta is too long".into()));
        }
        d |= u32::from(b & 0x7f) << shift;
        if b & 0x80 != 0 {
            shift += 7;
            continue;
        }
        last = last
            .checked_add(d)
            .ok_or_else(|| Error::Corruption("a position overflows".into()))?;
        out.push(last);
        (d, shift) = (0, 0);
    }
    if shift != 0 {
        return Err(Error::Corruption("a position list is cut short".into()));
    }
    Ok(())
}

/// The Unicode normalisation an index applies to every term (D309 rev 6, #1291).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Normalization {
    /// None: terms are compared as their code points are written.
    None,
    /// NFC (canonical composition): a precomposed `é` and `e` plus a combining accent are one
    /// term. The default.
    #[default]
    Nfc,
    /// NFKC (compatibility composition): NFC, and compatibility forms fold too (the `ﬁ`
    /// ligature to `fi`, full-width letters to ASCII).
    Nfkc,
}

impl Normalization {
    fn code(self) -> u8 {
        match self {
            Normalization::None => 0,
            Normalization::Nfc => 1,
            Normalization::Nfkc => 2,
        }
    }

    fn of(code: u8) -> Option<Self> {
        Some(match code {
            0 => Normalization::None,
            1 => Normalization::Nfc,
            2 => Normalization::Nfkc,
            _ => return None,
        })
    }
}

/// An index's options, fixed at create and stored in `G` (D309 §6).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IndexOptions {
    k1: f64,
    b: f64,
    stop_words: bool,
    max_term_bytes: u32,
    max_terms: u32,
    positions: bool,
    normalization: Normalization,
}

impl Default for IndexOptions {
    fn default() -> Self {
        IndexOptions {
            k1: 1.2,
            b: 0.75,
            stop_words: false,
            max_term_bytes: 64,
            max_terms: 10_000,
            positions: true,
            normalization: Normalization::Nfc,
        }
    }
}

impl IndexOptions {
    /// The defaults: `k1` 1.2, `b` 0.75, stop words off, 64-byte terms, 10,000 distinct
    /// terms per document, positions on, NFC normalisation.
    pub fn new() -> Self {
        Self::default()
    }

    /// BM25's `k1` (term-frequency saturation): finite and at least 0. Default 1.2.
    pub fn k1(mut self, k1: f64) -> Self {
        self.k1 = k1;
        self
    }

    /// BM25's `b` (length normalisation): between 0 and 1. Default 0.75.
    pub fn b(mut self, b: f64) -> Self {
        self.b = b;
        self
    }

    /// Whether the built-in tokeniser drops English stop words (a dropped word keeps its
    /// position). Default off. A custom tokeniser ignores it.
    pub fn stop_words(mut self, on: bool) -> Self {
        self.stop_words = on;
        self
    }

    /// The longest term kept, in bytes: a longer token is dropped and not counted in the
    /// document's length. Between 1 and 255; default 64.
    pub fn max_term_bytes(mut self, n: u32) -> Self {
        self.max_term_bytes = n;
        self
    }

    /// The most distinct terms one document may have: a larger document is refused
    /// (`InvalidArgument`) before anything is written. At least 1; default 10,000.
    pub fn max_terms(mut self, n: u32) -> Self {
        self.max_terms = n;
        self
    }

    /// Whether positions are stored, which phrase queries need. Default on. An index
    /// without them refuses a phrase query (`InvalidArgument`).
    ///
    /// Positions cost about as much again as the postings: on short documents (about 40
    /// words), about 40% more stored bytes, and a write about 2.5 times as long. Turn them off
    /// for a write-heavy index that doesn't need phrases.
    pub fn positions(mut self, on: bool) -> Self {
        self.positions = on;
        self
    }

    /// The Unicode normalisation of every term, after any tokeniser and before the term cap
    /// (so the cap counts normalised bytes). Default [`Normalization::Nfc`].
    pub fn normalization(mut self, n: Normalization) -> Self {
        self.normalization = n;
        self
    }

    /// The Unicode normalisation of every term.
    pub fn get_normalization(&self) -> Normalization {
        self.normalization
    }

    /// BM25's `k1`.
    pub fn get_k1(&self) -> f64 {
        self.k1
    }

    /// BM25's `b`.
    pub fn get_b(&self) -> f64 {
        self.b
    }

    /// Whether the built-in tokeniser drops stop words.
    pub fn get_stop_words(&self) -> bool {
        self.stop_words
    }

    /// The longest term kept, in bytes.
    pub fn get_max_term_bytes(&self) -> u32 {
        self.max_term_bytes
    }

    /// The most distinct terms one document may have.
    pub fn get_max_terms(&self) -> u32 {
        self.max_terms
    }

    /// Whether positions are stored.
    pub fn get_positions(&self) -> bool {
        self.positions
    }

    pub(crate) fn checked(self) -> Result<Self> {
        let bad = |m: &str| Err(Error::InvalidArgument(m.to_owned()));
        if !(self.k1.is_finite() && self.k1 >= 0.0) {
            return bad("k1 must be finite and at least 0");
        }
        if !(0.0..=1.0).contains(&self.b) {
            return bad("b must be between 0 and 1");
        }
        if !(1..=255).contains(&self.max_term_bytes) {
            return bad("max_term_bytes must be between 1 and 255");
        }
        if self.max_terms == 0 {
            return bad("max_terms must be at least 1");
        }
        Ok(self)
    }

    /// Version 1: `1`, `k1` and `b` (`f64` LE), stop words, `max_term_bytes` and
    /// `max_terms` (`u32` LE), positions, normalisation (0 none, 1 NFC, 2 NFKC).
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut v = vec![1u8];
        v.extend_from_slice(&self.k1.to_le_bytes());
        v.extend_from_slice(&self.b.to_le_bytes());
        v.push(u8::from(self.stop_words));
        v.extend_from_slice(&self.max_term_bytes.to_le_bytes());
        v.extend_from_slice(&self.max_terms.to_le_bytes());
        v.push(u8::from(self.positions));
        v.push(self.normalization.code());
        v
    }

    pub(crate) fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != 28 || b[0] != 1 {
            return None;
        }
        let f = |at: usize| f64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"));
        let u = |at: usize| u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"));
        IndexOptions {
            k1: f(1),
            b: f(9),
            stop_words: b[17] != 0,
            max_term_bytes: u(18),
            max_terms: u(22),
            positions: b[26] != 0,
            normalization: Normalization::of(b[27])?,
        }
        .checked()
        .ok()
    }
}

/// The column an index is built over: a table, family and qualifier (D309 §1, §6).
///
/// Each row of the table holding that column is one document, with the row key as its id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub(crate) table: String,
    pub(crate) family: String,
    pub(crate) qualifier: Vec<u8>,
}

impl Source {
    /// The column `family:qualifier` of `table`.
    pub fn new(table: &pigeonhole::Table, family: &str, qualifier: &[u8]) -> Self {
        Source {
            table: table.name().to_owned(),
            family: family.to_owned(),
            qualifier: qualifier.to_vec(),
        }
    }

    /// The source table's name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The source family.
    pub fn family(&self) -> &str {
        &self.family
    }

    /// The source qualifier.
    pub fn qualifier(&self) -> &[u8] {
        &self.qualifier
    }

    /// Lengths (`u32` LE) then bytes of the table, the family and the qualifier.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut v = Vec::new();
        for part in [
            self.table.as_bytes(),
            self.family.as_bytes(),
            &self.qualifier[..],
        ] {
            v.extend_from_slice(&(part.len() as u32).to_le_bytes());
            v.extend_from_slice(part);
        }
        v
    }

    pub(crate) fn decode(mut b: &[u8]) -> Option<Self> {
        let mut parts = Vec::with_capacity(3);
        for _ in 0..3 {
            let n = u32::from_le_bytes(b.get(..4)?.try_into().ok()?) as usize;
            parts.push(b.get(4..4 + n)?.to_vec());
            b = &b[4 + n..];
        }
        if !b.is_empty() {
            return None;
        }
        let qualifier = parts.pop()?;
        let family = String::from_utf8(parts.pop()?).ok()?;
        let table = String::from_utf8(parts.pop()?).ok()?;
        Some(Source {
            table,
            family,
            qualifier,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_round_trip() {
        let ps = [0, 1, 127, 128, 300, 70_000, u32::MAX];
        let mut out = Vec::new();
        decode_positions(&encode_positions(&ps), &mut out).unwrap();
        assert_eq!(out, ps);
        assert!(decode_positions(&[0x80], &mut out).is_err());
    }

    #[test]
    fn options_round_trip_and_check() {
        let o = IndexOptions::new()
            .k1(0.9)
            .b(0.4)
            .stop_words(true)
            .positions(false)
            .normalization(Normalization::Nfkc);
        assert_eq!(IndexOptions::decode(&o.encode()), Some(o));
        assert!(IndexOptions::new().b(1.5).checked().is_err());
        assert!(IndexOptions::new().k1(f64::NAN).checked().is_err());
        assert!(IndexOptions::new().max_term_bytes(0).checked().is_err());
        assert!(IndexOptions::new().max_terms(0).checked().is_err());
    }

    #[test]
    fn postings_pack_both_halves() {
        assert_eq!(unposting(posting(7, 123_456)), (7, 123_456));
    }
}
