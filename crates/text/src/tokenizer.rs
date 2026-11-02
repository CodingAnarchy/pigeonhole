//! Tokenisers (D309 §5, §6b): the [`Tokenizer`] trait and the built-in one.

use std::ops::Range;

use unicode_segmentation::UnicodeSegmentation;

/// One token of a text, as a [`Tokenizer`] emits it.
///
/// Built with [`Token::new`], so it can gain fields later.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Token<'a> {
    /// The term as indexed, which may differ from the source text (lowercased, stemmed).
    pub text: &'a str,
    /// The token's position. Positions never decrease. A gap is a dropped token (a stop
    /// word), and two tokens may share a position (synonyms).
    pub position: u32,
    /// The token's byte range in the **source text**, for highlighting. Two tokens at one
    /// position may share a range.
    pub range: Range<usize>,
}

impl<'a> Token<'a> {
    /// A token of `text` at `position`, from `range` of the source text.
    pub fn new(text: &'a str, position: u32, range: Range<usize>) -> Self {
        Token {
            text,
            position,
            range,
        }
    }
}

/// Splits text into terms, for documents and queries alike.
///
/// An index stores its tokeniser's [`name`](Tokenizer::name) and
/// [`fingerprint`](Tokenizer::fingerprint), and refuses to open with another. A tokeniser
/// must be deterministic: the same text always gives the same tokens, or the index's
/// [`verify`](crate::Index::verify) reports mismatches.
pub trait Tokenizer: Send + Sync {
    /// A stable name for the tokeniser.
    fn name(&self) -> &str;
    /// A stable hash of everything that changes its output (its options and version).
    fn fingerprint(&self) -> u64;
    /// Calls `out` with each token of `text`, in position order. A dropped word (a stop
    /// word, say) is never emitted, but advances the next token's position.
    fn tokens(&self, text: &str, out: &mut dyn FnMut(Token<'_>));
}

/// Lucene's English stop words (its `EnglishAnalyzer` default set).
const STOP_WORDS: [&str; 33] = [
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

/// The built-in tokeniser: Unicode word boundaries (UAX #29), then lowercasing.
/// Optionally it drops English stop words. A dropped word keeps its position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Standard {
    pub(crate) stop_words: bool,
}

impl Standard {
    pub(crate) const NAME: &'static str = "standard";
}

impl Tokenizer for Standard {
    fn name(&self) -> &str {
        Self::NAME
    }

    /// Covers the options and the word-boundary rules' Unicode version, since UAX #29
    /// changes between Unicode versions (cli692 on D309 rev 6): a dependency bump that
    /// moves it makes `open` refuse with `Mismatch`, and the index needs a `repair`.
    fn fingerprint(&self) -> u64 {
        let (major, minor, update) = unicode_segmentation::UNICODE_VERSION;
        let stop = if self.stop_words { " stop-en33" } else { "" };
        fnv1a(format!("standard/1 uax29 {major}.{minor}.{update} lowercase{stop}").as_bytes())
    }

    fn tokens(&self, text: &str, out: &mut dyn FnMut(Token<'_>)) {
        let mut lower = String::new();
        for (position, (at, word)) in text.unicode_word_indices().enumerate() {
            lower.clear();
            lower.extend(word.chars().flat_map(char::to_lowercase));
            if self.stop_words && STOP_WORDS.binary_search(&lower.as_str()).is_ok() {
                continue;
            }
            let position = u32::try_from(position).unwrap_or(u32::MAX);
            out(Token::new(&lower, position, at..at + word.len()));
        }
    }
}

/// FNV-1a, 64-bit: a stable hash for fingerprints (never `std`'s, which may change).
pub(crate) fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(t: &dyn Tokenizer, text: &str) -> Vec<(String, u32, Range<usize>)> {
        let mut v = Vec::new();
        t.tokens(text, &mut |tok| {
            v.push((tok.text.to_owned(), tok.position, tok.range));
        });
        v
    }

    #[test]
    fn the_stop_words_are_sorted_for_the_binary_search() {
        assert!(STOP_WORDS.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn words_are_lowercased_with_their_source_ranges() {
        let got = tokens(&Standard { stop_words: false }, "Chose Postgres, in MARCH.");
        assert_eq!(
            got,
            [
                ("chose".into(), 0, 0..5),
                ("postgres".into(), 1, 6..14),
                ("in".into(), 2, 16..18),
                ("march".into(), 3, 19..24),
            ]
        );
    }

    #[test]
    fn a_stop_word_is_dropped_but_keeps_its_position() {
        let got = tokens(&Standard { stop_words: true }, "state of the art");
        assert_eq!(got, [("state".into(), 0, 0..5), ("art".into(), 3, 13..16)]);
    }

    #[test]
    fn multi_byte_words_keep_byte_ranges() {
        let got = tokens(&Standard { stop_words: false }, "Ève à Zürich");
        assert_eq!(
            got,
            [
                ("ève".into(), 0, 0..4),
                ("à".into(), 1, 5..7),
                ("zürich".into(), 2, 8..15),
            ]
        );
    }

    #[test]
    fn the_fingerprint_follows_the_options() {
        let a = Standard { stop_words: false }.fingerprint();
        let b = Standard { stop_words: true }.fingerprint();
        assert_ne!(a, b);
        assert_eq!(a, Standard { stop_words: false }.fingerprint());
    }
}
