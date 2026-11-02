//! Highlighting (D309 §4.5): each hit's matches, recomputed by re-tokenising its source text,
//! and [`snippet`], a window of the text around the best of them.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use unicode_segmentation::UnicodeSegmentation;

use crate::sloppy::{Positions, sloppy};

/// One occurrence of a query clause in a hit's text ([`Hit::matches`](crate::Hit::matches)).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Match {
    /// Its byte range in the hit's text: a term's token, or an exact phrase from its first
    /// token to its last. A sloppy phrase match lists one range per phrase word.
    pub range: Range<usize>,
    /// The clause it matched: an index into [`Query::clauses`](crate::Query::clauses).
    pub clause: usize,
    /// Which of the clause's occurrences in the text it is, from 0 in the order they're
    /// found. The ranges of one sloppy phrase match share it.
    pub occurrence: u32,
}

/// A window of a hit's text ([`snippet`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Snippet {
    /// The window's byte range in the text.
    pub range: Range<usize>,
    /// The matches wholly inside it, by position.
    pub matches: Vec<Match>,
}

/// The window of `text`, at most `max_bytes` long, that holds the most distinct clauses of
/// `matches`, then the most matches, then starts earliest (D309 §4.5).
///
/// A window starts at the text's start or at a match, and ends on a word boundary (Unicode
/// UAX #29), so it never cuts a word or a character. Only matches wholly inside it are
/// returned. With no matches, it's the text's start.
pub fn snippet(text: &str, matches: &[Match], max_bytes: usize) -> Snippet {
    let mut bounds: Vec<usize> = text.split_word_bound_indices().map(|(i, _)| i).collect();
    bounds.push(text.len());
    let mut starts: Vec<usize> = std::iter::once(0)
        .chain(matches.iter().map(|m| m.range.start))
        .filter(|&s| s <= text.len())
        .collect();
    starts.sort_unstable();
    starts.dedup();
    let mut best: Option<((usize, usize), Range<usize>)> = None;
    for s in starts {
        let limit = s.saturating_add(max_bytes);
        // The last word boundary within the limit; the start itself when none is.
        let i = bounds.partition_point(|&b| b <= limit);
        let e = if i == 0 { s } else { bounds[i - 1].max(s) };
        let inside = matches
            .iter()
            .filter(|m| m.range.start >= s && m.range.end <= e);
        let clauses: BTreeSet<usize> = inside.clone().map(|m| m.clause).collect();
        let rank = (clauses.len(), inside.count());
        // Strictly better only, so ties keep the earliest start.
        if best.as_ref().is_none_or(|(r, _)| rank > *r) {
            best = Some((rank, s..e));
        }
    }
    let (_, range) = best.expect("the text's start is a candidate");
    let mut inside: Vec<Match> = matches
        .iter()
        .filter(|m| m.range.start >= range.start && m.range.end <= range.end)
        .cloned()
        .collect();
    inside.sort_by_key(|m| (m.range.start, m.range.end, m.clause, m.occurrence));
    Snippet {
        range,
        matches: inside,
    }
}

/// What one clause looks for in a hit's text.
#[derive(Debug, Clone)]
pub(crate) enum Want {
    /// Any of these terms (a term, text's terms, a fuzzy term's expansions).
    Terms(BTreeSet<String>),
    /// A phrase: each term with its offset from the first, and its slop.
    Phrase {
        terms: Vec<(String, u32)>,
        slop: u16,
    },
}

/// A document's tokens as the index holds them: each term, its position and its range.
pub(crate) type Spans = Vec<(String, u32, Range<usize>)>;

/// Every match of `wants` (clause index, what it wants) in a text's `spans`, by position.
pub(crate) fn find(spans: &Spans, wants: &[(usize, Want)]) -> Vec<Match> {
    let mut out = Vec::new();
    // (term, position) → range: the first token there with that term.
    let mut at: BTreeMap<(&str, u32), &Range<usize>> = BTreeMap::new();
    let mut lists: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
    for (t, p, r) in spans {
        at.entry((t.as_str(), *p)).or_insert(r);
        let l = lists.entry(t.as_str()).or_default();
        if l.last() != Some(p) {
            l.push(*p);
        }
    }
    for (clause, want) in wants {
        let clause = *clause;
        let mut occurrence = 0u32;
        match want {
            Want::Terms(terms) => {
                for (t, _, r) in spans {
                    if terms.contains(t) {
                        out.push(Match {
                            range: r.clone(),
                            clause,
                            occurrence,
                        });
                        occurrence += 1;
                    }
                }
            }
            Want::Phrase { terms, slop: 0 } => {
                let (first, _) = &terms[0];
                for &p in lists.get(first.as_str()).map_or(&[][..], Vec::as_slice) {
                    let rs: Option<Vec<&Range<usize>>> = terms
                        .iter()
                        .map(|(t, off)| at.get(&(t.as_str(), p + off)).copied())
                        .collect();
                    if let Some(rs) = rs {
                        let start = rs.iter().map(|r| r.start).min().expect("terms");
                        let end = rs.iter().map(|r| r.end).max().expect("terms");
                        out.push(Match {
                            range: start..end,
                            clause,
                            occurrence,
                        });
                        occurrence += 1;
                    }
                }
            }
            Want::Phrase { terms, slop } => {
                let ls: Option<Vec<&[u32]>> = terms
                    .iter()
                    .map(|(t, _)| lists.get(t.as_str()).map(Vec::as_slice))
                    .collect();
                let Some(ls) = ls else {
                    continue;
                };
                let walks: Vec<(&[u32], u32, usize)> = ls
                    .iter()
                    .zip(terms)
                    .map(|(l, (t, off))| {
                        let g = terms.iter().position(|(x, _)| x == t).expect("itself");
                        (*l, *off, g)
                    })
                    .collect();
                let mut rec = Positions::default();
                sloppy(&walks, *slop, &mut rec);
                for ps in rec.0 {
                    for ((t, _), p) in terms.iter().zip(ps) {
                        out.push(Match {
                            range: at[&(t.as_str(), p)].clone(),
                            clause,
                            occurrence,
                        });
                    }
                    occurrence += 1;
                }
            }
        }
    }
    out.sort_by_key(|m| (m.range.start, m.range.end, m.clause, m.occurrence));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(range: Range<usize>, clause: usize, occurrence: u32) -> Match {
        Match {
            range,
            clause,
            occurrence,
        }
    }

    #[test]
    fn the_snippet_prefers_distinct_clauses_then_more_matches_then_the_earliest() {
        //          0         1         2         3         4
        //          0123456789012345678901234567890123456789012345
        let text = "alpha beta alpha gamma delta beta alpha epsilon";
        let ms = [
            m(0..5, 0, 0),
            m(6..10, 1, 0),
            m(11..16, 0, 1),
            m(29..33, 1, 1),
            m(34..39, 0, 2),
        ];
        // 16 bytes: "alpha beta alpha" holds both clauses and three matches.
        let s = snippet(text, &ms, 16);
        assert_eq!(s.range, 0..16);
        assert_eq!(s.matches.len(), 3);
        // 10 bytes: "alpha beta" and two "beta alpha"s tie; the earliest wins.
        let s = snippet(text, &ms, 10);
        assert_eq!(s.range, 0..10);
        // A window never cuts a word: 13 bytes from 0 end after "beta ".
        let s = snippet(text, &ms, 13);
        assert_eq!(&text[s.range.clone()], "alpha beta ");
        // No matches: the text's start.
        let s = snippet(text, &[], 8);
        assert_eq!((s.range, s.matches.len()), (0..6, 0));
        // Multi-byte text is cut on a word boundary, so never inside a character: from 0,
        // 5 bytes is "café" (é is 2), and from the match at 6 the window can't reach past
        // "crème"'s start, so it holds nothing and the earlier start wins.
        let s = snippet("café crème", &[m(6..12, 0, 0)], 5);
        assert_eq!(s.range, 0..5);
        let s = snippet("café crème", &[m(6..12, 0, 0)], 6);
        assert_eq!((s.range, s.matches.len()), (6..12, 1));
    }
}
