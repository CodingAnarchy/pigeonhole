//! Fuzzy terms (D309 §4.4): a term's expansions, the index's terms within a few edits of it.
//!
//! The expansion walks a Levenshtein automaton (Schulz–Mihov, from `levenshtein_automata`,
//! with an adjacent transposition as one edit) against the sorted `P{term}` rows, reading
//! only each row's `df` counter. At a row whose key the automaton rejects partway, the walk
//! seeks to the smallest key above it that could still match, so the cost is the matching
//! terms plus one seek per dead branch, not the dictionary's size.

use std::ops::Bound;
use std::sync::OnceLock;

use levenshtein_automata::{DFA, Distance, LevenshteinAutomatonBuilder, SINK_STATE};
use pigeonhole::Snapshot;

use crate::error::{Result, corrupt};
use crate::index::Rows;
use crate::layout::*;
use crate::query::Fuzziness;

/// One term a fuzzy term matched.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Expansion {
    pub(crate) term: String,
    /// Its edit distance from the fuzzy term.
    pub(crate) distance: u8,
    pub(crate) df: i64,
    /// Lucene's FuzzyQuery boost: `1 − distance / min(len(q), len(t))`, in characters.
    pub(crate) boost: f64,
}

/// The edits a fuzzy term allows: [`Fuzziness::Auto`] by its length in characters.
pub(crate) fn edits(term: &str, fuzziness: Fuzziness) -> u8 {
    match fuzziness {
        Fuzziness::One => 1,
        Fuzziness::Two => 2,
        Fuzziness::Auto => match term.chars().count() {
            0..=2 => 0,
            3..=5 => 1,
            _ => 2,
        },
    }
}

/// The boost of a match `distance` edits away, or `None` when the edits are as many as the
/// shorter term's characters: such a "match" replaces a whole term (`a` and `b` are one
/// edit apart), and its boost would be 0 or below.
pub(crate) fn boost(q_chars: usize, t_chars: usize, distance: u8) -> Option<f64> {
    if distance == 0 {
        return Some(1.0);
    }
    let shorter = q_chars.min(t_chars);
    (usize::from(distance) < shorter).then(|| 1.0 - f64::from(distance) / shorter as f64)
}

/// The automaton builders, for 1 and 2 edits: a few milliseconds each to build, so built once.
fn builder(edits: u8) -> &'static LevenshteinAutomatonBuilder {
    static ONE: OnceLock<LevenshteinAutomatonBuilder> = OnceLock::new();
    static TWO: OnceLock<LevenshteinAutomatonBuilder> = OnceLock::new();
    let cell = if edits == 1 { &ONE } else { &TWO };
    cell.get_or_init(|| LevenshteinAutomatonBuilder::new(edits, true))
}

/// `term`'s expansions within `edits` (at most 2), whose first `prefix_chars` characters
/// equal `term`'s: the closest first, then the highest `df`, then the term (bytewise), at
/// most `max` of them. The flag says whether `max` cut any.
pub(crate) fn expand(
    t: &Rows,
    s: &Snapshot,
    term: &str,
    edits: u8,
    prefix_chars: usize,
    max: usize,
) -> Result<(Vec<Expansion>, bool)> {
    let q_chars = term.chars().count();
    let mut found = Vec::new();
    if edits == 0 {
        let df = df_at(t, s, &post_row(term))?;
        if df > 0 {
            found.push(Expansion {
                term: term.to_owned(),
                distance: 0,
                df,
                boost: 1.0,
            });
        }
    } else {
        let split = term
            .char_indices()
            .nth(prefix_chars)
            .map_or(term.len(), |(i, _)| i);
        let (head, tail) = term.split_at(split);
        let dfa = builder(edits).build_dfa(tail);
        walk(t, s, &post_row(head), &dfa, |key, distance, df| {
            let t_term = std::str::from_utf8(key).map_err(|_| corrupt("a term isn't UTF-8"))?;
            if let Some(boost) = boost(q_chars, t_term.chars().count(), distance) {
                found.push(Expansion {
                    term: t_term.to_owned(),
                    distance,
                    df,
                    boost,
                });
            }
            Ok(())
        })?;
    }
    found.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then(b.df.cmp(&a.df))
            .then_with(|| a.term.as_bytes().cmp(b.term.as_bytes()))
    });
    let cut = found.len() > max;
    found.truncate(max);
    Ok((found, cut))
}

/// Walks the `P` rows under `base` (the row prefix plus the fixed head) whose rest `dfa`
/// accepts, calling `hit` with each one's term, distance and `df` (above 0).
fn walk(
    t: &Rows,
    s: &Snapshot,
    base: &[u8],
    dfa: &DFA,
    mut hit: impl FnMut(&[u8], u8, i64) -> Result<()>,
) -> Result<()> {
    let end = prefix_end(base);
    let mut from = base.to_vec();
    let mut states = Vec::new();
    'seek: loop {
        let end_bound = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        let mut rows = t
            .scan_bounds(Bound::Included(&from), end_bound)
            .family(DF)
            .snapshot(s)
            .iter()?;
        while let Some(row) = rows.next_ref()? {
            let key = row.key();
            let rest = &key[base.len()..];
            // `states[i]`: the state before `rest[i]`.
            states.clear();
            let mut state = dfa.initial_state();
            let mut dead = None;
            for (i, &b) in rest.iter().enumerate() {
                states.push(state);
                state = dfa.transition(state, b);
                if state == SINK_STATE {
                    dead = Some(i);
                    break;
                }
            }
            match dead {
                None => {
                    if let Distance::Exact(d) = dfa.distance(state) {
                        let df = row.get(DF, N).and_then(|c| c.as_i64()).unwrap_or(0);
                        if df > 0 {
                            let term = &key[POST_PREFIX.len()..];
                            hit(term, d, df)?;
                        }
                    }
                }
                Some(j) => {
                    // Every key from here to the seek target dies where this one did.
                    match next_live(dfa, rest, &states, j) {
                        Some(next) => {
                            from = [base, &next].concat();
                            continue 'seek;
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
        return Ok(());
    }
}

/// The smallest byte string above `rest` that the automaton hasn't rejected, given that it
/// rejects `rest[..=dead]`: the deepest `i ≤ dead` where some byte above `rest[i]` keeps it
/// live, and that byte. `None` when there's none, so nothing after `rest` can match.
fn next_live(dfa: &DFA, rest: &[u8], states: &[u32], dead: usize) -> Option<Vec<u8>> {
    for i in (0..=dead).rev() {
        if rest[i] == u8::MAX {
            continue;
        }
        if let Some(b) =
            (rest[i] + 1..=u8::MAX).find(|&b| dfa.transition(states[i], b) != SINK_STATE)
        {
            let mut next = rest[..i].to_vec();
            next.push(b);
            return Some(next);
        }
    }
    None
}

/// The first key after every key starting with `prefix`, or `None` when there's none.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

fn df_at(t: &Rows, s: &Snapshot, row: &[u8]) -> Result<i64> {
    Ok(t.get_at(s, row, DF, N)?
        .and_then(|c| c.as_i64())
        .unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_automatic_distance_follows_the_length_in_characters() {
        assert_eq!(edits("ab", Fuzziness::Auto), 0);
        assert_eq!(edits("abc", Fuzziness::Auto), 1);
        assert_eq!(edits("ébcde", Fuzziness::Auto), 1);
        assert_eq!(edits("abcdef", Fuzziness::Auto), 2);
        assert_eq!(edits("ab", Fuzziness::Two), 2);
    }

    #[test]
    fn a_match_that_replaces_a_whole_term_has_no_boost() {
        assert_eq!(boost(1, 1, 0), Some(1.0));
        assert_eq!(boost(1, 1, 1), None);
        assert_eq!(boost(4, 2, 2), None);
        assert_eq!(boost(4, 5, 1), Some(0.75));
    }

    #[test]
    fn next_live_skips_the_dead_branch() {
        let dfa = builder(1).build_dfa("cat");
        // `cxy` is two edits into `cat` at `y`. No byte above `y` keeps `cx` live (only `t`
        // would, below it) but a multi-byte character's lead byte: the UTF-8 automaton
        // rejects a character at its last byte. So the seek lands on `cx\xC0`, a lower bound
        // the walk then rejects, and every key it skipped dies where `cxy` did.
        let rest = b"cxy";
        let mut states = Vec::new();
        let mut st = dfa.initial_state();
        let mut dead = None;
        for (i, &b) in rest.iter().enumerate() {
            states.push(st);
            st = dfa.transition(st, b);
            if st == SINK_STATE {
                dead = Some(i);
                break;
            }
        }
        assert_eq!(dead, Some(2));
        let next = next_live(&dfa, rest, &states, 2).expect("a later key can match");
        assert_eq!(next, b"cx\xC0");
        for b in b'y' + 1..=0xBF {
            assert_eq!(dfa.transition(states[2], b), SINK_STATE, "{b:#x}");
        }
        // At the last byte value, nothing is above.
        assert_eq!(next_live(&dfa, b"\xff", &[dfa.initial_state()], 0), None);
        assert_eq!(prefix_end(b"P"), Some(b"Q".to_vec()));
        assert_eq!(prefix_end(&[0xFF, 0xFF]), None);
    }
}
