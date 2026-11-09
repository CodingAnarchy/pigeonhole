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
    match plan(term, edits, prefix_chars) {
        None => {
            let df = df_at(t, s, &post_row(term))?;
            exact(&mut found, term, df);
        }
        Some((base, dfa)) => walk(t, s, &base, &dfa, |key, distance, df| {
            found_hit(&mut found, q_chars, key, distance, df)
        })?,
    }
    Ok(finish(found, max))
}

/// [`expand`] as a future (#1560): the same expansions, its dictionary walk a scan stream.
#[cfg(feature = "async")]
pub(crate) async fn expand_async(
    t: &Rows,
    s: &Snapshot,
    term: &str,
    edits: u8,
    prefix_chars: usize,
    max: usize,
) -> Result<(Vec<Expansion>, bool)> {
    let q_chars = term.chars().count();
    let mut found = Vec::new();
    match plan(term, edits, prefix_chars) {
        None => {
            let df = t
                .get_at_async(s, &post_row(term), DF, N)
                .await?
                .and_then(|c| c.as_i64())
                .unwrap_or(0);
            exact(&mut found, term, df);
        }
        Some((base, dfa)) => walk_async(t, s, &base, &dfa, &mut found, q_chars).await?,
    }
    Ok(finish(found, max))
}

/// The dictionary walk's base (the row prefix plus the fixed head) and automaton for the
/// rest, or `None` for an exact term (no edits).
fn plan(term: &str, edits: u8, prefix_chars: usize) -> Option<(Vec<u8>, DFA)> {
    if edits == 0 {
        return None;
    }
    let split = term
        .char_indices()
        .nth(prefix_chars)
        .map_or(term.len(), |(i, _)| i);
    let (head, tail) = term.split_at(split);
    Some((post_row(head), builder(edits).build_dfa(tail)))
}

/// An exact term's one expansion, if it's in the index.
fn exact(found: &mut Vec<Expansion>, term: &str, df: i64) {
    if df > 0 {
        found.push(Expansion {
            term: term.to_owned(),
            distance: 0,
            df,
            boost: 1.0,
        });
    }
}

/// A term the walk matched, `distance` edits away, kept if its boost is above 0.
fn found_hit(
    found: &mut Vec<Expansion>,
    q_chars: usize,
    key: &[u8],
    distance: u8,
    df: i64,
) -> Result<()> {
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
}

/// The expansions in order, at most `max`, and whether `max` cut any.
fn finish(mut found: Vec<Expansion>, max: usize) -> (Vec<Expansion>, bool) {
    found.sort_by(|a, b| {
        a.distance
            .cmp(&b.distance)
            .then(b.df.cmp(&a.df))
            .then_with(|| a.term.as_bytes().cmp(b.term.as_bytes()))
    });
    let cut = found.len() > max;
    found.truncate(max);
    (found, cut)
}

/// What the walk does at a dictionary row.
enum Visit {
    /// The automaton accepts the row's term at this distance.
    Hit(u8),
    /// It doesn't, but a later row may match.
    Skip,
    /// It rejected the term partway: every key up to `base` + this dies there too.
    Seek(Vec<u8>),
    /// Nothing after this row can match.
    End,
}

/// Runs `dfa` over `key`'s rest past `base_len`. `states[i]` is the state before `rest[i]`.
fn visit(dfa: &DFA, base_len: usize, key: &[u8], states: &mut Vec<u32>) -> Visit {
    let rest = &key[base_len..];
    states.clear();
    let mut state = dfa.initial_state();
    for (i, &b) in rest.iter().enumerate() {
        states.push(state);
        state = dfa.transition(state, b);
        if state == SINK_STATE {
            // Every key from here to the seek target dies where this one did.
            return match next_live(dfa, rest, states, i) {
                Some(next) => Visit::Seek(next),
                None => Visit::End,
            };
        }
    }
    match dfa.distance(state) {
        Distance::Exact(d) => Visit::Hit(d),
        Distance::AtLeast(_) => Visit::Skip,
    }
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
            match visit(dfa, base.len(), key, &mut states) {
                Visit::Hit(d) => {
                    let df = row.get(DF, N).and_then(|c| c.as_i64()).unwrap_or(0);
                    if df > 0 {
                        hit(&key[POST_PREFIX.len()..], d, df)?;
                    }
                }
                Visit::Skip => {}
                Visit::Seek(next) => {
                    from = [base, &next].concat();
                    continue 'seek;
                }
                Visit::End => return Ok(()),
            }
        }
        return Ok(());
    }
}

/// [`walk`] as a future: the same rows, from a scan stream, each re-seek a new one.
#[cfg(feature = "async")]
async fn walk_async(
    t: &Rows,
    s: &Snapshot,
    base: &[u8],
    dfa: &DFA,
    found: &mut Vec<Expansion>,
    q_chars: usize,
) -> Result<()> {
    use futures_core::Stream;
    let end = prefix_end(base);
    let mut from = base.to_vec();
    let mut states = Vec::new();
    'seek: loop {
        let end_bound = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        let mut rows = t
            .scan_bounds(Bound::Included(&from), end_bound)
            .family(DF)
            .snapshot(s)
            .stream();
        while let Some(row) =
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut rows).poll_next(cx)).await
        {
            let row = row?;
            let row = row.view();
            let key = row.key();
            match visit(dfa, base.len(), key, &mut states) {
                Visit::Hit(d) => {
                    let df = row.get(DF, N).and_then(|c| c.as_i64()).unwrap_or(0);
                    if df > 0 {
                        found_hit(found, q_chars, &key[POST_PREFIX.len()..], d, df)?;
                    }
                }
                Visit::Skip => {}
                Visit::Seek(next) => {
                    from = [base, &next].concat();
                    continue 'seek;
                }
                Visit::End => return Ok(()),
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
