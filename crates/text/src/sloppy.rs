//! Sloppy phrases (D309 rev 6 §B, #1290): a phrase's terms within `slop` position moves of
//! their places, with Lucene's sloppy-phrase semantics and frequency.
//!
//! Each phrase term `i` at offset `oᵢ` walks its positions as `pᵢ − oᵢ` (its "phrase
//! position"). A match is one position per term whose phrase positions span at most `slop`
//! (`max − min`, the match's distance). The walk keeps the terms in a queue ordered by
//! phrase position, then offset, then term index. It repeatedly takes the leading term and
//! advances it, shrinking the current match's distance while the lead stays at or before
//! the next term. When the lead passes the next term, the match it was minimising is done:
//! it counts `1 / (distance + 1)` if its distance is within `slop`, and the walk continues
//! from the new lead. An exact match has distance 0 and counts 1.
//!
//! A word repeated in the phrase (`"the cat the"`) is a repeat group. Its terms share one
//! position list but never stand on one position together: the `j`-th repeat starts at its
//! word's `j`-th position, and when two collide the lesser (by phrase position, then offset)
//! moves on. A phrase whose repeats can't all be placed doesn't match.
//!
//! This follows the algorithm as Lucene's `SloppyPhraseMatcher` documents it, written here
//! from that description. The tests check it against a brute force of the match rule and
//! against the exact evaluator at slop 0.

/// One phrase term's walk over its positions.
struct Walk<'a> {
    positions: &'a [u32],
    /// The next position to take.
    next: usize,
    offset: i64,
    /// The current phrase position: the position taken, less the offset.
    at: i64,
    /// The repeat group: terms of the same word.
    group: usize,
}

impl Walk<'_> {
    /// Moves to the next position; `false` if there's none.
    fn advance(&mut self) -> bool {
        match self.positions.get(self.next) {
            Some(&p) => {
                self.at = i64::from(p) - self.offset;
                self.next += 1;
                true
            }
            None => false,
        }
    }

    /// The position in the document (not less the offset).
    fn position(&self) -> i64 {
        self.at + self.offset
    }
}

/// What the walk tells its caller about each match it counts: nothing when scoring (a
/// zero-sized [`Count`], so the scoring walk does no extra work), the matched positions when
/// highlighting ([`Positions`]).
pub(crate) trait Record {
    /// Whether the walk keeps the positions of the match it's minimising.
    const ON: bool;
    /// A counted match: one document position per phrase term, in the phrase's order.
    fn matched(&mut self, positions: &[u32]);
}

/// Counts only.
pub(crate) struct Count;

impl Record for Count {
    const ON: bool = false;
    fn matched(&mut self, _: &[u32]) {}
}

/// Every counted match's positions, in the order the walk counts them.
#[derive(Default)]
pub(crate) struct Positions(pub(crate) Vec<Vec<u32>>);

impl Record for Positions {
    const ON: bool = true;
    fn matched(&mut self, positions: &[u32]) {
        self.0.push(positions.to_vec());
    }
}

/// The sloppy frequency of a phrase in one document.
///
/// `terms` holds each phrase term's sorted positions in the document, its offset in the
/// phrase, and its word's index (equal for repeats of one word). Every term must have at
/// least one position.
pub(crate) fn sloppy_freq(terms: &[(&[u32], u32, usize)], slop: u16) -> f64 {
    sloppy(terms, slop, &mut Count)
}

/// [`sloppy_freq`], telling `rec` about each match it counts: the positions where the
/// match's distance was smallest.
pub(crate) fn sloppy<R: Record>(terms: &[(&[u32], u32, usize)], slop: u16, rec: &mut R) -> f64 {
    let slop = i64::from(slop);
    let mut w: Vec<Walk<'_>> = terms
        .iter()
        .map(|&(positions, offset, group)| Walk {
            positions,
            next: 0,
            offset: i64::from(offset),
            at: 0,
            group,
        })
        .collect();
    let repeats = (0..w.len()).any(|i| (0..i).any(|j| w[j].group == w[i].group));
    for walk in &mut w {
        if !walk.advance() {
            return 0.0;
        }
    }
    if repeats {
        // The j-th repeat of a word starts at the word's j-th position.
        for i in 0..w.len() {
            let earlier = (0..i).filter(|&j| w[j].group == w[i].group).count();
            for _ in 0..earlier {
                if !w[i].advance() {
                    return 0.0;
                }
            }
        }
    }
    let mut end = w.iter().map(|x| x.at).max().expect("a phrase has terms");
    // The queue: every walk but the lead, which is held out of it.
    let mut queued = vec![true; w.len()];
    let pop = |w: &[Walk<'_>], queued: &mut [bool]| -> usize {
        let i = (0..w.len())
            .filter(|&i| queued[i])
            .min_by_key(|&i| (w[i].at, w[i].offset, i))
            .expect("a queued walk");
        queued[i] = false;
        i
    };
    let top = |w: &[Walk<'_>], queued: &[bool]| -> i64 {
        (0..w.len())
            .filter(|&i| queued[i])
            .map(|i| (w[i].at, w[i].offset, i))
            .min()
            .expect("a queued walk")
            .0
    };
    let advance = |w: &mut [Walk<'_>], i: usize, end: &mut i64| -> bool {
        if !w[i].advance() {
            return false;
        }
        *end = (*end).max(w[i].at);
        true
    };
    let weight = |distance: i64| 1.0 / (distance as f64 + 1.0);
    // The positions where the current match's distance was smallest (only with `R::ON`).
    let mut best: Vec<u32> = Vec::new();
    let snap = |w: &[Walk<'_>], best: &mut Vec<u32>| {
        if R::ON {
            best.clear();
            best.extend(w.iter().map(|x| x.position() as u32));
        }
    };
    let mut freq = 0.0;
    let mut lead = pop(&w, &mut queued);
    let mut distance = end - w[lead].at;
    snap(&w, &mut best);
    let mut next = top(&w, &queued);
    loop {
        if !advance(&mut w, lead, &mut end) {
            break;
        }
        if repeats && !resolve_collisions(&mut w, lead, &mut end) {
            break;
        }
        if w[lead].at > next {
            // The match the old lead was minimising is done.
            if distance <= slop {
                freq += weight(distance);
                rec.matched(&best);
            }
            queued[lead] = true;
            lead = pop(&w, &mut queued);
            next = top(&w, &queued);
            distance = end - w[lead].at;
            snap(&w, &mut best);
        } else if end - w[lead].at < distance {
            distance = end - w[lead].at;
            snap(&w, &mut best);
        }
    }
    if distance <= slop {
        freq += weight(distance);
        rec.matched(&best);
    }
    freq
}

/// After walk `moved` advanced: while it shares a document position with another walk of its
/// word, advances the lesser of the two (by phrase position, then offset). `false` when a
/// walk runs out.
fn resolve_collisions(w: &mut [Walk<'_>], moved: usize, end: &mut i64) -> bool {
    let mut cur = moved;
    while let Some(other) = (0..w.len())
        .find(|&j| j != cur && w[j].group == w[cur].group && w[j].position() == w[cur].position())
    {
        let lesser = if (w[cur].at, w[cur].offset) < (w[other].at, w[other].offset) {
            cur
        } else {
            other
        };
        if !w[lesser].advance() {
            return false;
        }
        *end = (*end).max(w[lesser].at);
        cur = lesser;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn freq(phrase: &[&str], doc: &str, slop: u16) -> f64 {
        let words: Vec<&str> = doc.split(' ').collect();
        let mut groups: Vec<&str> = Vec::new();
        let lists: Vec<Vec<u32>> = phrase
            .iter()
            .map(|t| {
                (0..words.len() as u32)
                    .filter(|&p| words[p as usize] == *t)
                    .collect()
            })
            .collect();
        if lists.iter().any(Vec::is_empty) {
            return 0.0;
        }
        let terms: Vec<(&[u32], u32, usize)> = phrase
            .iter()
            .zip(&lists)
            .zip(0u32..)
            .map(|((t, l), off)| {
                let g = groups.iter().position(|x| x == t).unwrap_or_else(|| {
                    groups.push(t);
                    groups.len() - 1
                });
                (l.as_slice(), off, g)
            })
            .collect();
        sloppy_freq(&terms, slop)
    }

    /// Whether some placement of the phrase's terms (distinct positions for a repeated word)
    /// spans at most `slop`: the match rule, by brute force.
    fn matches(phrase: &[&str], doc: &str, slop: u16) -> bool {
        let words: Vec<&str> = doc.split(' ').collect();
        fn go(
            phrase: &[&str],
            words: &[&str],
            i: usize,
            picked: &mut Vec<usize>,
            slop: i64,
        ) -> bool {
            if i == phrase.len() {
                let rel: Vec<i64> = picked
                    .iter()
                    .zip(0i64..)
                    .map(|(&p, o)| p as i64 - o)
                    .collect();
                return rel.iter().max().unwrap() - rel.iter().min().unwrap() <= slop;
            }
            for p in 0..words.len() {
                if words[p] == phrase[i] && !picked.contains(&p) {
                    picked.push(p);
                    if go(phrase, words, i + 1, picked, slop) {
                        return true;
                    }
                    picked.pop();
                }
            }
            false
        }
        go(phrase, &words, 0, &mut Vec::new(), i64::from(slop))
    }

    #[test]
    fn worked_examples() {
        // Exact.
        assert_eq!(freq(&["a", "b"], "a b", 0), 1.0);
        assert_eq!(freq(&["a", "b"], "a b a b", 0), 2.0);
        // One word between: distance 1.
        assert_eq!(freq(&["a", "b"], "a x b", 0), 0.0);
        assert_eq!(freq(&["a", "b"], "a x b", 1), 0.5);
        // Reversed: distance 2.
        assert_eq!(freq(&["a", "b"], "b a", 1), 0.0);
        assert_eq!(freq(&["a", "b"], "b a", 2), 1.0 / 3.0);
        // simp724's case on #1247: one match at distance 0 (the lead minimises it to 0
        // before the next term), so 1.0; MaxScore's bound (Σ maxtf) covers it either way.
        assert_eq!(freq(&["a", "b"], "a a a b", 2), 1.0);
        // Repeats never share a position.
        assert_eq!(freq(&["a", "a"], "a", 5), 0.0);
        assert_eq!(freq(&["a", "a"], "a a", 0), 1.0);
        assert!(freq(&["the", "cat", "the"], "the cat the", 0) > 0.0);
        assert_eq!(freq(&["the", "cat", "the"], "the cat", 3), 0.0);
    }

    /// Repeated words, traced by hand through the walk (simp724 on #1300).
    #[test]
    fn repeated_words_by_hand() {
        // "a a" on "a x a": the second a starts at its word's second position (2), one match
        // at distance 1; the lead's next step collides at 2 and the lesser runs out.
        assert_eq!(freq(&["a", "a"], "a x a", 1), 0.5);
        assert_eq!(freq(&["a", "a"], "a x a", 0), 0.0);
        // "a a" on "a a a": two exact matches (0-1 and 1-2), as the exact count gives.
        assert_eq!(freq(&["a", "a"], "a a a", 0), 2.0);
        // "the cat the" on "the cat the": one exact match; more slop finds no other.
        assert_eq!(freq(&["the", "cat", "the"], "the cat the", 0), 1.0);
        assert_eq!(freq(&["the", "cat", "the"], "the cat the", 2), 1.0);
        // "a b a" on "a b x a": the second a sits one place late, distance 1.
        assert_eq!(freq(&["a", "b", "a"], "a b x a", 1), 0.5);
        assert_eq!(freq(&["a", "b", "a"], "a b x a", 0), 0.0);
        // simp724's "a b a"~2 on "a b a b a": the lead a advances to 2 and collides with the
        // second a, which moves to 4; that closes a match at distance 0. The new lead b moves
        // to 3 and closes another at 0. That's the two exact occurrences (0-2, 2-4), sharing
        // position 2.
        assert_eq!(freq(&["a", "b", "a"], "a b a b a", 2), 2.0);
        assert_eq!(freq(&["a", "b", "a"], "a b a b a", 0), 2.0);
    }

    /// xorshift: deterministic per seed.
    fn rng(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    #[test]
    fn a_phrase_without_repeats_matches_exactly_when_the_rule_says() {
        let mut seed = 0x5_1090_u64;
        let vocab = ["a", "b", "c", "d"];
        for case in 0..3000 {
            let n = 2 + (rng(&mut seed) % 2) as usize;
            let mut phrase: Vec<&str> = vocab.to_vec();
            for i in (1..phrase.len()).rev() {
                phrase.swap(i, (rng(&mut seed) % (i as u64 + 1)) as usize);
            }
            phrase.truncate(n);
            let len = 1 + (rng(&mut seed) % 9) as usize;
            let doc: Vec<&str> = (0..len)
                .map(|_| vocab[(rng(&mut seed) % 4) as usize])
                .collect();
            let doc = doc.join(" ");
            let slop = (rng(&mut seed) % 5) as u16;
            let f = freq(&phrase, &doc, slop);
            assert_eq!(
                f > 0.0,
                matches(&phrase, &doc, slop),
                "case {case}: {phrase:?} ~{slop} in {doc:?} gave {f}"
            );
            // At slop 0, every match is exact and counts 1: the exact phrase's count.
            let words: Vec<&str> = doc.split(' ').collect();
            let exact = (0..words.len())
                .filter(|&i| {
                    i + n <= words.len() && phrase.iter().zip(&words[i..]).all(|(a, b)| a == b)
                })
                .count();
            assert_eq!(
                freq(&phrase, &doc, 0),
                exact as f64,
                "case {case}: {phrase:?} in {doc:?} at slop 0"
            );
        }
    }

    #[test]
    fn a_phrase_with_repeats_only_matches_where_the_rule_allows() {
        let mut seed = 0x5_1091_u64;
        let vocab = ["a", "b", "c"];
        for case in 0..3000 {
            let n = 2 + (rng(&mut seed) % 2) as usize;
            let phrase: Vec<&str> = (0..n)
                .map(|_| vocab[(rng(&mut seed) % 3) as usize])
                .collect();
            let len = 1 + (rng(&mut seed) % 8) as usize;
            let doc: Vec<&str> = (0..len)
                .map(|_| vocab[(rng(&mut seed) % 3) as usize])
                .collect();
            let doc = doc.join(" ");
            let slop = (rng(&mut seed) % 4) as u16;
            let f = freq(&phrase, &doc, slop);
            if f > 0.0 {
                assert!(
                    matches(&phrase, &doc, slop),
                    "case {case}: {phrase:?} ~{slop} in {doc:?} gave {f} without a placement"
                );
            }
        }
    }

    #[test]
    fn each_counted_match_is_a_placement_and_their_weights_are_the_frequency() {
        let mut seed = 0x5_1092_u64;
        let vocab = ["a", "b", "c"];
        for case in 0..3000 {
            let n = 2 + (rng(&mut seed) % 2) as usize;
            let phrase: Vec<&str> = (0..n)
                .map(|_| vocab[(rng(&mut seed) % 3) as usize])
                .collect();
            let len = 1 + (rng(&mut seed) % 9) as usize;
            let words: Vec<&str> = (0..len)
                .map(|_| vocab[(rng(&mut seed) % 3) as usize])
                .collect();
            let slop = (rng(&mut seed) % 4) as u16;
            let lists: Vec<Vec<u32>> = phrase
                .iter()
                .map(|t| {
                    (0..len as u32)
                        .filter(|&p| words[p as usize] == *t)
                        .collect()
                })
                .collect();
            if lists.iter().any(Vec::is_empty) {
                continue;
            }
            let terms: Vec<(&[u32], u32, usize)> = phrase
                .iter()
                .zip(&lists)
                .zip(0u32..)
                .map(|((t, l), off)| {
                    let g = phrase.iter().position(|x| x == t).expect("itself");
                    (l.as_slice(), off, g)
                })
                .collect();
            let mut rec = Positions::default();
            let f = sloppy(&terms, slop, &mut rec);
            assert_eq!(f, sloppy_freq(&terms, slop), "case {case}");
            let ctx = format!("case {case}: {phrase:?} ~{slop} in {words:?}");
            let mut sum = 0.0;
            for ps in &rec.0 {
                assert_eq!(ps.len(), n, "{ctx}");
                for (i, &p) in ps.iter().enumerate() {
                    assert_eq!(words[p as usize], phrase[i], "{ctx}: {ps:?}");
                    for j in 0..i {
                        assert!(
                            phrase[j] != phrase[i] || ps[j] != p,
                            "{ctx}: {ps:?} reuses {p}"
                        );
                    }
                }
                let at: Vec<i64> = ps
                    .iter()
                    .zip(0i64..)
                    .map(|(&p, o)| i64::from(p) - o)
                    .collect();
                let d = at.iter().max().unwrap() - at.iter().min().unwrap();
                assert!(d <= i64::from(slop), "{ctx}: {ps:?} spans {d}");
                sum += 1.0 / (d as f64 + 1.0);
            }
            assert!(
                (sum - f).abs() < 1e-12,
                "{ctx}: weights {sum} vs frequency {f}"
            );
        }
    }
}
