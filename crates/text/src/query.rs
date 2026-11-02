//! Queries (D309 §6a): the [`Query`] builder and the query syntax.

use crate::error::{Error, QueryErrorKind, Result};

/// How far a fuzzy term may be from the terms it matches, in edits (D309 §4.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Fuzziness {
    /// By the term's length in characters: 0 edits for 1–2, 1 for 3–5, 2 for 6 or more.
    #[default]
    Auto,
    /// One edit.
    One,
    /// Two edits.
    Two,
}

/// One clause of a [`Query`]: a disjunction's member.
///
/// [`Query::clauses`] lists them in first-occurrence order, with repeats dropped.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Clause {
    /// A term, matched exactly as given (never tokenised).
    Term(String),
    /// Words matched exactly as given (never tokenised), at consecutive positions, or
    /// within `slop` position moves of them.
    Phrase {
        /// The words.
        words: Vec<String>,
        /// The position moves a match may take: 0 for an exact phrase.
        slop: u16,
    },
    /// Text tokenised as documents are: each of its terms is matched. A bare word of the
    /// syntax, or [`Query::text`].
    Text(String),
    /// A quoted phrase of the syntax (`"..."`, or `"..."~N` with a slop): tokenised as
    /// documents are, and its stop words keep their places, so their positions must be
    /// there in a match.
    QuotedPhrase {
        /// The phrase's text.
        text: String,
        /// The position moves a match may take: 0 for an exact phrase.
        slop: u16,
    },
    /// A fuzzy term (D309 §4.4). The term is matched as given.
    Fuzzy {
        /// The term.
        term: String,
        /// How far a match may be.
        fuzziness: Fuzziness,
    },
}

/// A search query: a disjunction (OR) of clauses, scored by BM25 (D309 §4, §6a).
///
/// Build one with the methods here, or parse the query syntax with [`Query::parse`] (or a
/// `&str` given to [`Index::search`](crate::Index::search), parsed when the search runs):
/// - whitespace-separated words, each tokenised as documents are ([`Clause::Text`]);
/// - `"..."` for a phrase, and `"..."~N` for one within `N` position moves (0 to 65,535)
///   ([`Clause::QuotedPhrase`]);
/// - `word~`, `word~1` or `word~2` for a fuzzy term ([`Clause::Fuzzy`]);
/// - `\"`, `\~` and `\\` for a literal `"`, `~` or `\`.
///
/// **The builder's terms and phrase words are matched exactly as given, never tokenised:**
/// `.term("March")` matches nothing in an index that holds `march`. Use
/// [`text`](Query::text) for words that should be tokenised as documents are.
///
/// A repeated clause counts once, and so does a term that two clauses yield. A query with
/// no clauses matches nothing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Query {
    clauses: Vec<Clause>,
    /// A parse error from [`From<&str>`], returned when the search runs.
    error: Option<(usize, QueryErrorKind)>,
}

impl Query {
    /// An empty query.
    pub fn new() -> Self {
        Query::default()
    }

    /// Parses the query syntax (above). The `Query` it returns is the one a search of the
    /// same string runs, so its [`clauses`](Query::clauses) are the ones a search uses.
    pub fn parse(query: &str) -> Result<Query> {
        let mut q = Query::new();
        parse_into(query, &mut q).map_err(|(at, reason)| Error::InvalidQuery { at, reason })?;
        Ok(q)
    }

    /// Adds a term, matched exactly as given.
    pub fn term(self, term: impl Into<String>) -> Self {
        self.with(Clause::Term(term.into()))
    }

    /// Adds a phrase of words, matched exactly as given, at consecutive positions.
    pub fn phrase<I, S>(self, words: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.phrase_with_slop(words, 0)
    }

    /// Adds a phrase of words, matched as given, within `slop` position moves of
    /// consecutive positions (Lucene's sloppy phrase).
    ///
    /// A slop above 0 comes with D309 PR 2: until then a search with one is refused
    /// (`InvalidArgument`).
    pub fn phrase_with_slop<I, S>(self, words: I, slop: u16) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.with(Clause::Phrase {
            words: words.into_iter().map(Into::into).collect(),
            slop,
        })
    }

    /// Adds text, tokenised as documents are: each of its terms is a clause's term.
    pub fn text(self, text: impl Into<String>) -> Self {
        self.with(Clause::Text(text.into()))
    }

    /// Adds a fuzzy term, matched as given (D309 §4.4).
    ///
    /// Fuzzy terms come in D309 PR 4: until then a search with one is refused
    /// (`InvalidArgument`).
    pub fn fuzzy(self, term: impl Into<String>, fuzziness: Fuzziness) -> Self {
        self.with(Clause::Fuzzy {
            term: term.into(),
            fuzziness,
        })
    }

    /// The clauses, in first-occurrence order with repeats dropped.
    pub fn clauses(&self) -> &[Clause] {
        &self.clauses
    }

    fn with(mut self, clause: Clause) -> Self {
        self.push(clause);
        self
    }

    fn push(&mut self, clause: Clause) {
        if !self.clauses.contains(&clause) {
            self.clauses.push(clause);
        }
    }

    /// The parse error a `From<&str>` query carries.
    pub(crate) fn check(&self) -> Result<()> {
        match self.error {
            Some((at, reason)) => Err(Error::InvalidQuery { at, reason }),
            None => Ok(()),
        }
    }
}

/// Parses `s`; an error shows when the search runs ([`Query::parse`] reports it at once).
impl From<&str> for Query {
    fn from(s: &str) -> Self {
        let mut q = Query::new();
        if let Err(e) = parse_into(s, &mut q) {
            q.clauses.clear();
            q.error = Some(e);
        }
        q
    }
}

impl From<String> for Query {
    fn from(s: String) -> Self {
        Query::from(s.as_str())
    }
}

impl From<&String> for Query {
    fn from(s: &String) -> Self {
        Query::from(s.as_str())
    }
}

type ParseError = (usize, QueryErrorKind);

fn parse_into(s: &str, q: &mut Query) -> std::result::Result<(), ParseError> {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = s[i..].chars().next().expect("in bounds");
        if c.is_whitespace() {
            i += c.len_utf8();
            continue;
        }
        if c == '"' {
            let open = i;
            i += 1;
            let mut phrase = String::new();
            loop {
                let Some(c) = s[i..].chars().next() else {
                    return Err((open, QueryErrorKind::UnclosedQuote));
                };
                match c {
                    '"' => {
                        i += 1;
                        break;
                    }
                    '~' => return Err((i, QueryErrorKind::FuzzyInPhrase)),
                    '\\' => {
                        i += 1;
                        let Some(e) = s[i..].chars().next() else {
                            return Err((open, QueryErrorKind::UnclosedQuote));
                        };
                        phrase.push(e);
                        i += e.len_utf8();
                    }
                    c => {
                        phrase.push(c);
                        i += c.len_utf8();
                    }
                }
            }
            let mut slop = 0;
            if s[i..].starts_with('~') {
                let at = i;
                i += 1;
                let digits: String = s[i..].chars().take_while(char::is_ascii_digit).collect();
                i += digits.len();
                slop = digits
                    .parse::<u16>()
                    .map_err(|_| (at, QueryErrorKind::Slop))?;
                if let Some(c) = s[i..].chars().next()
                    && !c.is_whitespace()
                    && c != '"'
                {
                    return Err((at, QueryErrorKind::Slop));
                }
            }
            q.push(Clause::QuotedPhrase { text: phrase, slop });
            continue;
        }
        // A bare word: up to whitespace, a quote, or an unescaped `~`.
        let mut word = String::new();
        let mut fuzzy = None;
        while let Some(c) = s[i..].chars().next() {
            if c.is_whitespace() || c == '"' {
                break;
            }
            if c == '\\' {
                i += 1;
                if let Some(e) = s[i..].chars().next() {
                    word.push(e);
                    i += e.len_utf8();
                }
                continue;
            }
            if c == '~' {
                let at = i;
                i += 1;
                let digits: String = s[i..].chars().take_while(char::is_ascii_digit).collect();
                i += digits.len();
                fuzzy = Some(match digits.as_str() {
                    "" => Fuzziness::Auto,
                    "1" => Fuzziness::One,
                    "2" => Fuzziness::Two,
                    _ => return Err((at, QueryErrorKind::Distance)),
                });
                // The word ends at its `~`; anything glued on after the distance is refused.
                if let Some(c) = s[i..].chars().next()
                    && !c.is_whitespace()
                    && c != '"'
                {
                    return Err((at, QueryErrorKind::Distance));
                }
                break;
            }
            word.push(c);
            i += c.len_utf8();
        }
        match fuzzy {
            Some(fuzziness) => q.push(Clause::Fuzzy {
                term: word,
                fuzziness,
            }),
            None if !word.is_empty() => q.push(Clause::Text(word)),
            None => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clauses(s: &str) -> Vec<Clause> {
        Query::parse(s).unwrap().clauses().to_vec()
    }

    fn err(s: &str) -> (usize, QueryErrorKind) {
        match Query::parse(s) {
            Err(Error::InvalidQuery { at, reason }) => (at, reason),
            other => panic!("{s:?} parsed: {other:?}"),
        }
    }

    #[test]
    fn words_phrases_and_fuzzy_terms() {
        assert_eq!(
            clauses(r#"postgres "March meeting" postgress~ x~1 y~2"#),
            [
                Clause::Text("postgres".into()),
                Clause::QuotedPhrase {
                    text: "March meeting".into(),
                    slop: 0
                },
                Clause::Fuzzy {
                    term: "postgress".into(),
                    fuzziness: Fuzziness::Auto
                },
                Clause::Fuzzy {
                    term: "x".into(),
                    fuzziness: Fuzziness::One
                },
                Clause::Fuzzy {
                    term: "y".into(),
                    fuzziness: Fuzziness::Two
                },
            ]
        );
    }

    #[test]
    fn repeats_count_once_and_escapes_are_literal() {
        assert_eq!(
            clauses(r#"a a "b c" "b c" a\~b q\"x"#),
            [
                Clause::Text("a".into()),
                Clause::QuotedPhrase {
                    text: "b c".into(),
                    slop: 0
                },
                Clause::Text("a~b".into()),
                Clause::Text("q\"x".into()),
            ]
        );
        assert!(clauses("   ").is_empty());
    }

    #[test]
    fn refusals_name_their_byte_offset() {
        assert_eq!(err(r#"a "b c"#), (2, QueryErrorKind::UnclosedQuote));
        assert_eq!(err(r#""b c"~"#), (5, QueryErrorKind::Slop));
        assert_eq!(err(r#""b c"~65536"#), (5, QueryErrorKind::Slop));
        assert_eq!(err(r#""b c"~2x"#), (5, QueryErrorKind::Slop));
        assert_eq!(err(r#""b~ c""#), (2, QueryErrorKind::FuzzyInPhrase));
        assert_eq!(err("é x~3"), (4, QueryErrorKind::Distance));
        assert_eq!(err("x~1y"), (1, QueryErrorKind::Distance));
    }

    #[test]
    fn a_phrase_takes_a_slop() {
        assert_eq!(
            clauses(r#""b c"~2 "b c""#),
            [
                Clause::QuotedPhrase {
                    text: "b c".into(),
                    slop: 2
                },
                Clause::QuotedPhrase {
                    text: "b c".into(),
                    slop: 0
                },
            ]
        );
    }

    #[test]
    fn a_str_query_keeps_its_parse_error_for_the_search() {
        let q = Query::from(r#""open"#);
        assert!(q.clauses().is_empty());
        assert!(matches!(
            q.check(),
            Err(Error::InvalidQuery {
                at: 0,
                reason: QueryErrorKind::UnclosedQuote
            })
        ));
    }

    #[test]
    fn the_builder_keeps_terms_as_given() {
        let q = Query::new()
            .term("March")
            .phrase(["march", "meeting"])
            .text("Postgres")
            .term("March");
        assert_eq!(
            q.clauses(),
            [
                Clause::Term("March".into()),
                Clause::Phrase {
                    words: vec!["march".into(), "meeting".into()],
                    slop: 0
                },
                Clause::Text("Postgres".into()),
            ]
        );
    }
}
