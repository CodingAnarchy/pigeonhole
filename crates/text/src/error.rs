//! The crate's error.

use std::fmt;

use pigeonhole::ErrorCode;

/// An index operation's error.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The database refused or failed the operation.
    Db(pigeonhole::Error),
    /// The arguments don't fit the index: an option out of range, a document with more
    /// distinct terms than [`IndexOptions::max_terms`](crate::IndexOptions::max_terms), a
    /// valid query using a feature the index lacks (a phrase without positions), or a table
    /// that isn't an index.
    InvalidArgument(String),
    /// A query string didn't parse ([`Query::parse`](crate::Query::parse)). `at` is a byte
    /// offset into the string.
    InvalidQuery {
        /// Where in the query string, in bytes.
        at: usize,
        /// What was wrong there.
        reason: QueryErrorKind,
    },
    /// The index is being repaired ([`Index::repair`](crate::Index::repair)): writes through
    /// it and searches of it wait until the repair finishes.
    Busy,
    /// The index was created with another tokeniser (its name or fingerprint differs).
    Mismatch(String),
    /// The index's rows don't parse.
    Corruption(String),
}

/// Why a query string didn't parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum QueryErrorKind {
    /// A `"` with no closing `"`.
    UnclosedQuote,
    /// A phrase's slop (`"..."~N`) is missing, above 65,535, or has something glued to it.
    Slop,
    /// A `~` inside a phrase: a phrase's words can't be fuzzy.
    FuzzyInPhrase,
    /// A fuzzy distance other than `~`, `~1` or `~2`.
    Distance,
}

impl fmt::Display for QueryErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            QueryErrorKind::UnclosedQuote => "a quote is never closed",
            QueryErrorKind::Slop => "a phrase's slop is `~N`, with N from 0 to 65,535",
            QueryErrorKind::FuzzyInPhrase => "a phrase's words can't be fuzzy",
            QueryErrorKind::Distance => "a fuzzy distance is `~`, `~1` or `~2`",
        })
    }
}

impl Error {
    /// The closest Pigeonhole error code, for callers that branch on codes.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Db(e) => e.code(),
            Error::InvalidArgument(_) | Error::InvalidQuery { .. } | Error::Mismatch(_) => {
                ErrorCode::InvalidArgument
            }
            Error::Busy => ErrorCode::Busy,
            Error::Corruption(_) => ErrorCode::Corruption,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Db(e) => e.fmt(f),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::InvalidQuery { at, reason } => write!(f, "invalid query at byte {at}: {reason}"),
            Error::Busy => f.write_str("the index is being repaired"),
            Error::Mismatch(m) => write!(f, "tokeniser mismatch: {m}"),
            Error::Corruption(m) => write!(f, "index corruption: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Db(e) => Some(e),
            _ => None,
        }
    }
}

impl From<pigeonhole::Error> for Error {
    fn from(e: pigeonhole::Error) -> Self {
        Error::Db(e)
    }
}

/// An index operation's result.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn corrupt(what: &str) -> Error {
    Error::Corruption(what.to_owned())
}
