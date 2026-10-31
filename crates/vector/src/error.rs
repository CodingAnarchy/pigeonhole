//! The crate's error.

use std::fmt;

use pigeonhole::ErrorCode;

/// An index operation's error.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The database refused or failed the operation.
    Db(pigeonhole::Error),
    /// The arguments don't fit the index (a vector of another element type or dimension,
    /// an option out of range), or the table isn't an index.
    InvalidArgument(String),
    /// The index's rows contradict each other ([`Index::verify`](crate::Index::verify)), or
    /// a row the index wrote doesn't parse.
    Corruption(String),
    /// The layout kept changing under an insert or delete: it reloaded the centroids more
    /// often than its bound and gave up. Retrying the call is safe.
    Conflict(String),
}

impl Error {
    /// The closest Pigeonhole error code, for callers that branch on codes.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Db(e) => e.code(),
            Error::InvalidArgument(_) => ErrorCode::InvalidArgument,
            Error::Corruption(_) => ErrorCode::Corruption,
            Error::Conflict(_) => ErrorCode::Conflict,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Db(e) => e.fmt(f),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::Corruption(m) => write!(f, "index corruption: {m}"),
            Error::Conflict(m) => write!(f, "conflict: {m}"),
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
