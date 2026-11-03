//! The crate's error (D311 §10a, ICR 0107).

use std::fmt;

use pigeonhole::ErrorCode;

/// A memory store operation's error.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The database refused or failed the operation.
    Db(pigeonhole::Error),
    /// The text index refused or failed it (passed through, `Busy` during its repair
    /// included).
    Text(pigeonhole_text::Error),
    /// The arguments don't fit the store:
    /// - a bad namespace, or a malformed `MemoryId`;
    /// - a memory with neither text nor embedding;
    /// - an embedding of the wrong dimension, or given to a store without a model;
    /// - a write that adds content through a handle without the store's model;
    /// - a table that isn't a store.
    InvalidArgument(String),
    /// A link names a memory that doesn't exist. Its [`code`](Error::code) is
    /// `InvalidArgument`, since Pigeonhole's codes have no general not-found: match the
    /// variant to tell it apart.
    NotFound(String),
    /// The store was created with another embedding model than the one the handle names.
    Mismatch(String),
    /// The store's rows don't parse.
    Corruption(String),
    /// A [`ConsolidateLoop`](crate::ConsolidateLoop) of the same name started since this
    /// one last saved, and took the name over (D311 T9): this loop should stop. Its
    /// [`code`](Error::code) is `Conflict`.
    LoopTakenOver(String),
    /// The application's [`Consolidator`](crate::Consolidator) failed, which stops the pass
    /// ([`Store::consolidate`](crate::Store::consolidate)). The groups before it committed:
    /// `pass` counts them. Its [`code`](Error::code) is `MergeFailed`, the closest code: like
    /// a merge operator, the consolidator is application code that combines values. Match
    /// the variant to tell it apart.
    Consolidate {
        /// The consolidator's error.
        error: Box<dyn std::error::Error + Send + Sync>,
        /// The pass up to the failure.
        pass: crate::ConsolidatePass,
    },
}

impl Error {
    /// The closest Pigeonhole error code, for callers that branch on codes.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Db(e) => e.code(),
            Error::Text(e) => e.code(),
            Error::InvalidArgument(_) | Error::NotFound(_) | Error::Mismatch(_) => {
                ErrorCode::InvalidArgument
            }
            Error::Corruption(_) => ErrorCode::Corruption,
            Error::LoopTakenOver(_) => ErrorCode::Conflict,
            Error::Consolidate { .. } => ErrorCode::MergeFailed,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Db(e) => e.fmt(f),
            Error::Text(e) => e.fmt(f),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::NotFound(m) => write!(f, "not found: {m}"),
            Error::Mismatch(m) => write!(f, "embedding model mismatch: {m}"),
            Error::Corruption(m) => write!(f, "store corruption: {m}"),
            Error::LoopTakenOver(name) => write!(
                f,
                "another consolidation loop named {name:?} started on this store and took it over"
            ),
            Error::Consolidate { error, .. } => write!(f, "the consolidator failed: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Db(e) => Some(e),
            Error::Text(e) => Some(e),
            Error::Consolidate { error, .. } => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<pigeonhole::Error> for Error {
    fn from(e: pigeonhole::Error) -> Self {
        Error::Db(e)
    }
}

impl From<pigeonhole_text::Error> for Error {
    fn from(e: pigeonhole_text::Error) -> Self {
        match e {
            // The text index's database errors are the database's.
            pigeonhole_text::Error::Db(e) => Error::Db(e),
            e => Error::Text(e),
        }
    }
}

/// A memory store operation's result.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn corrupt(what: &str) -> Error {
    Error::Corruption(what.to_owned())
}
