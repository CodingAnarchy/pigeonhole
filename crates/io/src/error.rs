use std::fmt;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Broad classes of I/O failure that callers branch on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The path or shared-memory object does not exist.
    NotFound,
    /// Exclusive creation found an existing object.
    AlreadyExists,
    /// A byte-range lock is held by someone else (never blocks).
    Locked,
    /// The device is full (`ENOSPC`).
    NoSpace,
    /// A short read: the range extends past end of file.
    UnexpectedEof,
    /// The operation is not supported by this backend or platform.
    Unsupported,
    /// The simulated process crashed; every further operation fails until restart.
    Crashed,
    /// Anything else; see the source error.
    Other,
}

/// An I/O error: a kind for branching, a static context and the OS error if any.
#[derive(Debug)]
pub struct Error {
    /// The class of failure.
    pub kind: ErrorKind,
    /// What was being attempted.
    pub context: &'static str,
    /// The underlying OS error, if any.
    pub source: Option<std::io::Error>,
}

impl Error {
    /// An error without an OS source.
    pub fn new(kind: ErrorKind, context: &'static str) -> Self {
        Self {
            kind,
            context,
            source: None,
        }
    }

    /// Wraps an OS error, classifying it.
    pub fn os(context: &'static str, source: std::io::Error) -> Self {
        todo!()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e as _)
    }
}
