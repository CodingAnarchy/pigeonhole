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
    // After `Other`, so the existing variants keep their discriminants.
    /// Direct I/O at an offset, length or buffer address not aligned to the handle's
    /// [`File::direct_align`](crate::File::direct_align) (#403).
    Misaligned,
}

impl ErrorKind {
    fn describe(self) -> &'static str {
        match self {
            ErrorKind::NotFound => "not found",
            ErrorKind::AlreadyExists => "already exists",
            ErrorKind::Locked => "locked",
            ErrorKind::NoSpace => "no space left on device",
            ErrorKind::UnexpectedEof => "unexpected end of file",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::Crashed => "crashed",
            ErrorKind::Misaligned => "misaligned direct I/O",
            ErrorKind::Other => "I/O error",
        }
    }
}

/// An I/O error: a kind for branching, a static context and the OS error if any.
///
/// ```
/// use pigeonhole_io::{Error, ErrorKind};
///
/// let e = Error::os("open data.phdb", std::io::Error::from(std::io::ErrorKind::NotFound));
/// assert_eq!(e.kind, ErrorKind::NotFound);
/// assert!(e.to_string().starts_with("open data.phdb: not found"));
/// ```
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
        use std::io::ErrorKind as K;
        let kind = match source.kind() {
            K::NotFound => ErrorKind::NotFound,
            K::AlreadyExists => ErrorKind::AlreadyExists,
            K::UnexpectedEof => ErrorKind::UnexpectedEof,
            K::StorageFull | K::QuotaExceeded => ErrorKind::NoSpace,
            K::Unsupported => ErrorKind::Unsupported,
            _ => ErrorKind::Other,
        };
        Self {
            kind,
            context,
            source: Some(source),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.context, self.kind.describe())?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e as _)
    }
}
