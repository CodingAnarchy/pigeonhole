//! The crate's errors.

use std::path::PathBuf;

/// A `pigeonhole-embed` result.
pub type Result<T> = std::result::Result<T, Error>;

/// Why a load or an embedding failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A model file couldn't be read, written or renamed.
    Io {
        /// The file.
        path: PathBuf,
        /// The operating system's error.
        source: std::io::Error,
    },
    /// A model file's SHA-256 isn't the one pinned for the model's revision: a corrupt or
    /// different file. A download that fails the check is deleted; a file found in a
    /// directory or the cache is left as it is.
    Checksum {
        /// The file.
        path: PathBuf,
        /// The pinned SHA-256, in hex.
        expected: String,
        /// The file's SHA-256, in hex.
        actual: String,
    },
    /// A model file isn't in the directory or the cache, and it may not be downloaded
    /// (offline, or built without the `download` feature). The message says where to fetch
    /// it from and where to put it.
    Missing {
        /// Where the file was looked for.
        path: PathBuf,
        /// The pinned revision's URL for it.
        url: String,
        /// Why it wasn't downloaded.
        why: &'static str,
    },
    /// A download failed (the network, an HTTP status, or a redirect off huggingface.co).
    Download {
        /// The URL.
        url: String,
        /// What went wrong.
        message: String,
    },
    /// The model couldn't be built from its files, or inference failed.
    Model(String),
    /// The tokenizer couldn't be loaded, or a text couldn't be tokenised.
    Tokenizer(String),
    /// The embedder's thread pool couldn't be started.
    Threads(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io { path, source } => write!(f, "{}: {source}", path.display()),
            Error::Checksum {
                path,
                expected,
                actual,
            } => write!(
                f,
                "{}: SHA-256 {actual}, expected {expected} (a corrupt or different file)",
                path.display()
            ),
            Error::Missing { path, url, why } => write!(
                f,
                "{} is missing and {why}: fetch {url} and put it there, or load from a \
                 directory holding the model's files (LoadOptions::dir)",
                path.display()
            ),
            Error::Download { url, message } => write!(f, "downloading {url}: {message}"),
            Error::Model(m) => write!(f, "the model: {m}"),
            Error::Tokenizer(m) => write!(f, "the tokenizer: {m}"),
            Error::Threads(m) => write!(f, "the embedder's thread pool: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// A candle error as [`Error::Model`]. Not a `From` impl: that would put candle's type in
/// this crate's public API, so every candle minor would be a breaking change here.
pub(crate) fn model(e: candle_core::Error) -> Error {
    Error::Model(e.to_string())
}
