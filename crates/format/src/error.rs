//! Decode and encode errors.

use std::fmt;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An encoding or decoding failure. Decoders return these instead of panicking on any input.
///
/// ```
/// use pigeonhole_format::{Error, decode_key};
///
/// assert!(matches!(decode_key(b"short"), Err(Error::Truncated { .. })));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The input ended before the structure did.
    Truncated {
        /// What was being decoded.
        what: &'static str,
    },
    /// A magic number did not match.
    BadMagic {
        /// What was being decoded.
        what: &'static str,
    },
    /// A checksum did not match the bytes it covers.
    Checksum {
        /// What was being decoded.
        what: &'static str,
    },
    /// The structure's version is newer (or older) than this build understands.
    UnsupportedVersion {
        /// What was being decoded.
        what: &'static str,
        /// The version found in the bytes.
        found: u32,
    },
    /// The bytes are structurally invalid (bad tag, bad length, out-of-range offset).
    Corrupt {
        /// What was being decoded.
        what: &'static str,
    },
    /// A row key or qualifier exceeds [`MAX_KEY_PART`](crate::key::MAX_KEY_PART).
    KeyTooLarge,
    /// A value exceeds [`MAX_VALUE_LEN`](crate::value::MAX_VALUE_LEN).
    ValueTooLarge,
    /// A compression codec this build does not support.
    UnsupportedCompression(u8),
    /// The caller passed arguments an encoder cannot accept (for example a
    /// [`FamilyDelete`](crate::Kind::FamilyDelete) cell key, or keys out of order). Never
    /// produced by a decoder: bad bytes are [`Error::Corrupt`].
    InvalidArgument {
        /// What was wrong.
        what: &'static str,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Truncated { what } => write!(f, "{what}: input truncated"),
            Error::BadMagic { what } => write!(f, "{what}: bad magic number"),
            Error::Checksum { what } => write!(f, "{what}: checksum mismatch"),
            Error::UnsupportedVersion { what, found } => {
                write!(f, "{what}: unsupported version {found}")
            }
            Error::Corrupt { what } => write!(f, "{what}: corrupt"),
            Error::KeyTooLarge => write!(f, "row key or qualifier longer than 65536 bytes"),
            Error::ValueTooLarge => write!(f, "value longer than 2^32 - 1 bytes"),
            Error::UnsupportedCompression(c) => write!(f, "unsupported compression codec {c}"),
            Error::InvalidArgument { what } => write!(f, "invalid argument: {what}"),
        }
    }
}

impl std::error::Error for Error {}
