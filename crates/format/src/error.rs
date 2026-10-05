//! Decode and encode errors.

use std::fmt;

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An encoding or decoding failure. Decoders return these instead of panicking on any input.
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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}
