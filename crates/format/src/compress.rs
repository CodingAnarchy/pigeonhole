//! Block compression codecs. The codec byte is stored in every block trailer.
//!
//! ```
//! use pigeonhole_format::compress::{Compression, compress, decompress};
//!
//! let input = vec![7u8; 4096];
//! let mut packed = Vec::new();
//! assert_eq!(compress(Compression::Lz4, &input, &mut packed).unwrap(), Compression::Lz4);
//! let mut back = vec![0; input.len()];
//! decompress(Compression::Lz4, &packed, &mut back).unwrap();
//! assert_eq!(back, input);
//! ```

use crate::Error;

/// A block compression codec. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Compression {
    /// Stored as is.
    None = 0,
    /// LZ4 block format (no frame), the default.
    #[default]
    Lz4 = 1,
    /// zstd frames (libzstd) at the family's level. Trained dictionaries are not used yet.
    Zstd = 2,
}

impl Compression {
    /// Parses a codec byte.
    pub fn from_u8(b: u8) -> crate::Result<Self> {
        match b {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            _ => Err(Error::UnsupportedCompression(b)),
        }
    }
}

/// The zstd level [`compress`] uses (libzstd's default).
pub const DEFAULT_ZSTD_LEVEL: i8 = 3;

/// Compresses `input` with `codec`, appending to `out`. Returns the codec actually used:
/// [`Compression::None`] when compression would not save at least 1/8 of the size. zstd
/// runs at [`DEFAULT_ZSTD_LEVEL`]; see [`compress_with_level`].
pub fn compress(codec: Compression, input: &[u8], out: &mut Vec<u8>) -> crate::Result<Compression> {
    compress_with_level(codec, DEFAULT_ZSTD_LEVEL, input, out)
}

/// [`compress`] with a zstd `level` (libzstd's: negative is faster, up to 22 smaller; 0 is
/// its default; out-of-range levels are clamped). Other codecs ignore it.
///
/// ```
/// use pigeonhole_format::compress::{Compression, compress_with_level, decompress};
///
/// let input: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
/// let mut packed = Vec::new();
/// let used = compress_with_level(Compression::Zstd, 19, &input, &mut packed).unwrap();
/// assert_eq!(used, Compression::Zstd);
/// let mut back = vec![0; input.len()];
/// decompress(Compression::Zstd, &packed, &mut back).unwrap();
/// assert_eq!(back, input);
/// ```
pub fn compress_with_level(
    codec: Compression,
    level: i8,
    input: &[u8],
    out: &mut Vec<u8>,
) -> crate::Result<Compression> {
    match codec {
        Compression::None => {}
        Compression::Lz4 => {
            let start = out.len();
            out.resize(
                start + lz4_flex::block::get_maximum_output_size(input.len()),
                0,
            );
            let n = lz4_flex::block::compress_into(input, &mut out[start..]).map_err(|_| {
                Error::Corrupt {
                    what: "lz4 compress",
                }
            })?;
            out.truncate(start + n);
            // Keep the compressed form only if it saves at least 1/8 of the input.
            if n < input.len() && (input.len() - n) * 8 >= input.len() {
                return Ok(Compression::Lz4);
            }
            out.truncate(start);
        }
        Compression::Zstd => {
            let start = out.len();
            out.resize(start + zstd::zstd_safe::compress_bound(input.len()), 0);
            let n = zstd::bulk::compress_to_buffer(input, &mut out[start..], i32::from(level))
                .map_err(|_| Error::Corrupt {
                    what: "zstd compress",
                })?;
            out.truncate(start + n);
            if n < input.len() && (input.len() - n) * 8 >= input.len() {
                return Ok(Compression::Zstd);
            }
            out.truncate(start);
        }
    }
    out.extend_from_slice(input);
    Ok(Compression::None)
}

/// Decompresses `input` into `out`, which must be exactly `uncompressed_len` bytes long.
pub fn decompress(codec: Compression, input: &[u8], out: &mut [u8]) -> crate::Result<()> {
    match codec {
        Compression::None => {
            if input.len() != out.len() {
                return Err(Error::Corrupt {
                    what: "uncompressed block length",
                });
            }
            out.copy_from_slice(input);
            Ok(())
        }
        Compression::Lz4 => match lz4_flex::block::decompress_into(input, out) {
            Ok(n) if n == out.len() => Ok(()),
            _ => Err(Error::Corrupt { what: "lz4 block" }),
        },
        // Into a buffer of exactly the block's length: a frame that would decode to more
        // fails instead of growing it.
        Compression::Zstd => match zstd::bulk::decompress_to_buffer(input, out) {
            Ok(n) if n == out.len() => Ok(()),
            _ => Err(Error::Corrupt { what: "zstd block" }),
        },
    }
}
