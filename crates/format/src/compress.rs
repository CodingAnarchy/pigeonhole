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
    Compressor::default().compress(codec, level, input, out)
}

/// Compression state kept across blocks, so a writer compressing many blocks allocates and
/// zero-fills once rather than per block: the LZ4 hash table, the zstd context, and a scratch
/// buffer the codec writes into (sized to the largest output yet, zeroed only when it
/// grows), from which only the compressed bytes are copied out. The results are the same as
/// [`compress_with_level`]'s.
///
/// ```
/// use pigeonhole_format::compress::{Compression, Compressor, decompress};
///
/// let mut c = Compressor::default();
/// let mut packed = Vec::new();
/// for block in [vec![7u8; 4096], vec![9u8; 4096]] {
///     packed.clear();
///     assert_eq!(c.compress(Compression::Lz4, 3, &block, &mut packed).unwrap(), Compression::Lz4);
///     let mut back = vec![0; block.len()];
///     decompress(Compression::Lz4, &packed, &mut back).unwrap();
///     assert_eq!(back, block);
/// }
/// ```
#[derive(Default)]
pub struct Compressor {
    lz4: Option<lz4_flex::block::CompressTable>,
    zstd: Option<(i8, zstd::bulk::Compressor<'static>)>,
    scratch: Vec<u8>,
}

impl std::fmt::Debug for Compressor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compressor")
            .field("scratch", &self.scratch.len())
            .finish_non_exhaustive()
    }
}

impl Compressor {
    /// Compresses `input` with `codec` (zstd at `level`), appending to `out`. Returns the codec
    /// actually used: [`Compression::None`] when compression would not save at least 1/8 of
    /// the size (`input` is then appended as is).
    pub fn compress(
        &mut self,
        codec: Compression,
        level: i8,
        input: &[u8],
        out: &mut Vec<u8>,
    ) -> crate::Result<Compression> {
        let n = match codec {
            Compression::None => None,
            Compression::Lz4 => {
                let max = lz4_flex::block::get_maximum_output_size(input.len());
                let table = self.lz4.get_or_insert_with(Default::default);
                let scratch = grow(&mut self.scratch, max);
                let n = lz4_flex::block::compress_into_with_table(input, scratch, table).map_err(
                    |_| Error::Corrupt {
                        what: "lz4 compress",
                    },
                )?;
                Some(n)
            }
            Compression::Zstd => {
                let bound = zstd::zstd_safe::compress_bound(input.len());
                if self.zstd.as_ref().is_none_or(|(l, _)| *l != level) {
                    let c = zstd::bulk::Compressor::new(i32::from(level)).map_err(|_| {
                        Error::Corrupt {
                            what: "zstd compress",
                        }
                    })?;
                    self.zstd = Some((level, c));
                }
                let scratch = grow(&mut self.scratch, bound);
                let (_, c) = self.zstd.as_mut().expect("set above");
                let n = c
                    .compress_to_buffer(input, scratch)
                    .map_err(|_| Error::Corrupt {
                        what: "zstd compress",
                    })?;
                Some(n)
            }
        };
        // Keep the compressed form only if it saves at least 1/8 of the input.
        if let Some(n) = n
            && n < input.len()
            && (input.len() - n) * 8 >= input.len()
        {
            out.extend_from_slice(&self.scratch[..n]);
            return Ok(codec);
        }
        out.extend_from_slice(input);
        Ok(Compression::None)
    }
}

/// The first `len` bytes of `scratch`, grown (and zeroed) only past its largest size yet.
fn grow(scratch: &mut Vec<u8>, len: usize) -> &mut [u8] {
    if scratch.len() < len {
        scratch.resize(len, 0);
    }
    &mut scratch[..len]
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
