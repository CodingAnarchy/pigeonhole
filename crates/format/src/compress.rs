//! Block compression codecs. The codec byte is stored in every block trailer.

/// A block compression codec. Numbers are frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Compression {
    /// Stored as is.
    None = 0,
    /// LZ4 block format (no frame), the default.
    #[default]
    Lz4 = 1,
    /// zstd, optionally with the family's trained dictionary (Phase 2; not yet supported).
    Zstd = 2,
}

impl Compression {
    /// Parses a codec byte.
    pub fn from_u8(b: u8) -> crate::Result<Self> {
        todo!()
    }
}

/// Compresses `input` with `codec`, appending to `out`. Returns the codec actually used:
/// [`Compression::None`] when compression would not save at least 1/8 of the size.
pub fn compress(codec: Compression, input: &[u8], out: &mut Vec<u8>) -> crate::Result<Compression> {
    todo!()
}

/// Decompresses `input` into `out`, which must be exactly `uncompressed_len` bytes long.
pub fn decompress(codec: Compression, input: &[u8], out: &mut [u8]) -> crate::Result<()> {
    todo!()
}
