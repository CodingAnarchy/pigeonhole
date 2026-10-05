//! Bounds-checked little-endian readers shared by the decoders. Nothing here panics.

use crate::{Error, Result};

/// A forward reader over a byte slice. Every read checks bounds and reports `Truncated`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    what: &'static str,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8], what: &'static str) -> Self {
        Self { buf, pos: 0, what }
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::Truncated { what: self.what });
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn i8(&mut self) -> Result<i8> {
        Ok(self.u8()? as i8)
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8]> {
        let (b, n) = crate::varint::get_bytes(self.rest()).map_err(|e| self.relabel(e))?;
        self.pos += n;
        Ok(b)
    }

    pub(crate) fn string(&mut self) -> Result<String> {
        let b = self.bytes()?;
        std::str::from_utf8(b)
            .map(str::to_owned)
            .map_err(|_| Error::Corrupt { what: self.what })
    }

    fn relabel(&self, e: Error) -> Error {
        match e {
            Error::Truncated { .. } => Error::Truncated { what: self.what },
            _ => Error::Corrupt { what: self.what },
        }
    }
}

/// Reads a little-endian `u32` at `off`; the caller has checked bounds.
pub(crate) fn le_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

/// Reads a little-endian `u64` at `off`; the caller has checked bounds.
pub(crate) fn le_u64(b: &[u8], off: usize) -> u64 {
    let mut a = [0; 8];
    a.copy_from_slice(&b[off..off + 8]);
    u64::from_le_bytes(a)
}

/// Writes `v` at `off` of a fixed buffer.
pub(crate) fn put_at(buf: &mut [u8], off: usize, v: &[u8]) {
    buf[off..off + v.len()].copy_from_slice(v);
}
