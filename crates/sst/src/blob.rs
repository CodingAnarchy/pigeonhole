//! Blob extents (FORMAT §7): a logical blob file is a list of equal-size extents whose payload
//! areas (after a 64-byte header each) concatenate into one address space. A record is
//! `len u64, xxh3 u64, value` at a logical offset and may span extents.

use std::sync::Arc;

use pigeonhole_cache::{BlockCache, BlockData, BlockKey, Cell, Priority};
use pigeonhole_format::blob::{
    BLOB_EXTENT_HEADER_LEN, BLOB_RECORD_HEADER_LEN, BlobExtentHeader, encode_record_header,
    verify_record,
};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::value::BlobPointer;
use pigeonhole_format::{BlobFileId, Error as FormatError, FormatVersion};
use pigeonhole_io::{FileRef, IoBuf};

use crate::{Error, Result};

/// Payload bytes per extent of `size_class`.
fn payload_len(size_class: u8) -> u64 {
    ExtentRef {
        page: 0,
        size_class,
    }
    .len()
    .saturating_sub(BLOB_EXTENT_HEADER_LEN as u64)
}

/// Calls `f(absolute file offset, range of bytes)` for each extent-contiguous piece of the
/// logical range `[offset, offset + len)`. Fails if the range runs past the extents.
fn for_each_piece(
    extents: &[ExtentRef],
    payload: u64,
    offset: u64,
    len: usize,
    mut f: impl FnMut(u64, std::ops::Range<usize>) -> Result<()>,
) -> Result<()> {
    let corrupt = || {
        Error::Format(FormatError::Corrupt {
            what: "blob pointer offset",
        })
    };
    if payload == 0 {
        return Err(corrupt());
    }
    let end = offset.checked_add(len as u64).ok_or_else(corrupt)?;
    if end > payload.saturating_mul(extents.len() as u64) {
        return Err(corrupt());
    }
    let mut done = 0;
    while done < len {
        let at = offset + done as u64;
        let extent = extents[(at / payload) as usize];
        let within = at % payload;
        let n = ((payload - within) as usize).min(len - done);
        f(
            extent.offset() + BLOB_EXTENT_HEADER_LEN as u64 + within,
            done..done + n,
        )?;
        done += n;
    }
    Ok(())
}

pub(crate) struct Writer {
    file: FileRef,
    blob_file: BlobFileId,
    size_class: u8,
    payload: u64,
    extents: Vec<ExtentRef>,
    /// Logical bytes appended.
    pos: u64,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobWriter")
            .field("blob_file", &self.blob_file)
            .field("extents", &self.extents.len())
            .field("bytes", &self.pos)
            .finish()
    }
}

impl Writer {
    pub(crate) fn new(file: FileRef, blob_file: BlobFileId, size_class: u8) -> Self {
        Self {
            file,
            blob_file,
            size_class,
            payload: payload_len(size_class),
            extents: Vec::new(),
            pos: 0,
        }
    }

    fn capacity(&self) -> u64 {
        self.payload.saturating_mul(self.extents.len() as u64)
    }

    pub(crate) fn needs_extent(&self, len: usize) -> bool {
        self.pos + (BLOB_RECORD_HEADER_LEN + len) as u64 > self.capacity()
    }

    pub(crate) fn add_extent(&mut self, extent: ExtentRef) -> Result<()> {
        if extent.size_class != self.size_class {
            return Err(Error::Format(FormatError::InvalidArgument {
                what: "blob extent of the wrong size class",
            }));
        }
        let header = BlobExtentHeader {
            version: FormatVersion::CURRENT,
            blob_file: self.blob_file,
            extent_index: self.extents.len() as u32,
        };
        self.file.write_at(&header.encode(), extent.offset())?;
        self.extents.push(extent);
        Ok(())
    }

    pub(crate) fn append(&mut self, value: &[u8]) -> Result<BlobPointer> {
        let len = u32::try_from(value.len()).map_err(|_| FormatError::ValueTooLarge)?;
        if self.needs_extent(value.len()) {
            return Err(Error::ExtentFull);
        }
        let offset = self.pos;
        let header = encode_record_header(value);
        for (at, bytes) in [(offset, &header[..]), (offset + header.len() as u64, value)] {
            for_each_piece(&self.extents, self.payload, at, bytes.len(), |abs, r| {
                Ok(self.file.write_at(&bytes[r], abs)?)
            })?;
        }
        self.pos += (BLOB_RECORD_HEADER_LEN + value.len()) as u64;
        Ok(BlobPointer {
            blob_file: self.blob_file,
            len,
            offset,
        })
    }

    pub(crate) fn finish(self) -> (Vec<ExtentRef>, u64) {
        (self.extents, self.pos)
    }
}

pub(crate) struct Reader {
    file: FileRef,
    blob_file: BlobFileId,
    extents: Vec<ExtentRef>,
    payload: u64,
    cache: Arc<BlockCache>,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReader")
            .field("blob_file", &self.blob_file)
            .field("extents", &self.extents.len())
            .finish()
    }
}

impl Reader {
    pub(crate) fn new(
        file: FileRef,
        blob_file: BlobFileId,
        extents: Vec<ExtentRef>,
        cache: Arc<BlockCache>,
    ) -> Self {
        let payload = extents.first().map_or(0, |e| payload_len(e.size_class));
        Self {
            file,
            blob_file,
            extents,
            payload,
            cache,
        }
    }

    /// Reads a record (header and value) into the cache, verified; hits skip verification.
    pub(crate) fn read(&self, ptr: &BlobPointer) -> Result<Cell> {
        if ptr.blob_file != self.blob_file {
            return Err(Error::Format(FormatError::InvalidArgument {
                what: "blob pointer names another blob file",
            }));
        }
        let key = BlockKey {
            file: crate::blob_cache_file(self.blob_file),
            offset: ptr.offset,
        };
        let start = BLOB_RECORD_HEADER_LEN as u32;
        let Some(end) = start.checked_add(ptr.len) else {
            return Err(Error::Format(FormatError::Corrupt {
                what: "blob pointer length",
            }));
        };
        let range = start..end;
        if let Some(h) = self.cache.get(key)
            && h.len() == range.end as usize
        {
            return Ok(Cell::in_block(h, range));
        }
        let total = BLOB_RECORD_HEADER_LEN + ptr.len as usize;
        // Bounds first, so a bad pointer never sizes a buffer past the blob file.
        for_each_piece(
            &self.extents,
            self.payload,
            ptr.offset,
            total,
            |_, _| Ok(()),
        )?;
        let mut buf = IoBuf::zeroed(total);
        for_each_piece(&self.extents, self.payload, ptr.offset, total, |abs, r| {
            Ok(self.file.read_at(&mut buf[r], abs)?)
        })?;
        let (header, value) = buf.split_at(BLOB_RECORD_HEADER_LEN);
        verify_record(header, value, ptr.len)?;
        let h = self.cache.insert(key, BlockData::Io(buf), Priority::Low);
        Ok(Cell::in_block(h, range))
    }
}
