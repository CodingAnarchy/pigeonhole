//! Blob extents (FORMAT §7): a logical blob file is a list of equal-size extents whose payload
//! areas (after a 64-byte header each) concatenate into one address space. A record is
//! `len u64, xxh3 u64, value` at a logical offset and may span extents.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

/// Calls `f(extent index, absolute file offset, range of bytes)` for each extent-contiguous
/// piece of the logical range `[offset, offset + len)`. Fails if the range runs past the
/// extents.
fn for_each_piece(
    extents: &[ExtentRef],
    payload: u64,
    offset: u64,
    len: usize,
    mut f: impl FnMut(usize, u64, std::ops::Range<usize>) -> Result<()>,
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
        let index = (at / payload) as usize;
        let extent = extents[index];
        let within = at % payload;
        let n = ((payload - within) as usize).min(len - done);
        f(
            index,
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
            for_each_piece(&self.extents, self.payload, at, bytes.len(), |_, abs, r| {
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
    /// Hands out unshared handles for records too large to cache.
    uncached: BlockCache,
    /// Records above this many bytes are not cached.
    cache_limit: usize,
    /// Per extent: whether its header has been verified.
    verified: Vec<AtomicBool>,
}

/// Records larger than this are never cached (nor larger than an eighth of the cache).
const MAX_CACHED_RECORD: usize = 1 << 20;

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
            verified: extents.iter().map(|_| AtomicBool::new(false)).collect(),
            extents,
            payload,
            cache_limit: MAX_CACHED_RECORD.min(cache.capacity() / 8),
            cache,
            uncached: BlockCache::disabled(),
        }
    }

    /// Verifies extent `i`'s header (FORMAT §7: magic, version, checksum, blob file and
    /// position) the first time a read touches it.
    fn verify_extent(&self, i: usize) -> Result<()> {
        if self.verified[i].load(Ordering::Acquire) {
            return Ok(());
        }
        let mut b = [0; BLOB_EXTENT_HEADER_LEN];
        crate::reader::read_into(&self.file, &mut b, self.extents[i].offset())?;
        self.check_header(i, &b)
    }

    /// Checks extent `i`'s header bytes (FORMAT §7) and records the extent as verified.
    pub(crate) fn check_header(&self, i: usize, b: &[u8]) -> Result<()> {
        let extent = self.extents[i];
        let h = BlobExtentHeader::decode(b)?;
        if h.blob_file != self.blob_file
            || h.extent_index as usize != i
            || extent.size_class != self.extents[0].size_class
        {
            return Err(Error::Format(FormatError::Corrupt {
                what: "blob extent header",
            }));
        }
        self.verified[i].store(true, Ordering::Release);
        Ok(())
    }

    /// Whether extent `i`'s header has been verified.
    pub(crate) fn is_verified(&self, i: usize) -> bool {
        self.verified[i].load(Ordering::Acquire)
    }

    /// A cache-only read of `ptr` (ICR 0014): the value if its record is cached; otherwise
    /// `Error::WouldBlock` with the fetch it needs (the header of the first extent it
    /// touches that is not verified yet, then the record itself); or `None` for a record
    /// too large to cache, which the caller reads synchronously (D196, #398).
    pub(crate) fn read_cache_only(
        &self,
        owner: &Arc<crate::BlobReader>,
        ptr: &BlobPointer,
    ) -> Result<Option<Cell>> {
        if ptr.blob_file != self.blob_file {
            return Err(Error::Format(FormatError::InvalidArgument {
                what: "blob pointer names another blob file",
            }));
        }
        if let Some(cell) = self.cached(ptr) {
            return Ok(Some(cell));
        }
        let Some(total) = (ptr.len as usize).checked_add(BLOB_RECORD_HEADER_LEN) else {
            return Err(Error::Format(FormatError::Corrupt {
                what: "blob pointer length",
            }));
        };
        let mut unverified = None;
        for_each_piece(&self.extents, self.payload, ptr.offset, total, |i, _, _| {
            if unverified.is_none() && !self.is_verified(i) {
                unverified = Some(i);
            }
            Ok(())
        })?;
        if let Some(i) = unverified {
            return Err(Error::WouldBlock(Box::new(crate::Fetch::blob_header(
                self.file.clone(),
                Arc::clone(owner),
                i,
                self.extents[i].offset(),
                BLOB_EXTENT_HEADER_LEN,
            ))));
        }
        if total > self.cache_limit {
            return Ok(None);
        }
        let mut pieces = Vec::new();
        for_each_piece(
            &self.extents,
            self.payload,
            ptr.offset,
            total,
            |_, abs, r| {
                pieces.push((abs, r));
                Ok(())
            },
        )?;
        Err(Error::WouldBlock(Box::new(crate::Fetch::blob_record(
            self.file.clone(),
            Arc::clone(owner),
            *ptr,
            pieces,
            total,
        ))))
    }

    /// Verifies a record a cache-only read fetched and caches it (it is within the cache
    /// limit, or the read would not have fetched it).
    pub(crate) fn admit_record(
        &self,
        ptr: &BlobPointer,
        buf: IoBuf,
    ) -> Result<pigeonhole_cache::BlockHandle> {
        let (header, value) = buf.split_at(BLOB_RECORD_HEADER_LEN);
        verify_record(header, value, ptr.len)?;
        let key = BlockKey {
            file: crate::blob_cache_file(self.blob_file),
            offset: ptr.offset,
        };
        Ok(self.cache.insert(key, BlockData::Io(buf), Priority::Low))
    }

    /// The record's value if its record is cached (a lookup only: never reads the file).
    pub(crate) fn cached(&self, ptr: &BlobPointer) -> Option<Cell> {
        if ptr.blob_file != self.blob_file {
            return None;
        }
        let key = BlockKey {
            file: crate::blob_cache_file(self.blob_file),
            offset: ptr.offset,
        };
        let start = BLOB_RECORD_HEADER_LEN as u32;
        let end = start.checked_add(ptr.len)?;
        let h = self.cache.get(key)?;
        (h.len() == end as usize).then(|| Cell::in_block(h, start..end))
    }

    /// Reads a record (header and value), verified, into the cache unless it is large; hits skip
    /// verification.
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
        // Bounds and extent headers first, so a bad pointer never sizes a buffer past the
        // blob file.
        for_each_piece(&self.extents, self.payload, ptr.offset, total, |i, _, _| {
            self.verify_extent(i)
        })?;
        let mut buf = IoBuf::zeroed(total);
        for_each_piece(
            &self.extents,
            self.payload,
            ptr.offset,
            total,
            |_, abs, r| crate::reader::read_into(&self.file, &mut buf[r], abs),
        )?;
        let (header, value) = buf.split_at(BLOB_RECORD_HEADER_LEN);
        verify_record(header, value, ptr.len)?;
        let cache = if total <= self.cache_limit {
            &*self.cache
        } else {
            &self.uncached
        };
        let h = cache.insert(key, BlockData::Io(buf), Priority::Low);
        Ok(Cell::in_block(h, range))
    }
}
