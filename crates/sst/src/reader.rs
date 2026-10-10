//! Opening an SST and reading its blocks through the block cache.

use std::sync::Arc;

use pigeonhole_cache::{BlockCache, BlockData, BlockHandle, BlockKey, Priority};
use pigeonhole_format::Error as FormatError;
use pigeonhole_format::SstId;
use pigeonhole_format::block::{Block, BlockAddr, BlockKind, TRAILER_LEN, verify};
use pigeonhole_format::compress::{Compression, decompress};
use pigeonhole_format::filter::Filter;
use pigeonhole_format::manifest::SstMeta;
use pigeonhole_format::sst::{FOOTER_LEN, Footer, Properties};
use pigeonhole_io::{FileRef, IoBuf};

use crate::Result;

pub(crate) fn corrupt(what: &'static str) -> crate::Error {
    crate::Error::Format(FormatError::Corrupt { what })
}

/// What a cache-only read missed (`Error::WouldBlock`, ICR 0014): the read of the file it
/// needs (one range, or the pieces of a blob record that spans extents) and what to do with
/// the bytes. The caller submits the read ([`Fetch::submit`]), admits the bytes when it
/// completes ([`Fetch::admit`], which verifies them, keeps them, and returns a pinned handle
/// when there is one to hold), and reads again.
#[derive(Clone)]
pub struct Fetch {
    file: FileRef,
    /// Absolute file offset of each piece, and where it goes in the assembled bytes.
    pieces: Vec<(u64, std::ops::Range<usize>)>,
    len: usize,
    then: Then,
}

#[derive(Clone)]
enum Then {
    /// A block of this kind (checksummed and maybe compressed on disk), into the cache.
    Block {
        cache: Arc<BlockCache>,
        key: BlockKey,
        kind: BlockKind,
        priority: Priority,
    },
    /// An SST's footer, cached as its raw bytes for a cache-only open.
    Footer {
        cache: Arc<BlockCache>,
        key: BlockKey,
        priority: Priority,
    },
    /// A blob extent's header, verified and recorded as such in its reader.
    BlobHeader {
        reader: Arc<crate::BlobReader>,
        index: usize,
    },
    /// A blob record, verified and cached.
    BlobRecord {
        reader: Arc<crate::BlobReader>,
        ptr: pigeonhole_format::value::BlobPointer,
    },
}

impl std::fmt::Debug for Fetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.then {
            Then::Block { kind, .. } => format!("{kind:?}"),
            Then::Footer { .. } => "Footer".into(),
            Then::BlobHeader { index, .. } => format!("BlobHeader({index})"),
            Then::BlobRecord { .. } => "BlobRecord".into(),
        };
        f.debug_struct("Fetch")
            .field("offset", &self.pieces.first().map(|p| p.0))
            .field("len", &self.len)
            .field("pieces", &self.pieces.len())
            .field("kind", &kind)
            .finish()
    }
}

impl Fetch {
    fn one(file: FileRef, offset: u64, len: usize, then: Then) -> Self {
        Self {
            file,
            pieces: vec![(offset, 0..len)],
            len,
            then,
        }
    }

    pub(crate) fn blob_header(
        file: FileRef,
        reader: Arc<crate::BlobReader>,
        index: usize,
        offset: u64,
        len: usize,
    ) -> Self {
        Self::one(file, offset, len, Then::BlobHeader { reader, index })
    }

    pub(crate) fn blob_record(
        file: FileRef,
        reader: Arc<crate::BlobReader>,
        ptr: pigeonhole_format::value::BlobPointer,
        pieces: Vec<(u64, std::ops::Range<usize>)>,
        len: usize,
    ) -> Self {
        Self {
            file,
            pieces,
            len,
            then: Then::BlobRecord { reader, ptr },
        }
    }

    /// One read of `len` bytes at `offset`. On a direct handle (#403) it covers the aligned
    /// pages around them, and the buffer it resolves to keeps just them, in place.
    fn submit_piece(&self, offset: u64, len: usize) -> pigeonhole_io::Completion {
        match self.file.direct_align() {
            None => self.file.submit_read(IoBuf::zeroed(len), offset),
            Some(a) => {
                let (start, alen, head) = aligned(offset, len, a);
                self.file
                    .submit_read(IoBuf::zeroed(alen), start)
                    .map(move |r| {
                        r.map(|mut b| {
                            b.keep(head..head + len);
                            b
                        })
                    })
            }
        }
    }

    /// Submits the read through the VFS's asynchronous reads: one completion that wakes its
    /// poller once every piece has arrived.
    pub fn submit(&self) -> pigeonhole_io::Completion {
        if let [(offset, _)] = self.pieces.as_slice() {
            return self.submit_piece(*offset, self.len);
        }
        // Several pieces: each read's continuation copies its bytes into place, and the last
        // one resolves the joined completion (or the first failure does).
        struct Join {
            buf: Option<IoBuf>,
            left: usize,
            resolver: Option<pigeonhole_io::Resolver<IoBuf>>,
        }
        let (done, resolver) = pigeonhole_io::Completion::pair();
        let join = Arc::new(std::sync::Mutex::new(Join {
            buf: Some(IoBuf::zeroed(self.len)),
            left: self.pieces.len(),
            resolver: Some(resolver),
        }));
        for (offset, range) in &self.pieces {
            let (join, range) = (Arc::clone(&join), range.clone());
            let read = self.submit_piece(*offset, range.len());
            // The continuation runs when the read completes, whether or not the mapped
            // completion is kept.
            drop(read.map(move |r| {
                let mut j = join
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let finished = match r {
                    Ok(piece) => {
                        if let Some(buf) = j.buf.as_mut() {
                            buf[range].copy_from_slice(&piece);
                        }
                        j.left -= 1;
                        (j.left == 0).then(|| j.buf.take().map(Ok)).flatten()
                    }
                    Err(e) => {
                        j.buf = None;
                        Some(Err(e))
                    }
                };
                if let Some(result) = finished
                    && let Some(resolver) = j.resolver.take()
                {
                    drop(j);
                    resolver.resolve(result);
                }
                Ok(())
            }));
        }
        done
    }

    /// Whether what was fetched is kept now, so the read that missed will find it: the block
    /// or record in the cache, or the header recorded as verified. A block is not kept when
    /// the cache keeps nothing (capacity 0, or a block larger than a cache shard); the caller
    /// then reads synchronously instead of fetching again.
    pub fn is_kept(&self) -> bool {
        match &self.then {
            Then::Block { cache, key, .. } | Then::Footer { cache, key, .. } => {
                cache.get(*key).is_some()
            }
            Then::BlobHeader { reader, index } => reader.inner.is_verified(*index),
            Then::BlobRecord { reader, ptr } => reader.inner.cached(ptr).is_some(),
        }
    }

    /// Verifies and keeps the bytes `submit` read, returning a pinned handle when there is
    /// one (a block or record; hold it until the read that missed has run again).
    pub fn admit(&self, buf: IoBuf) -> Result<Option<BlockHandle>> {
        Ok(match &self.then {
            Then::Block {
                cache,
                key,
                kind,
                priority,
            } => Some(cache.insert(*key, decode_physical(buf, *kind)?, *priority)),
            Then::Footer {
                cache,
                key,
                priority,
            } => Some(cache.insert(*key, BlockData::from(buf.to_vec()), *priority)),
            Then::BlobHeader { reader, index } => {
                reader.inner.check_header(*index, &buf)?;
                None
            }
            Then::BlobRecord { reader, ptr } => Some(reader.inner.admit_record(ptr, buf)?),
        })
    }
}

/// The aligned read around `len` bytes at `offset`: its start, its length, and where the bytes
/// begin inside it.
fn aligned(offset: u64, len: usize, align: usize) -> (u64, usize, usize) {
    let a = align as u64;
    let start = offset / a * a;
    let end = (offset + len as u64).div_ceil(a) * a;
    (start, (end - start) as usize, (offset - start) as usize)
}

/// Reads `len` bytes at `offset` into a buffer the block cache can keep. On a direct handle
/// (#403) the read covers the aligned pages around them and the buffer keeps just them, in
/// place: no copy (a block's offset and length are not aligned on disk).
pub(crate) fn read_bytes(file: &FileRef, offset: u64, len: usize) -> Result<IoBuf> {
    match file.direct_align() {
        None => {
            let mut buf = IoBuf::zeroed(len);
            file.read_at(&mut buf, offset)?;
            Ok(buf)
        }
        Some(a) => {
            let (start, alen, head) = aligned(offset, len, a);
            let mut buf = IoBuf::zeroed(alen);
            file.read_at(&mut buf, start)?;
            buf.keep(head..head + len);
            Ok(buf)
        }
    }
}

/// Reads exactly `out.len()` bytes at `offset` into `out`, any slice: on a direct handle
/// through an aligned bounce buffer (a copy; for small or piecewise reads).
pub(crate) fn read_into(file: &FileRef, out: &mut [u8], offset: u64) -> Result<()> {
    if file.direct_align().is_none() {
        return Ok(file.read_at(out, offset)?);
    }
    let buf = read_bytes(file, offset, out.len())?;
    out.copy_from_slice(&buf);
    Ok(())
}

/// Writes `bytes` at `offset`, any range: on a direct handle (#403) through aligned pages,
/// reading back the pages it only partly covers (the bytes around the range stay as they
/// are). For writers without a page-sized buffer of their own (the blob writer).
pub(crate) fn write_any(file: &FileRef, bytes: &[u8], offset: u64) -> Result<()> {
    let Some(a) = file.direct_align() else {
        return Ok(file.write_at(bytes, offset)?);
    };
    if bytes.is_empty() {
        return Ok(());
    }
    let (start, len, head) = aligned(offset, bytes.len(), a);
    let mut buf = IoBuf::zeroed(len);
    if head != 0 {
        file.read_at(&mut buf[..a], start)?;
    }
    let last = len - a;
    if !(head + bytes.len()).is_multiple_of(a) && !(last == 0 && head != 0) {
        file.read_at(&mut buf[last..], start + last as u64)?;
    }
    buf[head..head + bytes.len()].copy_from_slice(bytes);
    Ok(file.write_at(&buf, start)?)
}

/// Writes `bytes` (a whole number of aligned pages, or the last pages of an SST padded with
/// zeros) at an aligned `offset` from an aligned copy, as a direct handle needs.
pub(crate) fn write_padded(file: &FileRef, bytes: &[u8], offset: u64, align: usize) -> Result<()> {
    let mut buf = IoBuf::zeroed(bytes.len().div_ceil(align) * align);
    buf[..bytes.len()].copy_from_slice(bytes);
    Ok(file.write_at(&buf, offset)?)
}

/// Checks a physical block and turns it into what the cache holds: the logical block. An
/// uncompressed block keeps its I/O buffer (trimmed of the trailer); a compressed one is
/// decompressed into a heap buffer.
pub(crate) fn decode_physical(mut buf: IoBuf, kind: BlockKind) -> Result<BlockData> {
    match decode_slice(&buf, kind)? {
        Some(data) => Ok(data),
        None => {
            let n = buf.len() - TRAILER_LEN;
            buf.resize(n);
            Ok(BlockData::Io(buf))
        }
    }
}

/// [`decode_physical`] over borrowed bytes: the decompressed block, or `None` if the block
/// is stored uncompressed (its logical bytes are the payload, which the caller keeps).
fn decode_slice(physical: &[u8], kind: BlockKind) -> Result<Option<BlockData>> {
    let (trailer, payload) = verify(physical)?;
    if trailer.kind != kind {
        return Err(corrupt("block kind"));
    }
    match trailer.compression {
        Compression::None => Ok(None),
        codec => {
            let mut out = vec![0; trailer.uncompressed_len as usize];
            decompress(codec, payload, &mut out)?;
            Ok(Some(BlockData::from(out)))
        }
    }
}

/// The open state of one SST; see [`crate::SstReader`].
pub(crate) struct Reader {
    pub(crate) blocks: Blocks,
    pub(crate) top: BlockHandle,
    row_filter: Option<Filter<BlockHandle>>,
    column_filter: Option<Filter<BlockHandle>>,
    pub(crate) properties: Properties,
}

/// Block access for one SST: the file, where the SST sits in it, and the cache.
pub(crate) struct Blocks {
    pub(crate) file: FileRef,
    pub(crate) id: SstId,
    /// Absolute file offset of the SST.
    base: u64,
    /// End of the block area (start of the footer).
    limit: u64,
    pub(crate) cache: Arc<BlockCache>,
    /// Hands out unshared handles for reads that must not fill the cache.
    uncached: BlockCache,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstReader")
            .field("id", &self.blocks.id)
            .field("len", &self.len_bytes())
            .field("entries", &self.properties.entries)
            .field("filters", &self.row_filter.is_some())
            .finish()
    }
}

impl Reader {
    pub(crate) fn open(
        file: FileRef,
        meta: &SstMeta,
        cache: Arc<BlockCache>,
        priority: Priority,
        cache_only: bool,
    ) -> Result<Self> {
        if meta.len < FOOTER_LEN as u64 || meta.len > meta.extent.len() {
            return Err(corrupt("sst length"));
        }
        let base = meta.extent.offset();
        let limit = meta.len - FOOTER_LEN as u64;
        let mut tail = [0; FOOTER_LEN];
        if cache_only {
            // The footer goes through the cache only here, so a cache-only open can fetch it
            // and run again; a sync open reads it directly, as before.
            let key = BlockKey {
                file: crate::sst_cache_file(meta.id),
                offset: limit,
            };
            match cache.get(key) {
                Some(h) if h.len() == FOOTER_LEN => tail.copy_from_slice(&h),
                _ => {
                    return Err(crate::Error::WouldBlock(Box::new(Fetch::one(
                        file,
                        base + limit,
                        FOOTER_LEN,
                        Then::Footer {
                            cache,
                            key,
                            priority,
                        },
                    ))));
                }
            }
        } else {
            read_into(&file, &mut tail, base + limit)?;
        }
        let footer = Footer::decode(&tail)?;
        let blocks = Blocks {
            file,
            id: meta.id,
            base,
            limit,
            cache,
            uncached: BlockCache::disabled(),
        };
        // A cache-only open reads only from the cache, and fills it with what it fetches
        // (the properties block too), so its retry finds everything.
        let read = |addr: BlockAddr, kind: BlockKind, fill: bool| {
            if cache_only {
                blocks.read_block_cache_only(addr, kind, priority)
            } else {
                blocks.read_block(addr, kind, fill, priority)
            }
        };
        let top = read(footer.top_index, BlockKind::TopIndex, true)?;
        // Checked once here, so every cursor can trust the top index's tables.
        Block::new(top.clone())?.validate()?;
        let filter = |addr: BlockAddr| -> Result<Option<Filter<BlockHandle>>> {
            if addr.len == 0 {
                return Ok(None);
            }
            let h = read(addr, BlockKind::Filter, true)?;
            Ok(Some(Filter::new(h)?))
        };
        let row_filter = filter(footer.row_filter)?;
        let column_filter = filter(footer.column_filter)?;
        let props = read(footer.properties, BlockKind::Properties, false)?;
        let properties = Properties::decode(&props)?;
        Ok(Self {
            blocks,
            top,
            row_filter,
            column_filter,
            properties,
        })
    }

    /// The SST's length in bytes (`SstMeta::len`).
    pub(crate) fn len_bytes(&self) -> u64 {
        self.blocks.limit + FOOTER_LEN as u64
    }

    pub(crate) fn may_contain_row(&self, hash: u64) -> bool {
        self.row_filter.as_ref().is_none_or(|f| f.may_contain(hash))
    }

    pub(crate) fn may_contain_column(&self, hash: u64) -> bool {
        self.column_filter
            .as_ref()
            .is_none_or(|f| f.may_contain(hash))
    }
}

impl Blocks {
    fn check(&self, addr: BlockAddr) -> Result<()> {
        let end = addr.offset.checked_add(u64::from(addr.len));
        if (addr.len as usize) < TRAILER_LEN || end.is_none_or(|e| e > self.limit) {
            return Err(corrupt("block address"));
        }
        Ok(())
    }

    pub(crate) fn key(&self, addr: BlockAddr) -> BlockKey {
        BlockKey {
            file: crate::sst_cache_file(self.id),
            offset: addr.offset,
        }
    }

    /// Whether `addr` is cached (pins it if so).
    pub(crate) fn cached(&self, addr: BlockAddr) -> Option<BlockHandle> {
        self.cache.get(self.key(addr))
    }

    /// Reads a block through the cache. Checksums are verified on reads from disk and skipped
    /// on hits.
    pub(crate) fn read_block(
        &self,
        addr: BlockAddr,
        kind: BlockKind,
        fill_cache: bool,
        priority: Priority,
    ) -> Result<BlockHandle> {
        self.check(addr)?;
        if let Some(h) = self.cached(addr) {
            return Ok(h);
        }
        crate::note_file_read();
        let buf = read_bytes(&self.file, self.base + addr.offset, addr.len as usize)?;
        let data = decode_physical(buf, kind)?;
        Ok(self.admit(addr, data, fill_cache, priority))
    }

    /// The block at `addr` if it is cached (pinned), after the same check as
    /// [`Blocks::read_block`]: the hit path of a read, for a cursor that handles its misses
    /// itself ([`Blocks::read_uncached`], or `Error::WouldBlock` in cache-only mode).
    #[inline]
    pub(crate) fn lookup(&self, addr: BlockAddr) -> Result<Option<BlockHandle>> {
        self.check(addr)?;
        Ok(self.cached(addr))
    }

    /// Reads a block that [`Blocks::lookup`] did not find, from the file.
    pub(crate) fn read_uncached(
        &self,
        addr: BlockAddr,
        kind: BlockKind,
        fill_cache: bool,
        priority: Priority,
    ) -> Result<BlockHandle> {
        crate::note_file_read();
        let buf = read_bytes(&self.file, self.base + addr.offset, addr.len as usize)?;
        let data = decode_physical(buf, kind)?;
        Ok(self.admit(addr, data, fill_cache, priority))
    }

    /// [`Blocks::read_block`] from the cache only: a miss fails with `Error::WouldBlock`
    /// naming the block instead of reading it (a cache-only open, ICR 0014).
    pub(crate) fn read_block_cache_only(
        &self,
        addr: BlockAddr,
        kind: BlockKind,
        priority: Priority,
    ) -> Result<BlockHandle> {
        match self.lookup(addr)? {
            Some(h) => Ok(h),
            None => Err(self.would_block(addr, kind, priority)),
        }
    }

    /// The `WouldBlock` error of a cache-only read that missed `addr`.
    #[cold]
    pub(crate) fn would_block(
        &self,
        addr: BlockAddr,
        kind: BlockKind,
        priority: Priority,
    ) -> crate::Error {
        crate::Error::WouldBlock(Box::new(self.fetch(addr, kind, priority)))
    }

    /// The fetch of the block at `addr`, admitted to the cache.
    pub(crate) fn fetch(&self, addr: BlockAddr, kind: BlockKind, priority: Priority) -> Fetch {
        Fetch::one(
            self.file.clone(),
            self.base + addr.offset,
            addr.len as usize,
            Then::Block {
                cache: Arc::clone(&self.cache),
                key: self.key(addr),
                kind,
                priority,
            },
        )
    }

    /// The fetch of `addr` if the cache does not hold it (and its address is valid).
    pub(crate) fn fetch_if_missing(
        &self,
        addr: BlockAddr,
        kind: BlockKind,
        priority: Priority,
    ) -> Option<Fetch> {
        match self.lookup(addr) {
            Ok(None) => Some(self.fetch(addr, kind, priority)),
            _ => None,
        }
    }

    /// Hands out a decoded block, inserting it into the cache if asked.
    pub(crate) fn admit(
        &self,
        addr: BlockAddr,
        data: BlockData,
        fill_cache: bool,
        priority: Priority,
    ) -> BlockHandle {
        let cache = if fill_cache {
            &*self.cache
        } else {
            &self.uncached
        };
        cache.insert(self.key(addr), data, priority)
    }

    /// Reads the contiguous run `addrs` (checked, ascending, adjacent) in one I/O and decodes
    /// each block.
    pub(crate) fn read_run(&self, addrs: &[BlockAddr], kind: BlockKind) -> Result<Vec<BlockData>> {
        let (Some(first), Some(last)) = (addrs.first(), addrs.last()) else {
            return Ok(Vec::new());
        };
        for a in addrs {
            self.check(*a)?;
        }
        let start = first.offset;
        let end = last.offset + u64::from(last.len);
        crate::note_file_read();
        let buf = read_bytes(&self.file, self.base + start, (end - start) as usize)?;
        addrs
            .iter()
            .map(|a| {
                let at = (a.offset - start) as usize;
                let physical = &buf[at..at + a.len as usize];
                // One copy per block: decompression, or the payload of a stored block.
                Ok(match decode_slice(physical, kind)? {
                    Some(data) => data,
                    None => BlockData::from(physical[..physical.len() - TRAILER_LEN].to_vec()),
                })
            })
            .collect()
    }
}
