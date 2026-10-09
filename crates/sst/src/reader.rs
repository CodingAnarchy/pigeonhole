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

/// Checks a physical block and turns it into what the cache holds: the logical block. An
/// uncompressed block keeps its I/O buffer (trimmed of the trailer); a compressed one is
/// decompressed into a heap buffer.
/// What a cache-only read missed (`Error::WouldBlock`, ICR 0014): one read of the file and
/// how its bytes enter the block cache. The caller submits the read ([`Fetch::submit`]), admits
/// the bytes when it completes ([`Fetch::admit`], which verifies them and returns them pinned),
/// and reads again, now a cache hit.
#[derive(Clone)]
pub struct Fetch {
    file: FileRef,
    /// Absolute file offset and length of the read.
    offset: u64,
    len: usize,
    cache: Arc<BlockCache>,
    key: BlockKey,
    kind: FetchKind,
    priority: Priority,
}

#[derive(Clone, Copy, Debug)]
enum FetchKind {
    /// A block of this kind (checksummed and maybe compressed on disk).
    Block(BlockKind),
    /// An SST's footer, cached as its raw bytes for a cache-only open.
    Footer,
}

impl std::fmt::Debug for Fetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetch")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .field("kind", &self.kind)
            .finish()
    }
}

impl Fetch {
    /// Submits the read (the VFS's asynchronous read: a completion that wakes its poller).
    pub fn submit(&self) -> pigeonhole_io::Completion {
        self.file.submit_read(IoBuf::zeroed(self.len), self.offset)
    }

    /// Whether the fetched bytes are in the block cache now. Not after [`Fetch::admit`] when
    /// the cache keeps nothing (capacity 0, or a block larger than a cache shard): the caller
    /// then reads synchronously instead of fetching again.
    pub fn is_cached(&self) -> bool {
        self.cache.get(self.key).is_some()
    }

    /// Verifies and decodes the bytes `submit` read and admits them to the block cache,
    /// returning them pinned: hold the handle until the read that missed has run again.
    pub fn admit(&self, buf: IoBuf) -> Result<BlockHandle> {
        let data = match self.kind {
            FetchKind::Block(kind) => decode_physical(buf, kind)?,
            FetchKind::Footer => BlockData::from(buf.to_vec()),
        };
        Ok(self.cache.insert(self.key, data, self.priority))
    }
}

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
                    return Err(crate::Error::WouldBlock(Box::new(Fetch {
                        file,
                        offset: base + limit,
                        len: FOOTER_LEN,
                        cache,
                        key,
                        kind: FetchKind::Footer,
                        priority,
                    })));
                }
            }
        } else {
            file.read_at(&mut tail, base + limit)?;
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
        let mut buf = IoBuf::zeroed(addr.len as usize);
        self.file.read_at(&mut buf, self.base + addr.offset)?;
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
        let mut buf = IoBuf::zeroed(addr.len as usize);
        self.file.read_at(&mut buf, self.base + addr.offset)?;
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
        crate::Error::WouldBlock(Box::new(Fetch {
            file: self.file.clone(),
            offset: self.base + addr.offset,
            len: addr.len as usize,
            cache: Arc::clone(&self.cache),
            key: self.key(addr),
            kind: FetchKind::Block(kind),
            priority,
        }))
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
        let mut buf = IoBuf::zeroed((end - start) as usize);
        self.file.read_at(&mut buf, self.base + start)?;
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
