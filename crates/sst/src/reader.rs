//! Opening an SST and reading its blocks through the block cache.

use std::sync::Arc;

use pigeonhole_cache::{BlockCache, BlockData, BlockHandle, BlockKey, Priority};
use pigeonhole_format::Error as FormatError;
use pigeonhole_format::SstId;
use pigeonhole_format::block::{BlockAddr, BlockKind, TRAILER_LEN, verify};
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
pub(crate) fn decode_physical(mut buf: IoBuf, kind: BlockKind) -> Result<BlockData> {
    let (trailer, payload) = verify(&buf)?;
    if trailer.kind != kind {
        return Err(corrupt("block kind"));
    }
    match trailer.compression {
        Compression::None => {
            let n = payload.len();
            buf.resize(n);
            Ok(BlockData::Io(buf))
        }
        codec => {
            let mut out = vec![0; trailer.uncompressed_len as usize];
            decompress(codec, payload, &mut out)?;
            Ok(BlockData::from(out))
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
            .field("len", &(self.blocks.limit + FOOTER_LEN as u64))
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
    ) -> Result<Self> {
        if meta.len < FOOTER_LEN as u64 || meta.len > meta.extent.len() {
            return Err(corrupt("sst length"));
        }
        let base = meta.extent.offset();
        let limit = meta.len - FOOTER_LEN as u64;
        let mut tail = [0; FOOTER_LEN];
        file.read_at(&mut tail, base + limit)?;
        let footer = Footer::decode(&tail)?;
        let blocks = Blocks {
            file,
            id: meta.id,
            base,
            limit,
            cache,
            uncached: BlockCache::disabled(),
        };
        let top = blocks.read_block(footer.top_index, BlockKind::TopIndex, true, priority)?;
        crate::block::BlockCursor::new().reset(top.clone())?;
        let filter = |addr: BlockAddr| -> Result<Option<Filter<BlockHandle>>> {
            if addr.len == 0 {
                return Ok(None);
            }
            let h = blocks.read_block(addr, BlockKind::Filter, true, priority)?;
            Ok(Some(Filter::new(h)?))
        };
        let row_filter = filter(footer.row_filter)?;
        let column_filter = filter(footer.column_filter)?;
        let props = blocks.read_block(footer.properties, BlockKind::Properties, false, priority)?;
        let properties = Properties::decode(&props)?;
        Ok(Self {
            blocks,
            top,
            row_filter,
            column_filter,
            properties,
        })
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
                let mut one = IoBuf::zeroed(a.len as usize);
                one.copy_from_slice(&buf[at..at + a.len as usize]);
                decode_physical(one, kind)
            })
            .collect()
    }
}
