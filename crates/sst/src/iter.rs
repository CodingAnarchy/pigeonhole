//! The SST cursor: partitioned-index seeks, filter pushdown in the block decoder, row skipping
//! and forward readahead.

use std::collections::VecDeque;
use std::sync::Arc;

use pigeonhole_cache::{BlockHandle, Cell};
use pigeonhole_format::Cursor;
use pigeonhole_format::block::{Block, BlockAddr, BlockIter, BlockKind};
use pigeonhole_format::key::row_prefix_len;

use crate::{Error, ReadOptions, Result, ScanFilter, SstReader};

/// A zero-copy cursor over one SST, from [`SstReader::iter`].
///
/// It owns an `Arc<SstReader>` and the pinned blocks it is positioned in, so it can be stored
/// anywhere (scan cursors, compaction jobs) without borrowing (decision D32). Keys and values
/// borrow the current data block; [`SstIter::value_cell`] hands out a pin that outlives the
/// cursor.
///
/// The cursor only ever stops on entries its [`ScanFilter`] admits: excluded qualifiers are
/// skipped with seeks ([`ScanFilter::next_admissible`]), so a filtered scan returns exactly
/// the unfiltered entries for which [`ScanFilter::admits`] holds (decision D22).
/// [`Cursor::skip_row`] uses each block's row-start table and crosses into the next block only
/// if the row continues there.
///
/// ```
/// use std::sync::Arc;
/// use pigeonhole_cache::{BlockCache, Priority};
/// use pigeonhole_format::key::{Kind, encode_key};
/// use pigeonhole_format::manifest::FamilyOptions;
/// use pigeonhole_format::superblock::ExtentRef;
/// use pigeonhole_format::{Cursor, FamilyId, SstId, TableId, TabletId};
/// use pigeonhole_io::{OpenOptions, Vfs};
/// use pigeonhole_io::sim::SimVfs;
/// use pigeonhole_sst::{QualifierFilter, ReadOptions, ScanFilter, SstReader, SstWriter, SstWriterOptions};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let vfs = SimVfs::new(1);
/// let file = vfs.open("/db".as_ref(), OpenOptions::read_write_create())?;
/// let options = SstWriterOptions::for_family(&FamilyOptions::default(), TableId(1), FamilyId(1), TabletId(1));
/// let extent = ExtentRef { page: 16, size_class: 0 };
/// let mut w = SstWriter::new(file.clone(), extent, SstId(1), options);
/// let mut key = Vec::new();
/// for row in [&b"a"[..], b"b"] {
///     for q in [&b"meta:x"[..], b"body"] {
///         key.clear();
///         encode_key(&mut key, row, q, 7, 1, Kind::Put)?;
///         // (keys must arrive sorted: "body" < "meta:x")
///     }
/// }
/// # let mut keys = Vec::new();
/// # for row in [&b"a"[..], b"b"] { for q in [&b"body"[..], b"meta:x"] {
/// #     let mut k = Vec::new(); encode_key(&mut k, row, q, 7, 1, Kind::Put)?; keys.push(k);
/// # } }
/// # for k in &keys { w.add(k, b"\x00v")?; }
/// let meta = w.finish()?;
///
/// let reader = Arc::new(SstReader::open(file, &meta, Arc::new(BlockCache::new(1 << 20, 1)), Priority::Normal)?);
/// let mut filter = ScanFilter::all();
/// filter.qualifiers = QualifierFilter::Prefix(b"meta:".to_vec());
/// let mut it = reader.iter(filter, ReadOptions::default());
/// it.seek_to_first()?;
/// let mut n = 0;
/// while it.valid() {
///     assert!(it.key().windows(5).any(|w| w == b"meta:"));
///     let pinned = it.value_cell(); // outlives the cursor
///     assert_eq!(&pinned[..], b"\x00v");
///     n += 1;
///     it.next()?;
/// }
/// assert_eq!(n, 2);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SstIter {
    reader: Arc<SstReader>,
    filter: ScanFilter,
    filter_all: bool,
    options: ReadOptions,
    top: Option<BlockIter<BlockHandle>>,
    index: Option<BlockIter<BlockHandle>>,
    data: Option<BlockIter<BlockHandle>>,
    /// Seek hints from the filter.
    hint: Vec<u8>,
    /// The row being skipped.
    row: Vec<u8>,
    /// Read-ahead blocks kept for a read that must not fill the cache, ascending by offset.
    prefetched: VecDeque<(u64, BlockHandle)>,
    run: Vec<BlockAddr>,
}

/// Points `slot` at `block`, reusing the cursor (and its key buffer) if there is one.
fn load(slot: &mut Option<BlockIter<BlockHandle>>, block: BlockHandle) -> Result<()> {
    match slot {
        Some(it) => it.reset(block)?,
        None => *slot = Some(Block::new(block)?.into_cursor()),
    }
    Ok(())
}

fn valid(slot: &Option<BlockIter<BlockHandle>>) -> bool {
    slot.as_ref().is_some_and(|it| it.valid())
}

/// The loaded cursor in `slot`. Every caller loads it first, so `None` never happens; it is
/// an error rather than a panic all the same.
fn loaded(slot: &mut Option<BlockIter<BlockHandle>>) -> Result<&mut BlockIter<BlockHandle>> {
    slot.as_mut()
        .ok_or(Error::Format(pigeonhole_format::Error::Corrupt {
            what: "sst cursor state",
        }))
}

impl SstIter {
    pub(crate) fn new(reader: Arc<SstReader>, filter: ScanFilter, options: ReadOptions) -> Self {
        Self {
            reader,
            filter_all: filter.is_all(),
            filter,
            options,
            top: None,
            index: None,
            data: None,
            hint: Vec::new(),
            row: Vec::new(),
            prefetched: VecDeque::new(),
            run: Vec::new(),
        }
    }

    /// The current value as a pinned [`Cell`] that outlives the cursor (no copy). Empty if
    /// the cursor is not valid.
    pub fn value_cell(&self) -> Cell {
        match &self.data {
            Some(it) if it.valid() => Cell::in_block(it.bytes().clone(), it.value_range()),
            _ => Cell::owned(Vec::new()),
        }
    }

    fn load_partition(&mut self) -> Result<()> {
        let addr = BlockAddr::decode_varint(loaded(&mut self.top)?.value())?;
        let h = self.reader.inner.blocks.read_block(
            addr,
            BlockKind::Index,
            self.options.fill_cache,
            self.options.priority,
        )?;
        load(&mut self.index, h)
    }

    /// Loads the data block the index cursor points at. `ahead` allows readahead (forward
    /// scans only, never for seeks).
    fn load_data(&mut self, ahead: bool) -> Result<()> {
        let addr = BlockAddr::decode_varint(loaded(&mut self.index)?.value())?;
        let h = match self.take_prefetched(addr) {
            Some(h) => h,
            None if ahead && self.options.readahead_blocks > 0 => self.read_ahead(addr)?,
            None => self.reader.inner.blocks.read_block(
                addr,
                BlockKind::Data,
                self.options.fill_cache,
                self.options.priority,
            )?,
        };
        load(&mut self.data, h)
    }

    fn take_prefetched(&mut self, addr: BlockAddr) -> Option<BlockHandle> {
        while let Some((offset, _)) = self.prefetched.front() {
            if *offset > addr.offset {
                return None;
            }
            let (offset, h) = self.prefetched.pop_front()?;
            if offset == addr.offset {
                return Some(h);
            }
        }
        None
    }

    /// Reads `addr` and up to `readahead_blocks` adjacent uncached blocks of the same
    /// partition in one I/O.
    fn read_ahead(&mut self, addr: BlockAddr) -> Result<BlockHandle> {
        let blocks = &self.reader.inner.blocks;
        if let Some(h) = blocks.cached(addr) {
            return Ok(h);
        }
        self.run.clear();
        self.run.push(addr);
        let mut peek = loaded(&mut self.index)?.clone();
        while self.run.len() <= self.options.readahead_blocks as usize {
            peek.next()?;
            if !peek.valid() {
                break;
            }
            let a = BlockAddr::decode_varint(peek.value())?;
            let prev = self.run[self.run.len() - 1];
            if a.offset != prev.offset + u64::from(prev.len) || blocks.cached(a).is_some() {
                break;
            }
            self.run.push(a);
        }
        let datas = blocks.read_run(&self.run, BlockKind::Data)?;
        let (fill, priority) = (self.options.fill_cache, self.options.priority);
        let mut first = None;
        for (a, d) in self.run.iter().zip(datas) {
            let h = blocks.admit(*a, d, fill, priority);
            if first.is_none() {
                first = Some(h);
            } else if !fill {
                self.prefetched.push_back((a.offset, h));
            }
        }
        first.ok_or(Error::Format(pigeonhole_format::Error::Corrupt {
            what: "sst readahead",
        }))
    }

    /// Positions on the first entry `>= target` (or the first entry), ignoring the filter.
    fn position(&mut self, target: Option<&[u8]>) -> Result<()> {
        // Read-ahead blocks are only useful in front of a forward scan; a seek (possibly
        // backwards) drops their pins.
        self.prefetched.clear();
        if self.top.is_none() {
            load(&mut self.top, self.reader.inner.top.clone())?;
        }
        let top = loaded(&mut self.top)?;
        match target {
            Some(t) => top.seek(t)?,
            None => top.seek_to_first()?,
        }
        loop {
            if !valid(&self.top) {
                self.data = None;
                return Ok(());
            }
            self.load_partition()?;
            let index = loaded(&mut self.index)?;
            match target {
                Some(t) => index.seek(t)?,
                None => index.seek_to_first()?,
            }
            if index.valid() {
                break;
            }
            loaded(&mut self.top)?.next()?;
        }
        self.load_data(false)?;
        let data = loaded(&mut self.data)?;
        match target {
            Some(t) => data.seek(t)?,
            None => data.seek_to_first()?,
        }
        if !data.valid() {
            // A separator may sort above the block's last key; the answer starts the next.
            self.advance_block()?;
        }
        Ok(())
    }

    /// Moves to the first entry of the next data block.
    fn advance_block(&mut self) -> Result<()> {
        loop {
            loaded(&mut self.index)?.next()?;
            while !valid(&self.index) {
                loaded(&mut self.top)?.next()?;
                if !valid(&self.top) {
                    self.data = None;
                    return Ok(());
                }
                self.load_partition()?;
                loaded(&mut self.index)?.seek_to_first()?;
            }
            self.load_data(true)?;
            let data = loaded(&mut self.data)?;
            data.seek_to_first()?;
            if data.valid() {
                return Ok(());
            }
        }
    }

    fn next_raw(&mut self) -> Result<()> {
        let Some(data) = self.data.as_mut() else {
            return Ok(());
        };
        data.next()?;
        if !data.valid() {
            self.advance_block()?;
        }
        Ok(())
    }

    fn seek_forward(&mut self, target: &[u8]) -> Result<()> {
        let data = loaded(&mut self.data)?;
        data.seek(target)?;
        if !data.valid() {
            self.position(Some(target))?;
        }
        Ok(())
    }

    fn skip_row_raw(&mut self) -> Result<()> {
        let Some(data) = self.data.as_mut().filter(|d| d.valid()) else {
            return Ok(());
        };
        let Ok(n) = row_prefix_len(data.key()) else {
            return self.next_raw();
        };
        self.row.clear();
        self.row.extend_from_slice(&data.key()[..n]);
        data.skip_row()?;
        if data.valid() {
            return Ok(());
        }
        self.advance_block()?;
        let Some(data) = self
            .data
            .as_mut()
            .filter(|d| d.valid() && d.key().starts_with(&self.row))
        else {
            return Ok(());
        };
        // The row continues into this block: skip within it, or, if it fills the block, seek
        // past the row. `escaped row ++ 00 02` sorts after every key of the row and before
        // every later row (a later row continues with `00 FF` or a byte >= 01 at that point,
        // or differs earlier).
        data.skip_row()?;
        if !data.valid() {
            let mut row = std::mem::take(&mut self.row);
            if let Some(last) = row.last_mut() {
                *last = 0x02;
            }
            let r = self.position(Some(&row));
            self.row = row;
            r?;
        }
        Ok(())
    }

    /// Advances to the first admitted entry at or after the current one.
    fn settle(&mut self) -> Result<()> {
        if self.filter_all {
            return Ok(());
        }
        while let Some(data) = self.data.as_ref().filter(|d| d.valid()) {
            let key = data.key();
            if self.filter.admits(key) {
                return Ok(());
            }
            self.hint.clear();
            if !self.filter.next_admissible(key, &mut self.hint) {
                self.skip_row_raw()?;
            } else if self.hint.as_slice() > key {
                let hint = std::mem::take(&mut self.hint);
                let r = self.seek_forward(&hint);
                self.hint = hint;
                r?;
            } else {
                self.next_raw()?;
            }
        }
        Ok(())
    }

    /// Runs a move and settles; on error the cursor becomes invalid.
    fn run(&mut self, f: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        let r = f(self).and_then(|()| self.settle());
        if r.is_err() {
            self.data = None;
        }
        r
    }
}

impl Cursor for SstIter {
    type Error = Error;

    fn valid(&self) -> bool {
        valid(&self.data)
    }

    fn key(&self) -> &[u8] {
        self.data.as_ref().map_or(&[], |d| d.key())
    }

    fn value(&self) -> &[u8] {
        self.data.as_ref().map_or(&[], |d| d.value())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.run(|it| it.position(None))
    }

    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.run(|it| it.position(Some(target)))
    }

    fn next(&mut self) -> Result<()> {
        let Some(data) = self.data.as_mut().filter(|d| d.valid()) else {
            return Ok(());
        };
        // The usual step: the next entry of the same block, no filter to apply.
        match data.next() {
            Ok(()) if data.valid() && self.filter_all => Ok(()),
            Ok(()) => self.run(|it| {
                if valid(&it.data) {
                    Ok(())
                } else {
                    it.advance_block()
                }
            }),
            Err(e) => {
                self.data = None;
                Err(e.into())
            }
        }
    }

    /// Uses the block's row-start table; crosses into the next block only if the row
    /// continues there.
    fn skip_row(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }
        self.run(Self::skip_row_raw)
    }
}
