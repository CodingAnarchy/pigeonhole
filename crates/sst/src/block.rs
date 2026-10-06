//! A cursor over one logical data or index block (FORMAT §4.2) held by a pinned cache
//! handle.
//!
//! `format::block::BlockIter` decodes the same layout, but a fresh iterator per block starts
//! with an empty key buffer (one allocation per block visited) and validates both offset
//! tables up front (O(entries) per open). This cursor is reset onto each new block and keeps
//! its key buffer, so a point lookup on hot blocks allocates nothing, and checks offsets
//! lazily as it uses them, so opening a block is O(1). Checksums are verified when a block is
//! read from disk; everything here is still bounds-checked and never panics on bad bytes.

use std::ops::Range;

use pigeonhole_cache::BlockHandle;
use pigeonhole_format::Error as FormatError;
use pigeonhole_format::varint;

use crate::Result;

fn corrupt(what: &'static str) -> crate::Error {
    crate::Error::Format(FormatError::Corrupt { what })
}

fn le_u32(b: &[u8], at: usize) -> usize {
    let mut x = [0; 4];
    x.copy_from_slice(&b[at..at + 4]);
    u32::from_le_bytes(x) as usize
}

/// Keys up to this long are rebuilt inline, so a cursor on hot blocks allocates nothing
/// even when it is created per lookup.
const INLINE_KEY: usize = 128;

/// A key buffer that lives inline until a key outgrows it.
#[derive(Debug, Clone)]
struct KeyBuf {
    inline: [u8; INLINE_KEY],
    len: usize,
    heap: Vec<u8>,
    spilled: bool,
}

impl KeyBuf {
    fn new() -> Self {
        Self {
            inline: [0; INLINE_KEY],
            len: 0,
            heap: Vec::new(),
            spilled: false,
        }
    }

    fn as_slice(&self) -> &[u8] {
        if self.spilled {
            &self.heap
        } else {
            &self.inline[..self.len]
        }
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn clear(&mut self) {
        self.len = 0;
        self.heap.clear();
        self.spilled = false;
    }

    fn truncate(&mut self, n: usize) {
        if self.spilled {
            self.heap.truncate(n);
        } else {
            self.len = self.len.min(n);
        }
    }

    fn extend_from_slice(&mut self, b: &[u8]) {
        if !self.spilled {
            if self.len + b.len() <= INLINE_KEY {
                self.inline[self.len..self.len + b.len()].copy_from_slice(b);
                self.len += b.len();
                return;
            }
            self.heap.clear();
            self.heap.extend_from_slice(&self.inline[..self.len]);
            self.spilled = true;
        }
        self.heap.extend_from_slice(b);
    }
}

/// Where the current key lives.
#[derive(Debug, Clone)]
enum KeySrc {
    /// A range of the block (restart entries store their key whole).
    Block(Range<usize>),
    /// Rebuilt in `key_buf`.
    Buf,
}

#[derive(Debug, Clone)]
pub(crate) struct BlockCursor {
    block: Option<BlockHandle>,
    /// End of the entries (start of the restart table).
    data_end: usize,
    restart_count: usize,
    row_start_count: usize,
    /// Offset of the current entry; `data_end` when not valid.
    cur: usize,
    /// Offset of the entry after the current one.
    next: usize,
    key: KeySrc,
    key_buf: KeyBuf,
    value: Range<usize>,
}

impl BlockCursor {
    pub(crate) fn new() -> Self {
        Self {
            block: None,
            data_end: 0,
            restart_count: 0,
            row_start_count: 0,
            cur: 0,
            next: 0,
            key: KeySrc::Block(0..0),
            key_buf: KeyBuf::new(),
            value: 0..0,
        }
    }

    /// Moves the cursor onto `block` (unpositioned), keeping the key buffer.
    pub(crate) fn reset(&mut self, block: BlockHandle) -> Result<()> {
        self.block = None;
        self.invalidate();
        let b: &[u8] = &block;
        let Some(tail) = b.len().checked_sub(8) else {
            return Err(corrupt("block tables"));
        };
        let r = le_u32(b, tail);
        let s = le_u32(b, tail + 4);
        let data_end = r
            .checked_add(s)
            .and_then(|n| n.checked_mul(4))
            .and_then(|t| tail.checked_sub(t))
            .ok_or_else(|| corrupt("block tables"))?;
        let first_ok = if r == 0 {
            data_end == 0
        } else {
            le_u32(b, data_end) == 0
        };
        if !first_ok {
            return Err(corrupt("block tables"));
        }
        self.data_end = data_end;
        self.restart_count = r;
        self.row_start_count = s;
        self.block = Some(block);
        self.invalidate();
        Ok(())
    }

    /// Drops the block and becomes invalid.
    pub(crate) fn clear(&mut self) {
        self.block = None;
        self.data_end = 0;
        self.restart_count = 0;
        self.row_start_count = 0;
        self.invalidate();
    }

    pub(crate) fn handle(&self) -> Option<&BlockHandle> {
        self.block.as_ref()
    }

    fn bytes(&self) -> &[u8] {
        self.block.as_deref().unwrap_or(&[])
    }

    pub(crate) fn valid(&self) -> bool {
        self.cur < self.data_end
    }

    pub(crate) fn key(&self) -> &[u8] {
        match &self.key {
            KeySrc::Block(r) => &self.bytes()[r.clone()],
            KeySrc::Buf => self.key_buf.as_slice(),
        }
    }

    pub(crate) fn value(&self) -> &[u8] {
        &self.bytes()[self.value.clone()]
    }

    pub(crate) fn value_range(&self) -> Range<u32> {
        self.value.start as u32..self.value.end as u32
    }

    pub(crate) fn invalidate(&mut self) {
        self.cur = self.data_end;
        self.next = self.data_end;
        self.key = KeySrc::Block(0..0);
        self.value = 0..0;
    }

    fn table(&self, i: usize, what: &'static str) -> Result<usize> {
        let v = le_u32(self.bytes(), self.data_end + 4 * i);
        if v >= self.data_end {
            return Err(corrupt(what));
        }
        Ok(v)
    }

    fn restart(&self, i: usize) -> Result<usize> {
        self.table(i, "block restart offset")
    }

    fn row_start(&self, i: usize) -> Result<usize> {
        self.table(self.restart_count + i, "block row-start offset")
    }

    /// Decodes the entry header at `offset`: shared length, unshared key range, value range.
    fn entry(&self, offset: usize) -> Result<(usize, Range<usize>, Range<usize>)> {
        let b = &self.bytes()[..self.data_end];
        let mut pos = offset;
        let mut field = || -> Result<usize> {
            let rest = b.get(pos..).ok_or_else(|| corrupt("block entry"))?;
            let (v, n) = varint::get_u64(rest)?;
            pos += n;
            usize::try_from(v).map_err(|_| corrupt("block entry"))
        };
        let shared = field()?;
        let unshared = field()?;
        let value_len = field()?;
        let key_end = pos
            .checked_add(unshared)
            .ok_or_else(|| corrupt("block entry"))?;
        let value_end = key_end
            .checked_add(value_len)
            .ok_or_else(|| corrupt("block entry"))?;
        if value_end > b.len() {
            return Err(corrupt("block entry"));
        }
        Ok((shared, pos..key_end, key_end..value_end))
    }

    /// Decodes the entry at `offset`, taking its shared prefix from the current key.
    fn load(&mut self, offset: usize) -> Result<()> {
        if offset >= self.data_end {
            self.invalidate();
            return Ok(());
        }
        let (shared, unshared, value) = match self.entry(offset) {
            Ok(e) => e,
            Err(e) => {
                self.invalidate();
                return Err(e);
            }
        };
        if shared == 0 {
            self.key = KeySrc::Block(unshared);
        } else {
            let bytes = self.block.as_deref().unwrap_or(&[]);
            match &self.key {
                KeySrc::Buf if shared <= self.key_buf.len() => self.key_buf.truncate(shared),
                KeySrc::Block(r) if shared <= r.len() => {
                    self.key_buf.clear();
                    self.key_buf
                        .extend_from_slice(&bytes[r.start..r.start + shared]);
                }
                _ => {
                    self.invalidate();
                    return Err(corrupt("block entry shared prefix"));
                }
            }
            self.key_buf.extend_from_slice(&bytes[unshared]);
            self.key = KeySrc::Buf;
        }
        self.cur = offset;
        self.next = value.end;
        self.value = value;
        Ok(())
    }

    fn load_restart(&mut self, i: usize) -> Result<()> {
        let at = self.restart(i)?;
        self.key = KeySrc::Block(0..0);
        self.load(at)
    }

    fn restart_key(&self, i: usize) -> Result<&[u8]> {
        let (shared, unshared, _) = self.entry(self.restart(i)?)?;
        if shared != 0 {
            return Err(corrupt("block restart entry"));
        }
        Ok(&self.bytes()[unshared])
    }

    pub(crate) fn seek_to_first(&mut self) -> Result<()> {
        if self.restart_count == 0 {
            self.invalidate();
            return Ok(());
        }
        self.load_restart(0)
    }

    /// Positions on the first entry `>= target`, or invalid if the block has none.
    pub(crate) fn seek(&mut self, target: &[u8]) -> Result<()> {
        if self.restart_count == 0 {
            self.invalidate();
            return Ok(());
        }
        // The last restart whose key is < target; scanning forward from it finds the answer.
        let (mut lo, mut hi) = (0, self.restart_count);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            match self.restart_key(mid) {
                Ok(k) if k < target => lo = mid,
                Ok(_) => hi = mid,
                Err(e) => {
                    self.invalidate();
                    return Err(e);
                }
            }
        }
        self.load_restart(lo)?;
        while self.valid() && self.key() < target {
            self.next()?;
        }
        Ok(())
    }

    pub(crate) fn next(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }
        self.load(self.next)
    }

    /// Moves to the next row start in this block (one binary search, no cell decoding), or
    /// becomes invalid if no later row starts here.
    pub(crate) fn skip_row(&mut self) -> Result<()> {
        if !self.valid() {
            return Ok(());
        }
        let (mut lo, mut hi) = (0, self.row_start_count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.row_start(mid)? <= self.cur {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == self.row_start_count {
            self.invalidate();
            return Ok(());
        }
        // The row start's shared prefix lies within the current key's row prefix (FORMAT
        // §4.2), so it decodes from the current key.
        let at = self.row_start(lo)?;
        if at <= self.cur {
            self.invalidate();
            return Err(corrupt("block row-start table"));
        }
        self.load(at)
    }
}
