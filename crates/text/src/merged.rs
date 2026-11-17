//! One term's postings in a version 2 index (#1585, ICR 0143): its cells over its packed
//! blocks, as one stream in document order.
//!
//! **The merged view.** A document's posting is its cell if it has one, else its posting in
//! a block. A tombstone cell ([`TOMBSTONE`], or an empty position list) hides the block's
//! posting and is no posting itself: it is passed, never scored.
//!
//! **Two tiers, moved apart.** The cell tier is a [`RowCells`] its owner keeps (a walk has
//! one in a version 1 index too); the block tier is this type's own. Each has a head, and
//! every move is a target ("at or after this document", "past this one"), so a move
//! interrupted by a `Pending` read is the same move when it is made again: nothing is
//! consumed twice. [`settle`](Merged::settle) then reads whatever head is missing and passes
//! tombstones, after which [`head`](Merged::head) says which tier the next posting is in.

use std::cmp::Ordering;
use std::task::Poll;

use pigeonhole::{RowCells, Snapshot};

use crate::block::Block;
use crate::error::{Result, corrupt};
use crate::index::Rows;
use crate::layout::*;
use crate::maxscore::{Bm25, Reads, under};

/// What lets a stepping walk pass a whole block (#1585): the scoring statistics, the term's
/// weight, and the walk's floor ([`Walk::floor`](crate::maxscore::Walk)). A block whose
/// bound, the most any of its postings adds, is under the floor is passed without being
/// decoded. Exact for the same reason a posting under the floor is passed; a cell over one
/// of the block's documents is still read, in the cell tier.
pub(crate) type Gate<'a> = Option<(&'a Bm25, f64, f64)>;

/// `Poll<Result<T>>` to `T`, returning a `Pending` or an error.
macro_rules! ready {
    ($e:expr) => {
        match $e {
            Poll::Ready(Ok(v)) => v,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
            Poll::Pending => return Poll::Pending,
        }
    };
}

/// Where the cell tier's head is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CellAt {
    /// Not read yet, or passed: the next read finds it.
    Need,
    /// Held in the buffers.
    Have,
    /// The row's cells are done.
    End,
}

/// Which tier the merged stream's next posting is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tier {
    Cell,
    Block,
}

/// What the cell tier says about one document, without a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    /// Its head is past the document, or the cells are done: the document has no cell.
    Absent,
    /// Its head is the document's cell.
    Here,
    /// Its head is before the document (or not read): only a read can say.
    Behind,
}

pub(crate) struct Merged<'t> {
    /// The cell tier's family: [`POST`] for postings, [`POS`] for positions.
    family: &'static str,
    prefix: Option<Vec<u8>>,
    cell: CellAt,
    /// The cell tier's head: buffers, reused.
    cell_id: Vec<u8>,
    cell_value: Vec<u8>,
    cell_number: Option<u64>,
    blocks: RowCells<'t>,
    /// The block the block tier stands in, and the posting it stands on there: the block's
    /// length when the next block has to be read.
    block: Block,
    at: usize,
    blocks_end: bool,
    /// Cells read, and block postings passed: the walk's steps.
    pub(crate) stepped: u64,
}

impl<'t> Merged<'t> {
    /// The merged stream of `row`, over the cell family `family`, for ids starting with
    /// `prefix`. `t` has the block family (the caller's check).
    pub(crate) fn new(
        t: &'t Rows,
        s: &Snapshot,
        row: &[u8],
        family: &'static str,
        prefix: Option<&[u8]>,
    ) -> Result<Self> {
        let read = t.row(row).family(POSTB).snapshot(s);
        // A block is named by its last id, so a block holding ids with the prefix can be
        // named past it: from the prefix on, cut where a block's ids leave it (`trim`).
        let read = match prefix {
            Some(p) => read.qualifier_range(p..),
            None => read,
        };
        Ok(Merged {
            family,
            prefix: prefix.map(<[u8]>::to_vec),
            cell: CellAt::Need,
            cell_id: Vec::new(),
            cell_value: Vec::new(),
            cell_number: None,
            blocks: read.cells()?,
            block: Block::default(),
            at: 0,
            blocks_end: false,
            stepped: 0,
        })
    }

    /// The block tier's head, if it has one read.
    #[inline]
    fn block_id(&self) -> Option<&[u8]> {
        (!self.blocks_end && self.at < self.block.len()).then(|| self.block.id(self.at))
    }

    /// Keeps the block tier inside the prefix: past ids before it, and ended at the first
    /// id after it.
    #[inline]
    fn trim(&mut self) {
        let Some(p) = &self.prefix else {
            return;
        };
        if self.at < self.block.len() && self.block.id(self.at) < p.as_slice() {
            self.at = self.block.seek(self.at, p);
        }
        if self.at < self.block.len() && !self.block.id(self.at).starts_with(p) {
            self.blocks_end = true;
        }
    }

    /// Reads the cell tier's head if it is missing: the next cell, or with `min` the first
    /// at or after it (a seek in an async walk, steps in a sync one).
    fn fill_cell<R: Reads>(
        &mut self,
        cells: &mut RowCells<'_>,
        cx: &mut R::Cx<'_, '_>,
        min: Option<&[u8]>,
    ) -> Poll<Result<()>> {
        while self.cell == CellAt::Need {
            let e = match min {
                Some(m) if R::ASYNC => ready!(R::seek(cells, cx, self.family, m)),
                _ => ready!(R::next(cells, cx)),
            };
            self.stepped += 1;
            match e {
                None => self.cell = CellAt::End,
                // A cell passed costs a key compare: only the head is copied.
                Some(e) if min.is_some_and(|m| e.qualifier < m) => {}
                Some(e) => {
                    self.cell_id.clear();
                    self.cell_id.extend_from_slice(e.qualifier);
                    self.cell_value.clear();
                    self.cell_value.extend_from_slice(e.cell.value());
                    self.cell_number = e.cell.as_u64();
                    self.cell = CellAt::Have;
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    /// Reads the block tier's next block if it stands in none: the next, or with `min` the
    /// first that can hold an id at or after it. A block passed is never decoded.
    fn fill_block<R: Reads>(
        &mut self,
        cx: &mut R::Cx<'_, '_>,
        min: Option<&[u8]>,
        gate: Gate<'_>,
    ) -> Poll<Result<()>> {
        while !self.blocks_end && self.at >= self.block.len() {
            let e = match min {
                Some(m) if R::ASYNC => ready!(R::seek(&mut self.blocks, cx, POSTB, m)),
                _ => ready!(R::next(&mut self.blocks, cx)),
            };
            let Some(e) = e else {
                self.blocks_end = true;
                break;
            };
            if min.is_some_and(|m| e.qualifier < m) {
                self.stepped += crate::block::MAX_POSTINGS as u64;
                continue;
            }
            if let Some((bm, weight, floor)) = gate {
                match Block::peek_bound(e.cell.value(), bm) {
                    Ok((n, bound)) if under(weight * bound, floor) => {
                        self.stepped += n as u64;
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        self.blocks_end = true;
                        return Poll::Ready(Err(e));
                    }
                }
            }
            let decoded = match self.family == POST {
                true => self.block.decode_postings(e.cell.value()),
                false => self.block.decode(e.cell.value()),
            };
            if let Err(e) = decoded {
                self.blocks_end = true;
                return Poll::Ready(Err(e));
            }
            if self.block.id(self.block.len() - 1) != e.qualifier {
                self.blocks_end = true;
                return Poll::Ready(Err(corrupt("a posting block isn't named by its last id")));
            }
            self.at = match min {
                Some(m) => self.block.seek(0, m),
                None => 0,
            };
            self.trim();
        }
        Poll::Ready(Ok(()))
    }

    /// Whether the cell tier's head is a tombstone.
    fn tombstone(&self) -> Result<bool> {
        if self.family == POST {
            match self.cell_number {
                Some(v) => Ok(v == TOMBSTONE),
                None => Err(corrupt("a posting isn't a u64")),
            }
        } else {
            Ok(self.cell_value.is_empty())
        }
    }

    /// Passes every posting at or before `doc`, in both tiers. Meant for a short step: a
    /// jump ahead goes by [`cells_to`](Self::cells_to) and [`blocks_to`](Self::blocks_to)
    /// first.
    #[inline]
    pub(crate) fn pass(&mut self, doc: &[u8]) {
        if self.cell == CellAt::Have && self.cell_id.as_slice() <= doc {
            self.cell = CellAt::Need;
        }
        if !self.blocks_end {
            let before = self.at;
            while self.at < self.block.len() && self.block.id(self.at) <= doc {
                self.at += 1;
            }
            if self.at != before {
                self.stepped += (self.at - before) as u64;
                self.trim();
            }
        }
    }

    /// Passes the head [`head`](Self::head) named: its cell, and the block's posting for the
    /// same document.
    #[inline]
    pub(crate) fn skip(&mut self, tier: Tier) {
        match tier {
            Tier::Cell => {
                if self.block_id() == Some(self.cell_id.as_slice()) {
                    self.at += 1;
                    self.trim();
                }
                self.cell = CellAt::Need;
            }
            Tier::Block => {
                self.at += 1;
                self.stepped += 1;
                self.trim();
            }
        }
    }

    /// Moves the cell tier to its first cell at or after `doc`.
    pub(crate) fn cells_to<R: Reads>(
        &mut self,
        cells: &mut RowCells<'_>,
        cx: &mut R::Cx<'_, '_>,
        doc: &[u8],
    ) -> Poll<Result<()>> {
        if self.cell == CellAt::Have && self.cell_id.as_slice() < doc {
            self.cell = CellAt::Need;
        }
        self.fill_cell::<R>(cells, cx, Some(doc))
    }

    /// Moves the block tier to its first posting at or after `doc`.
    pub(crate) fn blocks_to<R: Reads>(
        &mut self,
        cx: &mut R::Cx<'_, '_>,
        doc: &[u8],
    ) -> Poll<Result<()>> {
        if self.blocks_end {
            return Poll::Ready(Ok(()));
        }
        let n = self.block.len();
        if self.at < n {
            if self.block.id(n - 1) >= doc {
                let before = self.at;
                self.at = self.block.seek(self.at, doc);
                self.stepped += (self.at - before) as u64;
                self.trim();
                return Poll::Ready(Ok(()));
            }
            self.stepped += (n - self.at) as u64;
            self.at = n;
        }
        self.fill_block::<R>(cx, Some(doc), None)
    }

    /// Reads the heads that are missing and passes tombstones: after it,
    /// [`head`](Self::head) is the stream's next posting. With a `gate`, the next posting
    /// that a block the gate lets through holds.
    pub(crate) fn settle<R: Reads>(
        &mut self,
        cells: &mut RowCells<'_>,
        cx: &mut R::Cx<'_, '_>,
        gate: Gate<'_>,
    ) -> Poll<Result<()>> {
        loop {
            ready!(self.fill_cell::<R>(cells, cx, None));
            ready!(self.fill_block::<R>(cx, None, gate));
            if self.cell != CellAt::Have {
                return Poll::Ready(Ok(()));
            }
            let order = match self.block_id() {
                Some(b) => self.cell_id.as_slice().cmp(b),
                None => Ordering::Less,
            };
            if order == Ordering::Greater {
                return Poll::Ready(Ok(()));
            }
            match self.tombstone() {
                Ok(true) => {}
                Ok(false) => return Poll::Ready(Ok(())),
                Err(e) => return Poll::Ready(Err(e)),
            }
            // The tombstone, and the block's posting it hides.
            self.cell = CellAt::Need;
            if order == Ordering::Equal {
                self.at += 1;
                self.trim();
            }
        }
    }

    /// The tier the next posting is in, once [`settle`](Self::settle)d: the cell's when
    /// both have the document. `None` at the end.
    #[inline]
    pub(crate) fn head(&self) -> Option<Tier> {
        match (self.cell == CellAt::Have, self.block_id()) {
            (false, None) => None,
            (true, None) => Some(Tier::Cell),
            (false, Some(_)) => Some(Tier::Block),
            (true, Some(b)) => Some(if self.cell_id.as_slice() <= b {
                Tier::Cell
            } else {
                Tier::Block
            }),
        }
    }

    /// The head's document.
    #[inline]
    pub(crate) fn id(&self, tier: Tier) -> &[u8] {
        match tier {
            Tier::Cell => &self.cell_id,
            Tier::Block => self.block.id(self.at),
        }
    }

    /// The head's posting value ([`posting`]): over [`POST`] cells.
    #[inline]
    pub(crate) fn post(&self, tier: Tier) -> Result<u64> {
        match tier {
            Tier::Cell => self
                .cell_number
                .ok_or_else(|| corrupt("a posting isn't a u64")),
            Tier::Block => {
                let (tf, len) = self.block.posting(self.at);
                Ok(posting(tf, len))
            }
        }
    }

    /// The head's encoded positions: over [`POS`] cells.
    #[inline]
    pub(crate) fn positions(&self, tier: Tier) -> &[u8] {
        match tier {
            Tier::Cell => &self.cell_value,
            Tier::Block => self.block.positions(self.at),
        }
    }

    /// What the cell tier says of `doc` where it stands.
    pub(crate) fn cell_side(&self, doc: &[u8]) -> Side {
        match self.cell {
            CellAt::End => Side::Absent,
            CellAt::Need => Side::Behind,
            CellAt::Have => match self.cell_id.as_slice().cmp(doc) {
                Ordering::Greater => Side::Absent,
                Ordering::Equal => Side::Here,
                Ordering::Less => Side::Behind,
            },
        }
    }

    /// The cell tier's head as a posting, standing on the document asked for
    /// ([`Side::Here`]): `None` for a tombstone.
    pub(crate) fn cell_post(&self) -> Result<Option<u64>> {
        Ok((!self.tombstone()?).then_some(self.cell_number.unwrap_or(TOMBSTONE)))
    }

    /// The cell tier's head as positions ([`Side::Here`]): `None` for a tombstone.
    pub(crate) fn cell_positions(&self) -> Option<&[u8]> {
        (!self.cell_value.is_empty()).then_some(self.cell_value.as_slice())
    }

    /// Whether the block tier, moved to `doc` ([`blocks_to`](Self::blocks_to)), stands on
    /// it: then [`Tier::Block`]'s accessors are its posting.
    pub(crate) fn block_has(&self, doc: &[u8]) -> bool {
        self.block_id() == Some(doc)
    }
}

/// `doc`'s posting in the blocks of `row`, by one read: the first block named at or after
/// `doc`, and its posting there. With the block's qualifier and value, for a caller that
/// confirms the block inside a transaction.
pub(crate) struct Found {
    pub(crate) qualifier: Vec<u8>,
    pub(crate) value: Vec<u8>,
    /// The document's place in the block, if it is there.
    pub(crate) at: Option<usize>,
}

/// The block that would hold `doc` in `row`, decoded into `block`; `None` when no block is
/// named at or after it.
pub(crate) fn find_block(
    t: &Rows,
    s: Option<&Snapshot>,
    row: &[u8],
    doc: &[u8],
    block: &mut Block,
) -> Result<Option<Found>> {
    let read = t.row(row).family(POSTB).qualifier_range(doc..);
    let read = match s {
        Some(s) => read.snapshot(s),
        None => read,
    };
    let mut cells = read.cells()?;
    let Some(e) = cells.next()? else {
        return Ok(None);
    };
    block.decode(e.cell.value())?;
    let i = block.seek(0, doc);
    Ok(Some(Found {
        qualifier: e.qualifier.to_vec(),
        value: e.cell.value().to_vec(),
        at: (i < block.len() && block.id(i) == doc).then_some(i),
    }))
}
