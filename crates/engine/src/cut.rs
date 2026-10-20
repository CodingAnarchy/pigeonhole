//! Per-shard visibility (prototype for #717, option (b2); `.coord/proto-717b.md`).
//!
//! A snapshot is a *cut*: one seqno per shard plus the cross-shard seqnos in flight when it
//! was taken, which it excludes on every shard. An entry on shard S with seqno `e` is
//! visible at a cut iff `e <= v[S]` and `e` is not excluded. A single-shard commit is
//! visible once its own shard has written and applied its group; a cross-shard commit once
//! applied on every participant and released by its coordinator. No shard's component waits
//! for another shard's groups or for unrelated cross-shard commits.
//!
//! Each shard publishes one [`VisLine`]:
//! - `own`: the first seqno of its groups not yet applied, or not yet where a crash recovers
//!   them (D200); `u64::MAX` when none. Today's `pending` without `held`.
//! - `mark`: a cross-shard reservation in progress (`next_seqno` loaded before the
//!   `fetch_add`), or, with more held commits than slots, the oldest held one; `u64::MAX`
//!   otherwise. Every component is cut below the lowest mark.
//! - `held[..]`: the cross-shard seqnos this shard coordinates that are not yet applied
//!   everywhere, one per slot (0 = empty), with `count` as a hint to skip empty slots.
//!
//! Store order on a reservation: `mark` before the `fetch_add`; the slot, then `count`,
//! then `mark := MAX`. On a release: the slot and `count`, then `mark` (which only matters
//! in overflow). Load order of a cut: `next_seqno`; then, over every shard, `mark`, `count`
//! and the slots (pass 1); then, over every shard, `own` (pass 2). With pass 1 before pass
//! 2, a release observed as an empty slot happened before every `own` load, and each
//! participant had applied its share with `own` above the seqno (a participant applies a
//! share only then, `Shard::decide_share`), so the commit is either excluded everywhere or
//! visible on every participant.
//!
//! The shared-memory lines keep today's protocol (`pending = min(own, held)`), so reader
//! processes, durable snapshots, freezes and the compaction bound read the *floor*, which
//! is a valid cut (every component at the floor, nothing excluded).

use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole_format::Seqno;
use pigeonhole_shm::ShmRegion;
use smallvec::SmallVec;

/// Components inline up to this many shards (no allocation on the latest-read path).
const INLINE_SHARDS: usize = 16;
/// Excluded seqnos inline up to this many.
const INLINE_EXCLUDED: usize = 4;

/// Held slots per shard. Beyond them, the shard's oldest held seqno becomes a global floor
/// (`mark`) until the count drops.
pub(crate) const HELD_SLOTS: usize = 13;

/// `CommitInfo::shard` of a cross-shard commit: its visibility is its release plus the
/// marks, not one shard's component.
pub(crate) const NO_SHARD: u16 = u16::MAX;

/// One shard's published visibility state. One line: a cut touches it twice (`mark` and the
/// slots in pass 1, `own` in pass 2).
#[repr(align(128))]
#[derive(Debug)]
pub(crate) struct VisLine {
    pub own: AtomicU64,
    pub mark: AtomicU64,
    pub count: AtomicU64,
    pub held: [AtomicU64; HELD_SLOTS],
}

impl Default for VisLine {
    fn default() -> Self {
        Self {
            own: AtomicU64::new(u64::MAX),
            mark: AtomicU64::new(u64::MAX),
            count: AtomicU64::new(0),
            held: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Every shard's line, and the cut over them.
#[derive(Debug)]
pub(crate) struct Visibility {
    lines: Box<[VisLine]>,
    /// Cuts taken while a shard held more commits than slots (the floor fallback).
    pub overflow_cuts: AtomicU64,
}

impl Visibility {
    pub(crate) fn new(shards: usize) -> Self {
        Self {
            lines: (0..shards.max(1)).map(|_| VisLine::default()).collect(),
            overflow_cuts: AtomicU64::new(0),
        }
    }

    pub(crate) fn shards(&self) -> usize {
        self.lines.len()
    }

    #[inline]
    pub(crate) fn line(&self, shard: u16) -> &VisLine {
        &self.lines[usize::from(shard)]
    }

    /// Publishes shard `shard`'s `own` watermark (Release).
    #[inline]
    pub(crate) fn publish_own(&self, shard: u16, own: Seqno) {
        self.line(shard).own.store(own, Ordering::Release);
    }

    #[inline]
    pub(crate) fn own(&self, shard: u16) -> Seqno {
        self.line(shard).own.load(Ordering::Acquire)
    }

    /// The lowest mark over every shard (`u64::MAX` when none is set).
    #[inline]
    pub(crate) fn mark_min(&self) -> Seqno {
        self.lines
            .iter()
            .map(|l| l.mark.load(Ordering::Acquire))
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Whether a single-shard commit at `seqno` on `shard` is visible: its shard's groups up
    /// to it are written and applied, and no cross-shard reservation below it is in progress.
    #[inline]
    pub(crate) fn single_visible(&self, shard: u16, seqno: Seqno) -> bool {
        self.own(shard) > seqno && self.mark_min() > seqno
    }

    /// Whether a released cross-shard commit at `seqno` is visible: no mark below it. (Its
    /// participants' `own` passed it before they applied, and never drops below it again.)
    #[inline]
    pub(crate) fn cross_visible(&self, seqno: Seqno) -> bool {
        self.mark_min() > seqno
    }

    /// Whether a commit reported with `CommitInfo::shard` = `shard` is visible now.
    #[inline]
    pub(crate) fn visible_now(&self, shard: u16, seqno: Seqno) -> bool {
        if shard == NO_SHARD {
            self.cross_visible(seqno)
        } else {
            self.single_visible(shard, seqno)
        }
    }

    /// Takes a cut (see the module documentation for the load order). `pause` runs after
    /// pass 1 (`0`) and after pass 2's first shard (`1`): test hooks.
    pub(crate) fn cut(&self, shm: &ShmRegion, mut pause: impl FnMut(u8)) -> Cut {
        let n = shm.next_seqno();
        let mut r = u64::MAX;
        let mut e: SmallVec<[Seqno; INLINE_EXCLUDED]> = SmallVec::new();
        let mut overflow = false;
        for l in &self.lines {
            let m = l.mark.load(Ordering::Acquire);
            r = r.min(m);
            if l.count.load(Ordering::Acquire) != 0 {
                for s in &l.held {
                    let s = s.load(Ordering::Acquire);
                    if s != 0 {
                        e.push(s);
                    }
                }
            }
            if m != u64::MAX && l.count.load(Ordering::Relaxed) as usize >= HELD_SLOTS {
                overflow = true;
            }
        }
        pause(0);
        let mut v: SmallVec<[Seqno; INLINE_SHARDS]> = SmallVec::with_capacity(self.lines.len());
        for (i, l) in self.lines.iter().enumerate() {
            let own = l.own.load(Ordering::Acquire);
            v.push(n.min(r).min(own).saturating_sub(1));
            if i == 0 {
                pause(1);
            }
        }
        e.sort_unstable();
        e.dedup();
        if overflow {
            self.overflow_cuts.fetch_add(1, Ordering::Relaxed);
        }
        let floor = v
            .iter()
            .copied()
            .min()
            .unwrap_or(0)
            .min(e.first().map_or(u64::MAX, |f| f.saturating_sub(1)));
        Cut { floor, v, e }
    }

    // ---- the coordinating shard's side (one thread per shard) ----

    /// Before a cross-shard reservation: the mark is at most `next` (the lower bound of the
    /// seqno about to be reserved). In overflow the mark is already below it.
    #[inline]
    pub(crate) fn reserve_mark(&self, shard: u16, next: Seqno) {
        self.line(shard).mark.fetch_min(next, Ordering::AcqRel);
    }

    /// Publishes held seqno `seqno` in a free slot, or reports that none is free. Then sets
    /// the mark: `MAX`, or `overflow_min` (the shard's oldest held seqno) when the shard
    /// holds more than the slots take (`overflow_min` is `None` otherwise).
    pub(crate) fn held_insert(&self, shard: u16, seqno: Seqno, slot: Option<usize>) {
        let l = self.line(shard);
        if let Some(i) = slot {
            l.held[i].store(seqno, Ordering::Release);
            l.count.fetch_add(1, Ordering::Release);
        }
    }

    /// Clears held slot `i` (a release).
    pub(crate) fn held_clear(&self, shard: u16, i: usize) {
        let l = self.line(shard);
        l.held[i].store(0, Ordering::Release);
        l.count.fetch_sub(1, Ordering::Release);
    }

    /// Sets the mark after the slots are up to date: the shard's oldest held seqno that has
    /// no slot, or `MAX`.
    #[inline]
    pub(crate) fn set_mark(&self, shard: u16, mark: Seqno) {
        self.line(shard).mark.store(mark, Ordering::Release);
    }
}

/// A consistent cut: a read point per shard and the cross-shard seqnos excluded everywhere
/// (sorted). `floor` is the highest seqno at or below which every commit is included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cut {
    pub(crate) floor: Seqno,
    pub(crate) v: SmallVec<[Seqno; INLINE_SHARDS]>,
    pub(crate) e: SmallVec<[Seqno; INLINE_EXCLUDED]>,
}

impl Cut {
    /// The global cut at `seqno`: every component there, nothing excluded (a durable
    /// snapshot, a reader process's snapshot, a backup).
    pub(crate) fn global(seqno: Seqno, shards: usize) -> Self {
        Self {
            floor: seqno,
            v: std::iter::repeat_n(seqno, shards.max(1)).collect(),
            e: SmallVec::new(),
        }
    }

    /// The highest seqno at or below which every commit is in the cut.
    #[inline]
    pub fn floor(&self) -> Seqno {
        self.floor
    }

    /// Shard `shard`'s component.
    #[inline]
    pub(crate) fn component(&self, shard: u16) -> Seqno {
        self.v
            .get(usize::from(shard))
            .copied()
            .unwrap_or(self.floor)
    }

    /// The read point for a tablet on `shard` whose data moved there with everything at or
    /// below `tablet_floor` applied on the source (`TabletEntry::visible_floor`).
    #[inline]
    pub(crate) fn read_point(&self, shard: u16, tablet_floor: Seqno) -> ReadPoint<'_> {
        ReadPoint {
            seqno: self.component(shard).max(tablet_floor),
            floor: self.floor,
            excluded: &self.e,
        }
    }

    /// The same cut capped at `seqno` (a test hook: `Snapshot::at_seqno`).
    pub(crate) fn at_most(&self, seqno: Seqno) -> Self {
        Self {
            floor: self.floor.min(seqno),
            v: self.v.iter().map(|s| (*s).min(seqno)).collect(),
            e: self.e.iter().copied().filter(|e| *e <= seqno).collect(),
        }
    }

    /// Whether the cut includes seqno `seqno` of a commit on `shard` (`NO_SHARD`: a
    /// cross-shard commit, included iff not excluded and below every component it could
    /// have a share on; this checks every component).
    pub fn includes(&self, shard: u16, seqno: Seqno) -> bool {
        if self.e.binary_search(&seqno).is_ok() {
            return false;
        }
        if shard == NO_SHARD {
            self.v.iter().all(|c| *c >= seqno)
        } else {
            self.component(shard) >= seqno
        }
    }

    /// The components.
    pub fn components(&self) -> &[Seqno] {
        &self.v
    }

    /// The excluded seqnos.
    pub fn excluded(&self) -> &[Seqno] {
        &self.e
    }
}

/// What one shard's read uses of a cut.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadPoint<'a> {
    pub seqno: Seqno,
    pub floor: Seqno,
    pub excluded: &'a [Seqno],
}

impl ReadPoint<'_> {
    /// A read point with nothing excluded (unit tests).
    #[cfg(test)]
    pub(crate) const fn plain(seqno: Seqno) -> ReadPoint<'static> {
        ReadPoint {
            seqno,
            floor: seqno,
            excluded: &[],
        }
    }

    #[inline]
    pub(crate) fn visible(&self, seqno: Seqno) -> bool {
        seqno <= self.seqno && (seqno <= self.floor || self.excluded.binary_search(&seqno).is_err())
    }
}
