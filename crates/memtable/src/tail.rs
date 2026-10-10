//! The stale-tail index (D194, ICR 0013): for a node whose level-0 successor was in the
//! same column when it was linked, a later node of that column, as far down the column's
//! version chain as was then known. A reader that has finished a column jumps there instead
//! of stepping every superseded version.
//!
//! Process memory owned by the writer's [`Memtable`](crate::Memtable), never in the arena:
//! the shared layout is unchanged, and reader processes have no index. Each entry is
//! written once, by the writer, before the node it describes is linked, so a reader that
//! reached that node through a link (an acquire) sees it. An entry is never changed or
//! removed; the index is dropped with the last handle of the memtable.
//!
//! Nothing is allocated until the first entry: a memtable written in key order (no
//! overwrites) never records one, so it pays nothing beyond the empty index.

use std::ptr;

#[cfg(loom)]
use loom::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use crate::NULL;

/// Arena bytes per slot. A node is at least 36 bytes (a 12-byte header, one link and a
/// 17-byte internal key suffix, rounded to 4), so no two nodes start in one granule.
const GRANULE_BITS: u32 = 5;

/// Slots per page, a power of two, so a slot is found with shifts, not a division by the
/// arena's chunk size (#449): 8192 (a 32 KiB page per 256 KiB of arena). Loom models keep
/// pages small, since every slot is a modelled atomic.
#[cfg(not(loom))]
const PAGE_BITS: u32 = 13;
#[cfg(loom)]
const PAGE_BITS: u32 = 2;
const PAGE_LEN: usize = 1 << PAGE_BITS;

/// Pages of one `u32` per granule (an eighth of the arena bytes they cover), and a
/// directory of the pages; each allocated when the writer records its first entry.
pub(crate) struct TailIndex {
    /// Pages covering the arena: the directory's length.
    pages: usize,
    /// The directory (a leaked `Box<[AtomicPtr<AtomicU32>]>` of `pages` entries, freed on
    /// drop), or null. Each entry is a page (a leaked `Box<[AtomicU32]>` of [`PAGE_LEN`]
    /// slots, freed on drop), or null.
    dir: AtomicPtr<AtomicPtr<AtomicU32>>,
}

impl std::fmt::Debug for TailIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailIndex")
            .field("pages", &self.pages)
            .finish_non_exhaustive()
    }
}

impl Drop for TailIndex {
    fn drop(&mut self) {
        let dir = self.dir.load(Ordering::Acquire);
        if dir.is_null() {
            return;
        }
        // SAFETY: a non-null directory is a `Box<[AtomicPtr<AtomicU32>]>` of `pages`
        // entries that `set` leaked, and so is each non-null page (of `PAGE_LEN` slots). The
        // index is being dropped, so nothing refers into either any more.
        unsafe {
            let dir = Box::from_raw(ptr::slice_from_raw_parts_mut(dir, self.pages));
            for entry in dir.iter() {
                let page = entry.load(Ordering::Acquire);
                if !page.is_null() {
                    drop(Box::from_raw(ptr::slice_from_raw_parts_mut(page, PAGE_LEN)));
                }
            }
        }
    }
}

impl TailIndex {
    /// An empty index over an arena of `region_len` bytes.
    pub(crate) fn new(region_len: usize) -> Self {
        Self {
            pages: (region_len >> GRANULE_BITS).div_ceil(PAGE_LEN),
            dir: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// `node`'s page and slot in it.
    #[inline]
    fn locate(node: u32) -> (usize, usize) {
        let granule = (node >> GRANULE_BITS) as usize;
        (granule >> PAGE_BITS, granule & (PAGE_LEN - 1))
    }

    /// Records `tail` for `node`. Writer only, before `node` is linked.
    pub(crate) fn set(&self, node: u32, tail: u32) {
        let (page_no, slot) = Self::locate(node);
        let mut dir = self.dir.load(Ordering::Relaxed);
        if dir.is_null() {
            let fresh: Box<[AtomicPtr<AtomicU32>]> = (0..self.pages)
                .map(|_| AtomicPtr::new(ptr::null_mut()))
                .collect();
            dir = Box::into_raw(fresh).cast::<AtomicPtr<AtomicU32>>();
            self.dir.store(dir, Ordering::Release);
        }
        // SAFETY: `dir` is a directory of `pages` entries that the index owns until it
        // drops, and `page_no` is below that (`node` is an offset in the arena).
        let entry = unsafe { &*dir.add(page_no) };
        let mut page = entry.load(Ordering::Relaxed);
        if page.is_null() {
            let fresh: Box<[AtomicU32]> = (0..PAGE_LEN).map(|_| AtomicU32::new(NULL)).collect();
            page = Box::into_raw(fresh).cast::<AtomicU32>();
            entry.store(page, Ordering::Release);
        }
        // SAFETY: `page` is a page of `PAGE_LEN` slots that the index owns until it drops,
        // and `slot` is below that length.
        unsafe { &*page.add(slot) }.store(tail, Ordering::Relaxed);
    }

    /// `node`'s tail, or [`NULL`] when none was recorded.
    #[inline]
    pub(crate) fn get(&self, node: u32) -> u32 {
        let (page_no, slot) = Self::locate(node);
        let dir = self.dir.load(Ordering::Acquire);
        if dir.is_null() || page_no >= self.pages {
            return NULL;
        }
        // SAFETY: as in `set`: a published directory and its pages live as long as the
        // index, `page_no` is below the directory's length and `slot` below a page's. The
        // entry was written before the node was linked, and the caller reached the node
        // through a link it loaded with acquire, so it is visible.
        let page = unsafe { &*dir.add(page_no) }.load(Ordering::Acquire);
        if page.is_null() {
            return NULL;
        }
        // SAFETY: as above.
        unsafe { &*page.add(slot) }.load(Ordering::Relaxed)
    }
}
