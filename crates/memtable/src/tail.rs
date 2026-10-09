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
const GRANULE: usize = 32;

/// A page per arena chunk of one `u32` per granule (an eighth of the chunk), and a
/// directory of the pages; each allocated when the writer records its first entry.
pub(crate) struct TailIndex {
    chunk_size: usize,
    /// Arena chunks: the directory's length.
    chunks: usize,
    /// The directory (a leaked `Box<[AtomicPtr<AtomicU32>]>` of `chunks` entries, freed on
    /// drop), or null. Each entry is its chunk's page (a leaked `Box<[AtomicU32]>` of
    /// [`TailIndex::page_len`] slots, freed on drop), or null.
    dir: AtomicPtr<AtomicPtr<AtomicU32>>,
}

impl std::fmt::Debug for TailIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailIndex")
            .field("chunk_size", &self.chunk_size)
            .field("chunks", &self.chunks)
            .finish_non_exhaustive()
    }
}

impl Drop for TailIndex {
    fn drop(&mut self) {
        let dir = self.dir.load(Ordering::Acquire);
        if dir.is_null() {
            return;
        }
        let len = self.page_len();
        // SAFETY: a non-null directory is a `Box<[AtomicPtr<AtomicU32>]>` of `chunks`
        // entries that `set` leaked, and so is each non-null page (of `len` slots). The
        // index is being dropped, so nothing refers into either any more.
        unsafe {
            let dir = Box::from_raw(ptr::slice_from_raw_parts_mut(dir, self.chunks));
            for entry in dir.iter() {
                let page = entry.load(Ordering::Acquire);
                if !page.is_null() {
                    drop(Box::from_raw(ptr::slice_from_raw_parts_mut(page, len)));
                }
            }
        }
    }
}

impl TailIndex {
    /// An empty index over an arena of `region_len` bytes in `chunk_size` chunks.
    pub(crate) fn new(region_len: usize, chunk_size: usize) -> Self {
        Self {
            chunk_size,
            chunks: region_len.div_ceil(chunk_size),
            dir: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Slots per page: enough for every granule a chunk offset can fall in.
    fn page_len(&self) -> usize {
        self.chunk_size.div_ceil(GRANULE)
    }

    fn locate(&self, node: u32) -> (usize, usize) {
        let off = node as usize;
        (off / self.chunk_size, off % self.chunk_size / GRANULE)
    }

    /// Records `tail` for `node`. Writer only, before `node` is linked.
    pub(crate) fn set(&self, node: u32, tail: u32) {
        let (chunk, slot) = self.locate(node);
        let mut dir = self.dir.load(Ordering::Relaxed);
        if dir.is_null() {
            let fresh: Box<[AtomicPtr<AtomicU32>]> = (0..self.chunks)
                .map(|_| AtomicPtr::new(ptr::null_mut()))
                .collect();
            dir = Box::into_raw(fresh).cast::<AtomicPtr<AtomicU32>>();
            self.dir.store(dir, Ordering::Release);
        }
        // SAFETY: `dir` is a directory of `chunks` entries that the index owns until it
        // drops, and `chunk` is below that (`node` is an offset in the arena).
        let entry = unsafe { &*dir.add(chunk) };
        let mut page = entry.load(Ordering::Relaxed);
        if page.is_null() {
            let fresh: Box<[AtomicU32]> =
                (0..self.page_len()).map(|_| AtomicU32::new(NULL)).collect();
            page = Box::into_raw(fresh).cast::<AtomicU32>();
            entry.store(page, Ordering::Release);
        }
        // SAFETY: `page` is a page of `page_len()` slots that the index owns until it
        // drops, and `slot` is below that length.
        unsafe { &*page.add(slot) }.store(tail, Ordering::Relaxed);
    }

    /// `node`'s tail, or [`NULL`] when none was recorded.
    #[inline]
    pub(crate) fn get(&self, node: u32) -> u32 {
        let (chunk, slot) = self.locate(node);
        let dir = self.dir.load(Ordering::Acquire);
        if dir.is_null() || chunk >= self.chunks {
            return NULL;
        }
        // SAFETY: as in `set`: a published directory and its pages live as long as the
        // index, `chunk` is below the directory's length and `slot` below a page's. The
        // entry was written before the node was linked, and the caller reached the node
        // through a link it loaded with acquire, so it is visible.
        let page = unsafe { &*dir.add(chunk) }.load(Ordering::Acquire);
        if page.is_null() {
            return NULL;
        }
        // SAFETY: as above.
        unsafe { &*page.add(slot) }.load(Ordering::Relaxed)
    }
}
