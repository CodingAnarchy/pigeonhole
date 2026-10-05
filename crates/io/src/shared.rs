use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64};

use crate::Result;

/// How [`Vfs::open_shared`](crate::Vfs::open_shared) treats an existing region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedOpen {
    /// Create; fail with `AlreadyExists` if it exists.
    CreateNew,
    /// Attach to an existing region; fail with `NotFound` if absent.
    Attach,
}

/// A mapped shared-memory region (or a heap stand-in), cheap to clone.
///
/// Other processes may write the memory at any time, so this type never hands out `&[u8]`
/// or `&mut [u8]` over it: callers use atomics at fixed offsets, or copy bytes in and out.
/// `pigeonhole-memtable` builds its offset-linked skiplist on [`SharedRegion::base_ptr`].
#[derive(Debug, Clone)]
pub struct SharedRegion {
    _priv: (),
}

impl SharedRegion {
    /// A private, zeroed, heap-backed region (the mock used by tests and by `shm`'s in-memory
    /// mode). 64-byte aligned.
    pub fn heap(len: usize) -> Self {
        todo!()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        todo!()
    }

    /// Whether the region is empty.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// The `AtomicU32` at `offset` (4-byte aligned). Panics if out of bounds or misaligned.
    pub fn atomic_u32(&self, offset: usize) -> &AtomicU32 {
        todo!()
    }

    /// The `AtomicU64` at `offset` (8-byte aligned). Panics if out of bounds or misaligned.
    pub fn atomic_u64(&self, offset: usize) -> &AtomicU64 {
        todo!()
    }

    /// Copies `dst.len()` bytes out of the region (volatile; may observe concurrent writes,
    /// which callers detect with checksums or version re-checks).
    pub fn read(&self, offset: usize, dst: &mut [u8]) {
        todo!()
    }

    /// Copies `src` into the region (volatile). Callers ensure no one reads the range as
    /// stable data until they publish it with a release store.
    pub fn write(&self, offset: usize, src: &[u8]) {
        todo!()
    }

    /// Base address of the mapping, valid for [`SharedRegion::len`] bytes for as long as any
    /// clone of this region lives. For crates allowed `unsafe` (memtable).
    pub fn base_ptr(&self) -> NonNull<u8> {
        todo!()
    }

    /// Binds `[offset, offset + len)` to NUMA node `node` (`mbind` on Linux; a no-op
    /// elsewhere and for heap regions).
    pub fn bind_numa(&self, offset: usize, len: usize, node: u32) -> Result<()> {
        todo!()
    }
}
