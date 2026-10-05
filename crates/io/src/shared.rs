use std::alloc::{self, Layout};
use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;
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
///
/// ```
/// use std::sync::atomic::Ordering;
/// use pigeonhole_io::SharedRegion;
///
/// let region = SharedRegion::heap(4096);
/// let other = region.clone(); // same memory
/// region.write(64, b"view");
/// region.atomic_u64(8).store(42, Ordering::Release);
///
/// let mut bytes = [0u8; 4];
/// other.read(64, &mut bytes);
/// assert_eq!(&bytes, b"view");
/// assert_eq!(other.atomic_u64(8).load(Ordering::Acquire), 42);
/// ```
#[derive(Clone)]
pub struct SharedRegion {
    inner: Arc<RegionInner>,
}

struct RegionInner {
    ptr: NonNull<u8>,
    len: usize,
    backing: Backing,
}

enum Backing {
    Heap(Layout),
    // Held for its `Drop`, which unmaps.
    Mapped(#[allow(dead_code)] crate::os::Mapping),
}

// SAFETY: the region is plain memory that every accessor touches only through atomics or
// volatile copies; the pointer stays valid while the `Arc` lives, on any thread.
unsafe impl Send for RegionInner {}
// SAFETY: as above; `&RegionInner` exposes no non-atomic shared references into the memory.
unsafe impl Sync for RegionInner {}

/// Alignment of heap regions.
const HEAP_ALIGN: usize = 64;

impl SharedRegion {
    /// A private, zeroed, heap-backed region (the mock used by tests and by `shm`'s in-memory
    /// mode). 64-byte aligned.
    pub fn heap(len: usize) -> Self {
        let layout = Layout::from_size_align(len.max(1), HEAP_ALIGN).expect("region too large");
        // SAFETY: `layout` has a non-zero size.
        let p = unsafe { alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(p).unwrap_or_else(|| alloc::handle_alloc_error(layout));
        Self {
            inner: Arc::new(RegionInner {
                ptr,
                len,
                backing: Backing::Heap(layout),
            }),
        }
    }

    /// Wraps an OS mapping of `mapping.len()` bytes.
    pub(crate) fn mapped(mapping: crate::os::Mapping) -> Self {
        Self {
            inner: Arc::new(RegionInner {
                ptr: mapping.ptr(),
                len: mapping.len(),
                backing: Backing::Mapped(mapping),
            }),
        }
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.inner.len
    }

    /// Whether the region is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }

    /// Pointer to `offset`, after checking that `[offset, offset + len)` is in bounds.
    fn at(&self, offset: usize, len: usize) -> *mut u8 {
        let end = offset.checked_add(len);
        assert!(
            end.is_some_and(|end| end <= self.inner.len),
            "shared region access out of bounds: {offset}+{len} > {}",
            self.inner.len
        );
        // SAFETY: `offset <= len`, so the result stays within (or one past) the allocation.
        unsafe { self.inner.ptr.as_ptr().add(offset) }
    }

    /// The `AtomicU32` at `offset` (4-byte aligned). Panics if out of bounds or misaligned.
    pub fn atomic_u32(&self, offset: usize) -> &AtomicU32 {
        let p = self.at(offset, 4);
        assert!(
            p.cast::<AtomicU32>().is_aligned(),
            "misaligned AtomicU32 at {offset}"
        );
        // SAFETY: in bounds and aligned (checked above); the memory lives as long as `self`
        // and is only ever accessed atomically or through volatile copies.
        unsafe { AtomicU32::from_ptr(p.cast()) }
    }

    /// The `AtomicU64` at `offset` (8-byte aligned). Panics if out of bounds or misaligned.
    pub fn atomic_u64(&self, offset: usize) -> &AtomicU64 {
        let p = self.at(offset, 8);
        assert!(
            p.cast::<AtomicU64>().is_aligned(),
            "misaligned AtomicU64 at {offset}"
        );
        // SAFETY: as in `atomic_u32`.
        unsafe { AtomicU64::from_ptr(p.cast()) }
    }

    /// Copies `dst.len()` bytes out of the region (volatile; may observe concurrent writes,
    /// which callers detect with checksums or version re-checks). Panics if out of bounds.
    pub fn read(&self, offset: usize, dst: &mut [u8]) {
        let src = self.at(offset, dst.len());
        // SAFETY: `src` is valid for `dst.len()` bytes (bounds checked) and cannot overlap
        // `dst`, which is an exclusive Rust borrow.
        unsafe { copy_from_region(src, dst) };
    }

    /// Copies `src` into the region (volatile). Callers ensure no one reads the range as
    /// stable data until they publish it with a release store. Panics if out of bounds.
    pub fn write(&self, offset: usize, src: &[u8]) {
        let dst = self.at(offset, src.len());
        // SAFETY: `dst` is valid for `src.len()` bytes (bounds checked); `src` is a Rust
        // borrow and the region never hands out references, so they cannot overlap.
        unsafe { copy_to_region(src, dst) };
    }

    /// Base address of the mapping, valid for [`SharedRegion::len`] bytes for as long as any
    /// clone of this region lives. For crates allowed `unsafe` (memtable).
    pub fn base_ptr(&self) -> NonNull<u8> {
        self.inner.ptr
    }

    /// Binds `[offset, offset + len)` to NUMA node `node` (`mbind` on Linux; a no-op
    /// elsewhere and for heap regions). Panics if the range is out of bounds.
    pub fn bind_numa(&self, offset: usize, len: usize, node: u32) -> Result<()> {
        let p = self.at(offset, len);
        match &self.inner.backing {
            Backing::Heap(_) => Ok(()),
            Backing::Mapped(_) => crate::os::bind_numa(p, len, node),
        }
    }
}

const WORD: usize = std::mem::size_of::<u64>();

/// Volatile copy out of the region, a word at a time once the region side is aligned.
///
/// # Safety
/// `src` must be valid for reads of `dst.len()` bytes and must not overlap `dst`.
unsafe fn copy_from_region(src: *const u8, dst: &mut [u8]) {
    let len = dst.len();
    let d = dst.as_mut_ptr();
    let head = src.align_offset(WORD).min(len);
    let mut i = 0;
    while i < head {
        // SAFETY: `i < len`; both pointers are valid for `len` bytes (caller contract).
        unsafe { d.add(i).write(src.add(i).read_volatile()) };
        i += 1;
    }
    while i + WORD <= len {
        // SAFETY: `src + i` is word aligned and `[i, i + WORD)` is in bounds on both sides;
        // the destination write is unaligned-tolerant.
        unsafe {
            let w = src.add(i).cast::<u64>().read_volatile();
            d.add(i).cast::<u64>().write_unaligned(w);
        }
        i += WORD;
    }
    while i < len {
        // SAFETY: `i < len`.
        unsafe { d.add(i).write(src.add(i).read_volatile()) };
        i += 1;
    }
}

/// Volatile copy into the region, a word at a time once the region side is aligned.
///
/// # Safety
/// `dst` must be valid for writes of `src.len()` bytes and must not overlap `src`.
unsafe fn copy_to_region(src: &[u8], dst: *mut u8) {
    let len = src.len();
    let s = src.as_ptr();
    let head = dst.align_offset(WORD).min(len);
    let mut i = 0;
    while i < head {
        // SAFETY: `i < len`; both pointers are valid for `len` bytes (caller contract).
        unsafe { dst.add(i).write_volatile(s.add(i).read()) };
        i += 1;
    }
    while i + WORD <= len {
        // SAFETY: `dst + i` is word aligned and `[i, i + WORD)` is in bounds on both sides;
        // the source read is unaligned-tolerant.
        unsafe {
            let w = s.add(i).cast::<u64>().read_unaligned();
            dst.add(i).cast::<u64>().write_volatile(w);
        }
        i += WORD;
    }
    while i < len {
        // SAFETY: `i < len`.
        unsafe { dst.add(i).write_volatile(s.add(i).read()) };
        i += 1;
    }
}

impl Drop for RegionInner {
    fn drop(&mut self) {
        if let Backing::Heap(layout) = self.backing {
            // SAFETY: `ptr` was allocated in `SharedRegion::heap` with exactly `layout`.
            unsafe { alloc::dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}

impl fmt::Debug for SharedRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedRegion")
            .field("ptr", &self.inner.ptr)
            .field("len", &self.inner.len)
            .field(
                "backing",
                &match self.inner.backing {
                    Backing::Heap(_) => "heap",
                    Backing::Mapped(_) => "mapped",
                },
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn heap_is_zeroed_and_aligned() {
        let r = SharedRegion::heap(1000);
        assert_eq!(r.len(), 1000);
        assert_eq!(r.base_ptr().as_ptr() as usize % HEAP_ALIGN, 0);
        let mut buf = vec![1u8; 1000];
        r.read(0, &mut buf);
        assert!(buf.iter().all(|&b| b == 0));
        assert!(SharedRegion::heap(0).is_empty());
    }

    #[test]
    fn unaligned_copies_round_trip() {
        let r = SharedRegion::heap(256);
        for offset in 0..9 {
            for len in [0, 1, 7, 8, 9, 17, 64] {
                let src: Vec<u8> = (0..len as u8)
                    .map(|b| b.wrapping_mul(31) ^ offset as u8)
                    .collect();
                r.write(offset, &src);
                let mut dst = vec![0u8; len];
                r.read(offset, &mut dst);
                assert_eq!(dst, src, "offset {offset} len {len}");
            }
        }
    }

    #[test]
    fn atomics_are_shared_between_clones() {
        let r = SharedRegion::heap(64);
        let c = r.clone();
        r.atomic_u32(4).store(9, Ordering::Release);
        assert_eq!(c.atomic_u32(4).load(Ordering::Acquire), 9);
        let t = std::thread::spawn(move || c.atomic_u64(8).fetch_add(1, Ordering::AcqRel));
        t.join().unwrap();
        assert_eq!(r.atomic_u64(8).load(Ordering::Acquire), 1);
        r.bind_numa(0, 64, 0).unwrap();
    }

    #[test]
    #[should_panic(expected = "misaligned")]
    fn misaligned_atomic_panics() {
        SharedRegion::heap(64).atomic_u64(4);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn out_of_bounds_panics() {
        SharedRegion::heap(64).read(60, &mut [0; 8]);
    }
}
