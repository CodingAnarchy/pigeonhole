use std::alloc::{self, Layout};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

/// An owned, page-aligned (4096-byte) buffer for file I/O.
///
/// Aligned so the same buffer works for O_DIRECT and io_uring registered buffers in Phase 3.
/// Ownership moves into a submitted operation and comes back through its
/// [`Completion`](crate::Completion), so the kernel never sees a buffer Rust code can touch.
///
/// ```
/// use pigeonhole_io::IoBuf;
///
/// let mut buf = IoBuf::zeroed(100);
/// assert_eq!(buf.len(), 100);
/// assert_eq!(buf.capacity(), IoBuf::ALIGN);
/// assert_eq!(buf.as_ptr() as usize % IoBuf::ALIGN, 0);
/// buf[..5].copy_from_slice(b"hello");
/// buf.resize(3);
/// assert_eq!(&buf[..], b"hel");
/// ```
pub struct IoBuf {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

// SAFETY: `IoBuf` uniquely owns its allocation, exactly like `Vec<u8>`.
unsafe impl Send for IoBuf {}
// SAFETY: shared references only hand out `&[u8]`; mutation needs `&mut IoBuf`.
unsafe impl Sync for IoBuf {}

impl IoBuf {
    /// Alignment of every buffer.
    pub const ALIGN: usize = pigeonhole_format::PAGE_SIZE;

    /// A zeroed buffer of `len` bytes (capacity rounded up to [`IoBuf::ALIGN`]).
    pub fn zeroed(len: usize) -> Self {
        let cap = round_up(len);
        Self {
            ptr: allocate(cap),
            len,
            cap,
        }
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Capacity in bytes (a multiple of [`IoBuf::ALIGN`]).
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Changes the length within capacity; new bytes are zero. Growing past the capacity
    /// reallocates (keeping the alignment and the existing bytes).
    pub fn resize(&mut self, len: usize) {
        if len > self.cap {
            let cap = round_up(len);
            self.ptr = if self.cap == 0 {
                allocate(cap)
            } else {
                // SAFETY: `ptr` was allocated with `layout(self.cap)`, and `cap` is non-zero
                // and fits a valid layout (checked by `layout(cap)`).
                let p = unsafe { alloc::realloc(self.ptr.as_ptr(), layout(self.cap), cap) };
                NonNull::new(p).unwrap_or_else(|| alloc::handle_alloc_error(layout(cap)))
            };
            self.cap = cap;
        }
        if len > self.len {
            // SAFETY: `[self.len, len)` lies within the allocation of `self.cap >= len` bytes.
            unsafe {
                self.ptr
                    .as_ptr()
                    .add(self.len)
                    .write_bytes(0, len - self.len)
            };
        }
        self.len = len;
    }
}

fn round_up(len: usize) -> usize {
    len.checked_next_multiple_of(IoBuf::ALIGN)
        .expect("IoBuf length overflows")
}

fn layout(cap: usize) -> Layout {
    Layout::from_size_align(cap, IoBuf::ALIGN).expect("IoBuf length overflows")
}

fn allocate(cap: usize) -> NonNull<u8> {
    if cap == 0 {
        // Dangling but `ALIGN`-aligned, so even an empty buffer honors the alignment promise.
        return NonNull::new(std::ptr::without_provenance_mut(IoBuf::ALIGN)).expect("non-zero");
    }
    let layout = layout(cap);
    // SAFETY: `layout` has a non-zero size.
    let p = unsafe { alloc::alloc_zeroed(layout) };
    NonNull::new(p).unwrap_or_else(|| alloc::handle_alloc_error(layout))
}

impl Drop for IoBuf {
    fn drop(&mut self) {
        if self.cap != 0 {
            // SAFETY: `ptr` was allocated with exactly `layout(self.cap)`.
            unsafe { alloc::dealloc(self.ptr.as_ptr(), layout(self.cap)) };
        }
    }
}

impl Deref for IoBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the first `len` bytes are initialized (zeroed or written) and owned by us.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for IoBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`, and `&mut self` guarantees exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl fmt::Debug for IoBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoBuf")
            .field("len", &self.len)
            .field("capacity", &self.cap)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_length_buffer() {
        let mut b = IoBuf::zeroed(0);
        assert!(b.is_empty());
        assert_eq!(b.capacity(), 0);
        assert_eq!(b.as_ptr() as usize % IoBuf::ALIGN, 0);
        assert_eq!(&b[..], b"");
        b.resize(10);
        assert_eq!(&b[..], &[0; 10]);
        assert_eq!(b.capacity(), IoBuf::ALIGN);
    }

    #[test]
    fn resize_zeroes_new_bytes() {
        let mut b = IoBuf::zeroed(8);
        b.fill(0xAA);
        b.resize(4);
        b.resize(8);
        assert_eq!(&b[..], &[0xAA, 0xAA, 0xAA, 0xAA, 0, 0, 0, 0]);
    }

    #[test]
    fn grow_keeps_contents_and_alignment() {
        let mut b = IoBuf::zeroed(IoBuf::ALIGN);
        b.fill(7);
        b.resize(3 * IoBuf::ALIGN + 1);
        assert_eq!(b.capacity(), 4 * IoBuf::ALIGN);
        assert_eq!(b.as_ptr() as usize % IoBuf::ALIGN, 0);
        assert!(b[..IoBuf::ALIGN].iter().all(|&x| x == 7));
        assert!(b[IoBuf::ALIGN..].iter().all(|&x| x == 0));
    }
}
