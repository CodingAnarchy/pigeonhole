use std::ops::{Deref, DerefMut};

/// An owned, page-aligned (4096-byte) buffer for file I/O.
///
/// Aligned so the same buffer works for O_DIRECT and io_uring registered buffers in Phase 3.
/// Ownership moves into a submitted operation and comes back through its
/// [`Completion`](crate::Completion), so the kernel never sees a buffer Rust code can touch.
#[derive(Debug)]
pub struct IoBuf {
    _priv: (),
}

impl IoBuf {
    /// Alignment of every buffer.
    pub const ALIGN: usize = pigeonhole_format::PAGE_SIZE;

    /// A zeroed buffer of `len` bytes (capacity rounded up to [`IoBuf::ALIGN`]).
    pub fn zeroed(len: usize) -> Self {
        todo!()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        todo!()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// Capacity in bytes (a multiple of [`IoBuf::ALIGN`]).
    pub fn capacity(&self) -> usize {
        todo!()
    }

    /// Changes the length within capacity; new bytes are zero.
    pub fn resize(&mut self, len: usize) {
        todo!()
    }
}

impl Deref for IoBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        todo!()
    }
}

impl DerefMut for IoBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        todo!()
    }
}
