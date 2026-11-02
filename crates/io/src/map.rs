//! Writable file mappings (D312, [`File::map_write`](crate::File::map_write)).

#[cfg(any(target_os = "linux", test))]
use std::ptr::NonNull;

#[cfg(any(target_os = "linux", test))]
use crate::{Error, ErrorKind, Result};

/// Copies `bytes` to `at` within the `len` bytes at `base`, after checking they fit. The one
/// place a mapped store's `unsafe` lives: the real mapping calls it, and Miri runs it over heap
/// memory (`tests`).
///
/// # Safety
/// `base..base + len` must be valid for writes for the call, and nothing else may read or write
/// it during the call.
#[cfg(any(target_os = "linux", test))]
pub(crate) unsafe fn store(base: NonNull<u8>, len: usize, at: usize, bytes: &[u8]) -> Result<()> {
    if at.checked_add(bytes.len()).is_none_or(|end| end > len) {
        return Err(Error::new(ErrorKind::Other, "a store beyond the mapping"));
    }
    // SAFETY: `[at, at + bytes.len())` is inside `base..base + len` (checked above), which the
    // caller guarantees is writable and unaliased; `bytes` is a separate borrow, so the two
    // can't overlap.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), base.as_ptr().add(at), bytes.len());
    }
    Ok(())
}

/// A real file's shared mapping ([`crate::os::map_range`]).
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct FileWriteMap {
    mapping: crate::os::Mapping,
}

#[cfg(target_os = "linux")]
impl FileWriteMap {
    pub(crate) fn new(mapping: crate::os::Mapping) -> Self {
        Self { mapping }
    }
}

// SAFETY: the mapping is plain shared memory owned by this value. The only method that writes
// it takes `&mut self`, so no two threads store through one `FileWriteMap` at once, and `&self`
// methods only read its length.
#[cfg(target_os = "linux")]
unsafe impl Send for FileWriteMap {}
// SAFETY: as for `Send`: `&self` exposes only the length.
#[cfg(target_os = "linux")]
unsafe impl Sync for FileWriteMap {}

#[cfg(target_os = "linux")]
impl crate::WriteMap for FileWriteMap {
    fn len(&self) -> usize {
        self.mapping.len()
    }

    fn write(&mut self, at: usize, bytes: &[u8]) -> Result<()> {
        // SAFETY: the mapping is live while `self` is, and `&mut self` excludes every other
        // access through it in this process. Other processes see the same page-cache pages,
        // as with `write_at`; they read through the file, never through Rust references to
        // this memory.
        unsafe { store(self.mapping.ptr(), self.mapping.len(), at, bytes) }
    }
}

#[cfg(test)]
mod tests {
    use super::store;
    use std::ptr::NonNull;

    /// The store Miri checks: over a heap buffer, in bounds and at its edges, and refused past
    /// the end without touching memory.
    #[test]
    fn a_store_copies_in_bounds_and_refuses_past_the_end() {
        let mut buf = vec![0u8; 64];
        let base = NonNull::new(buf.as_mut_ptr()).unwrap();
        // SAFETY: `buf` is 64 writable bytes, borrowed by nothing else during the calls.
        unsafe {
            store(base, 64, 0, b"head").unwrap();
            store(base, 64, 60, b"tail").unwrap();
            store(base, 64, 64, b"").unwrap();
            assert!(store(base, 64, 61, b"tail").is_err());
            assert!(store(base, 64, usize::MAX, b"x").is_err());
        }
        assert_eq!(&buf[..4], b"head");
        assert_eq!(&buf[60..], b"tail");
        assert!(buf[4..60].iter().all(|&b| b == 0));
    }
}
