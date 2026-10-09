//! The byte-level arena backend.
//!
//! Every access to arena memory goes through [`Mem`], which has two implementations with the
//! same crate-private API:
//!
//! - the real one wraps a [`SharedRegion`](pigeonhole_io::SharedRegion) and a base pointer and
//!   uses `std` atomics for the published links and counters, plain reads for bytes that are
//!   immutable once published (node headers, keys, values) and plain writes for bytes that
//!   are not yet published (the writer fills a node, then links it with a release store);
//! - the `cfg(loom)` one stores the arena as a vector of `loom` atomic words, so the model
//!   checker sees every byte the skiplist touches. Non-atomic accesses become relaxed word
//!   accesses there: loom then returns a stale value for any read that the acquire/release
//!   discipline does not order, which is exactly the bug the loom tests must catch.
//!
//! Integers are native-endian in memory. Every process that maps a region runs on the same
//! host, and only little-endian targets are supported (the crate refuses to build on others,
//! decision D56), so this matches `FORMAT.md` §11.6.

use std::cmp::Ordering as Cmp;
use std::fmt;

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{Ordering, fence};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{Ordering, fence};

/// An acquire fence: pairs a relaxed load that observed a release store (such as an `Arc`
/// count decrement) with the accesses that preceded that store.
pub(crate) fn acquire_fence() {
    fence(Ordering::Acquire);
}

#[cfg(not(loom))]
mod imp {
    use std::ptr::{self, NonNull};
    use std::slice;
    use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

    use pigeonhole_io::SharedRegion;

    /// One arena's bytes: a base pointer into a mapping kept alive by `region`.
    #[derive(Clone)]
    pub(crate) struct Mem {
        /// Keeps the mapping alive (`base` points into it); read only through `base`.
        _region: SharedRegion,
        base: NonNull<u8>,
        len: usize,
    }

    // SAFETY: `base` points into `region`, whose memory stays valid for as long as any clone
    // lives and may be used from any thread. Shared accesses are either atomic (links,
    // counters, flags) or to bytes that are immutable once published (nodes), with the
    // publication ordered by release/acquire on the link; the writer alone touches
    // unpublished bytes. Those invariants are upheld by `lib.rs`, the only user.
    unsafe impl Send for Mem {}
    // SAFETY: as above.
    unsafe impl Sync for Mem {}

    impl Mem {
        /// `[offset, offset + len)` of `region`; the caller has checked the bounds.
        pub(crate) fn new(region: SharedRegion, offset: usize, len: usize) -> Self {
            assert!(
                offset
                    .checked_add(len)
                    .is_some_and(|end| end <= region.len()),
                "arena outside its region"
            );
            // SAFETY: `offset <= region.len()`, so the result is within (or one past) the
            // mapping, which stays valid while `region` lives.
            let base = unsafe { region.base_ptr().add(offset) };
            Self {
                _region: region,
                base,
                len,
            }
        }

        pub(crate) fn heap(len: usize) -> Self {
            Self::new(SharedRegion::heap(len), 0, len)
        }

        pub(crate) fn len(&self) -> usize {
            self.len
        }

        pub(crate) fn base_addr(&self) -> usize {
            self.base.as_ptr() as usize
        }

        /// Pointer to `[off, off + n)`, which must be inside the arena.
        #[inline]
        fn ptr(&self, off: usize, n: usize) -> *mut u8 {
            assert!(
                off.checked_add(n).is_some_and(|end| end <= self.len),
                "arena access out of bounds: {off}+{n} > {}",
                self.len
            );
            // SAFETY: `off <= len`, so the result stays within (or one past) the arena.
            unsafe { self.base.as_ptr().add(off) }
        }

        #[inline]
        fn atomic_u32(&self, off: usize) -> &AtomicU32 {
            let p = self.ptr(off, 4).cast::<u32>();
            debug_assert!(p.is_aligned(), "misaligned u32 at {off}");
            // SAFETY: in bounds (`ptr` checked) and 4-byte aligned: the arena base is
            // 64-byte aligned (heap regions are allocated 64-aligned, mappings are
            // page-aligned, and `ArenaRegion::new` requires a 64-aligned offset), node
            // offsets are multiples of 4 (nodes start 4-aligned and sizes round up to 4),
            // header roots are multiples of 64 (chunk starts and the 64-byte reserved
            // prefix), and the fields read here sit at 4-aligned offsets inside them;
            // readers reject any other offset before reaching here. Lives as long as
            // `self`; only ever accessed atomically while shared.
            unsafe { AtomicU32::from_ptr(p) }
        }

        #[inline]
        fn atomic_u64(&self, off: usize) -> &AtomicU64 {
            let p = self.ptr(off, 8).cast::<u64>();
            debug_assert!(p.is_aligned(), "misaligned u64 at {off}");
            // SAFETY: as in `atomic_u32`; the only `u64` fields are in the header, whose
            // root is a multiple of 64 and whose `u64` fields sit at 8-aligned offsets
            // (`H_BYTES` 16, `H_MAX_SEQNO` 24, `H_MIN_SEQNO` 32); `MemtableReader::open`
            // rejects a root that is not a multiple of 8.
            unsafe { AtomicU64::from_ptr(p) }
        }

        #[inline]
        fn atomic_u8(&self, off: usize) -> &AtomicU8 {
            let p = self.ptr(off, 1);
            // SAFETY: in bounds; bytes need no alignment; lives as long as `self`.
            unsafe { AtomicU8::from_ptr(p) }
        }

        #[inline]
        pub(crate) fn load_u32(&self, off: usize, ord: Ordering) -> u32 {
            self.atomic_u32(off).load(ord)
        }

        #[inline]
        pub(crate) fn store_u32(&self, off: usize, v: u32, ord: Ordering) {
            self.atomic_u32(off).store(v, ord);
        }

        #[cfg(test)]
        pub(crate) fn load_u64(&self, off: usize, ord: Ordering) -> u64 {
            self.atomic_u64(off).load(ord)
        }

        pub(crate) fn store_u64(&self, off: usize, v: u64, ord: Ordering) {
            self.atomic_u64(off).store(v, ord);
        }

        #[cfg(test)]
        pub(crate) fn load_u8(&self, off: usize, ord: Ordering) -> u8 {
            self.atomic_u8(off).load(ord)
        }

        pub(crate) fn fetch_or_u8(&self, off: usize, bits: u8, ord: Ordering) -> u8 {
            self.atomic_u8(off).fetch_or(bits, ord)
        }

        /// Plain read of a `u32` that is immutable once published.
        #[inline]
        pub(crate) fn read_u32(&self, off: usize) -> u32 {
            let p = self.ptr(off, 4).cast::<u32>();
            debug_assert!(p.is_aligned(), "misaligned u32 at {off}");
            // SAFETY: in bounds and aligned; the bytes were published with a release store
            // that the caller acquired, and are never written again while readable.
            unsafe { p.read() }
        }

        /// Plain read of a byte that is immutable once published.
        #[inline]
        pub(crate) fn read_u8(&self, off: usize) -> u8 {
            let p = self.ptr(off, 1);
            // SAFETY: as in `read_u32`.
            unsafe { p.read() }
        }

        /// Plain write of a `u32` into bytes no reader can reach yet.
        #[inline]
        pub(crate) fn write_u32(&self, off: usize, v: u32) {
            let p = self.ptr(off, 4).cast::<u32>();
            debug_assert!(p.is_aligned(), "misaligned u32 at {off}");
            // SAFETY: in bounds and aligned; the writer owns these bytes until it publishes
            // them with a release store, so nothing else accesses them now.
            unsafe { p.write(v) }
        }

        /// Plain copy into bytes no reader can reach yet.
        #[inline]
        pub(crate) fn write(&self, off: usize, src: &[u8]) {
            let p = self.ptr(off, src.len());
            // SAFETY: `p` is valid for `src.len()` bytes and exclusively the writer's until
            // published (see `write_u32`); `src` is a Rust borrow, so it cannot overlap.
            unsafe { ptr::copy_nonoverlapping(src.as_ptr(), p, src.len()) }
        }

        /// Published, immutable bytes as a slice tied to `self`.
        #[inline]
        pub(crate) fn slice(&self, off: usize, len: usize) -> &[u8] {
            let p = self.ptr(off, len);
            // SAFETY: in bounds and valid while `self` lives; the bytes were published with
            // a release store the caller acquired and are immutable until the memtable is
            // reclaimed, which the holder of the view pin prevents.
            unsafe { slice::from_raw_parts(p, len) }
        }

        pub(crate) fn cmp(&self, off: usize, len: usize, other: &[u8]) -> super::Cmp {
            pigeonhole_format::key::compare(self.slice(off, len), other)
        }

        #[cfg(test)]
        pub(crate) fn copy(&self, off: usize, len: usize) -> Vec<u8> {
            self.slice(off, len).to_vec()
        }
    }
}

#[cfg(loom)]
mod imp {
    use std::sync::Arc;

    use loom::sync::atomic::{AtomicU32, Ordering};

    /// The arena as loom atomic words, so the model checker tracks every byte.
    #[derive(Clone)]
    pub(crate) struct Mem {
        words: Arc<Vec<AtomicU32>>,
        len: usize,
    }

    impl Mem {
        pub(crate) fn heap(len: usize) -> Self {
            let words = (0..len.div_ceil(4)).map(|_| AtomicU32::new(0)).collect();
            Self {
                words: Arc::new(words),
                len,
            }
        }

        pub(crate) fn len(&self) -> usize {
            self.len
        }

        pub(crate) fn base_addr(&self) -> usize {
            Arc::as_ptr(&self.words) as usize
        }

        fn word(&self, off: usize) -> &AtomicU32 {
            assert!(off.is_multiple_of(4), "misaligned u32 at {off}");
            assert!(off + 4 <= self.len, "arena access out of bounds");
            &self.words[off / 4]
        }

        pub(crate) fn load_u32(&self, off: usize, ord: Ordering) -> u32 {
            self.word(off).load(ord)
        }

        pub(crate) fn store_u32(&self, off: usize, v: u32, ord: Ordering) {
            self.word(off).store(v, ord);
        }

        pub(crate) fn store_u64(&self, off: usize, v: u64, ord: Ordering) {
            self.word(off).store(v as u32, ord);
            self.word(off + 4).store((v >> 32) as u32, ord);
        }

        pub(crate) fn load_u8(&self, off: usize, ord: Ordering) -> u8 {
            let w = self.word(off & !3).load(ord);
            (w >> (8 * (off % 4))) as u8
        }

        pub(crate) fn fetch_or_u8(&self, off: usize, bits: u8, ord: Ordering) -> u8 {
            let w = self.word(off & !3);
            let shift = 8 * (off % 4);
            let old = w.load(Ordering::Relaxed);
            w.store(old | (u32::from(bits) << shift), ord);
            (old >> shift) as u8
        }

        pub(crate) fn read_u32(&self, off: usize) -> u32 {
            self.load_u32(off, Ordering::Relaxed)
        }

        pub(crate) fn read_u8(&self, off: usize) -> u8 {
            self.load_u8(off, Ordering::Relaxed)
        }

        pub(crate) fn write_u32(&self, off: usize, v: u32) {
            self.store_u32(off, v, Ordering::Relaxed);
        }

        /// Byte copy as relaxed read-modify-writes of whole words (the single writer owns
        /// the bytes, so no one else stores to them meanwhile).
        pub(crate) fn write(&self, off: usize, src: &[u8]) {
            assert!(off + src.len() <= self.len, "arena access out of bounds");
            let mut i = 0;
            while i < src.len() {
                let at = off + i;
                let word = self.word(at & !3);
                let first = at % 4;
                let n = (4 - first).min(src.len() - i);
                let mut bytes = word.load(Ordering::Relaxed).to_le_bytes();
                bytes[first..first + n].copy_from_slice(&src[i..i + n]);
                word.store(u32::from_le_bytes(bytes), Ordering::Relaxed);
                i += n;
            }
        }

        pub(crate) fn copy(&self, off: usize, len: usize) -> Vec<u8> {
            assert!(off + len <= self.len, "arena access out of bounds");
            let mut out = Vec::with_capacity(len);
            let mut i = 0;
            while i < len {
                let at = off + i;
                let first = at % 4;
                let n = (4 - first).min(len - i);
                let bytes = self.word(at & !3).load(Ordering::Relaxed).to_le_bytes();
                out.extend_from_slice(&bytes[first..first + n]);
                i += n;
            }
            out
        }

        pub(crate) fn cmp(&self, off: usize, len: usize, other: &[u8]) -> super::Cmp {
            self.copy(off, len).as_slice().cmp(other)
        }
    }
}

pub(crate) use imp::Mem;

impl fmt::Debug for Mem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mem")
            .field("base", &format_args!("{:#x}", self.base_addr()))
            .field("len", &self.len())
            .finish()
    }
}
