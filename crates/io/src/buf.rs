use std::alloc::{self, Layout};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, PoisonError};

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
    /// The start of the data: the allocation's start (aligned) unless [`IoBuf::keep`]
    /// dropped a prefix.
    ptr: NonNull<u8>,
    /// How far the data starts into the allocation (0 but after [`IoBuf::keep`]); only drop
    /// and growth use it, so reading the bytes costs nothing more.
    head: usize,
    len: usize,
    cap: usize,
    /// Where the memory comes from: a slot of this registered pool (`Some`, given back on
    /// drop; which slot follows from `ptr`) or the heap. One word, so heap buffers, the
    /// common case, stay as small as before.
    pool: Option<Arc<SlotPool>>,
    /// The [`BufPool`] a heap buffer goes back to when dropped (ICR 0026), if any.
    recycle: Option<Arc<BufPool>>,
}

/// A bounded pool of heap [`IoBuf`]s (ICR 0026): a buffer taken from it goes back when it is
/// dropped, wherever that happens, unless the pool is full. A reader that decodes block after
/// block reuses a few allocations instead of allocating and zero-filling one per block
/// (#372).
///
/// ```
/// use pigeonhole_io::BufPool;
///
/// let pool = BufPool::new(2);
/// let mut a = pool.take(100);
/// a.fill(7);
/// let p = a.as_ptr();
/// drop(a);
/// // The same allocation, its bytes as the last user left them, for the caller to overwrite.
/// let b = pool.take(50);
/// assert_eq!(b.as_ptr(), p);
/// assert!(b.iter().all(|&x| x == 7));
/// // Extended past its old length, only the new bytes are zeroed.
/// drop(b);
/// let c = pool.take(120);
/// assert!(c[..50].iter().all(|&x| x == 7) && c[50..].iter().all(|&x| x == 0));
/// ```
pub struct BufPool {
    max: usize,
    /// The largest capacity it keeps: a bigger buffer is freed when dropped.
    max_capacity: usize,
    free: Mutex<Vec<IoBuf>>,
}

impl BufPool {
    /// A pool keeping at most `max` buffers.
    pub fn new(max: usize) -> Arc<BufPool> {
        Self::with_max_capacity(max, usize::MAX)
    }

    /// A pool keeping at most `max` buffers of at most `max_capacity` bytes each.
    fn with_max_capacity(max: usize, max_capacity: usize) -> Arc<BufPool> {
        Arc::new(Self {
            max,
            max_capacity,
            free: Mutex::new(Vec::new()),
        })
    }

    /// A buffer of `len` bytes that goes back to this pool when dropped: a recycled one
    /// truncated or extended to `len` (only the extension is zero-filled), or a new zeroed
    /// one when the pool has none. Its bytes are initialized (zeros, or what a previous user
    /// wrote), for the caller to overwrite.
    pub fn take(self: &Arc<Self>, len: usize) -> IoBuf {
        let recycled = self
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let mut buf = match recycled {
            Some(mut b) => {
                b.resize(len);
                b
            }
            None => IoBuf::zeroed(len),
        };
        buf.recycle = Some(Arc::clone(self));
        buf
    }

    /// As [`BufPool::take`], but a recycled buffer more than twice the capacity `len` needs
    /// is freed instead, so a block the cache keeps is never charged for much more than its
    /// bytes ([`IoBuf::reused`]).
    fn take_fitting(self: &Arc<Self>, len: usize) -> IoBuf {
        let recycled = self
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .filter(|b| b.capacity() <= 2 * round_up(len).max(IoBuf::ALIGN));
        let mut buf = match recycled {
            Some(mut b) => {
                b.resize(len);
                b
            }
            None => IoBuf::zeroed(len),
        };
        buf.recycle = Some(Arc::clone(self));
        buf
    }

    /// Takes back `buf` (its handle already cleared, so the pool never holds an `Arc` of
    /// itself), or lets it go if the pool is full.
    fn put(&self, buf: IoBuf) {
        if buf.capacity() > self.max_capacity {
            return;
        }
        let mut free = self.free.lock().unwrap_or_else(PoisonError::into_inner);
        if free.len() < self.max {
            free.push(buf);
        }
    }

    /// Buffers held now.
    #[cfg(test)]
    fn held(&self) -> usize {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// Free buffers each thread's pool keeps ([`IoBuf::reused`]): a read path holds a buffer or
/// two at a time, and a block kept by the cache comes back when it is evicted.
const THREAD_POOL_MAX: usize = 4;

/// The largest buffer a thread's pool keeps: a block read (16 KiB by default) and then some,
/// not a large blob record's or a merged readahead run's buffer, which would otherwise sit
/// idle in every reading thread's pool.
const THREAD_POOL_MAX_CAPACITY: usize = 64 << 10;

thread_local! {
    /// This thread's [`IoBuf::reused`] buffers.
    static THREAD_POOL: Arc<BufPool> =
        BufPool::with_max_capacity(THREAD_POOL_MAX, THREAD_POOL_MAX_CAPACITY);
}

impl fmt::Debug for BufPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufPool").field("max", &self.max).finish()
    }
}

/// One aligned region of equal slots, which a backend registers with the kernel once
/// (io_uring's fixed buffers, #402): a read or write into a slot skips pinning its pages.
/// A slot-backed [`IoBuf`] gives its slot back when dropped. Such buffers are meant to be
/// short-lived (the buffer of one I/O); one kept longer ([`IoBuf::detached`]) moves to the
/// heap so the pool does not run dry.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct SlotPool {
    base: NonNull<u8>,
    slot_len: usize,
    slots: u32,
    free: Mutex<Vec<u32>>,
}

// SAFETY: the region is owned by the pool and only ever handed out one slot per buffer; the
// free list is behind a mutex.
unsafe impl Send for SlotPool {}
// SAFETY: as above.
unsafe impl Sync for SlotPool {}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl SlotPool {
    /// `slots` slots of `slot_len` bytes (a multiple of [`IoBuf::ALIGN`]), zeroed.
    pub(crate) fn new(slots: u32, slot_len: usize) -> Arc<Self> {
        assert!(slot_len > 0 && slot_len.is_multiple_of(IoBuf::ALIGN) && slots > 0);
        let total = slot_len * slots as usize;
        Arc::new(Self {
            base: allocate(total),
            slot_len,
            slots,
            free: Mutex::new((0..slots).rev().collect()),
        })
    }

    /// The start of slot `index` and the length of every slot (to register them).
    pub(crate) fn slot_ptr(&self, index: u32) -> *mut u8 {
        assert!(index < self.slots);
        // SAFETY: `index < slots`, so the offset is within the region.
        unsafe { self.base.as_ptr().add(index as usize * self.slot_len) }
    }

    pub(crate) fn slot_len(&self) -> usize {
        self.slot_len
    }

    pub(crate) fn slots(&self) -> u32 {
        self.slots
    }

    /// A buffer of `len` bytes in a free slot, or `None` when `len` exceeds a slot or none is
    /// free. Its bytes are whatever the slot last held (initialized: the region starts
    /// zeroed), for a read to overwrite.
    pub(crate) fn take(self: &Arc<Self>, len: usize) -> Option<IoBuf> {
        if len > self.slot_len {
            return None;
        }
        let index = self
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()?;
        Some(IoBuf {
            ptr: NonNull::new(self.slot_ptr(index)).expect("inside the region"),
            len,
            cap: self.slot_len,
            head: 0,
            pool: Some(Arc::clone(self)),
            recycle: None,
        })
    }

    /// The slot holding `ptr` (a pointer into the region).
    fn index_of(&self, ptr: NonNull<u8>) -> u32 {
        let offset = ptr.as_ptr() as usize - self.base.as_ptr() as usize;
        (offset / self.slot_len) as u32
    }

    fn give_back(&self, index: u32) {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(index);
    }

    /// Slots free now.
    #[cfg(test)]
    fn free_slots(&self) -> usize {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl Drop for SlotPool {
    fn drop(&mut self) {
        // SAFETY: the region was allocated with exactly this layout, and no buffer holds a
        // slot any more (each holds an `Arc` of the pool).
        unsafe {
            alloc::dealloc(
                self.base.as_ptr(),
                layout(self.slot_len * self.slots as usize),
            )
        };
    }
}

// SAFETY: `IoBuf` uniquely owns its allocation, exactly like `Vec<u8>`.
unsafe impl Send for IoBuf {}
// SAFETY: shared references only hand out `&[u8]`; mutation needs `&mut IoBuf`.
unsafe impl Sync for IoBuf {}

impl IoBuf {
    /// Alignment of every buffer.
    pub const ALIGN: usize = pigeonhole_format::PAGE_SIZE;

    /// A buffer of `len` initialized bytes for the caller to overwrite (#709): one this
    /// thread used before, from its small [`BufPool`], with whatever bytes a previous read
    /// left (zeros past its old length), or a new zeroed one; one more than twice the size
    /// needed is freed rather than reused. It goes back to that pool when
    /// dropped, on any thread. For buffers a read or a decoder fills completely before
    /// anyone reads them; skips the allocation and zero-fill of [`IoBuf::zeroed`].
    ///
    /// ```
    /// use pigeonhole_io::IoBuf;
    ///
    /// let mut a = IoBuf::reused(4096);
    /// a.fill(7);
    /// let p = a.as_ptr();
    /// drop(a);
    /// let b = IoBuf::reused(100);
    /// assert_eq!(b.as_ptr(), p);
    /// assert_eq!(b.len(), 100);
    /// ```
    pub fn reused(len: usize) -> Self {
        THREAD_POOL.with(|pool| pool.take_fitting(len))
    }

    /// A zeroed buffer of `len` bytes (capacity rounded up to [`IoBuf::ALIGN`]).
    pub fn zeroed(len: usize) -> Self {
        let cap = round_up(len);
        Self {
            ptr: allocate(cap),
            head: 0,
            len,
            cap,
            pool: None,
            recycle: None,
        }
    }

    /// Keeps only the bytes of `range`, in place: no copy, so a block read with its aligned
    /// surroundings (direct I/O, #403) stays in the buffer it was read into. The data then
    /// starts `range.start` bytes into the allocation, so it is aligned only if
    /// `range.start` is; the capacity still counts the whole allocation.
    ///
    /// ```
    /// use pigeonhole_io::IoBuf;
    ///
    /// let mut buf = IoBuf::zeroed(8192);
    /// buf[100..200].fill(7);
    /// buf.keep(100..200);
    /// assert_eq!(buf.len(), 100);
    /// assert!(buf.iter().all(|&b| b == 7));
    /// assert_eq!(buf.capacity(), 8192);
    /// ```
    ///
    /// # Panics
    /// If `range` is not within the buffer.
    pub fn keep(&mut self, range: std::ops::Range<usize>) {
        assert!(
            range.start <= range.end && range.end <= self.len,
            "IoBuf::keep out of range"
        );
        // SAFETY: `range.start <= len`, so the new start is within the allocation.
        self.ptr = unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(range.start)) };
        self.head += range.start;
        self.len = range.end - range.start;
    }

    /// This buffer, kept beyond the I/O it was read into (a block cached as read, say): a
    /// buffer in a registered slot is copied to the heap and its slot given back, so the
    /// pool does not run dry; a heap buffer is returned as is.
    pub fn detached(self) -> IoBuf {
        // A reused buffer more than its length needs (#709) is copied too, so what keeps it
        // (the block cache charges a buffer's whole capacity) is charged as for a new one.
        if self.pool.is_none() && (self.recycle.is_none() || self.cap <= round_up(self.len)) {
            return self;
        }
        let mut heap = IoBuf::zeroed(self.len);
        heap.copy_from_slice(&self);
        heap
    }

    /// Whether the buffer lives in a slot a backend registered with the kernel (a test
    /// hook).
    #[doc(hidden)]
    pub fn is_registered(&self) -> bool {
        self.pool.is_some()
    }

    /// The registered pool and slot of this buffer, if any.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn slot_of(&self) -> Option<(&Arc<SlotPool>, u32)> {
        self.pool
            .as_ref()
            .map(|pool| (pool, pool.index_of(self.ptr)))
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
        if len > self.cap - self.head && (self.head > 0 || self.pool.is_some()) {
            // Past the end of a kept range's allocation, or of a registered slot (which
            // cannot grow): move to a fresh heap buffer (a slot goes back when the old buffer
            // is dropped).
            let mut fresh = IoBuf::zeroed(len);
            fresh[..self.len].copy_from_slice(self);
            *self = fresh;
            return;
        }
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
            // SAFETY: `[self.len, len)` past the data's start lies within the allocation:
            // `head + len` is at most `cap` (checked or grown above).
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
        // A pooled heap buffer goes back whole: swapped for an empty buffer (which owns no
        // allocation, so the rest of this drop frees nothing), its handle cleared. A buffer
        // with a kept prefix is freed instead: a pooled buffer must start at its allocation.
        if let Some(recycle) = self.recycle.take()
            && self.head == 0
        {
            recycle.put(std::mem::replace(self, IoBuf::zeroed(0)));
            return;
        }
        if let Some(pool) = self.pool.take() {
            pool.give_back(pool.index_of(self.ptr));
            return;
        }
        if self.cap != 0 {
            // SAFETY: the allocation starts `head` bytes before `ptr` and was allocated with
            // exactly `layout(self.cap)`.
            unsafe { alloc::dealloc(self.ptr.as_ptr().sub(self.head), layout(self.cap)) };
        }
    }
}

impl Deref for IoBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the `len` bytes from `ptr` are initialized (zeroed or written), within the
        // allocation, and owned by us.
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
            .field("registered", &self.pool.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_pool_with_a_capacity_cap_frees_larger_buffers() {
        let pool = BufPool::with_max_capacity(4, 64 << 10);
        drop(pool.take_fitting(128 << 10));
        assert_eq!(pool.held(), 0, "a buffer over the cap is freed, not kept");
        drop(pool.take_fitting(64 << 10));
        assert_eq!(pool.held(), 1);
        // One that grew past the cap while reused is freed too.
        drop(pool.take_fitting(100 << 10));
        assert_eq!(pool.held(), 0);
    }

    #[test]
    fn detaching_a_reused_buffer_copies_it_only_if_it_is_larger_than_needed() {
        let pool = BufPool::new(2);
        drop(pool.take_fitting(32 << 10));
        // Reused for 20 KiB: capacity 32 KiB, more than the 20 KiB it needs, so detaching
        // copies it into a buffer of just that.
        let mut a = pool.take_fitting(20 << 10);
        a.fill(3);
        let p = a.as_ptr();
        let kept = a.detached();
        assert_ne!(kept.as_ptr(), p);
        assert_eq!(kept.capacity(), 20 << 10);
        assert!(kept.iter().all(|&x| x == 3));
        // One that fits stays where it is (a pool of its own: the 32 KiB buffer went back to
        // the first when it was copied).
        let fits = BufPool::new(2);
        drop(fits.take_fitting(20 << 10));
        let b = fits.take_fitting(20 << 10);
        let p = b.as_ptr();
        assert_eq!(b.detached().as_ptr(), p);
    }

    #[test]
    fn reused_buffers_come_back_and_never_much_larger_than_needed() {
        let pool = BufPool::new(2);
        let mut a = pool.take_fitting(64 << 10);
        a.fill(9);
        let p = a.as_ptr();
        drop(a);
        // Within twice the capacity it needs: the same allocation, its bytes as left.
        let b = pool.take_fitting(40 << 10);
        assert_eq!(b.as_ptr(), p);
        assert_eq!(b.len(), 40 << 10);
        assert!(b.iter().all(|&x| x == 9));
        drop(b);
        // More than twice: freed, and a fresh zeroed buffer of just the size needed.
        let c = pool.take_fitting(4 << 10);
        assert_eq!(c.capacity(), 4 << 10);
        assert!(c.iter().all(|&x| x == 0));
        assert_eq!(pool.held(), 0);
        drop(c);
        assert_eq!(pool.held(), 1);
    }

    use super::*;

    #[test]
    fn a_buf_pool_keeps_at_most_its_bound() {
        let pool = BufPool::new(2);
        let bufs: Vec<_> = (0..4).map(|_| pool.take(10)).collect();
        assert_eq!(pool.held(), 0);
        drop(bufs);
        assert_eq!(pool.held(), 2, "the rest are freed");
        let again: Vec<_> = (0..3).map(|_| pool.take(10)).collect();
        assert_eq!(pool.held(), 0);
        drop(again);
        assert_eq!(pool.held(), 2);
    }

    #[test]
    fn a_pooled_buffer_with_a_kept_prefix_is_freed_not_pooled() {
        let pool = BufPool::new(4);
        let mut b = pool.take(8192);
        b.keep(100..200);
        drop(b);
        assert_eq!(pool.held(), 0);
    }

    #[test]
    fn a_pooled_buffer_goes_back_from_another_thread_and_grows_past_its_capacity() {
        let pool = BufPool::new(4);
        let b = pool.take(100);
        std::thread::spawn(move || drop(b)).join().unwrap();
        assert_eq!(pool.held(), 1);
        // Recycled, then grown past its page: reallocated, old bytes kept, the rest zero.
        let mut c = pool.take(10);
        c.fill(9);
        c.resize(IoBuf::ALIGN + 1);
        assert!(c[..10].iter().all(|&x| x == 9) && c[10..].iter().all(|&x| x == 0));
        drop(c);
        assert_eq!(pool.held(), 1);
        // A plain buffer never joins a pool.
        drop(IoBuf::zeroed(10));
        assert_eq!(pool.held(), 1);
    }

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
    fn a_kept_range_reads_and_grows() {
        let mut b = IoBuf::zeroed(2 * IoBuf::ALIGN);
        for (i, x) in b.iter_mut().enumerate() {
            *x = (i % 251) as u8;
        }
        b.keep(10..IoBuf::ALIGN + 10);
        assert_eq!(b.len(), IoBuf::ALIGN);
        assert_eq!(b[0], 10);
        // Shrinking and regrowing within the allocation zeroes the new bytes.
        b.resize(5);
        b.resize(20);
        assert_eq!(&b[..5], &[10, 11, 12, 13, 14]);
        assert!(b[5..].iter().all(|&x| x == 0));
        // Growing past the allocation moves to a fresh, aligned buffer.
        b.resize(3 * IoBuf::ALIGN);
        assert_eq!(b.as_ptr() as usize % IoBuf::ALIGN, 0);
        assert_eq!(&b[..5], &[10, 11, 12, 13, 14]);
    }

    #[test]
    fn slots_are_taken_given_back_and_never_grow() {
        let pool = SlotPool::new(2, 2 * IoBuf::ALIGN);
        let mut a = pool.take(100).unwrap();
        assert!(a.is_registered() && a.len() == 100);
        assert_eq!(a.as_ptr() as usize % IoBuf::ALIGN, 0);
        let b = pool.take(2 * IoBuf::ALIGN).unwrap();
        assert!(pool.take(1).is_none(), "both slots are taken");
        assert!(pool.take(3 * IoBuf::ALIGN).is_none(), "longer than a slot");
        drop(b);
        assert_eq!(pool.free_slots(), 1);
        // Growing past the slot moves to the heap and gives the slot back.
        a.fill(3);
        a.resize(3 * IoBuf::ALIGN);
        assert!(!a.is_registered());
        assert!(a[..100].iter().all(|&x| x == 3) && a[100..].iter().all(|&x| x == 0));
        assert_eq!(pool.free_slots(), 2);
        // Detaching copies out and gives the slot back.
        let mut c = pool.take(10).unwrap();
        c.fill(9);
        let d = c.detached();
        assert!(!d.is_registered() && d.iter().all(|&x| x == 9));
        assert_eq!(pool.free_slots(), 2);
        // A slot buffer kept to a range (direct I/O) still finds its slot: on drop, and when
        // it grows past the slot's end.
        let mut k = pool.take(2 * IoBuf::ALIGN).unwrap();
        k[100..200].fill(5);
        k.keep(100..200);
        assert!(k.is_registered() && k.iter().all(|&x| x == 5));
        drop(k);
        assert_eq!(pool.free_slots(), 2);
        let mut k = pool.take(2 * IoBuf::ALIGN).unwrap();
        k.keep(IoBuf::ALIGN..IoBuf::ALIGN + 10);
        k.resize(2 * IoBuf::ALIGN);
        assert!(!k.is_registered() && k.len() == 2 * IoBuf::ALIGN);
        assert_eq!(pool.free_slots(), 2);
        // A buffer can outlive the pool's other owners.
        let e = pool.take(10).unwrap();
        drop(pool);
        drop(e);
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
