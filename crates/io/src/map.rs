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

/// A mapping shared between its [`FileWriteMap`] and the populate helper ([`populate`]),
/// unmapped when the last of them lets go.
#[cfg(target_os = "linux")]
#[derive(Debug)]
struct Shared(crate::os::Mapping);

// SAFETY: plain shared memory, owned here. Its bytes are written only through
// `FileWriteMap::write(&mut self)`, so by one thread at a time; the populate helper only passes
// the range to `madvise`, which never reads or writes a byte of it, and everything else reads
// only the pointer and length.
#[cfg(target_os = "linux")]
unsafe impl Send for Shared {}
// SAFETY: as for `Send`: shared references expose only the pointer, the length and `madvise`.
#[cfg(target_os = "linux")]
unsafe impl Sync for Shared {}

/// How far ahead of the stream's stores the populate helper faults pages in (D312, owner call
/// 3, perf287's S1 on #1369): a window, not the whole segment. Populating dirties a page, and a
/// WAL sync covers the whole file (a rollover, the feed's interval, a flush barrier), so every
/// sync writes back the window's pages not yet stored and write-protects them again: at most
/// this much (plus a step's rounding) per stream per sync, not the rest of a 64 MiB segment.
#[cfg(any(target_os = "linux", test))]
pub(crate) const POPULATE_WINDOW: usize = 4 << 20;

/// The populate helper's step, and what each range's end rounds up to: every range starts
/// where the last one ended, so every `madvise` starts page-aligned (perf287's B1 on #1369).
#[cfg(any(target_os = "linux", test))]
pub(crate) const POPULATE_STEP: usize = 2 << 20;

/// The next range to populate once a store ends at `end`, the ranges so far ending at
/// `queued_to`, in a mapping of `len` bytes: `None` until `end` comes within half a window of
/// `queued_to`. The range starts at `queued_to` and ends a window past `end`, rounded up to a
/// step (or at `len`), so with `queued_to` starting at 0 every range starts step-aligned.
#[cfg(any(target_os = "linux", test))]
pub(crate) fn next_window(queued_to: usize, end: usize, len: usize) -> Option<(usize, usize)> {
    if queued_to >= len || end.saturating_add(POPULATE_WINDOW / 2) < queued_to {
        return None;
    }
    let to = end
        .saturating_add(POPULATE_WINDOW)
        .div_ceil(POPULATE_STEP)
        .saturating_mul(POPULATE_STEP)
        .min(len);
    (to > queued_to).then_some((queued_to, to))
}

/// A real file's shared mapping ([`crate::os::map_range`]). The populate helper faults its
/// pages in writable in the background, [`POPULATE_WINDOW`] ahead of the stores (D312, owner
/// call 3), so the stores mostly find them faulted already.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct FileWriteMap {
    mapping: std::sync::Arc<Shared>,
    /// Where the ranges handed to the helper end: it's asked for more once a store comes
    /// within half a window of it.
    queued_to: usize,
}

#[cfg(target_os = "linux")]
impl FileWriteMap {
    pub(crate) fn new(mapping: crate::os::Mapping) -> Self {
        let mut map = Self {
            mapping: std::sync::Arc::new(Shared(mapping)),
            queued_to: 0,
        };
        map.populate_to(0);
        map
    }

    /// Asks the helper for the pages up to a window past `end` (a store's end), once `end` is
    /// within half a window of what was asked for already ([`next_window`]).
    #[inline]
    fn populate_to(&mut self, end: usize) {
        if let Some((from, to)) = next_window(self.queued_to, end, self.mapping.0.len()) {
            populate::queue(&self.mapping, from, to);
            self.queued_to = to;
        }
    }
}

#[cfg(target_os = "linux")]
impl crate::WriteMap for FileWriteMap {
    fn len(&self) -> usize {
        self.mapping.0.len()
    }

    fn write(&mut self, at: usize, bytes: &[u8]) -> Result<()> {
        // SAFETY: the mapping is live while `self` holds it, and `&mut self` excludes every
        // other store through it; the populate helper never reads or writes its bytes (only
        // `madvise`). Other processes see the same page-cache pages, as with `write_at`; they
        // read through the file, never through Rust references to this memory.
        unsafe { store(self.mapping.0.ptr(), self.mapping.0.len(), at, bytes)? };
        self.populate_to(at + bytes.len());
        Ok(())
    }
}

/// The populate helper (D312, owner call 3): one thread per process, started by the first
/// mapped segment, that faults the ranges each mapping asks for in writable
/// (`MADV_POPULATE_WRITE`), [`POPULATE_WINDOW`] ahead of the stream's stores. Off the stream's
/// thread, which would otherwise pay those faults page by page in its tail. It holds the
/// mapping while it works, so a rollover can't unmap it underneath; it skips a range whose
/// stream has already dropped the mapping. Whether the kernel knows the advice (5.14 on) is
/// decided once, by a probe; without it nothing is queued, and the stores fault as before. A
/// failed `madvise` only ends its own range.
#[cfg(target_os = "linux")]
mod populate {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::{Sender, channel};
    use std::sync::{Arc, Mutex, OnceLock};

    use super::{POPULATE_STEP as STEP, Shared};

    /// A range of a mapping to populate.
    type Job = (Arc<Shared>, usize, usize);

    static HELPER: OnceLock<Option<Mutex<Sender<Job>>>> = OnceLock::new();
    /// Whether the kernel knows `MADV_POPULATE_WRITE`, probed once (perf287's B1: a failed
    /// call never decides it).
    static SUPPORTED: OnceLock<bool> = OnceLock::new();

    pub(super) fn supported() -> bool {
        *SUPPORTED.get_or_init(crate::os::populate_supported)
    }
    /// Bytes populated, for tests (an inspection counter).
    pub(super) static POPULATED: AtomicU64 = AtomicU64::new(0);

    /// Hands `[from, to)` of `mapping` to the helper, starting it the first time. A helper
    /// that couldn't start leaves the mapping as it is.
    pub(super) fn queue(mapping: &Arc<Shared>, from: usize, to: usize) {
        if !supported() {
            return;
        }
        let helper = HELPER.get_or_init(|| {
            let (tx, rx) = channel::<Job>();
            std::thread::Builder::new()
                .name("pigeonhole-io-populate".into())
                .spawn(move || {
                    while let Ok((m, from, to)) = rx.recv() {
                        run(&m, from, to);
                    }
                })
                .ok()
                .map(|_| Mutex::new(tx))
        });
        if let Some(tx) = helper {
            let _ = tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .send((Arc::clone(mapping), from, to));
        }
    }

    fn run(m: &Arc<Shared>, from: usize, to: usize) {
        let mut at = from;
        // Only while the stream still holds it: once it rolled over, the rest is never stored.
        // (The queue holds clones too, so "still held" is counted loosely: at worst a step
        // more of a range nobody will store into.)
        while at < to && Arc::strong_count(m) > 1 {
            let n = STEP.min(to - at);
            if crate::os::populate_write(&m.0, at, n).is_err() {
                // This range only: the stores fault as they would have.
                return;
            }
            POPULATED.fetch_add(n as u64, Ordering::Relaxed);
            at += n;
        }
    }
}

/// Bytes the populate helper has faulted in so far (an inspection counter for tests).
#[cfg(all(target_os = "linux", test))]
pub(crate) fn populated_bytes() -> u64 {
    populate::POPULATED.load(std::sync::atomic::Ordering::Relaxed)
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

    /// perf287's B1 on #1369: every range starts where the last ended, step-aligned (so
    /// page-aligned for `madvise`), however unaligned the stores' ends; ranges never pass the
    /// mapping; and a store always has at least half a window populated ahead of it.
    #[test]
    fn populate_windows_are_contiguous_and_step_aligned() {
        use super::{POPULATE_STEP as S, POPULATE_WINDOW as W, next_window};
        for len in [3 * W + 4096, 16 * W, W / 2] {
            let mut queued = 0;
            let mut ranges = 0;
            let mut end = 0;
            while end <= len {
                if let Some((from, to)) = next_window(queued, end, len) {
                    assert_eq!(from, queued, "contiguous");
                    assert!(from % S == 0, "step-aligned start {from}");
                    assert!(to == len || to % S == 0, "step-aligned end {to}");
                    assert!(to <= len && to > from);
                    queued = to;
                    ranges += 1;
                }
                assert!(
                    queued == len || queued >= end + W / 2,
                    "half a window ahead of {end}"
                );
                // Unaligned store ends, as a WAL's groups have.
                end += 4096 * 37 + 123;
            }
            assert_eq!(queued, len);
            assert!(
                ranges >= len.div_ceil(W).min(3),
                "{ranges} ranges over {len}"
            );
        }
        assert_eq!(
            next_window(W, W / 2 - 1, 16 * W),
            None,
            "not yet within half a window"
        );
    }

    /// D312 owner call 3: a held mapping is populated in the background a window at a time as
    /// its stores advance, across many refills at unaligned store ends (perf287's B1), and
    /// stores made meanwhile land in the file. A kernel without `MADV_POPULATE_WRITE` (before
    /// 5.14) populates nothing: the fallback.
    #[cfg(target_os = "linux")]
    #[test]
    #[cfg_attr(
        miri,
        ignore = "a real file's mapping and madvise: foreign calls Miri can't run"
    )]
    fn a_held_mapping_is_populated_a_window_ahead_of_its_stores() {
        use super::{POPULATE_WINDOW as W, next_window};
        use crate::WriteMap;
        use std::io::{Read, Seek};
        let dir = std::env::temp_dir().join(format!("phio-populate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg");
        let len = 8 * W;
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(len as u64).unwrap();
        let supported = super::populate::supported();
        let before = super::populated_bytes();
        let wait_for = |n: usize| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while super::populated_bytes() < before + n as u64
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            if supported {
                assert_eq!(super::populated_bytes() - before, n as u64, "populated");
            }
        };
        let mapping = crate::os::map_range(&f, 0, len).unwrap().unwrap();
        let mut map = super::FileWriteMap::new(mapping);
        // The first window only, then each refill as the stores move on.
        let mut queued = next_window(0, 0, len).unwrap().1;
        wait_for(queued);
        let mut refills = 0;
        let mut at = 0;
        let rec = b"record";
        while at + rec.len() <= len {
            map.write(at, rec).unwrap();
            if let Some((_, to)) = next_window(queued, at + rec.len(), len) {
                queued = to;
                refills += 1;
                wait_for(queued);
            }
            // Unaligned ends, about 1 MiB apart.
            at += (1 << 20) + 4096 * 3 + 77;
        }
        assert!(refills >= 5, "{refills} refills");
        if supported {
            assert_eq!(queued, len, "populating stayed on to the end");
        }
        drop(map);
        let mut got = vec![0u8; len];
        f.rewind().unwrap();
        f.read_exact(&mut got).unwrap();
        let mut at = 0;
        let mut zeros = 0usize;
        while at < len {
            let n = rec.len().min(len - at);
            if at + rec.len() <= len {
                assert_eq!(&got[at..at + n], rec, "a store at {at}");
            }
            let next = (at + (1 << 20) + 4096 * 3 + 77).min(len);
            zeros += got[(at + n).min(len)..next]
                .iter()
                .filter(|&&b| b != 0)
                .count();
            at = next;
        }
        assert_eq!(zeros, 0, "populating writes nothing");
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }
}
