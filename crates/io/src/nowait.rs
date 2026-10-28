//! D292 (#749): a read the page cache holds is copied on the thread that submits it, with
//! `preadv2(RWF_NOWAIT)`, rather than handed to a pool thread or copied inside an io_uring
//! enter under the kernel's `uring_lock` that every submitter queues on. Linux only; a miss
//! (or a short read) is submitted as before.
//!
//! Each backend has its own switch and counters ([`Vfs::set_nowait_reads`],
//! [`Vfs::nowait_reads`], ICR 0083), and each file its own breaker.
//!
//! [`Vfs::set_nowait_reads`]: crate::Vfs::set_nowait_reads
//! [`Vfs::nowait_reads`]: crate::Vfs::nowait_reads

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};

/// Longest read tried inline. Shard and compaction threads submit reads too: a large blob
/// extent copied inline would be a long synchronous copy inside a turn (D289), so it is
/// submitted as before (perf287's S2).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_LEN: usize = 256 << 10;

/// Misses in a row, with no hit between them, that open a file's breaker (perf287's S1): a
/// file system that takes the flag but never finds anything cached (some FUSE and network
/// mounts) would otherwise cost a system call on every read.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const BREAK_AFTER: u32 = 64;

/// Reads an open breaker submits without trying, before the file tries again. A file read
/// cold at first (after a restart) trips it and recovers; one that never hits tries
/// `BREAK_AFTER` reads in every `BREAK_AFTER + SKIP`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const SKIP: u32 = 4096;

/// Upper bounds of the miss-latency buckets in microseconds: `[0, 1)`, `[1, 2)`, ...
/// `[512, 1024)`, then everything at or over 1024 µs.
pub const MISS_BUCKETS: usize = 12;

/// Hits are counted on stripes, one cache line each, by thread: a single counter would be a
/// line every client's reads share.
const STRIPES: usize = 16;

#[repr(align(128))]
#[derive(Default)]
struct Stripe(AtomicU64);

/// A backend's switch and counters.
pub(crate) struct Backend {
    on: AtomicBool,
    hits: [Stripe; STRIPES],
    misses: AtomicU64,
    miss_nanos: AtomicU64,
    miss_buckets: [AtomicU64; MISS_BUCKETS],
    refused: AtomicU64,
    breaks: AtomicU64,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            on: AtomicBool::new(true),
            hits: Default::default(),
            misses: AtomicU64::new(0),
            miss_nanos: AtomicU64::new(0),
            miss_buckets: Default::default(),
            refused: AtomicU64::new(0),
            breaks: AtomicU64::new(0),
        }
    }
}

impl Backend {
    pub(crate) fn set(&self, on: bool) {
        self.on.store(on, Relaxed);
    }

    pub(crate) fn stats(&self) -> NowaitReads {
        NowaitReads {
            on: self.on.load(Relaxed),
            hits: self.hits.iter().map(|s| s.0.load(Relaxed)).sum(),
            misses: self.misses.load(Relaxed),
            miss_nanos: self.miss_nanos.load(Relaxed),
            miss_buckets: std::array::from_fn(|i| self.miss_buckets[i].load(Relaxed)),
            refused: self.refused.load(Relaxed),
            breaks: self.breaks.load(Relaxed),
        }
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    #[inline]
    pub(crate) fn on(&self) -> bool {
        self.on.load(Relaxed)
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    #[inline]
    fn hit(&self) {
        thread_local! {
            static STRIPE: usize = {
                static NEXT: AtomicUsize = AtomicUsize::new(0);
                NEXT.fetch_add(1, Relaxed) % STRIPES
            };
        }
        let i = STRIPE.with(|s| *s);
        self.hits[i].0.fetch_add(1, Relaxed);
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    #[cold]
    fn miss(&self, nanos: u64) {
        self.misses.fetch_add(1, Relaxed);
        self.miss_nanos.fetch_add(nanos, Relaxed);
        let micros = nanos / 1000;
        let bucket = (u64::BITS - micros.leading_zeros()) as usize;
        self.miss_buckets[bucket.min(MISS_BUCKETS - 1)].fetch_add(1, Relaxed);
    }
}

/// A file's state: whether its file system refused the flag, and its breaker.
#[derive(Default)]
pub(crate) struct FileState {
    refused: AtomicBool,
    /// Misses since the last hit.
    streak: AtomicU32,
    /// Reads the open breaker still submits without trying.
    skip: AtomicU32,
}

/// What one attempt found ([`crate::os::read_cached_at`]).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cached {
    /// The page cache held all of the buffer, which now has the bytes.
    Hit,
    /// It did not (`EAGAIN`), the read came up short, or anything else went wrong (`EINTR`
    /// included: no retry in the fast path); the buffer holds nothing to rely on.
    Miss,
    /// The file system or kernel refuses `RWF_NOWAIT` (overlay, virtiofs and NFS say
    /// `EOPNOTSUPP`, a kernel without the flag `EINVAL`): it never will for this file.
    Refused,
}

impl FileState {
    /// Whether to try this read inline: the backend's switch is on, the file system took
    /// the flag, and the breaker is closed.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    #[inline]
    pub(crate) fn try_now(&self, backend: &Backend) -> bool {
        if !backend.on() || self.refused.load(Relaxed) {
            return false;
        }
        let mut skip = self.skip.load(Relaxed);
        if skip == 0 {
            return true;
        }
        // Open: count one skipped read, saturating at 0. Readers racing on the last skips
        // must not take it below 0, which would wrap it and open the breaker for good
        // (perf287's review of #1018). A loop of `compare_exchange` only while it is open.
        while let Some(next) = skip.checked_sub(1) {
            match self
                .skip
                .compare_exchange_weak(skip, next, Relaxed, Relaxed)
            {
                Ok(_) => break,
                Err(now) => skip = now,
            }
        }
        false
    }

    /// Records what an attempt found, `nanos` after it started.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    #[inline]
    pub(crate) fn record(&self, backend: &Backend, found: Cached, nanos: u64) {
        match found {
            Cached::Hit => {
                backend.hit();
                if self.streak.load(Relaxed) != 0 {
                    self.streak.store(0, Relaxed);
                }
            }
            Cached::Miss => {
                backend.miss(nanos);
                if self.streak.fetch_add(1, Relaxed) + 1 >= BREAK_AFTER {
                    self.streak.store(0, Relaxed);
                    self.skip.store(SKIP, Relaxed);
                    backend.breaks.fetch_add(1, Relaxed);
                }
            }
            Cached::Refused => {
                self.refused.store(true, Relaxed);
                backend.refused.fetch_add(1, Relaxed);
            }
        }
    }
}

/// A backend's inline page-cache reads since it started ([`crate::Vfs::nowait_reads`]):
/// for benches and tests, not part of the API.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct NowaitReads {
    /// Whether the backend tries them now.
    pub on: bool,
    /// Reads copied inline.
    pub hits: u64,
    /// Attempts that found the range not all cached (or failed): submitted instead.
    pub misses: u64,
    /// Time the missed attempts took, in nanoseconds.
    pub miss_nanos: u64,
    /// Missed attempts by time: bucket `i` under `2^i` µs (bucket 0 under 1 µs), the last
    /// one everything slower.
    pub miss_buckets: [u64; MISS_BUCKETS],
    /// Files whose file system refused `RWF_NOWAIT`.
    pub refused: u64,
    /// Times a file's breaker opened after misses with no hit.
    pub breaks: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn readers_racing_on_an_open_breakers_last_skips_never_wrap_it() {
        // perf287's review of #1018: two readers that both saw one skip left took the
        // count from 1 to 0 and then to u32::MAX, opening the breaker for good.
        let backend = Backend::default();
        // Miri runs threads interleaved one step at a time: a few rounds there (the
        // thousands are for real threads racing).
        let rounds = if cfg!(miri) { 4 } else { 2000 };
        for round in 0..rounds {
            let file = FileState::default();
            file.skip.store(2, Relaxed);
            let start = Barrier::new(8);
            std::thread::scope(|s| {
                for _ in 0..8 {
                    s.spawn(|| {
                        start.wait();
                        for _ in 0..4 {
                            let _ = file.try_now(&backend);
                        }
                    });
                }
            });
            assert_eq!(
                file.skip.load(Relaxed),
                0,
                "round {round}: the skip count wrapped"
            );
            assert!(
                file.try_now(&backend),
                "round {round}: a closed breaker tries"
            );
        }
    }
}
