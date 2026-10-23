//! A 16-byte word changed only by an atomic compare-exchange (#888, ICR 0069): the engine's
//! seqno and default-timestamp allocator keeps `(S, H, F)` in one ([D271]), so a reservation
//! is one atomic step with no lock.
//!
//! On x86-64 with `cmpxchg16b`, and on aarch64 with LSE's `caspal`, each operation is one
//! inline instruction (a full barrier). Elsewhere, and under Miri and loom (`--cfg
//! io_loom`), a lock guards the word; the path is chosen once per process, so every word in
//! it uses the same one.
//!
//! [D271]: https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/design/decisions/phase-3.md#d271

use std::cell::UnsafeCell;
use std::sync::PoisonError;
#[cfg(all(
    not(miri),
    not(io_loom),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(io_loom)]
use loom::sync::Mutex;
#[cfg(not(io_loom))]
use std::sync::Mutex;

/// How this process changes a word: one native instruction, or under the word's lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Path {
    Native,
    Lock,
}

/// This process's path, detected once (`0`: not yet).
fn path() -> Path {
    #[cfg(any(
        miri,
        io_loom,
        not(any(target_arch = "x86_64", target_arch = "aarch64"))
    ))]
    {
        Path::Lock
    }
    #[cfg(all(
        not(miri),
        not(io_loom),
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    {
        static PATH: AtomicU8 = AtomicU8::new(0);
        match PATH.load(Ordering::Relaxed) {
            1 => Path::Native,
            2 => Path::Lock,
            _ => {
                #[cfg(target_arch = "x86_64")]
                let native = std::arch::is_x86_feature_detected!("cmpxchg16b");
                #[cfg(target_arch = "aarch64")]
                let native = std::arch::is_aarch64_feature_detected!("lse");
                PATH.store(if native { 1 } else { 2 }, Ordering::Relaxed);
                if native { Path::Native } else { Path::Lock }
            }
        }
    }
}

/// The word itself, aligned as `cmpxchg16b` and `caspal` need.
#[repr(C, align(16))]
#[derive(Debug)]
struct Aligned(UnsafeCell<u128>);

/// A `u128` changed only by [`compare_exchange`](Self::compare_exchange), atomically and
/// sequentially consistent. Read it with [`load`](Self::load), which is a compare-exchange
/// too (16-byte plain loads are not atomic on every CPU this runs on): callers that update
/// it in a loop keep the value they last saw and pass that as the first guess instead.
#[derive(Debug)]
pub struct AtomicWord128 {
    word: Aligned,
    /// Guards `word` on the lock path; unused on the native one.
    lock: Mutex<()>,
}

// SAFETY: `word` is only read or written by the native instruction (atomic) or under `lock`,
// and every word of a process takes the same path (`path`), so no access is unsynchronized.
unsafe impl Sync for AtomicWord128 {}

impl Default for AtomicWord128 {
    fn default() -> Self {
        Self::new(0)
    }
}

impl AtomicWord128 {
    /// A word holding `v`.
    #[cfg(not(io_loom))]
    pub const fn new(v: u128) -> Self {
        Self {
            word: Aligned(UnsafeCell::new(v)),
            lock: Mutex::new(()),
        }
    }

    /// A word holding `v` (loom's lock is not `const`).
    #[cfg(io_loom)]
    pub fn new(v: u128) -> Self {
        Self {
            word: Aligned(UnsafeCell::new(v)),
            lock: Mutex::new(()),
        }
    }

    /// Whether this process changes words with one native instruction rather than a lock.
    pub fn is_lock_free() -> bool {
        path() == Path::Native
    }

    /// Stores `new` if the word holds `current`: `Ok(current)`, else `Err` with what it
    /// holds. Sequentially consistent either way.
    #[inline]
    pub fn compare_exchange(&self, current: u128, new: u128) -> Result<u128, u128> {
        self.compare_exchange_on(path(), current, new)
    }

    /// The word's value (a compare-exchange that stores nothing new).
    #[inline]
    pub fn load(&self) -> u128 {
        match self.compare_exchange(0, 0) {
            Ok(v) | Err(v) => v,
        }
    }

    #[inline]
    fn compare_exchange_on(&self, path: Path, current: u128, new: u128) -> Result<u128, u128> {
        match path {
            Path::Native => self.native(current, new),
            Path::Lock => {
                let _guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
                let p = self.word.0.get();
                // SAFETY: on the lock path every access to `word` holds `lock`.
                let held = unsafe { *p };
                if held == current {
                    // SAFETY: as above.
                    unsafe { *p = new };
                    Ok(current)
                } else {
                    Err(held)
                }
            }
        }
    }

    #[cfg(all(not(miri), not(io_loom), target_arch = "x86_64"))]
    #[inline]
    fn native(&self, current: u128, new: u128) -> Result<u128, u128> {
        let (mut lo, mut hi) = (current as u64, (current >> 64) as u64);
        let ok: u8;
        // SAFETY: `word` is 16-byte aligned and valid for the word's life; this process
        // detected `cmpxchg16b` (`path`), and every access on this path is atomic. `rbx`,
        // which LLVM reserves, is swapped in for the new low half and restored after.
        unsafe {
            std::arch::asm!(
                "xchg {t}, rbx",
                "lock cmpxchg16b xmmword ptr [{p}]",
                "sete {ok}",
                "mov rbx, {t}",
                p = in(reg) self.word.0.get(),
                t = inout(reg) new as u64 => _,
                ok = out(reg_byte) ok,
                inout("rax") lo,
                inout("rdx") hi,
                in("rcx") (new >> 64) as u64,
                options(nostack),
            );
        }
        let held = (u128::from(hi) << 64) | u128::from(lo);
        if ok != 0 { Ok(current) } else { Err(held) }
    }

    #[cfg(all(not(miri), not(io_loom), target_arch = "aarch64"))]
    #[inline]
    fn native(&self, current: u128, new: u128) -> Result<u128, u128> {
        let (mut lo, mut hi) = (current as u64, (current >> 64) as u64);
        // SAFETY: `word` is 16-byte aligned and valid for the word's life; this process
        // detected LSE (`path`), and every access on this path is atomic. `caspal` compares
        // the pair x0:x1 (low, high: little-endian, low half first) with the word, stores
        // x2:x3 if equal, and leaves the word's previous value in x0:x1 either way.
        unsafe {
            std::arch::asm!(
                ".arch_extension lse",
                "caspal x0, x1, x2, x3, [{p}]",
                p = in(reg) self.word.0.get(),
                inout("x0") lo,
                inout("x1") hi,
                in("x2") new as u64,
                in("x3") (new >> 64) as u64,
                options(nostack),
            );
        }
        let held = (u128::from(hi) << 64) | u128::from(lo);
        if held == current {
            Ok(current)
        } else {
            Err(held)
        }
    }

    #[cfg(any(
        miri,
        io_loom,
        not(any(target_arch = "x86_64", target_arch = "aarch64"))
    ))]
    #[inline]
    fn native(&self, _current: u128, _new: u128) -> Result<u128, u128> {
        unreachable!("no native 16-byte compare-exchange on this target")
    }
}

#[cfg(all(test, io_loom))]
mod loom_tests {
    use super::AtomicWord128;
    use loom::sync::Arc;
    use loom::thread;

    /// Two threads each add to both halves in one exchange, retrying with what they found,
    /// while a third reads: no update is lost or torn, and a read never sees half of one.
    #[test]
    fn loom_exchanges_on_both_halves_are_never_lost_or_torn() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let w = Arc::new(AtomicWord128::new(0));
            let adders: Vec<_> = (0..2)
                .map(|_| {
                    let w = Arc::clone(&w);
                    thread::spawn(move || {
                        let mut seen = 0u128;
                        loop {
                            let next = (seen + 1) + (3 << 64);
                            match w.compare_exchange(seen, next) {
                                Ok(_) => break,
                                Err(actual) => seen = actual,
                            }
                        }
                    })
                })
                .collect();
            let v = w.load();
            let (lo, hi) = (v as u64, (v >> 64) as u64);
            assert_eq!(u128::from(hi), 3 * u128::from(lo), "a torn read: {v:#x}");
            for a in adders {
                a.join().unwrap();
            }
            assert_eq!(w.load(), 2 | (6 << 64));
        });
    }
}

#[cfg(all(test, not(io_loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// The paths this process can test: the lock path always, the native one when detected.
    fn paths() -> Vec<Path> {
        let mut p = vec![Path::Lock];
        if path() == Path::Native {
            p.push(Path::Native);
        }
        p
    }

    #[test]
    fn a_compare_exchange_stores_only_on_a_match_and_reports_what_it_found() {
        for p in paths() {
            let w = AtomicWord128::new(7 | (9 << 64));
            assert_eq!(w.compare_exchange_on(p, 1, 2), Err(7 | (9 << 64)), "{p:?}");
            assert_eq!(
                w.compare_exchange_on(p, 7 | (9 << 64), u128::MAX),
                Ok(7 | (9 << 64))
            );
            assert_eq!(w.compare_exchange_on(p, 0, 0), Err(u128::MAX), "{p:?}");
            // The halves are compared together: a match on one half alone fails.
            assert_eq!(
                w.compare_exchange_on(p, u128::MAX >> 64, 5),
                Err(u128::MAX),
                "{p:?}"
            );
        }
    }

    #[test]
    fn the_native_path_is_taken_wherever_the_cpu_has_it() {
        // So the tests above run the instruction itself on CI's x86-64 and on aarch64 with
        // LSE, not only the lock.
        #[cfg(all(not(miri), target_arch = "x86_64"))]
        assert_eq!(
            AtomicWord128::is_lock_free(),
            std::arch::is_x86_feature_detected!("cmpxchg16b")
        );
        #[cfg(all(not(miri), target_arch = "aarch64"))]
        assert_eq!(
            AtomicWord128::is_lock_free(),
            std::arch::is_aarch64_feature_detected!("lse")
        );
        #[cfg(miri)]
        assert!(!AtomicWord128::is_lock_free());
    }

    #[test]
    fn load_reads_without_changing() {
        let w = AtomicWord128::new(42 << 70);
        assert_eq!(w.load(), 42 << 70);
        assert_eq!(w.load(), 42 << 70);
        let z = AtomicWord128::default();
        assert_eq!(z.load(), 0);
    }

    #[test]
    fn concurrent_updates_to_both_halves_lose_none() {
        // Each thread adds 1 to the low half and 3 to the high half in one exchange, in a
        // loop that retries with what it found: a torn or lost update breaks the sums.
        let (threads, per) = if cfg!(miri) { (2, 20) } else { (4, 20_000) };
        for p in paths() {
            let w = Arc::new(AtomicWord128::new(0));
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    let w = Arc::clone(&w);
                    std::thread::spawn(move || {
                        let mut seen = 0u128;
                        for _ in 0..per {
                            loop {
                                let next = (seen + 1) + (3 << 64);
                                match w.compare_exchange_on(p, seen, next) {
                                    Ok(_) => {
                                        seen = next;
                                        break;
                                    }
                                    Err(actual) => seen = actual,
                                }
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            let n = (threads * per) as u128;
            assert_eq!(
                w.compare_exchange_on(p, 0, 0),
                Err(n | ((3 * n) << 64)),
                "{p:?}"
            );
        }
    }
}
