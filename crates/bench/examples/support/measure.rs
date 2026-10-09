//! What callgrind counts in the measured examples (`scripts/instructions-per-cell.sh`): only
//! the work done while a [`Measured`] is alive, on the thread that holds it. The script runs
//! callgrind with collection off (`--collect-atstart=no`); a `Measured` turns it on for its
//! thread and off again when dropped, with callgrind's `TOGGLE_COLLECT` client request.
//!
//! Matching function names (`--toggle-collect`) is not reliable: callgrind sometimes does not
//! resolve a binary's symbols, and then it silently counts nothing (#350). A client request
//! is an instruction sequence, so it does not depend on symbols. Outside valgrind, and on
//! targets other than x86-64 Linux, it does nothing.

/// Counts the work of the current thread while it lives. Do not nest two on one thread: each
/// toggles collection, so the inner one would turn it off.
pub struct Measured(());

impl Measured {
    /// Starts counting on this thread.
    #[must_use = "counting stops when the guard is dropped"]
    pub fn start() -> Self {
        toggle_collect();
        Measured(())
    }
}

impl Drop for Measured {
    fn drop(&mut self) {
        toggle_collect();
    }
}

/// Callgrind's `CALLGRIND_TOGGLE_COLLECT` (`VG_USERREQ_TOOL_BASE('C', 'T') + 2`).
const TOGGLE_COLLECT: u64 = 0x4354_0002;

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
#[inline(never)]
fn toggle_collect() {
    let args: [u64; 6] = [TOGGLE_COLLECT, 0, 0, 0, 0, 0];
    // SAFETY: valgrind's client-request sequence for x86-64 (valgrind.h): four rotations of
    // rdi by 128 bits in all (rdi is unchanged) and `xchg rbx, rbx` (a no-op). On hardware it
    // changes nothing but the flags; under valgrind it delivers the request whose arguments
    // rax points at and writes its result to rdx, which starts as the default (0).
    unsafe {
        std::arch::asm!(
            "rol rdi, 3",
            "rol rdi, 13",
            "rol rdi, 61",
            "rol rdi, 51",
            "xchg rbx, rbx",
            in("rax") args.as_ptr(),
            inout("rdx") 0u64 => _,
            options(nostack),
        );
    }
}

#[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
fn toggle_collect() {
    let _ = TOGGLE_COLLECT;
}
