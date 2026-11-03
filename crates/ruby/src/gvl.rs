//! Releasing the GVL around engine calls: the crate's one hand-written `unsafe` (ICR 0111,
//! the owner's O3 exception to #696's macro-sites-only policy).
//!
//! magnus 0.9 doesn't wrap `rb_thread_call_without_gvl2`, so this module calls it through
//! `rb-sys`. Everything else goes through [`without_gvl`], whose bounds are the soundness
//! argument:
//!
//! - **`F: Send`, `R: Send`.** Every Ruby handle in magnus (`Value`, `RString`, the wrapped
//!   classes' references, `Ruby` itself) is `!Send`, so the closure can capture no Ruby
//!   object and return none: code that runs without the GVL is pure Rust by type.
//! - **`rb_thread_call_without_gvl2`, not `rb_thread_call_without_gvl`** (simp724's G1 on
//!   #697). The latter checks pending interrupts after the function returns and may
//!   `longjmp` straight over these Rust frames (undefined behaviour, and a commit that
//!   happened reported as an `Interrupt`). The `2` form never raises: it returns, and tells
//!   us through `ran` whether the function ran. If it did, its result is returned and any
//!   pending interrupt is handled by Ruby after the method returns. If an interrupt was
//!   pending before it started, nothing ran, and the caller handles the interrupt through
//!   magnus's safe `Ruby::thread_check_ints` before trying again.
//! - **No unblocking function** (`NULL`): Ruby never calls into this module while the
//!   function runs, so an engine call isn't interruptible midway. The calls are bounded
//!   (waits take timeouts), and the guide says so.
//! - **Panics** are caught inside the trampoline (`catch_unwind`) and resumed only after
//!   the GVL is back, so none unwinds through Ruby's C frames.
#![allow(unsafe_code)]

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

use magnus::{Error, Ruby};

/// The call the trampoline makes: the closure (taken when it runs) and its outcome.
struct Call<F, R> {
    f: Option<F>,
    out: Option<std::thread::Result<R>>,
}

/// Runs the closure in `data` once, without the GVL.
extern "C" fn trampoline<F: FnOnce() -> R, R>(data: *mut c_void) -> *mut c_void {
    // SAFETY: `data` is the `&mut Call<F, R>` that `try_once` passed, which lives on its
    // stack for the whole `rb_thread_call_without_gvl2` call; nothing else touches it until
    // that call returns, and Ruby calls this function at most once per call.
    let call = unsafe { &mut *data.cast::<Call<F, R>>() };
    if let Some(f) = call.f.take() {
        call.out = Some(catch_unwind(AssertUnwindSafe(f)));
    }
    std::ptr::null_mut()
}

/// One attempt: runs `call`'s closure without the GVL, unless an interrupt was pending.
fn try_once<F: FnOnce() -> R + Send, R: Send>(call: &mut Call<F, R>) {
    // SAFETY: called on a Ruby thread holding the GVL (every caller holds a `&Ruby`).
    // `trampoline::<F, R>` matches the `void *(*)(void *)` signature and reads `data` as
    // the `Call<F, R>` it is. No unblocking function is passed, so Ruby makes no other
    // call. The `2` variant never raises or jumps: it returns, ran or not.
    unsafe {
        rb_sys::rb_thread_call_without_gvl2(
            Some(trampoline::<F, R>),
            std::ptr::from_mut(call).cast::<c_void>(),
            None,
            std::ptr::null_mut(),
        );
    }
}

/// Runs `f` with the GVL released and returns its result. A pending interrupt
/// (`Thread#raise`, `Thread#kill`, a signal) that arrives before `f` starts is raised as a
/// Ruby exception and `f` doesn't run; one that arrives while `f` runs is handled after
/// this returns, so `f`'s result (a commit's outcome, say) is never lost.
pub(crate) fn without_gvl<F, R>(ruby: &Ruby, f: F) -> Result<R, Error>
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    let mut call = Call {
        f: Some(f),
        out: None,
    };
    loop {
        try_once(&mut call);
        match call.out.take() {
            Some(Ok(r)) => return Ok(r),
            Some(Err(panic)) => resume_unwind(panic),
            // Not run: an interrupt was pending. Raise it (or, if it was consumed
            // meanwhile, try again).
            None => ruby.thread_check_ints()?,
        }
    }
}
