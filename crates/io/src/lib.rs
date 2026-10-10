//! Vfs abstraction for Pigeonhole: files, aligned buffers, completions, pread pool and
//! fault-injecting simulated backend.
//!
//! Every byte any other crate reads or writes goes through a [`Vfs`], so the simulator can
//! inject faults and control time. Backends:
//!
//! - [`pread::PreadVfs`]: real files, a `pread`/`pwrite` thread pool for submitted I/O. The
//!   safe reference backend on every platform.
//! - [`sim::SimVfs`]: in-memory, deterministic from a seed, with torn writes, reordered
//!   fsyncs, ENOSPC and crashes. With [`sim::FaultPlan::none`] it is the plain in-memory mock
//!   other crates test against.
//! - `uring::UringVfs` (Linux only, so not linked here): real files with submitted I/O on
//!   io_uring (#402).
//!
//! This crate also owns the other OS-facing `unsafe` the engine needs: shared-memory mappings
//! ([`SharedRegion`]), byte-range locks, thread pinning and NUMA binding ([`sys`]).
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
// `unsafe` is permitted in this crate; every block carries a `// SAFETY:` argument.
#![deny(unsafe_op_in_unsafe_fn)]

mod buf;
mod completion;
mod error;
mod file;
mod os;
mod own;
pub mod pread;
mod shared;
pub mod sim;
pub mod sys;
#[cfg(target_os = "linux")]
pub mod uring;
mod vfs;

pub use buf::IoBuf;
pub use completion::{Completion, Resolver};
pub use error::{Error, ErrorKind, Result};
pub use file::{File, FileRef, Locality, LockMode, OpenOptions};
pub use own::{OwnIoWaker, own_io_in_flight, own_io_waker, reap_own_io};
pub use shared::{SharedOpen, SharedRegion};
pub use vfs::{FileIdentity, ProcessId, Vfs, VfsRef};
