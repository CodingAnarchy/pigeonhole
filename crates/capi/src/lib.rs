//! Stable C ABI for Pigeonhole (future).
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

// Future: the stable C ABI (`pigeonhole.h`): opaque handles over `Pigeonhole`, `Table`,
// `Snapshot` and `RowIter`, owned `Cell`/`Row` copies, and `ErrorCode` as the C error enum.
// The public crate keeps an owned or cursor-style form of every API for this purpose.
