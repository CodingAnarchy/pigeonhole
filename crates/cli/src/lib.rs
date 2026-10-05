//! phdb: command-line shell and tools for Pigeonhole databases.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

// Phase 4: the `phdb` commands (`shell`, `dump`, `compact`, `check`, `backup`) as library
// functions the binary calls, so they can be snapshot-tested. Builds on the public API only.
