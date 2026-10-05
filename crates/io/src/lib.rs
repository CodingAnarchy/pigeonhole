//! Vfs abstraction for Pigeonhole: files, aligned buffers, completions, pread pool and fault-injecting simulated backend.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
// `unsafe` is permitted in this crate; every block carries a `// SAFETY:` argument.
#![deny(unsafe_op_in_unsafe_fn)]
