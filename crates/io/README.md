# pigeonhole-io

Vfs abstraction for Pigeonhole: files, aligned buffers, completions, direct I/O, a pread pool, an io_uring backend (Linux) and a fault-injecting simulated backend.

This is an internal crate of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). Most users want the [`pigeonhole`](https://crates.io/crates/pigeonhole) crate. It makes **no stability promise of its own** (D282): `pigeonhole` pins it exactly, and any release may change it incompatibly, under a new major version of this crate. Depend on `pigeonhole` instead.
