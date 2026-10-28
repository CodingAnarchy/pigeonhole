# 0083: `Vfs::set_nowait_reads`, `Vfs::nowait_reads` and `Options::nowait_reads`, doc-hidden switches and counters for D292's inline page-cache reads

**Status:** Approved (coordinator, 2026-10-28; #749, after perf287's look at D292). Additive and doc-hidden: two provided methods on `pigeonhole_io::Vfs`, a doc-hidden struct, a doc-hidden `Options` setter and a doc-hidden `Pigeonhole` getter. No existing item changes, and no implementor has to change.

## Change

**`pigeonhole-io`:**

```rust
// On the `Vfs` trait, both provided and `#[doc(hidden)]`:
/// Turns D292's inline page-cache reads on or off for this backend (on by default where a
/// backend has them). The default does nothing.
fn set_nowait_reads(&self, on: bool) {}
/// D292's inline page-cache reads on this backend so far, or `None` for a backend without
/// them (the default).
fn nowait_reads(&self) -> Option<NowaitReads> { None }

#[doc(hidden)]
#[non_exhaustive]
pub struct NowaitReads {
    pub on: bool,
    pub hits: u64,
    pub misses: u64,
    pub miss_nanos: u64,
    pub miss_buckets: [u64; MISS_BUCKETS], // under 1, 2, 4 ... 1024 µs, then the rest
    pub refused: u64,                      // files whose file system refused RWF_NOWAIT
    pub breaks: u64,                       // times a file's breaker opened
}
```

- **`PreadVfs` overrides both.** `UringVfs` delegates to the `PreadVfs` that opens its files, so the two backends report alike.
  - The switch and counters belong to the backend: every file it opens shares them.
  - Hits are counted on per-thread stripes, so that no single line is shared by every reader.
  - On a target other than Linux, `nowait_reads` is `None` and the switch does nothing.
- **`SimVfs` and the wrapping test `Vfs`s keep the defaults.** The simulator never reads inline, so seeds replay unchanged.

**`pigeonhole`:**
- **`Options::nowait_reads(bool)`** (`#[doc(hidden)]`): at open, it calls `set_nowait_reads` on the database's backend, whether the options built that backend or it was supplied through `Options::vfs`.
- **`Pigeonhole::nowait_reads() -> Option<pigeonhole_io::NowaitReads>`** (`#[doc(hidden)]`). It returns the io type directly: it's a bench-only getter outside the API, so a pigeonhole-owned copy of the struct would add nothing.

## Why

D292 (#749) copies a read the page cache holds on the thread that submits it. Benches need to compare it on and off in one build (perf287's N5), and to report hits, misses, the time a miss costs (S3), refusals and breaker trips (S1).

perf287's N1 asked for the switch and counters to be per backend rather than process statics. Then tests that build several backends run side by side, and a bench reads the backend it opened.

The setting isn't on `OpenOptions`: that struct has public fields, so adding one would break every struct literal of it.

## Callers

- **`pigeonhole-bench`:** `PigeonholeRunner::nowait_reads(bool)` (`--no-nowait-reads`) sets `Options::nowait_reads(false)`. At close, the runner prints the counters to stderr.
- **Tests:**
  - `pigeonhole-io`'s `nowait_reads.rs`;
  - `uring.rs`'s cross-ring wait case, which needs its reads to reach the ring;
  - `pigeonhole-runtime`'s `driver_ring.rs`, for the same reason.
- **semver:** `cargo semver-checks` treats doc-hidden items as outside the API. Even counted, the provided trait methods are minor.
