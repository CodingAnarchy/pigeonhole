# 0069: `pigeonhole_io::AtomicWord128`, a 16-byte word changed by one atomic compare-exchange

**Status:** Number assigned by the coordinator (2026-10-23; #888). Additive. Needs perf287's review: it adds `unsafe` (inline assembly) to `pigeonhole-io`.

## Change

**`pigeonhole-io`:** a new public type, `AtomicWord128`:
- `new(u128)` (`const`), `Default` (0);
- `compare_exchange(&self, current, new) -> Result<u128, u128>`: stores `new` if the word holds `current`. Returns `Ok(current)`, or `Err` with what it holds. Sequentially consistent.
- `load(&self) -> u128`: a compare-exchange that stores nothing new. A plain 16-byte load is not atomic on every CPU this runs on.
- `is_lock_free() -> bool`: whether this process uses the native instruction.

**How it changes the word:**
- **x86-64 with `cmpxchg16b`:** one inline `lock cmpxchg16b`.
- **aarch64 with LSE:** one inline `caspal`.
- **Otherwise, and always under Miri and loom** (`--cfg io_loom`): under the word's own leaf `Mutex`.

The path is detected once per process (a cached `is_*_feature_detected!`), so every word in a process uses the same one.

**`pigeonhole-engine`:** `clock::Reservation` keeps D271's `(S, H, F)` in one `AtomicWord128` instead of a `Mutex`:
- `S` in the low half;
- `H` in the high half, with `F` in its top bit. Default timestamps are µs, far below 2^63, and `seed_ts` clamps a damaged file's value below it.
- Every change is one compare-exchange, retried with the value found.
- Each thread keeps the word as it last saw it in a thread-local, as its first guess, so a reservation with no race is a single exchange.
- The drift gauge (ICR 0064) stays a separate `AtomicU64`, written only when the lead grows.

## Why

#888, following D271 (#854, #886). The owner accepted #886's +50 instructions per single-group commit for the Mutex, and asked for this to be filed. The update has to stay one atomic step over `(S, H, F)`: two words don't keep D271's order (#854 §4).

`pigeonhole-engine` and `pigeonhole-shm` forbid `unsafe` code. Only `pigeonhole-io`, `-cache` and `-memtable` may hold it, so the type lives here.

Measured forms per single-group commit (x86 callgrind against main before D271's lock):

| form | per single-group commit |
|---|--:|
| the prototype's inline `cmpxchg16b` (22aed914) | +25 |
| `portable-atomic`'s `AtomicU128` (an outlined `cmpxchg16b` with runtime dispatch) | +77 |
| a Mutex taken once per group (#886, today) | +50 |
| this change | the PR's callgrind table |

## Callers

- **`pigeonhole-engine`, `clock.rs`:** `Reservation`, all of its methods.
- **Tests:**
  - `pigeonhole-io` `word128` unit tests: both paths (the native one where the CPU has it), torn and lost updates under threads, and a check that the native path is taken wherever the CPU supports it.
  - The `word128` loom model (`--cfg io_loom`): two updaters and a reader find no torn or lost update. Mutation-checked: releasing the lock between the read and the write fails it.
  - Miri runs the lock path.
  - The aarch64 native path runs in the Linux VM (`.coord/linux.sh`, LSE present).
  - The engine's `clock.rs` unit tests, `clock_loom.rs` (unchanged: it models the protocol's single atomic step) and `tablets.rs` `d142_sweep`.
