# 0065: `File::holds`, `sys::process_id`, and the locked reader-slot calls

**Status:** Number assigned by the coordinator (2026-10-22; #693 PR 3, #898). Additive. Comes from perf287's review of #898 (S1 and S2); the lock-based liveness itself is #693 PR 3.

## Change

**`pigeonhole-io`:**
- **`File::holds(&self, byte: u64) -> Option<LockMode>`:** the lock this handle holds on `byte`, if any.
  - It's a provided method whose default reports `None`, so existing implementations still compile.
  - `PreadFile` reads its per-handle `held` map, `SimFile` its node's holders, and `UringFile` forwards.
  - A wrapper that forwards `lock` must forward `holds` too. Every wrapper in the workspace's tests, benches and examples does.
- **`sys::process_id() -> u32`:** this process's id, as `std::process::id()`, but cached after the first call.
  - **Unix:** a `pthread_atfork` child handler resets the cache (one atomic store), so a forked child never sees its parent's id. If the handler can't be registered, the id is never cached.
  - **Windows** (no `fork`): a `OnceLock`.

**`pigeonhole-shm`:**
- **`ShmRegion::claim_reader_slot_locked(&self, process: ProcessId, file: &FileRef) -> Result<ReaderSlot>`:** claims a slot and takes its liveness byte (`READER_LOCK_BASE + index`, FORMAT §8.3) through `file`.
  - The byte is taken before the `free → claiming` CAS and held until the slot is freed.
  - A byte another handle holds, or one `file` already holds, means the slot is taken.
- **`ShmRegion::reclaim_dead_slots_locked(&self, file: &FileRef) -> usize`:** frees the slots whose liveness byte `file` can take: the reader is gone. Slots stuck in `claiming` are included.
- **`ReaderSlot::abandon(&self)`:** gives a slot up without freeing it. It releases the byte now; from then on the handle stores nothing, its drop does nothing, and `is_owned` is false.
- **Behaviour change, no signature change:** a `ReaderSlot` or `SlotOwnership` used in a process other than the one that claimed it (a forked child) is inert, as if abandoned.

`claim_reader_slot` and `reclaim_dead_slots` (liveness by pid) stay, for in-memory regions and tests. The engine no longer uses them.

## Why

**`holds` (S2):**
- Byte-range locks never conflict within one handle (OFD on Linux, `pread`'s registry on macOS and BSD). So a claim through a handle that already holds a live slot's byte would lock it, fail its CAS, and then *unlock* it: the live slot loses its lock, and the writer frees it under a live reader.
- Asking the handle first makes such a byte "taken", so a missed `ReaderSlot::abandon` costs a slot, never a live reader's lock.

**`process_id` (S1):**
- A forked child inherits a copy of a reader's slot handle, and on Linux the same open file description, so the slot's OFD lock too.
- If the child dropped its copy, it would free the parent's slot and release the shared lock. So the slot handle compares the current process with the one that claimed it, on every pin store.
- `std::process::id()` is a system call on Linux, and pin stores run on every snapshot in reader processes, so the id is cached. `owner.pid` can't serve: in the simulator it's a simulated id.

**The locked shm calls:** #693 PR 3's liveness protocol (FORMAT §11.5), which replaces the pid probe so that a live reader in another PID namespace is never judged dead.

## Callers

- `pigeonhole-io`: `file.rs` (the trait method), `pread.rs`, `uring.rs`, `sim.rs`, `sys.rs`, `os/unix.rs`, `os/windows.rs`.
- `pigeonhole-shm`: `region.rs` (`claim`, `reclaim_dead_slots_locked`, `ReaderSlot::inert` and `abandon`), `lib.rs` (`SlotOwnership::is_current`); `tests/sim.rs`, `tests/multiprocess.rs` (fork test).
- `pigeonhole-engine`: `engine.rs` (`claim_slot`, which debug-asserts the handle holds no slot byte; reader open, reattach and re-claim), `shard.rs` (`Shared::reclaim_dead_readers`), `snapshot.rs` (`ReaderPins::abandon`).
- `File` wrappers that forward `holds`: tests in `pigeonhole-engine`, `pigeonhole`, `pigeonhole-pager` and `pigeonhole-wal`, and `crates/bench/examples`.
- Docs: FORMAT §8.3 and §11.5, concurrency.md, the readers guide, and the changelog.
