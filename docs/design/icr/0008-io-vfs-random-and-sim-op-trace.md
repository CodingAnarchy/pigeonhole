# 0008: `pigeonhole-io` `Vfs::random_u64` and `SimVfs` operation traces

**Status:** Approved (coordinator, 2026-10-07; issue #61). Implemented in the PR that closes #61.

## Change

```rust
pub trait Vfs {
    // ...existing methods...

    /// A random 64-bit value, for identifiers that must be unique (a new database's id).
    fn random_u64(&self) -> u64 { /* RandomState keys + both clocks + pid */ }
}

// pigeonhole_io::sim
pub enum SimOp {
    Write { node: u64, offset: u64, len: u64, digest: u64 },
    SetLen { node: u64, len: u64 },
    Sync { node: u64, metadata: bool },
    Remove(PathBuf),
    SyncDir(PathBuf),
}

impl SimVfs {
    /// Starts recording every mutating operation.
    pub fn record_ops(&self);
    /// The operations recorded so far, in completion order.
    pub fn recorded_ops(&self) -> Vec<SimOp>;
}
```

Semantics:
- **Default `random_u64`:** mixes the standard library's per-process random hash keys (`RandomState`, seeded from the OS) with both clocks and the process id. It is not cryptographic. Callers can expect values that are unique across databases and processes, good enough to identify them. They must not rely on the values being unpredictable to an adversary.
- **`SimVfs::random_u64`:** derived from the seed and a draw counter of its own, so a seed replays the same values. The counter is separate from the fault generator, so asking for a value never shifts the fault decisions a seed makes.
- **Traces:** off until `record_ops`. Each completed mutating operation (the ones `FaultPlan::crash_after_ops` counts) is recorded with its file's node number and a digest of the bytes written, so two runs that did the same I/O in the same order record equal traces.

## Why

Issue #61 asks that a seed replay the same `SimVfs` operation trace on every run, background work included. The first operation of every run, the new database file's superblock, held a database id drawn from `RandomState`, because the `Vfs` had no source of randomness the simulator could seed. No test could observe a trace either.

## Callers

- `crates/io`: `vfs.rs` (the defaulted method), `sim.rs` (the override, `SimOp`, recording). Every other `Vfs` implementation is unaffected: `PreadVfs` and the test wrappers in `crates/wal/tests`, `crates/pager/tests` and `crates/engine/tests` keep the default.
- `crates/pager/src/lib.rs`: `random_db_id` uses `Vfs::random_u64` in place of its own `RandomState` mix.
- `crates/engine/tests`: the model-check harness (`run_traced`) and `a_seed_replays_the_same_io_trace` read the trace.
