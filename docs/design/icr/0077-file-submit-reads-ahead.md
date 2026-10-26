# 0077: `File::submit_reads_ahead`, a batch of reads nothing waits on yet

**Status:** Approved (coordinator, 2026-10-26; #974 B, after perf287's review). Additive: a new provided method on `pigeonhole_io::File`. No existing method changes, and no implementor has to change.

## Change

**`pigeonhole-io`:** a provided method on the `File` trait:

```rust
/// Submits reads that nothing waits on yet (a scan's readahead) as one batch, each
/// resolving as `submit_read` would. A backend may hand the whole batch to the kernel at
/// once and run each read off the submitting thread. The default submits each in turn.
fn submit_reads_ahead(&self, reads: Vec<(IoBuf, u64)>) -> Vec<Completion> {
    reads.into_iter().map(|(buf, offset)| self.submit_read(buf, offset)).collect()
}
```

- **The io_uring backend overrides it.** It inserts every read, pushes all their entries under the ring's `sq` mutex with `IOSQE_ASYNC`, releases `sq`, then makes one `io_uring_enter`.
  - A page-cache hit is then copied by an io-wq worker, not inline under the kernel's `uring_lock` in the submitter's call.
  - On a thread with its own ring, the batch goes to that ring with one enter but **without** `IOSQE_ASYNC`: the owner is its ring's only submitter, so there is no `uring_lock` queue to leave, and that thread's io-wq is not capped (amended 2026-10-26 after perf287's review of #977; implementation detail, interface unchanged).
  - On an inline thread (application-owned mode, no ring), each read runs inline, as `submit_read` does.
- **The pread and sim backends keep the default.** Under the simulator, a batch is exactly the reads in turn, so seeds replay unchanged.
- **Ring setup caps io-wq** (`IORING_REGISTER_IOWQ_MAX_WORKERS`, bounded only) on the shared ring.
  - In 6.8 the registering thread's io-wq takes the cap at once, and so does every other task whose first io_uring submit goes to this ring (its task context is created then, with the ring's limits: `__io_uring_add_tctx_node`).
  - A thread that submitted to another ring first keeps that ring's limits, or none. That's rare for client threads, since only shard and pool threads own rings.
  - The cap is ring setup, not interface.

## Why

From blob33's #712 profile (ycsb-e, 16 clients, io_uring 2.4× slower than pread):
- 70% of client switches are in `Ring::push`. It holds `sq` across one `io_uring_enter` per readahead block, and the kernel copies each page-cache hit inline under `ctx->uring_lock`.
- The design is in #974; perf287 reviewed it.
- **Why per op and not per ring:** an async get's block fetch is waited on by its task, so an io-wq hop would only add a wake (perf287). Readahead says so through this method, and every other read keeps `submit_read`.

## Callers

- `pigeonhole-sst`'s `SstIter::read_ahead`, the only caller: it submits one batch per readahead (single blocks and merged runs) through a `Blocks` helper that keeps the direct-I/O alignment `submit_bytes` applies.
- **Implementors** (workspace): `UringFile` overrides it. `PreadFile`, `SimFile` and every wrapping test or bench `File` (engine, pigeonhole, wal, pager tests; bench examples) keep the default, which calls their own `submit_read`.
- `cargo semver-checks` sees an added provided trait method (minor).
