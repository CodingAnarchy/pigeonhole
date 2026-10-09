# 0016: spin windows before parking: `Options::commit_spin`, `Options::shard_spin`, and their engine and runtime fields (D198)

**Status:** Approved (coordinator, 2026-10-09), implementing D198 (approved by the owner). Everything is additive.

## Change

1. **`pigeonhole`:** `Options::commit_spin(Duration)` (default 15 µs) and `Options::shard_spin(Duration)` (default 50 µs). Each is documented, and `Duration::ZERO` turns it off, as D198 requires for battery-powered or CPU-constrained hosts.
2. **`pigeonhole-engine`:**
   - `EngineOptions::commit_spin_nanos` and `shard_spin_nanos`, with the same defaults.
   - `PendingCommit::wait` polls a buffered or non-durable commit's result for up to the client window before it parks. A durable commit parks at once.
3. **`pigeonhole-runtime`:** `RuntimeConfig::idle_spin` (default zero). An engine-owned shard thread that handled a message polls its queue for up to that long before it parks. An idle shard never polls.

Both sides back off: a poll that finds nothing skips the next 1, 2, 4, … up to 32 polls.

## Why

D198: a buffered commit's latency is mostly the thread handoff (the commitpath tables in D198). The owner decided the defaults on these conditions:
- on once measured;
- the idle-CPU test still passes;
- both windows are documented options that can be set to 0.

## Callers

- `pigeonhole` (options), `pigeonhole-engine` (wait and options), `pigeonhole-runtime` (the shard loop).
- The bench's measured write shapes set `commit_spin(ZERO)`: the number of polls depends on thread timing, which would make the instruction counts nondeterministic.
- `commitpath` reports the engine's default spin (`threads`) against none (`threads-nospin`).
