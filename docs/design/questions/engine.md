# Engine questions

## Q: How should a commit cross from the client's thread to its shard's and back? Bounded spinning before parking, or combining (C, #64; the combining option needs the owner)
**Status:** a proposal for review. There is no code until it is approved.

**Why.** A buffered commit's latency is mostly the thread handoff, not the work. `crates/bench/examples/commitpath.rs` runs the same commits under several shard drivers: buffered one-field overwrites (ycsb-a's write), one client, one shard.

| how the shard runs | macOS p50 / p99 µs | Linux runner, run 1, p50 / p99 µs | Linux runner, run 2, p50 / p99 µs |
|---|--:|--:|--:|
| engine shard thread (today) | 4.96 / 10.46 | 33.02 / 45.77 | 25.05 / 52.32 |
| application thread that parks (the same wakeups) | 5.92 / 10.79 | 31.87 / 46.10 | 24.49 / 50.62 |
| shard spins, client parks | | | 13.00 / 25.48 |
| shard parks, client spins | | | 7.70 / 16.34 |
| both spin | 3.67 / 7.92 | 3.71 / 7.64 | 3.36 / 8.52 |
| inline: the client runs the shard right after submitting | 1.29 / 3.21 | 3.11 / 7.19 | 1.70 / 4.10 |

Where the macOS figures were measured:
- an Apple M5, not quiet, so indicative only;
- the Linux runner is a GitHub `ubuntu-latest` VM (`commit-latency.yml`).

The CPU work is the `inline` row: about 1.3–3 µs, and about 10.6K instructions per commit on callgrind (shard about 7.4K, client about 3.3K). Everything above it is the handoff:
- **Crossing threads with nobody sleeping** costs about 2–3 µs.
- **The wakeups cost the rest:** about 2 µs on macOS, and 20–30 µs on the Linux VM, about 90% of a buffered commit there.
- **Both sides' wakes matter.** On Linux run 2, spinning only the client saves 17 µs and spinning only the shard 11.5 µs; the savings overlap, so they do not add up.

RocksDB's buffered write runs on the caller's thread, with a write-group leader, which is the main reason `ycsb-a` and `ycsb-f` p50 sit at 2.2–2.5× RocksDB's in the Phase 3 baseline (`docs/bench.md`).

**Option C1: bounded, adaptive spinning before parking** (within the current spec)
- **Client (`PendingCommit::wait`):** after submitting, spin-poll its completion for up to `S_c`, about 10–20 µs, before parking. This is RocksDB's write-thread pattern: a short spin, then yields, then block.
- **Shard (engine-owned shard threads only):** when its queue drains, spin-poll the queue for up to `S_s` before parking, with the same adaptive shape. A pinned shard thread owns its core anyway (spec, "Thread-per-core execution"), so this spinning costs power, not another thread's CPU.
- **Adaptive:**
  - each side keeps a short history of how long its waits actually took, and skips the spin when waits are usually long (a durable commit waiting for an fsync, a stalled shard);
  - a spinning wait yields between polls, so an oversubscribed machine does not starve.
- **Expected effect:** from the table, buffered commit p50 drops from 25–33 µs to about 3–4 µs on the Linux runner, and from about 5 to about 3.7 µs on macOS, with p99 about halved.
- **Costs:**
  - CPU burned while waiting: up to `S_c` per commit on the client, and up to `S_s` per idle period on the shard;
  - no help for durable commits (sync time dominates), nor for an application-owned shard (the application's loop decides how it waits).
- **Off switch:** the spin limits are `EngineOptions` fields, with zero meaning today's behavior.

**Option C2: combining, where the committing thread runs its shard** (changes the threading model, so it needs the owner)
- **What it does:** a committing thread that finds its shard idle (a try-lock on the shard's driver) runs that shard's loop itself until its commit resolves, then hands the shard back. The engine's shard thread does the same when it wakes: whoever holds the driver runs the loop. This is RocksDB's write-group leader in Pigeonhole's terms.
- **Expected effect:** buffered commit p50 at about the `inline` row: 1.3–3 µs.
- **What it changes:** the spec says every write executes on exactly one pinned shard thread that owns its data outright, so it has no locks and no cross-core traffic. Under C2:
  - the shard's state moves between threads, and its cache lines with it;
  - the driver becomes a lock, uncontended in the common case;
  - shard-local work (flush, compaction steps, timers) may run on an application thread;
  - D88's refusal of blocking calls on a shard-driving thread, and the application-owned mode's contract, need re-reading;
  - reads are unaffected.
- **Risks:** priority inversion (an application thread preempted while it holds a shard), and NUMA placement (#405 hardware). The whole commit protocol also needs re-verification: group formation, the WAL group sync, visibility (D19), cross-shard commits.
- **Off switch:** an engine option, off until measured.

**Recommendation.**
1. Build C1 first, behind its options and off until measured. It stays within the spec and the table says it recovers most of the gap.
2. Measure C1 with `commitpath`, the instruction shapes (the CPU cost of a spin that finds work at once should be near zero), and the Phase 3 bench on #405.
3. Bring C2 to the owner only if C1's residual handoff (the `both spin` row against `inline`: about 1.5–2.5 µs) still keeps a write workload above the 1.5× RocksDB gate.

**For the owner:**
- **C2 now or later?** Is combining acceptable in principle, as a later step if C1 is not enough? It changes the spec's thread-per-core statement for writes.
- **Defaults for C1:** I propose on by default for engine-owned shards once measured, with about 15 µs for the client and about 50 µs for the shard, tuned on #405.

**Interim behavior:** the client parks on its commit, and the shard thread parks when its queue drains. That is today's behavior, with no spinning.
