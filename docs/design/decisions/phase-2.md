# Decisions made in Phase 2 (D163–)

Indexed in [README.md](README.md). Numbers are permanent and continue from Phase 1; code and docs cite them as `Dn`.

<a id="d163"></a>
## D163 — Fairness rules for the Phase 2 benchmark against SQLite EAV and hand-keyed stores (approved; bench, #54, #220)
The Phase 2 gate compares Pigeonhole's sparse-wide workload with SQLite EAV and hand-keyed RocksDB/fjall. Four questions from adding timestamped puts and family reads:

### Q: Do the key-value runners pay for versions the way Pigeonhole does?
Phase 2's gate is "sparse-wide beats SQLite EAV and hand-keyed RocksDB". Pigeonhole's
families keep `max_versions(1)`, and every runner overwrites a cell in place, so no workload
reads or retains more than the latest version. A hand-keyed RocksDB or fjall store that
supported versions would put an inverted timestamp in the key and scan to the newest, which
costs more than the overwrite the runners do now; SQLite EAV would add `ts` to the primary
key. Comparing a versioned Pigeonhole feature against engines that do not offer it is fair
only while nobody reads old versions.

**Interim behavior:** all engines keep the latest version only. Cells carry their timestamp
(Pigeonhole natively, the others as an 8-byte value prefix or a `ts` column), so the TTL work
is comparable. A versions workload needs a `BenchOp` that reads `n` versions and a keyed
layout in each comparison runner; defer until a Phase 2 gate names one.

### Q: Is a read-time TTL filter a fair stand-in for Pigeonhole's compaction-time expiry?
Pigeonhole drops expired cells during compaction and filters them on read. The comparison
runners only filter on read and never delete an expired cell, so their stores grow and their
scans step over dead cells, which Pigeonhole's compactions eventually remove. RocksDB has a
TTL compaction filter and fjall has none; a hand-written layout would want one. The first
effect favors Pigeonhole on space and on long runs; the second favors the others on write
cost.

**Interim behavior:** read-time filtering only, in every engine, so all of them return the
same cells (checked by the agreement tests). Compare store size and scan latency of
`time-series-ttl` with that in mind. A RocksDB compaction filter is the first thing to add if
the numbers look lopsided.

### Q: Event times and the wall clock
TTL is judged against the wall clock when a read runs, but the generator is deterministic
from the seed. `WorkloadConfig::epoch_micros` (0: wall clock at `Workload::new`) anchors event
times; loaded points sit at least about 9.6 minutes from the expiry boundary on either side,
so engines agree on which are live unless a run lasts that long between workload creation and
a read. A quarter of the loaded points are expired.

**Interim behavior:** as above. Runs where load plus measurement exceed ten minutes (`full`
scale on a slow disk) can see engines disagree at the boundary; the report does not detect
that. Consider re-anchoring `epoch_micros` after the load phase if it matters.

### Q: FIFO-by-time compaction
The issue asks to add it "once Phase 2 ships it". `pigeonhole::Compaction::FifoByTime` is in
the public API, but the compaction picker for it is still a stub (`picker.rs` ignores `now`
and the TTL), so selecting it would change nothing the bench could measure.

**Interim behavior:** the `metric` family uses the default leveled compaction. Switch the
runner to `FifoByTime` when the picker lands; the hand-written engines have no equivalent
(RocksDB has `FIFO` compaction with a TTL, which is the fair counterpart to add then).

**Coordinator:** confirmed, all four as interim:
1. **Versions:** every engine keeps the latest version only; the sparse-wide gate workload never reads old versions, so the comparison is fair. Versions are validated by Pigeonhole's own correctness tests, not by the gate bench.
2. **TTL:** read-time filtering in every engine, so all return the same cells; report store size next to `time-series-ttl` numbers. If they look lopsided, add a RocksDB TTL compaction filter first.
3. **Event times:** as described; re-anchoring after the load phase is #222, to land before the gate benchmark runs.
4. **FIFO-by-time:** leveled until the picker lands (#32); then switch the `metric` family and give RocksDB its FIFO-with-TTL compaction as the counterpart.
