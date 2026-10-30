### Changed
- A cross-shard commit takes one two-phase round instead of two (D263): its PREPAREs carry a provisional id, and the seqno and timestamp it applies at are picked when it is decided and held only until every participant applied it. Single-shard commits no longer wait for a cross-shard commit's PREPARE round to be visible (on the reference machine, single-shard p50 37 → 24 µs buffered and 193 → 104 µs at `GroupSync` with half the commits across shards, and +7–18% throughput on the cross-shard rows). While a cross-shard commit is undecided, a single-shard commit that writes one of its rows, or reads one of them in a transaction, waits for the decision, and another cross-shard commit on those rows is retried (D249), so each cell's versions stay in seqno order. Cross-shard p99.9 rises on contended workloads (about 1.5–2× on the reference machine's contended rows; p50 unchanged).
- The WAL has a new DECISION record (ICR 0046), part of file format 4. **0.2.x and 0.3.x cannot open a database this version has opened for writing:** they fail with `ErrorCode::UnsupportedFormat`. This version opens 0.1.x–0.3.x files and replays their WALs; opening one for writing upgrades it first.

### Added
- `CrossShardMetrics::share_waits` and `share_wait_nanos`: single-shard commits held back behind an undecided cross-shard commit's rows, and for how long (ICR 0046).

### Fixed
- A cross-shard commit refused behind an older one (D249) waited for ever if the older commit's retry was answered with an error at its own coordinator, whose WAL had failed or which was closing, before any further attempt: the retry now releases the commits waiting behind it. In 0.3.x this hit cross-shard transactions and `commit_if` after an I/O error (#776).
