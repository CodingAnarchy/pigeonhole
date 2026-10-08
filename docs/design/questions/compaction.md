# Questions: compaction

## Proposed decision: counter-family deletes purge at the bottom by seqno (#290)
D70 purges a bottommost delete only below `GcPolicy::min_ts_above`. Every source that holds a fixed-timestamp counter (D179) has minimum timestamp 0, so in a family that uses `incr` the bound stays 0 and no counter tombstone ever purged.

A counter delete hides only entries with a lower seqno in its scope. So a bottommost delete visible at every read point (stripe 0) is also purged when no other source that may hold keys of its row (`GcPolicy::other_sources`, filtered by key range per row) starts at or below its seqno. Nothing outside the inputs is then old enough for it to hide, and what it hides in the inputs is dropped with it (same stripe, lower seqno). Sources the engine leaves out of `other_sources` start above the newest input seqno, so they never block a purge. Later writes take seqnos above `visible`, and prepared shares at or below it are listed. When `other_sources` is `None`, only the timestamp rule applies.

**Interim behavior:** implemented in `gc.rs` (`counter_purgeable`) for cell and column deletes and family markers. Reads never change, so `Model::purge` stays a no-op for counter families. Tests: `counter_deletes_purge_at_the_bottom_by_seqno`, and the proptest `counter_purges_preserve_reads_with_other_sources`, which splits rows between the inputs and another source with interleaved seqnos. It catches both a purge that ignores the other sources and operand combining that ignores them.
