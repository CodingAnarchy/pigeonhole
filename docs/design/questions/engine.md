# Engine open questions

## Proposed decision: what `shrink` reports and what it commits (amends D60; issue #138)
D60 said `relocate` fails with `NoSpace` when no free extent of that size lies below the extent. The public docs turned that into "`shrink` fails with `NoSpace` when there is no free extent below". In practice `shrink` swallowed that error and returned `Ok(0)`. It also stopped before truncating when nothing needed to move, so a file whose tail was entirely free (every row deleted and compacted away) never shrank.

**Interim behavior:**
- Every round first reclaims and truncates the free tail, then relocates. A `shrink` call returns the bytes the file actually shrank by.
- An extent with no free extent of its class below it is skipped, not an error. Smaller extents past it still move. The file then ends after it. That is the allocator's floor (live extents packed, power-of-two aligned; follow-up #185), and the docs say so.
- `NoSpace` from `shrink` now means only that the disk filled while the manifest snapshot was rewritten to move the manifest extents. The batch is refused, nothing is lost, and the writer stays usable.
- The moves are committed as a `ReqKind::Catalog` change, computed against the catalog at commit time. A copy replaces its SST at every `(tablet, family)` and level that reference it *now*: a trivial move keeps its new level, and a split keeps both references. A copy of an SST that a compaction or `drop_table` removed meanwhile is abandoned. `relocate` failing because the extent was retired since the catalog read is a skip, and the next round plans again. A copy that is never committed (the request was dropped unrun) is abandoned too.
