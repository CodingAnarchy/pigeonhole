# pigeonhole-memory

Long-term memory for agents on [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole): memories with text, an embedding from a pinned model, a kind, an importance, sources and typed links, in exact namespaces, written in transactions with exact dedup (D311, ICR 0107).

**Status:** D311 (#1335–#1343):
- **The store:** create and open with the pinned embedding model, `remember` with exact and near-duplicate dedup, `get`, `delete`, `link`, `supersede`, `mark_used` and `verify`.
- **Recall:** vector, keyword and recency, fused by RRF within a byte budget, with graph expansion along links, in reader processes too. Optional IVF recall for large namespaces.
- **Lifecycle:** forgetting, and consolidation through a `Consolidator` the application supplies.
- **Events:** memory events from the change feed (`Watch`), and a feed-driven `ConsolidateLoop`.

See the [guide page](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/agent-memory.md). It follows the public 1.x version line, with semver from its first release.
