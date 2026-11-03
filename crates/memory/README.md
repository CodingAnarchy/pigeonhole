# pigeonhole-memory

Long-term memory for agents on [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole): memories with text, an embedding from a pinned model, a kind, an importance, sources and typed links, in exact namespaces, written in transactions with exact dedup (D311, ICR 0107).

**Status:** D311's T1 (#1335):
- **The store:** create and open with the pinned embedding model, `remember` with dedup, `get`, `delete`, `link`, `supersede`, `mark_used` and `verify`.
- **Recall:** vector, keyword and recency, fused by RRF within a byte budget, in reader processes too.

See the [guide page](../../docs/guide/agent-memory.md). Still to come: forgetting, consolidation and graph expansion. It follows the public 1.x version line, with semver from its first release.
