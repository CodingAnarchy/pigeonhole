# 0099: traversal pushdown, `Table::traverse(&KeySet)`

**Status:** Approved (number from the coordinator, 2026-10-31; owner accepted D304 at revision 2 on #489, with `max_nodes` defaulting to 10,000,000). PR 1 adds the sync traversal; PR 2 `run_async`.

## PR 1: the sync traversal
- **pigeonhole:**
  - `Table::traverse(&self, &KeySet) -> Traverse<'_>` and `ReadTable::traverse`.
  - `Traverse`:
    - the row-read selection (`family`, `families`, qualifier prefix, range and bounds, `latest`, `time_range`, `value_filter`, `snapshot`), applied to edges; exactly one family must be selected (`InvalidArgument` otherwise);
    - `hops(n)` (1 to 64, default 1), `per_node_limit(m)` (0: all), `hop_limit(n)` (0: all), `max_nodes(t)` (default 10,000,000, the seeds included), `parents(bool)` (default off), `parallel(n)` (default 1; a hop fans out only past 1,000 keys per thread);
    - `run(self) -> Result<Traversal>`. Empty seeds give no hops.
  - `Traversal` (`#[non_exhaustive]`): `hops(&self) -> &[Hop]`, `stats(&self) -> TraverseStats`.
  - `Hop` (`#[non_exhaustive]`): `nodes(&self) -> &KeySet` (sorted), `parents(&self) -> Option<&[u32]>` (each node's source, as an index in the previous hop's nodes or the seeds).
  - `TraverseStats` (`#[non_exhaustive]`, `Copy`): `rows`, `edges`, `hops_cut` (by `hop_limit` or `max_nodes`), `cut_by_max_nodes`.
- **Internal crates** (no promise):
  - **pigeonhole-engine:** `Engine::traverse(&Snapshot, TableId, Arc<KeySet>, &TraverseSpec) -> Result<Traversal>`; `TraverseSpec` (`new(family)`, with `hops`, `per_node_limit`, `hop_limit`, `max_nodes`, `read`, `parallelism`, `parallel_floor`), `Traversal`, `Hop`, `TraverseStats` (also `threads`), and the constants `MAX_HOPS`, `DEFAULT_MAX_NODES` and `TRAVERSE_PARALLEL_FLOOR`.

D304 rev 1 listed two more counts. **Not added:**
- **Unusable destinations:** every qualifier is a valid row key, since both are limited to 64 KiB (`MAX_KEY_PART`).
- **Rows cut by `per_node_limit`:** the visitor's counts don't come back from a bounded read.

## PR 2: async
- **pigeonhole** (behind the `async` feature): `Traverse::run_async(self) -> Result<Traversal>`, an `async fn` whose future is `Send`. It has `run`'s semantics and result, never fans out (`parallel(n)` is ignored), and a poll yields after 4,096 edges.
- **Internal crates** (no promise):
  - **pigeonhole-engine:** `Engine::traverse_async(&Snapshot, TableId, Arc<KeySet>, TraverseSpec) -> Result<PendingTraversal>` (a `Future<Output = Result<Traversal>>`), and `TRAVERSE_POLL_CELLS`.
  - Its `TraverseStats::{rows, edges}` count the work it did. They equal `run`'s when no hop was cut, and are lower when one was: a cut hop's sync read runs each tablet group to its own stop.

## Why
D304 (#489 item 3): a client BFS over `keys()` materialises every edge and sees every duplicate. In the engine, a hop keeps only new destinations, and a capped hub costs its first edges.

## Callers changed
None: additive. The pigeonhole model target runs a two-hop traversal from each multi-get's keys.
