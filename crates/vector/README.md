# pigeonhole-vector

An IVF (inverted-file) vector index for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole), built on its public API only (decision D306, #490 D).

- **Layout:** each list is one wide row of a Pigeonhole vector family, with one column per vector, and a query is one multi-get with a top-k nearest search over the `nprobe` lists nearest the query.
- **Rebalancing:** lists split and merge incrementally, inside the inserts and deletes that cause them. A `Rebalancer` the application drives can take that work instead. Every move is a transaction.
- **Target:** 95% recall@10 on SIFT1M at 10 ms or less, single-threaded.

**Status:** the index, bulk loading, inline or background rebalancing, and async search (D306). On SIFT1M it reaches 95.8% recall@10 at 3.4 ms per query, single-threaded. See the [vectors guide](../../docs/guide/vectors.md). It follows the public 1.x version line, with semver from its first release.
