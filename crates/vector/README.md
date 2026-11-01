# pigeonhole-vector

An IVF (inverted-file) vector index for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole), built on its public API only (decision D306, #490 D).

- **Layout:** each list is one wide row of a Pigeonhole vector family, with one column per vector, and a query is one multi-get with a top-k nearest search over the `nprobe` lists nearest the query.
- **Rebalancing:** lists split and merge incrementally, inside the inserts and deletes that cause them. A `Rebalancer` the application drives can take that work instead. Every move is a transaction.
- **Target:** 95% recall@10 on SIFT1M at 10 ms or less, single-threaded.

**Status:** the index, bulk loading, and inline or background rebalancing (D306 PRs 1 and 2). The measured defaults and a guide come next. It follows the public 1.x version line, with semver from its first release.
