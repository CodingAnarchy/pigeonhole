# 0148: counting transactions across partitions' external shares

**Status:** Approved (coordinator, 2026-11-27; #2221, D329).

## Change

Additive, following ICR 0144's pattern: engine `Metrics` fields, public `EngineMetrics` fields, `pigeonhole-otel` names, and two `phdb serve` gauges.

**`pigeonhole-engine`:**

```rust
pub struct Metrics {
    // ...
    /// Rounds of external shares (D329) prepared and logged here, each in doubt until decided.
    pub external_prepares: u64,
    /// Decides taken on those rounds, `(commit, abort)`: `Engine::decide`, so the resolver's
    /// and an operator's `txn decide` are included.
    pub external_decides: (u64, u64),
    /// Change-feed readers' external shares, `(held, evicted)`: shares a reader held from
    /// their PREPARE until it read their decision (one read again after a rewind counts
    /// again), and held shares evicted past a capped reader's cutoff (#2185).
    pub feed_external: (u64, u64),
}
```

The coordinating shard counts the prepares (when the round's xid enters the in-doubt registry) and the decides (in `on_external_decide`, once the round is found awaiting its decision). Each one is a `ShardMetrics` field with one writer, its shard thread. The feed counters are `Shared` fields. Any reader's thread adds to them with `fetch_add`. All five are Relaxed statistics, listed in concurrency.md.

**`pigeonhole` (`EngineMetrics`, `#[non_exhaustive]`):** `external_prepares`, `external_commits`, `external_aborts`, `feed_external_held` and `feed_external_evicted`, all `u64`.

**`pigeonhole-otel`**, and so `/metrics` on `phdb serve`, carried across automatic recoveries like the engine's other counters:

| Name | Prometheus | Attributes |
|---|---|---|
| `pigeonhole.txn.external.prepares` | `pigeonhole_txn_external_prepares_total` | |
| `pigeonhole.txn.external.decides` | `pigeonhole_txn_external_decides_total` | `pigeonhole.outcome` = `committed` / `aborted` |
| `pigeonhole.feed.external.holds` | `pigeonhole_feed_external_holds_total` | only while the feed is on |
| `pigeonhole.feed.external.evictions` | `pigeonhole_feed_external_evictions_total` | only while the feed is on |

**`phdb serve`:** two gauges, on cluster nodes only, one sample per partition:
- `pigeonhole_cluster_txn_in_doubt_shares{partition}`;
- `pigeonhole_cluster_txn_in_doubt_wal_bytes{partition}`.

They read `Status`'s source, the engine's in-doubt registry, under its lock for an instant, on each scrape.

## Why

#2221: nothing showed whether a run reached the external-share paths. That means the prepares, shares held in doubt, the feed's `Held::external` and #2185's eviction, and the resolver's decides. `phdb cluster status` lists the shares in doubt at a moment. Nothing counted them over a run. A benchmark that drives spanning transactions needs that to show its load reached the paths it is meant to measure. D335 and D336's benchmarks use the same load.

## Semantics

- The counts start at 0 at each open. Shares found in doubt at an open are neither prepared nor counted again, but their later decides are counted. So `external_prepares - external_commits - external_aborts` is the shares in doubt now only when none were found in doubt at the open.
- A decide that finds its xid already decided (`None`) counts nothing.
- A feed reader's hold counts each time a share is held. A rewind (#2185's eviction, a resume from a token) reads it again and counts it again.
- On a single node with no spanning transactions, every count is 0 and the gauges aren't shown.

## Callers

- `phdb serve`'s `/metrics` and `Admin.Metrics` (through otel). The serve guide and the observability guide's metrics table list them.
- `crates/cli/examples/serve_span_load.rs` (#2221 PR 2) reads them for its functional check.

## Open questions
_None._
