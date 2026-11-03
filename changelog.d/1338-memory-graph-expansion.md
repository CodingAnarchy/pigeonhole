### Added
- `pigeonhole-memory`: graph expansion in recall (D311 T4, #1338).
  - `Recall::expand(Expand::hops(n).max(m))` follows the best candidates' links in both directions through the engine's `Traverse`.
  - The memories it reaches get a graph signal (`Signals::graph`, `Weights::graph`, default 0.5).
  - The walk is bounded by `max`, and by the recall's byte budget, a hop at a time. `RecallStats::expanded` and `expansion_truncated` report it.
- `pigeonhole-memory`: a recall hit with `with_signals` now counts six signals (72 bytes) in the byte budget.
