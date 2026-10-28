### Added
- `pigeonhole-bench --offered-rate R` now also paces client runs (before, only the placement check): a run's measured operations are offered at about R per second in all, each client paced to R / clients, so latency is the store's service time at that load rather than at its maximum (#759: read p99 at equal offered load). The JSON record carries `detail.offered_rate`.
