### Added
- `pigeonhole-bench --rate OPS`: offers a client run's measured operations at about OPS per second in all (each client paced to OPS / clients), so latency is the store's service time at that load rather than at its maximum (#759: read p99 at equal offered load). The JSON record carries `detail.offered_rate`.
