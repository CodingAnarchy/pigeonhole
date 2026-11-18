### Added
- **Partition failover in a cluster** (D330 PR 2, #1484).
  - With the map's `failover` on, the writer promotes a down partition's freshest replica on its own, and records each failover and writer election in the map.
  - `phdb cluster status` shows the events, and why a down partition isn't failed over. `/metrics` adds `pigeonhole_cluster_failovers_total` and `pigeonhole_cluster_writer_term`.
  - `--cluster-failover-max-lag-ms` refuses a stale replica. A partition can opt out with its own `failover` field.

### Fixed
- **A cluster node's election check no longer delays its own lease renewals** while a peer accepts connections and never answers (#2056, unreleased).
- **A cluster node cut off from every other no longer stays fenced once it rejoins** (#2056, unreleased). It held a writer election it couldn't win, and its recorded vote then refused the live writer's maps. Elections now ask for a pre-vote first and record nothing unless a majority would vote.
