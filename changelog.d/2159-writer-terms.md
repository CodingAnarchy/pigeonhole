### Fixed
- The cluster map writer's election and map ordering (#2159):
  - A node's election vote now waits for a map install already past its term check. Before, a deposed writer's last map could still be mid-install on a voter when it voted, so the new writer could build on that voter's older map, and the voter granted leases under the deposed writer's map until the new one arrived.
  - An elected writer takes office only with a majority of the newest map's nodes as well as of its own map's. If the newest map a voter holds adds nodes, the candidate asks those nodes for the same vote. Before, a candidate holding an older map could take office on a majority of that map's nodes alone.
  - A lease round's leases apply only if the node still holds the round's exact map: the same writer term as well as the same version. Before, the version alone was compared, and a deposed writer's map and the elected writer's can share one.
  - At start, and when it pulls a map from a peer, a node takes a map with a higher writer term even when its version is lower. Before, a node holding a deposed writer's map at a higher version skipped the elected map until the next lease round.
