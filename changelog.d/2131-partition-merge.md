### Added
- **Partition merges** (D331, #1499): `phdb cluster merge S --into T` moves each of `S`'s ranges into `T` under writes, as `move-range` moves a range, then removes `S` from the map and moves its files aside on its nodes. A hand-over that leaves two neighbouring ranges of a table on one partition joins them. Rerun the command to go on after a stop.
  - An emptied `S` stays listed while something still needs it (its change feed's tail, its transactions' outcomes, open spanning transactions homed on it, shares in doubt); `phdb cluster status` reports it as `drop_waits`, and the merge ends saying what it waits for.
  - `drop-partition` is refused on a node the map lists as one of the partition's replicas, except for the copy an unfinished promote left.
