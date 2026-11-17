### Added
- `phdb cluster promote P --to ID [--old-unreachable]`: makes node ID, which holds a replica of partition P, its owner (D328, #1851). The replica's copy becomes a primary at the last commit it applied and takes the next epoch, with a new change-feed history. The report gives the commits lost, or says the loss is unknown when the old owner couldn't be asked. Run it again to finish one that stopped.

### Changed
- `phdb cluster move`, `relocate` and `restore --partition` onto a node that holds the partition's replica now take that node out of the partition's replicas and close its copy first, rather than refusing. `Admin.Promote` on a cluster node now names `phdb cluster promote`.
