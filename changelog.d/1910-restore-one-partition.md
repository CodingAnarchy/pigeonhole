### Added
- `phdb cluster restore DIR --partition P [--to ID]`: puts one partition back from a cluster backup into the live cluster, even after it moved or was restored since the backup (#1832). Only that partition changes, at a new epoch with a new feed history, through a relocation's checked map change; the other partitions and the tables stay as the live map has them. Its writes since the backup are lost. A table whose key range on the partition changed since the backup refuses it.

### Fixed
- `phdb cluster restore`: a restored partition's new epoch now also passes every copy of it a node reports, so a file a failed move or relocation stamped past the map's epoch can't share the restored partition's epoch (#1832).
