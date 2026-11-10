### Added
- **`phdb serve --affinity on|off`** (default `on`, ICR 0131): `off` keeps the same network, feed and blocking threads but no longer masks them off the shards' cores, for a host shared with other work. The shards stay pinned.
