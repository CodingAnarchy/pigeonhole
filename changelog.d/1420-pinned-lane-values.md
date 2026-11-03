### Changed
- Scans and nearest-neighbour reads no longer copy each plain value of 256 B to 4 KiB out of its block or memtable before returning it: the value is pinned where it lies (#1283). Results are unchanged; point gets and row reads still copy.
