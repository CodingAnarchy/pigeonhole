### Changed
- Reads pass a finished column's remaining SST entries by reading only their block entry headers, instead of stepping each one through the merge and the resolver (ICR 0041). Scans use about 1.7–2.4% fewer instructions on the bench's scan shapes. Results are unchanged.
