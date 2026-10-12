### Changed
- A scan reads each cell once to append it to the row it returns, instead of two or three times (#320, #387). `pigeonhole-engine` adds `ScanCursor::push_next_cell`.
