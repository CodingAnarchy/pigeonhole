### Changed
- A scan reads each cell once to append it to the row it returns, instead of two or three times, and qualifiers are unescaped a run at a time instead of a byte at a time (#320, #387). `pigeonhole-engine` adds `ScanCursor::push_next_cell`.
