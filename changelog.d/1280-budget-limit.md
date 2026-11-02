### Changed
- `Scan::read_within` with `limit` stops the read at whichever of the byte budget and the row limit comes first, inside the engine, so a small limit reads no more than its rows (#1280). A position from another table is now refused with `limit(0)` too.
