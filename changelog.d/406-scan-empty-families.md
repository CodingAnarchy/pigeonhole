### Changed
- A scan sets up nothing for families with no data in a tablet's range, and stops at its row limit without reading the next row. A 10-row scan of a table where 3 of 4 families are empty costs 9% fewer instructions (#406).
