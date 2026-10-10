### Changed
- A scan skips a family with no memtable or SST in a tablet before it takes a resolver or resolves options for it. A 10-row scan of a table where 3 of 4 families are empty costs another 10% fewer instructions (#406).
