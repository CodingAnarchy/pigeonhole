### Changed
- Point gets and row reads find the SSTs that may hold their row by a binary search over each disjoint level below 0, instead of checking every SST of the family, and an SST's first and last rows are parsed once when it is opened. Point reads no longer slow down as SST counts grow; a ycsb-c read costs 8.5% fewer instructions (#406).
