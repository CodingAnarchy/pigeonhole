### Changed
- Filtered scans and multi-version reads decode keys with fewer instructions: decoding a whole key finds the row's terminator eight bytes at a time instead of one (#1167). Results are unchanged, and so is the read path for unfiltered rows and scans. On Linux callgrind, the filtered-scan shapes take 0.6–1.6% fewer instructions per cell and `versions` 0.9% fewer.
