### Changed
- Scan readahead ramps up (#712). A scan cursor reads ahead only after it has moved through two blocks past its seek, 2 blocks first, then 4, 8 and 16. Before, the first block-cache miss fetched 16 blocks. Short scans, which read a few rows past their seek, no longer fetch, verify and decompress blocks they never reach.
