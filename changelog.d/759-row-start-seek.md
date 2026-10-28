### Changed
- A row read finds its row inside a data block with one binary search over the block's row starts, instead of a binary search over its restart points followed by a walk over up to 15 cells. New SSTs store the full key at every row start (0.6–0.8% more uncompressed data on YCSB-shaped rows); SSTs written before read as before, and every 1.x build reads the new ones (ICR XXXX, #759).
