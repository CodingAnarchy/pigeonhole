### Changed
- When a filtered scan's seek or row skip passes the end of the current SST data block, the cursor walks the index forward to the target block instead of re-seeking from the top of the index, skipping the blocks in between without reading them and keeping readahead (#29).
