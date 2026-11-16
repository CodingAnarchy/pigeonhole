### Fixed
- Reads: a multi-get (`Table::keys`) with a qualifier filter now ends each key at its row when the filter keeps nothing there. Before, the read skipped past such a row by seeking to the next one, and then the one after, through the rest of each file it read, as row reads did before #1924 (#1931).
