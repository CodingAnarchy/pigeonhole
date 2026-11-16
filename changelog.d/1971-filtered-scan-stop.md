### Fixed
- Reads: a bounded scan of several rows with a qualifier filter now ends at its end key when the filter keeps nothing in its last rows. Before, each file's reader went on past the end, skipping or seeking row by row through the rest of the file, as multi-gets did before #1954 (#1931).
