### Fixed
- The change feed's retention is counted in each WAL stream's own segment size. A database reopened with another `wal_segment_size` (a hidden test option) kept too little or far too much feed history, because existing WAL files keep the segment size they were made with (D310, #1471). Unreleased; found before 1.1.0.
