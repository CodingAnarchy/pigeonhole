### Changed
- File format: the first range move's copy or cleanup sets superblock `incompat` bit 3 (feed-silent batches, D331, ICR 0141). A database with the bit doesn't open on a build before 1.3; a database that never had a range move is unaffected. In a file without the bit, a batch carrying the mark is reported as corrupt (#2055).
