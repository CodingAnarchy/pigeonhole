### Fixed
- A panic inside a manifest commit (a flush's, a compaction's or a catalog change's) no longer makes closing the database hang. The close now returns an error that names the panic, and later manifest changes are refused until the database is reopened (#2129).
