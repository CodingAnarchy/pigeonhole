### Added
- `Pigeonhole::database_files` and `Pigeonhole::database_file_suffix`: every file of a database at a path (the main file, its WAL stream files in order, the change feed's sidecar), listed without opening it, and each file's suffix, so a closed database's file set can be moved, copied or removed as one (ICR 0135).
