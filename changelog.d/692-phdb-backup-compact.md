### Added
- `phdb backup`: a consistent single-file copy at a durable snapshot (`--latest` copies everything committed). `--check` checks the copy afterwards (#692).
- `phdb compact`: a full compaction with progress on standard error. `--shrink` then releases free space at the file's end (#692).
