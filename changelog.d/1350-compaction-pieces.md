### Fixed
- A full compaction no longer rewrites, on every `compact()`, an SST that ended just over one output piece in length. Compaction now cuts its output SSTs by their whole length, index and filters included, the same measure it uses to decide whether an SST needs cutting (#1324).
