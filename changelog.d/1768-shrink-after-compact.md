### Fixed
- `shrink` called right after `compact` could leave the file at its pre-compaction length (#1765). The compaction reported itself done while it still pinned the input SSTs it had just retired, so `shrink` could not reclaim them or move the manifest below them. A compaction now releases its inputs before `compact` returns.
