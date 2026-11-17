### Added
- `pigeonhole-text`: `Index::rebuild_with(step)` stops a rebuild between documents with the index consistent, and `resume_rebuild_with` carries on from a cursor the index keeps; `rebuild_pending` and `Index::pending_rebuilds` find stopped ones. `rebuild` and `repair` start over and drop the cursor (#1756).

### Changed
- `phdb serve`: `Text.Rebuild` runs a document at a time under the partition's write gate, so a fence or a shutdown stops it at the next document instead of waiting for the whole rebuild; the stopped rebuild resumes on its own once the partition takes writes again, and a client's `Text.Rebuild` always starts over (#1756).
