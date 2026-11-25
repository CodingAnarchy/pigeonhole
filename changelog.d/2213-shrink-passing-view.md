### Fixed

- `shrink` right after a compaction could, rarely, leave the file at the end of its manifest (several MiB with no row left; a second `shrink` finished the job): a view held for a moment by the engine's own maintenance kept the manifest in place in the call's last round. It now takes another round, at most 4 more per call (#2199).
