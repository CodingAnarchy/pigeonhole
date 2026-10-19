### Changed
- The Phase 3 gate window trims the data file system (`fstrim -v`) before every step at full scale, logs each trim in its output and `fstrim.tsv`, and records the trim before setup and the NVMe namespace's mapped part in `machine.json` (D258). A drive still mapping the terabytes earlier steps deleted had put the D213 group-commit row in a slow mode.
