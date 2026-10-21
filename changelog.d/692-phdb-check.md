### Added
- `phdb check`: an offline integrity check from the command line. It covers superblocks, the manifest, extents, every SST block, every blob record and a crashed database's WAL, and exits 3 on damage. `--deep` reads every separated value through. `--map` lists the file's regions. `--online` reads every table through a reader process while a writer runs (#692).
