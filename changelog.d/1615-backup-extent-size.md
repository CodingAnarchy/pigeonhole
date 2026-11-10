### Fixed
- A backup (`Pigeonhole::backup`, `backup_durable` and `phdb backup`) is about the size of what it holds again. Each SST it wrote from memtables, or rewrote without commits above its snapshot, took a 64 MiB-aligned slot of the copy, so a small database with several families made a copy of hundreds of MiB, nearly all free (#1595, backport of #1615).
