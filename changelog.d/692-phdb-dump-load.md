### Added
- `phdb dump`: exports every cell, with all retained versions, at one snapshot, as JSONL (the default) or CSV. `--table`, `--prefix` and `--durable` narrow it. A writer may be running. `phdb load` reads the JSONL back into a database, creating its tables and families with their stored options and writing every cell at its timestamp (#692).
