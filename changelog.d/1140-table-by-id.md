### Added
- `pigeonhole-engine`: `Engine::table_by_id`, a table's current catalog entry by id (ICR 0092). `pigeonhole`'s row reads use it to find their table's entry without hashing its name on every read.
