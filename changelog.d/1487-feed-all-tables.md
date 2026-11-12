### Added
- `Pigeonhole::feed()` (and `PigeonholeReader::feed()`) opens a change feed of every table, including tables created after it opened: one ordered stream of the whole database's commits (D315, #1436). It needs the change feed on for every table, and resumes only from its own kind of token.
