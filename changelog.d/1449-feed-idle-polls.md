### Fixed
- An idle change feed no longer re-reads its WAL streams at every poll (#1448). A reader process following a quiet writer spent 1-1.7 ms of CPU per poll there, about a quarter of a core at its 5 ms backoff. A poll now reads the streams only once a commit has moved the frontier, or while a cross-shard round is still held.
