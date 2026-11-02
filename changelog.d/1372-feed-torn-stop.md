### Fixed
- A change feed could fail with `Corruption` ("wal segment chain: predecessor end mismatch") when it read a log frame while that frame was still being written, and the log rolled over to its next segment in the same moment. The feed now reads that spot again on its next poll. A segment that is really short still fails as corruption (#1370, D310).
