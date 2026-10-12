### Changed
- `commit_async` and the other async commits cost about 43 instructions less per uncontended commit, and blocking commits about 4 less: an async commit that has to wait for room (D210) is now one state of the engine's pending commit instead of a layer of its own in the public future, and the engine's blocking commits wait on a leaner internal handle. No public API change.
