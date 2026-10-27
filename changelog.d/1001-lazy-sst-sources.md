### Changed
- A filtered scan or row read (a qualifier filter or time range) no longer reads SST blocks past the scan's position for an SST with nothing to return there, nor opens a level's next SST before the scan reaches it (D288).
