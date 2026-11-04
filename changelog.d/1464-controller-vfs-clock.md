### Fixed
- The runtime's driver-load controller measures its windows on the VFS clock, as it already did the drivers' busy time, so a simulated clock drives it deterministically. Its trace test no longer depends on runner timing (#1462).
