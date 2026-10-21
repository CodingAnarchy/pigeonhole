### Fixed
- An older build no longer silently misreads a file a newer build wrote (#496):
  - **A newer superblock slot** (valid checksum, newer version) is never passed over for the other slot. The open refuses with `UnsupportedFormat` rather than opening an older root the newer build may have freed.
  - **A newer WAL segment** (valid checksum, newer version) refuses the open. It used to be read as a blank or torn slot, and with every segment newer the stream read as empty, so its commits were silently missing.
  - **A WAL fragment of an unknown type** whose CRC passes is `UnsupportedFeature`, not a torn tail.
  - **A refused open never upgrades the file:** an open refused for an unregistered merge operator now refuses before upgrading the file's superblocks, leaving an older file as it was.
