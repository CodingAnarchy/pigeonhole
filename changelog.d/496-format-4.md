### Changed
- **Format version 4**, the format 1.x is frozen at (D269, #496). It reserves room to grow without another version bump:
  - feature bits in the superblock (compat, ro_compat, incompat), in the SST footer and in the WAL segment header, which grows to 128 bytes;
  - an extension area in the superblock, which every root commit carries forward unchanged;
  - must-understand (1 to 127) and skippable (128 to 255) manifest edit tags.

  No feature bit is defined yet. A writable open upgrades an older file as before: both superblock pages carry version 4 before any WAL write, and builds before format 4 then refuse the file with `UnsupportedFormat`. This build still reads files of versions 1 to 3. A file using a feature this build does not know is refused with `UnsupportedFeature`, or for a read-only-compatible feature, refused for writing only. FORMAT §8.2 states what a compat feature's data may rely on when a build that does not know the feature writes.
