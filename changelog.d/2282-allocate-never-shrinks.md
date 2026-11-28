### Fixed
- **A file's growth could truncate data another growth had just added, on macOS, Windows and Linux without `fallocate`** (#2279). Since 1.0.0, `File::allocate` read the file's length and then set it, so two growths of one file on two threads could set the length back down after the larger one had been written to. Linux with `fallocate` was never affected. Two places in the engine grow a file from two threads:
  - **The WAL:** a spare-segment preparation could cut off a segment the shard thread had just grown inline and synced. Its acknowledged commits were lost from the log. Recovery from a checkpoint before them ended the log early without an error, and a change feed reading the retained history stopped there for good.
  - **The data file:** a growth ahead of need could cut the file under an extent that an inline growth had just handed out.

  Now `allocate` never shrinks a file: where the platform must read the length and then set it, a handle orders those steps with its other length changes. The simulator does both in one step.
