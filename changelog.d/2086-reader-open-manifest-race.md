### Fixed
- A reader process's `open_reader` could fail with `Corruption("manifest block")` on a healthy file if the writer flushed or compacted while the reader was opening (#2085). The reader now pins the manifest it loads. If the writer restarts during the open, it returns `ErrorCode::Busy`: open again.
