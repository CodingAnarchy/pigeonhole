### Added
- `pigeonhole-sst` reads through a direct-I/O handle (#403): every read rounds out to the handle's aligned pages, and a block keeps just its bytes in the buffer it was read into (no copy). `pigeonhole-io` adds `IoBuf::keep`, which keeps a range of a buffer in place.
