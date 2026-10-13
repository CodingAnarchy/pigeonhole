### Added
- `phdb-bench --durability buffered|sync|group-sync|default`. `default` keeps Pigeonhole's `Options::default()` durability (`GroupSync`) where the bench used `Buffered`, and has the other stores fsync every write (D247). The bench's default stays `buffered`.
