### Added
- `phdb-bench --durability buffered|sync|group-sync|default`. `default` keeps Pigeonhole's `Options::default()` durability (`GroupSync`) where the bench used `Buffered`, and has the other stores fsync every write; with `group-sync` or `default`, every engine loads at buffered durability and switches after the settle step (D247). The bench's default stays `buffered`.
