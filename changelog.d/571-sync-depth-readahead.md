### Changed
- Scans read 16 data blocks ahead (was 4), still without merging adjacent fetches (D222). On the reference machine this scanned cold tables fastest on both I/O backends: 3.2–3.6 GiB/s, against 2.6 GiB/s with no readahead. The test variable `PIGEONHOLE_READAHEAD` is gone.
- The group sync depth for durable commits is settled at 2 on Linux and 1 on macOS (D221), and the measurement variable `PIGEONHOLE_GROUP_SYNC_DEPTH` is gone.
