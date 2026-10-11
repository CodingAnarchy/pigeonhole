### Changed
- Publishing a new read view after a manifest commit reuses what the commit didn't change: the SST set when no SST changed (and the blob-file set when no blob file did), the tablet list of the shared-memory view record until tablets change, and the record's buffers. A one-cell flush against a 2,048-SST catalog makes 23% fewer allocations; a 1 MiB separated put 5% fewer (#499).
