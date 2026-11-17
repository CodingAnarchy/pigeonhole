### Changed
- **`phdb serve` hands each feed subscriber's events to its connection in batches** (#2012, #1737). Delivering change-feed events costs the threads that also answer writes less per event. A subscriber's buffer is now bounded by its 256 KiB alone, with each event counted as at least 64 bytes; the separate 64-event cap is gone. Events, their order and their tokens are unchanged.
