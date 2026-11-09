### Changed
- Flushes and compactions write their output back to the device as they go, with at most 4 MiB in flight across the engine, so a durable commit's sync no longer waits behind tens of MiB of background output. At `GroupSync` this removes 10–90 ms put stalls under steady load (D319, #1621).

### Added
- `EngineMetrics::writeback_waits` and `EngineMetrics::writeback_peak_bytes`: how often background output waited for the device, and the most write-back in flight at once (ICR 0127). `phdb serve` exports them on `/metrics`.
