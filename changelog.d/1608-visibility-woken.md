### Fixed
- `EngineMetrics::visibility_woken` could count more wakes than `visibility_waits` over a window: change-feed readers' wakes were counted, and a commit that found its seqno visible as it registered left its entry to be woken and counted later. Neither is counted now (#1608).
