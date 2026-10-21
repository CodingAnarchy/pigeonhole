### Added
- `Options::reader_slots`: how many reader processes can attach at once (default 126, up to 65,536; ICR 0055). One more `open_reader` fails with `NoReaderSlot` at once.
- `engine_metrics()` reports reader processes: `reader_slots_active` out of `reader_slots`, `reader_slots_reclaimed`, and how far the oldest reader snapshot lags, as `reader_pin_view_lag` and `reader_pin_seqno_lag`. A lag that keeps growing is a reader holding a snapshot, which eventually stalls the writer's commits into `Busy` (#693).
- A guide page on reader processes (`docs/guide/readers.md`): what readers see, crashes and restarts on either side, long-held snapshots, the limits and the requirements. It includes Windows readers in another session, which attach through a shared `shm_dir`.
