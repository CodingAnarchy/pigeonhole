### Changed
- `Options::commit_spin` defaults to 50 µs instead of 15 µs (D253): it covers a cross-shard commit's two rounds, so far fewer waiting threads park. On the cross-shard mix this gave +16% throughput and a 27% lower p50. `Duration::ZERO` still turns polling off.
