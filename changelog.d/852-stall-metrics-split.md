### Added
- `EngineMetrics::room_stalls`, `room_stall_nanos`, `pacing_stalls` and `pacing_stall_nanos` count the two kinds of write stall apart: waits for memtable arena room (arena pressure) and L0 pacing while compaction catches up (compaction debt). `write_stalls` and `write_stall_nanos` stay, as their sum (#724, ICR 0054). The shell's `\metrics` lists both kinds.
