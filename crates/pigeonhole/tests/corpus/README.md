# Format corpus

Small database files that every later build must open, read, check and write (D269, #496; [FORMAT.md §13](../../../../FORMAT.md#13-compatibility-promise)). `tests/corpus.rs` is the check, and runs in CI with the workspace tests.

`format4/<entry>/` holds one database:
- `data.phdb`, the main file;
- `data.phdb-wal-N`, the WAL files of an entry left crashed (`wal_crash`). The clean entries closed cleanly, so they have none;
- `expected.txt`, every cell the generating build read back, one per line: table, row (hex), family, qualifier (hex), timestamp, value.

| Entry | What it holds |
|---|---|
| `empty` | a fresh database |
| `values` | bytes, i64 and f64 values (a NaN among them) in LZ4, zstd and uncompressed families, several versions, every delete kind, flushed and compacted |
| `counters` | a counter family over 2 shards, partly flushed |
| `blobs` | separated values, half overwritten and compacted |
| `merge` | a family with a custom merge operator (`corpus.concat`, which the check registers) |
| `wal_crash` | 2 shards, flushed rows, then synced single and cross-shard commits a process crash left only in the WAL |

The check copies each entry to a temporary directory, opens it with the real filesystem, and compares every cell with `expected.txt`. It then writes a row through a flush and a compaction, closes, runs the offline check, reopens, and reads everything again.

**Regenerating:** `cargo test -p pigeonhole --test corpus -- --ignored generate_corpus` rewrites `format4/`. Entries are written through the simulator, so the crash state is exact. The corpus is regenerated once, by the 1.0 release candidate, and frozen from then on. Until then it is regenerated whenever an entry is added. A change that makes the check fail on a frozen corpus breaks the compatibility promise. Fix the build, never the corpus.
