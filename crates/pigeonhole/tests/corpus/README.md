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

## Upgrade corpus

`v0.2.5/<entry>/` and `v0.3.3/<entry>/` hold databases written by the released 0.2.5 and 0.3.3 builds from crates.io: files from before format 4, which every 1.x build must open, upgrade and read exactly ([FORMAT.md §13](../../../../FORMAT.md#13-compatibility-promise): "every 1.x build opens every file written by 0.1.0 or later"). Each entry has the same layout as a `format4` entry. Its `expected.txt` is every cell the old build read back from a copy of its own files.

| Entry | What it holds |
|---|---|
| `wal_only` | 2 shards, synced commits and nothing flushed, left crashed: everything is in the WAL |
| `flushed` | bytes, i64 and f64 values in LZ4, zstd and uncompressed families, two versions and every delete kind, flushed |
| `compacted` | the same, flushed twice and compacted |
| `counters` | a counter family over 2 shards, partly flushed |
| `blobs` | separated values, half overwritten and compacted |
| `merge_crash` | a `corpus.concat` merge family beside a plain one, operands flushed and then synced into the WAL, left crashed |

`upgrade_corpus_entries_open_upgrade_and_read_exactly` in `tests/corpus.rs` checks each entry on a copy:
- **Read-only:** `merge_crash` is opened with `read_only_fallback` and without its merge operator registered, so the fallback is taken. The open replays the WAL into memory, reads the plain family exactly and refuses a write. Afterwards, no file has changed and the superblocks still carry the old format version.
- **Writable:** the open upgrades both superblock slots to format 4, reads every cell exactly, writes through a flush and a compaction, passes the offline check, and reads everything again after a reopen.

**Writing it:** `tests/corpus/legacy-gen` is a separate cargo project (its own workspace, not built in CI) that depends on `pigeonhole =0.2.5` and `=0.3.3`. `cargo run --release -- <out dir>` writes both versions' entries; crashed entries come from a child process that exits without closing. The corpus was written once, on 2026-10-29, and is frozen: never regenerate it. A build that fails this check breaks the promise; fix the build.
