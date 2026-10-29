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
| `families` | every family setting the manifest records (TTL, version limit, bloom bits, block size, cache priority, tiered and FIFO compaction), flushed three times and compacted, beside a table created, written, flushed and dropped |
| `wal_ops` | 2 shards, a flushed base, then one synced batch per operation (bytes, i64 and f64 puts, every delete kind, counter increments, merge operands, a separated value, rows on both shards, a row deleted) a process crash left only in the WAL |
| `checkpoints` | a counter family whose reads kept D291 bases, flushed: level 0 holds the bases beside increments flushed after them |

The check copies each entry to a temporary directory, opens it with the real filesystem, and compares every cell with `expected.txt`. It then writes a row through a flush and a compaction, closes, runs the offline check, reopens, and reads everything again.

**Regenerating:** `cargo test -p pigeonhole --test corpus -- --ignored generate_corpus` rewrites `format4/`, and `PIGEONHOLE_CORPUS_OUT=<dir>` writes the entries somewhere else instead. Entries are written through the simulator, so the crash state is exact. `format4/` is written by the current build and regenerated whenever an entry is added. Only an entry's contents are reproducible: `expected.txt` and the WAL files come out the same each run, but a main file's manifest can record positions that depend on background timing.

## Release corpus

`v1.0.0/` holds the files written by the 1.0.0 release build ([FORMAT.md §13](../../../../FORMAT.md#13-compatibility-promise): "a corpus of files written by the 1.0 release that every later build opens in CI"). Until the release it holds only its README.
- **Writing it:** at the release, `scripts/release-corpus.sh 1.0.0` runs `generate_corpus` from the release build with `PIGEONHOLE_CORPUS_RELEASE=1.0.0`. That writes every entry above, plus `frozen.txt` with each file's length and xxh3-64. The generator refuses a version other than the build's own, and refuses to rewrite a corpus that has a `frozen.txt`.
- **The check:** `release_corpus_entries_open_read_check_and_write` checks every release corpus. Its files must match `frozen.txt` byte for byte, and each entry must pass the same open, read, write and check as `format4/`. A build that fails it breaks the promise: fix the build, never the corpus.

## Newer files with 1.0.0

`compat-reader/` is a separate cargo project, its own workspace, pinned to `pigeonhole =1.0.0` from crates.io. `cargo run --release -- <dir>` checks a directory of entries that main wrote with `PIGEONHOLE_CORPUS_OUT`. 1.0.0 opens each one on a copy: it reads every cell exactly, writes through a flush and a compaction, passes the offline check, and reads everything again. An entry that uses a feature 1.0.0 lacks may be refused with `UnsupportedFeature`, which FORMAT §13 allows:
- if the feature is `ro_compat`, the reader reads the entry read-only instead;
- if it is `incompat`, the reader reports the entry and skips it.

The `Compatibility` workflow (`.github/workflows/compat.yml`) runs it on main and PRs, and enables itself once 1.0.0 is on crates.io.

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
