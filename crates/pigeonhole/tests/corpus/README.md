# Format corpus

Small database files that every later build must open, read, check and write (D269, #496; [FORMAT.md §13](../../../../FORMAT.md#13-compatibility-promise)). `tests/corpus.rs` is the check, and runs in CI with the workspace tests.

`format4/<entry>/` holds one database:
- `data.phdb`, the main file;
- `data.phdb-wal-N`, the WAL files of an entry left crashed (`wal_crash`), or of one whose change feed retains history (`feed`). The other clean entries closed cleanly, so they have none;
- `data.phdb-feed`, the change feed's sidecar, in the `feed` entries;
- `expected.txt`, every cell the generating build read back, one per line: table, row (hex), family, qualifier (hex), timestamp, value. A vector value is its element, dimension and stored bytes;
- `expected-extra.txt`, in the entries built on the public API: what the generating build's text index, vector index or memory store `verify` reported, or the commits a feed resumed from a stored token delivered. The check compares what the checking build reads with it;
- `refused-by-1.0.0.txt`, in an entry that uses a feature 1.0.0 lacks: the `incompat` bits the refusal names (see below).

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
| `aggregates` | min, max, HLL, bitwise, `f64`, exact-sum, mean/variance, histogram, quantile, bloom and set families (D294, D298, `incompat` bit 0), with their `kind_params`; flushed, then more a process crash left in the WAL |
| `vectors` | vector families of every element (`f32`, `f16`, `bf16`, `i8`, bits; value tag 0x04, `incompat` bit 0), flushed and compacted with a version and a delete on top, and a `pigeonhole-vector` IVF index whose lists split, with deletes |
| `text` | a `pigeonhole-text` index with positions over a source column, written in the documents' transactions, with a rewrite and a delete (its statistics are aggregate families, so `incompat` bit 0) |
| `memory` | a `pigeonhole-memory` store with an embedding model, its text index and its IVF index, over two namespaces, with a link, a supersede, uses and a delete |
| `feed` | the change feed on: 300 commits to two tables over several WAL segments, flushed twice and closed cleanly, so the WAL and the sidecar keep the history; a subscriber's token from before the first commit is stored in a table, and the check resumes from it |
| `feed_crash` | the same, left by a process crash |

The check copies each entry to a temporary directory, opens it with the real filesystem, and compares every cell with `expected.txt`. It then writes a row through a flush and a compaction, closes, runs the offline check, reopens, and reads everything again.

**Regenerating:** `cargo test -p pigeonhole --test corpus -- --ignored generate_corpus` rewrites `format4/`, and `PIGEONHOLE_CORPUS_OUT=<dir>` writes the entries somewhere else instead. Entries are written through the simulator, so the crash state is exact. `format4/` is written by the current build and regenerated whenever an entry is added. Only an entry's contents are reproducible: `expected.txt` and the WAL files come out the same each run, but a main file's manifest can record positions that depend on background timing.

## Release corpus

`v1.0.0/` holds the files written by the 1.0.0 release build ([FORMAT.md §13](../../../../FORMAT.md#13-compatibility-promise): "a corpus of files written by the 1.0 release that every later build opens in CI"). `v1.1.0/` will hold the 1.1.0 release build's, the first release to write the structures from `aggregates` on (owner, 2026-11-04). Until its release, a release corpus holds only its README.
- **Writing it:** at the release, `scripts/release-corpus.sh <version>` runs `generate_corpus` from the release build with `PIGEONHOLE_CORPUS_RELEASE=<version>`. That writes every entry `tests/corpus.rs` has then, plus `frozen.txt` with each file's length and xxh3-64. The generator refuses a version other than the build's own, and refuses to rewrite a corpus that has a `frozen.txt`.
- **The check:** `release_corpus_entries_open_read_check_and_write` checks every release corpus. Its files must match `frozen.txt` byte for byte, and each entry must pass the same open, read, write and check as `format4/`, with its `expected-extra.txt` where it has one. Entries are never removed from `tests/corpus.rs`, so every release corpus's entries are the current build's. A build that fails it breaks the promise: fix the build, never the corpus.

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
