# Format corpus

Small database files that every later build must open, read, check and write (D269, #496; [FORMAT.md §13](../../../../FORMAT.md#13-compatibility-promise)). `tests/corpus.rs` is the check, and runs in CI with the workspace tests.

`format4/<entry>/` holds one database:
- `data.phdb`, the main file;
- `data.phdb-wal-N`, the WAL files of an entry left crashed (`wal_crash`), or of one whose change feed retains history (`feed`). The other clean entries closed cleanly, so they have none;
- `data.phdb-feed`, the change feed's sidecar, in the `feed` entries;
- `expected.txt`, every cell the generating build read back, one per line: table, row (hex), family, qualifier (hex), timestamp, value. A vector value is its element, dimension and stored bytes;
- `expected-extra.txt`, in the entries built on the public API: what the generating build's text index, vector index or memory store `verify` reported, or the commits a feed resumed from a stored token delivered. The check compares what the checking build reads with it;
- `refused-by-1.0.0.txt`, in an entry that uses a feature 1.0.0 lacks: the `incompat` bits the refusal names (see below).
- `feed-resumes-by-1.1.0.txt`, in an entry whose stored table feed token 1.1.0 must resume: `compat-reader-1.1` resumes it and compares the commits with `expected-extra.txt` (all but the values' hashes).

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
| `aggregates` | min, max, HLL, bitwise, `f64`, exact-sum, mean/variance, histogram, quantile, bloom and set families (D294, D298, `incompat` bit 0), with their `kind_params`; flushed, then flushed and compacted so compaction folds the bases, then more a process crash left in the WAL |
| `vectors` | vector families of every element (`f32`, `f16`, `bf16`, `i8`, bits; value tag 0x04, `incompat` bit 0), flushed and compacted with a version and a delete on top, and a `pigeonhole-vector` IVF index whose lists split, with deletes |
| `text` | a `pigeonhole-text` index with positions over a source column, written in the documents' transactions, with a rewrite and a delete (its statistics are aggregate families, so `incompat` bit 0) |
| `memory` | a `pigeonhole-memory` store with an embedding model, its text index and its IVF index, over two namespaces, with a link, a supersede, uses and a delete |
| `feed` | the change feed on: 300 commits to two tables over several WAL segments, flushed twice and closed cleanly, so the WAL and the sidecar keep the history; a subscriber's token from before the first commit is stored in a table, and the check resumes from it |
| `feed_crash` | the same, left by a process crash |
| `feed_sessions` | the change feed over two sessions: 150 commits, a clean close, a reopen that inherits the feed, 150 more, and a clean close. The check's open is the third session, with the default WAL segment size (the writer's is 256 KiB), and a feed from the oldest history must start at the first session's first commit and deliver both (#1465, #1471) |
| `returning` | `commit_returning` increments (D314) on a hot counter whose base is an older SST's `Put`, flushed: its SST holds the newest write-time D291 checkpoint as the counter's base, at its commit's seqno, with the later operands above it |
| `feed_catalog` | the change feed for every table (D315): a feed of every table's token from before the first commit, stored in a table, then 120 rounds of commits among catalog changes: a table created with two families, a family added to it, and two tables dropped, one from before the token, beside a table from before the token that survives. The check resumes a feed of every table from the token and lists every event: the catalog changes with their names, and each commit's changes, with the table and family of a table dropped by the check listed as `(dropped)` |
| `feed_root_intent` | the D315 amendment (#1504): a first session with the feed off creates the tables, so no feeding session changes the catalog; a second, with the feed on, stores a table feed's token, then writes 60 commits to two tables with flushes, whose root commits record their intents and overwrite them with their Root entries, and closes cleanly. The check resumes the token. `feed-resumes-by-1.1.0.txt` makes `compat-reader-1.1` resume it too: 1.1.0 reads the sidecar 1.2 leaves after clean closes. (1.1.0 can't resume across a session that changed the catalog: it doesn't know type 4, and its feed expires there, fail closed.) |

The check copies each entry to a temporary directory, opens it with the real filesystem, and compares every cell with `expected.txt`. It then writes a row through a flush and a compaction, closes, runs the offline check, reopens, and reads everything again.

**Regenerating:** `cargo test -p pigeonhole --test corpus -- --ignored generate_corpus` rewrites `format4/`, and `PIGEONHOLE_CORPUS_OUT=<dir>` writes the entries somewhere else instead. Entries are written through the simulator, so the crash state is exact. `format4/` is written by the current build and regenerated whenever an entry is added. Only an entry's contents are reproducible: `expected.txt` and the WAL files come out the same each run, but a main file's manifest can record positions that depend on background timing.

## Release corpus

`v1.0.0/` holds the files written by the 1.0.0 release build ([FORMAT.md §13](../../../../FORMAT.md#13-compatibility-promise): "a corpus of files written by the 1.0 release that every later build opens in CI"). `v1.1.0/` holds the 1.1.0 release build's, the first release to write the structures from `aggregates` on (owner, 2026-11-04).
- **Writing it:** at the release, `scripts/release-corpus.sh <version>` runs `generate_corpus` from the release build with `PIGEONHOLE_CORPUS_RELEASE=<version>`. That writes every entry `tests/corpus.rs` has then, plus `frozen.txt` with each file's length and xxh3-64. The generator refuses a version other than the build's own, and refuses to rewrite a corpus that has a `frozen.txt`.
- **The check:** `release_corpus_entries_open_read_check_and_write` checks every release corpus. Its files must match `frozen.txt` byte for byte, and each entry must pass the same open, read, write and check as `format4/`, with its `expected-extra.txt` where it has one. Entries are never removed from `tests/corpus.rs`, so every release corpus's entries are the current build's. A build that fails it breaks the promise: fix the build, never the corpus.

## Rollback corpus

`rollback-1.1.3/` holds the 1.1.3 rollback (#1837): a database a 1.2 build left after a crash inside a root commit, then opened, written and closed by 1.1.3. 1.1.3's CHANGELOG and 1.2.0's upgrade note promise that the change feed survives the rollback and the move back.
- **The entries:** `landed` and `not_landed` (the crash came after the root's superblock landed, or before), each also `_catalog` (the root changed the catalog, so 1.2 also left its Catalog entry). Each holds the database's files and a `feed_tokens` table with a token for `events` taken before its first commit, and the seqnos of the commit before the rollback (`one`) and 1.1.3's (`two`).
- **Writing it:** `cargo run --release -- <new dir>` in `rollback-gen/`, a cargo project of its own pinned to 1.1.3's commit on `release/1.1.x` by revision. It runs 1.1.3's own code. What a 1.2 crash leaves is written into the sidecar by hand, as 1.2 writes it (FORMAT §10; the Catalog entry is a real one from a 1.2.0 sidecar). The session before the crash is written by 1.1.3: its Session and Root entries are the same bytes 1.2 writes. The generator also writes `frozen.txt`. Written once and frozen.
- **The check:** `a_rollback_to_1_1_3_and_back_keeps_the_feed` checks the files against `frozen.txt` byte for byte, then reopens each entry without feed options (as `phdb` or a tool does). The open must keep the feed, and the stored token must resume through `one` and `two` with nothing skipped, including across the Catalog entry 1.1.3 itself expires on.

## Newer files with the published releases

Two separate cargo projects, each its own workspace, check main's files with a published release. `cargo run --release -- <dir>` checks a directory of entries that main wrote with `PIGEONHOLE_CORPUS_OUT`:
- `compat-reader/` is pinned to `pigeonhole =1.0.0` from crates.io;
- `compat-reader-1.1/` is pinned to `=1.1.0` (#1489). It builds the same `compat-reader/src/main.rs`, with the `v1_1` feature for the value types 1.0.0 lacks.

Each reader's package version is the release it pins. The release opens each entry on a copy: it reads every cell exactly, writes through a flush and a compaction, passes the offline check, and reads everything again. An entry that uses a feature the release lacks may be refused with `UnsupportedFeature`, which FORMAT §13 allows:
- if the feature is `ro_compat`, the reader reads the entry read-only instead;
- if it is `incompat`, the reader reports the entry and skips it.

**Marked entries are strict.** An entry with `refused-by-1.0.0.txt` uses the `incompat` bits that file names, and the rule depends on which of them the release reads:
- **A release missing any of them** must refuse the entry, naming exactly the bits it lacks. 1.0.0 lacks all of them.
- **A release that reads every one** must open the entry read-write. 1.1.0 reads bit 0, so it must open `aggregates`, `vectors`, `text` and `memory`, and a refusal fails it.

The `Compatibility` workflow (`.github/workflows/compat.yml`) runs one arm per release on main and PRs. Each arm enables itself once its release is on crates.io.

**Neither reader commits a lockfile.** Each run resolves the pinned release's own dependencies afresh, within their semver ranges, as an application that adds that release today would. So a run's third-party versions can differ from the last one's. That's intended: the promise is about what the release reads, not the build that a lockfile would freeze.

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
