# The 1.1.0 corpus

This directory holds the database files written by the 1.1.0 release build, which every later build must open, read exactly, write and check ([FORMAT.md §13](../../../../../FORMAT.md#13-compatibility-promise)).

1.1.0 was the first release to write the structures 1.0.0 lacks, so its corpus freezes them too:
- **`aggregates`:** the D294 and D298 aggregate kinds and their `kind_params`, and roaring sets (`incompat` bit 0).
- **`vectors`:** vector families of every element (value tag 0x04, the same bit), and a `pigeonhole-vector` IVF index.
- **`text`:** a `pigeonhole-text` index with positions.
- **`memory`:** a `pigeonhole-memory` store, with its text and IVF indexes.
- **`feed`** and **`feed_crash`:** the change feed's `-feed` sidecar and its retained WAL history, closed cleanly and left crashed.
- **`feed_sessions`:** two closed sessions of retained history, which a feed from the oldest history must deliver from the first, after a reopen with another WAL segment size (#1465, #1471).
- **`returning`:** the D291 checkpoints `commit_returning` writes at commit time (D314).

**How it was written:** at the 1.1.0 release, from the release branch with the workspace version at `1.1.0`, by `scripts/release-corpus.sh 1.1.0`. That gave:
- one directory per entry of `tests/corpus.rs`'s `entries()`, laid out like `format4/`;
- `frozen.txt`, each file's length and xxh3-64;
- for the entries built on the public API, `expected-extra.txt`: what the index's or store's `verify`, or the feed's retained commits, read back.

The generator refuses to run from any other version, and refuses to run again now that `frozen.txt` exists.

**The corpus is frozen.** `release_corpus_entries_open_read_check_and_write` checks every file against `frozen.txt`, and every entry against its `expected.txt` and `expected-extra.txt`. A build that fails it breaks the promise: fix the build, never the corpus. This README isn't in `frozen.txt`, so its wording can change.
