# The 1.1.0 corpus

This directory will hold the database files written by the 1.1.0 release build, which every later build must open, read exactly, write and check ([FORMAT.md §13](../../../../../FORMAT.md#13-compatibility-promise)). Until the release it holds only this README.

1.1.0 is the first release to write the structures 1.0.0 lacks, so its corpus freezes them too:
- **`aggregates`:** the D294 and D298 aggregate kinds and their `kind_params`, and roaring sets (`incompat` bit 0).
- **`vectors`:** vector families of every element (value tag 0x04, the same bit), and a `pigeonhole-vector` IVF index.
- **`text`:** a `pigeonhole-text` index with positions.
- **`memory`:** a `pigeonhole-memory` store, with its text and IVF indexes.
- **`feed`** and **`feed_crash`:** the change feed's `-feed` sidecar and its retained WAL history, closed cleanly and left crashed.

**How it will be written:** from the 1.1.0 release commit, with the workspace version at `1.1.0`, by `scripts/release-corpus.sh 1.1.0`. That gives one directory per entry of `tests/corpus.rs`'s `entries()`, laid out like `format4/`, and `frozen.txt`, each file's length and xxh3-64. The entries built on the public API also get `expected-extra.txt`: what the index's or store's `verify`, or the feed's retained commits, read back.

**Once written, the corpus is frozen.** `release_corpus_entries_open_read_check_and_write` checks every file against `frozen.txt`, and every entry against its `expected.txt` and `expected-extra.txt`. A build that fails it breaks the promise: fix the build, never the corpus. This README isn't in `frozen.txt`, so its wording can change at the release.
