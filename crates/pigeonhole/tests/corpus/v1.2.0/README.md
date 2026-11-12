# The 1.2.0 corpus

This directory will hold the database files written by the 1.2.0 release build, which every later build must open, read exactly, write and check ([FORMAT.md §13](../../../../../FORMAT.md#13-compatibility-promise)). Until the release it holds only this README.

1.2.0 is the first release to write the structures 1.1.0 lacks, so its corpus freezes them too:
- **`feed_catalog`:** catalog changes in the change feed's `-feed` sidecar (D315): a feed of every table's token, then commits among created, extended and dropped tables.
- **`feed_root_intent`:** root commits' intents in the sidecar (the D315 amendment, #1504), from a session that flushes with the feed on and closes cleanly, so 1.1.0 still resumes its table feed.
- **`replica`:** a replica's state table and `ro_compat` bit 0 (D315 PR 3), which every earlier release opens read-only only. The entry is behind the `pigeonhole` crate's `__unstable-replication` feature, which `phdb serve --replica-of` builds with, so the generator runs with that feature.

**How it will be written:** from the 1.2.0 release commit, with the workspace version at `1.2.0`, by `scripts/release-corpus.sh 1.2.0`. That gives one directory per entry of `tests/corpus.rs`'s `entries()`, laid out like `format4/`, and `frozen.txt`, each file's length and xxh3-64. The entries built on the public API also get `expected-extra.txt`: what the index's or store's `verify`, or the feed's retained commits, read back.

**Once written, the corpus is frozen.** `release_corpus_entries_open_read_check_and_write` checks every file against `frozen.txt`, and every entry against its `expected.txt` and `expected-extra.txt`. A build without `__unstable-replication` still checks `replica`'s files against `frozen.txt`, but can't open the entry, so it skips it; CI's `--all-features` run opens it. A build that fails the check breaks the promise: fix the build, never the corpus. This README isn't in `frozen.txt`, so its wording can change at the release.
