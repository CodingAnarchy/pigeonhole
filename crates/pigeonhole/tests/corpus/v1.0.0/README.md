# The 1.0.0 corpus

This directory holds the database files written by the 1.0.0 release build, which every later build must open, read exactly, write and check ([FORMAT.md §13](../../../../../FORMAT.md#13-compatibility-promise)).

**How it was written:** at the 1.0.0 release, from the release branch with the workspace version at `1.0.0`, by `scripts/release-corpus.sh 1.0.0`. That gave:
- one directory per entry of `tests/corpus.rs`'s `entries()`, laid out like `format4/`;
- `frozen.txt`, each file's length and xxh3-64.

The generator refuses to run from any other version, and refuses to run again now that `frozen.txt` exists.

**The corpus is frozen.** `release_corpus_entries_open_read_check_and_write` checks every file against `frozen.txt` and every entry against its `expected.txt`. A build that fails it breaks the promise: fix the build, never the corpus. This README isn't in `frozen.txt`, so its wording can change.
