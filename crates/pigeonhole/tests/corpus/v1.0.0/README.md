# The 1.0.0 corpus (placeholder)

This directory holds the database files written by the 1.0.0 release build, which every later build must open, read exactly, write and check ([FORMAT.md §13](../../../../../FORMAT.md#13-compatibility-promise)). It is empty until the release.

**Writing it (once, at the 1.0.0 release):** from the release commit, where the workspace version is `1.0.0`, run `scripts/release-corpus.sh 1.0.0` and commit what it writes:
- one directory per entry of `tests/corpus.rs`'s `entries()`, laid out like `format4/`;
- `frozen.txt`, each file's length and xxh3-64.

The generator refuses to run from any other version, and refuses to run again once `frozen.txt` exists.

**After that the corpus is frozen.** `release_corpus_entries_open_read_check_and_write` checks every file against `frozen.txt` and every entry against its `expected.txt`. A build that fails it breaks the promise: fix the build, never the corpus. While this README is the only file here, the test reports the corpus as not written yet.
