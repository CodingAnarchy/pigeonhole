# The 1.3.0 corpus

This directory holds the database files written by the 1.3.0 release build, which every later build must open, read exactly, write and check ([FORMAT.md §13](../../../../../FORMAT.md#13-compatibility-promise)).

It has the same entries as the 1.2.0 corpus. What 1.3.0 writes differently, and so freezes here:
- **`feed_catalog`:** the stored token of a feed of every table is version 3, which carries the catalog at its position (D325, #1870). 1.2.0 refuses it; 1.2.1 and later resume it.
- **The `-feed` sidecars** of `feed_catalog`, `feed_root_intent` and `feed_sessions` are as 1.3.0 writes them.

**Not covered:** 1.3.0's two new `incompat` bits. Bit 2 (externally prepared transactions, D329) and bit 3 (feed-silent batches and Range Drop records, D331) are set only by a transaction across cluster partitions or a range move, and no entry does either. Bit 1 (D327) is reserved, and no build sets it. A packed text index (version 2, ICR 0143) isn't covered either: the `text` entry doesn't pack.

**How it was written:** at the 1.3.0 release, from the release commit with the workspace version at `1.3.0`, by `scripts/release-corpus.sh 1.3.0`, with the `pigeonhole` crate's `__unstable-replication` feature for the `replica` entry. That gave:
- one directory per entry of `tests/corpus.rs`'s `entries()`, laid out like `format4/`;
- `frozen.txt`, each file's length and xxh3-64;
- for the entries built on the public API, `expected-extra.txt`: what the index's or store's `verify`, or the feed's retained commits, read back.

The generator refuses to run from any other version, and refuses to run again now that `frozen.txt` exists.

**The corpus is frozen.** `release_corpus_entries_open_read_check_and_write` checks every file against `frozen.txt`, and every entry against its `expected.txt` and `expected-extra.txt`. A build without `__unstable-replication` still checks `replica`'s files against `frozen.txt`, but can't open the entry, so it skips it; CI's `--all-features` run opens it. A build that fails the check breaks the promise: fix the build, never the corpus. This README isn't in `frozen.txt`, so its wording can change.
