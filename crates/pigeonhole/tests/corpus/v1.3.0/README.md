# The 1.3.0 corpus

This directory holds the database files written by the 1.3.0 release build, which every later build must open, read exactly, write and check ([FORMAT.md §13](../../../../../FORMAT.md#13-compatibility-promise)).

It has the 1.2.0 corpus's entries plus three new ones (#2144), for 1.3.0's format additions. What 1.3.0 writes differently, and so freezes here:
- **`spanning_txn`:** `incompat` bit 2 (externally prepared transactions, D329, ICR 0140). Transactions over 2 shards: one committed and one aborted, each with an External Prepare on both WAL streams and a decision naming both; one with reads and no writes; and an xid-tagged commit. Left by a process crash, so a reopen replays the External Prepare, Decision and External Abort records. None is left in doubt.
- **`range_move`:** `incompat` bit 3 (feed-silent batches and Range Drop records, D331, ICR 0141). What a range move leaves: feed-silent copy batches, one carrying tombstones (a column delete with a newer value after it, and a cell delete); the timestamp floor raised; and Range Drops, one bounded and one to the table's end. Left by a process crash.
- **`text_packed`:** a packed text index (layout version 2, ICR 0143), with cells beside the blocks and a tombstone. Its listing pins the packed postings and tombstones counts.
- **`feed_catalog`:** the stored token of a feed of every table is version 3, which carries the catalog at its position (D325, #1870). 1.2.0 refuses it; 1.2.1 and later resume it.
- **The `-feed` sidecars** of `feed_catalog`, `feed_root_intent` and `feed_sessions` are as 1.3.0 writes them.

**Not covered:** `incompat` bit 1 (D327) is reserved, and no build sets it. Nothing in doubt is frozen: a file with a transaction still in doubt would bind every later build to keep it decidable.

**How it was written:** at the 1.3.0 release, from the release commit with the workspace version at `1.3.0`, by `scripts/release-corpus.sh 1.3.0`, with the `pigeonhole` crate's `__unstable-replication` feature (for the `replica` and `range_move` entries) and `__unstable-txn-prepare` feature (for `spanning_txn`). That gave:
- one directory per entry of `tests/corpus.rs`'s `entries()`, laid out like `format4/`;
- `frozen.txt`, each file's length and xxh3-64;
- for the entries built on the public API, `expected-extra.txt`: what the index's or store's `verify`, or the feed's retained commits, read back.

The generator refuses to run from any other version, and refuses to run again now that `frozen.txt` exists.

**The corpus is frozen.** `release_corpus_entries_open_read_check_and_write` checks every file against `frozen.txt`, and every entry against its `expected.txt` and `expected-extra.txt`. A build without one of those features still checks the entries that need it against `frozen.txt`, but can't open them, so it skips them; CI's `--all-features` run opens them. A build that fails the check breaks the promise: fix the build, never the corpus. This README isn't in `frozen.txt`, so its wording can change.
