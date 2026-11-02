# 0106: the change feed (D310)

**Status:** Approved (number from the coordinator; owner approved D310's seven calls at revision 3 on #1248, c6565998562, 2026-11-02, after simp724's correctness review and perf287's performance review). Each PR of D310 §6 adds its part here.

## PR 1: retention, the blob horizon, the sidecar and the mark
- **pigeonhole-format:**
  - `SegmentHeader::feed_session: u64`, bytes 76..84 of the format 4 header (FORMAT §10.2). A new public field of a struct that isn't `non_exhaustive`, so an incompatible change on the internal 2.x line (D295).
  - The `feed` module: `FeedHeader`, `FeedEntry` (`Session`, `Root`, `Unfed`, `Unknown`), `FeedTables`, `decode_entries`, `FEED_MAGIC`, `FEED_VERSION` and `FEED_SUFFIX` (FORMAT §10.4).
- **pigeonhole-wal:**
  - `WalOptions::retain_segments` and `WalOptions::feed_session`. `WalOptions` is `non_exhaustive`, so these are additive.
  - `Wal::trim_retained(now, max_age)` and `Wal::feed_marks()`, provided methods that are no-ops for `MemWal`.
  - `FeedMarks`: `head()`, `floor()` and `retained_bytes()`.
  - `WalCounters::retained_taken()`.
- **pigeonhole-pager:** `OpenedPager::sequence()`, `Pager::sequence()` and `Pager::on_root_commit(observer)`.
- **pigeonhole-engine:**
  - `EngineOptions::feed: Option<FeedOptions>`.
  - `FeedOptions { retention_bytes, max_age_micros, tables }`, `non_exhaustive`.
  - `DbInfo::feed: Option<FeedStorage>`.
  - `FeedStorage { retention_bytes, retained_wal_bytes, held_blob_bytes }`.
- **pigeonhole (semver, additive):**
  - `Options::change_feed(FeedRetention)` and `Options::feed_tables(tables)`. `feed_tables` without `change_feed` is `InvalidArgument` at open, and so is a name with no table.
  - `FeedRetention { bytes, age }`, `non_exhaustive`, with `FeedRetention::bytes(n)` and `.age(d)`.
  - `DatabaseInfo::feed: Option<FeedInfo>`.
  - `FeedInfo { retention_bytes, retained_wal_bytes, held_blob_bytes }`, `non_exhaustive`.

**Callers:**
- the engine's open, close, maintenance and manifest writer;
- `pigeonhole`'s `Options` and `DatabaseInfo`;
- the WAL's own tests, and `crates/engine/tests/it/feed_retention.rs`.

## PR 2: the written watermark and the lazy `None` flush
- **pigeonhole-format:**
  - `shm::watermark::WRITTEN` (16) and `shm::watermark::SYNCED` (24).
  - `ShmLayoutVersion::CURRENT` is now 7.
- **pigeonhole-shm** (additive):
  - `ShmRegion::publish_written(shard, seqno)` and `ShmRegion::publish_synced(shard, seqno)`;
  - `ShmRegion::written_seqno(durable)`.
- **pigeonhole-engine** (additive):
  - `Engine::feed_frontier(durable)`;
  - `Wal::write`'s doc states the M1 invariant.

**Callers:**
- the shard's commit path (`run_group`, `decide_share`), `publish_watermark` and the lazy flush;
- `crates/engine/tests/it/feed_written.rs`, and the shm and format tests.

## Later PRs
- PR 3: `Feed`, `Commit`, `Change`, tokens and `FeedExpired`.
- PR 4: reader processes.
- PR 5: `poll_next`.
