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
  - `Engine::feed_background_syncs()`;
  - `FeedOptions::sync_interval_micros`;
  - `Wal::write`'s doc states the M1 invariant.

- **pigeonhole** (semver, additive; `FeedRetention` is unreleased and `non_exhaustive`): `FeedRetention::sync_interval` (field and builder).

**Callers:**
- the shard's commit path (`run_group`, `decide_share`), `publish_watermark`, the lazy flush and the background sync;
- `crates/engine/tests/it/feed_written.rs`, and the shm and format tests.

## PR 3a: the writer-process feed reader (engine level)
- **pigeonhole-wal** (additive):
  - `FeedCursor` (`open`, `step`, `epoch`, `position`) and `Step` (`Record`, `End`, `Gone`);
  - `FeedMarks::checkpoint()`.
- **pigeonhole-engine** (additive):
  - `Engine::feed(FeedSpec) -> FeedReader`, writer only;
  - `FeedReader`: `try_next`, `wait(timeout)`, `token`, `frontier`, `delivered`;
  - `FeedSpec { tables, prefix, durable, start }` and `FeedStart` (`Now`, `Oldest`, `After(seqno)`, `Token(bytes)`);
  - `Commit { seqno, ts, changes }`, `Change { table, family, kind, ts, row, qualifier, value }`, `Expiry { reason, oldest_retained }`, and `Next` (`Commit`, `Pending`, `Expired`);
  - `Engine::feed_sidecar_syncs()`.
  - **Test-hooks inspection helpers only:** `FeedReader::held_prepares` and `Engine::feed_sidecar_sync_pending`.

**Callers:**
- `crates/engine/tests/feed_model.rs` (new sweep target), `it/feed_reader.rs` and `wal/tests/feed_cursor.rs`;
- PR 3b's public `Feed`.

## The cut fix (after 3a)
- **pigeonhole-wal:** `Step::Cut`, a new variant. `Step` is new since 1.0 (#1316, unreleased), so this breaks no released API. The engine's feed reader maps it to an expiry.

## The inherit amendment (the owner, 2026-11-02)
- **pigeonhole** (additive): `Options::change_feed_off()`.
  - **Behaviour:** with neither `change_feed` nor `change_feed_off`, an open inherits the last session's feed settings (retention, age, fed tables; not `sync_interval`, which is per open). If every table it fed was dropped since, the open records the feed off.
  - **The shard count:** with `shards` unset, an open takes the stream count the last session recorded, and only when that session fed. With no sidecar, or with the feed last off, unset `shards` keeps its old meaning (one per physical core).
- **pigeonhole-engine:** `EngineOptions::feed_inherit` (default `false`).

## PR 3b: the public change feed (pigeonhole, additive)
- `Table::feed() -> FeedBuilder`. The builder has `table`, `prefix`, `visible`, `from_snapshot`, `from_oldest`, `resume` and `open() -> Result<Feed>`.
- `Feed`: `next(Duration)` and `try_next()` return `Result<Option<&Commit>>`. It also has `token() -> FeedToken` and `oldest_retained() -> Option<u64>`.
- `Commit` (`seqno`, `timestamp`, `changes`) and `Change` (`table`, `family`, `kind`, `row`, `qualifier`, `timestamp`, `value() -> Option<Value>`).
- `ChangeKind` (`Put`, `Merge`, `DeleteCell`, `DeleteColumn`, `DeleteFamily`; `#[non_exhaustive]`, with `is_delete`).
- `FeedToken` (`as_bytes`, `from_bytes`).
- `ErrorCode::FeedExpired = 31`.
- **Python:** the `FeedExpired` exception class (code 31), with the feed API itself still Rust-only.
- **capi:** nothing to change. The crate has no code table yet, and its future C enum takes `ErrorCode`'s numbers.
- **Internal:** pigeonhole-engine's `Feed::needs` gains a `debug_assert` (perf287 on #1297).

## Later PRs
- PR 4: reader processes.
- PR 5: `poll_next`.
