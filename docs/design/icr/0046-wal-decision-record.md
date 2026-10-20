# 0046: the WAL DECISION record (type 4) and format version 3, refused by older builds by version

**Status:** Approved (owner, via the coordinator, 2026-10-20; D263).

## Change

**On disk** (FORMAT §10.3, §11.3, §12):
- **A new WAL record, `Decision` (type 4):** `seqno` (the commit's id) u64, `apply_seqno` u64, `commit_ts` u64, then the participant list as in `Commit`. Every Prepare whose id is `seqno` is applied at `apply_seqno` and `commit_ts`.
- **A Prepare's `seqno` is the commit's id**, a seqno reserved but never applied, and its `commit_ts` is unused (0): the timestamp is picked at the decision.
- **`Commit` (type 3) stays readable,** with its old meaning (Prepares at their own seqno and timestamp). This build writes only Decisions.
- **`FormatVersion::CURRENT` is 3.** A writable open of a version 1 or 2 file commits its root twice before writing any WAL record, so both superblock pages carry version 3.

**Public, `pigeonhole-format`:**
- `RecordType::Decision = 4` and `WalRecord::Decision { seqno, apply_seqno, commit_ts, participants }`;
- `RECORD_HEAD_MAX` grows from 21 to 25 bytes (a Decision's head);
- `FormatVersion::CURRENT` becomes `FormatVersion(3)`.

- `RecordType` and `WalRecord` become `#[non_exhaustive]` (owner, 2026-10-20), so a later record type is not another breaking change. Adding the variant and the attribute is breaking once against 0.3.0; the workspace moves to 0.4.0 for Phase 4 in its own PR. In the workspace, `pigeonhole-wal` (recovery's seqno bound) and `pigeonhole-engine` (replay, which fails a record type it does not know as corruption) gain a wildcard arm.

**Public, additive** (`pigeonhole-engine` and `pigeonhole` `CrossShardMetrics`, both `#[non_exhaustive]`): `share_waits` and `share_wait_nanos`, the singles a shard held back behind an undecided share and the time it held them (D263).

**Internal** (`pigeonhole-pager`): `OpenedPager` records whether either superblock page decodes at an older version; `finish` upgrades both pages when writable.

## Why
D263 moves the commit's seqno and timestamp to the decision, so the decision record has to name them.
- **Corruption, not version:** a version 2 build (0.2.x, 0.3.x) decodes a type-4 record as `Corrupt { "wal record type" }` and fails the open as corruption, not as a newer file.
- **Silent loss:** a version 2 build skips a WAL segment header of version 3 as torn (`Grid::read`), so the replay chain ends at the last version 2 segment and every later record, committed or not, is dropped without an error. It also prefers a valid version 2 superblock page over a version 3 one (`Superblock::choose`), so one upgraded page alone is not enough.
- **Checked against 0.3.0 itself** (crates.io `pigeonhole = "=0.3.0"`): a database 0.3.0 wrote, then opened by this build, which wrote 64 rows across shards with `Sync` and exited without closing. 0.3.0 then fails to open it with `ErrorCode::UnsupportedFormat` ("unsupported file format version 3"), and this build reads all 128 rows. With the two-page upgrade disabled, 0.3.0 opened the same sequence's file and returned 64 rows: the 64 synced rows were silently lost.

## Callers
- `pigeonhole-format`: `wal.rs` (the record, encode, decode, `RECORD_HEAD_MAX`), `version.rs`; golden files (`wal_record_decision.bin`, and every structure's version byte).
- `pigeonhole-wal`: `recovery.rs`'s seqno bound takes a Decision's `apply_seqno`.
- `pigeonhole-pager`: `read_superblocks`, `OpenedPager::finish`.
- `pigeonhole-engine`: `shard.rs` (the coordinator's `decide_commit`, the record a `CommitRecord` member appends), `engine.rs` (replay pairs Prepares with Decisions by id and applies them at `apply_seqno`/`commit_ts`; Commits as before).
- **Tests:**
  - `crates/pager/tests/format_upgrade.rs`: both pages upgraded, a reader never writes, a crash mid-upgrade finished at the next open, a new file untouched (the first two fail with the upgrade disabled);
  - `crates/engine/tests/format_upgrade.rs`: an older file's pages are both upgraded before any write to another file (fails with the upgrade disabled);
  - `crates/engine/tests/tablets.rs`: `a_cross_shard_commit_in_a_0_3_0_wal_replays` (hand-written type-2/3 records), `an_undecided_prepare_left_by_a_crash_is_never_applied_after_more_commits_and_a_crash`;
  - `crates/format/tests/golden.rs`, `roundtrip.rs`.
