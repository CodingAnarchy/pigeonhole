# 0137: `wal_pad_unit`, a GroupSync group starts on a fresh page while a sync holds the tail

**Status:** Approved. The owner approved D327 on 2026-11-14, option first, and the coordinator assigned the number (`.coord/numbers.md`). This is #1708, the residue of #1621. The name is the PR's, as the assignment left it. feed1248 reviews.

## The change
- **New public names:**
  - `pigeonhole::Options::wal_pad_unit(bytes: u32)`, default `0` (off). A power of two from 32 to 32,768 (`pigeonhole_format::wal::MIN_PAD_UNIT` to `FRAME_SIZE`); anything else fails the open with `InvalidArgument`, before the file is touched.
  - `phdb serve --wal-pad-unit BYTES`, default 0, passed to every database `serve` opens (a single node's and each cluster partition's, `served_options`).
  - `phdb-bench --wal-pad-unit BYTES` (the bench crate, unpublished), for the A/B.
- **Internal names (not semver-checked):**
  - `pigeonhole_wal::WalOptions::pad_unit: u32`, default `0`. `WalOptions` is `#[non_exhaustive]`. A stream with it set starts a group on a multiple of it, behind a Pad fragment (FORMAT §10.2 type 5), while a sync it submitted is still in flight.
  - `pigeonhole_wal::WalCounters::{pads, pad_bytes}`: the groups padded, and the bytes the pads took.
  - `pigeonhole_format::wal::FrameEncoder::padded_len(unit, len)`: a pad and a record's bytes together, for the stream's segment-full check.
- **Behaviour, with the option on:**
  - **The open flags the file** with `incompat` bit 1 (`INCOMPAT_WAL_PAD`, D327), in both superblock pages, before any stream writes. It does so after the change feed's root hooks are installed, so the two superblock commits are this session's roots like any other, each with its intent and Root entry: a crash right after them leaves a chain the feed reads through, not an expiry (audit120's N1 on #1929). The bit is never cleared: retained feed segments keep pads after a checkpoint. A build before 1.3 then refuses the file at open.
  - **A group pads only when it has to.** Its first record pads when the stream's latest submitted sync hasn't completed (the durable mark is below its target). At most one pad per sync, since the records after a pad are aligned. A stream whose commits never sync (`Buffered`, `None`) never pads. A group whose pad and record wouldn't fit the segment goes without the pad, rolling over if the record alone doesn't fit either (a new segment's first record starts a frame), so a pad is never the last thing in a segment.
  - **Visibility and durability don't change** (D227): a commit is visible when its write returns, and durable when the sync that covers it completes. Pads take no seqno, and the change feed steps over them (D327 PR 1).
  - **Off,** nothing changes, and the bit isn't set.

## Why
#1621's remaining stall: a GroupSync group's buffered `pwrite` starts in the tail page that the previous group's `fdatasync` is writing back, and waits on that page's folio lock (g69: 104/94 driver pwrites of 5 ms or more, mostly in a sustained session's tail). D324's direct writes removed the waits but cost more than they saved. D327 keeps the writes buffered and moves the next group off the locked page.

The unit is a public number, not a bool, so the box A/B (perf287, on g69's session) can measure the folio size on xfs and try units without a rebuild. The A/B decides the default and whether `phdb serve` turns it on. Acceptance (the owner): WAL bytes at most 1.2× on g69's session and the ycsb rows, and `Buffered` unchanged.

## Callers changed
None: the default is off. The engine's test harness reads `PIGEONHOLE_WAL_PAD=N` (with `PIGEONHOLE_DEFERRED_IO=1`, the only mode where a sync stays in flight) so its suites and sweeps can run over padded logs.
