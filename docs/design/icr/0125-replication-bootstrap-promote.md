# 0125: a replica's bootstrap and promotion (`backup_at`, `begin_replication`, `promote`)

**Status:** Approved (the number is from the coordinator, 2026-11-06; the design is agreed on #1436 with simp724's P1 and P2 and cli692's serve review). Everything here is hidden (`#[doc(hidden)]`) behind `__unstable-replication`. Whether `backup_at` becomes public is the owner's call; it's built so either way works.

## The change
**pigeonhole** (hidden, behind the feature):
- **`Pigeonhole::backup_at(&Snapshot, dest)`:** `backup` of a given snapshot. The copy holds exactly what the snapshot sees, so a feed opened `from_snapshot` on that snapshot starts where the copy ends.
- **`Pigeonhole::begin_replication(&FeedToken)`:** makes a bootstrap copy a replica that follows its primary from `token`.
  - **The token** is that of a feed of every table opened `from_snapshot` on the snapshot the copy was taken at.
  - **What it does:** creates the reserved state table (`ro_compat` bit 0, R6's write refusal) and stores the token's position and the primary's database id, so the first `apply_replicated` chains on it.
  - **Refused** on a replica, and after `promote`.
- **`Pigeonhole::promote() -> Option<FeedToken>`:** turns a replica into a primary at the last event it applied, and returns the primary's token stored with that event.
  - **Afterwards:** the database takes writes, its own feed goes on (the state table's drop is no feed event), and every apply is refused.
  - **Nothing fences the old primary:** the caller stops it first.

**pigeonhole-engine** (internal, additive):
- `Engine::{backup_at, begin_replication, promote}` and `all_tables_token_position`.
- **`backup_at` replaces a test hook of the same name,** so its test (`it::milestone_b`, #715) now calls the production method.
- **Only `begin_replication` creates the state table** (simp724's P1): the from-nothing first apply goes through it too, and an apply on a database without the table is refused ("not a replica").
- **`promote`:**
  - it sets a one-way promoted state under the manifest exclusion, in the catalog change that drops the table, so an apply racing it fails and nothing brings the table, the bit or the gate back;
  - a second commit then clears the bit (`flag_features` clears a stale bit before any commit that doesn't drop the table itself);
  - a writable open clears a bit left without its table after a crash. It never fails the open (`ManifestWriter::clear_stale_replica_bit`).
- **The replica's state cell (version 2) records the primary's database id.** An apply of another database's events is refused. Version 1 cells (D315 PR 3) still read.

**Not here:** the serve side (`phdb serve --replica-of`, the `Replication` service, re-bootstrap's file swap, `Admin.Promote`), which is in ICR 0112's amendment. Read-your-writes on a replica (#1577).

## Callers changed
None: hidden and additive. The `it::milestone_b` test moves from the removed hook to `Engine::backup_at`.
