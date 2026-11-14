# 0136: every-table feed tokens, version 3 (the catalog at the position)

**Status:** Approved. The number is from the coordinator (`.coord/numbers.md`), for D325 (#1853). The owner approved the layout on 2026-11-13.

## The change
- **The token layout.** A token of a feed of every table (`Pigeonhole::feed()`, the engine's `FeedSpec` with no tables) is now version 3: version 2's layout followed by 16 bytes, all little-endian.
  - `tables` (u64): the wrapping sum of xxh3-64 over each table id's 4 bytes. The replica's reserved state table is left out.
  - `next_table` (u32) and `next_family` (u32): the catalog's id counters.

  These are the catalog at the token's position. The engine writes version 3 for every token of a feed of every table. Per-table tokens stay version 1.
- **Resuming one.** The engine works the catalog at the token's position out from the live catalog and the sidecar's Catalog entries. If it doesn't match the token's, the resume answers `FeedExpired`: a session that records no catalog changes (1.1.x, after a rollback) changed the tables.
- **Version 2 tokens** (1.2.x's) still resume, with no catalog to check. The feed's later tokens are version 3.
- **No API change.** Tokens stay opaque bytes (`FeedToken::as_bytes`), and their length grows by 16. `all_tables_token_position`, which a replica's bootstrap uses, reads version 3 too.
- **FORMAT §13's cross-version paragraph** (the change feed's history across versions) gains the token's side:
  - 1.2.0 refuses a version 3 token (`InvalidArgument`).
  - 1.2.1 reads one as version 2.

## Why
#1853: a feed of every table resumed across a 1.1.x session that changed the schema delivered its commits without the change, and a replica following it refused a commit for ever or fell silently behind. D325 has the reasoning and what was rejected.

## Callers
- **Applications holding every-table tokens.** Nothing to change on 1.3. After a rollback to 1.2.0, start those feeds again; 1.2.1 resumes them.
- **`phdb serve` replicas** store the primary's token. A replica of a 1.3 primary stores version 3 tokens, and a 1.2.0 replica refuses them when it bootstraps (`begin_replication`). Replicas run the same release as their primary.
- **`pigeonhole` (`Feed::token`)** and **`pigeonhole-engine` (`FeedReader::token`, `all_tables_token_position`):** this crate's code.
