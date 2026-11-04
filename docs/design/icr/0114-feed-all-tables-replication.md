# 0114: a feed of every table, catalog events, and replication apply (D315)

**Status:** Approved (number from the coordinator, 2026-11-04; design D315 on #1436, reviewed by simp724). It's built in three PRs, and each one adds its section here.

## PR 1: a feed of every table
- **pigeonhole** (semver, additive):
  - `Pigeonhole::feed(&self) -> FeedBuilder` returns a feed of every table, including tables created after it opened. Changes to a table dropped before the feed reached them come with empty names, as for a table's feed.
  - `PigeonholeReader::feed(&self) -> FeedBuilder` is the same in a reader process.
  - **Refusals, both `InvalidArgument`:**
    - the builder's `open` refuses a database whose change feed has a `feed_tables` list;
    - `FeedBuilder::table` and `read_table` on such a builder are refused at `open`, since the feed already follows every table.
  - **Expiry:** a token from a feed of every table expires across an open with a table list, which ends its history.
- **pigeonhole-engine** (internal, 2.x line, additive in behavior): `FeedSpec::tables` empty means every table. It was refused before ("a change feed follows at least one table"). It needs the session, and every session of its history, to feed every table (`FeedTables::All`).

## PR 2: catalog events (to come)
Catalog changes (`create_table` with its family options, `add_family`, `drop_table`) as items of a feed of every table, from the sidecar's `Catalog` entry (FORMAT §10.4 type 4), behind the `CATALOG_HOLD` (shm layout 9).

## PR 3: replication apply (to come)
`apply_replicated` and `apply_catalog`: the primary's commits, applied with their original timestamps and ids.

## Callers changed
None for PR 1: it's additive. `phdb mcp`'s `feed_poll` still names its tables. A feed of every table there is the bindings' and CLI's to add, after #1397's design.
