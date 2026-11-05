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

## PR 2: catalog events
Catalog changes (`create_table` with its family options, `add_family`, `drop_table`) as items of a feed of every table, from the sidecar's `Catalog` entry (FORMAT §10.4 type 4), behind the `CATALOG_HOLD` (shm layout 9). Built in two PRs:
- **2a (#1493)** writes the entries and the hold; nothing is delivered from them.
- **2b** delivers them.

**2b, pigeonhole** (semver, additive; cli692's design):
- `FeedEvent { Commit(Commit), Catalog(CatalogChange) }`, `#[non_exhaustive]`, owned and `Clone`.
- `Feed::next_event(timeout)`, `try_next_event()` and `poll_next_event(cx)`, returning `&FeedEvent`, beside `next`, `try_next` and `poll_next`. Those stay commits-only, so they pass the catalog changes over.
- `CatalogChange::{seqno, timestamp, edits}`. `seqno` places the change: every commit below it is before it, every commit at or above it after.
- `CatalogEdit::{TableCreated { table, families: Vec<FamilyInfo> }, FamilyAdded { table, family: FamilyInfo }, TableDropped { table }}`, each variant `#[non_exhaustive]`. No ids are public: PR 3's `apply_catalog` takes a hidden payload behind `__unstable-replication`.
- `nonblocking::FeedEventStream`, a `Stream` of owned `FeedEvent`s.
- **Starts:** a feed opened now, or `from_snapshot`, passes over exactly the changes its start's catalog reflects. Ids only grow, so a created table or family is reflected once the catalog's counter passed its id, and a drop once the table is gone. A token resumes just past the last event delivered.

**2b, pigeonhole-engine** (internal, a break on the 2.x line):
- `Next::Catalog(&CatalogEvent)`, where `CatalogEvent` has `seqno_ceiling`, `timestamp`, `sequence`, `edits` and `names`.
- `FeedStart::Snapshot(Snapshot)`.

**2b, the format:** the `Catalog` entry gains an optional names trailer (FORMAT §10.4). Type 4 is unreleased, so no version changes, and an entry without names is byte-for-byte 2a's (`feed_sidecar_catalog.bin` is unchanged; `feed_sidecar_catalog_names.bin` freezes the trailer).

**2b, sidecar failure:** once the session's appends stop, the next catalog change's hold stays up for the rest of the session. A feed of every table in the writer's process expires. One in a reader process waits at that change's place: it can't see the stop (#1516). Only a feed of every table's frontier reads the hold (`ShmRegion::written_seqno(durable, catalog)`, an internal change), so feeds of some tables never wait for a catalog change.

**2b, starts:** an `Oldest` or `After` start passes over the changes with a ceiling at or below its bound in every sidecar load, not only in the entries at open. The hold doesn't bound visible seqnos, so a start past a change's ceiling can come before the change's entry is appended. A token of a feed of every table carries that bound after its last delivered change's sequence (the unreleased token version 2 grows by 8 bytes), so a resume passes over what its start did (simp724's F1b). It doesn't use the token's delivered seqno instead: a `Now` start can be past the ceiling of a change its view doesn't reflect yet, and it delivers that change.

## Amendment: a feed's position and a table's feed id (for #1518)
`phdb serve`'s feed hubs (D313 ticket 5, #1518) share one engine feed per table set and move clients between their own streams and the hub's ring. To rejoin exactly, they need a stream's covered position, and a key that tells a re-created table from the old one (simp724's F1 and F4 on #1518). The coordinator chose hidden accessors, approved 2026-11-04 and recorded in numbers.md.
- **pigeonhole** (`#[doc(hidden)]`, so outside the stable API and the public-API snapshot; not behind `__unstable-hooks`, since production `phdb serve` uses them):
  - `Feed::position()`, `nonblocking::FeedStream::position()` and `FeedEventStream::position()` return a `FeedPosition` with `seqno()` and `catalog_sequence()`. Every commit of the feed's tables at or below `seqno` was delivered or passed over, or precedes the start. In a feed of every table, a catalog change was delivered or passed over iff its sequence is at most `catalog_sequence`. The seqno says nothing about catalog changes: a `Now` or snapshot start can be past the ceiling of an unpublished change it still delivers (simp724's P1 on #1520). The parts are independent, so one position covers another iff both are at least as large (`FeedPosition::covers`). Neither part goes back, across a recovery's or a writer restart's resubscribe too.
  - `Table::feed_id()` returns the engine's table id. Ids are never reused.
- **pigeonhole-engine** (internal): `FeedReader::position() -> FeedPosition { seqno, catalog }`, with `seqno = max(delivered, min(F, k - 1, c - 1))`:
  - F is the frontier of the last complete read pass;
  - k is the smallest key read but not yet delivered;
  - c is the ceiling of the next catalog change loaded but not yet delivered, in a feed of every table.

  It is at least `delivered()`, which already follows other tables' records (passed over with no changes). It also covers the seqnos no record holds, and carries the catalog sequence.
- **Tests:**
  - `feed_model` checks, across reopens, power losses and reader processes, that the position never goes back and never passes an undelivered commit.
  - `it::feed_reader` covers other tables' writes, and a pending commit and catalog change.
  - `tests/feed.rs` covers the public form and `feed_id`.

## PR 3: replication apply (to come)
`apply_replicated` and `apply_catalog`: the primary's commits, applied with their original timestamps and ids.

## Callers changed
None for PR 1 or PR 2: they're additive. `phdb mcp`'s `feed_poll` still names its tables. A feed of every table there is the bindings' and CLI's to add, after #1397's design.
