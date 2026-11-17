# 0143: `Index::pack` and text index layout version 2 (packed posting blocks)

**Status:** Approved. The number and the names came from the coordinator (`.coord/numbers.md`), for #1585, in 1.3.0 by the owner's scope call. The design (`.coord/audit120-design-1585-1586.md`, Part 2) was reviewed by ai1249 (text) and perf2 (measurement). This file grows with #1585's PRs: PR 3b (below) adds the layout, the pack and the merged read path, with a walk that passes whole blocks by their bound; PR 5 (at the end) adds `phdb serve --text-pack`.

## Why

A search of a common word reads one engine cell per posting, and the engine's per-cell cost is about three quarters of that search (`text-term-hub`, callgrind: 66% in the engine's cell walk, 9% in `mem*` under it). No change inside `pigeonhole-text` reaches #1585's "several times faster" while a posting is a cell. Layout version 2 keeps the cells as the write tier and adds blocks of up to 128 postings under them, so a walk pays the per-cell cost once a block.

## PR 3b: layout version 2 and `Index::pack`

### Public API (`pigeonhole-text`, additive)

- `Index::pack(&self) -> Result<PackStats>`
- `Index::pack_with(&self, PackOptions, impl FnMut(&PackStats) -> ControlFlow<()>) -> Result<PackStats>`: the step runs after each chunk; `Break` stops the pack there, resumable.
- `Index::pack_pending(&self) -> Result<bool>`: whether a stopped pack's cursor is kept.
- `PackOptions` (`new`, `with_readers(bool)`), `PackStats { terms, postings, tombstones, stopped }` (`#[non_exhaustive]`).
- `IndexStats` gains `packed` (postings held in blocks) and `tombstones` (deleted postings a pack hasn't removed yet). Neither counts as a mismatch.
- `Error::PackRefused(String)`, code `Busy`.

Nothing else changes signature. `Search`, `ReadIndex`, `rebuild`, `repair` and `verify` work on both layouts.

### Nothing packs an index but `pack`

**1.2.1 is the release to roll back to from 1.3, and a 1.2 build can't read a packed index. So an index is version 1 until someone calls `pack`.** `create`, a write, a search, `rebuild` and `repair` never change the version. A server packs only with `phdb serve --text-pack` (a later PR; off by default).

- **The first pack is refused while reader processes are attached** to the database file (`Error::PackRefused`, naming the count), unless `PackOptions::with_readers(true)`. The file records that a reader is attached (`EngineMetrics::reader_slots_active`), not which build it is. Two limits:
  - a reader that exited without closing holds its slot until the writer reclaims it, so the refusal can outlast the reader;
  - the check and the pack's first commit aren't one step, so a reader that attaches between them isn't seen. The guide's rule covers that: pack only once no 1.2 build will open the index again.
- **A 1.2 build that already has the index open** when it is first packed fails every search and write with `Busy` from then on (it reads the state cell below as "a repair is running"). It doesn't return results that miss the packed postings.
- **A 1.2 build that opens the index afterwards** is refused: "layout version 2 is unknown".
- **The way back** is to drop the index and create it again on the older build. The source rows are untouched.

### The layout

Version 1 is D309 §2, unchanged. Version 2 adds:

- **`G`/`meta`/`layout` = 2.**
- **The state cell `G`/`meta`/`repair`.** In version 1 it is absent, or present (empty) while a repair runs. In version 2 its value is `v2` when no repair runs. Every search and every write through the index already read this cell for the repair flag, so they learn the layout from the same read: a version 1 search pays nothing for version 2, and a handle opened before the first pack needs no reopen.
- **The family `postb`** in the index table, added by the first pack. In `P{term}`, one cell is one block: the qualifier is the block's last document id, the value is the block (FORMAT.md §14). A term's blocks don't overlap.
- **Tombstones.** Where a version 1 writer deletes a posting's cells, a version 2 writer overwrites them: `post` with the value 0 (a real posting's frequency is at least 1) and `pos` with an empty value. A tombstone hides the document's posting in a block until a pack removes both. It is a blind write, as the delete is.
- **`G`/`meta`/`pack-after`:** a pass's cursor, the last posting cell its last chunk took (term length as `u32` LE, the term, the document id; empty before the first chunk). Absent when no pack is part way.

`df`, `maxtf`, `minlen`, the forward rows and the statistics are written exactly as in version 1. A pack never touches them.

**In a version 2 index a position cell never stands without its posting cell** (a writer that writes one writes both). A pack walks posting cells, so it meets every cell there is.

### The merged view

**A term's postings are its cells over its blocks: the cell wins where both have a document, and a tombstone means no posting.** Every reader uses it:

| Reader | In version 2 |
|---|---|
| a term's walk (`maxscore.rs`, sync and async) | both tiers stepped in document order; a block passed is never decoded, and once the top `k` are held, a block whose bound is under what a posting must add to matter is passed whole |
| a walk's point lookup | the block tier advances; the cell tier answers from where it stands when it can, else by the point-read rule it had |
| the phrase join (`search.rs`) | the lead's positions as the merged stream; each other term the same way as a lookup |
| `verify` | counts postings of both tiers, tombstones and packed postings apart |
| `rebuild`'s compare | a posting that is right in a block is left there |
| `repair` | empties the term rows, blocks included; the index stays version 2 |

- **`id_prefix`:** cells use the qualifier prefix as before. Blocks are read from the prefix on, and cut where a block's ids leave it, since a block named past the prefix can hold ids inside it.
- **A block's bound is exact for any statistics.** It is the best BM25 factor over the block's competitive (frequency, length) pairs, computed with the search's own statistics, so it holds for a cluster's global ones too. Passing a block by it is the same decision as passing each of its postings under the walk's floor (#2003). A cell over one of the block's documents is still read, in the cell tier.
- **A version 1 index runs the code it ran before**: the version 2 path is entered once per walk call, never per posting.

### What a pack does

**A pass walks the posting cells term by term, in chunks of at most 512 cells, one transaction a chunk:**

1. At one snapshot: the chunk's posting cells and their position cells, and the blocks their ids fall among.
2. Merge: a cell replaces the block's posting for its document, a tombstone removes it.
3. Cut the result into blocks again, evenly (each at most 128 postings or 4 KiB).
4. In the transaction: write the blocks, delete the blocks they replace, delete each cell read, and move the cursor.

- **A cell is deleted only if it is still the version the chunk read.** The transaction reads each cell again and compares its timestamp. A cell written since stays and hides the block's posting, which is then the older one. A write that lands before the commit makes it conflict and run again.
  - The design had the pack delete the version it read, by timestamp (`delete_cell`), with no read. That is wrong for a column that was overwritten: deleting its newest version uncovers the older one, so a posting a tombstone had replaced came back. Found by the first test of a second pack.
- **The cursor and the blocks a chunk replaces are read again inside its transaction** (ai1249's condition): only a pack and a rebuild's reset write those cells, so two packs, or a pack and a reset, can't leave overlapping blocks, and an ordinary write never conflicts on them.
- **A few postings with no block among them stay cells** (fewer than 32, in a chunk that isn't full) until more have gathered, so a term that grows a little between packs isn't cut into blocks of a few. Their tombstones are still removed.
- **A reader's snapshot** sees a chunk whole or not at all.
- **Writes after a pack are cells again**, and deletes are tombstones, until the next pack.

### Pack, rebuild and repair

- **A pack doesn't run while a rebuild is pending** (`rebuild_pending`): `PackRefused`, at its start and at any chunk.
- **A rebuild from the top** (`rebuild`, `rebuild_with`) drops a stopped pack's cursor in its first commit. The next pack starts over.
- **A rebuild compares against the merged view** and doesn't clear the blocks: searches run during a rebuild, and would miss every packed posting until its document was reached. A transaction reads single cells, so the block is found by a read outside it and read again inside it; unless the two agree, the posting is written as a cell, which is right either way.
- **`repair`** drops the cursor with the rebuild's, and its emptied rows have no blocks. At its end the state cell is `v2` again.

### Callers

- `pigeonhole-memory` and `phdb serve` use the text crate's public API only: unchanged in this PR, and neither packs.
- **A cluster (#1722):** each partition's index is its own table in its own file, packed on its own node. Nothing crosses partitions. A replica gets the blocks through replication as any cell and never packs. PR 2b's two-round search passes the scoring statistics in, which the block's bound is exact for (it stores the block's competitive frequency and length pairs, not a score).
- **The change feed:** a pack's block writes and cell deletes are commits like any other, so a feed of the index table sees them. The guide says so.

### Tests

`crates/text/tests/pack.rs` and `search_async.rs`: a packed index answers every search as a twin that is never packed, through writes, deletes and packs in between (sync, async, random queries, id prefixes, with and without positions); the first pack's rules; stop and resume; packs beside rebuilds, repairs and concurrent writers.

## PR 5: `phdb serve --text-pack`

### Public API (additive)

- **`phdb serve --text-pack`** (a switch, off by default, per node): the node's partitions pack their text indexes in the background. Without it a server never packs; there is no pack RPC.
- **`Index::list(db: &Pigeonhole) -> Result<Vec<String>>`** (`pigeonhole-text`): the text indexes in a database, by name. A catalog listing (the tables with an index's families), opening none as an index. `Index::pending_rebuilds` is this list filtered by its cursor read.
- **Metrics**, present only with the flag, per partition on a cluster node: `pigeonhole_serve_text_pack_passes_total`, `pigeonhole_serve_text_pack_postings_total`, `pigeonhole_serve_text_pack_refused_total`, `pigeonhole_serve_text_pack_conflicts_total`.

### What the server does

- **A thread per partition** (`phdb-serve-text-pack`, placed off the shards' cores as the rebuild's is), started where a stopped rebuild's resume is started: at the partition's open and after a promote, never on a replica copy. It passes over the partition's indexes at once, then every 60 seconds, until the partition closes.
- **A pass** packs each index with `Index::pack_with` and default options, a chunk at a time. Its step is the rebuild's (`Steps`): it enters the partition's write gate for each chunk, so a fence or a lost lease stops the pack at its next chunk, and the pass waits until the partition takes writes again.
- **It holds the node's rebuild slot as a resume does:** one rebuild or pack at a time on a node, and a pack stops at its next chunk for a client's `Text.Rebuild`.
- **An index with a rebuild pending** (stopped, or a cluster index still filling) is passed over: the resume thread finishes it, and a later pass packs it.
- **The first pack keeps `Index::pack`'s refusal:** with a reader process attached the pack is refused, logged once for the index, counted, and tried again each pass. The server never passes `with_readers(true)`: it can't know the readers' release.
- **A pack that ends in `Conflict`** (a chunk that kept conflicting with writers down to the smallest chunk) is not a failure: the cursor stays and the next pass carries on (ai1249's S2 on #2009). It is counted apart (`…_conflicts_total`), so a pack that conflicts on every pass shows. `Index::pack`'s own contract doesn't change.
- **A shutdown** stops the pack at its next chunk and waits for the thread before the database closes. The cursor stays, and the next start carries on from it.

### Tests

`crates/cli/src/serve/server_tests/text_tests.rs`: a server without the flag never packs and shows no pack metric; with it the index is packed, a search agrees before and after and finds a later write, and the counters show it; a reader process attached holds the first pack off until it closes; a fenced partition packs nothing until unfenced. `crates/text/tests/pack.rs`: `Index::list`.
