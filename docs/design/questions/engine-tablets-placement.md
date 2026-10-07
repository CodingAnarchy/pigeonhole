# Engine: tablet placement and the slot budget (#104)

## Q: Where do tablets go at open, now that owners are not persisted (D130)?
D130 re-derives every owner as `tablet % shards`. After splits, a reopen with fewer shards can put more `(tablet, family)` slots on one shard than its arena has chunks: a 20-family table split into four tablets is 80 slots, all on shard 0 of a one-shard reopen, against 64 chunks at a 4 MiB budget. A commit writing all of them, or a freeze of all of them, then never finds room, and that shard can neither shed slots nor receive moves (D136's `max_slots` refuses both).

Persisting owners (an `Edit::SetTabletOwner`, or an owner on `PutTablet`) needs a `format` change and an ICR, and a reopen with fewer shards still has to place the tablets of the missing shards somewhere. Placement at open is needed either way, so this change does only that.

**Interim behavior (with `tablet_changes` on):** at open, in tablet id order, a tablet goes to shard `tablet % shards` when that keeps the shard within its slot budget, else to the shard holding the fewest slots (`Catalog::place`). Owners are still not persisted: a reopen with the same shard count loses earlier moves, as D130 says, and the balancer moves tablets again if the load calls for it. With `tablet_changes` off, every tablet is on shard `tablet % shards`, as before.

## Q: How large is the slot budget, and what happens when the tablets need more?
D136 caps each shard at a quarter of its arena's chunks (`max_slots`). Chunks are `memtable_budget / 64` capped at 256 KiB, so at budgets up to 16 MiB every shard has 64 chunks and 16 slots. A shard holding a table with more than 16 families could never split by size or receive a move, and nothing reported it.

**Interim behavior (with `tablet_changes` on):** each arena is cut into at least 256 chunks (`arena / 256`, between 1 KiB and 256 KiB), so every shard serves at least 64 slots at any budget. The default 64 MiB budget already had 256 KiB chunks, so it does not change. When the tablets placed at open need more slots on a shard than that, the chunks shrink further (`arena / (4 × slots)`, never below 1 KiB). The chunk size is fixed for the life of the open, so splits and moves past the budget are still refused at run time: an explicit request fails with `Unsupported`, and the balancer passes over a size split it has no slots for. The balancer logs that skip under `PIGEONHOLE_TRACE`, but no metric counts it. Smaller chunks mean entries larger than a chunk take a run of contiguous chunks more often, which a fragmented arena may not have. With `tablet_changes` off, chunks are sized as before.

Open for the coordinator: whether a refused split should be counted in `Metrics`, and whether chunks should shrink at run time (for example, rebuilding a shard's arena once every memtable is flushed) instead of only at open.

## Q: When do empty slots give back their memtables?
D136 retires every idle slot's memtable after every flush. A slot written once per flush cycle then gets a fresh memtable each cycle. A reader process's pin keeps every chunk retired after the view it pinned (D118), so the arena filled twice as fast as with tablet changes off.

**Interim behavior (with `tablet_changes` on):** idle slots retire only when a commit waits for arena room (the room-wait path of `run_group`), not after each flush. Pinned retired memtables (flushed ones and, under pressure, idle ones) still stay allocated until the pin moves, as with tablet changes off.
