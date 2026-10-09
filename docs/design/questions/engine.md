# Engine: open questions

## Q: Should a memtable flush early when it is mostly overwritten versions? (#287)

**Context.** Reads of a hot row step over every version of every column that the memtable still holds. Flush-time version GC (D191) drops shadowed versions when a memtable flushes, so the same row costs far less once flushed. On the sparse-wide gate's hot rows (Linux callgrind, per returned cell):

| State | Instructions per cell |
|---|--:|
| as written (memtable over one SST) | ~2,600 |
| flushed | ~1,930 |
| compacted | ~1,340 |
| SQLite's index range scan, same rows | ~1,300 |

The gate's row-read p99 is about 2× SQLite's (455 µs vs 222–231 µs, runs 5–6). The slowest reads are as-written hot rows: about 2.4 entries stepped per returned cell, with stale versions accumulating in the memtable over the run. Making each step cheaper has reached diminishing returns (#375 and #377 closed, #378 and #381 about 1–2% each). Seeking past stale versions loses to stepping (`SKIP_STEPS` 2 / 4 / never: +60% / +34% / +31%). Marking superseded memtable nodes would change the shared-memory layout. Today a memtable freezes only on size (`memtable_freeze_bytes`), WAL pinning, tablet moves or an explicit flush.

**Proposal.** Also freeze a memtable once it is mostly other versions of columns it already holds:
- **Signal:** the memtable counts inserts that land next to another version of the same column (`Memtable::overwrites`). Versions of a column are adjacent in the skiplist, so this is one comparison with each level-0 neighbour of the insert position the insert already found. That's a few dozen instructions per insert, with no extra memory.
- **Trigger:** `overwrites >= share × entries` and `allocated_bytes >= memtable_stale_min_bytes` (default 1/16 of the budget, so the trigger never makes tiny SSTs), and the family keeps a limited number of versions (`max_versions > 0`) and isn't a counter family. Elsewhere every version is live and a flush drops none, so flushing early would only cost writes. The default family keeps every version, so the trigger only acts where an application limits versions. It is checked where the size threshold is checked: after each insert, and when the shard freezes. The family check comes last, so it runs only when the other two conditions pass.
- **Option:** `EngineOptions::memtable_stale_share`, 0 (off) by default. The public crate exposes `Options::experimental_stale_flush(share)`, doc-hidden and unstable. Tests and sweeps can turn it on with `PIGEONHOLE_TEST_STALE_SHARE` (only with `test-hooks`). The bench reads `PHDB_BENCH_STALE_FLUSH`.
- **Correctness:** it changes only *when* a memtable freezes. Everything after the freeze (D191's flush-time GC, guards, sequencing) is unchanged, and any freeze reason already takes that path.

**Costs to weigh.** More flushes, each smaller (write amplification and shard CPU), and more L0 SSTs between compactions. Each L0 SST is another source for reads until compaction merges it, which could cost reads of rows spread across many L0 files. A workload with no overwrites never trips it (tested), and overwrites of a few hot columns trip it only once the memtable passes the minimum size.

**Measurements before deciding** (with the option on and off):
- the sparse-wide gate on a quiet machine: ops/s, p99, row-read and scan p99;
- write cost: flushes per minute, bytes written, shard CPU, L0 depth over time;
- every readshapes and writepath shape on Linux callgrind;
- a write-heavy workload without overwrites, to confirm no spurious flushes.

**Interim behavior:** the trigger exists, off by default. Nothing changes unless an application or test sets the share.
