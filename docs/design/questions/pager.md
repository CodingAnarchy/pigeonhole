# Pager questions (Phase 2)

## Built (#185): level outputs are cut into power-of-two pieces, at most half the stream each
The coordinator approved option 1' below (no format change), and it is built as follows.

- **Where.** In the compaction job's sink (`pigeonhole_compaction::job`), for outputs at levels ≥ 1, through `output_piece_bytes(remaining, target)`.
  - While more than the target's class remains, a piece is that class (64 MiB SSTs by default).
  - Otherwise a piece is the power of two at or below half the remaining stream, and at least 1 MiB. Once less than 2 MiB remains, the minimum drops to 64 KiB.
  - L0 (flush, FIFO's L0 merges) still writes one SST per flush.
- **Remaining.** The input bytes not yet read, scaled by the ratio of output written to input consumed so far, once at least 1/64 of the input is read. The open SST's data bytes come from the new `SstWriter::data_len`.
- **Cut.** Each output's extent is exactly its piece. The output is cut between rows (D78) once less than 1/32 of the extent is left: the index, filters and footer fit in that. If a later projection calls for a smaller piece, the output is cut once it holds that piece. At finish the extent is trimmed to the output's class.
- **`Engine::compact`.** A full compaction re-cuts a lone SST larger than one piece, where it used to move it trivially (a flush output would otherwise stay whole). An SST holding a single row cannot be cut and is left alone.
- **Blob separation (#33).** A separated value leaves only its pointer in the SST, and the sink sees the pointer. So pieces are sized from SST bytes (input and output lengths), never from blob bytes. Flushes separate too, so a compaction's inputs and outputs compare like for like. Blob files keep their own extents (64 KiB to 1 MiB, several per file).
  - Measured: 20 MiB of separated 1 KiB values compacts into 5 SSTs holding 355 KB.
  - The file at rest is still 1.60× its data, because `shrink` does not relocate blob extents (#281).
- **Not done here.**
  - Backup copies (`copy_at`) still write through the flush sink, so a backup's last SST can be half empty.
  - The #200 `shrink` notes (reserve holes for the largest extents; release a skipped extent's claim at once).

### Built: measured (deterministic test `crates/engine/tests/footprint.rs`, file sizes only)
The setup is the probe's (1 shard, 1 KiB incompressible values, delete to the kept fraction, then `compact` and `shrink`), on `SimVfs` and an application-owned shard. The test asserts file ≤ 1.2 × live + 384 KiB (1.3× under 1 MiB of data), where 384 KiB is the header unit, a manifest snapshot unit and the manifest log's 256 KiB extent.

| Load, kept | Live | File | Ratio | SSTs (was) |
|---|---|---|---|---|
| 5 MiB, 100% | 5.2 MiB | 6 MiB | 1.16× | 11 (1) |
| 20 MiB, 100% | 20.8 MiB | 22.25 MiB | 1.07× | 14 (1) |
| 50 MiB, 100% | 51.9 MiB | 54.25 MiB | 1.05× | 15 (1) |
| 50 MiB, 10% | 5.2 MiB | 6 MiB | 1.15× | 11 (1) |
| 50 MiB, 1% | 0.5 MiB | 1 MiB | 1.91× | 5 (1) |

- **Small files.** At 0.5 MiB of data the fixed metadata dominates: the SST extents total 576 KiB (1.05× the data), and the manifest log's 256 KiB extent sets the rest.
- **SST counts.** A run's count grows with the log of its tail, about 2·log2(tail / 64 KiB). That is more than the model's 5–10 because the 64 KiB minimum applies to every stream's last 2 MiB.

## Proposal (approved as option 1'; kept for the measurements): cut level outputs into power-of-two pieces, at most half the stream each (#185)
A file at rest is 1.2–3.8× its live data. Extents are power-of-two sized and aligned to their size, unit 0 (the header) is never free, and an SST takes the class above its length. So a file is at least twice its largest extent, and the last SST of a compaction is often half empty.

### Measured (file sizes only)
The probe ran on a CI runner (Sweep workflow on the scratch branch `scratch/footprint-probe`, test `footprint_probe`, `PIGEONHOLE_FOOTPRINT=1`). Setup: 1 shard, 1 KiB incompressible values, `max_versions(1)`, delete to the kept fraction, `compact` twice, `shrink`. A Python model of the allocator (aligned buddy, lowest fit, unit 0 reserved, largest first as `shrink` places them) reproduces every measured size exactly. The option columns come from that model fed with the measured SST lengths.

| Load, kept | Live (SSTs) | Now | Opt 1: power-of-two cut | **Opt 1': power-of-two, ≤ half the stream** | Opt 4: unaligned exact extents | SSTs now → 1' |
|---|---|---|---|---|---|---|
| 5 MiB, 100% | 5.2 MiB | 16 MiB (3.08×) | 8 MiB (1.54×) | **6 MiB (1.16×)** | 5.2 MiB (1.01×) | 1 → 5 |
| 20 MiB, 100% | 20.7 MiB | 64 MiB (3.08×) | 32 MiB (1.54×) | **22 MiB (1.06×)** | 20.8 MiB (1.00×) | 1 → 7 |
| 50 MiB, 100% | 51.9 MiB | 128 MiB (2.47×) | 64 MiB (1.23×) | **53 MiB (1.02×)** | 51.9 MiB (1.00×) | 1 → 10 |
| 50 MiB, 10% | 5.2 MiB | 16 MiB (3.08×) | 8 MiB (1.54×) | **6 MiB (1.16×)** | 5.3 MiB (1.02×) | 1 → 5 |
| 50 MiB, 1% | 0.5 MiB | 2 MiB (3.84×) | 1 MiB (1.92×) | **0.6 MiB (1.2×)**, with a 64 KiB minimum for streams under 2 MiB | 0.6 MiB (1.20×) | 1 → 5 |
| 200 MiB, 50% | 103.7 MiB | 192 MiB (1.85×) | 128 MiB (1.23×) | **105 MiB (1.01×)** | 103.8 MiB (1.00×) | 2 → 12 |
| 500 MiB, 100% | 518.7 MiB | 640 MiB (1.23×) | 576 MiB (1.11×) | **520 MiB (1.00×)** | 518.8 MiB (1.00×) | 10 → 17 |

### Options
1. **Cut outputs at power-of-two sizes** (#185's suggestion). The stream's binary decomposition (51 MiB → 32 + 16 + 2 + 1). This halves the overhead, but the floor stays at twice the largest piece: an aligned class-`c` extent cannot start at unit 0.
1'. **The same, with each piece at most half the remaining stream** (recommended). Every piece then has room below it, and the pieces pack to within about a minimum piece of the live data. Pieces are full power-of-two classes up to `target_sst_bytes`, so large databases keep 64 MiB SSTs and only each stream's tail splits, into about 2·log2(tail / min piece) SSTs. The minimum piece is 1 MiB, or 64 KiB for streams under 2 MiB.
2. **A smaller `target_sst_bytes`.** This only bounds the hole, it is a throughput trade that needs bench numbers, and 1' makes it unnecessary.
3. **`shrink` splits an SST that has no hole of its class.** That is compaction-shaped work inside `shrink`, made unnecessary by 1'.
4. **Unaligned or exact-length extents, or multi-extent SSTs.** About 1.00×, but a **format change**. 0.1.0 readers reject a misaligned extent at load (`alloc::unit_of`), and multi-extent SSTs change `SstMeta`. It also needs a new allocator (best-fit over arbitrary ranges, with its own fragmentation), and it gains at most about 2% over 1' on these probes.

### Recommendation: 1', no format change
- **Where.** In `SstSink` (flush.rs), for outputs at levels ≥ 1 (compaction outputs, `Engine::compact`, backup copies). The output target becomes `min(class(target_sst_bytes), class(remaining / 2))`, rounded to a power-of-two class of at least the minimum piece, and an SST is cut once it fills its class. Cuts still fall between rows (D78), within the existing "less than an eighth left" slack.
- **Remaining.** It is estimated from the inputs: a compaction's input bytes not yet read, a flush's memtable bytes, a backup's sources. Garbage collection makes outputs smaller than inputs, so pieces can come out a class large and the last one trims to a smaller class. That costs a few percent, not 2×.
- **L0 stays one SST per flush.** Splitting flush outputs (or FIFO's L0 merges) would multiply the L0 file count and trigger compaction and the write stall sooner. Their rounding waste is transient, because L0 is compacted down.
- **FORMAT.md.** No change: extents stay aligned power-of-two classes. Only the writer's cut policy changes, and FORMAT.md §8.2 needs no edit.
- **Migration.** None is needed. 0.1.0 files open and stay valid. Each compaction rewrites its outputs in the new shape, and `compact()` then `shrink()` re-lays a whole file at once. Files written by the new code still open in 0.1.0 (same format), so downgrade works.
- **Costs.**
  - More SSTs per sorted run: on the probes 1 → 5–10 for small runs, and 10 → 17 at 500 MiB.
  - Each SST adds an index top level, filters, an open reader and a manifest entry of about 100 B.
  - Point reads still touch one SST per level, and a scan iterates the same number of runs.
  - The sparse-wide gate's data sizes put most bytes in full 64 MiB SSTs, so I expect no measurable read change; the coordinator's bench run would confirm.
- **Also folded in**, from the #200 review notes: `shrink` reserves holes for the largest extents first (or retries them after the small moves of a round), and releases a skipped extent's `busy_ssts` claim at once.

**Interim behavior:** superseded; see "Built" above.
