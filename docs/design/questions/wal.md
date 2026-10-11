# WAL questions

## Q: D209 (approved, number assigned): a reopened stream's first segment takes max seen + 3
FORMAT §10.1 rule 2 started the reopened segment at max seen + 1, and §10.1 said every epoch is "one more than the largest epoch in any segment header, so epochs never repeat". A successor whose header never reaches the disk breaks that, and its frames stay in the slot under that unwritten epoch. Two ways cause it:
- **The held header (main since #445, D200):** a crash while the rollover sync is in flight, after the successor's records were written.
- **A torn write (0.2.0 too):** a power loss keeps the record sectors of one header-and-records write, but not the header's.

Recovery ends at the full segment (epoch N) and reopens with N+1, usually in the same recycled slot, which isn't zero-filled. Once the new header is written, the stale frames replay as the new segment's records. Recovery never saw them, so their seqnos are reused by new commits (#508; seen as the engine model_check Protocol failure on seed 41 with deferred I/O, after an 8 → 4 shard reopen).

**Approved by the coordinator, 2026-10-11 (D209):** recovery starts the new segment at max seen + 3, where max seen covers every segment header and the replayed end. The coordinator first approved + 2; blob33's review showed a second non-durable header, and + 3 is the bound that covers it.
- **Why + 3 is enough:** at most two headers are ever not durable at once, because a header is written only after its predecessor is durable. When segment S1 fills:
  - S1's own header was written once S0's sync completed, and becomes durable only with S1's sync;
  - S2's header is held back for that same sync while S2's records are written;
  - S2's rollover waits for S1's sync before releasing S2's header, so a third non-durable header never exists.

  So frames that survive without their header carry at most the largest durable header's epoch + 2, and max seen is at least that durable epoch. The reopened segment's header is synced before anything is appended, so after a later crash max seen is at least its epoch, and skipped epochs are never reached again.
- **Evidence:**
  - a deterministic test of the double window (S1's header lost, S2's frames kept) fails at + 2 and passes at + 3;
  - a reordering power-loss sweep over Buffered rollovers catches + 1 and + 2 and passes + 3 (229,134 crash cases over 1,000 seeds).
- **0.2.0 has only the single window:** its `write_buf` waits for S1's sync before writing any of S2, so only the torn header-and-records write remains, which needs + 2. The 0.2.1 backport uses + 3 too, so FORMAT has one rule.
- **The alternatives were rejected:**
  - zero-filling a recycled slot at open costs up to a segment's worth of writes (64 MiB) per stream on every reopen after a crash;
  - holding a successor's records until its predecessor's header is durable gives up #19's overlap.
- Files written before D209 read unchanged, since chaining follows `prev_epoch`.

**Interim behavior:** as approved. `Recovery::into_stream` starts at max seen + `REOPEN_EPOCH_GAP` (3). The amended text is in FORMAT §10.1 (the epoch sentence, rule 1's window, rule 2).
- **Tests** in `crates/wal/tests/crash.rs`:
  - `records_written_under_a_held_header_never_replay_after_a_reopen_appends_nothing`;
  - `a_successor_whose_header_was_lost_over_a_recycled_slot_never_replays_after_a_reopen`;
  - `a_rollovers_double_window_never_replays_after_a_reopen` and blob33's `a_written_header_whose_sync_is_in_flight_and_the_held_one_after_it_are_both_lost` (408 of 4,096 seeds fail at + 2, none at + 3);
  - `reordering_power_losses_amid_buffered_rollovers_never_replay_new_records_after_a_reopen` (`PIGEONHOLE_SEED`/`PIGEONHOLE_SEEDS` widen it);
  - `a_stream_written_before_d209_recovers_and_reopens`, which reads a 0.2.0 fixture.
- Seed 41 with deferred I/O is in `harness_regressions_from_the_seed_sweep`.
