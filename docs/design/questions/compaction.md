# Compaction questions (Phase 2)

## Proposed decision: FIFO expiry timer and busy-window retry (#232; refines D167)
D167 noticed expiry only when a slot's maintenance ran (after a flush or compaction), so an idle FIFO family kept expired SSTs, invisible to reads, and their space until its next flush. Its window merge also gave up when the single longest L0 window held a busy file.

**Interim behavior:**
- **Expiry timer.** The added `CompactionPicker::next_expiry(levels, ttl_micros)` returns the earliest `ts_range.1 + ttl` among a FIFO-by-time family's SSTs (`None` for other styles or without a TTL). `maintain` takes the earliest still in the future across the shard's slots and arms the compaction retry timer for it (`arm_compaction_retry`, converting the wall-clock wait to monotonic nanoseconds). When it fires, `maintain` runs and drops what expired. It shares the backoff timer, which keeps the earlier deadline.
- **Stopped clock.** The timer is not armed while the existing one gave up on a stopped clock that still reads the same (D126, D161), so a frozen simulator clock cannot make the shard spin. There, expiry falls back to the next flush or compaction, as before.
- **Busy windows.** A busy L0 file now splits the windows `pick` considers, and the longest window without one merges. `score_at` still ignores busy files (D167's one exception).
