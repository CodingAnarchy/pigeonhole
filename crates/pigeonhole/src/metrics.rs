//! The engine's counters ([`Pigeonhole::engine_metrics`](crate::Pigeonhole::engine_metrics),
//! ICR 0044).

use crate::Durability;

/// What the writer has done since this instance opened: commits and their latency,
/// flushes, compactions, write stalls, the block cache, the WAL, file growth, the shard
/// threads, and cross-shard commits (ICR 0044).
///
/// **Cumulative counters** (most fields) start at 0 when the database opens and only grow
/// while it is open. Nothing resets them; a reopen starts again from 0. To measure an
/// interval, take two readings and subtract. **Gauges** (marked so below) report the value
/// now. Durations are nanoseconds; sizes are bytes.
///
/// **Two lifetimes.** The engine's fields restart from 0 when an automatic recovery
/// (`Options::auto_recover`) swaps in a new engine. The three transaction counters
/// (`transactions`, `transaction_conflicts`, `transaction_retries`) are kept by the handle
/// and span recoveries. Subtract readings taken across a recovery only for those three.
///
/// Every field is a sum over the shards. Reading is cheap (about 300 relaxed atomic loads
/// per shard, no I/O), so a monitoring loop can call it every second; it never waits for a
/// shard.
///
/// ```
/// # use pigeonhole::{Durability, Family, Options, Pigeonhole};
/// # let dir = pigeonhole::doc_support::temp_dir();
/// # let db = Pigeonhole::open(dir.join("m.phdb"), Options::default())?;
/// # let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// let before = db.engine_metrics();
/// t.mutate(b"row").put("f", b"q", b"v").commit()?;
/// let after = db.engine_metrics();
/// let commits = after.commits(Durability::GroupSync).count - before.commits(Durability::GroupSync).count;
/// assert_eq!(commits, 1);
/// assert_eq!(after.write_stalls - before.write_stalls, 0);
/// # drop(t);
/// # db.close()?;
/// # Ok::<(), pigeonhole::Error>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct EngineMetrics {
    commits: [CommitStats; 4],
    /// Memtable flushes completed (each writes one or more SSTs into the file).
    pub flushes: u64,
    /// Flushes that failed. They are retried after a growing backoff; the WAL keeps their
    /// data, so a failure loses nothing.
    pub flush_failures: u64,
    /// Compactions completed, background and requested (`compact()`).
    pub compactions: u64,
    /// Compactions that failed or could not start. Their slot backs off, then retries.
    pub compaction_failures: u64,
    /// Write stalls: times a shard paced or held its commits because L0 was too deep, or
    /// because its memtable arena had no room until a flush freed some. The sum of
    /// `room_stalls` and `pacing_stalls`.
    pub write_stalls: u64,
    /// Nanoseconds the shards spent in those stalls, summed over shards: the sum of
    /// `room_stall_nanos` and `pacing_stall_nanos`.
    pub write_stall_nanos: u64,
    /// Times a shard held its commits because its memtable arena had no room, until a
    /// flush freed some. Many of these point at arena pressure: too small an arena, or
    /// snapshots holding flushed memtables.
    pub room_stalls: u64,
    /// Nanoseconds the shards spent waiting for arena room, summed over shards.
    pub room_stall_nanos: u64,
    /// Times a shard paced its commits because L0 was too deep, until compaction caught
    /// up. Many of these point at compaction debt.
    pub pacing_stalls: u64,
    /// Nanoseconds the shards spent pacing commits, summed over shards.
    pub pacing_stall_nanos: u64,
    /// Block cache lookups that found the block. With the cache disabled
    /// (`Options::block_cache(0)`) nothing is counted.
    pub block_cache_hits: u64,
    /// Block cache lookups that did not, so the block was read from the file.
    pub block_cache_misses: u64,
    /// Gauge: the block cache's capacity in bytes (`Options::block_cache`).
    pub block_cache_capacity_bytes: u64,
    /// Gauge: bytes of SST top indexes and filters pinned in the block cache by the open
    /// SSTs. They count against the capacity and are never evicted: past about half of it,
    /// index partitions and data blocks no longer stay cached and a cold read costs two
    /// reads rather than one. Size the cache to at least twice this.
    pub block_cache_pinned_bytes: u64,
    /// Times `block_cache_pinned_bytes` rose past half the capacity: a warning that the
    /// cache is too small for the open SSTs' metadata. Counted at most once until the pinned
    /// bytes fall below 40% of the capacity again.
    pub block_cache_pinned_warnings: u64,
    /// Passes that flushed memtables pinning an old WAL checkpoint, to keep the WAL short.
    pub wal_unpin_passes: u64,
    /// Memtables those passes froze before they were full. Many per pass mean cold writes
    /// spread thinly over many shards.
    pub wal_unpin_flushes: u64,
    /// WAL segment rollovers that synced the full segment on a shard thread because no
    /// spare segment was ready. Expected to stay near 0.
    pub wal_inline_syncs: u64,
    /// Times a shard held a group back because its WAL segment was nearly full while that
    /// segment's header still waited for the previous segment's sync. Expected to stay
    /// near 0.
    pub wal_rollover_blocks: u64,
    /// Times the file grew because the page allocator ran out of free space.
    pub file_growths: u64,
    /// Nanoseconds those growths held the page allocator.
    pub file_growth_nanos: u64,
    /// Times the file grew ahead of need (64 MiB each, off the allocator's lock).
    pub file_growths_ahead: u64,
    /// Nanoseconds those growths' `fallocate` calls took.
    pub file_growth_ahead_nanos: u64,
    /// File reads made synchronously inside async reads: a separated value too large to
    /// cache, a block the cache could not keep, or a read that missed too often. 0 when
    /// async reads never block their executor thread.
    pub async_sync_reads: u64,
    /// Times an engine-owned shard thread found no work, spun, and parked.
    pub shard_parks: u64,
    /// Submits and notifications that found a shard asleep and woke it. Per commit, how
    /// often a commit pays a shard wakeup. Application-owned shards count only these.
    pub shard_wakes: u64,
    /// Nanoseconds the engine-owned shard threads spent working (handling messages and
    /// background tasks; not spinning or parked), summed over shards. Its rise over an
    /// interval, divided by the interval, is how many shards' worth of work the database
    /// did. Application-owned shards count nothing.
    pub shard_busy_nanos: u64,
    /// Nanoseconds the compaction pool's threads spent on the shards' flushes and
    /// compactions, summed over shards; zero with the pool off (that work is inside
    /// `shard_busy_nanos` then). Its rise over an interval, divided by the interval, is how
    /// many threads' worth of background work the database did.
    pub shard_pool_busy_nanos: u64,
    /// Nanoseconds the shard threads spent polling for their next message before parking
    /// (the spin after a handled message), summed over shards; not in `shard_busy_nanos`.
    /// Its rise over an interval, divided by the interval, is how many threads' worth of
    /// awake-but-idle time the database's clients caused.
    pub shard_spin_time_nanos: u64,
    /// Nanoseconds commits spent in flight, from `commit()` to its reply, summed over every
    /// commit completed, and over each further shard a commit occupied (its rows on more
    /// than one shard), from `commit()` to that shard's part of the decision. Its rise over
    /// an interval, divided by the interval, is the mean number of commit rounds in flight:
    /// how many clients had a write outstanding at once, each counted per shard its write
    /// occupied, which is what decides how many shards the database keeps active for them.
    pub shard_commit_inflight_nanos: u64,
    /// Blocking commit waits (`commit()`) that parked their thread after the short spin.
    /// Async commits never park.
    pub commit_parks: u64,
    /// Gauge: reader processes attached ([`Pigeonhole::open_reader`](crate::Pigeonhole::open_reader)
    /// handles holding a slot), out of [`reader_slots`](Self::reader_slots).
    pub reader_slots_active: u64,
    /// Gauge: the reader slot count ([`Options::reader_slots`](crate::Options::reader_slots)).
    pub reader_slots: u64,
    /// Reader slots freed because their process exited or was killed without closing.
    pub reader_slots_reclaimed: u64,
    /// Gauge: how many published views behind the current one the oldest reader snapshot is
    /// (0 with none). A reader holding one snapshot for a long time keeps this growing; it
    /// holds the memtables and file space since, and once the memtable budget is full,
    /// commits wait and then fail `Busy` after `write_stall_timeout`.
    pub reader_pin_view_lag: u64,
    /// Gauge: how many sequence numbers behind the visible one the oldest reader snapshot is
    /// (0 with none).
    pub reader_pin_seqno_lag: u64,
    /// Gauge: threads the engine started (shard and compaction threads). 0 in
    /// application-owned mode or once closed.
    pub threads_started: u64,
    /// Gauge: of those, threads pinned to a CPU of their own (`Options::pin_threads`).
    pub threads_pinned: u64,
    /// Gauge: why threads are unpinned although `pin_threads` asked for them, for people to
    /// read (the wording may change): the platform has no hard thread affinity (macOS), a
    /// CPU quota is narrower than the affinity set, too few free CPUs (with room for the
    /// shards only, just the compaction threads are unpinned), or the OS refused. `None`
    /// when every thread is pinned or pinning is off.
    pub unpinned_reason: Option<&'static str>,
    /// Async commits that found their commit not yet visible and waited for it.
    pub visibility_waits: u64,
    /// Waiters already queued ahead of each of those waits, summed: over
    /// `visibility_waits`, the mean queue depth.
    pub visibility_waiters_ahead: u64,
    /// Passes that woke waiting async commits as commits became visible.
    pub visibility_wake_passes: u64,
    /// Async commits those passes woke.
    pub visibility_woken: u64,
    /// Waits and wake passes that found the waiters' lock held by another thread.
    pub visibility_contended: u64,
    /// Transactions committed through this handle since open, write-free ones included
    /// (ICR 0061). Unlike the engine's counters, the three transaction counters span
    /// automatic recoveries (`Options::auto_recover`).
    pub transactions: u64,
    /// Transaction commits that failed with [`ErrorCode::Conflict`](crate::ErrorCode::Conflict).
    /// Over `transactions + transaction_conflicts`, the conflict rate.
    pub transaction_conflicts: u64,
    /// Attempts [`Pigeonhole::transact`](crate::Pigeonhole::transact) retried, for any
    /// reason its policy retries. A retry loop of the application's own (around
    /// [`RetryPolicy::next_pause`](crate::RetryPolicy::next_pause), say) is not counted.
    pub transaction_retries: u64,
    /// Cross-shard commits' two-phase rounds and the refusals that made rounds retry.
    pub cross_shard: CrossShardMetrics,
    /// Gauge: how far the largest default timestamp handed out is ahead of the clock now, in
    /// µs; 0 while the clock leads. Default timestamps run ahead of the clock only when one
    /// shard commits more than one group a µs, or a cross-shard decision and a commit after
    /// it fall in the same µs (D271, ICR 0064).
    pub default_ts_lead_micros: u64,
    /// The largest `default_ts_lead_micros` at any commit since open.
    pub max_default_ts_lead_micros: u64,
}

impl EngineMetrics {
    /// Commits at `durability` and their latency.
    pub fn commits(&self, durability: Durability) -> CommitStats {
        self.commits[durability as usize]
    }
}

/// Commits at one durability level ([`EngineMetrics::commits`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitStats {
    /// Commits completed at this level.
    pub count: u64,
    /// Median commit latency, in nanoseconds, from submission to the result, over every
    /// commit since open. Latencies are kept in half-power-of-two buckets, so this is the
    /// lower edge of the bucket the percentile falls in (within a factor of 1.5).
    pub p50_nanos: u64,
    /// The 99th percentile, as `p50_nanos`.
    pub p99_nanos: u64,
    /// The 99.9th percentile, as `p50_nanos`.
    pub p999_nanos: u64,
}

/// Cross-shard commits' two-phase rounds and the PREPAREs participants refused
/// ([`EngineMetrics::cross_shard`], ICR 0043). All cumulative since open.
///
/// A participant that refuses makes the round abort everywhere, and the commit runs again
/// in a new round (or fails with `Busy` at the write-stall timeout). `refused_rounds` over
/// the cross-shard commits in an interval is the wasted rounds per commit. One refused
/// round holds one refusal per refusing participant, so the four reasons can sum to more
/// than `refused_rounds`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CrossShardMetrics {
    /// Two-phase rounds decided.
    pub rounds: u64,
    /// Of those, rounds a participant refused. `rounds - refused_rounds` counts the rounds
    /// that committed, or failed for another reason (a conflict, an I/O error).
    pub refused_rounds: u64,
    /// Always 0 since 0.4.0: a cross-shard PREPARE carries no timestamp, so none is refused
    /// for being out of order with the participant's writes. Removed at 1.0.
    #[deprecated(note = "always 0 since 0.4.0; removed at 1.0")]
    pub refused_below_floor: u64,
    /// Always 0 since 0.4.0: a cross-shard PREPARE carries no timestamp, so none is refused
    /// below a tablet move's raise. Removed at 1.0.
    #[deprecated(note = "always 0 since 0.4.0; removed at 1.0")]
    pub refused_move_raise: u64,
    /// PREPAREs refused because a row's tablet was being split, merged or moved.
    pub refused_moved: u64,
    /// PREPAREs refused because the participant had no memtable room for its share.
    pub refused_room: u64,
    /// PREPAREs refused because they overlapped rows another cross-shard commit had
    /// prepared there and not yet decided. Such a commit is retried, not failed: only a real
    /// conflict with a write after its snapshot fails it with `Conflict`.
    pub refused_pending_overlap: u64,
    /// Singles held back behind an undecided cross-shard share (D263), once
    /// per group that held them.
    pub share_waits: u64,
    /// The time shards had a single held back behind an undecided share
    /// (D263), summed over shards.
    pub share_wait_nanos: u64,
    /// Always 0 since 0.4.0's single clock (D271): cross-shard writes to the same rows no
    /// longer wait for each other's decision, so none is chained. Kept for the API.
    pub chained: u64,
    /// Always 0 since 0.4.0's single clock (D271): nothing waits, so nothing is wounded.
    /// Kept for the API.
    pub wounds: u64,
    /// The most attempts (two-phase rounds) one committed cross-shard commit that only
    /// writes took. Not cumulative: the largest since open. Only commits that committed
    /// count: one that gave up with `ErrorCode::Busy` at the write-stall timeout never
    /// shows here, so read it together with the `Busy` failures your application saw.
    pub max_attempts_blind: u64,
    /// The same for commits that read (a transaction's validated reads), whose refusals
    /// have no hard bound: the worst case seen since open. Committed commits only, as
    /// above.
    pub max_attempts_reading: u64,
}

// The deprecated cross-shard refusal counters are always 0 (D263).
#[allow(deprecated)]
impl From<pigeonhole_engine::Metrics> for EngineMetrics {
    fn from(m: pigeonhole_engine::Metrics) -> Self {
        let x = m.cross_shard;
        Self {
            commits: std::array::from_fn(|d| CommitStats {
                count: m.commits[d],
                p50_nanos: m.commit_latency_nanos[d][0],
                p99_nanos: m.commit_latency_nanos[d][1],
                p999_nanos: m.commit_latency_nanos[d][2],
            }),
            flushes: m.flushes,
            flush_failures: m.flush_failures,
            compactions: m.compactions,
            compaction_failures: m.compaction_failures,
            write_stalls: m.stalls.0,
            write_stall_nanos: m.stalls.1,
            room_stalls: m.room_stalls.0,
            room_stall_nanos: m.room_stalls.1,
            pacing_stalls: m.pacing_stalls.0,
            pacing_stall_nanos: m.pacing_stalls.1,
            block_cache_hits: m.block_cache.0,
            block_cache_misses: m.block_cache.1,
            block_cache_capacity_bytes: m.block_cache_capacity,
            block_cache_pinned_bytes: m.block_cache_pinned,
            block_cache_pinned_warnings: m.block_cache_pinned_warnings,
            wal_unpin_passes: m.unpin.0,
            wal_unpin_flushes: m.unpin.1,
            wal_inline_syncs: m.wal_inline_syncs,
            wal_rollover_blocks: m.wal_rollover_blocks,
            file_growths: m.file_growths.0,
            file_growth_nanos: m.file_growths.1,
            file_growths_ahead: m.file_growths_ahead.0,
            file_growth_ahead_nanos: m.file_growths_ahead.1,
            async_sync_reads: m.async_sync_reads,
            default_ts_lead_micros: m.default_ts_lead_micros.0,
            max_default_ts_lead_micros: m.default_ts_lead_micros.1,
            shard_parks: m.shard_idle.0,
            shard_wakes: m.shard_idle.1,
            shard_busy_nanos: m.shard_busy_nanos,
            shard_pool_busy_nanos: m.shard_pool_busy_nanos,
            shard_spin_time_nanos: m.shard_spin_time_nanos,
            shard_commit_inflight_nanos: m.shard_commit_inflight_nanos,
            commit_parks: m.commit_parks,
            reader_slots_active: m.reader_slots.0,
            reader_slots: m.reader_slots.1,
            reader_slots_reclaimed: m.reader_slots_reclaimed,
            reader_pin_view_lag: m.reader_pin_lag.0,
            reader_pin_seqno_lag: m.reader_pin_lag.1,
            // The engine fills `pinned_threads` as (pinned, started).
            threads_started: m.pinned_threads.1,
            threads_pinned: m.pinned_threads.0,
            unpinned_reason: m.unpinned,
            visibility_waits: m.visibility_waits.0,
            visibility_waiters_ahead: m.visibility_waits.1,
            visibility_wake_passes: m.visibility_wakes.0,
            visibility_woken: m.visibility_wakes.1,
            visibility_contended: m.visibility_contended,
            // Filled from the handle's own counters (`Pigeonhole::engine_metrics`).
            transactions: 0,
            transaction_conflicts: 0,
            transaction_retries: 0,
            cross_shard: CrossShardMetrics {
                rounds: x.rounds,
                refused_rounds: x.refused_rounds,
                refused_below_floor: 0,
                refused_move_raise: 0,
                refused_moved: x.refused_moved,
                refused_room: x.refused_room,
                refused_pending_overlap: x.refused_pending_overlap,
                share_waits: x.share_waits,
                share_wait_nanos: x.share_wait_nanos,
                chained: x.chained,
                wounds: x.wounds,
                max_attempts_blind: x.max_attempts_blind,
                max_attempts_reading: x.max_attempts_reading,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_engine_counter_lands_in_its_named_field() {
        let mut m = pigeonhole_engine::Metrics::default();
        m.commits = [1, 2, 3, 4];
        m.commit_latency_nanos[Durability::Sync as usize] = [10, 20, 30];
        m.stalls = (5, 6);
        m.room_stalls = (31, 32);
        m.pacing_stalls = (33, 34);
        m.block_cache = (7, 8);
        m.unpin = (9, 10);
        m.file_growths = (11, 12);
        m.file_growths_ahead = (13, 14);
        m.shard_idle = (15, 16);
        m.pinned_threads = (3, 17); // (pinned, started)
        m.visibility_waits = (18, 19);
        m.visibility_wakes = (20, 21);
        m.cross_shard.rounds = 22;
        m.cross_shard.refused_room = 23;
        m.cross_shard.refused_pending_overlap = 24;
        let e = EngineMetrics::from(m);
        assert_eq!(e.commits(Durability::None).count, 1);
        assert_eq!(e.commits(Durability::GroupSync).count, 3);
        let sync = e.commits(Durability::Sync);
        assert_eq!(
            (sync.count, sync.p50_nanos, sync.p99_nanos, sync.p999_nanos),
            (4, 10, 20, 30)
        );
        assert_eq!((e.write_stalls, e.write_stall_nanos), (5, 6));
        assert_eq!((e.room_stalls, e.room_stall_nanos), (31, 32));
        assert_eq!((e.pacing_stalls, e.pacing_stall_nanos), (33, 34));
        assert_eq!((e.block_cache_hits, e.block_cache_misses), (7, 8));
        assert_eq!((e.wal_unpin_passes, e.wal_unpin_flushes), (9, 10));
        assert_eq!((e.file_growths, e.file_growth_nanos), (11, 12));
        assert_eq!((e.file_growths_ahead, e.file_growth_ahead_nanos), (13, 14));
        assert_eq!((e.shard_parks, e.shard_wakes), (15, 16));
        assert_eq!((e.threads_started, e.threads_pinned), (17, 3));
        assert_eq!((e.visibility_waits, e.visibility_waiters_ahead), (18, 19));
        assert_eq!((e.visibility_wake_passes, e.visibility_woken), (20, 21));
        assert_eq!((e.cross_shard.rounds, e.cross_shard.refused_room), (22, 23));
        assert_eq!(e.cross_shard.refused_pending_overlap, 24);
    }
}
