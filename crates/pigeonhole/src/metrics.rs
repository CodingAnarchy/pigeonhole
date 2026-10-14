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
    /// because its memtable arena had no room until a flush freed some.
    pub write_stalls: u64,
    /// Nanoseconds the shards spent in those stalls, summed over shards.
    pub write_stall_nanos: u64,
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
    /// Blocking commit waits (`commit()`) that parked their thread after the short spin.
    /// Async commits never park.
    pub commit_parks: u64,
    /// Gauge: threads the engine started (shard and compaction threads). 0 in
    /// application-owned mode or once closed.
    pub threads_started: u64,
    /// Gauge: of those, threads pinned to a CPU of their own (`Options::pin_threads`).
    pub threads_pinned: u64,
    /// Gauge: why no thread is pinned although `pin_threads` asked for it, for people to
    /// read (the wording may change): the platform has no hard thread affinity (macOS), a
    /// CPU quota is narrower than the affinity set, too few free CPUs, or the OS refused.
    /// `None` when every thread is pinned or pinning is off.
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
    /// Cross-shard commits' two-phase rounds and the refusals that made rounds retry.
    pub cross_shard: CrossShardMetrics,
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
    /// PREPAREs refused because the commit timestamp was out of order with a write the
    /// participant had already made (a later write at a lower timestamp, or an earlier one at
    /// a higher timestamp). The commit is retried with a fresh timestamp.
    pub refused_below_floor: u64,
    /// PREPAREs refused because the commit timestamp was below the floor a tablet move
    /// raised the participant to.
    pub refused_move_raise: u64,
    /// PREPAREs refused because a row's tablet was being split, merged or moved.
    pub refused_moved: u64,
    /// PREPAREs refused because the participant had no memtable room for its share.
    pub refused_room: u64,
}

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
            shard_parks: m.shard_idle.0,
            shard_wakes: m.shard_idle.1,
            shard_busy_nanos: m.shard_busy_nanos,
            commit_parks: m.commit_parks,
            // The engine fills `pinned_threads` as (pinned, started).
            threads_started: m.pinned_threads.1,
            threads_pinned: m.pinned_threads.0,
            unpinned_reason: m.unpinned,
            visibility_waits: m.visibility_waits.0,
            visibility_waiters_ahead: m.visibility_waits.1,
            visibility_wake_passes: m.visibility_wakes.0,
            visibility_woken: m.visibility_wakes.1,
            visibility_contended: m.visibility_contended,
            cross_shard: CrossShardMetrics {
                rounds: x.rounds,
                refused_rounds: x.refused_rounds,
                refused_below_floor: x.refused_below_floor,
                refused_move_raise: x.refused_move_raise,
                refused_moved: x.refused_moved,
                refused_room: x.refused_room,
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
        let e = EngineMetrics::from(m);
        assert_eq!(e.commits(Durability::None).count, 1);
        assert_eq!(e.commits(Durability::GroupSync).count, 3);
        let sync = e.commits(Durability::Sync);
        assert_eq!(
            (sync.count, sync.p50_nanos, sync.p99_nanos, sync.p999_nanos),
            (4, 10, 20, 30)
        );
        assert_eq!((e.write_stalls, e.write_stall_nanos), (5, 6));
        assert_eq!((e.block_cache_hits, e.block_cache_misses), (7, 8));
        assert_eq!((e.wal_unpin_passes, e.wal_unpin_flushes), (9, 10));
        assert_eq!((e.file_growths, e.file_growth_nanos), (11, 12));
        assert_eq!((e.file_growths_ahead, e.file_growth_ahead_nanos), (13, 14));
        assert_eq!((e.shard_parks, e.shard_wakes), (15, 16));
        assert_eq!((e.threads_started, e.threads_pinned), (17, 3));
        assert_eq!((e.visibility_waits, e.visibility_waiters_ahead), (18, 19));
        assert_eq!((e.visibility_wake_passes, e.visibility_woken), (20, 21));
        assert_eq!((e.cross_shard.rounds, e.cross_shard.refused_room), (22, 23));
    }
}
