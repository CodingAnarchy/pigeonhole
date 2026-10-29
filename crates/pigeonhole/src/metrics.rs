//! The engine's counters ([`Pigeonhole::engine_metrics`](crate::Pigeonhole::engine_metrics),
//! ICR 0044).

use crate::Durability;

/// What the writer has done since this instance opened: commits and their latency,
/// flushes, compactions, write stalls, the block cache, file growth, reader slots, the
/// engine's threads, transactions and cross-shard commits (ICR 0044, ICR 0087).
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
    /// Compactions an application-owned background worker
    /// ([`Pigeonhole::open_application_owned`](crate::Pigeonhole::open_application_owned),
    /// D284) held when its shard took them back, at the close or when the worker was
    /// dropped. They were dropped before their commit: not failures, and their slots compact
    /// again.
    pub compactions_reclaimed: u64,
    /// Flushes and compactions the shards took back from the background workers' queue
    /// because no worker looked at it for 10 ms: workers configured but not driven. 0 while
    /// the workers are driven; a count that keeps growing means a loop forgot its workers.
    pub background_fallback_take_backs: u64,
    /// Flushes a commit's wait for memtable room took back from a background worker that
    /// held them for 2 ms without running them. 0 while the workers are driven.
    pub background_room_take_backs: u64,
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
    /// Times the file grew because the page allocator ran out of free space.
    pub file_growths: u64,
    /// Nanoseconds those growths held the page allocator.
    pub file_growth_nanos: u64,
    /// File reads made synchronously inside async reads: a separated value too large to
    /// cache, a block the cache could not keep, or a read that missed too often. 0 when
    /// async reads never block their executor thread.
    pub async_sync_reads: u64,
    /// Nanoseconds the engine-owned shard threads spent working (handling messages and
    /// background tasks; not spinning or parked), summed over shards. Its rise over an
    /// interval, divided by the interval, is how many shards' worth of work the database
    /// did. Application-owned shards count nothing.
    pub shard_busy_nanos: u64,
    /// Gauge: reader processes attached ([`Pigeonhole::open_reader`](crate::Pigeonhole::open_reader)
    /// handles holding a slot), out of [`reader_slots`](Self::reader_slots).
    pub reader_slots_active: u64,
    /// Gauge: the reader slot count ([`Options::reader_slots`](crate::Options::reader_slots)).
    pub reader_slots: u64,
    /// Reader slots freed because their process exited or was killed without closing.
    pub reader_slots_reclaimed: u64,
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
    /// Cross-shard commits: their two-phase rounds and the commits that failed fast.
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

/// Cross-shard commits ([`EngineMetrics::cross_shard`], ICR 0043, ICR 0087). Cumulative
/// since open.
///
/// A commit whose rows are on several shards runs as two-phase rounds. A participant that
/// cannot take its share makes the round abort everywhere, and the commit runs again in a
/// new round (or fails with `Busy` at the write-stall timeout), so `rounds` over the
/// cross-shard commits in an interval is the rounds each commit took.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CrossShardMetrics {
    /// Two-phase rounds decided.
    pub rounds: u64,
    /// Commits that failed with `Conflict` without another round: a round was refused
    /// because it overlapped another commit's undecided share, and that share committed a
    /// write to a row this commit read, after its snapshot, so a retry could only fail
    /// validation (D276, ICR 0066). Counted once per commit.
    pub failed_fast: u64,
}

impl EngineMetrics {
    pub(crate) fn from_engine(m: pigeonhole_engine::Metrics) -> Self {
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
            compactions_reclaimed: m.compactions_reclaimed,
            background_fallback_take_backs: m.background_take_backs.0,
            background_room_take_backs: m.background_take_backs.1,
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
            file_growths: m.file_growths.0,
            file_growth_nanos: m.file_growths.1,
            async_sync_reads: m.async_sync_reads,
            shard_busy_nanos: m.shard_busy_nanos,
            reader_slots_active: m.reader_slots.0,
            reader_slots: m.reader_slots.1,
            reader_slots_reclaimed: m.reader_slots_reclaimed,
            reader_pin_seqno_lag: m.reader_pin_lag.1,
            // The engine fills `pinned_threads` as (pinned, started).
            threads_started: m.pinned_threads.1,
            threads_pinned: m.pinned_threads.0,
            unpinned_reason: m.unpinned,
            // Filled from the handle's own counters (`Pigeonhole::engine_metrics`).
            transactions: 0,
            transaction_conflicts: 0,
            transaction_retries: 0,
            cross_shard: CrossShardMetrics {
                rounds: x.rounds,
                failed_fast: x.failed_fast,
            },
        }
    }
}

/// Counters outside [`EngineMetrics`], by name (`Pigeonhole::internal_metrics`, ICR 0087): the engine's mechanisms that 1.x may
/// change or remove, for benchmarks and tests. Names, and whether a name is present at all,
/// may change in any release; a reader treats a missing name as absent.
#[cfg(feature = "__unstable-hooks")]
pub(crate) fn internal(
    m: &pigeonhole_engine::Metrics,
    pooled: Option<usize>,
    nowait: Option<pigeonhole_io::NowaitReads>,
) -> Vec<(&'static str, u64)> {
    let x = &m.cross_shard;
    let mut v = vec![
        ("wal_unpin_passes", m.unpin.0),
        ("wal_unpin_flushes", m.unpin.1),
        ("wal_inline_syncs", m.wal_inline_syncs),
        ("wal_rollover_blocks", m.wal_rollover_blocks),
        ("file_growths_ahead", m.file_growths_ahead.0),
        ("file_growth_ahead_nanos", m.file_growths_ahead.1),
        ("default_ts_lead_micros", m.default_ts_lead_micros.0),
        ("max_default_ts_lead_micros", m.default_ts_lead_micros.1),
        ("shard_parks", m.shard_idle.0),
        ("shard_wakes", m.shard_idle.1),
        ("shard_pool_busy_nanos", m.shard_pool_busy_nanos),
        ("shard_spin_time_nanos", m.shard_spin_time_nanos),
        ("shard_commit_inflight_nanos", m.shard_commit_inflight_nanos),
        ("commit_parks", m.commit_parks),
        ("reader_pin_view_lag", m.reader_pin_lag.0),
        ("visibility_waits", m.visibility_waits.0),
        ("visibility_waiters_ahead", m.visibility_waits.1),
        ("visibility_wake_passes", m.visibility_wakes.0),
        ("visibility_woken", m.visibility_wakes.1),
        ("visibility_contended", m.visibility_contended),
        ("cross_shard.refused_rounds", x.refused_rounds),
        ("cross_shard.refused_moved", x.refused_moved),
        ("cross_shard.refused_room", x.refused_room),
        (
            "cross_shard.refused_pending_overlap",
            x.refused_pending_overlap,
        ),
        ("cross_shard.share_waits", x.share_waits),
        ("cross_shard.share_wait_nanos", x.share_wait_nanos),
        ("cross_shard.chained", x.chained),
        ("cross_shard.wounds", x.wounds),
        ("cross_shard.max_attempts_blind", x.max_attempts_blind),
        ("cross_shard.max_attempts_reading", x.max_attempts_reading),
    ];
    if let Some(p) = pooled {
        v.push(("io_rings.pooled", p as u64));
    }
    if let Some(n) = nowait {
        v.extend([
            ("nowait.on", u64::from(n.on)),
            ("nowait.hits", n.hits),
            ("nowait.misses", n.misses),
            ("nowait.miss_nanos", n.miss_nanos),
            ("nowait.refused", n.refused),
            ("nowait.breaks", n.breaks),
        ]);
    }
    v
}

/// What the row cache did since open
/// ([`Pigeonhole::row_cache_stats`](crate::Pigeonhole::row_cache_stats)). Cumulative from 0
/// at open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RowCacheStats {
    /// Family rows and gets answered from the cache.
    pub hits: u64,
    /// Lookups that found nothing usable (absent, older epoch, expired).
    pub misses: u64,
    /// Family rows stored.
    pub fills: u64,
    /// Cached family rows a commit moved forward to its own write (D290's write-through),
    /// instead of leaving them to miss.
    pub write_throughs: u64,
}

impl RowCacheStats {
    pub(crate) fn from_engine(engine: &pigeonhole_engine::Engine) -> Self {
        let s = engine.row_cache_stats();
        Self {
            hits: s.hits,
            misses: s.misses,
            fills: s.fills,
            write_throughs: engine.row_cache_write_throughs(),
        }
    }
}

/// One shard's share of the work, for benchmarks that check writes spread over shards
/// (ICR 0010, a hidden bench hook outside the semver promise). Counters are cumulative since
/// open; take two and subtract for a phase. The fields are the engine's
/// (`pigeonhole_engine::ShardStats`).
#[cfg(feature = "__unstable-hooks")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShardStats {
    /// Commits the shard applied, summed over durability levels.
    pub commits: u64,
    /// Tablets the shard owns in the current view, over every table.
    pub tablets: u64,
    /// Splits the shard completed.
    pub splits: u64,
    /// Merges the shard completed.
    pub merges: u64,
    /// Moves the shard completed.
    pub moves: u64,
    /// Nanoseconds the shard's engine-owned thread spent in work turns (D217).
    pub busy_nanos: u64,
    /// Nanoseconds of group-sync in-flight time (D248).
    pub sync_busy_nanos: u64,
    /// Nanoseconds the compaction pool spent on this shard's work (D256).
    pub pool_busy_nanos: u64,
    /// Nanoseconds the shard's thread spent spinning before parking (D198, D256).
    pub spin_time_nanos: u64,
    /// Nanoseconds the shard's client commits spent in flight (D257, D262).
    pub commit_inflight_nanos: u64,
}

#[cfg(feature = "__unstable-hooks")]
impl ShardStats {
    pub(crate) fn from_engine(s: pigeonhole_engine::ShardStats) -> Self {
        Self {
            commits: s.commits,
            tablets: s.tablets,
            splits: s.splits,
            merges: s.merges,
            moves: s.moves,
            busy_nanos: s.busy_nanos,
            sync_busy_nanos: s.sync_busy_nanos,
            pool_busy_nanos: s.pool_busy_nanos,
            spin_time_nanos: s.spin_time_nanos,
            commit_inflight_nanos: s.commit_inflight_nanos,
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
        let e = EngineMetrics::from_engine(m.clone());
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
        assert_eq!((e.file_growths, e.file_growth_nanos), (11, 12));
        assert_eq!((e.threads_started, e.threads_pinned), (17, 3));
        assert_eq!(e.cross_shard.rounds, 22);

        // The rest by name (ICR 0087).
        let v = internal(&m, Some(25), None);
        let get = |name: &str| v.iter().find(|(n, _)| *n == name).map(|&(_, x)| x);
        assert_eq!(
            (get("wal_unpin_passes"), get("wal_unpin_flushes")),
            (Some(9), Some(10))
        );
        assert_eq!(
            (get("file_growths_ahead"), get("file_growth_ahead_nanos")),
            (Some(13), Some(14))
        );
        assert_eq!(
            (get("shard_parks"), get("shard_wakes")),
            (Some(15), Some(16))
        );
        assert_eq!(
            (get("visibility_waits"), get("visibility_waiters_ahead")),
            (Some(18), Some(19))
        );
        assert_eq!(
            (get("visibility_wake_passes"), get("visibility_woken")),
            (Some(20), Some(21))
        );
        assert_eq!(get("cross_shard.refused_room"), Some(23));
        assert_eq!(get("cross_shard.refused_pending_overlap"), Some(24));
        assert_eq!(get("io_rings.pooled"), Some(25));
        assert_eq!(get("nowait.hits"), None, "no backend counters were given");
        let mut names: Vec<_> = v.iter().map(|(n, _)| *n).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), v.len(), "a name appears twice");
    }
}
