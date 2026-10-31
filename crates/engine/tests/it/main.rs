//! The engine's integration tests that can share one process, one module per file (#1181):
//! one test binary instead of one each, so `tests/common` compiles once and the crate graph
//! links once. Their tests are named `it::<file>::<test>` (`cargo test -p pigeonhole-engine
//! --test it <file>::`).
//!
//! The files beside this directory stay binaries of their own: the targets a sweep or a CI
//! step names with `--test` (`model_check`, `counters`, `tablets`, `slots`, `deferred_waits`,
//! `api`), and the tests that need a process to themselves: `alloc_budget` (its own global
//! allocator), `footprint` (the process's memory), `idle_cpu`, `idle_spin` and `wait_parks`
//! (the process's CPU time), `reader_fork` and `writer_fork` (they fork), `manifest_wake` (a
//! reaper-less backend registered anywhere in the process hides the wake it tests) and
//! `tail_index` (it asserts on the process-wide jump counters). A new
//! integration test goes here, as a module below, unless it is one of those kinds.

#[path = "../common/mod.rs"]
mod common;
#[path = "../gate/mod.rs"]
mod gate;

mod admission;
mod aggregates;
mod applier_release;
mod background_workers;
mod blobs;
mod blocking_commits;
mod blocking_waits;
mod busy_nanos;
mod check;
mod checkpoint_budget;
mod checkpoint_creation;
mod close_crossed;
mod close_failures;
mod close_voided_flush;
mod coarse_clock;
mod compaction_styles;
mod conditional_groups;
mod counter_checkpoints;
mod cross_checkpoints;
mod durable_freeze;
mod durable_snapshot;
mod early_decide;
mod fifo_expiry;
mod file_growth;
mod flush_fold;
mod flush_gc;
mod flush_publish;
mod flushed_through;
mod format_upgrade;
mod freeze_wake;
mod full_compaction_purge;
mod group_sync_batch;
mod handover;
mod handover_real;
mod health;
mod held_header_visibility;
mod huge_value;
mod idle_counts;
mod inflight_window;
mod large_values;
mod milestone_b;
mod network_fs;
mod open_cost;
mod pager_growth;
mod poisoned_flush_barrier;
mod purge_bound;
mod purge_race;
mod read_only_fallback;
mod reader_backup;
mod reader_crash_matrix;
mod reader_pins;
mod reader_process;
mod reader_reclaim;
mod reader_reclaim_stall;
mod reader_row_cache;
mod reclaim;
mod reduce;
mod review;
mod room_freeze;
mod room_group;
mod row_cache_write_through;
mod scan_readahead_limit;
mod shrink;
mod smoke;
mod spin_pool_terms;
mod stall_backoff;
mod stall_deadline;
mod sync_faults;
mod vote_on_append;
mod wal_pin;
mod wal_spares_steps;
