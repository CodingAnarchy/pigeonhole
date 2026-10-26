//! RocksDB with a hand-written wide-column key encoding ([`super::keys`]): one key per cell,
//! latest value only, each value prefixed with the cell's timestamp; the TTL filter runs on
//! read. Default RocksDB options except the [`MemoryBudget`] (write buffer, LRU block cache)
//! and a 10-bit bloom filter; no compression codecs are compiled in.
//!
//! The `metric` family lives in its own column family with FIFO compaction and the family's
//! TTL, the counterpart of Pigeonhole's `FifoByTime` (D163, #236); every other family shares
//! the default one. A scan merges both column families once a `metric` cell was written, so
//! runs that never write one (the sparse-wide gate) scan one iterator as before.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::sync::atomic::{AtomicBool, Ordering};

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompactionStyle, Direction,
    FifoCompactOptions, IteratorMode, Options, WriteBatch, WriteOptions,
};

use super::{
    BLOOM_BITS, Counted, MemoryBudget, Touched, durability, encode_value, family_id, keys,
    live_value, now_micros, read_family, scan_rows, scan_rows_filtered,
};
use crate::workload::{METRIC_FAMILY, TIME_SERIES_TTL};

/// The column family of [`METRIC_FAMILY`].
const METRIC_CF: &str = "metric";
/// The counters workload's column family (D247): one `i64` (little-endian) per row,
/// incremented with an associative add merge operator.
const COUNTER_CF: &str = "counter";

/// The counter column family's merge: the stored value plus every operand, wrapping.
fn add_i64(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    let read = |v: &[u8]| v.try_into().map_or(0, i64::from_le_bytes);
    let sum = operands
        .iter()
        .fold(existing.map_or(0, read), |n, op| n.wrapping_add(read(op)));
    Some(sum.to_le_bytes().to_vec())
}
use crate::{BenchOp, Client, Runner};

/// Runs RocksDB (feature `rocksdb`).
///
/// ```
/// use pigeonhole_bench::{RocksDbRunner, Runner};
///
/// assert_eq!(RocksDbRunner::default().name(), "rocksdb");
/// ```
#[derive(Default)]
pub struct RocksDbRunner {
    sync: bool,
    /// The load writes unsynced, the measured phase at `sync` (D247).
    load_buffered: bool,
    /// The load is running (unsynced when `load_buffered`).
    loading: bool,
    memory: MemoryBudget,
    db: Option<Arc<DB>>,
    /// Whether a `metric` cell was written: scans then merge both column families.
    metric_written: Arc<AtomicBool>,
    /// A checkpoint and a copying backup beside the measured phase (D280, #715).
    riders: bool,
    /// The store's directory (the riders' checkpoint and backup go under it).
    dir: Option<PathBuf>,
    /// The riders' thread while the measured phase runs.
    rider: Option<riders::Running>,
}

impl std::fmt::Debug for RocksDbRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbRunner")
            .field("sync", &self.sync)
            .field("memory", &self.memory)
            .field("open", &self.db.is_some())
            .finish()
    }
}

impl RocksDbRunner {
    /// `true`: fsync the WAL on every write; `false` (default): write the WAL to the OS
    /// without fsync, like Pigeonhole's `Buffered`.
    pub fn sync(mut self, yes: bool) -> Self {
        self.sync = yes;
        self
    }

    /// Write buffer size and block cache (default [`MemoryBudget::default`]).
    pub fn memory(mut self, memory: MemoryBudget) -> Self {
        self.memory = memory;
        self
    }

    /// `true`: the load writes the WAL unsynced, and the warmup and the measured phase as
    /// [`sync`](Self::sync) says (D247).
    pub fn load_buffered(mut self, yes: bool) -> Self {
        self.load_buffered = yes;
        self
    }

    /// `true`: 2 s into the measured phase, a `Checkpoint` and then a `BackupEngine` backup,
    /// each timed (D280, #715: Pigeonhole's durable backup under load against them, at the
    /// same size), reported in the record's riders.
    ///
    /// ```
    /// use pigeonhole_bench::{RocksDbRunner, Runner};
    ///
    /// assert!(RocksDbRunner::default().riders(true).describe().contains(" riders"));
    /// ```
    pub fn riders(mut self, yes: bool) -> Self {
        self.riders = yes;
        self
    }
}

/// D280's riders on RocksDB (#715): a checkpoint, then a backup that copies every file,
/// started 2 s into the measured phase, as Pigeonhole's durable backup rider starts.
mod riders {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    use rocksdb::backup::{BackupEngine, BackupEngineOptions};
    use rocksdb::checkpoint::Checkpoint;
    use rocksdb::{DB, Env};

    use crate::Riders;

    /// How far into the measured phase the checkpoint starts (Pigeonhole's backup rider's).
    const AFTER: Duration = Duration::from_secs(2);

    /// The checkpoint's and the backup's milliseconds and the checkpoint's bytes, `None`
    /// if the phase ended before they started.
    type Work = JoinHandle<Result<Option<(f64, f64, u64)>, String>>;

    pub(super) struct Running {
        stop: Arc<AtomicBool>,
        work: Work,
        checkpoint: PathBuf,
        backup: PathBuf,
    }

    impl std::fmt::Debug for Running {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Running").finish_non_exhaustive()
        }
    }

    pub(super) fn start(db: &Arc<DB>, dir: &Path) -> Running {
        let stop = Arc::new(AtomicBool::new(false));
        let (checkpoint, backup) = (dir.join("rider-checkpoint"), dir.join("rider-backup"));
        let work = {
            let (db, stop) = (Arc::clone(db), Arc::clone(&stop));
            let (checkpoint, backup) = (checkpoint.clone(), backup.clone());
            std::thread::spawn(move || {
                let begun = Instant::now();
                while begun.elapsed() < AFTER {
                    if stop.load(Ordering::Relaxed) {
                        return Ok(None);
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                let e = |e: rocksdb::Error| e.to_string();
                // Once started, both run to the end, whenever the measured phase ends, as
                // Pigeonhole's backup and compaction do.
                let t = Instant::now();
                Checkpoint::new(&*db)
                    .map_err(e)?
                    .create_checkpoint(&checkpoint)
                    .map_err(e)?;
                let checkpoint_ms = t.elapsed().as_secs_f64() * 1e3;
                let bytes = crate::dir_bytes(&checkpoint);
                let t = Instant::now();
                let opts = BackupEngineOptions::new(&backup).map_err(e)?;
                let mut engine = BackupEngine::open(&opts, &Env::new().map_err(e)?).map_err(e)?;
                engine.create_new_backup_flush(&*db, true).map_err(e)?;
                Ok(Some((
                    checkpoint_ms,
                    t.elapsed().as_secs_f64() * 1e3,
                    bytes,
                )))
            })
        };
        Running {
            stop,
            work,
            checkpoint,
            backup,
        }
    }

    impl Running {
        pub(super) fn stop(self) -> Result<Riders, String> {
            self.stop.store(true, Ordering::Relaxed);
            let done = self
                .work
                .join()
                .map_err(|_| "the rocksdb rider panicked".to_owned())??;
            // Not counted in the store's size.
            let _ = std::fs::remove_dir_all(&self.checkpoint);
            let _ = std::fs::remove_dir_all(&self.backup);
            Ok(Riders {
                checkpoint_ms: done.map(|d| d.0),
                copy_backup_ms: done.map(|d| d.1),
                backup_bytes: done.map(|d| d.2),
                ..Riders::default()
            })
        }
    }
}

struct Handle {
    db: Arc<DB>,
    write: WriteOptions,
    metric_written: Arc<AtomicBool>,
}

impl RocksDbRunner {
    fn handle(&self, db: &Arc<DB>, sync: bool) -> Handle {
        let mut write = WriteOptions::default();
        write.set_sync(sync);
        Handle {
            db: Arc::clone(db),
            write,
            metric_written: Arc::clone(&self.metric_written),
        }
    }
}

impl Runner for RocksDbRunner {
    fn name(&self) -> &'static str {
        "rocksdb"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(usize::try_from(self.memory.write_buffer).unwrap_or(usize::MAX));
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&Cache::new_lru_cache(
            usize::try_from(self.memory.cache).unwrap_or(usize::MAX),
        ));
        table.set_bloom_filter(f64::from(BLOOM_BITS), false);
        opts.set_block_based_table_factory(&table);
        opts.create_missing_column_families(true);
        // `metric`: FIFO compaction with the family's TTL (judged on file age, not on the
        // cells' event times), and no size cap, as Pigeonhole's `FifoByTime` has none.
        let mut metric = opts.clone();
        metric.set_compaction_style(DBCompactionStyle::Fifo);
        let mut fifo = FifoCompactOptions::default();
        fifo.set_max_table_files_size(u64::MAX);
        metric.set_fifo_compaction_options(&fifo);
        metric.set_ttl(TIME_SERIES_TTL.as_secs());
        let mut counter = opts.clone();
        counter.set_merge_operator_associative("i64_add", add_i64);
        let db = DB::open_cf_descriptors(
            &opts,
            dir.join("rocksdb"),
            [
                ColumnFamilyDescriptor::new(METRIC_CF, metric),
                ColumnFamilyDescriptor::new(COUNTER_CF, counter),
            ],
        )
        .map_err(|e| e.to_string())?;
        self.db = Some(Arc::new(db));
        self.dir = Some(dir.to_path_buf());
        self.loading = self.load_buffered;
        Ok(())
    }

    fn start_riders(&mut self) -> Result<(), String> {
        if !self.riders {
            return Ok(());
        }
        let db = self.db.as_ref().ok_or("rocksdb runner is not open")?;
        let dir = self.dir.as_deref().ok_or("rocksdb runner is not open")?;
        self.rider = Some(riders::start(db, dir));
        Ok(())
    }

    fn stop_riders(&mut self) -> Result<Option<crate::Riders>, String> {
        self.rider.take().map(riders::Running::stop).transpose()
    }

    fn measured_durability(&mut self) -> Result<bool, String> {
        let switched = self.loading && self.sync;
        self.loading = false;
        Ok(switched)
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        if let Some(db) = self.db.take() {
            db.flush_wal(true).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        let Some(db) = &self.db else { return Ok(()) };
        db.flush().map_err(|e| e.to_string())?;
        if let Some(cf) = db.cf_handle(METRIC_CF) {
            db.flush_cf(cf).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn background(&self) -> Option<crate::Background> {
        let db = self.db.as_ref()?;
        let prop = |name: &str| db.property_int_value(name).ok().flatten().unwrap_or(0);
        let running = prop("rocksdb.num-running-compactions")
            + prop("rocksdb.num-running-flushes")
            + prop("rocksdb.compaction-pending")
            + prop("rocksdb.mem-table-flush-pending");
        Some(crate::Background {
            work: None,
            marker: prop("rocksdb.total-sst-files-size"),
            running: Some(running > 0),
            busy_nanos: None,
        })
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        Some(Box::new(self.handle(self.db.as_ref()?, self.sync)))
    }

    fn describe(&self) -> String {
        format!(
            "{} bloom={BLOOM_BITS} {}{}{}",
            self.memory,
            durability(self.sync),
            if self.load_buffered {
                " load=buffered"
            } else {
                ""
            },
            if self.riders { " riders" } else { "" }
        )
    }
}

impl Counted for RocksDbRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        let db = self.db.as_ref().ok_or("rocksdb runner is not open")?;
        self.handle(db, self.sync && !self.loading).execute(op)
    }
}

impl Client for Handle {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        Handle::execute(self, op).map(drop)
    }
}

impl Handle {
    /// The `metric` column family for `family`, or `None` for the default one.
    fn cf(&self, family: &str) -> Result<Option<&rocksdb::ColumnFamily>, String> {
        if family != METRIC_FAMILY {
            return Ok(None);
        }
        self.db
            .cf_handle(METRIC_CF)
            .map(Some)
            .ok_or_else(|| "no metric column family".to_owned())
    }

    fn counter_cf(&self) -> Result<&rocksdb::ColumnFamily, String> {
        self.db
            .cf_handle(COUNTER_CF)
            .ok_or_else(|| "no counter column family".to_owned())
    }

    /// One atomic batch of cells of one row, all stamped `ts`.
    fn put(
        &self,
        row: &[u8],
        family: &str,
        ts: u64,
        cells: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), String> {
        self.put_rows(family, ts, std::iter::once((row, cells)))
    }

    /// One atomic batch of cells of any rows, all stamped `ts`: a cross-shard commit's
    /// counterpart is one multi-key `WriteBatch` (D246).
    fn put_rows<'a>(
        &self,
        family: &str,
        ts: u64,
        rows: impl Iterator<Item = (&'a [u8], &'a [(Vec<u8>, Vec<u8>)])>,
    ) -> Result<(), String> {
        let f = family_id(family)?;
        let cf = self.cf(family)?;
        let mut batch = WriteBatch::default();
        for (row, cells) in rows {
            for (q, v) in cells {
                let (key, value) = (keys::cell(row, f, q), encode_value(ts, v));
                match cf {
                    Some(cf) => batch.put_cf(cf, key, value),
                    None => batch.put(key, value),
                }
            }
        }
        if cf.is_some() {
            self.metric_written.store(true, Ordering::Relaxed);
        }
        self.db
            .write_opt(batch, &self.write)
            .map_err(|e| e.to_string())
    }

    fn execute(&self, op: &BenchOp) -> Result<Touched, String> {
        let e = |e: rocksdb::Error| e.to_string();
        let mut t = Touched::default();
        match op {
            BenchOp::Get {
                row,
                family,
                qualifier,
            } => {
                let f = family_id(family)?;
                let key = keys::cell(row, f, qualifier);
                let stored = match self.cf(family)? {
                    Some(cf) => self.db.get_pinned_cf(cf, key),
                    None => self.db.get_pinned(key),
                }
                .map_err(e)?;
                if let Some(v) = stored
                    && let Some(v) = live_value(f, &v, now_micros())?
                {
                    t.cell(v);
                }
            }
            BenchOp::GetRow { row, family } => {
                let mut prefix = keys::row_prefix(row);
                prefix.push(family_id(family)?);
                let mode = IteratorMode::From(&prefix, Direction::Forward);
                t = match self.cf(family)? {
                    Some(cf) => read_family(self.db.iterator_cf(cf, mode), &prefix, now_micros())?,
                    None => read_family(self.db.iterator(mode), &prefix, now_micros())?,
                };
            }
            BenchOp::Put { row, family, cells } => {
                self.put(row, family, now_micros(), cells)?;
            }
            BenchOp::PutAt {
                row,
                family,
                ts,
                cells,
            } => self.put(row, family, *ts, cells)?,
            BenchOp::DeleteColumn {
                row,
                family,
                qualifier,
            } => {
                let key = keys::cell(row, family_id(family)?, qualifier);
                match self.cf(family)? {
                    Some(cf) => self.db.delete_cf_opt(cf, key, &self.write),
                    None => self.db.delete_opt(key, &self.write),
                }
                .map_err(e)?;
            }
            BenchOp::DeleteRow { row } => {
                // Every family: a range over the row's keys in each column family that holds any.
                let from = keys::row_prefix(row);
                let mut to = from.clone();
                *to.last_mut().expect("the row terminator") += 1;
                let mut batch = WriteBatch::default();
                batch.delete_range(&from, &to);
                if self.metric_written.load(Ordering::Relaxed) {
                    let cf = self.cf(METRIC_FAMILY)?.expect("the metric family");
                    batch.delete_range_cf(cf, &from, &to);
                }
                self.db.write_opt(batch, &self.write).map_err(e)?;
            }
            BenchOp::Transfer { .. } | BenchOp::ReadModifyWrite { .. } => {
                // D274: the transactional workloads run on RocksDB's own transactions
                // (`RocksDbTxnRunner`); the plain `DB` has no atomic read-modify-write.
                return Err(format!(
                    "{op:?} runs on RocksDB's transactions (D274, RocksDbTxnRunner)"
                ));
            }
            BenchOp::Incr { row, delta } => {
                let cf = self.counter_cf()?;
                self.db
                    .merge_cf_opt(cf, row, delta.to_le_bytes(), &self.write)
                    .map_err(e)?;
            }
            BenchOp::ReadCounter { row } => {
                if let Some(v) = self.db.get_pinned_cf(self.counter_cf()?, row).map_err(e)? {
                    t.cell(&v);
                }
            }
            BenchOp::Commit { family, rows } => self.put_rows(
                family,
                now_micros(),
                rows.iter().map(|r| (r.row.as_slice(), r.cells.as_slice())),
            )?,
            BenchOp::Scan { start, len } => {
                let from = keys::row_prefix(start);
                let mode = IteratorMode::From(&from, Direction::Forward);
                let main = self.db.iterator(mode);
                t = if self.metric_written.load(Ordering::Relaxed) {
                    let cf = self.cf(METRIC_FAMILY)?.expect("the metric family");
                    let metric = self.db.iterator_cf(cf, mode);
                    scan_rows(Merged::new(main, metric), *len, now_micros())?
                } else {
                    scan_rows(main, *len, now_micros())?
                };
            }
            BenchOp::ScanFiltered {
                start,
                len,
                family,
                qualifier_prefix,
                time_range,
            } => {
                let from = keys::row_prefix(start);
                let mode = IteratorMode::From(&from, Direction::Forward);
                let f = family_id(family)?;
                let q = qualifier_prefix.as_deref();
                t = match self.cf(family)? {
                    Some(cf) => scan_rows_filtered(
                        self.db.iterator_cf(cf, mode),
                        *len,
                        now_micros(),
                        f,
                        q,
                        *time_range,
                    )?,
                    None => scan_rows_filtered(
                        self.db.iterator(mode),
                        *len,
                        now_micros(),
                        f,
                        q,
                        *time_range,
                    )?,
                };
            }
        }
        Ok(t)
    }
}

/// Two ordered key-value iterators merged in key order (their keys never collide: the
/// column families hold different families, and the family byte is part of the key).
struct Merged<A: Iterator, B: Iterator> {
    a: std::iter::Peekable<A>,
    b: std::iter::Peekable<B>,
}

type Kv = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>;

impl<A: Iterator<Item = Kv>, B: Iterator<Item = Kv>> Merged<A, B> {
    fn new(a: A, b: B) -> Self {
        Self {
            a: a.peekable(),
            b: b.peekable(),
        }
    }
}

impl<A: Iterator<Item = Kv>, B: Iterator<Item = Kv>> Iterator for Merged<A, B> {
    type Item = Kv;

    fn next(&mut self) -> Option<Kv> {
        let take_a = match (self.a.peek(), self.b.peek()) {
            (None, None) => return None,
            (Some(_), None) | (Some(Err(_)), _) => true,
            (None, Some(_)) | (_, Some(Err(_))) => false,
            (Some(Ok((ka, _))), Some(Ok((kb, _)))) => ka <= kb,
        };
        if take_a { self.a.next() } else { self.b.next() }
    }
}
