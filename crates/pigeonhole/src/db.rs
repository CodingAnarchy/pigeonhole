use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use pigeonhole_engine::{Engine, EngineShard};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

use crate::table::TableCore;
use crate::{
    Error, ErrorCode, Options, ReadTable, ReaderOptions, Result, TableBuilder, Transaction,
    WriteBatch,
};

/// What every handle derived from one open database shares: the engine, whether
/// [`Pigeonhole::close`] ran, and the largest value a commit may carry.
#[derive(Debug)]
pub(crate) struct Db {
    pub(crate) engine: Arc<Engine>,
    closed: AtomicBool,
    /// Largest value accepted at commit (decision D16), for error messages; 0 for readers.
    pub(crate) max_value: usize,
}

impl Db {
    fn new(engine: Arc<Engine>, max_value: usize) -> Arc<Self> {
        Arc::new(Self {
            engine,
            closed: AtomicBool::new(false),
            max_value,
        })
    }

    /// Fails with `Closed` once the database was closed through any handle.
    #[inline]
    pub(crate) fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(pigeonhole_engine::Error::Closed.into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn snapshot(self: &Arc<Self>) -> Result<Snapshot> {
        self.check_open()?;
        Ok(Snapshot {
            inner: self.engine.snapshot()?,
            db: Arc::downgrade(self),
        })
    }

    /// Refuses a snapshot taken from another database.
    #[inline]
    pub(crate) fn check_snapshot(self: &Arc<Self>, snapshot: &Snapshot) -> Result<()> {
        if std::ptr::eq(Weak::as_ptr(&snapshot.db), Arc::as_ptr(self)) {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::InvalidArgument,
                "the snapshot belongs to another database",
            ))
        }
    }
}

/// An open database: the one writer process's handle. Cheap to clone; every clone shares the
/// same engine and shard threads.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let users = db
///     .table("users")?
///     .family("profile", Family::default().max_versions(1))
///     .create_if_missing()?;
/// users.mutate(b"user:42").put("profile", b"name", b"Ada").commit()?;
/// assert_eq!(db.tables(), ["users"]);
/// db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Pigeonhole {
    pub(crate) db: Arc<Db>,
}

impl Pigeonhole {
    /// Opens (or creates) a database as the writer. A second writer, in this or another
    /// process, fails with [`ErrorCode::WriterLocked`](crate::ErrorCode::WriterLocked).
    /// Opening replays the WAL sidecars; there is no full-file recovery scan.
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Pigeonhole> {
        let engine_options = options.to_engine()?;
        let max_value = max_value(&engine_options);
        let shm = ShmFootprint::of(&engine_options);
        let engine = Engine::open(path.as_ref(), engine_options).map_err(|e| shm.explain(e))?;
        Ok(Pigeonhole {
            db: Db::new(engine, max_value),
        })
    }

    /// Opens a read-only handle (Phase 4). Any number of reader processes may open the same
    /// file while a writer runs; they see each commit as soon as the writer publishes it.
    ///
    /// **Readers need write access to the file.** The handle never writes, but the process
    /// opens the `.phdb` file read-write, because the coordination locks it takes are
    /// exclusive byte-range locks, which POSIX grants only on a writable descriptor (the same
    /// requirement as SQLite in WAL mode). A file on read-only media, or one the reader's
    /// user cannot write, cannot be opened this way (decision D36).
    pub fn open_reader(path: impl AsRef<Path>, options: ReaderOptions) -> Result<PigeonholeReader> {
        let engine = Engine::open_reader(path.as_ref(), options.to_engine()?)?;
        Ok(PigeonholeReader {
            db: Db::new(engine, 0),
        })
    }

    /// Opens as the writer in application-owned mode: no shard or compaction threads are
    /// started; drive each returned [`Shard`] from one of your own (typically pinned)
    /// threads. The default I/O backend still runs a pool of 2-16 I/O threads for WAL syncs
    /// and reads; a threadless mode is planned for Phase 3. Because no compaction thread
    /// is started, [`Options::compaction_cores`] with `k > 0` is refused with
    /// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument) (decision D40).
    ///
    /// Every blocking call (`commit`, `flush`, `close`) waits for the shards to run, so make
    /// sure each shard is being driven before calling one. A thread that drives a shard (it
    /// last called [`Shard::run_once`]) must not block on shard work: there `commit`,
    /// `check_and_mutate`, transaction commits, `flush` and `compact` fail with
    /// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument) before doing
    /// anything, instead of deadlocking. A thread holding a shard it has never run is not
    /// detected, so run each shard before committing from its thread. Table creation, other
    /// catalog changes and `shrink` may be called from any thread.
    ///
    /// The loop for each shard: call [`Shard::run_once`] until it returns `false`, then sleep
    /// until the [`Shard::set_wakeup`] callback fires (work arrived or I/O completed) or
    /// [`Shard::next_wakeup`] passes (background work is due), whichever comes first. An idle
    /// shard then uses no CPU, even while a write stall or a failed compaction's retry is
    /// pending, beyond a short tablet-balancer pass (with [`Options::tablet_changes`] on, the
    /// default): every 100 ms after writes, backing off to one every 10 s while idle.
    ///
    /// To close, call [`Pigeonhole::close`] and keep driving each shard the same way until
    /// [`Shard::closed`] returns `Some`, then drop it: the close's flush and syncs run on
    /// the shards, and `run_once` returns `false` while their I/O is in flight. Called on a
    /// thread that drives no shard (once every shard has been run), `close` waits for this
    /// and returns the close's result. Called on a thread that drives a shard, or before
    /// every shard has been run, it cannot wait: it returns `Ok(())` once the shards are
    /// told to close, and the close's result (a failed or unclean close) is then reported
    /// only by [`Shard::closed`].
    ///
    /// ```
    /// use std::thread;
    /// use std::time::Duration;
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let (db, shards) = Pigeonhole::open_application_owned(dir.join("app.phdb"), Options::default().shards(2))?;
    /// let threads: Vec<_> = shards
    ///     .into_iter()
    ///     .map(|mut shard| {
    ///         thread::spawn(move || -> pigeonhole::Result<()> {
    ///             let me = thread::current();
    ///             shard.set_wakeup(Box::new(move || me.unpark()));
    ///             loop {
    ///                 // Your event loop: run the shard until it is idle, do your own work...
    ///                 while shard.run_once(Duration::from_micros(200)) {}
    ///                 // ...and stop once the database's close has finished on every shard.
    ///                 if let Some(closed) = shard.closed() {
    ///                     return closed;
    ///                 }
    ///                 // ...then sleep until work arrives or background work is due.
    ///                 match shard.next_wakeup() {
    ///                     Some(due) => thread::park_timeout(due),
    ///                     None => thread::park(),
    ///                 }
    ///             }
    ///         })
    ///     })
    ///     .collect();
    ///
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"v").commit()?;
    /// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"v");
    ///
    /// drop(t);
    /// // Waits for the shard threads to finish the close, and returns its result.
    /// db.close()?;
    /// for thread in threads {
    ///     thread.join().unwrap()?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_application_owned(
        path: impl AsRef<Path>,
        options: Options,
    ) -> Result<(Pigeonhole, Vec<Shard>)> {
        let engine_options = options.to_engine()?;
        let vfs = Arc::clone(&engine_options.vfs);
        let max_value = max_value(&engine_options);
        let shm = ShmFootprint::of(&engine_options);
        let (engine, shards) = Engine::open_application_owned(path.as_ref(), engine_options)
            .map_err(|e| shm.explain(e))?;
        let shards = shards
            .into_iter()
            .map(|inner| Shard {
                inner,
                vfs: Arc::clone(&vfs),
            })
            .collect();
        Ok((
            Pigeonhole {
                db: Db::new(engine, max_value),
            },
            shards,
        ))
    }

    /// Starts defining or opening a table: chain `.family(..)` and finish with
    /// `.create_if_missing()`, `.create()` or `.open()`.
    pub fn table(&self, name: &str) -> Result<TableBuilder<'_>> {
        Ok(TableBuilder::new(self, name))
    }

    /// Names of every table.
    pub fn tables(&self) -> Vec<String> {
        self.db
            .engine
            .tables()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    }

    /// Drops a table and all its data.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let info = self
            .db
            .engine
            .table(name)
            .ok_or_else(|| pigeonhole_engine::Error::TableNotFound(name.to_owned()))?;
        Ok(self.db.engine.drop_table(info.id)?)
    }

    /// A batch of writes across any rows and tables, committed with one durability point.
    pub fn write_batch(&self) -> WriteBatch {
        WriteBatch::new(Arc::clone(&self.db))
    }

    /// Starts an optimistic multi-row transaction (Phase 4).
    pub fn transaction(&self) -> Result<Transaction> {
        self.db.check_open()?;
        Ok(Transaction::new(
            Arc::clone(&self.db),
            self.db.engine.begin()?,
        ))
    }

    /// A consistent snapshot of everything committed so far.
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.db.snapshot()
    }

    /// Per-shard commits, tablets and tablet changes since open, indexed by shard: a bench
    /// hook (ICR 0010) that shows whether one table's writes spread over the shards.
    #[doc(hidden)]
    pub fn shard_stats(&self) -> Vec<crate::ShardStats> {
        self.db.engine.shard_stats()
    }

    /// The engine's counters since open: commits, flushes, compactions, write stalls, WAL
    /// unpin passes, inline WAL syncs and file growths. A bench hook (ICR 0015) that shows
    /// what stalled during a run; take two readings and subtract for a phase.
    #[doc(hidden)]
    pub fn engine_metrics(&self) -> crate::EngineMetrics {
        self.db.engine.metrics()
    }

    /// File reads made synchronously inside async reads since open (D196, #398): a
    /// separated value, or a block the cache could not keep. Zero means no async read
    /// blocked its executor thread on the file.
    #[cfg(feature = "async")]
    pub fn async_sync_reads(&self) -> u64 {
        self.db.engine.metrics().async_sync_reads
    }

    /// The writer default durability.
    pub fn default_durability(&self) -> Durability {
        self.db.engine.default_durability()
    }

    /// Changes the writer default; applies to commits that start afterwards.
    pub fn set_default_durability(&self, durability: Durability) {
        self.db.engine.set_default_durability(durability);
    }

    /// Writes every memtable into the file and returns once the data is there (the SSTs are
    /// in the manifest). Afterwards even [`Durability::None`] commits survive a crash, and
    /// the WAL behind the flushed data is checkpointed. Background flushes run on their own
    /// as memtables fill; call this to make everything so far durable in the file.
    ///
    /// Each flushed memtable needs a fresh one from the memtable arena. When snapshots pin
    /// the arena's memory, the flush waits as a stalled write does, and fails with
    /// [`ErrorCode::Busy`](crate::ErrorCode::Busy) if no room frees up within the
    /// write-stall timeout. Nothing is lost: drop old snapshots and retry.
    ///
    /// ```
    /// use pigeonhole::{Durability, Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let path = dir.join("app.phdb");
    /// let db = Pigeonhole::open(&path, Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"v").durability(Durability::None).commit()?;
    /// db.flush()?; // now in the file, whatever the commit's durability level was
    /// # drop(t);
    /// db.close()?;
    ///
    /// let db = Pigeonhole::open(&path, Options::default().shards(1))?;
    /// let t = db.table("t")?.open()?;
    /// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"v");
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush(&self) -> Result<()> {
        Ok(self.db.engine.flush()?)
    }

    /// Compacts every table fully: flushes, then merges every level into the last one.
    /// Versions beyond a family's `max_versions`, expired cells and tombstones that no
    /// snapshot can still need are dropped. Compaction otherwise runs in the background;
    /// call this after a bulk load or delete to reclaim space and speed up reads. Its flush
    /// can fail with [`ErrorCode::Busy`](crate::ErrorCode::Busy) as
    /// [`flush`](Self::flush) does.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let t = db
    ///     .table("t")?
    ///     .family("f", Family::default().max_versions(1))
    ///     .create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"old").commit()?;
    /// t.mutate(b"row").put("f", b"q", b"new").commit()?;
    /// db.compact()?;
    /// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"new");
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn compact(&self) -> Result<()> {
        Ok(self.db.engine.compact(None)?)
    }

    /// Writes a consistent single-file copy to `dest`, which must not exist, while writes
    /// continue. The copy holds exactly the commits visible when this is called and opens
    /// on its own, without WAL replay or sidecar files. Separated values (above a family's
    /// `blob_threshold`) are copied into the copy's own blob files, which hold only the
    /// values the copy references.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"row").put("f", b"q", b"v").commit()?;
    ///
    /// let copy_path = dir.join("backup.phdb");
    /// db.backup(&copy_path)?;
    /// t.mutate(b"row").put("f", b"q", b"later").commit()?; // not in the backup
    ///
    /// let copy = Pigeonhole::open(&copy_path, Options::default().shards(1).create_if_missing(false))?;
    /// let ct = copy.table("t")?.open()?;
    /// assert_eq!(ct.get(b"row", "f", b"q")?.unwrap().value(), b"v");
    /// # drop(ct);
    /// # copy.close()?;
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        Ok(self.db.engine.backup(dest.as_ref())?)
    }

    /// Returns free space at the end of the file to the filesystem and reports the bytes
    /// released. The file never shrinks by itself: space freed by compaction or by deleting
    /// rows is reused by later writes, but the file keeps its length. Call this after a
    /// large delete followed by [`compact`](Self::compact) when you need
    /// the disk space back (for example before copying the file, or on a small device).
    ///
    /// It cuts off the free space at the end of the file, moves the live data that sits past
    /// the point where the file could end into free space nearer the start, and truncates
    /// again. The cost is reading and rewriting that data (at most the live bytes in the
    /// file's tail) and a manifest commit per round, so run it rarely. It runs online: reads,
    /// writes, flushes and compactions continue on other threads, and in-flight flush or
    /// compaction output is never moved. It returns `0` when there is nothing to release,
    /// for example before `compact` has freed anything. Space still held by an open
    /// snapshot or scan is not released until that handle is dropped; call `shrink` again
    /// afterwards.
    ///
    /// The file can end no earlier than its live data packed toward the start. Data lives in
    /// extents of a power of two from 64 KiB to 64 MiB, each aligned to its size, and the
    /// first 64 KiB of the file is the header: a file holding one 64 MiB extent (a large
    /// SST) is at least 128 MiB, however little else it holds. Data with no free extent of
    /// its size below it stays where it is; that is not an error.
    ///
    /// Errors: [`ErrorCode::Closed`](crate::ErrorCode::Closed) after `close`;
    /// [`ErrorCode::ReadOnly`](crate::ErrorCode::ReadOnly) if this handle cannot write;
    /// [`ErrorCode::NoSpace`](crate::ErrorCode::NoSpace) when the disk is full and moving
    /// the manifest needs the file to grow by its few KiB first (free space and retry);
    /// [`ErrorCode::Io`](crate::ErrorCode::Io) on a disk failure. A failed shrink loses no
    /// data; if it fails while committing the manifest, reopen the database.
    ///
    /// ```
    /// use pigeonhole::{Durability, Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let path = dir.join("app.phdb");
    /// let db = Pigeonhole::open(&path, Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"keep").put("f", b"q", b"v").commit()?;
    /// let junk = db.table("junk")?.family("f", Family::default()).create_if_missing()?;
    /// for i in 0..2_000u32 {
    ///     junk.mutate(&i.to_be_bytes()).put("f", b"q", &[7u8; 512]).durability(Durability::None).commit()?;
    /// }
    /// db.flush()?;
    /// for i in 0..2_000u32 {
    ///     junk.mutate(&i.to_be_bytes()).delete_row().durability(Durability::None).commit()?;
    /// }
    /// # drop(junk);
    /// db.compact()?;
    ///
    /// let before = std::fs::metadata(&path).unwrap().len();
    /// let released = db.shrink()?;
    /// let after = std::fs::metadata(&path).unwrap().len();
    /// assert_eq!(before - after, released);
    /// assert_eq!(t.get(b"keep", "f", b"q")?.unwrap().value(), b"v");
    /// # drop(t);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn shrink(&self) -> Result<u64> {
        Ok(self.db.engine.shrink()?)
    }

    /// Closes this handle's database. Flushes every memtable and checkpoints the WAL; if
    /// this is the last process with the database open, also removes the sidecar files and
    /// shared-memory region, leaving one file. Dropping the last clone does the same,
    /// ignoring errors.
    ///
    /// A crash instead of a clean close leaves the sidecars; the next open replays them.
    /// Every handle derived from this database (tables, batches) fails with
    /// [`ErrorCode::Closed`](crate::ErrorCode::Closed) afterwards.
    pub fn close(self) -> Result<()> {
        self.db.closed.store(true, Ordering::Release);
        Ok(self.db.engine.close()?)
    }
}

/// A read-only handle, typically in another process. Has no write methods, so misuse does
/// not compile. Reads run on the caller's thread.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole, ReaderOptions};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let path = dir.join("shared.phdb");
/// let db = Pigeonhole::open(&path, Options::default().shards(1))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// t.mutate(b"row").put("f", b"q", b"v").commit()?;
///
/// // Normally in another process.
/// let reader = Pigeonhole::open_reader(&path, ReaderOptions::default())?;
/// let rt = reader.table("t")?;
/// assert_eq!(rt.get(b"row", "f", b"q")?.unwrap().value(), b"v");
/// # drop(rt);
/// # drop(reader);
/// # drop(t);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PigeonholeReader {
    db: Arc<Db>,
}

impl PigeonholeReader {
    /// File reads made synchronously inside async reads since open (D196, #398): a
    /// separated value, or a block the cache could not keep. Zero means no async read
    /// blocked its executor thread on the file.
    #[cfg(feature = "async")]
    pub fn async_sync_reads(&self) -> u64 {
        self.db.engine.metrics().async_sync_reads
    }

    /// Opens an existing table for reading.
    pub fn table(&self, name: &str) -> Result<ReadTable> {
        let info = self
            .db
            .engine
            .table(name)
            .ok_or_else(|| pigeonhole_engine::Error::TableNotFound(name.to_owned()))?;
        Ok(ReadTable {
            core: Arc::new(TableCore::new(Arc::clone(&self.db), info)),
        })
    }

    /// Names of every table.
    pub fn tables(&self) -> Vec<String> {
        self.db
            .engine
            .tables()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    }

    /// A consistent snapshot of what the writer has published. May do I/O: re-attach after
    /// a writer restart and reload the manifest if it changed.
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.db.snapshot()
    }
}

/// A point-in-time view for reads. Holding it pins the data it can see; drop it promptly.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// t.mutate(b"row").put("f", b"q", b"old").commit()?;
///
/// let snap = db.snapshot()?;
/// t.mutate(b"row").put("f", b"q", b"new").commit()?;
///
/// assert_eq!(t.get_at(&snap, b"row", "f", b"q")?.unwrap().value(), b"old");
/// assert_eq!(t.get(b"row", "f", b"q")?.unwrap().value(), b"new");
/// drop(snap);
/// # drop(t);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) inner: pigeonhole_engine::Snapshot,
    /// The database it was taken from (weak: a snapshot does not keep the database open).
    db: Weak<Db>,
}

impl Snapshot {
    /// The snapshot's sequence number.
    pub fn seqno(&self) -> u64 {
        self.inner.seqno()
    }
}

/// One shard in application-owned mode. Move it to the thread that should run it and call
/// [`Shard::run_once`] from that thread's event loop.
///
/// See [`Pigeonhole::open_application_owned`] for an example.
#[derive(Debug)]
pub struct Shard {
    inner: EngineShard,
    vfs: VfsRef,
}

impl Shard {
    /// Shard index.
    pub fn index(&self) -> usize {
        usize::from(self.inner.index())
    }

    /// Runs queued writes, group commit and background work for up to `budget`. Returns
    /// whether work remains.
    ///
    /// Background work waiting for a time (a write stall's pacing, a wait for memtable room,
    /// a failed compaction's retry, the tablet balancer's next pass) is not work that remains: with only that left it returns
    /// `false`, and [`next_wakeup`](Shard::next_wakeup) says when to call it again. The
    /// callback registered with [`set_wakeup`](Shard::set_wakeup) does not fire for it.
    pub fn run_once(&mut self, budget: std::time::Duration) -> bool {
        let budget = u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX);
        let deadline = self.vfs.monotonic_nanos().saturating_add(budget);
        self.inner.run_once(deadline)
    }

    /// How long until background work on this shard is due (a write stall's pacing, a wait
    /// for memtable room, a failed compaction's retry, the tablet balancer's next pass), or
    /// `None` when none is waiting for a time. `Some(Duration::ZERO)` means it is due now.
    /// With [`Options::tablet_changes`] on (the default) the balancer's next pass is always
    /// pending, so this is never `None`; it is bounded by the balancer's current interval:
    /// 100 ms after a write or a tablet change, doubling with each idle pass up to 10 s.
    ///
    /// After [`run_once`](Shard::run_once) returns `false`, sleep until the
    /// [`set_wakeup`](Shard::set_wakeup) callback fires or this much time passes, whichever
    /// comes first, then call `run_once` again. See [`Pigeonhole::open_application_owned`]
    /// for the loop.
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::{Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let (db, mut shards) = Pigeonhole::open_application_owned(dir.join("w.phdb"), Options::default().shards(1))?;
    /// let shard = &mut shards[0];
    /// while shard.run_once(Duration::from_micros(200)) {}
    /// // Idle: only the tablet balancer's next pass is waiting for a time (at most 10 s).
    /// assert!(shard.next_wakeup().is_some_and(|d| d <= Duration::from_secs(10)));
    /// db.close()?;
    /// // This thread drives the shard, so `close` cannot wait: drive it until it has closed
    /// // (a real loop sleeps between calls as above).
    /// while shard.closed().is_none() {
    ///     shard.run_once(Duration::from_micros(200));
    /// }
    /// shard.closed().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_wakeup(&self) -> Option<std::time::Duration> {
        let deadline = self.inner.next_deadline()?;
        let now = self.vfs.monotonic_nanos();
        Some(std::time::Duration::from_nanos(
            deadline.saturating_sub(now),
        ))
    }

    /// The database's close result once the close has finished (every shard closed, and the
    /// clean close recorded), or `None` before (including before [`Pigeonhole::close`]).
    /// After `close`, keep driving the shard until this is `Some`, then drop it. Every shard
    /// reports the same result.
    pub fn closed(&self) -> Option<Result<()>> {
        self.inner.closed().map(|r| r.map_err(Into::into))
    }

    /// Registers a callback invoked (from any thread) when work arrives for this shard. It
    /// does not fire when background work falls due: see [`next_wakeup`](Shard::next_wakeup).
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        self.inner.set_wakeup(wake);
    }
}

/// The shared-memory region a writer open creates, kept to explain `ShmUnavailable`: what it
/// needed, where, and how to make it fit.
struct ShmFootprint {
    shards: u64,
    budget: u64,
    /// The whole region: every arena (each rounded up to 2 MiB) plus the header, the two view
    /// buffers and the reader slots, laid out as the engine lays it out.
    region_len: u64,
    dir: Option<std::path::PathBuf>,
}

impl ShmFootprint {
    fn of(o: &pigeonhole_engine::EngineOptions) -> Self {
        // As the engine resolves it: 0 shards means one per available CPU.
        let shards = match o.shards {
            0 => pigeonhole_io::sys::available_cpus().max(1),
            n => n,
        };
        let count = u32::try_from(shards).unwrap_or(u32::MAX);
        let config = pigeonhole_shm::ShmConfig::new(count);
        let layout = pigeonhole_format::shm::ShmHeader::layout(
            [0; 16],
            count,
            o.reader_slots.max(1),
            config.view_buffer_bytes,
            o.memtable_budget,
            0,
            0,
        );
        Self {
            shards: shards as u64,
            budget: o.memtable_budget,
            region_len: layout.region_len,
            dir: o.shm_dir.clone(),
        }
    }

    /// `e` as an [`Error`], with the footprint and remedies when it is `ShmUnavailable`.
    fn explain(&self, e: pigeonhole_engine::Error) -> Error {
        if !matches!(e, pigeonhole_engine::Error::ShmUnavailable) {
            return e.into();
        }
        let arenas = self.shards.saturating_mul(self.budget);
        let need = format!(
            "{} ({} shards × {} memtable_budget, plus {} for views and reader slots)",
            bytes(self.region_len),
            self.shards,
            bytes(self.budget),
            bytes(self.region_len.saturating_sub(arenas)),
        );
        let fix = "or lower Options::memtable_budget or Options::shards";
        let message = match &self.dir {
            Some(dir) => format!(
                "the shared-memory region could not be created in {}: it needs {need}. The \
                 directory must exist and have that much free space; free some, point \
                 Options::shm_dir at a larger one, {fix}",
                dir.display()
            ),
            None if cfg!(any(target_os = "linux", target_os = "android")) => format!(
                "the shared-memory region could not be created in /dev/shm: it needs {need}, \
                 and /dev/shm is too small or missing (Docker and Kubernetes default it to 64 \
                 MiB). Enlarge it (docker run --shm-size; in Kubernetes, mount an emptyDir \
                 with medium: Memory at /dev/shm), point Options::shm_dir at a larger tmpfs, \
                 {fix}"
            ),
            None => format!(
                "the shared-memory region could not be created: it needs {need} of shared \
                 memory, more than the system would commit. Free memory, point \
                 Options::shm_dir at a directory with room, {fix}"
            ),
        };
        Error::new(ErrorCode::ShmUnavailable, message)
    }
}

/// `n` bytes in MiB when whole, else KiB (rounded up).
fn bytes(n: u64) -> String {
    if n.is_multiple_of(1 << 20) {
        format!("{} MiB", n >> 20)
    } else {
        format!("{} KiB", n.div_ceil(1 << 10))
    }
}

/// The largest value a commit carries inline (decision D16), as `pigeonhole-engine` computes
/// it at open: the WAL segment payload, 64 MiB, and half a shard's memtable arena. A longer
/// put is separated into a blob file at commit time; a longer merge operand is refused.
fn max_value(o: &pigeonhole_engine::EngineOptions) -> usize {
    (o.wal.segment_size as usize)
        .saturating_sub(64 * 1024)
        .min(64 << 20)
        .min((o.memtable_budget / 2) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn footprint(shards: usize, budget: u64, dir: Option<&str>) -> ShmFootprint {
        let mut o = pigeonhole_engine::EngineOptions::new(pigeonhole_io::sim::SimVfs::new(1));
        o.shards = shards;
        o.memtable_budget = budget;
        o.shm_dir = dir.map(Into::into);
        ShmFootprint::of(&o)
    }

    #[test]
    fn shm_unavailable_names_the_footprint_and_the_remedies() {
        let e = footprint(4, 64 << 20, None).explain(pigeonhole_engine::Error::ShmUnavailable);
        assert_eq!(e.code(), ErrorCode::ShmUnavailable);
        let m = e.message();
        assert!(
            m.contains("266 MiB (4 shards × 64 MiB memtable_budget, plus 10 MiB for views"),
            "{m}"
        );
        assert!(
            m.contains("Options::memtable_budget or Options::shards"),
            "{m}"
        );
        assert!(m.contains("Options::shm_dir"), "{m}");
        if cfg!(target_os = "linux") {
            assert!(m.contains("/dev/shm") && m.contains("--shm-size"), "{m}");
        }

        let e = footprint(3, 192 << 10, Some("/mnt/small"))
            .explain(pigeonhole_engine::Error::ShmUnavailable);
        assert_eq!(e.code(), ErrorCode::ShmUnavailable);
        let m = e.message();
        assert!(
            // Each arena rounds up to 2 MiB.
            m.contains("in /mnt/small: it needs 16 MiB (3 shards × 192 KiB memtable_budget"),
            "{m}"
        );
    }

    #[test]
    fn other_open_errors_pass_through() {
        let e = footprint(1, 1 << 20, None).explain(pigeonhole_engine::Error::WriterLocked);
        assert_eq!(e.code(), ErrorCode::WriterLocked);
        assert!(!e.message().contains("shared-memory region"));
    }
}
