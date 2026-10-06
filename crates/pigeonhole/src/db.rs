use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineShard};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

use crate::table::TableCore;
use crate::{Options, ReadTable, ReaderOptions, Result, TableBuilder, Transaction, WriteBatch};

/// An open database: the one writer process's handle. Cheap to clone; every clone shares the
/// same engine and shard threads.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = std::env::temp_dir().join(format!("pigeonhole-doc-{}-db", std::process::id()));
/// # std::fs::create_dir_all(&dir).unwrap();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
/// let users = db
///     .table("users")?
///     .family("profile", Family::default().max_versions(1))
///     .create_if_missing()?;
/// users.mutate(b"user:42").put("profile", b"name", b"Ada").commit()?;
/// assert_eq!(db.tables(), ["users"]);
/// db.close()?;
/// # std::fs::remove_dir_all(&dir).unwrap();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Pigeonhole {
    pub(crate) engine: Arc<Engine>,
}

impl Pigeonhole {
    /// Opens (or creates) a database as the writer. A second writer, in this or another
    /// process, fails with [`ErrorCode::WriterLocked`](crate::ErrorCode::WriterLocked).
    /// Opening replays the WAL sidecars; there is no full-file recovery scan.
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Pigeonhole> {
        let engine = Engine::open(path.as_ref(), options.to_engine())?;
        Ok(Pigeonhole { engine })
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
        let engine = Engine::open_reader(path.as_ref(), options.to_engine())?;
        Ok(PigeonholeReader { engine })
    }

    /// Opens as the writer in application-owned mode: no threads are started; drive each
    /// returned [`Shard`] from one of your own (typically pinned) threads. Because no thread
    /// is started, [`Options::compaction_cores`] with `k > 0` is refused with
    /// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument) (decision D40).
    ///
    /// Every blocking call (`commit`, `flush`, table creation) waits for a shard to run, so
    /// make sure each shard is being driven before calling one. After
    /// [`Pigeonhole::close`], keep driving each shard until [`Shard::run_once`] returns
    /// `false`, then drop it.
    ///
    /// ```
    /// use std::time::Duration;
    /// use pigeonhole::{Family, Options, Pigeonhole};
    ///
    /// # fn main() -> pigeonhole::Result<()> {
    /// # let dir = std::env::temp_dir().join(format!("pigeonhole-doc-{}-app-owned", std::process::id()));
    /// # std::fs::create_dir_all(&dir).unwrap();
    /// let (db, shards) = Pigeonhole::open_application_owned(dir.join("app.phdb"), Options::default().shards(2))?;
    /// let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    /// let threads: Vec<_> = shards
    ///     .into_iter()
    ///     .map(|mut shard| {
    ///         let stop = stop.clone();
    ///         std::thread::spawn(move || {
    ///             // Your event loop: run the shard, then do your own work.
    ///             while shard.run_once(Duration::from_micros(200))
    ///                 || !stop.load(std::sync::atomic::Ordering::Acquire)
    ///             {
    ///                 std::thread::yield_now();
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
    /// db.close()?;
    /// stop.store(true, std::sync::atomic::Ordering::Release);
    /// for thread in threads {
    ///     thread.join().unwrap();
    /// }
    /// # std::fs::remove_dir_all(&dir).unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_application_owned(
        path: impl AsRef<Path>,
        options: Options,
    ) -> Result<(Pigeonhole, Vec<Shard>)> {
        let engine_options = options.to_engine();
        let vfs = Arc::clone(&engine_options.vfs);
        let (engine, shards) = Engine::open_application_owned(path.as_ref(), engine_options)?;
        let shards = shards
            .into_iter()
            .map(|inner| Shard {
                inner,
                vfs: Arc::clone(&vfs),
            })
            .collect();
        Ok((Pigeonhole { engine }, shards))
    }

    /// Starts defining or opening a table: chain `.family(..)` and finish with
    /// `.create_if_missing()`, `.create()` or `.open()`.
    pub fn table(&self, name: &str) -> Result<TableBuilder<'_>> {
        Ok(TableBuilder::new(self, name))
    }

    /// Names of every table.
    pub fn tables(&self) -> Vec<String> {
        self.engine
            .tables()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    }

    /// Drops a table and all its data.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let info = self
            .engine
            .table(name)
            .ok_or_else(|| pigeonhole_engine::Error::TableNotFound(name.to_owned()))?;
        Ok(self.engine.drop_table(info.id)?)
    }

    /// A batch of writes across any rows and tables, committed with one durability point.
    pub fn write_batch(&self) -> WriteBatch {
        WriteBatch::new(Arc::clone(&self.engine))
    }

    /// Starts an optimistic multi-row transaction (Phase 4).
    pub fn transaction(&self) -> Result<Transaction> {
        Ok(Transaction::new(
            Arc::clone(&self.engine),
            self.engine.begin()?,
        ))
    }

    /// A consistent snapshot of everything committed so far.
    pub fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot {
            inner: self.engine.snapshot()?,
        })
    }

    /// The writer default durability.
    pub fn default_durability(&self) -> Durability {
        self.engine.default_durability()
    }

    /// Changes the writer default; applies to commits that start afterwards.
    pub fn set_default_durability(&self, durability: Durability) {
        self.engine.set_default_durability(durability);
    }

    /// Flushes every memtable to the file.
    ///
    /// Until the engine writes SSTs (the rest of Phase 1), this freezes the memtables and
    /// writes nothing to the file; the data stays durable through the WAL.
    pub fn flush(&self) -> Result<()> {
        Ok(self.engine.flush()?)
    }

    /// Compacts every table fully. Fails with
    /// [`ErrorCode::Unsupported`](crate::ErrorCode::Unsupported) until the engine writes SSTs.
    pub fn compact(&self) -> Result<()> {
        Ok(self.engine.compact(None)?)
    }

    /// Writes a consistent single-file copy to `dest` while writes continue. Fails with
    /// [`ErrorCode::Unsupported`](crate::ErrorCode::Unsupported) until the engine writes SSTs.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        Ok(self.engine.backup(dest.as_ref())?)
    }

    /// Closes this handle's database. If this is the last process with it open, checkpoints
    /// the WAL and removes the sidecar files and shared-memory region, leaving one file.
    /// Dropping the last clone does the same, ignoring errors.
    ///
    /// Until the engine writes SSTs, the WAL sidecar files stay (the data has nowhere else to
    /// go) and are replayed at the next open. Every handle derived from this database
    /// (tables, batches) fails with [`ErrorCode::Closed`](crate::ErrorCode::Closed) afterwards.
    pub fn close(self) -> Result<()> {
        Ok(self.engine.close()?)
    }
}

/// A read-only handle, typically in another process. Has no write methods, so misuse does
/// not compile. Reads run on the caller's thread.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole, ReaderOptions};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = std::env::temp_dir().join(format!("pigeonhole-doc-{}-reader", std::process::id()));
/// # std::fs::create_dir_all(&dir).unwrap();
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
/// # std::fs::remove_dir_all(&dir).unwrap();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PigeonholeReader {
    engine: Arc<Engine>,
}

impl PigeonholeReader {
    /// Opens an existing table for reading.
    pub fn table(&self, name: &str) -> Result<ReadTable> {
        let info = self
            .engine
            .table(name)
            .ok_or_else(|| pigeonhole_engine::Error::TableNotFound(name.to_owned()))?;
        Ok(ReadTable {
            core: Arc::new(TableCore::new(Arc::clone(&self.engine), info)),
        })
    }

    /// Names of every table.
    pub fn tables(&self) -> Vec<String> {
        self.engine
            .tables()
            .iter()
            .map(|t| t.name.clone())
            .collect()
    }

    /// A consistent snapshot of what the writer has published. May do I/O: re-attach after
    /// a writer restart and reload the manifest if it changed.
    pub fn snapshot(&self) -> Result<Snapshot> {
        Ok(Snapshot {
            inner: self.engine.snapshot()?,
        })
    }
}

/// A point-in-time view for reads. Holding it pins the data it can see; drop it promptly.
///
/// ```
/// use pigeonhole::{Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = std::env::temp_dir().join(format!("pigeonhole-doc-{}-snapshot", std::process::id()));
/// # std::fs::create_dir_all(&dir).unwrap();
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
/// # std::fs::remove_dir_all(&dir).unwrap();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) inner: pigeonhole_engine::Snapshot,
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
    pub fn run_once(&mut self, budget: std::time::Duration) -> bool {
        let budget = u64::try_from(budget.as_nanos()).unwrap_or(u64::MAX);
        let deadline = self.vfs.monotonic_nanos().saturating_add(budget);
        self.inner.run_once(deadline)
    }

    /// Registers a callback invoked (from any thread) when work arrives for this shard.
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        self.inner.set_wakeup(wake);
    }
}
