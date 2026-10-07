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
        let engine_options = options.to_engine();
        let max_value = max_value(&engine_options);
        let engine = Engine::open(path.as_ref(), engine_options)?;
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
        let engine = Engine::open_reader(path.as_ref(), options.to_engine())?;
        Ok(PigeonholeReader {
            db: Db::new(engine, 0),
        })
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
    /// # let dir = pigeonhole::doc_support::temp_dir();
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
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_application_owned(
        path: impl AsRef<Path>,
        options: Options,
    ) -> Result<(Pigeonhole, Vec<Shard>)> {
        let engine_options = options.to_engine();
        let vfs = Arc::clone(&engine_options.vfs);
        let max_value = max_value(&engine_options);
        let (engine, shards) = Engine::open_application_owned(path.as_ref(), engine_options)?;
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
    /// call this after a bulk load or delete to reclaim space and speed up reads.
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
    /// on its own, without WAL replay or sidecar files. Fails with
    /// [`ErrorCode::Unsupported`](crate::ErrorCode::Unsupported) for a database whose
    /// families store blob files (Phase 2; values stay inline today).
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

/// The largest value a commit may carry (decision D16), as `pigeonhole-engine` computes it at
/// open: the WAL segment payload, 64 MiB, and half a shard's memtable arena.
fn max_value(o: &pigeonhole_engine::EngineOptions) -> usize {
    (o.wal.segment_size as usize)
        .saturating_sub(64 * 1024)
        .min(64 << 20)
        .min((o.memtable_budget / 2) as usize)
}
