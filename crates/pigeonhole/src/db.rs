use std::path::Path;

use pigeonhole_format::Durability;

use crate::{Options, ReadTable, ReaderOptions, Result, TableBuilder, Transaction, WriteBatch};

/// An open database: the one writer process's handle. Cheap to clone; every clone shares the
/// same engine and shard threads.
#[derive(Debug, Clone)]
pub struct Pigeonhole {
    _priv: (),
}

impl Pigeonhole {
    /// Opens (or creates) a database as the writer. A second writer, in this or another
    /// process, fails with [`ErrorCode::WriterLocked`](crate::ErrorCode::WriterLocked).
    /// Opening replays the WAL sidecars; there is no full-file recovery scan.
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Pigeonhole> {
        todo!()
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
        todo!()
    }

    /// Opens as the writer in application-owned mode: no threads are started; drive each
    /// returned [`Shard`] from one of your own (typically pinned) threads. Because no thread
    /// is started, [`Options::compaction_cores`] with `k > 0` is refused with
    /// [`ErrorCode::InvalidArgument`](crate::ErrorCode::InvalidArgument) (decision D40).
    pub fn open_application_owned(
        path: impl AsRef<Path>,
        options: Options,
    ) -> Result<(Pigeonhole, Vec<Shard>)> {
        todo!()
    }

    /// Starts defining or opening a table: chain `.family(..)` and finish with
    /// `.create_if_missing()`, `.create()` or `.open()`.
    pub fn table(&self, name: &str) -> Result<TableBuilder<'_>> {
        todo!()
    }

    /// Names of every table.
    pub fn tables(&self) -> Vec<String> {
        todo!()
    }

    /// Drops a table and all its data.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        todo!()
    }

    /// A batch of writes across any rows and tables, committed with one durability point.
    pub fn write_batch(&self) -> WriteBatch {
        todo!()
    }

    /// Starts an optimistic multi-row transaction (Phase 4).
    pub fn transaction(&self) -> Result<Transaction> {
        todo!()
    }

    /// A consistent snapshot of everything committed so far.
    pub fn snapshot(&self) -> Result<Snapshot> {
        todo!()
    }

    /// The writer default durability.
    pub fn default_durability(&self) -> Durability {
        todo!()
    }

    /// Changes the writer default; applies to commits that start afterwards.
    pub fn set_default_durability(&self, durability: Durability) {
        todo!()
    }

    /// Flushes every memtable to the file.
    pub fn flush(&self) -> Result<()> {
        todo!()
    }

    /// Compacts every table fully.
    pub fn compact(&self) -> Result<()> {
        todo!()
    }

    /// Writes a consistent single-file copy to `dest` while writes continue.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        todo!()
    }

    /// Closes this handle's database. If this is the last process with it open, checkpoints
    /// the WAL and removes the sidecar files and shared-memory region, leaving one file.
    /// Dropping the last clone does the same, ignoring errors.
    pub fn close(self) -> Result<()> {
        todo!()
    }
}

/// A read-only handle, typically in another process. Has no write methods, so misuse does
/// not compile. Reads run on the caller's thread.
#[derive(Debug, Clone)]
pub struct PigeonholeReader {
    _priv: (),
}

impl PigeonholeReader {
    /// Opens an existing table for reading.
    pub fn table(&self, name: &str) -> Result<ReadTable> {
        todo!()
    }

    /// Names of every table.
    pub fn tables(&self) -> Vec<String> {
        todo!()
    }

    /// A consistent snapshot of what the writer has published. May do I/O: re-attach after
    /// a writer restart and reload the manifest if it changed.
    pub fn snapshot(&self) -> Result<Snapshot> {
        todo!()
    }
}

/// A point-in-time view for reads. Holding it pins the data it can see; drop it promptly.
#[derive(Debug, Clone)]
pub struct Snapshot {
    _priv: (),
}

impl Snapshot {
    /// The snapshot's sequence number.
    pub fn seqno(&self) -> u64 {
        todo!()
    }
}

/// One shard in application-owned mode. Move it to the thread that should run it and call
/// [`Shard::run_once`] from that thread's event loop.
#[derive(Debug)]
pub struct Shard {
    _priv: (),
}

impl Shard {
    /// Shard index.
    pub fn index(&self) -> usize {
        todo!()
    }

    /// Runs queued writes, group commit and background work for up to `budget`. Returns
    /// whether work remains.
    pub fn run_once(&mut self, budget: std::time::Duration) -> bool {
        todo!()
    }

    /// Registers a callback invoked (from any thread) when work arrives for this shard.
    pub fn set_wakeup(&mut self, wake: Box<dyn Fn() + Send + Sync>) {
        todo!()
    }
}
