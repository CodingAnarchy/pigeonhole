//! How `phdb` opens a database (#692's open policy).
//!
//! A read command uses the writer's process when there is one and becomes the writer
//! otherwise:
//! - It first tries to open as the writer. Taking the writer lock is the open's first step,
//!   so when another process holds it the attempt fails with `WriterLocked` having changed
//!   nothing, and phdb opens as a reader process instead. (A reader cannot open a database
//!   no writer has open: it attaches to the writer's shared memory.)
//! - As the writer, the open replays the WAL a crashed writer left, as `sqlite3` rolls back
//!   a hot journal, and the close at exit checkpoints it. phdb holds the writer lock only for
//!   the command.
//!
//! `--reader` and `--writer` force one role.
//!
//! A command that only reads (`info`, `dump`, `backup`, `shell --read-only`) opens through
//! [`for_inspect`] instead: as the writer it opens read-only (`Options::read_only`, ICR
//! 0109), so it writes nothing to the database. In particular it starts no change-feed
//! session, so an application's feed tokens stay valid across it (D310 amendment (d)). The
//! WAL a crashed writer left is replayed into memory and left in place for the
//! application's next open.

use std::path::Path;

use pigeonhole::{
    ErrorCode, Options, Pigeonhole, PigeonholeReader, ReadTable, ReaderOptions, Table,
};

use crate::args::Cli;
use crate::exit::Failure;

/// The block cache a phdb process opens with: a command reads each block about once.
const BLOCK_CACHE: usize = 16 << 20;

/// An open database, in whichever role phdb got.
pub(crate) enum Db {
    Writer(Pigeonhole),
    Reader(PigeonholeReader),
}

/// A table handle in either role.
pub(crate) enum Handle {
    Writer(Table),
    Reader(ReadTable),
}

impl Db {
    /// `table`'s handle in this role.
    pub(crate) fn handle(&self, table: &str) -> Result<Handle, Failure> {
        let fail = |e: pigeonhole::Error| Failure::db(&format!("table {table}"), &e);
        Ok(match self {
            Db::Writer(db) => Handle::Writer(db.table(table).and_then(|b| b.open()).map_err(fail)?),
            Db::Reader(db) => Handle::Reader(db.table(table).map_err(fail)?),
        })
    }

    /// `"writer"` or `"reader"`, for the output.
    pub(crate) fn mode(&self) -> &'static str {
        match self {
            Db::Writer(_) => "writer",
            Db::Reader(_) => "reader",
        }
    }

    /// Whether writes can succeed: the writer, unless it fell back to a read-only open.
    pub(crate) fn writable(&self) -> bool {
        matches!(self, Db::Writer(_)) && self.info().is_ok_and(|i| !i.read_only)
    }

    pub(crate) fn info(&self) -> pigeonhole::Result<pigeonhole::DatabaseInfo> {
        match self {
            Db::Writer(db) => db.info(),
            Db::Reader(db) => db.info(),
        }
    }

    /// A consistent single-file copy at a durable snapshot, or with `latest` at a snapshot of
    /// everything committed; from a reader process too (ICR 0049).
    pub(crate) fn backup(&self, dest: &Path, latest: bool) -> pigeonhole::Result<()> {
        match (self, latest) {
            (Db::Writer(db), false) => db.backup_durable(dest),
            (Db::Writer(db), true) => db.backup(dest),
            (Db::Reader(db), false) => db.backup_durable(dest),
            (Db::Reader(db), true) => db.backup(dest),
        }
    }

    /// Table names.
    pub(crate) fn tables(&self) -> Vec<String> {
        match self {
            Db::Writer(db) => db.tables(),
            Db::Reader(db) => db.tables(),
        }
    }

    /// A snapshot of everything committed, or with `durable` of what is durable.
    pub(crate) fn snapshot(&self, durable: bool) -> pigeonhole::Result<pigeonhole::Snapshot> {
        match (self, durable) {
            (Db::Writer(db), false) => db.snapshot(),
            (Db::Writer(db), true) => db.durable_snapshot(),
            (Db::Reader(db), false) => db.snapshot(),
            (Db::Reader(db), true) => db.durable_snapshot(),
        }
    }

    /// A table's families' stored options, in creation order.
    pub(crate) fn families(&self, table: &str) -> pigeonhole::Result<Vec<pigeonhole::FamilyInfo>> {
        let infos = |names: Vec<&str>, info: &dyn Fn(&str) -> Option<pigeonhole::FamilyInfo>| {
            names.into_iter().filter_map(info).collect()
        };
        Ok(match self {
            Db::Writer(db) => {
                let t = db.table(table)?.open()?;
                infos(t.families(), &|f| t.family_info(f))
            }
            Db::Reader(db) => {
                let t = db.table(table)?;
                infos(t.families(), &|f| t.family_info(f))
            }
        })
    }

    /// Calls `each` with every row of `table` starting with `prefix`, all retained versions,
    /// at `snapshot`, in key order. A read failure is the command's failure; `each` may stop
    /// the scan with its own error.
    pub(crate) fn scan<E: From<Failure>>(
        &self,
        table: &str,
        prefix: &[u8],
        snapshot: &pigeonhole::Snapshot,
        each: &mut dyn FnMut(pigeonhole::RowRef<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        let read = |e: pigeonhole::Error| E::from(Failure::db(&format!("read table {table}"), &e));
        match self {
            Db::Writer(db) => {
                let t = db.table(table).and_then(|b| b.open()).map_err(read)?;
                drive(
                    t.scan_prefix(prefix).versions(0).snapshot(snapshot),
                    read,
                    each,
                )
            }
            Db::Reader(db) => {
                let t = db.table(table).map_err(read)?;
                drive(
                    t.scan_prefix(prefix).versions(0).snapshot(snapshot),
                    read,
                    each,
                )
            }
        }
    }

    /// `table`'s rows starting with `prefix`, all retained versions, at `snapshot`, as an
    /// Arrow export that holds its table (ICR 0052).
    pub(crate) fn arrow(
        &self,
        table: &str,
        prefix: &[u8],
        snapshot: &pigeonhole::Snapshot,
        options: &pigeonhole_arrow::ArrowOptions,
    ) -> Result<pigeonhole_arrow::ArrowScan<'static>, Failure> {
        let read = |e: pigeonhole::Error| Failure::db(&format!("read table {table}"), &e);
        let scan = match self {
            Db::Writer(db) => {
                let t = db.table(table).and_then(|b| b.open()).map_err(read)?;
                t.scan_prefix(prefix).into_owned()
            }
            Db::Reader(db) => db
                .table(table)
                .map_err(read)?
                .scan_prefix(prefix)
                .into_owned(),
        };
        pigeonhole_arrow::scan_to_arrow(scan.versions(0).snapshot(snapshot), options).map_err(read)
    }

    /// Closes the database: as the writer this flushes and checkpoints, and a failure is
    /// the command's failure.
    pub(crate) fn close(self) -> Result<(), Failure> {
        match self {
            Db::Writer(db) => db.close().map_err(|e| Failure::db("close", &e)),
            Db::Reader(db) => {
                drop(db);
                Ok(())
            }
        }
    }
}

/// Opens `path` for a read command, by the policy above.
pub(crate) fn for_read(cli: &Cli, path: &Path) -> Result<Db, Failure> {
    for_read_with_feed(cli, path, None)
}

/// [`for_read`], with the change feed turned on (`phdb mcp --change-feed`) if `feed` gives
/// its retention in bytes. Without it, a writer open inherits the application's feed, if it
/// has one (D310 amendment (c)).
pub(crate) fn for_read_with_feed(cli: &Cli, path: &Path, feed: Option<u64>) -> Result<Db, Failure> {
    exists(path)?;
    if cli.reader {
        return reader(cli, path).map(Db::Reader);
    }
    match Pigeonhole::open(path, with_feed(options(cli, path, false), feed)) {
        Ok(db) => Ok(Db::Writer(db)),
        Err(e) if e.code() == ErrorCode::WriterLocked && !cli.writer => {
            reader(cli, path).map(Db::Reader)
        }
        Err(e) => Err(Failure::db("open", &e)),
    }
}

/// Opens `path` for a command that only reads: as [`for_read`], but a writer open is
/// read-only (ICR 0109) and writes nothing, so it leaves the database, and an
/// application's change feed, as it found them.
pub(crate) fn for_inspect(cli: &Cli, path: &Path) -> Result<Db, Failure> {
    exists(path)?;
    if cli.reader {
        return reader(cli, path).map(Db::Reader);
    }
    match Pigeonhole::open(
        path,
        options(cli, path, false)
            // A read-only open writes no feed session, so its shard count can't reshard:
            // one keeps phdb light (D-a: any count replays).
            .shards(1)
            .read_only(true),
    ) {
        Ok(db) => Ok(Db::Writer(db)),
        Err(e) if e.code() == ErrorCode::WriterLocked && !cli.writer => {
            reader(cli, path).map(Db::Reader)
        }
        // The replay of a crashed writer's log must fit in memory: say how to get past it.
        Err(e) if e.code() == ErrorCode::ReadOnly => Err(Failure::db("open", &e).hint(
            "phdb reads without writing, so a crashed writer's log is replayed into memory, \
             and this one is too large: open the database once with your application (or \
             phdb shell without --read-only) to checkpoint it",
        )),
        Err(e) => Err(Failure::db("open", &e)),
    }
}

/// Opens `path` as a reader process only (`phdb mcp --read-only`), whatever `--writer` says.
pub(crate) fn reader_only(cli: &Cli, path: &Path) -> Result<Db, Failure> {
    exists(path)?;
    reader(cli, path).map(Db::Reader)
}

fn exists(path: &Path) -> Result<(), Failure> {
    if path.exists() {
        Ok(())
    } else {
        Err(Failure::file_not_found(path))
    }
}

/// Opens `path` as the writer for a write command (`load`): a running writer is exit 4.
/// With `create`, a missing file is created.
pub(crate) fn for_write(cli: &Cli, path: &Path, create: bool) -> Result<Pigeonhole, Failure> {
    for_write_with_feed(cli, path, create, None)
}

/// [`for_write`], with the change feed on if `feed` gives its retention in bytes.
pub(crate) fn for_write_with_feed(
    cli: &Cli,
    path: &Path,
    create: bool,
    feed: Option<u64>,
) -> Result<Pigeonhole, Failure> {
    if !create {
        exists(path)?;
    }
    Pigeonhole::open(path, with_feed(options(cli, path, create), feed)).map_err(|e| {
        if e.code() == ErrorCode::WriterLocked {
            Failure::db_with(
                "open: a writer process has the database open; stop it to write to the \
                     database with phdb"
                    .into(),
                &e,
            )
        } else {
            Failure::db("open", &e)
        }
    })
}

/// The writer phdb opens as: one unpinned shard (so no tablet moves), small caches, never
/// creating the file, and falling back to a read-only open (ICR 0059) for a database phdb
/// cannot write: families whose merge operator phdb cannot know, or a feature a newer build
/// added that this one may only read. That open writes nothing to the file.
fn with_feed(options: Options, feed: Option<u64>) -> Options {
    match feed {
        Some(bytes) => options.change_feed(pigeonhole::FeedRetention::bytes(bytes)),
        None => options,
    }
}

/// The writer's options for `path`. One shard for a database phdb creates; an existing
/// one keeps its own count. An explicit count that differs from the one the application's
/// change feed recorded would reshard, ending the feed's history (D310 amendment (c)),
/// while an unset count inherits it.
fn options(cli: &Cli, path: &Path, create: bool) -> Options {
    let mut options = Options::default().create_if_missing(create);
    if !path.exists() {
        options = options.shards(1);
    }
    let mut options = options
        .pin_threads(false)
        .block_cache(BLOCK_CACHE)
        .row_cache(0)
        .read_only_fallback(true);
    if let Some(dir) = &cli.shm_dir {
        options = options.shm_dir(dir);
    }
    options
}

fn reader(cli: &Cli, path: &Path) -> Result<PigeonholeReader, Failure> {
    let mut options = ReaderOptions::default().block_cache(BLOCK_CACHE);
    if let Some(dir) = &cli.shm_dir {
        options = options.shm_dir(dir);
    }
    // With no writer running this is `NoWriter` (ICR 0057), whose message says so.
    Pigeonhole::open_reader(path, options).map_err(|e| Failure::db("open as a reader", &e))
}

fn drive<E>(
    scan: pigeonhole::Scan<'_>,
    read: impl Fn(pigeonhole::Error) -> E,
    each: &mut dyn FnMut(pigeonhole::RowRef<'_>) -> Result<(), E>,
) -> Result<(), E> {
    let mut it = scan.iter().map_err(&read)?;
    while let Some(row) = it.next_ref().map_err(&read)? {
        each(row)?;
    }
    Ok(())
}
