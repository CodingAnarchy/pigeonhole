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

use std::path::Path;

use pigeonhole::{ErrorCode, Options, Pigeonhole, PigeonholeReader, ReaderOptions};

use crate::args::Cli;
use crate::exit::Failure;

/// The block cache a phdb process opens with: a command reads each block about once.
const BLOCK_CACHE: usize = 16 << 20;

/// An open database, in whichever role phdb got.
pub(crate) enum Db {
    Writer(Pigeonhole),
    Reader(PigeonholeReader),
}

impl Db {
    /// `"writer"` or `"reader"`, for the output.
    pub(crate) fn mode(&self) -> &'static str {
        match self {
            Db::Writer(_) => "writer",
            Db::Reader(_) => "reader",
        }
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
    exists(path)?;
    if cli.reader {
        return reader(cli, path).map(Db::Reader);
    }
    match writer(cli, path) {
        Ok(db) => Ok(Db::Writer(db)),
        Err(e) if e.code() == ErrorCode::WriterLocked && !cli.writer => {
            reader(cli, path).map(Db::Reader)
        }
        Err(e) => Err(Failure::db("open", &e)),
    }
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
    if !create {
        exists(path)?;
    }
    Pigeonhole::open(path, options(cli, create)).map_err(|e| {
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
fn writer(cli: &Cli, path: &Path) -> pigeonhole::Result<Pigeonhole> {
    Pigeonhole::open(path, options(cli, false))
}

fn options(cli: &Cli, create: bool) -> Options {
    let mut options = Options::default()
        .create_if_missing(create)
        .shards(1)
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
