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

/// The writer phdb opens as: one unpinned shard with no tablet changes, small caches, never
/// creating the file, and accepting families whose merge operator phdb cannot know (they
/// open read-only).
fn writer(cli: &Cli, path: &Path) -> pigeonhole::Result<Pigeonhole> {
    let mut options = Options::default()
        .create_if_missing(false)
        .shards(1)
        .pin_threads(false)
        .tablet_changes(false)
        .block_cache(BLOCK_CACHE)
        .row_cache(0)
        .allow_unregistered_merge_operators(true);
    if let Some(dir) = &cli.shm_dir {
        options = options.shm_dir(dir);
    }
    Pigeonhole::open(path, options)
}

fn reader(cli: &Cli, path: &Path) -> Result<PigeonholeReader, Failure> {
    let mut options = ReaderOptions::default().block_cache(BLOCK_CACHE);
    if let Some(dir) = &cli.shm_dir {
        options = options.shm_dir(dir);
    }
    Pigeonhole::open_reader(path, options).map_err(|e| {
        if e.code() == ErrorCode::Io && cli.reader {
            // The usual cause: no writer has the database open, so there is no shared
            // memory to attach to.
            Failure::db("open as a reader (is a writer process running?)", &e)
        } else {
            Failure::db("open as a reader", &e)
        }
    })
}
