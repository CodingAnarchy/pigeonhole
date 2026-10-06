//! SQLite as an entity-attribute-value table: one row per cell in a `WITHOUT ROWID`
//! table keyed by `(row, family, qualifier)`, WAL journal mode.

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};

use super::{Counted, Touched, modified};
use crate::workload::YCSB_FAMILY;
use crate::{BenchOp, Client, Runner};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS cells (
    row BLOB NOT NULL,
    family TEXT NOT NULL,
    qualifier BLOB NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (row, family, qualifier)
) WITHOUT ROWID";

const GET: &str = "SELECT value FROM cells WHERE row = ?1 AND family = ?2 AND qualifier = ?3";
const PUT: &str =
    "INSERT OR REPLACE INTO cells (row, family, qualifier, value) VALUES (?1, ?2, ?3, ?4)";
const SCAN: &str = "SELECT row, value FROM cells WHERE row >= ?1 ORDER BY row, family, qualifier";

/// Runs SQLite as an EAV table (feature `sqlite`).
///
/// ```
/// use pigeonhole_bench::{Runner, SqliteRunner};
///
/// assert_eq!(SqliteRunner::default().name(), "sqlite-eav");
/// ```
#[derive(Debug, Default)]
pub struct SqliteRunner {
    sync: bool,
    path: Option<PathBuf>,
    conn: Option<Conn>,
}

impl SqliteRunner {
    /// `true`: `synchronous=FULL` (fsync the WAL every commit); `false` (default):
    /// `synchronous=NORMAL`, where a commit is written to the WAL without fsync, like
    /// Pigeonhole's `Buffered`.
    pub fn sync(mut self, yes: bool) -> Self {
        self.sync = yes;
        self
    }
}

#[derive(Debug)]
struct Conn(Connection);

fn connect(path: &Path, sync: bool) -> Result<Conn, String> {
    let e = |e: rusqlite::Error| e.to_string();
    let c = Connection::open(path).map_err(e)?;
    c.busy_timeout(std::time::Duration::from_secs(60))
        .map_err(e)?;
    c.pragma_update(None, "journal_mode", "WAL").map_err(e)?;
    c.pragma_update(None, "synchronous", if sync { "FULL" } else { "NORMAL" })
        .map_err(e)?;
    c.execute(SCHEMA, []).map_err(e)?;
    Ok(Conn(c))
}

impl Runner for SqliteRunner {
    fn name(&self) -> &'static str {
        "sqlite-eav"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let path = dir.join("bench.sqlite");
        self.conn = Some(connect(&path, self.sync)?);
        self.path = Some(path);
        Ok(())
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn close(&mut self) -> Result<(), String> {
        if let Some(Conn(c)) = self.conn.take() {
            c.close().map_err(|(_, e)| e.to_string())?;
        }
        Ok(())
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        let conn = connect(self.path.as_ref()?, self.sync).ok()?;
        Some(Box::new(conn))
    }

    fn describe(&self) -> String {
        if self.sync { "sync" } else { "buffered" }.to_owned()
    }
}

impl Counted for SqliteRunner {
    fn execute_counted(&mut self, op: &BenchOp) -> Result<Touched, String> {
        self.conn
            .as_mut()
            .ok_or("sqlite runner is not open")?
            .execute(op)
    }
}

impl Client for Conn {
    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        Conn::execute(self, op).map(drop)
    }
}

impl Conn {
    fn execute(&mut self, op: &BenchOp) -> Result<Touched, String> {
        let e = |e: rusqlite::Error| e.to_string();
        let mut t = Touched::default();
        match op {
            BenchOp::Get {
                row,
                family,
                qualifier,
            } => {
                let mut stmt = self.0.prepare_cached(GET).map_err(e)?;
                let mut rows = stmt.query(params![row, family, qualifier]).map_err(e)?;
                if let Some(r) = rows.next().map_err(e)? {
                    t.cell(
                        r.get_ref(0)
                            .map_err(e)?
                            .as_blob()
                            .map_err(|e| e.to_string())?,
                    );
                }
            }
            BenchOp::Put { row, family, cells } => {
                let tx = self.0.transaction().map_err(e)?;
                {
                    let mut stmt = tx.prepare_cached(PUT).map_err(e)?;
                    for (q, v) in cells {
                        stmt.execute(params![row, family, q, v]).map_err(e)?;
                    }
                }
                tx.commit().map_err(e)?;
            }
            BenchOp::Scan { start, len } => {
                let mut stmt = self.0.prepare_cached(SCAN).map_err(e)?;
                let mut rows = stmt.query(params![start]).map_err(e)?;
                let mut seen = 0u32;
                let mut current: Option<Vec<u8>> = None;
                while let Some(r) = rows.next().map_err(e)? {
                    let key = r
                        .get_ref(0)
                        .map_err(e)?
                        .as_blob()
                        .map_err(|e| e.to_string())?;
                    if current.as_deref() != Some(key) {
                        if seen == *len {
                            break;
                        }
                        seen += 1;
                        current = Some(key.to_vec());
                    }
                    t.cell(
                        r.get_ref(1)
                            .map_err(e)?
                            .as_blob()
                            .map_err(|e| e.to_string())?,
                    );
                }
            }
            BenchOp::ReadModifyWrite { row, qualifier } => {
                let tx = self.0.transaction().map_err(e)?;
                let old: Option<Vec<u8>> = tx
                    .prepare_cached(GET)
                    .map_err(e)?
                    .query_row(params![row, YCSB_FAMILY, qualifier], |r| r.get(0))
                    .optional()
                    .map_err(e)?;
                if let Some(v) = &old {
                    t.cell(v);
                }
                let new = modified(old.as_deref());
                tx.prepare_cached(PUT)
                    .map_err(e)?
                    .execute(params![row, YCSB_FAMILY, qualifier, new])
                    .map_err(e)?;
                tx.commit().map_err(e)?;
            }
        }
        Ok(t)
    }
}
