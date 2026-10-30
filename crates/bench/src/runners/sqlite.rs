//! SQLite as an entity-attribute-value table: one row per cell in a `WITHOUT ROWID`
//! table keyed by `(row, family, qualifier)`, WAL journal mode, with a page cache of
//! the whole [`MemoryBudget`] (SQLite has no separate write buffer): on every connection
//! ([`CacheModel::PerStore`]), or split across the connections ([`CacheModel::Total`]).

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{
    CacheModel, Counted, MemoryBudget, Touched, durability, modified, now_micros,
    transfer_balances, ttl_micros,
};
use crate::workload::{METRIC_FAMILY, YCSB_FAMILY};
use crate::{BenchOp, Client, Runner};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS cells (
    row BLOB NOT NULL,
    family TEXT NOT NULL,
    qualifier BLOB NOT NULL,
    ts INTEGER NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (row, family, qualifier)
) WITHOUT ROWID";

// `?4` is the oldest live timestamp of the cell's family: `ts > ?4` is the TTL filter.
/// The counters workload's table (D247): one integer per row.
const COUNTERS: &str = "CREATE TABLE IF NOT EXISTS counters (row BLOB PRIMARY KEY NOT NULL, n INTEGER NOT NULL) WITHOUT ROWID";
const INCR: &str = "INSERT INTO counters (row, n) VALUES (?1, ?2) ON CONFLICT (row) DO UPDATE SET n = n + excluded.n";
const READ_COUNTER: &str = "SELECT n FROM counters WHERE row = ?1";

/// D247's filtered scans: one family's cells from a row on, live and in a time range; the
/// qualifier prefix is checked in Rust.
const SCAN_FILTERED: &str = "SELECT row, qualifier, value FROM cells WHERE row >= ?1 AND family = ?2 \
    AND ts > ?3 AND ts >= ?4 AND ts < ?5 ORDER BY row, qualifier";

const DELETE_COLUMN: &str = "DELETE FROM cells WHERE row = ?1 AND family = ?2 AND qualifier = ?3";
const DELETE_ROW: &str = "DELETE FROM cells WHERE row = ?1";

const GET: &str =
    "SELECT value FROM cells WHERE row = ?1 AND family = ?2 AND qualifier = ?3 AND ts > ?4";
const GET_ROW: &str =
    "SELECT value FROM cells WHERE row = ?1 AND family = ?2 AND ts > ?3 ORDER BY qualifier";
const PUT: &str =
    "INSERT OR REPLACE INTO cells (row, family, qualifier, ts, value) VALUES (?1, ?2, ?3, ?4, ?5)";
// `?2` is the metric family's oldest live timestamp; other families have no TTL.
const SCAN: &str = "SELECT row, value FROM cells WHERE row >= ?1 AND (family <> ?3 OR ts > ?2) \
    ORDER BY row, family, qualifier";

/// The oldest timestamp of `family` that is still live at `now`: cells with `ts` above it
/// are. Families without a TTL keep everything (timestamps are never negative).
fn oldest_live(family: &str, now: u64) -> i64 {
    match ttl_micros(family) {
        0 => -1,
        ttl => i64::try_from(now.saturating_sub(ttl)).unwrap_or(i64::MAX),
    }
}

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
    /// The load commits at `synchronous=NORMAL`, the measured phase at `sync` (D247).
    load_buffered: bool,
    memory: MemoryBudget,
    path: Option<PathBuf>,
    conn: Option<Conn>,
    /// Client threads of the run ([`Runner::plan_clients`]); with the runner's own
    /// connection, what [`CacheModel::Total`] splits the page cache across.
    clients: usize,
}

impl SqliteRunner {
    /// `true`: `synchronous=FULL` (fsync the WAL every commit); `false` (default):
    /// `synchronous=NORMAL`, where a commit is written to the WAL without fsync, like
    /// Pigeonhole's `Buffered`.
    pub fn sync(mut self, yes: bool) -> Self {
        self.sync = yes;
        self
    }

    /// Page cache of `write_buffer + cache` bytes (default [`MemoryBudget::default`]).
    pub fn memory(mut self, memory: MemoryBudget) -> Self {
        self.memory = memory;
        self
    }

    /// `true`: the load commits at `synchronous=NORMAL`, and the warmup and the measured
    /// phase as [`sync`](Self::sync) says (D247).
    pub fn load_buffered(mut self, yes: bool) -> Self {
        self.load_buffered = yes;
        self
    }

    /// The whole page-cache budget, `write_buffer + cache`.
    fn budget(&self) -> u64 {
        self.memory.write_buffer + self.memory.cache
    }

    /// Page cache bytes of each connection: the budget, or under [`CacheModel::Total`] its
    /// share among the clients and the runner's own connection.
    fn page_cache(&self) -> u64 {
        match self.memory.model {
            CacheModel::PerStore => self.budget(),
            CacheModel::Total => self.budget() / (self.clients as u64 + 1),
        }
    }
}

#[derive(Debug)]
struct Conn(Connection);

fn connect(path: &Path, sync: bool, page_cache: u64) -> Result<Conn, String> {
    let e = |e: rusqlite::Error| e.to_string();
    let c = Connection::open(path).map_err(e)?;
    c.busy_timeout(std::time::Duration::from_secs(60))
        .map_err(e)?;
    c.pragma_update(None, "journal_mode", "WAL").map_err(e)?;
    c.pragma_update(None, "synchronous", if sync { "FULL" } else { "NORMAL" })
        .map_err(e)?;
    // Negative cache_size is in KiB.
    let kib = i64::try_from(page_cache / 1024).unwrap_or(i64::MAX);
    c.pragma_update(None, "cache_size", -kib).map_err(e)?;
    c.execute(SCHEMA, []).map_err(e)?;
    c.execute(COUNTERS, []).map_err(e)?;
    Ok(Conn(c))
}

impl Runner for SqliteRunner {
    fn name(&self) -> &'static str {
        "sqlite-eav"
    }

    fn open(&mut self, dir: &Path) -> Result<(), String> {
        let path = dir.join("bench.sqlite");
        let sync = self.sync && !self.load_buffered;
        self.conn = Some(connect(&path, sync, self.page_cache())?);
        self.path = Some(path);
        Ok(())
    }

    fn execute(&mut self, op: &BenchOp) -> Result<(), String> {
        self.execute_counted(op).map(drop)
    }

    fn flush(&mut self) -> Result<(), String> {
        // Checkpoint the WAL into the database file: SQLite's write buffer (D233).
        match &self.conn {
            Some(Conn(c)) => c
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .map_err(|e| e.to_string()),
            None => Ok(()),
        }
    }

    fn measured_durability(&mut self) -> Result<bool, String> {
        match &self.conn {
            Some(Conn(c)) if self.load_buffered => c
                .pragma_update(
                    None,
                    "synchronous",
                    if self.sync { "FULL" } else { "NORMAL" },
                )
                .map(|()| self.sync)
                .map_err(|e| e.to_string()),
            _ => Ok(false),
        }
    }

    fn close(&mut self) -> Result<(), String> {
        if let Some(Conn(c)) = self.conn.take() {
            c.close().map_err(|(_, e)| e.to_string())?;
        }
        Ok(())
    }

    fn plan_clients(&mut self, clients: usize) {
        self.clients = clients;
        if let Some(Conn(c)) = &self.conn {
            // The runner's own connection keeps its share (it loaded with the whole budget).
            let kib = i64::try_from(self.page_cache() / 1024).unwrap_or(i64::MAX);
            let _ = c.pragma_update(None, "cache_size", -kib);
        }
    }

    fn client(&self) -> Option<Box<dyn Client>> {
        let conn = connect(self.path.as_ref()?, self.sync, self.page_cache()).ok()?;
        Some(Box::new(conn))
    }

    fn describe(&self) -> String {
        format!(
            "page_cache={} {}{}",
            match self.memory.model {
                CacheModel::PerStore => format!("{}MiB", self.budget() >> 20),
                CacheModel::Total => format!("{}MiB total", self.budget() >> 20),
            },
            durability(self.sync),
            if self.load_buffered {
                " load=buffered"
            } else {
                ""
            }
        )
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

/// Column `i` of a result row, as the blob it holds.
fn blob<'r>(r: &'r rusqlite::Row<'_>, i: usize) -> Result<&'r [u8], String> {
    r.get_ref(i)
        .map_err(|e| e.to_string())?
        .as_blob()
        .map_err(|e| e.to_string())
}

/// A scan's row limit over cells ordered by row: admits every cell of the first `len` rows.
struct RowLimit {
    len: u32,
    seen: u32,
    current: Option<Vec<u8>>,
}

impl RowLimit {
    fn new(len: u32) -> RowLimit {
        RowLimit {
            len,
            seen: 0,
            current: None,
        }
    }

    /// Whether a cell of row `key` is inside the limit; `false` at the first cell past it.
    fn admit(&mut self, key: &[u8]) -> bool {
        if self.current.as_deref() != Some(key) {
            if self.seen == self.len {
                return false;
            }
            self.seen += 1;
            self.current = Some(key.to_vec());
        }
        true
    }
}

impl Conn {
    /// One transaction of cells of one row, all stamped `ts`.
    fn put(
        &mut self,
        row: &[u8],
        family: &str,
        ts: u64,
        cells: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), String> {
        self.put_rows(family, ts, std::iter::once((row, cells)))
    }

    /// One transaction of cells of any rows, all stamped `ts` (D246's cross-shard commit).
    fn put_rows<'a>(
        &mut self,
        family: &str,
        ts: u64,
        rows: impl Iterator<Item = (&'a [u8], &'a [(Vec<u8>, Vec<u8>)])>,
    ) -> Result<(), String> {
        let e = |e: rusqlite::Error| e.to_string();
        let ts = i64::try_from(ts).unwrap_or(i64::MAX);
        let tx = self.0.transaction().map_err(e)?;
        {
            let mut stmt = tx.prepare_cached(PUT).map_err(e)?;
            for (row, cells) in rows {
                for (q, v) in cells {
                    stmt.execute(params![row, family, q, ts, v]).map_err(e)?;
                }
            }
        }
        tx.commit().map_err(e)
    }

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
                let live = oldest_live(family, now_micros());
                let mut rows = stmt
                    .query(params![row, family, qualifier, live])
                    .map_err(e)?;
                if let Some(r) = rows.next().map_err(e)? {
                    t.cell(blob(r, 0)?);
                }
            }
            BenchOp::GetRow { row, family } => {
                let mut stmt = self.0.prepare_cached(GET_ROW).map_err(e)?;
                let live = oldest_live(family, now_micros());
                let mut rows = stmt.query(params![row, family, live]).map_err(e)?;
                while let Some(r) = rows.next().map_err(e)? {
                    t.cell(blob(r, 0)?);
                }
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
                self.0
                    .prepare_cached(DELETE_COLUMN)
                    .map_err(e)?
                    .execute(params![row, family, qualifier])
                    .map_err(e)?;
            }
            BenchOp::DeleteRow { row } => {
                self.0
                    .prepare_cached(DELETE_ROW)
                    .map_err(e)?
                    .execute(params![row])
                    .map_err(e)?;
            }
            BenchOp::Transfer { from, to, amount } => {
                // Immediate: the write lock from the reads on (D247).
                let tx = self
                    .0
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(e)?;
                let read = |row: &[u8]| -> Result<Option<Vec<u8>>, String> {
                    tx.prepare_cached(GET)
                        .map_err(e)?
                        .query_row(params![row, YCSB_FAMILY, b"bal".as_slice(), -1], |r| {
                            r.get(0)
                        })
                        .optional()
                        .map_err(e)
                };
                let (a, b) = (read(from)?, read(to)?);
                for v in [&a, &b].into_iter().flatten() {
                    t.cell(v);
                }
                let (na, nb) = transfer_balances(a.as_deref(), b.as_deref(), *amount);
                let ts = i64::try_from(now_micros()).unwrap_or(i64::MAX);
                {
                    let mut put = tx.prepare_cached(PUT).map_err(e)?;
                    put.execute(params![
                        from,
                        YCSB_FAMILY,
                        b"bal".as_slice(),
                        ts,
                        na.to_le_bytes().as_slice()
                    ])
                    .map_err(e)?;
                    put.execute(params![
                        to,
                        YCSB_FAMILY,
                        b"bal".as_slice(),
                        ts,
                        nb.to_le_bytes().as_slice()
                    ])
                    .map_err(e)?;
                }
                tx.commit().map_err(e)?;
            }
            BenchOp::Incr { row, delta } => {
                self.0
                    .prepare_cached(INCR)
                    .map_err(e)?
                    .execute(params![row, delta])
                    .map_err(e)?;
            }
            BenchOp::ReadCounter { row } => {
                let n: Option<i64> = self
                    .0
                    .prepare_cached(READ_COUNTER)
                    .map_err(e)?
                    .query_row(params![row], |r| r.get(0))
                    .optional()
                    .map_err(e)?;
                if let Some(n) = n {
                    t.cell(&n.to_le_bytes());
                }
            }
            BenchOp::Commit { family, rows } => self.put_rows(
                family,
                now_micros(),
                rows.iter().map(|r| (r.row.as_slice(), r.cells.as_slice())),
            )?,
            BenchOp::Scan { start, len } => {
                let mut stmt = self.0.prepare_cached(SCAN).map_err(e)?;
                let live = oldest_live(METRIC_FAMILY, now_micros());
                let mut rows = stmt.query(params![start, live, METRIC_FAMILY]).map_err(e)?;
                let mut limit = RowLimit::new(*len);
                while let Some(r) = rows.next().map_err(e)? {
                    if !limit.admit(blob(r, 0)?) {
                        break;
                    }
                    t.cell(blob(r, 1)?);
                }
            }
            BenchOp::ScanFiltered {
                start,
                len,
                family,
                qualifier_prefix,
                time_range,
            } => {
                let (a, b) = time_range.unwrap_or((0, u64::MAX));
                let ts = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
                let mut stmt = self.0.prepare_cached(SCAN_FILTERED).map_err(e)?;
                let live = oldest_live(family, now_micros());
                let mut rows = stmt
                    .query(params![start, family, live, ts(a), ts(b)])
                    .map_err(e)?;
                let mut limit = RowLimit::new(*len);
                while let Some(r) = rows.next().map_err(e)? {
                    if qualifier_prefix
                        .as_deref()
                        .is_some_and(|q| !blob(r, 1).is_ok_and(|v| v.starts_with(q)))
                    {
                        continue;
                    }
                    if !limit.admit(blob(r, 0)?) {
                        break;
                    }
                    t.cell(blob(r, 2)?);
                }
            }
            BenchOp::ReadModifyWrite { row, qualifier } => {
                // Immediate: the write lock from the read on, so concurrent clients never
                // lose an update or deadlock upgrading a read lock.
                let tx = self
                    .0
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(e)?;
                let old: Option<Vec<u8>> = tx
                    .prepare_cached(GET)
                    .map_err(e)?
                    .query_row(params![row, YCSB_FAMILY, qualifier, -1], |r| r.get(0))
                    .optional()
                    .map_err(e)?;
                if let Some(v) = &old {
                    t.cell(v);
                }
                let new = modified(old.as_deref());
                tx.prepare_cached(PUT)
                    .map_err(e)?
                    .execute(params![
                        row,
                        YCSB_FAMILY,
                        qualifier,
                        i64::try_from(now_micros()).unwrap_or(i64::MAX),
                        new
                    ])
                    .map_err(e)?;
                tx.commit().map_err(e)?;
            }
        }
        Ok(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_limit_admits_every_cell_of_the_first_len_rows_and_stops_at_the_next() {
        let cells: [&[u8]; 6] = [b"a", b"a", b"b", b"c", b"c", b"d"];
        let mut limit = RowLimit::new(2);
        let admitted = cells.iter().take_while(|k| limit.admit(k)).count();
        assert_eq!(admitted, 3, "a, a, b; stops at c");
        let mut none = RowLimit::new(0);
        assert!(!none.admit(b"a"));
    }

    #[test]
    fn total_cache_model_splits_the_page_cache_across_connections() {
        let total = MemoryBudget {
            cache: 1024 << 20,
            write_buffer: 64 << 20,
            model: CacheModel::Total,
        };
        let mut r = SqliteRunner::default().memory(total);
        // Before the run's clients are known, the runner's own connection has the budget.
        assert_eq!(r.page_cache(), 1088 << 20);
        r.plan_clients(16);
        assert_eq!(r.page_cache(), (1088 << 20) / 17);
        assert!(
            r.describe().starts_with("page_cache=1088MiB total "),
            "{}",
            r.describe()
        );
        // Per store (the default): every connection gets the whole budget.
        let mut r = SqliteRunner::default();
        r.plan_clients(16);
        assert_eq!(r.page_cache(), 320 << 20);
        assert!(
            r.describe().starts_with("page_cache=320MiB "),
            "{}",
            r.describe()
        );
    }
}
