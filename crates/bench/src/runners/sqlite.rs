//! SQLite as an entity-attribute-value table: one row per cell in a `WITHOUT ROWID`
//! table keyed by `(row, family, qualifier)`, WAL journal mode, with a page cache of
//! the whole [`MemoryBudget`] (SQLite has no separate write buffer).

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{
    Counted, MemoryBudget, Touched, durability, modified, now_micros, transfer_balances, ttl_micros,
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

    fn page_cache(&self) -> u64 {
        self.memory.write_buffer + self.memory.cache
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

    fn client(&self) -> Option<Box<dyn Client>> {
        let conn = connect(self.path.as_ref()?, self.sync, self.page_cache()).ok()?;
        Some(Box::new(conn))
    }

    fn describe(&self) -> String {
        format!(
            "page_cache={}MiB {}{}",
            self.page_cache() >> 20,
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
                    t.cell(
                        r.get_ref(0)
                            .map_err(e)?
                            .as_blob()
                            .map_err(|e| e.to_string())?,
                    );
                }
            }
            BenchOp::GetRow { row, family } => {
                let _probe = scratch759::Probe::start(&self.0);
                let mut stmt = self.0.prepare_cached(GET_ROW).map_err(e)?;
                let live = oldest_live(family, now_micros());
                let mut rows = stmt.query(params![row, family, live]).map_err(e)?;
                while let Some(r) = rows.next().map_err(e)? {
                    t.cell(
                        r.get_ref(0)
                            .map_err(e)?
                            .as_blob()
                            .map_err(|e| e.to_string())?,
                    );
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
                let (mut seen, mut current): (u32, Option<Vec<u8>>) = (0, None);
                while let Some(r) = rows.next().map_err(e)? {
                    let blob = |i: usize| -> Result<&[u8], String> {
                        r.get_ref(i)
                            .map_err(e)?
                            .as_blob()
                            .map_err(|e| e.to_string())
                    };
                    if qualifier_prefix
                        .as_deref()
                        .is_some_and(|q| !blob(1).is_ok_and(|v| v.starts_with(q)))
                    {
                        continue;
                    }
                    let key = blob(0)?;
                    if current.as_deref() != Some(key) {
                        if seen == *len {
                            break;
                        }
                        seen += 1;
                        current = Some(key.to_vec());
                    }
                    t.cell(blob(2)?);
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

/// SCRATCH #759: per row read, the pages SQLite's page cache served and the ones it read
/// from the file (`sqlite3_db_status` CACHE_HIT / CACHE_MISS), by latency bucket, during the
/// measured phase. Written to `PHDB_SQLITE_OUT`.
mod scratch759 {
    use std::cell::RefCell;
    use std::sync::Mutex;
    use std::time::Instant;

    use rusqlite::{Connection, ffi};

    const EDGES_US: [u64; 7] = [10, 20, 30, 40, 60, 100, u64::MAX];
    /// Per bucket: reads, ns, pages hit, pages missed, reads with a miss.
    type Stats = ([[u64; 5]; 7], Vec<u64>);
    static ALL: Mutex<Option<Stats>> = Mutex::new(None);

    struct Local(Stats);
    impl Drop for Local {
        fn drop(&mut self) {
            if self.0.0.iter().all(|b| b[0] == 0) {
                return;
            }
            let mut all = ALL.lock().unwrap();
            let all = all.get_or_insert_with(|| ([[0; 5]; 7], vec![0; 501]));
            for (a, b) in all.0.iter_mut().zip(&self.0.0) {
                for (x, y) in a.iter_mut().zip(b) {
                    *x += y;
                }
            }
            for (a, b) in all.1.iter_mut().zip(&self.0.1) {
                *a += b;
            }
            let Ok(path) = std::env::var("PHDB_SQLITE_OUT") else {
                return;
            };
            let n: u64 = all.1.iter().sum();
            let pct = |q: f64| {
                let want = (n as f64 * q).ceil() as u64;
                let mut acc = 0;
                for (i, c) in all.1.iter().enumerate() {
                    acc += c;
                    if acc >= want {
                        return i;
                    }
                }
                500
            };
            let mut out = format!(
                "sqlite row reads {n}: p50 {} us, p99 {} us, p99.9 {} us\nbucket_us count share us | pages/read hit missed | reads with a miss | page cache hit rate\n",
                pct(0.5), pct(0.99), pct(0.999)
            );
            let mut lo = 0;
            for (b, &hi) in all.0.iter().zip(&EDGES_US) {
                let c = b[0].max(1) as f64;
                out += &format!(
                    "{lo}-{} {} {:.2}% {:.1} | {:.2} {:.2} | {:.1}% | {:.1}%\n",
                    if hi == u64::MAX { "inf".into() } else { hi.to_string() },
                    b[0],
                    100.0 * b[0] as f64 / n.max(1) as f64,
                    b[1] as f64 / c / 1e3,
                    b[2] as f64 / c,
                    b[3] as f64 / c,
                    100.0 * b[4] as f64 / c,
                    100.0 * b[2] as f64 / ((b[2] + b[3]).max(1) as f64),
                );
                lo = hi;
            }
            let _ = std::fs::write(path, out);
        }
    }

    thread_local! {
        static LOCAL: RefCell<Local> = RefCell::new(Local(([[0; 5]; 7], vec![0; 501])));
    }

    #[allow(unsafe_code)]
    fn status(c: &Connection, op: i32) -> u64 {
        let (mut cur, mut hi) = (0, 0);
        // SAFETY: the handle is the open connection's; db_status only reads counters.
        unsafe { ffi::sqlite3_db_status(c.handle(), op, &mut cur, &mut hi, 0) };
        cur as u64
    }

    pub struct Probe<'a> {
        c: &'a Connection,
        t: Instant,
        hit: u64,
        miss: u64,
    }

    impl<'a> Probe<'a> {
        pub fn start(c: &'a Connection) -> Option<Self> {
            pigeonhole_engine::phases759::ON
                .load(std::sync::atomic::Ordering::Relaxed)
                .then(|| Probe {
                    c,
                    hit: status(c, ffi::SQLITE_DBSTATUS_CACHE_HIT),
                    miss: status(c, ffi::SQLITE_DBSTATUS_CACHE_MISS),
                    t: Instant::now(),
                })
        }
    }

    impl Drop for Probe<'_> {
        fn drop(&mut self) {
            let ns = self.t.elapsed().as_nanos() as u64;
            let hit = status(self.c, ffi::SQLITE_DBSTATUS_CACHE_HIT).saturating_sub(self.hit);
            let miss = status(self.c, ffi::SQLITE_DBSTATUS_CACHE_MISS).saturating_sub(self.miss);
            let _ = LOCAL.try_with(|l| {
                let s = &mut l.borrow_mut().0;
                let us = ns / 1000;
                s.1[(us as usize).min(500)] += 1;
                let i = EDGES_US.iter().position(|&e| us < e).unwrap_or(6);
                let b = &mut s.0[i];
                b[0] += 1;
                b[1] += ns;
                b[2] += hit;
                b[3] += miss;
                b[4] += u64::from(miss > 0);
            });
        }
    }
}
