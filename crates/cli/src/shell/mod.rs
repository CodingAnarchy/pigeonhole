//! `phdb shell`: a small command language over the public API, one command per line. Not
//! SQL (spec Risks: no SQL creep): each command is one API call with flags.
//!
//! ```text
//! get    <table> <row> [<family>[:<qualifier>]] [--versions N] [--time A..B] [--at SNAP]
//! scan   <table> [<start> [<end>]] [--prefix P] [--family F]... [--qualifier-prefix Q]
//!        [--value-eq V | --value-prefix V | --i64 <op><n>] [--versions N] [--time A..B]
//!        [--limit N] [--columns-per-row N] [--at SNAP]
//! put    <table> <row> <family>:<qualifier> <value> [--ts US] [--i64 | --f64]
//! incr   <table> <row> <family>:<qualifier> <delta> [--ts US]
//! delete <table> <row> [<family>[:<qualifier>]] [--ts US]
//! create <table> <family>[:counter]...
//! snapshot [--durable] <name>        release <name>
//! \tables  \schema [table]  \info  \metrics  \health  \format F  \timing on|off  \help  \quit
//! ```
//!
//! Byte literals are in [`lex`]. Reads print one row per cell: `row`, `family`, `qualifier`,
//! `ts`, `value`.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::ops::Bound;
use std::path::Path;
use std::time::Instant;

use pigeonhole::{Family, ReadTable, RowMutation, RowRef, Snapshot, Table, ValueFilter};
use serde_json::{Map, Value, json};

use crate::ExitStatus;
use crate::args::{Cli, Format};
use crate::cmd::dump::value_json;
use crate::cmd::schema::{bytes_json, family_json};
use crate::exit::Failure;
use crate::open::{self, Db};
use crate::output::{self, Column, Report};

pub(crate) mod lex;

use lex::Word;

const HELP: &str = "\
commands (one per line; values are bytes: \"quoted\\n\", 0xhex, b64:base64):
  get    <table> <row> [<family>[:<qualifier>]] [--versions N] [--time A..B] [--at SNAP]
  scan   <table> [<start> [<end>]] [--prefix P] [--family F]... [--qualifier-prefix Q]
         [--value-eq V | --value-prefix V | --i64 <op><n>] [--versions N] [--time A..B]
         [--limit N] [--columns-per-row N] [--at SNAP]
  put    <table> <row> <family>:<qualifier> <value> [--ts US] [--i64 | --f64]
  incr   <table> <row> <family>:<qualifier> <delta> [--ts US]
  delete <table> <row> [<family>[:<qualifier>]] [--ts US]
  create <table> <family>[:counter]...
  snapshot [--durable] <name>     release <name>
  \\tables  \\schema [table]  \\info  \\metrics  \\health
  \\format table|json|jsonl|csv  \\timing on|off  \\help  \\quit
";

/// What a command asks of the session loop.
enum Flow {
    Continue,
    Quit,
}

/// Runs a shell on `file`: the `-c` commands, else the `-f` script, else standard input
/// (interactive when it is a terminal). Without `--read-only`, it opens as
/// the writer when no writer runs; with a writer running it is a read-only reader process.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    cli: &Cli,
    file: &Path,
    commands: &[String],
    script: Option<&Path>,
    read_only: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<ExitStatus, Failure> {
    let db = open::for_read(cli, file)?;
    let writable = !read_only && matches!(db, Db::Writer(_));
    let mut session = Session {
        db,
        writable,
        snapshots: HashMap::new(),
        format: cli.format(),
        timing: false,
        path: file.to_path_buf(),
    };
    let interactive = commands.is_empty() && script.is_none() && is_terminal();
    let status = if interactive {
        if !writable {
            let _ = writeln!(
                stderr,
                "phdb: read-only ({}): writes are refused",
                session.db.mode()
            );
        }
        session.repl(stdout, stderr)
    } else {
        let lines: Box<dyn Iterator<Item = std::io::Result<String>> + '_> = if !commands.is_empty()
        {
            Box::new(commands.iter().cloned().map(Ok))
        } else if let Some(p) = script {
            let f = std::fs::File::open(p)
                .map_err(|e| Failure::io(&format!("open {}", p.display()), &e))?;
            Box::new(std::io::BufReader::new(f).lines())
        } else {
            Box::new(std::io::stdin().lock().lines())
        };
        session.batch(lines, stdout, stderr)
    };
    let closed = session.db.close();
    let status = status?;
    closed?;
    Ok(status)
}

fn is_terminal() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal()
}

struct Session {
    db: Db,
    writable: bool,
    snapshots: HashMap<String, Snapshot>,
    format: Format,
    timing: bool,
    path: std::path::PathBuf,
}

/// A table handle in either role.
enum Handle {
    Writer(Table),
    Reader(ReadTable),
}

impl Session {
    /// `-c`, `-f` or piped input: the first failing command stops the run with its status.
    fn batch(
        &mut self,
        lines: Box<dyn Iterator<Item = std::io::Result<String>> + '_>,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<ExitStatus, Failure> {
        for (i, line) in lines.enumerate() {
            let line = line.map_err(|e| Failure::io("read commands", &e))?;
            match self.line(&line, stdout, stderr) {
                Ok(Flow::Continue) => {}
                Ok(Flow::Quit) => break,
                Err(f) => {
                    let f = f.at_line(i + 1);
                    f.render(self.format, stderr);
                    return Ok(f.status());
                }
            }
        }
        Ok(ExitStatus::SUCCESS)
    }

    /// The interactive loop: a prompt on standard error, a line from standard input; errors
    /// print and the session goes on. No line editing (rustyline's Windows dependencies are
    /// BSL-1.0, outside D6's licenses; #811): `rlwrap phdb shell …` gives it.
    fn repl(
        &mut self,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<ExitStatus, Failure> {
        let stdin = std::io::stdin();
        let mut line = String::new();
        loop {
            let _ = write!(stderr, "phdb> ");
            let _ = stderr.flush();
            line.clear();
            let n = stdin
                .lock()
                .read_line(&mut line)
                .map_err(|e| Failure::io("read commands", &e))?;
            if n == 0 {
                let _ = writeln!(stderr);
                break;
            }
            match self.line(&line, stdout, stderr) {
                Ok(Flow::Continue) => {}
                Ok(Flow::Quit) => break,
                Err(f) => f.render(self.format, stderr),
            }
            let _ = stdout.flush();
        }
        Ok(ExitStatus::SUCCESS)
    }

    /// One line: run it, render its report, report its time with `\timing on`.
    fn line(
        &mut self,
        line: &str,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<Flow, Failure> {
        let words = lex::split(line).map_err(|m| Failure::usage(&m))?;
        if words.is_empty() {
            return Ok(Flow::Continue);
        }
        let started = Instant::now();
        let (flow, report) = self.exec(&words, stdout)?;
        if let Some(report) = report {
            output::render(&report, self.format, stdout)
                .map_err(|e| Failure::io("write output", &e))?;
        }
        if self.timing {
            let _ = writeln!(
                stderr,
                "time: {:.3} ms",
                started.elapsed().as_secs_f64() * 1e3
            );
        }
        Ok(flow)
    }

    fn exec(
        &mut self,
        words: &[Word],
        stdout: &mut dyn Write,
    ) -> Result<(Flow, Option<Report>), Failure> {
        let name = words[0].text().unwrap_or("");
        let args = Args::parse(&words[1..])?;
        let report = match name {
            "get" => self.get(&args)?,
            "scan" => self.scan(&args)?,
            "put" => self.put(&args)?,
            "incr" => self.incr(&args)?,
            "delete" => self.delete(&args)?,
            "create" => self.create(&args)?,
            "snapshot" => {
                args.only(&["durable"])?;
                let name = args.text(0, "snapshot name")?;
                args.max_positional(1)?;
                let s = self
                    .db
                    .snapshot(args.has("durable"))
                    .map_err(|e| Failure::db("snapshot", &e))?;
                let seqno = s.seqno();
                self.snapshots.insert(name.to_owned(), s);
                message(&[("snapshot", json!(name)), ("seqno", json!(seqno))])
            }
            "release" => {
                args.only(&[])?;
                let name = args.text(0, "snapshot name")?;
                if self.snapshots.remove(name).is_none() {
                    return Err(Failure::input(format!("no snapshot {name}")));
                }
                message(&[("released", json!(name))])
            }
            "\\tables" => {
                let mut names = self.db.tables();
                names.sort();
                let rows = names.into_iter().map(|n| vec![json!(n)]).collect();
                list(&["table"], rows)
            }
            "\\schema" => self.schema(&args)?,
            "\\info" => {
                let info = self.db.info().map_err(|e| Failure::db("info", &e))?;
                crate::cmd::info::report(&self.path, self.db.mode(), &info)
            }
            "\\metrics" => self.metrics()?,
            "\\health" => self.health(),
            "\\format" => {
                let f = args.text(0, "a format")?;
                self.format = match f {
                    "table" => Format::Table,
                    "json" => Format::Json,
                    "jsonl" => Format::Jsonl,
                    "csv" => Format::Csv,
                    _ => return Err(Failure::usage("formats: table, json, jsonl, csv")),
                };
                return Ok((Flow::Continue, None));
            }
            "\\timing" => {
                self.timing = match args.text(0, "on or off")? {
                    "on" => true,
                    "off" => false,
                    _ => return Err(Failure::usage("\\timing on|off")),
                };
                return Ok((Flow::Continue, None));
            }
            "\\help" | "help" => {
                let _ = write!(stdout, "{HELP}");
                return Ok((Flow::Continue, None));
            }
            "\\quit" | "\\q" | "quit" | "exit" => return Ok((Flow::Quit, None)),
            other => {
                return Err(Failure::usage(&format!(
                    "unknown command {other} (\\help lists them)"
                )));
            }
        };
        Ok((Flow::Continue, Some(report)))
    }

    fn handle(&self, table: &str) -> Result<Handle, Failure> {
        let fail = |e: pigeonhole::Error| Failure::db(&format!("table {table}"), &e);
        Ok(match &self.db {
            Db::Writer(db) => Handle::Writer(db.table(table).and_then(|b| b.open()).map_err(fail)?),
            Db::Reader(db) => Handle::Reader(db.table(table).map_err(fail)?),
        })
    }

    fn snapshot_arg(&self, args: &Args) -> Result<Option<&Snapshot>, Failure> {
        match args.value("at") {
            None => Ok(None),
            Some(w) => {
                let name = w.text().unwrap_or("");
                self.snapshots
                    .get(name)
                    .map(Some)
                    .ok_or_else(|| Failure::input(format!("no snapshot {name}")))
            }
        }
    }

    fn get(&self, args: &Args) -> Result<Report, Failure> {
        args.only(&["versions", "time", "at"])?;
        let table = args.text(0, "table")?;
        let row = args.bytes(1, "row")?;
        args.max_positional(3)?;
        let column = args
            .positional
            .get(2)
            .map(|w| column(w, true))
            .transpose()?;
        let snapshot = self.snapshot_arg(args)?;
        let h = self.handle(table)?;
        let mut cells = Vec::new();
        let read = |e: pigeonhole::Error| Failure::db("get", &e);
        macro_rules! go {
            ($t:expr) => {{
                let mut r = $t.row(row);
                if let Some((f, q)) = &column {
                    r = r.family(f);
                    if let Some(q) = q {
                        r = r.qualifier_bounds(Bound::Included(q), Bound::Included(q));
                    }
                }
                if let Some(n) = args.num("versions")? {
                    r = r.versions(n as u32);
                }
                if let Some(t) = args.range("time")? {
                    r = r.time_range(t);
                }
                if let Some(s) = snapshot {
                    r = r.snapshot(s);
                }
                if let Some(row) = r.read().map_err(read)? {
                    push_cells(&mut cells, &row);
                }
            }};
        }
        match &h {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        }
        Ok(list(CELL_COLUMNS, cells))
    }

    fn scan(&self, args: &Args) -> Result<Report, Failure> {
        args.only(&[
            "prefix",
            "family",
            "qualifier-prefix",
            "value-eq",
            "value-prefix",
            "i64",
            "versions",
            "time",
            "limit",
            "columns-per-row",
            "at",
        ])?;
        let table = args.text(0, "table")?;
        args.max_positional(3)?;
        let start = args.positional.get(1).map(|w| w.bytes.clone());
        let end = args.positional.get(2).map(|w| w.bytes.clone());
        let prefix = args.value("prefix").map(|w| w.bytes.clone());
        if prefix.is_some() && start.is_some() {
            return Err(Failure::usage("scan takes a range or --prefix, not both"));
        }
        let filters = [
            args.value("value-eq")
                .map(|w| ValueFilter::Equals(w.bytes.clone())),
            args.value("value-prefix")
                .map(|w| ValueFilter::Prefix(w.bytes.clone())),
            args.value("i64").map(i64_filter).transpose()?,
        ];
        let mut filters = filters.into_iter().flatten();
        let filter = filters.next();
        if filters.next().is_some() {
            return Err(Failure::usage("one value filter at a time"));
        }
        let families: Vec<&str> = args
            .values("family")
            .map(|w| {
                w.text()
                    .ok_or_else(|| Failure::usage("a family name is UTF-8"))
            })
            .collect::<Result<_, _>>()?;
        let snapshot = self.snapshot_arg(args)?;
        let h = self.handle(table)?;
        let mut cells = Vec::new();
        let read = |e: pigeonhole::Error| Failure::db("scan", &e);
        macro_rules! go {
            ($t:expr) => {{
                let mut s = match &prefix {
                    Some(p) => $t.scan_prefix(p),
                    None => $t.scan_bounds(
                        start.as_deref().map_or(Bound::Unbounded, Bound::Included),
                        end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
                    ),
                };
                if !families.is_empty() {
                    s = s.families(families.iter().copied());
                }
                if let Some(q) = args.value("qualifier-prefix") {
                    s = s.qualifier_prefix(&q.bytes);
                }
                if let Some(f) = filter.clone() {
                    s = s.value_filter(f);
                }
                if let Some(n) = args.num("versions")? {
                    s = s.versions(n as u32);
                }
                if let Some(t) = args.range("time")? {
                    s = s.time_range(t);
                }
                if let Some(n) = args.num("limit")? {
                    s = s.limit(n);
                }
                if let Some(n) = args.num("columns-per-row")? {
                    s = s.column_limit(n as u32);
                }
                if let Some(snap) = snapshot {
                    s = s.snapshot(snap);
                }
                let mut it = s.iter().map_err(read)?;
                while let Some(row) = it.next_ref().map_err(read)? {
                    push_cells(&mut cells, &row);
                }
            }};
        }
        match &h {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        }
        Ok(list(CELL_COLUMNS, cells))
    }

    /// A writer table for a write command, refused in read-only mode.
    fn writable(&self, table: &str) -> Result<Table, Failure> {
        if !self.writable {
            return Err(Failure::read_only(self.db.mode()));
        }
        match self.handle(table)? {
            Handle::Writer(t) => Ok(t),
            Handle::Reader(_) => Err(Failure::read_only("reader")),
        }
    }

    fn commit(m: RowMutation<'_>) -> Result<Report, Failure> {
        let info = m.commit().map_err(|e| Failure::db("commit", &e))?;
        Ok(message(&[
            ("result", json!("ok")),
            ("seqno", json!(info.seqno)),
        ]))
    }

    fn put(&self, args: &Args) -> Result<Report, Failure> {
        args.only(&["ts", "i64", "f64"])?;
        let table = args.text(0, "table")?;
        let row = args.bytes(1, "row")?;
        let (family, qualifier) = column(args.word(2, "family:qualifier")?, false)?;
        let qualifier = qualifier.expect("required");
        let value = args.word(3, "value")?;
        args.max_positional(4)?;
        let t = self.writable(table)?;
        let ts = args.num("ts")?;
        let number = || value.text().unwrap_or("");
        let m = t.mutate(row);
        let m = if args.has("i64") {
            let n: i64 = number()
                .parse()
                .map_err(|_| Failure::usage("--i64 takes an integer"))?;
            match ts {
                Some(ts) => m.put_i64_at(&family, &qualifier, ts, n),
                None => m.put_i64(&family, &qualifier, n),
            }
        } else if args.has("f64") {
            let x: f64 = number()
                .parse()
                .map_err(|_| Failure::usage("--f64 takes a number"))?;
            match ts {
                Some(ts) => m.put_f64_at(&family, &qualifier, ts, x),
                None => m.put_f64(&family, &qualifier, x),
            }
        } else {
            match ts {
                Some(ts) => m.put_at(&family, &qualifier, ts, &value.bytes),
                None => m.put(&family, &qualifier, &value.bytes),
            }
        };
        Self::commit(m)
    }

    fn incr(&self, args: &Args) -> Result<Report, Failure> {
        args.only(&["ts"])?;
        let table = args.text(0, "table")?;
        let row = args.bytes(1, "row")?;
        let (family, qualifier) = column(args.word(2, "family:qualifier")?, false)?;
        let qualifier = qualifier.expect("required");
        let delta: i64 = args
            .text(3, "delta")?
            .parse()
            .map_err(|_| Failure::usage("incr takes an integer delta"))?;
        args.max_positional(4)?;
        let t = self.writable(table)?;
        let m = t.mutate(row);
        let m = match args.num("ts")? {
            Some(ts) => m.incr_at(&family, &qualifier, ts, delta),
            None => m.incr(&family, &qualifier, delta),
        };
        Self::commit(m)
    }

    fn delete(&self, args: &Args) -> Result<Report, Failure> {
        args.only(&["ts"])?;
        let table = args.text(0, "table")?;
        let row = args.bytes(1, "row")?;
        args.max_positional(3)?;
        let t = self.writable(table)?;
        let m = t.mutate(row);
        let ts = args.num("ts")?;
        let m = match (
            args.positional
                .get(2)
                .map(|w| column(w, true))
                .transpose()?,
            ts,
        ) {
            (None, None) => m.delete_row(),
            (Some((f, None)), None) => m.delete_family(&f),
            (Some((f, Some(q))), None) => m.delete_column(&f, &q),
            (Some((f, Some(q))), Some(ts)) => m.delete_cell(&f, &q, ts),
            _ => {
                return Err(Failure::usage(
                    "--ts deletes one version: name its family:qualifier",
                ));
            }
        };
        Self::commit(m)
    }

    fn create(&self, args: &Args) -> Result<Report, Failure> {
        args.only(&[])?;
        if !self.writable {
            return Err(Failure::read_only(self.db.mode()));
        }
        let Db::Writer(db) = &self.db else {
            return Err(Failure::read_only("reader"));
        };
        let table = args.text(0, "table")?;
        if args.positional.len() < 2 {
            return Err(Failure::usage("create <table> <family>[:counter]..."));
        }
        let mut b = db.table(table).map_err(|e| Failure::db("create", &e))?;
        for w in &args.positional[1..] {
            let spec = w
                .text()
                .ok_or_else(|| Failure::usage("a family name is UTF-8"))?;
            b = match spec.strip_suffix(":counter") {
                Some(name) => b.family(name, Family::counter()),
                None => b.family(spec, Family::default()),
            };
        }
        b.create().map_err(|e| Failure::db("create", &e))?;
        Ok(message(&[("created", json!(table))]))
    }

    fn schema(&self, args: &Args) -> Result<Report, Failure> {
        args.only(&[])?;
        args.max_positional(1)?;
        let tables = match args.positional.first() {
            Some(w) => vec![w.text().unwrap_or("").to_owned()],
            None => {
                let mut all = self.db.tables();
                all.sort();
                all
            }
        };
        let mut objects = Vec::new();
        for t in &tables {
            let families = self
                .db
                .families(t)
                .map_err(|e| Failure::db(&format!("table {t}"), &e))?;
            for f in &families {
                let mut m = Map::new();
                m.insert("table".into(), json!(t));
                m.extend(family_json(f));
                objects.push(m);
            }
        }
        let columns: Vec<&'static str> = vec![
            "table",
            "name",
            "counter",
            "max_versions",
            "ttl_micros",
            "compression",
            "merge_operator",
        ];
        let rows = objects
            .iter()
            .map(|o| {
                columns
                    .iter()
                    .map(|c| o.get(*c).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        Ok(list(&columns, rows))
    }

    fn metrics(&self) -> Result<Report, Failure> {
        let Db::Writer(db) = &self.db else {
            return Err(Failure::input(
                "\\metrics: a reader process has no writer counters".into(),
            ));
        };
        let m = db.engine_metrics();
        let rows = [
            ("flushes", m.flushes),
            ("compactions", m.compactions),
            ("write_stalls", m.write_stalls),
            ("room_stalls", m.room_stalls),
            ("pacing_stalls", m.pacing_stalls),
            ("block_cache_hits", m.block_cache_hits),
            ("block_cache_misses", m.block_cache_misses),
        ]
        .into_iter()
        .map(|(k, v)| vec![json!(k), json!(v)])
        .collect();
        Ok(list(&["metric", "value"], rows))
    }

    fn health(&self) -> Report {
        let failure = match &self.db {
            Db::Writer(db) => db.failure().map(|f| format!("{f:?}")),
            Db::Reader(_) => None,
        };
        message(&[
            ("mode", json!(self.db.mode())),
            ("writable", json!(self.writable)),
            ("failed", json!(failure.is_some())),
            ("failure", json!(failure)),
        ])
    }
}

const CELL_COLUMNS: &[&str] = &["row", "family", "qualifier", "ts", "value"];

fn push_cells(cells: &mut Vec<Vec<Value>>, row: &RowRef<'_>) {
    for e in row.iter() {
        cells.push(vec![
            bytes_json(row.key()),
            json!(e.family),
            bytes_json(e.qualifier),
            json!(e.cell.timestamp()),
            value_json(e.cell.typed()),
        ]);
    }
}

/// A report of rows only.
fn list(columns: &[&'static str], rows: Vec<Vec<Value>>) -> Report {
    let objects: Vec<Value> = rows
        .iter()
        .map(|r| {
            Value::Object(
                columns
                    .iter()
                    .zip(r)
                    .map(|(c, v)| ((*c).to_owned(), v.clone()))
                    .collect(),
            )
        })
        .collect();
    let mut document = Map::new();
    document.insert("rows".into(), Value::Array(objects));
    Report {
        command: "shell",
        summary: Vec::new(),
        columns: columns
            .iter()
            .map(|&name| Column { name, brief: true })
            .collect(),
        rows,
        document,
        status: ExitStatus::SUCCESS,
    }
}

/// A report of summary fields only.
fn message(fields: &[(&'static str, Value)]) -> Report {
    Report {
        command: "shell",
        summary: fields.to_vec(),
        columns: Vec::new(),
        rows: Vec::new(),
        document: fields
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
        status: ExitStatus::SUCCESS,
    }
}

/// `family` or `family:qualifier` (split at the first `:`). `optional_qualifier`: a bare
/// family is allowed.
fn column(w: &Word, optional_qualifier: bool) -> Result<(String, Option<Vec<u8>>), Failure> {
    let at = w.bytes.iter().position(|&b| b == b':');
    let (f, q) = match at {
        Some(i) => (&w.bytes[..i], Some(w.bytes[i + 1..].to_vec())),
        None if optional_qualifier => (&w.bytes[..], None),
        None => return Err(Failure::usage("expected <family>:<qualifier>")),
    };
    let f = std::str::from_utf8(f).map_err(|_| Failure::usage("a family name is UTF-8"))?;
    Ok((f.to_owned(), q))
}

/// `--i64 <op><n>` with `<`, `=` or `>`: the value compares to `n` as given.
fn i64_filter(w: &Word) -> Result<ValueFilter, Failure> {
    let s = w.text().unwrap_or("");
    let (ord, n) = match s.as_bytes().first() {
        Some(b'<') => (std::cmp::Ordering::Less, &s[1..]),
        Some(b'>') => (std::cmp::Ordering::Greater, &s[1..]),
        Some(b'=') => (std::cmp::Ordering::Equal, &s[1..]),
        _ => return Err(Failure::usage("--i64 takes <n, =n or >n")),
    };
    let n = n
        .parse()
        .map_err(|_| Failure::usage("--i64 takes <n, =n or >n"))?;
    Ok(ValueFilter::I64(ord, n))
}

/// A command's words after its name: positional words, and flags (`--name` or
/// `--name value`).
struct Args {
    positional: Vec<Word>,
    flags: Vec<(String, Option<Word>)>,
}

/// Flags that take no value.
const SWITCHES: &[&str] = &["durable", "i64", "f64"];

impl Args {
    fn parse(words: &[Word]) -> Result<Self, Failure> {
        let mut positional = Vec::new();
        let mut flags = Vec::new();
        let mut it = words.iter();
        while let Some(w) = it.next() {
            if !w.is_flag() {
                positional.push(w.clone());
                continue;
            }
            let name = w.text().unwrap_or("")[2..].to_owned();
            // `--i64` is a switch on put and a filter on scan: it takes a value only when one
            // that looks like a comparison follows.
            let takes_value = !SWITCHES.contains(&name.as_str())
                || (name == "i64"
                    && it
                        .as_slice()
                        .first()
                        .is_some_and(|n| n.bytes.first().is_some_and(|b| b"<=>".contains(b))));
            let value = if takes_value {
                Some(
                    it.next()
                        .cloned()
                        .ok_or_else(|| Failure::usage(&format!("--{name} needs a value")))?,
                )
            } else {
                None
            };
            flags.push((name, value));
        }
        Ok(Self { positional, flags })
    }

    /// Fails on a flag not in `allowed`.
    fn only(&self, allowed: &[&str]) -> Result<(), Failure> {
        match self
            .flags
            .iter()
            .find(|(n, _)| !allowed.contains(&n.as_str()))
        {
            Some((n, _)) => Err(Failure::usage(&format!("unknown flag --{n}"))),
            None => Ok(()),
        }
    }

    fn max_positional(&self, n: usize) -> Result<(), Failure> {
        if self.positional.len() > n {
            return Err(Failure::usage(
                "too many arguments (\\help shows the syntax)",
            ));
        }
        Ok(())
    }

    fn has(&self, name: &str) -> bool {
        self.flags.iter().any(|(n, _)| n == name)
    }

    fn value(&self, name: &str) -> Option<&Word> {
        self.values(name).last()
    }

    fn values<'a>(&'a self, name: &str) -> impl Iterator<Item = &'a Word> + 'a {
        let name = name.to_owned();
        self.flags
            .iter()
            .filter(move |(n, _)| *n == name)
            .filter_map(|(_, v)| v.as_ref())
    }

    fn word(&self, i: usize, what: &str) -> Result<&Word, Failure> {
        self.positional
            .get(i)
            .ok_or_else(|| Failure::usage(&format!("missing {what} (\\help shows the syntax)")))
    }

    fn bytes(&self, i: usize, what: &str) -> Result<&[u8], Failure> {
        Ok(&self.word(i, what)?.bytes)
    }

    fn text(&self, i: usize, what: &str) -> Result<&str, Failure> {
        self.word(i, what)?
            .text()
            .ok_or_else(|| Failure::usage(&format!("{what} must be UTF-8")))
    }

    fn num(&self, name: &str) -> Result<Option<u64>, Failure> {
        self.value(name)
            .map(|w| {
                w.text()
                    .and_then(|t| t.parse().ok())
                    .ok_or_else(|| Failure::usage(&format!("--{name} takes a whole number")))
            })
            .transpose()
    }

    /// `--name A..B`: a half-open range of whole numbers.
    fn range(&self, name: &str) -> Result<Option<std::ops::Range<u64>>, Failure> {
        self.value(name)
            .map(|w| {
                let t = w.text().unwrap_or("");
                let (a, b) = t
                    .split_once("..")
                    .ok_or_else(|| Failure::usage(&format!("--{name} takes A..B")))?;
                let p = |s: &str| {
                    s.parse::<u64>()
                        .map_err(|_| Failure::usage(&format!("--{name} takes A..B")))
                };
                Ok(p(a)?..p(b)?)
            })
            .transpose()
    }
}
