//! `phdb load`: reads a `phdb dump` JSONL export back. Each table line creates its table and
//! families (an existing family keeps its stored options); each cell is written at its
//! timestamp, in batches at `Durability::None`, then one flush makes the load durable.

use std::collections::HashMap;
use std::io::BufRead;
use std::path::Path;

use pigeonhole::{Durability, Pigeonhole, Table, WriteBatch};
use serde_json::{Value, json};

use super::schema::{JsonValue, bytes_from_json, family_from_json, value_from_json};
use crate::ExitStatus;
use crate::args::Cli;
use crate::exit::Failure;
use crate::open;
use crate::output::{Column, Report};

/// Cells per batch commit.
const BATCH_CELLS: usize = 4096;
/// Bytes of keys and values per batch commit, about.
const BATCH_BYTES: usize = 1 << 20;

pub(crate) fn run(
    cli: &Cli,
    file: &Path,
    input: Option<&Path>,
    create: bool,
) -> Result<Report, Failure> {
    let mut reader: Box<dyn BufRead> = match input {
        None => Box::new(std::io::stdin().lock()),
        Some(p) if p == Path::new("-") => Box::new(std::io::stdin().lock()),
        Some(p) => Box::new(std::io::BufReader::new(
            std::fs::File::open(p)
                .map_err(|e| Failure::io(&format!("open {}", p.display()), &e))?,
        )),
    };
    // The header first: an input that isn't a dump (empty, say, from a dump that failed into a
    // pipe) is refused before the database is opened, so `--create` makes nothing (#1037).
    let header_line = header(&mut reader)?;
    let db = open::for_write(cli, file, create)?;
    let loaded = load(&db, reader, header_line);
    // Close even after a failure: what was committed stays, flushed and checkpointed.
    let closed = db.close().map_err(|e| Failure::db("close", &e));
    let n = loaded?;
    closed?;
    let summary = vec![
        ("path", json!(file.display().to_string())),
        ("tables", json!(n.tables)),
        ("cells", json!(n.cells)),
    ];
    let document = summary
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    Ok(Report {
        command: "load",
        summary,
        columns: Vec::<Column>::new(),
        rows: Vec::new(),
        document,
        status: ExitStatus::SUCCESS,
    })
}

#[derive(Default)]
struct Loaded {
    tables: u64,
    cells: u64,
}

/// Reads up to the first non-blank line, which must be a `phdb dump` header, and returns its
/// line number. Anything else, an empty input included, is not a dump (exit 2, #1037).
fn header(reader: &mut Box<dyn BufRead>) -> Result<usize, Failure> {
    let mut at = 0;
    let mut line = String::new();
    loop {
        line.clear();
        at += 1;
        if reader
            .read_line(&mut line)
            .map_err(|e| Failure::io("read the dump", &e))?
            == 0
        {
            return Err(Failure::not_a_dump(
                "the input is empty: not a phdb dump (a dump that failed writes nothing)".into(),
            ));
        }
        if !line.trim().is_empty() {
            break;
        }
    }
    let v: Value = serde_json::from_str(&line)
        .map_err(|e| Failure::not_a_dump(format!("line {at}: not a phdb dump (not JSON: {e})")))?;
    if v["phdb"] != 1 || v["command"] != "dump" {
        return Err(Failure::not_a_dump(format!(
            "line {at}: not a phdb dump (the first line is not its header)"
        )));
    }
    Ok(at)
}

/// Loads the lines after the header, which was line `header_line`.
fn load(db: &Pigeonhole, reader: Box<dyn BufRead>, header_line: usize) -> Result<Loaded, Failure> {
    let mut n = Loaded::default();
    let mut tables: HashMap<String, Table> = HashMap::new();
    let mut batch = db.write_batch();
    let mut batch_bytes = 0usize;
    let commit = |b: WriteBatch| {
        b.commit_with(Durability::None)
            .map(|_| ())
            .map_err(|e| Failure::db("load: commit", &e))
    };
    for (i, line) in reader.lines().enumerate() {
        let at = header_line + i + 1;
        let line = line.map_err(|e| Failure::io("read the dump", &e))?;
        if line.trim().is_empty() {
            continue;
        }
        let bad = |m: &str| Failure::input(format!("line {at}: {m}"));
        let v: Value = serde_json::from_str(&line).map_err(|e| bad(&format!("not JSON: {e}")))?;
        if let Some(families) = v.get("families") {
            let name = v["table"]
                .as_str()
                .ok_or_else(|| bad("a table line without a name"))?;
            let mut builder = db
                .table(name)
                .map_err(|e| Failure::db(&format!("line {at}: table {name}"), &e))?;
            for f in families
                .as_array()
                .ok_or_else(|| bad("families is not a list"))?
            {
                let (fname, family) = family_from_json(f).map_err(|m| bad(&m))?;
                builder = builder.family(&fname, family);
            }
            let table = builder
                .create_if_missing()
                .map_err(|e| Failure::db(&format!("line {at}: table {name}"), &e))?;
            tables.insert(name.to_owned(), table);
            n.tables += 1;
            continue;
        }
        let name = v["table"]
            .as_str()
            .ok_or_else(|| bad("a cell without a table"))?;
        let table = tables
            .get(name)
            .ok_or_else(|| bad(&format!("table {name} has no table line before its cells")))?;
        let row = bytes_from_json(&v["row"]).map_err(|m| bad(&format!("row: {m}")))?;
        let family = v["family"]
            .as_str()
            .ok_or_else(|| bad("a cell without a family"))?;
        let qualifier =
            bytes_from_json(&v["qualifier"]).map_err(|m| bad(&format!("qualifier: {m}")))?;
        let ts = v["ts"]
            .as_u64()
            .ok_or_else(|| bad("a cell without a timestamp"))?;
        let value = &v["value"];
        let mut size = row.len() + qualifier.len() + 16;
        match value_from_json(value).map_err(|m| bad(&format!("value: {m}")))? {
            JsonValue::I64(x) => batch.put_i64_at(table, &row, family, &qualifier, ts, x),
            JsonValue::F64(x) => batch.put_f64_at(table, &row, family, &qualifier, ts, x),
            JsonValue::Bytes(bytes) => {
                size += bytes.len();
                batch.put_at(table, &row, family, &qualifier, ts, &bytes)
            }
        };
        n.cells += 1;
        batch_bytes += size;
        if batch.len() >= BATCH_CELLS || batch_bytes >= BATCH_BYTES {
            commit(std::mem::replace(&mut batch, db.write_batch()))?;
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        commit(batch)?;
    }
    db.flush().map_err(|e| Failure::db("load: flush", &e))?;
    Ok(n)
}
