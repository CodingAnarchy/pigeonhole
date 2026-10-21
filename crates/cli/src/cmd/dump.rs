//! `phdb dump`: every cell, all retained versions, at one snapshot, streamed as `jsonl` (what
//! `phdb load` reads), `csv`, or `arrow`: an Arrow IPC stream of one table, in
//! `pigeonhole-arrow`'s schema with the typed value columns (#691).
//!
//! JSONL lines, in order:
//! - a header: `{"phdb": 1, "command": "dump", "format_version", "snapshot": "latest" |
//!   "durable", "seqno", "mode"}`;
//! - per table, before its cells: `{"table", "families": [<family options>]}`;
//! - per cell: `{"table", "row", "family", "qualifier", "ts", "value"}`. Keys, qualifiers and
//!   byte values are strings when valid UTF-8, else `{"base64": "…"}`; typed values are
//!   `{"i64": n}` or `{"f64": x}` (`{"f64_bits": n}` for a NaN or infinity).

use std::io::{BufWriter, Write};
use std::path::Path;

use pigeonhole::Value as Cell;
use serde_json::{Value, json};

use super::schema::{base64_encode, bytes_json, family_json};
use crate::ExitStatus;
use crate::args::{Cli, Format};
use crate::exit::Failure;
use crate::open;

pub(crate) fn run(
    cli: &Cli,
    file: &Path,
    tables: &[String],
    prefix: Option<&str>,
    durable: bool,
    stdout: &mut dyn Write,
) -> Result<ExitStatus, Failure> {
    let format = cli.format();
    if !matches!(format, Format::Jsonl | Format::Csv | Format::Arrow) {
        return Err(Failure::usage(
            "dump writes jsonl, csv or arrow (--format jsonl|csv|arrow)",
        ));
    }
    let db = open::for_read(cli, file)?;
    let result = dump(
        &db,
        format,
        tables,
        prefix.unwrap_or("").as_bytes(),
        durable,
        stdout,
    );
    db.close()?;
    match result {
        Ok(()) => Ok(ExitStatus::SUCCESS),
        Err(Out::Fail(f)) => Err(f),
        // A closed pipe (`phdb dump … | head`) is not an error worth a message.
        Err(Out::Io(e)) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(ExitStatus::SUCCESS),
        Err(Out::Io(e)) => Err(Failure::io("write output", &e)),
    }
}

enum Out {
    Fail(Failure),
    Io(std::io::Error),
}

impl From<std::io::Error> for Out {
    fn from(e: std::io::Error) -> Self {
        Out::Io(e)
    }
}

impl From<Failure> for Out {
    fn from(f: Failure) -> Self {
        Out::Fail(f)
    }
}

fn dump(
    db: &open::Db,
    format: Format,
    only: &[String],
    prefix: &[u8],
    durable: bool,
    stdout: &mut dyn Write,
) -> Result<(), Out> {
    let tables = if only.is_empty() {
        let mut all = db.tables();
        all.sort();
        all
    } else {
        only.to_vec()
    };
    let info = db.info().map_err(|e| Failure::db("dump", &e))?;
    let snapshot = db
        .snapshot(durable)
        .map_err(|e| Failure::db("dump: snapshot", &e))?;
    if format == Format::Arrow {
        let [table] = &tables[..] else {
            return Err(Failure::usage(
                "dump --format arrow writes one table: name it with --table",
            )
            .into());
        };
        return arrow(db, table, prefix, &snapshot, stdout);
    }
    let mut out = BufWriter::with_capacity(1 << 16, stdout);
    match format {
        Format::Csv => writeln!(out, "table,row,family,qualifier,ts,type,value")?,
        _ => writeln!(
            out,
            "{}",
            json!({
                "phdb": 1,
                "command": "dump",
                "format_version": info.format_version,
                "snapshot": if durable { "durable" } else { "latest" },
                "seqno": snapshot.seqno(),
                "mode": db.mode(),
            })
        )?,
    }
    for table in &tables {
        let families = db
            .families(table)
            .map_err(|e| Failure::db(&format!("dump: table {table}"), &e))?;
        if format == Format::Jsonl {
            let families: Vec<Value> = families
                .iter()
                .map(|f| Value::Object(family_json(f)))
                .collect();
            writeln!(out, "{}", json!({"table": table, "families": families}))?;
        }
        db.scan(table, prefix, &snapshot, &mut |row| -> Result<(), Out> {
            for entry in row.iter() {
                let cell = entry.cell;
                match format {
                    Format::Csv => csv_line(
                        &mut out,
                        table,
                        row.key(),
                        entry.family,
                        entry.qualifier,
                        cell.timestamp(),
                        cell.typed(),
                    )?,
                    _ => {
                        let line = json!({
                            "table": table,
                            "row": bytes_json(row.key()),
                            "family": entry.family,
                            "qualifier": bytes_json(entry.qualifier),
                            "ts": cell.timestamp(),
                            "value": value_json(cell.typed()),
                        });
                        writeln!(out, "{line}")?;
                    }
                }
            }
            Ok(())
        })?;
    }
    out.flush()?;
    Ok(())
}

/// One table as an Arrow IPC stream: `pigeonhole-arrow`'s long schema (one Arrow row per
/// cell) with `value_i64` and `value_f64`, every retained version, at `snapshot`.
fn arrow(
    db: &open::Db,
    table: &str,
    prefix: &[u8],
    snapshot: &pigeonhole::Snapshot,
    stdout: &mut dyn Write,
) -> Result<(), Out> {
    use pigeonhole_arrow::arrow_schema::ArrowError;
    let options = pigeonhole_arrow::ArrowOptions::default().typed_values(true);
    let export = db.arrow(table, prefix, snapshot, &options)?;
    let out = BufWriter::with_capacity(1 << 16, stdout);
    let fail = |e: ArrowError| -> Out {
        match e {
            ArrowError::IoError(_, e) => Out::Io(e),
            ArrowError::ExternalError(e) => match e.downcast::<pigeonhole::Error>() {
                Ok(e) => Failure::db(&format!("read table {table}"), &e).into(),
                Err(e) => Failure::export(format!("dump --format arrow: {e}")).into(),
            },
            // The export reports a cell an Arrow batch can't hold (a timestamp above
            // i64::MAX, a key or value over the 32-bit offsets) as an invalid argument.
            e @ ArrowError::InvalidArgumentError(_) => {
                Failure::unsupported(format!("dump --format arrow: {e}")).into()
            }
            // Anything else (the IPC writer's own errors) is a plain failure.
            e => Failure::export(format!("dump --format arrow: {e}")).into(),
        }
    };
    let mut w = arrow_ipc::writer::StreamWriter::try_new(out, &export.schema()).map_err(fail)?;
    for batch in export {
        w.write(&batch.map_err(fail)?).map_err(fail)?;
    }
    w.finish().map_err(fail)?;
    w.into_inner().map_err(fail)?.flush()?;
    Ok(())
}

pub(crate) fn value_json(v: Cell<'_>) -> Value {
    match v {
        Cell::Bytes(b) => bytes_json(b),
        Cell::I64(n) | Cell::Varint(n) => json!({ "i64": n }),
        Cell::F64(x) if x.is_finite() => json!({ "f64": x }),
        Cell::F64(x) => json!({ "f64_bits": x.to_bits() }),
    }
}

/// One CSV line. Keys and qualifiers that are not valid UTF-8 print as `b64:` and base64; a
/// value's `type` column says how to read it: `bytes`, `base64`, `i64` or `f64`.
fn csv_line(
    out: &mut dyn Write,
    table: &str,
    row: &[u8],
    family: &str,
    qualifier: &[u8],
    ts: u64,
    value: Cell<'_>,
) -> std::io::Result<()> {
    let text = |b: &[u8]| match std::str::from_utf8(b) {
        Ok(s) => s.to_owned(),
        Err(_) => format!("b64:{}", base64_encode(b)),
    };
    let (ty, v) = match value {
        Cell::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => ("bytes", s.to_owned()),
            Err(_) => ("base64", base64_encode(b)),
        },
        Cell::I64(n) | Cell::Varint(n) => ("i64", n.to_string()),
        Cell::F64(x) => ("f64", x.to_string()),
    };
    let fields = [
        table.to_owned(),
        text(row),
        family.to_owned(),
        text(qualifier),
        ts.to_string(),
        ty.to_owned(),
        v,
    ];
    let line: Vec<String> = fields.iter().map(|f| crate::output::csv_field(f)).collect();
    writeln!(out, "{}", line.join(","))
}
