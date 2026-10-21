//! Output: every command builds a [`Report`], rendered in the format `--format` names.
//!
//! - `json`: the report's document, pretty-printed. Its shape is stable within `"phdb": 1`:
//!   fields may be added, never renamed or removed (consumers ignore unknown fields).
//! - `jsonl`: a header object (`"phdb": 1`, the command, the summary fields), then one
//!   object per row.
//! - `csv`: a header line of column names, then one line per row (RFC 4180 quoting).
//! - `table`: the summary as `name  value` lines, then the rows' brief columns aligned.

use std::io::{self, Write};

use serde_json::{Map, Value};

use crate::args::Format;

/// A column of a report's rows.
pub(crate) struct Column {
    pub name: &'static str,
    /// Shown in `table` output (every column goes to `csv` and `jsonl`).
    pub brief: bool,
}

/// What a command produced.
pub(crate) struct Report {
    /// The `json` document (without the `phdb` and `command` fields, which are added).
    pub document: Map<String, Value>,
    /// Scalar fields for the `jsonl` header and the `table` summary, in order.
    pub summary: Vec<(&'static str, Value)>,
    pub columns: Vec<Column>,
    /// Each row has one value per column.
    pub rows: Vec<Vec<Value>>,
    pub command: &'static str,
    /// The exit status after rendering (`check` exits 3 when it found damage).
    pub status: crate::ExitStatus,
}

/// Writes `report` to `out` in `format`.
pub(crate) fn render(report: &Report, format: Format, out: &mut dyn Write) -> io::Result<()> {
    match format {
        Format::Json => {
            let mut doc = header(report);
            doc.extend(report.document.clone());
            serde_json::to_writer_pretty(&mut *out, &Value::Object(doc))?;
            writeln!(out)
        }
        Format::Jsonl => {
            let mut head = header(report);
            head.extend(
                report
                    .summary
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), v.clone())),
            );
            writeln!(out, "{}", Value::Object(head))?;
            for row in &report.rows {
                let obj: Map<String, Value> = report
                    .columns
                    .iter()
                    .zip(row)
                    .map(|(c, v)| (c.name.to_owned(), v.clone()))
                    .collect();
                writeln!(out, "{}", Value::Object(obj))?;
            }
            Ok(())
        }
        Format::Csv => {
            let names: Vec<String> = report.columns.iter().map(|c| csv_field(c.name)).collect();
            writeln!(out, "{}", names.join(","))?;
            for row in &report.rows {
                let fields: Vec<String> = row.iter().map(|v| csv_field(&plain(v, ""))).collect();
                writeln!(out, "{}", fields.join(","))?;
            }
            Ok(())
        }
        Format::Table => table(report, out),
    }
}

fn header(report: &Report) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("phdb".into(), 1.into());
    m.insert("command".into(), report.command.into());
    m
}

/// A value as plain text: strings unquoted, `null` as `none`, and the JSON shapes of bytes
/// and typed values (`{"base64": …}`, `{"i64": n}`, `{"f64": x}`, `{"f64_bits": n}`) as
/// `b64:…` and plain numbers.
fn plain(v: &Value, none: &str) -> String {
    match v {
        Value::Null => none.to_owned(),
        Value::String(s) => s.clone(),
        Value::Object(m) if m.len() == 1 => match m.iter().next() {
            Some((k, Value::String(b))) if k == "base64" => format!("b64:{b}"),
            Some((k, n)) if k == "i64" || k == "f64" => n.to_string(),
            Some((k, Value::Number(n))) if k == "f64_bits" => n
                .as_u64()
                .map_or_else(|| n.to_string(), |b| f64::from_bits(b).to_string()),
            _ => v.to_string(),
        },
        other => other.to_string(),
    }
}

/// One CSV field, quoted when it holds a comma, quote, CR or LF.
pub(crate) fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

fn table(report: &Report, out: &mut dyn Write) -> io::Result<()> {
    let width = report
        .summary
        .iter()
        .map(|(k, _)| k.len())
        .max()
        .unwrap_or(0);
    for (k, v) in &report.summary {
        writeln!(out, "{k:<width$}  {}", plain(v, "-"))?;
    }
    let cols: Vec<usize> = (0..report.columns.len())
        .filter(|&i| report.columns[i].brief)
        .collect();
    if cols.is_empty() || report.rows.is_empty() {
        return Ok(());
    }
    if !report.summary.is_empty() {
        writeln!(out)?;
    }
    let cells: Vec<Vec<String>> = report
        .rows
        .iter()
        .map(|r| cols.iter().map(|&i| plain(&r[i], "-")).collect())
        .collect();
    let widths: Vec<usize> = cols
        .iter()
        .enumerate()
        .map(|(j, &i)| {
            cells
                .iter()
                .map(|r| r[j].chars().count())
                .chain([report.columns[i].name.len()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let line = |out: &mut dyn Write, fields: &[&str]| -> io::Result<()> {
        let mut s = String::new();
        for (j, f) in fields.iter().enumerate() {
            if j > 0 {
                s.push_str("  ");
            }
            s.push_str(f);
            if j + 1 < fields.len() {
                s.extend(std::iter::repeat_n(' ', widths[j] - f.chars().count()));
            }
        }
        writeln!(out, "{s}")
    };
    let names: Vec<&str> = cols.iter().map(|&i| report.columns[i].name).collect();
    line(out, &names)?;
    for r in &cells {
        let fields: Vec<&str> = r.iter().map(String::as_str).collect();
        line(out, &fields)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_quotes_only_when_needed() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_field("two\nlines"), "\"two\nlines\"");
    }
}
