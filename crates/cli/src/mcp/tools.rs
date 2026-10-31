//! The tools `phdb mcp` serves: a few composable ones over the public API, each with a JSON
//! schema and a description written for an agent.
//!
//! | Tool | What it does |
//! |---|---|
//! | `list_tables` | Tables and their families' options. |
//! | `read_row` | One row's cells, paged at column boundaries. |
//! | `scan` | Rows in key order: a prefix or a range, with a row limit. |
//! | `traverse` | Breadth-first hops over an edge family (`Table::traverse`). |
//! | `write` | Puts, increments, aggregate inputs and deletes in one atomic batch. |
//! | `create_table` | A table and its families, if missing. |
//!
//! The JSON shapes of bytes and values are `phdb dump`'s. A result that would pass
//! [`Limits::result_bytes`] stops at a column, row or node boundary with a `cursor`; a
//! value longer than its limit comes back as `{"omitted_bytes": n}`. Nearest-neighbour
//! search joins the list once vector families' reads are in `pigeonhole` (D303, D296).

use std::collections::HashMap;
use std::ops::Bound;

use pigeonhole::{CellEntry, KeySet, RowPosition, Table, Value as Cell};
use serde_json::{Map, Value, json};

use super::Limits;
use super::cursor::Cursor;
use crate::cmd::dump::value_json;
use crate::cmd::schema::{
    JsonValue, bytes_from_json, bytes_json, family_from_json, family_json, value_from_json,
};
use crate::exit::Failure;
use crate::open::{Db, Handle};

/// The most rows a `scan`, and columns per row, one call may ask for.
const MAX_ROWS: u64 = 10_000;
/// The most versions per column a read may ask for.
const MAX_VERSIONS: u64 = 1_000;
/// The most seeds of a `traverse`, and operations in a `write`.
const MAX_ITEMS: usize = 10_000;
/// A traversal's visited-node bound unless the call sets one (the engine's default,
/// 10,000,000, is the most it may ask for).
const DEFAULT_MAX_NODES: u64 = 100_000;
const MAX_NODES: u64 = 10_000_000;

type Outcome = Result<Map<String, Value>, Failure>;

pub(super) struct Tools {
    db: Db,
    writable: bool,
    limits: Limits,
}

impl Tools {
    pub(super) fn new(db: Db, writable: bool, limits: Limits) -> Self {
        Self {
            db,
            writable,
            limits,
        }
    }

    pub(super) fn close(self) -> Result<(), Failure> {
        self.db.close()
    }

    /// `tools/list`: the write tools only when the database is writable.
    pub(super) fn list(&self) -> Vec<Value> {
        let mut tools = vec![list_tables(), read_row(), scan(), traverse()];
        if self.writable {
            tools.extend([write(), create_table()]);
        }
        tools
    }

    /// `tools/call`, or `None` for a tool this server doesn't have.
    pub(super) fn call(&self, name: &str, args: &Map<String, Value>) -> Option<Outcome> {
        let a = Args(args);
        Some(match name {
            "list_tables" => self.list_tables(a),
            "read_row" => self.read_row(a),
            "scan" => self.scan(a),
            "traverse" => self.traverse(a),
            "write" => self.write(a),
            "create_table" => self.create_table(a),
            _ => return None,
        })
    }

    fn list_tables(&self, a: Args<'_>) -> Outcome {
        a.only(&["table"])?;
        let names = match a.opt_str("table")? {
            Some(t) => vec![t.to_owned()],
            None => {
                let mut all = self.db.tables();
                all.sort();
                all
            }
        };
        let mut tables = Vec::new();
        for t in names {
            let families = self
                .db
                .families(&t)
                .map_err(|e| Failure::db(&format!("table {t}"), &e))?;
            let families: Vec<Value> = families.iter().map(family_brief).collect();
            tables.push(json!({"name": t, "families": families}));
        }
        Ok(object(json!({
            "tables": tables,
            "mode": self.db.mode(),
            "writable": self.writable,
        })))
    }

    fn read_row(&self, a: Args<'_>) -> Outcome {
        a.only(&[
            "table",
            "row",
            "families",
            "qualifier",
            "qualifier_prefix",
            "versions",
            "max_value_bytes",
            "cursor",
        ])?;
        let table = a.str("table")?;
        let row = a.bytes("row")?;
        let sel = Selection::parse(&a)?;
        let max_value = self.max_value(&a)?;
        let resume = match Cursor::decode(a.0).map_err(|m| Failure::usage(&m))? {
            None => None,
            Some(Cursor::Row(b)) => {
                Some(RowPosition::from_bytes(&b).map_err(|e| Failure::db("cursor", &e))?)
            }
            Some(_) => return Err(Failure::usage("cursor: not a read_row cursor")),
        };
        let read = |e: pigeonhole::Error| Failure::db(&format!("read_row {table}"), &e);
        let mut budget = Budget::new(self.limits.result_bytes);
        let mut cells = Vec::new();
        let mut cut = None;
        macro_rules! go {
            ($t:expr) => {{
                let mut r = $t.row(&row).versions(sel.versions);
                if !sel.families.is_empty() {
                    r = r.families(sel.families.iter().map(String::as_str));
                }
                if let Some(q) = &sel.qualifier_prefix {
                    r = r.qualifier_prefix(q);
                }
                if let Some(q) = &sel.qualifier {
                    r = r.qualifier_bounds(Bound::Included(q), Bound::Included(q));
                }
                if let Some(p) = &resume {
                    r = r.resume_after(p);
                }
                // Pages of one cell end on column boundaries: each is one column, all its
                // versions, and its position resumes after it.
                let mut pages = r.pages(1).map_err(read)?;
                let mut last = None;
                loop {
                    let column: Vec<Value> = match pages.next().map_err(read)? {
                        None => break,
                        Some(page) => page.iter().map(|e| cell_json(&e, max_value)).collect(),
                    };
                    if !budget.take(&column) {
                        cut = last;
                        break;
                    }
                    cells.extend(column);
                    last = pages.position();
                }
            }};
        }
        match self.db.handle(table)? {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        }
        let mut out = object(json!({"table": table, "row": bytes_json(&row), "cells": cells}));
        if let Some(p) = cut {
            out.insert(
                "cursor".into(),
                Cursor::Row(p.to_bytes()).encode(a.0).into(),
            );
        }
        Ok(out)
    }

    fn scan(&self, a: Args<'_>) -> Outcome {
        a.only(&[
            "table",
            "prefix",
            "start",
            "end",
            "families",
            "qualifier",
            "qualifier_prefix",
            "versions",
            "limit",
            "columns_per_row",
            "max_value_bytes",
            "cursor",
        ])?;
        let table = a.str("table")?;
        let sel = Selection::parse(&a)?;
        let max_value = self.max_value(&a)?;
        let limit = a.u64_or("limit", 100, 1, MAX_ROWS)?;
        let columns = a.u64_or("columns_per_row", 100, 1, MAX_ROWS)?;
        let prefix = a.opt_bytes("prefix")?;
        let (start, end) = (a.opt_bytes("start")?, a.opt_bytes("end")?);
        if prefix.is_some() && (start.is_some() || end.is_some()) {
            return Err(Failure::usage(
                "scan takes a prefix or a start/end range, not both",
            ));
        }
        let (mut lo, hi) = match prefix {
            Some(p) => {
                let hi = successor(&p).map_or(Bound::Unbounded, Bound::Excluded);
                (Bound::Included(p), hi)
            }
            None => (
                start.map_or(Bound::Unbounded, Bound::Included),
                end.map_or(Bound::Unbounded, Bound::Excluded),
            ),
        };
        match Cursor::decode(a.0).map_err(|m| Failure::usage(&m))? {
            None => {}
            Some(Cursor::Scan { key, inclusive }) => {
                lo = if inclusive {
                    Bound::Included(key)
                } else {
                    Bound::Excluded(key)
                };
            }
            Some(_) => return Err(Failure::usage("cursor: not a scan cursor")),
        }
        let read = |e: pigeonhole::Error| Failure::db(&format!("scan {table}"), &e);
        let mut budget = Budget::new(self.limits.result_bytes);
        let mut rows = Vec::new();
        let mut cut = None;
        macro_rules! go {
            ($t:expr) => {{
                let mut s = $t
                    .scan_bounds(
                        lo.as_ref().map(Vec::as_slice),
                        hi.as_ref().map(Vec::as_slice),
                    )
                    .versions(sel.versions)
                    .column_limit(columns as u32)
                    // One row past the limit says whether there are more.
                    .limit(limit + 1);
                if !sel.families.is_empty() {
                    s = s.families(sel.families.iter().map(String::as_str));
                }
                if let Some(q) = &sel.qualifier_prefix {
                    s = s.qualifier_prefix(q);
                }
                if let Some(q) = &sel.qualifier {
                    s = s.qualifier_bounds(Bound::Included(q), Bound::Included(q));
                }
                let mut it = s.iter().map_err(read)?;
                while let Some(row) = it.next_ref().map_err(read)? {
                    let key = row.key().to_vec();
                    if rows.len() as u64 == limit {
                        cut = Some(Cursor::Scan { key, inclusive: true });
                        break;
                    }
                    let mut cols: Vec<Vec<Value>> = Vec::new();
                    let mut at: Option<(&str, &[u8])> = None;
                    for e in row.iter() {
                        if at != Some((e.family, e.qualifier)) {
                            at = Some((e.family, e.qualifier));
                            cols.push(Vec::new());
                        }
                        cols.last_mut().expect("pushed").push(cell_json(&e, max_value));
                    }
                    let capped = cols.len() as u64 == columns;
                    let head = json!({"row": bytes_json(&key)});
                    let size = budget.size(&head) + cols.iter().map(|c| budget.sum(c)).sum::<usize>();
                    if budget.used + size <= budget.limit {
                        budget.used += size;
                        rows.push(row_json(&key, cols.concat(), capped, false));
                        continue;
                    }
                    if !rows.is_empty() {
                        cut = Some(Cursor::Scan { key, inclusive: true });
                        break;
                    }
                    // A first row too big for the result alone: as many columns as fit (one at
                    // least), and the scan goes on after it. read_row pages through the rest.
                    budget.used += budget.size(&head);
                    let mut kept = Vec::new();
                    for c in cols {
                        if !kept.is_empty() && !budget.take(&c) {
                            break;
                        }
                        kept.extend(c);
                    }
                    rows.push(row_json(&key, kept, capped, true));
                    cut = Some(Cursor::Scan { key, inclusive: false });
                    break;
                }
            }};
        }
        match self.db.handle(table)? {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        }
        let mut out = object(json!({"table": table, "rows": rows}));
        if let Some(c) = cut {
            out.insert("cursor".into(), c.encode(a.0).into());
        }
        Ok(out)
    }

    fn traverse(&self, a: Args<'_>) -> Outcome {
        a.only(&[
            "table",
            "family",
            "seeds",
            "hops",
            "per_node_limit",
            "hop_limit",
            "max_nodes",
            "parents",
            "qualifier_prefix",
            "cursor",
        ])?;
        let table = a.str("table")?;
        let family = a.str("family")?;
        let seeds = a.bytes_list("seeds")?;
        if seeds.is_empty() {
            return Err(Failure::usage("seeds: at least one row key"));
        }
        let hops = a.u64_or("hops", 1, 1, 64)? as u32;
        let per_node = a.opt_u64("per_node_limit", 1, u64::from(u32::MAX))?;
        let hop_limit = a.opt_u64("hop_limit", 1, u64::MAX)?;
        let max_nodes = a.u64_or("max_nodes", DEFAULT_MAX_NODES, 1, MAX_NODES)?;
        let parents = a.bool_or("parents", false)?;
        let qualifier_prefix = a.opt_bytes("qualifier_prefix")?;
        let (from_hop, from_index) = match Cursor::decode(a.0).map_err(|m| Failure::usage(&m))? {
            None => (0, 0),
            Some(Cursor::Traverse { hop, index }) => (hop as usize, index as usize),
            Some(_) => return Err(Failure::usage("cursor: not a traverse cursor")),
        };
        let seeds = KeySet::from_unsorted(&seeds);
        let read = |e: pigeonhole::Error| Failure::db(&format!("traverse {table}"), &e);
        macro_rules! go {
            ($t:expr) => {{
                let mut t = $t
                    .traverse(&seeds)
                    .family(family)
                    .hops(hops)
                    .max_nodes(max_nodes)
                    .parents(parents);
                if let Some(n) = per_node {
                    t = t.per_node_limit(n as u32);
                }
                if let Some(n) = hop_limit {
                    t = t.hop_limit(n);
                }
                if let Some(q) = &qualifier_prefix {
                    t = t.qualifier_prefix(q);
                }
                t.run().map_err(read)?
            }};
        }
        // A cursor runs the same traversal again and skips what was returned: it is
        // deterministic, so over unchanged data the pages join up exactly.
        let traversal = match self.db.handle(table)? {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        };
        let mut budget = Budget::new(self.limits.result_bytes);
        let mut out_hops = Vec::new();
        let mut cut = None;
        'hops: for (h, hop) in traversal.hops().iter().enumerate().skip(from_hop) {
            let prev = if h == 0 {
                &seeds
            } else {
                traversal.hops()[h - 1].nodes()
            };
            let first = if h == from_hop { from_index } else { 0 };
            let mut nodes = Vec::new();
            for (i, node) in hop.nodes().iter().enumerate().skip(first) {
                let v = match hop.parents() {
                    Some(p) => json!({
                        "node": bytes_json(node),
                        "parent": bytes_json(prev.get(p[i] as usize)),
                    }),
                    None => bytes_json(node),
                };
                if !budget.take(std::slice::from_ref(&v)) {
                    cut = Some(Cursor::Traverse {
                        hop: h as u32,
                        index: i as u64,
                    });
                    out_hops.push(hop_json(h, first, hop.nodes().len(), nodes));
                    break 'hops;
                }
                nodes.push(v);
            }
            out_hops.push(hop_json(h, first, hop.nodes().len(), nodes));
        }
        let stats = traversal.stats();
        let mut out = object(json!({
            "table": table,
            "hops": out_hops,
            "stats": {
                "rows": stats.rows,
                "edges": stats.edges,
                "cut_by_max_nodes": stats.cut_by_max_nodes,
            },
        }));
        if let Some(c) = cut {
            out.insert("cursor".into(), c.encode(a.0).into());
        }
        Ok(out)
    }

    fn write(&self, a: Args<'_>) -> Outcome {
        a.only(&["ops"])?;
        let Some(Value::Array(ops)) = a.0.get("ops") else {
            return Err(Failure::usage("ops: a list of operations"));
        };
        if ops.is_empty() || ops.len() > MAX_ITEMS {
            return Err(Failure::usage(&format!(
                "ops: between 1 and {MAX_ITEMS} operations"
            )));
        }
        let Db::Writer(db) = &self.db else {
            return Err(Failure::read_only(self.db.mode()));
        };
        if !self.writable {
            return Err(Failure::read_only(self.db.mode()));
        }
        // Every table first: the batch borrows them.
        let mut tables: HashMap<String, Table> = HashMap::new();
        for (i, op) in ops.iter().enumerate() {
            let name = op["table"]
                .as_str()
                .ok_or_else(|| Failure::usage(&format!("ops[{i}]: table: a table name")))?;
            if !tables.contains_key(name) {
                let t = db
                    .table(name)
                    .and_then(|b| b.open())
                    .map_err(|e| Failure::db(&format!("ops[{i}]: table {name}"), &e))?;
                tables.insert(name.to_owned(), t);
            }
        }
        let mut batch = db.write_batch();
        for (i, op) in ops.iter().enumerate() {
            let Value::Object(op) = op else {
                return Err(Failure::usage(&format!("ops[{i}]: an object")));
            };
            write_op(&mut batch, &tables, Args(op)).map_err(|f| f.within(&format!("ops[{i}]")))?;
        }
        let n = batch.len();
        let info = batch.commit().map_err(|e| Failure::db("write", &e))?;
        Ok(object(json!({"committed": n, "seqno": info.seqno})))
    }

    fn create_table(&self, a: Args<'_>) -> Outcome {
        a.only(&["table", "families"])?;
        let table = a.str("table")?;
        let Db::Writer(db) = &self.db else {
            return Err(Failure::read_only(self.db.mode()));
        };
        if !self.writable {
            return Err(Failure::read_only(self.db.mode()));
        }
        let Some(Value::Array(families)) = a.0.get("families") else {
            return Err(Failure::usage("families: a list of families"));
        };
        if families.is_empty() {
            return Err(Failure::usage("families: at least one"));
        }
        let fail = |e: pigeonhole::Error| Failure::db(&format!("create_table {table}"), &e);
        let mut b = db.table(table).map_err(fail)?;
        for (i, f) in families.iter().enumerate() {
            let spec = match f {
                Value::String(name) => json!({ "name": name }),
                other => other.clone(),
            };
            let (name, family) = family_from_json(&spec)
                .map_err(|m| Failure::usage(&format!("families[{i}]: {m}")))?;
            b = b.family(&name, family);
        }
        let t = b.create_if_missing().map_err(fail)?;
        let families: Vec<Value> = t
            .families()
            .into_iter()
            .filter_map(|f| t.family_info(f))
            .map(|f| family_brief(&f))
            .collect();
        Ok(object(json!({"table": table, "families": families})))
    }

    /// A read's value limit: its `max_value_bytes`, at most the result limit.
    fn max_value(&self, a: &Args<'_>) -> Result<usize, Failure> {
        let max = self.limits.result_bytes as u64;
        let v = a.u64_or(
            "max_value_bytes",
            self.limits.value_bytes.min(self.limits.result_bytes) as u64,
            0,
            max,
        )?;
        Ok(v as usize)
    }
}

/// One `write` operation, added to `batch`.
fn write_op(
    batch: &mut pigeonhole::WriteBatch,
    tables: &HashMap<String, Table>,
    op: Args<'_>,
) -> Result<(), Failure> {
    let usage = |m: String| Failure::usage(&m);
    let kind = op.str("op")?;
    let t = &tables[op.str("table")?];
    let row = op.bytes("row")?;
    let ts = op.opt_u64("ts", 0, u64::MAX)?;
    let family = op.opt_str("family")?;
    let qualifier = op.opt_bytes("qualifier")?;
    let column = || match (family, &qualifier) {
        (Some(f), Some(q)) => Ok((f, q.as_slice())),
        _ => Err(usage(format!("{kind} needs a family and a qualifier"))),
    };
    let value = || {
        op.get("value")
            .ok_or_else(|| usage(format!("{kind} needs a value")))
    };
    let value_of = |v: &Value| value_from_json(v).map_err(|m| usage(format!("value: {m}")));
    match kind {
        "put" => {
            op.only(&["op", "table", "row", "family", "qualifier", "value", "ts"])?;
            let (f, q) = column()?;
            match (value_of(value()?)?, ts) {
                (JsonValue::Bytes(b), None) => batch.put(t, &row, f, q, &b),
                (JsonValue::Bytes(b), Some(ts)) => batch.put_at(t, &row, f, q, ts, &b),
                (JsonValue::I64(n), None) => batch.put_i64(t, &row, f, q, n),
                (JsonValue::I64(n), Some(ts)) => batch.put_i64_at(t, &row, f, q, ts, n),
                (JsonValue::F64(x), None) => batch.put_f64(t, &row, f, q, x),
                (JsonValue::F64(x), Some(ts)) => batch.put_f64_at(t, &row, f, q, ts, x),
            };
        }
        "incr" => {
            op.only(&["op", "table", "row", "family", "qualifier", "value", "ts"])?;
            let (f, q) = column()?;
            let v = value()?;
            let delta = v
                .as_i64()
                .or_else(|| v.get("i64").and_then(Value::as_i64))
                .ok_or_else(|| usage("incr: value is an integer delta".into()))?;
            match ts {
                None => batch.incr(t, &row, f, q, delta),
                Some(ts) => batch.incr_at(t, &row, f, q, ts, delta),
            };
        }
        "aggregate" => {
            op.only(&["op", "table", "row", "family", "qualifier", "value", "ts"])?;
            let (f, q) = column()?;
            let v = value()?;
            if let Some(bits) = v.get("u64") {
                let bits = bits
                    .as_u64()
                    .ok_or_else(|| usage("value: u64 is not a u64".into()))?;
                match ts {
                    None => batch.aggregate_u64(t, &row, f, q, bits),
                    Some(ts) => batch.aggregate_u64_at(t, &row, f, q, ts, bits),
                };
                return Ok(());
            }
            let v = match v {
                Value::Number(n) if n.is_i64() => JsonValue::I64(n.as_i64().expect("i64")),
                Value::Number(n) => JsonValue::F64(n.as_f64().unwrap_or(f64::NAN)),
                v => value_of(v)?,
            };
            match (v, ts) {
                (JsonValue::I64(n), None) => batch.aggregate(t, &row, f, q, n),
                (JsonValue::I64(n), Some(ts)) => batch.aggregate_at(t, &row, f, q, ts, n),
                (JsonValue::F64(x), None) => batch.aggregate_f64(t, &row, f, q, x),
                (JsonValue::F64(x), Some(ts)) => batch.aggregate_f64_at(t, &row, f, q, ts, x),
                (JsonValue::Bytes(b), None) => batch.aggregate_bytes(t, &row, f, q, &b),
                (JsonValue::Bytes(b), Some(ts)) => batch.aggregate_bytes_at(t, &row, f, q, ts, &b),
            };
        }
        "delete" => {
            op.only(&["op", "table", "row", "family", "qualifier", "ts"])?;
            match (family, &qualifier, ts) {
                (None, None, None) => batch.delete_row(t, &row),
                (Some(f), None, None) => batch.delete_family(t, &row, f),
                (Some(f), Some(q), None) => batch.delete_column(t, &row, f, q),
                (Some(f), Some(q), Some(ts)) => batch.delete_cell(t, &row, f, q, ts),
                _ => {
                    return Err(usage(
                        "delete: a qualifier needs its family, and ts needs both".into(),
                    ));
                }
            };
        }
        other => {
            return Err(usage(format!(
                "op: {other:?} is not put, incr, aggregate or delete"
            )));
        }
    }
    Ok(())
}

/// The selection `read_row` and `scan` share.
struct Selection {
    families: Vec<String>,
    qualifier: Option<Vec<u8>>,
    qualifier_prefix: Option<Vec<u8>>,
    versions: u32,
}

impl Selection {
    fn parse(a: &Args<'_>) -> Result<Self, Failure> {
        let s = Self {
            families: a.str_list("families")?,
            qualifier: a.opt_bytes("qualifier")?,
            qualifier_prefix: a.opt_bytes("qualifier_prefix")?,
            versions: a.u64_or("versions", 1, 1, MAX_VERSIONS)? as u32,
        };
        if s.qualifier.is_some() && s.qualifier_prefix.is_some() {
            return Err(Failure::usage("qualifier or qualifier_prefix, not both"));
        }
        Ok(s)
    }
}

/// A result's size so far, in bytes of JSON.
struct Budget {
    used: usize,
    limit: usize,
}

impl Budget {
    fn new(limit: usize) -> Self {
        Self { used: 0, limit }
    }

    /// `v`'s size in the result, with its separator.
    fn size(&self, v: &Value) -> usize {
        serde_json::to_string(v).map_or(0, |s| s.len()) + 1
    }

    fn sum(&self, vs: &[Value]) -> usize {
        vs.iter().map(|v| self.size(v)).sum()
    }

    /// Whether `size` more bytes fit: always for the result's first item, so every call
    /// makes progress.
    fn fits(&self, size: usize) -> bool {
        self.used == 0 || self.used + size <= self.limit
    }

    /// Counts `vs` if they fit.
    fn take(&mut self, vs: &[Value]) -> bool {
        let size = self.sum(vs);
        let fits = self.fits(size);
        if fits {
            self.used += size;
        }
        fits
    }
}

/// A cell in a read's result: `dump`'s shape without the table and row, with a byte value
/// longer than `max_value` replaced by its length.
fn cell_json(e: &CellEntry<'_>, max_value: usize) -> Value {
    let value = match e.cell.typed() {
        Cell::Bytes(b) if b.len() > max_value => json!({ "omitted_bytes": b.len() }),
        v => value_json(v),
    };
    json!({
        "family": e.family,
        "qualifier": bytes_json(e.qualifier),
        "ts": e.cell.timestamp(),
        "value": value,
    })
}

/// A family's options as `phdb info` shows them, without the unset ones.
fn family_brief(f: &pigeonhole::FamilyInfo) -> Value {
    let mut m = family_json(f);
    m.retain(|_, v| !v.is_null());
    Value::Object(m)
}

fn row_json(key: &[u8], cells: Vec<Value>, capped: bool, truncated: bool) -> Value {
    let mut row = object(json!({"row": bytes_json(key), "cells": cells}));
    if capped {
        // The row may have more columns than `columns_per_row` let through.
        row.insert("columns_capped".into(), true.into());
    }
    if truncated {
        row.insert("truncated".into(), true.into());
    }
    Value::Object(row)
}

fn hop_json(h: usize, first: usize, len: usize, nodes: Vec<Value>) -> Value {
    json!({"hop": h + 1, "first_index": first, "node_count": len, "nodes": nodes})
}

/// The least key above every key starting with `prefix`, if there is one.
fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut s = prefix.to_vec();
    while let Some(&last) = s.last() {
        if last < 0xff {
            *s.last_mut().expect("non-empty") += 1;
            return Some(s);
        }
        s.pop();
    }
    None
}

fn object(v: Value) -> Map<String, Value> {
    match v {
        Value::Object(m) => m,
        _ => unreachable!("json! of an object literal"),
    }
}

/// A tool call's arguments.
#[derive(Clone, Copy)]
struct Args<'a>(&'a Map<String, Value>);

impl Args<'_> {
    /// Fails on an argument not in `allowed`, so a misspelt one isn't silently ignored.
    fn only(&self, allowed: &[&str]) -> Result<(), Failure> {
        match self.0.keys().find(|k| !allowed.contains(&k.as_str())) {
            Some(k) => Err(Failure::usage(&format!(
                "unknown argument {k} (this tool takes {})",
                allowed.join(", ")
            ))),
            None => Ok(()),
        }
    }

    fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name).filter(|v| !v.is_null())
    }

    fn str(&self, name: &str) -> Result<&str, Failure> {
        self.opt_str(name)?
            .ok_or_else(|| Failure::usage(&format!("{name} is required")))
    }

    fn opt_str(&self, name: &str) -> Result<Option<&str>, Failure> {
        self.get(name)
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| Failure::usage(&format!("{name}: a string")))
            })
            .transpose()
    }

    fn bytes(&self, name: &str) -> Result<Vec<u8>, Failure> {
        self.opt_bytes(name)?
            .ok_or_else(|| Failure::usage(&format!("{name} is required")))
    }

    fn opt_bytes(&self, name: &str) -> Result<Option<Vec<u8>>, Failure> {
        self.get(name)
            .map(|v| bytes_from_json(v).map_err(|m| Failure::usage(&format!("{name}: {m}"))))
            .transpose()
    }

    fn list(&self, name: &str) -> Result<&[Value], Failure> {
        match self.get(name) {
            None => Ok(&[]),
            Some(Value::Array(a)) if a.len() <= MAX_ITEMS => Ok(a),
            Some(Value::Array(_)) => Err(Failure::usage(&format!(
                "{name}: at most {MAX_ITEMS} items"
            ))),
            Some(_) => Err(Failure::usage(&format!("{name}: a list"))),
        }
    }

    fn str_list(&self, name: &str) -> Result<Vec<String>, Failure> {
        self.list(name)?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| Failure::usage(&format!("{name}: a list of strings")))
            })
            .collect()
    }

    fn bytes_list(&self, name: &str) -> Result<Vec<Vec<u8>>, Failure> {
        self.list(name)?
            .iter()
            .map(|v| bytes_from_json(v).map_err(|m| Failure::usage(&format!("{name}: {m}"))))
            .collect()
    }

    fn opt_u64(&self, name: &str, min: u64, max: u64) -> Result<Option<u64>, Failure> {
        self.get(name)
            .map(|v| {
                v.as_u64()
                    .filter(|n| (min..=max).contains(n))
                    .ok_or_else(|| {
                        Failure::usage(&format!("{name}: a whole number from {min} to {max}"))
                    })
            })
            .transpose()
    }

    fn u64_or(&self, name: &str, default: u64, min: u64, max: u64) -> Result<u64, Failure> {
        Ok(self.opt_u64(name, min, max)?.unwrap_or(default))
    }

    fn bool_or(&self, name: &str, default: bool) -> Result<bool, Failure> {
        self.get(name).map_or(Ok(default), |v| {
            v.as_bool()
                .ok_or_else(|| Failure::usage(&format!("{name}: true or false")))
        })
    }
}

// The tool definitions: name, description, input schema and hints (MCP `Tool`).

/// Bytes: a UTF-8 string, or `{"base64": …}`.
fn bytes_schema(description: &str) -> Value {
    json!({
        "description": description,
        "anyOf": [
            {"type": "string"},
            {
                "type": "object",
                "properties": {"base64": {"type": "string"}},
                "required": ["base64"],
                "additionalProperties": false,
            },
        ],
    })
}

fn tool(name: &str, title: &str, description: &str, schema: Value, read_only: bool) -> Value {
    json!({
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": schema,
        "annotations": {
            "title": title,
            "readOnlyHint": read_only,
            "destructiveHint": !read_only && name == "write",
            "idempotentHint": read_only || name == "create_table",
            "openWorldHint": false,
        },
    })
}

fn selection_properties() -> Map<String, Value> {
    object(json!({
        "families": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Only these families (default: all).",
        },
        "qualifier": bytes_schema("Only the column with this qualifier."),
        "qualifier_prefix": bytes_schema("Only columns whose qualifier starts with this."),
        "versions": {
            "type": "integer", "minimum": 1, "maximum": MAX_VERSIONS,
            "description": "Versions per column, newest first (default 1).",
        },
        "max_value_bytes": {
            "type": "integer", "minimum": 0,
            "description": "Byte values longer than this come back as {\"omitted_bytes\": n} \
                            (default: the server's limit). Raise it to read a long value.",
        },
        "cursor": {
            "type": "string",
            "description": "The cursor a previous call returned, to continue it. Pass the \
                            same other arguments.",
        },
    }))
}

fn list_tables() -> Value {
    tool(
        "list_tables",
        "List tables",
        "List the database's tables and each table's column families with their options \
         (aggregation kind, max_versions, ttl_micros, ...). Call this first: every other tool \
         needs a table name, and reads and writes name families. Also says whether the \
         database is writable.",
        json!({
            "type": "object",
            "properties": {
                "table": {"type": "string", "description": "Only this table."},
            },
            "additionalProperties": false,
        }),
        true,
    )
}

fn read_row() -> Value {
    let mut props = selection_properties();
    props.insert("table".into(), json!({"type": "string"}));
    props.insert("row".into(), bytes_schema("The row key."));
    tool(
        "read_row",
        "Read a row",
        "Read one row's cells: each has family, qualifier, ts (microseconds) and value. An \
         empty `cells` means no such row (or nothing matching the selection). A wide row \
         comes back in parts: when the result carries a `cursor`, call again with it for the \
         next columns. To read one cell, pass its family and qualifier.",
        json!({
            "type": "object",
            "properties": props,
            "required": ["table", "row"],
            "additionalProperties": false,
        }),
        true,
    )
}

fn scan() -> Value {
    let mut props = selection_properties();
    props.insert("table".into(), json!({"type": "string"}));
    props.insert(
        "prefix".into(),
        bytes_schema("Only rows whose key starts with this."),
    );
    props.insert(
        "start".into(),
        bytes_schema("The first row key (inclusive)."),
    );
    props.insert(
        "end".into(),
        bytes_schema("The key to stop before (exclusive)."),
    );
    props.insert(
        "limit".into(),
        json!({"type": "integer", "minimum": 1, "maximum": MAX_ROWS,
               "description": "At most this many rows (default 100)."}),
    );
    props.insert(
        "columns_per_row".into(),
        json!({"type": "integer", "minimum": 1, "maximum": MAX_ROWS,
               "description": "At most this many columns of each row (default 100); a row \
                               cut by it is marked columns_capped."}),
    );
    tool(
        "scan",
        "Scan rows",
        "Read rows in key order: those starting with `prefix`, or from `start` up to `end`, \
         or all. Returns `rows`, each with its key and cells. When there are more rows (past \
         `limit` or the result size), the result carries a `cursor`: call again with it to \
         continue. A row marked `truncated` was too big to return whole: read it with \
         read_row.",
        json!({
            "type": "object",
            "properties": props,
            "required": ["table"],
            "additionalProperties": false,
        }),
        true,
    )
}

fn traverse() -> Value {
    tool(
        "traverse",
        "Traverse a graph",
        "Breadth-first search over a graph stored as adjacency rows: a row per node, and in \
         an edge family one column per edge whose qualifier is the destination's row key. \
         From `seeds`, each hop follows the edges of the previous hop's nodes and keeps \
         nodes not reached before. Returns each hop's new nodes in key order (with \
         `parents`, each with the node that reached it, for shortest paths). Cap hubs with \
         per_node_limit and wide hops with hop_limit. A long result carries a `cursor`.",
        json!({
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "family": {"type": "string", "description": "The edge family."},
                "seeds": {
                    "type": "array", "minItems": 1, "maxItems": MAX_ITEMS,
                    "items": bytes_schema("A start node's row key."),
                },
                "hops": {"type": "integer", "minimum": 1, "maximum": 64,
                         "description": "Hops to take (default 1)."},
                "per_node_limit": {"type": "integer", "minimum": 1,
                                   "description": "Follow at most each node's first n edges."},
                "hop_limit": {"type": "integer", "minimum": 1,
                              "description": "At most n new nodes per hop."},
                "max_nodes": {"type": "integer", "minimum": 1, "maximum": MAX_NODES,
                              "description": "Stop after visiting this many nodes in all, \
                                              seeds included (default 100000)."},
                "parents": {"type": "boolean",
                            "description": "Return each node's parent (default false)."},
                "qualifier_prefix": bytes_schema("Only follow edges whose destination key \
                                                  starts with this."),
                "cursor": {"type": "string",
                           "description": "The cursor a previous call returned. Pass the \
                                           same other arguments."},
            },
            "required": ["table", "family", "seeds"],
            "additionalProperties": false,
        }),
        true,
    )
}

fn write() -> Value {
    tool(
        "write",
        "Write cells",
        "Apply operations in one atomic batch: all of them commit, or none (across rows and \
         tables). Each op names `op`, `table` and `row`:\n\
         - put: set family:qualifier to `value` (a string, {\"base64\"}, {\"i64\": n} or \
         {\"f64\": x}); a new version unless `ts` repeats one.\n\
         - incr: add an integer `value` to a counter family's cell.\n\
         - aggregate: add `value` to an aggregate family's cell (an integer, a number, \
         {\"u64\": n} for bitwise families, or bytes for hll and bloom).\n\
         - delete: the whole row; with family, that family; with qualifier too, the column; \
         with ts too, one version.\n\
         `ts` is microseconds since the Unix epoch, default now. Families must exist \
         (create_table adds them).",
        json!({
            "type": "object",
            "properties": {
                "ops": {
                    "type": "array", "minItems": 1, "maxItems": MAX_ITEMS,
                    "items": {
                        "type": "object",
                        "properties": {
                            "op": {"enum": ["put", "incr", "aggregate", "delete"]},
                            "table": {"type": "string"},
                            "row": bytes_schema("The row key."),
                            "family": {"type": "string"},
                            "qualifier": bytes_schema("The column within the family."),
                            "value": {"description": "The value: see the tool's description."},
                            "ts": {"type": "integer", "minimum": 0},
                        },
                        "required": ["op", "table", "row"],
                        "additionalProperties": false,
                    },
                },
            },
            "required": ["ops"],
            "additionalProperties": false,
        }),
        false,
    )
}

fn create_table() -> Value {
    tool(
        "create_table",
        "Create a table",
        "Create a table with its column families, if it does not exist (an existing table \
         and its families are left as they are). A family is a name, or an object with a \
         name and options as list_tables shows them: \"aggregation\" (sum for counters, min, \
         max, hll, ...), \"max_versions\", \"ttl_micros\". Plain families hold any bytes or \
         numbers; group data read together in one family.",
        json!({
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "families": {
                    "type": "array", "minItems": 1,
                    "items": {"anyOf": [
                        {"type": "string"},
                        {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "aggregation": {"type": "string"},
                                "max_versions": {"type": "integer", "minimum": 1},
                                "ttl_micros": {"type": "integer", "minimum": 1},
                            },
                            "required": ["name"],
                        },
                    ]},
                },
            },
            "required": ["table", "families"],
            "additionalProperties": false,
        }),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successor_bounds_a_prefix() {
        assert_eq!(successor(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(successor(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(successor(b"\xff\xff"), None);
        assert_eq!(successor(b""), None);
    }

    #[test]
    fn a_budget_always_takes_the_first_item() {
        let mut b = Budget::new(4);
        let big = [json!("a long string, past the limit")];
        assert!(b.take(&big));
        assert!(!b.take(&[json!(1)]));
    }

    #[test]
    fn every_tool_has_an_object_schema_and_a_description() {
        for t in [
            list_tables(),
            read_row(),
            scan(),
            traverse(),
            write(),
            create_table(),
        ] {
            assert_eq!(t["inputSchema"]["type"], "object", "{}", t["name"]);
            assert!(
                t["description"].as_str().unwrap().len() > 80,
                "{}",
                t["name"]
            );
            for r in t["inputSchema"]["required"]
                .as_array()
                .into_iter()
                .flatten()
            {
                let r = r.as_str().unwrap();
                assert!(
                    t["inputSchema"]["properties"].get(r).is_some(),
                    "{}: {r}",
                    t["name"]
                );
            }
        }
    }
}
