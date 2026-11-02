//! The tools `phdb mcp` serves: a few composable ones over the public API, each with a JSON
//! schema and a description written for an agent.
//!
//! | Tool | What it does |
//! |---|---|
//! | `list_tables` | Tables and their families' options. |
//! | `read_row` | One row's cells, paged at column boundaries. |
//! | `scan` | Rows in key order: a prefix or a range, with a row limit. |
//! | `traverse` | Breadth-first hops over an edge family (`Table::traverse`). |
//! | `nearest` | The `k` nearest vectors to a query in a vector family (`Scan::nearest_f32`). |
//! | `write` | Puts, increments, aggregate inputs and deletes in one atomic batch. |
//! | `create_table` | A table and its families, if missing. |
//! | `feed_poll` | Whole committed changes after a token (`Table::feed`), with the next token. |
//!
//! The JSON shapes of bytes and values are `phdb dump`'s. A result that would pass
//! [`Limits::result_bytes`] stops at a column, row or node boundary with a `cursor`; a
//! value longer than its limit comes back as `{"omitted_bytes": n}`. `nearest` is listed
//! only when the database has a vector family; its result is cut at a neighbour, with
//! `truncated`, since a ranking can't resume.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use pigeonhole::{
    Aggregation, CellEntry, Element, KeySet, RowPosition, Table, Value as Cell, VectorSpec,
};
use serde_json::{Map, Value, json};

use super::Limits;
use super::cursor::Cursor;
use crate::cmd::dump::value_json;
use crate::cmd::schema::{
    Components, JsonValue, base64_decode, base64_encode, bytes_from_json, bytes_json, element_name,
    family_from_json, family_json, value_from_json, vector_spec_json,
};
use crate::exit::Failure;
use crate::open::{Db, Handle};

/// The aggregation kinds `create_table` takes (`family_from_json`'s names).
const AGGREGATIONS: &[&str] = &[
    "sum",
    "min",
    "max",
    "hll",
    "bit_or",
    "bit_and",
    "bit_xor",
    "min_f64",
    "max_f64",
    "sum_f64",
    "mean_var",
    "histogram",
    "quantiles",
    "bloom",
];

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
/// The most neighbours a `nearest` may ask for.
const MAX_K: u64 = 1_000;
/// The most commits one `feed_poll` returns, and the longest it waits (milliseconds).
const MAX_COMMITS: u64 = 1_000;
const MAX_WAIT_MS: u64 = 30_000;
/// The largest traversal result the server keeps for its cursors (#1254), about: counted in
/// node-key bytes, plus 4 a node for parents. Past it, a cursor runs the traversal again.
const MAX_CACHED_TRAVERSAL_BYTES: usize = 64 << 20;

type Outcome = Result<Map<String, Value>, Failure>;

pub(super) struct Tools {
    db: Db,
    writable: bool,
    limits: Limits,
    /// The last cut traversal (#1254): its cursors page from it, in O(page), all from one
    /// consistent result. One at a time: the next traversal that returns a cursor replaces
    /// it (one that fits a single result leaves it alone).
    traversal: Mutex<Option<CachedTraversal>>,
    /// The id the next traversal run gets: its cursors carry it. Seeded from the clock, so a
    /// cursor kept across a server restart misses the new server's runs (ai1249 on #1365).
    runs: AtomicU64,
}

/// A traversal result kept for its cursors.
struct CachedTraversal {
    /// The call's fingerprint and run id, which its cursors carry.
    fingerprint: u64,
    run: u64,
    traversal: pigeonhole::Traversal,
}

impl Tools {
    pub(super) fn new(db: Db, writable: bool, limits: Limits) -> Self {
        Self {
            db,
            writable,
            limits,
            traversal: Mutex::new(None),
            runs: AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(1, |d| d.as_nanos() as u64),
            ),
        }
    }

    pub(super) fn close(self) -> Result<(), Failure> {
        self.db.close()
    }

    /// `tools/list`: `nearest` only when the database has a vector family, and the write
    /// tools only when it is writable. (`nearest` answers a call either way, for a vector
    /// family created after the list.)
    pub(super) fn list(&self) -> Vec<Value> {
        let mut tools = vec![list_tables(), read_row(), scan(), traverse()];
        if self.has_vector_family() {
            tools.push(nearest());
        }
        // The feed is on when the writer's open inherited one or `--change-feed` turned it on.
        if self.db.info().is_ok_and(|i| i.feed.is_some()) {
            tools.push(feed_poll());
        }
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
            "feed_poll" => self.feed_poll(a),
            "nearest" => self.nearest(a),
            "write" => self.write(a),
            "create_table" => self.create_table(a),
            _ => return None,
        })
    }

    /// Whether any table has a vector family.
    fn has_vector_family(&self) -> bool {
        self.db.tables().iter().any(|t| {
            self.db
                .families(t)
                .is_ok_and(|fs| fs.iter().any(|f| f.vector.is_some()))
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
        let (mut lo, hi) = key_range(&a, "scan")?;
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
        let (from_hop, from_index, run) = match Cursor::decode(a.0)
            .map_err(|m| Failure::usage(&m))?
        {
            None => (0, 0, None),
            Some(Cursor::Traverse { hop, index, run }) => (hop as usize, index as usize, Some(run)),
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
        // A cursor of the cached run pages from it (#1254). Otherwise (no cursor, or its run
        // is no longer cached) the traversal runs: it is deterministic, so over unchanged
        // data the pages of a re-run join up exactly.
        let fingerprint = super::cursor::fingerprint(a.0);
        let cached = run.and_then(|r| {
            let mut slot = self.traversal.lock().expect("not poisoned");
            match slot.take() {
                Some(c) if c.fingerprint == fingerprint && c.run == r => Some(c),
                other => {
                    *slot = other;
                    None
                }
            }
        });
        let (traversal, run) = match cached {
            Some(c) => (c.traversal, c.run),
            None => {
                let t = match self.db.handle(table)? {
                    Handle::Writer(t) => go!(t),
                    Handle::Reader(t) => go!(t),
                };
                (t, self.runs.fetch_add(1, Ordering::Relaxed))
            }
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
                        run,
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
            // Kept for the cursor's next page, unless too large (then it runs again).
            let bytes: usize = traversal
                .hops()
                .iter()
                .map(|h| {
                    let parents = if h.parents().is_some() { 4 } else { 0 };
                    (0..h.nodes().len())
                        .map(|i| h.nodes().get(i).len() + parents)
                        .sum::<usize>()
                })
                .sum();
            if bytes <= MAX_CACHED_TRAVERSAL_BYTES {
                *self.traversal.lock().expect("not poisoned") = Some(CachedTraversal {
                    fingerprint,
                    run,
                    traversal,
                });
            }
        }
        Ok(out)
    }

    fn nearest(&self, a: Args<'_>) -> Outcome {
        a.only(&[
            "table",
            "family",
            "query",
            "k",
            "row",
            "prefix",
            "start",
            "end",
            "qualifier",
            "qualifier_prefix",
        ])?;
        let table = a.str("table")?;
        let family = a.str("family")?;
        let k = a.u64_or("k", 10, 1, MAX_K)? as usize;
        let (lo, hi) = match a.opt_bytes("row")? {
            None => key_range(&a, "nearest")?,
            Some(_)
                if ["prefix", "start", "end"]
                    .iter()
                    .any(|n| a.get(n).is_some()) =>
            {
                return Err(Failure::usage(
                    "nearest takes a row, a prefix or a start/end range, not two of them",
                ));
            }
            Some(row) => (Bound::Included(row.clone()), Bound::Included(row)),
        };
        let qualifier = a.opt_bytes("qualifier")?;
        let qualifier_prefix = a.opt_bytes("qualifier_prefix")?;
        if qualifier.is_some() && qualifier_prefix.is_some() {
            return Err(Failure::usage("qualifier or qualifier_prefix, not both"));
        }
        let query = a
            .get("query")
            .ok_or_else(|| Failure::usage("query is required"))?;
        let read = |e: pigeonhole::Error| Failure::db(&format!("nearest {table}"), &e);
        macro_rules! go {
            ($t:expr) => {{
                let spec = $t
                    .family_info(family)
                    .ok_or_else(|| Failure::family_not_found(table, family))?
                    .vector
                    .ok_or_else(|| {
                        Failure::usage(&format!(
                            "family: {family} is not a vector family (list_tables shows each \
                             family's vector spec)"
                        ))
                    })?;
                let query = Query::parse(query, &spec)?;
                let mut s = $t
                    .scan_bounds(
                        lo.as_ref().map(Vec::as_slice),
                        hi.as_ref().map(Vec::as_slice),
                    )
                    .family(family);
                if let Some(q) = &qualifier_prefix {
                    s = s.qualifier_prefix(q);
                }
                if let Some(q) = &qualifier {
                    s = s.qualifier_bounds(Bound::Included(q), Bound::Included(q));
                }
                let n = match &query {
                    Query::F32(q) => s.nearest_f32(q, k),
                    Query::I8(q) => s.nearest_i8(q, k),
                    Query::Bits(q) => s.nearest_bits(q, k),
                };
                (spec, n.run().map_err(read)?)
            }};
        }
        let (spec, found) = match self.db.handle(table)? {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        };
        let mut budget = Budget::new(self.limits.result_bytes);
        let mut neighbours = Vec::new();
        let mut truncated = false;
        for n in &found {
            let v = json!({
                "row": bytes_json(&n.row),
                "qualifier": bytes_json(&n.qualifier),
                "ts": n.ts,
                "distance": n.distance,
            });
            if !budget.take(std::slice::from_ref(&v)) {
                truncated = true;
                break;
            }
            neighbours.push(v);
        }
        let mut out = object(json!({
            "table": table,
            "family": family,
            "vector": vector_spec_json(&spec),
            "neighbours": neighbours,
        }));
        if truncated {
            // A ranking can't resume where it stopped: a smaller k, or a narrower selection.
            out.insert("truncated".into(), true.into());
        }
        Ok(out)
    }

    fn feed_poll(&self, a: Args<'_>) -> Outcome {
        a.only(&[
            "tables",
            "prefix",
            "token",
            "from_oldest",
            "visible",
            "max_commits",
            "wait_ms",
            "max_value_bytes",
        ])?;
        let names = a.str_list("tables")?;
        if names.is_empty() {
            return Err(Failure::usage("tables: at least one table name"));
        }
        let prefix = a.opt_bytes("prefix")?;
        let token = a
            .opt_str("token")?
            .map(|t| {
                base64_decode(t).ok_or_else(|| {
                    Failure::usage("token: the token string a previous feed_poll returned")
                })
            })
            .transpose()?;
        let from_oldest = a.bool_or("from_oldest", false)?;
        if token.is_some() && from_oldest {
            return Err(Failure::usage("token or from_oldest, not both"));
        }
        let visible = a.bool_or("visible", false)?;
        let max_commits = a.u64_or("max_commits", 100, 1, MAX_COMMITS)? as usize;
        let wait = std::time::Duration::from_millis(a.u64_or("wait_ms", 0, 0, MAX_WAIT_MS)?);
        let max_value = self.max_value(&a)?;
        let Db::Writer(db) = &self.db else {
            return Err(Failure::unsupported(
                "feed_poll: a reader process can't follow the change feed yet; serve the \
                 database as its writer (stop the application, or wait for D310 PR 4)"
                    .into(),
            ));
        };
        let fail = |e: pigeonhole::Error| {
            let f = Failure::db("feed_poll", &e);
            match e.code() {
                pigeonhole::ErrorCode::FeedExpired => f.hint(
                    "the history this token needs is gone: re-read the tables (scan) and \
                     call feed_poll again without a token",
                ),
                pigeonhole::ErrorCode::InvalidArgument => f.hint(
                    "the database's change feed must be on (the application's, or \
                     --change-feed) and follow these tables, and the token must come from this \
                     database",
                ),
                _ => f,
            }
        };
        let mut tables = Vec::with_capacity(names.len());
        for n in &names {
            tables.push(
                db.table(n)
                    .and_then(|b| b.open())
                    .map_err(|e| Failure::db(&format!("table {n}"), &e))?,
            );
        }
        let mut b = tables[0].feed();
        for t in &tables[1..] {
            b = b.table(t);
        }
        if let Some(p) = &prefix {
            b = b.prefix(p);
        }
        if visible {
            b = b.visible();
        }
        if from_oldest {
            b = b.from_oldest();
        }
        if let Some(t) = token {
            b = b.resume(&pigeonhole::FeedToken::from_bytes(t));
        }
        let mut feed = b.open().map_err(fail)?;
        let mut budget = Budget::new(self.limits.result_bytes);
        let start = feed.token();
        let (commits, token, more) = gather(max_commits, &mut budget, start, |first| {
            // Wait for the first commit only; then take what is ready.
            let next = if first {
                feed.next(wait)
            } else {
                feed.try_next()
            };
            let v = match next.map_err(fail)? {
                Some(c) => commit_json(c, max_value),
                None => return Ok(None),
            };
            Ok(Some((v, feed.token())))
        })?;
        let mut out = object(json!({
            "commits": commits,
            "token": base64_encode(token.as_bytes()),
        }));
        if more {
            out.insert("more".into(), true.into());
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
        let info = batch.commit().map_err(|e| {
            let f = Failure::db("write", &e);
            // The library's message may name its Rust methods; say what to check here.
            if e.code() == pigeonhole::ErrorCode::InvalidArgument {
                f.hint("check each family's kind and options with list_tables")
            } else {
                f
            }
        })?;
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
                (
                    JsonValue::Vector {
                        element,
                        dim,
                        components,
                    },
                    ts,
                ) => {
                    let spec = t
                        .family_info(f)
                        .ok_or_else(|| Failure::family_not_found(t.name(), f))?
                        .vector
                        .ok_or_else(|| usage(format!("put: {f} is not a vector family")))?;
                    if spec.element() != element || spec.dim() != dim {
                        return Err(usage(format!(
                            "put: a {}[{dim}] vector, and {f}'s vectors are {}[{}]",
                            element_name(element),
                            element_name(spec.element()),
                            spec.dim()
                        )));
                    }
                    match (components, ts) {
                        (Components::Floats(xs), None) => batch.put_vector_f32(t, &row, f, q, &xs),
                        (Components::Floats(xs), Some(ts)) => {
                            batch.put_vector_f32_at(t, &row, f, q, ts, &xs)
                        }
                        (Components::Ints(xs), None) => batch.put_vector_i8(t, &row, f, q, &xs),
                        (Components::Ints(xs), Some(ts)) => {
                            batch.put_vector_i8_at(t, &row, f, q, ts, &xs)
                        }
                        (Components::Bits(b), None) => batch.put_vector_bits(t, &row, f, q, &b),
                        (Components::Bits(b), Some(ts)) => {
                            batch.put_vector_bits_at(t, &row, f, q, ts, &b)
                        }
                    }
                }
            };
        }
        "incr" => {
            op.only(&["op", "table", "row", "family", "qualifier", "value", "ts"])?;
            let (f, q) = column()?;
            if !is_counter(t, f)? {
                return Err(usage(format!(
                    "incr adds to a counter (sum) family, and {f} is not one: use put for a \
                     plain family, aggregate for another aggregate family (list_tables shows \
                     each family's aggregation)"
                )));
            }
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
            let Some(agg) = family_kind(t, f)? else {
                return Err(usage(format!(
                    "aggregate writes to an aggregate family, and {f} is a plain family: use put"
                )));
            };
            match (aggregate_input(&agg, value()?)?, ts) {
                (Input::I64(n), None) => batch.aggregate(t, &row, f, q, n),
                (Input::I64(n), Some(ts)) => batch.aggregate_at(t, &row, f, q, ts, n),
                (Input::U64(n), None) => batch.aggregate_u64(t, &row, f, q, n),
                (Input::U64(n), Some(ts)) => batch.aggregate_u64_at(t, &row, f, q, ts, n),
                (Input::F64(x), None) => batch.aggregate_f64(t, &row, f, q, x),
                (Input::F64(x), Some(ts)) => batch.aggregate_f64_at(t, &row, f, q, ts, x),
                (Input::Bytes(b), None) => batch.aggregate_bytes(t, &row, f, q, &b),
                (Input::Bytes(b), Some(ts)) => batch.aggregate_bytes_at(t, &row, f, q, ts, &b),
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

/// `family`'s aggregation in `t`: `None` for a plain family. A family `t` doesn't have
/// fails with `FamilyNotFound`, as the commit would.
fn family_kind(t: &Table, family: &str) -> Result<Option<Aggregation>, Failure> {
    match t.family_info(family) {
        Some(info) => Ok(info.aggregation),
        None => Err(Failure::family_not_found(t.name(), family)),
    }
}

/// Whether `incr` adds to `family`: a counter (`sum`) family, or a standard family that
/// names the built-in `i64` add operator, as 0.1.0 made counters (D41).
fn is_counter(t: &Table, family: &str) -> Result<bool, Failure> {
    let info = t
        .family_info(family)
        .ok_or_else(|| Failure::family_not_found(t.name(), family))?;
    Ok(matches!(info.aggregation, Some(Aggregation::Sum))
        || info.merge_operator.as_deref() == Some(I64_ADD))
}

/// D41's counter operator.
const I64_ADD: &str = "pigeonhole.i64_add";

/// An aggregate family's input.
enum Input {
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
}

/// `v` as an input to a family of kind `agg`, by the family's kind rather than the JSON
/// number's shape: an agent writes `3` to an f64 family as often as `3.0`.
fn aggregate_input(agg: &Aggregation, v: &Value) -> Result<Input, Failure> {
    let bad = |want: &str| Failure::usage(&format!("aggregate: this family takes {want}"));
    // `{"i64": n}`, `{"f64": x}` and `{"u64": n}` stand for the plain numbers.
    let number = v
        .get("i64")
        .or_else(|| v.get("f64"))
        .or_else(|| v.get("u64"))
        .unwrap_or(v);
    Ok(match agg {
        Aggregation::MinF64
        | Aggregation::MaxF64
        | Aggregation::SumF64
        | Aggregation::MeanVar
        | Aggregation::Histogram(_)
        | Aggregation::Quantiles(_) => Input::F64(number.as_f64().ok_or_else(|| bad("a number"))?),
        Aggregation::BitOr | Aggregation::BitAnd | Aggregation::BitXor => Input::U64(
            number
                .as_u64()
                .ok_or_else(|| bad("a non-negative integer (a 64-bit mask)"))?,
        ),
        // An element: an integer, or bytes.
        Aggregation::Hll | Aggregation::Bloom(_) => match number.as_i64() {
            Some(n) if !v.is_string() => Input::I64(n),
            _ => Input::Bytes(
                bytes_from_json(v).map_err(|_| bad("an integer, a string or {\"base64\"}"))?,
            ),
        },
        _ => Input::I64(number.as_i64().ok_or_else(|| bad("an integer"))?),
    })
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
    let value = typed_json(e.cell.typed(), max_value);
    json!({
        "family": e.family,
        "qualifier": bytes_json(e.qualifier),
        "ts": e.cell.timestamp(),
        "value": value,
    })
}

/// `feed_poll`'s loop: up to `max` commits from `pull` (`first` on the first call; each with
/// the token after it) that fit `budget`, the token after the last one kept, and whether
/// more are ready.
///
/// The token never passes a commit left out: one the budget refuses is delivered again from
/// it. An error after some commits ends the batch there, and they come back with their token
/// (feed1248 on #1360): they were delivered in order before it, and the next call, from that
/// token, meets the error again and reports it. An error before any is the call's error. An
/// error from the probe for `more` after `max` commits leaves `more` unset.
fn gather<T, E>(
    max: usize,
    budget: &mut Budget,
    mut token: T,
    mut pull: impl FnMut(bool) -> Result<Option<(Value, T)>, E>,
) -> Result<(Vec<Value>, T, bool), E> {
    let mut commits = Vec::new();
    while commits.len() < max {
        let (v, after) = match pull(commits.is_empty()) {
            Ok(Some(c)) => c,
            Ok(None) => return Ok((commits, token, false)),
            Err(e) if commits.is_empty() => return Err(e),
            Err(_) => return Ok((commits, token, false)),
        };
        if !budget.take(std::slice::from_ref(&v)) {
            return Ok((commits, token, true));
        }
        commits.push(v);
        token = after;
    }
    let more = matches!(pull(false), Ok(Some(_)));
    Ok((commits, token, more))
}

/// A change-feed commit in a `feed_poll` result: its changes in `dump`'s shapes, a value
/// longer than `max_value` replaced as reads replace it.
fn commit_json(c: &pigeonhole::Commit, max_value: usize) -> Value {
    let changes: Vec<Value> = c
        .changes()
        .iter()
        .map(|ch| {
            let mut m = object(json!({
                "table": ch.table(),
                "family": ch.family(),
                "kind": change_kind(ch.kind()),
                "row": bytes_json(ch.row()),
                "qualifier": bytes_json(ch.qualifier()),
                "ts": ch.timestamp(),
            }));
            if let Some(v) = ch.value() {
                m.insert("value".into(), typed_json(v, max_value));
            }
            Value::Object(m)
        })
        .collect();
    json!({"seqno": c.seqno(), "ts": c.timestamp(), "changes": changes})
}

fn change_kind(kind: pigeonhole::ChangeKind) -> &'static str {
    use pigeonhole::ChangeKind as K;
    match kind {
        K::Put => "put",
        K::Merge => "merge",
        K::DeleteCell => "delete_cell",
        K::DeleteColumn => "delete_column",
        K::DeleteFamily => "delete_family",
        _ => "unknown",
    }
}

/// A value in a result: `dump`'s shape, with a byte value longer than `max_value` replaced
/// by its length, and a vector whose payload is, by its shape.
fn typed_json(v: Cell<'_>, max_value: usize) -> Value {
    match v {
        Cell::Bytes(b) if b.len() > max_value => json!({ "omitted_bytes": b.len() }),
        Cell::Vector(v) if v.as_bytes().len() > max_value => json!({
            "omitted_vector": {"element": element_name(v.element()), "dim": v.dim()},
        }),
        v => value_json(v),
    }
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

/// A row key range: its lower and upper bounds.
type KeyRange = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// The row keys `prefix`, or `start` and `end`, select (all of them with none).
fn key_range(a: &Args<'_>, tool: &str) -> Result<KeyRange, Failure> {
    let prefix = a.opt_bytes("prefix")?;
    let (start, end) = (a.opt_bytes("start")?, a.opt_bytes("end")?);
    if prefix.is_some() && (start.is_some() || end.is_some()) {
        return Err(Failure::usage(&format!(
            "{tool} takes a prefix or a start/end range, not both"
        )));
    }
    Ok(match prefix {
        Some(p) => {
            let hi = successor(&p).map_or(Bound::Unbounded, Bound::Excluded);
            (Bound::Included(p), hi)
        }
        None => (
            start.map_or(Bound::Unbounded, Bound::Included),
            end.map_or(Bound::Unbounded, Bound::Excluded),
        ),
    })
}

/// A `nearest` query, in its family's element type.
enum Query {
    F32(Vec<f32>),
    I8(Vec<i8>),
    Bits(Vec<u8>),
}

impl Query {
    /// `v` as a query for a family of `spec`: a list of numbers (`i8` families take
    /// integers, bit families 0s and 1s), `{"base64"}` of a bit family's packed bytes, or
    /// `write`'s `{"vector"}` shape. Its element type and dimension must be the family's.
    fn parse(v: &Value, spec: &VectorSpec) -> Result<Self, Failure> {
        let q = Self::parse_shape(v, spec)?;
        // What the search would refuse, refused here as the agent's mistake (ai1249 on
        // #1354): a component past 2^56, and under cosine a vector with no direction.
        let usage = |m: &str| Failure::usage(&format!("query: {m}"));
        if let Query::F32(xs) = &q
            && xs.iter().any(|x| x.abs() > (1u64 << 56) as f32)
        {
            return Err(usage("a component is more than 2^56 in magnitude"));
        }
        let zero = match &q {
            Query::F32(xs) => xs.iter().all(|x| *x == 0.0),
            Query::I8(xs) => xs.iter().all(|x| *x == 0),
            Query::Bits(_) => false,
        };
        if zero && spec.metric() == pigeonhole::Metric::Cosine {
            return Err(usage(
                "an all-zero vector has no direction, so cosine can't rank against it",
            ));
        }
        Ok(q)
    }

    /// [`parse`](Self::parse)'s shapes, checked against the family's element and dimension.
    fn parse_shape(v: &Value, spec: &VectorSpec) -> Result<Self, Failure> {
        let usage = |m: String| Failure::usage(&format!("query: {m}"));
        let (element, dim) = (spec.element(), spec.dim() as usize);
        let want = format!("{}[{dim}]", element_name(element));
        let len = |n: usize| {
            if n == dim {
                Ok(())
            } else {
                Err(usage(format!(
                    "{n} components, and the family's vectors are {want}"
                )))
            }
        };
        if v.get("vector").is_some() {
            let JsonValue::Vector {
                element: e,
                dim: d,
                components,
            } = value_from_json(v).map_err(usage)?
            else {
                return Err(usage("not a vector".into()));
            };
            if e != element || d as usize != dim {
                return Err(usage(format!(
                    "a {}[{d}] vector, and the family's vectors are {want}",
                    element_name(e)
                )));
            }
            return Ok(match components {
                Components::Floats(xs) => Query::F32(xs),
                Components::Ints(xs) => Query::I8(xs),
                Components::Bits(b) => Query::Bits(b),
            });
        }
        if let Some(Value::String(_)) = v.get("base64") {
            if element != Element::Bit {
                return Err(usage(format!(
                    "{{\"base64\"}} is a bit family's query, and the family's vectors are {want}"
                )));
            }
            let b = bytes_from_json(v).map_err(usage)?;
            if b.len() != dim.div_ceil(8) {
                return Err(usage(format!(
                    "{} bytes, and a bit[{dim}] query packs into {}",
                    b.len(),
                    dim.div_ceil(8)
                )));
            }
            return Ok(Query::Bits(b));
        }
        let Value::Array(xs) = v else {
            return Err(usage(format!(
                "a list of {dim} numbers (or write's {{\"vector\"}} shape)"
            )));
        };
        len(xs.len())?;
        Ok(match element {
            Element::I8 => Query::I8(
                xs.iter()
                    .map(|x| {
                        x.as_i64()
                            .and_then(|n| i8::try_from(n).ok())
                            .ok_or_else(|| usage(format!("{x} is not an integer from -128 to 127")))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Element::Bit => {
                // Packed eight to a byte, least significant first (Scan::nearest_bits).
                let mut packed = vec![0u8; dim.div_ceil(8)];
                for (i, x) in xs.iter().enumerate() {
                    match x.as_u64() {
                        Some(0) => {}
                        Some(1) => packed[i / 8] |= 1 << (i % 8),
                        _ => return Err(usage(format!("{x} is not a bit (0 or 1)"))),
                    }
                }
                Query::Bits(packed)
            }
            // f32, and the halves: an f32 query, which the search compares in f32.
            _ => Query::F32(
                xs.iter()
                    .map(|x| {
                        x.as_f64()
                            .map(|f| f as f32)
                            .filter(|f| f.is_finite())
                            .ok_or_else(|| usage(format!("{x} is not a finite f32")))
                    })
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
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

fn nearest() -> Value {
    tool(
        "nearest",
        "Find nearest vectors",
        "Find the `k` vectors nearest to `query` in a vector family (list_tables shows each \
         family's vector spec: dim, element and metric): an exact search, nearest first, ties \
         by row then qualifier. Returns `neighbours`, each with its row, qualifier, ts and \
         distance under the family's metric (smaller is nearer); read a neighbour's cells \
         with read_row. The query is a list of `dim` numbers: integers from -128 to 127 for \
         an i8 family, 0s and 1s for a bit family (or {\"base64\"} of its packed bytes). \
         The query must be an embedding from the same model as the stored vectors: \
         distances between two models' vectors mean nothing. \
         Narrow the search to a `row`, a `prefix` or a `start`/`end` range, and to one \
         `qualifier` or a `qualifier_prefix`. A result marked `truncated` holds the nearest \
         that fit: ask for a smaller k.",
        json!({
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "family": {"type": "string", "description": "The vector family."},
                "query": {
                    "description": "The query vector: a list of dim numbers, or for a bit \
                                    family {\"base64\"} of its packed bytes.",
                    "anyOf": [
                        {"type": "array", "items": {"type": "number"}},
                        {"type": "object"},
                    ],
                },
                "k": {"type": "integer", "minimum": 1, "maximum": MAX_K,
                      "description": "How many neighbours (default 10)."},
                "row": bytes_schema("Only this row's vectors."),
                "prefix": bytes_schema("Only rows whose key starts with this."),
                "start": bytes_schema("The first row key (inclusive)."),
                "end": bytes_schema("The key to stop before (exclusive)."),
                "qualifier": bytes_schema("Only the column with this qualifier."),
                "qualifier_prefix": bytes_schema("Only columns whose qualifier starts with this."),
            },
            "required": ["table", "family", "query"],
            "additionalProperties": false,
        }),
        true,
    )
}

fn feed_poll() -> Value {
    tool(
        "feed_poll",
        "Poll committed changes",
        "Return the changes committed to `tables` since `token`, as whole commits in commit \
         order: each has seqno, ts and `changes` (table, family, kind, row, qualifier, ts and, \
         for put and merge, value; kind is put, merge, delete_cell, delete_column or \
         delete_family). Pass the returned `token` to the next call to continue exactly \
         where this one stopped: nothing is missed or repeated. The first call has no token \
         and starts with commits after it (or from_oldest: the oldest history kept); \
         wait_ms waits for a first commit. `more` says more commits are ready now. An error \
         FeedExpired means the history is gone: re-read with scan and start again without \
         a token. Otherwise keep the token: it survives the application's restarts and this \
         server's reconnects, so there's no need to re-read on every session.",
        json!({
            "type": "object",
            "properties": {
                "tables": {
                    "type": "array", "minItems": 1, "items": {"type": "string"},
                    "description": "The tables to follow; a commit across them arrives whole.",
                },
                "prefix": bytes_schema("Only changes to rows whose key starts with this."),
                "token": {"type": "string",
                          "description": "The token the previous feed_poll returned."},
                "from_oldest": {"type": "boolean",
                                "description": "Without a token: start with the oldest \
                                                history kept, not after now."},
                "visible": {"type": "boolean",
                            "description": "Return commits once reads see them, not once \
                                            durable (default false)."},
                "max_commits": {"type": "integer", "minimum": 1, "maximum": MAX_COMMITS,
                                "description": "At most this many commits (default 100)."},
                "wait_ms": {"type": "integer", "minimum": 0, "maximum": MAX_WAIT_MS,
                            "description": "Wait up to this long for a first commit \
                                            (default 0: return at once)."},
                "max_value_bytes": {
                    "type": "integer", "minimum": 0,
                    "description": "Byte values longer than this come back as \
                                    {\"omitted_bytes\": n} (default: the server's limit).",
                },
            },
            "required": ["tables"],
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
         {\"f64\": x}); a new version unless `ts` repeats one. In a vector family, the value \
         is {\"vector\": {\"dim\": n, \"element\": \"f32\", \"values\": [...]}} with the \
         family's dim and element (list_tables shows them).\n\
         - incr: add an integer `value` to a counter family's cell.\n\
         - aggregate: add `value` to an aggregate family's cell: a number (f64 kinds take \
         any number), a non-negative integer for bit_or/bit_and/bit_xor, an integer or a \
         string for hll and bloom.\n\
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
         name and options as list_tables shows them: \"aggregation\" (one of the kinds the \
         schema lists; sum is a counter), \"aggregation_params\" for histogram, quantiles \
         and bloom, \"max_versions\" (0 keeps every version), \"ttl_micros\", or \"vector\": \
         {\"dim\", \"element\" (f32, i8, bit, f16, bf16), \"metric\" (l2, inner_product, \
         cosine, hamming)} for a vector family. Plain families hold any bytes or \
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
                                "aggregation": {
                                    "enum": AGGREGATIONS,
                                    "description": "An aggregate family's kind: sum is a \
                                                    counter (incr); histogram, quantiles and \
                                                    bloom take aggregation_params.",
                                },
                                "aggregation_params": {
                                    "type": "object",
                                    "description": "histogram: {\"bounds\": [b0, b1, ...]} \
                                                    (ascending); quantiles: \
                                                    {\"mantissa_bits\": p, \"max_buckets\": n} \
                                                    (both optional); bloom: {\"bits\": m, \
                                                    \"hashes\": k} (both required).",
                                },
                                "max_versions": {"type": "integer", "minimum": 1},
                                "ttl_micros": {"type": "integer", "minimum": 1},
                                "vector": {
                                    "type": "object",
                                    "description": "A vector family's shape: every value is \
                                                    a vector of `dim` `element`s, compared \
                                                    by `metric`.",
                                    "properties": {
                                        "dim": {"type": "integer", "minimum": 1, "maximum": 4096},
                                        "element": {"enum": ["f32", "i8", "bit", "f16", "bf16"]},
                                        "metric": {"enum": ["l2", "inner_product", "cosine", "hamming"]},
                                    },
                                    "required": ["dim", "element", "metric"],
                                },
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

    /// feed1248 on #1360: an error after some commits returns them with the token after the
    /// last; before any, it's the call's error; a refused commit keeps the token before it.
    #[test]
    fn feed_poll_returns_what_it_gathered_before_an_error() {
        // A feed of commits 1, 2, then an error; each commit's token is its number.
        let pulls = |script: Vec<Result<Option<u32>, &'static str>>| {
            let mut it = script.into_iter();
            move |_first: bool| {
                it.next()
                    .unwrap_or(Ok(None))
                    .map(|c| c.map(|n| (json!(n), n)))
            }
        };
        let big = || Budget::new(1 << 20);
        let r = gather(
            10,
            &mut big(),
            0,
            pulls(vec![Ok(Some(1)), Ok(Some(2)), Err("expired")]),
        );
        assert_eq!(r, Ok((vec![json!(1), json!(2)], 2, false)));
        let r = gather(10, &mut big(), 0, pulls(vec![Err("expired")]));
        assert_eq!(r, Err("expired"));
        // The probe for `more` after `max`: an error there leaves `more` unset.
        let r = gather(
            2,
            &mut big(),
            0,
            pulls(vec![Ok(Some(1)), Ok(Some(2)), Err("x")]),
        );
        assert_eq!(r, Ok((vec![json!(1), json!(2)], 2, false)));
        let r = gather(
            2,
            &mut big(),
            0,
            pulls(vec![Ok(Some(1)), Ok(Some(2)), Ok(Some(3))]),
        );
        assert_eq!(r, Ok((vec![json!(1), json!(2)], 2, true)));
        // A commit the budget refuses: the token stays before it, and `more` is set.
        let mut small = Budget::new(2);
        let r = gather(10, &mut small, 0, pulls(vec![Ok(Some(1)), Ok(Some(22222))]));
        assert_eq!(r, Ok((vec![json!(1)], 1, true)));
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
            nearest(),
            feed_poll(),
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
