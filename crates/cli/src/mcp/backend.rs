//! What the MCP tools reach the database through (D313 ticket 8c-2, §7 as amended; #1915
//! PR 3): the tools' parsing, paging, cursors, budgets and JSON stay in
//! [`tools`](super::tools), and their database calls are a [`Backend`]'s operations, each
//! taking and giving plain data rather than a table handle.
//!
//! - **`phdb mcp`** (stdio) uses [`Db`], the database it opened: the calls are the ones the
//!   tools always made, on the library's API.
//! - **`phdb serve`'s `/mcp`** (8c-3, #1915 PR 3) makes each call a service op through
//!   `Router::dispatch`, after one `Service::admit` shared with gRPC and HTTP. So a tool
//!   routes, fans out and refuses exactly as those do (its scopes, the reserved `__phdb_`
//!   prefix, the served memory store's tables, a cluster's partitions), and `phdb mcp` keeps
//!   none of the server's dependencies.
//!
//! A resume (`read_row`'s) is the backend's own bytes: the tools carry it in their cursor
//! without reading it.

use std::ops::Bound;
use std::time::Duration;

use pigeonhole::{KeySet, Pigeonhole, RowPosition, Table, Vector};

use crate::cmd::schema::FamilyDef;
use crate::exit::Failure;
use crate::open::{Db, Handle};

/// A read's selection within each row: the tools' `families`, `qualifier`,
/// `qualifier_prefix` and `versions`.
pub(crate) struct Selection {
    pub(crate) families: Vec<String>,
    pub(crate) qualifier: Option<Vec<u8>>,
    pub(crate) qualifier_prefix: Option<Vec<u8>>,
    pub(crate) versions: u32,
}

/// A cell's value, owned.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CellValue {
    Bytes(Vec<u8>),
    /// A counter's, or a varint's (`dump` shows both as `i64`).
    I64(i64),
    F64(f64),
    Vector(Vector),
    /// A type this build doesn't know.
    Unknown,
}

impl CellValue {
    /// The library's value, owned.
    pub(crate) fn of(v: pigeonhole::Value<'_>) -> Self {
        match v {
            pigeonhole::Value::Bytes(b) => Self::Bytes(b.to_vec()),
            pigeonhole::Value::I64(n) | pigeonhole::Value::Varint(n) => Self::I64(n),
            pigeonhole::Value::F64(x) => Self::F64(x),
            pigeonhole::Value::Vector(v) => vector_of(&v).map_or(Self::Unknown, Self::Vector),
            _ => Self::Unknown,
        }
    }

    /// The library's value, borrowed from this one: `None` for [`CellValue::Unknown`].
    pub(crate) fn value(&self) -> Option<pigeonhole::Value<'_>> {
        Some(match self {
            Self::Bytes(b) => pigeonhole::Value::Bytes(b),
            Self::I64(n) => pigeonhole::Value::I64(*n),
            Self::F64(x) => pigeonhole::Value::F64(*x),
            Self::Vector(v) => pigeonhole::Value::Vector(v.as_ref()),
            Self::Unknown => return None,
        })
    }
}

/// An owned copy of a vector value: `None` for an element this build can't rebuild.
pub(crate) fn vector_of(v: &pigeonhole::VectorRef<'_>) -> Option<Vector> {
    vector_from(v.element(), v.dim(), v.as_bytes())
}

/// A vector from its element, dim and little-endian bytes.
pub(crate) fn vector_from(element: pigeonhole::Element, dim: u32, b: &[u8]) -> Option<Vector> {
    use pigeonhole::Element as E;
    let halves = || -> Vec<u16> {
        b.as_chunks::<2>()
            .0
            .iter()
            .map(|c| u16::from_le_bytes(*c))
            .collect()
    };
    match element {
        E::F32 => Vector::from_f32(
            &b.as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c))
                .collect::<Vec<_>>(),
        )
        .ok(),
        E::I8 => Vector::from_i8(&b.iter().map(|&x| x as i8).collect::<Vec<_>>()).ok(),
        E::F16 => Vector::from_f16_bits(&halves()).ok(),
        E::BF16 => Vector::from_bf16_bits(&halves()).ok(),
        E::Bit => Vector::from_bits(dim, b).ok(),
        _ => None,
    }
}

/// A cell a read returned.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CellOut {
    pub(crate) family: String,
    pub(crate) qualifier: Vec<u8>,
    pub(crate) ts: u64,
    pub(crate) value: CellValue,
}

impl CellOut {
    fn of(e: &pigeonhole::CellEntry<'_>) -> Self {
        Self {
            family: e.family.to_owned(),
            qualifier: e.qualifier.to_vec(),
            ts: e.cell.timestamp(),
            value: CellValue::of(e.cell.typed()),
        }
    }
}

/// A `traverse` call's arguments.
pub(crate) struct TraverseArgs<'a> {
    pub(crate) family: &'a str,
    /// The seeds, sorted and without repeats (a [`KeySet`]'s order): hop 0's parents index
    /// them.
    pub(crate) seeds: &'a KeySet,
    pub(crate) hops: u32,
    pub(crate) per_node_limit: Option<u32>,
    pub(crate) hop_limit: Option<u64>,
    pub(crate) max_nodes: u64,
    pub(crate) parents: bool,
    pub(crate) qualifier_prefix: Option<&'a [u8]>,
}

/// One hop of a traversal: its new nodes, and with `parents` each one's source (an index
/// into the previous hop's nodes, or the seeds).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HopOut {
    pub(crate) nodes: Vec<Vec<u8>>,
    pub(crate) parents: Option<Vec<u32>>,
}

/// A traversal's result.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TraverseOut {
    pub(crate) hops: Vec<HopOut>,
    pub(crate) rows: u64,
    pub(crate) edges: u64,
    pub(crate) cut_by_max_nodes: bool,
}

impl TraverseOut {
    /// The bytes it holds, about: what a cache of it costs.
    pub(crate) fn bytes(&self) -> usize {
        self.hops
            .iter()
            .map(|h| {
                let parents = if h.parents.is_some() { 4 } else { 0 };
                h.nodes.iter().map(|n| n.len() + parents).sum::<usize>()
            })
            .sum()
    }
}

/// A nearest search's query, in its family's element type.
pub(crate) enum Query {
    F32(Vec<f32>),
    I8(Vec<i8>),
    Bits(Vec<u8>),
}

/// A `nearest` call's arguments: rows in `lo..hi` (one row when both are that row), the
/// family's vectors under the qualifier selection.
pub(crate) struct NearestArgs<'a> {
    pub(crate) family: &'a str,
    pub(crate) lo: Bound<&'a [u8]>,
    pub(crate) hi: Bound<&'a [u8]>,
    pub(crate) qualifier: Option<&'a [u8]>,
    pub(crate) qualifier_prefix: Option<&'a [u8]>,
    pub(crate) query: &'a Query,
    pub(crate) k: usize,
}

/// A neighbour a nearest search found.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Neighbour {
    pub(crate) row: Vec<u8>,
    pub(crate) qualifier: Vec<u8>,
    pub(crate) ts: u64,
    pub(crate) distance: f64,
}

/// An aggregate family's input.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Input {
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
}

/// One `write` operation, checked against its family.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WriteOp {
    Put {
        family: String,
        qualifier: Vec<u8>,
        ts: Option<u64>,
        value: CellValue,
    },
    Incr {
        family: String,
        qualifier: Vec<u8>,
        ts: Option<u64>,
        delta: i64,
    },
    Aggregate {
        family: String,
        qualifier: Vec<u8>,
        ts: Option<u64>,
        input: Input,
    },
    DeleteRow,
    DeleteFamily {
        family: String,
    },
    DeleteColumn {
        family: String,
        qualifier: Vec<u8>,
    },
    DeleteCell {
        family: String,
        qualifier: Vec<u8>,
        ts: u64,
    },
}

/// A `write` operation with its table and row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowWrite {
    pub(crate) table: String,
    pub(crate) row: Vec<u8>,
    pub(crate) op: WriteOp,
}

/// A `feed_poll` call's feed.
pub(crate) struct FeedArgs<'a> {
    pub(crate) tables: &'a [String],
    pub(crate) prefix: Option<&'a [u8]>,
    pub(crate) token: Option<&'a [u8]>,
    pub(crate) from_oldest: bool,
    pub(crate) visible: bool,
}

/// A change in a committed write, as the feed delivers it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChangeOut {
    pub(crate) table: String,
    pub(crate) family: String,
    /// `put`, `merge`, `delete_cell`, `delete_column`, `delete_family`, or `unknown`.
    pub(crate) kind: &'static str,
    pub(crate) row: Vec<u8>,
    pub(crate) qualifier: Vec<u8>,
    pub(crate) ts: u64,
    pub(crate) value: Option<CellValue>,
}

/// A committed write, as the feed delivers it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CommitOut {
    pub(crate) seqno: u64,
    pub(crate) ts: u64,
    pub(crate) changes: Vec<ChangeOut>,
}

/// An open feed: whole commits in order, and the position after the last one taken.
pub(crate) trait FeedSource {
    /// The next commit, waiting up to `wait` for one: `None` if none came.
    fn next(&mut self, wait: Duration) -> Result<Option<CommitOut>, Failure>;
    /// The next commit if one is ready now.
    fn try_next(&mut self) -> Result<Option<CommitOut>, Failure>;
    /// The position after the last commit taken (at open, where the feed starts).
    fn token(&self) -> Vec<u8>;
}

/// The tools' database surface.
pub(crate) trait Backend: Send + Sync {
    /// How the database is open, for the tools' answers ("writer", "reader", ...).
    fn mode(&self) -> &'static str;
    /// Whether `feed_poll` belongs in `tools/list`: the writer's feed is on, or this is a
    /// reader process, which follows the writer's feed whatever its own `info` says (#1361).
    fn lists_feed(&self) -> bool;
    /// Whether writes can succeed here.
    fn writes(&self) -> bool;
    /// The tables, by name.
    fn tables(&self) -> Result<Vec<String>, Failure>;
    /// A table's families. The failures here name their table (`table <name>: ...`).
    fn families(&self, table: &str) -> Result<Vec<FamilyDef>, Failure>;
    /// Row `row`'s columns from `resume` (a resume `each` was given) on, in order, each with
    /// the resume that continues after it, until `each` says stop (`false`).
    fn read_row(
        &self,
        table: &str,
        row: &[u8],
        sel: &Selection,
        resume: Option<&[u8]>,
        each: &mut dyn FnMut(Vec<CellOut>, Vec<u8>) -> bool,
    ) -> Result<(), Failure>;
    /// The rows in `lo..hi`, in key order, each with its cells (at most `columns` columns),
    /// at most `limit` of them, until `each` says stop (`false`).
    #[allow(clippy::too_many_arguments)]
    fn scan(
        &self,
        table: &str,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        sel: &Selection,
        columns: u32,
        limit: u64,
        each: &mut dyn FnMut(&[u8], Vec<CellOut>) -> bool,
    ) -> Result<(), Failure>;
    /// A traversal from `a.seeds` over `a.family`'s edges.
    fn traverse(&self, table: &str, a: &TraverseArgs<'_>) -> Result<TraverseOut, Failure>;
    /// The `a.k` nearest vectors to `a.query`.
    fn nearest(&self, table: &str, a: &NearestArgs<'_>) -> Result<Vec<Neighbour>, Failure>;
    /// Commits `ops` as one atomic batch: the seqno it committed at.
    fn write(&self, ops: &[RowWrite]) -> Result<u64, Failure>;
    /// Creates `table` with `families`, or opens it if it exists: its families as they are.
    fn create_table(&self, table: &str, families: &[FamilyDef]) -> Result<Vec<FamilyDef>, Failure>;
    /// Opens a feed.
    fn feed(&self, a: &FeedArgs<'_>) -> Result<Box<dyn FeedSource + '_>, Failure>;
    /// Closes the database (stdio, at the end of input); a no-op where the backend doesn't
    /// own it.
    fn close(self: Box<Self>) -> Result<(), Failure>;
}

impl Db {
    /// The writer's database, if this is the writer.
    fn writer(&self) -> Option<&Pigeonhole> {
        match self {
            Db::Writer(db) => Some(db),
            Db::Reader(_) => None,
        }
    }

    /// A table to write, as the writer.
    fn open_for_write(&self, table: &str) -> Result<Table, Failure> {
        let db = self
            .writer()
            .ok_or_else(|| Failure::read_only(Db::mode(self)))?;
        db.table(table)
            .and_then(|b| b.open())
            .map_err(|e| Failure::db(&format!("table {table}"), &e))
    }
}

impl Backend for Db {
    fn mode(&self) -> &'static str {
        Db::mode(self)
    }

    fn lists_feed(&self) -> bool {
        matches!(self, Db::Reader(_)) || self.info().is_ok_and(|i| i.feed.is_some())
    }

    fn writes(&self) -> bool {
        self.writer().is_some()
    }

    fn tables(&self) -> Result<Vec<String>, Failure> {
        Ok(Db::tables(self))
    }

    fn families(&self, table: &str) -> Result<Vec<FamilyDef>, Failure> {
        Db::families(self, table)
            .map(|fs| fs.iter().map(FamilyDef::from_info).collect())
            .map_err(|e| Failure::db(&format!("table {table}"), &e))
    }

    fn read_row(
        &self,
        table: &str,
        row: &[u8],
        sel: &Selection,
        resume: Option<&[u8]>,
        each: &mut dyn FnMut(Vec<CellOut>, Vec<u8>) -> bool,
    ) -> Result<(), Failure> {
        let resume = resume
            .map(|b| RowPosition::from_bytes(b).map_err(|e| Failure::db("cursor", &e)))
            .transpose()?;
        let read = |e: pigeonhole::Error| Failure::db(&format!("read_row {table}"), &e);
        macro_rules! go {
            ($t:expr) => {{
                let mut r = $t.row(row).versions(sel.versions);
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
                while let Some(page) = pages.next().map_err(read)? {
                    let column: Vec<CellOut> = page.iter().map(|e| CellOut::of(&e)).collect();
                    let after = pages.position().map(|p| p.to_bytes()).unwrap_or_default();
                    if !each(column, after) {
                        break;
                    }
                }
            }};
        }
        match self.handle(table)? {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        }
        Ok(())
    }

    fn scan(
        &self,
        table: &str,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        sel: &Selection,
        columns: u32,
        limit: u64,
        each: &mut dyn FnMut(&[u8], Vec<CellOut>) -> bool,
    ) -> Result<(), Failure> {
        let read = |e: pigeonhole::Error| Failure::db(&format!("scan {table}"), &e);
        macro_rules! go {
            ($t:expr) => {{
                let mut s = $t
                    .scan_bounds(lo, hi)
                    .versions(sel.versions)
                    .column_limit(columns)
                    .limit(limit);
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
                    let cells: Vec<CellOut> = row.iter().map(|e| CellOut::of(&e)).collect();
                    if !each(row.key(), cells) {
                        break;
                    }
                }
            }};
        }
        match self.handle(table)? {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        }
        Ok(())
    }

    fn traverse(&self, table: &str, a: &TraverseArgs<'_>) -> Result<TraverseOut, Failure> {
        let read = |e: pigeonhole::Error| Failure::db(&format!("traverse {table}"), &e);
        macro_rules! go {
            ($t:expr) => {{
                let mut t = $t
                    .traverse(a.seeds)
                    .family(a.family)
                    .hops(a.hops)
                    .max_nodes(a.max_nodes)
                    .parents(a.parents);
                if let Some(n) = a.per_node_limit {
                    t = t.per_node_limit(n);
                }
                if let Some(n) = a.hop_limit {
                    t = t.hop_limit(n);
                }
                if let Some(q) = a.qualifier_prefix {
                    t = t.qualifier_prefix(q);
                }
                t.run().map_err(read)?
            }};
        }
        let t = match self.handle(table)? {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        };
        let stats = t.stats();
        Ok(TraverseOut {
            hops: t
                .hops()
                .iter()
                .map(|h| HopOut {
                    nodes: (0..h.nodes().len())
                        .map(|i| h.nodes().get(i).to_vec())
                        .collect(),
                    parents: h.parents().map(<[u32]>::to_vec),
                })
                .collect(),
            rows: stats.rows,
            edges: stats.edges,
            cut_by_max_nodes: stats.cut_by_max_nodes,
        })
    }

    fn nearest(&self, table: &str, a: &NearestArgs<'_>) -> Result<Vec<Neighbour>, Failure> {
        let read = |e: pigeonhole::Error| Failure::db(&format!("nearest {table}"), &e);
        macro_rules! go {
            ($t:expr) => {{
                let mut s = $t.scan_bounds(a.lo, a.hi).family(a.family);
                if let Some(q) = a.qualifier_prefix {
                    s = s.qualifier_prefix(q);
                }
                if let Some(q) = a.qualifier {
                    s = s.qualifier_bounds(Bound::Included(q), Bound::Included(q));
                }
                let n = match a.query {
                    Query::F32(q) => s.nearest_f32(q, a.k),
                    Query::I8(q) => s.nearest_i8(q, a.k),
                    Query::Bits(q) => s.nearest_bits(q, a.k),
                };
                n.run().map_err(read)?
            }};
        }
        let found = match self.handle(table)? {
            Handle::Writer(t) => go!(t),
            Handle::Reader(t) => go!(t),
        };
        Ok(found
            .into_iter()
            .map(|n| Neighbour {
                row: n.row,
                qualifier: n.qualifier,
                ts: n.ts,
                distance: n.distance,
            })
            .collect())
    }

    fn write(&self, ops: &[RowWrite]) -> Result<u64, Failure> {
        let db = self
            .writer()
            .ok_or_else(|| Failure::read_only(Db::mode(self)))?;
        // Every table first: the batch borrows them.
        let mut tables: std::collections::HashMap<&str, Table> = std::collections::HashMap::new();
        for (i, w) in ops.iter().enumerate() {
            if !tables.contains_key(w.table.as_str()) {
                let t = self
                    .open_for_write(&w.table)
                    .map_err(|f| f.within(&format!("ops[{i}]")))?;
                tables.insert(&w.table, t);
            }
        }
        let mut batch = db.write_batch();
        for w in ops {
            let t = &tables[w.table.as_str()];
            let row = &w.row[..];
            match &w.op {
                WriteOp::Put {
                    family: f,
                    qualifier: q,
                    ts,
                    value,
                } => match (value, *ts) {
                    (CellValue::Bytes(b), None) => batch.put(t, row, f, q, b),
                    (CellValue::Bytes(b), Some(ts)) => batch.put_at(t, row, f, q, ts, b),
                    (CellValue::I64(n), None) => batch.put_i64(t, row, f, q, *n),
                    (CellValue::I64(n), Some(ts)) => batch.put_i64_at(t, row, f, q, ts, *n),
                    (CellValue::F64(x), None) => batch.put_f64(t, row, f, q, *x),
                    (CellValue::F64(x), Some(ts)) => batch.put_f64_at(t, row, f, q, ts, *x),
                    (CellValue::Vector(v), ts) => {
                        let v = v.as_ref();
                        match (v.element(), ts) {
                            (pigeonhole::Element::I8, None) => {
                                batch.put_vector_i8(t, row, f, q, &i8s(&v))
                            }
                            (pigeonhole::Element::I8, Some(ts)) => {
                                batch.put_vector_i8_at(t, row, f, q, ts, &i8s(&v))
                            }
                            (pigeonhole::Element::Bit, None) => {
                                batch.put_vector_bits(t, row, f, q, v.as_bytes())
                            }
                            (pigeonhole::Element::Bit, Some(ts)) => {
                                batch.put_vector_bits_at(t, row, f, q, ts, v.as_bytes())
                            }
                            (_, ts) => {
                                let xs = v.to_f32_vec().unwrap_or_default();
                                match ts {
                                    None => batch.put_vector_f32(t, row, f, q, &xs),
                                    Some(ts) => batch.put_vector_f32_at(t, row, f, q, ts, &xs),
                                }
                            }
                        }
                    }
                    (CellValue::Unknown, _) => {
                        return Err(Failure::usage("put: a value of an unknown type"));
                    }
                },
                WriteOp::Incr {
                    family: f,
                    qualifier: q,
                    ts,
                    delta,
                } => match ts {
                    None => batch.incr(t, row, f, q, *delta),
                    Some(ts) => batch.incr_at(t, row, f, q, *ts, *delta),
                },
                WriteOp::Aggregate {
                    family: f,
                    qualifier: q,
                    ts,
                    input,
                } => match (input, *ts) {
                    (Input::I64(n), None) => batch.aggregate(t, row, f, q, *n),
                    (Input::I64(n), Some(ts)) => batch.aggregate_at(t, row, f, q, ts, *n),
                    (Input::U64(n), None) => batch.aggregate_u64(t, row, f, q, *n),
                    (Input::U64(n), Some(ts)) => batch.aggregate_u64_at(t, row, f, q, ts, *n),
                    (Input::F64(x), None) => batch.aggregate_f64(t, row, f, q, *x),
                    (Input::F64(x), Some(ts)) => batch.aggregate_f64_at(t, row, f, q, ts, *x),
                    (Input::Bytes(b), None) => batch.aggregate_bytes(t, row, f, q, b),
                    (Input::Bytes(b), Some(ts)) => batch.aggregate_bytes_at(t, row, f, q, ts, b),
                },
                WriteOp::DeleteRow => batch.delete_row(t, row),
                WriteOp::DeleteFamily { family } => batch.delete_family(t, row, family),
                WriteOp::DeleteColumn { family, qualifier } => {
                    batch.delete_column(t, row, family, qualifier)
                }
                WriteOp::DeleteCell {
                    family,
                    qualifier,
                    ts,
                } => batch.delete_cell(t, row, family, qualifier, *ts),
            };
        }
        let info = batch.commit().map_err(|e| {
            let f = Failure::db("write", &e);
            // The library's message may name its Rust methods; say what to check here.
            if e.code() == pigeonhole::ErrorCode::InvalidArgument {
                f.hint("check each family's kind and options with list_tables")
            } else {
                f
            }
        })?;
        Ok(info.seqno)
    }

    fn create_table(&self, table: &str, families: &[FamilyDef]) -> Result<Vec<FamilyDef>, Failure> {
        let db = self
            .writer()
            .ok_or_else(|| Failure::read_only(Db::mode(self)))?;
        let t = db
            .table(table)
            .and_then(|mut b| {
                for f in families {
                    b = b.family(&f.name, f.family());
                }
                b.create_if_missing()
            })
            .map_err(|e| Failure::db(&format!("create_table {table}"), &e))?;
        Ok(t.families()
            .into_iter()
            .filter_map(|f| t.family_info(f))
            .map(|f| FamilyDef::from_info(&f))
            .collect())
    }

    fn feed(&self, a: &FeedArgs<'_>) -> Result<Box<dyn FeedSource + '_>, Failure> {
        // As the writer or as a reader process (D310 PR 4, #1361): a reader follows the
        // writer's feed from shared memory, so `--read-only` serves it too.
        let mut handles = Vec::with_capacity(a.tables.len());
        for n in a.tables {
            handles.push(self.handle(n)?);
        }
        let start = |h: &Handle| match h {
            Handle::Writer(t) => t.feed(),
            Handle::Reader(t) => t.feed(),
        };
        let mut b = start(&handles[0]);
        for h in &handles[1..] {
            b = match h {
                Handle::Writer(t) => b.table(t),
                Handle::Reader(t) => b.read_table(t),
            };
        }
        if let Some(p) = a.prefix {
            b = b.prefix(p);
        }
        if a.visible {
            b = b.visible();
        }
        if a.from_oldest {
            b = b.from_oldest();
        }
        if let Some(t) = a.token {
            b = b.resume(&pigeonhole::FeedToken::from_bytes(t.to_vec()));
        }
        let feed = b.open().map_err(feed_failure)?;
        Ok(Box::new(LibFeed(feed)))
    }

    fn close(self: Box<Self>) -> Result<(), Failure> {
        Db::close(*self)
    }
}

fn i8s(v: &pigeonhole::VectorRef<'_>) -> Vec<i8> {
    v.as_bytes().iter().map(|&b| b as i8).collect()
}

/// The library's feed, as a [`FeedSource`].
struct LibFeed(pigeonhole::Feed);

impl LibFeed {
    fn take(c: Option<&pigeonhole::Commit>) -> Option<CommitOut> {
        c.map(commit_of)
    }
}

impl FeedSource for LibFeed {
    fn next(&mut self, wait: Duration) -> Result<Option<CommitOut>, Failure> {
        Ok(Self::take(self.0.next(wait).map_err(feed_failure)?))
    }

    fn try_next(&mut self) -> Result<Option<CommitOut>, Failure> {
        Ok(Self::take(self.0.try_next().map_err(feed_failure)?))
    }

    fn token(&self) -> Vec<u8> {
        self.0.token().as_bytes().to_vec()
    }
}

/// A library commit, owned.
fn commit_of(c: &pigeonhole::Commit) -> CommitOut {
    use pigeonhole::ChangeKind as K;
    CommitOut {
        seqno: c.seqno(),
        ts: c.timestamp(),
        changes: c
            .changes()
            .iter()
            .map(|ch| ChangeOut {
                table: ch.table().to_owned(),
                family: ch.family().to_owned(),
                kind: match ch.kind() {
                    K::Put => "put",
                    K::Merge => "merge",
                    K::DeleteCell => "delete_cell",
                    K::DeleteColumn => "delete_column",
                    K::DeleteFamily => "delete_family",
                    _ => "unknown",
                },
                row: ch.row().to_vec(),
                qualifier: ch.qualifier().to_vec(),
                ts: ch.timestamp(),
                value: ch.value().map(CellValue::of),
            })
            .collect(),
    }
}

/// A `feed_poll` failure, with a hint for the codes a caller can act on.
pub(crate) fn feed_failure(e: pigeonhole::Error) -> Failure {
    let f = Failure::db("feed_poll", &e);
    let code = f.code().to_owned();
    feed_hint(f, &code)
}

/// `f` with the hint for feed error `code`, if it has one.
pub(crate) fn feed_hint(f: Failure, code: &str) -> Failure {
    match code {
        "FeedExpired" => f.hint(
            "the history this token needs is gone: re-read the tables (scan) and \
             call feed_poll again without a token",
        ),
        "SnapshotExpired" => f.hint(
            "the application is restarting under this reader: call feed_poll again \
             with the same token",
        ),
        "NoWriter" => f.hint(
            "the application isn't running: call feed_poll again with the same token \
             once it is",
        ),
        "InvalidArgument" => f.hint(
            "the database's change feed must be on (the application's, or \
             --change-feed) and follow these tables, and the token must come from this \
             database",
        ),
        _ => f,
    }
}
