//! The `/mcp` tools' database surface on a server (#1915 PR 3, ICR 0139's PR 3 section):
//! each of a tool's calls is a service op through [`Router::dispatch_by`], as a gRPC or
//! `/v1` call is, so it routes, fans out and refuses as they do: scopes, the reserved
//! `__phdb_` prefix, the memory store's tables, a replica's write refusal, a cluster's
//! partitions.
//!
//! The tools run on a blocking thread under a `/mcp` place, never a `--blocking-threads`
//! permit (`serve::mcp::post`): each call blocks that thread on the runtime
//! (`Handle::block_on`) while its op runs, and the op takes a permit of its own. A `/mcp`
//! message therefore never holds a permit its op waits for.
//!
//! [`Router::dispatch_by`]: super::router::Router::dispatch_by

use std::collections::VecDeque;
use std::future::poll_fn;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures_core::Stream;
use pigeonhole::Pigeonhole;

use super::Shared;
use super::auth::Caller;
use super::errors::ApiError;
use super::proto;
use super::service::{ServiceOp, ServiceReply};
use crate::cmd::schema::FamilyDef;
use crate::exit::Failure;
use crate::mcp::backend::{
    Backend, CellOut, CellValue, ChangeOut, CommitOut, FeedArgs, FeedSource, HopOut, Input,
    NearestArgs, Neighbour, Query, RowWrite, Selection, TraverseArgs, TraverseOut, WriteOp,
    vector_from,
};
use crate::mcp::memory::{Forgotten, Hit, Marked, Recalled, Store as MemStore, Stored};
use crate::memcore::{RecallArgs, Remember};
use pigeonhole_memory::{EmbeddingModel, LinkType, MemoryId};

/// A result's pages are this size at most: the tools cut their results by their own budget
/// (`--mcp-max-result-bytes`), so the op's own cap is the server's largest.
const PAGE_BYTES: u64 = 8 << 20;

/// A service refusal as a tool's failure: its code and message.
pub(super) fn failure(e: ApiError) -> Failure {
    Failure::served(&e.code, e.code_num, &e.message)
}

/// The tools' database surface on a server: service ops through the router, as the session's
/// caller, within the current message's deadline.
#[derive(Clone)]
pub(super) struct ServeBackend {
    /// Weak: the session table, inside the service, holds the session that holds this.
    pub(super) shared: Weak<Shared>,
    /// The token the session was started with, which each call is made as.
    pub(super) caller: Caller,
    /// The served database, for what the tools say about it (`mode`, whether its feed is
    /// on): never read or written here.
    pub(super) db: Pigeonhole,
    pub(super) writable: bool,
    pub(super) replica: bool,
}

thread_local! {
    /// The deadline of the `/mcp` message this blocking thread runs (from its POST's
    /// arrival), which every call's op runs within (simp724's S1): per thread, since a
    /// session's messages can run at once.
    static DEADLINE: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

/// Runs `f` (a `/mcp` message, on its blocking thread) with its calls' ops within
/// `deadline`.
pub(super) fn within<T>(deadline: std::time::Instant, f: impl FnOnce() -> T) -> T {
    /// Clears the deadline however `f` ends, a panic included: the pool reuses the thread.
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            DEADLINE.with(|d| d.set(None));
        }
    }
    DEADLINE.with(|d| d.set(Some(deadline)));
    let _clear = Clear;
    f()
}

impl ServeBackend {
    fn shared(&self) -> Result<Arc<Shared>, Failure> {
        self.shared
            .upgrade()
            .ok_or_else(|| failure(ApiError::unavailable("the server is shutting down")))
    }

    /// `op` through the router, on this blocking thread: its reply, unwrapped of the
    /// in-flight charge, which is held until the reply is dropped.
    fn call(&self, op: ServiceOp) -> Result<Reply, Failure> {
        #[cfg(test)]
        hold(self.db.path());
        let shared = self.shared()?;
        let deadline = DEADLINE.with(std::cell::Cell::get);
        // Past its message's deadline, a tool makes no more calls: a message holds its place
        // for its deadline and one op at most, on any node (simp724 on #1915 PR 3a; a single
        // node's ops don't read the deadline themselves).
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return Err(failure(ApiError::deadline_exceeded()));
        }
        let rt = tokio::runtime::Handle::current();
        let reply = rt
            .block_on(async {
                // The memory tools read the fields `memory_tool_fields` names, so on a cluster
                // every owner the call reaches must have them (#1915 PR 3b): a `Remember` with
                // `supersedes` is gated by the router for every caller; these only here.
                if matches!(
                    op,
                    ServiceOp::Recall(_) | ServiceOp::MarkUsed(_) | ServiceOp::Forget(_)
                ) {
                    shared.router.memory_fields(&op).await?;
                }
                shared
                    .router
                    .dispatch_by(op, &self.caller, None, deadline)
                    .await
            })
            .map_err(failure)?;
        Ok(match reply {
            ServiceReply::Charged(r, charge) => Reply {
                reply: *r,
                _charge: Some(charge),
            },
            reply => Reply {
                reply,
                _charge: None,
            },
        })
    }

    /// The next item of `s`, on this blocking thread.
    fn next<S: Stream + Unpin>(s: &mut S) -> Option<S::Item> {
        tokio::runtime::Handle::current().block_on(poll_fn(|cx| Pin::new(&mut *s).poll_next(cx)))
    }
}

/// A hold seam (tests): each `/mcp` call on the database at a path waits at its barrier
/// before its op, so a test can have every message in flight, holding its place, before any
/// op takes a `--blocking-threads` permit (the nested-pool deadlock test).
#[cfg(test)]
pub(crate) static HOLD: std::sync::Mutex<Vec<(std::path::PathBuf, Arc<std::sync::Barrier>)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn hold(path: &std::path::Path) {
    let barrier = HOLD
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .find(|(p, _)| p == path)
        .map(|(_, b)| Arc::clone(b));
    if let Some(b) = barrier {
        b.wait();
    }
}

/// A reply, and the in-flight charge it holds until it's dropped.
struct Reply {
    reply: ServiceReply,
    _charge: Option<super::charge::Charge>,
}

fn unexpected(op: &str) -> Failure {
    failure(ApiError::internal(format!("{op} answered another reply")))
}

/// A read's selection as the service takes it: `qualifier` is a pair of inclusive bounds.
fn selection(sel: &Selection, column_limit: u32) -> proto::Selection {
    let q = |b: &Vec<u8>| proto::Bound {
        key: b.clone().into(),
        inclusive: true,
    };
    proto::Selection {
        families: sel.families.clone(),
        qualifier_prefix: sel.qualifier_prefix.clone().unwrap_or_default().into(),
        qualifier_start: sel.qualifier.as_ref().map(q),
        qualifier_end: sel.qualifier.as_ref().map(q),
        versions: sel.versions,
        column_limit,
        ..Default::default()
    }
}

/// A key range's bounds as the service takes them.
fn bounds(lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> proto::KeyRange {
    let bound = |b: Bound<&[u8]>| match b {
        Bound::Included(k) => Some(proto::Bound {
            key: k.to_vec().into(),
            inclusive: true,
        }),
        Bound::Excluded(k) => Some(proto::Bound {
            key: k.to_vec().into(),
            inclusive: false,
        }),
        Bound::Unbounded => None,
    };
    proto::KeyRange {
        range: Some(proto::key_range::Range::Bounds(proto::Bounds {
            start: bound(lo),
            end: bound(hi),
        })),
    }
}

/// A value as the service sends it.
fn value_of(v: Option<&proto::Value>, stored: &[u8]) -> CellValue {
    use proto::value::Kind;
    match v.and_then(|v| v.kind.as_ref()) {
        None => CellValue::Bytes(stored.to_vec()),
        Some(Kind::Bytes(b)) => CellValue::Bytes(b.to_vec()),
        Some(Kind::I64(n)) => CellValue::I64(*n),
        Some(Kind::F64(x)) => CellValue::F64(*x),
        Some(Kind::U64(n)) => i64::try_from(*n).map_or(CellValue::Unknown, CellValue::I64),
        Some(Kind::Vector(v)) => super::convert::element_of(v.element)
            .and_then(|e| vector_from(e, v.dim, &v.data))
            .map_or(CellValue::Unknown, CellValue::Vector),
    }
}

fn cell_of(c: &proto::Cell) -> CellOut {
    CellOut {
        family: c.family.clone(),
        qualifier: c.qualifier.to_vec(),
        ts: c.timestamp,
        value: value_of(c.typed.as_ref(), &c.value),
    }
}

/// A value as the service takes it.
fn value_out(v: &CellValue) -> Result<proto::Value, Failure> {
    use proto::value::Kind;
    let kind = match v {
        CellValue::Bytes(b) => Kind::Bytes(b.clone().into()),
        CellValue::I64(n) => Kind::I64(*n),
        CellValue::F64(x) => Kind::F64(*x),
        CellValue::Vector(v) => {
            let v = v.as_ref();
            Kind::Vector(proto::Vector {
                element: super::convert::element_code(v.element()),
                dim: v.dim(),
                data: v.as_bytes().to_vec().into(),
            })
        }
        CellValue::Unknown => return Err(Failure::usage("put: a value of an unknown type")),
    };
    Ok(proto::Value { kind: Some(kind) })
}

fn write_out(op: &WriteOp) -> Result<proto::Write, Failure> {
    use proto::write::Op;
    let op = match op {
        WriteOp::Put {
            family,
            qualifier,
            ts,
            value,
        } => Op::Put(proto::Put {
            family: family.clone(),
            qualifier: qualifier.clone().into(),
            timestamp: *ts,
            value: Some(value_out(value)?),
        }),
        WriteOp::Incr {
            family,
            qualifier,
            ts,
            delta,
        } => Op::Incr(proto::Incr {
            family: family.clone(),
            qualifier: qualifier.clone().into(),
            timestamp: *ts,
            delta: *delta,
        }),
        WriteOp::Aggregate {
            family,
            qualifier,
            ts,
            input,
        } => {
            use proto::value::Kind;
            let kind = match input {
                Input::I64(n) => Kind::I64(*n),
                Input::U64(n) => Kind::U64(*n),
                Input::F64(x) => Kind::F64(*x),
                Input::Bytes(b) => Kind::Bytes(b.clone().into()),
            };
            Op::Aggregate(proto::AggregateInput {
                family: family.clone(),
                qualifier: qualifier.clone().into(),
                timestamp: *ts,
                input: Some(proto::Value { kind: Some(kind) }),
            })
        }
        WriteOp::DeleteRow => Op::DeleteRow(proto::DeleteRow {}),
        WriteOp::DeleteFamily { family } => Op::DeleteFamily(proto::DeleteFamily {
            family: family.clone(),
        }),
        WriteOp::DeleteColumn { family, qualifier } => Op::DeleteColumn(proto::DeleteColumn {
            family: family.clone(),
            qualifier: qualifier.clone().into(),
        }),
        WriteOp::DeleteCell {
            family,
            qualifier,
            ts,
        } => Op::DeleteCell(proto::DeleteCell {
            family: family.clone(),
            qualifier: qualifier.clone().into(),
            timestamp: *ts,
        }),
    };
    Ok(proto::Write { op: Some(op) })
}

/// `read_row`'s resume on a server: where the page holding the next column starts (the
/// service's `next`, empty for the row's first page), and how many of that page's columns
/// were already returned. A page holds whole columns, but only the page carries a position.
fn row_resume(after: &[u8], skip: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(after.len() + 5);
    b.push(ROW_RESUME);
    b.extend_from_slice(&skip.to_le_bytes());
    b.extend_from_slice(after);
    b
}

/// [`row_resume`]'s tag, unlike a `RowPosition`'s first byte (its version), so a cursor
/// from `phdb mcp` or an older server is refused, not misread.
const ROW_RESUME: u8 = 0xA5;

fn row_resume_of(b: &[u8]) -> Result<(Vec<u8>, u32), Failure> {
    match b {
        [ROW_RESUME, s0, s1, s2, s3, after @ ..] => {
            Ok((after.to_vec(), u32::from_le_bytes([*s0, *s1, *s2, *s3])))
        }
        _ => Err(Failure::usage(
            "cursor: not a read_row cursor of this server",
        )),
    }
}

/// Groups `cells` (in column order) into columns.
fn columns(cells: Vec<CellOut>) -> Vec<Vec<CellOut>> {
    let mut out: Vec<Vec<CellOut>> = Vec::new();
    for c in cells {
        match out.last_mut() {
            Some(col) if col[0].family == c.family && col[0].qualifier == c.qualifier => {
                col.push(c)
            }
            _ => out.push(vec![c]),
        }
    }
    out
}

impl Backend for ServeBackend {
    fn mode(&self) -> &'static str {
        if self.replica { "replica" } else { "server" }
    }

    fn lists_feed(&self) -> bool {
        self.db.info().is_ok_and(|i| i.feed.is_some())
    }

    fn writes(&self) -> bool {
        self.writable
    }

    fn tables(&self) -> Result<Vec<String>, Failure> {
        match self
            .call(ServiceOp::ListTables(proto::ListTablesRequest {}))?
            .reply
        {
            ServiceReply::Tables(t) => Ok(t.tables),
            _ => Err(unexpected("ListTables")),
        }
    }

    fn families(&self, table: &str) -> Result<Vec<FamilyDef>, Failure> {
        let reply = self
            .call(ServiceOp::DescribeTable(proto::DescribeTableRequest {
                name: table.to_owned(),
            }))
            .map_err(|f| f.within(&format!("table {table}")))?;
        match reply.reply {
            ServiceReply::Table(t) => t
                .families
                .iter()
                .map(|f| super::convert::family_def_in(f).map_err(failure))
                .collect(),
            _ => Err(unexpected("DescribeTable")),
        }
    }

    fn read_row(
        &self,
        table: &str,
        row: &[u8],
        sel: &Selection,
        resume: Option<&[u8]>,
        each: &mut dyn FnMut(Vec<CellOut>, Vec<u8>) -> bool,
    ) -> Result<(), Failure> {
        let (mut after, mut skip) = match resume {
            Some(b) => row_resume_of(b)?,
            None => (Vec::new(), 0),
        };
        loop {
            let reply = self.call(ServiceOp::ReadRow(proto::ReadRowRequest {
                table: table.to_owned(),
                row: row.to_vec().into(),
                selection: Some(selection(sel, 0)),
                page_cells: 0,
                after: after.clone().into(),
                page_bytes: PAGE_BYTES,
            }))?;
            let ServiceReply::ReadRow(page) = reply.reply else {
                return Err(unexpected("ReadRow"));
            };
            let cells: Vec<CellOut> = page
                .row
                .map(|r| r.cells.iter().map(cell_of).collect())
                .unwrap_or_default();
            let cols = columns(cells);
            let n = cols.len() as u32;
            for (i, col) in cols.into_iter().enumerate().skip(skip as usize) {
                // After the page's last column, the next page's start; else this page's
                // start with one more column passed.
                let next = if i as u32 + 1 == n && !page.next.is_empty() {
                    row_resume(&page.next, 0)
                } else if i as u32 + 1 == n {
                    Vec::new()
                } else {
                    row_resume(&after, i as u32 + 1)
                };
                if !each(col, next) {
                    return Ok(());
                }
            }
            if page.next.is_empty() {
                return Ok(());
            }
            after = page.next.to_vec();
            skip = 0;
        }
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
        let reply = self.call(ServiceOp::Scan(proto::ScanRequest {
            table: table.to_owned(),
            range: Some(bounds(lo, hi)),
            selection: Some(selection(sel, columns)),
            limit,
            page_bytes: PAGE_BYTES,
            after: Default::default(),
            allow_partial: false,
        }))?;
        let ServiceReply::Scan(mut pages) = reply.reply else {
            return Err(unexpected("Scan"));
        };
        // A page can end inside a row: the next one starts with the rest of it.
        let mut held: Option<(Vec<u8>, Vec<CellOut>)> = None;
        while let Some(page) = Self::next(&mut pages) {
            let page = page.map_err(failure)?;
            for r in page.rows {
                let cells: Vec<CellOut> = r.cells.iter().map(cell_of).collect();
                match &mut held {
                    Some((key, so_far)) if key[..] == r.key[..] => so_far.extend(cells),
                    _ => {
                        if let Some((key, cells)) = held.take()
                            && !each(&key, cells)
                        {
                            return Ok(());
                        }
                        held = Some((r.key.to_vec(), cells));
                    }
                }
            }
        }
        if let Some((key, cells)) = held {
            each(&key, cells);
        }
        Ok(())
    }

    fn traverse(&self, table: &str, a: &TraverseArgs<'_>) -> Result<TraverseOut, Failure> {
        let reply = self.call(ServiceOp::Traverse(proto::TraverseRequest {
            table: table.to_owned(),
            seeds: a.seeds.iter().map(|k| k.to_vec().into()).collect(),
            family: a.family.to_owned(),
            hops: a.hops,
            per_node_limit: a.per_node_limit.unwrap_or(0),
            hop_limit: a.hop_limit.unwrap_or(0),
            max_nodes: a.max_nodes,
            parents: a.parents,
            qualifier_prefix: a.qualifier_prefix.unwrap_or_default().to_vec().into(),
            page_bytes: PAGE_BYTES,
            ..Default::default()
        }))?;
        let ServiceReply::Traverse(t) = reply.reply else {
            return Err(unexpected("Traverse"));
        };
        let stats = t.stats.unwrap_or_default();
        Ok(TraverseOut {
            hops: t
                .hops
                .into_iter()
                .map(|h| HopOut {
                    nodes: h.nodes.iter().map(|n| n.to_vec()).collect(),
                    parents: a.parents.then_some(h.parents),
                })
                .collect(),
            rows: stats.rows,
            edges: stats.edges,
            cut_by_max_nodes: stats.cut_by_max_nodes,
        })
    }

    fn nearest(&self, table: &str, a: &NearestArgs<'_>) -> Result<Vec<Neighbour>, Failure> {
        let query = match a.query {
            Query::F32(xs) => proto::Vector {
                element: proto::Element::F32 as i32,
                dim: xs.len() as u32,
                data: xs
                    .iter()
                    .flat_map(|x| x.to_le_bytes())
                    .collect::<Vec<_>>()
                    .into(),
            },
            Query::I8(xs) => proto::Vector {
                element: proto::Element::I8 as i32,
                dim: xs.len() as u32,
                data: xs.iter().map(|&x| x as u8).collect::<Vec<_>>().into(),
            },
            Query::Bits(b) => proto::Vector {
                element: proto::Element::Bit as i32,
                dim: (b.len() * 8) as u32,
                data: b.clone().into(),
            },
        };
        let qualifiers = (a.qualifier.is_some() || a.qualifier_prefix.is_some()).then(|| {
            let q = |b: &[u8]| proto::Bound {
                key: b.to_vec().into(),
                inclusive: true,
            };
            proto::QualifierSelection {
                prefix: a.qualifier_prefix.unwrap_or_default().to_vec().into(),
                start: a.qualifier.map(q),
                end: a.qualifier.map(q),
            }
        });
        let reply = self.call(ServiceOp::Nearest(proto::NearestRequest {
            table: table.to_owned(),
            source: Some(proto::nearest_request::Source::Range(bounds(a.lo, a.hi))),
            family: a.family.to_owned(),
            query: Some(query),
            k: a.k as u32,
            page_bytes: PAGE_BYTES,
            qualifiers,
            ..Default::default()
        }))?;
        let ServiceReply::Nearest(n) = reply.reply else {
            return Err(unexpected("Nearest"));
        };
        Ok(n.neighbours
            .into_iter()
            .map(|n| Neighbour {
                row: n.row.to_vec(),
                qualifier: n.qualifier.to_vec(),
                ts: n.timestamp,
                distance: n.distance,
            })
            .collect())
    }

    fn write(&self, ops: &[RowWrite]) -> Result<u64, Failure> {
        let mut rows = Vec::with_capacity(ops.len());
        for (i, w) in ops.iter().enumerate() {
            rows.push(proto::RowWrites {
                row: Some(proto::RowKey {
                    table: w.table.clone(),
                    row: w.row.clone().into(),
                }),
                writes: vec![write_out(&w.op).map_err(|f| f.within(&format!("ops[{i}]")))?],
            });
        }
        let reply = self
            .call(ServiceOp::Batch(proto::BatchRequest {
                rows,
                durability: 0,
            }))
            .map_err(|f| {
                if f.code() == "InvalidArgument" {
                    f.hint("check each family's kind and options with list_tables")
                } else {
                    f
                }
            })?;
        match reply.reply {
            ServiceReply::Mutate(m) => Ok(m.commit.map_or(0, |c| c.seqno)),
            _ => Err(unexpected("Batch")),
        }
    }

    fn create_table(&self, table: &str, families: &[FamilyDef]) -> Result<Vec<FamilyDef>, Failure> {
        let reply = self
            .call(ServiceOp::CreateTable(proto::CreateTableRequest {
                name: table.to_owned(),
                families: families
                    .iter()
                    .map(super::convert::family_def_out)
                    .collect(),
                if_missing: true,
                placement: None,
            }))
            .map_err(|f| f.within(&format!("create_table {table}")))?;
        match reply.reply {
            ServiceReply::Table(t) => t
                .families
                .iter()
                .map(|f| super::convert::family_def_in(f).map_err(failure))
                .collect(),
            _ => Err(unexpected("CreateTable")),
        }
    }

    fn feed(&self, a: &FeedArgs<'_>) -> Result<Box<dyn FeedSource + '_>, Failure> {
        let shared = self.shared()?;
        let rt = tokio::runtime::Handle::current();
        let deadline = DEADLINE.with(std::cell::Cell::get);
        let feed = rt.block_on(subscribe(&shared, &self.caller, a, deadline))?;
        Ok(Box::new(feed))
    }

    fn close(self: Box<Self>) -> Result<(), Failure> {
        Ok(())
    }
}

/// `a`'s subscription, opened through the router: on a cluster node with a checkpoint at
/// every event, so each one carries a token to resume from (simp724's M1 on #1915 PR 3).
pub(super) async fn subscribe(
    shared: &Shared,
    caller: &Caller,
    a: &FeedArgs<'_>,
    deadline: Option<std::time::Instant>,
) -> Result<ServeFeed, Failure> {
    use proto::subscribe_request::Start;
    let start = match (a.token, a.from_oldest) {
        (Some(t), _) => Some(Start::Resume(t.to_vec().into())),
        (None, true) => Some(Start::FromOldest(true)),
        (None, false) => None,
    };
    let cluster = shared.router.cluster_node().is_some();
    let op = ServiceOp::Subscribe(proto::SubscribeRequest {
        tables: a.tables.to_vec(),
        prefix: a.prefix.unwrap_or_default().to_vec().into(),
        visible: a.visible,
        start,
        checkpoint_every: if cluster { 1 } else { 0 },
        ..Default::default()
    });
    let reply = shared
        .router
        .dispatch_by(op, caller, None, deadline)
        .await
        .map_err(feed_failure)?;
    let reply = match reply {
        ServiceReply::Charged(r, _) => *r,
        r => r,
    };
    let ServiceReply::Feed(mut events) = reply else {
        return Err(unexpected("Subscribe"));
    };
    // Placed, or refused: a resume's bad token comes back here, as its error (simp724).
    let started = events.start();
    let start = match deadline {
        Some(d) => tokio::time::timeout_at(d.into(), started)
            .await
            .map_err(|_| feed_failure(ApiError::deadline_exceeded()))?,
        None => started.await,
    }
    .map_err(feed_failure)?;
    let token = match a.token {
        Some(t) => t.to_vec(),
        None => start,
    };
    Ok(ServeFeed {
        events,
        ready: VecDeque::new(),
        token,
    })
}

fn feed_failure(e: ApiError) -> Failure {
    let code = e.code.clone();
    crate::mcp::backend::feed_hint(failure(e), &code)
}

/// The gap after which a poll that doesn't wait stops taking commits: the server delivers
/// them asynchronously, where the library's feed has them at hand, so what arrives that close
/// behind the last is taken as ready (best effort; serve.md says so).
const GAP: Duration = Duration::from_millis(20);

/// A subscription as a `feed_poll`'s feed.
pub(super) struct ServeFeed {
    events: super::feed::FeedEvents,
    /// Commits received and not yet taken, each with the token after it.
    ready: VecDeque<(CommitOut, Vec<u8>)>,
    /// The position after the last commit taken (at open, the subscription's start).
    token: Vec<u8>,
}

impl ServeFeed {
    /// Waits up to `wait` for the next commit, asynchronously: `post` waits so before its
    /// `/mcp` place, never holding one for a poll's wait (simp724's M2 on #1915 PR 3).
    pub(super) async fn wait(&mut self, wait: Duration) -> Result<(), Failure> {
        if !self.ready.is_empty() {
            return Ok(());
        }
        let until = tokio::time::Instant::now() + wait;
        loop {
            let next = tokio::time::timeout_at(
                until,
                poll_fn(|cx| Pin::new(&mut self.events).poll_next(cx)),
            )
            .await;
            match next {
                Err(_) | Ok(None) => return Ok(()),
                Ok(Some(e)) => {
                    if self.take(e.map_err(feed_failure)?) {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Takes event `e`: a commit is queued (`true`); a progress or partition event moves
    /// the position past what the filters skipped, when no commit is waiting before it.
    fn take(&mut self, e: proto::FeedEvent) -> bool {
        use proto::feed_event::Event;
        match e.event {
            Some(Event::Commit(c)) => {
                self.ready.push_back((commit_of(c), e.token.to_vec()));
                true
            }
            _ => {
                if !e.token.is_empty() && self.ready.is_empty() {
                    self.token = e.token.to_vec();
                }
                false
            }
        }
    }

    /// Takes every event ready now, without waiting.
    fn drain(&mut self) -> Result<(), Failure> {
        loop {
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            match Pin::new(&mut self.events).poll_next(&mut cx) {
                std::task::Poll::Ready(Some(e)) => {
                    self.take(e.map_err(feed_failure)?);
                }
                std::task::Poll::Ready(None) | std::task::Poll::Pending => return Ok(()),
            }
        }
    }

    fn pop(&mut self) -> Option<CommitOut> {
        let (c, token) = self.ready.pop_front()?;
        if !token.is_empty() {
            self.token = token;
        }
        Some(c)
    }
}

impl FeedSource for ServeFeed {
    fn next(&mut self, wait: Duration) -> Result<Option<CommitOut>, Failure> {
        if self.ready.is_empty() && !wait.is_zero() {
            tokio::runtime::Handle::current().block_on(self.wait(wait))?;
        }
        self.try_next()
    }

    /// What's ready: what has arrived, else what arrives within [`GAP`], never a poll's own
    /// wait. The subscription has taken its start already ([`subscribe`]), so a refused
    /// resume never gets here.
    fn try_next(&mut self) -> Result<Option<CommitOut>, Failure> {
        if self.ready.is_empty() {
            self.drain()?;
        }
        if self.ready.is_empty() {
            tokio::runtime::Handle::current().block_on(self.wait(GAP))?;
        }
        Ok(self.pop())
    }

    fn token(&self) -> Vec<u8> {
        self.token.clone()
    }
}

fn commit_of(c: proto::FeedCommit) -> CommitOut {
    use proto::ChangeKind as K;
    CommitOut {
        seqno: c.seqno,
        ts: c.timestamp,
        changes: c
            .changes
            .into_iter()
            .map(|ch| ChangeOut {
                kind: match K::try_from(ch.kind).unwrap_or(K::Unspecified) {
                    K::Put => "put",
                    K::Merge => "merge",
                    K::DeleteCell => "delete_cell",
                    K::DeleteColumn => "delete_column",
                    K::DeleteFamily => "delete_family",
                    _ => "unknown",
                },
                value: ch.value.as_ref().map(|v| value_of(Some(v), &[])),
                table: ch.table,
                family: ch.family,
                row: ch.row.to_vec(),
                qualifier: ch.qualifier.to_vec(),
                ts: ch.timestamp,
            })
            .collect(),
    }
}

/// The memory tools' store on a server (#1915 PR 3b): each operation a `Memory` op through
/// the router, as the session's token (so within its namespaces, M4), within the message's
/// deadline. The op takes its own memory place (M6), as the RPC does: the message holds only
/// its `/mcp` place while it waits, so `/mcp` messages can't hold every memory place while
/// their ops wait for one.
pub(super) struct ServeMemory {
    pub(super) backend: ServeBackend,
    /// The node's store slot, for its pinned model.
    pub(super) slot: Arc<super::memslot::MemSlot>,
}

impl ServeMemory {
    fn model_of(&self, entry: super::memslot::Entry) -> Result<Option<EmbeddingModel>, Failure> {
        use super::memslot::Entry;
        match entry {
            Entry::Core(core) => Ok(core.model().cloned()),
            Entry::Missing => Err(failure(ApiError::unavailable(format!(
                "memory store {} isn't on this replica yet: the primary hasn't created it, or \
                 the replica hasn't applied that yet",
                self.slot.name()
            )))),
            Entry::Failed(e) => Err(failure(e)),
        }
    }
}

fn memory_id(raw: &[u8]) -> Result<MemoryId, Failure> {
    super::memory::id(raw).map_err(failure)
}

fn memory_ids(raw: Vec<prost::bytes::Bytes>) -> Result<Vec<MemoryId>, Failure> {
    raw.iter().map(|b| memory_id(b)).collect()
}

fn id_bytes(ids: &[MemoryId]) -> Vec<prost::bytes::Bytes> {
    ids.iter().map(|id| id.to_bytes().into()).collect()
}

impl MemStore for ServeMemory {
    fn model(&self) -> Option<EmbeddingModel> {
        match self.slot.cached()? {
            super::memslot::Entry::Core(core) => core.model().cloned(),
            _ => None,
        }
    }

    fn call_model(&self) -> Result<Option<EmbeddingModel>, Failure> {
        // The node's own store, opened if need be, on this `/mcp` thread (never a pool
        // permit's): every partition's store is pinned by the same flags.
        self.model_of(self.slot.get().map_err(failure)?)
    }

    fn remember(&self, r: Remember, supersedes: Option<MemoryId>) -> Result<Stored, Failure> {
        let op = ServiceOp::Remember(proto::RememberRequest {
            namespace: r.namespace,
            memory: Some(proto::NewMemory {
                text: r.text,
                embedding: r.embedding.as_deref().map(super::memory::vector_out),
                kind: r.kind.map_or(0, super::memory::kind_out),
                importance: r.importance,
                source: r.source.unwrap_or_default(),
                session: r.session.unwrap_or_default(),
                pinned: r.pinned,
                links: r
                    .links
                    .into_iter()
                    .map(|(to, ty, weight)| proto::MemoryLink {
                        to: to.to_bytes().into(),
                        link_type: Some(super::memory::link_type_out(ty)),
                        weight,
                    })
                    .collect(),
            }),
            supersedes: supersedes
                .map(|id| id.to_bytes().into())
                .unwrap_or_default(),
        });
        let ServiceReply::Remember(done) = self.backend.call(op)?.reply else {
            return Err(unexpected("Memory.Remember"));
        };
        let outcome = match proto::RememberOutcome::try_from(done.outcome) {
            Ok(proto::RememberOutcome::New) => "new",
            Ok(proto::RememberOutcome::Merged) => "merged",
            Ok(proto::RememberOutcome::Similar) => "similar",
            _ => "other",
        };
        Ok(Stored {
            id: memory_id(&done.id)?,
            outcome,
            supersede_error: done
                .supersede_error
                .map(|e| Failure::served(&e.code, e.code_num, &e.message)),
        })
    }

    fn recall(&self, a: RecallArgs) -> Result<Recalled, Failure> {
        let op = ServiceOp::Recall(proto::RecallRequest {
            namespaces: a.namespaces,
            text: a.text.unwrap_or_default(),
            embedding: a.embedding.as_deref().map(super::memory::vector_out),
            k: u32::try_from(a.k).unwrap_or(u32::MAX),
            byte_budget: a.byte_budget,
            expand_hops: a.expand_hops,
            exact: a.exact,
            include_superseded: a.include_superseded,
            session: a.session.unwrap_or_default(),
            with_embeddings: a.with_embeddings,
            with_signals: a.with_signals,
        });
        let ServiceReply::Recall(r) = self.backend.call(op)?.reply else {
            return Err(unexpected("Memory.Recall"));
        };
        let hits = r
            .hits
            .into_iter()
            .map(|h| {
                Ok(Hit {
                    id: memory_id(&h.id)?,
                    kind: super::memory::kind_in(h.kind)
                        .map_err(failure)?
                        .unwrap_or_default(),
                    text: h.text,
                    importance: h.importance,
                    created_us: h.created,
                    score: h.score,
                    superseded_by: if h.superseded_by.is_empty() {
                        None
                    } else {
                        Some(memory_id(&h.superseded_by)?)
                    },
                })
            })
            .collect::<Result<_, Failure>>()?;
        Ok(Recalled {
            hits,
            truncated: r.truncated,
        })
    }

    fn mark_used(&self, ids: &[MemoryId]) -> Result<Marked, Failure> {
        let op = ServiceOp::MarkUsed(proto::MarkUsedRequest { ids: id_bytes(ids) });
        let ServiceReply::MarkUsed(m) = self.backend.call(op)?.reply else {
            return Err(unexpected("Memory.MarkUsed"));
        };
        Ok(Marked {
            marked: m.marked,
            missing: memory_ids(m.missing)?,
        })
    }

    fn supersede(&self, old: &MemoryId, new: &MemoryId) -> Result<(), Failure> {
        let op = ServiceOp::Supersede(proto::SupersedeRequest {
            old: old.to_bytes().into(),
            new: new.to_bytes().into(),
        });
        match self.backend.call(op)?.reply {
            ServiceReply::Empty(_) => Ok(()),
            _ => Err(unexpected("Memory.Supersede")),
        }
    }

    fn link(
        &self,
        from: &MemoryId,
        to: &MemoryId,
        ty: LinkType,
        weight: f32,
    ) -> Result<(), Failure> {
        let op = ServiceOp::Link(proto::LinkRequest {
            from: from.to_bytes().into(),
            to: to.to_bytes().into(),
            link_type: Some(super::memory::link_type_out(ty)),
            weight,
        });
        match self.backend.call(op)?.reply {
            ServiceReply::Empty(_) => Ok(()),
            _ => Err(unexpected("Memory.Link")),
        }
    }

    fn forget(&self, ids: &[MemoryId]) -> Result<Forgotten, Failure> {
        // A failure part-way is the call's error, as `Memory.Forget` answers it: on a cluster,
        // `OutcomeUnknown` naming the partitions whose outcome isn't known (#1939's rule),
        // never a partial list that reads as "the rest weren't forgotten".
        let op = ServiceOp::Forget(proto::ForgetRequest { ids: id_bytes(ids) });
        let ServiceReply::Forget(f) = self.backend.call(op)?.reply else {
            return Err(unexpected("Memory.Forget"));
        };
        Ok(Forgotten {
            deleted: f.forgotten,
            missing: memory_ids(f.missing)?,
            failed: None,
        })
    }
}
