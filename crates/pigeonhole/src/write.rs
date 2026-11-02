use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{FamilyId, PendingCommit, Predicate, TableId, Txn, ValueRef};
use pigeonhole_format::key::MAX_KEY_PART;
use pigeonhole_format::vector::{Params as FormatParams, Query, VectorRef as FormatVectorRef};

use crate::db::Db;
use crate::table::{Operand, TableCore};
use crate::{CellRef, Condition, Error, ErrorCode, Result, Table};

type EngineBatch = pigeonhole_engine::WriteBatch;

/// The outcome of a commit.
///
/// ```
/// use pigeonhole::{Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// let a = t.mutate(b"row").put("f", b"q", b"1").commit()?;
/// let b = t.mutate(b"row").put("f", b"q", b"2").durability(Durability::Buffered).commit()?;
/// assert!(b.seqno > a.seqno);
/// assert_eq!(a.durability, Durability::GroupSync);
/// assert_eq!(b.durability, Durability::Buffered);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitInfo {
    /// The commit's sequence number. For a transaction that wrote nothing it is the
    /// snapshot point it read at, not a seqno of its own: other commits may share it, and it
    /// is 0 on an empty database (ICR 0060).
    pub seqno: u64,
    /// The durability level actually applied.
    pub durability: Durability,
}

impl CommitInfo {
    pub(crate) fn from_engine(c: pigeonhole_engine::CommitInfo) -> Self {
        Self {
            seqno: c.seqno,
            durability: Durability::from_engine(c.durability),
        }
    }
}

/// How durable a commit must be before it returns. Ordered from weakest to strongest.
///
/// Resolution order: per-call override, then the writer default
/// ([`Options::durability`](crate::Options::durability)), then [`Durability::GroupSync`].
/// The [durability guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/durability.md)
/// has what each level survives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
#[non_exhaustive]
pub enum Durability {
    /// Memtable only. Survives nothing past the last flush.
    None = 0,
    /// Handed to the kernel with `write()`, no fsync. Survives a process crash.
    Buffered = 1,
    /// fsync shared with every committer in the same group. Survives power loss.
    #[default]
    GroupSync = 2,
    /// A dedicated fsync, never batched. Survives power loss.
    Sync = 3,
}

impl Durability {
    /// The engine's level (D282: the public type is this crate's own).
    pub(crate) fn engine(self) -> pigeonhole_format::Durability {
        match self {
            Durability::None => pigeonhole_format::Durability::None,
            Durability::Buffered => pigeonhole_format::Durability::Buffered,
            Durability::GroupSync => pigeonhole_format::Durability::GroupSync,
            Durability::Sync => pigeonhole_format::Durability::Sync,
        }
    }

    /// The public level of an engine level.
    pub(crate) fn from_engine(d: pigeonhole_format::Durability) -> Self {
        match d {
            pigeonhole_format::Durability::None => Durability::None,
            pigeonhole_format::Durability::Buffered => Durability::Buffered,
            pigeonhole_format::Durability::GroupSync => Durability::GroupSync,
            pigeonhole_format::Durability::Sync => Durability::Sync,
        }
    }
}

/// The lengths of one mutation's parts, for size errors.
#[derive(Debug, Clone, Copy)]
struct Sizes {
    row: usize,
    qualifier: usize,
    value: usize,
}

impl Sizes {
    fn new(row: &[u8], qualifier: &[u8], value: usize) -> Self {
        Self {
            row: row.len(),
            qualifier: qualifier.len(),
            value,
        }
    }
}

/// An engine error with the sizes and limits filled in for `KeyTooLarge` and
/// `ValueTooLarge` (the engine's variants carry neither).
fn size_error(e: pigeonhole_engine::Error, sizes: Sizes, max_value: usize) -> Error {
    use pigeonhole_engine::Error as E;
    match e {
        E::KeyTooLarge => {
            let (what, len) = if sizes.row > MAX_KEY_PART {
                ("row key", sizes.row)
            } else {
                ("qualifier", sizes.qualifier)
            };
            Error::new(
                ErrorCode::KeyTooLarge,
                format!("{what} of {len} bytes exceeds the limit of {MAX_KEY_PART} bytes"),
            )
        }
        E::ValueTooLarge => value_too_large(sizes.value, max_value),
        e => e.into(),
    }
}

fn value_too_large(len: usize, max_value: usize) -> Error {
    Error::new(
        ErrorCode::ValueTooLarge,
        format!(
            "value of {len} bytes exceeds the limit: {MAX_PUT_VALUE} bytes for a put, \
             {max_value} bytes for a merge operand (the smallest of the WAL segment payload, \
             64 MiB and half a shard's memtable arena; decision D16)"
        ),
    )
}

/// The longest put value: a blob pointer's length is a `u32` and covers the tag byte too.
const MAX_PUT_VALUE: usize = u32::MAX as usize - 1;

/// A checked vector's value: the family's element and dimension over `payload`.
fn vector_value<'a>(p: &FormatParams, payload: &'a [u8]) -> ValueRef<'a> {
    ValueRef::Vector(
        FormatVectorRef::new(p.element, p.dim, payload).expect("checked against the family"),
    )
}

/// An engine batch plus the first error met while building it (builders never fail; the
/// error surfaces at commit).
#[derive(Debug, Default)]
struct Builder {
    batch: EngineBatch,
    error: Option<Error>,
    /// The largest value added, for a `ValueTooLarge` message at commit.
    largest_value: usize,
}

impl Builder {
    /// Records `e` as the batch's error, unless an earlier call failed.
    fn fail(&mut self, e: Error) {
        if self.error.is_none() {
            self.error = Some(e);
        }
    }

    /// Runs `f` against the batch with `family` resolved, unless an earlier call failed. For
    /// a typed operand, a family that refuses it is refused
    /// ([`TableCore::operand_family_id`]).
    fn with(
        &mut self,
        table: &TableCore,
        family: &str,
        operand: Option<Operand>,
        sizes: Sizes,
        f: impl FnOnce(&mut EngineBatch, FamilyId) -> pigeonhole_engine::Result<()>,
    ) {
        if self.error.is_some() {
            return;
        }
        self.largest_value = self.largest_value.max(sizes.value);
        let max_value = table.db.max_value;
        let id = match operand {
            Some(o) => table.operand_family_id(family, o),
            None => table.family_id(family),
        };
        let r =
            id.and_then(|id| f(&mut self.batch, id).map_err(|e| size_error(e, sizes, max_value)));
        if let Err(e) = r {
            self.error = Some(e);
        }
    }

    fn row_delete(&mut self, table: &TableCore, row: &[u8]) {
        if self.error.is_none()
            && let Err(e) = self.batch.delete_row(table.info.id, row, None)
        {
            self.error = Some(size_error(e, Sizes::new(row, &[], 0), 0));
        }
    }

    /// Refuses a table handle from another database.
    fn check_table(&mut self, db: &Arc<Db>, table: &Table) -> bool {
        if self.error.is_none()
            && let Err(e) = check_table(db, table)
        {
            self.error = Some(e);
        }
        self.error.is_none()
    }

    /// Submits an async commit (D210): at once while its shard has room, else as a pending
    /// commit that submits once admitted.
    #[cfg(feature = "async")]
    fn submit_async(
        self,
        engine: &pigeonhole_engine::Engine,
        max_value: usize,
        durability: Option<Durability>,
    ) -> Result<Submitted> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let largest_value = self.largest_value;
        engine
            .submit_async(self.batch, durability.map(Durability::engine))
            .map(|pending| Submitted {
                pending,
                largest_value,
                max_value,
                watch: Default::default(),
            })
            .map_err(|e| commit_error(e, largest_value, max_value))
    }

    fn submit(
        self,
        engine: &pigeonhole_engine::Engine,
        max_value: usize,
        durability: Option<Durability>,
    ) -> Result<Submitted> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let largest_value = self.largest_value;
        engine
            .submit(self.batch, durability.map(Durability::engine))
            .map(|pending| Submitted {
                pending,
                largest_value,
                max_value,
                watch: Default::default(),
            })
            .map_err(|e| commit_error(e, largest_value, max_value))
    }

    /// Commits through `db`, reporting the first builder error instead if there was one.
    fn commit(
        self,
        engine: &pigeonhole_engine::Engine,
        max_value: usize,
        durability: Option<Durability>,
    ) -> Result<CommitInfo> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let largest = self.largest_value;
        engine
            .commit(self.batch, durability.map(Durability::engine))
            .map(CommitInfo::from_engine)
            .map_err(|e| commit_error(e, largest, max_value))
    }
}

/// A commit handed to the engine and not yet resolved, with what its error needs: the base
/// of async commits and of [`CommitTicket`].
#[derive(Debug)]
pub(crate) struct Submitted {
    pub(crate) pending: PendingCommit,
    largest_value: usize,
    max_value: usize,
    /// With `Options::auto_recover`: how to find the database again for a refusal by its
    /// failed engine (`Db::refused_later`, #966). Copied, no reference taken.
    watch: crate::db::Watch,
}

impl Submitted {
    /// The commit's result, as the sync `commit` reports it.
    pub(crate) fn outcome(
        &self,
        r: pigeonhole_engine::Result<pigeonhole_engine::CommitInfo>,
    ) -> Result<CommitInfo> {
        r.map(CommitInfo::from_engine).map_err(|e| {
            self.watch
                .refused(commit_error(e, self.largest_value, self.max_value))
        })
    }

    /// With `watch` (taken before the engine was chosen, [`Db::watch`]) for its failed
    /// engine's refusals (#966).
    pub(crate) fn watched(mut self, watch: crate::db::Watch) -> Self {
        self.watch = watch;
        self
    }
}

/// A submitted commit, to wait on now or check later ([`WriteBatch::commit_with_ticket`];
/// D196). The commit is already on its way: dropping the ticket does not roll it back, and
/// the commit lands or fails atomically either way. With the `async` feature a ticket is
/// also a future (`ticket.await`).
///
/// ```
/// use pigeonhole::{Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
/// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
/// let mut wb = db.write_batch();
/// wb.put(&t, b"r", "f", b"q", b"v");
/// let mut ticket = wb.commit_with_ticket(Durability::Buffered)?;
/// // Do other work, check without blocking, then wait.
/// let _maybe = ticket.seqno();
/// let info = ticket.wait()?;
/// assert!(info.seqno > 0);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "dropping a ticket does not cancel the commit, but its result is lost"]
pub struct CommitTicket {
    state: TicketState,
}

#[derive(Debug)]
pub(crate) enum TicketState {
    Pending(Submitted),
    Done(Result<CommitInfo>),
}

impl CommitTicket {
    fn new(submitted: Submitted) -> Self {
        Self {
            state: TicketState::Pending(submitted),
        }
    }

    /// Blocks until the commit meets its durability level and is visible, as
    /// [`WriteBatch::commit_with`] does, and returns its result.
    pub fn wait(self) -> Result<CommitInfo> {
        match self.state {
            TicketState::Done(r) => r,
            TicketState::Pending(s) => {
                let Submitted {
                    pending,
                    largest_value,
                    max_value,
                    watch,
                } = s;
                pending
                    .wait()
                    .map(CommitInfo::from_engine)
                    .map_err(|e| watch.refused(commit_error(e, largest_value, max_value)))
            }
        }
    }

    /// The commit's result if it has resolved (durable at its level and visible), without
    /// blocking; `None` while it is in flight. Once resolved, every call returns the same
    /// result.
    pub fn try_result(&mut self) -> Option<Result<CommitInfo>> {
        if let TicketState::Pending(s) = &mut self.state {
            let mut cx = Context::from_waker(Waker::noop());
            let done = match Pin::new(&mut s.pending).poll(&mut cx) {
                Poll::Ready(r) => s.outcome(r),
                Poll::Pending => return None,
            };
            self.state = TicketState::Done(done);
        }
        match &self.state {
            TicketState::Done(r) => Some(r.clone()),
            TicketState::Pending(_) => None,
        }
    }

    /// The commit's sequence number once it has resolved successfully (D196: a seqno is
    /// assigned by the owning shard after submission, so it is not known at once). It takes
    /// `&mut self` because it polls the commit, as [`try_result`](Self::try_result) does: a
    /// `None` may become `Some` on a later call, and never changes back.
    pub fn seqno(&mut self) -> Option<u64> {
        self.try_result()?.ok().map(|c| c.seqno)
    }

    #[cfg(feature = "async")]
    pub(crate) fn into_state(self) -> TicketState {
        self.state
    }
}

/// A commit's error: `ValueTooLarge` names the largest value of the commit.
fn commit_error(e: pigeonhole_engine::Error, largest_value: usize, max_value: usize) -> Error {
    match e {
        pigeonhole_engine::Error::ValueTooLarge => value_too_large(largest_value, max_value),
        e => e.into(),
    }
}

/// Refuses a table handle from another database.
fn check_table(db: &Arc<Db>, table: &Table) -> Result<()> {
    if Arc::ptr_eq(db, &table.core.db) {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::InvalidArgument,
            format!("table {:?} belongs to another database", table.name()),
        ))
    }
}

/// A single-row atomic mutation under construction. Every change commits all-or-nothing,
/// across families. Errors (unknown family, oversized key) surface at commit.
///
/// ```
/// use pigeonhole::{Condition, Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
/// let pages = db.table("pages")?
///     .family("meta", Family::default())
///     .family("links", Family::default())
///     .family("stats", Family::counter())
///     .create_if_missing()?;
/// pages
///     .mutate(b"com.example/a")
///     .put("meta", b"status", b"200")
///     .put("links", b"com.example/b", b"")
///     .incr("stats", b"hits", 1)
///     .durability(Durability::Buffered)
///     .commit()?;
///
/// // Compare-and-set: only the first claim wins.
/// let unclaimed = Condition::Absent { family: "meta".into(), qualifier: b"owner".to_vec() };
/// let first = pages.mutate(b"com.example/a").put("meta", b"owner", b"w1").commit_if(&unclaimed)?;
/// let second = pages.mutate(b"com.example/a").put("meta", b"owner", b"w2").commit_if(&unclaimed)?;
/// assert!(first.is_some() && second.is_none());
/// assert_eq!(pages.get(b"com.example/a", "meta", b"owner")?.unwrap().value(), b"w1");
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a mutation does nothing until .commit()"]
pub struct RowMutation<'t> {
    table: &'t TableCore,
    row: Vec<u8>,
    builder: Builder,
    durability: Option<Durability>,
}

impl<'t> RowMutation<'t> {
    pub(crate) fn new(table: &'t Table, row: &[u8]) -> Self {
        Self {
            table: &table.core,
            row: row.to_vec(),
            builder: Builder::default(),
            durability: None,
        }
    }

    /// Adds one mutation of this row in `family` (`qualifier` and `value` only size errors).
    fn op(
        self,
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId, &[u8]) -> pigeonhole_engine::Result<()>,
    ) -> Self {
        self.op_as(None, family, qualifier, value, f)
    }

    /// [`op`](Self::op) for a typed operand, refused by families that don't take it (D294).
    fn op_as(
        mut self,
        operand: Option<Operand>,
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId, &[u8]) -> pigeonhole_engine::Result<()>,
    ) -> Self {
        let (table, row) = (self.table.info.id, &self.row);
        let sizes = Sizes::new(row, qualifier, value);
        self.builder
            .with(self.table, family, operand, sizes, |b, fam| {
                f(b, table, fam, row)
            });
        self
    }
}

impl RowMutation<'_> {
    /// Puts bytes at the commit timestamp.
    pub fn put(self, family: &str, qualifier: &[u8], value: &[u8]) -> Self {
        self.op(family, qualifier, value.len(), |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::Bytes(value))
        })
    }

    /// Puts bytes at an explicit timestamp (event time).
    pub fn put_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[u8]) -> Self {
        self.op(family, qualifier, value.len(), |b, t, f, row| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::Bytes(value))
        })
    }

    /// Puts a typed `i64` at the commit timestamp; in a counter family, sets the counter
    /// (later increments add to it).
    pub fn put_i64(self, family: &str, qualifier: &[u8], value: i64) -> Self {
        self.op(family, qualifier, 8, |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::I64(value))
        })
    }

    /// Puts a typed `i64` at an explicit timestamp; in a counter family, sets that bucket.
    pub fn put_i64_at(self, family: &str, qualifier: &[u8], ts: u64, value: i64) -> Self {
        self.op(family, qualifier, 8, |b, t, f, row| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::I64(value))
        })
    }

    /// Puts a `u64` as the `i64` with the same bits, as a bitwise aggregate family holds it
    /// ([`Aggregation::BitOr`](crate::Aggregation::BitOr), D298); reads back with `as_u64`.
    pub fn put_u64(self, family: &str, qualifier: &[u8], value: u64) -> Self {
        self.put_i64(family, qualifier, value as i64)
    }

    /// [`put_u64`](Self::put_u64) at an explicit timestamp; in a bitwise family, sets that
    /// bucket.
    pub fn put_u64_at(self, family: &str, qualifier: &[u8], ts: u64, value: u64) -> Self {
        self.put_i64_at(family, qualifier, ts, value as i64)
    }

    /// Puts a typed `f64`.
    pub fn put_f64(self, family: &str, qualifier: &[u8], value: f64) -> Self {
        self.op(family, qualifier, 8, |b, t, f, row| {
            b.put(t, f, row, qualifier, None, ValueRef::F64(value))
        })
    }

    /// Puts a typed `f64` at an explicit timestamp (ICR 0051).
    pub fn put_f64_at(self, family: &str, qualifier: &[u8], ts: u64, value: f64) -> Self {
        self.op(family, qualifier, 8, |b, t, f, row| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::F64(value))
        })
    }

    /// Puts the `f32` vector `value` into a vector family (D303,
    /// [`VectorSpec`](crate::VectorSpec)) at the commit timestamp. A vector of another
    /// element type or dimension, an `f32` component that isn't finite or exceeds 2^56 in
    /// magnitude, a vector with a zero norm in a cosine
    /// family, or a family that isn't a vector family fails with `InvalidArgument`.
    pub fn put_vector_f32(self, family: &str, qualifier: &[u8], value: &[f32]) -> Self {
        self.vector(family, qualifier, None, Query::F32(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) at an explicit timestamp.
    pub fn put_vector_f32_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[f32]) -> Self {
        self.vector(family, qualifier, Some(ts), Query::F32(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) for an `i8` vector family.
    pub fn put_vector_i8(self, family: &str, qualifier: &[u8], value: &[i8]) -> Self {
        self.vector(family, qualifier, None, Query::I8(value))
    }

    /// [`put_vector_i8`](Self::put_vector_i8) at an explicit timestamp.
    pub fn put_vector_i8_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[i8]) -> Self {
        self.vector(family, qualifier, Some(ts), Query::I8(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) for a bit vector family: `value` is the bits,
    /// packed eight to a byte, least significant first.
    pub fn put_vector_bits(self, family: &str, qualifier: &[u8], value: &[u8]) -> Self {
        self.vector(family, qualifier, None, Query::Bits(value))
    }

    /// [`put_vector_bits`](Self::put_vector_bits) at an explicit timestamp.
    pub fn put_vector_bits_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[u8]) -> Self {
        self.vector(family, qualifier, Some(ts), Query::Bits(value))
    }

    /// Puts the `f16` vector `value` (IEEE binary16 bit patterns) into an `f16` vector
    /// family (#1263) at the commit timestamp, refused as
    /// [`put_vector_f32`](Self::put_vector_f32) is. An `f32` vector can go into the same
    /// family through `put_vector_f32`, which rounds it to nearest-even.
    pub fn put_vector_f16(self, family: &str, qualifier: &[u8], value: &[u16]) -> Self {
        self.vector(family, qualifier, None, Query::F16(value))
    }

    /// [`put_vector_f16`](Self::put_vector_f16) at an explicit timestamp.
    pub fn put_vector_f16_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[u16]) -> Self {
        self.vector(family, qualifier, Some(ts), Query::F16(value))
    }

    /// [`put_vector_f16`](Self::put_vector_f16) for a `bf16` vector family (bfloat16 bit
    /// patterns).
    pub fn put_vector_bf16(self, family: &str, qualifier: &[u8], value: &[u16]) -> Self {
        self.vector(family, qualifier, None, Query::BF16(value))
    }

    /// [`put_vector_bf16`](Self::put_vector_bf16) at an explicit timestamp.
    pub fn put_vector_bf16_at(
        self,
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u16],
    ) -> Self {
        self.vector(family, qualifier, Some(ts), Query::BF16(value))
    }

    /// A vector put, checked against the family's spec when it is built.
    fn vector(mut self, family: &str, qualifier: &[u8], ts: Option<u64>, q: Query<'_>) -> Self {
        let mut buf = Vec::new();
        match self.table.vector_payload(family, q, &mut buf) {
            Err(e) => {
                self.builder.fail(e);
                self
            }
            Ok(p) => self.op(family, qualifier, buf.len(), |b, t, f, row| {
                b.put(t, f, row, qualifier, ts, vector_value(&p, &buf))
            }),
        }
    }

    /// Atomically adds `delta` (wrapping) to the counter in a counter family
    /// ([`Family::counter`](crate::Family::counter)) without reading it. Increments of one
    /// counter combine into one cell. Within one mutation (or batch) the writes of a counter
    /// apply in order: `incr(c, 1).incr(c, 2)` adds 3, and `put_i64(c, 5).incr(c, 1)` sets
    /// 6 (decision D186). In a counter family it is [`aggregate`](Self::aggregate). Other
    /// families fail with `InvalidArgument` at commit, min and max families included.
    pub fn incr(self, family: &str, qualifier: &[u8], delta: i64) -> Self {
        self.op_as(Some(Operand::Incr), family, qualifier, 8, |b, t, f, row| {
            b.merge(t, f, row, qualifier, ValueRef::I64(delta))
        })
    }

    /// Adds `delta` to the bucket at timestamp `ts` of a counter family (hourly or daily
    /// totals, say): each bucket is a version of its own.
    pub fn incr_at(self, family: &str, qualifier: &[u8], ts: u64, delta: i64) -> Self {
        self.op_as(Some(Operand::Incr), family, qualifier, 8, |b, t, f, row| {
            b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(delta))
        })
    }

    /// Writes the input `value` to the cell of an aggregate family
    /// ([`Family::aggregate`](crate::Family::aggregate)) without reading it: the cell
    /// combines it with the family's aggregation (added to a sum, kept by a min or max if it
    /// is the smallest or largest, counted as an element by an HLL family). The cell lives
    /// at one fixed timestamp (0), as a
    /// counter's does. Within one mutation (or batch) the writes of a cell apply in order:
    /// `put_i64(c, 5).aggregate(c, 9)` in a min family sets 5. Other families fail with
    /// `InvalidArgument` at commit.
    pub fn aggregate(self, family: &str, qualifier: &[u8], value: i64) -> Self {
        self.op_as(
            Some(Operand::Aggregate),
            family,
            qualifier,
            8,
            |b, t, f, row| b.merge(t, f, row, qualifier, ValueRef::I64(value)),
        )
    }

    /// Writes the input `value` to the bucket at timestamp `ts` of an aggregate family (an
    /// hourly minimum, say): each bucket is a version of its own.
    pub fn aggregate_at(self, family: &str, qualifier: &[u8], ts: u64, value: i64) -> Self {
        self.op_as(
            Some(Operand::Aggregate),
            family,
            qualifier,
            8,
            |b, t, f, row| b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(value)),
        )
    }

    /// Writes the `u64` input `value` to the cell of a bitwise aggregate family
    /// ([`Aggregation::BitOr`](crate::Aggregation::BitOr), `BitAnd`, `BitXor`; D298), stored
    /// as an `i64` with the same bits. Other families fail with `InvalidArgument`, naming the
    /// method their inputs take.
    pub fn aggregate_u64(self, family: &str, qualifier: &[u8], value: u64) -> Self {
        self.op_as(Some(Operand::U64), family, qualifier, 8, |b, t, f, row| {
            b.merge(t, f, row, qualifier, ValueRef::I64(value as i64))
        })
    }

    /// [`aggregate_u64`](Self::aggregate_u64) to the bucket at timestamp `ts`.
    pub fn aggregate_u64_at(self, family: &str, qualifier: &[u8], ts: u64, value: u64) -> Self {
        self.op_as(Some(Operand::U64), family, qualifier, 8, |b, t, f, row| {
            b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(value as i64))
        })
    }

    /// Writes the `f64` input `value` to the cell of an `f64` aggregate family
    /// ([`Aggregation::MinF64`](crate::Aggregation::MinF64), `MaxF64`; D298). NaN, and other
    /// families, fail with `InvalidArgument`.
    pub fn aggregate_f64(self, family: &str, qualifier: &[u8], value: f64) -> Self {
        self.op_as(Some(Operand::F64), family, qualifier, 8, |b, t, f, row| {
            b.merge(t, f, row, qualifier, ValueRef::F64(value))
        })
    }

    /// [`aggregate_f64`](Self::aggregate_f64) to the bucket at timestamp `ts`.
    pub fn aggregate_f64_at(self, family: &str, qualifier: &[u8], ts: u64, value: f64) -> Self {
        self.op_as(Some(Operand::F64), family, qualifier, 8, |b, t, f, row| {
            b.merge_at(t, f, row, qualifier, ts, ValueRef::F64(value))
        })
    }

    /// Writes the bytes element `value` (a string key, say) to the cell of an HLL or bloom
    /// family ([`Aggregation::Hll`](crate::Aggregation::Hll),
    /// [`Aggregation::Bloom`](crate::Aggregation::Bloom); D298). It hashes apart from any
    /// `i64` element, behind a domain tag. Other families fail with `InvalidArgument`.
    pub fn aggregate_bytes(self, family: &str, qualifier: &[u8], value: &[u8]) -> Self {
        self.op_as(
            Some(Operand::Bytes),
            family,
            qualifier,
            value.len(),
            |b, t, f, row| b.merge(t, f, row, qualifier, ValueRef::Bytes(value)),
        )
    }

    /// [`aggregate_bytes`](Self::aggregate_bytes) to the bucket at timestamp `ts`.
    pub fn aggregate_bytes_at(self, family: &str, qualifier: &[u8], ts: u64, value: &[u8]) -> Self {
        self.op_as(
            Some(Operand::Bytes),
            family,
            qualifier,
            value.len(),
            |b, t, f, row| b.merge_at(t, f, row, qualifier, ts, ValueRef::Bytes(value)),
        )
    }

    /// Writes an untyped operand for the family's merge operator (custom operators, Phase 2).
    /// The built-in `i64` add takes typed operands only: use `incr`, or the read fails with
    /// [`ErrorCode::MergeFailed`](crate::ErrorCode::MergeFailed).
    pub fn merge(self, family: &str, qualifier: &[u8], operand: &[u8]) -> Self {
        self.op(family, qualifier, operand.len(), |b, t, f, row| {
            b.merge(t, f, row, qualifier, ValueRef::Bytes(operand))
        })
    }

    /// Deletes one version.
    pub fn delete_cell(self, family: &str, qualifier: &[u8], ts: u64) -> Self {
        self.op(family, qualifier, 0, |b, t, f, row| {
            b.delete_cell(t, f, row, qualifier, ts)
        })
    }

    /// Deletes every version of a column.
    pub fn delete_column(self, family: &str, qualifier: &[u8]) -> Self {
        self.op(family, qualifier, 0, |b, t, f, row| {
            b.delete_column(t, f, row, qualifier, None)
        })
    }

    /// Deletes every column of a family in this row.
    pub fn delete_family(self, family: &str) -> Self {
        self.op(family, &[], 0, |b, t, f, row| {
            b.delete_family(t, f, row, None)
        })
    }

    /// Deletes the whole row.
    pub fn delete_row(mut self) -> Self {
        self.builder.row_delete(self.table, &self.row);
        self
    }

    /// Overrides durability for this commit.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = Some(durability);
        self
    }

    /// Commits.
    pub fn commit(self) -> Result<CommitInfo> {
        let RowMutation {
            table,
            builder,
            durability,
            ..
        } = self;
        // A recovery running: wait for its swap, then commit on the new engine (D265).
        let generation = table.db.before_commit()?;
        builder
            .commit(&table.engine(), table.db.max_value, durability)
            .map_err(|e| table.db.after_commit_closed(generation, e))
    }

    /// Submits as an async commit (D210; see [`Builder::submit_async`]).
    #[cfg(feature = "async")]
    pub(crate) fn submit_async(self) -> Result<Submitted> {
        let watch = self.table.db.watch();
        self.builder
            .submit_async(
                &self.table.engine(),
                self.table.db.max_value,
                self.durability,
            )
            .map(|s| s.watched(watch))
            // A refusal at submit (the failed or closing engine's `Closed`) as one at resolution
            // (#970).
            .map_err(|e| watch.refused(e))
    }

    /// Submits as an async conditional commit (D210): the condition is resolved now; the
    /// pending check submits once admitted when it has to wait for room.
    #[cfg(feature = "async")]
    pub(crate) fn submit_if_async(mut self, condition: &Condition) -> Result<SubmittedCheck> {
        if let Some(e) = self.builder.error.take() {
            return Err(e);
        }
        let predicate = self.predicate(condition)?;
        let (db, table) = (&self.table.db, self.table.info.id);
        let (largest_value, max_value) = (self.builder.largest_value, db.max_value);
        let durability = self.durability;
        let watch = db.watch();
        self.table
            .engine()
            .submit_check_and_mutate_async(
                table,
                &self.row,
                &predicate,
                self.builder.batch,
                durability.map(Durability::engine),
            )
            .map(|pending| SubmittedCheck {
                pending,
                largest_value,
                max_value,
                watch,
            })
            .map_err(|e| watch.refused(commit_error(e, largest_value, max_value)))
    }

    /// Commits only if `condition` holds on this row, atomically (BigTable's
    /// `check_and_mutate`; Phase 2). Returns `None` if the condition failed.
    pub fn commit_if(mut self, condition: &Condition) -> Result<Option<CommitInfo>> {
        if let Some(e) = self.builder.error.take() {
            return Err(e);
        }
        let predicate = self.predicate(condition)?;
        let db = &self.table.db;
        let largest = self.builder.largest_value;
        let generation = db.before_commit()?;
        let (applied, info) = self
            .table
            .engine()
            .check_and_mutate(
                self.table.info.id,
                &self.row,
                &predicate,
                self.builder.batch,
                self.durability.map(Durability::engine),
            )
            .map_err(|e| {
                db.after_commit_closed(generation, commit_error(e, largest, db.max_value))
            })?;
        Ok(if applied {
            info.map(CommitInfo::from_engine)
        } else {
            None
        })
    }

    /// The engine's form of `condition`, on this row's table.
    fn predicate(&self, condition: &Condition) -> Result<Predicate> {
        Ok(match condition {
            Condition::Exists { family, qualifier } => Predicate::Exists {
                family: self.table.family_id(family)?,
                qualifier: qualifier.clone(),
            },
            Condition::Absent { family, qualifier } => Predicate::Absent {
                family: self.table.family_id(family)?,
                qualifier: qualifier.clone(),
            },
            Condition::Value {
                family,
                qualifier,
                filter,
            } => Predicate::Value {
                family: self.table.family_id(family)?,
                qualifier: qualifier.clone(),
                predicate: filter.to_engine(),
            },
        })
    }
}

/// A submitted [`RowMutation::commit_if`] (the async front door), with what its error
/// mapping needs.
#[cfg(feature = "async")]
#[derive(Debug)]
pub(crate) struct SubmittedCheck {
    pub(crate) pending: pigeonhole_engine::PendingCheck,
    largest_value: usize,
    max_value: usize,
    /// As [`Submitted`]'s.
    watch: crate::db::Watch,
}

#[cfg(feature = "async")]
impl SubmittedCheck {
    /// The public result of the engine's outcome: `None` when the condition failed.
    pub(crate) fn outcome(
        &self,
        r: pigeonhole_engine::Result<(bool, Option<pigeonhole_engine::CommitInfo>)>,
    ) -> Result<Option<CommitInfo>> {
        let (applied, info) = r.map_err(|e| {
            self.watch
                .refused(commit_error(e, self.largest_value, self.max_value))
        })?;
        Ok(if applied {
            info.map(CommitInfo::from_engine)
        } else {
            None
        })
    }
}

/// A multi-row write with one durability point. Atomic across rows (two-phase commit when the
/// rows live on different shards). Errors surface at commit.
///
/// ```
/// use pigeonhole::{Durability, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
/// let g = db.table("g")?
///     .family("out", Family::default())
///     .family("in", Family::default())
///     .family("degree", Family::counter())
///     .create_if_missing()?;
/// // Add the edge a -> b in both directions, atomically.
/// let mut wb = db.write_batch();
/// wb.put(&g, b"node:a", "out", b"node:b", b"")
///     .put(&g, b"node:b", "in", b"node:a", b"")
///     .incr(&g, b"node:a", "degree", b"out", 1);
/// assert_eq!(wb.len(), 3);
/// let info = wb.commit_with(Durability::Sync)?;
/// assert_eq!(info.durability, Durability::Sync);
/// assert!(g.get(b"node:b", "in", b"node:a")?.is_some());
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct WriteBatch {
    db: Arc<Db>,
    cached: crate::db::Cached,
    builder: Builder,
}

impl WriteBatch {
    pub(crate) fn new(db: Arc<Db>, cached: crate::db::Cached) -> Self {
        Self {
            db,
            cached,
            builder: Builder::default(),
        }
    }

    /// Adds one mutation of `row` in `table`'s `family` (`qualifier` and `value` only size
    /// errors).
    fn op(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId) -> pigeonhole_engine::Result<()>,
    ) -> &mut Self {
        self.op_as(None, table, row, family, qualifier, value, f)
    }

    /// [`op`](Self::op) for a typed operand, refused by families that don't take it (D294).
    #[allow(clippy::too_many_arguments)]
    fn op_as(
        &mut self,
        operand: Option<Operand>,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId) -> pigeonhole_engine::Result<()>,
    ) -> &mut Self {
        if self.builder.check_table(&self.db, table) {
            let id = table.core.info.id;
            let sizes = Sizes::new(row, qualifier, value);
            self.builder
                .with(&table.core, family, operand, sizes, |b, fam| f(b, id, fam));
        }
        self
    }

    /// Puts bytes at the commit timestamp.
    pub fn put(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, value.len(), |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::Bytes(value))
        })
    }

    /// Puts bytes at an explicit timestamp.
    pub fn put_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, value.len(), |b, t, f| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::Bytes(value))
        })
    }

    /// Puts a typed `i64` (in a counter family, sets the counter).
    pub fn put_i64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: i64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::I64(value))
        })
    }

    /// Puts a typed `i64` at an explicit timestamp (in a counter family, sets that bucket).
    pub fn put_i64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: i64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::I64(value))
        })
    }

    /// [`RowMutation::put_u64`]: a `u64`'s bits as an `i64`.
    pub fn put_u64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: u64,
    ) -> &mut Self {
        self.put_i64(table, row, family, qualifier, value as i64)
    }

    /// [`RowMutation::put_u64_at`].
    pub fn put_u64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: u64,
    ) -> &mut Self {
        self.put_i64_at(table, row, family, qualifier, ts, value as i64)
    }

    /// Puts a typed `f64`.
    pub fn put_f64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: f64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::F64(value))
        })
    }

    /// Puts a typed `f64` at an explicit timestamp (ICR 0051).
    pub fn put_f64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: f64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::F64(value))
        })
    }

    /// Puts the `f32` vector `value` into a vector family (D303), as
    /// [`RowMutation::put_vector_f32`] does.
    pub fn put_vector_f32(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[f32],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::F32(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) at an explicit timestamp.
    pub fn put_vector_f32_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[f32],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::F32(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) for an `i8` vector family.
    pub fn put_vector_i8(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[i8],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::I8(value))
    }

    /// [`put_vector_i8`](Self::put_vector_i8) at an explicit timestamp.
    pub fn put_vector_i8_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[i8],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::I8(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) for a bit vector family (bits packed eight to
    /// a byte, least significant first).
    pub fn put_vector_bits(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::Bits(value))
    }

    /// [`put_vector_bits`](Self::put_vector_bits) at an explicit timestamp.
    pub fn put_vector_bits_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u8],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::Bits(value))
    }

    /// Puts a vector into an `f16` vector family (IEEE binary16 bit patterns) (#1263), as [`RowMutation::put_vector_f16`] does.
    pub fn put_vector_f16(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u16],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::F16(value))
    }

    /// [`put_vector_f16`](Self::put_vector_f16) at an explicit timestamp.
    pub fn put_vector_f16_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u16],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::F16(value))
    }

    /// Puts a vector into a `bf16` vector family (bfloat16 bit patterns) (#1263), as [`RowMutation::put_vector_bf16`] does.
    pub fn put_vector_bf16(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u16],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::BF16(value))
    }

    /// [`put_vector_bf16`](Self::put_vector_bf16) at an explicit timestamp.
    pub fn put_vector_bf16_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u16],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::BF16(value))
    }

    /// A vector put, checked against the family's spec when it is built.
    fn vector(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: Option<u64>,
        q: Query<'_>,
    ) -> &mut Self {
        let mut buf = Vec::new();
        match table.core.vector_payload(family, q, &mut buf) {
            Err(e) => {
                self.builder.fail(e);
                self
            }
            Ok(p) => self.op(table, row, family, qualifier, buf.len(), |b, t, f| {
                b.put(t, f, row, qualifier, ts, vector_value(&p, &buf))
            }),
        }
    }

    /// Adds to the counter of a counter family (see [`RowMutation::incr`]).
    pub fn incr(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        delta: i64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Incr),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::I64(delta)),
        )
    }

    /// Adds to the bucket at `ts` of a counter family (see [`RowMutation::incr_at`]).
    pub fn incr_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        delta: i64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Incr),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(delta)),
        )
    }

    /// Writes an input to the cell of an aggregate family (see
    /// [`RowMutation::aggregate`]).
    pub fn aggregate(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: i64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Aggregate),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::I64(value)),
        )
    }

    /// Writes an input to the bucket at `ts` of an aggregate family (see
    /// [`RowMutation::aggregate_at`]).
    pub fn aggregate_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: i64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Aggregate),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(value)),
        )
    }

    /// [`RowMutation::aggregate_u64`]: a `u64` input to a bitwise aggregate family.
    pub fn aggregate_u64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: u64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::U64),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::I64(value as i64)),
        )
    }

    /// [`RowMutation::aggregate_u64_at`].
    pub fn aggregate_u64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: u64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::U64),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(value as i64)),
        )
    }

    /// [`RowMutation::aggregate_f64`]: an `f64` input to an `f64` aggregate family.
    pub fn aggregate_f64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: f64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::F64),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::F64(value)),
        )
    }

    /// [`RowMutation::aggregate_f64_at`].
    pub fn aggregate_f64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: f64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::F64),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::F64(value)),
        )
    }

    /// [`RowMutation::aggregate_bytes`]: a bytes element for an HLL or bloom family.
    pub fn aggregate_bytes(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Bytes),
            table,
            row,
            family,
            qualifier,
            value.len(),
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::Bytes(value)),
        )
    }

    /// [`RowMutation::aggregate_bytes_at`].
    pub fn aggregate_bytes_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u8],
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Bytes),
            table,
            row,
            family,
            qualifier,
            value.len(),
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::Bytes(value)),
        )
    }

    /// Writes an untyped operand for the family's merge operator (custom operators, Phase 2).
    /// The built-in `i64` add takes typed operands only: use `incr`, or the read fails with
    /// [`ErrorCode::MergeFailed`](crate::ErrorCode::MergeFailed).
    pub fn merge(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        operand: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, operand.len(), |b, t, f| {
            b.merge(t, f, row, qualifier, ValueRef::Bytes(operand))
        })
    }

    /// Deletes one version.
    pub fn delete_cell(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 0, |b, t, f| {
            b.delete_cell(t, f, row, qualifier, ts)
        })
    }

    /// Deletes every version of a column.
    pub fn delete_column(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 0, |b, t, f| {
            b.delete_column(t, f, row, qualifier, None)
        })
    }

    /// Deletes every column of a family in a row.
    pub fn delete_family(&mut self, table: &Table, row: &[u8], family: &str) -> &mut Self {
        self.op(table, row, family, &[], 0, |b, t, f| {
            b.delete_family(t, f, row, None)
        })
    }

    /// Deletes a whole row.
    pub fn delete_row(&mut self, table: &Table, row: &[u8]) -> &mut Self {
        if self.builder.check_table(&self.db, table) {
            self.builder.row_delete(&table.core, row);
        }
        self
    }

    /// Mutations so far.
    pub fn len(&self) -> usize {
        self.builder.batch.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.builder.batch.is_empty()
    }

    /// Commits with the writer default durability.
    pub fn commit(self) -> Result<CommitInfo> {
        let Self {
            db,
            cached,
            builder,
        } = self;
        let generation = db.before_commit()?;
        builder
            .commit(&cached.get(&db), db.max_value, None)
            .map_err(|e| db.after_commit_closed(generation, e))
    }

    /// Commits with `durability` for this commit only.
    pub fn commit_with(self, durability: Durability) -> Result<CommitInfo> {
        let Self {
            db,
            cached,
            builder,
        } = self;
        let generation = db.before_commit()?;
        builder
            .commit(&cached.get(&db), db.max_value, Some(durability))
            .map_err(|e| db.after_commit_closed(generation, e))
    }

    /// Submits with `durability` and returns at once with a [`CommitTicket`] to wait on or
    /// check later (D196). Fails here only if the batch could not be submitted (a builder
    /// error, a closed database); the commit's own outcome comes through the ticket.
    pub fn commit_with_ticket(self, durability: Durability) -> Result<CommitTicket> {
        self.submit(Some(durability)).map(CommitTicket::new)
    }

    /// Submits without waiting (the async front door's commit).
    pub(crate) fn submit(self, durability: Option<Durability>) -> Result<Submitted> {
        let Self {
            db,
            cached,
            builder,
        } = self;
        let generation = db.before_commit()?;
        let watch = db.watch_at(generation);
        builder
            .submit(&cached.get(&db), db.max_value, durability)
            .map(|s| s.watched(watch))
            .map_err(|e| db.after_commit_closed(generation, e))
    }

    /// Submits as an async commit (D210; see [`Builder::submit_async`]).
    #[cfg(feature = "async")]
    pub(crate) fn submit_async(self, durability: Option<Durability>) -> Result<Submitted> {
        let Self {
            db,
            cached,
            builder,
        } = self;
        let watch = db.watch();
        builder
            .submit_async(&cached.get(&db), db.max_value, durability)
            .map(|s| s.watched(watch))
            .map_err(|e| watch.refused(e))
    }
}

/// An optimistic multi-row transaction (Phase 4): serializable for the ranges it reads.
///
/// ```
/// use pigeonhole::{ErrorCode, Family, Options, Pigeonhole};
///
/// # fn main() -> pigeonhole::Result<()> {
/// # let dir = pigeonhole::doc_support::temp_dir();
/// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
/// let bank = db.table("bank")?.family("acct", Family::default()).create_if_missing()?;
/// bank.mutate(b"alice").put("acct", b"balance", b"10").commit()?;
///
/// let mut txn = db.transaction()?;
/// let balance = txn.get(&bank, b"alice", "acct", b"balance")?.map(|c| c.to_owned());
/// assert_eq!(balance.unwrap().value(), b"10");
/// txn.put(&bank, b"alice", "acct", b"balance", b"5")
///     .put(&bank, b"bob", "acct", b"balance", b"5");
///
/// // A concurrent write to what the transaction read makes it conflict.
/// bank.mutate(b"alice").put("acct", b"balance", b"11").commit()?;
/// assert_eq!(txn.commit().unwrap_err().code(), ErrorCode::Conflict);
/// # db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Transaction {
    pub(crate) db: Arc<Db>,
    txn: Txn,
    error: Option<Error>,
    largest_value: usize,
    /// The engine generation it began in (D265): its reads and its commit fail with
    /// `SnapshotExpired` once the database recovered onto a newer engine.
    generation: u64,
    /// The first error one of its own reads failed with: how `Pigeonhole::transact` tells
    /// a retryable failure from the application giving up (ICR 0060).
    failed_read: Option<Error>,
}

impl Transaction {
    pub(crate) fn new(db: Arc<Db>, txn: Txn, generation: u64) -> Self {
        Self {
            db,
            txn,
            error: None,
            largest_value: 0,
            generation,
            failed_read: None,
        }
    }

    /// Buffers one mutation of `row` in `table`'s `family`.
    fn op(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId) -> pigeonhole_engine::Result<()>,
    ) -> &mut Self {
        self.op_as(None, table, row, family, qualifier, value, f)
    }

    /// [`op`](Self::op) for a typed operand, refused by families that don't take it (D294).
    #[allow(clippy::too_many_arguments)]
    fn op_as(
        &mut self,
        operand: Option<Operand>,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: usize,
        f: impl FnOnce(&mut EngineBatch, TableId, FamilyId) -> pigeonhole_engine::Result<()>,
    ) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        self.largest_value = self.largest_value.max(value);
        let sizes = Sizes::new(row, qualifier, value);
        let max_value = self.db.max_value;
        let r = check_table(&self.db, table).and_then(|()| {
            let fam = match operand {
                Some(o) => table.core.operand_family_id(family, o)?,
                None => table.core.family_id(family)?,
            };
            f(self.txn.batch(), table.core.info.id, fam)
                .map_err(|e| size_error(e, sizes, max_value))
        });
        if let Err(e) = r {
            self.error = Some(e);
        }
        self
    }

    /// Submits as an async commit (D210): at once while its shard has room, else as a
    /// pending commit that submits the transaction once admitted.
    #[cfg(feature = "async")]
    pub(crate) fn submit_async(self, durability: Option<Durability>) -> Result<Submitted> {
        if let Some(e) = self.error {
            return Err(e);
        }
        self.db.check_generation(self.generation)?;
        let (largest_value, max_value) = (self.largest_value, self.db.max_value);
        // The transaction runs on the engine it began on: its generation.
        let watch = self.db.watch_at(self.generation).transaction();
        self.txn
            .submit_async(durability.map(Durability::engine))
            .map(|pending| Submitted {
                pending,
                largest_value,
                max_value,
                watch: Default::default(),
            })
            .map(|s| s.watched(watch))
            .map_err(|e| watch.refused(commit_error(e, largest_value, max_value)))
    }

    fn finish(self, durability: Option<Durability>) -> Result<CommitInfo> {
        if let Some(e) = self.error {
            return Err(e);
        }
        self.db.check_generation(self.generation)?;
        let (largest, max) = (self.largest_value, self.db.max_value);
        // A transaction's engine is the one it began on: a recovery running means its
        // snapshot expires with the swap, so no wait and no retry, only the error's name.
        let (db, generation) = (Arc::clone(&self.db), self.generation);
        let result = self
            .txn
            .commit(durability.map(Durability::engine))
            .map(CommitInfo::from_engine)
            .map_err(|e| {
                db.after_transaction_commit_closed(generation, commit_error(e, largest, max))
            });
        db.txn_counts.count(&result);
        result
    }

    /// Reads a cell at the transaction's snapshot and records the read: a commit that writes
    /// this cell, or deletes its family or row, makes the transaction conflict; one that writes
    /// another cell of the row doesn't (D275).
    pub fn get(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> Result<Option<CellRef<'_>>> {
        let read = (|| {
            check_table(&self.db, table)?;
            self.db.check_generation(self.generation)?;
            let family = table.core.family_id_at(self.txn.snapshot(), family)?;
            Ok(self.txn.get(table.core.info.id, family, row, qualifier)?)
        })();
        match read {
            Ok(cell) => Ok(cell.map(CellRef::owned)),
            Err(e) => {
                let e: Error = e;
                if self.failed_read.is_none() {
                    self.failed_read = Some(e.clone());
                }
                Err(e)
            }
        }
    }

    /// The first error one of this transaction's reads failed with.
    pub(crate) fn failed_read(&self) -> Option<Error> {
        self.failed_read.clone()
    }

    /// Reads a row at the transaction's snapshot (the newest version of each column) and
    /// records a read of each family it covers: `families`, or every family of the table
    /// when empty (ICR 0062). A family the row has no cell in still counts as read, so a
    /// commit that adds one conflicts. `None` if the row has no visible cell.
    pub fn get_row(
        &mut self,
        table: &Table,
        row: &[u8],
        families: &[&str],
    ) -> Result<Option<crate::RowRef<'_>>> {
        let read = (|| {
            check_table(&self.db, table)?;
            self.db.check_generation(self.generation)?;
            // The families as the transaction's snapshot saw them, which name its cells (#892).
            let snapshot = self.txn.snapshot();
            let ids = families
                .iter()
                .map(|f| table.core.family_id_at(snapshot, f))
                .collect::<Result<crate::read::FamilyIds>>()?;
            let mut spec = pigeonhole_engine::ReadSpec::default();
            spec.versions = 1;
            let mut buf = crate::cell::RowBuf::new(table.core.info_at(snapshot), 0);
            let any = self
                .txn
                .read_row_into(table.core.info.id, row, &ids, &spec, &mut buf)?;
            Ok((any, buf))
        })();
        match read {
            Ok((false, _)) => Ok(None),
            Ok((true, mut buf)) => {
                buf.key = row.to_vec();
                Ok(Some(crate::RowRef::owned(buf)))
            }
            Err(e) => {
                let e: Error = e;
                if self.failed_read.is_none() {
                    self.failed_read = Some(e.clone());
                }
                Err(e)
            }
        }
    }

    /// [`get`](Transaction::get) as a future resolving to an owned [`Cell`](crate::Cell): the
    /// read is recorded at the call (so it counts at commit even if the future is dropped
    /// unpolled), then made at the transaction's snapshot as
    /// [`Table::get_async`](crate::Table::get_async) does.
    #[cfg(feature = "async")]
    pub fn get_async(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> crate::nonblocking::GetFuture {
        let started = check_table(&self.db, table)
            .and_then(|()| self.db.check_generation(self.generation))
            .and_then(|()| table.core.family_id_at(self.txn.snapshot(), family))
            .map(|family| {
                self.txn
                    .get_async(table.core.info.id, family, row, qualifier)
            });
        crate::nonblocking::GetFuture::new(started)
    }

    /// Buffers a put.
    pub fn put(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, value.len(), |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::Bytes(value))
        })
    }

    /// Buffers a column delete.
    pub fn delete_column(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 0, |b, t, f| {
            b.delete_column(t, f, row, qualifier, None)
        })
    }

    /// Buffers a put at an explicit timestamp (ICR 0062; see [`RowMutation::put_at`]).
    pub fn put_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, value.len(), |b, t, f| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::Bytes(value))
        })
    }

    /// Buffers a typed `i64` put (in a counter family, it sets the counter).
    pub fn put_i64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: i64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::I64(value))
        })
    }

    /// Buffers a [`RowMutation::put_u64`]: a `u64`'s bits as an `i64`.
    pub fn put_u64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: u64,
    ) -> &mut Self {
        self.put_i64(table, row, family, qualifier, value as i64)
    }

    /// [`put_u64`](Self::put_u64) at an explicit timestamp.
    pub fn put_u64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: u64,
    ) -> &mut Self {
        self.put_i64_at(table, row, family, qualifier, ts, value as i64)
    }

    /// Buffers a typed `f64` put.
    pub fn put_f64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: f64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, None, ValueRef::F64(value))
        })
    }

    /// Buffers an add to the counter of a counter family (see [`RowMutation::incr`]). It is
    /// a blind write: it reads nothing, so it conflicts with nothing unless the transaction
    /// also read the cell.
    pub fn incr(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        delta: i64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Incr),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::I64(delta)),
        )
    }

    /// Buffers an input to the cell of an aggregate family (see
    /// [`RowMutation::aggregate`]). Like [`incr`](Self::incr), a blind write.
    pub fn aggregate(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: i64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Aggregate),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::I64(value)),
        )
    }

    /// Buffers an input to the bucket at `ts` of an aggregate family (see
    /// [`RowMutation::aggregate_at`]).
    pub fn aggregate_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: i64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Aggregate),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(value)),
        )
    }

    /// [`RowMutation::aggregate_u64`]: a `u64` input to a bitwise aggregate family.
    pub fn aggregate_u64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: u64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::U64),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::I64(value as i64)),
        )
    }

    /// [`RowMutation::aggregate_u64_at`].
    pub fn aggregate_u64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: u64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::U64),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(value as i64)),
        )
    }

    /// [`RowMutation::aggregate_f64`]: an `f64` input to an `f64` aggregate family.
    pub fn aggregate_f64(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: f64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::F64),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::F64(value)),
        )
    }

    /// [`RowMutation::aggregate_f64_at`].
    pub fn aggregate_f64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: f64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::F64),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::F64(value)),
        )
    }

    /// [`RowMutation::aggregate_bytes`]: a bytes element for an HLL or bloom family.
    pub fn aggregate_bytes(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Bytes),
            table,
            row,
            family,
            qualifier,
            value.len(),
            |b, t, f| b.merge(t, f, row, qualifier, ValueRef::Bytes(value)),
        )
    }

    /// [`RowMutation::aggregate_bytes_at`].
    pub fn aggregate_bytes_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u8],
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Bytes),
            table,
            row,
            family,
            qualifier,
            value.len(),
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::Bytes(value)),
        )
    }

    /// [`put_i64`](Self::put_i64) at an explicit timestamp.
    pub fn put_i64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: i64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::I64(value))
        })
    }

    /// [`put_f64`](Self::put_f64) at an explicit timestamp.
    pub fn put_f64_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: f64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 8, |b, t, f| {
            b.put(t, f, row, qualifier, Some(ts), ValueRef::F64(value))
        })
    }

    /// Puts the `f32` vector `value` into a vector family (D303), as
    /// [`RowMutation::put_vector_f32`] does.
    pub fn put_vector_f32(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[f32],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::F32(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) at an explicit timestamp.
    pub fn put_vector_f32_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[f32],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::F32(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) for an `i8` vector family.
    pub fn put_vector_i8(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[i8],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::I8(value))
    }

    /// [`put_vector_i8`](Self::put_vector_i8) at an explicit timestamp.
    pub fn put_vector_i8_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[i8],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::I8(value))
    }

    /// [`put_vector_f32`](Self::put_vector_f32) for a bit vector family (bits packed eight to
    /// a byte, least significant first).
    pub fn put_vector_bits(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u8],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::Bits(value))
    }

    /// [`put_vector_bits`](Self::put_vector_bits) at an explicit timestamp.
    pub fn put_vector_bits_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u8],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::Bits(value))
    }

    /// Puts a vector into an `f16` vector family (IEEE binary16 bit patterns) (#1263), as [`RowMutation::put_vector_f16`] does.
    pub fn put_vector_f16(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u16],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::F16(value))
    }

    /// [`put_vector_f16`](Self::put_vector_f16) at an explicit timestamp.
    pub fn put_vector_f16_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u16],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::F16(value))
    }

    /// Puts a vector into a `bf16` vector family (bfloat16 bit patterns) (#1263), as [`RowMutation::put_vector_bf16`] does.
    pub fn put_vector_bf16(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        value: &[u16],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, None, Query::BF16(value))
    }

    /// [`put_vector_bf16`](Self::put_vector_bf16) at an explicit timestamp.
    pub fn put_vector_bf16_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        value: &[u16],
    ) -> &mut Self {
        self.vector(table, row, family, qualifier, Some(ts), Query::BF16(value))
    }

    /// A vector put, checked against the family's spec when it is built.
    fn vector(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: Option<u64>,
        q: Query<'_>,
    ) -> &mut Self {
        let mut buf = Vec::new();
        match table.core.vector_payload(family, q, &mut buf) {
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(e);
                }
                self
            }
            Ok(p) => self.op(table, row, family, qualifier, buf.len(), |b, t, f| {
                b.put(t, f, row, qualifier, ts, vector_value(&p, &buf))
            }),
        }
    }

    /// Buffers an add to the bucket at `ts` of a counter family (see
    /// [`RowMutation::incr_at`]).
    pub fn incr_at(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
        delta: i64,
    ) -> &mut Self {
        self.op_as(
            Some(Operand::Incr),
            table,
            row,
            family,
            qualifier,
            8,
            |b, t, f| b.merge_at(t, f, row, qualifier, ts, ValueRef::I64(delta)),
        )
    }

    /// Buffers an untyped operand for the family's merge operator (see
    /// [`RowMutation::merge`]).
    pub fn merge(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        operand: &[u8],
    ) -> &mut Self {
        self.op(table, row, family, qualifier, operand.len(), |b, t, f| {
            b.merge(t, f, row, qualifier, ValueRef::Bytes(operand))
        })
    }

    /// Buffers the delete of one version.
    pub fn delete_cell(
        &mut self,
        table: &Table,
        row: &[u8],
        family: &str,
        qualifier: &[u8],
        ts: u64,
    ) -> &mut Self {
        self.op(table, row, family, qualifier, 0, |b, t, f| {
            b.delete_cell(t, f, row, qualifier, ts)
        })
    }

    /// Buffers the delete of every column of a family in a row.
    pub fn delete_family(&mut self, table: &Table, row: &[u8], family: &str) -> &mut Self {
        self.op(table, row, family, &[], 0, |b, t, f| {
            b.delete_family(t, f, row, None)
        })
    }

    /// Buffers the delete of a whole row.
    pub fn delete_row(&mut self, table: &Table, row: &[u8]) -> &mut Self {
        if self.error.is_some() {
            return self;
        }
        let r = check_table(&self.db, table).and_then(|()| {
            self.txn
                .batch()
                .delete_row(table.core.info.id, row, None)
                .map_err(|e| size_error(e, Sizes::new(row, &[], 0), 0))
        });
        if let Err(e) = r {
            self.error = Some(e);
        }
        self
    }

    /// Commits with the writer default; fails with
    /// [`ErrorCode::Conflict`](crate::ErrorCode::Conflict) on a conflicting commit.
    ///
    /// A transaction that wrote nothing commits at once and never conflicts: its reads all
    /// come from one snapshot, so it is serializable there (ICR 0060). `Ok` then means what
    /// it read was consistent **as of its snapshot**, not that it was still current when
    /// `commit` ran: to confirm a value still holds at commit, write something in the
    /// transaction (it is then validated) or use
    /// [`RowMutation::commit_if`](crate::RowMutation::commit_if) on the cell. Its
    /// [`CommitInfo`] names the snapshot's seqno (not a seqno of its own, so other commits
    /// may share it) and [`Durability::None`]: it wrote nothing, and what it read is visible
    /// but not necessarily durable, as for any snapshot. For reads of only durable data, read
    /// at [`Pigeonhole::durable_snapshot`](crate::Pigeonhole::durable_snapshot).
    pub fn commit(self) -> Result<CommitInfo> {
        self.finish(None)
    }

    /// Commits with `durability`.
    pub fn commit_with(self, durability: Durability) -> Result<CommitInfo> {
        self.finish(Some(durability))
    }
}
