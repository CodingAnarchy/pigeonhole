//! Between ICR 0112's messages and the `pigeonhole` API: writes onto mutations and batches,
//! conditions and selections onto reads, and cells back into messages. Validation errors
//! (a missing field, a vector of the wrong size) are `InvalidArgument`, before anything is
//! submitted.

use std::ops::Bound;

use pigeonhole::{Element, RowMutation, Table, WriteBatch};

use super::errors::ApiError;
use super::proto::{self, value, write};

type Result<T> = std::result::Result<T, ApiError>;

pub(crate) fn durability(d: i32) -> Option<pigeonhole::Durability> {
    match proto::Durability::try_from(d).unwrap_or(proto::Durability::Unspecified) {
        proto::Durability::Unspecified => None,
        proto::Durability::None => Some(pigeonhole::Durability::None),
        proto::Durability::Buffered => Some(pigeonhole::Durability::Buffered),
        proto::Durability::GroupSync => Some(pigeonhole::Durability::GroupSync),
        proto::Durability::Sync => Some(pigeonhole::Durability::Sync),
    }
}

pub(crate) fn durability_out(d: pigeonhole::Durability) -> i32 {
    (match d {
        pigeonhole::Durability::None => proto::Durability::None,
        pigeonhole::Durability::Buffered => proto::Durability::Buffered,
        pigeonhole::Durability::GroupSync => proto::Durability::GroupSync,
        pigeonhole::Durability::Sync => proto::Durability::Sync,
        _ => proto::Durability::Unspecified,
    }) as i32
}

pub(crate) fn commit_info(c: pigeonhole::CommitInfo) -> proto::CommitInfo {
    proto::CommitInfo {
        seqno: c.seqno,
        durability: durability_out(c.durability),
    }
}

/// A vector's elements, decoded from the packed little-endian bytes.
pub(crate) enum Elements {
    F32(Vec<f32>),
    I8(Vec<i8>),
    Bits(Vec<u8>),
    F16(Vec<u16>),
    Bf16(Vec<u16>),
}

fn elements(v: &proto::Vector) -> Result<Elements> {
    let dim = v.dim as usize;
    let data = &v.data;
    let want = |each: usize| {
        if data.len() == dim * each {
            Ok(())
        } else {
            Err(ApiError::invalid(format!(
                "a vector of {dim} elements needs {} bytes of data, not {}",
                dim * each,
                data.len()
            )))
        }
    };
    let le16 = |c: &[u8; 2]| u16::from_le_bytes(*c);
    Ok(
        match proto::Element::try_from(v.element).unwrap_or(proto::Element::Unspecified) {
            proto::Element::F32 => {
                want(4)?;
                Elements::F32(
                    data.as_chunks::<4>()
                        .0
                        .iter()
                        .map(|c| f32::from_le_bytes(*c))
                        .collect(),
                )
            }
            proto::Element::I8 => {
                want(1)?;
                Elements::I8(data.iter().map(|&b| b as i8).collect())
            }
            proto::Element::Bit => {
                if data.len() != dim.div_ceil(8) {
                    return Err(ApiError::invalid(format!(
                        "a bit vector of {dim} bits needs {} bytes",
                        dim.div_ceil(8)
                    )));
                }
                Elements::Bits(data.to_vec())
            }
            proto::Element::F16 => {
                want(2)?;
                Elements::F16(data.as_chunks::<2>().0.iter().map(le16).collect())
            }
            proto::Element::Bf16 => {
                want(2)?;
                Elements::Bf16(data.as_chunks::<2>().0.iter().map(le16).collect())
            }
            proto::Element::Unspecified => {
                return Err(ApiError::invalid("a vector needs its element type"));
            }
        },
    )
}

fn value_of(v: Option<&proto::Value>) -> Result<&value::Kind> {
    v.and_then(|v| v.kind.as_ref())
        .ok_or_else(|| ApiError::invalid("a put needs a value"))
}

/// A typed value, decoded from its proto form.
pub(crate) enum Typed<'w> {
    Bytes(&'w [u8]),
    I64(i64),
    U64(u64),
    F64(f64),
    Vector(Elements),
}

/// One write, checked and decoded. A row mutation, a batch and a transaction all buffer
/// these, through one dispatch (`dispatch!`). A transaction prepares a request's writes
/// before buffering any (simp724's T1 on #1511), so a malformed one fails with nothing
/// buffered.
pub(crate) enum TxnOp<'w> {
    Put {
        family: &'w str,
        qualifier: &'w [u8],
        ts: Option<u64>,
        value: Typed<'w>,
    },
    Incr {
        family: &'w str,
        qualifier: &'w [u8],
        ts: Option<u64>,
        delta: i64,
    },
    Aggregate {
        family: &'w str,
        qualifier: &'w [u8],
        ts: Option<u64>,
        input: Typed<'w>,
    },
    Merge {
        family: &'w str,
        qualifier: &'w [u8],
        operand: &'w [u8],
    },
    DeleteCell {
        family: &'w str,
        qualifier: &'w [u8],
        ts: u64,
    },
    DeleteColumn {
        family: &'w str,
        qualifier: &'w [u8],
    },
    DeleteFamily {
        family: &'w str,
    },
    DeleteRow,
}

fn typed(kind: &value::Kind) -> Result<Typed<'_>> {
    Ok(match kind {
        value::Kind::Bytes(b) => Typed::Bytes(b),
        value::Kind::I64(n) => Typed::I64(*n),
        value::Kind::U64(n) => Typed::U64(*n),
        value::Kind::F64(x) => Typed::F64(*x),
        value::Kind::Vector(v) => Typed::Vector(elements(v)?),
    })
}

/// One write, checked and decoded.
fn txn_op(w: &proto::Write) -> Result<TxnOp<'_>> {
    Ok(
        match w
            .op
            .as_ref()
            .ok_or_else(|| ApiError::invalid("a write needs an op"))?
        {
            write::Op::Put(p) => TxnOp::Put {
                family: &p.family,
                qualifier: &p.qualifier,
                ts: p.timestamp,
                value: typed(value_of(p.value.as_ref())?)?,
            },
            write::Op::Incr(i) => TxnOp::Incr {
                family: &i.family,
                qualifier: &i.qualifier,
                ts: i.timestamp,
                delta: i.delta,
            },
            write::Op::Aggregate(a) => {
                let kind = value_of(a.input.as_ref())?;
                if matches!(kind, value::Kind::Vector(_)) {
                    return Err(ApiError::invalid(
                        "an aggregate input is an i64, u64, f64 or bytes",
                    ));
                }
                TxnOp::Aggregate {
                    family: &a.family,
                    qualifier: &a.qualifier,
                    ts: a.timestamp,
                    input: typed(kind)?,
                }
            }
            write::Op::Merge(g) => TxnOp::Merge {
                family: &g.family,
                qualifier: &g.qualifier,
                operand: &g.operand,
            },
            write::Op::DeleteCell(d) => TxnOp::DeleteCell {
                family: &d.family,
                qualifier: &d.qualifier,
                ts: d.timestamp,
            },
            write::Op::DeleteColumn(d) => TxnOp::DeleteColumn {
                family: &d.family,
                qualifier: &d.qualifier,
            },
            write::Op::DeleteFamily(d) => TxnOp::DeleteFamily { family: &d.family },
            write::Op::DeleteRow(_) => TxnOp::DeleteRow,
        },
    )
}

/// Checks and decodes a row's `writes` for a transaction, buffering nothing.
pub(crate) fn txn_ops(writes: &[proto::Write]) -> Result<Vec<TxnOp<'_>>> {
    writes.iter().map(txn_op).collect()
}

/// Buffers one decoded op through `$call!(method, args...)`, the target's way of calling
/// its typed write methods (`RowMutation`'s take the cell's coordinates; a batch's and a
/// transaction's take the table and row first). One match for the three targets.
macro_rules! dispatch {
    ($op:expr, $call:ident) => {
        match $op {
            TxnOp::Put {
                family: f,
                qualifier: q,
                ts,
                value,
            } => {
                let (f, q) = (*f, *q);
                match (value, *ts) {
                    (Typed::Bytes(b), None) => $call!(put, f, q, b),
                    (Typed::Bytes(b), Some(ts)) => $call!(put_at, f, q, ts, b),
                    (Typed::I64(n), None) => $call!(put_i64, f, q, *n),
                    (Typed::I64(n), Some(ts)) => $call!(put_i64_at, f, q, ts, *n),
                    (Typed::F64(x), None) => $call!(put_f64, f, q, *x),
                    (Typed::F64(x), Some(ts)) => $call!(put_f64_at, f, q, ts, *x),
                    (Typed::U64(n), None) => $call!(put_u64, f, q, *n),
                    (Typed::U64(n), Some(ts)) => $call!(put_u64_at, f, q, ts, *n),
                    (Typed::Vector(v), ts) => match (v, ts) {
                        (Elements::F32(x), None) => $call!(put_vector_f32, f, q, x),
                        (Elements::F32(x), Some(ts)) => $call!(put_vector_f32_at, f, q, ts, x),
                        (Elements::I8(x), None) => $call!(put_vector_i8, f, q, x),
                        (Elements::I8(x), Some(ts)) => $call!(put_vector_i8_at, f, q, ts, x),
                        (Elements::Bits(x), None) => $call!(put_vector_bits, f, q, x),
                        (Elements::Bits(x), Some(ts)) => $call!(put_vector_bits_at, f, q, ts, x),
                        (Elements::F16(x), None) => $call!(put_vector_f16, f, q, x),
                        (Elements::F16(x), Some(ts)) => $call!(put_vector_f16_at, f, q, ts, x),
                        (Elements::Bf16(x), None) => $call!(put_vector_bf16, f, q, x),
                        (Elements::Bf16(x), Some(ts)) => $call!(put_vector_bf16_at, f, q, ts, x),
                    },
                }
            }
            TxnOp::Incr {
                family,
                qualifier,
                ts,
                delta,
            } => match ts {
                None => $call!(incr, family, qualifier, *delta),
                Some(ts) => $call!(incr_at, family, qualifier, *ts, *delta),
            },
            TxnOp::Aggregate {
                family: f,
                qualifier: q,
                ts,
                input,
            } => {
                let (f, q) = (*f, *q);
                match (input, *ts) {
                    (Typed::I64(n), None) => $call!(aggregate, f, q, *n),
                    (Typed::I64(n), Some(ts)) => $call!(aggregate_at, f, q, ts, *n),
                    (Typed::U64(n), None) => $call!(aggregate_u64, f, q, *n),
                    (Typed::U64(n), Some(ts)) => $call!(aggregate_u64_at, f, q, ts, *n),
                    (Typed::F64(x), None) => $call!(aggregate_f64, f, q, *x),
                    (Typed::F64(x), Some(ts)) => $call!(aggregate_f64_at, f, q, ts, *x),
                    (Typed::Bytes(b), None) => $call!(aggregate_bytes, f, q, b),
                    (Typed::Bytes(b), Some(ts)) => $call!(aggregate_bytes_at, f, q, ts, b),
                    // `txn_op` refuses a vector input before decoding it.
                    (Typed::Vector(_), _) => {}
                }
            }
            TxnOp::Merge {
                family,
                qualifier,
                operand,
            } => $call!(merge, family, qualifier, operand),
            TxnOp::DeleteCell {
                family,
                qualifier,
                ts,
            } => $call!(delete_cell, family, qualifier, *ts),
            TxnOp::DeleteColumn { family, qualifier } => $call!(delete_column, family, qualifier),
            TxnOp::DeleteFamily { family } => $call!(delete_family, family),
            TxnOp::DeleteRow => $call!(delete_row,),
        }
    };
}

/// Applies `writes` to a row mutation, one at a time.
pub(crate) fn mutation<'t>(
    mut m: RowMutation<'t>,
    writes: &[proto::Write],
) -> Result<RowMutation<'t>> {
    for w in writes {
        let op = txn_op(w)?;
        macro_rules! on_mutation {
            ($method:ident, $($a:expr),*) => {
                m = m.$method($($a),*)
            };
        }
        dispatch!(&op, on_mutation);
    }
    Ok(m)
}

/// Applies one row's `writes` to a batch, one at a time. A batch takes neither `u64` nor
/// vector puts, nor aggregate inputs: refused as they come, before their values decode.
pub(crate) fn batch(
    b: &mut WriteBatch,
    table: &Table,
    row: &[u8],
    writes: &[proto::Write],
) -> Result<()> {
    for w in writes {
        // The batch's only guard against u64 and vector puts and aggregate inputs: `dispatch!`
        // expands those arms for `WriteBatch` too (ai1249's N2 on #1555).
        match w.op.as_ref() {
            Some(write::Op::Put(p))
                if matches!(
                    p.value.as_ref().and_then(|v| v.kind.as_ref()),
                    Some(value::Kind::U64(_) | value::Kind::Vector(_))
                ) =>
            {
                return Err(ApiError::invalid(
                    "a batch put takes bytes, i64 or f64 (use Mutate for u64 and vectors)",
                ));
            }
            Some(write::Op::Aggregate(_)) => {
                return Err(ApiError::invalid(
                    "a batch doesn't take aggregate inputs (use Mutate)",
                ));
            }
            _ => {}
        }
        let op = txn_op(w)?;
        macro_rules! on_batch {
            ($method:ident, $($a:expr),*) => {{
                b.$method(table, row, $($a),*);
            }};
        }
        dispatch!(&op, on_batch);
    }
    Ok(())
}

/// Buffers prepared ops in a transaction. Nothing here fails: what only the engine can check
/// (a family, a size) is recorded in the transaction and fails its commit, as in-process.
pub(crate) fn txn_apply(
    t: &mut pigeonhole::Transaction,
    table: &Table,
    row: &[u8],
    ops: &[TxnOp<'_>],
) {
    for op in ops {
        macro_rules! on_txn {
            ($method:ident, $($a:expr),*) => {{
                t.$method(table, row, $($a),*);
            }};
        }
        dispatch!(op, on_txn);
    }
}

pub(crate) fn condition(c: Option<&proto::Condition>) -> Result<pigeonhole::Condition> {
    use proto::condition::Kind;
    let col = |c: &Option<proto::Column>| {
        c.as_ref()
            .map(|c| (c.family.clone(), c.qualifier.to_vec()))
            .ok_or_else(|| ApiError::invalid("a condition names a column"))
    };
    Ok(
        match c
            .and_then(|c| c.kind.as_ref())
            .ok_or_else(|| ApiError::invalid("CheckAndMutate needs a condition"))?
        {
            Kind::Exists(c) => pigeonhole::Condition::Exists {
                family: c.family.clone(),
                qualifier: c.qualifier.to_vec(),
            },
            Kind::Absent(c) => pigeonhole::Condition::Absent {
                family: c.family.clone(),
                qualifier: c.qualifier.to_vec(),
            },
            Kind::Value(v) => {
                let (family, qualifier) = col(&v.column)?;
                pigeonhole::Condition::Value {
                    family,
                    qualifier,
                    filter: value_filter(v.filter.as_ref())?,
                }
            }
        },
    )
}

pub(crate) fn value_filter(f: Option<&proto::ValueFilter>) -> Result<pigeonhole::ValueFilter> {
    use proto::value_filter::Kind;
    Ok(
        match f
            .and_then(|f| f.kind.as_ref())
            .ok_or_else(|| ApiError::invalid("a value filter needs a kind"))?
        {
            Kind::Equals(b) => pigeonhole::ValueFilter::Equals(b.to_vec()),
            Kind::Prefix(b) => pigeonhole::ValueFilter::Prefix(b.to_vec()),
            Kind::I64(c) => {
                let ord = match proto::Ordering::try_from(c.ordering)
                    .unwrap_or(proto::Ordering::Unspecified)
                {
                    proto::Ordering::Less => std::cmp::Ordering::Less,
                    proto::Ordering::Equal => std::cmp::Ordering::Equal,
                    proto::Ordering::Greater => std::cmp::Ordering::Greater,
                    proto::Ordering::Unspecified => {
                        return Err(ApiError::invalid("an i64 comparison needs its ordering"));
                    }
                };
                pigeonhole::ValueFilter::I64(ord, c.operand)
            }
        },
    )
}

fn bound(b: Option<&proto::Bound>) -> Bound<&[u8]> {
    match b {
        None => Bound::Unbounded,
        Some(b) if b.inclusive => Bound::Included(&b.key[..]),
        Some(b) => Bound::Excluded(&b.key[..]),
    }
}

/// Applies a selection to any of the read builders, which share these methods.
macro_rules! select {
    ($s:expr, $snap:expr, $r:expr) => {{
        let mut r = $r;
        if let Some(s) = $s {
            if !s.families.is_empty() {
                r = r.families(s.families.iter().map(String::as_str));
            }
            if !s.qualifier_prefix.is_empty() {
                r = r.qualifier_prefix(&s.qualifier_prefix);
            }
            if s.qualifier_start.is_some() || s.qualifier_end.is_some() {
                r = r.qualifier_bounds(
                    bound(s.qualifier_start.as_ref()),
                    bound(s.qualifier_end.as_ref()),
                );
            }
            if s.all_versions {
                r = r.versions(0);
            } else if s.versions > 0 {
                r = r.versions(s.versions);
            }
            if let Some(t) = &s.time_range {
                r = r.time_range(t.start..t.end);
            }
            if s.column_limit > 0 {
                r = r.column_limit(s.column_limit);
            }
            if s.value_filter.is_some() {
                r = r.value_filter(value_filter(s.value_filter.as_ref())?);
            }
        }
        if let Some(snap) = $snap {
            r = r.snapshot(snap);
        }
        Ok(r)
    }};
}

/// A selection's parts, checked once and applied to row reads and multi-gets alike, at the
/// pinned snapshot its `snapshot_id` names (resolved by the service, `at`), or now.
pub(crate) struct Select<'s> {
    s: Option<&'s proto::Selection>,
    snapshot: Option<pigeonhole::Snapshot>,
}

impl<'s> Select<'s> {
    pub(crate) fn new(s: Option<&'s proto::Selection>) -> Result<Self> {
        Ok(Self { s, snapshot: None })
    }

    /// Reads at `snapshot` (a pinned snapshot session's), or now with `None`.
    pub(crate) fn at(mut self, snapshot: Option<pigeonhole::Snapshot>) -> Self {
        self.snapshot = snapshot;
        self
    }

    pub(crate) fn aggregate(&self) -> Option<&'s proto::AggregateQuery> {
        self.s.and_then(|s| s.aggregate.as_ref())
    }

    pub(crate) fn row<'t>(&self, r: pigeonhole::RowRead<'t>) -> Result<pigeonhole::RowRead<'t>> {
        select!(self.s, self.snapshot.as_ref(), r)
    }

    pub(crate) fn keys<'t>(&self, r: pigeonhole::KeysRead<'t>) -> Result<pigeonhole::KeysRead<'t>> {
        select!(self.s, self.snapshot.as_ref(), r)
    }

    /// A scan of `range` in `table` (no range: the whole table) with this selection.
    pub(crate) fn scan<'t>(
        &self,
        table: &'t pigeonhole::Table,
        range: Option<&proto::KeyRange>,
    ) -> Result<pigeonhole::Scan<'t>> {
        use proto::key_range::Range;
        let r = match range.and_then(|r| r.range.as_ref()) {
            Some(Range::Prefix(p)) => table.scan_prefix(p),
            Some(Range::Bounds(b)) => {
                table.scan_bounds(bound(b.start.as_ref()), bound(b.end.as_ref()))
            }
            None => table.scan_bounds(Bound::Unbounded, Bound::Unbounded),
        };
        select!(self.s, self.snapshot.as_ref(), r)
    }
}

/// The readings a cell offers, owned (`Cell`) or borrowed (`CellRef`) alike.
pub(crate) trait Readable {
    fn value(&self) -> &[u8];
    fn timestamp(&self) -> u64;
    fn typed(&self) -> pigeonhole::Value<'_>;
    fn hll_estimate(&self) -> Option<u64>;
    fn mean_var(&self) -> Option<pigeonhole::MeanVar>;
    fn histogram_counts(&self) -> Option<Vec<u64>>;
    fn quantiles_count(&self) -> Option<u64>;
    fn quantile(&self, q: f64) -> Option<f64>;
    fn bloom_fill(&self) -> Option<f64>;
    fn bloom_contains(&self, v: i64) -> Option<bool>;
    fn bloom_contains_bytes(&self, v: &[u8]) -> Option<bool>;
    fn set_members(&self) -> Option<Vec<u32>>;
}

macro_rules! readable {
    ($t:ty) => {
        impl Readable for $t {
            fn value(&self) -> &[u8] {
                <$t>::value(self)
            }
            fn timestamp(&self) -> u64 {
                <$t>::timestamp(self)
            }
            fn typed(&self) -> pigeonhole::Value<'_> {
                <$t>::typed(self)
            }
            fn hll_estimate(&self) -> Option<u64> {
                <$t>::hll_estimate(self)
            }
            fn mean_var(&self) -> Option<pigeonhole::MeanVar> {
                <$t>::mean_var(self)
            }
            fn histogram_counts(&self) -> Option<Vec<u64>> {
                <$t>::histogram_counts(self)
            }
            fn quantiles_count(&self) -> Option<u64> {
                <$t>::quantiles_count(self)
            }
            fn quantile(&self, q: f64) -> Option<f64> {
                <$t>::quantile(self, q)
            }
            fn bloom_fill(&self) -> Option<f64> {
                <$t>::bloom_fill(self)
            }
            fn bloom_contains(&self, v: i64) -> Option<bool> {
                <$t>::bloom_contains(self, v)
            }
            fn bloom_contains_bytes(&self, v: &[u8]) -> Option<bool> {
                <$t>::bloom_contains_bytes(self, v)
            }
            fn set_members(&self) -> Option<Vec<u32>> {
                <$t>::as_set(self).map(|s| s.iter().collect())
            }
        }
    };
}

readable!(pigeonhole::Cell);
readable!(pigeonhole::CellRef<'_>);

/// A value as the feed delivers it (`Change::value`), in its proto form.
pub(crate) fn value_out(v: pigeonhole::Value<'_>) -> proto::Value {
    let kind = match v {
        pigeonhole::Value::I64(n) | pigeonhole::Value::Varint(n) => value::Kind::I64(n),
        pigeonhole::Value::F64(x) => value::Kind::F64(x),
        pigeonhole::Value::Vector(v) => value::Kind::Vector(proto::Vector {
            element: element_out(v.element()),
            dim: v.dim(),
            data: v.as_bytes().to_vec().into(),
        }),
        pigeonhole::Value::Bytes(b) => value::Kind::Bytes(b.to_vec().into()),
        // A kind a newer library adds: its bytes are all a v1 client can take.
        _ => value::Kind::Bytes(Vec::new().into()),
    };
    proto::Value { kind: Some(kind) }
}

fn element_out(e: Element) -> i32 {
    (match e {
        Element::F32 => proto::Element::F32,
        Element::F16 => proto::Element::F16,
        Element::BF16 => proto::Element::Bf16,
        Element::I8 => proto::Element::I8,
        Element::Bit => proto::Element::Bit,
        _ => proto::Element::Unspecified,
    }) as i32
}

/// A cell as the API returns it. `typed` is set for typed values only: for plain bytes it
/// would repeat `value`, so it's left out.
pub(crate) fn cell(
    family: &str,
    qualifier: &[u8],
    c: &impl Readable,
    q: Option<&proto::AggregateQuery>,
) -> proto::Cell {
    let typed = match c.typed() {
        pigeonhole::Value::I64(n) | pigeonhole::Value::Varint(n) => Some(value::Kind::I64(n)),
        pigeonhole::Value::F64(x) => Some(value::Kind::F64(x)),
        pigeonhole::Value::Vector(v) => Some(value::Kind::Vector(proto::Vector {
            element: element_out(v.element()),
            dim: v.dim(),
            data: v.as_bytes().to_vec().into(),
        })),
        _ => None,
    };
    proto::Cell {
        family: family.to_owned(),
        qualifier: qualifier.to_vec().into(),
        timestamp: c.timestamp(),
        value: c.value().to_vec().into(),
        typed: typed.map(|k| proto::Value { kind: Some(k) }),
        aggregate: aggregate(c, q),
    }
}

fn aggregate(c: &impl Readable, q: Option<&proto::AggregateQuery>) -> Option<proto::AggregateView> {
    let mut v = proto::AggregateView {
        hll_estimate: c.hll_estimate(),
        mean_var: c.mean_var().map(|m| proto::MeanVar {
            count: m.count(),
            mean: m.mean(),
            variance: m.variance(),
        }),
        histogram_counts: c.histogram_counts().unwrap_or_default(),
        quantiles: None,
        bloom: None,
        set_members: c.set_members().unwrap_or_default(),
    };
    if let Some(count) = c.quantiles_count() {
        let values = q
            .map(|q| {
                q.quantiles
                    .iter()
                    .map(|&p| c.quantile(p).unwrap_or(f64::NAN))
                    .collect()
            })
            .unwrap_or_default();
        v.quantiles = Some(proto::QuantileReading { count, values });
    }
    if let Some(fill) = c.bloom_fill() {
        let contains = q
            .map(|q| {
                q.bloom_i64
                    .iter()
                    .map(|&n| c.bloom_contains(n).unwrap_or(false))
                    .chain(
                        q.bloom_bytes
                            .iter()
                            .map(|b| c.bloom_contains_bytes(b).unwrap_or(false)),
                    )
                    .collect()
            })
            .unwrap_or_default();
        v.bloom = Some(proto::BloomReading { fill, contains });
    }
    let empty = v.hll_estimate.is_none()
        && v.mean_var.is_none()
        && v.histogram_counts.is_empty()
        && v.quantiles.is_none()
        && v.bloom.is_none()
        && v.set_members.is_empty();
    (!empty).then_some(v)
}

/// A row as the API returns it.
pub(crate) fn row(r: pigeonhole::RowRef<'_>, q: Option<&proto::AggregateQuery>) -> proto::Row {
    let mut cells = Vec::with_capacity(r.len());
    for i in 0..r.len() {
        if let Some(e) = r.entry(i) {
            cells.push(cell(e.family, e.qualifier, &e.cell, q));
        }
    }
    proto::Row {
        key: r.key().to_vec().into(),
        cells,
    }
}

/// D308's size of a cell: its qualifier, its value and 16 bytes.
pub(crate) fn cell_bytes(c: &proto::Cell) -> u64 {
    (c.qualifier.len() + c.value.len() + 16) as u64
}

// ---- Schema (D313 ticket 6): families in and out ----

fn element_in(e: i32) -> Result<Element> {
    Ok(
        match proto::Element::try_from(e).unwrap_or(proto::Element::Unspecified) {
            proto::Element::F32 => Element::F32,
            proto::Element::F16 => Element::F16,
            proto::Element::Bf16 => Element::BF16,
            proto::Element::I8 => Element::I8,
            proto::Element::Bit => Element::Bit,
            proto::Element::Unspecified => {
                return Err(ApiError::invalid("a vector family needs its element type"));
            }
        },
    )
}

fn metric_in(m: i32) -> Result<pigeonhole::Metric> {
    use pigeonhole::Metric;
    Ok(
        match proto::Metric::try_from(m).unwrap_or(proto::Metric::Unspecified) {
            proto::Metric::L2 => Metric::L2,
            proto::Metric::Cosine => Metric::Cosine,
            proto::Metric::InnerProduct => Metric::InnerProduct,
            proto::Metric::Hamming => Metric::Hamming,
            proto::Metric::Unspecified => {
                return Err(ApiError::invalid("a vector family needs its metric"));
            }
        },
    )
}

fn metric_out(m: pigeonhole::Metric) -> i32 {
    use pigeonhole::Metric;
    (match m {
        Metric::L2 => proto::Metric::L2,
        Metric::Cosine => proto::Metric::Cosine,
        Metric::InnerProduct => proto::Metric::InnerProduct,
        Metric::Hamming => proto::Metric::Hamming,
        _ => proto::Metric::Unspecified,
    }) as i32
}

/// A `FamilySpec` as the `Family` to create: unset options take the library's defaults.
pub(crate) fn family_in(s: &proto::FamilySpec) -> Result<pigeonhole::Family> {
    use pigeonhole::{Aggregation as A, BloomParams, Family, HistogramParams, QuantileParams};
    use proto::family_spec::Kind;
    let bad = |what: &str, e: pigeonhole::Error| {
        ApiError::invalid(format!("family {}: {what}: {}", s.name, e.message()))
    };
    let narrow =
        |what: &str| ApiError::invalid(format!("family {}: {what} is out of range", s.name));
    let mut f = match &s.kind {
        None => Family::default(),
        Some(Kind::MergeOperator(op)) => Family::default().merge_operator(op),
        Some(Kind::Vector(v)) => Family::vector(
            pigeonhole::VectorSpec::new(v.dim, element_in(v.element)?, metric_in(v.metric)?)
                .map_err(|e| bad("the vector spec", e))?,
        ),
        Some(Kind::Aggregate(a)) => {
            use proto::AggregationKind as K;
            let kind = K::try_from(a.kind).unwrap_or(K::Unspecified);
            Family::aggregate(match kind {
                K::Sum => A::Sum,
                K::Min => A::Min,
                K::Max => A::Max,
                K::Hll => A::Hll,
                K::BitOr => A::BitOr,
                K::BitAnd => A::BitAnd,
                K::BitXor => A::BitXor,
                K::MinF64 => A::MinF64,
                K::MaxF64 => A::MaxF64,
                K::SumF64 => A::SumF64,
                K::MeanVar => A::MeanVar,
                K::Set => A::Set,
                K::Histogram => A::Histogram(
                    HistogramParams::new(a.histogram_bounds.clone())
                        .map_err(|e| bad("the histogram bounds", e))?,
                ),
                K::Quantiles => {
                    let mut q = QuantileParams::default();
                    if let Some(n) = a.quantile_mantissa_bits {
                        q = q.mantissa_bits(u8::try_from(n).map_err(|_| narrow("mantissa_bits"))?);
                    }
                    if let Some(n) = a.quantile_max_buckets {
                        q = q.max_buckets(u16::try_from(n).map_err(|_| narrow("max_buckets"))?);
                    }
                    A::Quantiles(q)
                }
                K::Bloom => A::Bloom(
                    BloomParams::new(
                        a.bloom_bits,
                        u8::try_from(a.bloom_hashes).map_err(|_| narrow("bloom_hashes"))?,
                    )
                    .map_err(|e| bad("the bloom filter", e))?,
                ),
                K::Unspecified => {
                    return Err(ApiError::invalid(format!(
                        "family {}: an aggregate family needs its kind",
                        s.name
                    )));
                }
            })
        }
    };
    if let Some(n) = s.max_versions {
        f = f.max_versions(n);
    }
    // 0 is "no TTL", as `DescribeTable` reports it, so a described family creates again.
    if let Some(us) = s.ttl_micros.filter(|&us| us > 0) {
        f = f.ttl(std::time::Duration::from_micros(us));
    }
    if let Some(n) = s.bloom_bits {
        f = f.bloom_bits(u8::try_from(n).map_err(|_| narrow("bloom_bits"))?);
    }
    if let Some(n) = s.blob_threshold {
        f = f.blob_threshold(n);
    }
    if let Some(n) = s.block_size {
        f = f.block_size(n);
    }
    if let Some(c) = &s.compression {
        use proto::CompressionCodec as C;
        f = match C::try_from(c.codec).unwrap_or(C::Unspecified) {
            C::Unspecified => f,
            C::None => f.compression(pigeonhole::Compression::None),
            C::Lz4 => f.compression(pigeonhole::Compression::Lz4),
            C::Zstd => f.compression(pigeonhole::Compression::Zstd(if c.zstd_level == 0 {
                3
            } else {
                i8::try_from(c.zstd_level).map_err(|_| narrow("zstd_level"))?
            })),
        };
    }
    f = match proto::Priority::try_from(s.cache_priority).unwrap_or(proto::Priority::Unspecified) {
        proto::Priority::Unspecified => f,
        proto::Priority::Low => f.cache_priority(pigeonhole::Priority::Low),
        proto::Priority::Normal => f.cache_priority(pigeonhole::Priority::Normal),
        proto::Priority::High => f.cache_priority(pigeonhole::Priority::High),
    };
    f = match proto::CompactionStyle::try_from(s.compaction)
        .unwrap_or(proto::CompactionStyle::Unspecified)
    {
        proto::CompactionStyle::Unspecified => f,
        proto::CompactionStyle::Leveled => f.compaction(pigeonhole::Compaction::Leveled),
        proto::CompactionStyle::Tiered => f.compaction(pigeonhole::Compaction::Tiered),
        proto::CompactionStyle::FifoByTime => f.compaction(pigeonhole::Compaction::FifoByTime),
    };
    Ok(f)
}

/// A family as `DescribeTable` returns it: every option set, as stored.
pub(crate) fn family_out(i: &pigeonhole::FamilyInfo) -> proto::FamilySpec {
    use pigeonhole::Aggregation as A;
    use proto::AggregationKind as K;
    use proto::family_spec::Kind;
    let kind = if let Some(a) = &i.aggregation {
        let mut out = proto::Aggregation::default();
        out.kind = match a {
            A::Sum => K::Sum,
            A::Min => K::Min,
            A::Max => K::Max,
            A::Hll => K::Hll,
            A::BitOr => K::BitOr,
            A::BitAnd => K::BitAnd,
            A::BitXor => K::BitXor,
            A::MinF64 => K::MinF64,
            A::MaxF64 => K::MaxF64,
            A::SumF64 => K::SumF64,
            A::MeanVar => K::MeanVar,
            A::Set => K::Set,
            A::Histogram(p) => {
                out.histogram_bounds = p.bounds();
                K::Histogram
            }
            A::Quantiles(q) => {
                out.quantile_mantissa_bits = Some(u32::from(q.get_mantissa_bits()));
                out.quantile_max_buckets = Some(u32::from(q.get_max_buckets()));
                K::Quantiles
            }
            A::Bloom(b) => {
                out.bloom_bits = b.bits();
                out.bloom_hashes = u32::from(b.hashes());
                K::Bloom
            }
            _ => K::Unspecified,
        } as i32;
        Some(Kind::Aggregate(out))
    } else if let Some(v) = &i.vector {
        Some(Kind::Vector(proto::VectorSpec {
            dim: v.dim(),
            element: element_out(v.element()),
            metric: metric_out(v.metric()),
        }))
    } else {
        i.merge_operator.clone().map(Kind::MergeOperator)
    };
    let (codec, zstd_level) = match i.compression {
        pigeonhole::Compression::None => (proto::CompressionCodec::None, 0),
        pigeonhole::Compression::Lz4 => (proto::CompressionCodec::Lz4, 0),
        pigeonhole::Compression::Zstd(l) => (proto::CompressionCodec::Zstd, i32::from(l)),
        _ => (proto::CompressionCodec::Unspecified, 0),
    };
    proto::FamilySpec {
        name: i.name.clone(),
        kind,
        max_versions: Some(i.max_versions),
        ttl_micros: Some(
            i.ttl
                .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX)),
        ),
        bloom_bits: Some(u32::from(i.bloom_bits)),
        blob_threshold: Some(i.blob_threshold),
        compression: Some(proto::Compression {
            codec: codec as i32,
            zstd_level,
        }),
        block_size: Some(i.block_size),
        cache_priority: match i.cache_priority {
            pigeonhole::Priority::Low => proto::Priority::Low,
            pigeonhole::Priority::Normal => proto::Priority::Normal,
            pigeonhole::Priority::High => proto::Priority::High,
            _ => proto::Priority::Unspecified,
        } as i32,
        compaction: match i.compaction {
            pigeonhole::Compaction::Leveled => proto::CompactionStyle::Leveled,
            pigeonhole::Compaction::Tiered => proto::CompactionStyle::Tiered,
            pigeonhole::Compaction::FifoByTime => proto::CompactionStyle::FifoByTime,
            _ => proto::CompactionStyle::Unspecified,
        } as i32,
    }
}
