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
enum Elements {
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

/// Whether `v`'s data fits its element type and dimension.
pub(crate) fn check_vector(v: &proto::Vector) -> Result<()> {
    elements(v).map(|_| ())
}

fn value_of(v: Option<&proto::Value>) -> Result<&value::Kind> {
    v.and_then(|v| v.kind.as_ref())
        .ok_or_else(|| ApiError::invalid("a put needs a value"))
}

/// Applies `writes` to a row mutation.
pub(crate) fn mutation<'t>(
    mut m: RowMutation<'t>,
    writes: &[proto::Write],
) -> Result<RowMutation<'t>> {
    for w in writes {
        m = match w
            .op
            .as_ref()
            .ok_or_else(|| ApiError::invalid("a write needs an op"))?
        {
            write::Op::Put(p) => {
                let (f, q) = (p.family.as_str(), &p.qualifier[..]);
                match (value_of(p.value.as_ref())?, p.timestamp) {
                    (value::Kind::Bytes(b), None) => m.put(f, q, b),
                    (value::Kind::Bytes(b), Some(ts)) => m.put_at(f, q, ts, b),
                    (value::Kind::I64(n), None) => m.put_i64(f, q, *n),
                    (value::Kind::I64(n), Some(ts)) => m.put_i64_at(f, q, ts, *n),
                    (value::Kind::F64(x), None) => m.put_f64(f, q, *x),
                    (value::Kind::F64(x), Some(ts)) => m.put_f64_at(f, q, ts, *x),
                    (value::Kind::U64(n), None) => m.put_u64(f, q, *n),
                    (value::Kind::U64(n), Some(ts)) => m.put_u64_at(f, q, ts, *n),
                    (value::Kind::Vector(v), ts) => match (elements(v)?, ts) {
                        (Elements::F32(x), None) => m.put_vector_f32(f, q, &x),
                        (Elements::F32(x), Some(ts)) => m.put_vector_f32_at(f, q, ts, &x),
                        (Elements::I8(x), None) => m.put_vector_i8(f, q, &x),
                        (Elements::I8(x), Some(ts)) => m.put_vector_i8_at(f, q, ts, &x),
                        (Elements::Bits(x), None) => m.put_vector_bits(f, q, &x),
                        (Elements::Bits(x), Some(ts)) => m.put_vector_bits_at(f, q, ts, &x),
                        (Elements::F16(x), None) => m.put_vector_f16(f, q, &x),
                        (Elements::F16(x), Some(ts)) => m.put_vector_f16_at(f, q, ts, &x),
                        (Elements::Bf16(x), None) => m.put_vector_bf16(f, q, &x),
                        (Elements::Bf16(x), Some(ts)) => m.put_vector_bf16_at(f, q, ts, &x),
                    },
                }
            }
            write::Op::Incr(i) => match i.timestamp {
                None => m.incr(&i.family, &i.qualifier, i.delta),
                Some(ts) => m.incr_at(&i.family, &i.qualifier, ts, i.delta),
            },
            write::Op::Aggregate(a) => {
                let (f, q) = (a.family.as_str(), &a.qualifier[..]);
                match (value_of(a.input.as_ref())?, a.timestamp) {
                    (value::Kind::I64(n), None) => m.aggregate(f, q, *n),
                    (value::Kind::I64(n), Some(ts)) => m.aggregate_at(f, q, ts, *n),
                    (value::Kind::U64(n), None) => m.aggregate_u64(f, q, *n),
                    (value::Kind::U64(n), Some(ts)) => m.aggregate_u64_at(f, q, ts, *n),
                    (value::Kind::F64(x), None) => m.aggregate_f64(f, q, *x),
                    (value::Kind::F64(x), Some(ts)) => m.aggregate_f64_at(f, q, ts, *x),
                    (value::Kind::Bytes(b), None) => m.aggregate_bytes(f, q, b),
                    (value::Kind::Bytes(b), Some(ts)) => m.aggregate_bytes_at(f, q, ts, b),
                    (value::Kind::Vector(_), _) => {
                        return Err(ApiError::invalid(
                            "an aggregate input is an i64, u64, f64 or bytes",
                        ));
                    }
                }
            }
            write::Op::Merge(g) => m.merge(&g.family, &g.qualifier, &g.operand),
            write::Op::DeleteCell(d) => m.delete_cell(&d.family, &d.qualifier, d.timestamp),
            write::Op::DeleteColumn(d) => m.delete_column(&d.family, &d.qualifier),
            write::Op::DeleteFamily(d) => m.delete_family(&d.family),
            write::Op::DeleteRow(_) => m.delete_row(),
        };
    }
    Ok(m)
}

/// Applies one row's `writes` to a batch.
pub(crate) fn batch(
    b: &mut WriteBatch,
    table: &Table,
    row: &[u8],
    writes: &[proto::Write],
) -> Result<()> {
    for w in writes {
        match w
            .op
            .as_ref()
            .ok_or_else(|| ApiError::invalid("a write needs an op"))?
        {
            write::Op::Put(p) => {
                let (f, q) = (p.family.as_str(), &p.qualifier[..]);
                match (value_of(p.value.as_ref())?, p.timestamp) {
                    (value::Kind::Bytes(v), None) => b.put(table, row, f, q, v),
                    (value::Kind::Bytes(v), Some(ts)) => b.put_at(table, row, f, q, ts, v),
                    (value::Kind::I64(n), None) => b.put_i64(table, row, f, q, *n),
                    (value::Kind::I64(n), Some(ts)) => b.put_i64_at(table, row, f, q, ts, *n),
                    (value::Kind::F64(x), None) => b.put_f64(table, row, f, q, *x),
                    (value::Kind::F64(x), Some(ts)) => b.put_f64_at(table, row, f, q, ts, *x),
                    (value::Kind::U64(_) | value::Kind::Vector(_), _) => {
                        return Err(ApiError::invalid(
                            "a batch put takes bytes, i64 or f64 (use Mutate for u64 and vectors)",
                        ));
                    }
                };
            }
            write::Op::Incr(i) => {
                match i.timestamp {
                    None => b.incr(table, row, &i.family, &i.qualifier, i.delta),
                    Some(ts) => b.incr_at(table, row, &i.family, &i.qualifier, ts, i.delta),
                };
            }
            write::Op::Merge(g) => {
                b.merge(table, row, &g.family, &g.qualifier, &g.operand);
            }
            write::Op::DeleteCell(d) => {
                b.delete_cell(table, row, &d.family, &d.qualifier, d.timestamp);
            }
            write::Op::DeleteColumn(d) => {
                b.delete_column(table, row, &d.family, &d.qualifier);
            }
            write::Op::DeleteFamily(d) => {
                b.delete_family(table, row, &d.family);
            }
            write::Op::DeleteRow(_) => {
                b.delete_row(table, row);
            }
            write::Op::Aggregate(_) => {
                return Err(ApiError::invalid(
                    "a batch doesn't take aggregate inputs (use Mutate)",
                ));
            }
        }
    }
    Ok(())
}

/// Buffers one row's `writes` in a transaction: the same ops as a row mutation.
pub(crate) fn txn_writes(
    t: &mut pigeonhole::Transaction,
    table: &Table,
    row: &[u8],
    writes: &[proto::Write],
) -> Result<()> {
    for w in writes {
        match w
            .op
            .as_ref()
            .ok_or_else(|| ApiError::invalid("a write needs an op"))?
        {
            write::Op::Put(p) => {
                let (f, q) = (p.family.as_str(), &p.qualifier[..]);
                match (value_of(p.value.as_ref())?, p.timestamp) {
                    (value::Kind::Bytes(b), None) => t.put(table, row, f, q, b),
                    (value::Kind::Bytes(b), Some(ts)) => t.put_at(table, row, f, q, ts, b),
                    (value::Kind::I64(n), None) => t.put_i64(table, row, f, q, *n),
                    (value::Kind::I64(n), Some(ts)) => t.put_i64_at(table, row, f, q, ts, *n),
                    (value::Kind::F64(x), None) => t.put_f64(table, row, f, q, *x),
                    (value::Kind::F64(x), Some(ts)) => t.put_f64_at(table, row, f, q, ts, *x),
                    (value::Kind::U64(n), None) => t.put_u64(table, row, f, q, *n),
                    (value::Kind::U64(n), Some(ts)) => t.put_u64_at(table, row, f, q, ts, *n),
                    (value::Kind::Vector(v), ts) => match (elements(v)?, ts) {
                        (Elements::F32(x), None) => t.put_vector_f32(table, row, f, q, &x),
                        (Elements::F32(x), Some(ts)) => {
                            t.put_vector_f32_at(table, row, f, q, ts, &x)
                        }
                        (Elements::I8(x), None) => t.put_vector_i8(table, row, f, q, &x),
                        (Elements::I8(x), Some(ts)) => t.put_vector_i8_at(table, row, f, q, ts, &x),
                        (Elements::Bits(x), None) => t.put_vector_bits(table, row, f, q, &x),
                        (Elements::Bits(x), Some(ts)) => {
                            t.put_vector_bits_at(table, row, f, q, ts, &x)
                        }
                        (Elements::F16(x), None) => t.put_vector_f16(table, row, f, q, &x),
                        (Elements::F16(x), Some(ts)) => {
                            t.put_vector_f16_at(table, row, f, q, ts, &x)
                        }
                        (Elements::Bf16(x), None) => t.put_vector_bf16(table, row, f, q, &x),
                        (Elements::Bf16(x), Some(ts)) => {
                            t.put_vector_bf16_at(table, row, f, q, ts, &x)
                        }
                    },
                };
            }
            write::Op::Incr(i) => {
                match i.timestamp {
                    None => t.incr(table, row, &i.family, &i.qualifier, i.delta),
                    Some(ts) => t.incr_at(table, row, &i.family, &i.qualifier, ts, i.delta),
                };
            }
            write::Op::Aggregate(a) => {
                let (f, q) = (a.family.as_str(), &a.qualifier[..]);
                match (value_of(a.input.as_ref())?, a.timestamp) {
                    (value::Kind::I64(n), None) => t.aggregate(table, row, f, q, *n),
                    (value::Kind::I64(n), Some(ts)) => t.aggregate_at(table, row, f, q, ts, *n),
                    (value::Kind::U64(n), None) => t.aggregate_u64(table, row, f, q, *n),
                    (value::Kind::U64(n), Some(ts)) => t.aggregate_u64_at(table, row, f, q, ts, *n),
                    (value::Kind::F64(x), None) => t.aggregate_f64(table, row, f, q, *x),
                    (value::Kind::F64(x), Some(ts)) => t.aggregate_f64_at(table, row, f, q, ts, *x),
                    (value::Kind::Bytes(b), None) => t.aggregate_bytes(table, row, f, q, b),
                    (value::Kind::Bytes(b), Some(ts)) => {
                        t.aggregate_bytes_at(table, row, f, q, ts, b)
                    }
                    (value::Kind::Vector(_), _) => {
                        return Err(ApiError::invalid(
                            "an aggregate input is an i64, u64, f64 or bytes",
                        ));
                    }
                };
            }
            write::Op::Merge(g) => {
                t.merge(table, row, &g.family, &g.qualifier, &g.operand);
            }
            write::Op::DeleteCell(d) => {
                t.delete_cell(table, row, &d.family, &d.qualifier, d.timestamp);
            }
            write::Op::DeleteColumn(d) => {
                t.delete_column(table, row, &d.family, &d.qualifier);
            }
            write::Op::DeleteFamily(d) => {
                t.delete_family(table, row, &d.family);
            }
            write::Op::DeleteRow(_) => {
                t.delete_row(table, row);
            }
        }
    }
    Ok(())
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
