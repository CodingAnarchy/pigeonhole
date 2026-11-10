//! Tables: family options, the table builder, reads, and writes recorded as operations that
//! one engine call applies (a row mutation, a batch or a transaction).
//!
//! The methods here are the native core: positional arguments, `nil` for "not given". The
//! public, keyword-taking methods are Ruby (`lib/pigeonhole/api.rb`).

use std::sync::Mutex;

use magnus::{Error, Float, Integer, RArray, RHash, RString, Ruby, Symbol, Value, prelude::*};
use pigeonhole::{Aggregation, Compression, ErrorCode};

use crate::classes::{
    Asked, Builder, Cell, Column, Condition, Family, FamilyInfo, Incremented, Mutation,
    MutationState, Row, ScanArgs, ScanSpec, Seen, Snapshot, Table, TableBuilder, VectorSpec,
};
use crate::convert::{
    self, Bytes, CellValue, Name, Secs, Val, arg_error, class_name, lock, type_error,
};
use crate::error::{OrRb, engine, returned_value, spent, with_code};
use crate::fork::ForkSafe;
use crate::gvl::without_gvl;
use crate::read::snapshot_arg;

/// How durable a commit is before it returns: `:none`, `:buffered`, `:group_sync` or `:sync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Durability {
    None_,
    Buffered,
    GroupSync,
    Sync,
}

impl From<Durability> for pigeonhole::Durability {
    fn from(d: Durability) -> Self {
        match d {
            Durability::None_ => Self::None,
            Durability::Buffered => Self::Buffered,
            Durability::GroupSync => Self::GroupSync,
            Durability::Sync => Self::Sync,
        }
    }
}

// ---- Family -------------------------------------------------------------------------------

/// `Family.new(max_versions:, ttl:, bloom_bits:, block_size:, blob_threshold:, compression:,
/// zstd_level:)`: a plain family.
#[allow(clippy::too_many_arguments)]
pub(crate) fn family_new(
    ruby: &Ruby,
    max_versions: Option<u32>,
    ttl: Option<Secs>,
    bloom_bits: Option<u8>,
    block_size: Option<u32>,
    blob_threshold: Option<u32>,
    compression: Option<Name>,
    zstd_level: Option<i8>,
) -> Result<Family, Error> {
    options(
        ruby,
        pigeonhole::Family::default(),
        max_versions,
        ttl,
        bloom_bits,
        block_size,
        blob_threshold,
        compression,
        zstd_level,
    )
}

/// `Family.counter(max_versions:, ttl:)`: `incr` adds to its cells.
pub(crate) fn family_counter(
    ruby: &Ruby,
    max_versions: Option<u32>,
    ttl: Option<Secs>,
) -> Result<Family, Error> {
    options(
        ruby,
        pigeonhole::Family::counter(),
        max_versions,
        ttl,
        None,
        None,
        None,
        None,
        None,
    )
}

/// `Family.vector(dim, element, metric, max_versions:, ttl:)`: a vector family (D303).
pub(crate) fn family_vector(
    ruby: &Ruby,
    dim: u32,
    element: Value,
    metric: Value,
    max_versions: Option<u32>,
    ttl: Option<Secs>,
) -> Result<Family, Error> {
    let spec = crate::vector::spec(ruby, dim, element, metric)?;
    options(
        ruby,
        pigeonhole::Family::vector(spec),
        max_versions,
        ttl,
        None,
        None,
        None,
        None,
        None,
    )
}

/// `Family.aggregate(kind, bounds:, mantissa_bits:, max_buckets:, bits:, hashes:,
/// max_versions:, ttl:)`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn family_aggregate(
    ruby: &Ruby,
    kind: Name,
    bounds: Option<Vec<f64>>,
    mantissa_bits: Option<u8>,
    max_buckets: Option<u16>,
    bits: Option<u32>,
    hashes: Option<u8>,
    max_versions: Option<u32>,
    ttl: Option<Secs>,
) -> Result<Family, Error> {
    let a = match kind.0.as_str() {
        "sum" => Aggregation::Sum,
        "min" => Aggregation::Min,
        "max" => Aggregation::Max,
        "hll" => Aggregation::Hll,
        "bit_or" => Aggregation::BitOr,
        "bit_and" => Aggregation::BitAnd,
        "bit_xor" => Aggregation::BitXor,
        "min_f64" => Aggregation::MinF64,
        "max_f64" => Aggregation::MaxF64,
        "sum_f64" => Aggregation::SumF64,
        "mean_var" => Aggregation::MeanVar,
        "histogram" => {
            let bounds = bounds.ok_or_else(|| arg_error(ruby, "a histogram needs its bounds:"))?;
            Aggregation::Histogram(pigeonhole::HistogramParams::new(bounds).or_rb(ruby)?)
        }
        "quantiles" => {
            let mut q = pigeonhole::QuantileParams::default();
            if let Some(p) = mantissa_bits {
                q = q.mantissa_bits(p);
            }
            if let Some(n) = max_buckets {
                q = q.max_buckets(n);
            }
            Aggregation::Quantiles(q)
        }
        "bloom" => {
            let (Some(bits), Some(hashes)) = (bits, hashes) else {
                return Err(arg_error(ruby, "a bloom family needs bits: and hashes:"));
            };
            Aggregation::Bloom(pigeonhole::BloomParams::new(bits, hashes).or_rb(ruby)?)
        }
        other => return Err(arg_error(ruby, format!("unknown aggregation {other:?}"))),
    };
    options(
        ruby,
        pigeonhole::Family::aggregate(a),
        max_versions,
        ttl,
        None,
        None,
        None,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn options(
    ruby: &Ruby,
    mut f: pigeonhole::Family,
    max_versions: Option<u32>,
    ttl: Option<Secs>,
    bloom_bits: Option<u8>,
    block_size: Option<u32>,
    blob_threshold: Option<u32>,
    compression: Option<Name>,
    zstd_level: Option<i8>,
) -> Result<Family, Error> {
    if let Some(n) = max_versions {
        f = f.max_versions(n);
    }
    if let Some(t) = ttl {
        f = f.ttl(t.0);
    }
    if let Some(n) = bloom_bits {
        f = f.bloom_bits(n);
    }
    if let Some(n) = block_size {
        f = f.block_size(n);
    }
    if let Some(n) = blob_threshold {
        f = f.blob_threshold(n);
    }
    f = match (compression.as_ref().map(|c| c.0.as_str()), zstd_level) {
        (None, None) => f,
        (Some("lz4"), None) => f.compression(Compression::Lz4),
        (Some("none"), None) => f.compression(Compression::None),
        (Some("zstd"), level) => f.compression(Compression::Zstd(level.unwrap_or(3))),
        (None | Some(_), Some(_)) => {
            return Err(arg_error(ruby, "zstd_level: is for compression: :zstd"));
        }
        (Some(other), None) => {
            return Err(arg_error(ruby, format!("unknown compression {other:?}")));
        }
    };
    Ok(Family(f))
}

// ---- FamilyInfo ---------------------------------------------------------------------------

impl FamilyInfo {
    /// The family's name.
    pub(crate) fn name(&self) -> String {
        self.0.name.clone()
    }

    /// Whether it is a counter (`sum`) family, or a standard family naming the built-in
    /// `i64` add operator.
    pub(crate) fn counter(&self) -> bool {
        self.0.counter
    }

    /// The aggregation kind (`:sum`, `:min`, ...), or `nil` for a plain family.
    pub(crate) fn aggregation(ruby: &Ruby, rb_self: &Self) -> Option<Symbol> {
        rb_self
            .0
            .aggregation
            .as_ref()
            .map(|a| ruby.to_symbol(aggregation_name(a)))
    }

    /// Versions kept per column (0 keeps all).
    pub(crate) fn max_versions(&self) -> u32 {
        self.0.max_versions
    }

    /// How long cells live after their timestamp, in seconds, or `nil`.
    pub(crate) fn ttl(&self) -> Option<f64> {
        self.0.ttl.map(|d| d.as_secs_f64())
    }

    /// Bloom filter bits per key.
    pub(crate) fn bloom_bits(&self) -> u8 {
        self.0.bloom_bits
    }

    /// Values longer than this are stored in blob files.
    pub(crate) fn blob_threshold(&self) -> u32 {
        self.0.blob_threshold
    }

    /// The data block size.
    pub(crate) fn block_size(&self) -> u32 {
        self.0.block_size
    }

    /// `:lz4`, `:zstd` or `:none`.
    pub(crate) fn compression(ruby: &Ruby, rb_self: &Self) -> Symbol {
        ruby.to_symbol(match rb_self.0.compression {
            Compression::None => "none",
            Compression::Zstd(_) => "zstd",
            Compression::Lz4 => "lz4",
            _ => "unknown",
        })
    }

    /// The zstd level, with `compression == :zstd`.
    pub(crate) fn zstd_level(&self) -> Option<i8> {
        match self.0.compression {
            Compression::Zstd(l) => Some(l),
            _ => None,
        }
    }

    /// The merge operator a standard family names, or `nil`.
    pub(crate) fn merge_operator(&self) -> Option<String> {
        self.0.merge_operator.clone()
    }

    /// `:low`, `:normal` or `:high`: the family's block cache priority.
    pub(crate) fn cache_priority(ruby: &Ruby, rb_self: &Self) -> Symbol {
        ruby.to_symbol(match rb_self.0.cache_priority {
            pigeonhole::Priority::Low => "low",
            pigeonhole::Priority::Normal => "normal",
            pigeonhole::Priority::High => "high",
            _ => "unknown",
        })
    }

    /// `:leveled`, `:tiered` or `:fifo_by_time`.
    pub(crate) fn compaction(ruby: &Ruby, rb_self: &Self) -> Symbol {
        ruby.to_symbol(match rb_self.0.compaction {
            pigeonhole::Compaction::Leveled => "leveled",
            pigeonhole::Compaction::Tiered => "tiered",
            pigeonhole::Compaction::FifoByTime => "fifo_by_time",
            _ => "unknown",
        })
    }

    /// A vector family's spec, or `nil`.
    pub(crate) fn vector(&self) -> Option<VectorSpec> {
        self.0.vector.map(VectorSpec)
    }

    /// The family's options as a `Family`, to create the same family elsewhere.
    pub(crate) fn family(&self) -> Family {
        Family(self.0.family.clone())
    }

    /// `#<Pigeonhole::FamilyInfo name=... aggregation=... max_versions=...>`.
    pub(crate) fn inspect(&self) -> String {
        format!(
            "#<Pigeonhole::FamilyInfo name={:?} aggregation={} max_versions={}>",
            self.0.name,
            self.0
                .aggregation
                .as_ref()
                .map_or("nil".to_owned(), |a| format!(":{}", aggregation_name(a))),
            self.0.max_versions
        )
    }
}

fn aggregation_name(a: &Aggregation) -> &'static str {
    match a {
        Aggregation::Sum => "sum",
        Aggregation::Min => "min",
        Aggregation::Max => "max",
        Aggregation::Hll => "hll",
        Aggregation::BitOr => "bit_or",
        Aggregation::BitAnd => "bit_and",
        Aggregation::BitXor => "bit_xor",
        Aggregation::MinF64 => "min_f64",
        Aggregation::MaxF64 => "max_f64",
        Aggregation::SumF64 => "sum_f64",
        Aggregation::MeanVar => "mean_var",
        Aggregation::Histogram(_) => "histogram",
        Aggregation::Quantiles(_) => "quantiles",
        Aggregation::Bloom(_) => "bloom",
        _ => "unknown",
    }
}

// ---- Condition ----------------------------------------------------------------------------

impl Condition {
    /// The column has no live cell.
    pub(crate) fn absent(family: Name, qualifier: Bytes) -> Self {
        Self(pigeonhole::Condition::Absent {
            family: family.0,
            qualifier: qualifier.0,
        })
    }

    /// The column has a live cell.
    pub(crate) fn exists(family: Name, qualifier: Bytes) -> Self {
        Self(pigeonhole::Condition::Exists {
            family: family.0,
            qualifier: qualifier.0,
        })
    }

    /// The column's newest value is exactly `value` (bytes).
    pub(crate) fn equals(family: Name, qualifier: Bytes, value: Bytes) -> Self {
        Self(pigeonhole::Condition::Value {
            family: family.0,
            qualifier: qualifier.0,
            filter: pigeonhole::ValueFilter::Equals(value.0),
        })
    }
}

// ---- Recorded writes ----------------------------------------------------------------------

/// One write, recorded: the row it applies to is the mutation's, the batch op's or the
/// transaction op's.
#[derive(Debug, Clone)]
pub(crate) enum Op {
    Put(String, Vec<u8>, Option<u64>, Val),
    PutU64(String, Vec<u8>, Option<u64>, u64),
    Incr(String, Vec<u8>, Option<u64>, i64),
    Aggregate(String, Vec<u8>, Option<u64>, Input),
    PutVector(String, Vec<u8>, Option<u64>, crate::vector::Vector),
    DeleteRow,
    DeleteFamily(String),
    DeleteColumn(String, Vec<u8>),
    DeleteCell(String, Vec<u8>, u64),
}

/// An aggregate family's input, by the family's kind.
#[derive(Debug, Clone)]
pub(crate) enum Input {
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
}

/// `v` as an input to `table`'s `family`, read by the family's kind: a Numeric into an
/// `f64` family is a float; into a bitwise one an Integer mask; into `hll` or `bloom` an
/// Integer or a String element; otherwise an Integer.
pub(crate) fn aggregate_input(
    ruby: &Ruby,
    table: &pigeonhole::Table,
    family: &str,
    v: Value,
) -> Result<Input, Error> {
    let t = table.clone();
    let f = family.to_owned();
    let info = without_gvl(ruby, move || t.family_info(&f))?.ok_or_else(|| {
        with_code(
            ruby,
            ErrorCode::FamilyNotFound,
            &format!("table {} has no family {family}", table.name()),
        )
    })?;
    let bad = |want: &str| {
        type_error(
            ruby,
            format!("family {family} takes {want}, not {}", class_name(v)),
        )
    };
    let int = Integer::from_value(v);
    Ok(match info.aggregation {
        None => {
            return Err(type_error(
                ruby,
                format!(
                    "aggregate writes to an aggregate family, and {family} is a plain family: use put"
                ),
            ));
        }
        Some(
            Aggregation::MinF64
            | Aggregation::MaxF64
            | Aggregation::SumF64
            | Aggregation::MeanVar
            | Aggregation::Histogram(_)
            | Aggregation::Quantiles(_),
        ) => match (int, Float::from_value(v)) {
            (Some(n), _) => Input::F64(f64::try_convert(n.as_value())?),
            (None, Some(x)) => Input::F64(x.to_f64()),
            (None, None) => return Err(bad("a Numeric")),
        },
        Some(Aggregation::BitOr | Aggregation::BitAnd | Aggregation::BitXor) => match int {
            Some(n) => Input::U64(n.to_u64()?),
            None => return Err(bad("an Integer (a 64-bit mask)")),
        },
        Some(Aggregation::Hll | Aggregation::Bloom(_)) => match (int, RString::from_value(v)) {
            (Some(n), _) => Input::I64(n.to_i64()?),
            (None, Some(s)) => Input::Bytes(s.to_bytes().to_vec()),
            (None, None) => return Err(bad("an Integer or a String")),
        },
        Some(_) => match int {
            Some(n) => Input::I64(n.to_i64()?),
            None => return Err(bad("an Integer")),
        },
    })
}

/// `v` as a vector for `table`'s `family`, read by the family's element type.
pub(crate) fn vector_input(
    ruby: &Ruby,
    table: &pigeonhole::Table,
    family: &str,
    v: Value,
) -> Result<crate::vector::Vector, Error> {
    let spec = crate::vector::family_spec(
        ruby,
        &Handle::Writer(table.clone()),
        family,
        "put_vector writes to",
    )?;
    crate::vector::vector(ruby, v, spec.element())
}

/// A delete's scope: the row; with `family`, that family; with `qualifier` too, the column;
/// with `ts` too, that one version.
pub(crate) fn delete_op(
    ruby: &Ruby,
    family: Option<Name>,
    qualifier: Option<Bytes>,
    ts: Option<u64>,
) -> Result<Op, Error> {
    Ok(
        match (family.map(|f| f.0), convert::opt_bytes(qualifier), ts) {
            (None, None, None) => Op::DeleteRow,
            (Some(f), None, None) => Op::DeleteFamily(f),
            (Some(f), Some(q), None) => Op::DeleteColumn(f, q),
            (Some(f), Some(q), Some(ts)) => Op::DeleteCell(f, q, ts),
            _ => {
                return Err(arg_error(
                    ruby,
                    "delete: a qualifier needs its family, and ts: needs both",
                ));
            }
        },
    )
}

/// Applies `ops` to `table`'s `row` as one row mutation and commits it (with `condition`,
/// conditionally). Run without the GVL.
pub(crate) fn commit_row(
    table: &pigeonhole::Table,
    row: &[u8],
    ops: &[Op],
    asked: &Asked,
    durability: Option<Durability>,
    condition: Option<&pigeonhole::Condition>,
) -> pigeonhole::Result<Option<u64>> {
    // A mutation with `return_previous` or `return_current` requests is refused here
    // (ICR 0113).
    let m = row_mutation(table, row, ops, asked, durability);
    match condition {
        None => m.commit().map(|i| Some(i.seqno)),
        Some(c) => m.commit_if(c).map(|i| i.map(|i| i.seqno)),
    }
}

/// Commits `ops` like [`commit_row`] and returns every counter cell they incremented or set,
/// as the commit left it (D314, ICR 0113). Run without the GVL.
pub(crate) fn commit_row_returning(
    table: &pigeonhole::Table,
    row: &[u8],
    ops: &[Op],
    asked: &Asked,
    durability: Option<Durability>,
) -> pigeonhole::Result<pigeonhole::Incremented> {
    row_mutation(table, row, ops, asked, durability).commit_returning()
}

/// The column a condition reads, which `commit_if_returning` always reports.
fn condition_column(c: &pigeonhole::Condition) -> Option<Column> {
    use pigeonhole::Condition as C;
    match c {
        C::Exists { family, qualifier }
        | C::Absent { family, qualifier }
        | C::Value {
            family, qualifier, ..
        } => Some((family.clone(), qualifier.clone())),
        // `Condition` is non-exhaustive. A variant added later that reads a column must be
        // named here, or `current` treats its column as unasked: `InvalidArgument`, not
        // `ReturnedValueError` (ai1249's nit on #1579).
        _ => None,
    }
}

/// `ops` buffered on a row mutation of `row`, uncommitted.
fn row_mutation<'t>(
    table: &'t pigeonhole::Table,
    row: &[u8],
    ops: &[Op],
    asked: &Asked,
    durability: Option<Durability>,
) -> pigeonhole::RowMutation<'t> {
    let mut m = table.mutate(row);
    if let Some(d) = durability {
        m = m.durability(d.into());
    }
    for op in ops {
        m = match op {
            Op::Put(f, q, None, Val::Bytes(b)) => m.put(f, q, b),
            Op::Put(f, q, Some(ts), Val::Bytes(b)) => m.put_at(f, q, *ts, b),
            Op::Put(f, q, None, Val::I64(n)) => m.put_i64(f, q, *n),
            Op::Put(f, q, Some(ts), Val::I64(n)) => m.put_i64_at(f, q, *ts, *n),
            Op::Put(f, q, None, Val::F64(x)) => m.put_f64(f, q, *x),
            Op::Put(f, q, Some(ts), Val::F64(x)) => m.put_f64_at(f, q, *ts, *x),
            Op::PutU64(f, q, None, n) => m.put_u64(f, q, *n),
            Op::PutU64(f, q, Some(ts), n) => m.put_u64_at(f, q, *ts, *n),
            Op::Incr(f, q, None, d) => m.incr(f, q, *d),
            Op::Incr(f, q, Some(ts), d) => m.incr_at(f, q, *ts, *d),
            Op::Aggregate(f, q, None, Input::I64(n)) => m.aggregate(f, q, *n),
            Op::Aggregate(f, q, Some(ts), Input::I64(n)) => m.aggregate_at(f, q, *ts, *n),
            Op::Aggregate(f, q, None, Input::U64(n)) => m.aggregate_u64(f, q, *n),
            Op::Aggregate(f, q, Some(ts), Input::U64(n)) => m.aggregate_u64_at(f, q, *ts, *n),
            Op::Aggregate(f, q, None, Input::F64(x)) => m.aggregate_f64(f, q, *x),
            Op::Aggregate(f, q, Some(ts), Input::F64(x)) => m.aggregate_f64_at(f, q, *ts, *x),
            Op::Aggregate(f, q, None, Input::Bytes(b)) => m.aggregate_bytes(f, q, b),
            Op::Aggregate(f, q, Some(ts), Input::Bytes(b)) => m.aggregate_bytes_at(f, q, *ts, b),
            Op::PutVector(f, q, ts, v) => {
                use crate::vector::Vector;
                match (v, ts) {
                    (Vector::F32(x), None) => m.put_vector_f32(f, q, x),
                    (Vector::F32(x), Some(ts)) => m.put_vector_f32_at(f, q, *ts, x),
                    (Vector::I8(x), None) => m.put_vector_i8(f, q, x),
                    (Vector::I8(x), Some(ts)) => m.put_vector_i8_at(f, q, *ts, x),
                    (Vector::Bits(x), None) => m.put_vector_bits(f, q, x),
                    (Vector::Bits(x), Some(ts)) => m.put_vector_bits_at(f, q, *ts, x),
                }
            }
            Op::DeleteRow => m.delete_row(),
            Op::DeleteFamily(f) => m.delete_family(f),
            Op::DeleteColumn(f, q) => m.delete_column(f, q),
            Op::DeleteCell(f, q, ts) => m.delete_cell(f, q, *ts),
        };
    }
    for (f, q) in &asked.previous {
        m = m.return_previous(f, q);
    }
    for (f, q) in &asked.current {
        m = m.return_current(f, q);
    }
    m
}

// ---- Table --------------------------------------------------------------------------------

/// A table handle, as the writer or as a reader process.
#[derive(Clone)]
pub(crate) enum Handle {
    Writer(pigeonhole::Table),
    Reader(pigeonhole::ReadTable),
}

/// Runs `$body` with `$t` bound to either kind of table.
macro_rules! either {
    ($h:expr, |$t:ident| $body:expr) => {
        match $h {
            $crate::table::Handle::Writer($t) => $body,
            $crate::table::Handle::Reader($t) => $body,
        }
    };
}
pub(crate) use either;

impl Handle {
    /// The writer's table, or `ReadOnly` for a reader process's.
    pub(crate) fn writer(&self, ruby: &Ruby) -> Result<&pigeonhole::Table, Error> {
        match self {
            Self::Writer(t) => Ok(t),
            Self::Reader(_) => Err(with_code(
                ruby,
                ErrorCode::ReadOnly,
                "a reader process's table can't be written",
            )),
        }
    }
}

impl Table {
    /// The handle, or `Unsupported` in a forked child.
    pub(crate) fn handle(&self, ruby: &Ruby) -> Result<&Handle, Error> {
        self.0.get(ruby)
    }

    /// The writer's table (`ReadOnly` for a reader's).
    pub(crate) fn writer(&self, ruby: &Ruby) -> Result<&pigeonhole::Table, Error> {
        self.handle(ruby)?.writer(ruby)
    }

    /// The table's name.
    pub(crate) fn name(ruby: &Ruby, rb_self: &Self) -> Result<String, Error> {
        Ok(either!(rb_self.handle(ruby)?, |t| t.name().to_owned()))
    }

    /// The family names, in creation order.
    pub(crate) fn families(ruby: &Ruby, rb_self: &Self) -> Result<Vec<String>, Error> {
        let h = rb_self.handle(ruby)?.clone();
        without_gvl(ruby, move || {
            either!(&h, |t| t
                .families()
                .into_iter()
                .map(str::to_owned)
                .collect())
        })
    }

    /// A family's stored options, or `nil` for no such family.
    pub(crate) fn family_info(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
    ) -> Result<Option<FamilyInfo>, Error> {
        let h = rb_self.handle(ruby)?.clone();
        Ok(without_gvl(ruby, move || either!(&h, |t| t.family_info(&family.0)))?.map(FamilyInfo))
    }

    /// `get(row, family, qualifier, snapshot)`: the newest version of one cell, or `nil`.
    pub(crate) fn get(
        ruby: &Ruby,
        rb_self: &Self,
        row: Bytes,
        family: Name,
        qualifier: Bytes,
        snapshot: Option<&Snapshot>,
    ) -> Result<Option<Cell>, Error> {
        let snap = snapshot_arg(ruby, snapshot)?;
        let h = rb_self.handle(ruby)?.clone();
        let (row, family, q) = (row.0, family.0, qualifier.0);
        Ok(engine(ruby, move || {
            either!(&h, |t| match &snap {
                Some(s) => t
                    .get_at(s, &row, &family, &q)
                    .map(|c| c.map(|c| c.to_owned())),
                None => t.get(&row, &family, &q).map(|c| c.map(|c| c.to_owned())),
            })
        })?
        .map(Cell))
    }

    /// `row(row, families, qualifier_prefix, versions, column_limit, snapshot)`: one row's
    /// cells, or `nil` when the row has none the read selects.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn row(
        ruby: &Ruby,
        rb_self: &Self,
        row: Bytes,
        families: Option<RArray>,
        qualifier_prefix: Option<Bytes>,
        versions: u32,
        column_limit: Option<u32>,
        snapshot: Option<&Snapshot>,
    ) -> Result<Option<Row>, Error> {
        let families = convert::names(families)?;
        let qp = convert::opt_bytes(qualifier_prefix);
        let snap = snapshot_arg(ruby, snapshot)?;
        let h = rb_self.handle(ruby)?.clone();
        let row = row.0;
        Ok(engine(ruby, move || {
            either!(&h, |t| {
                let mut r = t.row(&row).versions(versions);
                if let Some(f) = &families {
                    r = r.families(f.iter().map(String::as_str));
                }
                if let Some(q) = &qp {
                    r = r.qualifier_prefix(q);
                }
                if let Some(n) = column_limit {
                    r = r.column_limit(n);
                }
                if let Some(s) = &snap {
                    r = r.snapshot(s);
                }
                r.read().map(|r| r.map(|r| r.to_owned()))
            })
        })?
        .map(Row))
    }

    /// `scan_spec(prefix, start, end, families, qualifier_prefix, versions, limit,
    /// column_limit, chunk)`: a scan's arguments, checked now; each enumeration opens its
    /// own cursor from them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_spec(
        ruby: &Ruby,
        rb_self: &Self,
        prefix: Option<Bytes>,
        start: Option<Bytes>,
        end: Option<Bytes>,
        families: Option<RArray>,
        qp: Option<Bytes>,
        versions: u32,
        limit: Option<u64>,
        column_limit: Option<u32>,
        chunk: usize,
    ) -> Result<ScanSpec, Error> {
        let (prefix, start, end) = (
            convert::opt_bytes(prefix),
            convert::opt_bytes(start),
            convert::opt_bytes(end),
        );
        if prefix.is_some() && (start.is_some() || end.is_some()) {
            return Err(arg_error(
                ruby,
                "scan takes a prefix or start:/end:, not both",
            ));
        }
        if chunk == 0 {
            return Err(arg_error(ruby, "chunk: must be at least 1"));
        }
        Ok(ScanSpec(ForkSafe::new(ScanArgs {
            table: rb_self.handle(ruby)?.clone(),
            prefix,
            start,
            end,
            families: convert::names(families)?,
            qualifier_prefix: convert::opt_bytes(qp),
            versions,
            limit,
            column_limit,
            chunk,
        })))
    }

    /// `mutate(row)`: a `Mutation` recording writes to `row`, applied by its `commit`.
    pub(crate) fn mutate(ruby: &Ruby, rb_self: &Self, row: Bytes) -> Result<Mutation, Error> {
        Ok(Mutation(ForkSafe::new(MutationState {
            table: rb_self.writer(ruby)?.clone(),
            row: row.0,
            ops: Mutex::new(Some(Vec::new())),
            asked: Mutex::new(Asked::default()),
            durability: Mutex::new(None),
        })))
    }

    /// One write committed alone; returns the commit's sequence number.
    fn commit_one(
        ruby: &Ruby,
        rb_self: &Self,
        row: Vec<u8>,
        op: Op,
        durability: Option<Durability>,
    ) -> Result<u64, Error> {
        let t = rb_self.writer(ruby)?.clone();
        engine(ruby, move || {
            commit_row(&t, &row, &[op], &Asked::default(), durability, None)
        })
        .map(|s| s.expect("unconditional"))
    }

    /// `put(row, family, qualifier, value, ts, durability)`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn put(
        ruby: &Ruby,
        rb_self: &Self,
        row: Bytes,
        family: Name,
        qualifier: Bytes,
        value: CellValue,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> Result<u64, Error> {
        let op = Op::Put(family.0, qualifier.0, ts, value.0);
        Self::commit_one(ruby, rb_self, row.0, op, durability)
    }

    /// `incr(row, family, qualifier, delta, ts, durability)`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn incr(
        ruby: &Ruby,
        rb_self: &Self,
        row: Bytes,
        family: Name,
        qualifier: Bytes,
        delta: i64,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> Result<u64, Error> {
        let op = Op::Incr(family.0, qualifier.0, ts, delta);
        Self::commit_one(ruby, rb_self, row.0, op, durability)
    }

    /// `incr_returning(row, family, qualifier, delta, ts, durability)`: the counter's value
    /// after the commit (D314); `ReturnedValueError` if the commit applied but the read failed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn incr_returning(
        ruby: &Ruby,
        rb_self: &Self,
        row: Bytes,
        family: Name,
        qualifier: Bytes,
        delta: i64,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> Result<Option<i64>, Error> {
        let t = rb_self.writer(ruby)?.clone();
        // A failed commit raises its own exception; a failed read after it, a
        // `ReturnedValueError` (perf2's S1 on #1473).
        let (seqno, value) = engine(ruby, move || {
            let op = Op::Incr(family.0.clone(), qualifier.0.clone(), ts, delta);
            let r = commit_row_returning(&t, &row.0, &[op], &Asked::default(), durability)?;
            Ok((
                r.info.seqno,
                r.value_at(&family.0, &qualifier.0, ts.unwrap_or(0)),
            ))
        })?;
        value.map_err(|e| returned_value(ruby, seqno, &e))
    }

    /// `delete(row, family, qualifier, ts, durability)`.
    pub(crate) fn delete(
        ruby: &Ruby,
        rb_self: &Self,
        row: Bytes,
        family: Option<Name>,
        qualifier: Option<Bytes>,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> Result<u64, Error> {
        let op = delete_op(ruby, family, qualifier, ts)?;
        Self::commit_one(ruby, rb_self, row.0, op, durability)
    }

    /// `#<Pigeonhole::Table "name">`.
    pub(crate) fn inspect(ruby: &Ruby, rb_self: &Self) -> Result<String, Error> {
        Ok(format!(
            "#<Pigeonhole::Table {:?}>",
            Self::name(ruby, rb_self)?
        ))
    }
}

// ---- Mutation -----------------------------------------------------------------------------

impl Mutation {
    fn push(ruby: &Ruby, rb_self: &Self, op: Op) -> Result<(), Error> {
        let m = rb_self.0.get(ruby)?;
        match lock(&m.ops).as_mut() {
            Some(ops) => {
                ops.push(op);
                Ok(())
            }
            None => Err(spent(ruby, "the mutation was already committed")),
        }
    }

    /// `put(family, qualifier, value, ts)`.
    pub(crate) fn put(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
        value: CellValue,
        ts: Option<u64>,
    ) -> Result<(), Error> {
        Self::push(ruby, rb_self, Op::Put(family.0, qualifier.0, ts, value.0))
    }

    /// `put_u64(family, qualifier, value, ts)`.
    pub(crate) fn put_u64(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
        value: u64,
        ts: Option<u64>,
    ) -> Result<(), Error> {
        Self::push(ruby, rb_self, Op::PutU64(family.0, qualifier.0, ts, value))
    }

    /// `incr(family, qualifier, delta, ts)`.
    pub(crate) fn incr(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
        delta: i64,
        ts: Option<u64>,
    ) -> Result<(), Error> {
        Self::push(ruby, rb_self, Op::Incr(family.0, qualifier.0, ts, delta))
    }

    /// `aggregate(family, qualifier, value, ts)`.
    pub(crate) fn aggregate(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
        value: Value,
        ts: Option<u64>,
    ) -> Result<(), Error> {
        let input = aggregate_input(ruby, &rb_self.0.get(ruby)?.table, &family.0, value)?;
        Self::push(
            ruby,
            rb_self,
            Op::Aggregate(family.0, qualifier.0, ts, input),
        )
    }

    /// `put_vector(family, qualifier, vector, ts)`: read by the family's element type.
    pub(crate) fn put_vector(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
        vector: Value,
        ts: Option<u64>,
    ) -> Result<(), Error> {
        let v = vector_input(ruby, &rb_self.0.get(ruby)?.table, &family.0, vector)?;
        Self::push(ruby, rb_self, Op::PutVector(family.0, qualifier.0, ts, v))
    }

    /// `delete(family, qualifier, ts)`.
    pub(crate) fn delete(
        ruby: &Ruby,
        rb_self: &Self,
        family: Option<Name>,
        qualifier: Option<Bytes>,
        ts: Option<u64>,
    ) -> Result<(), Error> {
        let op = delete_op(ruby, family, qualifier, ts)?;
        Self::push(ruby, rb_self, op)
    }

    /// `durability(durability)`.
    pub(crate) fn set_durability(
        ruby: &Ruby,
        rb_self: &Self,
        durability: Durability,
    ) -> Result<(), Error> {
        *lock(&rb_self.0.get(ruby)?.durability) = Some(durability);
        Ok(())
    }

    /// `return_previous(family, qualifier)`: asks `commit_returning` for that column as it was
    /// just before the commit (ICR 0113).
    pub(crate) fn return_previous(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
    ) -> Result<(), Error> {
        let m = rb_self.0.get(ruby)?;
        // Under the writes' lock, as a commit takes both (ai1249's Q1 on #1552).
        let ops = lock(&m.ops);
        if ops.is_none() {
            return Err(spent(ruby, "the mutation was already committed"));
        }
        lock(&m.asked).previous.push((family.0, qualifier.0));
        drop(ops);
        Ok(())
    }

    /// `return_current(family, qualifier)`: asks `commit_if_returning` for that column as
    /// the commit left it, or as the condition saw it when it didn't hold (D314 amendment 2).
    pub(crate) fn return_current(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
    ) -> Result<(), Error> {
        let m = rb_self.0.get(ruby)?;
        // Under the writes' lock, as a commit takes both (Q1).
        let ops = lock(&m.ops);
        if ops.is_none() {
            return Err(spent(ruby, "the mutation was already committed"));
        }
        lock(&m.asked).current.push((family.0, qualifier.0));
        drop(ops);
        Ok(())
    }

    /// Commits the recorded writes as one atomic row mutation; returns its sequence number.
    pub(crate) fn commit(ruby: &Ruby, rb_self: &Self) -> Result<u64, Error> {
        Self::finish(ruby, rb_self, None).map(|s| s.expect("unconditional"))
    }

    /// Commits only if `condition` holds; `nil` (nothing written) when it doesn't.
    pub(crate) fn commit_if(
        ruby: &Ruby,
        rb_self: &Self,
        condition: &Condition,
    ) -> Result<Option<u64>, Error> {
        Self::finish(ruby, rb_self, Some(condition.0.clone()))
    }

    /// Commits like `commit` and returns each counter cell the writes incremented or set,
    /// after the commit (D314).
    pub(crate) fn commit_returning(ruby: &Ruby, rb_self: &Self) -> Result<Incremented, Error> {
        let m = rb_self.0.get(ruby)?;
        let durability = *lock(&m.durability);
        // As in `finish`: the writes are taken inside the call.
        let r = without_gvl(ruby, move || {
            let (ops, asked) = {
                let mut ops = lock(&m.ops);
                // Under the writes' lock, as `return_previous` pushes (Q1).
                (ops.take()?, std::mem::take(&mut *lock(&m.asked)))
            };
            let r = commit_row_returning(&m.table, &m.row, &ops, &asked, durability);
            Some(r.map(|r| Incremented(r, asked, None)))
        })?;
        match r {
            Some(r) => r.or_rb(ruby),
            None => Err(spent(ruby, "the mutation was already committed")),
        }
    }

    /// Commits only if `condition` holds, and returns the row either way (D314 amendment 2):
    /// an `Incremented` (as `Returned`) when it applied, a `Seen` when it didn't. `api.rb`
    /// wraps it in a `Checked`.
    pub(crate) fn commit_if_returning(
        ruby: &Ruby,
        rb_self: &Self,
        condition: &Condition,
    ) -> Result<Value, Error> {
        let m = rb_self.0.get(ruby)?;
        let durability = *lock(&m.durability);
        let c = condition.0.clone();
        // As in `finish`: the writes are taken inside the call.
        let r = without_gvl(ruby, move || {
            let (ops, asked) = {
                let mut ops = lock(&m.ops);
                // Under the writes' lock, as `return_current` pushes (Q1).
                (ops.take()?, std::mem::take(&mut *lock(&m.asked)))
            };
            let r =
                row_mutation(&m.table, &m.row, &ops, &asked, durability).commit_if_returning(&c);
            let column = condition_column(&c);
            Some(r.map(|r| (r, asked, column)))
        })?;
        let (r, asked, column) = match r {
            Some(r) => r.or_rb(ruby)?,
            None => return Err(spent(ruby, "the mutation was already committed")),
        };
        Ok(match r {
            pigeonhole::Checked::Applied(ret) => ruby.into_value(Incremented(ret, asked, column)),
            pigeonhole::Checked::NotApplied(seen) => ruby.into_value(Seen(seen)),
        })
    }

    fn finish(
        ruby: &Ruby,
        rb_self: &Self,
        condition: Option<pigeonhole::Condition>,
    ) -> Result<Option<u64>, Error> {
        let m = rb_self.0.get(ruby)?;
        let durability = *lock(&m.durability);
        // The writes are taken inside the call: if an interrupt was pending and the call
        // didn't run, the mutation keeps them.
        let r = without_gvl(ruby, move || {
            let (ops, asked) = {
                let mut ops = lock(&m.ops);
                // Under the writes' lock, as `return_previous` pushes (Q1).
                (ops.take()?, std::mem::take(&mut *lock(&m.asked)))
            };
            Some(commit_row(
                &m.table,
                &m.row,
                &ops,
                &asked,
                durability,
                condition.as_ref(),
            ))
        })?;
        match r {
            Some(r) => r.or_rb(ruby),
            None => Err(spent(ruby, "the mutation was already committed")),
        }
    }
}

// ---- Incremented --------------------------------------------------------------------------

impl Incremented {
    /// The commit's sequence number.
    pub(crate) fn seqno(&self) -> u64 {
        self.0.info.seqno
    }

    /// `value(family, qualifier, ts)`: the counter (or bucket) after the commit; `nil` when a
    /// read at the commit sees no cell. `InvalidArgument` if the commit didn't write it;
    /// `ReturnedValueError` if it couldn't be read after the commit applied.
    pub(crate) fn value(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
        ts: Option<u64>,
    ) -> Result<Option<i64>, Error> {
        let ts = ts.unwrap_or(0);
        let wrote = rb_self
            .0
            .iter()
            .any(|(f, q, t, _)| f == family.0 && q == qualifier.0.as_slice() && t == ts);
        let v = rb_self.0.value_at(&family.0, &qualifier.0, ts);
        if wrote {
            v.map_err(|e| returned_value(ruby, rb_self.0.info.seqno, &e))
        } else {
            // `InvalidArgument`, naming the cell.
            v.or_rb(ruby)
        }
    }

    /// `previous(family, qualifier)`: the column `return_previous` asked for, as it was just
    /// before the commit, or `nil` if it had no visible version. `InvalidArgument` if the
    /// mutation didn't ask for it; `ReturnedValueError` if it couldn't be read after the
    /// commit applied.
    pub(crate) fn previous(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
    ) -> Result<Option<Cell>, Error> {
        let asked = rb_self
            .1
            .previous
            .iter()
            .any(|(f, q)| *f == family.0 && *q == qualifier.0);
        let c = rb_self
            .0
            .previous(&family.0, &qualifier.0)
            .map(|c| c.map(Cell));
        if asked {
            c.map_err(|e| returned_value(ruby, rb_self.0.info.seqno, &e))
        } else {
            // `InvalidArgument`, naming the column.
            c.or_rb(ruby)
        }
    }

    /// `current(family, qualifier)`: after an applied `commit_if_returning`, the column as of
    /// the commit, or `nil` if a read at the commit sees none. `InvalidArgument` if neither
    /// `return_current` nor the condition named it; `ReturnedValueError` if it couldn't be
    /// read after the commit applied.
    pub(crate) fn current(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
    ) -> Result<Option<Cell>, Error> {
        let c = rb_self
            .0
            .current(&family.0, &qualifier.0)
            .map(|c| c.map(Cell));
        if rb_self
            .1
            .has_current(&family.0, &qualifier.0, rb_self.2.as_ref())
        {
            c.map_err(|e| returned_value(ruby, rb_self.0.info.seqno, &e))
        } else {
            // `InvalidArgument`, naming the column.
            c.or_rb(ruby)
        }
    }

    /// `{[family, qualifier, ts] => value}` in the order the mutation first wrote each cell
    /// (`ts` 0 for a plain counter); raises `ReturnedValueError` for the first unreadable value.
    pub(crate) fn to_h(ruby: &Ruby, rb_self: &Self) -> Result<RHash, Error> {
        let h = ruby.hash_new();
        for (f, q, ts, v) in rb_self.0.iter() {
            let (family, qualifier) = (ruby.str_new(f), convert::binary(ruby, q));
            family.freeze();
            qualifier.freeze();
            let key = ruby.ary_new_from_values(&[family.as_value(), qualifier.as_value()]);
            key.push(ts)?;
            key.freeze();
            let v = v.map_err(|e| returned_value(ruby, rb_self.0.info.seqno, &e))?;
            h.aset(key, v)?;
        }
        Ok(h)
    }

    /// The number of cells.
    pub(crate) fn size(&self) -> usize {
        self.0.iter().count()
    }

    /// `#<Pigeonhole::Incremented seqno=... cells=...>`.
    pub(crate) fn inspect(&self) -> String {
        format!(
            "#<Pigeonhole::Incremented seqno={} cells={}>",
            self.0.info.seqno,
            self.size()
        )
    }
}

// ---- TableBuilder -------------------------------------------------------------------------

/// What the builder does at the end.
enum Kind {
    Create,
    CreateIfMissing,
    Open,
}

impl TableBuilder {
    /// A builder for `name` in `db`.
    pub(crate) fn new(db: pigeonhole::Pigeonhole, name: String) -> Self {
        Self(ForkSafe::new(Builder {
            db,
            name,
            families: Mutex::new(Vec::new()),
        }))
    }

    /// `family(name, options)`: adds a family (options: a `Family`, or `nil` for a plain
    /// one).
    pub(crate) fn family(
        ruby: &Ruby,
        rb_self: &Self,
        name: Name,
        options: Option<&Family>,
    ) -> Result<(), Error> {
        let b = rb_self.0.get(ruby)?;
        lock(&b.families).push((
            name.0,
            options.map_or_else(pigeonhole::Family::default, |f| f.0.clone()),
        ));
        Ok(())
    }

    /// Creates the table (`TableExists` if it exists).
    pub(crate) fn create(ruby: &Ruby, rb_self: &Self) -> Result<Table, Error> {
        Self::build(ruby, rb_self, Kind::Create)
    }

    /// Creates the table unless it exists, then opens it.
    pub(crate) fn create_if_missing(ruby: &Ruby, rb_self: &Self) -> Result<Table, Error> {
        Self::build(ruby, rb_self, Kind::CreateIfMissing)
    }

    /// Opens the table (`TableNotFound` if it doesn't exist).
    pub(crate) fn open(ruby: &Ruby, rb_self: &Self) -> Result<Table, Error> {
        Self::build(ruby, rb_self, Kind::Open)
    }

    fn build(ruby: &Ruby, rb_self: &Self, kind: Kind) -> Result<Table, Error> {
        let b = rb_self.0.get(ruby)?;
        let db = b.db.clone();
        let name = b.name.clone();
        let families = lock(&b.families).clone();
        engine(ruby, move || {
            let mut b = db.table(&name)?;
            for (f, opts) in families {
                b = b.family(&f, opts);
            }
            match kind {
                Kind::Create => b.create(),
                Kind::CreateIfMissing => b.create_if_missing(),
                Kind::Open => b.open(),
            }
        })
        .map(|t| Table(ForkSafe::new(Handle::Writer(t))))
    }
}

// ---- Seen ---------------------------------------------------------------------------------

impl Seen {
    /// `current(family, qualifier)`: the column as the condition saw it, its newest visible
    /// version then, or `nil` if it had none. `InvalidArgument` if neither `return_current`
    /// nor the condition named it. Nothing was written, so a read error is an ordinary one.
    pub(crate) fn current(
        ruby: &Ruby,
        rb_self: &Self,
        family: Name,
        qualifier: Bytes,
    ) -> Result<Option<Cell>, Error> {
        rb_self
            .0
            .current(&family.0, &qualifier.0)
            .map(|c| c.map(Cell))
            .or_rb(ruby)
    }
}

// ---- Under a fiber scheduler (#1574, ICR 0126) ---------------------------------------------

/// The mutation's four commits as fiber ops: each takes the writes and submits at the call,
/// as its blocking form does, and the result converts as that form's does.
#[cfg(unix)]
impl Mutation {
    /// The writes and the asked columns, taken; `None` if already committed.
    fn take_for_op(m: &MutationState) -> Option<(Vec<Op>, Asked)> {
        let mut ops = lock(&m.ops);
        // Under the writes' lock, as `return_previous` pushes (Q1).
        Some((ops.take()?, std::mem::take(&mut *lock(&m.asked))))
    }

    fn spent_op() -> crate::fiber::Fut {
        crate::fiber::ready(Box::new(|ruby: &Ruby| {
            Err(spent(ruby, "the mutation was already committed"))
        }))
    }

    /// `native_commit_op(bridge)`: `commit`, as a fiber op.
    pub(crate) fn commit_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &crate::fiber::FiberBridge,
    ) -> Result<crate::fiber::FiberOp, Error> {
        let m = rb_self.0.get(ruby)?;
        let durability = *lock(&m.durability);
        crate::fiber::FiberBridge::start(ruby, bridge, move || {
            let Some((ops, asked)) = Self::take_for_op(m) else {
                return Self::spent_op();
            };
            let fut = row_mutation(&m.table, &m.row, &ops, &asked, durability).commit_async();
            crate::fiber::map(fut, |ruby, r| Ok(ruby.into_value(r.or_rb(ruby)?.seqno)))
        })
    }

    /// `native_commit_if_op(bridge, condition)`: `commit_if`, as a fiber op.
    pub(crate) fn commit_if_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &crate::fiber::FiberBridge,
        condition: &Condition,
    ) -> Result<crate::fiber::FiberOp, Error> {
        let m = rb_self.0.get(ruby)?;
        let durability = *lock(&m.durability);
        let c = condition.0.clone();
        crate::fiber::FiberBridge::start(ruby, bridge, move || {
            let Some((ops, asked)) = Self::take_for_op(m) else {
                return Self::spent_op();
            };
            let fut = row_mutation(&m.table, &m.row, &ops, &asked, durability).commit_if_async(&c);
            crate::fiber::map(fut, |ruby, r| {
                Ok(ruby.into_value(r.or_rb(ruby)?.map(|i| i.seqno)))
            })
        })
    }

    /// `native_commit_returning_op(bridge)`: `commit_returning`, as a fiber op.
    pub(crate) fn commit_returning_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &crate::fiber::FiberBridge,
    ) -> Result<crate::fiber::FiberOp, Error> {
        let m = rb_self.0.get(ruby)?;
        let durability = *lock(&m.durability);
        crate::fiber::FiberBridge::start(ruby, bridge, move || {
            let Some((ops, asked)) = Self::take_for_op(m) else {
                return Self::spent_op();
            };
            let fut =
                row_mutation(&m.table, &m.row, &ops, &asked, durability).commit_returning_async();
            crate::fiber::map(fut, move |ruby, r| {
                Ok(ruby.into_value(Incremented(r.or_rb(ruby)?, asked, None)))
            })
        })
    }

    /// `native_commit_if_returning_op(bridge, condition)`: `commit_if_returning`, as a fiber
    /// op: an `Incremented` (as `Returned`) or a `Seen`, which `api.rb` wraps in a `Checked`.
    pub(crate) fn commit_if_returning_op(
        ruby: &Ruby,
        rb_self: &Self,
        bridge: &crate::fiber::FiberBridge,
        condition: &Condition,
    ) -> Result<crate::fiber::FiberOp, Error> {
        let m = rb_self.0.get(ruby)?;
        let durability = *lock(&m.durability);
        let c = condition.0.clone();
        crate::fiber::FiberBridge::start(ruby, bridge, move || {
            let Some((ops, asked)) = Self::take_for_op(m) else {
                return Self::spent_op();
            };
            let column = condition_column(&c);
            let fut = row_mutation(&m.table, &m.row, &ops, &asked, durability)
                .commit_if_returning_async(&c);
            crate::fiber::map(fut, move |ruby, r| {
                Ok(match r.or_rb(ruby)? {
                    pigeonhole::Checked::Applied(ret) => {
                        ruby.into_value(Incremented(ret, asked, column))
                    }
                    pigeonhole::Checked::NotApplied(seen) => ruby.into_value(Seen(seen)),
                })
            })
        })
    }
}
