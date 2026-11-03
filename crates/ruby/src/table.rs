//! Tables: family options, the table builder, reads, and writes recorded as operations that
//! one engine call applies (a row mutation, a batch or a transaction).
//!
//! The methods here are the native core: positional arguments, `nil` for "not given". The
//! public, keyword-taking methods are Ruby (`lib/pigeonhole/api.rb`).

use std::sync::Mutex;

use magnus::{Error, Float, Integer, RArray, RString, Ruby, Symbol, Value, prelude::*};
use pigeonhole::{Aggregation, Compression, ErrorCode};

use crate::classes::{
    Builder, Cell, Condition, Family, FamilyInfo, Mutation, MutationState, Row, ScanArgs, ScanSpec,
    Snapshot, Table, TableBuilder,
};
use crate::convert::{
    self, Bytes, CellValue, Name, Secs, Val, arg_error, class_name, lock, type_error,
};
use crate::error::{OrRb, engine, spent, with_code};
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
    durability: Option<Durability>,
    condition: Option<&pigeonhole::Condition>,
) -> pigeonhole::Result<Option<u64>> {
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
            Op::DeleteRow => m.delete_row(),
            Op::DeleteFamily(f) => m.delete_family(f),
            Op::DeleteColumn(f, q) => m.delete_column(f, q),
            Op::DeleteCell(f, q, ts) => m.delete_cell(f, q, *ts),
        };
    }
    match condition {
        None => m.commit().map(|i| Some(i.seqno)),
        Some(c) => m.commit_if(c).map(|i| i.map(|i| i.seqno)),
    }
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
        engine(ruby, move || commit_row(&t, &row, &[op], durability, None))
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
            let ops = lock(&m.ops).take()?;
            Some(commit_row(
                &m.table,
                &m.row,
                &ops,
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
