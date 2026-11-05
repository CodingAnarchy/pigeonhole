//! Tables: families, the table builder, reads, and writes recorded as operations that one
//! engine call applies (a row mutation, a batch or a transaction).

use crate::Maybe;
use std::ops::Bound as KeyBound;
use std::sync::Mutex;

use pigeonhole::{Aggregation, Compression};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::convert::{self, Val};
use crate::error::{OrPy, returned_value, with_code};
use crate::read::{Cell, Row, ScanIter, Snapshot, snapshot_arg};

/// How durable a commit is before it returns: `pigeonhole.Durability`, an `enum.IntEnum`
/// defined in the Python package (`NONE` 0, `BUFFERED` 1, `GROUP_SYNC` 2, `SYNC` 3). Any int
/// with one of those values is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Durability {
    None_,
    Buffered,
    GroupSync,
    Sync,
}

impl<'a, 'py> FromPyObject<'a, 'py> for Durability {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if ob.is_instance_of::<pyo3::types::PyBool>() {
            return Err(PyTypeError::new_err(
                "durability is a pigeonhole.Durability",
            ));
        }
        match ob.extract::<i64>() {
            Ok(0) => Ok(Self::None_),
            Ok(1) => Ok(Self::Buffered),
            Ok(2) => Ok(Self::GroupSync),
            Ok(3) => Ok(Self::Sync),
            _ => Err(PyValueError::new_err(
                "durability is a pigeonhole.Durability (NONE, BUFFERED, GROUP_SYNC or SYNC)",
            )),
        }
    }
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

/// A column family's options. `Family()` is a plain family; `Family.counter()` and
/// `Family.aggregate(kind, ...)` make aggregate families.
#[pyclass(module = "pigeonhole", frozen)]
#[derive(Clone)]
pub(crate) struct Family(pub(crate) pigeonhole::Family);

#[pymethods]
impl Family {
    /// A plain family.
    ///
    /// - `max_versions`: versions kept per column (0 keeps all).
    /// - `ttl`: cells expire this long after their timestamp (`timedelta` or seconds).
    /// - `bloom_bits`, `block_size`, `blob_threshold`: storage tuning.
    /// - `compression`: `"lz4"` (the default), `"zstd"` or `"none"`; `zstd_level` with zstd.
    #[new]
    #[pyo3(signature = (*, max_versions=None, ttl=None, bloom_bits=None, block_size=None, blob_threshold=None, compression=None, zstd_level=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_versions: Option<u32>,
        ttl: Option<&Bound<'_, PyAny>>,
        bloom_bits: Option<u8>,
        block_size: Option<u32>,
        blob_threshold: Option<u32>,
        compression: Option<&str>,
        zstd_level: Option<i8>,
    ) -> PyResult<Self> {
        options(
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

    /// A counter family: `incr` adds to its cells.
    #[staticmethod]
    #[pyo3(signature = (*, max_versions=None, ttl=None))]
    fn counter(max_versions: Option<u32>, ttl: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        options(
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

    /// A vector family (D303): `dim` elements of `element` (`Element.F32`, `I8` or `BIT`),
    /// compared by `metric` (`Metric.L2`, `COSINE`, `INNER_PRODUCT` or `HAMMING`). Its
    /// cells are written with `put_vector`.
    #[staticmethod]
    #[pyo3(signature = (dim, element, metric, *, max_versions=None, ttl=None))]
    fn vector(
        dim: u32,
        element: &Bound<'_, PyAny>,
        metric: &Bound<'_, PyAny>,
        max_versions: Option<u32>,
        ttl: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let spec = crate::vector::VectorSpec::new_spec(dim, element, metric)?;
        options(
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

    /// An aggregate family of `kind`: `"sum"`, `"min"`, `"max"`, `"hll"`, `"bit_or"`,
    /// `"bit_and"`, `"bit_xor"`, `"min_f64"`, `"max_f64"`, `"sum_f64"`, `"mean_var"`,
    /// `"histogram"` (with `bounds`), `"quantiles"` (with optional `mantissa_bits` and
    /// `max_buckets`) or `"bloom"` (with `bits` and `hashes`).
    #[staticmethod]
    #[pyo3(signature = (kind, *, bounds=None, mantissa_bits=None, max_buckets=None, bits=None, hashes=None, max_versions=None, ttl=None))]
    #[allow(clippy::too_many_arguments)]
    fn aggregate(
        kind: &str,
        bounds: Option<Vec<f64>>,
        mantissa_bits: Option<u8>,
        max_buckets: Option<u16>,
        bits: Option<u32>,
        hashes: Option<u8>,
        max_versions: Option<u32>,
        ttl: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let a = match kind {
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
                let bounds =
                    bounds.ok_or_else(|| PyValueError::new_err("a histogram needs its bounds"))?;
                Aggregation::Histogram(pigeonhole::HistogramParams::new(bounds).or_py()?)
            }
            "quantiles" => {
                let mut q = pigeonhole::QuantileParams::default();
                q = q.maybe(mantissa_bits, |q, p| q.mantissa_bits(p));
                q = q.maybe(max_buckets, |q, n| q.max_buckets(n));
                Aggregation::Quantiles(q)
            }
            "bloom" => {
                let (Some(bits), Some(hashes)) = (bits, hashes) else {
                    return Err(PyValueError::new_err(
                        "a bloom family needs bits and hashes",
                    ));
                };
                Aggregation::Bloom(pigeonhole::BloomParams::new(bits, hashes).or_py()?)
            }
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown aggregation {other:?}"
                )));
            }
        };
        options(
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
}

#[allow(clippy::too_many_arguments)]
fn options(
    mut f: pigeonhole::Family,
    max_versions: Option<u32>,
    ttl: Option<&Bound<'_, PyAny>>,
    bloom_bits: Option<u8>,
    block_size: Option<u32>,
    blob_threshold: Option<u32>,
    compression: Option<&str>,
    zstd_level: Option<i8>,
) -> PyResult<Family> {
    f = f.maybe(max_versions, |f, n| f.max_versions(n));
    if let Some(t) = ttl {
        f = f.ttl(convert::duration(t)?);
    }
    f = f.maybe(bloom_bits, |f, n| f.bloom_bits(n));
    f = f.maybe(block_size, |f, n| f.block_size(n));
    f = f.maybe(blob_threshold, |f, n| f.blob_threshold(n));
    f = match (compression, zstd_level) {
        (None, None) => f,
        (Some("lz4"), None) => f.compression(Compression::Lz4),
        (Some("none"), None) => f.compression(Compression::None),
        (Some("zstd"), level) => f.compression(Compression::Zstd(level.unwrap_or(3))),
        (None, Some(_)) | (Some(_), Some(_)) => {
            return Err(PyValueError::new_err(
                "zstd_level is for compression=\"zstd\"",
            ));
        }
        (Some(other), None) => {
            return Err(PyValueError::new_err(format!(
                "unknown compression {other:?}"
            )));
        }
    };
    Ok(Family(f))
}

/// A family's stored options (`Table.family_info`).
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct FamilyInfo(pigeonhole::FamilyInfo);

#[pymethods]
impl FamilyInfo {
    /// The family's name.
    #[getter]
    fn name(&self) -> String {
        self.0.name.clone()
    }

    /// Whether it is a counter (`sum`) family, or a standard family naming the built-in
    /// `i64` add operator.
    #[getter]
    fn counter(&self) -> bool {
        self.0.counter
    }

    /// The aggregation kind (`"sum"`, `"min"`, ..., the names `Family.aggregate` takes), or
    /// `None` for a plain family.
    #[getter]
    fn aggregation(&self) -> Option<&'static str> {
        self.0.aggregation.as_ref().map(aggregation_name)
    }

    /// Versions kept per column (0 keeps all).
    #[getter]
    fn max_versions(&self) -> u32 {
        self.0.max_versions
    }

    /// How long cells live after their timestamp, or `None`.
    #[getter]
    fn ttl(&self) -> Option<std::time::Duration> {
        self.0.ttl
    }

    /// Bloom filter bits per key.
    #[getter]
    fn bloom_bits(&self) -> u8 {
        self.0.bloom_bits
    }

    /// Values longer than this are stored in blob files.
    #[getter]
    fn blob_threshold(&self) -> u32 {
        self.0.blob_threshold
    }

    /// The data block size.
    #[getter]
    fn block_size(&self) -> u32 {
        self.0.block_size
    }

    /// `"lz4"`, `"zstd"` or `"none"`.
    #[getter]
    fn compression(&self) -> &'static str {
        match self.0.compression {
            Compression::None => "none",
            Compression::Zstd(_) => "zstd",
            Compression::Lz4 => "lz4",
            _ => "unknown",
        }
    }

    /// The zstd level, with `compression == "zstd"`.
    #[getter]
    fn zstd_level(&self) -> Option<i8> {
        match self.0.compression {
            Compression::Zstd(l) => Some(l),
            _ => None,
        }
    }

    /// The merge operator a standard family names, or `None`.
    #[getter]
    fn merge_operator(&self) -> Option<String> {
        self.0.merge_operator.clone()
    }

    /// `"low"`, `"normal"` or `"high"`: the family's block cache priority.
    #[getter]
    fn cache_priority(&self) -> &'static str {
        match self.0.cache_priority {
            pigeonhole::Priority::Low => "low",
            pigeonhole::Priority::Normal => "normal",
            pigeonhole::Priority::High => "high",
            _ => "unknown",
        }
    }

    /// `"leveled"`, `"tiered"` or `"fifo-by-time"`.
    #[getter]
    fn compaction(&self) -> &'static str {
        match self.0.compaction {
            pigeonhole::Compaction::Leveled => "leveled",
            pigeonhole::Compaction::Tiered => "tiered",
            pigeonhole::Compaction::FifoByTime => "fifo-by-time",
            _ => "unknown",
        }
    }

    /// A vector family's spec, or `None`.
    #[getter]
    fn vector(&self) -> Option<crate::vector::VectorSpec> {
        self.0.vector.map(crate::vector::VectorSpec)
    }

    /// The family's options as a `Family`, to create the same family elsewhere.
    #[getter]
    fn family(&self) -> Family {
        Family(self.0.family.clone())
    }

    fn __repr__(&self) -> String {
        format!(
            "FamilyInfo(name={:?}, aggregation={:?}, max_versions={})",
            self.0.name,
            self.aggregation(),
            self.0.max_versions
        )
    }
}

/// A condition a row must meet for `Mutation.commit_if` to apply.
#[pyclass(module = "pigeonhole", frozen)]
#[derive(Clone)]
pub(crate) struct Condition(pub(crate) pigeonhole::Condition);

#[pymethods]
impl Condition {
    /// The column has no live cell.
    #[staticmethod]
    fn absent(family: String, qualifier: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self(pigeonhole::Condition::Absent {
            family,
            qualifier: convert::bytes(qualifier)?,
        }))
    }

    /// The column has a live cell.
    #[staticmethod]
    fn exists(family: String, qualifier: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self(pigeonhole::Condition::Exists {
            family,
            qualifier: convert::bytes(qualifier)?,
        }))
    }

    /// The column's newest value is exactly `value` (bytes).
    #[staticmethod]
    fn equals(
        family: String,
        qualifier: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        Ok(Self(pigeonhole::Condition::Value {
            family,
            qualifier: convert::bytes(qualifier)?,
            filter: pigeonhole::ValueFilter::Equals(convert::bytes(value)?),
        }))
    }
}

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

/// `ob` as an input to `table`'s `family`, read by the family's kind (an `int` into an
/// `f64` family is a float; into a bitwise one a mask; into `hll` or `bloom` an element).
pub(crate) fn aggregate_input(
    table: &pigeonhole::Table,
    family: &str,
    ob: &Bound<'_, PyAny>,
) -> PyResult<Input> {
    let info = table.family_info(family).ok_or_else(|| {
        with_code(
            pigeonhole::ErrorCode::FamilyNotFound,
            &format!("table {} has no family {family}", table.name()),
        )
    })?;
    let bad = |want: &str| PyTypeError::new_err(format!("family {family} takes {want}"));
    Ok(match info.aggregation {
        None => {
            return Err(PyTypeError::new_err(format!(
                "aggregate writes to an aggregate family, and {family} is a plain family: use put"
            )));
        }
        Some(
            Aggregation::MinF64
            | Aggregation::MaxF64
            | Aggregation::SumF64
            | Aggregation::MeanVar
            | Aggregation::Histogram(_)
            | Aggregation::Quantiles(_),
        ) => Input::F64(ob.extract().map_err(|_| bad("a number"))?),
        Some(Aggregation::BitOr | Aggregation::BitAnd | Aggregation::BitXor) => Input::U64(
            ob.extract()
                .map_err(|_| bad("a non-negative int (a 64-bit mask)"))?,
        ),
        Some(Aggregation::Hll | Aggregation::Bloom(_)) => match ob.extract::<i64>() {
            Ok(n) => Input::I64(n),
            Err(_) => Input::Bytes(convert::bytes(ob).map_err(|_| bad("an int, or bytes"))?),
        },
        Some(_) => Input::I64(ob.extract().map_err(|_| bad("an int"))?),
    })
}

/// `ob` as a vector for `table`'s `family`, read by the family's element type.
pub(crate) fn vector_input(
    table: &pigeonhole::Table,
    family: &str,
    ob: &Bound<'_, PyAny>,
) -> PyResult<crate::vector::Vector> {
    let info = table.family_info(family).ok_or_else(|| {
        with_code(
            pigeonhole::ErrorCode::FamilyNotFound,
            &format!("table {} has no family {family}", table.name()),
        )
    })?;
    let spec = info.vector.ok_or_else(|| {
        PyTypeError::new_err(format!(
            "put_vector writes to a vector family, and {family} is not one"
        ))
    })?;
    crate::vector::vector(ob, spec.element())
}

/// A table handle, as the writer or as a reader process.
#[derive(Clone)]
pub(crate) enum Handle {
    Writer(pigeonhole::Table),
    Reader(pigeonhole::ReadTable),
}

/// A table: reads, and with a writer handle, writes. Get one from `Database.table(...)`
/// (`.create()`, `.create_if_missing()`, `.open()`) or `Reader.table(name)`.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Table(pub(crate) Handle);

impl Table {
    pub(crate) fn writer(&self) -> PyResult<&pigeonhole::Table> {
        match &self.0 {
            Handle::Writer(t) => Ok(t),
            Handle::Reader(_) => Err(with_code(
                pigeonhole::ErrorCode::ReadOnly,
                "a reader process's table can't be written",
            )),
        }
    }
}

/// Runs `$body` with `$t` bound to either kind of table.
macro_rules! either {
    ($h:expr, |$t:ident| $body:expr) => {
        match $h {
            Handle::Writer($t) => $body,
            Handle::Reader($t) => $body,
        }
    };
}

#[pymethods]
impl Table {
    /// The table's name.
    #[getter]
    fn name(&self) -> String {
        either!(&self.0, |t| t.name().to_owned())
    }

    /// The family names, in creation order.
    fn families(&self) -> Vec<String> {
        either!(&self.0, |t| t
            .families()
            .into_iter()
            .map(str::to_owned)
            .collect())
    }

    /// A family's stored options, or `None` for no such family.
    fn family_info(&self, family: &str) -> Option<FamilyInfo> {
        either!(&self.0, |t| t.family_info(family)).map(FamilyInfo)
    }

    /// The newest version of one cell, or `None`.
    #[pyo3(signature = (row, family, qualifier, *, snapshot=None))]
    fn get(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyAny>,
        family: &str,
        qualifier: &Bound<'_, PyAny>,
        snapshot: Option<&Bound<'_, Snapshot>>,
    ) -> PyResult<Option<Cell>> {
        let (row, q) = (convert::bytes(row)?, convert::bytes(qualifier)?);
        let snap = snapshot_arg(snapshot)?;
        let h = self.0.clone();
        let family = family.to_owned();
        py.detach(move || {
            either!(&h, |t| match &snap {
                Some(s) => t
                    .get_at(s, &row, &family, &q)
                    .map(|c| c.map(|c| c.to_owned())),
                None => t.get(&row, &family, &q).map(|c| c.map(|c| c.to_owned())),
            })
        })
        .or_py()
        .map(|c| c.map(Cell))
    }

    /// One row's cells, or `None` when the row has none the read selects.
    ///
    /// `families` selects families (and their order), `qualifier_prefix` columns,
    /// `versions` the versions per column (default 1; 0 for all), and `column_limit` at
    /// most that many columns per family.
    #[pyo3(signature = (row, *, families=None, qualifier_prefix=None, versions=1, column_limit=None, snapshot=None))]
    #[allow(clippy::too_many_arguments)]
    fn row(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyAny>,
        families: Option<Vec<String>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        versions: u32,
        column_limit: Option<u32>,
        snapshot: Option<&Bound<'_, Snapshot>>,
    ) -> PyResult<Option<Row>> {
        let row = convert::bytes(row)?;
        let qp = qualifier_prefix.map(convert::bytes).transpose()?;
        let snap = snapshot_arg(snapshot)?;
        let h = self.0.clone();
        py.detach(move || {
            either!(&h, |t| {
                let mut r = t.row(&row).versions(versions);
                r = r.maybe(families.as_ref(), |r, f| {
                    r.families(f.iter().map(String::as_str))
                });
                r = r.maybe(qp.as_ref(), |r, q| r.qualifier_prefix(q));
                r = r.maybe(column_limit, |r, n| r.column_limit(n));
                r = r.maybe(snap.as_ref(), |r, s| r.snapshot(s));
                r.read().map(|r| r.map(|r| r.to_owned()))
            })
        })
        .or_py()
        .map(|r| r.map(Row))
    }

    /// Rows in key order: those starting with `prefix`, or from `start` (inclusive) to
    /// `end` (exclusive), or all. Returns an iterator; rows are read `chunk` at a time.
    #[pyo3(signature = (prefix=None, *, start=None, end=None, families=None, qualifier_prefix=None, versions=1, limit=None, column_limit=None, snapshot=None, chunk=256))]
    #[allow(clippy::too_many_arguments)]
    fn scan(
        &self,
        prefix: Option<&Bound<'_, PyAny>>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        families: Option<Vec<String>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        versions: u32,
        limit: Option<u64>,
        column_limit: Option<u32>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        chunk: usize,
    ) -> PyResult<ScanIter> {
        let prefix = prefix.map(convert::bytes).transpose()?;
        let start = start.map(convert::bytes).transpose()?;
        let end = end.map(convert::bytes).transpose()?;
        if prefix.is_some() && (start.is_some() || end.is_some()) {
            return Err(PyValueError::new_err(
                "scan takes a prefix or start/end, not both",
            ));
        }
        let qp = qualifier_prefix.map(convert::bytes).transpose()?;
        let snap = snapshot_arg(snapshot)?;
        let scan = either!(&self.0, |t| {
            let mut s = match &prefix {
                Some(p) => t.scan_prefix(p),
                None => t.scan_bounds(
                    start
                        .as_deref()
                        .map_or(KeyBound::Unbounded, KeyBound::Included),
                    end.as_deref()
                        .map_or(KeyBound::Unbounded, KeyBound::Excluded),
                ),
            }
            .versions(versions);
            s = s.maybe(families.as_ref(), |s, f| {
                s.families(f.iter().map(String::as_str))
            });
            s = s.maybe(qp.as_ref(), |s, q| s.qualifier_prefix(q));
            s = s.maybe(limit, |s, n| s.limit(n));
            s = s.maybe(column_limit, |s, n| s.column_limit(n));
            s = s.maybe(snap.as_ref(), |s, sn| s.snapshot(sn));
            s.into_owned()
        });
        ScanIter::new(scan, chunk)
    }

    /// The rows of `keys` (a multi-get, D299), with the selection `row` takes, aligned with
    /// `keys`: one entry per key given, in that order, `None` where the row has no cell the
    /// selection keeps (a repeated key repeats its row).
    #[pyo3(signature = (keys, *, families=None, qualifier_prefix=None, versions=1, column_limit=None, snapshot=None))]
    #[allow(clippy::too_many_arguments)]
    fn multi_get(
        &self,
        py: Python<'_>,
        keys: Vec<Bound<'_, PyAny>>,
        families: Option<Vec<String>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        versions: u32,
        column_limit: Option<u32>,
        snapshot: Option<&Bound<'_, Snapshot>>,
    ) -> PyResult<Vec<Option<Row>>> {
        crate::walk::multi_get(
            py,
            &self.0,
            &keys,
            families,
            qualifier_prefix,
            versions,
            column_limit,
            snapshot,
        )
    }

    /// A breadth-first traversal (D304) of a graph stored as adjacency rows: each hop
    /// follows `family`'s edges (a column per edge, its qualifier the destination's key)
    /// from the previous hop's nodes, and keeps the nodes not visited before.
    /// `byte_budget` stops it at that many bytes of nodes (each its key and 16, plus 4 with
    /// `parents`; the seeds aren't counted), D308: the hop it cuts keeps its in-budget
    /// prefix, and `cut_by_budget` says so. `None`, the default, sets no budget; 0 is
    /// refused.
    #[pyo3(signature = (seeds, family, *, hops=1, per_node_limit=None, hop_limit=None, max_nodes=None, byte_budget=None, parents=false, qualifier_prefix=None, snapshot=None, parallel=None))]
    #[allow(clippy::too_many_arguments)]
    fn traverse(
        &self,
        py: Python<'_>,
        seeds: Vec<Bound<'_, PyAny>>,
        family: String,
        hops: u32,
        per_node_limit: Option<u32>,
        hop_limit: Option<u64>,
        max_nodes: Option<u64>,
        byte_budget: Option<u64>,
        parents: bool,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        parallel: Option<usize>,
    ) -> PyResult<crate::walk::Traversal> {
        crate::walk::traverse(
            py,
            &self.0,
            &seeds,
            family,
            hops,
            per_node_limit,
            hop_limit,
            max_nodes,
            byte_budget,
            parents,
            qualifier_prefix,
            snapshot,
            parallel,
        )
    }

    /// The `k` cells of vector family `family` nearest `query` (D296): over `prefix`, or
    /// `start`/`end`, or the whole table, or the rows of `keys`; in (distance, row,
    /// qualifier) order. `query` is read by the family's element type, as `put_vector`'s
    /// vector is. An exact search: every cell in the read is scored (`parallel` threads may
    /// share the work).
    #[pyo3(signature = (family, query, k, *, prefix=None, start=None, end=None, keys=None, qualifier_prefix=None, snapshot=None, parallel=None))]
    #[allow(clippy::too_many_arguments)]
    fn nearest(
        &self,
        py: Python<'_>,
        family: String,
        query: &Bound<'_, PyAny>,
        k: usize,
        prefix: Option<&Bound<'_, PyAny>>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        keys: Option<Vec<Bound<'_, PyAny>>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        parallel: Option<usize>,
    ) -> PyResult<Vec<crate::walk::Neighbour>> {
        let (found, _) = crate::walk::nearest(
            py,
            &self.0,
            family,
            query,
            k,
            prefix,
            start,
            end,
            keys,
            qualifier_prefix,
            snapshot,
            parallel,
            None,
        )?;
        Ok(found.into_iter().map(crate::walk::Neighbour).collect())
    }

    /// `nearest`'s neighbours, nearest first, that fit `bytes` (D308): each counts its row
    /// key, its qualifier and 16 bytes, and a neighbour is in whole or not at all. The budget
    /// bounds the result, not the work: the search reads and ranks as `nearest` does. There's
    /// no resume position: ask again with a larger budget or `k`.
    #[pyo3(signature = (family, query, k, *, bytes, prefix=None, start=None, end=None, keys=None, qualifier_prefix=None, snapshot=None, parallel=None))]
    #[allow(clippy::too_many_arguments)]
    fn nearest_within(
        &self,
        py: Python<'_>,
        family: String,
        query: &Bound<'_, PyAny>,
        k: usize,
        bytes: u64,
        prefix: Option<&Bound<'_, PyAny>>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        keys: Option<Vec<Bound<'_, PyAny>>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        parallel: Option<usize>,
    ) -> PyResult<crate::budget::NearestPage> {
        let (found, page) = crate::walk::nearest(
            py,
            &self.0,
            family,
            query,
            k,
            prefix,
            start,
            end,
            keys,
            qualifier_prefix,
            snapshot,
            parallel,
            Some(bytes),
        )?;
        let (bytes, next_bytes) = page.expect("a budgeted search returns its page");
        Ok(crate::budget::NearestPage {
            neighbours: found
                .into_iter()
                .map(|n| Py::new(py, crate::walk::Neighbour(n)))
                .collect::<PyResult<_>>()?,
            bytes,
            next_bytes,
        })
    }

    /// A page of `scan`'s cells, in scan order, at most `bytes` long (D308): each cell counts
    /// its qualifier, its value and 16 bytes, and each row its key once. A column (all the
    /// versions the read selects) is kept whole or not at all; a row may be cut between
    /// columns. `limit` applies too: whichever comes first stops the read. `after` resumes
    /// where an earlier page stopped (its `resume`). Without a `snapshot`, a chain of pages
    /// is not one consistent read.
    #[pyo3(signature = (prefix=None, *, bytes, start=None, end=None, families=None, qualifier_prefix=None, versions=1, limit=None, column_limit=None, snapshot=None, after=None))]
    #[allow(clippy::too_many_arguments)]
    fn scan_within(
        &self,
        py: Python<'_>,
        prefix: Option<&Bound<'_, PyAny>>,
        bytes: u64,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        families: Option<Vec<String>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        versions: u32,
        limit: Option<u64>,
        column_limit: Option<u32>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        after: Option<&crate::budget::ScanPosition>,
    ) -> PyResult<crate::budget::ScanPage> {
        crate::budget::scan_within(
            py,
            &self.0,
            bytes,
            prefix,
            start,
            end,
            families,
            qualifier_prefix,
            versions,
            limit,
            column_limit,
            snapshot,
            after,
        )
    }

    /// A page of `multi_get`'s cells, in key order, at most `bytes` long (D308), as
    /// `scan_within` counts them. Unlike `multi_get`, the rows aren't aligned with `keys`:
    /// they come in key order, and keys with no cell the selection keeps return nothing. `after` resumes where an earlier page stopped:
    /// at the first key at or after its position's key, so keys added or removed between
    /// pages resume at the right one.
    #[pyo3(signature = (keys, *, bytes, families=None, qualifier_prefix=None, versions=1, column_limit=None, snapshot=None, after=None))]
    #[allow(clippy::too_many_arguments)]
    fn multi_get_within(
        &self,
        py: Python<'_>,
        keys: Vec<Bound<'_, PyAny>>,
        bytes: u64,
        families: Option<Vec<String>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        versions: u32,
        column_limit: Option<u32>,
        snapshot: Option<&Bound<'_, Snapshot>>,
        after: Option<&crate::budget::KeysPosition>,
    ) -> PyResult<crate::budget::KeysPage> {
        crate::budget::multi_get_within(
            py,
            &self.0,
            &keys,
            bytes,
            families,
            qualifier_prefix,
            versions,
            column_limit,
            snapshot,
            after,
        )
    }

    /// Subscribes to this table's committed changes (the change feed, D310): a `Feed` of
    /// whole commits, once each, in commit order. The database's change feed must be on, set
    /// with `change_feed=` or inherited from the last open, and follow this table. A reader
    /// process's table (`Reader.table(...)`) feeds too, from the running writer's history.
    ///
    /// `tables` adds more tables to the same feed (a commit across them arrives whole),
    /// `prefix` keeps only changes to rows under it, and `visible=True` delivers a commit as
    /// soon as reads see it rather than once it is durable. The feed starts after this call,
    /// or after the commits `from_snapshot` sees, or at the oldest history kept
    /// (`from_oldest=True`), or after a stored `resume` token (`Feed.token()`'s bytes).
    #[pyo3(signature = (*, tables=None, prefix=None, visible=false, from_snapshot=None, from_oldest=false, resume=None))]
    #[allow(clippy::too_many_arguments)]
    fn feed(
        &self,
        py: Python<'_>,
        tables: Option<Vec<Bound<'_, Table>>>,
        prefix: Option<&Bound<'_, PyAny>>,
        visible: bool,
        from_snapshot: Option<&Bound<'_, Snapshot>>,
        from_oldest: bool,
        resume: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<crate::feed::Feed> {
        let starts = usize::from(from_snapshot.is_some())
            + usize::from(from_oldest)
            + usize::from(resume.is_some());
        if starts > 1 {
            return Err(PyValueError::new_err(
                "one of from_snapshot, from_oldest and resume, not several",
            ));
        }
        // A reader process's table feeds from the writer's shared history (D310 PR 4, #1361).
        let mut b = either!(&self.0, |t| t.feed());
        for other in tables.iter().flatten() {
            b = match &other.get().0 {
                Handle::Writer(t) => b.table(t),
                Handle::Reader(t) => b.read_table(t),
            };
        }
        if let Some(p) = prefix {
            b = b.prefix(&convert::bytes(p)?);
        }
        if visible {
            b = b.visible();
        }
        if let Some(s) = snapshot_arg(from_snapshot)? {
            b = b.from_snapshot(&s);
        }
        if from_oldest {
            b = b.from_oldest();
        }
        if let Some(r) = resume {
            b = b.resume(&pigeonhole::FeedToken::from_bytes(convert::bytes(r)?));
        }
        py.detach(move || b.open())
            .or_py()
            .map(crate::feed::Feed::new)
    }

    /// A wide row's pages, at most `size` cells each (a column's versions never split), each
    /// read when it is asked for; `after` resumes after an earlier walk's `position`.
    #[pyo3(signature = (row, size, *, families=None, qualifier_prefix=None, versions=1, snapshot=None, after=None))]
    #[allow(clippy::too_many_arguments)]
    fn pages(
        &self,
        row: &Bound<'_, PyAny>,
        size: usize,
        families: Option<Vec<String>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        versions: u32,
        snapshot: Option<&Bound<'_, Snapshot>>,
        after: Option<&crate::walk::RowPosition>,
    ) -> PyResult<crate::walk::Pages> {
        crate::walk::Pages::new(
            &self.0,
            row,
            size,
            families,
            qualifier_prefix,
            versions,
            snapshot,
            after,
        )
    }

    /// A mutation of one row: chain writes, then `commit()` them atomically.
    fn mutate(&self, row: &Bound<'_, PyAny>) -> PyResult<Mutation> {
        let table = self.writer()?.clone();
        Ok(Mutation {
            table,
            row: convert::bytes(row)?,
            ops: Mutex::new(Some(Vec::new())),
            asked: Mutex::new(Asked::default()),
            durability: Mutex::new(None),
        })
    }

    /// Puts one cell and commits; returns the commit's sequence number. The value's Python
    /// type picks the cell type: bytes or str, int (`i64`), float (`f64`).
    #[pyo3(signature = (row, family, qualifier, value, *, ts=None, durability=None))]
    #[allow(clippy::too_many_arguments)]
    fn put(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyAny>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> PyResult<u64> {
        let op = Op::Put(
            family,
            convert::bytes(qualifier)?,
            ts,
            convert::value(value)?,
        );
        commit_row(
            py,
            self.writer()?,
            convert::bytes(row)?,
            vec![op],
            Asked::default(),
            durability,
            None,
        )
        .map(|s| s.expect("unconditional"))
    }

    /// Adds `delta` to a counter cell and commits.
    #[pyo3(signature = (row, family, qualifier, delta, *, ts=None, durability=None))]
    #[allow(clippy::too_many_arguments)]
    fn incr(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyAny>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        delta: i64,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> PyResult<u64> {
        let op = Op::Incr(family, convert::bytes(qualifier)?, ts, delta);
        commit_row(
            py,
            self.writer()?,
            convert::bytes(row)?,
            vec![op],
            Asked::default(),
            durability,
            None,
        )
        .map(|s| s.expect("unconditional"))
    }

    /// Adds `delta` to a counter cell, commits, and returns the counter's value after the
    /// commit: concurrent callers adding 1 from zero get exactly `1..=N` (D314). `None` only
    /// for a bucket (`ts`) already past the family's TTL. A failed commit raises its usual
    /// exception; `ReturnedValueError` means the commit applied but reading the value failed,
    /// so **the increment landed**: don't retry it.
    #[pyo3(signature = (row, family, qualifier, delta, *, ts=None, durability=None))]
    #[allow(clippy::too_many_arguments)]
    fn incr_returning(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyAny>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        delta: i64,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> PyResult<Option<i64>> {
        let q = convert::bytes(qualifier)?;
        let op = Op::Incr(family.clone(), q.clone(), ts, delta);
        let r = commit_row_returning(
            py,
            self.writer()?,
            convert::bytes(row)?,
            vec![op],
            Asked::default(),
            durability,
        )?;
        r.0.value_at(&family, &q, ts.unwrap_or(0))
            .map_err(|e| returned_value(r.0.info.seqno, e))
    }

    /// `incr_returning(...)` as an `asyncio.Future` of the running loop (#1479): the
    /// counter's value after the commit, with the same `None` and `ReturnedValueError` cases.
    #[pyo3(signature = (row, family, qualifier, delta, *, ts=None, durability=None))]
    #[allow(clippy::too_many_arguments)]
    fn incr_returning_async<'py>(
        &self,
        py: Python<'py>,
        row: &Bound<'_, PyAny>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        delta: i64,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let q = convert::bytes(qualifier)?;
        let row = convert::bytes(row)?;
        let table = self.writer()?.clone();
        let (event_loop, fut) = crate::aio::new_future(py)?;
        let op = Op::Incr(family.clone(), q.clone(), ts, delta);
        crate::aio::start(py, &event_loop, &fut, move || {
            let f = row_mutation(&table, &row, &[op], &Asked::default(), durability)
                .commit_returning_async();
            async move {
                let r = f.await;
                Box::new(move |py: Python<'_>| {
                    let value = r.or_py().and_then(|r| {
                        r.value_at(&family, &q, ts.unwrap_or(0))
                            .map_err(|e| returned_value(r.info.seqno, e))
                    });
                    match value.and_then(|v| v.into_py_any(py)) {
                        Ok(v) => crate::aio::Entry::Value(v),
                        Err(e) => crate::aio::Entry::Error(e),
                    }
                }) as crate::aio::Output
            }
        })?;
        Ok(fut)
    }

    /// Deletes and commits: the row; with `family`, that family; with `qualifier` too, the
    /// column; with `ts` too, that one version.
    #[pyo3(signature = (row, family=None, qualifier=None, *, ts=None, durability=None))]
    fn delete(
        &self,
        py: Python<'_>,
        row: &Bound<'_, PyAny>,
        family: Option<String>,
        qualifier: Option<&Bound<'_, PyAny>>,
        ts: Option<u64>,
        durability: Option<Durability>,
    ) -> PyResult<u64> {
        let op = delete_op(family, qualifier.map(convert::bytes).transpose()?, ts)?;
        commit_row(
            py,
            self.writer()?,
            convert::bytes(row)?,
            vec![op],
            Asked::default(),
            durability,
            None,
        )
        .map(|s| s.expect("unconditional"))
    }

    fn __repr__(&self) -> String {
        format!("Table({:?})", self.name())
    }
}

pub(crate) fn delete_op(
    family: Option<String>,
    qualifier: Option<Vec<u8>>,
    ts: Option<u64>,
) -> PyResult<Op> {
    Ok(match (family, qualifier, ts) {
        (None, None, None) => Op::DeleteRow,
        (Some(f), None, None) => Op::DeleteFamily(f),
        (Some(f), Some(q), None) => Op::DeleteColumn(f, q),
        (Some(f), Some(q), Some(ts)) => Op::DeleteCell(f, q, ts),
        _ => {
            return Err(PyValueError::new_err(
                "delete: a qualifier needs its family, and ts needs both",
            ));
        }
    })
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

/// Commits `ops` to `row` of `table` as one row mutation, with the GIL released: the
/// sequence number, or `None` when `condition` failed.
pub(crate) fn commit_row(
    py: Python<'_>,
    table: &pigeonhole::Table,
    row: Vec<u8>,
    ops: Vec<Op>,
    previous: Asked,
    durability: Option<Durability>,
    condition: Option<pigeonhole::Condition>,
) -> PyResult<Option<u64>> {
    let table = table.clone();
    py.detach(move || {
        // A mutation with `return_previous` requests is refused here (ICR 0113).
        let m = row_mutation(&table, &row, &ops, &previous, durability);
        match condition {
            None => m.commit().map(|i| Some(i.seqno)),
            Some(c) => m.commit_if(&c).map(|i| i.map(|i| i.seqno)),
        }
    })
    .or_py()
}

/// Commits `ops` to `row` of `table` like [`commit_row`], returning every counter cell they
/// incremented or set, as the commit left it (D314, ICR 0113).
pub(crate) fn commit_row_returning(
    py: Python<'_>,
    table: &pigeonhole::Table,
    row: Vec<u8>,
    ops: Vec<Op>,
    previous: Asked,
    durability: Option<Durability>,
) -> PyResult<Incremented> {
    let table = table.clone();
    let asked = previous.clone();
    py.detach(move || row_mutation(&table, &row, &ops, &previous, durability).commit_returning())
        .map(|r| Incremented::new(r, asked))
        .or_py()
}

/// A column: family and qualifier.
type Column = (String, Vec<u8>);

/// The columns a mutation's `return_previous` and `return_current` asked for (ICR 0113).
#[derive(Debug, Clone, Default)]
pub(crate) struct Asked {
    previous: Vec<Column>,
    current: Vec<Column>,
}

impl Asked {
    /// Whether `return_current` asked for the column, or it's `condition`'s (always
    /// reported).
    fn has_current(&self, family: &str, q: &[u8], condition: Option<&Column>) -> bool {
        self.current
            .iter()
            .chain(condition)
            .any(|(f, cq)| f == family && cq == q)
    }
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
    m = m.maybe(durability, |m, d| m.durability(d.into()));
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

/// Writes to one row, applied atomically by `commit()` (or `commit_if(condition)`).
/// Methods return the mutation, so writes chain.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Mutation {
    table: pigeonhole::Table,
    row: Vec<u8>,
    /// `None` once committed.
    ops: Mutex<Option<Vec<Op>>>,
    /// The columns `return_previous` and `return_current` asked for.
    asked: Mutex<Asked>,
    durability: Mutex<Option<Durability>>,
}

impl Mutation {
    fn push(slf: &Bound<'_, Self>, op: Op) -> PyResult<()> {
        match slf.get().ops.lock().expect("not poisoned").as_mut() {
            Some(ops) => {
                ops.push(op);
                Ok(())
            }
            None => Err(PyValueError::new_err("the mutation was already committed")),
        }
    }
}

#[pymethods]
impl Mutation {
    /// Puts a cell: bytes or str, int (`i64`) or float (`f64`).
    #[pyo3(signature = (family, qualifier, value, *, ts=None))]
    fn put<'py>(
        slf: &Bound<'py, Self>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        ts: Option<u64>,
    ) -> PyResult<Bound<'py, Self>> {
        Self::push(
            slf,
            Op::Put(
                family,
                convert::bytes(qualifier)?,
                ts,
                convert::value(value)?,
            ),
        )?;
        Ok(slf.clone())
    }

    /// Puts a cell holding an unsigned 64-bit int (for example a bitwise family's base).
    #[pyo3(signature = (family, qualifier, value, *, ts=None))]
    fn put_u64<'py>(
        slf: &Bound<'py, Self>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        value: u64,
        ts: Option<u64>,
    ) -> PyResult<Bound<'py, Self>> {
        Self::push(
            slf,
            Op::PutU64(family, convert::bytes(qualifier)?, ts, value),
        )?;
        Ok(slf.clone())
    }

    /// Adds `delta` to a counter cell.
    #[pyo3(signature = (family, qualifier, delta, *, ts=None))]
    fn incr<'py>(
        slf: &Bound<'py, Self>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        delta: i64,
        ts: Option<u64>,
    ) -> PyResult<Bound<'py, Self>> {
        Self::push(slf, Op::Incr(family, convert::bytes(qualifier)?, ts, delta))?;
        Ok(slf.clone())
    }

    /// Adds an input to an aggregate family's cell, read by the family's kind.
    #[pyo3(signature = (family, qualifier, value, *, ts=None))]
    fn aggregate<'py>(
        slf: &Bound<'py, Self>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        ts: Option<u64>,
    ) -> PyResult<Bound<'py, Self>> {
        let input = aggregate_input(&slf.get().table, &family, value)?;
        Self::push(
            slf,
            Op::Aggregate(family, convert::bytes(qualifier)?, ts, input),
        )?;
        Ok(slf.clone())
    }

    /// Puts a vector in a vector family: a buffer (a NumPy array, `array.array`) or a
    /// sequence, of the family's element type (bits: packed, bytes-like).
    #[pyo3(signature = (family, qualifier, vector, *, ts=None))]
    fn put_vector<'py>(
        slf: &Bound<'py, Self>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
        vector: &Bound<'_, PyAny>,
        ts: Option<u64>,
    ) -> PyResult<Bound<'py, Self>> {
        let v = vector_input(&slf.get().table, &family, vector)?;
        Self::push(
            slf,
            Op::PutVector(family, convert::bytes(qualifier)?, ts, v),
        )?;
        Ok(slf.clone())
    }

    /// Deletes the row; with `family`, that family; with `qualifier` too, the column; with
    /// `ts` too, that one version.
    #[pyo3(signature = (family=None, qualifier=None, *, ts=None))]
    fn delete<'py>(
        slf: &Bound<'py, Self>,
        family: Option<String>,
        qualifier: Option<&Bound<'_, PyAny>>,
        ts: Option<u64>,
    ) -> PyResult<Bound<'py, Self>> {
        Self::push(
            slf,
            delete_op(family, qualifier.map(convert::bytes).transpose()?, ts)?,
        )?;
        Ok(slf.clone())
    }

    /// This mutation's durability (default: the database's).
    fn durability<'py>(slf: &Bound<'py, Self>, durability: Durability) -> Bound<'py, Self> {
        *slf.get().durability.lock().expect("not poisoned") = Some(durability);
        slf.clone()
    }

    /// Commits every write atomically; returns the commit's sequence number.
    fn commit(&self, py: Python<'_>) -> PyResult<u64> {
        self.finish(py, None).map(|s| s.expect("unconditional"))
    }

    /// Commits only if `condition` holds; returns the sequence number, or `None` when the
    /// condition failed (nothing is written).
    fn commit_if(&self, py: Python<'_>, condition: &Condition) -> PyResult<Option<u64>> {
        self.finish(py, Some(condition.0.clone()))
    }

    /// Asks `commit_returning` for the column `family:qualifier` as it was just before the
    /// commit, read in the same step: `previous(family, qualifier)` on the result (ICR 0113).
    /// Only `commit_returning` (and its async form) takes such a mutation: `commit` and
    /// `commit_if` raise `InvalidArgument`, as does a mutation that writes nothing. Asking
    /// twice for one column is one request.
    fn return_previous<'py>(
        slf: &Bound<'py, Self>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, Self>> {
        let q = convert::bytes(qualifier)?;
        // Under the writes' lock, as a commit takes both (ai1249's Q1 on #1552): a commit on
        // another thread either sees this request or makes it fail as already committed.
        let ops = slf.get().ops.lock().expect("not poisoned");
        if ops.is_none() {
            return Err(PyValueError::new_err("the mutation was already committed"));
        }
        slf.get()
            .asked
            .lock()
            .expect("not poisoned")
            .previous
            .push((family, q));
        drop(ops);
        Ok(slf.clone())
    }

    /// Asks `commit_if_returning` for the column `family:qualifier` as it is, whichever way
    /// the condition goes (D314 amendment 2, ICR 0113): as of the commit when it applies,
    /// as the condition saw it when it doesn't. The condition's own column is always
    /// reported, without asking. Only `commit_if_returning` (and its async form) takes
    /// such a mutation: `commit`, `commit_if` and `commit_returning` raise `InvalidArgument`.
    /// Asking twice for one column is one request.
    fn return_current<'py>(
        slf: &Bound<'py, Self>,
        family: String,
        qualifier: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, Self>> {
        let q = convert::bytes(qualifier)?;
        // Under the writes' lock, as `return_previous` (ai1249's Q1 on #1552).
        let ops = slf.get().ops.lock().expect("not poisoned");
        if ops.is_none() {
            return Err(PyValueError::new_err("the mutation was already committed"));
        }
        slf.get()
            .asked
            .lock()
            .expect("not poisoned")
            .current
            .push((family, q));
        drop(ops);
        Ok(slf.clone())
    }

    /// Commits every write atomically if `condition` holds, like `commit_if`, and reports
    /// the row either way (compare-and-set, then read; D314 amendment 2): a `Checked`. When
    /// it applied, `checked.returned` is the `Returned` (the counters, `return_previous`'s
    /// columns, and `current` as of the commit); when it didn't, `checked.seen` is the
    /// `Seen` (`current` as the condition saw it), and nothing was written. A mutation that
    /// writes nothing is a check-and-read.
    fn commit_if_returning(&self, py: Python<'_>, condition: &Condition) -> PyResult<Checked> {
        let (table, row, ops, asked, durability) = self.parts()?;
        let c = condition.0.clone();
        let column = condition_column(&c);
        let req = asked.clone();
        let r = py
            .detach(move || {
                row_mutation(&table, &row, &ops, &req, durability).commit_if_returning(&c)
            })
            .or_py()?;
        Checked::new(py, r, asked, column)
    }

    /// `commit_if_returning(condition)` as an `asyncio.Future` of the running loop: the
    /// `Checked`.
    fn commit_if_returning_async<'py>(
        &self,
        py: Python<'py>,
        condition: &Condition,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (event_loop, fut) = crate::aio::new_future(py)?;
        let (table, row, ops, asked, durability) = self.parts()?;
        let c = condition.0.clone();
        let column = condition_column(&c);
        let req = asked.clone();
        crate::aio::start(py, &event_loop, &fut, move || {
            let f =
                row_mutation(&table, &row, &ops, &req, durability).commit_if_returning_async(&c);
            async move {
                crate::aio::output(f.await, move |py, r| {
                    Ok(Py::new(py, Checked::new(py, r, asked, column)?)?.into_any())
                })
            }
        })?;
        Ok(fut)
    }

    /// Commits every write atomically, like `commit`, and returns each counter cell it
    /// incremented or set, as a read at the commit sees it (D314), and the columns
    /// `return_previous` asked for, as they were just before it. Concurrent increments of
    /// one cell each get their own value.
    fn commit_returning(&self, py: Python<'_>) -> PyResult<Incremented> {
        let (_, _, ops, previous, durability) = self.parts()?;
        commit_row_returning(py, &self.table, self.row.clone(), ops, previous, durability)
    }

    /// `commit()` as an `asyncio.Future` of the running loop (#1479): the commit's sequence
    /// number. No thread waits for it. Cancelled while the commit waits for room, nothing is
    /// written; once it's submitted it may still land (`asyncio.shield` keeps the outcome).
    fn commit_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (event_loop, fut) = crate::aio::new_future(py)?;
        let (table, row, ops, previous, durability) = self.parts()?;
        crate::aio::start(py, &event_loop, &fut, move || {
            let f = row_mutation(&table, &row, &ops, &previous, durability).commit_async();
            async move { crate::aio::output(f.await, |py, i| i.seqno.into_py_any(py)) }
        })?;
        Ok(fut)
    }

    /// `commit_if(condition)` as an `asyncio.Future`: the sequence number, or `None` when
    /// the condition failed.
    fn commit_if_async<'py>(
        &self,
        py: Python<'py>,
        condition: &Condition,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (event_loop, fut) = crate::aio::new_future(py)?;
        let (table, row, ops, previous, durability) = self.parts()?;
        let c = condition.0.clone();
        crate::aio::start(py, &event_loop, &fut, move || {
            let f = row_mutation(&table, &row, &ops, &previous, durability).commit_if_async(&c);
            async move { crate::aio::output(f.await, |py, i| i.map(|i| i.seqno).into_py_any(py)) }
        })?;
        Ok(fut)
    }

    /// `commit_returning()` as an `asyncio.Future`: the `Incremented` cells.
    fn commit_returning_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (event_loop, fut) = crate::aio::new_future(py)?;
        let (table, row, ops, previous, durability) = self.parts()?;
        let asked = previous.clone();
        crate::aio::start(py, &event_loop, &fut, move || {
            let f =
                row_mutation(&table, &row, &ops, &previous, durability).commit_returning_async();
            async move {
                crate::aio::output(f.await, move |py, r| {
                    Ok(Py::new(py, Incremented::new(r, asked))?.into_any())
                })
            }
        })?;
        Ok(fut)
    }
}

impl Mutation {
    fn finish(
        &self,
        py: Python<'_>,
        condition: Option<pigeonhole::Condition>,
    ) -> PyResult<Option<u64>> {
        let (_, _, ops, previous, durability) = self.parts()?;
        commit_row(
            py,
            &self.table,
            self.row.clone(),
            ops,
            previous,
            durability,
            condition,
        )
    }

    /// What a commit takes: the table, the row, the buffered writes and the durability.
    fn parts(&self) -> PyResult<CommitParts> {
        let (ops, previous) = {
            let mut ops = self.ops.lock().expect("not poisoned");
            let taken = ops
                .take()
                .ok_or_else(|| PyValueError::new_err("the mutation was already committed"))?;
            // Under the writes' lock, as `return_previous` pushes (Q1).
            let previous = std::mem::take(&mut *self.asked.lock().expect("not poisoned"));
            (taken, previous)
        };
        let durability = *self.durability.lock().expect("not poisoned");
        Ok((
            self.table.clone(),
            self.row.clone(),
            ops,
            previous,
            durability,
        ))
    }
}

/// What a mutation's commit takes: its table, row, writes, `return_previous` requests and
/// durability.
type CommitParts = (
    pigeonhole::Table,
    Vec<u8>,
    Vec<Op>,
    Asked,
    Option<Durability>,
);

/// The counter cells a `Mutation.commit_returning()` incremented or set, after its commit
/// (D314): `value(family, qualifier, ts=None)`, or index it with `(family, qualifier)` or
/// `(family, qualifier, ts)`. Iterating gives the keys `(family, qualifier, ts)`, `ts` 0 for
/// a plain counter, in the order the mutation first wrote each.
///
/// It also holds the columns `return_previous` asked for, as they were just before the
/// commit: `previous(family, qualifier)`. `Returned` is the same class (ICR 0113).
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Incremented(pigeonhole::Incremented, Asked, Option<Column>);

/// One of `Incremented.cells()`: family, qualifier, ts, value.
type IncrementedCell<'py> = (String, Bound<'py, PyBytes>, u64, Option<i64>);

impl Incremented {
    pub(crate) fn new(r: pigeonhole::Incremented, asked: Asked) -> Self {
        Self(r, asked, None)
    }

    /// The cell `key` names: `(family, qualifier)` or `(family, qualifier, ts)`.
    fn key(key: &Bound<'_, PyAny>) -> PyResult<(String, Vec<u8>, u64)> {
        let t = key.cast::<pyo3::types::PyTuple>().map_err(|_| {
            PyTypeError::new_err(
                "an Incremented key is (family, qualifier) or (family, qualifier, ts)",
            )
        })?;
        let ts = match t.len() {
            2 => 0,
            3 => t.get_item(2)?.extract()?,
            _ => {
                return Err(PyTypeError::new_err(
                    "an Incremented key is (family, qualifier) or (family, qualifier, ts)",
                ));
            }
        };
        Ok((
            t.get_item(0)?.extract()?,
            convert::bytes(&t.get_item(1)?)?,
            ts,
        ))
    }

    fn contains(&self, family: &str, qualifier: &[u8], ts: u64) -> bool {
        self.0
            .iter()
            .any(|(f, q, t, _)| f == family && q == qualifier && t == ts)
    }

    /// A cell the commit wrote: its value, or `ReturnedValueError` if it couldn't be read.
    fn read(&self, family: &str, qualifier: &[u8], ts: u64) -> PyResult<Option<i64>> {
        self.0
            .value_at(family, qualifier, ts)
            .map_err(|e| returned_value(self.0.info.seqno, e))
    }
}

#[pymethods]
impl Incremented {
    /// The commit's sequence number.
    #[getter]
    fn seqno(&self) -> u64 {
        self.0.info.seqno
    }

    /// The counter (or with `ts`, the bucket) in `family` at `qualifier`, after the commit.
    /// `None` when a read at the commit sees no cell: one the mutation deleted, or a bucket
    /// already past the family's TTL. Raises `InvalidArgument` if the commit didn't write
    /// that cell, and `ReturnedValueError` if the value couldn't be read after the commit
    /// applied: **the commit succeeded** then, so don't retry it.
    #[pyo3(signature = (family, qualifier, *, ts=None))]
    fn value(
        &self,
        family: &str,
        qualifier: &Bound<'_, PyAny>,
        ts: Option<u64>,
    ) -> PyResult<Option<i64>> {
        let (q, ts) = (convert::bytes(qualifier)?, ts.unwrap_or(0));
        if self.contains(family, &q, ts) {
            self.read(family, &q, ts)
        } else {
            // `InvalidArgument`, naming the cell.
            self.0.value_at(family, &q, ts).or_py()
        }
    }

    /// The column `family:qualifier` that `return_previous` asked for, as it was just before
    /// the commit: its newest visible version then, as a `Cell`, or `None` if it had none.
    /// Raises `InvalidArgument` if the mutation didn't ask for that column, and
    /// `ReturnedValueError` if it couldn't be read after the commit applied: **the commit
    /// succeeded** then, so don't retry it.
    fn previous(&self, family: &str, qualifier: &Bound<'_, PyAny>) -> PyResult<Option<Cell>> {
        let q = convert::bytes(qualifier)?;
        let asked = self
            .1
            .previous
            .iter()
            .any(|(f, aq)| f == family && *aq == q);
        let r = self.0.previous(family, &q);
        if asked {
            r.map(|c| c.map(Cell))
                .map_err(|e| returned_value(self.0.info.seqno, e))
        } else {
            // `InvalidArgument`, naming the column.
            r.map(|c| c.map(Cell)).or_py()
        }
    }

    /// After an applied `commit_if_returning`: the column `family:qualifier` as of the
    /// commit (the condition's own column is the new value; `return_previous` gives the old
    /// one), or `None` if a read at the commit sees none. Raises `InvalidArgument` if neither
    /// `return_current` nor the condition named the column, and `ReturnedValueError` if it
    /// couldn't be read after the commit applied.
    fn current(&self, family: &str, qualifier: &Bound<'_, PyAny>) -> PyResult<Option<Cell>> {
        let q = convert::bytes(qualifier)?;
        let r = self.0.current(family, &q).map(|c| c.map(Cell));
        if self.1.has_current(family, &q, self.2.as_ref()) {
            r.map_err(|e| returned_value(self.0.info.seqno, e))
        } else {
            // `InvalidArgument`, naming the column.
            r.or_py()
        }
    }

    /// Every cell as `(family, qualifier, ts, value)`, in the order the mutation first wrote
    /// each; raises `ReturnedValueError` for the first unreadable value, as `value` does.
    fn cells<'py>(&self, py: Python<'py>) -> PyResult<Vec<IncrementedCell<'py>>> {
        let seqno = self.0.info.seqno;
        self.0
            .iter()
            .map(|(f, q, ts, v)| {
                let v = v.map_err(|e| returned_value(seqno, e))?;
                Ok((f.to_owned(), PyBytes::new(py, q), ts, v))
            })
            .collect()
    }

    fn __getitem__(&self, key: &Bound<'_, PyAny>) -> PyResult<Option<i64>> {
        let (f, q, ts) = Self::key(key)?;
        if !self.contains(&f, &q, ts) {
            return Err(pyo3::exceptions::PyKeyError::new_err(key.clone().unbind()));
        }
        self.read(&f, &q, ts)
    }

    fn __contains__(&self, key: &Bound<'_, PyAny>) -> bool {
        Self::key(key).is_ok_and(|(f, q, ts)| self.contains(&f, &q, ts))
    }

    fn __len__(&self) -> usize {
        self.0.iter().count()
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let keys: Vec<(String, Bound<'py, PyBytes>, u64)> = self
            .0
            .iter()
            .map(|(f, q, ts, _)| (f.to_owned(), PyBytes::new(py, q), ts))
            .collect();
        Ok(pyo3::types::PyList::new(py, keys)?
            .as_any()
            .try_iter()?
            .into_any())
    }

    fn __repr__(&self) -> String {
        format!(
            "Incremented(seqno={}, cells={})",
            self.0.info.seqno,
            self.0.iter().count()
        )
    }
}

/// What `Mutation.commit_if_returning` returns (D314 amendment 2): whether the condition
/// held (`applied`), with the row either way: `returned` when it applied, `seen` when it
/// didn't (and nothing was written).
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Checked {
    returned: Option<Py<Incremented>>,
    seen: Option<Py<Seen>>,
}

impl Checked {
    fn new(
        py: Python<'_>,
        r: pigeonhole::Checked,
        asked: Asked,
        column: Option<Column>,
    ) -> PyResult<Self> {
        Ok(match r {
            pigeonhole::Checked::Applied(ret) => Self {
                returned: Some(Py::new(py, Incremented(ret, asked, column))?),
                seen: None,
            },
            pigeonhole::Checked::NotApplied(seen) => Self {
                returned: None,
                seen: Some(Py::new(py, Seen(seen))?),
            },
        })
    }
}

#[pymethods]
impl Checked {
    /// Whether the condition held and the mutation committed.
    #[getter]
    fn applied(&self) -> bool {
        self.returned.is_some()
    }

    /// When it applied: the `Returned` (`current`, `previous`, the counters), else `None`.
    #[getter]
    fn returned(&self, py: Python<'_>) -> Option<Py<Incremented>> {
        self.returned.as_ref().map(|r| r.clone_ref(py))
    }

    /// When it didn't: the `Seen` (`current` as the condition saw it), else `None`.
    #[getter]
    fn seen(&self, py: Python<'_>) -> Option<Py<Seen>> {
        self.seen.as_ref().map(|s| s.clone_ref(py))
    }

    fn __repr__(&self) -> String {
        format!(
            "Checked(applied={})",
            if self.applied() { "True" } else { "False" }
        )
    }
}

/// The row as a failed `commit_if_returning`'s condition saw it: `current(family,
/// qualifier)` for the condition's column and `return_current`'s.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Seen(pigeonhole::Seen);

#[pymethods]
impl Seen {
    /// The column as the condition saw it: its newest visible version then, as a `Cell`, or
    /// `None` if it had none. Raises `InvalidArgument` if neither `return_current` nor the
    /// condition named it. Nothing was written, so a read error here is an ordinary one.
    fn current(&self, family: &str, qualifier: &Bound<'_, PyAny>) -> PyResult<Option<Cell>> {
        self.0
            .current(family, &convert::bytes(qualifier)?)
            .map(|c| c.map(Cell))
            .or_py()
    }

    fn __repr__(&self) -> &'static str {
        "Seen(...)"
    }
}

/// Creates or opens a table: `Database.table(name).family(...)...` then `create()`,
/// `create_if_missing()` or `open()`.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct TableBuilder {
    pub(crate) db: pigeonhole::Pigeonhole,
    pub(crate) name: String,
    pub(crate) families: Vec<(String, pigeonhole::Family)>,
}

#[pymethods]
impl TableBuilder {
    /// Adds a family (`Family()` for a plain one when `options` is left out).
    #[pyo3(signature = (name, options=None))]
    fn family(&self, name: String, options: Option<&Family>) -> Self {
        let mut families = self.families.clone();
        families.push((
            name,
            options.map_or_else(pigeonhole::Family::default, |f| f.0.clone()),
        ));
        Self {
            db: self.db.clone(),
            name: self.name.clone(),
            families,
        }
    }

    /// Creates the table (`TableExists` if it exists).
    fn create(&self, py: Python<'_>) -> PyResult<Table> {
        self.build(py, Kind::Create)
    }

    /// Creates the table if missing; an existing one is opened, its families unchanged.
    fn create_if_missing(&self, py: Python<'_>) -> PyResult<Table> {
        self.build(py, Kind::CreateIfMissing)
    }

    /// Opens an existing table (`TableNotFound` if there is none).
    fn open(&self, py: Python<'_>) -> PyResult<Table> {
        self.build(py, Kind::Open)
    }
}

enum Kind {
    Create,
    CreateIfMissing,
    Open,
}

impl TableBuilder {
    fn build(&self, py: Python<'_>, kind: Kind) -> PyResult<Table> {
        let db = self.db.clone();
        let name = self.name.clone();
        let families = self.families.clone();
        py.detach(move || {
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
        .or_py()
        .map(|t| Table(Handle::Writer(t)))
    }
}
