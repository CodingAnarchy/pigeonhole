//! Tables: families, the table builder, reads, and writes recorded as operations that one
//! engine call applies (a row mutation, a batch or a transaction).

use std::ops::Bound as KeyBound;
use std::sync::Mutex;

use pigeonhole::{Aggregation, Compression};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

use crate::convert::{self, Val};
use crate::error::{OrPy, with_code};
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
    if let Some(n) = max_versions {
        f = f.max_versions(n);
    }
    if let Some(t) = ttl {
        f = f.ttl(convert::duration(t)?);
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
            if let Some(f) = &families {
                s = s.families(f.iter().map(String::as_str));
            }
            if let Some(q) = &qp {
                s = s.qualifier_prefix(q);
            }
            if let Some(n) = limit {
                s = s.limit(n);
            }
            if let Some(n) = column_limit {
                s = s.column_limit(n);
            }
            if let Some(sn) = &snap {
                s = s.snapshot(sn);
            }
            s.into_owned()
        });
        ScanIter::new(scan, chunk)
    }

    /// The rows of `keys` (a multi-get, D299), in key order, with the selection `row` takes;
    /// keys whose row has no cell the selection keeps are left out.
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
    ) -> PyResult<Vec<Row>> {
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
    #[pyo3(signature = (seeds, family, *, hops=1, per_node_limit=None, hop_limit=None, max_nodes=None, parents=false, qualifier_prefix=None, snapshot=None, parallel=None))]
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
            parents,
            qualifier_prefix,
            snapshot,
            parallel,
        )
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
            durability,
            None,
        )
        .map(|s| s.expect("unconditional"))
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
    durability: Option<Durability>,
    condition: Option<pigeonhole::Condition>,
) -> PyResult<Option<u64>> {
    let table = table.clone();
    py.detach(move || {
        let mut m = table.mutate(&row);
        if let Some(d) = durability {
            m = m.durability(d.into());
        }
        for op in &ops {
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
                Op::Aggregate(f, q, Some(ts), Input::Bytes(b)) => {
                    m.aggregate_bytes_at(f, q, *ts, b)
                }
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
        match condition {
            None => m.commit().map(|i| Some(i.seqno)),
            Some(c) => m.commit_if(&c).map(|i| i.map(|i| i.seqno)),
        }
    })
    .or_py()
}

/// Writes to one row, applied atomically by `commit()` (or `commit_if(condition)`).
/// Methods return the mutation, so writes chain.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Mutation {
    table: pigeonhole::Table,
    row: Vec<u8>,
    /// `None` once committed.
    ops: Mutex<Option<Vec<Op>>>,
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
}

impl Mutation {
    fn finish(
        &self,
        py: Python<'_>,
        condition: Option<pigeonhole::Condition>,
    ) -> PyResult<Option<u64>> {
        let ops = self
            .ops
            .lock()
            .expect("not poisoned")
            .take()
            .ok_or_else(|| PyValueError::new_err("the mutation was already committed"))?;
        let durability = *self.durability.lock().expect("not poisoned");
        commit_row(
            py,
            &self.table,
            self.row.clone(),
            ops,
            durability,
            condition,
        )
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
