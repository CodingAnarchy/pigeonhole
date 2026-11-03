//! Read results: cells, rows, snapshots, and the native scan cursor the Ruby `Enumerator`
//! reads in chunks.

use std::ops::Bound as KeyBound;
use std::sync::Mutex;

use magnus::{Error, Integer, IntoValue, RArray, RString, Ruby, TryConvert, Value, prelude::*};
use pigeonhole::ErrorCode;

use crate::classes::{Cell, Row, ScanCursor, ScanSpec, Snapshot};
use crate::convert::{Bytes, Name, binary, class_name, lock, type_error};
use crate::error::{engine, with_code};
use crate::fork::ForkSafe;
use crate::gvl::without_gvl;
use crate::table::either;

// ---- Cell ---------------------------------------------------------------------------------

/// A cell's value as its Ruby type: a binary String, an Integer (an `i64` cell) or a Float
/// (an `f64` cell); `nil` for a type this build doesn't read (vectors, PR 2).
fn typed(ruby: &Ruby, v: pigeonhole::Value<'_>) -> Value {
    match v {
        pigeonhole::Value::I64(n) | pigeonhole::Value::Varint(n) => n.into_value_with(ruby),
        pigeonhole::Value::F64(x) => x.into_value_with(ruby),
        pigeonhole::Value::Bytes(b) => binary(ruby, b).as_value(),
        _ => ruby.qnil().as_value(),
    }
}

impl Cell {
    /// The value's bytes, as stored (an `i64`/`f64` cell's 8 bytes, little-endian), as a
    /// binary String.
    pub(crate) fn value(ruby: &Ruby, rb_self: &Self) -> RString {
        binary(ruby, rb_self.0.value())
    }

    /// The timestamp, in microseconds since the Unix epoch.
    pub(crate) fn ts(&self) -> u64 {
        self.0.timestamp()
    }

    /// The value as its type (see [`typed`]).
    pub(crate) fn typed(ruby: &Ruby, rb_self: &Self) -> Value {
        typed(ruby, rb_self.0.typed())
    }

    /// The value as an Integer, if it is an `i64` cell (counters included).
    pub(crate) fn as_int(&self) -> Option<i64> {
        self.0.as_i64()
    }

    /// The value's 8 bytes as an unsigned Integer (bitwise aggregate families, `put_u64`).
    pub(crate) fn as_u64(&self) -> Option<u64> {
        self.0.as_u64()
    }

    /// The value as a Float, if it is an `f64` cell.
    pub(crate) fn as_float(&self) -> Option<f64> {
        self.0.as_f64()
    }

    /// An `hll` family's distinct-count estimate.
    pub(crate) fn hll_estimate(&self) -> Option<u64> {
        self.0.hll_estimate()
    }

    /// A `sum_f64` family's exact sum, rounded once.
    pub(crate) fn sum_f64(&self) -> Option<f64> {
        self.0.sum_f64()
    }

    /// A `mean_var` family's `[count, mean, variance]`.
    pub(crate) fn mean_var(&self) -> Option<(u64, f64, f64)> {
        self.0
            .mean_var()
            .map(|m| (m.count(), m.mean(), m.variance()))
    }

    /// A `histogram` family's bucket counts.
    pub(crate) fn histogram_counts(&self) -> Option<Vec<u64>> {
        self.0.histogram_counts()
    }

    /// A `quantiles` family's estimate of quantile `q` (0 to 1).
    pub(crate) fn quantile(&self, q: f64) -> Option<f64> {
        self.0.quantile(q)
    }

    /// Whether a `bloom` family may contain `element` (an Integer or a String).
    pub(crate) fn bloom_contains(
        ruby: &Ruby,
        rb_self: &Self,
        element: Value,
    ) -> Result<Option<bool>, Error> {
        if let Some(n) = Integer::from_value(element) {
            return Ok(rb_self.0.bloom_contains(n.to_i64()?));
        }
        if let Some(s) = RString::from_value(element) {
            return Ok(rb_self.0.bloom_contains_bytes(&s.to_bytes()));
        }
        Err(type_error(
            ruby,
            format!(
                "a bloom element is an Integer or a String, not {}",
                class_name(element)
            ),
        ))
    }

    /// Equal value bytes and timestamp.
    pub(crate) fn eq(&self, other: Value) -> bool {
        <&Cell>::try_convert(other)
            .is_ok_and(|o| o.0.value() == self.0.value() && o.0.timestamp() == self.0.timestamp())
    }

    /// `#<Pigeonhole::Cell ts=... value=...>`.
    pub(crate) fn inspect(ruby: &Ruby, rb_self: &Self) -> Result<String, Error> {
        let v: String = typed(ruby, rb_self.0.typed()).funcall("inspect", ())?;
        Ok(format!(
            "#<Pigeonhole::Cell ts={} value={v}>",
            rb_self.0.timestamp()
        ))
    }
}

// ---- Row ----------------------------------------------------------------------------------

impl Row {
    /// The row key, as a binary String.
    pub(crate) fn key(ruby: &Ruby, rb_self: &Self) -> RString {
        binary(ruby, rb_self.0.key())
    }

    /// The newest version of `family:qualifier` in the row, if the read returned it.
    pub(crate) fn get(&self, family: Name, qualifier: Bytes) -> Option<Cell> {
        self.0.get(&family.0, &qualifier.0).map(|c| Cell(c.clone()))
    }

    /// The cells as `[family, qualifier, cell]` Arrays: by family (in the read's family
    /// order), then qualifier, then newest version first.
    pub(crate) fn cells(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let out = ruby.ary_new_capa(rb_self.0.len());
        for e in (0..rb_self.0.len()).filter_map(|i| rb_self.0.entry(i)) {
            let entry = ruby.ary_new_capa(3);
            entry.push(e.family)?;
            entry.push(binary(ruby, e.qualifier))?;
            entry.push(Cell(e.cell.to_owned()))?;
            out.push(entry)?;
        }
        Ok(out)
    }

    /// The number of cells.
    pub(crate) fn size(&self) -> usize {
        self.0.len()
    }

    /// `#<Pigeonhole::Row key=... cells=n>`.
    pub(crate) fn inspect(ruby: &Ruby, rb_self: &Self) -> Result<String, Error> {
        let k: String = binary(ruby, rb_self.0.key()).funcall("inspect", ())?;
        Ok(format!(
            "#<Pigeonhole::Row key={k} cells={}>",
            rb_self.0.len()
        ))
    }
}

// ---- Snapshot -----------------------------------------------------------------------------

impl Snapshot {
    /// Wraps an engine snapshot.
    pub(crate) fn new(s: pigeonhole::Snapshot) -> Self {
        Self(ForkSafe::new(Mutex::new(Some(s))))
    }

    /// The snapshot, or `SnapshotExpired` once released.
    pub(crate) fn get(&self, ruby: &Ruby) -> Result<pigeonhole::Snapshot, Error> {
        lock(self.0.get(ruby)?).clone().ok_or_else(|| {
            with_code(
                ruby,
                ErrorCode::SnapshotExpired,
                "the snapshot was released",
            )
        })
    }

    /// The snapshot's sequence number.
    pub(crate) fn seqno(ruby: &Ruby, rb_self: &Self) -> Result<u64, Error> {
        Ok(rb_self.get(ruby)?.seqno())
    }

    /// Releases the snapshot: reads with it raise `SnapshotExpired` afterwards. Releasing
    /// twice does nothing.
    pub(crate) fn release(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let s = lock(rb_self.0.get(ruby)?).take();
        // Dropping the last handle unpins its data: no Ruby value involved.
        without_gvl(ruby, move || drop(s))
    }
}

/// An optional `snapshot:` argument.
pub(crate) fn snapshot_arg(
    ruby: &Ruby,
    s: Option<&Snapshot>,
) -> Result<Option<pigeonhole::Snapshot>, Error> {
    s.map(|s| s.get(ruby)).transpose()
}

// ---- Scans --------------------------------------------------------------------------------

impl ScanSpec {
    /// Starts one enumeration: a cursor reading at `snapshot`, or at a snapshot it takes
    /// now (simp724's R3: every chunk of one enumeration comes from one snapshot).
    pub(crate) fn open(
        ruby: &Ruby,
        rb_self: &Self,
        snapshot: Option<&Snapshot>,
    ) -> Result<ScanCursor, Error> {
        let snap = snapshot_arg(ruby, snapshot)?;
        let a = rb_self.0.get(ruby)?;
        let table = a.table.clone();
        let (prefix, start, end) = (a.prefix.clone(), a.start.clone(), a.end.clone());
        let (families, qp) = (a.families.clone(), a.qualifier_prefix.clone());
        let (versions, limit, column_limit) = (a.versions, a.limit, a.column_limit);
        let it = engine(ruby, move || {
            either!(&table, |t| {
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
                // The iterator holds the snapshot it started at (the given one, or the
                // latest) until it finishes or is dropped.
                s.into_owned().iter()
            })
        })?;
        Ok(ScanCursor {
            rows: ForkSafe::new(Mutex::new(Some(it))),
            chunk: a.chunk,
        })
    }
}

impl ScanCursor {
    /// The next chunk of rows (an Array, empty at the end), read with the GVL released.
    pub(crate) fn next_chunk(ruby: &Ruby, rb_self: &Self) -> Result<RArray, Error> {
        let rows = rb_self.rows.get(ruby)?;
        let chunk = rb_self.chunk;
        // The cursor's mutex is only ever taken without the GVL, so a second thread
        // waiting on it never blocks a thread that needs the GVL back.
        let fetched = engine(ruby, || {
            let mut guard = lock(rows);
            let Some(it) = guard.as_mut() else {
                return Ok(Vec::new());
            };
            let mut out = Vec::with_capacity(chunk);
            while out.len() < chunk {
                match it.next_ref() {
                    Ok(Some(r)) => out.push(r.to_owned()),
                    Ok(None) => {
                        *guard = None;
                        break;
                    }
                    Err(e) => {
                        *guard = None;
                        return Err(e);
                    }
                }
            }
            Ok(out)
        })?;
        let a = ruby.ary_new_capa(fetched.len());
        for r in fetched {
            a.push(Row(r))?;
        }
        Ok(a)
    }

    /// Ends the enumeration: its read and snapshot are released now.
    pub(crate) fn close(ruby: &Ruby, rb_self: &Self) -> Result<(), Error> {
        let rows = rb_self.rows.get(ruby)?;
        without_gvl(ruby, || drop(lock(rows).take()))
    }
}
