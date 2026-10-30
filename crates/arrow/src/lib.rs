//! Arrow `RecordBatch` export for Pigeonhole scans (#691).
//!
//! [`scan_to_arrow`] turns a configured [`pigeonhole::Scan`] into an [`ArrowScan`]: a
//! streaming [`RecordBatchReader`] of batches with one Arrow row per cell (the "long"
//! schema), for handoff to DuckDB, Polars or anything else that reads Arrow. Everything the
//! scan selects (families, qualifier filters, versions, time range, value filter, snapshot,
//! limit) passes through unchanged, so the batches hold exactly a plain scan's cells, in its
//! order.
//!
//! | column | type | |
//! |---|---|---|
//! | `row` | `Binary` | `Dictionary<Int32, Binary>` with [`ArrowOptions::row_dictionary`]; `Utf8` with [`ArrowOptions::utf8_keys`] |
//! | `family` | `Dictionary<Int32, Utf8>` | |
//! | `qualifier` | `Binary` | `Utf8` with [`ArrowOptions::utf8_keys`] |
//! | `ts` | `Timestamp(Microsecond, "UTC")` | `UInt64` with [`TsType::UInt64`] |
//! | `value` | `Binary` | [`CellRef::value`](pigeonhole::CellRef::value): the bytes, or a typed value's little-endian encoding |
//! | `value_type` | `Dictionary<Int8, Utf8>` | `bytes`, `i64`, `f64` or `varint` |
//! | `value_i64` | `Int64`, nullable | with [`ArrowOptions::typed_values`]: i64 and varint cells (counters, min and max families) |
//! | `value_f64` | `Float64`, nullable | with [`ArrowOptions::typed_values`]: f64 cells |
//!
//! Each key and value is copied once into the batch's buffers; nothing is allocated per
//! cell. A batch closes at [`ArrowOptions::batch_cells`] cells or
//! [`ArrowOptions::batch_bytes`] bytes of keys and values, whichever comes first, so a wide
//! row may span batches.
//!
//! ```
//! use pigeonhole::{Family, Options, Pigeonhole};
//! use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = pigeonhole::doc_support::temp_dir();
//! let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
//! let t = db.table("users")?.family("profile", Family::default()).create_if_missing()?;
//! t.mutate(b"u1").put("profile", b"name", b"Ada").put_i64("profile", b"age", 36).commit()?;
//!
//! let mut cells = 0;
//! for batch in scan_to_arrow(t.scan_prefix(b""), &ArrowOptions::default())? {
//!     cells += batch?.num_rows();
//! }
//! assert_eq!(cells, 2);
//! # db.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! The crate pins one arrow-rs major and re-exports the crates it uses
//! ([`arrow_array`], [`arrow_buffer`], [`arrow_schema`]), so callers can name the same types.
//!
//! Part of [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole). See the crate README.
#![forbid(unsafe_code)]

mod batch;
mod options;

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use pigeonhole::{Row, RowIter, Scan};

pub use arrow_array;
pub use arrow_buffer;
pub use arrow_schema;
pub use options::{ArrowOptions, TsType, schema};

use batch::Builder;

/// Starts `scan` and returns its cells as a stream of Arrow batches.
///
/// An error starting the scan (a closed database, an unknown family, a snapshot of another
/// database) is returned here; errors reading it come out of the stream.
///
/// [`Scan::into_owned`] makes the stream `Send + 'static`, for a consumer that keeps it.
pub fn scan_to_arrow<'t>(
    scan: Scan<'t>,
    options: &ArrowOptions,
) -> pigeonhole::Result<ArrowScan<'t>> {
    let rows = scan.iter()?;
    Ok(ArrowScan {
        rows,
        builder: Builder::new(options.clone()),
        carry: None,
        done: false,
    })
}

/// A scan's cells as Arrow batches: an [`Iterator`] and a [`RecordBatchReader`]. See the
/// [crate docs](crate) for the schema.
///
/// After an error the stream is finished.
#[derive(Debug)]
#[must_use = "a stream does nothing unless iterated"]
pub struct ArrowScan<'t> {
    rows: RowIter<'t>,
    builder: Builder,
    /// The rest of a row the last batch closed in: the row (owned once the lending
    /// iterator moves on) and the next cell's index.
    carry: Option<(Row, usize)>,
    done: bool,
}

impl ArrowScan<'_> {
    /// The batches' schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(self.builder.schema())
    }

    /// Fills the builder with the next batch's cells. `Ok(false)`: the scan is done and the
    /// builder is empty.
    fn fill(&mut self) -> Result<bool, ArrowError> {
        if let Some((row, from)) = self.carry.take() {
            let view = row.view();
            if let Some(next) = self.builder.push_row(&view, from)? {
                drop(view);
                self.carry = Some((row, next));
                return Ok(true);
            }
            if self.builder.full() {
                return Ok(true);
            }
        }
        loop {
            let Some(row) = self.rows.next_ref().map_err(external)? else {
                return Ok(!self.builder.is_empty());
            };
            if let Some(next) = self.builder.push_row(&row, 0)? {
                self.carry = Some((row.to_owned(), next));
                return Ok(true);
            }
            if self.builder.full() {
                return Ok(true);
            }
        }
    }
}

impl Iterator for ArrowScan<'_> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.fill() {
            Ok(true) => Some(self.builder.finish()),
            Ok(false) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(feature = "ffi")]
impl ArrowScan<'static> {
    /// The stream as an Arrow C stream (`ArrowArrayStream`, the Arrow C stream interface),
    /// for a consumer built on another arrow major or in another language: DuckDB, Polars,
    /// pyarrow, or duckdb-rs and polars-rs, which pin arrow versions of their own.
    ///
    /// Start from [`Scan::into_owned`]: the C stream may outlive any borrow. The consumer owns
    /// the stream and calls its `release` callback when done (moving it into a
    /// `Box::into_raw` pointer is the usual way to pass it over). The stream is `Send`, not
    /// `Sync`: the consumer serializes its calls, from any thread, as the C stream
    /// interface requires. Feature `ffi`.
    ///
    /// - **After [`Pigeonhole::close`](pigeonhole::Pigeonhole::close):** the next
    ///   `get_next` fails with the database's `Closed` error (`get_last_error` says "the
    ///   database is closed") and the stream ends; `release` is still the consumer's job and
    ///   is safe to call.
    /// - **A panic** inside `get_next` (an `extern "C"` callback) aborts the consumer's
    ///   process rather than coming back as an error. The export does not panic on any input
    ///   it accepts, but an embedder should know the failure mode.
    ///
    /// ```
    /// use pigeonhole::{Family, Options, Pigeonhole};
    /// use pigeonhole_arrow::arrow_array::ffi_stream::ArrowArrayStreamReader;
    /// use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = pigeonhole::doc_support::temp_dir();
    /// let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
    /// let t = db.table("t")?.family("f", Family::default()).create_if_missing()?;
    /// t.mutate(b"r").put("f", b"q", b"v").commit()?;
    ///
    /// let stream = scan_to_arrow(t.scan_prefix(b"").into_owned(), &ArrowOptions::default())?
    ///     .into_ffi_stream();
    /// // The consumer's side: here arrow-rs's own reader of a C stream.
    /// let reader = ArrowArrayStreamReader::try_new(stream)?;
    /// let cells: usize = reader.map(|b| b.map(|b| b.num_rows())).sum::<Result<_, _>>()?;
    /// assert_eq!(cells, 1);
    /// # db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_ffi_stream(self) -> arrow_array::ffi_stream::FFI_ArrowArrayStream {
        arrow_array::ffi_stream::FFI_ArrowArrayStream::new(Box::new(self))
    }
}

impl RecordBatchReader for ArrowScan<'_> {
    fn schema(&self) -> SchemaRef {
        ArrowScan::schema(self)
    }
}

fn external(e: pigeonhole::Error) -> ArrowError {
    ArrowError::ExternalError(Box::new(e))
}

/// The user guide's Arrow page and the README, compiled and run as doctests.
#[cfg(doctest)]
mod guide {
    #[doc = include_str!("../../../docs/guide/arrow.md")]
    struct Arrow;
    #[doc = include_str!("../README.md")]
    struct CrateReadme;
}
