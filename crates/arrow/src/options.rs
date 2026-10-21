//! What an export emits: batch limits and the column types.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// How an export shapes its batches and columns. See the [crate docs](crate) for the schema.
///
/// ```
/// use pigeonhole_arrow::{ArrowOptions, TsType};
///
/// let options = ArrowOptions::default()
///     .batch_cells(64 * 1024)
///     .typed_values(true)
///     .ts(TsType::UInt64);
/// assert_eq!(pigeonhole_arrow::schema(&options).fields().len(), 8);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrowOptions {
    pub(crate) batch_cells: usize,
    pub(crate) batch_bytes: usize,
    pub(crate) row_dictionary: bool,
    pub(crate) utf8_keys: bool,
    pub(crate) typed_values: bool,
    pub(crate) ts: TsType,
}

/// The largest [`ArrowOptions::batch_bytes`]: the byte columns use 32-bit offsets.
const MAX_BATCH_BYTES: usize = 1 << 30;

impl Default for ArrowOptions {
    fn default() -> Self {
        Self {
            batch_cells: 8192,
            batch_bytes: 8 << 20,
            row_dictionary: false,
            utf8_keys: false,
            typed_values: false,
            ts: TsType::Timestamp,
        }
    }
}

impl ArrowOptions {
    /// Close a batch at `n` cells (Arrow rows). Default 8192; at least 1.
    pub fn batch_cells(mut self, n: usize) -> Self {
        self.batch_cells = n.max(1);
        self
    }

    /// Close a batch once its rows, qualifiers and values reach `n` bytes. Default 8 MiB;
    /// at least 1, at most 1 GiB. A batch always holds at least one cell, however large.
    pub fn batch_bytes(mut self, n: usize) -> Self {
        self.batch_bytes = n.clamp(1, MAX_BATCH_BYTES);
        self
    }

    /// `row` as `Dictionary<Int32, Binary>` (or `Utf8` values with
    /// [`utf8_keys`](Self::utf8_keys)): each row key is stored once per batch. Default off.
    /// Rows arrive in key order, so building it costs no hashing; each batch has its own
    /// dictionary.
    pub fn row_dictionary(mut self, on: bool) -> Self {
        self.row_dictionary = on;
        self
    }

    /// `row` and `qualifier` as `Utf8` instead of `Binary`. A key that is not valid UTF-8
    /// fails the batch with an error naming it; nothing is replaced. Default off.
    pub fn utf8_keys(mut self, on: bool) -> Self {
        self.utf8_keys = on;
        self
    }

    /// Adds the nullable columns `value_i64` (i64 and varint cells, so counters) and
    /// `value_f64` (f64 cells). Default off.
    pub fn typed_values(mut self, on: bool) -> Self {
        self.typed_values = on;
        self
    }

    /// The `ts` column's type. Default [`TsType::Timestamp`].
    pub fn ts(mut self, ts: TsType) -> Self {
        self.ts = ts;
        self
    }
}

/// The type of the `ts` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TsType {
    /// `Timestamp(Microsecond, "UTC")`: Pigeonhole's timestamps are microseconds since the
    /// Unix epoch (D11). A timestamp above `i64::MAX` fails the batch with an error naming
    /// its row.
    Timestamp,
    /// `UInt64`: the timestamp as stored, whatever its value.
    UInt64,
}

/// The schema an export with `options` produces (every batch of an [`ArrowScan`](crate::ArrowScan)
/// has it).
pub fn schema(options: &ArrowOptions) -> SchemaRef {
    let key = if options.utf8_keys {
        DataType::Utf8
    } else {
        DataType::Binary
    };
    let row = if options.row_dictionary {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(key.clone()))
    } else {
        key.clone()
    };
    let ts = match options.ts {
        TsType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        TsType::UInt64 => DataType::UInt64,
    };
    let mut fields = vec![
        Field::new("row", row, false),
        Field::new(
            "family",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("qualifier", key, false),
        Field::new("ts", ts, false),
        Field::new("value", DataType::Binary, false),
        Field::new(
            "value_type",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
    ];
    if options.typed_values {
        fields.push(Field::new("value_i64", DataType::Int64, true));
        fields.push(Field::new("value_f64", DataType::Float64, true));
    }
    Arc::new(Schema::new(fields))
}
