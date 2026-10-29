//! One batch under construction: plain buffers the cells are copied into, which become the
//! batch's Arrow buffers without a second copy. Each buffer is sized from the previous
//! batch, so a batch makes a fixed number of allocations, whatever its cell count.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, BinaryArray, DictionaryArray, Float64Array, Int8Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow_buffer::{Buffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, SchemaRef};
use pigeonhole::{CellEntry, RowRef, Value};

use crate::options::{ArrowOptions, TsType, schema};

/// The hard cap on a batch's key and value bytes: the byte columns' offsets are `i32`.
const MAX_BYTES: usize = 1 << 30;

/// `value_type`'s dictionary, in key order.
const VALUE_TYPES: [&str; 4] = ["bytes", "i64", "f64", "varint"];

/// A byte column: values back to back, with `i32` offsets.
#[derive(Debug)]
struct Bytes {
    data: Vec<u8>,
    offsets: Vec<i32>,
}

impl Bytes {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            offsets: vec![0],
        }
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), ArrowError> {
        self.data.extend_from_slice(bytes);
        let end = i32::try_from(self.data.len()).map_err(|_| {
            ArrowError::InvalidArgumentError(format!(
                "a {}-byte key or value does not fit an Arrow batch's 32-bit offsets",
                bytes.len()
            ))
        })?;
        self.offsets.push(end);
        Ok(())
    }

    /// The column's buffers, leaving it empty with room for as much again.
    fn take(&mut self) -> (OffsetBuffer<i32>, Buffer) {
        let data = take(&mut self.data);
        let mut offsets = Vec::with_capacity(self.offsets.len());
        offsets.push(0);
        let offsets = std::mem::replace(&mut self.offsets, offsets);
        (
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(data),
        )
    }

    /// The column as `Binary`, or as `Utf8` (validated: an invalid key is named).
    fn finish(&mut self, utf8: bool, what: &str) -> Result<ArrayRef, ArrowError> {
        let (offsets, data) = self.take();
        if !utf8 {
            return Ok(Arc::new(BinaryArray::try_new(offsets, data, None)?));
        }
        // Arrow validates the whole buffer once (valid UTF-8, every offset on a character
        // boundary), which holds exactly when every key is valid; only a failure walks the
        // keys, to name the first invalid one. The clones are reference counts.
        match StringArray::try_new(offsets.clone(), data.clone(), None) {
            Ok(array) => Ok(Arc::new(array)),
            Err(e) => {
                let invalid = offsets
                    .windows(2)
                    .map(|w| &data[w[0] as usize..w[1] as usize])
                    .find(|k| std::str::from_utf8(k).is_err());
                Err(match invalid {
                    Some(key) => ArrowError::InvalidArgumentError(format!(
                        "the {what} {} is not valid UTF-8 (utf8_keys)",
                        display_key(key)
                    )),
                    None => e,
                })
            }
        }
    }
}

/// A key for an error message: as text if it is UTF-8, else as hex.
fn display_key(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(s) => format!("{s:?}"),
        Err(_) => {
            let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
            format!("0x{hex}")
        }
    }
}

/// `v`'s contents, leaving it empty with room for as many again.
fn take<T>(v: &mut Vec<T>) -> Vec<T> {
    let room = Vec::with_capacity(v.len());
    std::mem::replace(v, room)
}

/// The batch being built.
#[derive(Debug)]
pub(crate) struct Builder {
    options: ArrowOptions,
    schema: SchemaRef,
    cells: usize,
    bytes: usize,
    /// `row`: one key per cell, or with `row_dictionary`, one per row and an index per cell.
    rows: Bytes,
    row_keys: Vec<i32>,
    /// `family`: indexes into `families`, which grows over the whole scan (so each batch's
    /// dictionary extends the previous one's), and the last family found.
    family_keys: Vec<i32>,
    families: Vec<String>,
    last_family: usize,
    qualifiers: Bytes,
    ts: Vec<u64>,
    values: Bytes,
    type_keys: Vec<i8>,
    type_names: ArrayRef,
    ints: Vec<i64>,
    int_nulls: NullBufferBuilder,
    floats: Vec<f64>,
    float_nulls: NullBufferBuilder,
}

impl Builder {
    pub(crate) fn new(options: ArrowOptions) -> Self {
        Self {
            schema: schema(&options),
            options,
            cells: 0,
            bytes: 0,
            rows: Bytes::new(),
            row_keys: Vec::new(),
            family_keys: Vec::new(),
            families: Vec::new(),
            last_family: 0,
            qualifiers: Bytes::new(),
            ts: Vec::new(),
            values: Bytes::new(),
            type_keys: Vec::new(),
            type_names: Arc::new(StringArray::from_iter_values(VALUE_TYPES)),
            ints: Vec::new(),
            int_nulls: NullBufferBuilder::new(0),
            floats: Vec::new(),
            float_nulls: NullBufferBuilder::new(0),
        }
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.cells == 0
    }

    /// Whether the batch has reached a limit and should close.
    pub(crate) fn full(&self) -> bool {
        self.cells >= self.options.batch_cells || self.bytes >= self.options.batch_bytes
    }

    /// Adds `row`'s cells from index `from` (the caller only calls this on a batch that is
    /// not full). `Some(i)`: the batch filled up before cell `i`, which goes in the next one.
    pub(crate) fn push_row(
        &mut self,
        row: &RowRef<'_>,
        from: usize,
    ) -> Result<Option<usize>, ArrowError> {
        let key = row.key();
        let mut first = true;
        for i in from..row.len() {
            if self.full() {
                return Ok(Some(i));
            }
            let Some(entry) = row.entry(i) else {
                break;
            };
            let cell_bytes = key.len() + entry.qualifier.len() + entry.cell.value().len();
            if !self.is_empty() && self.bytes + cell_bytes > MAX_BYTES {
                return Ok(Some(i));
            }
            self.push_cell(key, first, &entry)?;
            first = false;
            self.cells += 1;
            self.bytes += cell_bytes;
        }
        Ok(None)
    }

    fn push_cell(&mut self, key: &[u8], first: bool, e: &CellEntry<'_>) -> Result<(), ArrowError> {
        let ts = e.cell.timestamp();
        if self.options.ts == TsType::Timestamp && i64::try_from(ts).is_err() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "the timestamp {ts} of row {} is above i64::MAX, which an Arrow Timestamp \
                 cannot hold (TsType::UInt64 exports it as stored)",
                display_key(key)
            )));
        }
        if self.options.row_dictionary {
            if first {
                self.rows.push(key)?;
            }
            // Under 2^31 rows: each holds at least one of a batch's at most 2^30 bytes or
            // usize cells, and the dictionary is per batch.
            self.row_keys
                .push(i32::try_from(self.rows.len() - 1).unwrap_or(i32::MAX));
        } else {
            self.rows.push(key)?;
        }
        self.push_family(e.family);
        self.qualifiers.push(e.qualifier)?;
        self.ts.push(ts);
        let value = e.cell.value();
        self.values.push(value)?;
        let (kind, int, float) = match e.cell.typed() {
            Value::Bytes(_) => (0, None, None),
            Value::I64(v) => (1, Some(v), None),
            Value::F64(v) => (2, None, Some(v)),
            Value::Varint(v) => (3, Some(v), None),
            // A value type added after this build (`Value` is non-exhaustive): refused, never
            // mislabelled as bytes (perf287's S1 on #941). Arrow is built against its own
            // `pigeonhole` (exact versions), so only a variant added without updating this
            // exporter reaches here.
            other => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "unsupported feature: value type {other:?} has no Arrow export in this \
                     build of pigeonhole-arrow"
                )));
            }
        };
        self.type_keys.push(kind);
        if self.options.typed_values {
            self.ints.push(int.unwrap_or(0));
            self.int_nulls.append(int.is_some());
            self.floats.push(float.unwrap_or(0.0));
            self.float_nulls.append(float.is_some());
        }
        Ok(())
    }

    /// Cells come grouped by family, so this is one comparison per cell and a search per
    /// family run.
    fn push_family(&mut self, family: &str) {
        if self.families.get(self.last_family).map(String::as_str) != Some(family) {
            self.last_family = match self.families.iter().position(|f| f == family) {
                Some(i) => i,
                None => {
                    self.families.push(family.to_owned());
                    self.families.len() - 1
                }
            };
        }
        // Families per table fit in an i32.
        self.family_keys
            .push(i32::try_from(self.last_family).unwrap_or(i32::MAX));
    }

    /// The batch, leaving the builder empty (also after an error).
    pub(crate) fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        self.cells = 0;
        self.bytes = 0;
        let utf8 = self.options.utf8_keys;
        let rows = self.rows.finish(utf8, "row key");
        let row_keys = take(&mut self.row_keys);
        let family_keys = take(&mut self.family_keys);
        let qualifiers = self.qualifiers.finish(utf8, "qualifier");
        let ts = Buffer::from_vec(take(&mut self.ts));
        let values = self.values.finish(false, "value");
        let type_keys = take(&mut self.type_keys);
        let ints = take(&mut self.ints);
        let floats = take(&mut self.floats);
        // Sized like the rest, so a batch with nulls does not grow its bitmap as it fills.
        let int_nulls =
            std::mem::replace(&mut self.int_nulls, NullBufferBuilder::new(ints.len())).finish();
        let float_nulls =
            std::mem::replace(&mut self.float_nulls, NullBufferBuilder::new(floats.len())).finish();

        let rows = rows?;
        let row: ArrayRef = if self.options.row_dictionary {
            Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(row_keys),
                rows,
            )?)
        } else {
            rows
        };
        let family = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(family_keys),
            Arc::new(StringArray::from_iter_values(&self.families)),
        )?;
        let len = ts.len() / 8;
        let ts: ArrayRef = match self.options.ts {
            // Every timestamp was checked to fit an i64 as it was pushed.
            TsType::Timestamp => Arc::new(
                TimestampMicrosecondArray::new(ScalarBuffer::new(ts, 0, len), None)
                    .with_timezone("UTC"),
            ),
            TsType::UInt64 => Arc::new(UInt64Array::new(ScalarBuffer::new(ts, 0, len), None)),
        };
        let value_type =
            DictionaryArray::try_new(Int8Array::from(type_keys), Arc::clone(&self.type_names))?;
        let mut columns: Vec<ArrayRef> = vec![
            row,
            Arc::new(family),
            qualifiers?,
            ts,
            values?,
            Arc::new(value_type),
        ];
        if self.options.typed_values {
            columns.push(Arc::new(Int64Array::new(
                ScalarBuffer::from(ints),
                int_nulls,
            )));
            columns.push(Arc::new(Float64Array::new(
                ScalarBuffer::from(floats),
                float_nulls,
            )));
        }
        RecordBatch::try_new(Arc::clone(&self.schema), columns)
    }
}
