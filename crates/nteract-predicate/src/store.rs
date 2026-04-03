use arrow::array::{Array, AsArray, Float64Array, Int32Array, Int64Array, UInt32Array, StringArray, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, TimeUnit};
use arrow_select::concat::concat;
use arrow_ord::sort::{sort_to_indices, SortOptions};
use crate::summary::{CategoryCount, HistogramBin};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Mutex;
use wasm_bindgen::prelude::*;

/// A loaded dataset stored in WASM memory.
struct DataStore {
    batches: Vec<RecordBatch>,
    /// Prefix sum of batch row counts for O(log n) row→batch lookup
    batch_offsets: Vec<usize>,
    total_rows: usize,
    num_cols: usize,
    col_names: Vec<String>,
    col_types: Vec<String>, // "numeric", "categorical", "boolean", "timestamp"
    /// Original column arrays saved before casting, keyed by column index.
    /// Used to restore original data when casting back to the original type.
    original_columns: HashMap<usize, (Vec<arrow::array::ArrayRef>, String)>,
}

impl DataStore {
    fn resolve_row(&self, row: usize) -> Option<(usize, usize)> {
        if row >= self.total_rows { return None; }
        // Binary search for the batch containing this row
        let batch_idx = match self.batch_offsets.binary_search(&row) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        let local_row = row - self.batch_offsets[batch_idx];
        Some((batch_idx, local_row))
    }

    fn detect_col_type(dt: &DataType) -> &'static str {
        match dt {
            DataType::Boolean => "boolean",
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float16 | DataType::Float32 | DataType::Float64
            | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "numeric",
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => "timestamp",
            _ => "categorical",
        }
    }
}

// Global store: handle → DataStore
static STORES: Mutex<Option<HashMap<u32, DataStore>>> = Mutex::new(None);
static NEXT_HANDLE: Mutex<u32> = Mutex::new(1);

fn with_stores<F, R>(f: F) -> R
where F: FnOnce(&mut HashMap<u32, DataStore>) -> R {
    let mut guard = STORES.lock().unwrap();
    let stores = guard.get_or_insert_with(HashMap::new);
    f(stores)
}

fn with_store<F, R>(handle: u32, f: F) -> Result<R, String>
where F: FnOnce(&DataStore) -> R {
    with_stores(|stores| {
        stores.get(&handle)
            .map(f)
            .ok_or_else(|| format!("Invalid handle: {}", handle))
    })
}

/// Store a vec of RecordBatches, returning a handle.
fn store_batches(batches: Vec<RecordBatch>, schema: &arrow::datatypes::Schema) -> u32 {
    let num_cols = schema.fields().len();
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let col_types: Vec<String> = schema.fields().iter()
        .map(|f| DataStore::detect_col_type(f.data_type()).to_string())
        .collect();

    let mut batch_offsets = Vec::new();
    let mut total_rows = 0;
    for batch in &batches {
        batch_offsets.push(total_rows);
        total_rows += batch.num_rows();
    }

    let handle = {
        let mut h = NEXT_HANDLE.lock().unwrap();
        let id = *h;
        *h += 1;
        id
    };

    with_stores(|stores| {
        stores.insert(handle, DataStore {
            batches,
            batch_offsets,
            total_rows,
            num_cols,
            col_names,
            col_types,
            original_columns: HashMap::new(),
        });
    });

    handle
}

/// Load Arrow IPC bytes into WASM memory. Returns a handle for subsequent operations.
#[wasm_bindgen]
pub fn load_ipc(ipc_bytes: &[u8]) -> Result<u32, JsValue> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let schema = reader.schema();

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| JsValue::from_str(&e.to_string()))?);
    }

    Ok(store_batches(batches, &schema))
}

/// Load Parquet bytes into WASM memory. Returns a handle for subsequent operations.
/// This replaces the need for parquet-wasm — one WASM binary for everything.
#[wasm_bindgen]
pub fn load_parquet(parquet_bytes: &[u8]) -> Result<u32, JsValue> {
    let bytes = bytes::Bytes::copy_from_slice(parquet_bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let schema = builder.schema().clone();
    let reader = builder.build()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| JsValue::from_str(&e.to_string()))?);
    }

    Ok(store_batches(batches, &schema))
}

/// Free a loaded dataset from WASM memory.
#[wasm_bindgen]
pub fn free(handle: u32) {
    with_stores(|stores| { stores.remove(&handle); });
}

/// Get the number of rows in a loaded dataset.
#[wasm_bindgen]
pub fn num_rows(handle: u32) -> Result<u32, JsValue> {
    with_store(handle, |s| s.total_rows as u32)
        .map_err(|e| JsValue::from_str(&e))
}

/// Get the number of columns in a loaded dataset.
#[wasm_bindgen]
pub fn num_cols(handle: u32) -> Result<u32, JsValue> {
    with_store(handle, |s| s.num_cols as u32)
        .map_err(|e| JsValue::from_str(&e))
}

/// Get column names as a JSON array.
#[wasm_bindgen]
pub fn col_names(handle: u32) -> Result<JsValue, JsValue> {
    with_store(handle, |s| {
        serde_wasm_bindgen::to_value(&s.col_names).unwrap()
    }).map_err(|e| JsValue::from_str(&e))
}

/// Get the detected type of a column ("numeric", "categorical", "boolean", "timestamp").
#[wasm_bindgen]
pub fn col_type(handle: u32, col: usize) -> Result<String, JsValue> {
    with_store(handle, |s| {
        s.col_types.get(col).cloned().unwrap_or_default()
    }).map_err(|e| JsValue::from_str(&e))
}

/// Check if a cell is null.
#[wasm_bindgen]
pub fn is_null(handle: u32, row: usize, col: usize) -> Result<bool, JsValue> {
    with_store(handle, |s| {
        let (batch_idx, local_row) = s.resolve_row(row).unwrap_or((0, 0));
        let column = s.batches[batch_idx].column(col);
        column.is_null(local_row)
    }).map_err(|e| JsValue::from_str(&e))
}

/// Get a cell value as a formatted string (for display).
#[wasm_bindgen]
pub fn get_cell_string(handle: u32, row: usize, col: usize) -> Result<String, JsValue> {
    with_store(handle, |s| {
        let (batch_idx, local_row) = match s.resolve_row(row) {
            Some(r) => r,
            None => return String::new(),
        };
        let column = s.batches[batch_idx].column(col);
        if column.is_null(local_row) {
            return String::new();
        }

        match column.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                column.as_any().downcast_ref::<StringArray>()
                    .map(|a| a.value(local_row).to_string())
                    .unwrap_or_default()
            }
            DataType::Boolean => {
                column.as_any().downcast_ref::<BooleanArray>()
                    .map(|a| if a.value(local_row) { "Yes".into() } else { "No".into() })
                    .unwrap_or_default()
            }
            DataType::Int32 => {
                column.as_any().downcast_ref::<Int32Array>()
                    .map(|a| a.value(local_row).to_string())
                    .unwrap_or_default()
            }
            DataType::Int64 => {
                column.as_any().downcast_ref::<Int64Array>()
                    .map(|a| a.value(local_row).to_string())
                    .unwrap_or_default()
            }
            DataType::Float64 => {
                column.as_any().downcast_ref::<Float64Array>()
                    .map(|a| format!("{}", a.value(local_row)))
                    .unwrap_or_default()
            }
            DataType::Dictionary(_, _) => {
                // Dictionary-encoded: look up the value
                let dict_arr = column.as_any_dictionary();
                let keys = dict_arr.keys();
                let values = dict_arr.values();
                if let Some(str_values) = values.as_any().downcast_ref::<StringArray>() {
                    if let Some(int_keys) = keys.as_any().downcast_ref::<Int32Array>() {
                        let key = int_keys.value(local_row) as usize;
                        return str_values.value(key).to_string();
                    }
                }
                String::new()
            }
            _ => format!("{:?}", column.as_ref())
        }
    }).map_err(|e| JsValue::from_str(&e))
}

/// Get a cell value as f64 (for numeric sorting/comparison). Returns NaN for non-numeric or null.
#[wasm_bindgen]
pub fn get_cell_f64(handle: u32, row: usize, col: usize) -> Result<f64, JsValue> {
    with_store(handle, |s| {
        let (batch_idx, local_row) = match s.resolve_row(row) {
            Some(r) => r,
            None => return f64::NAN,
        };
        let column = s.batches[batch_idx].column(col);
        if column.is_null(local_row) {
            return f64::NAN;
        }

        match column.data_type() {
            DataType::Float64 => {
                column.as_any().downcast_ref::<Float64Array>()
                    .map(|a| a.value(local_row))
                    .unwrap_or(f64::NAN)
            }
            DataType::Int32 => {
                column.as_any().downcast_ref::<Int32Array>()
                    .map(|a| a.value(local_row) as f64)
                    .unwrap_or(f64::NAN)
            }
            DataType::Int64 => {
                column.as_any().downcast_ref::<Int64Array>()
                    .map(|a| a.value(local_row) as f64)
                    .unwrap_or(f64::NAN)
            }
            _ => f64::NAN,
        }
    }).map_err(|e| JsValue::from_str(&e))
}

/// Compute value_counts for a column in a loaded store. Much faster than
/// the JS accumulator path since it iterates batches in Rust.
#[wasm_bindgen]
pub fn store_value_counts(handle: u32, col: usize) -> Result<JsValue, JsValue> {
    with_store(handle, |s| {
        let mut freq: HashMap<String, u32> = HashMap::new();
        for batch in &s.batches {
            let column = batch.column(col);
            match column.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            *freq.entry(arr.value(i).to_string()).or_insert(0) += 1;
                        }
                    }
                }
                DataType::Boolean => {
                    let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            let key = if arr.value(i) { "Yes" } else { "No" };
                            *freq.entry(key.to_string()).or_insert(0) += 1;
                        }
                    }
                }
                DataType::Dictionary(_, _) => {
                    let dict_arr = column.as_any_dictionary();
                    let keys = dict_arr.keys();
                    let values = dict_arr.values();
                    if let Some(str_values) = values.as_any().downcast_ref::<StringArray>() {
                        if let Some(int_keys) = keys.as_any().downcast_ref::<Int32Array>() {
                            for i in 0..int_keys.len() {
                                if !int_keys.is_null(i) {
                                    let key = int_keys.value(i) as usize;
                                    *freq.entry(str_values.value(key).to_string()).or_insert(0) += 1;
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Stringify other types
                    for i in 0..column.len() {
                        if !column.is_null(i) {
                            *freq.entry(format!("{}", i)).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
        let mut counts: Vec<CategoryCount> = freq
            .into_iter()
            .map(|(label, count)| CategoryCount { label, count })
            .collect();
        counts.sort_by(|a, b| b.count.cmp(&a.count));
        serde_wasm_bindgen::to_value(&counts).unwrap()
    }).map_err(|e| JsValue::from_str(&e))
}

/// Compute histogram for a numeric column in a loaded store.
#[wasm_bindgen]
pub fn store_histogram(handle: u32, col: usize, num_bins: usize) -> Result<JsValue, JsValue> {
    with_store(handle, |s| {
        let mut values: Vec<f64> = Vec::new();
        for batch in &s.batches {
            let column = batch.column(col);
            match column.data_type() {
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            let v = arr.value(i);
                            if v.is_finite() { values.push(v); }
                        }
                    }
                }
                DataType::Int32 => {
                    let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) { values.push(arr.value(i) as f64); }
                    }
                }
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) { values.push(arr.value(i) as f64); }
                    }
                }
                _ => {}
            }
        }
        if values.is_empty() {
            return serde_wasm_bindgen::to_value(&Vec::<HistogramBin>::new()).unwrap();
        }
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let bin_width = if (max - min).abs() < f64::EPSILON { 1.0 } else { (max - min) / num_bins as f64 };
        let mut bins: Vec<HistogramBin> = (0..num_bins)
            .map(|i| HistogramBin {
                x0: min + i as f64 * bin_width,
                x1: min + (i + 1) as f64 * bin_width,
                count: 0,
            })
            .collect();
        for v in &values {
            let mut idx = ((v - min) / bin_width) as usize;
            if idx >= num_bins { idx = num_bins - 1; }
            bins[idx].count += 1;
        }
        serde_wasm_bindgen::to_value(&bins).unwrap()
    }).map_err(|e| JsValue::from_str(&e))
}

/// Count boolean values in a column: returns [true_count, false_count, null_count].
#[wasm_bindgen]
pub fn store_bool_counts(handle: u32, col: usize) -> Result<Vec<u32>, JsValue> {
    with_store(handle, |s| {
        let mut true_count: u32 = 0;
        let mut false_count: u32 = 0;
        let mut null_count: u32 = 0;
        for batch in &s.batches {
            let column = batch.column(col);
            if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                for i in 0..arr.len() {
                    if arr.is_null(i) { null_count += 1; }
                    else if arr.value(i) { true_count += 1; }
                    else { false_count += 1; }
                }
            }
        }
        vec![true_count, false_count, null_count]
    }).map_err(|e| JsValue::from_str(&e))
}

/// Sort a column and return sorted row indices.
/// `ascending`: true for asc, false for desc.
/// Nulls are always sorted to the end.
#[wasm_bindgen]
pub fn store_sort_indices(handle: u32, col: usize, ascending: bool) -> Result<Vec<u32>, JsValue> {
    with_store(handle, |s| {
        // Concatenate column across all batches into a single array
        let arrays: Vec<&dyn Array> = s.batches.iter()
            .map(|b| b.column(col).as_ref())
            .collect();

        if arrays.is_empty() {
            return Ok(Vec::new());
        }

        let combined = concat(&arrays)
            .map_err(|e| format!("concat error: {}", e))?;

        let options = SortOptions {
            descending: !ascending,
            nulls_first: false, // nulls always at end
        };

        let indices = sort_to_indices(combined.as_ref(), Some(options), None)
            .map_err(|e| format!("sort error: {}", e))?;

        // Convert UInt32Array to Vec<u32>
        Ok(indices.values().iter().copied().collect())
    })
    .map_err(|e| JsValue::from_str(&e))?
    .map_err(|e: String| JsValue::from_str(&e))
}

/// Get a viewport slice as Arrow IPC bytes.
/// Returns the rows [start_row, end_row) serialized as Arrow IPC stream.
/// This is the hot-path function — one call per scroll frame.
#[wasm_bindgen]
pub fn get_viewport(handle: u32, start_row: u32, end_row: u32) -> Result<Vec<u8>, JsValue> {
    with_store(handle, |s| {
        let start = start_row as usize;
        let end = (end_row as usize).min(s.total_rows);
        if start >= end {
            return Err("empty viewport".to_string());
        }

        let schema = s.batches[0].schema();
        let mut slices: Vec<RecordBatch> = Vec::new();

        // Walk batches, slicing the ones that overlap [start, end)
        for (batch_idx, batch) in s.batches.iter().enumerate() {
            let batch_start = s.batch_offsets[batch_idx];
            let batch_end = batch_start + batch.num_rows();

            // Skip batches entirely before or after the viewport
            if batch_end <= start || batch_start >= end {
                continue;
            }

            // Compute the overlap
            let local_start = if start > batch_start { start - batch_start } else { 0 };
            let local_end = if end < batch_end { end - batch_start } else { batch.num_rows() };

            slices.push(batch.slice(local_start, local_end - local_start));
        }

        if slices.is_empty() {
            return Err("no data in viewport".to_string());
        }

        // Serialize to Arrow IPC stream
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| format!("IPC writer error: {}", e))?;
        for slice in &slices {
            writer.write(slice).map_err(|e| format!("IPC write error: {}", e))?;
        }
        writer.finish().map_err(|e| format!("IPC finish error: {}", e))?;
        drop(writer);

        Ok(buf)
    })
    .map_err(|e| JsValue::from_str(&e))?
    .map_err(|e: String| JsValue::from_str(&e))
}

/// Get a viewport slice for specific rows by index (for sorted/filtered views).
/// `indices` is a Uint32Array of row indices to fetch.
/// Returns Arrow IPC bytes containing those specific rows in order.
#[wasm_bindgen]
pub fn get_viewport_by_indices(handle: u32, indices: &[u32]) -> Result<Vec<u8>, JsValue> {
    with_store(handle, |s| {
        if indices.is_empty() || s.batches.is_empty() {
            return Err("empty indices".to_string());
        }

        let schema = s.batches[0].schema();
        let num_cols = schema.fields().len();

        // For each column, gather values at the requested indices using arrow take
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            // Concatenate column across all batches
            let arrays: Vec<&dyn Array> = s.batches.iter()
                .map(|b| b.column(col_idx).as_ref())
                .collect();
            let combined = concat(&arrays)
                .map_err(|e| format!("concat error: {}", e))?;

            // Build indices array
            let idx_array = UInt32Array::from(indices.to_vec());
            let taken = arrow_select::take::take(combined.as_ref(), &idx_array, None)
                .map_err(|e| format!("take error: {}", e))?;
            columns.push(taken);
        }

        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| format!("batch error: {}", e))?;

        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())
            .map_err(|e| format!("IPC writer error: {}", e))?;
        writer.write(&batch).map_err(|e| format!("IPC write error: {}", e))?;
        writer.finish().map_err(|e| format!("IPC finish error: {}", e))?;
        drop(writer);

        Ok(buf)
    })
    .map_err(|e| JsValue::from_str(&e))?
    .map_err(|e: String| JsValue::from_str(&e))
}

/// Get Parquet metadata: number of row groups and total rows.
/// Returns [num_row_groups, total_rows] as Vec<u32>.
#[wasm_bindgen]
pub fn parquet_metadata(parquet_bytes: &[u8]) -> Result<Vec<u32>, JsValue> {
    let bytes = bytes::Bytes::copy_from_slice(parquet_bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups() as u32;
    let total_rows = metadata.file_metadata().num_rows() as u32;
    Ok(vec![num_row_groups, total_rows])
}

/// Extract key-value metadata from a Parquet file's schema.
/// Returns a JSON object with metadata keys like "pandas", "huggingface", etc.
#[wasm_bindgen]
pub fn parquet_schema_metadata(parquet_bytes: &[u8]) -> Result<JsValue, JsValue> {
    let bytes = bytes::Bytes::copy_from_slice(parquet_bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let schema = builder.schema();
    let metadata = schema.metadata();
    let map: HashMap<String, String> = metadata.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    serde_wasm_bindgen::to_value(&map)
        .map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Load a single Parquet row group into a new or existing store.
/// If handle is 0, creates a new store and returns the handle.
/// If handle is non-zero, appends the row group to the existing store.
#[wasm_bindgen]
pub fn load_parquet_row_group(parquet_bytes: &[u8], row_group: usize, handle: u32) -> Result<u32, JsValue> {
    let bytes = bytes::Bytes::copy_from_slice(parquet_bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let schema = builder.schema().clone();

    let reader = builder
        .with_row_groups(vec![row_group])
        .build()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| JsValue::from_str(&e.to_string()))?);
    }

    if handle == 0 {
        // Create new store
        Ok(store_batches(batches, &schema))
    } else {
        // Append to existing store
        with_stores(|stores| {
            if let Some(store) = stores.get_mut(&handle) {
                for batch in batches {
                    store.batch_offsets.push(store.total_rows);
                    store.total_rows += batch.num_rows();
                    store.batches.push(batch);
                }
                Ok(handle)
            } else {
                Err(JsValue::from_str(&format!("Invalid handle: {}", handle)))
            }
        })
    }
}

/// Cast a column to a different type in-place.
/// Supported casts: string→timestamp (parse ISO dates), string→numeric, etc.
/// Uses arrow-cast for type conversion. Updates the store's column type metadata.
/// Saves the original column data so it can be restored when casting back.
#[wasm_bindgen]
pub fn cast_column(handle: u32, col: usize, target_type: &str) -> Result<(), JsValue> {
    with_stores(|stores| {
        let store = stores.get_mut(&handle)
            .ok_or_else(|| JsValue::from_str(&format!("Invalid handle: {}", handle)))?;

        let target_dt = match target_type {
            "timestamp" => DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            "numeric" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "categorical" => DataType::Utf8,
            _ => return Err(JsValue::from_str(&format!("Unknown target type: {}", target_type))),
        };

        // Check if we have saved originals for this column and the target matches
        if let Some((original_cols, original_type)) = store.original_columns.get(&col) {
            if target_type == original_type {
                // Restore original column data instead of arrow-casting
                let mut new_batches = Vec::new();
                for (batch_idx, batch) in store.batches.iter().enumerate() {
                    let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
                    for i in 0..batch.num_columns() {
                        if i == col {
                            columns.push(original_cols[batch_idx].clone());
                        } else {
                            columns.push(batch.column(i).clone());
                        }
                    }
                    let mut fields: Vec<arrow::datatypes::FieldRef> = batch.schema().fields().iter().cloned().collect();
                    fields[col] = std::sync::Arc::new(
                        arrow::datatypes::Field::new(fields[col].name(), original_cols[batch_idx].data_type().clone(), true)
                    );
                    let new_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
                    new_batches.push(RecordBatch::try_new(new_schema, columns)
                        .map_err(|e| JsValue::from_str(&format!("Batch rebuild error: {}", e)))?);
                }
                store.batches = new_batches;
                store.col_types[col] = original_type.clone();
                store.original_columns.remove(&col);
                return Ok(());
            }
        }

        // Save original column data before casting (only if not already saved)
        if !store.original_columns.contains_key(&col) {
            let originals: Vec<arrow::array::ArrayRef> = store.batches.iter()
                .map(|b| b.column(col).clone())
                .collect();
            let original_type = store.col_types[col].clone();
            store.original_columns.insert(col, (originals, original_type));
        }

        // Cast the column in each batch
        let mut new_batches = Vec::new();
        for batch in &store.batches {
            let column = batch.column(col);
            let source_dt = column.data_type();

            let casted = if source_dt == &target_dt {
                column.clone()
            } else if target_type == "timestamp" && matches!(source_dt, DataType::Utf8 | DataType::LargeUtf8) {
                // String → Timestamp: parse ISO date strings manually
                let str_arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                let mut builder = arrow::array::TimestampMillisecondArray::builder(str_arr.len());
                for i in 0..str_arr.len() {
                    if str_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let s = str_arr.value(i);
                        if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            let ts = dt.and_hms_opt(0, 0, 0).unwrap()
                                .and_utc().timestamp_millis();
                            builder.append_value(ts);
                        } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                            builder.append_value(dt.and_utc().timestamp_millis());
                        } else {
                            builder.append_null();
                        }
                    }
                }
                std::sync::Arc::new(builder.finish()) as arrow::array::ArrayRef
            } else {
                // Use arrow-cast for other conversions
                arrow_cast::cast::cast(column.as_ref(), &target_dt)
                    .map_err(|e| JsValue::from_str(&format!("Cast error: {}", e)))?
            };

            // Rebuild the batch with the casted column
            let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
            for i in 0..batch.num_columns() {
                if i == col {
                    columns.push(casted.clone());
                } else {
                    columns.push(batch.column(i).clone());
                }
            }

            // Update schema for this column
            let mut fields: Vec<arrow::datatypes::FieldRef> = batch.schema().fields().iter().cloned().collect();
            fields[col] = std::sync::Arc::new(
                arrow::datatypes::Field::new(fields[col].name(), target_dt.clone(), true)
            );
            let new_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
            new_batches.push(RecordBatch::try_new(new_schema, columns)
                .map_err(|e| JsValue::from_str(&format!("Batch rebuild error: {}", e)))?);
        }

        store.batches = new_batches;
        store.col_types[col] = match target_type {
            "timestamp" => "timestamp".to_string(),
            "numeric" => "numeric".to_string(),
            "boolean" => "boolean".to_string(),
            _ => "categorical".to_string(),
        };

        Ok(())
    })
}
