use arrow::array::{Array, AsArray, Float64Array, Int32Array, Int64Array, StringArray, BooleanArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
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

/// Load Arrow IPC bytes into WASM memory. Returns a handle for subsequent operations.
#[wasm_bindgen]
pub fn load_ipc(ipc_bytes: &[u8]) -> Result<u32, JsValue> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let schema = reader.schema();
    let num_cols = schema.fields().len();
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let col_types: Vec<String> = schema.fields().iter()
        .map(|f| DataStore::detect_col_type(f.data_type()).to_string())
        .collect();

    let mut batches = Vec::new();
    let mut batch_offsets = Vec::new();
    let mut total_rows = 0;

    for batch in reader {
        let batch = batch.map_err(|e| JsValue::from_str(&e.to_string()))?;
        batch_offsets.push(total_rows);
        total_rows += batch.num_rows();
        batches.push(batch);
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
        });
    });

    Ok(handle)
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
