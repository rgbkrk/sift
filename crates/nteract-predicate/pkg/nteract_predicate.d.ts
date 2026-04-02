/* tslint:disable */
/* eslint-disable */

/**
 * Get column names as a JSON array.
 */
export function col_names(handle: number): any;

/**
 * Get the detected type of a column ("numeric", "categorical", "boolean", "timestamp").
 */
export function col_type(handle: number, col: number): string;

/**
 * Filter rows by a boolean mask and return filtered Arrow IPC bytes.
 *
 * Takes: Arrow IPC bytes, boolean mask as Uint8Array (0/1 per row)
 * Returns: Filtered Arrow IPC bytes
 */
export function filter_rows(ipc_bytes: Uint8Array, mask: Uint8Array): Uint8Array;

/**
 * Free a loaded dataset from WASM memory.
 */
export function free(handle: number): void;

/**
 * Get a cell value as f64 (for numeric sorting/comparison). Returns NaN for non-numeric or null.
 */
export function get_cell_f64(handle: number, row: number, col: number): number;

/**
 * Get a cell value as a formatted string (for display).
 */
export function get_cell_string(handle: number, row: number, col: number): string;

/**
 * Compute a histogram (binned counts) for a numeric column.
 *
 * Takes: Arrow IPC bytes, column index, number of bins
 * Returns: JSON array of { x0, x1, count }
 */
export function histogram(ipc_bytes: Uint8Array, column_index: number, num_bins: number): any;

/**
 * Initialize the WASM module. Call once before using other functions.
 */
export function init(): void;

/**
 * Check if a cell is null.
 */
export function is_null(handle: number, row: number, col: number): boolean;

/**
 * Load Arrow IPC bytes into WASM memory. Returns a handle for subsequent operations.
 */
export function load_ipc(ipc_bytes: Uint8Array): number;

/**
 * Load Parquet bytes into WASM memory. Returns a handle for subsequent operations.
 * This replaces the need for parquet-wasm — one WASM binary for everything.
 */
export function load_parquet(parquet_bytes: Uint8Array): number;

/**
 * Get the number of columns in a loaded dataset.
 */
export function num_cols(handle: number): number;

/**
 * Get the number of rows in a loaded dataset.
 */
export function num_rows(handle: number): number;

/**
 * Count boolean values in a column: returns [true_count, false_count, null_count].
 */
export function store_bool_counts(handle: number, col: number): Uint32Array;

/**
 * Compute histogram for a numeric column in a loaded store.
 */
export function store_histogram(handle: number, col: number, num_bins: number): any;

/**
 * Compute value_counts for a column in a loaded store. Much faster than
 * the JS accumulator path since it iterates batches in Rust.
 */
export function store_value_counts(handle: number, col: number): any;

/**
 * Search a string column for values containing a substring.
 * Returns indices of matching rows as a Uint32Array.
 *
 * Takes: Arrow IPC bytes, column index, search query
 * Returns: Array of matching row indices
 */
export function string_contains(ipc_bytes: Uint8Array, column_index: number, query: string): Uint32Array;

/**
 * Compute a frequency table (value_counts) for a string column
 * passed as Arrow IPC bytes.
 *
 * Takes: Arrow IPC bytes containing a single string/dictionary column
 * Returns: JSON array of { label, count } sorted by count descending
 */
export function value_counts(ipc_bytes: Uint8Array, column_index: number): any;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly col_names: (a: number, b: number) => void;
    readonly col_type: (a: number, b: number, c: number) => void;
    readonly free: (a: number) => void;
    readonly get_cell_f64: (a: number, b: number, c: number, d: number) => void;
    readonly get_cell_string: (a: number, b: number, c: number, d: number) => void;
    readonly is_null: (a: number, b: number, c: number, d: number) => void;
    readonly load_ipc: (a: number, b: number, c: number) => void;
    readonly load_parquet: (a: number, b: number, c: number) => void;
    readonly num_cols: (a: number, b: number) => void;
    readonly num_rows: (a: number, b: number) => void;
    readonly store_bool_counts: (a: number, b: number, c: number) => void;
    readonly store_histogram: (a: number, b: number, c: number, d: number) => void;
    readonly store_value_counts: (a: number, b: number, c: number) => void;
    readonly filter_rows: (a: number, b: number, c: number, d: number, e: number) => void;
    readonly histogram: (a: number, b: number, c: number, d: number, e: number) => void;
    readonly init: () => void;
    readonly string_contains: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly value_counts: (a: number, b: number, c: number, d: number) => void;
    readonly rust_zstd_wasm_shim_calloc: (a: number, b: number) => number;
    readonly rust_zstd_wasm_shim_free: (a: number) => void;
    readonly rust_zstd_wasm_shim_malloc: (a: number) => number;
    readonly rust_zstd_wasm_shim_memcmp: (a: number, b: number, c: number) => number;
    readonly rust_zstd_wasm_shim_memcpy: (a: number, b: number, c: number) => number;
    readonly rust_zstd_wasm_shim_memmove: (a: number, b: number, c: number) => number;
    readonly rust_zstd_wasm_shim_memset: (a: number, b: number, c: number) => number;
    readonly rust_zstd_wasm_shim_qsort: (a: number, b: number, c: number, d: number) => void;
    readonly __wbindgen_export: (a: number, b: number) => number;
    readonly __wbindgen_export2: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
    readonly __wbindgen_export3: (a: number, b: number, c: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
