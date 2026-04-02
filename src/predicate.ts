/**
 * Lazy-loading wrapper for the nteract-predicate WASM module.
 *
 * The WASM binary is loaded on first use, so it doesn't affect
 * initial page load for users who don't need compute operations.
 */

type PredicateModule = {
  value_counts(ipc_bytes: Uint8Array, column_index: number): { label: string; count: number }[]
  histogram(ipc_bytes: Uint8Array, column_index: number, num_bins: number): { x0: number; x1: number; count: number }[]
  filter_rows(ipc_bytes: Uint8Array, mask: Uint8Array): Uint8Array
  string_contains(ipc_bytes: Uint8Array, column_index: number, query: string): Uint32Array
}

let mod: PredicateModule | null = null

async function ensureModule(): Promise<PredicateModule> {
  if (mod) return mod
  // Dynamic import with string indirection so TypeScript doesn't
  // require the WASM pkg to exist at type-check time.
  // The pkg is built separately: cd crates/compute && wasm-pack build --target web
  const path = '../crates/compute/pkg/nteract_predicate.js'
  const wasm = await import(/* @vite-ignore */ path)
  await wasm.default()
  mod = wasm as unknown as PredicateModule
  return mod
}

/**
 * Search a string column for values containing a substring.
 * Returns indices of matching rows.
 */
export async function stringContains(
  ipcBytes: Uint8Array,
  columnIndex: number,
  query: string,
): Promise<Uint32Array> {
  const m = await ensureModule()
  return m.string_contains(ipcBytes, columnIndex, query)
}

/**
 * Compute value_counts for a string column.
 * Returns sorted array of { label, count }.
 */
export async function valueCounts(
  ipcBytes: Uint8Array,
  columnIndex: number,
): Promise<{ label: string; count: number }[]> {
  const m = await ensureModule()
  return m.value_counts(ipcBytes, columnIndex)
}

/**
 * Compute histogram bins for a numeric column.
 */
export async function histogram(
  ipcBytes: Uint8Array,
  columnIndex: number,
  numBins: number,
): Promise<{ x0: number; x1: number; count: number }[]> {
  const m = await ensureModule()
  return m.histogram(ipcBytes, columnIndex, numBins)
}

/**
 * Filter rows by a boolean mask, return filtered Arrow IPC bytes.
 */
export async function filterRows(
  ipcBytes: Uint8Array,
  mask: Uint8Array,
): Promise<Uint8Array> {
  const m = await ensureModule()
  return m.filter_rows(ipcBytes, mask)
}

/**
 * Check if the WASM module is available (built and loadable).
 */
export async function isAvailable(): Promise<boolean> {
  try {
    await ensureModule()
    return true
  } catch {
    return false
  }
}
