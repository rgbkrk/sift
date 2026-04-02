/**
 * Creates a TableData backed by the nteract-predicate WASM data store.
 *
 * Data lives in WASM memory. Cell access uses viewport caching:
 * prefetchViewport() loads visible rows in one WASM call, then
 * getCell/getCellRaw read from the JS-side cache — no per-cell FFI.
 */
import { tableFromIPC } from 'apache-arrow'
import { getModuleSync } from './predicate'
import type { TableData, Column, ColumnType } from './table'
import { formatCell } from './accumulators'

/** Map WASM col_type strings to our ColumnType */
function mapColType(wasmType: string): ColumnType {
  switch (wasmType) {
    case 'numeric': return 'numeric'
    case 'boolean': return 'boolean'
    case 'timestamp': return 'timestamp'
    default: return 'categorical'
  }
}

/** Guess a reasonable default width for a column */
function autoWidth(name: string, colType: ColumnType): number {
  if (colType === 'boolean') return 100
  if (colType === 'timestamp') return 140
  if (colType === 'numeric') return 120
  return Math.max(100, Math.min(250, name.length * 12 + 40))
}

export type WasmTableHandle = {
  handle: number
  tableData: TableData
  columns: Column[]
  /** Prefetch visible rows into a JS-side cache. Call before render. */
  prefetchViewport: (dataRowIndices: number[]) => void
}

/**
 * Build a TableData from a WASM store handle.
 * The module must already be initialized (call ensureModule() first).
 */
export function createWasmTableData(handle: number): WasmTableHandle {
  const mod = getModuleSync()

  const numRows = mod.num_rows(handle)
  const numCols = mod.num_cols(handle)
  const names: string[] = mod.col_names(handle)

  const columns: Column[] = []
  for (let c = 0; c < numCols; c++) {
    const wasmType = mod.col_type(handle, c)
    const colType = mapColType(wasmType)
    columns.push({
      key: names[c],
      label: names[c],
      width: autoWidth(names[c], colType),
      sortable: true,
      numeric: colType === 'numeric',
      columnType: colType,
    })
  }

  // Viewport cache: maps data row index → { strings[], raws[] }
  const cache = new Map<number, { strings: string[]; raws: unknown[] }>()

  function prefetchViewport(dataRowIndices: number[]) {
    if (dataRowIndices.length === 0) return

    // Check if all requested rows are already cached
    const uncached = dataRowIndices.filter(r => !cache.has(r))
    if (uncached.length === 0) return

    // Fetch uncached rows in one WASM call
    const indices = new Uint32Array(uncached)
    const ipcBytes = mod.get_viewport_by_indices(handle, indices)
    const table = tableFromIPC(ipcBytes)

    // Populate cache from the Arrow table
    for (let i = 0; i < uncached.length; i++) {
      const dataRow = uncached[i]
      const strings: string[] = []
      const raws: unknown[] = []

      for (let c = 0; c < numCols; c++) {
        const col = table.getChildAt(c)!
        const val = col.get(i)
        const colType = columns[c].columnType

        if (val == null) {
          strings.push('')
          raws.push(null)
        } else if (colType === 'boolean') {
          const boolVal = typeof val === 'boolean' ? val : Boolean(val)
          strings.push(boolVal ? 'Yes' : 'No')
          raws.push(boolVal)
        } else if (colType === 'timestamp') {
          const numVal = typeof val === 'bigint' ? Number(val) : Number(val)
          strings.push(formatCell('timestamp', numVal))
          raws.push(numVal)
        } else if (colType === 'numeric') {
          const numVal = typeof val === 'bigint' ? Number(val) : Number(val)
          strings.push(String(val))
          raws.push(numVal)
        } else {
          strings.push(String(val))
          raws.push(val)
        }
      }

      cache.set(dataRow, { strings, raws })
    }
  }

  const tableData: TableData = {
    columns,
    rowCount: numRows,
    getCell(row: number, col: number): string {
      // Try cache first (populated by prefetchViewport)
      const cached = cache.get(row)
      if (cached) return cached.strings[col]

      // Fallback to per-cell WASM call
      if (mod.is_null(handle, row, col)) return ''
      const colType = columns[col].columnType
      if (colType === 'timestamp') {
        const v = mod.get_cell_f64(handle, row, col)
        if (Number.isFinite(v)) return formatCell('timestamp', v)
      }
      return mod.get_cell_string(handle, row, col)
    },
    getCellRaw(row: number, col: number): unknown {
      // Try cache first
      const cached = cache.get(row)
      if (cached) return cached.raws[col]

      // Fallback to per-cell WASM call
      if (mod.is_null(handle, row, col)) return null
      const colType = columns[col].columnType
      if (colType === 'numeric' || colType === 'timestamp') {
        return mod.get_cell_f64(handle, row, col)
      }
      if (colType === 'boolean') {
        return mod.get_cell_string(handle, row, col) === 'Yes'
      }
      return mod.get_cell_string(handle, row, col)
    },
    columnSummaries: columns.map(() => null),
  }

  return { handle, tableData, columns, prefetchViewport }
}
