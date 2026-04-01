import { tableFromIPC, Table } from 'apache-arrow'
import {
  createTable,
  type Column,
  type TableData,
  type ColumnSummary,
  type NumericColumnSummary,
  type CategoricalColumnSummary,
} from './table'
import './style.css'

async function boot() {
  const app = document.getElementById('app')!

  // Show loading state
  app.innerHTML = '<div class="pt-loading">Loading data…</div>'

  // Fetch Arrow IPC file
  const response = await fetch('/data.arrow')
  if (!response.ok) {
    app.innerHTML =
      '<div class="pt-loading">Missing data.arrow — run <code>npm run generate</code> first.</div>'
    return
  }

  const buffer = await response.arrayBuffer()
  const table: Table = tableFromIPC(buffer)

  const schema = table.schema
  const fieldNames = schema.fields.map(f => f.name)

  // Define columns with nice labels and widths
  const columnDefs: Record<string, Partial<Column>> = {
    id:         { label: 'ID', width: 90, sortable: true, numeric: true },
    name:       { label: 'Name', width: 180, sortable: true, numeric: false },
    location:   { label: 'Location', width: 180, sortable: true, numeric: false },
    department: { label: 'Department', width: 160, sortable: true, numeric: false },
    note:       { label: 'Note', width: 340, sortable: false, numeric: false },
    status:     { label: 'Status', width: 140, sortable: true, numeric: false },
    priority:   { label: 'Priority', width: 120, sortable: true, numeric: false },
    score:      { label: 'Score', width: 120, sortable: true, numeric: true },
  }

  const columns: Column[] = fieldNames.map(name => ({
    key: name,
    label: columnDefs[name]?.label ?? name,
    width: columnDefs[name]?.width ?? 150,
    sortable: columnDefs[name]?.sortable ?? true,
    numeric: columnDefs[name]?.numeric ?? false,
  }))

  // Precompute string representations for all cells (Arrow → string once)
  const rowCount = table.numRows
  const stringCols: string[][] = []
  const rawCols: unknown[][] = []

  for (let c = 0; c < fieldNames.length; c++) {
    const col = table.getChild(fieldNames[c])!
    const strings: string[] = new Array(rowCount)
    const raws: unknown[] = new Array(rowCount)
    for (let r = 0; r < rowCount; r++) {
      const val = col.get(r)
      raws[r] = val
      strings[r] = val == null ? '' : String(val)
    }
    stringCols.push(strings)
    rawCols.push(raws)
  }

  // Compute column summaries for header sparklines
  const BIN_COUNT = 25
  const TOP_CATEGORIES = 3

  const columnSummaries: ColumnSummary[] = columns.map((col, c) => {
    if (col.numeric) {
      const vals = rawCols[c] as number[]
      let min = Infinity, max = -Infinity
      for (let r = 0; r < rowCount; r++) {
        const v = vals[r] as number
        if (v < min) min = v
        if (v > max) max = v
      }
      const binWidth = (max - min) / BIN_COUNT || 1
      const bins: NumericColumnSummary['bins'] = []
      for (let b = 0; b < BIN_COUNT; b++) {
        bins.push({ x0: min + b * binWidth, x1: min + (b + 1) * binWidth, count: 0 })
      }
      for (let r = 0; r < rowCount; r++) {
        const v = vals[r] as number
        let idx = Math.floor((v - min) / binWidth)
        if (idx >= BIN_COUNT) idx = BIN_COUNT - 1
        if (idx < 0) idx = 0
        bins[idx].count++
      }
      return { kind: 'numeric', min, max, bins } satisfies NumericColumnSummary
    } else {
      const freq = new Map<string, number>()
      for (let r = 0; r < rowCount; r++) {
        const s = stringCols[c][r]
        freq.set(s, (freq.get(s) ?? 0) + 1)
      }
      const sorted = [...freq.entries()].sort((a, b) => b[1] - a[1])
      const topCategories = sorted.slice(0, TOP_CATEGORIES).map(([label, count]) => ({
        label,
        count,
        pct: Math.round((count / rowCount) * 1000) / 10,
      }))
      const othersCount = sorted.slice(TOP_CATEGORIES).reduce((s, e) => s + e[1], 0)
      const othersPct = Math.round((othersCount / rowCount) * 1000) / 10
      return {
        kind: 'categorical',
        uniqueCount: freq.size,
        topCategories,
        othersCount,
        othersPct,
      } satisfies CategoricalColumnSummary
    }
  })

  const tableData: TableData = {
    columns,
    rowCount,
    getCell: (row, col) => stringCols[col][row],
    getCellRaw: (row, col) => rawCols[col][row],
    columnSummaries,
  }

  // Clear loading and mount table
  app.innerHTML = `
    <div class="pt-page">
      <div class="pt-intro">
        <p class="pt-eyebrow">Pretext × Arrow</p>
        <h1>Table Viewer</h1>
        <p class="pt-subtitle">
          ${rowCount.toLocaleString()} rows from Arrow IPC.
          Row heights computed by <a href="https://github.com/chenglou/pretext">pretext</a> — no DOM measurement.
          Drag column edges to resize. Click headers to sort.
        </p>
      </div>
      <div id="table-root"></div>
    </div>
  `

  createTable(document.getElementById('table-root')!, tableData)
}

boot()
