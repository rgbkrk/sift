import { RecordBatchReader, Type } from 'apache-arrow'
import type { RecordBatch, Field } from 'apache-arrow'
import {
  createTable,
  type Column,
  type ColumnType,
  type TableData,
  type ColumnSummary,
  type NumericColumnSummary,
  type CategoricalColumnSummary,
  type BooleanColumnSummary,
  type TimestampColumnSummary,
} from './table'
import './style.css'

// --- Column type detection from Arrow schema ---

function detectColumnType(field: Field): ColumnType {
  const t = field.type.typeId
  if (t === Type.Bool) return 'boolean'
  if (t === Type.Timestamp || t === Type.Date || t === Type.DateMillisecond || t === Type.DateDay) return 'timestamp'
  if (t === Type.Int || t === Type.Float || t === Type.Decimal || t === Type.Int8 || t === Type.Int16 || t === Type.Int32 || t === Type.Int64 || t === Type.Float16 || t === Type.Float32 || t === Type.Float64) return 'numeric'
  return 'categorical'
}

// --- Summary accumulators ---

const BIN_COUNT = 25
const TOP_CATEGORIES = 3

interface SummaryAccumulator {
  add(rawCol: unknown[], startRow: number, count: number): void
  snapshot(totalRows: number): ColumnSummary
}

class NumericAccumulator implements SummaryAccumulator {
  min = Infinity; max = -Infinity
  monotonic = true; lastValue = -Infinity
  // Only finite values go into the histogram
  private finiteValues: number[] = []
  private nanCount = 0
  private infCount = 0
  private negInfCount = 0
  private nullCount = 0

  add(rawCol: unknown[], startRow: number, count: number) {
    for (let r = startRow; r < startRow + count; r++) {
      const raw = rawCol[r]
      if (raw == null) { this.nullCount++; continue }
      const v = raw as number
      if (Number.isNaN(v)) { this.nanCount++; continue }
      if (v === Infinity) { this.infCount++; continue }
      if (v === -Infinity) { this.negInfCount++; continue }
      this.finiteValues.push(v)
      if (v < this.min) this.min = v
      if (v > this.max) this.max = v
      if (v < this.lastValue) this.monotonic = false
      this.lastValue = v
    }
  }

  snapshot(): ColumnSummary {
    if (this.monotonic && this.nanCount === 0 && this.infCount === 0 && this.negInfCount === 0 && this.nullCount === 0) return null
    if (this.finiteValues.length === 0) return null
    const binWidth = (this.max - this.min) / BIN_COUNT || 1
    const bins: NumericColumnSummary['bins'] = []
    for (let b = 0; b < BIN_COUNT; b++) {
      bins.push({ x0: this.min + b * binWidth, x1: this.min + (b + 1) * binWidth, count: 0 })
    }
    for (const v of this.finiteValues) {
      let idx = Math.floor((v - this.min) / binWidth)
      if (idx >= BIN_COUNT) idx = BIN_COUNT - 1
      if (idx < 0) idx = 0
      bins[idx].count++
    }
    return { kind: 'numeric', min: this.min, max: this.max, bins }
  }
}

class TimestampAccumulator implements SummaryAccumulator {
  min = Infinity; max = -Infinity
  private allValues: number[] = []

  add(rawCol: unknown[], startRow: number, count: number) {
    for (let r = startRow; r < startRow + count; r++) {
      const v = Number(rawCol[r])
      this.allValues.push(v)
      if (v < this.min) this.min = v
      if (v > this.max) this.max = v
    }
  }

  snapshot(): ColumnSummary {
    if (this.allValues.length === 0) return null
    const binWidth = (this.max - this.min) / BIN_COUNT || 1
    const bins: TimestampColumnSummary['bins'] = []
    for (let b = 0; b < BIN_COUNT; b++) {
      bins.push({ x0: this.min + b * binWidth, x1: this.min + (b + 1) * binWidth, count: 0 })
    }
    for (const v of this.allValues) {
      let idx = Math.floor((v - this.min) / binWidth)
      if (idx >= BIN_COUNT) idx = BIN_COUNT - 1
      if (idx < 0) idx = 0
      bins[idx].count++
    }
    return { kind: 'timestamp', min: this.min, max: this.max, bins }
  }
}

class CategoricalAccumulator implements SummaryAccumulator {
  private freq = new Map<string, number>()
  private stringCol: string[]

  constructor(stringCol: string[]) {
    this.stringCol = stringCol
  }

  add(_rawCol: unknown[], startRow: number, count: number) {
    for (let r = startRow; r < startRow + count; r++) {
      const s = this.stringCol[r]
      this.freq.set(s, (this.freq.get(s) ?? 0) + 1)
    }
  }

  snapshot(totalRows: number): ColumnSummary {
    const sorted = [...this.freq.entries()].sort((a, b) => b[1] - a[1])
    const topCategories = sorted.slice(0, TOP_CATEGORIES).map(([label, count]) => ({
      label, count,
      pct: Math.round((count / totalRows) * 1000) / 10,
    }))
    const othersCount = sorted.slice(TOP_CATEGORIES).reduce((s, e) => s + e[1], 0)
    const othersPct = Math.round((othersCount / totalRows) * 1000) / 10
    return {
      kind: 'categorical',
      uniqueCount: this.freq.size,
      topCategories,
      othersCount,
      othersPct,
    } satisfies CategoricalColumnSummary
  }
}

class BooleanAccumulator implements SummaryAccumulator {
  trueCount = 0; falseCount = 0

  add(rawCol: unknown[], startRow: number, count: number) {
    for (let r = startRow; r < startRow + count; r++) {
      if (rawCol[r]) this.trueCount++
      else this.falseCount++
    }
  }

  snapshot(totalRows: number): ColumnSummary {
    return {
      kind: 'boolean',
      trueCount: this.trueCount,
      falseCount: this.falseCount,
      total: totalRows,
    } satisfies BooleanColumnSummary
  }
}

// --- Column definitions ---

const columnOverrides: Record<string, Partial<Column>> = {
  id:         { label: 'ID', width: 90, sortable: true },
  name:       { label: 'Name', width: 180, sortable: true },
  location:   { label: 'Location', width: 180, sortable: true },
  department: { label: 'Department', width: 160, sortable: true },
  note:       { label: 'Note', width: 300, sortable: false },
  status:     { label: 'Status', width: 120, sortable: true },
  priority:   { label: 'Priority', width: 100, sortable: true },
  score:      { label: 'Score', width: 100, sortable: true },
  email:      { label: 'Email', width: 200, sortable: true },
  verified:   { label: 'Verified', width: 100, sortable: true },
  joined:     { label: 'Joined', width: 120, sortable: true },
  chaos:      { label: 'Chaos', width: 130, sortable: true },
}

// --- Boot ---

async function boot() {
  const app = document.getElementById('app')!
  app.innerHTML = '<div class="pt-loading">Loading data…</div>'

  const response = await fetch('/data.arrow')
  if (!response.ok) {
    app.innerHTML =
      '<div class="pt-loading">Missing data.arrow — run <code>npm run generate</code> first.</div>'
    return
  }

  const reader = await RecordBatchReader.from(response)
  await reader.open()
  const schema = reader.schema
  const fieldNames = schema.fields.map(f => f.name)

  // Column type overrides (for columns whose Arrow type doesn't match intent)
  const typeOverrides: Record<string, ColumnType> = {
    joined: 'timestamp',
  }

  // Build column definitions from Arrow schema
  const columns: Column[] = schema.fields.map(field => {
    const colType = typeOverrides[field.name] ?? detectColumnType(field)
    const overrides = columnOverrides[field.name]
    return {
      key: field.name,
      label: overrides?.label ?? field.name,
      width: overrides?.width ?? 150,
      sortable: overrides?.sortable ?? true,
      numeric: colType === 'numeric',
      columnType: colType,
    }
  })

  // Growable column stores
  const stringCols: string[][] = fieldNames.map(() => [])
  const rawCols: unknown[][] = fieldNames.map(() => [])

  // Summary accumulators
  const accumulators: SummaryAccumulator[] = columns.map((col, c) => {
    switch (col.columnType) {
      case 'numeric': return new NumericAccumulator()
      case 'timestamp': return new TimestampAccumulator()
      case 'boolean': return new BooleanAccumulator()
      case 'categorical': return new CategoricalAccumulator(stringCols[c])
    }
  })

  let totalRows = 0

  // Mutable table data object — grows as batches arrive
  const tableData: TableData = {
    columns,
    rowCount: 0,
    getCell: (row, col) => stringCols[col][row],
    getCellRaw: (row, col) => rawCols[col][row],
    columnSummaries: columns.map(() => null),
  }

  function formatCell(colIndex: number, val: unknown): string {
    if (val == null) return ''
    switch (columns[colIndex].columnType) {
      case 'timestamp': {
        const d = new Date(Number(val))
        return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })
      }
      case 'boolean':
        return val ? 'Yes' : 'No'
      default:
        return String(val)
    }
  }

  function appendBatch(batch: RecordBatch) {
    const batchRows = batch.numRows
    const startRow = totalRows

    for (let c = 0; c < fieldNames.length; c++) {
      const col = batch.getChild(fieldNames[c])!
      for (let r = 0; r < batchRows; r++) {
        const val = col.get(r)
        rawCols[c].push(val)
        stringCols[c].push(formatCell(c, val))
      }
      accumulators[c].add(rawCols[c], startRow, batchRows)
    }

    totalRows += batchRows
    tableData.rowCount = totalRows
    tableData.columnSummaries = accumulators.map(a => a.snapshot(totalRows))
  }

  // Mount page shell
  app.innerHTML = `
    <div class="pt-page">
      <div class="pt-intro">
        <p class="pt-eyebrow">Pretext × Arrow × Semiotic</p>
        <h1>Table Viewer</h1>
        <p class="pt-subtitle">
          Streaming from Arrow IPC. No DOM measurement. Resize columns, click headers to sort.
        </p>
      </div>
      <div id="table-root"></div>
    </div>
  `

  // Read first batch to initialize the table
  const firstResult = await reader.next()
  if (firstResult.done) {
    app.innerHTML = '<div class="pt-loading">No data in Arrow file.</div>'
    return
  }
  appendBatch(firstResult.value)

  const engine = createTable(document.getElementById('table-root')!, tableData)

  // Stream remaining batches
  for await (const batch of reader) {
    appendBatch(batch)
    engine.onBatchAppended()
  }
}

boot()
