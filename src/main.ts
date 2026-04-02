import { RecordBatchReader } from 'apache-arrow'
import type { RecordBatch } from 'apache-arrow'
import {
  createTable,
  type Column,
  type ColumnType,
  type TableData,
} from './table'
import {
  detectColumnType,
  formatCell,
  NumericAccumulator,
  TimestampAccumulator,
  CategoricalAccumulator,
  BooleanAccumulator,
  type SummaryAccumulator,
} from './accumulators'
import './style.css'

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

  const response = await fetch(`${import.meta.env.BASE_URL}data.arrow`)
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

  // (formatCell is imported from ./accumulators)

  function appendBatch(batch: RecordBatch) {
    const batchRows = batch.numRows
    const startRow = totalRows

    for (let c = 0; c < fieldNames.length; c++) {
      const col = batch.getChild(fieldNames[c])!
      for (let r = 0; r < batchRows; r++) {
        const val = col.get(r)
        rawCols[c].push(val)
        stringCols[c].push(formatCell(columns[c].columnType, val))
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
