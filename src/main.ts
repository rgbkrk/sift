import { RecordBatchReader } from 'apache-arrow'
import type { RecordBatch } from 'apache-arrow'
import {
  createTable,
  type Column,
  type ColumnType,
  type TableData,
  type TableEngine,
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
import { DATASETS, type DatasetEntry } from './datasets'
import { loadHuggingFaceParquet } from './parquet-loader'
import './style.css'

// --- Column definitions for the generated dataset ---

const generatedColumnOverrides: Record<string, Partial<Column>> = {
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

/** Guess a reasonable default width for a column based on type and name length */
function autoWidth(name: string, colType: ColumnType): number {
  if (colType === 'boolean') return 100
  if (colType === 'timestamp') return 140
  if (colType === 'numeric') return 120
  // Categorical: scale with name length, clamped
  return Math.max(100, Math.min(250, name.length * 12 + 40))
}

// --- State ---
let currentEngine: TableEngine | null = null
let currentDatasetId = 'generated'

// --- Boot ---

function getInitialDataset(): string {
  const params = new URLSearchParams(window.location.search)
  return params.get('dataset') ?? 'generated'
}

async function boot() {
  const app = document.getElementById('app')!
  currentDatasetId = getInitialDataset()
  renderShell(app)
  await loadDataset(currentDatasetId)
}

function renderShell(app: HTMLElement) {
  const dataset = DATASETS.find(d => d.id === currentDatasetId) ?? DATASETS[0]

  app.innerHTML = `
    <div class="pt-page">
      <div class="pt-intro">
        <p class="pt-eyebrow">Pretext × Arrow × Semiotic</p>
        <div class="pt-intro-row">
          <h1>Table Viewer</h1>
          <div class="pt-dataset-picker">
            <select id="dataset-select">
              ${DATASETS.map(d => `
                <option value="${d.id}" ${d.id === currentDatasetId ? 'selected' : ''}>
                  ${d.label}${d.rows ? ` (${d.rows})` : ''}
                </option>
              `).join('')}
            </select>
          </div>
        </div>
        <p class="pt-subtitle" id="dataset-description">${dataset.description}</p>
      </div>
      <div id="table-root"></div>
    </div>
  `

  document.getElementById('dataset-select')!.addEventListener('change', (e) => {
    const select = e.target as HTMLSelectElement
    const newId = select.value
    if (newId !== currentDatasetId) {
      currentDatasetId = newId
      // Update URL without reload
      const url = new URL(window.location.href)
      if (newId === 'generated') {
        url.searchParams.delete('dataset')
      } else {
        url.searchParams.set('dataset', newId)
      }
      window.history.pushState({}, '', url)
      loadDataset(newId)
    }
  })
}

async function loadDataset(datasetId: string) {
  const dataset = DATASETS.find(d => d.id === datasetId)
  if (!dataset) return

  // Update description
  const descEl = document.getElementById('dataset-description')
  if (descEl) descEl.textContent = dataset.description

  // Clean up previous table
  if (currentEngine) {
    currentEngine.destroy()
    currentEngine = null
  }

  const tableRoot = document.getElementById('table-root')!
  tableRoot.innerHTML = '<div class="pt-loading">Loading data…</div>'

  try {
    if (dataset.source === 'local') {
      await loadLocalArrow(dataset, tableRoot)
    } else {
      await loadHuggingFace(dataset, tableRoot)
    }
  } catch (err) {
    console.error('Failed to load dataset:', err)
    tableRoot.innerHTML = `<div class="pt-loading">
      Failed to load dataset: ${err instanceof Error ? err.message : String(err)}
    </div>`
  }
}

async function loadLocalArrow(dataset: DatasetEntry, tableRoot: HTMLElement) {
  const response = await fetch(`${import.meta.env.BASE_URL}${dataset.path}`)
  if (!response.ok) {
    tableRoot.innerHTML =
      '<div class="pt-loading">Missing data.arrow — run <code>npm run generate</code> first.</div>'
    return
  }

  const reader = await RecordBatchReader.from(response)
  await reader.open()

  const { columns, fieldNames, stringCols, rawCols, accumulators, tableData } =
    buildTableState(reader.schema, dataset, generatedColumnOverrides)

  let totalRows = 0

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

  const firstResult = await reader.next()
  if (firstResult.done) {
    tableRoot.innerHTML = '<div class="pt-loading">No data in Arrow file.</div>'
    return
  }
  appendBatch(firstResult.value)

  tableRoot.innerHTML = ''
  currentEngine = createTable(tableRoot, tableData)

  for await (const batch of reader) {
    appendBatch(batch)
    currentEngine.onBatchAppended()
  }
}

async function loadHuggingFace(dataset: DatasetEntry, tableRoot: HTMLElement) {
  const { ipcBytes } = await loadHuggingFaceParquet(
    dataset.path,
    dataset.config,
    undefined,
    (status) => {
      tableRoot.innerHTML = `<div class="pt-loading">${status}</div>`
    },
  )

  tableRoot.innerHTML = '<div class="pt-loading">Parsing Arrow data…</div>'

  const reader = await RecordBatchReader.from(ipcBytes)
  await reader.open()

  const { columns, fieldNames, stringCols, rawCols, accumulators, tableData } =
    buildTableState(reader.schema, dataset)

  let totalRows = 0

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

  const firstResult = await reader.next()
  if (firstResult.done) {
    tableRoot.innerHTML = '<div class="pt-loading">No data in dataset.</div>'
    return
  }
  appendBatch(firstResult.value)

  tableRoot.innerHTML = ''
  currentEngine = createTable(tableRoot, tableData)

  for await (const batch of reader) {
    appendBatch(batch)
    currentEngine.onBatchAppended()
  }
}

/** Build table columns, stores, and accumulators from an Arrow schema. */
function buildTableState(
  schema: import('apache-arrow').Schema,
  dataset: DatasetEntry,
  columnOverrides?: Record<string, Partial<Column>>,
) {
  const fieldNames = schema.fields.map(f => f.name)
  const typeOverrides = dataset.typeOverrides ?? {}

  const columns: Column[] = schema.fields.map(field => {
    const colType = typeOverrides[field.name] ?? detectColumnType(field)
    const overrides = columnOverrides?.[field.name]
    return {
      key: field.name,
      label: overrides?.label ?? field.name,
      width: overrides?.width ?? autoWidth(field.name, colType),
      sortable: overrides?.sortable ?? true,
      numeric: colType === 'numeric',
      columnType: colType,
    }
  })

  const stringCols: string[][] = fieldNames.map(() => [])
  const rawCols: unknown[][] = fieldNames.map(() => [])

  const accumulators: SummaryAccumulator[] = columns.map((col, c) => {
    switch (col.columnType) {
      case 'numeric': return new NumericAccumulator()
      case 'timestamp': return new TimestampAccumulator()
      case 'boolean': return new BooleanAccumulator()
      case 'categorical': return new CategoricalAccumulator(stringCols[c])
    }
  })

  const tableData: TableData = {
    columns,
    rowCount: 0,
    getCell: (row, col) => stringCols[col][row],
    getCellRaw: (row, col) => rawCols[col][row],
    columnSummaries: columns.map(() => null),
  }

  return { columns, fieldNames, stringCols, rawCols, accumulators, tableData }
}

boot()
