# Embedding the Sift Table Engine

Practical guide for using `@nteract/data-explorer` in your app.

## Install

```sh
npm install @nteract/data-explorer apache-arrow
```

Peer dependencies: `react`, `react-dom`, `apache-arrow`, `semiotic`, `@chenglou/pretext`.

---

## 1. Quick Start (React)

The simplest path: point `<SiftTable>` at an Arrow IPC file.

```tsx
import { SiftTable } from '@nteract/data-explorer'

function App() {
  return (
    <div style={{ height: 600 }}>
      <SiftTable
        url="/data.arrow"
        onChange={(state) => {
          console.log('Filtered:', state.filteredCount, '/', state.totalCount)
          console.log('Sort:', state.sort)
          console.log('Filters:', state.filters)
        }}
      />
    </div>
  )
}
```

The component streams batches from the URL, builds the table on the first batch, and progressively appends the rest.

### Props

| Prop | Type | Description |
|------|------|-------------|
| `data` | `TableData` | Pre-built data (mutually exclusive with `url`) |
| `url` | `string` | Arrow IPC URL to stream from |
| `typeOverrides` | `Record<string, ColumnType>` | Force column types by name |
| `columnOverrides` | `Record<string, Partial<Column>>` | Override label, width, sortable |
| `onChange` | `(state: TableEngineState) => void` | Sort/filter state callback |
| `className` | `string` | CSS class for container |
| `style` | `CSSProperties` | Inline styles for container |

### Overrides

```tsx
<SiftTable
  url="/events.arrow"
  typeOverrides={{ created_at: 'timestamp' }}
  columnOverrides={{ id: { width: 80, sortable: false } }}
/>
```

---

## 2. Vanilla JS

Use `createTable()` directly without React. You manage the DOM container and data lifecycle.

```ts
import { createTable, type TableData, type TableEngine } from '@nteract/data-explorer'

// 1. Build your TableData
const tableData: TableData = {
  columns: [
    { key: 'name', label: 'Name', width: 200, sortable: true, numeric: false, columnType: 'categorical' },
    { key: 'age',  label: 'Age',  width: 100, sortable: true, numeric: true,  columnType: 'numeric' },
  ],
  rowCount: 3,
  getCell: (row, col) => strings[col][row],       // formatted display string
  getCellRaw: (row, col) => rawValues[col][row],   // original value for sorting/filtering
  columnSummaries: [null, null],                    // null = no header chart yet
}

// 2. Mount
const container = document.getElementById('table')!
const engine = createTable(container, tableData, {
  onChange: (state) => console.log(state),
})

// 3. Clean up
engine.destroy()
```

### Streaming Arrow batches

Build data incrementally from `RecordBatchReader`:

```ts
import { RecordBatchReader } from 'apache-arrow'
import {
  createTable,
  detectColumnType,
  formatCell,
  NumericAccumulator,
  CategoricalAccumulator,
  BooleanAccumulator,
  TimestampAccumulator,
} from '@nteract/data-explorer'

const response = await fetch('/data.arrow')
const reader = await RecordBatchReader.from(response)
await reader.open()

const fields = reader.schema.fields
const stringCols: string[][] = fields.map(() => [])
const rawCols: unknown[][] = fields.map(() => [])

const columns = fields.map(f => {
  const colType = detectColumnType(f)
  return { key: f.name, label: f.name, width: 150, sortable: true, numeric: colType === 'numeric', columnType: colType }
})

const accumulators = columns.map((c, i) => {
  switch (c.columnType) {
    case 'numeric':     return new NumericAccumulator()
    case 'timestamp':   return new TimestampAccumulator()
    case 'boolean':     return new BooleanAccumulator()
    case 'categorical': return new CategoricalAccumulator(stringCols[i])
  }
})

const tableData = {
  columns,
  rowCount: 0,
  getCell: (row: number, col: number) => stringCols[col][row],
  getCellRaw: (row: number, col: number) => rawCols[col][row],
  columnSummaries: columns.map(() => null),
}

let totalRows = 0
let engine: ReturnType<typeof createTable> | null = null

for await (const batch of reader) {
  const n = batch.numRows
  for (let c = 0; c < fields.length; c++) {
    const col = batch.getChild(fields[c].name)!
    for (let r = 0; r < n; r++) {
      const val = col.get(r)
      rawCols[c].push(val)
      stringCols[c].push(formatCell(columns[c].columnType, val))
    }
    accumulators[c].add(rawCols[c], totalRows, n)
  }
  totalRows += n
  tableData.rowCount = totalRows
  tableData.columnSummaries = accumulators.map(a => a.snapshot(totalRows))

  if (!engine) {
    engine = createTable(document.getElementById('table')!, tableData)
  } else {
    engine.onBatchAppended()
  }
}
engine!.setStreamingDone()
```

---

## 3. TableData Shape

The engine is data-source agnostic. It reads from two accessor functions:

```ts
type TableData = {
  columns: Column[]                              // column metadata
  rowCount: number                               // current total (mutate for streaming)
  getCell: (row: number, col: number) => string  // formatted display string
  getCellRaw: (row: number, col: number) => unknown  // raw value for sort/filter
  columnSummaries: ColumnSummary[]               // header chart data (null = none)
}
```

Back these with arrays, Arrow columns, or any data structure.

| ColumnType | getCellRaw | Header chart |
|------------|-----------|--------------|
| `numeric` | `number` | histogram |
| `categorical` | `string` | top-3 bars + popover |
| `boolean` | `boolean` | ratio bar |
| `timestamp` | `number` (epoch ms) | date histogram |

---

## 4. Engine API

### Sort

```ts
engine.setSort('age', 'desc')
engine.getSort()  // { column: 'age', direction: 'desc' }
```

### Filter

```ts
engine.setFilter(1, { kind: 'range', min: 18, max: 65 })              // numeric range
engine.setFilter(0, { kind: 'set', values: new Set(['CA', 'NY']) })    // categorical
engine.setFilter(2, { kind: 'boolean', value: true })                  // boolean
engine.clearFilter(1)
engine.clearAllFilters()
```

### State snapshot and serialization

```ts
const state = engine.getState()
// { sort, filters, filteredCount, totalCount }

import { engineStateToExplorerState, predicateToSQL, predicateToPandas } from '@nteract/data-explorer'
const explorer = engineStateToExplorerState(state)
explorer.filters.map(predicateToSQL)    // ["age BETWEEN 18 AND 65"]
explorer.filters.map(predicateToPandas) // ["df[(df['age'] >= 18) & (df['age'] <= 65)]"]
```

### Streaming lifecycle

```ts
engine.onBatchAppended()   // after mutating tableData.rowCount + columnSummaries
engine.setStreamingDone()  // all batches loaded (hides progress bar)
```

---

## 5. Styling

The table uses CSS custom properties scoped to `:root`. Override them to theme.

| Variable | Light | Dark | Purpose |
|----------|-------|------|---------|
| `--page` | `#f5f2ec` | `#1a1816` | page background |
| `--panel` | `#fffdf9` | `#242120` | table background |
| `--ink` | `#1e1a18` | `#e8e2dc` | text color |
| `--muted` | `#6e655f` | `#9a918a` | secondary text |
| `--rule` | `#d8cec3` | `#3a3533` | borders |
| `--accent` | `#955f3b` | `#d4896a` | highlights, links |
| `--row-alt` | `rgba(0,0,0,0.018)` | `rgba(255,255,255,0.025)` | striped rows |
| `--font` | `Inter, sans-serif` | same | font stack |

Dark mode activates via `@media (prefers-color-scheme: dark)` or `[data-theme="dark"]`.

Override to match your host app:

```css
.my-explorer-wrapper {
  --page: var(--my-app-bg);
  --ink: var(--my-app-text);
  --accent: var(--my-app-primary);
}
```

The container must have a defined height. The table fills its parent.

---

## 6. WASM Compute (Optional)

The `nteract-predicate` WASM crate accelerates filtering and summary computation using arrow-rs kernels. It is **completely optional** -- the table works with pure JS accumulators out of the box.

Without WASM, summaries are computed in JS via accumulator classes (`NumericAccumulator`, etc.) -- handles ~500K rows smoothly on the main thread.

With WASM, the `nteract-predicate` module provides `filter_rows`, `value_counts`, `histogram`, and `string_contains` via arrow-rs. It loads lazily on first use. To include it:

```sh
# Build the crate
cd crates/nteract-predicate
wasm-pack build --target web --release

# Copy to your app's public/wasm/
cp pkg/nteract_predicate_bg.wasm public/wasm/
cp pkg/nteract_predicate.js public/wasm/
```

The engine detects the WASM module at runtime. If it is not available, all operations fall back to JS.

---

## Library Build

The library is built with Vite in library mode:

```sh
npm run build:lib   # → lib/index.js (ESM, ~35KB)
```

External dependencies (react, apache-arrow, semiotic, pretext) are not bundled -- they are expected as peer dependencies in the consuming app.
