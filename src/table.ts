import { prepare, layout, type PreparedText } from '@chenglou/pretext'
import { renderColumnSummary, unmountColumnSummary } from './sparkline'
import {
  NumericAccumulator,
  TimestampAccumulator,
  CategoricalAccumulator,
  BooleanAccumulator,
} from './accumulators'

// --- Types ---

export type ColumnType = 'numeric' | 'categorical' | 'timestamp' | 'boolean'

export type Column = {
  key: string
  label: string
  width: number
  sortable: boolean
  numeric: boolean
  columnType: ColumnType
}

export type NumericColumnSummary = {
  kind: 'numeric'
  min: number
  max: number
  bins: { x0: number; x1: number; count: number }[]
}

export type CategoryEntry = { label: string; count: number; pct: number }

export type CategoricalColumnSummary = {
  kind: 'categorical'
  uniqueCount: number
  topCategories: CategoryEntry[]
  othersCount: number
  othersPct: number
  /** All categories sorted by frequency (descending). Used by the filter popover. */
  allCategories: CategoryEntry[]
}

export type BooleanColumnSummary = {
  kind: 'boolean'
  trueCount: number
  falseCount: number
  nullCount: number
  total: number
}

export type TimestampColumnSummary = {
  kind: 'timestamp'
  min: number
  max: number
  bins: { x0: number; x1: number; count: number }[]
}

export type ColumnSummary =
  | NumericColumnSummary
  | CategoricalColumnSummary
  | BooleanColumnSummary
  | TimestampColumnSummary
  | null

export type TableData = {
  columns: Column[]
  rowCount: number
  getCell: (row: number, col: number) => string
  getCellRaw: (row: number, col: number) => unknown
  columnSummaries: ColumnSummary[]
}

// --- Filter types ---

export type RangeFilter = { kind: 'range'; min: number; max: number }
export type SetFilter = { kind: 'set'; values: Set<string> }
export type BooleanFilter = { kind: 'boolean'; value: boolean }
export type ColumnFilter = RangeFilter | SetFilter | BooleanFilter | null

export type TableEngineState = {
  sort: { column: string; direction: 'asc' | 'desc' } | null
  filters: { column: string; filter: ColumnFilter }[]
  filteredCount: number
  totalCount: number
}

export type TableEngineOptions = {
  /** Called whenever sort or filter state changes from UI interaction. */
  onChange?: (state: TableEngineState) => void
}

export type TableEngine = {
  onBatchAppended(): void
  destroy(): void
  setFilter(colIndex: number, filter: ColumnFilter): void
  clearFilter(colIndex: number): void
  clearAllFilters(): void
  /** Get current sort state. */
  getSort(): { column: string; direction: 'asc' | 'desc' } | null
  /** Programmatically sort by column name and direction. Pass null to clear. */
  setSort(column: string, direction: 'asc' | 'desc'): void
  /** Get current filter state for all columns. */
  getFilters(): { column: string; filter: ColumnFilter }[]
  /** Get the full explorer state (sort + filters + counts) in a serializable format. */
  getState(): TableEngineState
}

type SortState = { col: number; dir: 'asc' | 'desc' } | null

// --- Constants ---

const FONT = '14px Inter, "Helvetica Neue", Helvetica, Arial, sans-serif'
const LINE_HEIGHT = 20
const CELL_PAD_H = 24 // 12px each side
const CELL_PAD_V = 16 // 8px top + 8px bottom
const MIN_COL_WIDTH = 60
const OVERSCAN = 10 // extra rows above/below viewport

// --- Table Engine ---

export function createTable(container: HTMLElement, data: TableData, options?: TableEngineOptions): TableEngine {
  const { columns } = data

  // Mutable row count — grows as batches arrive
  let rowCount = data.rowCount

  // Filter state — one per column, null = no filter
  const filters: ColumnFilter[] = columns.map(() => null)
  let filteredCount = rowCount

  // Unfiltered summaries (saved so we can restore when filters are cleared)
  let unfilteredSummaries: ColumnSummary[] = [...data.columnSummaries]

  // Sort state
  let sortState: SortState = null

  // viewIndices: sorted position → data row (filtered + sorted)
  let viewIndices: Int32Array

  // Growable typed arrays with capacity doubling
  let capacity = Math.max(8192, rowCount)
  let rowHeights = new Float64Array(capacity)
  let rowPositions = new Float64Array(capacity + 1)
  viewIndices = new Int32Array(capacity)
  for (let i = 0; i < rowCount; i++) viewIndices[i] = i

  let totalHeight = 0
  let heightsDirty = true

  function growBuffers(needed: number) {
    if (needed <= capacity) return
    while (capacity < needed) capacity *= 2
    const newHeights = new Float64Array(capacity)
    newHeights.set(rowHeights.subarray(0, rowCount))
    rowHeights = newHeights
    const newPositions = new Float64Array(capacity + 1)
    newPositions.set(rowPositions.subarray(0, rowCount + 1))
    rowPositions = newPositions
    const newSorted = new Int32Array(capacity)
    newSorted.set(viewIndices.subarray(0, rowCount))
    viewIndices = newSorted
  }

  // Per-cell cache: prepared text + last laid-out width/height
  type CellCache = { prepared: PreparedText; lastWidth: number; lastHeight: number }
  const cellCaches: (CellCache[] | null)[] = []

  function prepareCellRow(r: number): CellCache[] {
    const row = new Array<CellCache>(columns.length)
    for (let c = 0; c < columns.length; c++) {
      row[c] = {
        prepared: prepare(data.getCell(r, c), FONT),
        lastWidth: -1,
        lastHeight: 0,
      }
    }
    return row
  }

  // Cells are prepared lazily when they enter the viewport.
  // computeRowHeight() handles null caches with an estimated height.

  // Column widths (mutable copy)
  const colWidths = columns.map(c => c.width)

  // --- Compute heights ---

  function computeRowHeight(sortedRow: number): number {
    const dataRow = viewIndices[sortedRow]
    const cache = cellCaches[dataRow]
    if (!cache) return LINE_HEIGHT + CELL_PAD_V // estimate for unprepared rows

    let maxH = LINE_HEIGHT
    for (let c = 0; c < columns.length; c++) {
      const cell = cache[c]
      const cellW = Math.max(1, colWidths[c] - CELL_PAD_H)
      if (cell.lastWidth !== cellW) {
        const { height } = layout(cell.prepared, cellW, LINE_HEIGHT)
        cell.lastHeight = height
        cell.lastWidth = cellW
      }
      if (cell.lastHeight > maxH) maxH = cell.lastHeight
    }
    return maxH + CELL_PAD_V
  }

  function recomputeAllHeights() {
    for (let i = 0; i < filteredCount; i++) {
      rowHeights[i] = computeRowHeight(i)
    }
    rebuildPositions()
    heightsDirty = false
  }

  function rebuildPositions() {
    rowPositions[0] = 0
    for (let i = 0; i < filteredCount; i++) {
      rowPositions[i + 1] = rowPositions[i] + rowHeights[i]
    }
    totalHeight = rowPositions[filteredCount]
  }

  function rowAtOffset(offset: number): number {
    let lo = 0, hi = filteredCount
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (rowPositions[mid + 1] <= offset) lo = mid + 1
      else hi = mid
    }
    return lo
  }

  // --- Filter + Sort ---

  function rowPassesFilters(dataRow: number): boolean {
    for (let c = 0; c < columns.length; c++) {
      const f = filters[c]
      if (!f) continue
      const raw = data.getCellRaw(dataRow, c)
      switch (f.kind) {
        case 'range': {
          if (raw == null) return false
          const v = Number(raw)
          if (!Number.isFinite(v)) return false
          if (v < f.min || v > f.max) return false
          break
        }
        case 'set': {
          const s = data.getCell(dataRow, c)
          if (!f.values.has(s)) return false
          break
        }
        case 'boolean': {
          if (Boolean(raw) !== f.value) return false
          break
        }
      }
    }
    return true
  }

  function hasActiveFilters(): boolean {
    return filters.some(f => f !== null)
  }

  function applyFilterAndSort() {
    // Step 1: filter
    const filtered: number[] = []
    if (hasActiveFilters()) {
      for (let i = 0; i < rowCount; i++) {
        if (rowPassesFilters(i)) filtered.push(i)
      }
    } else {
      for (let i = 0; i < rowCount; i++) filtered.push(i)
    }
    filteredCount = filtered.length

    // Step 2: sort the filtered set
    if (sortState) {
      const { col, dir } = sortState
      const colType = columns[col].columnType
      const isNumeric = colType === 'numeric' || colType === 'timestamp'
      filtered.sort((a, b) => {
        let cmp: number
        if (isNumeric) {
          const rawA = data.getCellRaw(a, col)
          const rawB = data.getCellRaw(b, col)
          const va = rawA == null ? NaN : Number(rawA)
          const vb = rawB == null ? NaN : Number(rawB)
          const aOk = Number.isFinite(va) || va === Infinity || va === -Infinity
          const bOk = Number.isFinite(vb) || vb === Infinity || vb === -Infinity
          if (!aOk && !bOk) cmp = 0
          else if (!aOk) return 1
          else if (!bOk) return -1
          else cmp = va - vb
        } else if (colType === 'boolean') {
          const rawA = data.getCellRaw(a, col)
          const rawB = data.getCellRaw(b, col)
          // Nulls always sort to the end
          if (rawA == null && rawB == null) cmp = 0
          else if (rawA == null) return 1
          else if (rawB == null) return -1
          else cmp = (rawA ? 1 : 0) - (rawB ? 1 : 0)
        } else {
          const rawA = data.getCellRaw(a, col)
          const rawB = data.getCellRaw(b, col)
          // Nulls always sort to the end
          if (rawA == null && rawB == null) cmp = 0
          else if (rawA == null) return 1
          else if (rawB == null) return -1
          else {
            const sa = data.getCell(a, col)
            const sb = data.getCell(b, col)
            cmp = sa < sb ? -1 : sa > sb ? 1 : 0
          }
        }
        return dir === 'asc' ? cmp : -cmp
      })
    }

    // Step 3: write to viewIndices
    if (filtered.length > capacity) growBuffers(filtered.length)
    for (let i = 0; i < filtered.length; i++) {
      viewIndices[i] = filtered[i]
    }

    heightsDirty = true
  }

  // --- DOM ---

  container.innerHTML = ''
  container.classList.add('pt-table-container')
  container.setAttribute('role', 'grid')
  container.setAttribute('aria-label', 'Data table')

  // Header
  const headerEl = document.createElement('div')
  headerEl.className = 'pt-header'
  const headerRowEl = document.createElement('div')
  headerRowEl.className = 'pt-header-row'

  const summaryContainers: HTMLDivElement[] = []

  for (let c = 0; c < columns.length; c++) {
    const th = document.createElement('div')
    th.className = 'pt-th'
    th.setAttribute('role', 'columnheader')
    th.style.width = colWidths[c] + 'px'
    th.dataset.col = String(c)

    const topRow = document.createElement('div')
    topRow.className = 'pt-th-top'

    const label = document.createElement('span')
    label.className = 'pt-th-label'
    label.textContent = columns[c].label
    topRow.appendChild(label)

    if (columns[c].sortable) {
      const arrow = document.createElement('span')
      arrow.className = 'pt-sort-arrow'
      arrow.textContent = ''
      topRow.appendChild(arrow)
      topRow.style.cursor = 'pointer'
      topRow.addEventListener('click', () => onSortClick(c))
    }

    th.appendChild(topRow)

    const summaryEl = document.createElement('div')
    summaryEl.className = 'pt-th-summary'
    // Prevent clicks on summary charts (filter interactions) from triggering sort
    summaryEl.addEventListener('click', (e) => e.stopPropagation())
    th.appendChild(summaryEl)
    summaryContainers.push(summaryEl)

    if (c < columns.length - 1) {
      const handle = document.createElement('div')
      handle.className = 'pt-resize-handle'
      handle.addEventListener('pointerdown', (e) => onResizeStart(e, c))
      th.appendChild(handle)
    }

    headerRowEl.appendChild(th)
  }

  headerEl.appendChild(headerRowEl)
  container.appendChild(headerEl)

  // Create stable filter callbacks per column
  const filterCallbacks: ((filter: ColumnFilter) => void)[] = columns.map((_, c) =>
    (filter: ColumnFilter) => setFilter(c, filter)
  )

  function renderSummary(c: number, visibleBins?: number[]) {
    const summary = data.columnSummaries[c]
    if (summary) {
      const unfiltered = hasActiveFilters() ? unfilteredSummaries[c] ?? undefined : undefined
      renderColumnSummary(
        summaryContainers[c], summary, colWidths[c] - CELL_PAD_H,
        visibleBins, filters[c], filterCallbacks[c], unfiltered,
      )
    }
  }

  function renderAllSummaries() {
    for (let c = 0; c < columns.length; c++) renderSummary(c)
  }
  renderAllSummaries()

  // Scroll viewport
  const viewport = document.createElement('div')
  viewport.className = 'pt-viewport'

  const scrollContent = document.createElement('div')
  scrollContent.className = 'pt-scroll-content'
  // Set min-width so horizontal scroll position is preserved when pool rows are hidden
  function updateScrollContentWidth() {
    const totalW = colWidths.reduce((s, w) => s + w, 0)
    scrollContent.style.minWidth = totalW + 'px'
  }
  updateScrollContentWidth()

  const rowPool = document.createElement('div')
  rowPool.className = 'pt-row-pool'

  scrollContent.appendChild(rowPool)

  // Empty state overlay (shown when filters exclude all rows)
  const emptyEl = document.createElement('div')
  emptyEl.className = 'pt-empty-state'
  emptyEl.textContent = 'No matching rows'
  emptyEl.style.display = 'none'
  viewport.appendChild(emptyEl)

  viewport.appendChild(scrollContent)
  container.appendChild(viewport)

  // Stats bar
  const statsEl = document.createElement('div')
  statsEl.className = 'pt-stats'

  function makeStatSpan(className: string): HTMLSpanElement {
    const el = document.createElement('span')
    el.className = `pt-stat-value ${className}`
    return el
  }

  const statRows = makeStatSpan('pt-stat-rows')
  const statRange = makeStatSpan('pt-stat-range')
  const statDom = makeStatSpan('pt-stat-dom')
  const statFrame = makeStatSpan('pt-stat-frame')

  function updateRowCountDisplay() {
    if (hasActiveFilters()) {
      statRows.textContent = `${filteredCount.toLocaleString()} of ${rowCount.toLocaleString()} rows`
    } else {
      statRows.textContent = `${rowCount.toLocaleString()} rows`
    }
  }
  updateRowCountDisplay()

  // Fullscreen toggle
  const fullscreenBtn = document.createElement('button')
  fullscreenBtn.className = 'pt-fullscreen-btn'
  fullscreenBtn.title = 'Toggle fullscreen'
  fullscreenBtn.textContent = '⛶'
  fullscreenBtn.addEventListener('click', () => {
    if (document.fullscreenElement === container) {
      document.exitFullscreen()
    } else {
      container.requestFullscreen()
    }
  })

  // Update button label on fullscreen change
  function onFullscreenChange() {
    const isFS = document.fullscreenElement === container
    fullscreenBtn.textContent = isFS ? '⛶' : '⛶'
    fullscreenBtn.title = isFS ? 'Exit fullscreen' : 'Toggle fullscreen'
    // Trigger re-render since dimensions changed
    heightsDirty = true
    scheduleRender()
  }
  document.addEventListener('fullscreenchange', onFullscreenChange)

  const filterPillsEl = document.createElement('div')
  filterPillsEl.className = 'pt-filter-pills'

  const statsSpacer = document.createElement('div')
  statsSpacer.style.flex = '1'

  statsEl.append(statRows, sep(), statRange, sep(), statDom, sep(), statFrame, filterPillsEl, statsSpacer, fullscreenBtn)
  container.appendChild(statsEl)

  // Expand columns to fill container width when there are few columns
  {
    const containerW = viewport.clientWidth
    const totalW = colWidths.reduce((s, w) => s + w, 0)
    if (containerW > 0 && totalW < containerW) {
      const scale = containerW / totalW
      const ths = headerRowEl.children as HTMLCollectionOf<HTMLDivElement>
      for (let c = 0; c < columns.length; c++) {
        colWidths[c] = Math.round(colWidths[c] * scale)
        ths[c].style.width = colWidths[c] + 'px'
      }
      updateScrollContentWidth()
      heightsDirty = true
    }
  }

  function rebuildFilterPills() {
    filterPillsEl.innerHTML = ''
    for (let c = 0; c < columns.length; c++) {
      const f = filters[c]
      if (!f) continue
      const pill = document.createElement('span')
      pill.className = 'pt-filter-pill'

      let text = columns[c].label + ': '
      switch (f.kind) {
        case 'range': {
          const colType = columns[c].columnType
          if (colType === 'timestamp') {
            const fmt = (v: number) => new Date(v).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: '2-digit' })
            text += `${fmt(f.min)} – ${fmt(f.max)}`
          } else {
            text += `${f.min.toLocaleString(undefined, { maximumFractionDigits: 1 })} – ${f.max.toLocaleString(undefined, { maximumFractionDigits: 1 })}`
          }
          break
        }
        case 'set':
          text += [...f.values].map(v => v.length > 12 ? v.slice(0, 11) + '…' : v).join(', ')
          break
        case 'boolean':
          text += f.value ? 'Yes' : 'No'
          break
      }

      const label = document.createElement('span')
      label.textContent = text

      const closeBtn = document.createElement('button')
      closeBtn.className = 'pt-filter-pill-x'
      closeBtn.textContent = '×'
      closeBtn.addEventListener('click', (e) => {
        e.stopPropagation()
        clearFilter(c)
      })

      pill.append(label, closeBtn)
      filterPillsEl.appendChild(pill)
    }
  }

  function sep(): HTMLSpanElement {
    const s = document.createElement('span')
    s.className = 'pt-stat-sep'
    s.textContent = ' · '
    return s
  }

  let prevRange = '', prevDom = '', prevFrame = ''

  function updateStat(el: HTMLSpanElement, value: string, prev: string): string {
    if (value !== prev) {
      el.textContent = value
      el.classList.remove('pt-stat-flash')
      void el.offsetWidth
      el.classList.add('pt-stat-flash')
    }
    return value
  }

  // --- Visible range overlay for header sparklines ---

  let lastVisFirst = -1
  let lastVisLast = -1

  function updateVisibleOverlays(visFirst: number, visLast: number) {
    for (let c = 0; c < columns.length; c++) {
      const summary = data.columnSummaries[c]
      if (!summary || (summary.kind !== 'numeric' && summary.kind !== 'timestamp')) continue

      const bins = summary.bins
      const visibleBins = new Array<number>(bins.length).fill(0)
      const binWidth = (summary.max - summary.min) / bins.length || 1

      for (let r = visFirst; r <= visLast; r++) {
        const dataRow = viewIndices[r]
        const raw = data.getCellRaw(dataRow, c)
        if (raw == null) continue
        const v = Number(raw)
        if (!Number.isFinite(v)) continue
        let idx = Math.floor((v - summary.min) / binWidth)
        if (idx >= bins.length) idx = bins.length - 1
        if (idx < 0) idx = 0
        visibleBins[idx]++
      }

      renderSummary(c, visibleBins)
    }
  }

  // --- Row pool ---

  type PooledRow = {
    el: HTMLDivElement
    cells: HTMLDivElement[]
    assignedRow: number
  }

  const pool: PooledRow[] = []

  function getPooledRow(): PooledRow {
    for (const pr of pool) {
      if (pr.assignedRow === -1) return pr
    }
    const el = document.createElement('div')
    el.className = 'pt-row'
    const cells: HTMLDivElement[] = []
    for (let c = 0; c < columns.length; c++) {
      const cell = document.createElement('div')
      cell.className = 'pt-cell'
      cell.style.width = colWidths[c] + 'px'
      el.appendChild(cell)
      cells.push(cell)
    }
    rowPool.appendChild(el)
    const pr: PooledRow = { el, cells, assignedRow: -1 }
    pool.push(pr)
    return pr
  }

  // --- Cell rendering (type-aware) ---

  function renderCell(cellEl: HTMLDivElement, dataRow: number, colIndex: number) {
    const col = columns[colIndex]
    const raw = data.getCellRaw(dataRow, colIndex)
    const str = data.getCell(dataRow, colIndex)

    // Clear previous content
    cellEl.textContent = ''
    cellEl.className = 'pt-cell'

    // Null values get a distinct badge regardless of column type
    if (raw == null) {
      const badge = document.createElement('span')
      badge.className = 'pt-badge pt-badge-null'
      badge.textContent = 'null'
      cellEl.appendChild(badge)
      return
    }

    switch (col.columnType) {
      case 'boolean': {
        const badge = document.createElement('span')
        badge.className = raw ? 'pt-badge pt-badge-true' : 'pt-badge pt-badge-false'
        badge.textContent = raw ? 'Yes' : 'No'
        cellEl.appendChild(badge)
        break
      }
      case 'timestamp': {
        cellEl.textContent = str
        cellEl.classList.add('pt-cell-timestamp')
        break
      }
      default:
        cellEl.textContent = str
    }
  }

  // --- Render loop ---

  let lastScrollTop = -1
  let lastViewportHeight = -1
  let scheduledRaf: number | null = null

  function scheduleRender() {
    if (scheduledRaf !== null) return
    scheduledRaf = requestAnimationFrame(() => {
      scheduledRaf = null
      render()
    })
  }

  function render() {
    const t0 = performance.now()

    if (heightsDirty) {
      recomputeAllHeights()
      scrollContent.style.height = totalHeight + 'px'
    }

    if (filteredCount === 0) {
      emptyEl.style.display = ''
      scrollContent.style.display = 'none'
      return
    }
    emptyEl.style.display = 'none'
    scrollContent.style.display = ''

    const scrollTop = viewport.scrollTop
    const viewportH = viewport.clientHeight

    const first = Math.max(0, rowAtOffset(scrollTop) - OVERSCAN)
    const lastY = scrollTop + viewportH
    let last = rowAtOffset(lastY) + OVERSCAN
    if (last >= filteredCount) last = filteredCount - 1

    for (const pr of pool) {
      if (pr.assignedRow !== -1 && (pr.assignedRow < first || pr.assignedRow > last)) {
        pr.el.style.display = 'none'
        pr.assignedRow = -1
      }
    }

    let lazyPrepared = false
    for (let r = first; r <= last; r++) {
      const dataRow = viewIndices[r]

      // Lazy-prepare cells on first visibility
      if (!cellCaches[dataRow]) {
        cellCaches[dataRow] = prepareCellRow(dataRow)
        // Recompute this row's height now that we have real measurements
        rowHeights[r] = computeRowHeight(r)
        lazyPrepared = true
      }

      let existing = false
      for (const pr of pool) {
        if (pr.assignedRow === r) { existing = true; break }
      }
      if (existing) continue

      const pr = getPooledRow()
      pr.assignedRow = r
      pr.el.style.display = ''
      pr.el.style.transform = `translateY(${rowPositions[r]}px)`
      pr.el.style.height = rowHeights[r] + 'px'

      if (r % 2 === 1) pr.el.classList.add('pt-row-alt')
      else pr.el.classList.remove('pt-row-alt')

      for (let c = 0; c < columns.length; c++) {
        renderCell(pr.cells[c], dataRow, c)
        pr.cells[c].style.width = colWidths[c] + 'px'
      }
    }

    // If we lazy-prepared any rows, positions shifted — recycle everything
    // and re-render with corrected positions
    if (lazyPrepared) {
      rebuildPositions()
      scrollContent.style.height = totalHeight + 'px'
      // Reset all assignments so the next pass positions them correctly
      for (const pr of pool) {
        pr.assignedRow = -1
        pr.el.style.display = 'none'
      }
      // Re-assign with corrected positions (no more lazy-prep this pass since we just did it)
      for (let r = first; r <= last; r++) {
        const dataRow = viewIndices[r]
        const pr = getPooledRow()
        pr.assignedRow = r
        pr.el.style.display = ''
        pr.el.style.transform = `translateY(${rowPositions[r]}px)`
        pr.el.style.height = rowHeights[r] + 'px'
        if (r % 2 === 1) pr.el.classList.add('pt-row-alt')
        else pr.el.classList.remove('pt-row-alt')
        for (let c = 0; c < columns.length; c++) {
          renderCell(pr.cells[c], dataRow, c)
          pr.cells[c].style.width = colWidths[c] + 'px'
        }
      }
    }

    if (lastScrollTop === scrollTop && lastViewportHeight === viewportH) {
      for (const pr of pool) {
        if (pr.assignedRow === -1) continue
        for (let c = 0; c < columns.length; c++) {
          pr.cells[c].style.width = colWidths[c] + 'px'
        }
        pr.el.style.height = rowHeights[pr.assignedRow] + 'px'
        pr.el.style.transform = `translateY(${rowPositions[pr.assignedRow]}px)`
      }
    }

    if (first !== lastVisFirst || last !== lastVisLast) {
      lastVisFirst = first
      lastVisLast = last
      updateVisibleOverlays(first, Math.min(last, rowCount - 1))
    }

    lastScrollTop = scrollTop
    lastViewportHeight = viewportH

    const elapsed = performance.now() - t0
    const rangeStr = `showing ${first}–${Math.min(last, filteredCount - 1)}`
    const domStr = `${pool.filter(p => p.assignedRow !== -1).length} DOM rows`
    const frameStr = `${elapsed.toFixed(1)}ms frame`
    prevRange = updateStat(statRange, rangeStr, prevRange)
    prevDom = updateStat(statDom, domStr, prevDom)
    prevFrame = updateStat(statFrame, frameStr, prevFrame)
  }

  // --- Scroll handler ---

  function onScroll() {
    headerEl.scrollLeft = viewport.scrollLeft
    scheduleRender()
  }

  function onHeaderWheel(e: WheelEvent) {
    viewport.scrollTop += e.deltaY
    viewport.scrollLeft += e.deltaX
  }

  viewport.addEventListener('scroll', onScroll, { passive: true })
  headerEl.addEventListener('wheel', onHeaderWheel, { passive: true })
  window.addEventListener('resize', scheduleRender)

  // --- Column resize ---

  function onResizeStart(e: PointerEvent, colIndex: number) {
    e.preventDefault()
    e.stopPropagation()
    const handle = e.currentTarget as HTMLDivElement
    handle.classList.add('dragging')
    handle.setPointerCapture(e.pointerId)
    const startX = e.clientX
    const startWidth = colWidths[colIndex]

    const onMove = (ev: PointerEvent) => {
      const delta = ev.clientX - startX
      colWidths[colIndex] = Math.max(MIN_COL_WIDTH, startWidth + delta)
      const ths = headerRowEl.children as HTMLCollectionOf<HTMLDivElement>
      ths[colIndex].style.width = colWidths[colIndex] + 'px'
      renderSummary(colIndex)
      updateScrollContentWidth()
      heightsDirty = true
      scheduleRender()
    }

    const onUp = () => {
      handle.classList.remove('dragging')
      handle.removeEventListener('pointermove', onMove)
      handle.removeEventListener('pointerup', onUp)
    }

    handle.addEventListener('pointermove', onMove)
    handle.addEventListener('pointerup', onUp)
  }

  // --- Sort ---

  function onSortClick(col: number) {
    if (sortState && sortState.col === col) {
      if (sortState.dir === 'asc') sortState = { col, dir: 'desc' }
      else sortState = null
    } else {
      sortState = { col, dir: 'asc' }
    }

    updateSortUI()
    applyFilterAndSort()
    // Reset vertical scroll but preserve horizontal position
    viewport.scrollTop = 0
    for (const pr of pool) {
      pr.assignedRow = -1
      pr.el.style.display = 'none'
    }
    scheduleRender()
    notifyChange()
  }

  function updateSortUI() {
    const ths = headerRowEl.children as HTMLCollectionOf<HTMLDivElement>
    for (let c = 0; c < columns.length; c++) {
      const arrow = ths[c].querySelector('.pt-sort-arrow')
      if (!arrow) continue
      if (sortState && sortState.col === c) {
        arrow.textContent = sortState.dir === 'asc' ? ' ↑' : ' ↓'
        ths[c].setAttribute('aria-sort', sortState.dir === 'asc' ? 'ascending' : 'descending')
      } else {
        arrow.textContent = ''
        ths[c].removeAttribute('aria-sort')
      }
    }
  }

  // --- Batch append handler ---

  function onBatchAppended() {
    const newRowCount = data.rowCount
    growBuffers(newRowCount)

    // Cells will be lazy-prepared when they enter the viewport

    rowCount = newRowCount
    // Re-filter and re-sort with new data
    applyFilterAndSort()

    // Save the latest unfiltered summaries
    unfilteredSummaries = [...data.columnSummaries]

    // If filters are active, recompute from filtered rows
    if (hasActiveFilters()) {
      recomputeFilteredSummaries()
    }

    // Update stats and summaries
    updateRowCountDisplay()
    renderAllSummaries()

    heightsDirty = true
    // Force visible rows to refresh
    for (const pr of pool) {
      pr.assignedRow = -1
      pr.el.style.display = 'none'
    }
    lastVisFirst = -1
    lastVisLast = -1
    scheduleRender()
  }

  // --- Boot ---

  document.fonts.ready.then(() => {
    for (let r = 0; r < rowCount; r++) {
      const cache = cellCaches[r]
      if (!cache) continue
      for (let c = 0; c < columns.length; c++) {
        cache[c].prepared = prepare(data.getCell(r, c), FONT)
        cache[c].lastWidth = -1
      }
    }
    heightsDirty = true
    scheduleRender()
  })

  scheduleRender()

  // --- Keyboard navigation ---

  container.tabIndex = 0
  container.style.outline = 'none'

  function onKeyDown(e: KeyboardEvent) {
    const oneRow = LINE_HEIGHT + CELL_PAD_V
    const pageH = viewport.clientHeight

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault()
        viewport.scrollTop += oneRow
        break
      case 'ArrowUp':
        e.preventDefault()
        viewport.scrollTop -= oneRow
        break
      case 'PageDown':
        e.preventDefault()
        viewport.scrollTop += pageH
        break
      case 'PageUp':
        e.preventDefault()
        viewport.scrollTop -= pageH
        break
      case 'Home':
        e.preventDefault()
        viewport.scrollTop = 0
        break
      case 'End':
        e.preventDefault()
        viewport.scrollTop = viewport.scrollHeight
        break
      case 'Escape':
        if (hasActiveFilters()) {
          e.preventDefault()
          clearAllFilters()
        }
        break
    }
  }

  container.addEventListener('keydown', onKeyDown)

  // --- Destroy ---

  function destroy() {
    // Cancel pending render
    if (scheduledRaf !== null) {
      cancelAnimationFrame(scheduledRaf)
      scheduledRaf = null
    }

    // Remove event listeners
    viewport.removeEventListener('scroll', onScroll)
    headerEl.removeEventListener('wheel', onHeaderWheel)
    container.removeEventListener('keydown', onKeyDown)
    window.removeEventListener('resize', scheduleRender)
    document.removeEventListener('fullscreenchange', onFullscreenChange)

    // Unmount React roots
    for (const el of summaryContainers) {
      unmountColumnSummary(el)
    }

    // Clear DOM
    container.innerHTML = ''
    container.classList.remove('pt-table-container')
  }

  // --- Filter API ---

  function recomputeFilteredSummaries() {
    if (!hasActiveFilters()) {
      // Restore unfiltered summaries
      data.columnSummaries = [...unfilteredSummaries]
      return
    }

    // Build fresh accumulators from the filtered row set
    for (let c = 0; c < columns.length; c++) {
      const colType = columns[c].columnType
      // Temporary string column for categorical accumulator
      const filteredStrings: string[] = []
      const filteredRaw: unknown[] = []

      for (let i = 0; i < filteredCount; i++) {
        const dataRow = viewIndices[i]
        filteredRaw.push(data.getCellRaw(dataRow, c))
        if (colType === 'categorical') {
          filteredStrings.push(data.getCell(dataRow, c))
        }
      }

      let acc
      switch (colType) {
        case 'numeric': acc = new NumericAccumulator(); break
        case 'timestamp': acc = new TimestampAccumulator(); break
        case 'boolean': acc = new BooleanAccumulator(); break
        case 'categorical': acc = new CategoricalAccumulator(filteredStrings); break
      }
      acc.add(filteredRaw, 0, filteredCount)
      data.columnSummaries[c] = acc.snapshot(filteredCount)
    }
  }

  function filterLabel(f: ColumnFilter): string {
    if (!f) return ''
    switch (f.kind) {
      case 'range': return `Filtering to ${f.min.toFixed(0)}–${f.max.toFixed(0)}`
      case 'set': {
        const vals = [...f.values]
        if (vals.length === 1) return vals[0]
        return `Filtering by ${vals.length} values`
      }
      case 'boolean': return `Filtering to ${f.value ? 'Yes' : 'No'}`
    }
  }

  function hiddenLabel(c: number): string | null {
    const full = unfilteredSummaries[c]
    const filtered = data.columnSummaries[c]
    if (!full || !filtered) return null

    if (full.kind === 'categorical' && filtered.kind === 'categorical') {
      const hidden = full.uniqueCount - filtered.uniqueCount
      if (hidden > 0) return `${hidden} values hidden`
      return null
    }
    if (full.kind === 'boolean' && filtered.kind === 'boolean') {
      const anyHidden =
        (full.trueCount > 0 && filtered.trueCount === 0) ||
        (full.falseCount > 0 && filtered.falseCount === 0) ||
        (full.nullCount > 0 && filtered.nullCount === 0)
      return anyHidden ? 'Some values hidden' : null
    }
    // Numeric/timestamp — handled via asterisk on label
    return null
  }

  function updateFilteredLabels() {
    const active = hasActiveFilters()
    const ths = headerRowEl.children as HTMLCollectionOf<HTMLDivElement>
    for (let c = 0; c < columns.length; c++) {
      const th = ths[c]
      const label = th.querySelector('.pt-th-label') as HTMLElement

      // Remove existing filter line
      const existing = th.querySelector('.pt-filter-line')
      if (existing) existing.remove()

      const f = filters[c]
      const colType = columns[c].columnType
      const isNumericType = colType === 'numeric' || colType === 'timestamp'

      // Reset label
      if (label) {
        label.style.color = f ? 'var(--accent)' : ''
        // Asterisk on numeric/timestamp columns when others are filtered
        if (!f && active && isNumericType) {
          label.textContent = columns[c].label + ' *'
        } else {
          label.textContent = columns[c].label
        }
      }

      if (active) {
        const parts: string[] = []
        if (f) parts.push(filterLabel(f))
        const hidden = hiddenLabel(c)
        if (hidden) parts.push(hidden)

        if (parts.length > 0) {
          const line = document.createElement('div')
          line.className = 'pt-filter-line'
          line.textContent = parts.join(' · ')
          th.appendChild(line)
        }

        // Detailed hover tooltip showing which filters affect this column
        if (!f) {
          const activeFilters: string[] = []
          for (let i = 0; i < columns.length; i++) {
            if (filters[i]) {
              activeFilters.push(`${columns[i].label}: ${filterLabel(filters[i])}`)
            }
          }
          th.title = `Values filtered by ${activeFilters.join(', ')}`
        } else {
          th.title = ''
        }
      } else {
        th.title = ''
      }
    }
  }

  function onFilterChanged() {
    applyFilterAndSort()
    recomputeFilteredSummaries()
    updateRowCountDisplay()
    updateFilteredLabels()
    rebuildFilterPills()
    renderAllSummaries()
    viewport.scrollTop = 0
    for (const pr of pool) {
      pr.assignedRow = -1
      pr.el.style.display = 'none'
    }
    lastVisFirst = -1
    lastVisLast = -1
    scheduleRender()
    notifyChange()
  }

  function setFilter(colIndex: number, filter: ColumnFilter) {
    filters[colIndex] = filter
    onFilterChanged()
  }

  function clearFilter(colIndex: number) {
    filters[colIndex] = null
    onFilterChanged()
  }

  function clearAllFilters() {
    for (let i = 0; i < filters.length; i++) filters[i] = null
    onFilterChanged()
  }

  // --- State getters ---

  function getSort(): TableEngineState['sort'] {
    if (!sortState) return null
    return { column: columns[sortState.col].key, direction: sortState.dir }
  }

  function setSortByName(column: string, direction: 'asc' | 'desc') {
    const colIndex = columns.findIndex(c => c.key === column)
    if (colIndex === -1) return
    sortState = { col: colIndex, dir: direction }

    updateSortUI()
    applyFilterAndSort()
    viewport.scrollTop = 0
    for (const pr of pool) {
      pr.assignedRow = -1
      pr.el.style.display = 'none'
    }
    scheduleRender()
  }

  function getFilters(): TableEngineState['filters'] {
    const result: TableEngineState['filters'] = []
    for (let i = 0; i < columns.length; i++) {
      if (filters[i]) {
        result.push({ column: columns[i].key, filter: filters[i] })
      }
    }
    return result
  }

  function getState(): TableEngineState {
    return {
      sort: getSort(),
      filters: getFilters(),
      filteredCount,
      totalCount: rowCount,
    }
  }

  function notifyChange() {
    options?.onChange?.(getState())
  }

  return {
    onBatchAppended, destroy,
    setFilter, clearFilter, clearAllFilters,
    getSort, setSort: setSortByName, getFilters, getState,
  }
}
