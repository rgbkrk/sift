import { prepare, layout, type PreparedText } from '@chenglou/pretext'
import { animationFrameScheduler, interval, Subject, withLatestFrom, map, scan } from 'rxjs'
import { renderColumnSummary, unmountColumnSummary } from './sparkline'
import { mountColumnMenu, unmountColumnMenu, type ColumnAction } from './column-menu'
import { fitColumnWidths } from './auto-width'
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
  /** Number of distinct finite values seen (capped at tracking limit). */
  uniqueCount?: number
  /** True if this column looks like an index/ID (suppress histogram). */
  isIndex?: boolean
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
  /** Optional: prefetch visible rows in batch (WASM viewport optimization). */
  prefetchViewport?: (dataRowIndices: number[]) => void
  /** Optional: cast a column to a different type (WASM type override). */
  castColumn?: (colIndex: number, targetType: ColumnType) => void
  /** Optional: recompute all column summaries (e.g. after a cast changes the data). */
  recomputeSummaries?: () => void
  /** Optional: return sorted row indices for a column (WASM sort optimization). */
  sortColumn?: (colIndex: number, ascending: boolean) => Uint32Array
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
  /** Signal that all batches have been loaded and streaming is complete. */
  setStreamingDone(): void
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

  // Pinned columns + visual ordering
  const pinnedColumns = new Set<number>([0]) // first column pinned by default
  // Visual column order: pinned columns first, then the rest
  let visualOrder: number[] = computeVisualOrder()

  function computeVisualOrder(): number[] {
    const pinned = [...pinnedColumns].sort((a, b) => a - b)
    const unpinned = columns.map((_, i) => i).filter(i => !pinnedColumns.has(i))
    return [...pinned, ...unpinned]
  }

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

  // Column widths — header-based floor, refined by sampled cell data
  const colWidths = columns.map(c => c.width)
  fitColumnWidths(data, colWidths)

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
      if (data.sortColumn) {
        // WASM fast path: get sorted indices for all rows, then intersect with filter
        const sortedAll = data.sortColumn(col, dir === 'asc')
        if (hasActiveFilters()) {
          const filterSet = new Set(filtered)
          filtered.length = 0
          for (let i = 0; i < sortedAll.length; i++) {
            if (filterSet.has(sortedAll[i])) filtered.push(sortedAll[i])
          }
        } else {
          filtered.length = 0
          for (let i = 0; i < sortedAll.length; i++) {
            filtered.push(sortedAll[i])
          }
        }
      } else {
        // JS fallback: sort the filtered set with comparators
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
            if (rawA == null && rawB == null) cmp = 0
            else if (rawA == null) return 1
            else if (rawB == null) return -1
            else cmp = (rawA ? 1 : 0) - (rawB ? 1 : 0)
          } else {
            const rawA = data.getCellRaw(a, col)
            const rawB = data.getCellRaw(b, col)
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
  container.setAttribute('aria-rowcount', String(rowCount))
  container.setAttribute('aria-colcount', String(columns.length))

  // Streaming state — progress bar is appended after stats bar below
  let streaming = true
  const progressBar = document.createElement('div')
  progressBar.className = 'pt-progress-bar'
  progressBar.innerHTML = '<div class="pt-progress-bar-fill"></div>'

  // Header — lives inside the scroll content so it scrolls
  // horizontally with the data. position: sticky keeps it at top.
  const headerEl = document.createElement('div')
  headerEl.className = 'pt-header'

  const headerRowEl = document.createElement('div')
  headerRowEl.className = 'pt-header-row'
  headerRowEl.setAttribute('role', 'row')

  const summaryContainers: HTMLDivElement[] = []

  for (let c = 0; c < columns.length; c++) {
    const th = document.createElement('div')
    th.className = 'pt-th'
    th.setAttribute('role', 'columnheader')
    th.setAttribute('aria-colindex', String(c + 1))
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

    const typeIcon = document.createElement('span')
    typeIcon.className = 'pt-type-icon'
    typeIcon.textContent = columns[c].columnType === 'numeric' ? '#'
      : columns[c].columnType === 'boolean' ? '◉'
      : columns[c].columnType === 'timestamp' ? '◷'
      : 'Aa'
    typeIcon.title = columns[c].columnType
    th.appendChild(typeIcon)

    const summaryEl = document.createElement('div')
    summaryEl.className = 'pt-th-summary'
    // Prevent clicks on summary charts (filter interactions) from triggering sort
    summaryEl.addEventListener('click', (e) => e.stopPropagation())
    th.appendChild(summaryEl)
    summaryContainers.push(summaryEl)

    // Keyboard shortcuts on column headers
    th.setAttribute('tabindex', '0')
    th.addEventListener('keydown', (e) => {
      if (e.key === 'p' || e.key === 'P') {
        e.preventDefault()
        const action: ColumnAction = pinnedColumns.has(c) ? { kind: 'unpin' } : { kind: 'pin' }
        handleColumnAction(c, action)
      } else if (e.key === 'Enter' && columns[c].sortable) {
        e.preventDefault()
        onSortClick(c)
      } else if (e.key === 'ArrowRight') {
        e.preventDefault()
        const ths = Array.from(headerRowEl.children) as HTMLDivElement[]
        const curVi = ths.indexOf(th)
        if (curVi < ths.length - 1) (ths[curVi + 1] as HTMLElement).focus()
      } else if (e.key === 'ArrowLeft') {
        e.preventDefault()
        const ths = Array.from(headerRowEl.children) as HTMLDivElement[]
        const curVi = ths.indexOf(th)
        if (curVi > 0) (ths[curVi - 1] as HTMLElement).focus()
      }
    })

    // Context menu: right-click (desktop) + long-press (mobile)
    function openColumnMenu(x: number, y: number) {
      mountColumnMenu(
        {
          colIndex: c,
          colName: columns[c].label,
          colType: columns[c].columnType,
          isPinned: pinnedColumns.has(c),
          sortDirection: sortState?.col === c ? sortState.dir : null,
          x, y,
        },
        handleColumnAction,
      )
    }

    th.addEventListener('contextmenu', (e) => {
      e.preventDefault()
      openColumnMenu(e.clientX, e.clientY)
    })

    // Long-press for touch devices (500ms threshold)
    let longPressTimer: ReturnType<typeof setTimeout> | null = null
    th.addEventListener('touchstart', (e) => {
      const touch = e.touches[0]
      const startX = touch.clientX
      const startY = touch.clientY
      longPressTimer = setTimeout(() => {
        longPressTimer = null
        openColumnMenu(startX, startY)
      }, 500)
    }, { passive: true })
    th.addEventListener('touchmove', () => {
      if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null }
    }, { passive: true })
    th.addEventListener('touchend', () => {
      if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null }
    })

    if (c < columns.length - 1) {
      const handle = document.createElement('div')
      handle.className = 'pt-resize-handle'
      handle.addEventListener('pointerdown', (e) => onResizeStart(e, c))
      th.appendChild(handle)
    }

    headerRowEl.appendChild(th)
  }

  headerEl.appendChild(headerRowEl)
  // Header is appended to scroll content (not container) so it scrolls
  // horizontally with data. position: sticky; top: 0 keeps it visible.

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

  scrollContent.appendChild(headerEl) // Header inside scroll content for natural H scroll
  scrollContent.appendChild(rowPool)

  // Empty state (shown when filters exclude all rows)
  const emptyEl = document.createElement('div')
  emptyEl.className = 'pt-empty-state'
  emptyEl.style.display = 'none'

  const emptyText = document.createElement('div')
  emptyText.className = 'pt-empty-text'
  emptyText.textContent = 'No matching rows'

  const emptyClearBtn = document.createElement('button')
  emptyClearBtn.className = 'pt-empty-clear'
  emptyClearBtn.textContent = 'Clear all filters'
  emptyClearBtn.addEventListener('click', () => clearAllFilters())

  emptyEl.appendChild(emptyText)
  emptyEl.appendChild(emptyClearBtn)

  // Empty state goes inside scroll content (after header, before row pool)
  scrollContent.appendChild(emptyEl)

  viewport.appendChild(scrollContent)
  container.appendChild(viewport)

  // Stats bar
  const statsEl = document.createElement('div')
  statsEl.className = 'pt-stats'

  // ARIA live region for screen reader announcements (filter changes, streaming)
  const ariaLive = document.createElement('div')
  ariaLive.setAttribute('aria-live', 'polite')
  ariaLive.setAttribute('role', 'status')
  ariaLive.className = 'sr-only'
  ariaLive.style.cssText = 'position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)'
  container.appendChild(ariaLive)

  function makeStatSpan(className: string): HTMLSpanElement {
    const el = document.createElement('span')
    el.className = `pt-stat-value ${className}`
    return el
  }

  // Status indicator (streaming dot → checkmark)
  const statusIndicator = document.createElement('span')
  statusIndicator.className = 'pt-status-indicator pt-status-streaming'
  statusIndicator.textContent = '●'
  statusIndicator.title = 'Loading data…'

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
    // Keep ARIA row count in sync
    container.setAttribute('aria-rowcount', String(filteredCount + 1)) // +1 for header row
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

  statsEl.append(statusIndicator, statRows, sep(), statRange, sep(), statDom, sep(), statFrame, filterPillsEl, statsSpacer, fullscreenBtn)
  container.appendChild(statsEl)

  // Streaming progress bar — at the bottom of the table, below the stats bar
  container.appendChild(progressBar)

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

  let prevRange = '', prevDom = '', prevFps = ''

  // RxJS FPS: frame counter on animationFrameScheduler, render cost from Subject
  const renderCost$ = new Subject<number>() // receives elapsed ms per render
  const fps$ = interval(0, animationFrameScheduler).pipe(
    // Each emission = one display frame. Pair with timestamps for FPS.
    scan<number, { prevTime: number; deltas: number[] }>((state, _) => {
      const now = performance.now()
      if (state.prevTime > 0) {
        const delta = now - state.prevTime
        const deltas = [...state.deltas, delta].slice(-30)
        return { prevTime: now, deltas }
      }
      return { prevTime: now, deltas: [] }
    }, { prevTime: 0, deltas: [] }),
    map(({ deltas }) => {
      if (deltas.length === 0) return '– fps'
      const avg = deltas.reduce((a, b) => a + b, 0) / deltas.length
      return `${Math.round(1000 / avg)} fps`
    }),
  )
  const fpsSub = fps$.pipe(
    withLatestFrom(renderCost$),
  ).subscribe(([fpsStr, cost]) => {
    const str = `${fpsStr}·${cost.toFixed(1)}ms`
    prevFps = updateStat(statFrame, str, prevFps)
  })

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
    el.setAttribute('role', 'row')
    const cells: HTMLDivElement[] = []
    // Create cells in data order (cells[c] = column c)
    for (let c = 0; c < columns.length; c++) {
      const cell = document.createElement('div')
      cell.className = 'pt-cell'
      cell.setAttribute('role', 'gridcell')
      cell.setAttribute('aria-colindex', String(c + 1))
      cell.style.width = colWidths[c] + 'px'
      cells.push(cell)
    }
    // Append in visual order
    for (const c of visualOrder) {
      el.appendChild(cells[c])
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
      // Account for header height — row pool is absolute inside scroll-content
      const headerH = headerEl.offsetHeight
      rowPool.style.top = headerH + 'px'
      scrollContent.style.height = (totalHeight + headerH) + 'px'
    }

    if (filteredCount === 0) {
      emptyEl.style.display = ''
      rowPool.style.display = 'none'
      return
    }
    emptyEl.style.display = 'none'
    rowPool.style.display = ''

    const headerH = headerEl.offsetHeight
    const scrollTop = Math.max(0, viewport.scrollTop - headerH)
    const viewportH = viewport.clientHeight

    const first = Math.max(0, rowAtOffset(scrollTop) - OVERSCAN)
    const lastY = scrollTop + viewportH
    let last = rowAtOffset(lastY) + OVERSCAN
    if (last >= filteredCount) last = filteredCount - 1

    // Prefetch visible rows in batch (WASM viewport optimization)
    if (data.prefetchViewport) {
      const visibleDataRows: number[] = []
      for (let r = first; r <= last; r++) {
        visibleDataRows.push(viewIndices[r])
      }
      data.prefetchViewport(visibleDataRows)
    }

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
      pr.el.setAttribute('aria-rowindex', String(r + 2)) // 1-based, header is row 1

      if (r % 2 === 1) pr.el.classList.add('pt-row-alt')
      else pr.el.classList.remove('pt-row-alt')

      for (let c = 0; c < columns.length; c++) {
        renderCell(pr.cells[c], dataRow, c)
        pr.cells[c].style.width = colWidths[c] + 'px'
        applyCellPinStyle(pr.cells[c], c)
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
        pr.el.setAttribute('aria-rowindex', String(r + 2))
        if (r % 2 === 1) pr.el.classList.add('pt-row-alt')
        else pr.el.classList.remove('pt-row-alt')
        for (let c = 0; c < columns.length; c++) {
          renderCell(pr.cells[c], dataRow, c)
          pr.cells[c].style.width = colWidths[c] + 'px'
          applyCellPinStyle(pr.cells[c], c)
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

    // Emit render cost — FPS display handled by RxJS observable
    renderCost$.next(elapsed)

    prevRange = updateStat(statRange, rangeStr, prevRange)
    prevDom = updateStat(statDom, domStr, prevDom)
  }

  // --- Scroll handler ---

  function onScroll() {
    // Header scrolls naturally with content (it's inside scroll-content)
    scheduleRender()
  }


  viewport.addEventListener('scroll', onScroll, { passive: true })

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

  function setStreamingDone() {
    if (!streaming) return
    streaming = false
    progressBar.classList.add('pt-progress-bar-done')
    updateRowCountDisplay()
    // Switch status indicator from streaming dot to checkmark
    statusIndicator.classList.remove('pt-status-streaming')
    statusIndicator.classList.add('pt-status-ready')
    statusIndicator.textContent = '✓'
    statusIndicator.title = 'All data loaded'
    // Remove the progress bar from DOM after fade-out transition
    progressBar.addEventListener('transitionend', () => progressBar.remove(), { once: true })
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

  // Watch for header height changes (e.g. when React summary charts mount async)
  // so rowPool.style.top stays in sync
  let headerResizeObserver: ResizeObserver | null = null
  if (typeof ResizeObserver !== 'undefined') {
    headerResizeObserver = new ResizeObserver(() => {
      heightsDirty = true
      scheduleRender()
    })
    headerResizeObserver.observe(headerEl)
  }

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
    // Cancel pending render + FPS observable
    if (scheduledRaf !== null) {
      cancelAnimationFrame(scheduledRaf)
      scheduledRaf = null
    }
    fpsSub.unsubscribe()

    // Remove event listeners and observers
    headerResizeObserver?.disconnect()
    viewport.removeEventListener('scroll', onScroll)

    container.removeEventListener('keydown', onKeyDown)
    window.removeEventListener('resize', scheduleRender)
    document.removeEventListener('fullscreenchange', onFullscreenChange)

    // Unmount React roots
    for (const el of summaryContainers) {
      unmountColumnSummary(el)
    }
    unmountColumnMenu()

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

  function filterLabel(f: ColumnFilter, colIndex?: number): string {
    if (!f) return ''
    switch (f.kind) {
      case 'range': {
        if (colIndex !== undefined && columns[colIndex].columnType === 'timestamp') {
          const fmt = (v: number) => new Date(v).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: '2-digit' })
          return `${fmt(f.min)} – ${fmt(f.max)}`
        }
        return `${f.min.toLocaleString(undefined, { maximumFractionDigits: 1 })} – ${f.max.toLocaleString(undefined, { maximumFractionDigits: 1 })}`
      }
      case 'set': {
        const vals = [...f.values]
        if (vals.length === 1) return vals[0]
        return `${vals.length} values`
      }
      case 'boolean': return f.value ? 'Yes' : 'No'
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
    // Numeric/timestamp — check if range narrowed
    if ((full.kind === 'numeric' && filtered.kind === 'numeric') ||
        (full.kind === 'timestamp' && filtered.kind === 'timestamp')) {
      if (full.min !== filtered.min || full.max !== filtered.max) {
        return 'values hidden'
      }
    }
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

      if (label) {
        label.style.color = f ? 'var(--accent)' : ''
        label.textContent = columns[c].label
      }

      if (active) {
        const parts: string[] = []
        if (f) parts.push(filterLabel(f, c))
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
              activeFilters.push(`${columns[i].label}: ${filterLabel(filters[i], i)}`)
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
    // Announce to screen readers
    ariaLive.textContent = hasActiveFilters()
      ? `Filtered to ${filteredCount.toLocaleString()} of ${rowCount.toLocaleString()} rows`
      : `${rowCount.toLocaleString()} rows`
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

  // --- Column context menu action handler ---

  function handleColumnAction(colIndex: number, action: ColumnAction) {
    switch (action.kind) {
      case 'sort':
        if (sortState?.col === colIndex && sortState.dir === action.direction) {
          sortState = null // toggle off
        } else {
          sortState = { col: colIndex, dir: action.direction }
        }
        updateSortUI()
        applyFilterAndSort()
        viewport.scrollTop = 0
        for (const pr of pool) { pr.assignedRow = -1; pr.el.style.display = 'none' }
        scheduleRender()
        notifyChange()
        break

      case 'pin':
        pinnedColumns.add(colIndex)
        visualOrder = computeVisualOrder()
        reorderColumns()
        updatePinnedStyles()
        break

      case 'unpin':
        pinnedColumns.delete(colIndex)
        visualOrder = computeVisualOrder()
        reorderColumns()
        updatePinnedStyles()
        break

      case 'cast':
        if (data.castColumn) {
          data.castColumn(colIndex, action.targetType)
          // Update the column metadata
          columns[colIndex].columnType = action.targetType
          columns[colIndex].numeric = action.targetType === 'numeric'
          // Update type icon
          const ths = headerRowEl.children as HTMLCollectionOf<HTMLDivElement>
          const icon = ths[colIndex].querySelector('.pt-type-icon')
          if (icon) {
            icon.textContent = action.targetType === 'numeric' ? '#'
              : action.targetType === 'boolean' ? '◉'
              : action.targetType === 'timestamp' ? '◷' : 'Aa'
            icon.setAttribute('title', action.targetType)
          }
          // Recompute summaries from source (WASM or accumulators)
          if (data.recomputeSummaries) data.recomputeSummaries()
          unfilteredSummaries = [...data.columnSummaries]
          if (hasActiveFilters()) recomputeFilteredSummaries()
          renderAllSummaries()
          heightsDirty = true
          for (const pr of pool) { pr.assignedRow = -1; pr.el.style.display = 'none' }
          scheduleRender()
        }
        break
    }
  }

  // Precompute cumulative left offsets for pinned columns
  let pinnedLeftOffsets: number[] = []
  function recomputePinnedOffsets() {
    pinnedLeftOffsets = new Array(columns.length).fill(-1)
    let cumLeft = 0
    for (let c = 0; c < columns.length; c++) {
      if (pinnedColumns.has(c)) {
        pinnedLeftOffsets[c] = cumLeft
        cumLeft += colWidths[c]
      }
    }
  }
  recomputePinnedOffsets()
  // Apply initial pinned styles to header THs (not just cells)
  updatePinnedStyles()

  function applyCellPinStyle(cell: HTMLElement, colIndex: number) {
    if (pinnedColumns.has(colIndex)) {
      cell.style.position = 'sticky'
      cell.style.left = pinnedLeftOffsets[colIndex] + 'px'
      cell.style.zIndex = '1'
      cell.style.background = 'var(--panel)'
      cell.style.boxShadow = '2px 0 4px var(--pin-shadow)'
    } else {
      cell.style.position = ''
      cell.style.left = ''
      cell.style.zIndex = ''
      cell.style.background = ''
      cell.style.boxShadow = ''
    }
  }

  function reorderColumns() {
    // Reorder header TH elements to match visualOrder
    const ths = Array.from(headerRowEl.children) as HTMLDivElement[]
    for (const colIdx of visualOrder) {
      headerRowEl.appendChild(ths[colIdx])
    }

    // Reorder cells in each pooled row
    for (const pr of pool) {
      for (const colIdx of visualOrder) {
        pr.el.appendChild(pr.cells[colIdx])
      }
    }

    // Force re-render to update positions
    heightsDirty = true
    for (const pr of pool) { pr.assignedRow = -1; pr.el.style.display = 'none' }
    scheduleRender()
  }

  function updatePinnedStyles() {
    recomputePinnedOffsets()
    // Find the last pinned visual index
    let lastPinnedVi = -1
    for (let vi = 0; vi < visualOrder.length; vi++) {
      if (pinnedColumns.has(visualOrder[vi])) lastPinnedVi = vi
    }
    // Iterate in visual order since DOM has been reordered
    const ths = Array.from(headerRowEl.children) as HTMLDivElement[]
    for (let vi = 0; vi < visualOrder.length; vi++) {
      const dataCol = visualOrder[vi]
      const th = ths[vi]
      const handle = th.querySelector('.pt-resize-handle') as HTMLElement | null
      if (pinnedColumns.has(dataCol)) {
        th.style.position = 'sticky'
        th.style.left = pinnedLeftOffsets[dataCol] + 'px'
        th.style.zIndex = '6'
        th.style.background = 'color-mix(in srgb, var(--panel) 90%, var(--page) 10%)'
        th.style.boxShadow = vi === lastPinnedVi ? '2px 0 4px var(--pin-shadow)' : ''
        // Hide resize bar on last pinned column (shadow provides the edge)
        if (handle) handle.style.opacity = vi === lastPinnedVi ? '0' : ''
      } else {
        th.style.position = ''
        th.style.left = ''
        th.style.zIndex = ''
        th.style.background = ''
        th.style.boxShadow = ''
        if (handle) handle.style.opacity = ''
      }
    }
  }

  return {
    onBatchAppended, setStreamingDone, destroy,
    setFilter, clearFilter, clearAllFilters,
    getSort, setSort: setSortByName, getFilters, getState,
  }
}
