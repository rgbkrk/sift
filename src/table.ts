import { prepare, layout, type PreparedText } from '@chenglou/pretext'
import { renderColumnSummary } from './sparkline'

// --- Types ---

export type Column = {
  key: string
  label: string
  width: number
  sortable: boolean
  numeric: boolean
}

export type NumericColumnSummary = {
  kind: 'numeric'
  min: number
  max: number
  bins: { x0: number; x1: number; count: number }[]
}

export type CategoricalColumnSummary = {
  kind: 'categorical'
  uniqueCount: number
  topCategories: { label: string; count: number; pct: number }[]
  othersCount: number
  othersPct: number
}

export type ColumnSummary = NumericColumnSummary | CategoricalColumnSummary | null

export type TableData = {
  columns: Column[]
  rowCount: number
  getCell: (row: number, col: number) => string
  getCellRaw: (row: number, col: number) => unknown
  columnSummaries: ColumnSummary[]
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

export function createTable(container: HTMLElement, data: TableData) {
  const { columns, rowCount } = data

  // Sort state
  let sortState: SortState = null
  let sortedIndices: Int32Array = new Int32Array(rowCount)
  for (let i = 0; i < rowCount; i++) sortedIndices[i] = i

  // Per-cell cache: prepared text + last laid-out width/height
  // Keyed by data row index (not sorted index) so cache survives re-sorts
  type CellCache = { prepared: PreparedText; lastWidth: number; lastHeight: number }
  const cellCaches: CellCache[][] = new Array(rowCount)
  for (let r = 0; r < rowCount; r++) {
    const row = new Array<CellCache>(columns.length)
    for (let c = 0; c < columns.length; c++) {
      row[c] = {
        prepared: prepare(data.getCell(r, c), FONT),
        lastWidth: -1,
        lastHeight: 0,
      }
    }
    cellCaches[r] = row
  }

  // Row height cache: sorted row index → height (including padding)
  const rowHeights = new Float64Array(rowCount)
  // Prefix sums for scroll position → row mapping
  const rowPositions = new Float64Array(rowCount + 1)
  let totalHeight = 0
  let heightsDirty = true

  // Column widths (mutable copy)
  const colWidths = columns.map(c => c.width)

  // --- Compute heights (only re-layouts cells whose width changed) ---

  function computeRowHeight(sortedRow: number): number {
    const dataRow = sortedIndices[sortedRow]
    let maxH = LINE_HEIGHT // minimum one line
    for (let c = 0; c < columns.length; c++) {
      const cell = cellCaches[dataRow][c]
      const cellW = Math.max(1, colWidths[c] - CELL_PAD_H)
      // Only re-layout if width actually changed
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
    for (let i = 0; i < rowCount; i++) {
      rowHeights[i] = computeRowHeight(i)
    }
    rebuildPositions()
    heightsDirty = false
  }

  function rebuildPositions() {
    rowPositions[0] = 0
    for (let i = 0; i < rowCount; i++) {
      rowPositions[i + 1] = rowPositions[i] + rowHeights[i]
    }
    totalHeight = rowPositions[rowCount]
  }

  // Binary search: scroll offset → first visible row
  function rowAtOffset(offset: number): number {
    let lo = 0, hi = rowCount
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (rowPositions[mid + 1] <= offset) lo = mid + 1
      else hi = mid
    }
    return lo
  }

  // --- Sort ---

  function applySort() {
    if (!sortState) {
      for (let i = 0; i < rowCount; i++) sortedIndices[i] = i
    } else {
      const { col, dir } = sortState
      const isNumeric = columns[col].numeric
      // Build index array and sort
      const indices = new Int32Array(rowCount)
      for (let i = 0; i < rowCount; i++) indices[i] = i
      indices.sort((a, b) => {
        let cmp: number
        if (isNumeric) {
          const va = data.getCellRaw(a, col) as number
          const vb = data.getCellRaw(b, col) as number
          cmp = va - vb
        } else {
          const sa = data.getCell(a, col)
          const sb = data.getCell(b, col)
          cmp = sa < sb ? -1 : sa > sb ? 1 : 0
        }
        return dir === 'asc' ? cmp : -cmp
      })
      sortedIndices = indices
    }
    heightsDirty = true
  }

  // --- DOM ---

  container.innerHTML = ''
  container.classList.add('pt-table-container')

  // Header
  const headerEl = document.createElement('div')
  headerEl.className = 'pt-header'
  const headerRowEl = document.createElement('div')
  headerRowEl.className = 'pt-header-row'

  const summaryContainers: HTMLDivElement[] = []

  for (let c = 0; c < columns.length; c++) {
    const th = document.createElement('div')
    th.className = 'pt-th'
    th.style.width = colWidths[c] + 'px'
    th.dataset.col = String(c)

    // Top row: label + sort arrow
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
      th.style.cursor = 'pointer'
      th.addEventListener('click', () => onSortClick(c))
    }

    th.appendChild(topRow)

    // Summary chart
    const summaryEl = document.createElement('div')
    summaryEl.className = 'pt-th-summary'
    th.appendChild(summaryEl)
    summaryContainers.push(summaryEl)

    // Resize handle
    if (c < columns.length - 1) {
      const handle = document.createElement('div')
      handle.className = 'pt-resize-handle'
      handle.addEventListener('pointerdown', (e) => onResizeStart(e, c))
      th.appendChild(handle)
    }

    headerRowEl.appendChild(th)
  }

  // Render summary charts after header is in DOM
  headerEl.appendChild(headerRowEl)
  container.appendChild(headerEl)

  for (let c = 0; c < columns.length; c++) {
    const summary = data.columnSummaries[c]
    if (summary) {
      renderColumnSummary(summaryContainers[c], summary, colWidths[c] - CELL_PAD_H)
    }
  }

  // Scroll viewport
  const viewport = document.createElement('div')
  viewport.className = 'pt-viewport'

  const scrollContent = document.createElement('div')
  scrollContent.className = 'pt-scroll-content'

  const rowPool = document.createElement('div')
  rowPool.className = 'pt-row-pool'

  scrollContent.appendChild(rowPool)
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

  statRows.textContent = `${rowCount.toLocaleString()} rows`
  statsEl.append(statRows, sep(), statRange, sep(), statDom, sep(), statFrame)
  container.appendChild(statsEl)

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
      // Force reflow to restart animation
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
      if (!summary || summary.kind !== 'numeric') continue

      const bins = summary.bins
      const visibleBins = new Array<number>(bins.length).fill(0)
      const binWidth = (summary.max - summary.min) / bins.length || 1

      for (let r = visFirst; r <= visLast; r++) {
        const dataRow = sortedIndices[r]
        const v = data.getCellRaw(dataRow, c) as number
        let idx = Math.floor((v - summary.min) / binWidth)
        if (idx >= bins.length) idx = bins.length - 1
        if (idx < 0) idx = 0
        visibleBins[idx]++
      }

      renderColumnSummary(
        summaryContainers[c],
        summary,
        colWidths[c] - CELL_PAD_H,
        visibleBins,
      )
    }
  }

  // --- Row pool (recycle DOM nodes) ---

  type PooledRow = {
    el: HTMLDivElement
    cells: HTMLDivElement[]
    assignedRow: number // sorted row index, -1 if unused
  }

  const pool: PooledRow[] = []

  function getPooledRow(): PooledRow {
    for (const pr of pool) {
      if (pr.assignedRow === -1) return pr
    }
    // Create new row
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

    const scrollTop = viewport.scrollTop
    const viewportH = viewport.clientHeight

    // Find visible range
    const first = Math.max(0, rowAtOffset(scrollTop) - OVERSCAN)
    const lastY = scrollTop + viewportH
    let last = rowAtOffset(lastY) + OVERSCAN
    if (last >= rowCount) last = rowCount - 1

    // Recycle rows outside new range
    for (const pr of pool) {
      if (pr.assignedRow !== -1 && (pr.assignedRow < first || pr.assignedRow > last)) {
        pr.el.style.display = 'none'
        pr.assignedRow = -1
      }
    }

    // Assign rows in visible range
    for (let r = first; r <= last; r++) {
      const dataRow = sortedIndices[r]
      // Check if already assigned
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

      // Stripe
      if (r % 2 === 1) pr.el.classList.add('pt-row-alt')
      else pr.el.classList.remove('pt-row-alt')

      for (let c = 0; c < columns.length; c++) {
        const cell = pr.cells[c]
        cell.textContent = data.getCell(dataRow, c)
        cell.style.width = colWidths[c] + 'px'
      }
    }

    // Update column widths on existing visible rows if resizing
    if (lastScrollTop === scrollTop && lastViewportHeight === viewportH) {
      // Might be a resize-only render — update cell widths
      for (const pr of pool) {
        if (pr.assignedRow === -1) continue
        for (let c = 0; c < columns.length; c++) {
          pr.cells[c].style.width = colWidths[c] + 'px'
        }
        pr.el.style.height = rowHeights[pr.assignedRow] + 'px'
        pr.el.style.transform = `translateY(${rowPositions[pr.assignedRow]}px)`
      }
    }

    // Update header sparkline overlays when visible range changes
    if (first !== lastVisFirst || last !== lastVisLast) {
      lastVisFirst = first
      lastVisLast = last
      updateVisibleOverlays(first, Math.min(last, rowCount - 1))
    }

    lastScrollTop = scrollTop
    lastViewportHeight = viewportH

    const elapsed = performance.now() - t0
    const rangeStr = `showing ${first}–${Math.min(last, rowCount - 1)}`
    const domStr = `${pool.filter(p => p.assignedRow !== -1).length} DOM rows`
    const frameStr = `${elapsed.toFixed(1)}ms frame`
    prevRange = updateStat(statRange, rangeStr, prevRange)
    prevDom = updateStat(statDom, domStr, prevDom)
    prevFrame = updateStat(statFrame, frameStr, prevFrame)
  }

  // --- Scroll handler ---

  viewport.addEventListener('scroll', () => {
    // Sync header horizontal scroll with viewport
    headerEl.scrollLeft = viewport.scrollLeft
    scheduleRender()
  }, { passive: true })
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
      // Update header
      const ths = headerRowEl.children as HTMLCollectionOf<HTMLDivElement>
      ths[colIndex].style.width = colWidths[colIndex] + 'px'
      // Re-render summary chart at new width
      const summary = data.columnSummaries[colIndex]
      if (summary) {
        renderColumnSummary(
          summaryContainers[colIndex],
          summary,
          colWidths[colIndex] - CELL_PAD_H,
        )
      }
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

    // Update header arrows
    const ths = headerRowEl.children as HTMLCollectionOf<HTMLDivElement>
    for (let c = 0; c < columns.length; c++) {
      const arrow = ths[c].querySelector('.pt-sort-arrow')
      if (!arrow) continue
      if (sortState && sortState.col === c) {
        arrow.textContent = sortState.dir === 'asc' ? ' ↑' : ' ↓'
      } else {
        arrow.textContent = ''
      }
    }

    applySort()
    // Reset scroll and re-render
    viewport.scrollTop = 0
    for (const pr of pool) {
      pr.assignedRow = -1
      pr.el.style.display = 'none'
    }
    scheduleRender()
  }

  // --- Boot ---

  document.fonts.ready.then(() => {
    // Re-prepare all cells after fonts load for accurate widths
    for (let r = 0; r < rowCount; r++) {
      for (let c = 0; c < columns.length; c++) {
        const cell = cellCaches[r][c]
        cell.prepared = prepare(data.getCell(r, c), FONT)
        cell.lastWidth = -1 // force re-layout
      }
    }
    heightsDirty = true
    scheduleRender()
  })

  scheduleRender()
}
