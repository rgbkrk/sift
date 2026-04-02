import { createRoot, type Root } from 'react-dom/client'
import { createPortal } from 'react-dom'
import { useRef, useCallback, useState, useMemo, useEffect } from 'react'
import { BarChart } from 'semiotic/ordinal'
import type {
  NumericColumnSummary,
  CategoricalColumnSummary,
  BooleanColumnSummary,
  TimestampColumnSummary,
  CategoryEntry,
  ColumnFilter,
  RangeFilter,
} from './table'

type NonNullSummary =
  | NumericColumnSummary
  | CategoricalColumnSummary
  | BooleanColumnSummary
  | TimestampColumnSummary

type FilterCallback = (filter: ColumnFilter) => void

const CHART_HEIGHT = 48

// --- Histogram brush layer ---

function BrushLayer({ width, min, max, activeFilter, onFilter }: {
  width: number
  min: number
  max: number
  activeFilter?: RangeFilter | null
  onFilter: FilterCallback
}) {
  const svgRef = useRef<SVGSVGElement>(null)
  const [brushState, setBrushState] = useState<{ startX: number; currentX: number } | null>(null)

  const xToValue = useCallback((px: number) => {
    return min + (px / width) * (max - min)
  }, [width, min, max])

  const valueToX = useCallback((v: number) => {
    return ((v - min) / (max - min)) * width
  }, [width, min, max])

  const onPointerDown = useCallback((e: React.PointerEvent) => {
    const svg = svgRef.current!
    svg.setPointerCapture(e.pointerId)
    const rect = svg.getBoundingClientRect()
    const x = e.clientX - rect.left
    setBrushState({ startX: x, currentX: x })
  }, [])

  const onPointerMove = useCallback((e: React.PointerEvent) => {
    if (!brushState) return
    const rect = svgRef.current!.getBoundingClientRect()
    const x = Math.max(0, Math.min(width, e.clientX - rect.left))
    setBrushState({ ...brushState, currentX: x })
  }, [brushState, width])

  const onPointerUp = useCallback(() => {
    if (!brushState) return
    const x0 = Math.min(brushState.startX, brushState.currentX)
    const x1 = Math.max(brushState.startX, brushState.currentX)
    setBrushState(null)

    // If the drag was tiny, treat as a click → clear filter
    if (x1 - x0 < 3) {
      onFilter(null)
      return
    }

    const v0 = xToValue(x0)
    const v1 = xToValue(x1)
    onFilter({ kind: 'range', min: v0, max: v1 })
  }, [brushState, xToValue, onFilter])

  // Render brush rect for active selection
  let brushRect = null
  if (brushState) {
    const x = Math.min(brushState.startX, brushState.currentX)
    const w = Math.abs(brushState.currentX - brushState.startX)
    brushRect = <rect x={x} y={0} width={w} height={CHART_HEIGHT} fill="var(--accent)" opacity={0.2} />
  } else if (activeFilter) {
    const x = valueToX(activeFilter.min)
    const w = valueToX(activeFilter.max) - x
    brushRect = <rect x={x} y={0} width={w} height={CHART_HEIGHT} fill="var(--accent)" opacity={0.15} rx={2} />
  }

  return (
    <svg
      ref={svgRef}
      width={width}
      height={CHART_HEIGHT}
      viewBox={`0 0 ${width} ${CHART_HEIGHT}`}
      style={{ position: 'absolute', top: 0, left: 0, cursor: 'crosshair' }}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
    >
      {brushRect}
    </svg>
  )
}

// --- Numeric histogram ---

function NumericHistogram({ summary, width, visibleBins, activeFilter, onFilter }: {
  summary: NumericColumnSummary
  width: number
  visibleBins?: number[]
  activeFilter?: RangeFilter | null
  onFilter: FilterCallback
}) {
  const data = summary.bins.map((bin, i) => ({ bin: i, count: bin.count }))
  const hasOverlay = visibleBins && Math.max(...visibleBins) > 0
  const isFiltered = !!activeFilter

  return (
    <div>
      <div style={{ position: 'relative', width, height: CHART_HEIGHT }}>
        <BarChart
          data={data}
          categoryAccessor="bin"
          valueAccessor="count"
          width={width}
          height={CHART_HEIGHT}
          margin={{ top: 0, right: 0, bottom: 0, left: 0 }}
          color={isFiltered ? 'rgba(149, 95, 59, 0.15)' : hasOverlay ? 'rgba(149, 95, 59, 0.2)' : 'rgba(149, 95, 59, 0.7)'}
          barPadding={1}
          enableHover={false}
          showGrid={false}
          showCategoryTicks={false}
          accessibleTable={false}
        />
        {hasOverlay && <VisibleOverlay bins={summary.bins} visibleBins={visibleBins} width={width} />}
        <BrushLayer width={width} min={summary.min} max={summary.max} activeFilter={activeFilter} onFilter={onFilter} />
      </div>
      <span className="pt-th-range">
        {formatNum(summary.min)} – {formatNum(summary.max)}
      </span>
    </div>
  )
}

function VisibleOverlay({ bins, visibleBins, width }: {
  bins: NumericColumnSummary['bins']
  visibleBins: number[]
  width: number
}) {
  const visMax = Math.max(...visibleBins)
  if (visMax === 0) return null

  const barW = Math.max(1, (width - (bins.length - 1)) / bins.length)

  return (
    <svg
      width={width}
      height={CHART_HEIGHT}
      viewBox={`0 0 ${width} ${CHART_HEIGHT}`}
      style={{ position: 'absolute', top: 0, left: 0, pointerEvents: 'none' }}
    >
      {visibleBins.map((count, i) => {
        if (count <= 0) return null
        const x = i * (barW + 1)
        const h = (count / visMax) * CHART_HEIGHT
        return (
          <rect
            key={i}
            x={x}
            y={CHART_HEIGHT - h}
            width={barW}
            height={h}
            fill="var(--accent)"
            opacity={0.85}
          />
        )
      })}
    </svg>
  )
}

// --- Category filter popover ---

const POPOVER_ROW_HEIGHT = 30
const POPOVER_MAX_VISIBLE = 8

function CategoryPopover({ allCategories, activeSet, onFilter, onClose, anchorRef }: {
  allCategories: CategoryEntry[]
  activeSet: Set<string> | null
  onFilter: FilterCallback
  onClose: () => void
  anchorRef: React.RefObject<HTMLElement | null>
}) {
  const [searchInput, setSearchInput] = useState('')
  const [search, setSearch] = useState('')
  const listRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  const [pos, setPos] = useState({ top: 0, left: 0 })

  // Pre-lowercase all labels once (avoids 89k toLowerCase() calls per keystroke)
  const lowercased = useMemo(
    () => allCategories.map(c => c.label.toLowerCase()),
    [allCategories],
  )

  // Debounce search to avoid filtering 89k entries on every keystroke
  useEffect(() => {
    const timer = setTimeout(() => setSearch(searchInput), 80)
    return () => clearTimeout(timer)
  }, [searchInput])

  useEffect(() => {
    inputRef.current?.focus()
    if (anchorRef.current) {
      const rect = anchorRef.current.getBoundingClientRect()
      setPos({ top: rect.bottom + 4, left: rect.left })
    }
  }, [])

  // Close on Escape
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') { e.stopPropagation(); onClose() }
    }
    document.addEventListener('keydown', onKey, true)
    return () => document.removeEventListener('keydown', onKey, true)
  }, [onClose])

  // Close on click outside
  const popoverRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    function onClick(e: MouseEvent) {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        onClose()
      }
    }
    // Delay to avoid catching the opening click
    const timer = setTimeout(() => document.addEventListener('click', onClick, true), 0)
    return () => { clearTimeout(timer); document.removeEventListener('click', onClick, true) }
  }, [onClose])

  const filtered = useMemo(() => {
    if (!search) return allCategories
    const q = search.toLowerCase()
    const result: CategoryEntry[] = []
    for (let i = 0; i < allCategories.length; i++) {
      if (lowercased[i].includes(q)) result.push(allCategories[i])
    }
    return result
  }, [allCategories, lowercased, search])

  // Simple virtual scroll: track scroll offset
  const [scrollTop, setScrollTop] = useState(0)
  const totalHeight = filtered.length * POPOVER_ROW_HEIGHT
  const visibleCount = POPOVER_MAX_VISIBLE
  const first = Math.floor(scrollTop / POPOVER_ROW_HEIGHT)
  const last = Math.min(filtered.length - 1, first + visibleCount + 1)

  const allSelected = activeSet === null
  const selectedCount = activeSet ? activeSet.size : allCategories.length

  function toggleItem(label: string) {
    if (activeSet?.has(label)) {
      const next = new Set(activeSet)
      next.delete(label)
      onFilter(next.size > 0 ? { kind: 'set', values: next } : null)
    } else {
      const next = new Set(activeSet ?? [])
      next.add(label)
      onFilter({ kind: 'set', values: next })
    }
  }

  function selectAll() { onFilter(null) }
  function clearAll() { onFilter({ kind: 'set', values: new Set<string>() }) }

  return createPortal(
    <div ref={popoverRef} className="pt-cat-popover" style={{ position: 'fixed', top: pos.top, left: pos.left }}>
      <input
        ref={inputRef}
        className="pt-cat-popover-search"
        type="text"
        placeholder={`Search ${allCategories.length} values…`}
        value={searchInput}
        onChange={e => { setSearchInput(e.target.value); setScrollTop(0) }}
      />
      <div className="pt-cat-popover-actions">
        <button onClick={selectAll} className="pt-cat-popover-btn">All</button>
        <button onClick={clearAll} className="pt-cat-popover-btn">None</button>
        <span className="pt-cat-popover-count">{selectedCount} selected</span>
      </div>
      <div
        ref={listRef}
        className="pt-cat-popover-list"
        style={{ height: Math.min(filtered.length, visibleCount) * POPOVER_ROW_HEIGHT }}
        onScroll={e => setScrollTop((e.target as HTMLElement).scrollTop)}
      >
        <div style={{ height: totalHeight, position: 'relative' }}>
          {Array.from({ length: last - first + 1 }, (_, i) => {
            const idx = first + i
            const cat = filtered[idx]
            if (!cat) return null
            const checked = allSelected || (activeSet?.has(cat.label) ?? false)
            return (
              <label
                key={cat.label}
                className="pt-cat-popover-row"
                style={{
                  position: 'absolute',
                  top: idx * POPOVER_ROW_HEIGHT,
                  left: 0,
                  right: 0,
                  height: POPOVER_ROW_HEIGHT,
                }}
              >
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => toggleItem(cat.label)}
                  className="pt-cat-popover-check"
                />
                <span className="pt-cat-popover-label">{cat.label}</span>
                <span className="pt-cat-popover-pct">{cat.pct}%</span>
              </label>
            )
          })}
        </div>
      </div>
      {filtered.length === 0 && (
        <div className="pt-cat-popover-empty">No matches</div>
      )}
    </div>,
    document.body,
  )
}

// --- Categorical bars (click to filter) ---

function CategoricalBars({ summary, unfilteredAllCategories, activeFilter, onFilter }: {
  summary: CategoricalColumnSummary
  unfilteredAllCategories?: CategoryEntry[]
  activeFilter?: ColumnFilter
  onFilter: FilterCallback
}) {
  const [popoverOpen, setPopoverOpen] = useState(false)
  const othersRef = useRef<HTMLDivElement>(null)

  const items = [
    ...summary.topCategories.map(c => ({ label: c.label, count: c.count, pct: c.pct, isOthers: false })),
  ]
  if (summary.othersCount > 0) {
    items.push({
      label: `${summary.uniqueCount - summary.topCategories.length} others`,
      count: summary.othersCount,
      pct: summary.othersPct,
      isOthers: true,
    })
  }

  const activeSet = activeFilter?.kind === 'set' ? activeFilter.values : null

  return (
    <div className="pt-cat-summary">
      {items.map(item => {
        const isActive = activeSet ? activeSet.has(item.label) : true
        return (
          <div
            key={item.label}
            ref={item.isOthers ? othersRef : undefined}
            className={`pt-cat-row pt-cat-clickable`}
            style={{ opacity: activeSet && !isActive && !item.isOthers ? 0.3 : 1 }}
            onClick={item.isOthers
              ? () => setPopoverOpen(!popoverOpen)
              : () => {
                if (activeSet?.has(item.label)) {
                  const next = new Set(activeSet)
                  next.delete(item.label)
                  onFilter(next.size > 0 ? { kind: 'set', values: next } : null)
                } else {
                  const next = new Set(activeSet ?? [])
                  next.add(item.label)
                  onFilter({ kind: 'set', values: next })
                }
              }
            }
          >
            <div className="pt-cat-bar-track">
              <div className="pt-cat-bar-fill" style={{ width: `${item.pct}%` }} />
            </div>
            <span className="pt-cat-label">{item.isOthers ? item.label + ' ▾' : truncate(item.label, 16)}</span>
            <span className="pt-cat-pct">{item.pct}%</span>
          </div>
        )
      })}
      {popoverOpen && (
        <CategoryPopover
          allCategories={unfilteredAllCategories ?? summary.allCategories}
          activeSet={activeSet}
          onFilter={onFilter}
          onClose={() => setPopoverOpen(false)}
          anchorRef={othersRef}
        />
      )}
    </div>
  )
}

// --- Boolean ratio bar (click to filter) ---

function BooleanRatioBar({ summary, activeFilter, onFilter }: {
  summary: BooleanColumnSummary
  activeFilter?: ColumnFilter
  onFilter: FilterCallback
}) {
  const nonNull = summary.trueCount + summary.falseCount
  const truePct = summary.total > 0 ? (summary.trueCount / summary.total) * 100 : 0
  const falsePct = summary.total > 0 ? (summary.falseCount / summary.total) * 100 : 0
  const nullPct = summary.total > 0 ? (summary.nullCount / summary.total) * 100 : 0
  const activeValue = activeFilter?.kind === 'boolean' ? activeFilter.value : null
  const hasNulls = summary.nullCount > 0

  return (
    <div className="pt-bool-summary">
      <div className="pt-bool-bar">
        <div
          className="pt-bool-true pt-bool-clickable"
          style={{ width: `${truePct}%`, opacity: activeValue === false ? 0.3 : 1 }}
          onClick={() => onFilter(activeValue === true ? null : { kind: 'boolean', value: true })}
        />
        <div
          className="pt-bool-false pt-bool-clickable"
          style={{ width: `${falsePct}%`, opacity: activeValue === true ? 0.3 : 1 }}
          onClick={() => onFilter(activeValue === false ? null : { kind: 'boolean', value: false })}
        />
        {hasNulls && (
          <div
            className="pt-bool-null"
            style={{ width: `${nullPct}%` }}
            title={`${summary.nullCount} null values`}
          />
        )}
      </div>
      <div className="pt-bool-labels">
        <span>Yes {nonNull > 0 ? truePct.toFixed(0) : 0}%</span>
        {hasNulls && <span className="pt-bool-null-label">{nullPct.toFixed(0)}% null</span>}
        <span>No {nonNull > 0 ? falsePct.toFixed(0) : 0}%</span>
      </div>
    </div>
  )
}

// --- Timestamp histogram ---

function TimestampHistogram({ summary, width, visibleBins, activeFilter, onFilter }: {
  summary: TimestampColumnSummary
  width: number
  visibleBins?: number[]
  activeFilter?: RangeFilter | null
  onFilter: FilterCallback
}) {
  const minDate = new Date(summary.min).toLocaleDateString(undefined, { year: 'numeric', month: 'short' })
  const maxDate = new Date(summary.max).toLocaleDateString(undefined, { year: 'numeric', month: 'short' })

  const data = summary.bins.map((bin, i) => ({ bin: i, count: bin.count }))
  const hasOverlay = visibleBins && Math.max(...visibleBins) > 0
  const isFiltered = !!activeFilter

  return (
    <div>
      <div style={{ position: 'relative', width, height: CHART_HEIGHT }}>
        <BarChart
          data={data}
          categoryAccessor="bin"
          valueAccessor="count"
          width={width}
          height={CHART_HEIGHT}
          margin={{ top: 0, right: 0, bottom: 0, left: 0 }}
          color={isFiltered ? 'rgba(149, 95, 59, 0.15)' : hasOverlay ? 'rgba(149, 95, 59, 0.2)' : 'rgba(149, 95, 59, 0.7)'}
          barPadding={1}
          enableHover={false}
          showGrid={false}
          showCategoryTicks={false}
          accessibleTable={false}
        />
        {hasOverlay && <VisibleOverlay bins={summary.bins} visibleBins={visibleBins} width={width} />}
        <BrushLayer width={width} min={summary.min} max={summary.max} activeFilter={activeFilter} onFilter={onFilter} />
      </div>
      <span className="pt-th-range">{minDate} – {maxDate}</span>
    </div>
  )
}

// --- Dispatch ---

function ColumnSummaryChart({ summary, unfilteredSummary, width, visibleBins, activeFilter, onFilter }: {
  summary: NonNullSummary
  unfilteredSummary?: NonNullSummary
  width: number
  visibleBins?: number[]
  activeFilter?: ColumnFilter
  onFilter: FilterCallback
}) {
  switch (summary.kind) {
    case 'numeric':
      return <NumericHistogram summary={summary} width={width} visibleBins={visibleBins}
        activeFilter={activeFilter?.kind === 'range' ? activeFilter : null} onFilter={onFilter} />
    case 'timestamp':
      return <TimestampHistogram summary={summary} width={width} visibleBins={visibleBins}
        activeFilter={activeFilter?.kind === 'range' ? activeFilter : null} onFilter={onFilter} />
    case 'boolean':
      return <BooleanRatioBar summary={summary} activeFilter={activeFilter} onFilter={onFilter} />
    case 'categorical': {
      const unfilteredCategorical = unfilteredSummary?.kind === 'categorical' ? unfilteredSummary : undefined
      return <CategoricalBars summary={summary} unfilteredAllCategories={unfilteredCategorical?.allCategories} activeFilter={activeFilter} onFilter={onFilter} />
    }
  }
}

// --- Helpers ---

function formatNum(n: number): string {
  if (Number.isInteger(n)) return n.toLocaleString()
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s
}

// --- Mount / update ---

const roots = new WeakMap<HTMLElement, Root>()

export function renderColumnSummary(
  container: HTMLElement,
  summary: NonNullSummary,
  width: number,
  visibleBins?: number[],
  activeFilter?: ColumnFilter,
  onFilter?: FilterCallback,
  unfilteredSummary?: NonNullSummary,
) {
  let root = roots.get(container)
  if (!root) {
    root = createRoot(container)
    roots.set(container, root)
  }
  root.render(
    <ColumnSummaryChart
      summary={summary}
      unfilteredSummary={unfilteredSummary}
      width={width}
      visibleBins={visibleBins}
      activeFilter={activeFilter ?? null}
      onFilter={onFilter ?? (() => {})}
    />
  )
}

export function unmountColumnSummary(container: HTMLElement) {
  const root = roots.get(container)
  if (root) {
    root.unmount()
    roots.delete(container)
  }
}
