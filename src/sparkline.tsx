import { createRoot, type Root } from 'react-dom/client'
import { useRef, useCallback, useState } from 'react'
import { BarChart } from 'semiotic/ordinal'
import type {
  NumericColumnSummary,
  CategoricalColumnSummary,
  BooleanColumnSummary,
  TimestampColumnSummary,
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

// --- Categorical bars (click to filter) ---

function CategoricalBars({ summary, activeFilter, onFilter }: {
  summary: CategoricalColumnSummary
  activeFilter?: ColumnFilter
  onFilter: FilterCallback
}) {
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
        const isClickable = !item.isOthers
        return (
          <div
            key={item.label}
            className={`pt-cat-row ${isClickable ? 'pt-cat-clickable' : ''}`}
            style={{ opacity: activeSet && !isActive ? 0.3 : 1 }}
            onClick={isClickable ? () => {
              if (activeSet?.has(item.label)) {
                // Deselect: if it was the only one, clear filter
                const next = new Set(activeSet)
                next.delete(item.label)
                onFilter(next.size > 0 ? { kind: 'set', values: next } : null)
              } else {
                // Select: add to set (or start new set)
                const next = new Set(activeSet ?? [])
                next.add(item.label)
                onFilter({ kind: 'set', values: next })
              }
            } : undefined}
          >
            <div className="pt-cat-bar-track">
              <div className="pt-cat-bar-fill" style={{ width: `${item.pct}%` }} />
            </div>
            <span className="pt-cat-label">{truncate(item.label, 16)}</span>
            <span className="pt-cat-pct">{item.pct}%</span>
          </div>
        )
      })}
    </div>
  )
}

// --- Boolean ratio bar (click to filter) ---

function BooleanRatioBar({ summary, activeFilter, onFilter }: {
  summary: BooleanColumnSummary
  activeFilter?: ColumnFilter
  onFilter: FilterCallback
}) {
  const truePct = summary.total > 0 ? (summary.trueCount / summary.total) * 100 : 0
  const falsePct = summary.total > 0 ? (summary.falseCount / summary.total) * 100 : 0
  const activeValue = activeFilter?.kind === 'boolean' ? activeFilter.value : null

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
      </div>
      <div className="pt-bool-labels">
        <span>Yes {truePct.toFixed(0)}%</span>
        <span>No {falsePct.toFixed(0)}%</span>
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

function ColumnSummaryChart({ summary, width, visibleBins, activeFilter, onFilter }: {
  summary: NonNullSummary
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
    case 'categorical':
      return <CategoricalBars summary={summary} activeFilter={activeFilter} onFilter={onFilter} />
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
) {
  let root = roots.get(container)
  if (!root) {
    root = createRoot(container)
    roots.set(container, root)
  }
  root.render(
    <ColumnSummaryChart
      summary={summary}
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
