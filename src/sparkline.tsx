import { createRoot, type Root } from 'react-dom/client'
import { BarChart } from 'semiotic/ordinal'
import type { NumericColumnSummary, CategoricalColumnSummary } from './table'

type NonNullSummary = NumericColumnSummary | CategoricalColumnSummary

const CHART_HEIGHT = 48

function NumericHistogram({ summary, width, visibleBins }: {
  summary: NumericColumnSummary
  width: number
  visibleBins?: number[]
}) {
  const data = summary.bins.map((bin, i) => ({ bin: i, count: bin.count }))
  const hasOverlay = visibleBins && Math.max(...visibleBins) > 0

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
          color={hasOverlay ? 'rgba(149, 95, 59, 0.2)' : 'rgba(149, 95, 59, 0.7)'}
          barPadding={1}
          enableHover={false}
          showGrid={false}
          showCategoryTicks={false}
          accessibleTable={false}
        />
        {hasOverlay && <VisibleOverlay bins={summary.bins} visibleBins={visibleBins} width={width} />}
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

function CategoricalBars({ summary }: { summary: CategoricalColumnSummary }) {
  const items = [
    ...summary.topCategories.map(c => ({ label: c.label, count: c.count, pct: c.pct })),
  ]
  if (summary.othersCount > 0) {
    items.push({
      label: `${summary.uniqueCount - summary.topCategories.length} others`,
      count: summary.othersCount,
      pct: summary.othersPct,
    })
  }

  return (
    <div className="pt-cat-summary">
      {items.map(item => (
        <div key={item.label} className="pt-cat-row">
          <div className="pt-cat-bar-track">
            <div className="pt-cat-bar-fill" style={{ width: `${item.pct}%` }} />
          </div>
          <span className="pt-cat-label">{truncate(item.label, 16)}</span>
          <span className="pt-cat-pct">{item.pct}%</span>
        </div>
      ))}
    </div>
  )
}

function ColumnSummaryChart({ summary, width, visibleBins }: {
  summary: NonNullSummary
  width: number
  visibleBins?: number[]
}) {
  if (summary.kind === 'numeric') {
    return <NumericHistogram summary={summary} width={width} visibleBins={visibleBins} />
  }
  return <CategoricalBars summary={summary} />
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
) {
  let root = roots.get(container)
  if (!root) {
    root = createRoot(container)
    roots.set(container, root)
  }
  root.render(<ColumnSummaryChart summary={summary} width={width} visibleBins={visibleBins} />)
}
