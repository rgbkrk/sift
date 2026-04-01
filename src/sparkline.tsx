import { createRoot, type Root } from 'react-dom/client'
import { createElement } from 'react'
import { BarChart } from 'semiotic/ordinal'
import type { ColumnSummary, NumericColumnSummary, CategoricalColumnSummary } from './table'

const CHART_HEIGHT = 48

function NumericHistogram({ summary, width }: { summary: NumericColumnSummary; width: number }) {
  const data = summary.bins.map((bin, i) => ({
    bin: i,
    count: bin.count,
  }))

  return createElement('div', null,
    createElement(BarChart, {
      data,
      categoryAccessor: 'bin',
      valueAccessor: 'count',
      width,
      height: CHART_HEIGHT,
      margin: { top: 2, right: 2, bottom: 2, left: 2 },
      color: '#955f3b',
      barPadding: 2,
      enableHover: false,
      showGrid: false,
      accessibleTable: false,
    } as Record<string, unknown>),
    createElement('span', { className: 'pt-th-range' },
      `${formatNum(summary.min)} – ${formatNum(summary.max)}`
    ),
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

  return createElement('div', { className: 'pt-cat-summary' },
    ...items.map(item =>
      createElement('div', { key: item.label, className: 'pt-cat-row' },
        createElement('div', { className: 'pt-cat-bar-track' },
          createElement('div', {
            className: 'pt-cat-bar-fill',
            style: { width: `${item.pct}%` },
          }),
        ),
        createElement('span', { className: 'pt-cat-label' }, truncate(item.label, 16)),
        createElement('span', { className: 'pt-cat-pct' }, `${item.pct}%`),
      )
    )
  )
}

function ColumnSummaryChart({ summary, width }: { summary: ColumnSummary; width: number }) {
  if (summary.kind === 'numeric') {
    return createElement(NumericHistogram, { summary, width })
  }
  return createElement(CategoricalBars, { summary })
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
  summary: ColumnSummary,
  width: number,
) {
  let root = roots.get(container)
  if (!root) {
    root = createRoot(container)
    roots.set(container, root)
  }
  root.render(createElement(ColumnSummaryChart, { summary, width }))
}
