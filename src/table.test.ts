import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createTable, type Column, type TableData, type TableEngine } from './table'

// --- Test helpers ---

function makeColumns(): Column[] {
  return [
    { key: 'id', label: 'ID', width: 80, sortable: true, numeric: true, columnType: 'numeric' },
    { key: 'name', label: 'Name', width: 150, sortable: true, numeric: false, columnType: 'categorical' },
    { key: 'score', label: 'Score', width: 100, sortable: true, numeric: true, columnType: 'numeric' },
    { key: 'active', label: 'Active', width: 80, sortable: true, numeric: false, columnType: 'boolean' },
  ]
}

function makeTableData(rows: unknown[][]): TableData {
  const columns = makeColumns()
  return {
    columns,
    rowCount: rows.length,
    getCell: (r, c) => String(rows[r][c] ?? ''),
    getCellRaw: (r, c) => rows[r][c],
    columnSummaries: columns.map(() => null),
  }
}

function makeRows(count: number): unknown[][] {
  const rows: unknown[][] = []
  for (let i = 0; i < count; i++) {
    rows.push([i + 1, `Person ${i}`, Math.round(Math.random() * 100), i % 3 !== 0])
  }
  return rows
}

async function flushRAF() {
  await vi.advanceTimersByTimeAsync(0)
}

// --- Tests ---

describe('createTable', () => {
  let container: HTMLDivElement
  let rows: unknown[][]
  let data: TableData
  let engine: TableEngine

  beforeEach(() => {
    vi.useFakeTimers()
    container = document.createElement('div')
    document.body.appendChild(container)
    rows = makeRows(50)
    data = makeTableData(rows)
    engine = createTable(container, data)
  })

  describe('DOM structure', () => {
    it('creates header, viewport, and stats bar', async () => {
      await flushRAF()
      expect(container.querySelector('.pt-header')).not.toBeNull()
      expect(container.querySelector('.pt-viewport')).not.toBeNull()
      expect(container.querySelector('.pt-stats')).not.toBeNull()
    })

    it('renders correct header labels', async () => {
      await flushRAF()
      const labels = container.querySelectorAll('.pt-th-label')
      expect(labels).toHaveLength(4)
      expect(labels[0].textContent).toBe('ID')
      expect(labels[1].textContent).toBe('Name')
      expect(labels[2].textContent).toBe('Score')
      expect(labels[3].textContent).toBe('Active')
    })

    it('shows row count in stats bar', async () => {
      await flushRAF()
      const stats = container.querySelector('.pt-stat-rows')
      expect(stats?.textContent).toContain('50')
    })
  })

  describe('filtering', () => {
    it('setFilter reduces visible row count', async () => {
      await flushRAF()
      engine.setFilter(2, { kind: 'range', min: 0, max: 30 })
      await flushRAF()
      const stats = container.querySelector('.pt-stat-rows')
      expect(stats?.textContent).toContain('of')
      expect(stats?.textContent).toContain('50')
    })

    it('setFilter creates a filter pill', async () => {
      await flushRAF()
      engine.setFilter(2, { kind: 'range', min: 10, max: 50 })
      await flushRAF()
      const pills = container.querySelectorAll('.pt-filter-pill')
      expect(pills.length).toBe(1)
      expect(pills[0].textContent).toContain('Score')
    })

    it('clearFilter removes the pill', async () => {
      await flushRAF()
      engine.setFilter(2, { kind: 'range', min: 10, max: 50 })
      await flushRAF()
      engine.clearFilter(2)
      await flushRAF()
      const pills = container.querySelectorAll('.pt-filter-pill')
      expect(pills.length).toBe(0)
    })

    it('clearAllFilters removes all pills', async () => {
      await flushRAF()
      engine.setFilter(1, { kind: 'set', values: new Set(['Person 1']) })
      engine.setFilter(3, { kind: 'boolean', value: true })
      await flushRAF()
      expect(container.querySelectorAll('.pt-filter-pill').length).toBe(2)
      engine.clearAllFilters()
      await flushRAF()
      expect(container.querySelectorAll('.pt-filter-pill').length).toBe(0)
    })

    it('boolean filter works', async () => {
      await flushRAF()
      engine.setFilter(3, { kind: 'boolean', value: true })
      await flushRAF()
      const stats = container.querySelector('.pt-stat-rows')
      expect(stats?.textContent).toContain('of')
    })

    it('set filter works', async () => {
      await flushRAF()
      engine.setFilter(1, { kind: 'set', values: new Set(['Person 0', 'Person 1']) })
      await flushRAF()
      const stats = container.querySelector('.pt-stat-rows')
      expect(stats?.textContent).toContain('of')
    })
  })

  describe('destroy', () => {
    it('clears container contents', async () => {
      await flushRAF()
      engine.destroy()
      expect(container.innerHTML).toBe('')
    })

    it('removes pt-table-container class', async () => {
      await flushRAF()
      expect(container.classList.contains('pt-table-container')).toBe(true)
      engine.destroy()
      expect(container.classList.contains('pt-table-container')).toBe(false)
    })
  })

  describe('onBatchAppended', () => {
    it('updates row count when data grows', async () => {
      await flushRAF()
      // Add 10 more rows
      for (let i = 50; i < 60; i++) {
        rows.push([i + 1, `Person ${i}`, Math.round(Math.random() * 100), i % 3 !== 0])
      }
      data.rowCount = 60
      engine.onBatchAppended()
      await flushRAF()
      const stats = container.querySelector('.pt-stat-rows')
      expect(stats?.textContent).toContain('60')
    })
  })
})
