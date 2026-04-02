import { describe, it, expect } from 'vitest'
import {
  NumericAccumulator,
  TimestampAccumulator,
  CategoricalAccumulator,
  BooleanAccumulator,
  formatCell,
  BIN_COUNT,
} from './accumulators'
import type { NumericColumnSummary, TimestampColumnSummary, CategoricalColumnSummary, BooleanColumnSummary } from './table'

describe('NumericAccumulator', () => {
  it('computes min/max and bins for normal data', () => {
    const acc = new NumericAccumulator()
    const data = [10, 20, 30, 40, 50, 15, 25, 35, 45, 5]
    acc.add(data, 0, data.length)
    const result = acc.snapshot() as NumericColumnSummary
    expect(result).not.toBeNull()
    expect(result.kind).toBe('numeric')
    expect(result.min).toBe(5)
    expect(result.max).toBe(50)
    expect(result.bins).toHaveLength(BIN_COUNT)
    expect(result.bins.reduce((s, b) => s + b.count, 0)).toBe(data.length)
  })

  it('returns null for monotonically increasing data with no special values', () => {
    const acc = new NumericAccumulator()
    const data = [1, 2, 3, 4, 5]
    acc.add(data, 0, data.length)
    expect(acc.snapshot()).toBeNull()
  })

  it('excludes NaN, Infinity, -Infinity, null from bins', () => {
    const acc = new NumericAccumulator()
    const data: (number | null)[] = [10, NaN, 20, Infinity, -Infinity, null, 30, 15, 25]
    acc.add(data, 0, data.length)
    const result = acc.snapshot() as NumericColumnSummary
    expect(result).not.toBeNull()
    // Only 10, 20, 30, 15, 25 in bins = 5 finite values
    expect(result.bins.reduce((s, b) => s + b.count, 0)).toBe(5)
    expect(result.min).toBe(10)
    expect(result.max).toBe(30)
  })

  it('returns non-null when monotonic but has special values', () => {
    const acc = new NumericAccumulator()
    const data: (number | null)[] = [1, 2, 3, null]
    acc.add(data, 0, data.length)
    // Monotonic but has null → should still produce histogram
    const result = acc.snapshot()
    expect(result).not.toBeNull()
  })

  it('returns null when only special values (no finite data)', () => {
    const acc = new NumericAccumulator()
    const data: (number | null)[] = [NaN, Infinity, null]
    acc.add(data, 0, data.length)
    expect(acc.snapshot()).toBeNull()
  })

  it('handles all identical values', () => {
    const acc = new NumericAccumulator()
    const data = [42, 42, 42, 42, 42, 42, 42, 42, 42, 42]
    acc.add(data, 0, data.length)
    expect(acc.snapshot()).toBeNull()
  })

  it('handles incremental batches', () => {
    const acc = new NumericAccumulator()
    const batch1 = [10, 20, 30]
    const batch2 = [5, 40, 25]
    acc.add(batch1, 0, batch1.length)
    acc.add(batch2, 0, batch2.length)
    const result = acc.snapshot() as NumericColumnSummary
    expect(result).not.toBeNull()
    expect(result.min).toBe(5)
    expect(result.max).toBe(40)
    expect(result.bins.reduce((s, b) => s + b.count, 0)).toBe(6)
  })
})

describe('TimestampAccumulator', () => {
  it('computes min/max and bins', () => {
    const acc = new TimestampAccumulator()
    const now = Date.now()
    const data = [now - 1000, now - 500, now - 200, now]
    acc.add(data, 0, data.length)
    const result = acc.snapshot() as TimestampColumnSummary
    expect(result).not.toBeNull()
    expect(result.kind).toBe('timestamp')
    expect(result.min).toBe(now - 1000)
    expect(result.max).toBe(now)
    expect(result.bins).toHaveLength(BIN_COUNT)
    expect(result.bins.reduce((s, b) => s + b.count, 0)).toBe(4)
  })

  it('returns null for empty data', () => {
    const acc = new TimestampAccumulator()
    expect(acc.snapshot()).toBeNull()
  })
})

describe('CategoricalAccumulator', () => {
  it('returns top 3 categories in descending order', () => {
    const strings = ['A', 'B', 'A', 'C', 'A', 'B', 'D', 'A', 'B', 'C']
    const acc = new CategoricalAccumulator(strings)
    acc.add([], 0, strings.length)
    const result = acc.snapshot(strings.length) as CategoricalColumnSummary
    expect(result.kind).toBe('categorical')
    expect(result.topCategories[0].label).toBe('A')
    expect(result.topCategories[0].count).toBe(4)
    expect(result.topCategories[1].label).toBe('B')
    expect(result.topCategories[1].count).toBe(3)
    expect(result.topCategories[2].label).toBe('C')
    expect(result.topCategories[2].count).toBe(2)
  })

  it('aggregates others', () => {
    const strings = ['A', 'A', 'A', 'B', 'B', 'C', 'D', 'E']
    const acc = new CategoricalAccumulator(strings)
    acc.add([], 0, strings.length)
    const result = acc.snapshot(strings.length) as CategoricalColumnSummary
    // Top 3: A(3), B(2), C(1). Others: D(1) + E(1) = 2
    expect(result.othersCount).toBe(2)
    expect(result.uniqueCount).toBe(5)
  })

  it('computes percentages', () => {
    const strings = ['A', 'A', 'A', 'A', 'B', 'B', 'B', 'B', 'C', 'C']
    const acc = new CategoricalAccumulator(strings)
    acc.add([], 0, strings.length)
    const result = acc.snapshot(10) as CategoricalColumnSummary
    expect(result.topCategories[0].pct).toBe(40)
    expect(result.topCategories[1].pct).toBe(40)
    expect(result.topCategories[2].pct).toBe(20)
  })
})

describe('BooleanAccumulator', () => {
  it('counts true and false', () => {
    const acc = new BooleanAccumulator()
    const data = [true, true, false, true, false]
    acc.add(data, 0, data.length)
    const result = acc.snapshot(data.length) as BooleanColumnSummary
    expect(result.kind).toBe('boolean')
    expect(result.trueCount).toBe(3)
    expect(result.falseCount).toBe(2)
    expect(result.total).toBe(5)
  })

  it('handles all true', () => {
    const acc = new BooleanAccumulator()
    const data = [true, true, true]
    acc.add(data, 0, data.length)
    const result = acc.snapshot(3) as BooleanColumnSummary
    expect(result.trueCount).toBe(3)
    expect(result.falseCount).toBe(0)
  })
})

describe('formatCell', () => {
  it('formats boolean as Yes/No', () => {
    expect(formatCell('boolean', true)).toBe('Yes')
    expect(formatCell('boolean', false)).toBe('No')
  })

  it('formats timestamp as date string', () => {
    const result = formatCell('timestamp', 1672531200000)
    // Locale-dependent — just check it produces a non-empty date-like string
    expect(result.length).toBeGreaterThan(0)
    expect(result).toMatch(/\d/)
  })

  it('returns empty string for null', () => {
    expect(formatCell('numeric', null)).toBe('')
    expect(formatCell('categorical', undefined)).toBe('')
  })

  it('stringifies numbers and strings', () => {
    expect(formatCell('numeric', 42)).toBe('42')
    expect(formatCell('categorical', 'hello')).toBe('hello')
  })
})
