/**
 * Column width utilities.
 *
 * autoWidth() — initial width from header label measurement + type minimums.
 * fitColumnWidths() — refine widths by sampling actual cell data.
 */
import type { ColumnType, TableData } from './table'

const LABEL_FONT = '600 11px Inter, "Helvetica Neue", Helvetica, Arial, sans-serif'
const CELL_FONT = '14px Inter, "Helvetica Neue", Helvetica, Arial, sans-serif'
const HEADER_CHROME = 60 // cell padding + type icon + sort arrow
const CELL_PAD = 24      // 12px each side

let measureCanvas: CanvasRenderingContext2D | null = null

/** Measure the rendered width of text using canvas */
function measureText(text: string, font: string): number {
  if (!measureCanvas) {
    if (typeof document === 'undefined') return text.length * 7
    measureCanvas = document.createElement('canvas').getContext('2d')
  }
  if (!measureCanvas) return text.length * 7
  measureCanvas.font = font
  return measureCanvas.measureText(text).width
}

/** Compute initial column width from header label + type constraints */
export function autoWidth(name: string, colType: ColumnType): number {
  const labelW = measureText(name.toUpperCase(), LABEL_FONT) + HEADER_CHROME

  switch (colType) {
    case 'boolean':
      return Math.max(90, Math.ceil(labelW))
    case 'timestamp':
      return Math.max(130, Math.ceil(labelW))
    case 'numeric':
      return Math.max(100, Math.ceil(labelW))
    case 'categorical':
      return Math.max(120, Math.min(280, Math.ceil(labelW)))
  }
}

/**
 * Refine column widths by sampling actual cell data.
 * Uses the median single-line width — avoids outlier-driven expansion.
 * Only widens columns, never shrinks below the header-based width.
 */
export function fitColumnWidths(
  data: TableData,
  colWidths: number[],
  maxWidth = 300,
): void {
  const sampleSize = Math.min(30, data.rowCount)
  if (sampleSize === 0) return

  for (let c = 0; c < data.columns.length; c++) {
    const widths: number[] = []
    for (let r = 0; r < sampleSize; r++) {
      const text = data.getCell(r, c)
      if (!text) continue
      const w = measureText(text, CELL_FONT) + CELL_PAD
      widths.push(w)
    }
    if (widths.length === 0) continue

    // Use median — stable, not skewed by long outliers
    widths.sort((a, b) => a - b)
    const median = widths[Math.floor(widths.length / 2)]
    const fitted = Math.min(maxWidth, Math.ceil(median))

    // Only widen, never shrink below header-based width
    if (fitted > colWidths[c]) {
      colWidths[c] = fitted
    }
  }
}
