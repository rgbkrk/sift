/**
 * Compute a reasonable default column width.
 *
 * Uses canvas text measurement for the header label to get accurate
 * pixel widths, then adds padding for type icon, sort arrow, and
 * summary chart. Falls back to type-based minimums.
 */
import type { ColumnType } from './table'

const LABEL_FONT = '600 11px Inter, "Helvetica Neue", Helvetica, Arial, sans-serif'
// Extra width: 24px cell padding + 20px type icon + 16px sort arrow
const HEADER_CHROME = 60

let measureCanvas: CanvasRenderingContext2D | null = null

/** Measure the rendered width of text using canvas */
function measureText(text: string, font: string): number {
  if (!measureCanvas) {
    if (typeof document === 'undefined') return text.length * 7 // SSR fallback
    measureCanvas = document.createElement('canvas').getContext('2d')
  }
  if (!measureCanvas) return text.length * 7
  measureCanvas.font = font
  return measureCanvas.measureText(text).width
}

/** Compute column width from header label + type constraints */
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
