# pretext-table

A virtual-scrolling table viewer that streams Arrow IPC data in batches, uses pretext for DOM-free row height calculation, and renders column summary charts with Semiotic.

## Stack

- **Vite** — dev server + build
- **TypeScript** — vanilla, no framework (React only for header charts)
- **@chenglou/pretext** — text measurement & layout (the whole point)
- **apache-arrow** — columnar data, streamed via `RecordBatchReader`
- **React** + **Semiotic** — header summary charts (histograms, category bars, boolean ratio bars)

## Commands

```sh
npm install              # install deps
npm run generate         # 50k rows in 10 batches → public/data.arrow
npm run dev              # start Vite dev server (hot reload)
npm run build            # production build
```

## Architecture

- `src/generate-data.ts` — Generates synthetic Arrow IPC stream with 10 batches of 5k rows. Columns: id (int), name, location, department, note (multilingual text), status, priority, score (float), email, verified (boolean), joined (timestamp as epoch ms). Run via `npm run generate`.
- `src/main.ts` — Entry point. Streams Arrow IPC via `RecordBatchReader.from(fetch())`. Detects column types from Arrow schema. Maintains incremental summary accumulators that update as each batch arrives. Mounts the table after the first batch and calls `engine.onBatchAppended()` for each subsequent batch.
- `src/table.ts` — The table engine. Returns a `TableEngine` handle for incremental updates. Key design decisions:
  - **Lazy cell preparation** — `prepare()` is called only when a cell first enters the viewport, not upfront. Unprepared rows use an estimated one-line height.
  - **Per-cell width tracking** — `layout()` is only called when a cell's column width actually changes (chenglou's table-viewer pattern)
  - **Growable typed arrays** — rowHeights, rowPositions, sortedIndices use capacity-doubling as batches arrive
  - **Type-aware cell rendering** — boolean badges, formatted timestamps, plain text for the rest
  - **Visible-range overlay** — on each scroll, computes which histogram bins the visible rows fall into and updates the header chart overlay
  - **Header wheel forwarding** — wheel events on the header forward to the viewport
  - Row positions stored in a prefix-sum array for O(log n) scroll-to-row lookup
- `src/sparkline.tsx` — React (JSX) components for header summary charts:
  - **NumericHistogram** — Semiotic `BarChart` for full distribution (muted) + hand-drawn SVG overlay for visible rows (bright, scaled to own max)
  - **TimestampHistogram** — same as numeric but with date-formatted range labels
  - **CategoricalBars** — CSS bar chart showing top 3 categories + "N others" with percentages
  - **BooleanRatioBar** — green/red stacked bar with Yes/No percentages
  - React roots managed via `WeakMap` for stable re-renders on column resize and scroll
- `src/style.css` — Table styles, header chart styles, boolean badges, stats bar animation.
- `public/data.arrow` — Generated synthetic dataset (not checked in, run `npm run generate`).

## Key pretext API usage

```ts
import { prepare, layout, type PreparedText } from '@chenglou/pretext'

// One-time per cell: measure text segments (expensive, cached per cell)
const prepared = prepare(cellText, '14px Inter')

// Hot path: compute height for a given column width (pure arithmetic, ~0.0002ms)
const { height } = layout(prepared, columnWidth, lineHeight)
```

The critical insight: `layout()` is so fast that recalculating heights for thousands of visible cells on every column drag frame is cheaper than a single DOM reflow.

## Data flow

1. `fetch('/data.arrow')` → `RecordBatchReader.from(response)` → `reader.open()`
2. First batch: materialize strings/raws, create summary accumulators, mount table
3. Subsequent batches: append to column stores, update accumulators, call `engine.onBatchAppended()`
4. `onBatchAppended()`: grow buffers, extend sort indices, update stats + summaries, schedule render
5. `render()`: lazy-prepare visible cells, compute heights, position rows, update overlay

## Column types

Detected from Arrow schema (`Type.Bool`, `Type.Int`, `Type.Float`, etc.) with name-based overrides (e.g. `joined` → `timestamp`).

| Type | Cell rendering | Header summary | Sort |
|------|---------------|----------------|------|
| numeric | plain text | histogram + visible overlay | numeric comparison |
| categorical | plain text | top-3 category bars | string comparison |
| boolean | green/red badge | ratio bar (Yes/No %) | boolean comparison |
| timestamp | formatted date | date histogram + visible overlay | numeric comparison |
