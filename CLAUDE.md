# pretext-table

A high-performance table viewer powered by [@chenglou/pretext](https://github.com/chenglou/pretext) for DOM-free row height calculation, with Apache Arrow for columnar data.

## What this is

A virtual-scrolling data table that:
- Loads data from Apache Arrow IPC (`.arrow`) files
- Uses pretext's `prepare()` + `layout()` to compute every row height without DOM measurement
- Virtualizes rows so it handles 100k+ rows without breaking a sweat
- Supports column resizing via drag — pretext recalculates heights instantly (pure arithmetic, no reflow)
- Supports column sorting (click headers)
- Ships multilingual synthetic data (Latin, CJK, Arabic, Thai, Devanagari, emoji) to show off pretext's i18n

## Stack

- **Vite** — dev server + build
- **TypeScript** — vanilla, no framework
- **React** + **Semiotic** — column summary charts in headers (histograms for numeric, category bars for categorical)
- **@chenglou/pretext** — text measurement & layout (the whole point)
- **apache-arrow** — columnar data format, zero-copy reads

## Commands

```sh
npm install              # install deps
npm run generate         # create synthetic Arrow data in public/data.arrow
npm run dev              # start Vite dev server (hot reload)
npm run build            # production build
```

## Architecture

- `src/generate-data.ts` — Node/Bun script that generates synthetic Arrow IPC data with realistic multilingual text. Run via `npm run generate`. Outputs `public/data.arrow`.
- `src/sparkline.tsx` — React components for column summary charts in headers. Uses Semiotic's `BarChart` for numeric histograms and pure CSS bars for categorical distributions. React roots are managed via `WeakMap` for stable re-renders on column resize.
- `src/table.ts` — The table engine. Handles virtual scrolling, pretext preparation/layout, column resize, sorting, header summary charts. Key design decisions:
  - `prepare()` is called once per visible cell when it first enters the viewport (cached in a `Map`)
  - `layout()` is called per cell on resize — this is the ~0.0002ms hot path
  - Virtual scroll window is calculated from pretext heights, not DOM
  - Row positions are stored in a prefix-sum array for O(log n) scroll-to-row lookup
  - Header horizontal scroll syncs with viewport scroll
- `src/main.ts` — Entry point. Fetches the Arrow file, reads it with `apache-arrow`, computes column summaries (histograms/category frequencies), boots the table.
- `src/style.css` — Minimal table styles.
- `public/data.arrow` — Generated synthetic dataset (not checked in, run `npm run generate`).

## Key pretext API usage

```ts
import { prepare, layout, type PreparedText } from '@chenglou/pretext'

// One-time: measure text segments (expensive, cached)
const prepared = prepare(cellText, '14px Inter')

// Hot path: compute height for a given column width (pure arithmetic, ~0.0002ms)
const { height } = layout(prepared, columnWidth, lineHeight)
```

The critical insight: `layout()` is so fast that recalculating heights for thousands of visible cells on every column drag frame is cheaper than a single DOM reflow.

## TODO

- [x] Project setup (Vite + TS + deps)
- [x] Synthetic data generator (`src/generate-data.ts`)
- [x] Arrow file loading + parsing (`src/main.ts`)
- [x] Virtual scroll table engine (`src/table.ts`)
- [x] Column resizing with pretext reflow
- [x] Column sorting
- [x] Styles
- [x] Performance stats overlay
- [x] Header sparklines — Deepnote/Noteable-style column summaries (histograms + category bars)
