# Sift

Crossfilter data explorer. [Pretext](https://github.com/chenglou/pretext) × [Arrow](https://arrow.apache.org/) × [Semiotic](https://semiotic.nteract.io/).

![Sift — Spotify 114k tracks](https://img.runt.run/2026/04/03/870bad2e6deb.png)

Becoming [`@nteract/sift`](https://github.com/nteract). Try it at [rgbkrk.github.io/sift](https://rgbkrk.github.io/sift/).

## Features

- **Stream Arrow IPC or load Parquet directly** — WASM-native, no server
- **Crossfilter** — filter one column, all summaries update
- **Column summaries** — histograms, category bars, boolean ratios, searchable popovers
- **Smart column detection** — pandas index columns, HuggingFace ClassLabel features, type casting with undo
- **Virtual scroll** — 100k+ rows at 120fps. Pretext layout at 0.0002ms/cell.
- **Dark mode**, keyboard nav, pinned columns, ARIA grid
- **8 HuggingFace datasets** built into the demo

## Quick start

```sh
npm install
npm run dev
```

## Use as a library

```tsx
import { SiftTable } from '@nteract/sift'

<SiftTable url="/data.arrow" onChange={console.log} />
```

Load Parquet directly — decoded entirely in WASM:

```tsx
<SiftTable url="https://huggingface.co/datasets/spotify/resolve/main/data.parquet" />
```

## Engine API

```ts
engine.setSort('age', 'desc')
engine.setFilter(colIndex, { kind: 'range', min: 18, max: 65 })
engine.clearAllFilters()

// State → SQL/pandas
const state = engine.getState()
engineStateToExplorerState(state).filters.map(predicateToSQL)
```

See [`docs/embedding-guide.md`](docs/embedding-guide.md) for the full API.

## WASM compute (`nteract-predicate`)

Rust crate wrapping arrow-rs. Parquet decoding, sort, histograms, value counts, string search, column casting — all in one WASM binary. Data lives in WASM memory; JS holds handles.

```sh
cd crates/nteract-predicate
wasm-pack build --target web --release
```

## Stack

| Layer | What |
|-------|------|
| Layout | [@chenglou/pretext](https://github.com/chenglou/pretext) — DOM-free text measurement |
| Data | [Apache Arrow](https://arrow.apache.org/) — columnar, streamed via RecordBatchReader |
| Compute | [nteract-predicate](crates/nteract-predicate/) — arrow-rs WASM kernels |
| Reactivity | [RxJS](https://rxjs.dev/) — FPS monitoring, streaming pipeline |
| Charts | Raw SVG — header sparklines |

## Development

```sh
npm install              # deps
npm run dev              # vite dev server
npm test                 # vitest unit tests
npm run test:e2e         # playwright E2E
npm run build            # production build (demo)
npm run build:lib        # library build → lib/index.js
```
