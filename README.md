# Sift

A crossfilter data explorer built on [Arrow](https://arrow.apache.org/), [pretext](https://github.com/chenglou/pretext), and [Semiotic](https://semiotic.nteract.io/). The demo at [rgbkrk.github.io/sift](https://rgbkrk.github.io/sift/) streams datasets from HuggingFace directly in the browser.

Headed toward [`@nteract/data-explorer`](https://github.com/nteract) — a high-performance dataframe viewer for notebook environments.

## What it does

- **Streams Arrow IPC** — data arrives in batches, table grows progressively
- **DOM-free row heights** — pretext's `layout()` replaces `getBoundingClientRect` (~0.0002ms per cell)
- **Crossfilter summaries** — header charts update to reflect filtered data
- **Searchable category filter** — click "N others ▾" for a virtual-scrolling popover with search
- **Brush to filter** — drag across histograms to set range filters
- **HuggingFace datasets** — loads Parquet via `parquet-wasm`, 7 curated datasets
- **Smart type detection** — string dates auto-promote to timestamp; null sentinels ("?", "N/A") recognized
- **Dark mode** — system preference + manual toggle
- **Keyboard navigation** — arrow keys, Page Up/Down, Home/End, Escape clears filters

## Quick start

```sh
npm install
npm run generate   # 100k rows → public/data.arrow
npm run dev        # http://localhost:5173
```

## Library

```tsx
import { PretextTable } from './src/react'

<PretextTable url="/data.arrow" onChange={state => console.log(state)} />
```

Build the library bundle (35KB ESM, peer deps externalized):

```sh
npm run build:lib   # → lib/index.js
```

## WASM compute (nteract-predicate)

Rust/WASM compute kernels wrapping `arrow-rs` for filter, aggregate, and string search:

```sh
cd crates/nteract-predicate
wasm-pack build --target web --release   # → 912KB WASM
```

Exposes: `value_counts`, `histogram`, `filter_rows`, `string_contains`.

## Performance (100k rows)

| Operation | Time |
|-----------|------|
| Scroll frame (avg) | 9.8ms |
| Sort | 48ms |
| Filter | 13ms |
| Column resize frame | **0.12ms** |

## Stack

[Pretext](https://github.com/chenglou/pretext) × [Arrow](https://arrow.apache.org/) × [Semiotic](https://semiotic.nteract.io/) × [Rust/WASM](https://rustwasm.github.io/) × Vite × TypeScript × React
