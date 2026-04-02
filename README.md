# Sift

Fast dataframe viewer. [Pretext](https://github.com/chenglou/pretext) × [Arrow](https://arrow.apache.org/) × [Semiotic](https://semiotic.nteract.io/).

Virtual scroll, crossfilter summaries, WASM-native compute. Try it at [rgbkrk.github.io/sift](https://rgbkrk.github.io/sift/).

Becoming [`@nteract/data-explorer`](https://github.com/nteract).

## Features

- Stream Arrow IPC or load Parquet directly (via WASM, no server)
- Crossfilter: filter one column, all summaries update
- Brush histograms, click categories, toggle booleans
- Searchable category popover for high-cardinality columns
- 100k rows at 60fps. Column resize at 0.12ms/frame.
- Dark mode, keyboard navigation, null visualization
- 7 HuggingFace datasets built in

## Quick start

```sh
npm install
npm run generate   # 100k rows → public/data.arrow
npm run dev
```

## Use as a library

```tsx
import { PretextTable } from './src/react'

<PretextTable url="/data.arrow" onChange={console.log} />
```

## WASM compute (`nteract-predicate`)

Rust crate wrapping arrow-rs. Parquet reading, cell access, sorting, filtering, histograms — all in one 4.3MB WASM binary.

```sh
cd crates/nteract-predicate
wasm-pack build --target web --release
```
