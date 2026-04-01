# pretext-table

A virtual-scrolling table viewer that streams [Apache Arrow](https://arrow.apache.org/) IPC data, computes row heights with [@chenglou/pretext](https://github.com/chenglou/pretext) (no DOM measurement), and renders column summaries with [Semiotic](https://semiotic.nteract.io/).

50k rows. 11 columns. Multilingual text. No jank.

```sh
npm install
npm run generate   # 50k rows → public/data.arrow (10 batches)
npm run dev
```

## What it does

- **Streams Arrow IPC** — data arrives in batches, table grows progressively
- **DOM-free row heights** — pretext's `prepare()` + `layout()` replaces `getBoundingClientRect`
- **Lazy cell preparation** — cells are only measured when they scroll into view
- **Header sparklines** — histograms (numeric/timestamp), category bars, boolean ratio bars
- **Visible-range overlay** — header histograms highlight which part of the distribution you're looking at
- **Column resize** — drag edges, pretext recalculates heights per frame (pure arithmetic)
- **Column sort** — click headers
- **Type-aware rendering** — boolean badges, formatted timestamps, right-aligned numbers
- **Per-cell layout caching** — only re-layouts cells whose column width actually changed

## Stack

[Pretext](https://github.com/chenglou/pretext) × [Arrow](https://arrow.apache.org/) × [Semiotic](https://semiotic.nteract.io/) × Vite × TypeScript × React
