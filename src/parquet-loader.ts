/**
 * Fetches a Parquet file from HuggingFace and converts it to Arrow IPC bytes.
 * Uses parquet-wasm for the Parquet→Arrow conversion, loaded lazily.
 */

type ReadParquetFn = (data: Uint8Array) => { intoIPCStream(): Uint8Array }

let readParquetFn: ReadParquetFn | null = null

async function ensureWasm(): Promise<ReadParquetFn> {
  if (readParquetFn) return readParquetFn
  const mod = await import('parquet-wasm/esm/parquet_wasm.js')
  await mod.default()
  readParquetFn = mod.readParquet as ReadParquetFn
  return readParquetFn
}

/**
 * Resolve the Parquet file URL(s) for a HuggingFace dataset.
 */
async function resolveParquetUrl(
  dataset: string,
  config = 'default',
  split = 'train',
): Promise<string> {
  const apiUrl = `https://datasets-server.huggingface.co/parquet?dataset=${encodeURIComponent(dataset)}`
  const resp = await fetch(apiUrl)
  if (!resp.ok) {
    throw new Error(`HuggingFace API error: ${resp.status} ${resp.statusText}`)
  }
  const data = await resp.json()

  const files = data.parquet_files as Array<{
    config: string
    split: string
    url: string
    filename: string
    size: number
  }>

  if (!files || files.length === 0) {
    throw new Error(`No Parquet files found for ${dataset}`)
  }

  // Try exact config/split match, then split-only, then first available
  let match = files.find(f => f.config === config && f.split === split)
  if (!match) match = files.find(f => f.split === split)
  if (!match) match = files[0]

  return match.url
}

export interface ParquetLoadResult {
  ipcBytes: Uint8Array
}

/**
 * Fetch a HuggingFace dataset as Parquet, convert to Arrow IPC stream bytes.
 */
export async function loadHuggingFaceParquet(
  dataset: string,
  config?: string,
  split?: string,
  onProgress?: (status: string) => void,
): Promise<ParquetLoadResult> {
  onProgress?.('Resolving dataset…')
  const url = await resolveParquetUrl(dataset, config, split)

  // Start WASM init and Parquet download in parallel
  onProgress?.('Downloading Parquet…')
  const [resp, readParquet] = await Promise.all([
    fetch(url),
    ensureWasm(),
  ])

  if (!resp.ok) {
    throw new Error(`Failed to fetch Parquet: ${resp.status} ${resp.statusText}`)
  }
  const buffer = new Uint8Array(await resp.arrayBuffer())

  onProgress?.('Converting to Arrow…')
  const wasmTable = readParquet(buffer)
  const ipcBytes = wasmTable.intoIPCStream()

  return { ipcBytes }
}
