// Synthetic data generator — produces an Arrow IPC file with realistic multilingual text.
// Run: npx tsx src/generate-data.ts

import {
  tableFromArrays,
  tableToIPC,
} from 'apache-arrow'
import { writeFileSync, mkdirSync } from 'node:fs'

const ROW_COUNT = 50_000

// --- Name pools ---

const firstNames = [
  'Mina', 'Yuki', 'Somchai', 'Olga', 'Ram', 'Sujin', 'Émile', 'Wei',
  'Fatima', 'Hiroshi', 'Priya', 'Dmitri', 'Aiko', 'Carlos', 'Leila',
  'Björn', 'Amara', 'Kenji', 'Nadia', 'Tariq', 'Sakura', 'Ingrid',
  'Rashid', 'Mei', 'Alejandra', 'Kwame', 'Yuna', 'Henrik', 'Zara', 'Ravi',
  'สมชาย', 'राम', '太郎', '수진', 'محمد', 'Ólafur', 'Nguyễn',
]

const lastNames = [
  'Al-Farsi', 'Tanaka', 'สมชาย', 'Petrova', 'Sharma', 'Kim', 'Dubois',
  'Chen', 'García', 'Nakamura', 'Okafor', 'Johansson', 'Müller', 'Singh',
  'Watanabe', 'López', 'Andersen', 'Kowalski', 'Ali', 'Sato', 'Park',
  'Nguyen', 'Ivanov', 'Yamamoto', 'Hernández', 'Öztürk', 'Larsson',
  'शर्मा', '田中', '이', 'الحسن',
]

// --- Location pools ---

const locations = [
  'القاهرة, Egypt', '東京, Japan', 'กรุงเทพ, Thailand', 'Москва, Russia',
  'मुम्बई, India', '서울, Korea', 'Paris, France', '深圳, China',
  'São Paulo, Brazil', 'Lagos, Nigeria', 'Istanbul, Türkiye', 'Jakarta, Indonesia',
  'Berlin, Germany', 'Mexico City, Mexico', 'Nairobi, Kenya', 'Stockholm, Sweden',
  'Dubai, UAE', 'Singapore', 'Toronto, Canada', 'Sydney, Australia',
  'Lima, Peru', 'Warsaw, Poland', 'Hanoi, Vietnam', 'Accra, Ghana',
  'Reykjavik, Iceland', 'Buenos Aires, Argentina', 'Taipei, Taiwan',
  'Riyadh, Saudi Arabia', 'Zürich, Switzerland', 'Oslo, Norway',
]

// --- Department pools ---

const departments = [
  'Engineering', 'Product', 'Design', 'Data Science', 'Infrastructure',
  'Security', 'DevOps', 'Research', 'QA', 'Platform', 'Mobile', 'Frontend',
  'Backend', 'ML/AI', 'Analytics', 'Developer Experience', 'SRE',
]

// --- Note templates (multilingual, varied lengths) ---

const noteTemplates = [
  // Short
  'Shipped the fix. ✅',
  'On PTO until next week.',
  'LGTM — merging now.',
  'Blocked on API review.',
  'Done ✨',
  '🚀 Deployed to prod.',
  // Medium — English
  'Refactored the authentication middleware to handle edge cases around session token rotation. Tests pass on all three browsers.',
  'The dashboard now renders 2x faster after switching from DOM measurement to pretext for height calculation.',
  'Investigating a regression in the search indexer — looks like the Urdu tokenizer is splitting compound words incorrectly.',
  'Wrote a comprehensive test suite for the new billing module. Coverage went from 43% to 91%.',
  'Pair-programmed with the new hire on the GraphQL migration. They picked it up fast.',
  // Medium — mixed script
  'CJK行の折り返しが修正されました。句読点の結合ルールが正しく動作しています。',
  'แก้ไขการตัดคำภาษาไทยในระบบค้นหาแล้ว ผลลัพธ์ดีขึ้นมาก',
  'تم تحسين أداء واجهة المستخدم العربية. النص ثنائي الاتجاه يعمل بشكل صحيح الآن.',
  'देवनागरी स्क्रिप्ट में संयुक्ताक्षर विभाजन अब सही तरीके से काम कर रहा है।',
  'Mixed text wrapping: "hello" 世界 مرحبا สวัสดี works across all 4 scripts now.',
  'URL wrapping for https://example.com/reports/q3?lang=ar&mode=full splits correctly at the query delimiter.',
  'Emoji ZWJ sequences like 👨‍👩‍👧‍👦 and 🏳️‍🌈 now measure correctly on all browsers.',
  // Long
  'Spent the day profiling the virtual scroll implementation. The bottleneck was not rendering — it was layout reflow from getBoundingClientRect calls. Switched to pretext for height prediction and the 99th percentile frame time dropped from 34ms to 4ms. Next step: verify this holds with the Arabic/Thai mixed-text corpus where grapheme boundaries get tricky.',
  'The new preprocessing pipeline handles Arabic punctuation-plus-mark clusters, CJK kinsoku prohibitions, and Southeast Asian word boundaries all in one pass. Previously these were three separate post-processing steps that sometimes conflicted. The unified approach also opened up a clean path for soft-hyphen support.',
  'Completed the accessibility audit for the data grid component. Screen readers now announce column headers, sort state, and row count. The virtual scroll needed a live region to announce "showing rows 1-50 of 10,000" on scroll. Also fixed a contrast ratio issue in the header resize handles.',
  'Debated whether to use Arrow IPC or Parquet for the client-side data layer. Arrow IPC won because it is zero-copy — the browser can memory-map the ArrayBuffer directly into typed arrays without parsing. For a 50k-row table with 6 string columns, load time went from 340ms (JSON) to 12ms (Arrow IPC).',
  'ソフトハイフンの処理を修正しました。非表示時はゼロ幅で、改行選択時にはハイフンが正しく表示されます。layoutWithLines()の出力でline.textに末尾のハイフンが含まれるようになりました。全ブラウザでテスト済み。',
  'تم مراجعة الشفرة البرمجية لوحدة التخطيط. أصبح النص العربي مع علامات الترقيم يلتف بشكل صحيح. تم اختبار ذلك مع نصوص متعددة اللغات تجمع بين العربية والإنجليزية واليابانية.',
]

const statuses = ['Active', 'On Leave', 'Contractor', 'Probation', 'Remote', 'Hybrid']
const priorities = ['P0', 'P1', 'P2', 'P3', 'P4']

// --- Deterministic PRNG (mulberry32) ---

function mulberry32(seed: number) {
  return function () {
    seed |= 0
    seed = (seed + 0x6d2b79f5) | 0
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

const rand = mulberry32(42)

function pick<T>(arr: T[]): T {
  return arr[Math.floor(rand() * arr.length)]
}

// --- Generate rows ---

const ids = new Int32Array(ROW_COUNT)
const names: string[] = []
const locationCol: string[] = []
const departmentCol: string[] = []
const notes: string[] = []
const statusCol: string[] = []
const priorityCol: string[] = []
const scores = new Float64Array(ROW_COUNT)

for (let i = 0; i < ROW_COUNT; i++) {
  ids[i] = i + 1
  names.push(`${pick(firstNames)} ${pick(lastNames)}`)
  locationCol.push(pick(locations))
  departmentCol.push(pick(departments))
  notes.push(pick(noteTemplates))
  statusCol.push(pick(statuses))
  priorityCol.push(pick(priorities))
  scores[i] = Math.round(rand() * 10000) / 100
}

// --- Build Arrow table and write IPC ---

const table = tableFromArrays({
  id: ids,
  name: names,
  location: locationCol,
  department: departmentCol,
  note: notes,
  status: statusCol,
  priority: priorityCol,
  score: scores,
})

mkdirSync('public', { recursive: true })
const ipc = tableToIPC(table)
writeFileSync('public/data.arrow', ipc)

console.log(`Generated ${ROW_COUNT} rows → public/data.arrow (${(ipc.byteLength / 1024 / 1024).toFixed(1)} MB)`)
