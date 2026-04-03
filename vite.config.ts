import { defineConfig } from 'vite'
import tailwindcss from '@tailwindcss/vite'
import { copyFileSync, mkdirSync, existsSync } from 'fs'
import { resolve } from 'path'

/** Copy WASM pkg from crates/ to public/wasm/ so it's served in dev and included in builds */
function copyWasmPlugin() {
  const src = resolve(__dirname, 'crates/nteract-predicate/pkg')
  const dest = resolve(__dirname, 'public/wasm')
  const files = ['nteract_predicate.js', 'nteract_predicate_bg.wasm']

  function copyFiles() {
    if (!existsSync(src)) return
    mkdirSync(dest, { recursive: true })
    for (const f of files) {
      const s = resolve(src, f)
      if (existsSync(s)) copyFileSync(s, resolve(dest, f))
    }
  }

  return {
    name: 'copy-wasm',
    buildStart: copyFiles,
    configureServer() { copyFiles() },
  }
}

export default defineConfig({
  base: process.env.GITHUB_ACTIONS ? '/sift/' : '/',
  plugins: [tailwindcss(), copyWasmPlugin()],
})
