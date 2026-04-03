import { defineConfig } from 'vite'
import { resolve } from 'path'

/**
 * Library build config — produces ESM + CJS bundles for npm consumers.
 * Run with: npm run build:lib
 *
 * The demo app uses the default vite.config.ts.
 */
export default defineConfig({
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.ts'),
      formats: ['es'],
      fileName: 'index',
    },
    outDir: 'lib',
    rollupOptions: {
      // Don't bundle peer dependencies
      external: [
        'react',
        'react-dom',
        'react-dom/client',
        'react/jsx-runtime',
        'apache-arrow',
        'semiotic',
        'semiotic/ordinal',
        '@chenglou/pretext',
        /^@radix-ui\//,
      ],
    },
  },
})
