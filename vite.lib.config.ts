import { defineConfig } from 'vite'
import { resolve } from 'path'
import tailwindcss from '@tailwindcss/vite'

/**
 * Library build config — produces ESM bundle + compiled CSS for npm consumers.
 * Run with: npm run build:lib
 *
 * The demo app uses the default vite.config.ts.
 */
export default defineConfig({
  plugins: [tailwindcss()],
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
        '@chenglou/pretext',
        /^@radix-ui\//,
      ],
    },
  },
})
