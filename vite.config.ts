/// <reference types="vitest" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'node',
    exclude: ['tests/**', 'node_modules/**'],
  },
  resolve: {
    alias: {
      '@assets': path.resolve(__dirname, 'attached_assets'),
    },
  },
  server: {
    host: '0.0.0.0',
    port: 3004,
    strictPort: true,
    allowedHosts: true,
    proxy: {
      '/api/platform': {
        target: process.env.VITE_PLATFORM_URL || 'http://localhost:8006',
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/api\/platform/, '/api'),
      },
      '/api': {
        // DCL UI shows the same DCL the pipeline ingests to (Farm push + NLQ
        // read both target :8104). Override via VITE_DCL_API_URL.
        target: process.env.VITE_DCL_API_URL || 'http://localhost:8104',
        changeOrigin: true,
      },
    },
  },
  preview: {
    host: true,
    port: 3004,
  },
})
