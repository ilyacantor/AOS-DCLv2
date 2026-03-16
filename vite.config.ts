import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
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
        target: 'http://localhost:8004',
        changeOrigin: true,
      },
    },
  },
  preview: {
    host: true,
    port: 3004,
  },
})
