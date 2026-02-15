import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5000,
    strictPort: true,
    allowedHosts: [
      '.replit.dev',
    ],
    hmr: {
      protocol: 'wss',
      // Replit sets REPL_SLUG at runtime; falls back to wildcard match via allowedHosts
      host: process.env.REPL_SLUG
        ? `${process.env.REPL_SLUG}.picard.replit.dev`
        : undefined,
      clientPort: 443,
    },
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
  preview: {
    host: true,
    port: 5000,
  },
})
