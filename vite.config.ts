import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    host: true,
    port: 5000,
    strictPort: true,
    allowedHosts: [
      '.replit.dev',
      'aab3c90e-30cf-4951-8ddd-e8c2f4334aa7-00-ytm76ihwy3bl.picard.replit.dev'
    ],
    hmr: {
      protocol: 'wss',
      host: 'aab3c90e-30cf-4951-8ddd-e8c2f4334aa7-00-ytm76ihwy3bl.picard.replit.dev',
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
