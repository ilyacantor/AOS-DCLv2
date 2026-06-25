import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'
import './styles/dcl-ui.css'

// The Glass Box commercial demo is a standalone surface at /glassbox. Lazy-load
// it so React Flow / zustand never enter the operator-console bundle.
const GlassBox = React.lazy(() => import('./glassbox/GlassBox'))
const isGlassBox = window.location.pathname.startsWith('/glassbox')

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    {isGlassBox ? (
      <React.Suspense fallback={<div style={{ padding: 24, color: '#71717a' }}>Loading Glass Box…</div>}>
        <GlassBox />
      </React.Suspense>
    ) : (
      <App />
    )}
  </React.StrictMode>,
)
