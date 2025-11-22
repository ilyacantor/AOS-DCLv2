# AutonomOS Color Palette

## Brand Identity
**Typography**: Quicksand (Google Fonts)  
**Color Philosophy**: Green, Blue, Purple with Cyan accents for a modern, tech-forward aesthetic with dark mode optimization

---

## Primary Brand Colors

### 🌊 Cyan (Primary Accent)
Primary highlight color for interactive elements and brand identity
- **Cyan-400**: `#22d3ee` - Text, icons, primary highlights
- **Cyan-500**: `#0bcad9` (Custom) - Primary brand color, tap highlights
- **Cyan-600**: `#0891b2` - Hover states, gradients

**Usage**: Primary CTAs, active states, loading indicators, live badges

---

### 💚 Green (Success & Active)
Represents success, active connections, healthy status
- **Green-400**: `#4ade80` - Success text, status indicators
- **Green-500**: `#22c55e` - Success backgrounds, active badges
- **Green-900**: `rgba(20, 83, 45, 0.2)` - Success background tint

**Usage**: Live status badges, success states, healthy metrics, active connections

---

### 💙 Blue (Info & Processing)
Represents information, processing states, trust
- **Blue-400**: `#60a5fa` - Info text
- **Blue-500**: `#3b82f6` - Info backgrounds, processing states
- **Blue-600**: `#2563eb` - Gradient endpoints, hover states
- **Blue-900**: `rgba(30, 58, 138, 0.2)` - Info background tint

**Usage**: Processing indicators, informational elements, gradient mixes

---

### 💜 Purple (Intelligence & AI)
Represents AI features, intelligence, orchestration
- **Purple-400**: `#c084fc` - AI text, intelligence indicators
- **Purple-500**: `#a855f7` - AI backgrounds, orchestration states
- **Purple-900**: `rgba(88, 28, 135, 0.2)` - AI background tint

**Usage**: AI/LLM features, orchestration layer, intelligent agents

---

## Alert & Status Colors

### 🔴 Red (Critical)
Critical errors, drift alerts, failures
- **Red-400**: `#f87171` - Error text
- **Red-500**: `#ef4444` - Error backgrounds, critical alerts
- **Red-900**: `rgba(127, 29, 29, 0.2)` - Error background tint

**Usage**: Critical drift alerts, error states, failed operations

---

### 🟠 Orange (Warning)
Important warnings, moderate drift
- **Orange-400**: `#fb923c` - Warning text
- **Orange-500**: `#f97316` - Warning backgrounds
- **Orange-900**: `rgba(124, 45, 18, 0.2)` - Warning background tint

**Usage**: Moderate drift alerts, important warnings

---

### 🟡 Yellow (Caution)
Minor issues, attention needed
- **Yellow-400**: `#facc15` - Caution text
- **Yellow-500**: `#eab308` - Caution backgrounds
- **Yellow-900**: `rgba(113, 63, 18, 0.2)` - Caution background tint

**Usage**: Minor drift alerts, attention indicators, pending states

---

## Neutral/Structural Colors

### ⚫ Slate (Dark Mode Foundation)
Primary dark mode palette for backgrounds and structure
- **Slate-400**: `#94a3b8` - Secondary text
- **Slate-500**: `#64748b` - Muted text, labels
- **Slate-600**: `#475569` - Borders (lighter)
- **Slate-700**: `#334155` - Primary borders, dividers
- **Slate-800**: `#1e293b` - Card backgrounds, panels (60% opacity: `rgba(30, 41, 59, 0.6)`)
- **Slate-900**: `#0f172a` - Deep backgrounds, contrast panels (50% opacity: `rgba(15, 23, 42, 0.5)`)

**Usage**: Backgrounds, borders, cards, panels, structure

---

### ⚪ Gray (Neutral Elements)
Secondary neutral palette for UI elements
- **Gray-300**: `#d1d5db` - Light borders (light mode)
- **Gray-400**: `#9ca3af` - Secondary text, placeholders
- **Gray-500**: `#6b7280` - Muted text
- **Gray-600**: `#4b5563` - Borders
- **Gray-700**: `#374151` - Darker borders, hover backgrounds
- **Gray-800**: `#1f2937` - Panel backgrounds (50% opacity: `rgba(31, 41, 55, 0.5)`)
- **Gray-900**: `#111827` - Deep backgrounds (30% opacity: `rgba(17, 24, 39, 0.3)`)

**Usage**: Neutral UI elements, secondary panels, hover states

---

## Special Effects & Gradients

### ✨ Gradient Combinations
- **Cyan → Blue**: `from-cyan-500 to-blue-600` - Primary brand gradient
- **Cyan → Blue → Cyan**: `from-cyan-500 via-blue-500 to-cyan-500` - Animated loading bars
- **Green/Blue/Purple**: Individual gradients for status cards

### 💫 Opacity Variants
- **20% (0.2)**: Background tints for status badges
- **30% (0.3)**: Scrollbar thumbs, tap highlights
- **40% (0.4)**: Card backgrounds (`bg-gray-800/40`)
- **50% (0.5)**: Panel overlays, hover scrollbars
- **60% (0.6)**: Primary card backgrounds (`bg-slate-800/60`)

### 🔆 Ring/Glow Effects
- **Cyan Ring**: `ring-1 ring-cyan-500/10` - Subtle glow on cards
- **Border Glow**: Combined with semi-transparent borders for depth

---

## Semantic Usage Guide

### Live Status Indicators
- **Live**: Green with pulsing animation
- **Demo**: Amber (`#f59e0b`) with static badge
- **Inactive**: Gray with reduced opacity

### Connection Status
- **ACTIVE**: Green-500
- **PROCESSING**: Blue-500
- **PENDING**: Yellow-500
- **ERROR**: Red-500

### Drift Severity
- **Critical**: Red (3+ field changes)
- **Moderate**: Orange (2 field changes)
- **Minor**: Yellow (1 field change)

### Background Hierarchy
1. **Page Background**: `bg-slate-950` (darkest)
2. **Card Background**: `bg-slate-800/60` (semi-transparent)
3. **Panel Background**: `bg-slate-900/50` (inner panels)
4. **Interactive Elements**: Hover with `bg-slate-700`

---

## Accessibility Notes

- **Touch Targets**: Minimum 44x44px for mobile
- **Text Contrast**: All text colors meet WCAG AA standards against dark backgrounds
- **Focus States**: Cyan ring indicators for keyboard navigation
- **Color Blindness**: Status is communicated through both color and iconography

---

## Mobile Optimizations

- **Tap Highlight**: `rgba(11, 202, 217, 0.3)` - Cyan with 30% opacity
- **Text Sizes**: Minimum 16px to prevent iOS zoom
- **Safe Areas**: Respects notched device insets
- **Scrollbar**: Thin (8px) with semi-transparent gray thumb

---

## Implementation References

**Tailwind Config**: Uses default Tailwind palette  
**Custom CSS**: `frontend/src/index.css`  
**Font**: Quicksand via Google Fonts  
**Dark Mode**: Default, no light mode toggle
