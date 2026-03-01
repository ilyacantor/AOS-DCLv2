# Phases 2-5: Remaining Demo Screens — Single Sprint

## Context

You are building the remaining 4 screens for the ContextOS demo. Phase 0 (dark theme, routes, service wiring) and Phase 1 (Intelligence Dashboard) are complete. The DemoShell with keyboard navigation is working. `/demo/intelligence` is live.

This is a demo, not production code. Prioritize visual impact and smooth flow over edge case handling. Most components already exist in this repo — reuse them with layout changes, don't rebuild.

Existing components you should reuse:
- `PreMeetPortal` — pre-meeting upload portal
- `ChatWindow` — conversation UI
- `MessageBubble` — chat messages
- `FileDropZone` — file upload in chat
- `InlineTable` — tables rendered inline in conversation
- `InlineHierarchy` — hierarchy trees in conversation
- `ComparisonView` — system-vs-system comparison
- `ConfirmWidget` — accept/edit/skip controls
- `ProgressTracker` — section progress sidebar
- `FDEReview` — contour map review with approve/reject

All are already dark-themed from Phase 0.

---

## Screen 2: Pre-Meeting Preparation

**File:** `src/client/components/demo/PreMeetingPrep.tsx`

**Presenter says:** "Based on the gaps, the agent generates a personalized prep request. This went out 3 days before the meeting."

### Layout

Split view, two columns:

**Left column (55%):** The prep email/message
- Dark card (bg-secondary), styled like an email preview
- "To:" line showing VP Finance name/email
- Subject line in text-primary, bold
- Body: 3-4 specific artifact requests generated from the gap list
- Each request has: artifact name (bold), why it helps (one line, text-secondary), estimated time to find ("< 2 minutes", teal text)
- The email content should be hardcoded for the demo but look like it was generated. Write it to match the gaps from the intel data:
  1. "Your latest board deck P&L — shows how the C-suite sees segments. This saves 15 minutes in our conversation."
  2. "A cost center export from NetSuite — confirms your financial hierarchy. We detected 47 cost centers but need the rollup structure."
  3. "Your Workday department tree — confirms the org structure we inferred from job postings."

**Right column (45%):** Upload portal with pre-loaded artifacts
- Reuse the existing `PreMeetPortal` component or adapt its layout
- Show 2 pre-loaded sample artifacts with processing status:
  - "Board Deck Q3.pdf" — Status: Parsed ✓ — "3 segments identified, 12 cost centers in P&L, revenue split confirmed"
  - "Cost_Centers_Export.xlsx" — Status: Parsed ✓ — "47 cost centers extracted, 3-level hierarchy detected"
- Each artifact is a card showing: filename, file type icon (just a colored rectangle with PDF/XLSX), upload timestamp, extraction summary
- A "Drop files here" zone at the bottom (reuse FileDropZone, styled but non-functional for demo)

### Backend

Add to seed data or hardcode: the prep email content and the artifact processing results. No actual email sending or file parsing during this screen — it's all pre-computed display.

If the existing pre-meeting endpoints (`/api/sessions/:id/premeet/send`, `/api/sessions/:id/premeet/upload`) already return useful data, use them. Otherwise, serve static demo data from the seed loader.

### Commit: `feat: build Screen 2 - Pre-Meeting Preparation with email preview and artifact cards`

---

## Screen 3: The Interview

**File:** `src/client/components/demo/DemoInterview.tsx`

**Presenter says:** "Watch the agent interview a VP Finance. Everything from the intelligence brief feeds into the conversation."

### Layout

Two columns:

**Left column (70%):** Conversation UI
- Reuse `ChatWindow` as the foundation
- The chat should be wrapped in the demo layout (no standalone page chrome)
- On load, auto-create a demo session via `POST /api/sessions` with:
  ```json
  {
    "customer_name": "TechWave",
    "stakeholder_name": "Alex Rivera",
    "stakeholder_role": "VP Finance"
  }
  ```
- Then start the interview via `POST /api/sessions/:id/start`
- The agent's first message should incorporate intel context (this is already built — the conversation service uses the intel brief). If the agent's opening message doesn't reference the intel data, check that the intel brief is being passed to the LLM prompt.
- All existing inline components work here: InlineTable, InlineHierarchy, ComparisonView, ConfirmWidget, FileDropZone
- The presenter (or a colleague) types real responses. The LLM responds in real time.

**Right column (30%):** Context panel
- Reuse `ProgressTracker` at the top showing section progress (1-5)
- Below it, add a live stats area:
  - "Confidence" — a number ticking up as the agent learns (read from session data)
  - "Gaps Remaining" — count decreasing (computed from contour map completeness)
  - "Systems Validated" — count of systems where SOR has been confirmed
- Below stats, a mini org hierarchy that fills in as the interview progresses. This can be a simplified version of the OrgStructureTree from Screen 1, showing only confirmed nodes (green) vs. still-unknown (grey outlines)

### Session Management

The demo needs a consistent session. Options:
1. Create a new session on screen load and use it for the interview
2. Reuse an existing demo session (check if one exists, create if not)

Go with option 2 — check for a session with `customer_name: "TechWave"`. If it exists and has messages, resume it. If not, create and start fresh. This way the presenter can reset the demo and get a clean session, or continue where they left off.

### Context Injection

The agent needs to know about the intel brief and the pre-loaded artifacts. Check how `src/server/services/context.ts` builds the LLM prompt. It should include:
- The intel data (segments, org structure, systems discovered)
- The artifact extraction results (cost centers found, segments from board deck)
- What's already confirmed vs. what's still a gap

If this isn't wired up, add it: when building the system prompt for the interview, inject a summary of the intel brief and artifact results. The agent should say things like "I see from your board deck that you have 3 segments" not "Can you tell me about your organizational structure?"

### Commit: `feat: build Screen 3 - Interview with context panel and session management`

---

## Screen 4: Contour Map Visualization

**File:** `src/client/components/demo/ContourMapView.tsx`

**Presenter says:** "After the interview, the agent produces the Enterprise Contour Map. This is the organizational DNA."

### Layout

Full-screen visualization with tabs and a review overlay.

**Tab bar** across the top — one tab per dimension:
- Legal Entity | Division | Cost Center | Department | Geography | Profit Center | Segment | Customer Segment

Each tab shows an interactive hierarchy tree for that dimension. Reuse or adapt `InlineHierarchy` from the existing components. The hierarchy data comes from the session's contour map (`GET /api/sessions/:id/contour`).

If the contour map is empty (interview didn't run or incomplete), fall back to the sample contour from `data/contour/sample_contour.json`.

### Node Design (same as OrgStructureTree from Phase 1)

- Name, source tag, confidence indicator (left border color)
- Green: confirmed by stakeholder
- Teal: confirmed by artifact
- Amber: inferred from systems
- Grey: gap

### SOR Badge

On each dimension tab, show a badge indicating which system is authoritative:
- "SOR: Workday" (for Department)
- "SOR: NetSuite" (for Cost Center, Legal Entity)
- "SOR: Stakeholder Interview" (for Management Overlay / Segment)

### Cross-Dimension View

A toggle button "Cross-Dimension View" switches from single-tab to a combined view showing how dimensions relate. This is the visual that explains why cross-system queries are hard:

- Show Division tree on the left, Cost Center tree in the middle, Segment tree on the right
- Draw connecting lines between related nodes (Cloud division → Cloud East + Cloud West cost centers → Cloud board segment)
- Lines colored by confidence

This doesn't need to be a full graph visualization — a simple 3-column layout with SVG lines between nodes is enough for the demo.

### FDE Review Overlay

Reuse the existing `FDEReview` component. Add a "Review Mode" toggle in the top-right. When toggled:
- Each dimension section gets an Approve/Reject button pair
- Approving all sections triggers: `POST /api/sessions/:id/contour/approve`
- After approval, show a brief animation: "Rebuilding semantic graph..." (2-3 seconds with a pulsing teal bar), then "Graph rebuilt. 107 concepts. 325 pairings. Ready for queries." with a green checkmark
- The "rebuild" animation is cosmetic for the demo — it doesn't need to actually call DCL's rebuild endpoint unless DCL is running

### Data Source

Primary: `GET /api/sessions/:id/contour` (from the interview session)
Fallback: `data/contour/sample_contour.json` (if no interview was conducted)

The sample contour should match the intel data structure (same divisions, cost centers, departments). Update the sample contour JSON if needed to align with TechWave's org structure from the intel data.

### Commit: `feat: build Screen 4 - Contour Map with dimension tabs, cross-view, and FDE review`

---

## Screen 5: Query Resolution

**File:** `src/client/components/demo/QueryResolution.tsx`

**Presenter says:** "The graph is built. Let's see what it can do."

### Layout

Two areas:

**Top (40%):** Query interface
- Large text input field, centered, prominent. Placeholder: "Ask a question about TechWave..."
- Below the input: suggestion chips for the 5 demo queries (clickable, teal outline):
  1. "What is total revenue?"
  2. "Revenue by region"
  3. "Revenue by cost center for the Cloud division"
  4. "Segmented financials"
  5. "Pipeline by segment"
- Clicking a chip fills the input and auto-submits

**Bottom (60%):** Results area
- Shows the answer after each query
- Each result is a card (bg-secondary) containing:
  - The question (text-secondary, 11px, above the answer)
  - The answer: large metric numbers (JetBrains Mono, teal, 32px) with labels
  - For breakdowns: a horizontal bar chart or simple table showing the split
  - Confidence badge (pill with score)
  - Source systems (small tags: "NetSuite", "Workday", etc.)
  - "Show Provenance" toggle

### Provenance Graph

When "Show Provenance" is toggled on a result, expand a panel below the result showing the assembly path as a flow diagram.

Build this as an SVG component. Left-to-right flow:

```
[Source Systems] → [Classification] → [Graph Traversal] → [Join Resolution] → [Answer]
```

Each stage is a rounded rectangle with:
- Stage name at top (11px, text-secondary)
- Detail inside (system names, concept names, join paths, confidence)
- Arrows connecting stages, colored by confidence

For the boss query, the provenance shows:
```
[NetSuite: revenue] → [DCL: classified as FIN-001] → [Graph: SLICEABLE_BY cost_center] → [Join: NetSuite ↔ Workday via cost_center_code] → [Filter: Cloud → Cloud East + Cloud West] → [$X.XM]
```

This doesn't need to be a full D3 force graph. A simple horizontal flow with boxes and arrows built in SVG is enough. Make it look clean and informative, not complex.

### Rapid-Fire Mode

After the first query resolves, subsequent queries animate in faster. When a chip is clicked:
1. Input fills (200ms)
2. Brief "thinking" pulse on the input (500ms)
3. Result card slides in below previous results (300ms)
4. Previous results compress slightly to make room

The presenter clicks chips in sequence: 1 → 2 → 3 → 4 → 5. By query 3 (the boss query), the audience has seen the progression from simple to complex. Query 4 and 5 show persona switching.

### Backend Integration

Queries go through the proxy: `POST /api/proxy/nlq/query` → forwards to NLQ service.

If NLQ is not running (connection error), fall back to hardcoded demo responses:

```typescript
const DEMO_RESPONSES: Record<string, any> = {
  "What is total revenue?": {
    answer: "$48.0M",
    metric: "revenue",
    confidence: 0.95,
    source_systems: ["NetSuite"],
    resolution_path: "graph",
    provenance: { /* ... */ }
  },
  "Revenue by region": {
    answer: { "AMER": "$24.0M", "EMEA": "$14.4M", "APAC": "$9.6M" },
    // ...
  },
  // ... all 5 demo queries with full responses
};
```

Include complete hardcoded responses for all 5 demo queries. This is the fallback plan — the demo never shows an error on the payoff screen.

### Commit: `feat: build Screen 5 - Query Resolution with provenance graph and rapid-fire mode`

---

## Screen 6 (final): Presenter Mode Polish

### Screen Transitions

Update `DemoShell.tsx`:
- Smooth fade/slide transitions between screens (CSS transition, 300ms)
- Screen indicator in top bar shows which screen is active (1-5 dots or labels)
- Keyboard: ← → to navigate, number keys 1-5 to jump to specific screen
- Each screen has a title that appears briefly on entry (fade in, hold 1s, fade to small label in header)

### Presenter Cues

Small text area at the bottom of the DemoShell (bg-primary, 90% opacity, text-secondary, 11px):
- Screen 1: "Talk about what the agent already knows. Click 'Run Scan' for the animation. → for next."
- Screen 2: "Show the personalized prep email. Point out the artifact processing. → for next."
- Screen 3: "Conduct the live interview. Keep to 5 minutes. → for next when ready."
- Screen 4: "Show the contour map tabs. Toggle cross-dimension view. Approve to trigger rebuild. → for next."
- Screen 5: "Click the query chips in order. Pause on the boss query provenance."

Presenter cues can be toggled on/off with the `P` key.

### Demo Reset

Update `POST /api/demo/reset` to:
1. Delete all sessions from the DB
2. Reload seed data
3. Return `{ status: "reset", ready: true }`

Add a reset button in the DemoShell header (small, text-secondary, "Reset Demo") that calls this endpoint and navigates back to Screen 1.

### Commit: `feat: add presenter mode, screen transitions, keyboard nav, demo reset`

---

## Final Verification

Run through the full demo flow:

1. Load `/demo` — redirects to `/demo/intelligence`
2. Screen 1: Intel dashboard loads, scan animation plays, all 4 quadrants + gaps visible
3. Press → or 2: Screen 2 loads with email and artifacts
4. Press → or 3: Screen 3 loads, session created, agent sends opening message with intel context
5. Type a response to the agent — confirm real-time LLM response via WebSocket
6. Drop a file in the chat — confirm file upload works
7. Press → or 4: Screen 4 loads contour map (from session or fallback)
8. Click through dimension tabs, toggle cross-dimension view
9. Click "Approve" in review mode — rebuild animation plays
10. Press → or 5: Screen 5 loads with query input and chips
11. Click each chip in sequence — results appear with provenance
12. Toggle provenance on the boss query — flow diagram shows cross-system assembly
13. Press P — presenter cues toggle
14. Click "Reset Demo" — confirm clean slate, navigate to Screen 1

If NLQ/DCL are not running, confirm Screen 5 falls back to hardcoded responses gracefully.

### Commit: `test: full demo flow verification, all 5 screens end-to-end`

---

## Success Criteria

- [ ] Screen 2: Prep email + artifact cards, dark theme, data from intel/seed
- [ ] Screen 3: Live interview with context panel, reuses existing chat components, real LLM
- [ ] Screen 4: 8 dimension tabs, confidence-coded hierarchy, cross-dimension view, FDE review with rebuild animation
- [ ] Screen 5: Query input with chips, results with metrics + provenance graph, rapid-fire mode, hardcoded fallback
- [ ] Presenter mode: keyboard nav, screen transitions, cues toggle, demo reset
- [ ] Full flow works end-to-end (5 screens in sequence)
- [ ] 5 commits pushed
