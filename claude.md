# Claude Project Guidelines - DCL Engine

## Critical Constraints

### Graph Rendering - DO NOT BREAK
- Sankey graph must always render: **23 nodes, 134 links**
- Any change to data structures must preserve graph output
- Test with screenshot after any backend changes

### Code Quality Standards
- No shortcuts, bandaids, or tech debt
- Clean architecture with single source of truth
- No duplicate data arrays - consolidate where possible
- Always propose changes first, implement only after approval

### Data Architecture

#### Fact Base Structure (`data/fact_base.json`)
- **Single source of truth** for each metric
- `quota_by_rep`: `{quota, attainment, attainment_pct}` - contains all quota data
- `win_rate_by_rep`: simple percentage values
- All 36 reps must have differentiated values

#### Query Endpoint (`backend/api/query.py`)
- Uses `NESTED_VALUE_KEY_MAP` to extract nested values
- `_extract_value()` helper handles both simple and nested dictionaries
- Maps metric aliases to canonical data keys

### Rep Performance Ranking (for reference)
| Rank | Rep | Attainment | Win Rate |
|------|-----|------------|----------|
| #1 | Sarah Williams | 115% | 52% |
| #36 | Thomas Anderson | 83% | 32% |

### Semantic Catalog Stats
- Metrics: 37
- Entities: 29
- Personas: CFO, CRO, CHRO, COO, CTO

## File Locations

| Purpose | File |
|---------|------|
| Query endpoint | `backend/api/query.py` |
| Semantic export | `backend/api/semantic_export.py` |
| Fact data | `data/fact_base.json` |
| Mode state | `backend/core/mode_state.py` |
| Persona views | `backend/engine/persona_view.py` |

## Testing Commands

```bash
# Test quota_attainment query
curl -X POST http://localhost:8000/api/dcl/query \
  -H "Content-Type: application/json" \
  -d '{"metric":"quota_attainment","dimensions":["rep"],"time_range":{"start":"2026-Q4","end":"2026-Q4"}}'

# Check semantic catalog
curl http://localhost:8000/api/dcl/semantic-export | python -c "import sys,json; d=json.load(sys.stdin); print(f'Metrics: {len(d[\"metrics\"])}, Entities: {len(d[\"entities\"])}')"

# Verify graph data
curl http://localhost:8000/api/topology | python -c "import sys,json; d=json.load(sys.stdin); print(f'Nodes: {len(d[\"nodes\"])}, Links: {len(d[\"links\"])}')"
```

## User Preferences
- Simple, everyday language
- No mock data
- PST timezone
- Propose changes before implementing
