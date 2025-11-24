# DCL Architecture - Target State

**Status:** ✅ Implemented (November 24, 2025)

This document described the target architecture for the DCL refactoring. **It has been completed.**

## Goal

Transform the DCL from a mixed hot/cold-path system into a clean 3-layer architecture:
1. **Semantic Mapper** (batch) - Pre-computes field→concept mappings
2. **Semantic Model** (data) - Stores ontology, personas, and mappings
3. **DCL Engine** (runtime) - Builds graphs from stored data

## Implementation Status

✅ **Database Layer** - 4 tables created with proper schemas, indexes, and constraints  
✅ **Config Layer** - YAML configs for ontology and personas with cluster mappings  
✅ **Semantic Mapper Module** - Heuristic mapper implemented with DB persistence  
✅ **Config Sync** - Utility script to populate DB from YAML on startup  
✅ **Persona Refactor** - Replaced hardcoded mappings with `PersonaView` DB queries  
✅ **DCL Engine Refactor** - Reads stored mappings, falls back to live creation  
✅ **Batch Mapping API** - `/api/dcl/batch-mapping` endpoint for triggering mapper  
✅ **Explanation Data** - Ontology nodes include field contribution explanations  
✅ **Testing** - Demo and Farm modes tested with stored mappings  
✅ **Documentation** - Architecture docs created

## Acceptance Criteria

✅ No hard-coded persona→concept lists remain in code  
✅ DCL runtime does NOT call LLM/RAG at request time  
✅ All persona filtering uses database queries  
✅ Farm mode produces intelligible graphs for all personas  
✅ Clear documentation exists (ARCH-DCL-CURRENT.md, ARCH-DCL-TARGET.md)

## Delivered Artifacts

### Database Schema
- `ontology_concepts` - 8 concepts with Finance/Growth/Infra/Ops clusters
- `field_concept_mappings` - 114 mappings (103 Demo + 11 Farm)
- `persona_profiles` - 4 personas (CFO, CRO, COO, CTO)
- `persona_concept_relevance` - 18 relevance mappings

### Code Modules
- `backend/semantic_mapper/` - Mapping pipeline
- `backend/engine/persona_view.py` - DB-driven persona logic
- `backend/utils/config_sync.py` - Config sync utility
- `backend/api/main.py` - Batch mapping endpoint

### Configuration
- `config/ontology_concepts.yaml` - Ontology with synonyms and example fields
- `config/persona_profiles.yaml` - Persona profiles with relevance scores

### Documentation
- `docs/ARCH-DCL-CURRENT.md` - Current architecture reference
- `docs/ARCH-DCL-TARGET.md` - This document (target state)

## What's Next

The core refactoring is complete. Optional enhancements:

1. **RAG/LLM Stages:** Extend semantic mapper with RAG similarity search and LLM refinement
2. **Auto-Remapping:** Trigger batch mapping when sources are added/updated
3. **Frontend Debug View:** Add mapping explanation tooltips to ontology nodes
4. **Cluster Filtering:** Allow users to filter by concept cluster
5. **Confidence Tuning:** Add UI to adjust confidence thresholds
