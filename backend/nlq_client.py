"""
NLQ Graph Client — resolves natural-language questions via DCL's graph.

This module provides the integration layer between NLQ and DCL:
  1. parse_intent()        — extracts structured QueryIntent from plain English
  2. resolve_via_graph()   — calls POST /api/dcl/resolve
  3. resolve_via_flat()    — falls back to POST /api/dcl/query
  4. resolve_question()    — graph-first, flat-fallback orchestrator

This lives in DCLv2 to demonstrate the pattern; the AOS-NLQ repo would
import this or re-implement the same logic with its HTTP client.
"""

import re
from typing import Dict, List, Optional, Tuple

from backend.domain import (
    FilterClause,
    QueryIntent,
    QueryResolution,
    ProvenanceStep,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


# ── Lightweight intent parser ─────────────────────────────────────────
# This is a simplified intent extractor for integration testing.
# The real NLQ parser lives in AOS-NLQ and is more sophisticated.

# Concept keywords → canonical concept IDs
_CONCEPT_KEYWORDS: Dict[str, str] = {
    "revenue": "revenue",
    "total revenue": "revenue",
    "sales": "revenue",
    "income": "revenue",
    "arr": "arr",
    "mrr": "mrr",
    "headcount": "headcount",
    "employees": "headcount",
    "employee": "headcount",
    "pipeline": "pipeline",
    "cost": "cost",
    "spend": "cloud_spend",
    "cloud spend": "cloud_spend",
    "cloud cost": "cloud_cost",
    "churn": "churn_rate",
    "attrition": "attrition_rate",
    "sprint velocity": "throughput",
    "velocity": "throughput",
    "uptime": "uptime",
    "mttr": "mttr",
    "deploy": "deploy_frequency",
    "deployment": "deploy_frequency",
    "win rate": "win_rate",
    "quota": "quota_attainment",
    "nrr": "nrr",
    "engagement": "engagement_score",
    "cycle time": "cycle_time",
    "sla": "sla_compliance",
    "slo": "slo_attainment",
    "burn rate": "burn_rate",
    "gross margin": "gross_margin",
    "training": "training_hours",
    "florbatz": "florbatz",  # Unknown concept for negative test
}

# Dimension keywords → canonical dimension IDs
_DIMENSION_KEYWORDS: Dict[str, str] = {
    "region": "region",
    "geography": "region",
    "department": "department",
    "dept": "department",
    "team": "team",
    "cost center": "cost_center",
    "cost centers": "cost_center",
    "division": "division",
    "segment": "segment",
    "product": "product",
    "customer": "customer",
    "service": "service",
    "rep": "rep",
    "stage": "stage",
    "cohort": "cohort",
    "severity": "severity",
    "profit center": "profit_center",
    "board segment": "board_segment",
    "resource type": "resource_type",
}


def parse_intent(question: str) -> QueryIntent:
    """
    Extract a structured QueryIntent from a natural-language question.

    This is a keyword-based parser for integration testing. The real NLQ
    parser uses LLM-based intent extraction.
    """
    q_lower = question.lower()

    # Step 1: Extract dimensions first (longest match first) and mask them
    dimensions: List[str] = []
    seen_dims: set = set()
    masked = q_lower  # working copy with dimension phrases masked
    sorted_dim_keywords = sorted(_DIMENSION_KEYWORDS.keys(), key=len, reverse=True)
    for keyword in sorted_dim_keywords:
        if keyword in masked and _DIMENSION_KEYWORDS[keyword] not in seen_dims:
            dimensions.append(_DIMENSION_KEYWORDS[keyword])
            seen_dims.add(_DIMENSION_KEYWORDS[keyword])
            # Mask so "cost center" doesn't let "cost" match as concept
            masked = masked.replace(keyword, " _DIM_ ")

    # Step 2: Extract concepts from the masked text (longest match first)
    concepts: List[str] = []
    seen_concepts: set = set()
    sorted_keywords = sorted(_CONCEPT_KEYWORDS.keys(), key=len, reverse=True)
    for keyword in sorted_keywords:
        if keyword in masked and _CONCEPT_KEYWORDS[keyword] not in seen_concepts:
            concepts.append(_CONCEPT_KEYWORDS[keyword])
            seen_concepts.add(_CONCEPT_KEYWORDS[keyword])

    # Step 3: Extract filters from the original text
    filters: List[FilterClause] = []
    seen_filter_dims: set = set()
    filter_patterns = [
        # "for the Cloud division"
        r"for (?:the )?(\w+(?:\s+\w+)?)\s+(division|department|region|segment|cost centers?)",
        # "for Engineering cost centers"
        r"for (\w+(?:\s+\w+)?)\s+(cost centers?|departments?|divisions?)",
    ]
    for pattern in filter_patterns:
        match = re.search(pattern, q_lower)
        if match:
            value = match.group(1).strip()
            dim = match.group(2).strip().rstrip("s")  # normalize plural
            dim_id = _DIMENSION_KEYWORDS.get(dim, dim)
            if value not in _CONCEPT_KEYWORDS and dim_id not in seen_filter_dims:
                filters.append(FilterClause(dimension=dim_id, value=value.title()))
                seen_filter_dims.add(dim_id)

    return QueryIntent(
        concepts=concepts,
        dimensions=dimensions,
        filters=filters,
    )


# ── Graph resolution (calls POST /api/dcl/resolve internally) ────────

def resolve_via_graph(intent: QueryIntent) -> QueryResolution:
    """
    Resolve a QueryIntent via DCL's graph traversal engine.

    In production NLQ, this would be an HTTP call to POST /api/dcl/resolve.
    Here we call the resolver directly since we're in the same process.
    """
    from backend.graph_resolver import resolve
    return resolve(intent)


# ── Flat query fallback (calls POST /api/dcl/query internally) ────────

def resolve_via_flat(intent: QueryIntent) -> Optional[QueryResolution]:
    """
    Fall back to DCL's flat query endpoint.

    Translates the intent into a QueryRequest and checks if data exists.
    """
    from backend.api.query import QueryRequest, handle_query, QueryError

    if not intent.concepts:
        return None

    metric = intent.concepts[0]
    filters_dict = {f.dimension: f.value for f in intent.filters}

    request = QueryRequest(
        metric=metric,
        dimensions=intent.dimensions,
        filters=filters_dict,
    )

    result = handle_query(request)
    if isinstance(result, QueryError):
        return QueryResolution(
            can_answer=False,
            reason=f"Flat query failed: {result.error}",
            concepts_found=[],
        )

    return QueryResolution(
        can_answer=True,
        concepts_found=[metric],
        dimensions_used=intent.dimensions,
        confidence=0.6,  # Lower confidence for flat path
        provenance=[],
        warnings=["Resolved via flat query (no graph traversal)"],
    )


# ── Orchestrator: graph-first, flat-fallback ──────────────────────────

def resolve_question(question: str) -> Dict:
    """
    Full NLQ → DCL resolution pipeline.

    1. Parse the natural-language question into QueryIntent
    2. Try graph resolution first
    3. If graph can't answer, fall back to flat query
    4. Format the result for the user

    Returns a dict with the resolution result and metadata.
    """
    # Step 1: Parse intent
    intent = parse_intent(question)
    logger.info(f"[NLQ] Parsed intent: concepts={intent.concepts}, dims={intent.dimensions}, filters={[f.model_dump() for f in intent.filters]}")

    # Step 2: Try graph resolution
    resolution = resolve_via_graph(intent)

    resolution_path = "graph"

    # Step 3: Fall back to flat query if graph can't answer
    if not resolution.can_answer:
        flat_result = resolve_via_flat(intent)
        if flat_result and flat_result.can_answer:
            resolution = flat_result
            resolution_path = "flat_fallback"

    # Step 4: Format for user
    return {
        "question": question,
        "intent": intent.model_dump(),
        "resolution": resolution.model_dump(),
        "resolution_path": resolution_path,
        "answer_summary": _format_answer(question, resolution),
    }


def _format_answer(question: str, resolution: QueryResolution) -> str:
    """Format a QueryResolution into a human-readable answer summary."""
    if not resolution.can_answer:
        return f"Cannot answer: {resolution.reason or 'Unknown reason'}"

    parts = [f"Found: {', '.join(resolution.concepts_found)}"]

    if resolution.dimensions_used:
        parts.append(f"sliced by {', '.join(resolution.dimensions_used)}")

    if resolution.primary_system:
        parts.append(f"primary source: {resolution.primary_system}")

    if resolution.filters_resolved:
        for fr in resolution.filters_resolved:
            if len(fr.resolved_to) > 1:
                parts.append(f"'{fr.value}' expanded to {fr.resolved_to}")

    parts.append(f"confidence: {resolution.confidence}")

    if resolution.warnings:
        parts.append(f"warnings: {len(resolution.warnings)}")

    return " | ".join(parts)
