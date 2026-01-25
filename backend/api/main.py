import os
import json
import uuid
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Literal, Optional, Dict, Any
from backend.domain import Persona, GraphSnapshot, RunMetrics
from backend.engine import DCLEngine
from backend.engine.schema_loader import SchemaLoader
from backend.semantic_mapper import SemanticMapper
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

from backend.core.security_constraints import (
    validate_no_disk_payload_writes,
    assert_metadata_only_mode,
)

app = FastAPI(title="DCL Engine API")


@app.on_event("startup")
async def enforce_security_constraints():
    """Enforce Zero-Trust metadata-only constraints at startup."""
    logger.info("=== DCL Zero-Trust Security Check ===")
    
    try:
        assert_metadata_only_mode()
        logger.info("[SECURITY] Metadata-only mode: ENABLED")
    except Exception as e:
        logger.warning(f"[SECURITY] Metadata-only assertion failed: {e}")
    
    violations = validate_no_disk_payload_writes()
    if violations:
        logger.warning(f"[SECURITY] Found {len(violations)} potential payload write paths:")
        for v in violations[:5]:
            logger.warning(f"  - {v}")
        logger.warning("[SECURITY] Review ARCH-GLOBAL-PIVOT.md for migration guidance")
    else:
        logger.info("[SECURITY] No payload write violations detected")
    
    logger.info("=== DCL Engine Ready (Metadata-Only Mode) ===")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = DCLEngine()


class RunRequest(BaseModel):
    mode: Literal["Demo", "Farm"] = "Demo"
    run_mode: Literal["Dev", "Prod"] = "Dev"
    personas: Optional[List[Persona]] = None
    source_limit: Optional[int] = 5  # Number of sources to fetch from Farm


class RunResponse(BaseModel):
    graph: GraphSnapshot
    run_metrics: RunMetrics
    run_id: str


@app.get("/api/health")
def health():
    from backend.bll.definitions import list_definitions
    return {
        "status": "DCL Engine API is running",
        "version": "1.0.0",
        "bll_definitions": len(list_definitions()),  # Should be 16 after update
    }


@app.post("/api/dcl/run", response_model=RunResponse)
def run_dcl(request: RunRequest):
    run_id = str(uuid.uuid4())
    
    personas = request.personas or [Persona.CFO, Persona.CRO, Persona.COO, Persona.CTO]
    
    try:
        snapshot, metrics = engine.build_graph_snapshot(
            mode=request.mode,
            run_mode=request.run_mode,
            personas=personas,
            run_id=run_id,
            source_limit=request.source_limit or 5
        )
        
        return RunResponse(
            graph=snapshot,
            run_metrics=metrics,
            run_id=run_id
        )
    except Exception as e:
        logger.error(f"DCL run failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dcl/narration/{run_id}")
def get_narration(run_id: str):
    messages = engine.narration.get_messages(run_id)
    return {"run_id": run_id, "messages": messages}


@app.post("/api/ingest/provision")
@app.get("/api/ingest/provision")
def ingest_provision_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


@app.get("/api/ingest/config")
def ingest_config_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


@app.get("/api/dcl/monitor/{run_id}")
def get_monitor(run_id: str):
    return {
        "run_id": run_id,
        "monitor_data": {
            "message": "Monitor data endpoint ready",
            "sources": [],
            "ontology": [],
            "conflicts": []
        }
    }


@app.get("/api/ingest/telemetry")
def ingest_telemetry_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


class MappingRequest(BaseModel):
    mode: Literal["Demo", "Farm"] = "Demo"
    mapping_mode: Literal["heuristic", "full"] = "heuristic"
    clear_existing: bool = False


class MappingResponse(BaseModel):
    status: str
    mappings_created: int
    sources_processed: int
    stats: dict


@app.post("/api/dcl/batch-mapping", response_model=MappingResponse)
def run_batch_mapping(request: MappingRequest):
    
    try:
        if request.mode == "Demo":
            sources = SchemaLoader.load_demo_schemas()
        else:
            sources = SchemaLoader.load_farm_schemas(engine.narration, str(uuid.uuid4()))
        
        semantic_mapper = SemanticMapper()
        mappings, stats = semantic_mapper.run_mapping(
            sources=sources,
            mode=request.mapping_mode,
            clear_existing=request.clear_existing
        )
        
        return MappingResponse(
            status="success",
            mappings_created=stats['mappings_created'],
            sources_processed=stats['sources_processed'],
            stats=stats
        )
    except Exception as e:
        logger.error(f"Batch mapping failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


from backend.core.topology_api import topology_api, ConnectionHealth, ConnectionStatus

# =============================================================================
# NLQ Answerability Circles Endpoints
# =============================================================================

from backend.nlq import (
    AnswerabilityRequest,
    AnswerabilityResponse,
    ExplainRequest,
    ExplainResponse,
    AnswerabilityScorer,
    NLQPersistence,
    # Data model types for registration
    CanonicalEvent,
    Entity,
    Binding,
    Definition,
    DefinitionVersion,
    DefinitionVersionSpec,
    ProofHook,
)
from backend.nlq.explainer import HypothesisExplainer
from backend.nlq.routes_registry import router as registry_router
from backend.bll.routes import router as bll_router
from backend.farm.routes import router as farm_router
from backend.dcl.routes import router as dcl_router

# Include routers
app.include_router(registry_router)
app.include_router(bll_router)
app.include_router(farm_router)
app.include_router(dcl_router)

# Initialize NLQ components
nlq_persistence = NLQPersistence()
answerability_scorer = AnswerabilityScorer(persistence=nlq_persistence)
hypothesis_explainer = HypothesisExplainer(persistence=nlq_persistence)


@app.post("/api/nlq/answerability_rank", response_model=AnswerabilityResponse)
def rank_answerability(request: AnswerabilityRequest):
    """
    Rank hypotheses for a natural language question.

    Returns 2-3 "answer circles" (hypotheses) with:
    - size = probability_of_answer
    - rank = left→right order (most likely answerable first)
    - color = confidence (evidence quality: hot/warm/cool)
    - extracted_params: limit, time_window, order_by extracted from question

    No LLM calls in the hot path. Uses deterministic rules + stored metadata.

    Example request:
    {
        "question": "Services revenue (25% of total) is down 50% QoQ — what's happening?",
        "tenant_id": "t_123",
        "context": {
            "time_window": "QoQ",
            "metric_hint": "services_revenue"
        }
    }
    """
    from backend.nlq.param_extractor import extract_params, apply_limit_clamp
    from backend.nlq.models import ExtractedParams

    try:
        # Rank hypotheses
        circles = answerability_scorer.rank_hypotheses(
            question=request.question,
            tenant_id=request.tenant_id,
            context=request.context,
        )

        # Check if clarification needed
        needs_context = answerability_scorer.get_needs_context(circles)

        # Extract execution parameters from the question
        exec_args = extract_params(request.question)
        extracted_limit = None
        if exec_args.limit:
            extracted_limit = apply_limit_clamp(exec_args.limit, max_limit=100)

        extracted_params = ExtractedParams(
            limit=extracted_limit,
            time_window=exec_args.time_window,
            order_by=None,  # Ordering handled by definition spec, not NLQ inference
        )

        return AnswerabilityResponse(
            question=request.question,
            circles=circles,
            needs_context=needs_context,
            extracted_params=extracted_params,
        )
    except Exception as e:
        logger.error(f"Answerability ranking failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# NLQ Ask Endpoint (Ranking + Param Extraction + BLL Execution)
# =============================================================================

from backend.nlq.param_extractor import extract_params, apply_limit_clamp, ExecutionArgs
from backend.bll.executor import execute_definition as bll_execute
from backend.bll.models import ExecuteRequest as BLLExecuteRequest
from backend.bll.definitions import get_definition as get_bll_definition, list_definitions as list_bll_definitions
from backend.dcl.definitions.registry import DefinitionKind


class NLQAskRequest(BaseModel):
    """Request to ask a natural language question."""
    question: str
    tenant_id: str = "default"
    dataset_id: str = "demo9"


class NLQExtractParamsRequest(BaseModel):
    """Request to extract execution parameters from a question."""
    question: str


class NLQExtractParamsResponse(BaseModel):
    """Response with extracted execution parameters.
    
    PRODUCTION BOUNDARY: NLQ extracts TopN(limit) only. Ordering is declared in
    definition.capabilities.default_order_by, not inferred by NLQ.
    """
    question: str
    limit: Optional[int] = None
    time_window: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    extraction_confidence: float = 1.0  # How confident we are in the extraction
    raw_params: Dict[str, Any] = {}  # Full ExecutionArgs dict


class NLQClarificationCandidate(BaseModel):
    """A candidate definition for clarification."""
    definition_id: str
    name: str
    description: str
    score: float


class NLQAskResponse(BaseModel):
    """Response from NLQ ask endpoint."""
    question: str
    definition_id: str
    confidence_score: float
    execution_args: Dict[str, Any]
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    summary: Optional[Dict[str, Any]] = None
    caveats: List[str] = []
    # Clarification fields (when needs_clarification=True)
    needs_clarification: bool = False
    clarification_prompt: Optional[str] = None
    candidates: Optional[List[NLQClarificationCandidate]] = None


import re
from difflib import SequenceMatcher

# Synonym mappings for common terms
SYNONYMS = {
    "slo": ["slo", "service level", "uptime", "availability", "reliability"],
    "sla": ["sla", "service level agreement", "availability"],
    "arr": ["arr", "annual recurring revenue", "recurring revenue"],
    "mrr": ["mrr", "monthly recurring revenue"],
    "burn": ["burn", "burn rate", "cash burn", "spending rate", "runway"],
    "mttr": ["mttr", "mean time to recovery", "recovery time", "time to recover"],
    "deploy": ["deploy", "deployment", "release", "ship", "push to prod"],
    "incident": ["incident", "outage", "page", "alert", "sev1", "sev2"],
    "customer": ["customer", "client", "account", "buyer"],
    "revenue": ["revenue", "sales", "income", "earnings"],
    "cost": ["cost", "spend", "spending", "expense", "price"],
    "zombie": ["zombie", "idle", "unused", "orphan", "wasted"],
    "trend": ["trend", "trending", "over time", "change", "growth"],
    "dora": ["dora", "dora metrics", "four keys", "engineering metrics"],
}


def _tokenize(text: str) -> set[str]:
    """Extract word tokens from text."""
    return set(re.findall(r'\b[a-z0-9]+\b', text.lower()))


def _fuzzy_match(word: str, target: str, threshold: float = 0.8) -> bool:
    """Check if word fuzzy-matches target (handles typos)."""
    if len(word) < 3 or len(target) < 3:
        return word == target
    return SequenceMatcher(None, word, target).ratio() >= threshold


def _expand_synonyms(tokens: set[str]) -> set[str]:
    """Expand tokens with synonyms."""
    expanded = set(tokens)
    for token in tokens:
        for key, synonyms in SYNONYMS.items():
            if token in synonyms or _fuzzy_match(token, key):
                expanded.update(synonyms)
    return expanded


def _match_question_to_definition(question: str) -> tuple[str, float, list[str]]:
    """
    Match a question to the best BLL definition using improved NLP matching.

    Features:
    - Word tokenization with boundary detection
    - Synonym expansion
    - Fuzzy matching for typo tolerance
    - Description matching
    - Multi-keyword boost

    Returns (definition_id, confidence_score, matched_keywords).
    """
    question_lower = question.lower()
    question_tokens = _tokenize(question_lower)
    expanded_tokens = _expand_synonyms(question_tokens)

    best_match = None
    best_score = 0.0
    best_keywords = []

    # Category-specific term weights
    category_terms = {
        "finops": {"spend": 0.15, "cost": 0.15, "revenue": 0.15, "arr": 0.2,
                   "burn": 0.15, "saas": 0.1, "mrr": 0.15, "budget": 0.1},
        "aod": {"zombie": 0.2, "finding": 0.15, "security": 0.15, "identity": 0.15,
                "idle": 0.15, "orphan": 0.1, "unowned": 0.15, "gap": 0.1},
        "crm": {"customer": 0.15, "deal": 0.15, "pipeline": 0.15, "account": 0.15,
                "opportunity": 0.1, "sales": 0.1},
        "infra": {"slo": 0.25, "sla": 0.2, "deploy": 0.2, "mttr": 0.25,
                  "incident": 0.2, "dora": 0.25, "uptime": 0.15, "availability": 0.15},
    }

    for defn in list_bll_definitions():
        score = 0.0
        matched = []

        # 1. Check explicit keywords (highest weight)
        for kw in defn.keywords:
            kw_lower = kw.lower()
            kw_tokens = _tokenize(kw_lower)

            # Exact phrase match
            if kw_lower in question_lower:
                score += 0.35
                matched.append(f"kw:{kw}")
            # Token overlap match
            elif kw_tokens & expanded_tokens:
                overlap = len(kw_tokens & expanded_tokens) / len(kw_tokens)
                score += 0.25 * overlap
                matched.append(f"kw~:{kw}")

        # 2. Check definition name
        name_tokens = _tokenize(defn.name.lower())
        name_overlap = len(name_tokens & expanded_tokens)
        if name_overlap > 0:
            score += 0.2 * (name_overlap / len(name_tokens))
            matched.append(f"name:{defn.name}")

        # 3. Check description words
        desc_tokens = _tokenize(defn.description.lower())
        desc_overlap = len(desc_tokens & expanded_tokens)
        if desc_overlap >= 2:
            score += 0.1 * min(desc_overlap / 5, 1.0)
            matched.append(f"desc:{desc_overlap}words")

        # 4. Check category-specific terms with weighted scoring
        cat_terms = category_terms.get(defn.category.value, {})
        for term, weight in cat_terms.items():
            if term in expanded_tokens:
                score += weight
                if term not in [m.split(":")[-1] for m in matched]:
                    matched.append(f"cat:{term}")

        # 5. Fuzzy match against definition ID parts
        defn_id_parts = defn.definition_id.replace(".", "_").split("_")
        for part in defn_id_parts:
            for token in question_tokens:
                if _fuzzy_match(token, part, 0.85):
                    score += 0.15
                    matched.append(f"id~:{part}")
                    break

        # 6. Multi-keyword boost (more matches = higher confidence)
        if len(matched) >= 3:
            score *= 1.2
        elif len(matched) >= 2:
            score *= 1.1

        # Cap at 0.99
        score = min(score, 0.99)

        if score > best_score:
            best_score = score
            best_match = defn.definition_id
            best_keywords = matched

    # Default fallback with low confidence
    if not best_match or best_score < 0.1:
        best_match = "finops.arr"
        best_score = 0.1
        best_keywords = ["fallback:default"]

    return best_match, best_score, best_keywords


# =============================================================================
# NLQ Parameter Extraction Endpoint
# =============================================================================

@app.post("/api/nlq/extract_params", response_model=NLQExtractParamsResponse)
def nlq_extract_params(request: NLQExtractParamsRequest):
    """
    Extract execution parameters from a natural language question.

    This is a standalone endpoint that BLL can call to get extracted parameters
    without executing a query. Use this with answerability_rank + execute flow.

    Extracts:
    - limit: "top 5", "first 10", etc.
    - order_by: "by revenue", "sorted by cost", etc.
    - time_window: "last month", "this quarter", "YTD", etc.
    - filters: (reserved for future use)

    Example:
    {
        "question": "Show me the top 5 customers by revenue"
    }

    Returns:
    {
        "question": "Show me the top 5 customers by revenue",
        "limit": 5,
        "order_by": [{"field": "revenue", "direction": "desc"}],
        "time_window": null,
        "filters": null,
        "extraction_confidence": 1.0,
        "raw_params": {"limit": 5, "order_by": [...]}
    }
    """
    try:
        question = request.question

        # Extract all parameters using the param_extractor module
        exec_args = extract_params(question)

        # Apply limit clamping (max 100 for safety)
        if exec_args.limit:
            exec_args.limit = apply_limit_clamp(exec_args.limit, max_limit=100)

        # Calculate extraction confidence based on how many params were found
        confidence = 1.0
        if exec_args.has_params():
            # High confidence if we found explicit parameters
            confidence = 0.95
        else:
            # No parameters found - either question doesn't have any, or we missed them
            confidence = 0.8

        logger.info(f"[NLQ] Extracted params from '{question}': {exec_args.to_dict()}")

        return NLQExtractParamsResponse(
            question=question,
            limit=exec_args.limit,
            time_window=exec_args.time_window,
            filters=exec_args.filters,
            extraction_confidence=confidence,
            raw_params=exec_args.to_dict(),
        )
    except Exception as e:
        logger.error(f"Parameter extraction failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Parameter extraction failed: {str(e)}")


@app.get("/api/nlq/extract_params")
def nlq_extract_params_get(question: str):
    """
    GET version of extract_params for easy testing.

    Example: GET /api/nlq/extract_params?question=Show%20me%20top%205%20customers
    """
    return nlq_extract_params(NLQExtractParamsRequest(question=question))


@app.post("/api/nlq/ask", response_model=NLQAskResponse)
def nlq_ask(request: NLQAskRequest):
    """
    Ask a natural language question and get an answer.

    Flow:
    1. Match question to best BLL definition (keyword-based)
    2. Extract parameters from question (top N, etc.)
    3. Execute definition with extracted parameters
    4. Store in history for replay
    5. Return data + computed summary

    Key Fix: If user asks "top 5", limit=5 is extracted and applied.
    If limit is missing for ranked-list definitions, a warning is added.

    Example:
    {
        "question": "Show me the top 5 customers by revenue"
    }

    Returns:
    - definition_id: matched definition
    - confidence_score: match confidence (0.0-1.0)
    - execution_args: extracted parameters (limit, etc.)
    - data: query results (limited to extracted top-N)
    - summary: human-readable answer with aggregations
    - caveats: any limitations applied
    """
    import time
    from backend.nlq.intent_matcher import match_question_with_details, AMBIGUOUS_GROUPS
    from backend.bll.definitions import get_definition
    from backend.dcl.history.persistence import get_history_store
    from backend.dcl.definitions.registry import DefinitionRegistry
    from backend.nlq.normalized_intent import (
        extract_normalized_intent,
        validate_output_against_intent,
        OutputShape,
        IntentViolationError,
    )

    start_time = time.time()

    try:
        question = request.question

        # Step 1: Match question to definition with ambiguity detection
        match_result = match_question_with_details(question, top_k=5)
        definition_id = match_result.best_match
        confidence = match_result.confidence
        matched_keywords = match_result.matched_keywords

        logger.info(f"[NLQ] Matched '{question}' to {definition_id} (conf={confidence:.2f}, ambiguous={match_result.is_ambiguous})")

        # Check for delta capability mismatch (query needs delta but definition doesn't support)
        delta_capability_mismatch = False
        if match_result.operators and match_result.operators.requires_delta:
            matched_defn = get_definition(definition_id)
            if matched_defn and hasattr(matched_defn, 'capabilities'):
                if not matched_defn.capabilities.supports_delta:
                    delta_capability_mismatch = True
                    logger.info(f"[NLQ] Delta capability mismatch: query needs delta but {definition_id} doesn't support it")

        # Step 1.5: Check for ambiguity - return NEEDS_CLARIFICATION if ambiguous
        is_truly_ambiguous = match_result.is_ambiguous and len(match_result.top_candidates) >= 2
        if delta_capability_mismatch:
            is_truly_ambiguous = False

        if is_truly_ambiguous:
            candidates = []
            for candidate in match_result.top_candidates[:4]:
                defn = get_definition(candidate.definition_id)
                if defn:
                    candidates.append(NLQClarificationCandidate(
                        definition_id=candidate.definition_id,
                        name=defn.name,
                        description=defn.description,
                        score=round(candidate.score, 3),
                    ))

            clarification_msg = "Your question matches multiple definitions. Which one did you mean?"
            question_lower = question.lower()
            for group_key, group_info in AMBIGUOUS_GROUPS.items():
                if group_key in question_lower:
                    clarification_msg = group_info.get("clarification", clarification_msg)
                    break

            return NLQAskResponse(
                question=question,
                definition_id=definition_id,
                confidence_score=confidence,
                execution_args={},
                data=[],
                metadata={
                    "dataset_id": request.dataset_id,
                    "ambiguity_gap": round(match_result.ambiguity_gap, 3),
                },
                summary=None,
                caveats=["Ambiguous query - clarification needed"],
                needs_clarification=True,
                clarification_prompt=clarification_msg,
                candidates=candidates,
            )

        # Step 2: Extract parameters from question
        exec_args = extract_params(question)

        # Apply limit clamping (max 100)
        if exec_args.limit:
            exec_args.limit = apply_limit_clamp(exec_args.limit, max_limit=100)

        logger.info(f"[NLQ] Extracted params: {exec_args.to_dict()}")

        # Step 2.5: Extract NORMALIZED INTENT (BINDING CONTRACT)
        # output_shape is derived from INTENT, NOT from definition
        intent_result = extract_normalized_intent(question, definition_id, confidence)
        normalized_intent = intent_result.intent

        logger.info(f"[NLQ] Intent: output_shape={normalized_intent.output_shape.value if normalized_intent else 'none'}, "
                    f"aggregation={normalized_intent.aggregation.value if normalized_intent else 'none'}")

        # Step 2.6: Handle execution based on INTENT output_shape
        # CRITICAL: output_shape is BINDING - it determines what execution can return
        is_ranked_list = DefinitionRegistry.is_ranked_list(definition_id)
        meta = DefinitionRegistry.get_metadata(definition_id)
        default_limit = DefinitionRegistry.get_default_limit(definition_id)
        effective_limit = exec_args.limit
        limit_warning = None

        # INTENT-BASED EXECUTION CONTROL (replaces definition-based)
        is_scalar_intent = normalized_intent and normalized_intent.output_shape == OutputShape.SCALAR

        if is_scalar_intent:
            # SCALAR intent: get all data for aggregation, but NO ranking
            effective_limit = 1000  # Get all data
            logger.info(f"[NLQ] SCALAR intent - returning aggregate total, NO ranking")
        elif is_ranked_list and exec_args.limit is None:
            if default_limit:
                effective_limit = default_limit
                logger.info(f"[NLQ] Using default limit {default_limit} for {definition_id}")
            else:
                limit_warning = "MISSING_LIMIT: No limit specified for ranked list query"
                effective_limit = 1000

        # Step 3: Execute definition with extracted parameters
        bll_request = BLLExecuteRequest(
            dataset_id=request.dataset_id,
            definition_id=definition_id,
            limit=effective_limit or 1000,
            offset=0,
        )

        result = bll_execute(bll_request)

        # Step 3.5: HARD INTENT GATE - validate output against intent
        # For SCALAR intent, we MUST NOT return ranked data
        if normalized_intent and is_scalar_intent:
            # Extract aggregations from result
            aggregations = result.summary.aggregations if result.summary else {}

            # Validate: SCALAR intent must not have ranking indicators
            try:
                validate_output_against_intent(
                    normalized_intent,
                    result.data,
                    aggregations,
                    limit_applied=effective_limit if effective_limit and effective_limit < 1000 else None
                )
            except IntentViolationError as e:
                logger.error(f"[NLQ] Intent violation: {e}")
                # Don't fail the request, but log and fix the response
                pass

            # For SCALAR intent: suppress rows, return only the aggregate
            # This is the BINDING enforcement
            scalar_data = []  # NO rows for scalar queries
            if result.summary:
                # Fix the summary to be scalar-appropriate
                agg = result.summary.aggregations
                if agg.get("population_total"):
                    result.summary.answer = f"Your current {normalized_intent.metric.upper()} is ${agg['population_total']/1_000_000:,.2f}M"
        else:
            scalar_data = result.data

        # Step 4: Build response with caveats
        caveats = []
        if delta_capability_mismatch:
            metric = match_result.operators.metric_type if match_result.operators else "this metric"
            caveats.append(f"Month-over-month comparison not available for {metric}; showing current values")
        if limit_warning:
            caveats.append(limit_warning)
        if exec_args.limit and not is_scalar_intent:
            caveats.append(f"Limited to top {exec_args.limit}")
            caveats.append("Sorted by definition default ordering")
        if not caveats:
            caveats.append("Based on available data bindings")

        # Compute execution time
        execution_time_ms = int((time.time() - start_time) * 1000)

        # Use scalar_data for SCALAR intent, else full data
        response_data = scalar_data if is_scalar_intent else result.data

        response = NLQAskResponse(
            question=question,
            definition_id=definition_id,
            confidence_score=confidence,
            execution_args=exec_args.to_dict(),
            data=response_data,
            metadata={
                "dataset_id": result.metadata.dataset_id,
                "definition_id": result.metadata.definition_id,
                "row_count": len(response_data),
                "total_available": result.metadata.row_count,
                "execution_time_ms": execution_time_ms,
                "matched_keywords": matched_keywords,
                "effective_limit": effective_limit,
                "intent_output_shape": normalized_intent.output_shape.value if normalized_intent else None,
            },
            summary=result.summary.model_dump() if result.summary else None,
            caveats=caveats,
            needs_clarification=False,
        )

        # Step 5: Store in history for replay
        try:
            history_store = get_history_store()
            history_store.add(
                question=question,
                dataset_id=request.dataset_id,
                definition_id=definition_id,
                extracted_params=exec_args.to_dict(),
                response=response.model_dump(),
                latency_ms=execution_time_ms,
                status="success",
                tenant_id=request.tenant_id,
            )
        except Exception as hist_err:
            logger.warning(f"[NLQ] Failed to store history: {hist_err}")

        return response

    except Exception as e:
        logger.error(f"NLQ ask failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"DCL execution failed: {str(e)}")


@app.post("/api/nlq/explain", response_model=ExplainResponse)
def explain_hypothesis(request: ExplainRequest):
    """
    Get a deterministic explanation for a hypothesis.

    Returns a short explanation with:
    - headline: Summary of the finding
    - why: List of supporting facts with confidence
    - go_deeper: Bridge analysis and drilldown options
    - proof: Source system pointers and query hashes
    - next: Suggested next actions

    For MVP, facts and proof are stubbed. No real query execution.

    Example request:
    {
        "question": "Services revenue is down 50% QoQ — what's happening?",
        "tenant_id": "t_123",
        "hypothesis_id": "h_volume",
        "plan_id": "plan_services_rev_bridge"
    }
    """
    try:
        response = hypothesis_explainer.explain(request)
        return response
    except Exception as e:
        logger.error(f"Explanation generation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# NLQ Registration API - Semantic Layer Management
# =============================================================================


@app.post("/api/nlq/bindings", response_model=Binding, tags=["NLQ Registration"])
def register_binding(binding: Binding):
    """
    Register or update a source system binding.

    Bindings map source system fields to canonical event fields.
    This enables the semantic layer to understand how source data
    relates to canonical business events.

    Example:
    {
        "id": "netsuite_revenue",
        "tenant_id": "t_123",
        "source_system": "NetSuite",
        "canonical_event_id": "revenue_recognized",
        "mapping_json": {
            "tran_date": "recognized_at",
            "amount": "amount",
            "customer": "customer_id"
        },
        "dims_coverage_json": {
            "customer": true,
            "service_line": true
        },
        "quality_score": 0.9,
        "freshness_score": 0.95
    }
    """
    try:
        return nlq_persistence.register_binding(binding)
    except Exception as e:
        logger.error(f"Binding registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/bindings", response_model=List[Binding], tags=["NLQ Registration"])
def list_bindings(tenant_id: str = "default"):
    """List all bindings for a tenant."""
    return nlq_persistence.get_bindings(tenant_id)


@app.get("/api/nlq/bindings/{binding_id}", response_model=Binding, tags=["NLQ Registration"])
def get_binding(binding_id: str, tenant_id: str = "default"):
    """Get a specific binding by ID."""
    bindings = nlq_persistence.get_bindings(tenant_id)
    for b in bindings:
        if b.id == binding_id:
            return b
    raise HTTPException(status_code=404, detail=f"Binding {binding_id} not found")


@app.delete("/api/nlq/bindings/{binding_id}", tags=["NLQ Registration"])
def delete_binding(binding_id: str, tenant_id: str = "default"):
    """Delete a binding."""
    if nlq_persistence.delete_binding(binding_id, tenant_id):
        return {"status": "deleted", "id": binding_id}
    raise HTTPException(status_code=404, detail=f"Binding {binding_id} not found")


@app.post("/api/nlq/events", response_model=CanonicalEvent, tags=["NLQ Registration"])
def register_event(event: CanonicalEvent):
    """
    Register or update a canonical event type.

    Canonical events are system-agnostic business event types like
    revenue_recognized, invoice_posted, contract_signed.

    Example:
    {
        "id": "revenue_recognized",
        "tenant_id": "t_123",
        "schema_json": {
            "fields": [
                {"name": "amount", "type": "decimal"},
                {"name": "customer_id", "type": "string"},
                {"name": "recognized_at", "type": "timestamp"}
            ]
        },
        "time_semantics_json": {
            "event_time": "recognized_at",
            "calendar": "fiscal"
        }
    }
    """
    try:
        return nlq_persistence.register_event(event)
    except Exception as e:
        logger.error(f"Event registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/events", response_model=List[CanonicalEvent], tags=["NLQ Registration"])
def list_events(tenant_id: str = "default"):
    """List all canonical events for a tenant."""
    return nlq_persistence.get_events(tenant_id)


@app.post("/api/nlq/entities", response_model=Entity, tags=["NLQ Registration"])
def register_entity(entity: Entity):
    """
    Register or update an entity (dimension).

    Entities are business dimensions like customer, service_line, region
    that events can be grouped/filtered by.

    Example:
    {
        "id": "customer",
        "tenant_id": "t_123",
        "identifiers_json": {
            "primary": "customer_id",
            "aliases": ["account_id", "client_id"]
        }
    }
    """
    try:
        return nlq_persistence.register_entity(entity)
    except Exception as e:
        logger.error(f"Entity registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/entities", response_model=List[Entity], tags=["NLQ Registration"])
def list_entities(tenant_id: str = "default"):
    """List all entities for a tenant."""
    return nlq_persistence.get_entities(tenant_id)


@app.post("/api/nlq/definitions", response_model=Definition, tags=["NLQ Registration"])
def register_definition(definition: Definition):
    """
    Register or update a metric/view definition.

    Definitions describe business metrics like services_revenue, ARR, DSO.

    Example:
    {
        "id": "services_revenue",
        "tenant_id": "t_123",
        "kind": "metric",
        "description": "Revenue from professional services",
        "default_time_semantics_json": {
            "event": "revenue_recognized",
            "time_field": "recognized_at",
            "calendar": "fiscal"
        }
    }
    """
    try:
        return nlq_persistence.register_definition(definition)
    except Exception as e:
        logger.error(f"Definition registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/definitions", response_model=List[Definition], tags=["NLQ Registration"])
def list_definitions(tenant_id: str = "default"):
    """List all definitions for a tenant."""
    return nlq_persistence.get_definitions(tenant_id)


@app.get("/api/nlq/definitions/{definition_id}", response_model=Definition, tags=["NLQ Registration"])
def get_definition(definition_id: str, tenant_id: str = "default"):
    """Get a specific definition by ID."""
    definition = nlq_persistence.get_definition(definition_id, tenant_id)
    if definition:
        return definition
    raise HTTPException(status_code=404, detail=f"Definition {definition_id} not found")


@app.post("/api/nlq/definition_versions", response_model=DefinitionVersion, tags=["NLQ Registration"])
def register_definition_version(version: DefinitionVersion):
    """
    Register or update a definition version.

    Definition versions contain the full spec for computing a metric:
    - required_events: Events needed to compute the metric
    - measure: Aggregation operation (sum, avg, count)
    - filters: Filter DSL conditions
    - allowed_dims: Dimensions that can be used for grouping
    - joins: How to join events to entities
    - time_field: Field to use for time-based filtering

    Example:
    {
        "id": "services_revenue_v1",
        "tenant_id": "t_123",
        "definition_id": "services_revenue",
        "version": "v1",
        "status": "published",
        "spec": {
            "required_events": ["revenue_recognized"],
            "measure": {"op": "sum", "field": "amount"},
            "filters": {
                "service_line": {"op": "in", "values": ["Professional Services"]}
            },
            "allowed_dims": ["customer", "service_line"],
            "time_field": "recognized_at"
        }
    }
    """
    try:
        return nlq_persistence.register_definition_version(version)
    except Exception as e:
        logger.error(f"Definition version registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/definition_versions", response_model=List[DefinitionVersion], tags=["NLQ Registration"])
def list_definition_versions(tenant_id: str = "default"):
    """List all definition versions for a tenant."""
    return nlq_persistence.get_definition_versions(tenant_id)


@app.post("/api/nlq/proof_hooks", response_model=ProofHook, tags=["NLQ Registration"])
def register_proof_hook(hook: ProofHook):
    """
    Register or update a proof hook.

    Proof hooks link definitions to source system evidence for explainability.

    Example:
    {
        "id": "services_revenue_netsuite",
        "tenant_id": "t_123",
        "definition_id": "services_revenue",
        "pointer_template_json": {
            "system": "NetSuite",
            "type": "saved_search",
            "ref_template": "saved_search:{search_id}"
        },
        "availability_score": 0.9
    }
    """
    try:
        return nlq_persistence.register_proof_hook(hook)
    except Exception as e:
        logger.error(f"Proof hook registration failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/nlq/proof_hooks", response_model=List[ProofHook], tags=["NLQ Registration"])
def list_proof_hooks(tenant_id: str = "default"):
    """List all proof hooks for a tenant."""
    return nlq_persistence.get_proof_hooks(tenant_id)


class TopologyResponse(BaseModel):
    nodes: List[Dict[str, Any]]
    links: List[Dict[str, Any]]
    metadata: Dict[str, Any]


@app.get("/api/topology", response_model=TopologyResponse)
async def get_topology(include_health: bool = True):
    """
    Get the unified topology graph.
    
    Merges DCL semantic graph with AAM health data.
    This is the TopologyAPI service that absorbs visualization from AAM.
    """
    try:
        return await topology_api.get_topology(include_health=include_health)
    except Exception as e:
        logger.error(f"Failed to get topology: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topology/health")
async def get_connection_health(connector_id: Optional[str] = None):
    """
    Get connection health data from the mesh.
    
    This ingests data from AAM's GetConnectionHealth endpoint.
    """
    try:
        return await topology_api.get_connection_health(connector_id)
    except Exception as e:
        logger.error(f"Failed to get connection health: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topology/stats")
def get_topology_stats():
    """Get topology service statistics."""
    return topology_api.get_stats()


DIST_DIR = Path(__file__).parent.parent.parent / "dist"

if DIST_DIR.exists() and (DIST_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="assets")


@app.get("/")
async def serve_root():
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"status": "DCL Engine API is running", "version": "1.0.0", "note": "Frontend not built"}


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API route not found")
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    raise HTTPException(status_code=404, detail="Frontend not built")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
