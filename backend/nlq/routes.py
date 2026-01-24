"""
NLQ (Natural Language Query) Routes - Answerability ranking and execution.

These endpoints are used by BLL's NLQ consumer to:
1. Rank definitions by how well they can answer a question
2. Execute the best-matching definition
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import re

from backend.bll.definitions import list_definitions, get_definition
from backend.bll.executor import execute_definition
from backend.bll.models import ExecuteRequest

router = APIRouter(prefix="/api/nlq", tags=["nlq"])


KEYWORD_MAP = {
    "finops.saas_spend": [
        "spend", "spending", "saas", "cloud spend", "vendor spend",
        "expense", "expenses", "subscription", "subscriptions", "software spend"
    ],
    "finops.top_vendor_deltas_mom": [
        "change", "changes", "delta", "deltas", "mom", "month-over-month", "month over month",
        "trending", "trend", "increase", "decrease", "variance", "vendor cost change"
    ],
    "finops.unallocated_spend": [
        "unallocated", "unassigned", "orphan", "orphaned", "no owner", "missing",
        "untagged", "unknown", "cost center", "unallocated spend"
    ],
    "finops.arr": [
        "arr", "annual recurring revenue", "recurring revenue", "mrr", "monthly recurring",
        "revenue", "subscription revenue", "contract value", "acv", "tcv", "bookings",
        "total revenue", "current revenue"
    ],
    "finops.burn_rate": [
        "burn", "burn rate", "runway", "cash burn", "spend rate", "consumption", "monthly burn"
    ],
    "aod.findings_by_severity": [
        "finding", "findings", "security", "severity", "critical", "high", "medium", "low",
        "vulnerability", "vulnerabilities", "compliance", "risk", "risks", "security findings"
    ],
    "aod.identity_gap_financially_anchored": [
        "identity", "ownership", "owner gap", "gap", "missing owner", "unowned", "orphan resource",
        "financial anchor", "identity gap"
    ],
    "aod.zombies_overview": [
        "zombie", "zombies", "idle", "unused", "underutilized", "waste", "wasted",
        "inactive", "dormant", "stale", "zombie resources"
    ],
    "crm.pipeline": [
        "pipeline", "deal", "deals", "opportunity", "opportunities", "sales pipeline",
        "forecast", "stage", "stages", "funnel", "sales funnel"
    ],
    "crm.top_customers": [
        "customer", "customers", "top customers", "largest customers", "biggest customers",
        "top accounts", "best customers", "customer revenue", "account", "accounts",
        "top 5", "top 10"
    ]
}


class RankRequest(BaseModel):
    question: str


class RankResponse(BaseModel):
    definition_id: str
    confidence_score: float
    hypothesis_matches: list[str]


class RegistryExecuteRequest(BaseModel):
    definition_id: str
    dims: Optional[list[str]] = None
    time_window: Optional[str] = None
    filters: Optional[list[dict]] = None


PRIORITY_PHRASES = {
    "finops.arr": ["arr", "annual recurring revenue", "recurring revenue", "mrr", "current arr", "total arr", "our arr"],
    "finops.burn_rate": ["burn rate", "burn", "runway", "current burn"],
    "finops.saas_spend": ["saas spend", "cloud spend", "total spend"],
    "crm.top_customers": ["top customers", "top 5", "top 10", "largest customers", "biggest customers", "customer revenue"],
    "crm.pipeline": ["pipeline", "sales pipeline", "funnel"],
    "aod.zombies_overview": ["zombie", "zombies"],
    "aod.findings_by_severity": ["finding", "findings", "security"],
}


def _score_definition(question: str, definition_id: str) -> tuple[float, list[str]]:
    """Score how well a definition can answer the question."""
    question_lower = question.lower()
    words = set(re.findall(r'\b\w+\b', question_lower))
    
    keywords = KEYWORD_MAP.get(definition_id, [])
    
    definition = get_definition(definition_id)
    if definition:
        keywords = list(keywords) + [
            definition.name.lower(),
        ]
    
    matches = []
    priority_matched = False
    
    priority_phrases = PRIORITY_PHRASES.get(definition_id, [])
    for phrase in priority_phrases:
        if phrase.lower() in question_lower:
            matches.append(phrase)
            priority_matched = True
    
    for kw in keywords:
        if kw.lower() in question_lower:
            matches.append(kw)
        else:
            kw_words = set(kw.lower().split())
            if len(kw_words) == 1 and kw_words & words:
                matches.append(kw)
    
    matches = list(set(matches))
    
    if not matches:
        return 0.0, []
    
    if priority_matched:
        base_score = min(0.85 + len(matches) * 0.05, 1.0)
    else:
        base_score = min(len(matches) / 3.0, 0.75)
    
    return round(base_score, 4), matches


@router.post("/answerability_rank", response_model=RankResponse)
def answerability_rank(request: RankRequest):
    """
    Rank definitions by how well they can answer the given question.
    Returns the best-matching definition with confidence score.
    """
    definitions = list_definitions()
    
    best_id = None
    best_score = 0.0
    best_matches: list[str] = []
    
    for defn in definitions:
        score, matches = _score_definition(request.question, defn.definition_id)
        if score > best_score:
            best_score = score
            best_id = defn.definition_id
            best_matches = matches
    
    if not best_id:
        best_id = definitions[0].definition_id if definitions else "finops.saas_spend"
        best_score = 0.1
        best_matches = []
    
    return RankResponse(
        definition_id=best_id,
        confidence_score=best_score,
        hypothesis_matches=best_matches
    )


@router.post("/registry/execute")
def registry_execute(request: RegistryExecuteRequest):
    """
    Execute a definition from the registry.
    This is the NLQ-specific execution endpoint.
    """
    exec_request = ExecuteRequest(
        definitionId=request.definition_id,
        datasetId="demo9",
        dimensions=request.dims
    )
    
    try:
        result = execute_definition(exec_request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")
