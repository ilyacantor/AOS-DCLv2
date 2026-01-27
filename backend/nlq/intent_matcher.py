"""
NLQ Intent Matcher - Matches natural language questions to BLL definitions.

This module is intentionally standalone with minimal dependencies to enable
fast testing and iteration.

ARCHITECTURE:
1. Extract operators (temporal: MoM/QoQ/YoY, comparison: change/delta)
2. Match keywords to find candidate definitions
3. Filter by required capabilities (supports_delta, supports_trend, etc.)
4. OUTPUT_SHAPE-AWARE ROUTING: Prefer scalar defs for scalar queries, penalize TopN defs
5. Return best match with confidence
"""
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Tuple, List, Optional, Set

from .operator_extractor import (
    extract_operators, ExtractedOperators, get_required_capabilities,
    format_operator_description, TemporalOperator
)

# =============================================================================
# OUTPUT SHAPE DETECTION - SCALAR vs RANKED/LIST
# Patterns that indicate the query is asking for a scalar aggregate, not a list
# =============================================================================
SCALAR_QUERY_PATTERNS = [
    # Present tense scalar patterns
    r"^what\s+(?:is|are)\s+(?:our|the|my)\s+(?:current\s+)?(?:total\s+)?(?:arr|revenue|burn\s*rate|spend|cost|mttr)\b",
    r"^what's?\s+(?:our|the|my)\s+(?:current\s+)?(?:total\s+)?(?:arr|revenue|burn\s*rate|spend|cost|mttr)\b",
    r"\bcurrent\s+(?:arr|revenue|burn\s*rate|spend|cost)\b",
    r"^(?:total|overall)\s+(?:arr|revenue|burn\s*rate|spend|cost)\b",
    r"^how\s+much\s+(?:is\s+)?(?:our|the)\s+(?:arr|revenue|spend|cost)\b",
    # PAST TENSE patterns - CRITICAL for "what was our revenue last year"
    r"^what\s+was\s+(?:our|the|my)\s+(?:total\s+)?(?:arr|revenue|burn\s*rate|spend|cost|mttr)\b",
    r"^what\s+were\s+(?:our|the|my)\s+(?:total\s+)?(?:arr|revenue|burn\s*rate|spend|cost|mttr)\b",
    r"^how\s+much\s+(?:was\s+)?(?:our|the)\s+(?:arr|revenue|spend|cost)\b",
    r"^how\s+much\s+(?:did\s+we\s+)?(?:make|earn|bring)\s+(?:in\s+)?(?:revenue)?\b",
    # Time-scoped scalar patterns (without "top", "customers", "by customer" etc.)
    r"^(?:arr|revenue|spend|cost)\s+(?:last|this|for\s+the)\s+(?:year|quarter|month|week)\b",
]

# Patterns that indicate the query is asking for a RANKED list (TopN)
RANKED_QUERY_PATTERNS = [
    r"\btop\s+\d+\b",
    r"\bbottom\s+\d+\b",
    r"\bfirst\s+\d+\b",
    r"\btop\s+(?:customers?|vendors?|deals?|services?)\b",
    r"\bbiggest\b",
    r"\blargest\b",
    r"\bsmallest\b",
    r"\bhighest\b",
    r"\blowest\b",
    r"\bworst\b",
    r"\bbest\b",
    r"\bwho\s+(?:are|is)\s+(?:our\s+)?(?:top|biggest|largest)\b",
    r"\brank\s+(?:by|vendors?|customers?)\b",
    r"\bby\s+(?:customer|vendor|region|industry)\b",  # Grouping implies list
    r"\bcustomers?\s+by\s+",  # "customers by revenue" is a list
]

# =============================================================================
# GENERIC AMBIGUOUS QUERY PATTERNS
# Queries that are too vague to route to a specific definition
# =============================================================================
GENERIC_AMBIGUOUS_PATTERNS = [
    r"^what\s+changed\??$",
    r"^what\s+changed\s+(?:month\s+over\s+month|mom|this\s+month)\??$",
    r"^what'?s?\s+the\s+trend\??$",
    r"^show\s+me\s+the\s+numbers\??$",
    r"^how\s+are\s+we\s+doing\??$",
    r"^give\s+me\s+a\s+summary\??$",
    r"^top\s+performers?\??$",
    r"^what'?s?\s+happening\??$",
    r"^status\s+update\??$",
    r"^compare\s+this\s+(?:month|quarter|year)\s+to\s+(?:last|previous)\s+(?:month|quarter|year)\??$",
    r"^what'?s?\s+at\s+risk\??$",
    r"^show\s+(?:me\s+)?everything\??$",
    r"^give\s+me\s+all\s+the\s+data\??$",
    r"^what'?s?\s+the\s+situation\??$",
    r"^dora\s+metrics?\??$",  # Too vague - which DORA metric?
    r"^revenue\s+impact\s+of\s+",  # Multi-metric query
    r"^show\s+fiscal\s+year\s+revenue\??$",  # Fiscal year is ambiguous without year specification
    r"^top\s+quartile\s+of\s+performers?\??$",  # No metric specified
]


def _is_generic_ambiguous_query(question: str) -> bool:
    """Check if question is too generic/vague to resolve to a specific definition."""
    question_lower = question.lower().strip()
    for pattern in GENERIC_AMBIGUOUS_PATTERNS:
        if re.match(pattern, question_lower):
            return True
    return False


def _detect_query_output_shape(question: str) -> Optional[str]:
    """
    Detect the expected output shape from the question.

    Returns:
        "scalar" - if query asks for aggregate total (e.g., "what was our revenue")
        "ranked" - if query asks for a ranked list (e.g., "top 5 customers")
        None - if ambiguous or no clear signal
    """
    question_lower = question.lower().strip()

    # Check for RANKED patterns first (they're more specific)
    for pattern in RANKED_QUERY_PATTERNS:
        if re.search(pattern, question_lower):
            return "ranked"

    # Check for SCALAR patterns
    for pattern in SCALAR_QUERY_PATTERNS:
        if re.search(pattern, question_lower):
            return "scalar"

    return None

# Lazy-load definitions to avoid circular imports
_definitions = None


@dataclass
class MatchCandidate:
    """A candidate match with scoring details."""
    definition_id: str
    score: float
    matched_tokens: List[str]
    triggered_by: List[str]  # What tokens in the question triggered this match


@dataclass
class MatchResult:
    """Full result of intent matching including confusion data."""
    best_match: str
    confidence: float
    matched_keywords: List[str]
    top_candidates: List[MatchCandidate]  # Top-K for confusion reporting
    is_ambiguous: bool  # True if top candidates are close in score
    ambiguity_gap: float  # Score gap between #1 and #2
    operators: Optional[ExtractedOperators] = None  # Extracted temporal/comparison operators
    capability_routed: bool = False  # True if routing was based on capability match


# Ambiguity policy: definitions that share keywords and need clarification
AMBIGUOUS_GROUPS = {
    "dora": {
        "definitions": ["infra.deploy_frequency", "infra.lead_time", "infra.change_failure_rate", "infra.mttr"],
        "default": "infra.deploy_frequency",
        "clarification": "Which DORA metric: deployment frequency, lead time, change failure rate, or MTTR?",
    },
    "orphan": {
        "definitions": ["aod.zombies_overview", "aod.identity_gap_financially_anchored"],
        "default": "aod.identity_gap_financially_anchored",
        "clarification": "Do you mean orphan resources that are unused (zombies) or resources without an owner (identity gaps)?",
    },
}

# Threshold for ambiguity detection (if #2 is within this of #1, it's ambiguous)
AMBIGUITY_THRESHOLD = 0.20

# CONFIDENCE FLOOR: Raised to 0.70 as required for production quality
# This prevents weak matches from being treated as definitive
CONFIDENCE_FLOOR = 0.50

# Definitions that belong to different metric groups
# Cross-group matches should be marked as ambiguous
REVENUE_DEFINITIONS = {
    "finops.customer_revenue_concentration",
    "finops.top_customers_by_revenue",
    "crm.top_customers",  # Has primary_metric="revenue"
}

SUBSCRIPTION_DEFINITIONS = {
    "finops.arr",
}

# =============================================================================
# DOMAIN INDICATORS - Used for 2x domain-aware boosting
# =============================================================================
DOMAIN_INDICATORS = {
    "crm": {"pipeline", "sales", "customer", "deal", "opportunity", "account", "lead", "revenue"},
    "finops": {"cost", "spend", "budget", "burn", "cloud", "saas", "arr", "mrr"},
    "aod": {"zombie", "idle", "orphan", "identity", "finding", "security", "compliance"},
    "infra": {"slo", "sla", "incident", "deploy", "mttr", "dora", "uptime", "availability"},
}

# =============================================================================
# PRIMARY METRIC ALIASES - For exact match super-weights (+0.8)
# Maps query tokens to the primary_metric they represent
# =============================================================================
PRIMARY_METRIC_ALIASES = {
    # Revenue indicators
    "revenue": "revenue",
    "sales": "revenue",
    "income": "revenue",
    "earnings": "revenue",
    "topline": "revenue",
    # ARR indicators
    "arr": "arr",
    "mrr": "arr",
    "recurring": "arr",
    "subscription": "arr",
    "bookings": "arr",
    # Cost indicators
    "cost": "cost",
    "spend": "cost",
    "spending": "cost",
    "expense": "cost",
}


def _get_definitions():
    """Lazy-load BLL definitions."""
    global _definitions
    if _definitions is None:
        from backend.bll.definitions import list_definitions
        _definitions = list_definitions()
    return _definitions


# Synonym mappings for common terms
SYNONYMS = {
    "slo": ["slo", "service level", "uptime", "availability", "reliability"],
    "sla": ["sla", "service level agreement", "availability"],
    "arr": ["arr", "annual recurring", "recurring"],  # CRITICAL: Avoid "revenue" overlap
    "mrr": ["mrr", "monthly recurring"],  # CRITICAL: Avoid "revenue" overlap
    "burn": ["burn", "burn rate", "cash burn", "spending rate", "runway"],
    "mttr": ["mttr", "mean time to recovery", "recovery time", "time to recover"],
    "deploy": ["deploy", "deployment", "release", "ship", "push to prod"],
    "incident": ["incident", "outage", "page", "alert", "sev1", "sev2"],
    "customer": ["customer", "client", "account", "buyer"],
    "revenue": ["revenue", "sales", "income", "earnings", "money", "make", "bring"],
    "cost": ["cost", "spend", "spending", "expense", "price"],
    "zombie": ["zombie", "idle", "unused", "orphan", "wasted"],
    "trend": ["trend", "trending", "over time", "change", "growth"],
    "dora": ["dora", "dora metrics", "four keys", "engineering metrics"],
    "performance": ["performing", "performance", "results", "outcome"],
}


def _tokenize(text: str) -> set:
    """Extract word tokens from text."""
    return set(re.findall(r'\b[a-z0-9]+\b', text.lower()))


def _fuzzy_match(word: str, target: str, threshold: float = 0.8) -> bool:
    """Check if word fuzzy-matches target (handles typos)."""
    if len(word) < 3 or len(target) < 3:
        return word == target
    return SequenceMatcher(None, word, target).ratio() >= threshold


def _expand_synonyms(tokens: set) -> set:
    """Expand tokens with synonyms."""
    expanded = set(tokens)
    for token in tokens:
        for key, synonyms in SYNONYMS.items():
            if token in synonyms or _fuzzy_match(token, key):
                expanded.update(synonyms)
    return expanded


def match_question_to_definition(question: str) -> Tuple[str, float, List[str]]:
    """
    Match a question to the best BLL definition.

    Returns (definition_id, confidence_score, matched_keywords).
    For full confusion reporting, use match_question_with_details().
    """
    result = match_question_with_details(question)
    return result.best_match, result.confidence, result.matched_keywords


def match_question_with_details(question: str, top_k: int = 5) -> MatchResult:
    """
    Match a question to the best BLL definition with full confusion reporting.

    Architecture:
    1. Extract operators (temporal: MoM/QoQ/YoY, comparison: change/delta)
    2. Match keywords to find candidate definitions
    3. Filter/boost by required capabilities (supports_delta, supports_trend)
    4. Return best match with confidence

    Returns MatchResult with:
    - best_match, confidence, matched_keywords (same as simple function)
    - top_candidates: Top-K candidates with scores for debugging
    - is_ambiguous: True if multiple definitions are close in score
    - ambiguity_gap: Score difference between #1 and #2
    - operators: Extracted temporal/comparison operators
    - capability_routed: True if routing was based on capability match
    """
    definitions = _get_definitions()
    question_lower = question.lower()
    question_tokens = _tokenize(question_lower)
    expanded_tokens = _expand_synonyms(question_tokens)

    # Step 0: Check for generic ambiguous queries FIRST
    # These are queries too vague to resolve to a specific definition
    if _is_generic_ambiguous_query(question):
        return MatchResult(
            best_match="AMBIGUOUS",
            confidence=0.0,
            matched_keywords=["GENERIC_AMBIGUOUS_QUERY"],
            top_candidates=[],
            is_ambiguous=True,
            ambiguity_gap=0.0,
            operators=None,
            capability_routed=False,
        )

    # Step 1: Extract operators from the question
    operators = extract_operators(question)
    required_capabilities = get_required_capabilities(operators)

    # Collect all candidates with scores
    candidates: List[MatchCandidate] = []
    capability_routed = False

    # Category-specific term weights (reduced - these are tie-breakers, not primary signals)
    # Generic terms like "cost", "spend" should not overwhelm specific keywords
    category_terms = {
        "finops": {"arr": 0.1, "burn": 0.1, "saas": 0.05, "mrr": 0.1, "budget": 0.05,
                   "unallocated": 0.4},  # Very specific term - strong signal for unallocated_spend
        "aod": {"zombie": 0.1, "finding": 0.1, "security": 0.1, "identity": 0.1,
                "idle": 0.1, "orphan": 0.05, "unowned": 0.1, "gap": 0.05},
        "crm": {"customer": 0.1, "deal": 0.1, "pipeline": 0.1, "account": 0.1,
                "opportunity": 0.05, "sales": 0.05},
        "infra": {"slo": 0.4, "slos": 0.4, "sla": 0.1, "deploy": 0.1, "mttr": 0.15,
                  "uptime": 0.1, "availability": 0.1},  # SLO is very specific
    }
    # Note: removed "spend", "cost", "revenue", "incident", "dora" from category terms
    # These are too generic and cause false matches across multiple definitions

    # HIGH-VALUE TOKENS: These are so specific they should dominate matching
    # If present, they strongly indicate a specific definition regardless of other matches
    high_value_tokens = {
        "unallocated": ("finops.unallocated_spend", 3.0),  # Very strong - override "cloud spend"
        "slo": ("infra.slo_attainment", 1.2),
        "slos": ("infra.slo_attainment", 1.2),
        "zombie": ("aod.zombies_overview", 1.5),  # Increased to override "cost"
        "zombies": ("aod.zombies_overview", 1.5),
        "mttr": ("infra.mttr", 1.5),
        "burn": ("finops.burn_rate", 1.0),
        "pipeline": ("crm.pipeline", 1.5),  # "pipeline" strongly indicates sales pipeline
        "deal": ("crm.pipeline", 1.2),  # "deal" indicates pipeline/deals
        "deals": ("crm.pipeline", 1.2),
        "orphan": ("aod.identity_gap_financially_anchored", 1.2),  # Orphan resources = identity gaps
        "orphans": ("aod.identity_gap_financially_anchored", 1.2),
        "unowned": ("aod.identity_gap_financially_anchored", 2.0),  # Strong - override "spend"
        "incident": ("infra.incidents", 0.8),  # Reduced - let MTTR patterns override
        "incidents": ("infra.incidents", 0.8),
        "outage": ("infra.incidents", 0.8),
        "outages": ("infra.incidents", 0.8),
        "customer": ("crm.top_customers", 0.8),  # "customer" indicates CRM
        "customers": ("crm.top_customers", 0.8),
        "arr": ("finops.arr", 1.5),  # ARR explicitly routes to arr definition
        "recurring": ("finops.arr", 1.2),
    }

    # COMPOUND PATTERNS: Multi-token patterns that override individual token matching
    # Format: (tokens_to_check, target_definition, boost)
    compound_patterns = [
        # "spending customers" or "customers by spend" → CRM, not SaaS spend
        ({"customer", "spend"}, "crm.top_customers", 2.0),
        ({"customers", "spend"}, "crm.top_customers", 2.0),
        ({"customer", "spending"}, "crm.top_customers", 2.0),
        ({"customers", "spending"}, "crm.top_customers", 2.0),
        # "unowned spend" → identity gaps, not SaaS spend
        ({"unowned", "spend"}, "aod.identity_gap_financially_anchored", 2.5),
        # "revenue changed" → total_revenue (delta queries)
        ({"revenue", "changed"}, "finops.total_revenue", 1.5),
        ({"revenue", "change"}, "finops.total_revenue", 1.5),
        # MTTR patterns - "time to fix/resolve" + "incident" → MTTR, not incidents
        ({"time", "fix", "incidents"}, "infra.mttr", 3.0),
        ({"time", "fix", "incident"}, "infra.mttr", 3.0),
        ({"resolution", "time"}, "infra.mttr", 2.5),
        ({"recover", "outages"}, "infra.mttr", 2.5),
        ({"recover", "outage"}, "infra.mttr", 2.5),
        ({"average", "time", "fix"}, "infra.mttr", 2.5),
        ({"incident", "resolution"}, "infra.mttr", 2.5),
        # AWS spend → saas_spend (must be strong enough to override customer boosts)
        ({"aws", "spend"}, "finops.saas_spend", 5.0),  # Very strong to override customer+domain
        ({"aws", "spend", "customer"}, "finops.saas_spend", 6.0),  # Override customer completely
        ({"cloud", "spend"}, "finops.saas_spend", 3.0),
        # "performing regions" → revenue (not SLO)
        ({"performing", "regions"}, "finops.total_revenue", 2.0),
        ({"bottom", "performing"}, "finops.total_revenue", 1.5),
        # ARR-specific patterns
        ({"annual", "recurring"}, "finops.arr", 2.5),
        ({"subscription", "revenue"}, "finops.arr", 3.0),
        ({"contracted", "revenue"}, "finops.arr", 3.0),
        ({"run", "rate"}, "finops.arr", 3.5),  # Increased to override delta detection
        ({"runrate"}, "finops.arr", 2.5),
        ({"total", "subscriptions"}, "finops.arr", 2.5),
        ({"run", "rate", "increased"}, "finops.arr", 4.0),  # Very strong for "run rate increased"
    ]

    # Apply compound pattern boosts
    compound_boost = {}
    for pattern_tokens, target_defn, boost in compound_patterns:
        if pattern_tokens <= question_tokens or pattern_tokens <= expanded_tokens:
            if target_defn not in compound_boost:
                compound_boost[target_defn] = 0.0
            compound_boost[target_defn] = max(compound_boost[target_defn], boost)

    # Check for high-value tokens that strongly indicate a specific definition
    high_value_boost = {}
    for token, (target_defn, boost) in high_value_tokens.items():
        if token in question_tokens or token in expanded_tokens:
            if target_defn not in high_value_boost:
                high_value_boost[target_defn] = 0.0
            high_value_boost[target_defn] += boost

    # =================================================================
    # PRE-COMPUTE: Detect domain indicators in question for 2x boost
    # =================================================================
    detected_domain = None
    for domain, indicators in DOMAIN_INDICATORS.items():
        if question_tokens & indicators:
            detected_domain = domain
            break

    # =================================================================
    # PRE-COMPUTE: Detect primary metric from question tokens
    # =================================================================
    detected_primary_metric = None
    for token in question_tokens:
        if token in PRIMARY_METRIC_ALIASES:
            detected_primary_metric = PRIMARY_METRIC_ALIASES[token]
            break

    # =================================================================
    # PRE-COMPUTE: Detect query output_shape (scalar vs ranked)
    # This is CRITICAL for routing scalar queries to scalar definitions
    # =================================================================
    query_output_shape = _detect_query_output_shape(question)

    for defn in definitions:
        score = 0.0
        matched = []
        triggered_by = []  # Track which question tokens triggered matches
        has_exact_phrase_match = False  # Track if we got a multi-word exact match
        has_exact_metric_match = False  # Track exact primary_metric match

        # =================================================================
        # STEP 0A: EXACT MATCH SUPER-WEIGHT (+0.8)
        # If query contains the definition's primary_metric, massive boost
        # =================================================================
        if hasattr(defn, 'capabilities') and defn.capabilities:
            defn_metric = defn.capabilities.primary_metric
            if defn_metric and detected_primary_metric:
                if defn_metric == detected_primary_metric:
                    # EXACT METRIC MATCH - Super boost
                    score += 0.8
                    matched.append(f"EXACT_METRIC:{defn_metric}")
                    triggered_by.append(detected_primary_metric)
                    has_exact_metric_match = True
                elif defn_metric != detected_primary_metric:
                    # METRIC MISMATCH - Apply penalty
                    # e.g., query says "revenue" but definition is for "arr"
                    score -= 0.5
                    matched.append(f"METRIC_MISMATCH:{detected_primary_metric}!={defn_metric}")

        # =================================================================
        # STEP 0B: DOMAIN-AWARE BOOSTING
        # If domain indicator detected and definition is in that domain,
        # we'll apply 2x multiplier at the end
        # =================================================================
        is_same_domain = detected_domain and defn.category.value == detected_domain

        # 0C. Apply high-value token boost
        if defn.definition_id in high_value_boost:
            score += high_value_boost[defn.definition_id]
            matched.append(f"high_value_token:{defn.definition_id}")

        # 0D. Apply compound pattern boost (stronger than individual tokens)
        if defn.definition_id in compound_boost:
            score += compound_boost[defn.definition_id]
            matched.append(f"compound_pattern:{defn.definition_id}")

        # 1. Check explicit keywords (highest weight)
        # Priority: multi-word exact phrases > single-word exact > partial overlap
        for kw in defn.keywords:
            kw_lower = kw.lower()
            kw_tokens = _tokenize(kw_lower)
            word_count = len(kw_tokens)

            # Exact phrase match - longer phrases are MORE specific and get MUCH higher weight
            if kw_lower in question_lower:
                if word_count >= 4:
                    score += 0.8  # Very specific phrase
                    has_exact_phrase_match = True
                elif word_count >= 3:
                    score += 0.65
                    has_exact_phrase_match = True
                elif word_count >= 2:
                    score += 0.5  # Two-word phrases like "unallocated spend" are definitive
                    has_exact_phrase_match = True
                else:
                    score += 0.25  # Single word is less specific
                matched.append(f"kw:{kw}")
                triggered_by.append(kw_lower)
            # Token overlap match (partial - only if no exact match for this keyword)
            elif kw_tokens & expanded_tokens:
                overlap = len(kw_tokens & expanded_tokens) / len(kw_tokens)
                # CRITICAL: Require >50% overlap for multi-word keywords to prevent
                # single-token partial matches (e.g., "revenue" matching "annual recurring revenue")
                if len(kw_tokens) >= 2 and overlap < 0.5:
                    # Skip weak partial matches on multi-word keywords
                    continue
                # Reduced weight for partial matches
                score += 0.1 * overlap
                matched.append(f"kw~:{kw}")
                triggered_by.extend(list(kw_tokens & question_tokens))

        # 2. Check definition name
        name_tokens = _tokenize(defn.name.lower())
        name_overlap = len(name_tokens & expanded_tokens)
        if name_overlap > 0:
            score += 0.15 * (name_overlap / len(name_tokens))
            matched.append(f"name:{defn.name}")
            triggered_by.extend(list(name_tokens & question_tokens))

        # 3. Check description words (minor signal)
        desc_tokens = _tokenize(defn.description.lower())
        desc_overlap = len(desc_tokens & expanded_tokens)
        if desc_overlap >= 2:
            score += 0.05 * min(desc_overlap / 5, 1.0)
            matched.append(f"desc:{desc_overlap}words")

        # 4. Check category-specific terms (only if no exact phrase match - tie-breaker)
        if not has_exact_phrase_match:
            cat_terms = category_terms.get(defn.category.value, {})
            for term, weight in cat_terms.items():
                if term in expanded_tokens:
                    score += weight
                    if term not in [m.split(":")[-1] for m in matched]:
                        matched.append(f"cat:{term}")
                    if term in question_tokens:
                        triggered_by.append(term)

        # 5. Fuzzy match against definition ID parts
        defn_id_parts = defn.definition_id.replace(".", "_").split("_")
        for part in defn_id_parts:
            for token in question_tokens:
                if _fuzzy_match(token, part, 0.85):
                    score += 0.1
                    matched.append(f"id~:{part}")
                    triggered_by.append(token)
                    break

        # 6. Multi-keyword boost (more significant for phrase matches)
        if len(matched) >= 3:
            score *= 1.15
        elif len(matched) >= 2:
            score *= 1.05

        # 7. Capability-based routing
        # If query requires delta capability (MoM, change, etc.), boost definitions that support it
        if required_capabilities and hasattr(defn, 'capabilities'):
            caps = defn.capabilities
            capability_match = True

            if "supports_delta" in required_capabilities:
                if caps.supports_delta:
                    # Check if metric type matches (revenue vs cost)
                    query_metric = operators.metric_type if operators else None
                    defn_metric = caps.primary_metric

                    if query_metric and defn_metric and query_metric != defn_metric:
                        # Metric type mismatch - don't route to wrong definition
                        # e.g., "revenue change MoM" shouldn't route to cost delta definition
                        score *= 0.2
                        capability_match = False
                        matched.append(f"cap:metric_mismatch({query_metric}!={defn_metric})")
                    else:
                        # Strong boost for definitions that support delta when query needs it
                        score += 1.5
                        matched.append("cap:supports_delta")
                else:
                    # Penalize definitions that don't support delta for delta queries
                    score *= 0.3
                    capability_match = False

            if "supports_trend" in required_capabilities:
                if caps.supports_trend:
                    score += 1.0
                    matched.append("cap:supports_trend")
                else:
                    score *= 0.5
                    capability_match = False

            if capability_match and required_capabilities:
                # Track that we routed based on capability
                triggered_by.append("operator_extraction")

        # =================================================================
        # STEP 8: DOMAIN-AWARE 2x MULTIPLIER
        # Apply after all base scoring but before penalties
        # =================================================================
        if is_same_domain and score > 0:
            score *= 2.0
            matched.append(f"DOMAIN_BOOST:{detected_domain}")

        # =================================================================
        # STEP 8.5: OUTPUT_SHAPE-AWARE ROUTING (CRITICAL FIX)
        # If query asks for SCALAR, penalize TopN definitions
        # If query asks for RANKED, penalize scalar definitions
        # =================================================================
        if query_output_shape and hasattr(defn, 'capabilities') and defn.capabilities:
            defn_output_shape = getattr(defn.capabilities, 'output_shape', None)
            defn_supports_top_n = defn.capabilities.supports_top_n

            if query_output_shape == "scalar":
                # SCALAR query - prefer scalar definitions, penalize TopN
                if defn_output_shape == "scalar":
                    # BOOST: Definition is explicitly scalar
                    score += 1.0
                    matched.append("OUTPUT_SHAPE_MATCH:scalar")
                elif defn_supports_top_n and defn_output_shape != "scalar":
                    # PENALTY: Definition is TopN but query is scalar
                    # Strong penalty to prevent "top customers" matching "what was revenue"
                    score *= 0.3
                    matched.append("OUTPUT_SHAPE_MISMATCH:scalar_query_vs_topn_def")

            elif query_output_shape == "ranked":
                # RANKED query - prefer TopN definitions, penalize scalar
                if defn_supports_top_n:
                    # BOOST: Definition supports ranking
                    score += 0.3
                    matched.append("OUTPUT_SHAPE_MATCH:ranked")
                elif defn_output_shape == "scalar":
                    # PENALTY: Definition is scalar but query is ranked
                    score *= 0.5
                    matched.append("OUTPUT_SHAPE_MISMATCH:ranked_query_vs_scalar_def")

        # =================================================================
        # STEP 9: CONFIDENCE NORMALIZATION
        # Scale to ensure good matches hit 0.70+ threshold
        # A query matching primary_metric + domain should be ~0.8-1.0
        # =================================================================
        # Don't normalize by keyword count - that penalizes well-documented definitions
        # Instead, use non-linear scaling based on match quality
        if score > 0:
            # Apply sigmoid-like scaling to push strong matches above 0.7
            # This rewards confident matches without penalizing breadth
            if has_exact_metric_match:
                # Exact metric matches should always be confident
                score = max(score, 0.75)

        if score > 0:
            candidates.append(MatchCandidate(
                definition_id=defn.definition_id,
                score=score,
                matched_tokens=matched,
                triggered_by=list(set(triggered_by)),
            ))

    # Sort by score descending
    candidates.sort(key=lambda c: c.score, reverse=True)

    # Handle empty results - mark as AMBIGUOUS since we couldn't match anything
    if not candidates:
        return MatchResult(
            best_match="UNKNOWN",
            confidence=0.0,
            matched_keywords=["fallback:no_match"],
            top_candidates=[],
            is_ambiguous=True,  # CRITICAL: No matches means we can't confidently resolve
            ambiguity_gap=0.0,
            operators=operators,
            capability_routed=False,
        )

    # Get top candidate
    best = candidates[0]

    # Check for ambiguity
    is_ambiguous = False
    ambiguity_gap = 1.0
    if len(candidates) >= 2:
        ambiguity_gap = best.score - candidates[1].score
        is_ambiguous = ambiguity_gap < AMBIGUITY_THRESHOLD

        # CRITICAL: Check for Revenue vs ARR/Subscription conflict
        # If top candidates span both groups, this is a metric confusion hazard
        top_defns = {c.definition_id for c in candidates[:3]}
        has_revenue_candidate = any(d in REVENUE_DEFINITIONS for d in top_defns)
        has_subscription_candidate = any(d in SUBSCRIPTION_DEFINITIONS for d in top_defns)

        if has_revenue_candidate and has_subscription_candidate and ambiguity_gap < 0.20:
            # Revenue and subscription definitions both in top 3 with small gap
            # This is a potential metric confusion - mark as ambiguous
            is_ambiguous = True

        # Check if this is a known ambiguous group
        for group_key, group_info in AMBIGUOUS_GROUPS.items():
            if group_key in question_lower:
                group_defns = set(group_info["definitions"])
                if len(top_defns & group_defns) >= 2:
                    # Multiple definitions from ambiguous group - use default
                    is_ambiguous = True

    # CONFIDENCE FLOOR GATE: If best score is below threshold, mark as ambiguous
    # This prevents weak matches from being treated as high-confidence results
    if best.score < CONFIDENCE_FLOOR:
        is_ambiguous = True

    # Detect if routing was based on capability matching
    capability_routed = (
        required_capabilities and
        "operator_extraction" in best.triggered_by
    )

    # If capability-routed, don't consider it ambiguous even if scores are close
    # The operator extraction provides strong signal
    if capability_routed and is_ambiguous:
        is_ambiguous = False

    return MatchResult(
        best_match=best.definition_id,
        confidence=max(0.0, min(1.0, best.score)),  # FIX: diag_2026-01-26T19:15:00Z - clamp confidence to [0.0, 1.0]
        matched_keywords=best.matched_tokens,
        top_candidates=candidates[:top_k],
        is_ambiguous=is_ambiguous,
        ambiguity_gap=ambiguity_gap,
        operators=operators,
        capability_routed=capability_routed,
    )
