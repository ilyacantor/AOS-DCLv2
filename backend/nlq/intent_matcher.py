"""
NLQ Intent Matcher - Matches natural language questions to BLL definitions.

This module is intentionally standalone with minimal dependencies to enable
fast testing and iteration.
"""
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Tuple, List, Optional

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


# Ambiguity policy: definitions that share keywords and need clarification
AMBIGUOUS_GROUPS = {
    "dora": {
        "definitions": ["infra.deploy_frequency", "infra.lead_time", "infra.change_failure_rate", "infra.mttr"],
        "default": "infra.deploy_frequency",
        "clarification": "Which DORA metric: deployment frequency, lead time, change failure rate, or MTTR?",
    },
}

# Threshold for ambiguity detection (if #2 is within this of #1, it's ambiguous)
AMBIGUITY_THRESHOLD = 0.15


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

    Returns MatchResult with:
    - best_match, confidence, matched_keywords (same as simple function)
    - top_candidates: Top-K candidates with scores for debugging
    - is_ambiguous: True if multiple definitions are close in score
    - ambiguity_gap: Score difference between #1 and #2

    This is the function to use for confusion reporting and debugging.
    """
    definitions = _get_definitions()
    question_lower = question.lower()
    question_tokens = _tokenize(question_lower)
    expanded_tokens = _expand_synonyms(question_tokens)

    # Collect all candidates with scores
    candidates: List[MatchCandidate] = []

    # Category-specific term weights (reduced - these are tie-breakers, not primary signals)
    # Generic terms like "cost", "spend" should not overwhelm specific keywords
    category_terms = {
        "finops": {"arr": 0.1, "burn": 0.1, "saas": 0.05, "mrr": 0.1, "budget": 0.05},
        "aod": {"zombie": 0.1, "finding": 0.1, "security": 0.1, "identity": 0.1,
                "idle": 0.1, "orphan": 0.05, "unowned": 0.1, "gap": 0.05},
        "crm": {"customer": 0.1, "deal": 0.1, "pipeline": 0.1, "account": 0.1,
                "opportunity": 0.05, "sales": 0.05},
        "infra": {"slo": 0.15, "sla": 0.1, "deploy": 0.1, "mttr": 0.15,
                  "uptime": 0.1, "availability": 0.1},
    }
    # Note: removed "spend", "cost", "revenue", "incident", "dora" from category terms
    # These are too generic and cause false matches across multiple definitions

    for defn in definitions:
        score = 0.0
        matched = []
        triggered_by = []  # Track which question tokens triggered matches
        has_exact_phrase_match = False  # Track if we got a multi-word exact match

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

        # No cap - let scores accumulate naturally for better discrimination

        if score > 0:
            candidates.append(MatchCandidate(
                definition_id=defn.definition_id,
                score=score,
                matched_tokens=matched,
                triggered_by=list(set(triggered_by)),
            ))

    # Sort by score descending
    candidates.sort(key=lambda c: c.score, reverse=True)

    # Handle empty results
    if not candidates:
        return MatchResult(
            best_match="finops.arr",
            confidence=0.1,
            matched_keywords=["fallback:default"],
            top_candidates=[],
            is_ambiguous=False,
            ambiguity_gap=0.0,
        )

    # Get top candidate
    best = candidates[0]

    # Check for ambiguity
    is_ambiguous = False
    ambiguity_gap = 1.0
    if len(candidates) >= 2:
        ambiguity_gap = best.score - candidates[1].score
        is_ambiguous = ambiguity_gap < AMBIGUITY_THRESHOLD

        # Check if this is a known ambiguous group
        for group_key, group_info in AMBIGUOUS_GROUPS.items():
            if group_key in question_lower:
                group_defns = set(group_info["definitions"])
                top_defns = {c.definition_id for c in candidates[:4]}
                if len(top_defns & group_defns) >= 2:
                    # Multiple definitions from ambiguous group - use default
                    is_ambiguous = True

    return MatchResult(
        best_match=best.definition_id,
        confidence=best.score,
        matched_keywords=best.matched_tokens,
        top_candidates=candidates[:top_k],
        is_ambiguous=is_ambiguous,
        ambiguity_gap=ambiguity_gap,
    )
