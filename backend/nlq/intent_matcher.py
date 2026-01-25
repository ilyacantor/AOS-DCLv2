"""
NLQ Intent Matcher - Matches natural language questions to BLL definitions.

This module is intentionally standalone with minimal dependencies to enable
fast testing and iteration.
"""
import re
from difflib import SequenceMatcher
from typing import Tuple, List

# Lazy-load definitions to avoid circular imports
_definitions = None


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
    Match a question to the best BLL definition using improved NLP matching.

    Features:
    - Word tokenization with boundary detection
    - Synonym expansion
    - Fuzzy matching for typo tolerance
    - Description matching
    - Multi-keyword boost

    Returns (definition_id, confidence_score, matched_keywords).
    """
    definitions = _get_definitions()
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

    for defn in definitions:
        score = 0.0
        matched = []

        # 1. Check explicit keywords (highest weight)
        # Multi-word exact phrases get bonus points for specificity
        for kw in defn.keywords:
            kw_lower = kw.lower()
            kw_tokens = _tokenize(kw_lower)
            word_count = len(kw_tokens)

            # Exact phrase match - longer phrases are more specific
            if kw_lower in question_lower:
                if word_count >= 4:
                    score += 0.6  # Very specific phrase ("mean time to recovery")
                elif word_count >= 3:
                    score += 0.5  # Specific phrase ("time to recovery")
                elif word_count >= 2:
                    score += 0.4  # Two-word phrase ("cost change")
                else:
                    score += 0.3  # Single word
                matched.append(f"kw:{kw}")
            # Token overlap match
            elif kw_tokens & expanded_tokens:
                overlap = len(kw_tokens & expanded_tokens) / len(kw_tokens)
                score += 0.2 * overlap
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
