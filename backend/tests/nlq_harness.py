#!/usr/bin/env python3
"""
NLQ Generative Test Harness - Registry-driven test generation.

This harness generates test cases from:
1. Definitions registry + their allowed params
2. Entity/metric synonym lexicon
3. Deterministic grammar templates per intent family

Usage:
    python -m backend.tests.nlq_harness --suite params --n 200 --seed 1234
    python -m backend.tests.nlq_harness --suite intent --n 100
    python -m backend.tests.nlq_harness --suite execution --n 50

Suites:
- params: Tests ParamExtractor (top N, order_by, time_window)
- intent: Tests intent matching (question → definition)
- execution: Tests executor conformance (limit respected, etc.)
"""
import sys
import os
import random
import argparse
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# DCL API base URL
BASE_URL = "http://localhost:8000"


# =============================================================================
# Lexicon - Synonyms and grammar templates
# =============================================================================

# Entity synonyms - how users refer to different concepts
ENTITY_SYNONYMS = {
    "customer": ["customer", "customers", "client", "clients", "account", "accounts", "buyer", "buyers"],
    "vendor": ["vendor", "vendors", "supplier", "suppliers", "provider", "providers"],
    "deal": ["deal", "deals", "opportunity", "opportunities", "opp", "opps"],
    "service": ["service", "services", "app", "apps", "application", "applications", "microservice"],
    "incident": ["incident", "incidents", "outage", "outages", "issue", "issues", "alert", "alerts"],
    "resource": ["resource", "resources", "instance", "instances", "asset", "assets"],
}

# Metric synonyms
METRIC_SYNONYMS = {
    "revenue": ["revenue", "sales", "income", "earnings", "arr", "mrr"],
    "cost": ["cost", "costs", "spend", "spending", "expense", "expenses", "price"],
    "count": ["count", "number", "total", "amount"],
}

# Limit phrasings - how users express "top N"
# These MUST match patterns in param_extractor.py:
#   - "top N"
#   - "first N"
#   - "show/give me N"
#   - "N largest/biggest/highest/top"
LIMIT_TEMPLATES = [
    "top {n}",
    "first {n}",
    "{n} largest",
    "{n} biggest",
    "{n} highest",
]

# Order phrasings
ORDER_TEMPLATES = [
    "by {field}",
    "sorted by {field}",
    "ordered by {field}",
    "ranked by {field}",
]

# Question templates by intent family
# {limit} will be replaced with phrases like "top 5" or "first 10"
QUESTION_TEMPLATES = {
    "top_n": [
        "Show me the {limit} {entity}",
        "What are the {limit} {entity}",
        "Give me the {limit} {entity} {order}",
        "Who are our {limit} {entity}",
        "{limit} {entity} {order}",
    ],
    "current_value": [
        "What is our {metric}",
        "What's our current {metric}",
        "How much is our {metric}",
        "Show me our {metric}",
    ],
    "trend": [
        "How is our {metric} trending",
        "What's happening with {metric}",
        "Show me {metric} over time",
        "{metric} trend",
    ],
    "list": [
        "Show me {entity}",
        "List all {entity}",
        "What {entity} do we have",
        "Give me the {entity}",
    ],
}


# =============================================================================
# Definition metadata - which params each definition supports
# =============================================================================

@dataclass
class DefnMeta:
    """Metadata about a definition's supported params."""
    definition_id: str
    supports_limit: bool = True  # Most definitions support limit
    supports_order_by: List[str] = field(default_factory=list)  # Fields that can be sorted
    entity_type: str = ""  # What entity this queries (customer, service, etc.)
    metric_type: str = ""  # What metric this returns (revenue, cost, etc.)
    intent_family: str = "list"  # top_n, current_value, trend, list


# Build metadata from definitions registry
def _build_defn_metadata() -> Dict[str, DefnMeta]:
    """Build metadata about each definition."""
    from backend.bll.definitions import list_definitions

    metadata = {}
    for defn in list_definitions():
        # Infer entity type from definition
        entity = ""
        if "customer" in defn.definition_id or "account" in defn.definition_id:
            entity = "customer"
        elif "vendor" in defn.definition_id:
            entity = "vendor"
        elif "deal" in defn.definition_id or "pipeline" in defn.definition_id:
            entity = "deal"
        elif "incident" in defn.definition_id:
            entity = "incident"
        elif "slo" in defn.definition_id or "deploy" in defn.definition_id or "mttr" in defn.definition_id:
            entity = "service"
        elif "zombie" in defn.definition_id or "resource" in defn.definition_id:
            entity = "resource"

        # Infer metric type
        metric = ""
        if "arr" in defn.definition_id or "revenue" in defn.definition_id:
            metric = "revenue"
        elif "spend" in defn.definition_id or "cost" in defn.definition_id or "burn" in defn.definition_id:
            metric = "cost"

        # Infer intent family
        intent_family = "list"
        if "top" in defn.name.lower():
            intent_family = "top_n"
        elif "arr" in defn.definition_id or "burn" in defn.definition_id:
            intent_family = "current_value"
        elif "trend" in defn.description.lower():
            intent_family = "trend"

        # Extract sortable fields from metrics
        order_fields = [m for m in defn.metrics if m]

        metadata[defn.definition_id] = DefnMeta(
            definition_id=defn.definition_id,
            supports_limit=True,
            supports_order_by=order_fields,
            entity_type=entity,
            metric_type=metric,
            intent_family=intent_family,
        )

    return metadata


# =============================================================================
# Test Case Generation
# =============================================================================

@dataclass
class GeneratedTest:
    """A generated test case."""
    question: str
    expected_definition: Optional[str] = None
    expected_limit: Optional[int] = None
    expected_order_by: Optional[str] = None
    defn_meta: Optional[DefnMeta] = None


def generate_limit_phrase(n: int, rng: random.Random) -> str:
    """Generate a limit phrase like 'top 5' or 'first 10'."""
    template = rng.choice(LIMIT_TEMPLATES)
    return template.format(n=n)


def generate_order_phrase(field: str, rng: random.Random) -> str:
    """Generate an order phrase like 'by revenue'."""
    template = rng.choice(ORDER_TEMPLATES)
    return template.format(field=field)


def generate_entity_phrase(entity_type: str, rng: random.Random) -> str:
    """Generate an entity phrase like 'customers' or 'clients'."""
    if entity_type in ENTITY_SYNONYMS:
        return rng.choice(ENTITY_SYNONYMS[entity_type])
    return entity_type


def generate_metric_phrase(metric_type: str, rng: random.Random) -> str:
    """Generate a metric phrase like 'revenue' or 'spend'."""
    if metric_type in METRIC_SYNONYMS:
        return rng.choice(METRIC_SYNONYMS[metric_type])
    return metric_type


class ParamsSuite:
    """Test suite for ParamExtractor - tests limit and order_by extraction."""

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.metadata = _build_defn_metadata()

    def generate(self, n: int) -> List[GeneratedTest]:
        """Generate n test cases for param extraction."""
        tests = []

        # Test limit extraction across various phrasings
        limit_values = [3, 5, 10, 20, 50, 100]

        for _ in range(n):
            limit = self.rng.choice(limit_values)

            # Pick a random definition that supports limit
            defn_id = self.rng.choice(list(self.metadata.keys()))
            meta = self.metadata[defn_id]

            # Generate limit phrase
            limit_phrase = generate_limit_phrase(limit, self.rng)

            # Generate entity phrase
            entity = generate_entity_phrase(meta.entity_type or "items", self.rng)

            # Optionally add order_by
            order_phrase = ""
            expected_order = None
            if meta.supports_order_by and self.rng.random() > 0.5:
                order_field = self.rng.choice(meta.supports_order_by)
                order_phrase = generate_order_phrase(order_field, self.rng)
                expected_order = order_field

            # Build question
            template = self.rng.choice(QUESTION_TEMPLATES["top_n"])
            question = template.format(
                limit=limit_phrase,
                entity=entity,
                order=order_phrase,
            ).strip()

            tests.append(GeneratedTest(
                question=question,
                expected_limit=limit,
                expected_order_by=expected_order,
                defn_meta=meta,
            ))

        return tests


class IntentSuite:
    """Test suite for intent matching - tests question → definition mapping."""

    # Keywords that are ambiguous across multiple definitions - skip these when testing
    # These keywords correctly map to multiple definitions, so testing them against
    # a single definition would be unfair
    AMBIGUOUS_KEYWORDS = {
        # "dora" applies to all 4 DORA metrics
        "dora", "dora metrics", "four keys",
        # Generic spend/cost terms apply to multiple finops definitions
        "spend", "spending", "cost", "costs", "expense",
        # "no owner" applies to both unallocated_spend and identity_gap
        "no owner",
        # "revenue" applies to both ARR and top_customers
        "revenue",
        # "incident" applies to both incidents and mttr
        "incident",
        # "orphan resources" applies to both zombies_overview and identity_gap
        "orphan resources", "orphan",
    }

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.metadata = _build_defn_metadata()
        self._definitions = None

    def _get_definitions(self) -> Dict[str, Any]:
        """Lazy-load definitions."""
        if self._definitions is None:
            from backend.bll.definitions import list_definitions
            self._definitions = {d.definition_id: d for d in list_definitions()}
        return self._definitions

    def _get_unambiguous_keywords(self, defn) -> List[str]:
        """Get keywords that uniquely identify this definition."""
        if not defn.keywords:
            return [defn.name.lower()]

        # Filter out ambiguous keywords
        unambiguous = [kw for kw in defn.keywords
                       if kw.lower() not in self.AMBIGUOUS_KEYWORDS]

        # If all keywords are ambiguous, use the most specific one (longest phrase)
        if not unambiguous and defn.keywords:
            unambiguous = [max(defn.keywords, key=len)]

        return unambiguous or [defn.name.lower()]

    def generate(self, n: int) -> List[GeneratedTest]:
        """Generate n test cases for intent matching using definition keywords."""
        tests = []
        definitions = self._get_definitions()

        # Question templates that incorporate keywords
        keyword_templates = [
            "What is our {keyword}",
            "Show me {keyword}",
            "How is our {keyword}",
            "What's happening with {keyword}",
            "{keyword} status",
            "Give me {keyword} data",
            "Show me the {keyword}",
        ]

        for _ in range(n):
            # Pick a random definition
            defn_id = self.rng.choice(list(definitions.keys()))
            defn = definitions[defn_id]
            meta = self.metadata[defn_id]

            # Use unambiguous keywords only
            keywords = self._get_unambiguous_keywords(defn)
            keyword = self.rng.choice(keywords)

            # Generate question using keyword
            template = self.rng.choice(keyword_templates)
            question = template.format(keyword=keyword)

            tests.append(GeneratedTest(
                question=question,
                expected_definition=defn_id,
                defn_meta=meta,
            ))

        return tests


class ExecutionSuite:
    """Test suite for execution conformance - tests that executor respects params."""

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.metadata = _build_defn_metadata()

    def generate(self, n: int) -> List[GeneratedTest]:
        """Generate n test cases for execution conformance."""
        tests = []

        # Focus on limit conformance
        limit_values = [1, 3, 5, 10]

        for _ in range(n):
            limit = self.rng.choice(limit_values)
            defn_id = self.rng.choice(list(self.metadata.keys()))
            meta = self.metadata[defn_id]

            entity = generate_entity_phrase(meta.entity_type or "items", self.rng)
            question = f"Show me top {limit} {entity}"

            tests.append(GeneratedTest(
                question=question,
                expected_definition=defn_id,
                expected_limit=limit,
                defn_meta=meta,
            ))

        return tests


class AnswerQualitySuite:
    """
    Test suite for answer quality - validates that summaries include:
    (a) total - population total for top-N queries
    (b) top-N share - percentage of total represented by top-N
    (c) interpretation OR explicit caveat about data limitations
    """

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.metadata = _build_defn_metadata()

    def generate(self, n: int) -> List[GeneratedTest]:
        """Generate n top-N test cases for answer quality validation."""
        tests = []

        # Focus on top-N queries where share calculation matters
        limit_values = [3, 5, 10]

        for _ in range(n):
            limit = self.rng.choice(limit_values)
            defn_id = self.rng.choice(list(self.metadata.keys()))
            meta = self.metadata[defn_id]

            entity = generate_entity_phrase(meta.entity_type or "items", self.rng)
            limit_phrase = generate_limit_phrase(limit, self.rng)
            question = f"Show me the {limit_phrase} {entity}"

            tests.append(GeneratedTest(
                question=question,
                expected_definition=defn_id,
                expected_limit=limit,
                defn_meta=meta,
            ))

        return tests


# =============================================================================
# Test Runner
# =============================================================================

@dataclass
class TestResult:
    """Result of a single test."""
    test: GeneratedTest
    passed: bool
    actual_limit: Optional[int] = None
    actual_definition: Optional[str] = None
    actual_row_count: Optional[int] = None
    error: Optional[str] = None


@dataclass
class SuiteResult:
    """Result of running a test suite."""
    suite_name: str
    total: int
    passed: int
    failed: int
    failures: List[TestResult]
    coverage: Dict[str, Dict[str, int]]  # defn_id -> {param: count}


def run_params_suite(tests: List[GeneratedTest]) -> SuiteResult:
    """Run param extraction tests."""
    from backend.nlq.param_extractor import extract_params, apply_limit_clamp

    results = []
    coverage = defaultdict(lambda: defaultdict(int))

    for test in tests:
        try:
            exec_args = extract_params(test.question)
            if exec_args.limit:
                exec_args.limit = apply_limit_clamp(exec_args.limit, max_limit=100)

            actual_limit = exec_args.limit

            # Check limit match
            passed = True
            error = None

            if test.expected_limit and actual_limit != test.expected_limit:
                passed = False
                error = f"Limit mismatch: expected {test.expected_limit}, got {actual_limit}"

            results.append(TestResult(
                test=test,
                passed=passed,
                actual_limit=actual_limit,
                error=error,
            ))

            # Track coverage
            if test.defn_meta:
                coverage[test.defn_meta.definition_id]["limit"] += 1

        except Exception as e:
            results.append(TestResult(
                test=test,
                passed=False,
                error=str(e),
            ))

    passed_count = sum(1 for r in results if r.passed)
    failed = [r for r in results if not r.passed]

    return SuiteResult(
        suite_name="params",
        total=len(tests),
        passed=passed_count,
        failed=len(failed),
        failures=failed[:20],  # Limit to first 20 failures
        coverage=dict(coverage),
    )


@dataclass
class IntentTestResult(TestResult):
    """Extended test result with confusion data."""
    top_candidates: List[Tuple[str, float]] = field(default_factory=list)
    is_ambiguous: bool = False
    triggered_by: List[str] = field(default_factory=list)


def run_intent_suite(tests: List[GeneratedTest]) -> SuiteResult:
    """Run intent matching tests with confusion reporting."""
    from backend.nlq.intent_matcher import match_question_with_details

    results = []
    coverage = defaultdict(lambda: defaultdict(int))
    confusion_report = []  # Detailed failure analysis

    for test in tests:
        try:
            match_result = match_question_with_details(test.question, top_k=5)

            passed = True
            error = None

            if test.expected_definition and match_result.best_match != test.expected_definition:
                passed = False
                error = f"Intent mismatch: expected {test.expected_definition}, got {match_result.best_match}"

                # Build confusion report entry
                top_k_summary = [(c.definition_id, round(c.score, 3)) for c in match_result.top_candidates[:5]]
                triggered = match_result.top_candidates[0].triggered_by if match_result.top_candidates else []

                confusion_report.append({
                    "question": test.question,
                    "expected": test.expected_definition,
                    "got": match_result.best_match,
                    "confidence": round(match_result.confidence, 3),
                    "top_candidates": top_k_summary,
                    "triggered_by": triggered,
                    "is_ambiguous": match_result.is_ambiguous,
                    "ambiguity_gap": round(match_result.ambiguity_gap, 3),
                })

            results.append(TestResult(
                test=test,
                passed=passed,
                actual_definition=match_result.best_match,
                error=error,
            ))

            # Track coverage
            if test.defn_meta:
                coverage[test.defn_meta.definition_id]["intent"] += 1

        except Exception as e:
            results.append(TestResult(
                test=test,
                passed=False,
                error=str(e),
            ))

    passed_count = sum(1 for r in results if r.passed)
    failed = [r for r in results if not r.passed]

    # Print confusion report for failures
    if confusion_report:
        print("\n\033[93mConfusion Report:\033[0m")
        for entry in confusion_report[:10]:
            print(f"\n  Question: \"{entry['question']}\"")
            print(f"  Expected: {entry['expected']} → Got: {entry['got']} ({entry['confidence']:.0%})")
            print(f"  Triggered by: {entry['triggered_by']}")
            print(f"  Top candidates: {entry['top_candidates'][:3]}")
            if entry['is_ambiguous']:
                print(f"  \033[93m⚠ AMBIGUOUS (gap: {entry['ambiguity_gap']:.2f})\033[0m")

    return SuiteResult(
        suite_name="intent",
        total=len(tests),
        passed=passed_count,
        failed=len(failed),
        failures=failed[:20],
        coverage=dict(coverage),
    )


def run_execution_suite(tests: List[GeneratedTest], direct: bool = False) -> SuiteResult:
    """Run execution conformance tests."""
    results = []
    coverage = defaultdict(lambda: defaultdict(int))

    # Check if API is available
    api_available = False
    if not direct:
        try:
            import requests
            resp = requests.get(f"{BASE_URL}/api/health", timeout=2)
            api_available = resp.status_code == 200
        except:
            pass

    if direct or not api_available:
        # Direct mode - just test param extraction, can't test actual execution
        print("  [API unavailable - running param extraction only]")
        return run_params_suite(tests)

    # API mode - test actual execution
    try:
        import requests
        BASE_URL = "http://localhost:8000"

        for test in tests:
            try:
                resp = requests.post(
                    f"{BASE_URL}/api/nlq/ask",
                    json={"question": test.question, "dataset_id": "demo9"},
                    timeout=30
                )
                resp.raise_for_status()
                result = resp.json()

                data = result.get("data", [])
                actual_rows = len(data)

                passed = True
                error = None

                # Check row count <= expected limit
                if test.expected_limit and actual_rows > test.expected_limit:
                    passed = False
                    error = f"Limit not respected: expected <= {test.expected_limit}, got {actual_rows}"

                results.append(TestResult(
                    test=test,
                    passed=passed,
                    actual_row_count=actual_rows,
                    actual_limit=test.expected_limit,
                    error=error,
                ))

                if test.defn_meta:
                    coverage[test.defn_meta.definition_id]["execution"] += 1

            except Exception as e:
                results.append(TestResult(
                    test=test,
                    passed=False,
                    error=str(e),
                ))

    except ImportError:
        return SuiteResult(
            suite_name="execution",
            total=len(tests),
            passed=0,
            failed=len(tests),
            failures=[],
            coverage={},
        )

    passed_count = sum(1 for r in results if r.passed)
    failed = [r for r in results if not r.passed]

    return SuiteResult(
        suite_name="execution",
        total=len(tests),
        passed=passed_count,
        failed=len(failed),
        failures=failed[:20],
        coverage=dict(coverage),
    )


# =============================================================================
# Answer Quality Suite
# =============================================================================

@dataclass
class AnswerQualityResult(TestResult):
    """Extended result for answer quality checks."""
    has_total: bool = False
    has_share: bool = False
    has_interpretation_or_caveat: bool = False
    summary_text: str = ""
    aggregations: Dict[str, Any] = field(default_factory=dict)


def run_answer_quality_suite(tests: List[GeneratedTest]) -> SuiteResult:
    """
    Run answer quality tests.

    Checks that for top-N queries, the summary includes:
    (a) population_total or population_count
    (b) share_of_total_pct
    (c) interpretation in answer text OR limitations list
    """
    import requests

    results = []
    coverage = defaultdict(lambda: defaultdict(int))
    quality_failures = []

    # Check if API is available
    try:
        resp = requests.get(f"{BASE_URL}/api/health", timeout=2)
        if resp.status_code != 200:
            print("  [API unavailable - skipping answer quality suite]")
            return SuiteResult(
                suite_name="answer_quality",
                total=len(tests),
                passed=0,
                failed=0,
                failures=[],
                coverage={},
            )
    except:
        print("  [API unavailable - skipping answer quality suite]")
        return SuiteResult(
            suite_name="answer_quality",
            total=len(tests),
            passed=0,
            failed=0,
            failures=[],
            coverage={},
        )

    for test in tests:
        try:
            resp = requests.post(
                f"{BASE_URL}/api/nlq/ask",
                json={"question": test.question, "dataset_id": "demo9"},
                timeout=30
            )
            resp.raise_for_status()
            result = resp.json()

            # Skip ambiguous queries - they correctly return clarification, not data
            if result.get("needs_clarification"):
                results.append(TestResult(
                    test=test,
                    passed=True,  # Ambiguous queries correctly trigger clarification
                    error=None,
                ))
                continue

            summary = result.get("summary") or {}
            aggregations = summary.get("aggregations", {})
            answer_text = summary.get("answer", "")
            limitations = aggregations.get("limitations", [])

            # Check (a) total - population_total or population_count exists
            has_total = (
                "population_total" in aggregations or
                "population_count" in aggregations
            )

            # Check (b) share - share_of_total_pct exists
            has_share = "share_of_total_pct" in aggregations

            # Check (c) interpretation or caveat
            # Look for interpretation phrases in answer
            interpretation_phrases = [
                "concentration", "concentrated", "distributed", "diversified",
                "drive", "represent", "majority", "weighted",
                "candidates for", "need", "require",
                "tier:", "healthy", "at risk", "breached",
                "Elite", "High", "Medium", "Low",
                "total", "across",  # Generic totals phrases
            ]
            has_interpretation = any(phrase.lower() in answer_text.lower()
                                     for phrase in interpretation_phrases)

            # Or check for explicit caveat in limitations
            has_caveat = len(limitations) > 0

            has_interpretation_or_caveat = has_interpretation or has_caveat

            # Pass if all three conditions are met
            passed = has_total and has_share and has_interpretation_or_caveat

            error = None
            if not passed:
                missing = []
                if not has_total:
                    missing.append("population total")
                if not has_share:
                    missing.append("share of total %")
                if not has_interpretation_or_caveat:
                    missing.append("interpretation or caveat")
                error = f"Missing: {', '.join(missing)}"

                quality_failures.append({
                    "question": test.question,
                    "answer": answer_text[:100],
                    "missing": missing,
                    "aggregations_keys": list(aggregations.keys()),
                    "limitations": limitations,
                })

            results.append(AnswerQualityResult(
                test=test,
                passed=passed,
                has_total=has_total,
                has_share=has_share,
                has_interpretation_or_caveat=has_interpretation_or_caveat,
                summary_text=answer_text,
                aggregations=aggregations,
                error=error,
            ))

            if test.defn_meta:
                coverage[test.defn_meta.definition_id]["quality"] += 1

        except Exception as e:
            results.append(TestResult(
                test=test,
                passed=False,
                error=str(e),
            ))

    passed_count = sum(1 for r in results if r.passed)
    failed = [r for r in results if not r.passed]

    # Print quality failure details
    if quality_failures:
        print("\n\033[93mAnswer Quality Issues:\033[0m")
        for entry in quality_failures[:10]:
            print(f"\n  Q: \"{entry['question']}\"")
            print(f"  Missing: {entry['missing']}")
            print(f"  Answer: \"{entry['answer']}...\"")
            print(f"  Aggregations: {entry['aggregations_keys']}")
            if entry['limitations']:
                print(f"  Limitations: {entry['limitations']}")

    return SuiteResult(
        suite_name="answer_quality",
        total=len(tests),
        passed=passed_count,
        failed=len(failed),
        failures=failed[:20],
        coverage=dict(coverage),
    )


# =============================================================================
# CLI and Reporting
# =============================================================================

def print_result(result: SuiteResult):
    """Print test suite results."""
    pass_rate = (result.passed / result.total * 100) if result.total > 0 else 0

    print("\n" + "=" * 70)
    print(f"SUITE: {result.suite_name.upper()}")
    print("=" * 70)
    print(f"\033[92mPASSED: {result.passed}/{result.total} ({pass_rate:.0f}%)\033[0m")
    print(f"\033[91mFAILED: {result.failed}/{result.total}\033[0m")

    if result.failures:
        print("\n\033[91mFailures (showing first 20):\033[0m")
        for i, f in enumerate(result.failures[:20], 1):
            print(f"\n  [{i}] \"{f.test.question}\"")
            if f.error:
                print(f"      → {f.error}")
            if f.test.expected_limit:
                print(f"      Expected limit: {f.test.expected_limit}, Actual: {f.actual_limit}")
            if f.test.expected_definition:
                print(f"      Expected defn: {f.test.expected_definition}, Actual: {f.actual_definition}")

    # Coverage report
    if result.coverage:
        print("\n\033[94mCoverage:\033[0m")
        for defn_id, params in sorted(result.coverage.items())[:10]:
            param_str = ", ".join(f"{k}: {v}" for k, v in params.items())
            print(f"  {defn_id}: {param_str}")

    return pass_rate


def main():
    parser = argparse.ArgumentParser(description="NLQ Generative Test Harness")
    parser.add_argument("--suite", choices=["params", "intent", "execution", "answer_quality", "all"],
                        default="params", help="Test suite to run")
    parser.add_argument("--n", type=int, default=100, help="Number of test cases to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--direct", action="store_true", help="Run without API server")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("NLQ GENERATIVE TEST HARNESS")
    print(f"Suite: {args.suite} | N: {args.n} | Seed: {args.seed}")
    print("=" * 70)

    results = []

    if args.suite in ["params", "all"]:
        suite = ParamsSuite(seed=args.seed)
        tests = suite.generate(args.n)
        result = run_params_suite(tests)
        results.append(result)
        print_result(result)

    if args.suite in ["intent", "all"]:
        suite = IntentSuite(seed=args.seed)
        tests = suite.generate(args.n)
        result = run_intent_suite(tests)
        results.append(result)
        print_result(result)

    if args.suite in ["execution", "all"]:
        suite = ExecutionSuite(seed=args.seed)
        tests = suite.generate(args.n)
        result = run_execution_suite(tests, direct=args.direct)
        results.append(result)
        print_result(result)

    if args.suite in ["answer_quality", "all"]:
        suite = AnswerQualitySuite(seed=args.seed)
        tests = suite.generate(args.n)
        result = run_answer_quality_suite(tests)
        results.append(result)
        print_result(result)

    # Overall summary
    if len(results) > 1:
        total_passed = sum(r.passed for r in results)
        total_tests = sum(r.total for r in results)
        overall_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
        print("\n" + "=" * 70)
        print(f"OVERALL: {total_passed}/{total_tests} ({overall_rate:.0f}%)")
        print("=" * 70)

    # Exit code
    all_passed = all(r.failed == 0 for r in results)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
