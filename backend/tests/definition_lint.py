#!/usr/bin/env python3
"""
Definition Linter - Enforces quality standards for BLL definitions.

This linter checks that definitions have:
1. Sufficient keywords (at least 3)
2. At least one multi-word keyword (2+ words) for specificity
3. No duplicate keywords across definitions
4. Valid source references
5. At least one metric or dimension

Run as part of CI to catch definition quality issues before they cause
NLQ matching problems.

Usage:
    python -m backend.tests.definition_lint
"""
import sys
import os
from dataclasses import dataclass, field
from typing import List, Dict, Set
from collections import defaultdict

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@dataclass
class LintIssue:
    """A single lint issue."""
    definition_id: str
    severity: str  # "error", "warning"
    message: str
    rule: str


@dataclass
class LintResult:
    """Result of linting all definitions."""
    issues: List[LintIssue] = field(default_factory=list)
    definitions_checked: int = 0

    @property
    def errors(self) -> List[LintIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> List[LintIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0


# Minimum requirements
MIN_KEYWORDS = 3
MIN_MULTIWORD_KEYWORDS = 1  # At least one 2+ word keyword for specificity


def lint_definitions() -> LintResult:
    """Run all linting rules on definitions."""
    from backend.bll.definitions import list_definitions

    result = LintResult()
    definitions = list_definitions()
    result.definitions_checked = len(definitions)

    # Track keywords across all definitions for duplicate detection
    keyword_to_defns: Dict[str, List[str]] = defaultdict(list)

    for defn in definitions:
        defn_id = defn.definition_id

        # Rule 1: Minimum keyword count
        if len(defn.keywords) < MIN_KEYWORDS:
            result.issues.append(LintIssue(
                definition_id=defn_id,
                severity="error",
                message=f"Has {len(defn.keywords)} keywords, minimum is {MIN_KEYWORDS}",
                rule="min_keywords",
            ))

        # Rule 2: At least one multi-word keyword
        multiword_count = sum(1 for kw in defn.keywords if len(kw.split()) >= 2)
        if multiword_count < MIN_MULTIWORD_KEYWORDS:
            result.issues.append(LintIssue(
                definition_id=defn_id,
                severity="warning",
                message=f"No multi-word keywords. Add specific phrases like '{defn.name.lower()}'",
                rule="multiword_keywords",
            ))

        # Track keywords for duplicate detection
        for kw in defn.keywords:
            kw_lower = kw.lower().strip()
            keyword_to_defns[kw_lower].append(defn_id)

        # Rule 3: Has at least one metric or dimension
        if not defn.metrics and not defn.dimensions:
            result.issues.append(LintIssue(
                definition_id=defn_id,
                severity="warning",
                message="No metrics or dimensions defined",
                rule="has_schema",
            ))

        # Rule 4: Has output schema
        if not defn.output_schema:
            result.issues.append(LintIssue(
                definition_id=defn_id,
                severity="warning",
                message="No output schema defined",
                rule="has_output_schema",
            ))

        # Rule 5: Has sources
        if not defn.sources:
            result.issues.append(LintIssue(
                definition_id=defn_id,
                severity="warning",
                message="No data sources defined",
                rule="has_sources",
            ))

        # Rule 6: Description is meaningful (not too short)
        if len(defn.description) < 20:
            result.issues.append(LintIssue(
                definition_id=defn_id,
                severity="warning",
                message=f"Description too short ({len(defn.description)} chars)",
                rule="meaningful_description",
            ))

    # Rule 7: Check for duplicate keywords across definitions
    # Some duplicates are OK (synonyms), but exact matches are suspicious
    for kw, defn_ids in keyword_to_defns.items():
        if len(defn_ids) > 1:
            # Only warn about single-word generic keywords shared across definitions
            if len(kw.split()) == 1 and kw in ["spend", "cost", "revenue", "incident"]:
                # These are intentionally ambiguous - skip
                pass
            elif len(defn_ids) > 2:
                # Same keyword in 3+ definitions is definitely a problem
                result.issues.append(LintIssue(
                    definition_id=defn_ids[0],
                    severity="warning",
                    message=f"Keyword '{kw}' shared by {len(defn_ids)} definitions: {', '.join(defn_ids)}",
                    rule="unique_keywords",
                ))

    return result


def print_result(result: LintResult) -> bool:
    """Print lint results and return True if passed."""
    print("\n" + "=" * 70)
    print("DEFINITION LINTER")
    print(f"Checked: {result.definitions_checked} definitions")
    print("=" * 70)

    if result.errors:
        print(f"\n\033[91mERRORS ({len(result.errors)}):\033[0m")
        for issue in result.errors:
            print(f"  [{issue.definition_id}] {issue.message}")

    if result.warnings:
        print(f"\n\033[93mWARNINGS ({len(result.warnings)}):\033[0m")
        for issue in result.warnings:
            print(f"  [{issue.definition_id}] {issue.message}")

    if result.passed:
        print(f"\n\033[92m✓ PASSED\033[0m (0 errors, {len(result.warnings)} warnings)")
    else:
        print(f"\n\033[91m✗ FAILED\033[0m ({len(result.errors)} errors)")

    return result.passed


def main():
    result = lint_definitions()
    passed = print_result(result)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
