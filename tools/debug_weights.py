#!/usr/bin/env python3
"""
Debug script to verify keyword weights and prevent revenue/ARR confusion.

This script MUST throw an error if:
1. finops.arr gets points for the word "revenue"
2. A revenue query resolves to ARR with low confidence

Usage:
    python tools/debug_weights.py
"""
import sys
sys.path.insert(0, "/home/user/AOS-DCLv2")

from backend.nlq.intent_matcher import match_question_with_details
from backend.bll.definitions import get_definition


def debug_match(question: str) -> dict:
    """Get detailed match info for a question."""
    result = match_question_with_details(question, top_k=5)

    print(f"\n{'='*60}")
    print(f"Question: \"{question}\"")
    print(f"{'='*60}")
    print(f"Best Match: {result.best_match}")
    print(f"Confidence: {result.confidence:.3f}")
    print(f"Matched Keywords: {result.matched_keywords}")
    print(f"Is Ambiguous: {result.is_ambiguous}")
    print(f"Ambiguity Gap: {result.ambiguity_gap:.3f}")

    print(f"\nTop Candidates:")
    for i, candidate in enumerate(result.top_candidates[:5]):
        defn = get_definition(candidate.definition_id)
        keywords = defn.keywords if defn else []
        print(f"  {i+1}. {candidate.definition_id}")
        print(f"     Score: {candidate.score:.3f}")
        print(f"     Triggered By: {candidate.triggered_by}")
        print(f"     Keywords: {keywords[:5]}...")  # First 5 keywords

    return {
        "question": question,
        "best_match": result.best_match,
        "confidence": result.confidence,
        "matched_keywords": result.matched_keywords,
        "candidates": result.top_candidates,
    }


def verify_no_revenue_in_arr():
    """Verify that finops.arr does NOT have 'revenue' as a keyword."""
    defn = get_definition("finops.arr")
    if defn is None:
        print("WARNING: finops.arr definition not found")
        return True

    keywords = [kw.lower() for kw in defn.keywords]

    # Check for poisoned keywords
    poisoned = []
    if "revenue" in keywords:
        poisoned.append("revenue")

    if poisoned:
        print(f"\n{'!'*60}")
        print("CRITICAL ERROR: finops.arr contains poisoned keywords!")
        print(f"Poisoned keywords found: {poisoned}")
        print(f"{'!'*60}")
        return False

    print(f"\n✅ finops.arr keywords are clean (no generic 'revenue')")
    print(f"   Keywords: {defn.keywords}")
    return True


def verify_revenue_query_not_arr(question: str):
    """Verify that a revenue query does NOT resolve to ARR."""
    result = match_question_with_details(question, top_k=5)

    # Check if finops.arr is in the top candidates
    arr_in_top = False
    arr_score = 0.0
    for candidate in result.top_candidates[:3]:
        if candidate.definition_id == "finops.arr":
            arr_in_top = True
            arr_score = candidate.score
            # Check if 'revenue' triggered the match
            if "revenue" in candidate.triggered_by:
                print(f"\n{'!'*60}")
                print(f"CRITICAL ERROR: finops.arr matched on 'revenue' keyword!")
                print(f"Question: {question}")
                print(f"Triggered by: {candidate.triggered_by}")
                print(f"{'!'*60}")
                return False

    # If best match is ARR for a revenue query, this is wrong
    if result.best_match == "finops.arr" and "revenue" in question.lower():
        print(f"\n{'!'*60}")
        print(f"CRITICAL ERROR: Revenue query resolved to finops.arr!")
        print(f"Question: {question}")
        print(f"Best Match: {result.best_match}")
        print(f"Confidence: {result.confidence:.3f}")
        print(f"{'!'*60}")
        return False

    print(f"\n✅ Revenue query correctly NOT matching ARR")
    print(f"   Question: {question}")
    print(f"   Best Match: {result.best_match} (conf={result.confidence:.3f})")
    if arr_in_top:
        print(f"   ARR was in top 3 with score {arr_score:.3f} (acceptable if not triggered by 'revenue')")
    return True


def main():
    print("="*60)
    print("DEBUG WEIGHTS VERIFICATION")
    print("="*60)

    errors = []

    # Step 1: Verify ARR keywords are clean
    if not verify_no_revenue_in_arr():
        errors.append("finops.arr contains poisoned 'revenue' keyword")

    # Step 2: Debug specific queries
    test_queries = [
        "revenue last month",
        "what was revenue last year",
        "total money from sales last year",
        "annual earnings from transactions",
        "current ARR",
        "what is our arr",
    ]

    print("\n" + "="*60)
    print("QUERY SCORE BREAKDOWN")
    print("="*60)

    for query in test_queries:
        debug_match(query)

    # Step 3: Verify revenue queries don't match ARR
    print("\n" + "="*60)
    print("REVENUE vs ARR VERIFICATION")
    print("="*60)

    revenue_queries = [
        "revenue last month",
        "what was revenue last year",
        "total money from sales",
        "annual earnings",
    ]

    for query in revenue_queries:
        if not verify_revenue_query_not_arr(query):
            errors.append(f"Revenue query matched ARR: {query}")

    # Final report
    print("\n" + "="*60)
    print("FINAL VERIFICATION RESULT")
    print("="*60)

    if errors:
        print(f"\n❌ VERIFICATION FAILED with {len(errors)} errors:")
        for err in errors:
            print(f"   - {err}")
        sys.exit(1)
    else:
        print("\n✅ ALL VERIFICATIONS PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
