"""
COFA Completeness Gate Tests
=============================
7 test cases validating the COFACompletionGate.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.engine.cofa_validation import COFACompletionGate


def make_source_coa(count: int, start: int = 1000) -> list[dict]:
    """Generate a source CoA with N accounts."""
    return [
        {"account_number": str(start + i * 100), "account_name": f"Account {start + i * 100}"}
        for i in range(count)
    ]


def make_mappings_from_coa(
    source_coa: list[dict],
    skip_indices: list[int] | None = None,
    field_name: str = "entity_a_account_number",
) -> list[dict]:
    """Generate mapping entries covering source accounts, optionally skipping some."""
    skip = set(skip_indices or [])
    return [
        {
            field_name: acct["account_number"],
            "unified_account_name": acct["account_name"],
            "unified_type": "Asset",
        }
        for i, acct in enumerate(source_coa)
        if i not in skip
    ]


def test_complete_mapping_passes():
    """Test 1: Complete mapping passes (45 source, 45 mapped)."""
    gate = COFACompletionGate()
    source = make_source_coa(45)
    mappings = make_mappings_from_coa(source)
    result = gate.validate_mapping_completeness(source, mappings)
    assert result["complete"] is True, f"got complete={result['complete']}"


def test_incomplete_mapping_fails():
    """Test 2: Incomplete mapping fails (45 source, 44 mapped, missing index 4)."""
    gate = COFACompletionGate()
    source = make_source_coa(45)
    mappings = make_mappings_from_coa(source, skip_indices=[4])
    result = gate.validate_and_reject(source, mappings)
    assert result["complete"] is False, f"got complete={result['complete']}"


def test_rejection_message_includes_account_details():
    """Test 3: Rejection message includes account details."""
    gate = COFACompletionGate()
    source = make_source_coa(45)
    mappings = make_mappings_from_coa(source, skip_indices=[4])
    result = gate.validate_and_reject(source, mappings)
    orphaned = result.get("orphaned_accounts", [])
    assert len(orphaned) > 0 and "account_number" in orphaned[0], f"orphaned={orphaned}"
    assert "account_name" in orphaned[0], f"orphaned={orphaned}"
    assert "rejection_message" in result and "1400" in result.get("rejection_message", ""), (
        f"rejection_message={result.get('rejection_message', 'MISSING')}"
    )


def test_empty_mapping_fails():
    """Test 4: Empty mapping fails (45 source, 0 mapped)."""
    gate = COFACompletionGate()
    source = make_source_coa(45)
    result = gate.validate_mapping_completeness(source, [])
    assert result["complete"] is False, f"complete={result['complete']}"
    assert len(result["orphaned_accounts"]) == 45, f"orphaned={len(result['orphaned_accounts'])}"


def test_empty_source_rejects():
    """Test 5: Empty source rejects (0 source, 0 mapped)."""
    gate = COFACompletionGate()
    result = gate.validate_mapping_completeness([], [])
    assert result["complete"] is False, f"got complete={result['complete']}"
    assert "empty" in result["message"].lower(), f"message={result['message']}"


def test_misconfigured_source_key_rejects():
    """Test 6: Misconfigured source_key rejects (key typo)."""
    gate = COFACompletionGate()
    source = make_source_coa(10)
    mappings = make_mappings_from_coa(source)
    result = gate.validate_mapping_completeness(source, mappings, source_key="acct_num")
    assert result["complete"] is False, f"got complete={result['complete']}"
    assert "acct_num" in result["message"], f"message={result['message']}"


def test_blank_account_values_reject():
    """Test 7: All-blank account_number values reject."""
    gate = COFACompletionGate()
    blank_source = [{"account_number": "", "account_name": "Blank"} for _ in range(5)]
    result = gate.validate_mapping_completeness(blank_source, [])
    assert result["complete"] is False, f"got complete={result['complete']}"
    assert "zero" in result["message"].lower(), f"message={result['message']}"
