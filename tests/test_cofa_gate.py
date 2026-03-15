"""
COFA Completeness Gate Tests
=============================
5 test cases validating the COFACompletionGate.
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


gate = COFACompletionGate()
passed = 0
failed = 0
total = 5


def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        print(f"  [PASS] {name}")
        passed += 1
    else:
        print(f"  [FAIL] {name} — {detail}")
        failed += 1


# -----------------------------------------------------------------------
# Test 1: Complete mapping passes
# -----------------------------------------------------------------------
print("\nTest 1: Complete mapping passes (45 source, 45 mapped)")
source = make_source_coa(45)
mappings = make_mappings_from_coa(source)
result = gate.validate_mapping_completeness(source, mappings)
check(
    "complete=true",
    result["complete"] is True,
    f"got complete={result['complete']}",
)

# -----------------------------------------------------------------------
# Test 2: Incomplete mapping fails (missing account 6200)
# -----------------------------------------------------------------------
print("\nTest 2: Incomplete mapping fails (45 source, 44 mapped, missing index 4)")
source = make_source_coa(45)
# Skip index 4 → account_number "1400"
mappings = make_mappings_from_coa(source, skip_indices=[4])
result = gate.validate_and_reject(source, mappings)
check(
    "complete=false",
    result["complete"] is False,
    f"got complete={result['complete']}",
)

# -----------------------------------------------------------------------
# Test 3: Rejection message includes account details
# -----------------------------------------------------------------------
print("\nTest 3: Rejection message includes account details")
orphaned = result.get("orphaned_accounts", [])
has_account_number = len(orphaned) > 0 and "account_number" in orphaned[0]
has_account_name = len(orphaned) > 0 and "account_name" in orphaned[0]
has_rejection_msg = "rejection_message" in result and "1400" in result.get("rejection_message", "")
check(
    "orphan has account_number and account_name and rejection_message cites it",
    has_account_number and has_account_name and has_rejection_msg,
    f"orphaned={orphaned}, rejection_message={result.get('rejection_message', 'MISSING')}",
)

# -----------------------------------------------------------------------
# Test 4: Empty mapping fails
# -----------------------------------------------------------------------
print("\nTest 4: Empty mapping fails (45 source, 0 mapped)")
source = make_source_coa(45)
result = gate.validate_mapping_completeness(source, [])
check(
    "complete=false, 45 orphaned",
    result["complete"] is False and len(result["orphaned_accounts"]) == 45,
    f"complete={result['complete']}, orphaned={len(result['orphaned_accounts'])}",
)

# -----------------------------------------------------------------------
# Test 5: Empty source passes
# -----------------------------------------------------------------------
print("\nTest 5: Empty source passes (0 source, 0 mapped)")
result = gate.validate_mapping_completeness([], [])
check(
    "complete=true",
    result["complete"] is True,
    f"got complete={result['complete']}",
)

# -----------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------
print(f"\n{'=' * 50}")
print(f"COFA Gate Tests: {passed}/{total} passed")
if failed > 0:
    print(f"FAILURES: {failed}")
    sys.exit(1)
else:
    print("All tests passed.")
    sys.exit(0)
